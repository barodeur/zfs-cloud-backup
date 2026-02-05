mod crypto;
mod plan;
mod s3;
mod types;
mod zfs;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::{Parser, Subcommand};
use ::s3::Bucket;
use tokio::io::AsyncWriteExt;

use crate::s3::S3Config;
use crate::types::SendPlan;

#[derive(Parser)]
#[command(name = "zfs-cloud-backup", about = "Encrypted ZFS snapshots to S3")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Send a ZFS snapshot (full or incremental) to S3
    Send {
        #[arg(long, env = "ZCB_DATASET")]
        dataset: String,

        #[arg(long, env = "ZCB_BUCKET")]
        bucket: String,

        #[arg(long, env = "ZCB_ENDPOINT")]
        endpoint: String,

        #[arg(long, env = "ZCB_REGION", default_value = "auto")]
        region: String,

        #[arg(long, env = "ZCB_PREFIX", default_value = "")]
        prefix: String,

        #[arg(long, env = "ZCB_AGE_RECIPIENT")]
        age_recipient: String,

        /// Max age before forcing a new full backup (e.g. "7d", "24h")
        #[arg(long, env = "ZCB_FULL_INTERVAL", default_value = "7d")]
        full_interval: String,

        /// Use recursive ZFS send (-R flag)
        #[arg(long)]
        recursive: bool,
    },

    /// List backups stored in S3
    List {
        #[arg(long, env = "ZCB_DATASET")]
        dataset: String,

        #[arg(long, env = "ZCB_BUCKET")]
        bucket: String,

        #[arg(long, env = "ZCB_ENDPOINT")]
        endpoint: String,

        #[arg(long, env = "ZCB_REGION", default_value = "auto")]
        region: String,

        #[arg(long, env = "ZCB_PREFIX", default_value = "")]
        prefix: String,
    },

    /// Restore a snapshot from S3
    Restore {
        #[arg(long, env = "ZCB_DATASET")]
        dataset: String,

        #[arg(long)]
        snapshot: String,

        #[arg(long, env = "ZCB_BUCKET")]
        bucket: String,

        #[arg(long, env = "ZCB_ENDPOINT")]
        endpoint: String,

        #[arg(long, env = "ZCB_REGION", default_value = "auto")]
        region: String,

        #[arg(long, env = "ZCB_PREFIX", default_value = "")]
        prefix: String,

        #[arg(long, env = "ZCB_AGE_IDENTITY")]
        age_identity: String,

        /// Force receive (zfs receive -F)
        #[arg(long)]
        force: bool,
    },

    /// Prune old backup chains beyond retention
    Prune {
        #[arg(long, env = "ZCB_DATASET")]
        dataset: String,

        #[arg(long, env = "ZCB_BUCKET")]
        bucket: String,

        #[arg(long, env = "ZCB_ENDPOINT")]
        endpoint: String,

        #[arg(long, env = "ZCB_REGION", default_value = "auto")]
        region: String,

        #[arg(long, env = "ZCB_PREFIX", default_value = "")]
        prefix: String,

        /// Number of full backup chains to keep
        #[arg(long, default_value = "4")]
        keep_full: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Send {
            dataset,
            bucket,
            endpoint,
            region,
            prefix,
            age_recipient,
            full_interval,
            recursive,
        } => {
            cmd_send(
                &dataset,
                &S3Config {
                    bucket,
                    endpoint,
                    region,
                    prefix,
                },
                &age_recipient,
                &full_interval,
                recursive,
            )
            .await
        }
        Commands::List {
            dataset,
            bucket,
            endpoint,
            region,
            prefix,
        } => {
            cmd_list(
                &dataset,
                &S3Config {
                    bucket,
                    endpoint,
                    region,
                    prefix,
                },
            )
            .await
        }
        Commands::Restore {
            dataset,
            snapshot,
            bucket,
            endpoint,
            region,
            prefix,
            age_identity,
            force,
        } => {
            cmd_restore(
                &dataset,
                &snapshot,
                &S3Config {
                    bucket,
                    endpoint,
                    region,
                    prefix,
                },
                &age_identity,
                force,
            )
            .await
        }
        Commands::Prune {
            dataset,
            bucket,
            endpoint,
            region,
            prefix,
            keep_full,
        } => {
            cmd_prune(
                &dataset,
                &S3Config {
                    bucket,
                    endpoint,
                    region,
                    prefix,
                },
                keep_full,
            )
            .await
        }
    }
}

async fn cmd_send(
    dataset: &str,
    s3cfg: &S3Config,
    age_recipient: &str,
    full_interval: &str,
    recursive: bool,
) -> Result<()> {
    let interval = humantime::parse_duration(full_interval)
        .context("invalid --full-interval format (try e.g. '7d' or '24h')")?;

    eprintln!("listing local snapshots for {}...", dataset);
    let local_snaps = zfs::list_snapshots(dataset).await?;
    if local_snaps.is_empty() {
        bail!("no snapshots found for dataset {}", dataset);
    }
    eprintln!("  found {} snapshots", local_snaps.len());

    let bucket = s3::create_bucket(s3cfg)?;
    let ds_prefix = s3::dataset_prefix(&s3cfg.prefix, dataset);
    eprintln!("listing S3 objects under {}...", ds_prefix);
    let objects = s3::list_objects(&bucket, &ds_prefix).await?;
    let entries = plan::parse_all_entries(&objects, &s3cfg.prefix, dataset);
    eprintln!("  found {} backup entries", entries.len());

    let send_plan = plan::decide_send(&entries, &local_snaps, interval, Utc::now())?;

    match &send_plan {
        SendPlan::NothingToDo => {
            eprintln!("nothing to do — latest snapshot already backed up");
            return Ok(());
        }
        SendPlan::Full { snapshot } => {
            eprintln!("plan: full send of {}@{}", dataset, snapshot);
            let key = format!("{}/full/{}.zfs.age", ds_prefix, snapshot);

            let child = zfs::spawn_zfs_send_full(dataset, snapshot, recursive)?;
            let stdout = child.stdout.context("no stdout from zfs send")?;
            let reader = stdout.into_owned_fd().context("cannot get owned fd")?;
            let reader = std::fs::File::from(reader);

            let encrypted = crypto::encrypt_stream(reader, age_recipient)?;

            eprintln!("uploading to s3://{}...", key);
            s3::multipart_upload(&bucket, &key, encrypted).await?;
            eprintln!("done: full backup uploaded to {}", key);
        }
        SendPlan::Incremental {
            base_snapshot,
            target_snapshot,
        } => {
            eprintln!(
                "plan: incremental send {}@{} -> {}@{}",
                dataset, base_snapshot, dataset, target_snapshot
            );
            let key = format!(
                "{}/incr/{}..{}.zfs.age",
                ds_prefix, base_snapshot, target_snapshot
            );

            let child = zfs::spawn_zfs_send_incremental(
                dataset,
                base_snapshot,
                target_snapshot,
                recursive,
            )?;
            let stdout = child.stdout.context("no stdout from zfs send")?;
            let reader = stdout.into_owned_fd().context("cannot get owned fd")?;
            let reader = std::fs::File::from(reader);

            let encrypted = crypto::encrypt_stream(reader, age_recipient)?;

            eprintln!("uploading to s3://{}...", key);
            s3::multipart_upload(&bucket, &key, encrypted).await?;
            eprintln!("done: incremental backup uploaded to {}", key);
        }
    }

    Ok(())
}

async fn cmd_list(dataset: &str, s3cfg: &S3Config) -> Result<()> {
    let bucket = s3::create_bucket(s3cfg)?;
    let ds_prefix = s3::dataset_prefix(&s3cfg.prefix, dataset);
    let objects = s3::list_objects(&bucket, &ds_prefix).await?;
    let mut entries = plan::parse_all_entries(&objects, &s3cfg.prefix, dataset);

    if entries.is_empty() {
        eprintln!("no backups found for {}", dataset);
        return Ok(());
    }

    entries.sort_by(|a, b| a.last_modified.cmp(&b.last_modified));

    println!(
        "{:<6} {:<40} {:<40} {:>10} {}",
        "TYPE", "SNAPSHOT", "BASE", "SIZE", "DATE"
    );
    for entry in &entries {
        let type_str = match &entry.backup_type {
            types::BackupType::Full => "full",
            types::BackupType::Incremental { .. } => "incr",
        };
        let base = match &entry.backup_type {
            types::BackupType::Full => "—".to_string(),
            types::BackupType::Incremental { base_snapshot } => base_snapshot.clone(),
        };
        let size_mb = entry.size as f64 / 1_048_576.0;
        println!(
            "{:<6} {:<40} {:<40} {:>8.1}MB {}",
            type_str,
            entry.snapshot,
            base,
            size_mb,
            entry.last_modified.format("%Y-%m-%d %H:%M")
        );
    }

    Ok(())
}

async fn cmd_restore(
    dataset: &str,
    snapshot: &str,
    s3cfg: &S3Config,
    age_identity: &str,
    force: bool,
) -> Result<()> {
    let bucket = s3::create_bucket(s3cfg)?;
    let ds_prefix = s3::dataset_prefix(&s3cfg.prefix, dataset);

    // List objects under the dataset prefix to find backup entries
    let objects = s3::list_objects(&bucket, &ds_prefix).await?;
    let entries = plan::parse_all_entries(&objects, &s3cfg.prefix, dataset);

    if entries.is_empty() {
        // Try scanning all objects to find the snapshot in any dataset
        let all_objects = s3::list_objects(&bucket, &s3cfg.prefix).await?;
        let mut found_entries = Vec::new();
        let mut source_dataset = None;

        for obj in &all_objects {
            if let Some(ds) = extract_dataset_from_key(&obj.key, &s3cfg.prefix) {
                if let Some(entry) = plan::parse_backup_entry(obj, &s3cfg.prefix, &ds) {
                    if entry.snapshot == snapshot {
                        source_dataset = Some(ds.clone());
                    }
                    found_entries.push(entry);
                }
            }
        }

        let source_ds = source_dataset.context("could not find snapshot in S3")?;
        let chain_entries: Vec<_> = found_entries
            .into_iter()
            .filter(|e| e.dataset == source_ds)
            .collect();

        let chain = plan::build_restore_chain(&chain_entries, snapshot)?;
        return run_restore_chain(&bucket, &chain, age_identity, dataset, force).await;
    }

    let chain = plan::build_restore_chain(&entries, snapshot)?;
    run_restore_chain(&bucket, &chain, age_identity, dataset, force).await
}

async fn run_restore_chain(
    bucket: &Bucket,
    chain: &types::RestoreChain,
    age_identity: &str,
    dataset: &str,
    force: bool,
) -> Result<()> {
    eprintln!(
        "restore chain: 1 full + {} incrementals",
        chain.incrementals.len()
    );

    eprintln!("restoring full: {}...", chain.full.key);
    restore_one(bucket, &chain.full.key, age_identity, dataset, force).await?;

    for incr in &chain.incrementals {
        eprintln!("restoring incremental: {}...", incr.key);
        restore_one(bucket, &incr.key, age_identity, dataset, force).await?;
    }

    eprintln!("restore complete");
    Ok(())
}

async fn restore_one(
    bucket: &Bucket,
    key: &str,
    age_identity: &str,
    dataset: &str,
    force: bool,
) -> Result<()> {
    // Download the encrypted backup into memory
    let response = bucket
        .get_object(key)
        .await
        .context("failed to download backup")?;

    if response.status_code() != 200 {
        bail!("S3 download returned status {}", response.status_code());
    }

    let data = response.to_vec();
    eprintln!(
        "  downloaded {:.1} MB, decrypting...",
        data.len() as f64 / 1_048_576.0
    );

    // Decrypt in memory
    let mut decrypted = crypto::decrypt_reader(std::io::Cursor::new(data), age_identity)?;
    let mut plaintext = Vec::new();
    std::io::Read::read_to_end(&mut decrypted, &mut plaintext)?;

    // Pipe plaintext into zfs receive
    let mut child = zfs::spawn_zfs_receive(dataset, force)?;
    let mut stdin = child.stdin.take().context("no stdin for zfs receive")?;

    stdin.write_all(&plaintext).await?;
    drop(stdin);

    let output = child.wait_with_output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("zfs receive failed: {}", stderr.trim());
    }

    Ok(())
}

fn extract_dataset_from_key(key: &str, prefix: &str) -> Option<String> {
    let rest = if prefix.is_empty() {
        key
    } else {
        key.strip_prefix(&format!("{}/", prefix.trim_end_matches('/')))?
    };

    if let Some(idx) = rest.find("/full/") {
        Some(rest[..idx].to_string())
    } else if let Some(idx) = rest.find("/incr/") {
        Some(rest[..idx].to_string())
    } else {
        None
    }
}

async fn cmd_prune(dataset: &str, s3cfg: &S3Config, keep_full: usize) -> Result<()> {
    let bucket = s3::create_bucket(s3cfg)?;
    let ds_prefix = s3::dataset_prefix(&s3cfg.prefix, dataset);
    let objects = s3::list_objects(&bucket, &ds_prefix).await?;
    let entries = plan::parse_all_entries(&objects, &s3cfg.prefix, dataset);

    let to_remove = plan::plan_prune(&entries, keep_full);

    if to_remove.is_empty() {
        eprintln!("nothing to prune");
        return Ok(());
    }

    eprintln!("will delete {} objects:", to_remove.len());
    for entry in &to_remove {
        eprintln!("  {}", entry.key);
    }

    for entry in &to_remove {
        s3::delete_object(&bucket, &entry.key).await?;
        eprintln!("  deleted {}", entry.key);
    }

    eprintln!("prune complete");
    Ok(())
}
