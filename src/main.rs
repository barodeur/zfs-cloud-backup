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
use crate::types::{BackupMode, SendPlan};

#[derive(Parser)]
#[command(name = "zfs-cloud-backup", version = env!("GIT_VERSION"), about = "Encrypted ZFS snapshots to S3")]
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

        /// Backup mode: single (default), replication (-R), or individual (per-dataset)
        #[arg(long, env = "ZCB_MODE", default_value = "single", value_enum)]
        mode: BackupMode,

        /// Deprecated: use --mode replication instead
        #[arg(long, hide = true)]
        replication: bool,

        /// Raw mode: send encrypted datasets without decrypting (-w)
        #[arg(long, env = "ZCB_RAW")]
        raw: bool,
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

        /// Restore only this descendant (relative path, e.g. "child" or "child/grandchild")
        #[arg(long)]
        target: Option<String>,
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
            mode,
            replication,
            raw,
        } => {
            // --replication flag overrides --mode for backward compatibility
            let effective_mode = if replication {
                eprintln!("warning: --replication is deprecated, use --mode replication");
                BackupMode::Replication
            } else {
                mode
            };

            let s3cfg = S3Config {
                bucket,
                endpoint,
                region,
                prefix,
            };

            match effective_mode {
                BackupMode::Individual => {
                    cmd_send_individual(&dataset, &s3cfg, &age_recipient, &full_interval, raw).await
                }
                _ => {
                    let repl = effective_mode == BackupMode::Replication;
                    send_one_dataset(&dataset, &s3cfg, &age_recipient, &full_interval, repl, raw).await
                }
            }
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
            target,
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
                target.as_deref(),
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

/// Send a single dataset to S3 (used by both single and replication modes,
/// and called in a loop by individual mode).
async fn send_one_dataset(
    dataset: &str,
    s3cfg: &S3Config,
    age_recipient: &str,
    full_interval: &str,
    replication: bool,
    raw: bool,
) -> Result<()> {
    let interval = humantime::parse_duration(full_interval)
        .context("invalid --full-interval format (try e.g. '7d' or '24h')")?;

    eprintln!("listing local snapshots for {}...", dataset);
    let all_snaps = zfs::list_snapshots(dataset).await?;

    // Always plan against the exact dataset's snapshots — child dataset
    // snapshots may have different names and would confuse decide_send.
    // The replication flag only controls zfs send options (-R / -R -I).
    let local_snaps: Vec<_> = all_snaps
        .into_iter()
        .filter(|s| s.dataset == dataset)
        .collect();

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

            let mut child = zfs::spawn_zfs_send_full(dataset, snapshot, replication, raw)?;
            let stdout = child.stdout.take().context("no stdout from zfs send")?;
            let reader = stdout.into_owned_fd().context("cannot get owned fd")?;
            let reader = std::fs::File::from(reader);

            let encrypted = crypto::encrypt_stream(reader, age_recipient)?;

            eprintln!("uploading to s3://{}...", key);
            s3::multipart_upload(&bucket, &key, encrypted).await?;

            let output = child.wait_with_output().await.context("failed to wait for zfs send")?;
            if !output.status.success() {
                let _ = s3::delete_object(&bucket, &key).await;
                let stderr = String::from_utf8_lossy(&output.stderr);
                bail!("zfs send failed: {}", stderr.trim());
            }

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

            let mut child = zfs::spawn_zfs_send_incremental(
                dataset,
                base_snapshot,
                target_snapshot,
                replication,
                raw,
            )?;
            let stdout = child.stdout.take().context("no stdout from zfs send")?;
            let reader = stdout.into_owned_fd().context("cannot get owned fd")?;
            let reader = std::fs::File::from(reader);

            let encrypted = crypto::encrypt_stream(reader, age_recipient)?;

            eprintln!("uploading to s3://{}...", key);
            s3::multipart_upload(&bucket, &key, encrypted).await?;

            let output = child.wait_with_output().await.context("failed to wait for zfs send")?;
            if !output.status.success() {
                let _ = s3::delete_object(&bucket, &key).await;
                let stderr = String::from_utf8_lossy(&output.stderr);
                bail!("zfs send failed: {}", stderr.trim());
            }

            eprintln!("done: incremental backup uploaded to {}", key);
        }
    }

    Ok(())
}

/// Send each descendant dataset as its own independent backup chain.
async fn cmd_send_individual(
    dataset: &str,
    s3cfg: &S3Config,
    age_recipient: &str,
    full_interval: &str,
    raw: bool,
) -> Result<()> {
    eprintln!("individual mode: enumerating datasets under {}...", dataset);
    let descendants = zfs::list_descendants(dataset).await?;

    let mut all_datasets = vec![dataset.to_string()];
    all_datasets.extend(descendants);
    eprintln!(
        "  found {} datasets to back up",
        all_datasets.len()
    );

    let mut errors: Vec<(String, anyhow::Error)> = Vec::new();

    for ds in &all_datasets {
        eprintln!("\n--- backing up {} ---", ds);
        if let Err(e) = send_one_dataset(ds, s3cfg, age_recipient, full_interval, false, raw).await {
            eprintln!("ERROR backing up {}: {:#}", ds, e);
            errors.push((ds.clone(), e));
        }
    }

    if errors.is_empty() {
        eprintln!("\nall {} datasets backed up successfully", all_datasets.len());
        Ok(())
    } else {
        eprintln!("\n{}/{} datasets failed:", errors.len(), all_datasets.len());
        for (ds, e) in &errors {
            eprintln!("  {}: {:#}", ds, e);
        }
        bail!(
            "{} of {} datasets failed to back up",
            errors.len(),
            all_datasets.len()
        );
    }
}

async fn cmd_list(dataset: &str, s3cfg: &S3Config) -> Result<()> {
    let bucket = s3::create_bucket(s3cfg)?;
    let ds_prefix = s3::dataset_prefix(&s3cfg.prefix, dataset);
    let objects = s3::list_objects(&bucket, &ds_prefix).await?;

    let datasets = plan::discover_datasets_in_objects(&objects, &s3cfg.prefix, dataset);

    if datasets.is_empty() {
        eprintln!("no backups found for {}", dataset);
        return Ok(());
    }

    let multi = datasets.len() > 1;

    for ds in &datasets {
        let mut entries = plan::parse_all_entries(&objects, &s3cfg.prefix, ds);
        if entries.is_empty() {
            continue;
        }
        entries.sort_by(|a, b| a.last_modified.cmp(&b.last_modified));

        if multi {
            println!("\n{}:", ds);
        }

        println!(
            "{:<6} {:<40} {:<40} {:>10} DATE",
            "TYPE", "SNAPSHOT", "BASE", "SIZE",
        );
        for entry in &entries {
            let type_str = match &entry.backup_type {
                types::BackupType::Full => "full",
                types::BackupType::Incremental { .. } => "incr",
            };
            let base = match &entry.backup_type {
                types::BackupType::Full => "\u{2014}".to_string(),
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
    }

    Ok(())
}

async fn cmd_restore(
    dataset: &str,
    snapshot: &str,
    s3cfg: &S3Config,
    age_identity: &str,
    force: bool,
    target: Option<&str>,
) -> Result<()> {
    let bucket = s3::create_bucket(s3cfg)?;
    let ds_prefix = s3::dataset_prefix(&s3cfg.prefix, dataset);

    // List objects under the dataset prefix to find backup entries
    let objects = s3::list_objects(&bucket, &ds_prefix).await?;
    let datasets = plan::discover_datasets_in_objects(&objects, &s3cfg.prefix, dataset);

    if datasets.is_empty() {
        // Fallback: try scanning all objects to find the snapshot
        let all_objects = s3::list_objects(&bucket, &s3cfg.prefix).await?;
        let mut found_entries = Vec::new();
        let mut source_dataset = None;

        for obj in &all_objects {
            if let Some(ds) = plan::extract_dataset_from_key(&obj.key, &s3cfg.prefix) {
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

    // If --target is specified, restore only that one descendant
    if let Some(rel_target) = target {
        let full_target = format!("{}/{}", dataset, rel_target);
        if !datasets.iter().any(|ds| ds == &full_target) {
            bail!(
                "target '{}' not found in S3 (available: {})",
                rel_target,
                datasets
                    .iter()
                    .filter(|ds| *ds != dataset)
                    .map(|ds| ds.strip_prefix(&format!("{}/", dataset)).unwrap_or(ds))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        let entries = plan::parse_all_entries(&objects, &s3cfg.prefix, &full_target);
        let chain = plan::build_restore_chain(&entries, snapshot)?;
        return run_restore_chain(&bucket, &chain, age_identity, &full_target, force).await;
    }

    // Multiple datasets (individual mode backups) — restore each one
    if datasets.len() > 1 {
        eprintln!(
            "restoring {} datasets under {}...",
            datasets.len(),
            dataset
        );
        for ds in &datasets {
            eprintln!("\n--- restoring {} ---", ds);
            let entries = plan::parse_all_entries(&objects, &s3cfg.prefix, ds);
            let chain = plan::build_restore_chain(&entries, snapshot)?;
            run_restore_chain(&bucket, &chain, age_identity, ds, force).await?;
        }
        eprintln!("\nall {} datasets restored", datasets.len());
        return Ok(());
    }

    // Single dataset — original behavior
    let entries = plan::parse_all_entries(&objects, &s3cfg.prefix, dataset);
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

async fn cmd_prune(dataset: &str, s3cfg: &S3Config, keep_full: usize) -> Result<()> {
    let bucket = s3::create_bucket(s3cfg)?;
    let ds_prefix = s3::dataset_prefix(&s3cfg.prefix, dataset);
    let objects = s3::list_objects(&bucket, &ds_prefix).await?;

    let datasets = plan::discover_datasets_in_objects(&objects, &s3cfg.prefix, dataset);

    if datasets.is_empty() {
        eprintln!("nothing to prune");
        return Ok(());
    }

    let mut total_removed = 0;

    for ds in &datasets {
        let entries = plan::parse_all_entries(&objects, &s3cfg.prefix, ds);
        let to_remove = plan::plan_prune(&entries, keep_full);

        if to_remove.is_empty() {
            continue;
        }

        if datasets.len() > 1 {
            eprintln!("\n{}:", ds);
        }
        eprintln!("will delete {} objects:", to_remove.len());
        for entry in &to_remove {
            eprintln!("  {}", entry.key);
        }

        for entry in &to_remove {
            s3::delete_object(&bucket, &entry.key).await?;
            eprintln!("  deleted {}", entry.key);
        }

        total_removed += to_remove.len();
    }

    if total_removed == 0 {
        eprintln!("nothing to prune");
    } else {
        eprintln!("prune complete: deleted {} objects", total_removed);
    }

    Ok(())
}
