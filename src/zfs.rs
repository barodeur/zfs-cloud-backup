use anyhow::{Context, Result, bail};
use chrono::{TimeZone, Utc};
use std::process::Stdio;
use tokio::process::Command;

use crate::types::SnapshotInfo;

/// List all snapshots for a given dataset, sorted by creation time.
pub async fn list_snapshots(dataset: &str) -> Result<Vec<SnapshotInfo>> {
    let output = Command::new("zfs")
        .args(["list", "-t", "snapshot", "-H", "-p", "-o", "name,creation", "-s", "creation", "-r"])
        .arg(dataset)
        .output()
        .await
        .context("failed to run zfs list")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("zfs list failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8(output.stdout).context("invalid utf-8 from zfs list")?;
    let mut snapshots = Vec::new();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let full_name = parts.next().context("missing snapshot name")?.to_string();
        let creation_str = parts.next().context("missing creation timestamp")?;

        // -p flag gives us epoch seconds
        let epoch: i64 = creation_str.trim().parse().context("invalid creation timestamp")?;
        let creation = Utc
            .timestamp_opt(epoch, 0)
            .single()
            .context("invalid epoch timestamp")?;

        let at_pos = full_name.find('@').context("snapshot name missing @")?;
        let dataset_part = &full_name[..at_pos];
        let snap_part = &full_name[at_pos + 1..];

        // Only include exact dataset matches (not child datasets) unless they match the prefix
        if dataset_part == dataset || dataset_part.starts_with(&format!("{}/", dataset)) {
            snapshots.push(SnapshotInfo {
                dataset: dataset_part.to_string(),
                snap_name: snap_part.to_string(),
                full_name,
                creation,
            });
        }
    }

    Ok(snapshots)
}

/// List all descendant datasets under `dataset` (excluding the dataset itself).
pub async fn list_descendants(dataset: &str) -> Result<Vec<String>> {
    let output = Command::new("zfs")
        .args(["list", "-r", "-H", "-o", "name"])
        .arg(dataset)
        .output()
        .await
        .context("failed to run zfs list")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("zfs list failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8(output.stdout).context("invalid utf-8 from zfs list")?;
    let descendants: Vec<String> = stdout
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|name| !name.is_empty() && name != dataset)
        .collect();

    Ok(descendants)
}

/// Spawn `zfs send` for a full snapshot. Returns the child process with stdout piped.
/// When `replication` is true, passes `-R` to include child datasets and properties.
pub fn spawn_zfs_send_full(
    dataset: &str,
    snapshot: &str,
    replication: bool,
) -> Result<tokio::process::Child> {
    let snap_ref = format!("{}@{}", dataset, snapshot);
    let mut cmd = std::process::Command::new("zfs");
    cmd.arg("send");
    if replication {
        cmd.arg("-R");
    }
    cmd.arg(&snap_ref);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let child = tokio::process::Command::from(cmd)
        .spawn()
        .with_context(|| format!("failed to spawn zfs send {}", snap_ref))?;

    Ok(child)
}

/// Spawn `zfs send` for an incremental snapshot. Returns the child process with stdout piped.
/// When `replication` is true, passes `-R -I` to include child datasets and all
/// intermediate snapshots. Otherwise uses `-i` for a single delta.
pub fn spawn_zfs_send_incremental(
    dataset: &str,
    base_snapshot: &str,
    target_snapshot: &str,
    replication: bool,
) -> Result<tokio::process::Child> {
    let base_ref = format!("{}@{}", dataset, base_snapshot);
    let target_ref = format!("{}@{}", dataset, target_snapshot);
    let mut cmd = std::process::Command::new("zfs");
    cmd.arg("send");
    if replication {
        cmd.args(["-R", "-I", &base_ref, &target_ref]);
    } else {
        cmd.args(["-i", &base_ref, &target_ref]);
    }
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let child = tokio::process::Command::from(cmd)
        .spawn()
        .with_context(|| format!("failed to spawn zfs send incremental {} {}", base_ref, target_ref))?;

    Ok(child)
}

/// Spawn `zfs receive` that reads from stdin. Returns the child process with stdin piped.
pub fn spawn_zfs_receive(dataset: &str, force: bool) -> Result<tokio::process::Child> {
    let mut cmd = std::process::Command::new("zfs");
    cmd.arg("receive");
    if force {
        cmd.arg("-F");
    }
    cmd.arg(dataset);
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let child = tokio::process::Command::from(cmd)
        .spawn()
        .with_context(|| format!("failed to spawn zfs receive {}", dataset))?;

    Ok(child)
}
