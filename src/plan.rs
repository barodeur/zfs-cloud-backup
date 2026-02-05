use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use s3::serde_types::Object;

use crate::types::{BackupEntry, BackupType, RestoreChain, SendPlan, SnapshotInfo};

/// Parse an S3 object key into a BackupEntry.
/// Expected formats:
///   {prefix}/{dataset}/full/{snapshot-name}.zfs.age
///   {prefix}/{dataset}/incr/{base-snapshot}..{target-snapshot}.zfs.age
pub fn parse_backup_entry(obj: &Object, prefix: &str, dataset: &str) -> Option<BackupEntry> {
    let key = &obj.key;
    let dataset_prefix = if prefix.is_empty() {
        format!("{}/", dataset)
    } else {
        format!("{}/{}/", prefix.trim_end_matches('/'), dataset)
    };

    let rest = key.strip_prefix(&dataset_prefix)?;

    let (backup_type, snapshot) = if let Some(name) = rest.strip_prefix("full/") {
        let snap = name.strip_suffix(".zfs.age")?;
        (BackupType::Full, snap.to_string())
    } else if let Some(name) = rest.strip_prefix("incr/") {
        let raw = name.strip_suffix(".zfs.age")?;
        let (base, target) = raw.split_once("..")?;
        (
            BackupType::Incremental {
                base_snapshot: base.to_string(),
            },
            target.to_string(),
        )
    } else {
        return None;
    };

    let last_modified = DateTime::parse_from_rfc3339(&obj.last_modified)
        .or_else(|_| DateTime::parse_from_rfc2822(&obj.last_modified))
        .ok()?
        .with_timezone(&Utc);

    Some(BackupEntry {
        dataset: dataset.to_string(),
        snapshot,
        backup_type,
        key: key.clone(),
        size: obj.size,
        last_modified,
    })
}

/// Parse all S3 objects into backup entries for the given dataset.
pub fn parse_all_entries(objects: &[Object], prefix: &str, dataset: &str) -> Vec<BackupEntry> {
    objects
        .iter()
        .filter_map(|obj| parse_backup_entry(obj, prefix, dataset))
        .collect()
}

/// Determine whether to do a full or incremental send.
pub fn decide_send(
    entries: &[BackupEntry],
    local_snapshots: &[SnapshotInfo],
    full_interval: std::time::Duration,
    now: DateTime<Utc>,
) -> Result<SendPlan> {
    if local_snapshots.is_empty() {
        bail!("no local snapshots found for dataset");
    }

    let latest_local = &local_snapshots[local_snapshots.len() - 1];

    // Find the most recent full backup
    let last_full = entries
        .iter()
        .filter(|e| e.backup_type == BackupType::Full)
        .max_by_key(|e| &e.last_modified);

    let needs_full = match last_full {
        None => true,
        Some(full) => {
            let age = now
                .signed_duration_since(full.last_modified)
                .to_std()
                .unwrap_or(std::time::Duration::ZERO);
            age > full_interval
        }
    };

    if needs_full {
        return Ok(SendPlan::Full {
            snapshot: latest_local.snap_name.clone(),
        });
    }

    // Find the most recently sent snapshot (full or incremental)
    let last_sent = entries
        .iter()
        .max_by_key(|e| &e.last_modified)
        .map(|e| &e.snapshot);

    let last_sent = match last_sent {
        Some(s) => s,
        None => {
            return Ok(SendPlan::Full {
                snapshot: latest_local.snap_name.clone(),
            })
        }
    };

    // Check if the last sent snapshot still exists locally
    let base_exists = local_snapshots.iter().any(|s| s.snap_name == *last_sent);
    if !base_exists {
        eprintln!(
            "warning: last sent snapshot {} no longer exists locally, forcing full send",
            last_sent
        );
        return Ok(SendPlan::Full {
            snapshot: latest_local.snap_name.clone(),
        });
    }

    // If last sent is already the latest, nothing to do
    if *last_sent == latest_local.snap_name {
        return Ok(SendPlan::NothingToDo);
    }

    Ok(SendPlan::Incremental {
        base_snapshot: last_sent.clone(),
        target_snapshot: latest_local.snap_name.clone(),
    })
}

/// Build a restore chain for a specific target snapshot.
/// Returns the full backup and ordered list of incrementals needed.
pub fn build_restore_chain(
    entries: &[BackupEntry],
    target_snapshot: &str,
) -> Result<RestoreChain> {
    // Walk backwards from the target to find the chain
    let mut chain: Vec<BackupEntry> = Vec::new();
    let mut current = target_snapshot.to_string();

    loop {
        // Find the entry whose target snapshot matches `current`
        let entry = entries
            .iter()
            .find(|e| e.snapshot == current)
            .with_context(|| format!("cannot find backup for snapshot {}", current))?;

        match &entry.backup_type {
            BackupType::Full => {
                return Ok(RestoreChain {
                    full: entry.clone(),
                    incrementals: {
                        chain.reverse();
                        chain
                    },
                });
            }
            BackupType::Incremental { base_snapshot } => {
                chain.push(entry.clone());
                current = base_snapshot.clone();
            }
        }
    }
}

/// Identify full backup chains and return entries to prune, keeping `keep_full` most recent chains.
pub fn plan_prune(entries: &[BackupEntry], keep_full: usize) -> Vec<BackupEntry> {
    // Get all full backups, sorted newest first
    let mut fulls: Vec<&BackupEntry> = entries
        .iter()
        .filter(|e| e.backup_type == BackupType::Full)
        .collect();
    fulls.sort_by(|a, b| b.last_modified.cmp(&a.last_modified));

    if fulls.len() <= keep_full {
        return Vec::new();
    }

    // The full backups to remove
    let to_remove_fulls: Vec<&BackupEntry> = fulls[keep_full..].to_vec();
    let keep_snaps: std::collections::HashSet<String> = fulls[..keep_full]
        .iter()
        .map(|f| f.snapshot.clone())
        .collect();

    let mut to_remove = Vec::new();

    // Remove old full backups and their incremental chains
    for full in &to_remove_fulls {
        to_remove.push((*full).clone());

        // Find all incrementals that belong to this chain
        // An incremental belongs to a chain if walking back its base_snapshot leads to this full
        for entry in entries {
            if let BackupType::Incremental { base_snapshot } = &entry.backup_type {
                // Walk back to see if this incremental is rooted at a full we're removing
                let mut cur = base_snapshot.clone();
                let mut belongs_to_removed = false;
                let mut seen = std::collections::HashSet::new();
                loop {
                    if to_remove_fulls.iter().any(|f| f.snapshot == cur) {
                        belongs_to_removed = true;
                        break;
                    }
                    if keep_snaps.contains(&cur) {
                        break;
                    }
                    if !seen.insert(cur.clone()) {
                        break; // cycle guard
                    }
                    // Find the entry for `cur`
                    match entries.iter().find(|e| e.snapshot == cur) {
                        Some(e) => match &e.backup_type {
                            BackupType::Full => {
                                if to_remove_fulls.iter().any(|f| f.snapshot == e.snapshot) {
                                    belongs_to_removed = true;
                                }
                                break;
                            }
                            BackupType::Incremental { base_snapshot: bs } => {
                                cur = bs.clone();
                            }
                        },
                        None => break,
                    }
                }
                if belongs_to_removed {
                    to_remove.push(entry.clone());
                }
            }
        }
    }

    // Deduplicate
    to_remove.sort_by(|a, b| a.key.cmp(&b.key));
    to_remove.dedup_by(|a, b| a.key == b.key);

    to_remove
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_full(snapshot: &str, modified: &str) -> BackupEntry {
        BackupEntry {
            dataset: "pool/data".to_string(),
            snapshot: snapshot.to_string(),
            backup_type: BackupType::Full,
            key: format!("backup/pool/data/full/{}.zfs.age", snapshot),
            size: 1000,
            last_modified: DateTime::parse_from_rfc3339(modified)
                .unwrap()
                .with_timezone(&Utc),
        }
    }

    fn make_incr(base: &str, target: &str, modified: &str) -> BackupEntry {
        BackupEntry {
            dataset: "pool/data".to_string(),
            snapshot: target.to_string(),
            backup_type: BackupType::Incremental {
                base_snapshot: base.to_string(),
            },
            key: format!("backup/pool/data/incr/{}..{}.zfs.age", base, target),
            size: 500,
            last_modified: DateTime::parse_from_rfc3339(modified)
                .unwrap()
                .with_timezone(&Utc),
        }
    }

    fn make_snap(name: &str, epoch: i64) -> SnapshotInfo {
        SnapshotInfo {
            full_name: format!("pool/data@{}", name),
            dataset: "pool/data".to_string(),
            snap_name: name.to_string(),
            creation: Utc.timestamp_opt(epoch, 0).unwrap(),
        }
    }

    #[test]
    fn test_decide_send_no_backups() {
        let entries = vec![];
        let snaps = vec![make_snap("snap1", 1000)];
        let plan =
            decide_send(&entries, &snaps, std::time::Duration::from_secs(86400 * 7), Utc::now())
                .unwrap();
        assert_eq!(
            plan,
            SendPlan::Full {
                snapshot: "snap1".to_string()
            }
        );
    }

    #[test]
    fn test_decide_send_recent_full_no_new_snaps() {
        let entries = vec![make_full("snap1", "2026-02-05T00:00:00Z")];
        let snaps = vec![make_snap("snap1", 1000)];
        let now = Utc.with_ymd_and_hms(2026, 2, 5, 1, 0, 0).unwrap();
        let plan =
            decide_send(&entries, &snaps, std::time::Duration::from_secs(86400 * 7), now).unwrap();
        assert_eq!(plan, SendPlan::NothingToDo);
    }

    #[test]
    fn test_decide_send_incremental() {
        let entries = vec![make_full("snap1", "2026-02-05T00:00:00Z")];
        let snaps = vec![make_snap("snap1", 1000), make_snap("snap2", 2000)];
        let now = Utc.with_ymd_and_hms(2026, 2, 5, 1, 0, 0).unwrap();
        let plan =
            decide_send(&entries, &snaps, std::time::Duration::from_secs(86400 * 7), now).unwrap();
        assert_eq!(
            plan,
            SendPlan::Incremental {
                base_snapshot: "snap1".to_string(),
                target_snapshot: "snap2".to_string(),
            }
        );
    }

    #[test]
    fn test_decide_send_old_full_forces_new_full() {
        let entries = vec![make_full("snap1", "2026-01-01T00:00:00Z")];
        let snaps = vec![make_snap("snap1", 1000), make_snap("snap2", 2000)];
        let now = Utc.with_ymd_and_hms(2026, 2, 5, 0, 0, 0).unwrap();
        let plan =
            decide_send(&entries, &snaps, std::time::Duration::from_secs(86400 * 7), now).unwrap();
        assert_eq!(
            plan,
            SendPlan::Full {
                snapshot: "snap2".to_string()
            }
        );
    }

    #[test]
    fn test_build_restore_chain() {
        let entries = vec![
            make_full("snap1", "2026-02-01T00:00:00Z"),
            make_incr("snap1", "snap2", "2026-02-02T00:00:00Z"),
            make_incr("snap2", "snap3", "2026-02-03T00:00:00Z"),
        ];
        let chain = build_restore_chain(&entries, "snap3").unwrap();
        assert_eq!(chain.full.snapshot, "snap1");
        assert_eq!(chain.incrementals.len(), 2);
        assert_eq!(chain.incrementals[0].snapshot, "snap2");
        assert_eq!(chain.incrementals[1].snapshot, "snap3");
    }

    #[test]
    fn test_plan_prune_keeps_recent() {
        let entries = vec![
            make_full("snap1", "2026-01-01T00:00:00Z"),
            make_incr("snap1", "snap2", "2026-01-02T00:00:00Z"),
            make_full("snap3", "2026-02-01T00:00:00Z"),
            make_incr("snap3", "snap4", "2026-02-02T00:00:00Z"),
        ];
        let to_prune = plan_prune(&entries, 1);
        assert_eq!(to_prune.len(), 2); // snap1 full + snap1..snap2 incr
        assert!(to_prune.iter().any(|e| e.snapshot == "snap1"));
        assert!(to_prune.iter().any(|e| e.snapshot == "snap2"));
    }

    #[test]
    fn test_plan_prune_nothing_to_prune() {
        let entries = vec![
            make_full("snap1", "2026-01-01T00:00:00Z"),
            make_full("snap3", "2026-02-01T00:00:00Z"),
        ];
        let to_prune = plan_prune(&entries, 4);
        assert!(to_prune.is_empty());
    }

    #[test]
    fn test_parse_backup_entry_full() {
        let obj = Object {
            key: "backup/pool/data/full/snap1.zfs.age".to_string(),
            last_modified: "2026-02-01T00:00:00Z".to_string(),
            size: 1000,
            e_tag: None,
            storage_class: None,
            owner: None,
        };
        let entry = parse_backup_entry(&obj, "backup", "pool/data").unwrap();
        assert_eq!(entry.snapshot, "snap1");
        assert_eq!(entry.backup_type, BackupType::Full);
    }

    #[test]
    fn test_parse_backup_entry_incremental() {
        let obj = Object {
            key: "backup/pool/data/incr/snap1..snap2.zfs.age".to_string(),
            last_modified: "2026-02-02T00:00:00Z".to_string(),
            size: 500,
            e_tag: None,
            storage_class: None,
            owner: None,
        };
        let entry = parse_backup_entry(&obj, "backup", "pool/data").unwrap();
        assert_eq!(entry.snapshot, "snap2");
        assert_eq!(
            entry.backup_type,
            BackupType::Incremental {
                base_snapshot: "snap1".to_string()
            }
        );
    }
}
