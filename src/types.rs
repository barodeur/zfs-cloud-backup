use chrono::{DateTime, Utc};

/// Backup mode: how datasets are sent to S3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum BackupMode {
    /// Send only the named dataset as a single stream
    Single,
    /// Send a monolithic `zfs send -R` replication stream (includes children)
    Replication,
    /// Send each descendant dataset as its own independent backup chain
    Individual,
}

/// A ZFS snapshot as reported by `zfs list -t snapshot`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// Full snapshot name, e.g. "pool/data@autosnap_2026-02-05_03.00.00_hourly"
    pub full_name: String,
    /// Dataset portion, e.g. "pool/data"
    pub dataset: String,
    /// Snapshot name portion (after @), e.g. "autosnap_2026-02-05_03.00.00_hourly"
    pub snap_name: String,
    /// Creation timestamp
    pub creation: DateTime<Utc>,
}

/// Type of backup stored in S3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupType {
    Full,
    Incremental { base_snapshot: String },
}

/// A backup entry parsed from an S3 object key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupEntry {
    /// The dataset this backup belongs to
    pub dataset: String,
    /// The target snapshot name
    pub snapshot: String,
    /// Full or incremental (with base snapshot name)
    pub backup_type: BackupType,
    /// The full S3 object key
    pub key: String,
    /// Object size in bytes
    pub size: u64,
    /// Last modified timestamp
    pub last_modified: DateTime<Utc>,
}

/// What the plan module decides to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendPlan {
    Full {
        snapshot: String,
    },
    Incremental {
        base_snapshot: String,
        target_snapshot: String,
    },
    NothingToDo,
}

/// A chain of backups needed to restore to a specific snapshot.
#[derive(Debug, Clone)]
pub struct RestoreChain {
    pub full: BackupEntry,
    pub incrementals: Vec<BackupEntry>,
}
