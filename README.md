# zfs-cloud-backup

Encrypted ZFS snapshot backups to any S3-compatible storage.

Streams `zfs send` output through [age](https://age-encryption.org/) encryption and uploads it to S3 using multipart upload. Supports full and incremental backups, replication mode for nested datasets and zvols, and retention-based pruning.

## Features

- **Full & incremental backups** -- automatically decides based on `--full-interval`
- **age encryption** -- snapshots are encrypted before leaving the machine
- **Replication mode** -- `--replication` uses `zfs send -R` / `-R -I` to include child datasets, zvols, and properties
- **Restore chains** -- automatically applies full + incrementals in order
- **Prune** -- remove old backup chains beyond a retention count
- **S3-compatible** -- works with AWS S3, MinIO, Backblaze B2, Tigris, etc.
- **Environment variables** -- all connection parameters can be set via `ZCB_*` env vars

## Install

```
cargo build --release
cp target/release/zfs-cloud-backup /usr/local/bin/
```

## Quick start

Generate an age keypair:

```bash
age-keygen -o /root/backup-key.txt
# Public key: age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Back up a dataset:

```bash
zfs-cloud-backup send \
  --dataset tank/data \
  --bucket my-backups \
  --endpoint https://s3.amazonaws.com \
  --age-recipient age1xxxxxxxx
```

List backups:

```bash
zfs-cloud-backup list \
  --dataset tank/data \
  --bucket my-backups \
  --endpoint https://s3.amazonaws.com
```

```
TYPE   SNAPSHOT                  BASE                      SIZE       DATE
full   daily-2025-01-01         --                         12.3MB    2025-01-01 00:00
incr   daily-2025-01-02         daily-2025-01-01            1.1MB    2025-01-02 00:00
```

Restore a snapshot:

```bash
zfs-cloud-backup restore \
  --dataset tank/recovered \
  --snapshot daily-2025-01-02 \
  --bucket my-backups \
  --endpoint https://s3.amazonaws.com \
  --age-identity /root/backup-key.txt
```

Prune old backups (keep last 4 full chains):

```bash
zfs-cloud-backup prune \
  --dataset tank/data \
  --bucket my-backups \
  --endpoint https://s3.amazonaws.com \
  --keep-full 4
```

## Commands

### `send`

Sends the latest local snapshot as a full or incremental backup. If a recent full backup exists (within `--full-interval`), sends an incremental delta. Otherwise sends a full.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--dataset` | `ZCB_DATASET` | | ZFS dataset to back up |
| `--bucket` | `ZCB_BUCKET` | | S3 bucket name |
| `--endpoint` | `ZCB_ENDPOINT` | | S3 endpoint URL |
| `--region` | `ZCB_REGION` | `auto` | S3 region |
| `--prefix` | `ZCB_PREFIX` | | Key prefix in the bucket |
| `--age-recipient` | `ZCB_AGE_RECIPIENT` | | age public key for encryption |
| `--full-interval` | `ZCB_FULL_INTERVAL` | `7d` | Max time between full backups (e.g. `7d`, `24h`) |
| `--replication` | | `false` | Include child datasets and zvols (`-R` / `-R -I`) |

### `list`

Lists all backups for a dataset stored in S3.

### `restore`

Restores a snapshot by downloading and applying the full backup plus any required incrementals.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--dataset` | `ZCB_DATASET` | | Target dataset to receive into |
| `--snapshot` | | | Snapshot name to restore |
| `--age-identity` | `ZCB_AGE_IDENTITY` | | Path to age private key file |
| `--force` | | `false` | Pass `-F` to `zfs receive` |

### `prune`

Removes old full backup chains and their associated incrementals.

| Flag | Default | Description |
|------|---------|-------------|
| `--keep-full` | `4` | Number of most recent full chains to keep |

## S3 key layout

```
{prefix}/{dataset}/full/{snapshot}.zfs.age
{prefix}/{dataset}/incr/{base}..{target}.zfs.age
```

## Environment variables

All connection parameters support env vars so you can configure once:

```bash
export ZCB_BUCKET=my-backups
export ZCB_ENDPOINT=https://s3.amazonaws.com
export ZCB_REGION=us-east-1
export ZCB_AGE_RECIPIENT=age1xxxxxxxx
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
```

Then commands simplify to:

```bash
zfs-cloud-backup send --dataset tank/data
zfs-cloud-backup list --dataset tank/data
zfs-cloud-backup prune --dataset tank/data --keep-full 4
```

## Cron example

```bash
# Daily incremental (full every 7 days), prune to keep last 4 fulls
0 2 * * * zfs-cloud-backup send --dataset tank/data && zfs-cloud-backup prune --dataset tank/data --keep-full 4
```

## Integration tests

Tests run the full send/list/restore/prune cycle against real ZFS and MinIO inside a Lima VM (macOS) or any Docker host with ZFS kernel modules.

```bash
# First run creates the Lima VM (~3 min), subsequent runs reuse it:
./tests/run.sh

# Tear down when done:
limactl stop zcb-test && limactl delete zcb-test
```

**What's tested:**

| Test | Description |
|------|-------------|
| Full backup + restore | Send, list, restore, diff files |
| Incremental chain | Full + incremental, restore chain, verify |
| Replication (`-R`) | Nested child datasets, full + incremental |
| Replication with zvols | ext4 on zvols, backup, modify, restore, verify revert |
| Prune | 3 fulls, prune to 1, verify newest survives |
