#!/usr/bin/env bash
set -euo pipefail

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
BOLD='\033[1m'
RESET='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo -e "  ${GREEN}PASS${RESET} $1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo -e "  ${RED}FAIL${RESET} $1"
}

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass "$desc"
  else
    fail "$desc (expected: '$expected', got: '$actual')"
  fi
}

assert_contains() {
  local desc="$1" haystack="$2" needle="$3"
  if echo "$haystack" | grep -q "$needle"; then
    pass "$desc"
  else
    fail "$desc (output does not contain '$needle')"
  fi
}

assert_file_eq() {
  local desc="$1" file_a="$2" file_b="$3"
  if diff -q "$file_a" "$file_b" > /dev/null 2>&1; then
    pass "$desc"
  else
    fail "$desc (files differ)"
  fi
}

assert_count() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" -eq "$actual" ]; then
    pass "$desc"
  else
    fail "$desc (expected $expected, got $actual)"
  fi
}

ZCB="zfs-cloud-backup"

# --- Setup ---
echo -e "${BOLD}=== Setup ===${RESET}"

# Ensure sbin is in PATH (needed for zpool, zfs in containers)
export PATH="/usr/sbin:/sbin:$PATH"

# ZFS kernel module must be loaded in the host (Lima VM).
# In a privileged container we can try modprobe, but it may already be loaded.
modprobe zfs 2>/dev/null || true
if ! lsmod | grep -q '^zfs'; then
  echo "ERROR: ZFS kernel module not loaded"
  exit 1
fi
echo "ZFS kernel module is loaded"

# Clean up any leftover pool from previous runs
zpool destroy testpool 2>/dev/null || true

truncate -s 512M /tmp/zpool.img
LOOP_DEV=$(losetup --find --show /tmp/zpool.img)
zpool create -f testpool "$LOOP_DEV"
echo "created testpool on $LOOP_DEV"

# Generate age keypair
AGE_KEY_FILE="/tmp/age-key.txt"
AGE_KEYGEN_OUT=$(age-keygen -o "$AGE_KEY_FILE" 2>&1)
AGE_RECIPIENT=$(echo "$AGE_KEYGEN_OUT" | grep -i 'public key:' | awk '{print $NF}')
echo "age recipient: $AGE_RECIPIENT"

# Create MinIO bucket
mc alias set minio "$ZCB_ENDPOINT" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
mc rb --force "minio/$ZCB_BUCKET" 2>/dev/null || true
mc mb "minio/$ZCB_BUCKET"
echo "created S3 bucket: $ZCB_BUCKET"

# Cleanup trap
cleanup() {
  echo ""
  echo -e "${BOLD}=== Cleanup ===${RESET}"
  zpool destroy testpool 2>/dev/null || true
  [ -n "${LOOP_DEV:-}" ] && losetup -d "$LOOP_DEV" 2>/dev/null || true
  rm -f /tmp/zpool.img

  echo ""
  echo -e "${BOLD}=== Results ===${RESET}"
  echo -e "  ${GREEN}Passed: $PASS_COUNT${RESET}"
  echo -e "  ${RED}Failed: $FAIL_COUNT${RESET}"

  if [ "$FAIL_COUNT" -gt 0 ]; then
    exit 1
  fi
}
trap cleanup EXIT

# =============================================================================
# Test 1: Full backup + list + restore
# =============================================================================
echo ""
echo -e "${BOLD}=== Test 1: Full backup + list + restore ===${RESET}"

zfs create testpool/data1
echo "hello world" > /testpool/data1/file1.txt
dd if=/dev/urandom of=/testpool/data1/random.bin bs=1K count=64 2>/dev/null
zfs snapshot testpool/data1@snap1

$ZCB send \
  --dataset testpool/data1 \
  --age-recipient "$AGE_RECIPIENT"

LIST_OUT=$($ZCB list --dataset testpool/data1)
assert_contains "list shows full snap1" "$LIST_OUT" "full"
assert_contains "list shows snap1 name" "$LIST_OUT" "snap1"

$ZCB restore \
  --dataset testpool/restored1 \
  --snapshot snap1 \
  --age-identity "$AGE_KEY_FILE"

assert_file_eq "file1.txt matches" /testpool/data1/file1.txt /testpool/restored1/file1.txt
assert_file_eq "random.bin matches" /testpool/data1/random.bin /testpool/restored1/random.bin

# =============================================================================
# Test 2: Incremental backup + restore chain
# =============================================================================
echo ""
echo -e "${BOLD}=== Test 2: Incremental backup + restore chain ===${RESET}"

echo "updated content" > /testpool/data1/file1.txt
echo "new file" > /testpool/data1/file2.txt
zfs snapshot testpool/data1@snap2

$ZCB send \
  --dataset testpool/data1 \
  --age-recipient "$AGE_RECIPIENT" \
  --full-interval 7d

LIST_OUT=$($ZCB list --dataset testpool/data1)
assert_contains "list shows incr" "$LIST_OUT" "incr"
assert_contains "list shows snap2" "$LIST_OUT" "snap2"

$ZCB restore \
  --dataset testpool/restored2 \
  --snapshot snap2 \
  --age-identity "$AGE_KEY_FILE"

assert_file_eq "file1.txt updated content" /testpool/data1/file1.txt /testpool/restored2/file1.txt
assert_file_eq "file2.txt exists in restore" /testpool/data1/file2.txt /testpool/restored2/file2.txt

# =============================================================================
# Test 3: Replication mode (nested datasets)
# =============================================================================
echo ""
echo -e "${BOLD}=== Test 3: Replication mode (nested datasets) ===${RESET}"

zfs create testpool/repl
zfs create testpool/repl/child
echo "parent data" > /testpool/repl/parent.txt
echo "child data" > /testpool/repl/child/child.txt
zfs snapshot -r testpool/repl@replsnap1

$ZCB send \
  --dataset testpool/repl \
  --age-recipient "$AGE_RECIPIENT" \
  --replication

LIST_OUT=$($ZCB list --dataset testpool/repl)
assert_contains "list shows replsnap1" "$LIST_OUT" "replsnap1"

$ZCB restore \
  --dataset testpool/repl_restored \
  --snapshot replsnap1 \
  --age-identity "$AGE_KEY_FILE"

# Check child dataset was created
CHILD_EXISTS=$(zfs list -H -o name testpool/repl_restored/child 2>/dev/null || echo "")
assert_eq "full -R: child dataset exists" "testpool/repl_restored/child" "$CHILD_EXISTS"
assert_file_eq "full -R: parent.txt matches" /testpool/repl/parent.txt /testpool/repl_restored/parent.txt
assert_file_eq "full -R: child.txt matches" /testpool/repl/child/child.txt /testpool/repl_restored/child/child.txt

# Incremental replication (-R -I): mutate both parent and child, take snap2
echo "parent data v2" > /testpool/repl/parent.txt
echo "child data v2" > /testpool/repl/child/child.txt
echo "new child file" > /testpool/repl/child/extra.txt
zfs snapshot -r testpool/repl@replsnap2

$ZCB send \
  --dataset testpool/repl \
  --age-recipient "$AGE_RECIPIENT" \
  --replication \
  --full-interval 7d

LIST_OUT=$($ZCB list --dataset testpool/repl)
assert_contains "incr -R -I: list shows replsnap2" "$LIST_OUT" "replsnap2"
assert_contains "incr -R -I: incremental type" "$LIST_OUT" "incr"

$ZCB restore \
  --dataset testpool/repl_restored2 \
  --snapshot replsnap2 \
  --age-identity "$AGE_KEY_FILE"

CHILD_EXISTS=$(zfs list -H -o name testpool/repl_restored2/child 2>/dev/null || echo "")
assert_eq "incr -R -I: child dataset exists" "testpool/repl_restored2/child" "$CHILD_EXISTS"
assert_file_eq "incr -R -I: parent.txt updated" /testpool/repl/parent.txt /testpool/repl_restored2/parent.txt
assert_file_eq "incr -R -I: child.txt updated" /testpool/repl/child/child.txt /testpool/repl_restored2/child/child.txt
assert_file_eq "incr -R -I: extra.txt in child" /testpool/repl/child/extra.txt /testpool/repl_restored2/child/extra.txt

# =============================================================================
# Test 4: Replication with zvol children (mounted filesystems)
# =============================================================================
echo ""
echo -e "${BOLD}=== Test 4: Replication with zvol children ===${RESET}"

zfs create testpool/zvtest
zfs create -V 10M testpool/zvtest/vol1
zfs create -V 10M testpool/zvtest/vol2
udevadm settle 2>/dev/null || sleep 2

# Format and mount both zvols
mkfs.ext4 -q /dev/zvol/testpool/zvtest/vol1
mkfs.ext4 -q /dev/zvol/testpool/zvtest/vol2
mkdir -p /mnt/vol1 /mnt/vol2
mount /dev/zvol/testpool/zvtest/vol1 /mnt/vol1
mount /dev/zvol/testpool/zvtest/vol2 /mnt/vol2

# Write original data
echo "vol1 original" > /mnt/vol1/data.txt
echo "vol2 original" > /mnt/vol2/data.txt
sync

# Snapshot and backup
umount /mnt/vol1 /mnt/vol2
zfs snapshot -r testpool/zvtest@zsnap1

$ZCB send \
  --dataset testpool/zvtest \
  --age-recipient "$AGE_RECIPIENT" \
  --replication

# Now modify both zvols (post-backup changes)
mount /dev/zvol/testpool/zvtest/vol1 /mnt/vol1
mount /dev/zvol/testpool/zvtest/vol2 /mnt/vol2
echo "vol1 MODIFIED" > /mnt/vol1/data.txt
echo "vol2 MODIFIED" > /mnt/vol2/data.txt
echo "should not survive restore" > /mnt/vol1/new_file.txt
sync

# Read back to confirm the modifications are there
VOL1_CURRENT=$(cat /mnt/vol1/data.txt)
VOL2_CURRENT=$(cat /mnt/vol2/data.txt)
assert_eq "vol1 is modified" "vol1 MODIFIED" "$VOL1_CURRENT"
assert_eq "vol2 is modified" "vol2 MODIFIED" "$VOL2_CURRENT"
umount /mnt/vol1 /mnt/vol2

# Restore from backup
$ZCB restore \
  --dataset testpool/zvtest_restored \
  --snapshot zsnap1 \
  --age-identity "$AGE_KEY_FILE"
udevadm settle 2>/dev/null || sleep 2

# Verify zvols exist and are volumes
VOL1_TYPE=$(zfs get -H -o value type testpool/zvtest_restored/vol1 2>/dev/null || echo "")
VOL2_TYPE=$(zfs get -H -o value type testpool/zvtest_restored/vol2 2>/dev/null || echo "")
assert_eq "vol1 is a volume" "volume" "$VOL1_TYPE"
assert_eq "vol2 is a volume" "volume" "$VOL2_TYPE"

# Mount restored zvols and verify original data is back
mkdir -p /mnt/vol1_r /mnt/vol2_r
mount /dev/zvol/testpool/zvtest_restored/vol1 /mnt/vol1_r
mount /dev/zvol/testpool/zvtest_restored/vol2 /mnt/vol2_r

VOL1_RESTORED=$(cat /mnt/vol1_r/data.txt)
VOL2_RESTORED=$(cat /mnt/vol2_r/data.txt)
assert_eq "vol1 restore reverted to original" "vol1 original" "$VOL1_RESTORED"
assert_eq "vol2 restore reverted to original" "vol2 original" "$VOL2_RESTORED"

# The new file written after the snapshot should not exist
if [ ! -f /mnt/vol1_r/new_file.txt ]; then
  pass "new_file.txt absent in restored vol1"
else
  fail "new_file.txt should not exist in restored vol1"
fi

umount /mnt/vol1_r /mnt/vol2_r

# =============================================================================
# Test 5: Prune
# =============================================================================
echo ""
echo -e "${BOLD}=== Test 5: Prune ===${RESET}"

zfs create testpool/prunedata
echo "gen1" > /testpool/prunedata/data.txt

zfs snapshot testpool/prunedata@psnap1
$ZCB send \
  --dataset testpool/prunedata \
  --age-recipient "$AGE_RECIPIENT" \
  --full-interval 0s
sleep 2

echo "gen2" > /testpool/prunedata/data.txt
zfs snapshot testpool/prunedata@psnap2
$ZCB send \
  --dataset testpool/prunedata \
  --age-recipient "$AGE_RECIPIENT" \
  --full-interval 0s
sleep 2

echo "gen3" > /testpool/prunedata/data.txt
zfs snapshot testpool/prunedata@psnap3
$ZCB send \
  --dataset testpool/prunedata \
  --age-recipient "$AGE_RECIPIENT" \
  --full-interval 0s

LIST_OUT=$($ZCB list --dataset testpool/prunedata)
FULL_COUNT=$(echo "$LIST_OUT" | grep -c "^full" || true)
assert_count "3 full backups before prune" 3 "$FULL_COUNT"

$ZCB prune \
  --dataset testpool/prunedata \
  --keep-full 1

LIST_OUT=$($ZCB list --dataset testpool/prunedata)
FULL_COUNT=$(echo "$LIST_OUT" | grep -c "^full" || true)
assert_count "1 full backup after prune" 1 "$FULL_COUNT"
assert_contains "newest backup survives" "$LIST_OUT" "psnap3"

# =============================================================================
# Test 6: Encrypted dataset with --raw (zvol children)
# =============================================================================
echo ""
echo -e "${BOLD}=== Test 6: Encrypted dataset with --raw (zvol children) ===${RESET}"

# Create an encrypted parent dataset with two zvol children
echo "testpassword" | zfs create \
  -o encryption=aes-256-gcm \
  -o keyformat=passphrase \
  testpool/encrypted
zfs create -V 10M testpool/encrypted/vol1
zfs create -V 10M testpool/encrypted/vol2
udevadm settle 2>/dev/null || sleep 2

# Write data to the parent filesystem
echo "secret data" > /testpool/encrypted/secret.txt
dd if=/dev/urandom of=/testpool/encrypted/random.bin bs=1K count=64 2>/dev/null

# Format, mount, and write data to both zvols
mkfs.ext4 -q /dev/zvol/testpool/encrypted/vol1
mkfs.ext4 -q /dev/zvol/testpool/encrypted/vol2
mkdir -p /mnt/enc_vol1 /mnt/enc_vol2
mount /dev/zvol/testpool/encrypted/vol1 /mnt/enc_vol1
mount /dev/zvol/testpool/encrypted/vol2 /mnt/enc_vol2
echo "enc vol1 data" > /mnt/enc_vol1/data.txt
echo "enc vol2 data" > /mnt/enc_vol2/data.txt
sync
umount /mnt/enc_vol1 /mnt/enc_vol2

zfs snapshot -r testpool/encrypted@esnap1

# Sending with --replication but WITHOUT --raw should fail on encrypted datasets.
# Before the fix, zfs send would error to stderr but the tool would silently
# upload an empty stream and exit 0.
if $ZCB send \
  --dataset testpool/encrypted \
  --age-recipient "$AGE_RECIPIENT" \
  --replication 2>/dev/null; then
  fail "send -R without --raw should fail on encrypted dataset"
else
  pass "send -R without --raw fails on encrypted dataset"
fi

# Verify no backup was left in S3 from the failed send
FAILED_LIST=$($ZCB list --dataset testpool/encrypted 2>/dev/null || true)
FAILED_COUNT=$(echo "$FAILED_LIST" | grep -c "^full" || true)
assert_count "no backup left after failed send" 0 "$FAILED_COUNT"

# Sending with --replication --raw should succeed
$ZCB send \
  --dataset testpool/encrypted \
  --age-recipient "$AGE_RECIPIENT" \
  --replication \
  --raw

LIST_OUT=$($ZCB list --dataset testpool/encrypted)
assert_contains "raw send: list shows esnap1" "$LIST_OUT" "esnap1"
assert_contains "raw send: list shows full" "$LIST_OUT" "full"

# Restore the raw backup and verify data integrity
$ZCB restore \
  --dataset testpool/enc_restored \
  --snapshot esnap1 \
  --age-identity "$AGE_KEY_FILE"
udevadm settle 2>/dev/null || sleep 2

# Raw-received encrypted datasets need their key loaded before mounting
echo "testpassword" | zfs load-key -r testpool/enc_restored
zfs mount testpool/enc_restored

# Verify parent filesystem
assert_file_eq "encrypted: secret.txt matches" /testpool/encrypted/secret.txt /testpool/enc_restored/secret.txt
assert_file_eq "encrypted: random.bin matches" /testpool/encrypted/random.bin /testpool/enc_restored/random.bin

# Verify zvol children exist and are volumes
ENC_VOL1_TYPE=$(zfs get -H -o value type testpool/enc_restored/vol1 2>/dev/null || echo "")
ENC_VOL2_TYPE=$(zfs get -H -o value type testpool/enc_restored/vol2 2>/dev/null || echo "")
assert_eq "encrypted vol1 is a volume" "volume" "$ENC_VOL1_TYPE"
assert_eq "encrypted vol2 is a volume" "volume" "$ENC_VOL2_TYPE"

# Mount restored zvols and verify data
mkdir -p /mnt/enc_vol1_r /mnt/enc_vol2_r
mount /dev/zvol/testpool/enc_restored/vol1 /mnt/enc_vol1_r
mount /dev/zvol/testpool/enc_restored/vol2 /mnt/enc_vol2_r

ENC_VOL1_RESTORED=$(cat /mnt/enc_vol1_r/data.txt)
ENC_VOL2_RESTORED=$(cat /mnt/enc_vol2_r/data.txt)
assert_eq "encrypted vol1 data matches" "enc vol1 data" "$ENC_VOL1_RESTORED"
assert_eq "encrypted vol2 data matches" "enc vol2 data" "$ENC_VOL2_RESTORED"

umount /mnt/enc_vol1_r /mnt/enc_vol2_r
