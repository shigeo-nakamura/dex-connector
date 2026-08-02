#!/bin/bash
# Archive the Arcus Spot collector dumps to S3 (bot-strategy#758).
#
# The arcus-spot-quote-collector (Python, bot-strategy) and
# arcus-spot-rust-recorder (Rust, dex-connector) both write append-only
# JSONL samples to /var/lib/debot-arcus/{spot-quote,spot-rust}/ on
# debot-arcus. That data is the only copy of the Arcus Spot pair/cost
# research evidence (bot-strategy#756) and lives on a single EC2 disk. This
# mirrors it to S3 so it survives host loss, disk pressure, or an AMI
# reclone, the same durability the RWA logger archive gets
# (archive-rwa-logs.sh, bot-strategy#574).
#
# `aws s3 sync` is idempotent and append-only friendly. The instance role
# can Get/PutObject under the isolated arcus-archive/ prefix (GetObject
# needed for the pre-sync regression check below, via HeadObject) but NOT
# DeleteObject or write to any other prefix in the shared bucket -- List and
# object permissions are two separate IAM statements (bucket-ARN +
# s3:prefix condition for ListBucket, object-ARN glob for
# GetObject/PutObject), since combining them in one statement silently
# breaks the object actions (bot-strategy IAM incident, see
# feedback_iam_s3_prefix_condition in project memory).
#
# `sync` overwrites the fixed samples.jsonl key on every run, which would
# otherwise let a truncated/reset local collector file (disk pressure, an
# operator mistake, a collector bug) permanently destroy already-archived
# history with no recovery path -- precisely the kind of loss this backup
# exists to prevent (Codex P1 follow-up, dex-connector#50). S3 versioning
# is enabled on debot-dashboard (2026-08-01) as a recovery window, but a
# 90-day NoncurrentVersionExpiration lifecycle rule means that window is
# temporary, not a permanent guarantee: a reset that goes unnoticed for
# over 90 days still loses the pre-reset history for good (Codex P1
# follow-up, dex-connector#50 round 13). Guarded here instead: refuse to
# sync a local file that is smaller than what is already archived in S3,
# so a regression requires deliberate operator intervention (delete the
# stale S3 object, or accept the loss knowingly) rather than silently
# overwriting irreplaceable history on the next scheduled run.
#
# S3 layout (isolated from the arcus-quote-collector/arcus-spot-recorder/
# deploy/ deploy-artifact prefixes the same bucket also holds):
#   s3://<bucket>/<prefix>/spot-quote/samples.jsonl
#   s3://<bucket>/<prefix>/spot-rust/samples.jsonl
#
# Runs daily from archive-arcus-quotes.timer. Read-only w.r.t. the
# collectors; does NOT touch debot-pair-btceth.
#
# Environment overrides (mostly for testing):
#   S3_BUCKET        - default debot-dashboard
#   S3_PREFIX         - default arcus-archive
#   ARCUS_QUOTE_DIR   - default /var/lib/debot-arcus/spot-quote
#   ARCUS_RUST_DIR    - default /var/lib/debot-arcus/spot-rust
set -euo pipefail

S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-arcus-archive}"
ARCUS_QUOTE_DIR="${ARCUS_QUOTE_DIR:-/var/lib/debot-arcus/spot-quote}"
ARCUS_RUST_DIR="${ARCUS_RUST_DIR:-/var/lib/debot-arcus/spot-rust}"

# Both collectors are expected to be running on this host; a missing
# directory, or one that exists but has not produced any sample data yet
# (e.g. StateDirectory created before the collector's first successful
# run), is a real regression -- not an optional source to skip. Silently
# archiving only whichever one has data would leave the other's
# irreplaceable dataset stale (or entirely absent) in S3 while this script
# keeps reporting success (Codex P2 follow-up, dex-connector#50).
for dir in "$ARCUS_QUOTE_DIR" "$ARCUS_RUST_DIR"; do
    # `-s` follows symlinks when checking size, so a samples.jsonl replaced
    # by a symlink to some other nonempty file would still pass this check
    # -- but the sync below (--no-follow-symlinks, by design) then skips
    # that entry entirely, letting the script report "sync complete" while
    # silently archiving nothing for that collector. Reject the symlink
    # case explicitly rather than let it slip through both checks (Codex
    # P2 follow-up, dex-connector#50).
    if [ -L "$dir/samples.jsonl" ]; then
        echo "ERROR: '$dir/samples.jsonl' is a symlink, refusing to treat it as collector data" >&2
        exit 1
    fi
    # `-s` alone only checks nonzero size, which a directory also satisfies
    # on this filesystem (an inode entry has nonzero apparent size); a
    # samples.jsonl accidentally replaced by a directory would then pass
    # this check while `aws s3 sync`'s include/exclude globbing has no
    # regular file to upload, again reaching "sync complete" with nothing
    # actually archived for that collector.
    if [ ! -f "$dir/samples.jsonl" ] || [ ! -s "$dir/samples.jsonl" ]; then
        echo "ERROR: expected a nonempty regular file at '$dir/samples.jsonl'" >&2
        exit 1
    fi
done

# S3 (not a local tracking file, which could be wiped by the same
# disk-pressure/reclone event this guards against) is the ground truth for
# "already archived". A HEAD on a key that has never been synced returns no
# ContentLength, which `head-object`'s default text output renders as the
# literal string "None" -- treated as 0 (nothing archived yet, any local
# size clears it) rather than a failure.
check_no_size_regression() {
    local local_file="$1" s3_key="$2" label="$3"
    local local_size remote_size
    local_size=$(stat -c%s "$local_file")
    remote_size=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$s3_key" \
        --query 'ContentLength' --output text 2>/dev/null)
    if [ -z "$remote_size" ] || [ "$remote_size" = "None" ]; then
        remote_size=0
    fi
    if [ "$local_size" -lt "$remote_size" ]; then
        echo "ERROR: $label local file '$local_file' ($local_size bytes) is smaller than the already-archived s3://${S3_BUCKET}/${s3_key} ($remote_size bytes) -- refusing to sync a regression that would permanently truncate archived history. Investigate before retrying." >&2
        exit 1
    fi
}

check_no_size_regression "$ARCUS_QUOTE_DIR/samples.jsonl" "${S3_PREFIX}/spot-quote/samples.jsonl" "spot-quote"
check_no_size_regression "$ARCUS_RUST_DIR/samples.jsonl" "${S3_PREFIX}/spot-rust/samples.jsonl" "spot-rust"

# This oneshot runs as root (no User= in the unit) so it can read both
# collectors' state directories regardless of which unprivileged account
# owns each. `aws s3 sync` follows local symlinks by default and uploads
# their targets; a compromised collector could otherwise plant a symlink
# in its own writable directory (e.g. named *.jsonl) pointing at any
# root-readable file, and this job would upload that target to S3 under
# the symlink's name, breaking the collector service's privilege boundary
# (Codex P1 follow-up, dex-connector#50). --no-follow-symlinks makes sync
# skip symlinks entirely instead.
dest="s3://${S3_BUCKET}/${S3_PREFIX}/spot-quote/"
echo "[archive_arcus_quotes] src=$ARCUS_QUOTE_DIR dest=$dest"
aws s3 sync --no-progress --no-follow-symlinks "$ARCUS_QUOTE_DIR/" "$dest" \
    --exclude '*' --include '*.jsonl'

dest="s3://${S3_BUCKET}/${S3_PREFIX}/spot-rust/"
echo "[archive_arcus_quotes] src=$ARCUS_RUST_DIR dest=$dest"
aws s3 sync --no-progress --no-follow-symlinks "$ARCUS_RUST_DIR/" "$dest" \
    --exclude '*' --include '*.jsonl'

echo "[archive_arcus_quotes] sync complete"
