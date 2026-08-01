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
# can PutObject under the isolated arcus-archive/ prefix but NOT
# DeleteObject or write to any other prefix in the shared bucket -- List and
# object permissions are two separate IAM statements (bucket-ARN +
# s3:prefix condition for ListBucket, object-ARN glob for
# GetObject/PutObject), since combining them in one statement silently
# breaks the object actions (bot-strategy IAM incident, see
# feedback_iam_s3_prefix_condition in project memory).
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
    if [ ! -s "$dir/samples.jsonl" ]; then
        echo "ERROR: expected nonempty '$dir/samples.jsonl'" >&2
        exit 1
    fi
done

dest="s3://${S3_BUCKET}/${S3_PREFIX}/spot-quote/"
echo "[archive_arcus_quotes] src=$ARCUS_QUOTE_DIR dest=$dest"
aws s3 sync --no-progress "$ARCUS_QUOTE_DIR/" "$dest" \
    --exclude '*' --include '*.jsonl'

dest="s3://${S3_BUCKET}/${S3_PREFIX}/spot-rust/"
echo "[archive_arcus_quotes] src=$ARCUS_RUST_DIR dest=$dest"
aws s3 sync --no-progress "$ARCUS_RUST_DIR/" "$dest" \
    --exclude '*' --include '*.jsonl'

echo "[archive_arcus_quotes] sync complete"
