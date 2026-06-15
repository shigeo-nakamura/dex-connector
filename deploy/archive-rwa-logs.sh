#!/bin/bash
# Archive the RWA Phase-0 logger dumps to S3 (bot-strategy#574).
#
# The rwa-spot-logger / apex-perp-logger write per-UTC-day JSONL files to
# /opt/debot-rwa/. Those dumps are the only copy of 2-3 weeks of irreplaceable
# read-side data and live on a single EC2 disk. This script mirrors them to S3
# so the collection survives host loss, disk pressure, or an AMI reclone — the
# same durability the pairtrade BT replay archive gets (archive_bt_replay_events.sh,
# bot-strategy#255).
#
# `aws s3 sync` is idempotent and append-only friendly: each run re-uploads only
# new/changed files, so a daily timer both ships yesterday's now-complete file
# and backfills anything not yet archived (e.g. today's partial, which is simply
# overwritten with the complete version on the next run). The instance role can
# PutObject under debot/rwa-logs/ but NOT DeleteObject, which suits an
# append-only archive (verified 2026-06-15).
#
# S3 layout:
#   s3://<bucket>/<prefix>/<host-tag>/rwa_spot_<YYYYMMDD>.jsonl
#   s3://<bucket>/<prefix>/<host-tag>/apex_perp_<YYYYMMDD>.jsonl
#
# Runs daily from archive-rwa-logs.timer. Read-only w.r.t. the loggers; does NOT
# touch debot-pair-btceth.
#
# Environment overrides (mostly for testing):
#   S3_BUCKET   - default debot-dashboard
#   S3_PREFIX   - default debot/rwa-logs
#   RWA_LOG_DIR - default /opt/debot-rwa
#   HOST_TAG    - default auto-detected from instance region
set -euo pipefail

S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-debot/rwa-logs}"
RWA_LOG_DIR="${RWA_LOG_DIR:-/opt/debot-rwa}"

if [ -z "${HOST_TAG:-}" ]; then
    REGION=$(curl -fs --max-time 2 \
        http://169.254.169.254/latest/dynamic/instance-identity/document 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["region"])' \
        2>/dev/null || true)
    case "$REGION" in
        eu-central-1)   HOST_TAG=frankfurt ;;
        ap-northeast-1) HOST_TAG=tokyo ;;
        *) echo "ERROR: cannot derive HOST_TAG from region '$REGION'; set HOST_TAG explicitly" >&2; exit 1 ;;
    esac
fi

if [ ! -d "$RWA_LOG_DIR" ]; then
    echo "ERROR: RWA_LOG_DIR '$RWA_LOG_DIR' does not exist" >&2
    exit 1
fi

DEST="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/"

echo "[archive_rwa_logs] host=$HOST_TAG src=$RWA_LOG_DIR dest=$DEST"

# Only the JSONL dumps; nothing else in the dir should ship.
aws s3 sync --no-progress "$RWA_LOG_DIR/" "$DEST" \
    --exclude '*' --include '*.jsonl'

echo "[archive_rwa_logs] sync complete"
