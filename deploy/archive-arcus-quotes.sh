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
# The instance role can Get/PutObject under the isolated arcus-archive/
# prefix (GetObject needed for the regression check below, via
# HeadObject/GetObject) but NOT DeleteObject or write to any other prefix
# in the shared bucket -- List and object permissions are two separate IAM
# statements (bucket-ARN + s3:prefix condition for ListBucket, object-ARN
# glob for GetObject/PutObject), since combining them in one statement
# silently breaks the object actions (bot-strategy IAM incident, see
# feedback_iam_s3_prefix_condition in project memory).
#
# Each fixed samples.jsonl key is overwritten on every run, which would
# otherwise let a truncated/reset local collector file (disk pressure, an
# operator mistake, a collector bug) permanently destroy already-archived
# history with no recovery path -- precisely the kind of loss this backup
# exists to prevent (Codex P1 follow-up, dex-connector#50). S3 versioning
# is enabled on debot-dashboard (2026-08-01) as a recovery window, but a
# 90-day NoncurrentVersionExpiration lifecycle rule means that window is
# temporary, not a permanent guarantee (Codex P1 follow-up, dex-connector#50
# round 13). Guarded here instead: refuse to upload a source file that is
# smaller than, or does not still start with the same bytes as, what is
# already archived in S3, so a regression requires deliberate operator
# intervention rather than silently overwriting irreplaceable history on
# the next scheduled run.
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

# Every mktemp'd path created below (snapshots, FIFOs, head-object stderr
# captures) is registered here and removed unconditionally on exit --
# whether the script finishes normally or aborts via `set -e`/an explicit
# `exit 1` partway through a check -- so a persistent regression or
# transient S3 error can't leave archive-sized leftovers accumulating in
# /tmp across daily timer runs (Codex P2 follow-up, dex-connector#50
# round 16).
TEMP_PATHS=()
cleanup_temp_paths() {
    rm -f "${TEMP_PATHS[@]}" 2>/dev/null || true
}
trap cleanup_temp_paths EXIT

# S3 (not a local tracking file, which could be wiped by the same
# disk-pressure/reclone event this guards against) is the ground truth for
# "already archived". On the very first deployment neither key exists yet,
# so `head-object` returns a 404 service error (nonzero exit) rather than
# a successful "None" ContentLength; used directly in a plain assignment,
# `set -e` would abort the script on that 404 before either upload ever
# runs, permanently wedging a fresh install unless both objects were
# manually pre-created. Handled below with an `if cmd; then ... else ...`
# guard (the standard idiom for tolerating a failure under `set -e`) so
# only the not-found case is treated as size zero; permission and
# connectivity errors still abort (Codex P1 follow-up, dex-connector#50
# round 14).
check_no_size_regression() {
    local snapshot="$1" s3_key="$2" label="$3"
    local local_size remote_size head_err

    local_size=$(stat -c%s "$snapshot")

    head_err=$(mktemp)
    TEMP_PATHS+=("$head_err")
    if remote_size=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$s3_key" \
        --query 'ContentLength' --output text 2>"$head_err"); then
        :
    elif grep -q '404' "$head_err"; then
        remote_size=0
    else
        echo "ERROR: failed to check archived size for $label at s3://${S3_BUCKET}/${s3_key}:" >&2
        cat "$head_err" >&2
        exit 1
    fi

    if [ "$local_size" -lt "$remote_size" ]; then
        echo "ERROR: $label ($local_size bytes) is smaller than the already-archived s3://${S3_BUCKET}/${s3_key} ($remote_size bytes) -- refusing to upload a regression that would permanently truncate archived history. Investigate before retrying." >&2
        exit 1
    fi

    # A size match alone doesn't prove the content matches: a collector
    # file that gets truncated/reset and then grows back to the archived
    # size (or larger) before the next daily run would pass the check
    # above while the upload replaces the complete archived object with an
    # unrelated file, silently losing the history this guard exists to
    # protect (Codex P1 follow-up, dex-connector#50 round 14). Require the
    # source to still start with the same bytes already archived --
    # append-only growth passes, a truncate-and-regrow does not.
    if [ "$remote_size" -gt 0 ]; then
        # `aws s3api get-object`'s trailing argument is a literal outfile
        # path (per the AWS CLI reference), not the `aws s3 cp` stdout
        # convention -- passing `-` creates a file actually named "-"
        # once an archive key exists, so every run after the first would
        # silently compare against that empty/wrong file and reject a
        # valid source (Codex P1 follow-up, dex-connector#50 round 16).
        # Stream through a FIFO instead of a regular file so the archived
        # object -- itself unbounded, since the collectors never rotate --
        # is never staged as a second full-size copy on disk either
        # (Codex P2 follow-up, dex-connector#50 round 15). No fixed local
        # timeout on either side: a short one would reject an otherwise
        # healthy download of a large archived object well before the
        # unit's own TimeoutStartSec (3600s) is reached, permanently
        # stalling the backup on every subsequent run (Codex P2
        # follow-up, dex-connector#50 round 17) -- that TimeoutStartSec
        # budget, not a constant re-guessed here, is what should bound
        # this.
        local fifo
        fifo=$(mktemp -u)
        mkfifo "$fifo"
        TEMP_PATHS+=("$fifo")
        aws s3api get-object --bucket "$S3_BUCKET" --key "$s3_key" \
            --range "bytes=0-$((remote_size - 1))" "$fifo" >/dev/null 2>&1 &
        local get_pid=$!
        local content_matches=0
        cmp -s "$fifo" <(head -c "$remote_size" "$snapshot") || content_matches=1
        wait "$get_pid" || content_matches=1
        rm -f "$fifo"
        if [ "$content_matches" -ne 0 ]; then
            echo "ERROR: $label does not match (or could not be verified against) the content already archived at s3://${S3_BUCKET}/${s3_key} (first $remote_size bytes) -- refusing to upload; either the source was reset and regrown rather than simply appended to, or the archived content could not be downloaded for verification. Investigate before retrying." >&2
            exit 1
        fi
    fi
}

# Validates and uploads one collector's samples.jsonl. Takes a root-owned
# snapshot immediately after the symlink/regular-file checks and runs both
# the regression check and the upload against that snapshot rather than
# the live path: the collector account remains active for this script's
# entire run (HeadObject/GetObject/PutObject round trips can take
# seconds), so without a stable snapshot it could rewrite samples.jsonl
# between validation and upload, bypassing every check above (Codex P1
# follow-up, dex-connector#50 round 15).
archive_source() {
    local src_dir="$1" s3_subprefix="$2" label="$3"
    local src_file="$src_dir/samples.jsonl"
    local s3_key="${S3_PREFIX}/${s3_subprefix}/samples.jsonl"

    # `-s` follows symlinks when checking size, so a samples.jsonl replaced
    # by a symlink to some other nonempty file would still pass a naive
    # size check; reject the symlink case explicitly instead of letting it
    # reach the snapshot step below (Codex P2 follow-up, dex-connector#50).
    if [ -L "$src_file" ]; then
        echo "ERROR: '$src_file' is a symlink, refusing to treat it as collector data" >&2
        exit 1
    fi
    # A directory also has nonzero apparent size on this filesystem, so
    # `-s` alone isn't sufficient; require a regular file too (Codex P2
    # follow-up, dex-connector#50).
    if [ ! -f "$src_file" ] || [ ! -s "$src_file" ]; then
        echo "ERROR: expected a nonempty regular file at '$src_file'" >&2
        exit 1
    fi

    local snapshot
    snapshot=$(mktemp)
    TEMP_PATHS+=("$snapshot")
    # `--no-dereference` so a collector account that swaps samples.jsonl
    # for a symlink in the window between the checks above and this copy
    # can't make us silently snapshot (and later archive) an arbitrary
    # root-readable file: if the source has become a symlink by the time
    # we get here, `cp -P` preserves it as one instead of following it,
    # and the check right below catches that -- we never read the
    # symlink target's content either way (Codex P1 follow-up,
    # dex-connector#50 round 16).
    cp --no-dereference "$src_file" "$snapshot"
    if [ -L "$snapshot" ]; then
        echo "ERROR: '$src_file' became a symlink during snapshotting, refusing to treat it as collector data" >&2
        exit 1
    fi

    check_no_size_regression "$snapshot" "$s3_key" "$label ($src_file)"

    local dest="s3://${S3_BUCKET}/${s3_key}"
    echo "[archive_arcus_quotes] src=$src_file dest=$dest"
    aws s3 cp --no-progress "$snapshot" "$dest"
}

archive_source "$ARCUS_QUOTE_DIR" "spot-quote" "spot-quote"
archive_source "$ARCUS_RUST_DIR" "spot-rust" "spot-rust"

echo "[archive_arcus_quotes] sync complete"
