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
# prefix (GetObject needed for the regression check below) but NOT
# DeleteObject or write to any other prefix in the shared bucket -- List
# and object permissions are two separate IAM statements (bucket-ARN +
# s3:prefix condition for ListBucket, object-ARN glob for
# GetObject/PutObject), since combining them in one statement silently
# breaks the object actions (bot-strategy IAM incident, see
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
# Both sides of that comparison (the local snapshot and the downloaded
# archived content) are bounded, capacity-checked regular temp files
# rather than a FIFO stream: an earlier FIFO-based design avoided
# materializing the archived object on disk, but shell-level FIFO
# producer/consumer synchronization (making a background writer's
# failure promptly deliver EOF to a reader without a background process
# inheriting -- and thereby permanently holding open -- the coordinating
# descriptor) proved to have more edge cases than the disk-usage problem
# it was solving; seven follow-up review rounds surfaced blocking-open
# races, FD_CLOEXEC/inheritance bugs, and a fallback-writer-vs-reader
# ordering race that manual testing confirmed could still hang the
# service (Codex P1/P2 follow-ups, dex-connector#50 rounds 15-23). A
# plain bounded temp file, guarded by the same /tmp capacity check
# either way, is simpler and provably race-free.
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

# Every mktemp'd path created below (snapshots, downloaded archive
# copies, head-object stderr captures) is registered here and removed
# unconditionally on exit -- whether the script finishes normally or
# aborts via `set -e`/an explicit `exit 1` partway through a check -- so
# a persistent regression or transient S3 error can't leave
# archive-sized leftovers accumulating in /tmp across daily timer runs
# (Codex P2 follow-up, dex-connector#50 round 16).
TEMP_PATHS=()
cleanup_temp_paths() {
    rm -rf "${TEMP_PATHS[@]}" 2>/dev/null || true
}
trap cleanup_temp_paths EXIT

# Both archives grow without bound, so materializing a full-size copy of
# either one in /tmp -- the local snapshot below, or the downloaded
# archived content in check_no_size_regression -- could in principle
# consume enough of /tmp to disrupt other host processes sharing the
# same filesystem; check headroom explicitly before each and fail loudly
# instead of letting the copy run /tmp down to empty (Codex P2
# follow-up, dex-connector#50 round 18).
check_tmp_capacity() {
    local needed_bytes="$1" what="$2"
    local avail_bytes tmp_dir margin_bytes
    tmp_dir="${TMPDIR:-/tmp}"
    avail_bytes=$(df --output=avail -B1 "$tmp_dir" | tail -n 1 | tr -d ' ')
    margin_bytes=$((100 * 1024 * 1024))
    if [ "$avail_bytes" -lt "$((needed_bytes + margin_bytes))" ]; then
        echo "ERROR: not enough free space in $tmp_dir for $what ($needed_bytes bytes needed, $avail_bytes available) -- refusing to risk exhausting /tmp for other host processes. Investigate before retrying." >&2
        exit 1
    fi
}

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
    # append-only growth passes, a truncate-and-regrow does not. `aws s3
    # cp ... -` (unlike `aws s3api get-object`, whose outfile argument is
    # a literal path, not a stdout convention -- Codex P1 follow-up,
    # dex-connector#50 round 16) streams the object body straight to
    # stdout, so this downloads the object exactly once into a bounded,
    # capacity-checked temp file rather than a FIFO -- see the header
    # comment for why a FIFO-streaming design was dropped.
    if [ "$remote_size" -gt 0 ]; then
        check_tmp_capacity "$remote_size" "the archived content of $label at s3://${S3_BUCKET}/${s3_key}"

        local remote_copy
        remote_copy=$(mktemp)
        TEMP_PATHS+=("$remote_copy")
        if ! aws s3 cp --no-progress "s3://${S3_BUCKET}/${s3_key}" - >"$remote_copy" 2>/dev/null; then
            echo "ERROR: failed to download archived content for $label at s3://${S3_BUCKET}/${s3_key} to verify against local file" >&2
            exit 1
        fi
        if ! cmp -s "$remote_copy" <(head -c "$remote_size" "$snapshot"); then
            echo "ERROR: $label does not match the content already archived at s3://${S3_BUCKET}/${s3_key} (first $remote_size bytes) -- refusing to upload; the source appears to have been reset and regrown rather than simply appended to. Investigate before retrying." >&2
            exit 1
        fi
        rm -f "$remote_copy"
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

    local src_size
    src_size=$(stat -c%s "$src_file")
    check_tmp_capacity "$src_size" "a snapshot of '$src_file'"

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

    # Free this snapshot immediately rather than waiting for the
    # process-wide EXIT trap: both sources are unbounded, and the second
    # source's capacity preflight would otherwise see the first
    # source's already-uploaded snapshot still occupying /tmp, failing a
    # combined-size-only regression even though each fits individually
    # (Codex P2 follow-up, dex-connector#50 round 21). `rm -f` here is
    # safe to also run again from the trap on later failures.
    rm -f "$snapshot"
}

archive_source "$ARCUS_QUOTE_DIR" "spot-quote" "spot-quote"
archive_source "$ARCUS_RUST_DIR" "spot-rust" "spot-rust"

echo "[archive_arcus_quotes] sync complete"
