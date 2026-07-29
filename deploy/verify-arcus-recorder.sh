#!/usr/bin/env bash
# Verify a freshly (re)started arcus-spot-rust-recorder produced a fresh,
# correctly-shaped sample (bot-strategy#758). Used by
# deploy-arcus-spot-recorder.yml via SSM.
#
# The recorder is a oneshot: a successful `systemctl start` only proves the
# process exited 0, not that it appended a well-formed row for every
# configured pair/notional. Parse the newest JSONL line and require exactly
# `expected_rows` round trips (bot-strategy#756 documents 3 pairs x 4
# notionals = 12 as the expected count); a malformed config or a schema
# regression would otherwise deploy green while writing rows for only some
# of the configured routes.
#
# Args:
#   $1 jsonl          path to the recorder's JSONL (e.g. /var/lib/debot-arcus/spot-rust/samples.jsonl)
#   $2 baseline_file   file holding the pre-restart line count
#   $3 expected_rows   round_trips length required in the freshest line (e.g. 12)
set -u

f=$1
baseline_file=$2
expected_rows=$3

base=$(cat "$baseline_file" 2>/dev/null || echo 0)

cur=$base
# Poll up to ~60s: the oneshot service is triggered once by this deploy, so
# give the process time to run its request/retry pacing to completion.
for _ in $(seq 1 30); do
  cur=$(wc -l < "$f" 2>/dev/null || echo 0)
  if [ "$cur" -gt "$base" ]; then
    break
  fi
  sleep 2
done

if [ "$cur" -le "$base" ]; then
  echo "FAIL arcus-spot-rust-recorder: no new line appended (${base} -> ${cur})"
  journalctl -u arcus-spot-rust-recorder.service -n 40 --no-pager 2>/dev/null || true
  exit 1
fi

python3 - "$f" "$expected_rows" <<'PY'
import json
import sys

path, expected = sys.argv[1], int(sys.argv[2])
with open(path, "rb") as handle:
    last = None
    for line in handle:
        line = line.strip()
        if line:
            last = line
if last is None:
    print("FAIL arcus-spot-rust-recorder: JSONL is empty")
    sys.exit(1)

row = json.loads(last)
round_trips = row.get("round_trips")
if not isinstance(round_trips, list):
    print("FAIL arcus-spot-rust-recorder: freshest line has no round_trips array")
    sys.exit(1)
if len(round_trips) != expected:
    print(
        f"FAIL arcus-spot-rust-recorder: freshest line has {len(round_trips)} "
        f"round trips, expected {expected}"
    )
    sys.exit(1)

print(
    f"OK arcus-spot-rust-recorder: freshest line has {len(round_trips)} round trips "
    f"(schema_version={row.get('schema_version')!r}, mode={row.get('mode')!r})"
)
PY
