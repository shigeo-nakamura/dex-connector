#!/usr/bin/env bash
# Verify a freshly (re)started arcus-spot-rust-recorder produced a fresh,
# correctly-shaped sample (bot-strategy#758). Used by
# deploy-arcus-spot-recorder.yml via SSM.
#
# The recorder is a oneshot: a successful `systemctl start` only proves the
# process exited 0, not that it appended a well-formed row for every
# configured pair/notional. A row *count* match alone is not enough either:
# a regression that duplicates or mislabels a pair/notional would still pass
# a count-only check while silently corrupting the intended sample matrix.
# Parse the ARCUS_SPOT_PAIRS/ARCUS_SPOT_NOTIONALS_USD actually baked into the
# deployed unit's Environment lines and require the freshest line's
# round_trips to be exactly that (sell_symbol, buy_symbol, notional_usd) set,
# with no duplicates and the expected schema_version/mode.
#
# Args:
#   $1 jsonl   path to the recorder's JSONL (e.g. /var/lib/debot-arcus/spot-rust/samples.jsonl)
#   $2 baseline_file   file holding the pre-restart line count
#   $3 unit    systemd unit name (e.g. arcus-spot-rust-recorder)
set -u

f=$1
baseline_file=$2
unit=$3

# Unit dir is overridable only so the script can be exercised in a test harness.
unit_dir="${UNIT_DIR:-/etc/systemd/system}"
unit_file="${unit_dir}/${unit}.service"

pairs_csv=$(sed -n 's/^Environment="ARCUS_SPOT_PAIRS=\(.*\)"$/\1/p' "$unit_file")
notionals_csv=$(sed -n 's/^Environment="ARCUS_SPOT_NOTIONALS_USD=\(.*\)"$/\1/p' "$unit_file")
if [ -z "$pairs_csv" ] || [ -z "$notionals_csv" ]; then
  echo "FAIL $unit: could not parse ARCUS_SPOT_PAIRS/ARCUS_SPOT_NOTIONALS_USD from ${unit_file}"
  exit 1
fi

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
  echo "FAIL $unit: no new line appended (${base} -> ${cur})"
  journalctl -u "${unit}.service" -n 40 --no-pager 2>/dev/null || true
  exit 1
fi

python3 - "$f" "$pairs_csv" "$notionals_csv" <<'PY'
import json
import sys

# Must match dex-connector's SUPPORTED_RECORDER_SCHEMA_VERSION /
# PUBLIC_RECORDER_MODE (src/arcus_spot_connector/recorder.rs) and the
# consuming pairtrade runtime's SUPPORTED_RECORDER_SCHEMA_VERSION /
# PUBLIC_RECORDER_MODE (src/arcus_spot/runtime.rs). A row count match alone
# does not prove the deployed binary's envelope is still consumable: a
# schema/mode change without a matching row-count change would otherwise
# deploy green while breaking every downstream reader.
EXPECTED_SCHEMA_VERSION = 3
EXPECTED_MODE = "public_indicative_read_only"

path, pairs_csv, notionals_csv = sys.argv[1], sys.argv[2], sys.argv[3]
expected = {
    (pair.split("/")[0], pair.split("/")[1], notional)
    for pair in pairs_csv.split(",")
    for notional in notionals_csv.split(",")
}

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
schema_version = row.get("schema_version")
if schema_version != EXPECTED_SCHEMA_VERSION:
    print(
        f"FAIL arcus-spot-rust-recorder: freshest line has schema_version="
        f"{schema_version!r}, expected {EXPECTED_SCHEMA_VERSION!r}"
    )
    sys.exit(1)
mode = row.get("mode")
if mode != EXPECTED_MODE:
    print(
        f"FAIL arcus-spot-rust-recorder: freshest line has mode={mode!r}, "
        f"expected {EXPECTED_MODE!r}"
    )
    sys.exit(1)

round_trips = row.get("round_trips")
if not isinstance(round_trips, list):
    print("FAIL arcus-spot-rust-recorder: freshest line has no round_trips array")
    sys.exit(1)

seen = []
for trip in round_trips:
    try:
        pair = trip["pair"]
        seen.append((pair["sell_symbol"], pair["buy_symbol"], trip["notional_usd"]))
    except (KeyError, TypeError) as error:
        print(f"FAIL arcus-spot-rust-recorder: round trip missing pair/notional_usd: {error}")
        sys.exit(1)

duplicates = {key for key in seen if seen.count(key) > 1}
if duplicates:
    print(f"FAIL arcus-spot-rust-recorder: duplicate (sell, buy, notional_usd) round trips: {sorted(duplicates)}")
    sys.exit(1)

actual = set(seen)
missing = expected - actual
extra = actual - expected
if missing or extra:
    print(
        "FAIL arcus-spot-rust-recorder: round-trip (sell, buy, notional_usd) set does not "
        f"match the deployed unit's configuration; missing={sorted(missing)} extra={sorted(extra)}"
    )
    sys.exit(1)

print(
    f"OK arcus-spot-rust-recorder: freshest line has all {len(expected)} configured "
    f"round trips (schema_version={schema_version!r}, mode={mode!r})"
)
PY
