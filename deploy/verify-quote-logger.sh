#!/usr/bin/env bash
# Verify the RWA quote-status logger wrote fresh status rows for every
# configured label and notional (bot-strategy#592). RATE_LIMITED/ERROR rows
# are accepted as liveness evidence; useful-routability coverage is a separate
# readout, not a deploy-health condition.
#
# Args:
#   $1 unit      systemd unit name (default: rwa-quote-logger)
#   $2 jsonl     path to today's JSONL
#   $3 baseline  file holding the pre-restart line count
set -u

unit=${1:-rwa-quote-logger}
f=${2:-/opt/debot-rwa/rwa_quote_$(date -u +%Y%m%d).jsonl}
baseline_file=${3:-/tmp/rwa_quote_baseline}
base=$(cat "$baseline_file" 2>/dev/null || echo 0)
unit_dir="${UNIT_DIR:-/etc/systemd/system}"

spec=$(sed -n 's/^Environment=RWA_QUOTE_TOKENS=//p' "${unit_dir}/${unit}.service")
notionals=$(sed -n 's/^Environment=RWA_QUOTE_NOTIONALS_USD=//p' "${unit_dir}/${unit}.service")

if [ -z "$spec" ] || [ -z "$notionals" ]; then
  echo "FAIL $unit: could not parse RWA_QUOTE_TOKENS/RWA_QUOTE_NOTIONALS_USD from unit"
  exit 1
fi

cur=$base
err="(no new rows)"
for _ in $(seq 1 90); do
  cur=$(wc -l < "$f" 2>/dev/null || echo 0)
  if [ "$cur" -gt "$base" ]; then
    if python3 - "$f" "$base" "$spec" "$notionals" <<'PY'
import json
import sys

path = sys.argv[1]
base = int(sys.argv[2])
spec = sys.argv[3]
notionals = [float(x) for x in sys.argv[4].split(",") if x.strip()]
labels = [entry.split(":", 1)[0] for entry in spec.split(",") if entry.strip()]
expected = {(label, n) for label in labels for n in notionals}
seen = set()
statuses = {"ROUTABLE", "NO_ROUTE", "RATE_LIMITED", "ERROR", "PARTIAL_ROUTABLE"}

with open(path) as f:
    for i, line in enumerate(f, start=1):
        if i <= base:
            continue
        row = json.loads(line)
        status = row.get("quote_status")
        if status not in statuses:
            raise SystemExit(f"bad quote_status {status!r} in {row}")
        label = row.get("label")
        notional = row.get("notional_usd")
        if isinstance(notional, (int, float)):
            for expected_notional in notionals:
                if abs(float(notional) - expected_notional) < 1e-9:
                    seen.add((label, expected_notional))

missing = sorted(expected - seen)
if missing:
    raise SystemExit(f"missing rows: {missing[:12]}")
print(f"ok labels={len(labels)} notionals={len(notionals)} rows={len(seen)}")
PY
    then
      echo "OK $unit: all labels/notionals have fresh quote_status rows (${base} -> ${cur})"
      tail -1 "$f"
      exit 0
    else
      err="python validation failed"
    fi
  fi
  sleep 2
done

echo "FAIL $unit: grew ${base} -> ${cur}; ${err}"
journalctl -u "$unit" -n 80 --no-pager 2>/dev/null || true
exit 1
