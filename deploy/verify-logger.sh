#!/usr/bin/env bash
# Verify a freshly (re)started RWA logger is producing FRESH, per-symbol
# non-null data (bot-strategy#574). Used by deploy-rwa-logger.yml via SSM.
#
# It is not enough that the output file grows: ApeX/Jupiter return HTTP 200
# with an empty/null payload for a bad/delisted symbol, so a typo for some of
# the configured symbols would otherwise deploy green while collecting nulls
# for those symbols. So we require, for EVERY configured label, a fresh row
# (written after the pre-restart baseline) whose value field is non-null.
#
# Args:
#   $1 unit         systemd unit name (e.g. rwa-spot-logger)
#   $2 jsonl        path to today's JSONL (e.g. /opt/debot-rwa/rwa_spot_YYYYMMDD.jsonl)
#   $3 env_var      Environment var in the unit listing label:value pairs (RWA_TOKENS|APEX_SYMBOLS)
#   $4 field        JSON value field that must be non-null (usd_price|funding_rate)
#   $5 baseline     file holding the pre-restart line count
set -u

unit=$1
f=$2
env_var=$3
field=$4
baseline_file=$5

base=$(cat "$baseline_file" 2>/dev/null || echo 0)

# Unit dir is overridable only so the script can be exercised in a test harness.
unit_dir="${UNIT_DIR:-/etc/systemd/system}"

# Configured labels, parsed from the DEPLOYED unit's Environment=<env_var>=...
spec=$(sed -n "s/^Environment=${env_var}=//p" "${unit_dir}/${unit}.service")
labels=""
for pair in ${spec//,/ }; do
  labels="$labels ${pair%%:*}"
done
if [ -z "${labels// /}" ]; then
  echo "FAIL $unit: could not parse any labels from ${env_var} in the unit file"
  exit 1
fi

cur=$base
miss="(no new rows)"
# Poll up to ~60s for the restarted process to emit a fresh row per label.
for _ in $(seq 1 30); do
  cur=$(wc -l < "$f" 2>/dev/null || echo 0)
  if [ "$cur" -gt "$base" ]; then
    new=$(tail -n +"$((base + 1))" "$f")
    miss=""
    for label in $labels; do
      # -F fixed-string incl. the closing quote so e.g. SPCX != SPCXx/SPCXon.
      if ! printf '%s\n' "$new" \
        | grep -F "\"label\":\"${label}\"" \
        | grep -qE "\"${field}\":-?[0-9]"; then
        miss="$miss $label"
      fi
    done
    [ -z "$miss" ] && break
  fi
  sleep 2
done

if [ -z "$miss" ] && [ "$cur" -gt "$base" ]; then
  echo "OK $unit: all configured labels have fresh non-null ${field} (${base} -> ${cur} lines)"
  tail -1 "$f"
  exit 0
fi

echo "FAIL $unit: grew ${base} -> ${cur}; labels missing fresh non-null ${field}:${miss}"
journalctl -u "$unit" -n 40 --no-pager 2>/dev/null || true
exit 1
