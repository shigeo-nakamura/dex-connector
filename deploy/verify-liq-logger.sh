#!/usr/bin/env bash
# Verify a freshly (re)started liquidation logger is LIVE and BOTH venues are
# polling OK (bot-strategy#571). Used by deploy-rwa-logger.yml via SSM.
#
# Unlike verify-logger.sh (rwa-spot / apex-perp), this logger must NOT be
# checked by "did the output file grow per label": liquidations are rare events,
# so a healthy logger normally writes zero rows for minutes/hours. Instead we
# assert the per-poll heartbeat line shows every configured market polling OK:
#
#   [HEARTBEAT] poll ok: lighter=2/2, extended=2/2, new_liq=0, total_liq=0
#
# i.e. for each venue, ok == total (a configured-but-unreachable venue would
# show ok < total), and at least one venue has total >= 1.
#
# Args:
#   $1 unit   systemd unit name (default: liq-logger)
#   $2 since  journalctl --since value to scope to this restart (e.g.
#             "2026-06-15 18:40:00"); optional but recommended.
set -u

unit=${1:-liq-logger}
since=${2:-}

if ! systemctl is-active --quiet "$unit"; then
  echo "FAIL $unit: unit is not active"
  journalctl -u "$unit" -n 40 --no-pager 2>/dev/null || true
  exit 1
fi

since_arg=()
[ -n "$since" ] && since_arg=(--since "$since")

# Poll up to ~60s for a heartbeat in which BOTH venues are fully OK.
last=""
for _ in $(seq 1 30); do
  line=$(journalctl -u "$unit" "${since_arg[@]}" --no-pager 2>/dev/null \
    | grep -F '[HEARTBEAT] poll ok:' | tail -1)
  if [ -n "$line" ]; then
    last=$line
    lighter=$(printf '%s' "$line" | grep -oE 'lighter=[0-9]+/[0-9]+' | head -1 | cut -d= -f2)
    extended=$(printf '%s' "$line" | grep -oE 'extended=[0-9]+/[0-9]+' | head -1 | cut -d= -f2)
    lo=${lighter%/*}; lt=${lighter#*/}
    eo=${extended%/*}; et=${extended#*/}
    # Each configured venue must be fully OK (ok==total); an unconfigured venue
    # is total=0 (trivially OK). Require at least one venue actually polling.
    if [ "${lo:-0}" = "${lt:-0}" ] && [ "${eo:-0}" = "${et:-0}" ] \
       && [ "$(( ${lt:-0} + ${et:-0} ))" -ge 1 ]; then
      echo "OK $unit: heartbeat healthy ($line)"
      exit 0
    fi
  fi
  sleep 2
done

echo "FAIL $unit: no heartbeat with both venues fully OK within ~60s"
[ -n "$last" ] && echo "last heartbeat: $last"
journalctl -u "$unit" -n 40 --no-pager 2>/dev/null || true
exit 1
