#!/bin/zsh
# @use_case Development/UseCase/UC_CDC
# @facet Store
#
# launchd entry point of the box's CDC streamer (com.iq.cdc, install-cdc.sh): `iq-cdc.sh serve`, which readies the
# slot -- a lost one rebuilt, a needed or incomplete snapshot run, a down IQ Server waited out -- and then streams
# every IQ_CDC_INTERVAL seconds (default 10), so the slot is read and never holds WAL to its cap. Installed by hand,
# never by the app. A box whose Postgres is down, or that has no slot, ends the run; launchd starts it again after
# its ThrottleInterval.
#
#   iq-cdc-launchd.sh            run (launchd does)
#   iq-cdc-launchd.sh --print    the configuration it would run with, and exit
set -eu
IQ_REPO="${IQ_REPO:-$HOME/NoIcloud/Iq}"
CDC="$IQ_REPO/vendor/PostgreSqlIq/cdc"
INTERVAL="${IQ_CDC_INTERVAL:-10}"
HOP="${IQ_CDC_HOP:-}"
say() { echo "[iq-cdc] $*" >&2; }

ARGS=()
[ -n "$HOP" ] && ARGS+=(--hop "$HOP")
ARGS+=(serve --interval "$INTERVAL")
if [ "${1:-}" = "--print" ]; then
  echo "cdc=$CDC/iq-cdc.sh args=${ARGS[*]} server=${IQ_MEMORY:-https://memory.intentionalquality.com}"
  exit 0
fi
if ! "$IQ_REPO/vendor/PostgreSqlIq/local/iq-pg.sh" --check; then
  say "the box's Postgres is down: waiting for the next start"; sleep 60; exit 1
fi
say "serving ${HOP:-the default hop} every ${INTERVAL}s"
exec "$CDC/iq-cdc.sh" "${ARGS[@]}"
