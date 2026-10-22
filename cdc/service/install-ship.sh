#!/bin/zsh
# @use_case Development/UseCase/UC_CDC
# @facet Store
#
# A domain's shipper as a per-user LaunchAgent (com.iq.cdc-ship): iq-ship-launchd.sh, kept alive by launchd, ships
# this box's plane rows of one namespace to the global Citus cluster as they are written (apply.py ship).
#   ./install-ship.sh u-<k>|g-<h>    install (or reinstall) for that namespace and start it
#   ./install-ship.sh --remove       unload the agent and remove it (the slot stays; apply.py's teardown under
#                                    --hop cluster drops it, else it holds WAL up to the cap)
set -e
HERE="${0:A:h}"
LABEL="com.iq.cdc-ship"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG="$HOME/.iq/cdc-ship.log"
DOMAIN="gui/$(id -u)"

launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
if [ "${1:-}" = "--remove" ]; then rm -f "$PLIST"; echo "[iq-ship] $LABEL removed"; exit 0; fi
NS="${1:-}"
[[ "$NS" =~ '^[ug]-[0-9a-f]{24}$' ]] || { echo "usage: $0 u-<k>|g-<h>  (the namespace this box is the domain of)"; exit 2; }
IQ_SHIP_NAMESPACE="$NS" "$HERE/iq-ship-launchd.sh" --print
mkdir -p "$HOME/Library/LaunchAgents" "${LOG:h}"
cat > "$PLIST" <<PL
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!--
@use_case Development/UseCase/UC_CDC
@facet Store
Rendered by PostgreSqlIq cdc/service/install-ship.sh: ship this box's namespace $NS to the Citus cluster.
-->
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array><string>/bin/zsh</string><string>$HERE/iq-ship-launchd.sh</string></array>
  <key>EnvironmentVariables</key><dict><key>IQ_SHIP_NAMESPACE</key><string>$NS</string></dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>300</integer>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
</dict>
</plist>
PL
plutil -lint "$PLIST" >/dev/null
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "[iq-ship] $LABEL installed for $NS; log: $LOG"
