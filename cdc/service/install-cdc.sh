#!/bin/zsh
# @use_case Development/UseCase/UC_CDC
# @facet Store
#
# The box's CDC streamer as your own LaunchAgent (com.iq.cdc): iq-cdc-launchd.sh, kept alive by launchd, streams
# the box's Postgres to the IQ Server (iq-cdc.sh serve) so its slot is read and never lost to the WAL cap. Opted
# into here, by hand; the IQ app never installs it.
#   ./install-cdc.sh             install (or reinstall) and start it
#   ./install-cdc.sh --remove    unload and remove it (the slot stays: iq-cdc.sh teardown drops it)
set -e
HERE="${0:A:h}"
LABEL="com.iq.cdc"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG="$HOME/.iq/cdc/cdc.log"
DOMAIN="gui/$(id -u)"

launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
if [ "${1:-}" = "--remove" ]; then rm -f "$PLIST"; echo "[iq-cdc] $LABEL removed"; exit 0; fi
"$HERE/iq-cdc-launchd.sh" --print
mkdir -p "$HOME/Library/LaunchAgents" "${LOG:h}"
cat > "$PLIST" <<PL
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!--
@use_case Development/UseCase/UC_CDC
@facet Store
Rendered by PostgreSqlIq cdc/service/install-cdc.sh: stream the box's Postgres to the IQ Server (iq-cdc.sh serve).
-->
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array><string>/bin/zsh</string><string>$HERE/iq-cdc-launchd.sh</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>120</integer>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
</dict>
</plist>
PL
plutil -lint "$PLIST" >/dev/null
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "[iq-cdc] $LABEL installed; log: $LOG"
