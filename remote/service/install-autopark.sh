#!/bin/zsh
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# The Citus autopark as a per-user LaunchAgent (com.iq.citus-autopark): every IQ_CITUS_AUTOPARK_INTERVAL seconds
# (default 900) `deploy-citus.sh autopark` reads how long since a client last had a session on the coordinator
# (service/install-usewatch.sh); idle >= IDLE_SECS (default 7200) parks the hosts cluster -- every node stopped,
# the four snapshotted, then destroyed -- so an unused cluster stops billing. `deploy-citus.sh hosts-restore`
# brings it back. Only instances labelled iq-citus-* are touched. As UC_MarkLogic's autopark: launchd, not cron.
#   ./install-autopark.sh            install (or reinstall) and run one check now
#   ./install-autopark.sh --remove   unload the agent
set -e
UC="${0:A:h:h}"
LABEL="com.iq.citus-autopark"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG="$HOME/.iq/citus/autopark.log"
DOMAIN="gui/$(id -u)"
INTERVAL="${IQ_CITUS_AUTOPARK_INTERVAL:-900}"
IDLE="${IDLE_SECS:-7200}"

launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
if [ "${1:-}" = "--remove" ]; then rm -f "$PLIST"; echo "[autopark] $LABEL removed"; exit 0; fi

"$UC/../../../Development/UseCase/UC_App/iq-vault.sh" has VULTR_API_KEY || { echo "[autopark] no VULTR_API_KEY in the Vault; nothing installed"; exit 1; }
mkdir -p "$HOME/Library/LaunchAgents" "${LOG:h}"
cat > "$PLIST" <<PL
<!--
@use_case Development/UseCase/UC_Citus
@facet Store
Rendered by UC_Citus/service/install-autopark.sh: park the idle Citus hosts cluster (deploy-citus.sh autopark).
-->
<plist version="1.0">
<dict>
  <key>Label</key>              <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$UC/deploy-citus.sh</string>
    <string>autopark</string>
  </array>
  <key>StartInterval</key>      <integer>$INTERVAL</integer>
  <key>RunAtLoad</key>          <true/>
  <key>StandardOutPath</key>    <string>$LOG</string>
  <key>StandardErrorPath</key>  <string>$LOG</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>             <string>/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>IDLE_SECS</key>        <string>$IDLE</string>
    <key>VULTR_SOCKS_PORT</key> <string>11083</string>
  </dict>
</dict>
</plist>
PL
plutil -lint "$PLIST" >/dev/null
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "[autopark] $LABEL loaded: every ${INTERVAL}s, park after ${IDLE}s idle; log $LOG"
