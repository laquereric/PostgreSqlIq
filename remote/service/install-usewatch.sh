#!/bin/bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# What autopark reads: once a minute (systemd timer iq-citus-use) the coordinator touches /var/lib/iq-citus/last-use
# when any application role -- iq, iq_gate, iq_agent -- has a session open. Superuser maintenance over ssh (postgres)
# and the timer itself are not use. Runs ON the coordinator as root. Idempotent.
set -euo pipefail
mkdir -p /var/lib/iq-citus
cat > /usr/local/bin/iq-citus-use <<'SH'
#!/bin/bash
n=$(sudo -u postgres /usr/pgsql-17/bin/psql -X -At -p 5432 -d iq -c \
  "SELECT count(*) FROM pg_stat_activity WHERE usename IN ('iq', 'iq_gate', 'iq_agent') AND backend_type = 'client backend'" 2>/dev/null || echo 0)
[ "${n:-0}" -gt 0 ] && touch /var/lib/iq-citus/last-use
exit 0
SH
chmod 755 /usr/local/bin/iq-citus-use
cat > /etc/systemd/system/iq-citus-use.service <<'UNIT'
[Unit]
Description=IQ Citus: note a client session (for autopark)
[Service]
Type=oneshot
ExecStart=/usr/local/bin/iq-citus-use
UNIT
cat > /etc/systemd/system/iq-citus-use.timer <<'UNIT'
[Unit]
Description=IQ Citus: note client sessions once a minute
[Timer]
OnBootSec=60
OnUnitActiveSec=60
[Install]
WantedBy=timers.target
UNIT
systemctl daemon-reload
systemctl enable -q --now iq-citus-use.timer
[ -e /var/lib/iq-citus/last-use ] || touch /var/lib/iq-citus/last-use
echo "use watch on: $(systemctl is-active iq-citus-use.timer)"
