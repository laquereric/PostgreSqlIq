#!/bin/bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# pgBackRest on one host, for every Citus node on it: WAL archived continuously and full backups, into Vultr Object
# Storage (bucket BUCKET on HOST, path /<cluster>/<stanza>). Runs ON the host as root (deploy-citus.sh backup-up,
# which pipes the S3 key in on stdin -- it is kept only in /etc/pgbackrest/pgbackrest.conf, root:postgres 0640).
# Each node is restarted once (archive_mode); then stanza-create, check, and a first full backup. Then the schedule,
# as systemd timers per stanza: a full backup every Sunday at 03:00 UTC and a differential on the other days, so the
# WAL replayed after a restore stays a week at most and retention-full=2 keeps two weeks. Idempotent.
set -euo pipefail
: "${HOST:?}" "${BUCKET:?}" "${CLUSTER:?}"
read -r KEYLINE
K=${KEYLINE%%:*}; S=${KEYLINE#*:}
BIN=/usr/pgsql-17/bin
rpm -q pgbackrest >/dev/null 2>&1 || dnf -y -q install pgbackrest
mkdir -p /etc/pgbackrest /var/log/pgbackrest /var/spool/pgbackrest
chown postgres:postgres /var/log/pgbackrest /var/spool/pgbackrest
{
  cat <<CONF
[global]
repo1-type=s3
repo1-s3-endpoint=$HOST
repo1-s3-bucket=$BUCKET
repo1-s3-region=us-east-1
repo1-s3-uri-style=path
repo1-s3-key=$K
repo1-s3-key-secret=$S
repo1-path=/$CLUSTER
repo1-retention-full=2
compress-type=zst
start-fast=y
archive-async=y
spool-path=/var/spool/pgbackrest
log-level-console=warn
log-level-file=info
CONF
  for conf in /var/lib/pgsql/17/*/iq.conf; do
    d=$(dirname "$conf"); unit=$(basename "$d"); port=$(awk '/^port/ {print $3}' "$conf")
    printf '\n[%s-%s]\npg1-path=%s\npg1-port=%s\npg1-socket-path=/run/postgresql\n' "$(hostname)" "$unit" "$d" "$port"
  done
} > /etc/pgbackrest/pgbackrest.conf
chown root:postgres /etc/pgbackrest/pgbackrest.conf; chmod 640 /etc/pgbackrest/pgbackrest.conf
for conf in /var/lib/pgsql/17/*/iq.conf; do
  d=$(dirname "$conf"); unit=$(basename "$d"); port=$(awk '/^port/ {print $3}' "$conf"); stanza="$(hostname)-$unit"
  if ! grep -q '^archive_mode = on' "$conf"; then
    printf "archive_mode = on\narchive_command = 'pgbackrest --stanza=%s archive-push %%p'\n" "$stanza" >> "$conf"
    systemctl restart "iq-citus@$unit"
    for _ in $(seq 30); do sudo -u postgres "$BIN/pg_isready" -q -p "$port" && break; sleep 1; done
  fi
  sudo -u postgres pgbackrest --stanza="$stanza" stanza-create >/dev/null
  sudo -u postgres pgbackrest --stanza="$stanza" check >/dev/null
  sudo -u postgres pgbackrest --stanza="$stanza" --type=full backup
  echo "$stanza: $(sudo -u postgres pgbackrest --stanza="$stanza" info --output=json | python3 -c 'import json,sys; s=json.load(sys.stdin)[0]; print(s["status"]["message"], len(s["backup"]), "backup(s)")')"
done

# the schedule: iq-pgbackrest-<type>@<stanza>.service, each started by its timer
for kind in full diff; do
  cat > "/etc/systemd/system/iq-pgbackrest-$kind@.service" <<UNIT
[Unit]
Description=pgBackRest $kind backup of %i
After=network-online.target
[Service]
Type=oneshot
User=postgres
ExecStart=/usr/bin/pgbackrest --stanza=%i --type=$kind backup
UNIT
done
for conf in /var/lib/pgsql/17/*/iq.conf; do
  stanza="$(hostname)-$(basename "$(dirname "$conf")")"
  for t in full:"Sun *-*-* 03:00:00" diff:"Mon..Sat *-*-* 03:00:00"; do
    kind=${t%%:*}; when=${t#*:}
    cat > "/etc/systemd/system/iq-pgbackrest-$kind-$stanza.timer" <<TIMER
[Unit]
Description=pgBackRest $kind backup of $stanza
[Timer]
OnCalendar=$when UTC
Persistent=true
Unit=iq-pgbackrest-$kind@$stanza.service
[Install]
WantedBy=timers.target
TIMER
  done
  systemctl daemon-reload
  systemctl enable -q --now "iq-pgbackrest-full-$stanza.timer" "iq-pgbackrest-diff-$stanza.timer"
  echo "$stanza: next diff $(systemctl show -p NextElapseUSecRealtime --value "iq-pgbackrest-diff-$stanza.timer"), next full $(systemctl show -p NextElapseUSecRealtime --value "iq-pgbackrest-full-$stanza.timer")"
done
