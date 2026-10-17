#!/bin/bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# A Citus cluster on one Vultr host (Rocky Linux 9), for testing Postgres at scale against MarkLogic's transaction
# guarantees. Runs ON the host as root (`ssh root@<host> bash -s < install-citus.sh`), so no password leaves it.
# Idempotent.
#
#   PostgreSQL 17 (PGDG) -- the major version of IQ's pgembed -- with Citus 14.2 (citusdata/community) and pgvector
#   four nodes, each its own cluster and systemd unit iq-citus@<node>, all on 127.0.0.1 only:
#       coord :5432   the coordinator (holds the reference tables, no shards)
#       w1    :5433   worker
#       w2    :5434   worker
#       w3    :5435   a spare worker, running but NOT in the cluster: the scale-out scenario adds it and rebalances
#   wal_level=logical (shards move online), max_prepared_transactions (two-phase commit across nodes)
#   roles: postgres (superuser, reached over ssh only) and iq (owner of database iq, no superuser); both passwords
#   generated here into /root/citus-{postgres,iq}.txt (0600) and /var/lib/pgsql/.pgpass, which is how nodes reach
#   one another. Nothing listens beyond loopback: the client comes in through an SSH tunnel to :5432.
set -euo pipefail
PGV=17
CITUS_PKG="${CITUS_PKG:-citus142_17}"
BIN=/usr/pgsql-$PGV/bin
BASE=/var/lib/pgsql/$PGV
NODES="coord:5432 w1:5433 w2:5434 w3:5435"
say() { echo "$(date '+%F %T') $*"; }

# 1. packages
if ! rpm -q "$CITUS_PKG" >/dev/null 2>&1; then
  rpm -q pgdg-redhat-repo >/dev/null 2>&1 || dnf -y -q install \
    https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
  dnf -qy module disable postgresql
  cat > /etc/yum.repos.d/citusdata_community.repo <<'REPO'
[citusdata_community]
name=citusdata_community
baseurl=https://packagecloud.io/citusdata/community/el/9/$basearch
repo_gpgcheck=1
gpgcheck=0
enabled=1
gpgkey=https://packagecloud.io/citusdata/community/gpgkey
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
REPO
  dnf -y -q install "postgresql$PGV-server" "postgresql$PGV-contrib" "pgvector_$PGV" "$CITUS_PKG"
  say "installed $(rpm -q "$CITUS_PKG" "pgvector_$PGV" "postgresql$PGV-server" | tr '\n' ' ')"
fi
[ -f /swapfile ] || { fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap -q /swapfile && swapon /swapfile && echo '/swapfile none swap sw 0 0' >> /etc/fstab; }

# 2. passwords (generated once, here)
umask 077
for r in postgres iq; do [ -s /root/citus-$r.txt ] || printf '%s:%s\n' "$r" "$(openssl rand -hex 24)" > /root/citus-$r.txt; done
PW_PG="$(cut -d: -f2- /root/citus-postgres.txt)"; PW_IQ="$(cut -d: -f2- /root/citus-iq.txt)"
{ for h in localhost 127.0.0.1; do printf '%s:*:*:postgres:%s\n%s:*:*:iq:%s\n' "$h" "$PW_PG" "$h" "$PW_IQ"; done; } > /var/lib/pgsql/.pgpass
chown postgres:postgres /var/lib/pgsql/.pgpass; chmod 600 /var/lib/pgsql/.pgpass
umask 022

# 3. the nodes
cat > /etc/systemd/system/iq-citus@.service <<UNIT
[Unit]
Description=IQ Citus node %i
After=network.target
[Service]
Type=notify
User=postgres
Group=postgres
ExecStart=$BIN/postgres -D $BASE/%i
ExecReload=/bin/kill -HUP \$MAINPID
KillMode=mixed
KillSignal=SIGINT
TimeoutSec=300
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
for np in $NODES; do
  n=${np%%:*}; p=${np##*:}; d=$BASE/$n
  if [ ! -s "$d/PG_VERSION" ]; then
    sudo -u postgres "$BIN/initdb" -D "$d" -E UTF8 --data-checksums --auth-local=peer --auth-host=scram-sha-256 >/dev/null
    echo "include_if_exists = 'iq.conf'" >> "$d/postgresql.conf"
  fi
  cat > "$d/iq.conf" <<CONF
port = $p
listen_addresses = '127.0.0.1'
unix_socket_directories = '/run/postgresql'
shared_preload_libraries = 'citus'
wal_level = logical
max_prepared_transactions = 100
max_connections = 100
shared_buffers = 128MB
password_encryption = scram-sha-256
CONF
  chown postgres:postgres "$d/iq.conf"
  systemctl enable -q --now "iq-citus@$n"
done
psql_n() { sudo -u postgres "$BIN/psql" -X -q -v ON_ERROR_STOP=1 -p "$1" "${@:2}"; }
for np in $NODES; do
  p=${np##*:}
  for _ in $(seq 30); do sudo -u postgres "$BIN/pg_isready" -q -p "$p" && break; sleep 1; done
  # passwords go in on stdin, never in argv
  psql_n "$p" -d postgres <<SQL
ALTER ROLE postgres PASSWORD '$PW_PG';
DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iq') THEN CREATE ROLE iq LOGIN; END IF; END \$\$;
ALTER ROLE iq PASSWORD '$PW_IQ';
SQL
  [ "$(psql_n "$p" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = 'iq'")" = 1 ] || psql_n "$p" -d postgres -c "CREATE DATABASE iq OWNER iq"
  psql_n "$p" -d iq -c "CREATE EXTENSION IF NOT EXISTS citus; CREATE EXTENSION IF NOT EXISTS vector;"
done

# 4. the cluster: coordinator + w1 + w2 (w3 waits for the scale-out scenario)
psql_n 5432 -d iq <<'SQL'
SELECT citus_set_coordinator_host('localhost', 5432);
SELECT citus_add_node('localhost', 5433) WHERE NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodeport = 5433);
SELECT citus_add_node('localhost', 5434) WHERE NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodeport = 5434);
SQL
say "citus $(psql_n 5432 -d iq -Atc "SELECT extversion FROM pg_extension WHERE extname='citus'"), pgvector $(psql_n 5432 -d iq -Atc "SELECT extversion FROM pg_extension WHERE extname='vector'"), $(psql_n 5432 -d iq -Atc 'SHOW server_version')"
say "workers: $(psql_n 5432 -d iq -Atc "SELECT string_agg(node_name || ':' || node_port, ' ' ORDER BY node_port) FROM citus_get_active_worker_nodes()")"
