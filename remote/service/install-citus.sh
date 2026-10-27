#!/bin/bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# A Citus node, or a whole cluster on one host, on a Vultr instance (Rocky Linux 9), for testing Postgres at scale
# against MarkLogic's transaction guarantees. Runs ON the host as root (`ssh root@<host> ROLE=... bash -s <
# install-citus.sh`), so no password leaves it. Idempotent.
#
#   PostgreSQL 17 (PGDG) -- the major version of IQ's pgembed -- with Citus 14.2 (citusdata/community) and pgvector
#   ROLE=single (slice 1, the default): four nodes on this host, each its own cluster and systemd unit
#       iq-citus@<node>, all on 127.0.0.1 only:
#         coord :5432 the coordinator (reference tables, no shards); w1 :5433, w2 :5434 workers; w3 :5435 a spare,
#         running but NOT in the cluster (the scale-out scenario adds it)
#   ROLE=server (the default shape, deploy-citus.sh shape): ONE plain PostgreSQL 17 on :5432, loopback only -- the
#       Citus package is installed but neither preloaded nor created, so making this server a Citus coordinator later
#       is a migration on the same box (MIGRATE-to-Citus.md), not a reinstall
#   ROLE=coordinator | worker (slice 2): one node, iq-citus@coord or iq-citus@node on :5432, listening on loopback
#       and on PRIVATE_IP (the Vultr VPC); pg_hba and firewalld admit SUBNET only, to 5432 only. A worker expects
#       /root/citus-{postgres,iq}.txt already copied from the coordinator (deploy-citus.sh does it over ssh).
#   wal_level=logical (shards move online), max_prepared_transactions (two-phase commit across nodes)
#   roles: postgres (superuser, reached over ssh only) and iq (owner of database iq, no superuser); both passwords
#   generated on the coordinator into /root/citus-{postgres,iq}.txt (0600) and ~postgres/.pgpass, which is how the
#   nodes reach one another. Nothing listens on the public address: the client comes in through an SSH tunnel.
set -euo pipefail
PGV=17
CITUS_PKG="${CITUS_PKG:-citus142_17}"
BIN=/usr/pgsql-$PGV/bin
BASE=/var/lib/pgsql/$PGV
ROLE="${ROLE:-single}"
PRELOAD=citus; [ "$ROLE" = server ] && PRELOAD=''
case "$ROLE" in
  server) NODES="coord:5432"; LISTEN=127.0.0.1 ;;
  single) NODES="coord:5432 w1:5433 w2:5434 w3:5435"; LISTEN=127.0.0.1 ;;
  coordinator) NODES="coord:5432"; LISTEN="127.0.0.1,${PRIVATE_IP:?}"; : "${SUBNET:?}" ;;
  worker) NODES="node:5432"; LISTEN="127.0.0.1,${PRIVATE_IP:?}"; : "${SUBNET:?}"
          [ -s /root/citus-iq.txt ] && [ -s /root/citus-postgres.txt ] || { echo "no passwords from the coordinator" >&2; exit 1; } ;;
  *) echo "ROLE is server, single, coordinator or worker" >&2; exit 2 ;;
esac
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
printf '*:*:*:postgres:%s\n*:*:*:iq:%s\n' "$PW_PG" "$PW_IQ" > /var/lib/pgsql/.pgpass
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
listen_addresses = '$LISTEN'
unix_socket_directories = '/run/postgresql'
shared_preload_libraries = '$PRELOAD'
wal_level = logical
max_prepared_transactions = 100
max_connections = 100
shared_buffers = 128MB
password_encryption = scram-sha-256
CONF
  chown postgres:postgres "$d/iq.conf"
  if [ -n "${SUBNET:-}" ] && ! grep -q "$SUBNET" "$d/pg_hba.conf"; then
    echo "host all all $SUBNET scram-sha-256" >> "$d/pg_hba.conf"
  fi
  systemctl enable -q --now "iq-citus@$n"
done
psql_n() { sudo -u postgres "$BIN/psql" -X -q -v ON_ERROR_STOP=1 -p "$1" "${@:2}"; }
for np in $NODES; do
  p=${np##*:}
  for _ in $(seq 30); do sudo -u postgres "$BIN/pg_isready" -q -p "$p" && break; sleep 1; done
  # a worker already in the cluster takes role changes from the coordinator only ("not allowed on this node")
  if [ "$p" != 5432 ] || [ "$ROLE" = worker ]; then
    members=$(psql_n "$p" -d iq -Atc "SELECT count(*) FROM pg_dist_node" 2>/dev/null || echo 0)
    [ "$members" -gt 0 ] && continue
  fi
  # passwords go in on stdin, never in argv
  psql_n "$p" -d postgres <<SQL
ALTER ROLE postgres PASSWORD '$PW_PG';
DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iq') THEN CREATE ROLE iq LOGIN; END IF; END \$\$;
ALTER ROLE iq PASSWORD '$PW_IQ';
SQL
  [ "$(psql_n "$p" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = 'iq'")" = 1 ] || psql_n "$p" -d postgres -c "CREATE DATABASE iq OWNER iq"
  [ "$ROLE" = server ] || psql_n "$p" -d iq -c "CREATE EXTENSION IF NOT EXISTS citus;"
  psql_n "$p" -d iq -c "CREATE EXTENSION IF NOT EXISTS vector;"
done

# the VPC: Postgres on the private address, from the subnet only
if [ -n "${SUBNET:-}" ]; then
  rule="rule family=ipv4 source address=$SUBNET port port=5432 protocol=tcp accept"
  firewall-cmd -q --query-rich-rule="$rule" || { firewall-cmd -q --permanent --add-rich-rule="$rule"; firewall-cmd -q --reload; }
fi
[ "$ROLE" = server ] && { say "server: PostgreSQL $(psql_n 5432 -d iq -Atc 'SHOW server_version'), pgvector $(psql_n 5432 -d iq -Atc "SELECT extversion FROM pg_extension WHERE extname='vector'"), Citus installed, not loaded"; exit 0; }
[ "$ROLE" = worker ] && { say "worker on $PRIVATE_IP:5432: $(psql_n 5432 -d iq -Atc "SELECT 'citus ' || extversion FROM pg_extension WHERE extname='citus'")"; exit 0; }

# 4. the cluster (single): coordinator + w1 + w2 (w3 waits for the scale-out scenario); a coordinator's workers are
#    added by deploy-citus.sh once they are installed
if [ "$ROLE" = single ]; then
  psql_n 5432 -d iq <<'SQL'
SELECT citus_set_coordinator_host('localhost', 5432);
SELECT citus_add_node('localhost', 5433) WHERE NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodeport = 5433);
SELECT citus_add_node('localhost', 5434) WHERE NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodeport = 5434);
SQL
else
  psql_n 5432 -d iq -c "SELECT citus_set_coordinator_host('$PRIVATE_IP', 5432)"
fi
say "citus $(psql_n 5432 -d iq -Atc "SELECT extversion FROM pg_extension WHERE extname='citus'"), pgvector $(psql_n 5432 -d iq -Atc "SELECT extversion FROM pg_extension WHERE extname='vector'"), $(psql_n 5432 -d iq -Atc 'SHOW server_version')"
say "workers: $(psql_n 5432 -d iq -Atc "SELECT string_agg(node_name || ':' || node_port, ' ' ORDER BY node_port) FROM citus_get_active_worker_nodes()")"
