#!/bin/bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# The cold tier on the coordinator: Iceberg tables through pg_lake, their files on S3-compatible object storage.
# Runs ON the coordinator as root (deploy-citus.sh lake-up copies the pg_lake build up first, to /root). Idempotent.
#
#   pg_lake       the build from build-pg-lake.sh, unpacked under /usr/pgsql-17 (+ /usr/local), its runtime rpms
#                 installed; `pg_extension_base` preloaded after citus; the extensions created on the COORDINATOR
#                 only (DDL propagation off: the workers hold no Iceberg tables)
#   pgduck_server DuckDB behind the Postgres protocol, systemd unit iq-pgduck, unix socket only
#                 (/var/run/iq-pgduck, group postgres), its S3 secret in an init file readable by postgres only
#   object store  S3_ENDPOINT set (deploy-citus.sh lake-vultr): Vultr Object Storage, bucket S3_BUCKET, key in S3_KEY_FILE.
#                 S3_ENDPOINT unset (a test with no bucket): SeaweedFS 4.48 (Apache-2.0; MinIO's community binaries are withdrawn), pinned by
#                 sha256, unit iq-s3 on 127.0.0.1:8333 only, one bucket `iq-lake`, a key generated here into
#                 /root/iq-s3.txt.
set -euo pipefail
BIN=/usr/pgsql-17/bin
DATA=/var/lib/pgsql/17/coord
WEED_URL=https://github.com/seaweedfs/seaweedfs/releases/download/4.48/linux_amd64.tar.gz
WEED_SHA=4a7d108384d044d95212d1342cdda9533fa55842c1c9b41f606ca3c8a9561124
say() { echo "$(date '+%F %T') $*"; }
TAR=$(ls -t /root/pg_lake-*-pg17-el9.tar.gz | head -1)

# 1. pg_lake
if [ ! -x "$BIN/pgduck_server" ]; then
  dnf -y -q install epel-release; dnf config-manager --set-enabled crb
  tar -xzf "$TAR" -C / usr/pgsql-17/share/pg_lake.runtime-rpms
  xargs -r dnf -y -q install < /usr/pgsql-17/share/pg_lake.runtime-rpms
  tar -xzf "$TAR" -C /
  printf '/usr/local/lib\n/usr/local/lib64\n' > /etc/ld.so.conf.d/iq-pg_lake.conf; ldconfig
  say "unpacked $(basename "$TAR"); missing libraries: $(ldd "$BIN/pgduck_server" | grep -c 'not found' || true)"
fi

# 2. the object store
umask 077
if [ -z "${S3_ENDPOINT:-}" ]; then
  if [ ! -x /usr/local/bin/weed ]; then
    curl -fsSL -o /root/weed.tar.gz "$WEED_URL"
    echo "$WEED_SHA  /root/weed.tar.gz" | sha256sum -c --quiet
    tar -xzf /root/weed.tar.gz -C /usr/local/bin weed; rm -f /root/weed.tar.gz
  fi
  [ -s /root/iq-s3.txt ] || printf '%s:%s\n' "iq$(openssl rand -hex 8)" "$(openssl rand -hex 24)" > /root/iq-s3.txt
  KEY=$(cut -d: -f1 /root/iq-s3.txt); SECRET=$(cut -d: -f2- /root/iq-s3.txt)
  mkdir -p /etc/iq-s3 /var/lib/iq-s3
  cat > /etc/iq-s3/s3.json <<JSON
{"identities": [{"name": "iq", "credentials": [{"accessKey": "$KEY", "secretKey": "$SECRET"}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
JSON
  cat > /etc/systemd/system/iq-s3.service <<'UNIT'
[Unit]
Description=IQ cold tier object store (SeaweedFS S3, loopback only)
After=network.target
[Service]
ExecStart=/usr/local/bin/weed server -dir=/var/lib/iq-s3 -ip=127.0.0.1 -ip.bind=127.0.0.1 -s3 -s3.port=8333 -s3.config=/etc/iq-s3/s3.json -volume.max=8
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload; systemctl enable -q --now iq-s3
  for _ in $(seq 60); do curl -s -o /dev/null http://127.0.0.1:8333/ && break; sleep 1; done
  sleep 3
  echo "s3.bucket.create -name iq-lake" | /usr/local/bin/weed shell -master=127.0.0.1:9333 >/dev/null 2>&1 || true
  S3_ENDPOINT=127.0.0.1:8333; S3_BUCKET=iq-lake; S3_SSL=false
else
  KEY=$(cut -d: -f1 "${S3_KEY_FILE:?}"); SECRET=$(cut -d: -f2- "$S3_KEY_FILE"); S3_SSL=true
fi

# 3. pgduck_server, its secret readable by postgres only
mkdir -p /etc/iq-pgduck /var/run/iq-pgduck /var/lib/iq-pgduck
cat > /etc/iq-pgduck/init.sql <<SQL
CREATE OR REPLACE PERSISTENT SECRET iq_lake (TYPE s3, SCOPE 's3://$S3_BUCKET', KEY_ID '$KEY', SECRET '$SECRET',
  ENDPOINT '$S3_ENDPOINT', URL_STYLE 'path', USE_SSL $S3_SSL, REGION 'us-east-1');
SQL
chown -R postgres:postgres /etc/iq-pgduck /var/run/iq-pgduck /var/lib/iq-pgduck; chmod 700 /etc/iq-pgduck
cat > /etc/systemd/system/iq-pgduck.service <<UNIT
[Unit]
Description=IQ pgduck_server (DuckDB for pg_lake), unix socket only
After=network.target iq-s3.service
[Service]
User=postgres
Group=postgres
RuntimeDirectory=iq-pgduck
RuntimeDirectoryMode=0750
ExecStart=$BIN/pgduck_server --unix_socket_directory /var/run/iq-pgduck --unix_socket_group postgres --port 5332 \
  --cache_dir /var/lib/iq-pgduck/cache --duckdb_database_file_path /var/lib/iq-pgduck/duckdb.db --init_file_path /etc/iq-pgduck/init.sql \
  --memory_limit 1GB
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload; systemctl enable -q iq-pgduck; systemctl restart iq-pgduck
for _ in $(seq 30); do [ -S /var/run/iq-pgduck/.s.PGSQL.5332 ] && break; sleep 1; done
[ -S /var/run/iq-pgduck/.s.PGSQL.5332 ] || { journalctl -u iq-pgduck --no-pager | tail -20; exit 1; }

# 4. the coordinator: preload, point at pgduck and the bucket, create the extensions here only
if grep -q '^pg_lake_iceberg.default_location_prefix' "$DATA/iq.conf"; then
  sed -i "s#^pg_lake_iceberg.default_location_prefix = .*#pg_lake_iceberg.default_location_prefix = 's3://$S3_BUCKET/iceberg'#" "$DATA/iq.conf"
  sudo -u postgres "$BIN/pg_ctl" -D "$DATA" reload >/dev/null
fi
if ! grep -q pg_extension_base "$DATA/iq.conf"; then
  sed -i "s/^shared_preload_libraries = 'citus'$/shared_preload_libraries = 'citus,pg_extension_base'/" "$DATA/iq.conf"
  cat >> "$DATA/iq.conf" <<CONF
pg_lake_engine.host = 'host=/var/run/iq-pgduck port=5332'
pg_lake_iceberg.default_location_prefix = 's3://$S3_BUCKET/iceberg'
CONF
  systemctl restart iq-citus@coord
  for _ in $(seq 30); do sudo -u postgres "$BIN/pg_isready" -q -p 5432 && break; sleep 1; done
fi
sudo -u postgres "$BIN/psql" -X -q -v ON_ERROR_STOP=1 -p 5432 -d iq <<'SQL'
SET citus.enable_ddl_propagation = off;
CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE;
GRANT USAGE ON FOREIGN SERVER pg_lake TO iq;
SQL
say "pg_lake $(sudo -u postgres "$BIN/psql" -X -At -p 5432 -d iq -c "SELECT string_agg(extname || ' ' || extversion, ', ') FROM pg_extension WHERE extname LIKE 'pg_lake%'"); cold tier s3://$S3_BUCKET/iceberg on $S3_ENDPOINT"
