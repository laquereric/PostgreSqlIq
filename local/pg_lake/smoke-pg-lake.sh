#!/bin/zsh
# @use_case Development/UseCase/UC_PostgreSqlIq
# @facet Store
#
# The pg_lake build, proven end to end on scratch: a RustFS with throwaway keys (UC_BlobStore's iq-rustfs.sh), a
# pgduck_server on its own socket (spatial from the pgembed's duckdb_extensions, installs off), and a throwaway
# PostgreSQL 17 cluster from the same pgembed with pg_extension_base preloaded. It creates pg_lake, writes an Iceberg
# table under s3://iq-blobs/lake/root, rolls one insert back, reads the rows, asks the catalog, and lists the bucket.
# Everything is stopped and removed after; nothing touches the box's Postgres (~/.iq/pg) or its Vault.
#   smoke-pg-lake.sh <image python>        exits 0 when every check holds
set -u
PY="${1:?usage: smoke-pg-lake.sh <image python>}"
here="${0:A:h}"; REPO="${here:h:h:h:h}"
PGROOT="$(dirname "$(dirname "$("$PY" -c 'import os, pgembed; print(os.path.join(os.path.dirname(pgembed.__file__), "pginstall", "bin", "pg_config"))')")")"
RUSTFS="$REPO/Development/UseCase/UC_BlobStore/rustfs/iq-rustfs.sh"
S="$(mktemp -d /tmp/pgl.XXXXXX)"                      # short: a unix socket path is at most 103 bytes
mkdir -p "$S"/{rfs,sock,pgsock,cache}
AK="$(openssl rand -hex 10)"; SK="$(openssl rand -hex 20)"
free_port() { "$PY" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
RP="$(free_port)"; DP="$(free_port)"; PP="$(free_port)"
RPID=""; DPID=""
cleanup() {
  [ -f "$S/data/postmaster.pid" ] && "$PGROOT/bin/pg_ctl" -D "$S/data" -m fast stop >/dev/null 2>&1
  [ -n "$DPID" ] && kill "$DPID" 2>/dev/null
  [ -n "$RPID" ] && { pkill -P "$RPID" 2>/dev/null; kill "$RPID" 2>/dev/null; }
  rm -rf "$S"
}
trap cleanup EXIT
fail() { echo "[smoke-pg-lake] FAIL: $*" >&2; tail -20 "$S"/*.log >&2 2>/dev/null; exit 1; }
rfs() { env IQ_VAULT_FILE=/nonexistent IQ_RUSTFS_DATA="$S/rfs" IQ_RUSTFS_PORT="$RP" RUSTFS_ACCESS_KEY="$AK" \
          RUSTFS_SECRET_KEY="$SK" "$RUSTFS" "$@"; }

rfs run > "$S/rustfs.log" 2>&1 & RPID=$!
for _ in $(seq 60); do curl -fs "http://127.0.0.1:$RP/health" >/dev/null && break; sleep 0.5; done
rfs bucket >/dev/null || fail "no bucket on the scratch RustFS"

cat > "$S/init.sql" <<SQL
CREATE SECRET rustfs (TYPE S3, KEY_ID '$AK', SECRET '$SK', ENDPOINT '127.0.0.1:$RP', URL_STYLE 'path',
                      USE_SSL false, REGION 'us-east-1', SCOPE 's3://iq-blobs');
SQL
"$PGROOT/bin/pgduck_server" --unix_socket_directory "$S/sock" --port "$DP" --cache_dir "$S/cache" \
  --duckdb_database_file_path "$S/duck.db" --init_file_path "$S/init.sql" --no_extension_install \
  --extensions_dir "$PGROOT/share/postgresql/pg_lake/duckdb_extensions" > "$S/pgduck.log" 2>&1 & DPID=$!
for _ in $(seq 60); do [ -S "$S/sock/.s.PGSQL.$DP" ] && break; kill -0 $DPID 2>/dev/null || fail "pgduck_server exited"; sleep 0.5; done
[ -S "$S/sock/.s.PGSQL.$DP" ] || fail "pgduck_server did not listen"

"$PGROOT/bin/initdb" -D "$S/data" -E UTF8 --no-locale -U postgres -A trust > "$S/initdb.log" 2>&1 || fail initdb
cat >> "$S/data/postgresql.conf" <<C
shared_preload_libraries = 'pg_extension_base'
pg_lake_engine.host = 'host=$S/sock port=$DP'
listen_addresses = ''
unix_socket_directories = '$S/pgsock'
port = $PP
C
"$PGROOT/bin/pg_ctl" -D "$S/data" -l "$S/pg.log" -w start >/dev/null || fail "postgres did not start"

out="$("$PGROOT/bin/psql" -h "$S/pgsock" -p "$PP" -U postgres -d postgres -X -A -t -v ON_ERROR_STOP=1 2>&1 <<'SQL'
CREATE EXTENSION pg_lake CASCADE;
SET pg_lake_iceberg.default_location_prefix = 's3://iq-blobs/lake/root';
CREATE TABLE gold_smoke (id int, label text) USING iceberg;
INSERT INTO gold_smoke VALUES (1, 'one'), (2, 'two');
BEGIN; INSERT INTO gold_smoke VALUES (3, 'rolled back'); ROLLBACK;
SELECT 'rows=' || string_agg(id || ':' || label, ',' ORDER BY id) FROM gold_smoke;
SELECT 'catalog=' || count(*) FROM iceberg_tables WHERE table_name = 'gold_smoke';
SQL
)" || fail "psql: $out"
echo "$out" | grep -qx 'rows=1:one,2:two' || fail "rows read back: $out"
echo "$out" | grep -qx 'catalog=1' || fail "catalog: $out"

listing="$(AK="$AK" SK="$SK" RP="$RP" "$PY" - "$REPO/GherkinIq/Facet/Store" <<'P'
import os, sys
sys.path.append(sys.argv[1])
from s3 import S3Client
keys = S3Client("http://127.0.0.1:%s" % os.environ["RP"], "iq-blobs", os.environ["AK"], os.environ["SK"], "us-east-1").list("")
print(len(keys), all(k.startswith("lake/root/") for k in keys))
P
)"
case "$listing" in "0 "*|*False) fail "the bucket holds: $listing" ;; esac
echo "[smoke-pg-lake] ok: pg_lake created, an Iceberg table written under lake/root (${listing%% *} objects, nothing elsewhere), a rollback kept out, rows and catalog read back"
