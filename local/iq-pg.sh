#!/bin/zsh
# @use_case Development/UseCase/UC_PostgreSqlIq
# @facet Store
#
# The box's own Postgres -- the IQ store standard (pgembed, IQ_REGISTRY_DSN) -- brought up by the IQ app the way its
# Python and its Bun are: the server binaries ship INSIDE the image's Python (pgembed's pginstall, bundled in IQ.app as
# Resources/python), the data lives under $IQ_PGDATA (~/.iq/pg), and the DSN is exported to every process the app spawns.
#   Usage: iq-pg.sh            -> ensure: the data dir initialised, postgres running on its unix socket (no TCP port),
#                                 the `iq` database present; prints the DSN   postgresql://postgres@/iq?host=~/.iq/pg
#          iq-pg.sh --dsn      -> just the DSN (no ensure)
#          iq-pg.sh --check    -> exit 0 iff the server answers on that DSN
#          iq-pg.sh --stop     -> stop it (pg_ctl from the image)
#   Env: IQ_PGDATA (~/.iq/pg), IQ_PGDATABASE (iq), IQ_PYTHON (the image's python; else iq-python.sh --bin)
# Never a Homebrew or system postgres: the image's, or nothing (exit 1, fail closed).
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
APP="${IQ_REPO:-$HERE/../../..}/Development/UseCase/UC_App"   # IQ vendors this repo at vendor/PostgreSqlIq
PGDATA="${IQ_PGDATA:-$HOME/.iq/pg}"; DB="${IQ_PGDATABASE:-iq}"
DSN="postgresql://postgres@/$DB?host=$PGDATA"
case "${1:-}" in
  --dsn) echo "$DSN"; exit 0 ;;
esac
PY="${IQ_PYTHON:-$("$APP/iq-python.sh" --bin)}" || { echo "[iq-pg] no IQ python" >&2; exit 1; }
"$PY" -c "import pgembed, psycopg" 2>/dev/null || { echo "[iq-pg] the IQ python at $PY lacks pgembed/psycopg -- rebuild the image runtime: $APP/iq-python.sh" >&2; exit 1; }
case "${1:-}" in
  --check)
    exec "$PY" -c "import psycopg,sys
try: psycopg.connect('$DSN', connect_timeout=3).close()
except Exception as e: print('[iq-pg] down:', str(e).splitlines()[0], file=sys.stderr); sys.exit(1)" ;;
  --stop)
    BIN="$("$PY" -c 'import os, pgembed; print(os.path.join(os.path.dirname(pgembed.__file__), "pginstall", "bin"))')"
    exec "$BIN/pg_ctl" -D "$PGDATA" -m fast stop ;;
  "")
    mkdir -p "$PGDATA"
    exec "$PY" - "$PGDATA" "$DB" <<'PYEOF'
import sys, pgembed, psycopg
pgdata, db = sys.argv[1], sys.argv[2]
srv = pgembed.get_server(pgdata, cleanup_mode=None)          # None: leave postgres running after we exit
srv.ensure_pgdata_inited()
srv.ensure_postgres_running()
with psycopg.connect("postgresql://postgres@/postgres?host=" + pgdata, autocommit=True) as c:
    if not c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db,)).fetchone():
        c.execute("CREATE DATABASE " + db)
dsn = "postgresql://postgres@/%s?host=%s" % (db, pgdata)
with psycopg.connect(dsn) as c:
    v = c.execute("select version()").fetchone()[0].split(" on ")[0]
print("[iq-pg] %s up at %s" % (v, pgdata), file=sys.stderr)
print(dsn)
PYEOF
    ;;
  *) echo "usage: iq-pg.sh [--dsn|--check|--stop]" >&2; exit 2 ;;
esac
