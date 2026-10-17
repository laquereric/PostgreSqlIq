#!/bin/zsh
# @use_case Development/UseCase/UC_CDC
# @facet Store
#
# CDC from the box's Postgres (pgembed, ~/.iq/pg) to the IQ Server (memory.*), only while the IQ Server is up.
# NEVER autostarted: no launchd job, no app hook -- you run it, it streams in the foreground until Ctrl-C.
#   iq-cdc.sh setup [schema.table ...] [--restart]  wal_level=logical (+1GB WAL cap), publication + slot iq_cdc;
#                                                    --restart lets it restart Postgres when wal_level must change
#   iq-cdc.sh status                                 slot position, WAL held, server up/down
#   iq-cdc.sh once                                   one cycle (exit 0 = shipped or nothing to ship, server up)
#   iq-cdc.sh run [--interval 10]                    stream until stopped; a down server is waited out
#   iq-cdc.sh teardown                               drop the slot and publication (stop holding WAL)
# Env: IQ_REGISTRY_DSN / IQ_PGDATA, IQ_MEMORY (https://memory.intentionalquality.com), IQ_HARNESS_SESSION
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
APP="$HERE/../UC_App"
PY="${IQ_PYTHON:-$("$APP/iq-python.sh" --bin)}" || { echo "[iq-cdc] no IQ python" >&2; exit 1; }
"$APP/iq-pg.sh" --check || { echo "[iq-cdc] the box's Postgres is down -- start the IQ app or run UC_App/iq-pg.sh" >&2; exit 1; }
exec "$PY" "$HERE/cdc.py" "$@"
