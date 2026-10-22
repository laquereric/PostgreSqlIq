#!/bin/zsh
# @use_case Development/UseCase/UC_CDC
# @facet Store
#
# CDC from the box's Postgres (pgembed, ~/.iq/pg) to the IQ Server (memory.*), only while the IQ Server is up.
# Never started by the app: you run it, it streams in the foreground until Ctrl-C -- or you install it as your own
# LaunchAgent (service/install-cdc.sh: com.iq.cdc runs `serve`), opted into by hand, removed with --remove.
#   iq-cdc.sh setup [schema.table ...] [--restart]  wal_level=logical (+1GB WAL cap), publication + slot iq_cdc;
#                                                    --restart lets it restart Postgres when wal_level must change
#   iq-cdc.sh snapshot                               ship the rows that existed before the slot (drains it first);
#                                                    the stream then skips what the snapshot already holds
#   iq-cdc.sh filter [schema.table "<where>" [--full] | schema.table --clear | --reset]
#                                                    capture only matching rows (publication row filter); refused
#                                                    if it would break the table's UPDATE/DELETE (unless --full)
#   iq-cdc.sh columns [schema.table a,b,c | schema.table --clear]
#                                                    capture only these columns (publication column list); must
#                                                    include the replica identity, refused otherwise
#   iq-cdc.sh status                                 slot position, WAL held, server up/down
#   iq-cdc.sh once                                   one cycle (exit 0 = shipped or nothing to ship, server up)
#   iq-cdc.sh run [--interval 10]                    stream until stopped; a down server is waited out
#   iq-cdc.sh serve [--interval 10]                  as run, after readying the slot: a lost one rebuilt, a needed
#                                                    or incomplete snapshot run (a down server waited out)
#   iq-cdc.sh teardown                               drop the slot and publication (stop holding WAL)
#   iq-cdc.sh rebuild [--snapshot] [--force]         a lost slot (WAL past the cap) made again, the publication
#                                                    kept; --snapshot then ships what it never did (server up)
#   iq-cdc.sh publish ducklake                       add the lake's catalog schema, REPLICA IDENTITY FULL
#   iq-cdc.sh --hop proxy setup|snapshot|once|run|status|teardown
#                                                    the domain's second hop: token status to the proxy, its own
#                                                    slot iq_cdc_proxy; what it captures is fixed (cdc.HOPS).
#                                                    The proxy puts it with: apply.py plane <routing DSN> --host <box>
# Env: IQ_RECORD_DSN / IQ_REGISTRY_DSN / IQ_PGDATA, IQ_MEMORY (https://memory.intentionalquality.com), IQ_HARNESS_SESSION
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
APP="${IQ_REPO:-$HERE/../../..}/Development/UseCase/UC_App"   # IQ vendors this repo at vendor/PostgreSqlIq
PY="${IQ_PYTHON:-$("$APP/iq-python.sh" --bin)}" || { echo "[iq-cdc] no IQ python" >&2; exit 1; }
"$HERE/../local/iq-pg.sh" --check || { echo "[iq-cdc] the box's Postgres is down -- start the IQ app or run vendor/PostgreSqlIq/local/iq-pg.sh" >&2; exit 1; }
exec "$PY" "$HERE/cdc.py" "$@"
