#!/bin/zsh
# @use_case Development/UseCase/UC_CDC
# @facet Store
#
# launchd entry point of a domain's shipper (com.iq.cdc-ship, install-ship.sh): this box's Postgres is the domain of
# one namespace (IQ_SHIP_NAMESPACE, u-<k> or g-<h>), and its plane rows go to the global Citus cluster as they are
# written (apply.py ship, the `cluster` hop's own slot, narrowed to the namespace). Each start:
#
#   1. an SSH tunnel to the coordinator (UC_Citus topology, ~/.iq/citus/topology.json), held by this script
#   2. the role iq's password read over ssh into PGPASSWORD -- never on a command line, never on disk
#   3. apply.py ship-setup (the slot, idempotent), then ship --resync: every row of the namespace put again (what
#      went by while the slot was away), then a cycle every IQ_SHIP_INTERVAL seconds (default 30)
#
# A cluster that cannot be reached for IQ_SHIP_GIVE_UP cycles in a row (default 10: a parked cluster, a dropped
# tunnel) ends the run; launchd starts it again after its ThrottleInterval, with a new tunnel. The slot keeps what
# was not shipped (up to the WAL cap; past it the slot is lost, made again and resynced). The shipper connects to the
# cluster only when there is something to ship, so a quiet domain never keeps the cluster from parking.
#
#   iq-ship-launchd.sh            run (launchd does)
#   iq-ship-launchd.sh --print    the configuration it would run with, and exit
set -eu
IQ_REPO="${IQ_REPO:-$HOME/NoIcloud/Iq}"
CDC="$IQ_REPO/vendor/PostgreSqlIq/cdc"
NS="${IQ_SHIP_NAMESPACE:-}"
TOPO="${IQ_CITUS_TOPOLOGY:-$HOME/.iq/citus/topology.json}"
INTERVAL="${IQ_SHIP_INTERVAL:-30}"
GIVE_UP="${IQ_SHIP_GIVE_UP:-10}"
TABLE="${IQ_SHIP_TABLE:-iq.cluster_datum}"
say() { echo "[iq-ship] $*" >&2; }
fail() { say "$1"; sleep "${2:-60}"; exit 1; }       # the sleep spaces launchd's retries

[[ "$NS" =~ '^[ug]-[0-9a-f]{24}$' ]] || { say "IQ_SHIP_NAMESPACE is not a domain's namespace (u-<k> or g-<h>): '$NS'"; exit 2; }
PY="$("$IQ_REPO/Development/UseCase/UC_App/iq-python.sh" --bin)" || fail "no image python"
DOMAIN_DSN="${IQ_SHIP_DOMAIN_DSN:-$("$IQ_REPO/vendor/PostgreSqlIq/local/iq-pg.sh" --dsn)}"
[ -f "$TOPO" ] || fail "no Citus cluster ($TOPO): UC_Citus deploy-citus.sh hosts-up" 300
COORD="$("$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["coordinator"]["host"])' "$TOPO")"
PORT="$("$PY" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
CLUSTER_DSN="postgresql://iq@127.0.0.1:$PORT/iq?connect_timeout=10"

if [ "${1:-}" = "--print" ]; then
  echo "namespace=$NS domain=$DOMAIN_DSN coordinator=$COORD cluster=$CLUSTER_DSN table=$TABLE interval=$INTERVAL give_up=$GIVE_UP"
  exit 0
fi

SSHO=(-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes)
ssh "${SSHO[@]}" -N -L "127.0.0.1:$PORT:127.0.0.1:5432" "root@$COORD" &
TUNNEL=$!
trap 'kill $TUNNEL 2>/dev/null || true' EXIT INT TERM
for _ in {1..40}; do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 0.25; done
nc -z 127.0.0.1 "$PORT" 2>/dev/null || fail "no tunnel to $COORD (the cluster parked?)" 300
PGPASSWORD="$(ssh "${SSHO[@]}" "root@$COORD" 'cut -d: -f2- /root/citus-iq.txt')" || fail "no password for iq on $COORD"
export PGPASSWORD
say "shipping $NS from $DOMAIN_DSN to the cluster at $COORD (tunnel :$PORT)"
"$PY" "$CDC/apply.py" ship-setup --domain-dsn "$DOMAIN_DSN" --namespace "$NS" >&2 || fail "the slot could not be set up"
# not exec: the shell stays, so its trap takes the tunnel down with the shipper, however the shipper ends
rc=0
"$PY" "$CDC/apply.py" ship "$CLUSTER_DSN" --domain-dsn "$DOMAIN_DSN" --namespace "$NS" --table "$TABLE" --resync \
  --interval "$INTERVAL" --give-up "$GIVE_UP" &
SHIPPER=$!
trap 'kill $SHIPPER $TUNNEL 2>/dev/null || true' EXIT INT TERM
wait $SHIPPER || rc=$?
exit $rc
