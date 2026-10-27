#!/usr/bin/env bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# UC_Citus: IQ's remote store on Vultr, private: only :22 is open; Postgres is reached through an SSH tunnel.
#
# The store has a SHAPE, a switch: `server` (the default) is ONE plain PostgreSQL 17 -- the same schema, the tables
# local; `citus` is a Citus cluster -- the same schema, the record distributed. Citus is the later migration step: a
# server holding the record becomes a cluster by MIGRATE-to-Citus.md, never by flipping the switch.
#
#   ./deploy-citus.sh shape [server|citus]   # the shape `up` builds (~/.iq/citus/shape; SHAPE overrides); setting
#                                 # it is refused while the other shape is up
#   ./deploy-citus.sh up          # the shape: server -- one instance (iq-pg), PostgreSQL 17 + pgvector, Citus
#                                 # installed but not loaded; citus -- slice 1: one instance (iq-citus), the whole
#                                 # cluster on it (coordinator + 3 workers by port)
#   ./deploy-citus.sh down        # destroy it (on demand: the schema is re-applied by the scenarios)
#   ./deploy-citus.sh hosts-up    # slice 2: a VPC and four instances -- coordinator, w1, w2 and a spare w3 --
#                                 # one node each, the nodes talking over the VPC's private addresses only
#   ./deploy-citus.sh hosts-down  # destroy the four and the VPC
#   ./deploy-citus.sh hosts-park  # stop every node cleanly, snapshot the four, destroy them (a snapshot bills by size,
#                                 # an instance by the hour); hosts-restore brings them back on the same VPC, each
#                                 # node renamed in Citus to the private address it was given
#   ./deploy-citus.sh autopark    # park the hosts cluster after IDLE_SECS (7200) with no client session
#                                 # (service/install-autopark.sh runs it every 15 min from launchd)
#   ./deploy-citus.sh lake-build  # pg_lake for PG17/el9, built on a throwaway vc2-6c-16gb (iq-citus-build), packed into
#                                 # ~/.iq/citus/pg_lake-<tag>-pg17-el9.tar.gz; the box is destroyed after
#   ./deploy-citus.sh lake-up     # the cold tier on the coordinator: that build, pgduck_server, and Vultr Object
#                                 # Storage as its store (lake-vultr); LAKE_STORE=loopback: SeaweedFS on loopback
#   ./deploy-citus.sh lake-vultr  # the cold tier moved onto Vultr Object Storage (the subscription holding iq-blobs),
#                                 # in its own bucket iq-lake; the key goes from the API to the coordinator only
#   ./deploy-citus.sh search-up   # ParadeDB's pg_search (BM25) on every node (service/install-search.sh)
#   ./deploy-citus.sh trgm-up     # pg_trgm (contrib, on every node already) and the GiST trigram index (trgm.sql)
#   ./deploy-citus.sh gate-up     # the gate writer's login role iq_gate (slice 5; up and hosts-up run it too)
#   ./deploy-citus.sh backup-up   # pgBackRest on every node into Vultr bucket iq-citus-backup: WAL + a full backup
#   ./deploy-citus.sh status      # every iq-citus instance
#   ./deploy-citus.sh psql [sql]  # the coordinator as postgres, over ssh (superuser stays on the host)
#
# What is up is described in ~/.iq/citus/topology.json (the coordinator's public address, each worker's name as
# Citus knows it, its host and its systemd unit, the spare), which the scenarios read; ~/.iq/citus/host is the
# coordinator. A vc2-2c-4gb is $20/mo, billed by the hour (about $0.03/h) while it exists, halted or not -- `down`
# / `hosts-down` is what stops it costing. The Vultr key and the API route are UC_MarkLogic/deploy-vultr.sh's:
# VULTR_API_KEY from the environment or the IQ Vault, carried over an SSH SOCKS tunnel through VULTR_VIA so the key's
# IP allowlist holds one address.
if [ -n "${BASH_SOURCE:-}" ] && [ "${BASH_SOURCE[0]}" != "$0" ]; then echo "run it, don't source it" >&2; return 2; fi
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
REGION="${REGION:-ewr}"
PLAN="${PLAN:-vc2-2c-4gb}"
SSH_KEY_ID="${SSH_KEY_ID-4d07a7fe-a68a-4f92-8f7b-e7968f71b6be}"
API=https://api.vultr.com/v2
STATE="$HOME/.iq/citus"; mkdir -p "$STATE"
SHAPE="${SHAPE:-$(cat "$STATE/shape" 2>/dev/null || echo server)}"
case "$SHAPE" in server|citus) ;; *) echo "SHAPE is server or citus, not $SHAPE" >&2; exit 2 ;; esac
LABEL="${LABEL:-$([ "$SHAPE" = server ] && echo iq-pg || echo iq-citus)}"
SSHO=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
VULTR_VIA="${VULTR_VIA-root@45.77.77.5}"
SOCKS_PORT="${VULTR_SOCKS_PORT:-11081}"
PROXY=()
say() { echo "$(date '+%F %T') $*"; }

tunnel() {
  [ -n "$VULTR_VIA" ] || return 0
  local sock; sock="$(mktemp -u "${TMPDIR:-/tmp}/vultr-via.XXXXXX")"
  ssh -f -N -M -S "$sock" -D "127.0.0.1:$SOCKS_PORT" -o BatchMode=yes -o ExitOnForwardFailure=yes \
      -o ConnectTimeout=10 "$VULTR_VIA" || { echo "no tunnel through $VULTR_VIA" >&2; exit 1; }
  trap "ssh -S '$sock' -O exit '$VULTR_VIA' 2>/dev/null || true" EXIT
  PROXY=(--socks5-hostname "127.0.0.1:$SOCKS_PORT")
}
api() { # method path [json]
  local key="${VULTR_API_KEY:-$("$here/../../../Development/UseCase/UC_App/iq-vault.sh" get VULTR_API_KEY || true)}"
  [ -n "$key" ] || { echo "no VULTR_API_KEY: add it on the IQ app's Vault page" >&2; exit 1; }
  curl -fsS ${PROXY[@]+"${PROXY[@]}"} -X "$1" "$API$2" -H "Authorization: Bearer $key" -H 'Content-Type: application/json' ${3:+-d "$3"}
}
instance() { api GET "/instances?label=$LABEL" | jq -r --arg l "$LABEL" '[.instances[] | select(.label==$l)][0] // empty | "\(.id) \(.main_ip) \(.power_status)"'; }
wait_ssh() { for _ in $(seq 1 60); do ssh "${SSHO[@]}" -o ConnectTimeout=5 "root@$1" true 2>/dev/null && return 0; sleep 10; done; return 1; }

firewall() { # the ssh-only firewall group, made once
  local fw_id
  fw_id=$(api GET /firewalls | jq -r --arg d "$LABEL-ssh-only" '.firewall_groups[] | select(.description==$d) | .id' | head -1)
  [ -n "$fw_id" ] || fw_id=$(api POST /firewalls "$(jq -n --arg d "$LABEL-ssh-only" '{description:$d}')" | jq -r .firewall_group.id)
  [ "$(api GET "/firewalls/$fw_id/rules" | jq '.firewall_rules | length')" -gt 0 ] || for t in v4 v6; do
    sub=$([ $t = v4 ] && echo 0.0.0.0 || echo ::)
    api POST "/firewalls/$fw_id/rules" "$(jq -n --arg t $t --arg s $sub '{ip_type:$t,protocol:"tcp",subnet:$s,subnet_size:0,port:"22",notes:"ssh"}')" >/dev/null
  done
  echo "$fw_id"
}

launch() {
  local os_id fw_id id ip st
  os_id=$(api GET '/os?per_page=500' | jq -r '.os[] | select(.name|test("^Rocky Linux 9 x64")) | .id' | head -1)
  [ -n "$os_id" ] || { echo "no Rocky Linux 9 image found" >&2; exit 1; }
  fw_id=$(firewall)
  id=$(api POST /instances "$(jq -n --arg r "$REGION" --arg p "$PLAN" --argjson o "$os_id" --arg l "$LABEL" \
      --arg k "$SSH_KEY_ID" --arg f "$fw_id" \
      '{region:$r,plan:$p,os_id:$o,label:$l,hostname:$l,sshkey_id:[$k],firewall_group_id:$f,backups:"disabled"}')" | jq -r .instance.id)
  say "instance $id ($PLAN, $REGION, firewall $fw_id); waiting for it"
  for _ in $(seq 1 90); do
    read -r st ip < <(api GET "/instances/$id" | jq -r '.instance | "\(.status) \(.main_ip)"')
    [ "$st" = active ] && [ "$ip" != 0.0.0.0 ] && break; sleep 10
  done
  ssh-keygen -R "$ip" >/dev/null 2>&1 || true
  wait_ssh "$ip" || { say "no ssh to $ip"; exit 1; }
  echo "$ip" > "$STATE/host"
}

single_topology() {
  jq -n --arg h "$1" '{mode:"single", coordinator:{host:$h, port:5432, unit:"coord"},
    workers:{"localhost:5433":{name:"w1",host:$h,port:5433,unit:"w1"}, "localhost:5434":{name:"w2",host:$h,port:5434,unit:"w2"},
             "localhost:5435":{name:"w3",host:$h,port:5435,unit:"w3"}}, spare:"localhost:5435"}' > "$STATE/topology.json"
}

server_topology() { # the server shape: the one node is the coordinator, and there are no workers
  jq -n --arg h "$1" '{mode:"server", coordinator:{host:$h, port:5432, unit:"coord"}, workers:{}, spare:null}' \
    > "$STATE/topology.json"
}

# the switch: which shape `up` builds. A shape that is up holds the record, so the switch never moves away from it:
# server -> citus is MIGRATE-to-Citus.md; a cluster -> server is a dump and restore, by hand.
shape() {
  local want="${1:-}" up
  up=$(jq -r .mode "$STATE/topology.json" 2>/dev/null || true)
  if [ -z "$want" ]; then
    echo "shape: $SHAPE$([ -f "$STATE/shape" ] || echo ' (the default)')"
    [ -n "$up" ] && echo "up: $up ($(jq -r .coordinator.host "$STATE/topology.json"))" || echo "up: nothing"
    [ -f "$STATE/topology-parked.json" ] && echo "parked: a Citus cluster ($STATE/topology-parked.json; hosts-restore)"
    return 0
  fi
  case "$want" in server|citus) ;; *) echo "shape is server or citus" >&2; exit 2 ;; esac
  if [ "$want" = citus ] && [ "$up" = server ]; then
    echo "refused: a server is up and holds the record -- moving it to Citus is the migration (MIGRATE-to-Citus.md), not the switch" >&2; exit 1
  fi
  if [ "$want" = server ] && { [ "$up" = single ] || [ "$up" = hosts ]; }; then
    echo "refused: a Citus cluster is up ($up) and holds the record -- take it down or park it first" >&2; exit 1
  fi
  echo "$want" > "$STATE/shape"
  say "shape: $want"
  [ "$want" = server ] && [ -f "$STATE/topology-parked.json" ] && say "the parked Citus cluster stays parked; a new server starts empty"
  return 0
}

# --- slice 2: one node per instance, over a VPC ------------------------------------------------------------------
VPC_SUBNET="${VPC_SUBNET:-10.31.0.0}"; VPC_MASK=24
HOSTS="coord w1 w2 w3"

vpc() { # the VPC, made once
  local id
  id=$(api GET /vpcs | jq -r --arg d "$LABEL" '.vpcs[] | select(.description==$d) | .id' | head -1)
  [ -n "$id" ] || id=$(api POST /vpcs "$(jq -n --arg r "$REGION" --arg d "$LABEL" --arg s "$VPC_SUBNET" --argjson m $VPC_MASK \
      '{region:$r,description:$d,v4_subnet:$s,v4_subnet_mask:$m}')" | jq -r .vpc.id)
  echo "$id"
}
by_label() { api GET "/instances?label=$1" | jq -r --arg l "$1" '[.instances[] | select(.label==$l)][0] // empty | .id'; }
private_ip() { api GET "/instances/$1/vpcs" | jq -r '.vpcs[0].ip_address // empty'; }

on_vpc() { # host private_ip: bring the VPC interface up with its address if cloud-init has not
  ssh "${SSHO[@]}" "root@$1" "PRIV=$2 MASK=$VPC_MASK bash -s" <<'NET'
set -e
ip -br addr | grep -q " $PRIV/" && exit 0
dev=$(ip -br link | awk '$1 != "lo" && $1 != "enp1s0" {print $1; exit}')
[ -n "$dev" ] || { echo "no VPC interface" >&2; exit 1; }
nmcli -t -f NAME con show | grep -qx iq-vpc || nmcli con add type ethernet ifname "$dev" con-name iq-vpc ipv4.method manual \
  ipv4.addresses "$PRIV/$MASK" ipv6.method disabled connection.autoconnect yes >/dev/null
nmcli con up iq-vpc >/dev/null
firewall-cmd -q --zone=public --change-interface="$dev" --permanent; firewall-cmd -q --reload
NET
}

hosts_up() {
  local os_id fw_id vpc_id n id ip st priv
  os_id=$(api GET '/os?per_page=500' | jq -r '.os[] | select(.name|test("^Rocky Linux 9 x64")) | .id' | head -1)
  fw_id=$(firewall); vpc_id=$(vpc)
  kv() { eval "$1_$2=\$3"; }; at() { eval "echo \${$1_$2}"; }     # bash 3.2 (macOS): no associative arrays
  for n in $HOSTS; do
    id=$(by_label "$LABEL-$n")
    [ -n "$id" ] || { id=$(api POST /instances "$(jq -n --arg r "$REGION" --arg p "$PLAN" --argjson o "$os_id" --arg l "$LABEL-$n" \
        --arg k "$SSH_KEY_ID" --arg f "$fw_id" --arg v "$vpc_id" \
        '{region:$r,plan:$p,os_id:$o,label:$l,hostname:$l,sshkey_id:[$k],firewall_group_id:$f,attach_vpc:[$v],backups:"disabled"}')" \
        | jq -r .instance.id); say "launched $LABEL-$n ($id)"; }
    kv ID "$n" "$id"
  done
  for n in $HOSTS; do
    for _ in $(seq 1 90); do
      read -r st ip < <(api GET "/instances/$(at ID "$n")" | jq -r '.instance | "\(.status) \(.main_ip)"')
      priv=$(private_ip "$(at ID "$n")")
      [ "$st" = active ] && [ "$ip" != 0.0.0.0 ] && [ -n "$priv" ] && break; sleep 10
    done
    kv PUB "$n" "$ip"; kv PRIV "$n" "$priv"
    ssh-keygen -R "$ip" >/dev/null 2>&1 || true
    wait_ssh "$ip" || { say "no ssh to $LABEL-$n $ip"; exit 1; }
    on_vpc "$ip" "$priv"
    say "$LABEL-$n: $ip, private $priv"
  done
  local subnet="$VPC_SUBNET/$VPC_MASK"
  ssh "${SSHO[@]}" "root@$PUB_coord" "ROLE=coordinator PRIVATE_IP=$PRIV_coord SUBNET=$subnet bash -s" < "$here/service/install-citus.sh"
  local pids=""
  for n in w1 w2 w3; do   # the passwords go coordinator -> worker through this process's memory, not its disk
    ssh "${SSHO[@]}" "root@$PUB_coord" 'tar -C /root -cf - citus-postgres.txt citus-iq.txt' |
      ssh "${SSHO[@]}" "root@$(at PUB "$n")" 'umask 077; tar -C /root -xf -'
    ssh "${SSHO[@]}" "root@$(at PUB "$n")" "ROLE=worker PRIVATE_IP=$(at PRIV "$n") SUBNET=$subnet bash -s" \
      < "$here/service/install-citus.sh" > "$STATE/install-$n.log" 2>&1 &
    pids="$pids $!"
  done
  for p in $pids; do wait "$p" || { say "a worker install failed: $STATE/install-*.log"; exit 1; }; done
  cat "$STATE"/install-w*.log | grep -E "worker on|ERROR" || true
  ssh "${SSHO[@]}" "root@$PUB_coord" "sudo -u postgres /usr/pgsql-17/bin/psql -X -q -At -v ON_ERROR_STOP=1 -d iq" <<SQL
SELECT citus_add_node('$PRIV_w1', 5432) WHERE NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodename = '$PRIV_w1');
SELECT citus_add_node('$PRIV_w2', 5432) WHERE NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodename = '$PRIV_w2');
SELECT 'workers: ' || string_agg(node_name || ':' || node_port, ' ') FROM citus_get_active_worker_nodes();
SQL
  echo "$PUB_coord" > "$STATE/host"
  jq -n --arg c "$PUB_coord" --arg p1 "$PRIV_w1" --arg h1 "$PUB_w1" --arg p2 "$PRIV_w2" --arg h2 "$PUB_w2" \
        --arg p3 "$PRIV_w3" --arg h3 "$PUB_w3" --arg s "$subnet" \
    '{mode:"hosts", subnet:$s, coordinator:{host:$c, port:5432, unit:"coord"},
      workers:{($p1+":5432"):{name:"w1",host:$h1,port:5432,unit:"node"}, ($p2+":5432"):{name:"w2",host:$h2,port:5432,unit:"node"},
               ($p3+":5432"):{name:"w3",host:$h3,port:5432,unit:"node"}}, spare:($p3+":5432")}' > "$STATE/topology.json"
  gate_up
  use_watch "$PUB_coord"
  say "running: $LABEL over hosts (coordinator $PUB_coord)"
}

# --- parking (slice 5): stopped cleanly, snapshotted, destroyed; restored on the same VPC, renamed in Citus -----------
PARKED="$STATE/parked.json"

hosts_park() {
  local n id ip snap st
  [ "$(jq -r .mode "$STATE/topology.json" 2>/dev/null)" = hosts ] || { echo "no hosts cluster to park" >&2; exit 1; }
  # every node stopped as a shutdown stops it (checkpoint, clients told), the coordinator first: no new transaction
  # reaches a worker while the workers stop; then the disks are synced and the instances halted
  for n in coord w1 w2 w3; do
    ip=$(jq -r --arg n "$n" 'if $n == "coord" then .coordinator.host else (.workers[] | select(.name == $n) | .host) end' "$STATE/topology.json")
    ssh "${SSHO[@]}" "root@$ip" 'systemctl stop iq-pgduck iq-s3 2>/dev/null; for u in $(systemctl list-units --plain --no-legend "iq-citus@*" | awk "{print \$1}"); do systemctl stop "$u"; done; sync' || true
    say "stopped $LABEL-$n"
  done
  echo '{}' > "$PARKED.new"
  for n in coord w1 w2 w3; do
    id=$(by_label "$LABEL-$n"); [ -n "$id" ] || continue
    api POST "/instances/$id/halt" >/dev/null
  done
  for n in coord w1 w2 w3; do
    id=$(by_label "$LABEL-$n"); [ -n "$id" ] || continue
    for _ in $(seq 60); do [ "$(api GET "/instances/$id" | jq -r .instance.power_status)" = stopped ] && break; sleep 5; done
    snap=$(api POST /snapshots "$(jq -n --arg i "$id" --arg d "$LABEL-parked $n $(date -u +%FT%TZ)" '{instance_id:$i,description:$d}')" | jq -r .snapshot.id)
    jq --arg n "$n" --arg s "$snap" --arg i "$id" '.[$n] = {snapshot: $s, instance: $i}' "$PARKED.new" > "$PARKED.tmp" && mv "$PARKED.tmp" "$PARKED.new"
    say "snapshotting $LABEL-$n -> $snap"
  done
  for n in coord w1 w2 w3; do
    snap=$(jq -r --arg n "$n" '.[$n].snapshot' "$PARKED.new"); st=""
    for _ in $(seq 240); do st=$(api GET "/snapshots/$snap" | jq -r .snapshot.status); [ "$st" = complete ] && break; sleep 15; done
    [ "$st" = complete ] || { say "snapshot $snap of $LABEL-$n is '$st': nothing destroyed; start the instances again"; exit 1; }
  done
  cp "$STATE/topology.json" "$STATE/topology-parked.json"
  mv "$PARKED.new" "$PARKED"
  for n in coord w1 w2 w3; do
    id=$(jq -r --arg n "$n" '.[$n].instance' "$PARKED"); api DELETE "/instances/$id" >/dev/null; say "destroyed $LABEL-$n ($id)"
  done
  rm -f "$STATE/topology.json" "$STATE/host"
  say "parked: $(jq -r 'to_entries | map("\(.key) \(.value.snapshot)") | join(", ")' "$PARKED")"
}

on_vpc_fix() { # host private_ip: the private interface gets the address Vultr gave the restored instance
  ssh "${SSHO[@]}" "root@$1" "PRIV=$2 MASK=$VPC_MASK bash -s" <<'NET'
set -e
for _ in $(seq 30); do ip -br addr | grep -q " $PRIV/" && exit 0; sleep 2; done
dev=$(ip -br link | awk '$1 != "lo" && $1 != "enp1s0" {print $1; exit}')
con=$(nmcli -t -f NAME,DEVICE con show | awk -F: -v d="$dev" '$2 == d {print $1; exit}')
[ -n "$con" ] || con=$(nmcli -t -f NAME con show | grep -x "System $dev" || true)
nmcli con mod "$con" ipv4.method manual ipv4.addresses "$PRIV/$MASK"
nmcli con up "$con" >/dev/null
NET
}

hosts_restore() {
  local fw_id vpc_id n snap id st ip priv old
  [ -s "$PARKED" ] || { echo "nothing parked" >&2; exit 1; }
  fw_id=$(firewall); vpc_id=$(vpc)
  kv() { eval "$1_$2=\$3"; }; at() { eval "echo \${$1_$2}"; }
  for n in coord w1 w2 w3; do
    snap=$(jq -r --arg n "$n" '.[$n].snapshot' "$PARKED")
    id=$(api POST /instances "$(jq -n --arg r "$REGION" --arg p "$PLAN" --arg s "$snap" --arg l "$LABEL-$n" --arg f "$fw_id" --arg v "$vpc_id" \
        '{region:$r,plan:$p,snapshot_id:$s,label:$l,hostname:$l,firewall_group_id:$f,attach_vpc:[$v],backups:"disabled"}')" | jq -r .instance.id)
    kv ID "$n" "$id"; say "restoring $LABEL-$n from $snap -> $id"
  done
  for n in coord w1 w2 w3; do
    id=$(at ID "$n")
    for _ in $(seq 1 120); do
      read -r st ip < <(api GET "/instances/$id" | jq -r '.instance | "\(.status) \(.main_ip)"')
      priv=$(private_ip "$id")
      [ "$st" = active ] && [ "$ip" != 0.0.0.0 ] && [ -n "$priv" ] && break; sleep 10
    done
    ssh-keygen -R "$ip" >/dev/null 2>&1 || true
    wait_ssh "$ip" || { say "no ssh to $LABEL-$n $ip"; exit 1; }
    on_vpc_fix "$ip" "$priv"
    kv PUB "$n" "$ip"; kv PRIV "$n" "$priv"
    say "$LABEL-$n: $ip, private $priv"
  done
  # each node listens on its new private address; the workers start first, then the coordinator
  for n in w1 w2 w3 coord; do
    ssh "${SSHO[@]}" "root@$(at PUB "$n")" "sed -i \"s/^listen_addresses = .*/listen_addresses = '127.0.0.1,$(at PRIV "$n")'/\" /var/lib/pgsql/17/*/iq.conf
      for u in /var/lib/pgsql/17/*/iq.conf; do systemctl restart iq-citus@\$(basename \$(dirname \$u)); done
      systemctl start iq-s3 iq-pgduck 2>/dev/null || true"
  done
  # Citus knows its nodes by their private addresses: rename every one that moved
  local sql="SELECT citus_set_coordinator_host('$PRIV_coord', 5432);"
  for n in w1 w2 w3; do
    old=$(jq -r --arg n "$n" '.workers | to_entries[] | select(.value.name == $n) | .key | split(":")[0]' "$STATE/topology-parked.json")
    [ "$old" = "$(at PRIV "$n")" ] || sql="$sql SELECT citus_update_node(nodeid, '$(at PRIV "$n")', 5432) FROM pg_dist_node WHERE nodename = '$old';"
  done
  ssh "${SSHO[@]}" "root@$PUB_coord" "for _ in \$(seq 60); do sudo -u postgres /usr/pgsql-17/bin/pg_isready -q -p 5432 && break; sleep 1; done
    sudo -u postgres /usr/pgsql-17/bin/psql -X -q -At -v ON_ERROR_STOP=1 -d iq" <<SQL
$sql
SELECT 'workers: ' || string_agg(node_name || ':' || node_port, ' ') FROM citus_get_active_worker_nodes();
SQL
  echo "$PUB_coord" > "$STATE/host"
  jq --arg c "$PUB_coord" --arg p1 "$PRIV_w1" --arg h1 "$PUB_w1" --arg p2 "$PRIV_w2" --arg h2 "$PUB_w2" --arg p3 "$PRIV_w3" --arg h3 "$PUB_w3" \
    '.coordinator.host = $c | .workers = {($p1+":5432"):{name:"w1",host:$h1,port:5432,unit:"node"}, ($p2+":5432"):{name:"w2",host:$h2,port:5432,unit:"node"},
      ($p3+":5432"):{name:"w3",host:$h3,port:5432,unit:"node"}} | .spare = ($p3+":5432")' "$STATE/topology-parked.json" > "$STATE/topology.json"
  for n in coord w1 w2 w3; do api DELETE "/snapshots/$(jq -r --arg n "$n" '.[$n].snapshot' "$PARKED")" >/dev/null || true; done
  rm -f "$PARKED"
  use_watch "$PUB_coord"
  say "running again: $LABEL over hosts (coordinator $PUB_coord)"
}

IDLE_SECS="${IDLE_SECS:-7200}"
use_watch() { ssh "${SSHO[@]}" "root@$1" bash -s < "$here/service/install-usewatch.sh"; }

autopark() { # park the hosts cluster when no client has had a session for IDLE_SECS (service/install-autopark.sh)
  local c idle
  [ "$(jq -r .mode "$STATE/topology.json" 2>/dev/null)" = hosts ] || { say "autopark: no hosts cluster running"; return 0; }
  c=$(jq -r .coordinator.host "$STATE/topology.json")
  idle=$(ssh "${SSHO[@]}" "root@$c" 'up=$(cut -d. -f1 /proc/uptime); f=/var/lib/iq-citus/last-use
    if [ -e "$f" ]; then since=$(( $(date +%s) - $(stat -c %Y "$f") )); else since=$up; fi
    echo $(( since < up ? since : up ))') || { say "autopark: no ssh to the coordinator $c"; return 0; }
  if [ "$idle" -ge "$IDLE_SECS" ]; then say "idle ${idle}s >= ${IDLE_SECS}s: parking"; hosts_park
  else say "in use: idle ${idle}s < ${IDLE_SECS}s"; fi
}

hosts_down() {
  local n id vpc_id
  for n in $HOSTS; do
    id=$(by_label "$LABEL-$n"); [ -n "$id" ] && { api DELETE "/instances/$id" >/dev/null; say "destroyed $LABEL-$n ($id)"; }
  done
  vpc_id=$(api GET /vpcs | jq -r --arg d "$LABEL" '.vpcs[] | select(.description==$d) | .id' | head -1)
  if [ -n "$vpc_id" ]; then   # the VPC goes once nothing is attached to it
    for _ in $(seq 1 30); do api DELETE "/vpcs/$vpc_id" >/dev/null 2>&1 && { say "deleted VPC $vpc_id"; break; }; sleep 10; done
  fi
  [ "$(jq -r .mode "$STATE/topology.json" 2>/dev/null)" = hosts ] && rm -f "$STATE/topology.json" "$STATE/host"
  return 0
}

lake_build() {
  local os_id fw_id id ip st label="$LABEL-build" tag="${PG_LAKE_TAG:-v3.5.7}"
  local out="$STATE/pg_lake-$tag-pg17-el9.tar.gz"
  [ -s "$out" ] && { say "already built: $out"; return 0; }
  os_id=$(api GET '/os?per_page=500' | jq -r '.os[] | select(.name|test("^Rocky Linux 9 x64")) | .id' | head -1)
  fw_id=$(firewall)
  id=$(by_label "$label")
  [ -n "$id" ] || id=$(api POST /instances "$(jq -n --arg r "$REGION" --arg p "${BUILD_PLAN:-vc2-6c-16gb}" --argjson o "$os_id" \
      --arg l "$label" --arg k "$SSH_KEY_ID" --arg f "$fw_id" \
      '{region:$r,plan:$p,os_id:$o,label:$l,hostname:$l,sshkey_id:[$k],firewall_group_id:$f,backups:"disabled"}')" | jq -r .instance.id)
  for _ in $(seq 1 90); do
    read -r st ip < <(api GET "/instances/$id" | jq -r '.instance | "\(.status) \(.main_ip)"')
    [ "$st" = active ] && [ "$ip" != 0.0.0.0 ] && break; sleep 10
  done
  ssh-keygen -R "$ip" >/dev/null 2>&1 || true
  wait_ssh "$ip" || { say "no ssh to $label $ip"; exit 1; }
  say "building pg_lake $tag on $label ($ip); log: ssh root@$ip tail -f /root/build-pg-lake.log"
  scp -q "${SSHO[@]}" "$here/service/build-pg-lake.sh" "root@$ip:/root/build-pg-lake.sh"   # on disk: the build outlives ssh
  ssh "${SSHO[@]}" "root@$ip" "[ -f /root/build-pg-lake.done ] || systemctl is-active -q iq-build-pg-lake || \
    systemd-run -q --unit=iq-build-pg-lake --setenv=PG_LAKE_TAG=$tag --setenv=HOME=/root bash /root/build-pg-lake.sh"
  local done=""
  for _ in $(seq 1 360); do   # up to 3 h
    done=$(ssh "${SSHO[@]}" "root@$ip" 'cat /root/build-pg-lake.done 2>/dev/null' || true)
    [ -n "$done" ] && break; sleep 30
  done
  ssh "${SSHO[@]}" "root@$ip" 'tail -n 5 /root/build-pg-lake.log'
  if [ "$done" = ok ]; then
    scp -q "${SSHO[@]}" "root@$ip:/root/pg_lake-$tag-pg17-el9.tar.gz" "$out"
    api DELETE "/instances/$id" >/dev/null; say "fetched $out; destroyed $label"
  else
    say "build ${done:-timed out}; $label ($ip) kept for a look -- deploy-citus.sh lake-build-down destroys it"; exit 1
  fi
}

lake_build_down() { local id; id=$(by_label "$LABEL-build"); [ -n "$id" ] && api DELETE "/instances/$id" >/dev/null && say "destroyed $LABEL-build"; return 0; }

object_storage() { # the Object Storage subscription that holds iq-blobs (UC_BlobStore): id, hostname
  api GET /object-storage | jq -r --arg h "${S3_HOSTNAME:-ewr1.vultrobjects.com}" \
    '[.object_storages[] | select(.s3_hostname==$h and .status=="active")][0] // empty | "\(.id) \(.s3_hostname) \(.label)"'
}

make_bucket() { # host bucket: create a bucket on the Object Storage subscription, from the coordinator, by its key file
  ssh "${SSHO[@]}" "root@$1" "HOST=$2 BUCKET=$3 bash -s" <<'BUCKET'
set -euo pipefail
K=$(cut -d: -f1 /root/iq-lake-s3.txt); S=$(cut -d: -f2- /root/iq-lake-s3.txt)
EMPTY=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855   # sha256(""): Vultr wants the payload hash signed
code=$(curl -s -o /tmp/bucket.out -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" --user "$K:$S" \
  -H "x-amz-content-sha256: $EMPTY" -H "Content-Length: 0" -X PUT "https://$HOST/$BUCKET")
case "$code" in
  200) echo "bucket $BUCKET on $HOST" ;;
  409) grep -q BucketAlreadyOwnedByYou /tmp/bucket.out && echo "bucket $BUCKET is already ours" || { cat /tmp/bucket.out; exit 1; } ;;
  *) echo "bucket $BUCKET: HTTP $code"; cat /tmp/bucket.out; exit 1 ;;
esac
rm -f /tmp/bucket.out
BUCKET
}

backup_up() { # pgBackRest on every host into its own bucket (slice 5); the key goes coordinator -> host on stdin
  local c sub host bucket="${BACKUP_BUCKET:-iq-citus-backup}" hosts h
  c=$(jq -r .coordinator.host "$STATE/topology.json")
  read -r sub host _ <<<"$(object_storage)"
  ssh "${SSHO[@]}" "root@$c" 'test -s /root/iq-lake-s3.txt' || \
    api GET "/object-storage/$sub" | jq -r '.object_storage | "\(.s3_access_key):\(.s3_secret_key)"' | ssh "${SSHO[@]}" "root@$c" 'umask 077; cat > /root/iq-lake-s3.txt'
  make_bucket "$c" "$host" "$bucket"
  hosts=$(jq -r '[.coordinator.host] + [.workers[].host] | unique | .[]' "$STATE/topology.json")
  for h in $hosts; do
    scp -q "${SSHO[@]}" "$here/service/install-backup.sh" "root@$h:/root/install-backup.sh"
    ssh "${SSHO[@]}" "root@$c" 'cat /root/iq-lake-s3.txt' | \
      ssh "${SSHO[@]}" "root@$h" "HOST=$host BUCKET=$bucket CLUSTER=$LABEL bash /root/install-backup.sh"
  done
}

lake_vultr() { # the cold tier on Vultr Object Storage: a bucket of its own (LAKE_BUCKET), never iq-blobs
  local h sub host bucket="${LAKE_BUCKET:-iq-lake}"
  h=$(jq -r .coordinator.host "$STATE/topology.json")
  read -r sub host _ <<<"$(object_storage)"; [ -n "$sub" ] || { echo "no active Object Storage on ${S3_HOSTNAME:-ewr1.vultrobjects.com}" >&2; exit 1; }
  [ "$bucket" != iq-blobs ] || { echo "the cold tier does not share iq-blobs" >&2; exit 1; }
  # the subscription's S3 key, from the API straight into a root-only file on the coordinator (never on this disk)
  api GET "/object-storage/$sub" | jq -r '.object_storage | "\(.s3_access_key):\(.s3_secret_key)"' |
    ssh "${SSHO[@]}" "root@$h" 'umask 077; cat > /root/iq-lake-s3.txt'
  make_bucket "$h" "$host" "$bucket"
  # what the loopback store held was test data: its Iceberg table and the store itself go
  ssh "${SSHO[@]}" "root@$h" "sudo -u postgres /usr/pgsql-17/bin/psql -X -q -d iq -c 'DROP TABLE IF EXISTS iq.event_cold CASCADE' 2>&1 | grep -v NOTICE || true
    systemctl disable -q --now iq-s3 2>/dev/null || true"
  ssh "${SSHO[@]}" "root@$h" "S3_ENDPOINT=$host S3_BUCKET=$bucket S3_KEY_FILE=/root/iq-lake-s3.txt bash -s" < "$here/service/install-lake.sh"
}

gate_up() { # the gate writer's login (slice 5): made on the coordinator, into every node's .pgpass, never on this disk
  local c hosts h
  c=$(jq -r .coordinator.host "$STATE/topology.json")
  ssh "${SSHO[@]}" "root@$c" 'bash -s' <<'GATE'
set -euo pipefail
umask 077
[ -s /root/citus-gate.txt ] || printf 'iq_gate:%s\n' "$(openssl rand -hex 24)" > /root/citus-gate.txt
PW=$(cut -d: -f2- /root/citus-gate.txt)
sudo -u postgres /usr/pgsql-17/bin/psql -X -q -v ON_ERROR_STOP=1 -d iq <<SQL
DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iq_gate') THEN CREATE ROLE iq_gate; END IF; END \$\$;
ALTER ROLE iq_gate LOGIN PASSWORD '$PW';
SQL
GATE
  hosts=$(jq -r '[.coordinator.host] + [.workers[].host] | unique | .[]' "$STATE/topology.json")
  for h in $hosts; do
    ssh "${SSHO[@]}" "root@$c" 'cut -d: -f2- /root/citus-gate.txt' | ssh "${SSHO[@]}" "root@$h" \
      'read -r pw; f=/var/lib/pgsql/.pgpass; grep -v ":iq_gate:" "$f" > "$f.new" || true; printf "*:*:*:iq_gate:%s\n" "$pw" >> "$f.new"; mv "$f.new" "$f"; chown postgres:postgres "$f"; chmod 600 "$f"'
  done
  say "iq_gate can log in; its password is on the coordinator (/root/citus-gate.txt) and in each node's .pgpass"
}

search_up() { # pg_search on every node's host, then the extension (created on the coordinator, propagated)
  local hosts h
  hosts=$(jq -r '[.coordinator.host] + [.workers[].host] | unique | .[]' "$STATE/topology.json")
  for h in $hosts; do ssh "${SSHO[@]}" "root@$h" bash -s < "$here/service/install-search.sh"; done
  h=$(jq -r .coordinator.host "$STATE/topology.json")
  ssh "${SSHO[@]}" "root@$h" "sudo -u postgres /usr/pgsql-17/bin/psql -X -q -v ON_ERROR_STOP=1 -d iq -c 'CREATE EXTENSION IF NOT EXISTS pg_search' -c \"SELECT 'pg_search ' || extversion FROM pg_extension WHERE extname = 'pg_search'\" -At"
}

trgm_up() { # pg_trgm ships in postgresql17-contrib on every node: create it and the index on the coordinator
  local h
  h=$(jq -r .coordinator.host "$STATE/topology.json" 2>/dev/null) || { echo "no cluster (up or hosts-up)" >&2; exit 1; }
  ssh "${SSHO[@]}" "root@$h" "sudo -u postgres /usr/pgsql-17/bin/psql -X -q -v ON_ERROR_STOP=1 -d iq" < "$here/trgm.sql"
  if [ "$(jq -r .mode "$STATE/topology.json")" = server ]; then
    ssh "${SSHO[@]}" "root@$h" "sudo -u postgres /usr/pgsql-17/bin/psql -X -At -d iq -c \"SELECT 'pg_trgm ' || extversion FROM pg_extension WHERE extname = 'pg_trgm'\""
    return 0
  fi
  ssh "${SSHO[@]}" "root@$h" "sudo -u postgres /usr/pgsql-17/bin/psql -X -At -d iq -c \"SELECT 'pg_trgm ' || extversion FROM pg_extension WHERE extname = 'pg_trgm'\" -c \"SELECT 'shards with datum_trgm ' || sum(result::int) FROM run_command_on_workers('SELECT count(*) FROM pg_class WHERE relname LIKE ''datum_trgm%''')\""
}

lake_up() {
  local h tar
  h=$(jq -r .coordinator.host "$STATE/topology.json" 2>/dev/null) || { echo "no cluster (up or hosts-up)" >&2; exit 1; }
  tar=$(ls -t "$STATE"/pg_lake-*-pg17-el9.tar.gz 2>/dev/null | head -1)
  [ -n "$tar" ] || { echo "no pg_lake build: $0 lake-build" >&2; exit 1; }
  scp -q "${SSHO[@]}" "$tar" "root@$h:/root/"
  if [ "${LAKE_STORE:-vultr}" = loopback ]; then   # a test with no bucket: SeaweedFS on the coordinator's loopback
    ssh "${SSHO[@]}" "root@$h" bash -s < "$here/service/install-lake.sh"
  else
    tunnel; lake_vultr
  fi
}

up() {
  local i ip role=single
  [ "$SHAPE" = server ] && role=server
  i=$(instance)
  if [ -n "$i" ]; then read -r _ ip _ <<<"$i"; echo "$ip" > "$STATE/host"; else launch; ip=$(cat "$STATE/host"); fi
  say "installing $SHAPE on $ip"
  ssh "${SSHO[@]}" "root@$ip" "ROLE=$role bash -s" < "$here/service/install-citus.sh"
  if [ "$SHAPE" = server ]; then server_topology "$ip"; else single_topology "$ip"; fi
  gate_up
  say "running: $LABEL $ip"
}

down() {
  local i id ip
  i=$(instance); [ -n "$i" ] || { say "absent"; rm -f "$STATE/host"; return 0; }
  read -r id ip _ <<<"$i"
  api DELETE "/instances/$id" >/dev/null
  ssh-keygen -R "$ip" >/dev/null 2>&1 || true
  case "$(jq -r .mode "$STATE/topology.json" 2>/dev/null)" in single|server) rm -f "$STATE/topology.json" "$STATE/host" ;; esac
  say "destroyed $LABEL ($id, $ip)"
}

status() {
  local rows
  rows=$(api GET '/instances?per_page=100' | jq -r --arg l "$LABEL" '.instances[] | select(.label==$l or (.label|startswith($l+"-"))) | "\(.label) \(.main_ip) \(.power_status) \(.id)"')
  [ -n "$rows" ] && echo "$rows" || echo "absent"
  [ -f "$STATE/topology.json" ] && echo "topology: $(jq -r .mode "$STATE/topology.json")"
  echo "shape: $SHAPE"
  return 0
}

case "${1:-}" in
  shape) shape "${2:-}" ;;
  up|down|status) tunnel; "$1" ;;
  hosts-up|hosts-down|hosts-park|hosts-restore|lake-build|lake-build-down|autopark) tunnel; f="${1//-/_}"; "$f" ;;
  lake-up) lake_up ;;
  search-up) search_up ;;
  trgm-up) trgm_up ;;
  gate-up) gate_up ;;
  backup-up) tunnel; backup_up ;;
  object-storage) tunnel; object_storage ;;
  lake-vultr) tunnel; lake_vultr ;;
  psql) h=$(cat "$STATE/host" 2>/dev/null) || { echo "no host (run up)" >&2; exit 1; }
        shift; ssh "${SSHO[@]}" "root@$h" sudo -u postgres /usr/pgsql-17/bin/psql -X -p 5432 -d iq ${1:+-c "$(printf %q "$*")"} ;;
  *) echo "usage: $0 shape [server|citus] | up | down | hosts-up | hosts-down | hosts-park | hosts-restore | autopark | lake-build | lake-build-down | lake-up | lake-vultr | search-up | trgm-up | gate-up | backup-up | object-storage | status | psql [sql]" >&2; exit 2 ;;
esac
