#!/usr/bin/env bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# UC_Citus: an on-demand Citus cluster on Vultr, private: only :22 is open; Postgres is reached through an SSH tunnel.
#
#   ./deploy-citus.sh up          # slice 1: one instance, the whole cluster on it (coordinator + 3 workers by port)
#   ./deploy-citus.sh down        # destroy it (on demand: the schema is re-applied by the scenarios)
#   ./deploy-citus.sh hosts-up    # slice 2: a VPC and four instances -- coordinator, w1, w2 and a spare w3 --
#                                 # one node each, the nodes talking over the VPC's private addresses only
#   ./deploy-citus.sh hosts-down  # destroy the four and the VPC
#   ./deploy-citus.sh lake-build  # pg_lake for PG17/el9, built on a throwaway vc2-6c-16gb (iq-citus-build), packed into
#                                 # ~/.iq/citus/pg_lake-<tag>-pg17-el9.tar.gz; the box is destroyed after
#   ./deploy-citus.sh lake-up     # the cold tier on the coordinator: that build, pgduck_server, an S3 store
#                                 # (service/install-lake.sh; SeaweedFS on loopback unless S3_ENDPOINT is given)
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
LABEL="${LABEL:-iq-citus}"
SSH_KEY_ID="${SSH_KEY_ID-4d07a7fe-a68a-4f92-8f7b-e7968f71b6be}"
API=https://api.vultr.com/v2
STATE="$HOME/.iq/citus"; mkdir -p "$STATE"
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
  local key="${VULTR_API_KEY:-$("$here/../UC_App/iq-vault.sh" get VULTR_API_KEY || true)}"
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
  say "running: $LABEL over hosts (coordinator $PUB_coord)"
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

lake_up() {
  local h tar
  h=$(jq -r .coordinator.host "$STATE/topology.json" 2>/dev/null) || { echo "no cluster (up or hosts-up)" >&2; exit 1; }
  tar=$(ls -t "$STATE"/pg_lake-*-pg17-el9.tar.gz 2>/dev/null | head -1)
  [ -n "$tar" ] || { echo "no pg_lake build: $0 lake-build" >&2; exit 1; }
  scp -q "${SSHO[@]}" "$tar" "root@$h:/root/"
  ssh "${SSHO[@]}" "root@$h" bash -s < "$here/service/install-lake.sh"
}

up() {
  local i ip
  i=$(instance)
  if [ -n "$i" ]; then read -r _ ip _ <<<"$i"; echo "$ip" > "$STATE/host"; else launch; ip=$(cat "$STATE/host"); fi
  say "installing on $ip"
  ssh "${SSHO[@]}" "root@$ip" bash -s < "$here/service/install-citus.sh"
  single_topology "$ip"
  say "running: $LABEL $ip"
}

down() {
  local i id ip
  i=$(instance); [ -n "$i" ] || { say "absent"; rm -f "$STATE/host"; return 0; }
  read -r id ip _ <<<"$i"
  api DELETE "/instances/$id" >/dev/null
  ssh-keygen -R "$ip" >/dev/null 2>&1 || true
  [ "$(jq -r .mode "$STATE/topology.json" 2>/dev/null)" = single ] && rm -f "$STATE/topology.json" "$STATE/host"
  say "destroyed $LABEL ($id, $ip)"
}

status() {
  local rows
  rows=$(api GET '/instances?per_page=100' | jq -r --arg l "$LABEL" '.instances[] | select(.label==$l or (.label|startswith($l+"-"))) | "\(.label) \(.main_ip) \(.power_status) \(.id)"')
  [ -n "$rows" ] && echo "$rows" || echo "absent"
  [ -f "$STATE/topology.json" ] && echo "topology: $(jq -r .mode "$STATE/topology.json")"
  return 0
}

case "${1:-}" in
  up|down|status) tunnel; "$1" ;;
  hosts-up|hosts-down|lake-build|lake-build-down) tunnel; f="${1//-/_}"; "$f" ;;
  lake-up) lake_up ;;
  psql) h=$(cat "$STATE/host" 2>/dev/null) || { echo "no host (run up)" >&2; exit 1; }
        shift; ssh "${SSHO[@]}" "root@$h" sudo -u postgres /usr/pgsql-17/bin/psql -X -p 5432 -d iq ${1:+-c "$(printf %q "$*")"} ;;
  *) echo "usage: $0 up | down | hosts-up | hosts-down | lake-build | lake-build-down | lake-up | status | psql [sql]" >&2; exit 2 ;;
esac
