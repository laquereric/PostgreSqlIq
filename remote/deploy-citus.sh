#!/usr/bin/env bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# UC_Citus: an on-demand Citus cluster on Vultr, private: only :22 is open; Postgres is reached through an SSH tunnel.
#
#   ./deploy-citus.sh up          # launch the instance (if absent) + install the cluster (service/install-citus.sh)
#   ./deploy-citus.sh status      # running (ip) | absent
#   ./deploy-citus.sh down        # destroy the instance (on demand: the schema is re-applied by the scenarios)
#   ./deploy-citus.sh psql [sql]  # the coordinator as postgres, over ssh (superuser stays on the host)
#
# The host is kept in ~/.iq/citus/host. A vc2-2c-4gb is $20/mo, billed by the hour (about $0.03/h) while it exists,
# halted or not -- `down` is what stops it costing. The Vultr key and the API route are UC_MarkLogic/deploy-vultr.sh's:
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

launch() {
  local os_id fw_id id ip st
  os_id=$(api GET '/os?per_page=500' | jq -r '.os[] | select(.name|test("^Rocky Linux 9 x64")) | .id' | head -1)
  [ -n "$os_id" ] || { echo "no Rocky Linux 9 image found" >&2; exit 1; }
  fw_id=$(api GET /firewalls | jq -r --arg d "$LABEL-ssh-only" '.firewall_groups[] | select(.description==$d) | .id' | head -1)
  [ -n "$fw_id" ] || fw_id=$(api POST /firewalls "$(jq -n --arg d "$LABEL-ssh-only" '{description:$d}')" | jq -r .firewall_group.id)
  [ "$(api GET "/firewalls/$fw_id/rules" | jq '.firewall_rules | length')" -gt 0 ] || for t in v4 v6; do
    sub=$([ $t = v4 ] && echo 0.0.0.0 || echo ::)
    api POST "/firewalls/$fw_id/rules" "$(jq -n --arg t $t --arg s $sub '{ip_type:$t,protocol:"tcp",subnet:$s,subnet_size:0,port:"22",notes:"ssh"}')" >/dev/null
  done
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

up() {
  local i ip
  i=$(instance)
  if [ -n "$i" ]; then read -r _ ip _ <<<"$i"; echo "$ip" > "$STATE/host"; else launch; ip=$(cat "$STATE/host"); fi
  say "installing on $ip"
  ssh "${SSHO[@]}" "root@$ip" bash -s < "$here/service/install-citus.sh"
  say "running: $LABEL $ip"
}

down() {
  local i id ip
  i=$(instance); [ -n "$i" ] || { say "absent"; rm -f "$STATE/host"; return 0; }
  read -r id ip _ <<<"$i"
  api DELETE "/instances/$id" >/dev/null
  ssh-keygen -R "$ip" >/dev/null 2>&1 || true
  rm -f "$STATE/host"
  say "destroyed $LABEL ($id, $ip)"
}

status() {
  local i; i=$(instance)
  if [ -n "$i" ]; then read -r id ip pw <<<"$i"; echo "running: $LABEL $ip ($pw, instance $id)"; else echo "absent"; fi
}

case "${1:-}" in
  up|down|status) tunnel; "$1" ;;
  psql) h=$(cat "$STATE/host" 2>/dev/null) || { echo "no host (run up)" >&2; exit 1; }
        shift; ssh "${SSHO[@]}" "root@$h" sudo -u postgres /usr/pgsql-17/bin/psql -X -p 5432 -d iq ${1:+-c "$(printf %q "$*")"} ;;
  *) echo "usage: $0 up | status | down | psql [sql]" >&2; exit 2 ;;
esac
