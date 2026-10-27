#!/bin/bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# ParadeDB's pg_search (BM25; AGPL-3.0, accepted for IQ) on one Citus node: the PGDG/el9 package for PostgreSQL 17,
# pinned by sha256. Runs ON each node's host as root (deploy-citus.sh search-up: the coordinator and every worker,
# since an index on a distributed table is built on every shard). Idempotent.
set -euo pipefail
VER=0.26.2
URL=https://github.com/paradedb/paradedb/releases/download/v$VER/pg_search_17-$VER-1PARADEDB.el9.x86_64.rpm
SHA=2ee7a5869950a052e892f57223f1fcca73209baad0e875129828ae0b157f68de
if ! rpm -q pg_search_17 >/dev/null 2>&1; then
  curl -fsSL -o /root/pg_search.rpm "$URL"
  echo "$SHA  /root/pg_search.rpm" | sha256sum -c --quiet
  dnf -y -q install /root/pg_search.rpm; rm -f /root/pg_search.rpm
fi
# preloaded on every node here, after citus when there is Citus (it must come first) -- alone on the server shape,
# whose list is empty; each node restarted once for it
for conf in /var/lib/pgsql/17/*/iq.conf; do
  unit=$(basename "$(dirname "$conf")")
  cur=$(sed -n "s/^shared_preload_libraries = '\([^']*\)'.*/\1/p" "$conf")
  want=$(printf '%s\n' "${cur//,/$'\n'}" pg_search | awk 'NF && !seen[$0]++' | paste -sd, -)
  if [ "$cur" != "$want" ]; then
    sed -i "s/^shared_preload_libraries = '[^']*'/shared_preload_libraries = '$want'/" "$conf"
    systemctl restart "iq-citus@$unit"
    port=$(awk '/^port/ {print $3}' "$conf")
    for _ in $(seq 30); do sudo -u postgres /usr/pgsql-17/bin/pg_isready -q -p "$port" && break; sleep 1; done
  fi
done
echo "$(hostname): $(rpm -q pg_search_17); preload: $(grep -h '^shared_preload_libraries' /var/lib/pgsql/17/*/iq.conf | sort -u | tr '\n' ' ')"
