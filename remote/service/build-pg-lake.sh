#!/bin/bash
# @use_case Development/UseCase/UC_Citus
# @facet Store
#
# pg_lake (Snowflake-Labs, Apache-2.0) built for the cluster: PGDG PostgreSQL 17 on Rocky Linux 9, the same as
# install-citus.sh installs. Runs ON a throwaway build instance as root (deploy-citus.sh lake-build launches it,
# fetches the result, destroys it): DuckDB and the Azure SDK compile for a long time and need more memory than a
# cluster node has. pg_lake's own install.sh does the build (system deps, vcpkg, the extensions, pgduck_server);
# everything it installs is then packed, with the list of runtime libraries it links, into
# /root/pg_lake-<tag>-pg17-el9.tar.gz. Progress goes to /root/build-pg-lake.log; /root/build-pg-lake.done at the end.
set -euo pipefail
TAG="${PG_LAKE_TAG:-v3.5.7}"
OUT="/root/pg_lake-$TAG-pg17-el9.tar.gz"
exec >> /root/build-pg-lake.log 2>&1
say() { echo "$(date '+%F %T') $*"; }
trap 'say "FAILED at line $LINENO"; echo failed > /root/build-pg-lake.done' ERR

say "PostgreSQL 17 (PGDG) and its headers"
rpm -q pgdg-redhat-repo >/dev/null 2>&1 || dnf -y -q install \
  https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
dnf -qy module disable postgresql
dnf -y -q install epel-release
dnf config-manager --set-enabled crb
dnf -y -q install postgresql17-server postgresql17-devel git sudo krb5-devel libzstd-devel   # PGDG headers include GSSAPI
export PATH=/usr/pgsql-17/bin:$PATH

say "pg_lake $TAG"
mkdir -p /root/build && cd /root/build
[ -d pg_lake ] || git clone -q --depth 1 --branch "$TAG" https://github.com/Snowflake-Labs/pg_lake.git
cd pg_lake
touch /root/build-marker
sleep 1
MAKEFLAGS="-j$(nproc) with_llvm=no" ./install.sh --with-system-deps   # no JIT bitcode: PGDG builds with LLVM, the box has no clang

say "packing what the build installed"
find /usr/pgsql-17 /usr/local -newer /root/build-marker -type f > /root/pg_lake.files
grep -c . /root/pg_lake.files
for f in $(grep -E '\.so|/bin/' /root/pg_lake.files); do ldd "$f" 2>/dev/null | awk '/=>/ {print $3}'; done | sort -u \
  | xargs -r rpm -qf --qf '%{NAME}\n' 2>/dev/null | sort -u | grep -v 'not owned' > /root/pg_lake.runtime-rpms || true
cp /root/pg_lake.files /root/pg_lake.runtime-rpms /usr/pgsql-17/share/ 2>/dev/null || true
tar -czf "$OUT" -T /root/pg_lake.files /usr/pgsql-17/share/pg_lake.files /usr/pgsql-17/share/pg_lake.runtime-rpms
say "packed $OUT ($(du -h "$OUT" | cut -f1)); runtime rpms: $(tr '\n' ' ' < /root/pg_lake.runtime-rpms)"
echo ok > /root/build-pg-lake.done
