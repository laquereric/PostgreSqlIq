#!/bin/zsh
# @use_case Development/UseCase/UC_PostgreSqlIq
# @facet Store
#
# pg_trgm (trigram similarity, with the GiST operator class gist_trgm_ops) built INTO the image python's pgembed:
# pgembed ships no contrib, while the cluster (postgresql17-contrib) and PGlite (contrib/pg_trgm) both carry pg_trgm
# 1.6, so without it the box is the one tier that cannot answer a trigram search. Built from PostgreSQL 17.9's own
# contrib (the pgembed's version, sha256-verified source) through that pgembed's PGXS, and installed into its
# pginstall (lib/postgresql/pg_trgm.dylib, share/postgresql/extension/pg_trgm*). iq-python.sh runs this for every
# image it builds or finds, as it runs local/age/build-age.sh. Idempotent: an image that already carries pg_trgm
# is left alone. Nothing comes from Homebrew; the source tarball is kept in ~/.iq/dev so a rebuild needs no network.
#   Usage: build-pg-trgm.sh <image python>
set -e
PY="${1:?usage: build-pg-trgm.sh <image python>}"
PG_VER="17.9"
PG_SHA256="3b9a62538a8da151e807a3ddb1198e8605f2032544d78f403ae883d27ecf1ee4"
TRGM_VER="1.6"
DEV="${IQ_DEV_DIR:-$HOME/.iq/dev}"

PGC="$("$PY" -c 'import os, pgembed; print(os.path.join(os.path.dirname(pgembed.__file__), "pginstall", "bin", "pg_config"))')"
[ -x "$PGC" ] || { echo "[build-pg-trgm] no pgembed pg_config under $PY" >&2; exit 1; }
EXT="$("$PGC" --sharedir)/extension"
LIB="$("$PGC" --pkglibdir)"
if [ -f "$EXT/pg_trgm.control" ] && grep -q "default_version = '$TRGM_VER'" "$EXT/pg_trgm.control" \
   && { [ -f "$LIB/pg_trgm.dylib" ] || [ -f "$LIB/pg_trgm.so" ]; }; then
  exit 0
fi
[ "$("$PGC" --version)" = "PostgreSQL $PG_VER" ] || { echo "[build-pg-trgm] pgembed is $("$PGC" --version), not $PG_VER" >&2; exit 1; }

TAR="$DEV/postgresql-$PG_VER.tar.bz2"
mkdir -p "$DEV"
[ -f "$TAR" ] || curl -fsSL -o "$TAR" "https://ftp.postgresql.org/pub/source/v$PG_VER/postgresql-$PG_VER.tar.bz2"
[ "$(shasum -a 256 "$TAR" | awk '{print $1}')" = "$PG_SHA256" ] || { rm -f "$TAR"; echo "[build-pg-trgm] postgresql-$PG_VER sha256 mismatch" >&2; exit 1; }
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
tar -xjf "$TAR" -C "$TMP" "postgresql-$PG_VER/contrib/pg_trgm" "postgresql-$PG_VER/contrib/contrib-global.mk"

echo "[build-pg-trgm] building pg_trgm $TRGM_VER against $("$PGC" --version) ..." >&2
# pgembed's PGXS remembers its CI machine: the macOS SDK path (PG_SYSROOT) and a Homebrew perl. Point both here,
# and drop a caller's MACOSX_DEPLOYMENT_TARGET (cargo/tauri set an old one), as build-age.sh does.
MK=(PG_CONFIG="$PGC" PERL=/usr/bin/perl USE_PGXS=1 with_llvm=no)
[ "$(uname)" = "Darwin" ] && MK+=(PG_SYSROOT="$(xcrun --show-sdk-path)")
(cd "$TMP/postgresql-$PG_VER/contrib/pg_trgm" && unset MACOSX_DEPLOYMENT_TARGET && make "${MK[@]}" -j8 && make "${MK[@]}" install) >"$TMP/trgm.log" 2>&1 \
  || { grep -E "error|Error" "$TMP/trgm.log" | tail -20 >&2; exit 1; }
grep -q "default_version = '$TRGM_VER'" "$EXT/pg_trgm.control" || { echo "[build-pg-trgm] installed pg_trgm is not $TRGM_VER" >&2; exit 1; }
echo "[build-pg-trgm] pg_trgm $TRGM_VER installed into $(dirname "$(dirname "$PGC")")" >&2
