#!/bin/zsh
# @use_case Development/UseCase/UC_Postgres
# @facet Store
#
# Apache AGE (openCypher graphs in Postgres) built INTO the image python's pgembed: AGE has no pgembed companion
# wheel, so it is compiled from the Apache source against that pgembed's own pg_config (PostgreSQL 17) and installed
# into its pginstall (lib/postgresql/age.dylib, share/postgresql/extension/age*). iq-python.sh runs this for every
# image it builds or finds, so an IQ.app built from the repo runtime carries AGE. Idempotent: an image that already
# carries this AGE version is left alone. Nothing comes from Homebrew: bison 3 (AGE's parser needs it; macOS ships
# 2.3) is built from the GNU source into ~/.iq/dev/bison-<v> once, both sources verified by checksum.
#   Usage: build-age.sh <image python>
set -e
PY="${1:?usage: build-age.sh <image python>}"
AGE_VER="1.7.0"
AGE_URL="https://github.com/apache/age/releases/download/PG17%2Fv${AGE_VER}-rc0/apache-age-${AGE_VER}-src.tar.gz"
AGE_SHA512="301f02088f6828adb8cfa70e7701d9fb541ee9212de97a99f2cf9106e0b0d8bce82f95c20579302bdac5efc3d5ffe35732755b1d093526cec4beba9f30ec82f0"
BISON_VER="3.8.2"
BISON_URL="https://ftp.gnu.org/gnu/bison/bison-${BISON_VER}.tar.xz"
BISON_SHA256="9bba0214ccf7f1079c5d59210045227bcf619519840ebfa80cd3849cff5a5bf2"
DEV="${IQ_DEV_DIR:-$HOME/.iq/dev}"

PGC="$("$PY" -c 'import os, pgembed; print(os.path.join(os.path.dirname(pgembed.__file__), "pginstall", "bin", "pg_config"))')"
[ -x "$PGC" ] || { echo "[build-age] no pgembed pg_config under $PY" >&2; exit 1; }
EXT="$("$PGC" --sharedir)/extension"
LIB="$("$PGC" --pkglibdir)"
if [ -f "$EXT/age--${AGE_VER}.sql" ] && { [ -f "$LIB/age.dylib" ] || [ -f "$LIB/age.so" ]; }; then
  exit 0
fi

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
BISON="$DEV/bison-${BISON_VER}/bin/bison"
if [ ! -x "$BISON" ]; then
  echo "[build-age] building bison ${BISON_VER} into $DEV ..." >&2
  curl -fsSL -o "$TMP/bison.tar.xz" "$BISON_URL"
  [ "$(shasum -a 256 "$TMP/bison.tar.xz" | awk '{print $1}')" = "$BISON_SHA256" ] || { echo "[build-age] bison sha256 mismatch" >&2; exit 1; }
  tar -xJf "$TMP/bison.tar.xz" -C "$TMP"
  (cd "$TMP/bison-${BISON_VER}" && ./configure --prefix="$DEV/bison-${BISON_VER}" --disable-nls -q && make -s -j8 && make -s install) >"$TMP/bison.log" 2>&1 \
    || { tail -20 "$TMP/bison.log" >&2; exit 1; }
fi

echo "[build-age] building AGE ${AGE_VER} against $("$PGC" --version) ..." >&2
curl -fsSL -o "$TMP/age.tar.gz" "$AGE_URL"
[ "$(shasum -a 512 "$TMP/age.tar.gz" | awk '{print $1}')" = "$AGE_SHA512" ] || { echo "[build-age] AGE sha512 mismatch" >&2; exit 1; }
tar -xzf "$TMP/age.tar.gz" -C "$TMP"
# pgembed's PGXS remembers its CI machine: the macOS SDK path (PG_SYSROOT) and a Homebrew perl. Point both here.
MK=(PG_CONFIG="$PGC" BISON="$BISON" PERL=/usr/bin/perl)
[ "$(uname)" = "Darwin" ] && MK+=(PG_SYSROOT="$(xcrun --show-sdk-path)")
# a caller's MACOSX_DEPLOYMENT_TARGET (cargo/tauri set an old one for build.rs) would make AGE's -Werror reject
# preadv/pwritev in PostgreSQL's own headers; the server this links into targets this machine's macOS anyway
(cd "$TMP/apache-age-${AGE_VER}" && unset MACOSX_DEPLOYMENT_TARGET && make "${MK[@]}" -j8 && make "${MK[@]}" install) >"$TMP/age.log" 2>&1 \
  || { grep -E "error|Error" "$TMP/age.log" | tail -20 >&2; exit 1; }
echo "[build-age] AGE ${AGE_VER} installed into $(dirname "$(dirname "$PGC")")" >&2
