#!/bin/zsh
# @use_case Development/UseCase/UC_PostgreSqlIq
# @facet Store
#
# pg_lake (Snowflake-Labs, Apache-2.0: Iceberg tables in Postgres, Postgres as the catalog) built for the image
# python's pgembed on macOS arm64 (PostgreSQL 17), for UC_BlobStore slice 6 / UC_Group's Iceberg Gold (the user's
# choice 2026-10-21, over Lakekeeper and PyIceberg). The cluster's EL9 build is remote/service/build-pg-lake.sh; this
# is the domain's. Like local/age/build-age.sh, nothing comes from Homebrew at run time: vcpkg (pinned) builds the
# static libraries (OpenSSL, curl, the AWS and Azure SDKs, snappy, jansson, zlib, lzma) and fetches its own cmake and
# ninja; compilers are Xcode's. The build trees are large (DuckDB with httpfs, aws, azure and pg_lake's own extension),
# so they live on a build volume -- by default ~/.iq/dev/build, an APFS sparse image on the external drive -- and only
# what is installed lands on the internal disk.
#
#   build-pg-lake.sh <image python> [stage ...]     stages, in order (each skipped once its marker exists):
#     source    pg_lake at the pinned tag and its pinned submodules (duckdb, duckdb-postgres, avro)
#     vcpkg     vcpkg at its pinned tag, bootstrapped; cmake and ninja fetched by it
#     avro      Avro C (pg_lake's patch), static deps from vcpkg, into the pgembed lib dir
#     duckdb    libduckdb.dylib with pg_lake's extensions (the long one), into the pgembed lib dir
#     btree     btree_gist from PostgreSQL 17.9's contrib (pg_lake_table requires it; pgembed lacks it)
#     ext       pg_extension_base, pg_map, pg_extension_updater, pg_lake_engine, pg_lake_iceberg, pg_lake_table,
#               pg_lake_copy, pg_lake and pgduck_server, through the pgembed's PGXS
#     spatial   DuckDB's own signed spatial extension, installed once by pgduck_server into the pgembed's
#               share/pg_lake/duckdb_extensions (pgduck_server LOADs spatial at start; run it with --extensions_dir
#               there and --no_extension_install, and it needs no network)
#     relink    each binary finds libpq where pgembed's own do (its .dylibs, by @loader_path) and its libraries by a
#               relative rpath, then is signed again (arm64 refuses a changed binary unsigned): PGXS bakes in the
#               pgembed CI machine's libpq path and an absolute rpath, neither of which exists inside IQ.app
#     verify    every installed binary links only the system, the pgembed and itself (no /opt/homebrew, no build dir,
#               no absolute rpath)
#     pack      the installed files as ~/.iq/dev/pg_lake-<tag>-pg17-macos-arm64.tar.gz (a reinstall needs no build)
#   no stage: all of them. Logs: <build>/logs/<stage>.log. IQ_PGLAKE_JOBS caps parallelism (DuckDB's link wants
#   ~3 GB a job; default 6 on this 24 GB box).
set -euo pipefail
PY="${1:?usage: build-pg-lake.sh <image python> [stage ...]}"; shift
TAG="v3.5.7"; TAG_COMMIT="bbbdbb3170d66c0cf2d654f637b0653334098f3d"
VCPKG_TAG="2025.10.17"
PG_VER="17.9"; PG_SHA256="3b9a62538a8da151e807a3ddb1198e8605f2032544d78f403ae883d27ecf1ee4"
BUILD="${IQ_PGLAKE_BUILD:-$HOME/.iq/dev/build}"
JOBS="${IQ_PGLAKE_JOBS:-6}"
SRC="$BUILD/pg_lake"; DEPS="$BUILD/deps"; LOGS="$BUILD/logs"; MARK="$BUILD/markers"
OUT="$HOME/.iq/dev/pg_lake-$TAG-pg17-macos-arm64.tar.gz"
STAGES=(source vcpkg avro duckdb btree ext spatial relink verify pack)
(( $# )) && STAGES=("$@")

PGC="$("$PY" -c 'import os, pgembed; print(os.path.join(os.path.dirname(pgembed.__file__), "pginstall", "bin", "pg_config"))')"
[ -x "$PGC" ] || { echo "[build-pg-lake] no pgembed pg_config under $PY" >&2; exit 1; }
PGROOT="$(dirname "$(dirname "$PGC")")"
case "$("$PGC" --version)" in "PostgreSQL 17."*) ;; *) echo "[build-pg-lake] pgembed is not PostgreSQL 17" >&2; exit 1 ;; esac
[ -d "$BUILD" ] && [ -w "$BUILD" ] || { echo "[build-pg-lake] no build volume at $BUILD (mount the sparse image)" >&2; exit 1; }
case "$BUILD" in *" "*) echo "[build-pg-lake] the build path has a space; mount it somewhere without one" >&2; exit 1 ;; esac
mkdir -p "$DEPS" "$LOGS" "$MARK"

say() { echo "$(date '+%F %T') [build-pg-lake] $*" >&2; }
# pgembed's PGXS remembers its CI machine (the SDK path, a Homebrew perl): point both here, as build-age.sh does. A
# caller's MACOSX_DEPLOYMENT_TARGET (cargo/tauri) is dropped: this links into a server built for this macOS.
unset MACOSX_DEPLOYMENT_TARGET
SDK="$(xcrun --show-sdk-path)"
MK=(PG_CONFIG="$PGC" PERL=/usr/bin/perl PG_SYSROOT="$SDK" with_llvm=no)
export PATH="$PGROOT/bin:/usr/bin:/bin:/usr/sbin:/sbin"       # no /opt/homebrew on the path: nothing found there
export VCPKG_FORCE_DOWNLOADED_BINARIES=1 VCPKG_DISABLE_METRICS=1
export VCPKG_DEFAULT_BINARY_CACHE="$DEPS/vcpkg-cache"; mkdir -p "$VCPKG_DEFAULT_BINARY_CACHE"
TRIPLET=arm64-osx
# Our arm64-osx triplet, overlaid: static libraries, release only, and CMake told to look nowhere in Homebrew -- a
# Homebrew OpenSSL 4 on the box was found by curl's configure and broke it, and would have linked at run time.
NOBREW="/opt/homebrew;/usr/local"
mkdir -p "$DEPS/triplets"
cat > "$DEPS/triplets/$TRIPLET.cmake" <<T
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES arm64)
set(VCPKG_BUILD_TYPE release)
set(VCPKG_CMAKE_CONFIGURE_OPTIONS "-DCMAKE_IGNORE_PREFIX_PATH=$NOBREW" "-DCMAKE_SYSTEM_IGNORE_PREFIX_PATH=$NOBREW")
T
export VCPKG_OVERLAY_TRIPLETS="$DEPS/triplets"

tools() {   # vcpkg's own cmake and ninja, first on the path
  local cm nj
  cm="$("$DEPS/vcpkg/vcpkg" fetch cmake 2>/dev/null | tail -1)"; nj="$("$DEPS/vcpkg/vcpkg" fetch ninja 2>/dev/null | tail -1)"
  export PATH="$(dirname "$cm"):$(dirname "$nj"):$PATH"
}

stage_source() {
  [ -d "$SRC/.git" ] || git clone -q --branch "$TAG" https://github.com/Snowflake-Labs/pg_lake.git "$SRC"
  [ "$(git -C "$SRC" rev-parse HEAD)" = "$TAG_COMMIT" ] || { say "$TAG is not $TAG_COMMIT"; return 1; }
  git -C "$SRC" submodule update --init avro duckdb_pglake/duckdb duckdb_pglake/duckdb-postgres
  git -C "$SRC/duckdb_pglake/duckdb-postgres" submodule update --init database-connector
}

stage_vcpkg() {
  [ -d "$DEPS/vcpkg/.git" ] || git clone -q --branch "$VCPKG_TAG" https://github.com/microsoft/vcpkg.git "$DEPS/vcpkg"
  [ -x "$DEPS/vcpkg/vcpkg" ] || "$DEPS/vcpkg/bootstrap-vcpkg.sh" -disableMetrics
  "$DEPS/vcpkg/vcpkg" install --triplet "$TRIPLET" --x-install-root="$DEPS/vcpkg-installed" snappy jansson zlib liblzma
  tools; cmake --version | head -1; ninja --version
}

stage_avro() {
  tools
  local a="$SRC/avro"
  (cd "$a" && git checkout -f . && git apply --ignore-whitespace ../avro.patch)
  rm -rf "$a/lang/c/build"; mkdir -p "$a/lang/c/build"
  local inst="$DEPS/vcpkg-installed/$TRIPLET"
  cmake -G Ninja -S "$a/lang/c" -B "$a/lang/c/build" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX="$DEPS/avrolib" -DCMAKE_PREFIX_PATH="$inst" -DCMAKE_OSX_SYSROOT="$SDK" \
    -DSNAPPY_INCLUDE_DIRS="$inst/include" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_SHARED_LINKER_FLAGS=-lc++ -DCMAKE_EXE_LINKER_FLAGS=-lc++     # snappy is C++: libavro carries it, static
  cmake --build "$a/lang/c/build" -j "$JOBS"
  cmake --install "$a/lang/c/build"
  local lib="$("$PGC" --libdir)"
  rm -f "$lib"/libavro.*                                  # -lavro must find the dylib, never a static archive
  cp -fP "$DEPS/avrolib/lib/"libavro*.dylib "$lib"/      # the versioned file and its symlinks
  local real; real="$(ls "$lib"/libavro.*.*.*.dylib)"     # its install name: by rpath, not the build tree's path
  install_name_tool -id "@rpath/$(basename "$(readlink "$lib/libavro.dylib")")" "$real"
  codesign -f -s - "$real"
  mkdir -p "$("$PGC" --includedir)/avro"; cp -Rf "$DEPS/avrolib/include/." "$("$PGC" --includedir)/"
}

stage_duckdb() {
  tools
  # pg_lake's overlay makes openssl an empty port (the distro's, on Linux); here vcpkg builds it, static
  [ -d "$SRC/duckdb_pglake/vcpkg_ports/openssl" ] && mv "$SRC/duckdb_pglake/vcpkg_ports/openssl" "$DEPS/openssl-port-of-pg_lake"
  rm -rf "$SRC/duckdb_pglake/build/release"
  (cd "$SRC/duckdb_pglake" && make "${MK[@]}" GEN=ninja OSX_BUILD_ARCH=arm64 VCPKG_TARGET_TRIPLET="$TRIPLET" \
     EXT_RELEASE_FLAGS="-DCMAKE_IGNORE_PREFIX_PATH='$NOBREW' -DCMAKE_SYSTEM_IGNORE_PREFIX_PATH='$NOBREW' -DCMAKE_MAKE_PROGRAM=$(command -v ninja)" \
     VCPKG_TOOLCHAIN_PATH="$DEPS/vcpkg/scripts/buildsystems/vcpkg.cmake" NCORES="$JOBS" \
     CMAKE_BUILD_PARALLEL_LEVEL="$JOBS" install)
}

stage_btree() {
  local t="$DEPS/postgresql-$PG_VER.tar.bz2"
  [ -f "$t" ] || curl -fsSL -o "$t" "https://ftp.postgresql.org/pub/source/v$PG_VER/postgresql-$PG_VER.tar.bz2"
  [ "$(shasum -a 256 "$t" | cut -c1-64)" = "$PG_SHA256" ] || { say "postgresql-$PG_VER sha256 mismatch"; rm -f "$t"; return 1; }
  [ -d "$DEPS/postgresql-$PG_VER" ] || tar -xjf "$t" -C "$DEPS"
  (cd "$DEPS/postgresql-$PG_VER/contrib/btree_gist" && make "${MK[@]}" USE_PGXS=1 -j "$JOBS" && make "${MK[@]}" USE_PGXS=1 install)
}

stage_ext() {
  local d
  for d in pg_extension_base pg_map pg_extension_updater pg_lake_engine pg_lake_iceberg pg_lake_table pg_lake_copy \
           pg_lake pgduck_server; do
    say "  $d"
    (cd "$SRC/$d" && make "${MK[@]}" clean >/dev/null && make "${MK[@]}" -j "$JOBS" && make "${MK[@]}" install)
  done
}

stage_spatial() {
  local ext="$("$PGC" --sharedir)/pg_lake/duckdb_extensions" tmp sock pid
  mkdir -p "$ext"; tmp="$(mktemp -d /tmp/pgl.XXXXXX)"; sock="$tmp/s"; mkdir -p "$sock"
  "$PGROOT/bin/pgduck_server" --unix_socket_directory "$sock" --port 5398 --cache_dir "$tmp/cache" \
    --duckdb_database_file_path "$tmp/duck.db" --extensions_dir "$ext" > "$tmp/log" 2>&1 & pid=$!
  for _ in $(seq 1 240); do
    [ -S "$sock/.s.PGSQL.5398" ] && break
    kill -0 $pid 2>/dev/null || { cat "$tmp/log"; return 1; }
    perl -e 'select(undef,undef,undef,0.5)'
  done
  kill $pid; wait $pid 2>/dev/null || true; cat "$tmp/log"; rm -rf "$tmp"
  ls "$ext"/*/*/spatial.duckdb_extension
}

installed() {   # every file the stages installed into the pgembed, newer than the first stage's marker
  find "$PGROOT" -type f -newer "$MARK/source" \( -path '*/lib/*' -o -path '*/bin/*' -o -path '*/share/*' \) | sort
}

relpath() {   # the path from directory $1 to file $2, both absolute
  "$PY" -c 'import os, sys; print(os.path.relpath(sys.argv[2], sys.argv[1]))' "$1" "$2"
}

stage_relink() {
  local f dep rp pq; pq="$(dirname "$PGROOT")/.dylibs/libpq.5.dylib"
  [ -f "$pq" ] || { say "no $pq"; return 1; }
  for f in $(installed | grep -E '\.(dylib|so)$|/bin/'); do
    file -b "$f" | grep -q Mach-O || continue
    for dep in $(otool -L "$f" | tail -n +2 | awk '{print $1}' | grep '/libpq\.5\.dylib$' | grep -v '^@'); do
      install_name_tool -change "$dep" "@loader_path/$(relpath "$(dirname "$f")" "$pq")" "$f"
    done
    for rp in $(otool -l "$f" | awk '/LC_RPATH/ {getline; getline; print $2}' | grep '^/'); do
      install_name_tool -rpath "$rp" "@loader_path/$(relpath "$(dirname "$f")" "$rp")" "$f"
    done
    codesign -f -s - "$f" 2>/dev/null
  done
}

stage_verify() {
  local bad=0 f dep
  for f in $(installed | grep -E '\.(dylib|so)$|/bin/'); do
    file -b "$f" | grep -q Mach-O || continue
    for dep in $(otool -L "$f" | tail -n +2 | awk '{print $1}'); do
      case "$dep" in
        /usr/lib/*|/System/*|@rpath/*|@loader_path/*|@executable_path/*) ;;
        *) say "  $f links $dep"; bad=1 ;;
      esac
    done
    for dep in $(otool -l "$f" | awk '/LC_RPATH/ {getline; getline; print $2}' | grep '^/'); do
      say "  $f has the absolute rpath $dep"; bad=1
    done
    codesign -v "$f" 2>/dev/null || { say "  $f is not validly signed"; bad=1; }
  done
  [ "$bad" = 0 ] || { say "verify: a binary links outside the system and the pgembed"; return 1; }
  say "verify: $(installed | wc -l | tr -d ' ') files, every binary self-contained"
}

stage_pack() {
  installed | sed "s#^$PGROOT/##" > "$BUILD/pg_lake.files"
  tar -czf "$OUT" -C "$PGROOT" -T "$BUILD/pg_lake.files"
  say "packed $OUT ($(du -h "$OUT" | cut -f1))"
}

for s in "${STAGES[@]}"; do
  if [ -f "$MARK/$s" ]; then say "$s: done before"; continue; fi
  say "$s ..."
  # a subshell with errexit, outside any condition: under `if`, zsh would run the stage with errexit off and a
  # failing command inside it (Avro's shared link, the first time) would pass unseen
  set +e; ( set -e; "stage_$s" ) > "$LOGS/$s.log" 2>&1; rc=$?; set -e
  if [ "$rc" = 0 ]; then
    touch "$MARK/$s"; say "$s: ok"
  else
    say "$s: FAILED (tail of $LOGS/$s.log):"; tail -25 "$LOGS/$s.log" >&2; exit 1
  fi
done
