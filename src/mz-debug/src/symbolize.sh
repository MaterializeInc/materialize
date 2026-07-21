#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# symbolize.sh: symbolize Materialize pprof profiles, using Docker so that
# no other tooling needs to be installed.
#
# This script ships inside mz-debug dumps next to the profiles it operates
# on, so it must stay fully self-contained. The part of it that runs on the
# host must also stay compatible with the bash 3.2 that macOS ships.

set -euo pipefail

# Pinned google/pprof version. The pprof bundled with the Go toolchain is too
# old to understand Materialize binaries, so we install this version into the
# tool image. Bumping the pin changes the image hash, which triggers a rebuild
# on the next run.
PPROF_VERSION=v0.0.0-20260709232956-b9395ee17fa0

# The debuginfod-style HTTP front for the bucket that Materialize CI uploads
# executables and debug info to, keyed by GNU build ID. No authentication is
# required.
DEBUGINFO_URL=${MZ_DEBUGINFO_URL:-https://debuginfo.dev.materialize.com}

# Binaries that Materialize CI uploads debug info for. Keep in sync with
# DEBUGINFO_BINS in misc/python/materialize/ci_util/upload_debug_symbols_to_s3.py.
MZ_BINARIES='environmentd|clusterd|balancerd|materialized'

# Cache directory, relative to the profiles directory. Doubles as the
# PPROF_BINARY_PATH tree: <cache>/<buildid>/<binary name> plus a
# <binary name>.debug twin.
CACHE_DIR=.debuginfo

# Fixed port inside the container for --http mode. The host side is
# configurable via --port.
INNER_PORT=8080

msg() {
    echo "$*" >&2
}

die() {
    echo "error: $*" >&2
    exit 1
}

usage() {
    cat <<'USAGE'
usage: bash symbolize.sh [options] [PROFILE...]

Symbolizes Materialize pprof profiles (*.pprof.gz) so they can be viewed and
shared with function names. Run it inside the profiles/ directory of an
mz-debug dump, or pass profile paths relative to the current directory.

With no arguments, every *.pprof.gz in the current directory is symbolized
and written next to the original as <name>.symbolized.pprof.gz. Share those
directly, for example by uploading to https://pprof.me.

options:
  --http [PROFILE]  serve an interactive pprof web UI for one profile
  --port PORT       host port for --http (default 8080)
  --rebuild         rebuild the Docker tool image from scratch
  -h, --help        show this help

Requirements: Docker, plus network access on the first run (to build the tool
image) and whenever debug info is not cached yet. Debug info is fetched from
https://debuginfo.dev.materialize.com by build ID and cached under
./.debuginfo, which is safe to delete. Profiles from PR or main builds may
have expired upstream, tagged release builds are retained. On Windows, run
this under WSL2.
USAGE
}

MODE="batch"
PORT=8080
REBUILD=0
FILES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --http) MODE=http ;;
        --port)
            shift
            [[ $# -gt 0 ]] || die "--port requires a value"
            PORT=$1
            ;;
        --rebuild) REBUILD=1 ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            while [[ $# -gt 0 ]]; do
                FILES+=("$1")
                shift
            done
            break
            ;;
        -*) die "unknown option: $1 (see --help)" ;;
        *) FILES+=("$1") ;;
    esac
    shift
done

# ---------------------------------------------------------------------------
# Host phase. Builds the tool image if needed, then re-invokes this script
# inside the container with MZ_SYMBOLIZE_INNER=1.
# ---------------------------------------------------------------------------

outer_main() {
    command -v docker >/dev/null 2>&1 \
        || die "docker is required. Install Docker Desktop (macOS) or docker-ce (Linux)."
    docker info >/dev/null 2>&1 \
        || die "cannot talk to the Docker daemon. Is Docker running?"

    # Profile arguments travel into the container via the $PWD bind mount, so
    # they must stay inside the current directory.
    local f
    for f in ${FILES[@]+"${FILES[@]}"}; do
        case "$f" in
            /*|*..*) die "pass profile paths relative to the current directory: $f" ;;
        esac
    done

    # The Dockerfile is hashed to produce the image tag, so any change to it
    # (including the pprof pin) makes the next run rebuild automatically. The
    # unversioned llvm tool names are not always present, so link them if
    # needed. pprof prefers the llvm tools and they can read ELF and DWARF for
    # foreign architectures, which lets e.g. arm64 profiles symbolize on an
    # x86_64 machine.
    local dockerfile
    dockerfile="FROM golang:1.24-bookworm AS build
RUN go install github.com/google/pprof@$PPROF_VERSION
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        binutils ca-certificates curl graphviz llvm \
    && rm -rf /var/lib/apt/lists/* \
    && for tool in llvm-symbolizer llvm-addr2line llvm-nm; do \
        command -v \"\$tool\" >/dev/null \
            || ln -s \"\$(ls /usr/bin/\$tool-* | head -n1)\" \"/usr/local/bin/\$tool\" \
            || true; \
    done
COPY --from=build /go/bin/pprof /usr/local/bin/pprof"

    local tag image
    if command -v shasum >/dev/null 2>&1; then
        tag=$(printf '%s' "$dockerfile" | shasum -a 256 | cut -c1-12)
    else
        tag=$(printf '%s' "$dockerfile" | sha256sum | cut -c1-12)
    fi
    image="mz-pprof-symbolize:$tag"

    if [[ "$REBUILD" = 1 ]] || ! docker image inspect "$image" >/dev/null 2>&1; then
        msg "Building the symbolization tool image $image. This happens once per version and needs network access."
        local build_args=(-t "$image" -)
        if [[ "$REBUILD" = 1 ]]; then
            build_args=(--no-cache "${build_args[@]}")
        fi
        printf '%s\n' "$dockerfile" | docker build "${build_args[@]}" \
            || die "failed to build the tool image. The first run needs network access to pull base images."
    fi

    # Resolve our own absolute path so the container can re-run us regardless
    # of how we were invoked and whether the executable bit survived.
    local script_path
    script_path=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")

    local run_args=(
        --rm -i --init
        -u "$(id -u):$(id -g)"
        -e HOME=/tmp
        -e MZ_SYMBOLIZE_INNER=1
        -e MZ_DEBUGINFO_URL="$DEBUGINFO_URL"
        -v "$PWD:/work"
        -w /work
        -v "$script_path:/symbolize.sh:ro"
    )
    if [[ -t 1 ]]; then
        run_args+=(-t)
    fi

    local inner_args=()
    if [[ "$MODE" = http ]]; then
        run_args+=(-p "127.0.0.1:$PORT:$INNER_PORT")
        inner_args+=(--http)
        msg "pprof web UI will be at http://localhost:$PORT once ready. Ctrl-C to stop."
    fi

    exec docker run "${run_args[@]}" "$image" \
        bash /symbolize.sh ${inner_args[@]+"${inner_args[@]}"} ${FILES[@]+"${FILES[@]}"}
}

# ---------------------------------------------------------------------------
# Container phase. Everything below runs inside the tool image, where bash 5,
# pprof, curl, and the llvm tools are available.
# ---------------------------------------------------------------------------

# Prints "buildid name" lines for every mapping in the profile that carries a
# build ID. The Mappings section is the last one in `pprof -raw` output and
# its lines look like:
#   1: 0x0/0xffffffffffffffff/0x0 /usr/local/bin/environmentd 0ad9842ad36507fd
extract_mappings() {
    local profile=$1
    # NOTE: -symbolize=none matters, without it pprof attempts symbolization
    # just to dump the raw profile. The build ID check avoids regex interval
    # expressions like {8,} because mawk, the default awk on Debian, does not
    # support them.
    pprof -raw -symbolize=none "$profile" 2>/dev/null | awk '
        /^Mappings/ { in_mappings = 1; next }
        !in_mappings { next }
        NF == 0 { next }
        $1 !~ /^[0-9]+:$/ { exit }
        NF >= 4 && length($4) >= 8 && $4 ~ /^[0-9a-f]+$/ {
            n = split($3, parts, "/")
            print $4, parts[n]
        }'
}

is_mz_binary() {
    [[ "$1" =~ ^($MZ_BINARIES)$ ]]
}

# Fetches executable and debug info for one (buildid, name) pair into the
# cache. Sets fetched=OK, fetched=MISSING (404 upstream), or fetched=ERROR.
TRANSPORT_FAILED=0
fetch_pair() {
    local bid=$1 name=$2
    local dir=$CACHE_DIR/$bid
    local exe=$dir/$name
    local dbg=$exe.debug
    fetched=OK

    if [[ -s "$exe" && -s "$dbg" ]]; then
        return
    fi
    if [[ "$TRANSPORT_FAILED" = 1 ]]; then
        fetched=ERROR
        return
    fi
    mkdir -p "$dir"

    local kind target tmp code curl_rc
    for kind in executable debuginfo; do
        target=$exe
        if [[ "$kind" = debuginfo ]]; then
            target=$dbg
        fi
        if [[ -s "$target" ]]; then
            continue
        fi
        msg "Fetching $kind for $name (build ID $bid)..."
        tmp="$target.tmp.$$"
        local curl_args=(-SL --retry 3 --retry-connrefused -o "$tmp" -w '%{http_code}')
        if [[ ! -t 2 ]]; then
            curl_args=(-s "${curl_args[@]}")
        else
            curl_args=(--progress-bar "${curl_args[@]}")
        fi
        curl_rc=0
        code=$(curl "${curl_args[@]}" "$DEBUGINFO_URL/buildid/$bid/$kind") || curl_rc=$?
        if [[ "$curl_rc" != 0 ]]; then
            rm -f "$tmp"
            TRANSPORT_FAILED=1
            fetched=ERROR
            msg "cannot reach $DEBUGINFO_URL, check network and VPN. Continuing with cached debug info only."
            return
        fi
        case "$code" in
            200)
                # The rename is atomic, so an interrupted download can never
                # poison the cache.
                mv "$tmp" "$target"
                ;;
            404)
                rm -f "$tmp"
                fetched=MISSING
                msg "debug info for $name (build ID $bid) is not available upstream (HTTP 404)."
                msg "  PR and main build artifacts expire after a retention window, tagged release builds are retained."
                msg "  Frames from this binary will stay unsymbolized."
                return
                ;;
            *)
                rm -f "$tmp"
                fetched=ERROR
                msg "fetching $kind for build ID $bid failed with HTTP $code, skipping"
                return
                ;;
        esac
    done
}

inner_main() {
    local files=()
    if [[ ${#FILES[@]} -gt 0 ]]; then
        files=("${FILES[@]}")
    else
        shopt -s nullglob
        local f
        for f in *.pprof.gz; do
            # Skip our own outputs so that re-runs stay idempotent.
            if [[ "$f" != *.symbolized.pprof.gz ]]; then
                files+=("$f")
            fi
        done
        shopt -u nullglob
    fi
    [[ ${#files[@]} -gt 0 ]] \
        || die "no *.pprof.gz files found here. Run from an mz-debug profiles/ directory or pass profile paths."

    if [[ "$MODE" = http && ${#files[@]} -gt 1 ]]; then
        msg "pick one profile to serve: bash symbolize.sh --http <file>"
        for f in "${files[@]}"; do
            msg "  $f"
        done
        exit 1
    fi

    mkdir -p "$CACHE_DIR"

    # Pass 1: read the mappings of every profile and collect the set of
    # Materialize binaries to fetch. Fetching is deduplicated across profiles
    # since replicas of the same build share one binary.
    declare -A file_status file_note file_pairs
    declare -A wanted
    local f pair bid name mappings
    for f in "${files[@]}"; do
        if [[ ! -s "$f" ]]; then
            file_status[$f]=SKIPPED
            file_note[$f]="file not found or empty"
            continue
        fi
        if ! mappings=$(extract_mappings "$f") || [[ -z "$mappings" ]]; then
            file_status[$f]=SKIPPED
            file_note[$f]="not a pprof profile or no mappings to symbolize"
            continue
        fi
        file_pairs[$f]=""
        while read -r bid name; do
            if is_mz_binary "$name"; then
                wanted["$bid $name"]=1
                file_pairs[$f]+="$bid $name"$'\n'
            fi
        done <<<"$mappings"
        if [[ -z "${file_pairs[$f]}" ]]; then
            file_status[$f]=SKIPPED
            file_note[$f]="no Materialize binaries in the profile mappings"
        fi
    done

    # Pass 2: fetch.
    declare -A pair_status
    for pair in "${!wanted[@]}"; do
        read -r bid name <<<"$pair"
        fetch_pair "$bid" "$name"
        pair_status[$pair]=$fetched
    done

    # Pass 3: symbolize.
    local errlog=$CACHE_DIR/last-symbolize.log
    : >"$errlog"
    local any_bad=0
    for f in "${files[@]}"; do
        if [[ -n "${file_status[$f]:-}" ]]; then
            continue
        fi

        local missing=""
        local have_one=0
        while read -r bid name; do
            [[ -n "$bid" ]] || continue
            if [[ "${pair_status[$bid $name]}" = OK ]]; then
                have_one=1
            else
                missing+="$name $bid "
            fi
        done <<<"${file_pairs[$f]}"

        if [[ "$have_one" = 0 ]]; then
            file_status[$f]=FAILED
            file_note[$f]="no debug info available for any binary in the profile"
            any_bad=1
            continue
        fi

        local out="${f%.pprof.gz}.symbolized.pprof.gz"
        local tmperr
        tmperr=$(mktemp)
        if ! PPROF_BINARY_PATH=/work/$CACHE_DIR \
            pprof -proto -symbolize=local -output="$out" "$f" 2>"$tmperr"; then
            file_status[$f]=FAILED
            file_note[$f]="pprof failed, see $errlog"
            rm -f "$out"
            any_bad=1
        elif grep -E "Local symbolization failed for ($MZ_BINARIES)" "$tmperr" >/dev/null; then
            file_status[$f]=PARTIAL
            file_note[$f]="unsymbolized: ${missing:-see $errlog}"
            any_bad=1
        elif [[ -n "$missing" ]]; then
            file_status[$f]=PARTIAL
            file_note[$f]="unsymbolized: $missing"
            any_bad=1
        else
            file_status[$f]=OK
            file_note[$f]="-> $out"
        fi
        {
            echo "==== $f"
            cat "$tmperr"
        } >>"$errlog"
        rm -f "$tmperr"
    done

    if [[ "$MODE" = http ]]; then
        f=${files[0]}
        if [[ "${file_status[$f]}" = SKIPPED || "${file_status[$f]}" = FAILED ]]; then
            die "cannot serve $f: ${file_note[$f]}"
        fi
        msg "Serving the pprof web UI for $f. Ctrl-C to stop."
        exec env PPROF_BINARY_PATH=/work/$CACHE_DIR \
            pprof -http="0.0.0.0:$INNER_PORT" -no_browser "$f"
    fi

    # System library mappings (libc and friends) are expected to stay
    # unsymbolized, only mention them once.
    local syslib_count
    syslib_count=$(grep -cE "Local symbolization failed" "$errlog" 2>/dev/null || true)

    echo
    for f in "${files[@]}"; do
        printf '%-10s %s\n' "${file_status[$f]:-UNKNOWN}" "$f"
        if [[ -n "${file_note[$f]:-}" ]]; then
            printf '%-10s   %s\n' "" "${file_note[$f]:-}"
        fi
    done
    echo
    if [[ "$syslib_count" -gt 0 ]]; then
        echo "note: some system library mappings stayed unsymbolized, which is expected. Full pprof output: $errlog"
    fi
    echo "Share the *.symbolized.pprof.gz files directly, for example at https://pprof.me."
    echo "The $CACHE_DIR directory is a download cache and is safe to delete."

    exit "$any_bad"
}

if [[ "${MZ_SYMBOLIZE_INNER:-}" = 1 ]]; then
    inner_main
else
    outer_main
fi
