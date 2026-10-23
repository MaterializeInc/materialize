#!/usr/bin/env bash
# THROWAWAY: fresh snapshot of a shard into Valkey, then verify.
# Usage: snap.sh SHARD PREFIX [extra host args...]
set -uo pipefail
S=$(dirname "$(readlink -f "$0")")
SHARD=$1; PREFIX=$2; shift 2
"$S/vk.sh" DEL "$PREFIX:progress" >/dev/null
timeout 900 "$S/host.sh" "$SHARD" "$S/rid-$PREFIX" --prefix "$PREFIX" --exit-after-snapshot "$@" 2>&1 | grep -v "^guest: opened" | tail -6
python3 -I "$S/verify.py" 127.0.0.1 6390 "$PREFIX" 2>&1 | tail -3
