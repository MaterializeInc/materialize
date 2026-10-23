#!/usr/bin/env bash
# THROWAWAY: release every standalone-host critical hold on SHARD.
S=$(dirname "$(readlink -f "$0")")
for rid in "$S"/rid-*; do
  echo "$rid: $("$S/host.sh" "$1" "$rid" --release-hold 2>&1 | tail -1)"
done
