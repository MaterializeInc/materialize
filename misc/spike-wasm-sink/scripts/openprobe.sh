#!/usr/bin/env bash
# THROWAWAY: time the host's persist open steps with a given reader id file.
S=$(dirname "$(readlink -f "$0")")
export VALKEY=127.0.0.1:6391
timeout "${2:-120}" "$S/host.sh" sb2116565-c51f-4b9d-8070-3aab46e01d48 "$S/$1" --prefix c --null-guest --exit-after-snapshot 2>&1 \
  | while IFS= read -r l; do echo "$(date +%s%3N) $l"; done | cut -c1-200 | grep -v STATS
echo "exit=${PIPESTATUS[0]}"
