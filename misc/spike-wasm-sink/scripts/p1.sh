#!/usr/bin/env bash
# THROWAWAY: one probe with timestamped output. Usage: p1.sh PROBE [host args]
S=$(dirname "$(readlink -f "$0")")
P=$1; shift
timeout 90 "$S/host.sh" sb2116565-c51f-4b9d-8070-3aab46e01d48 "$S/rid-probe" --prefix probe \
  --wasm "$S/guest/target/wasm32-wasip2/release/wasm_sink_guest.wasm" \
  --guest-extra "{\"probe\":\"$P\"}" "$@" 2>&1 | while IFS= read -r l; do echo "$(date +%s%3N) $l"; done | cut -c1-200
echo "exit=${PIPESTATUS[0]}"
