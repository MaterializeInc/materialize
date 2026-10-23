#!/usr/bin/env bash
# THROWAWAY T6: sandbox probes through the standalone host.
S=$(dirname "$(readlink -f "$0")")
SHARD=sb2116565-c51f-4b9d-8070-3aab46e01d48
W="$S/guest/target/wasm32-wasip2/release/wasm_sink_guest.wasm"
probe() {
  local name=$1; shift
  local t0; t0=$(date +%s%3N)
  local out; out=$(timeout 60 "$S/host.sh" "$SHARD" "$S/rid-probe" --prefix probe --wasm "$W" "$@" 2>&1 | grep -E "sandbox:|sink failed|trap|probe" | head -3 | cut -c1-220)
  echo "PROBE $name elapsed_ms=$(( $(date +%s%3N) - t0 ))"
  echo "$out" | sed 's/^/    /'
}
probe connect-other --guest-extra '{"probe":"connect:127.0.0.1:5440"}'
probe connect-dns --guest-extra '{"probe":"connect:localhost:6390"}'
probe read-file --guest-extra '{"probe":"read:/etc/passwd"}'
probe alloc-128m --guest-extra '{"probe":"alloc:134217728"}'
probe alloc-32m --guest-extra '{"probe":"alloc:33554432"}'
probe spin-epoch-2s --call-deadline-ms 2000 --guest-extra '{"probe":"spin"}'
probe spin-fuel --fuel-base 100000000 --guest-extra '{"probe":"spin"}'
"$S/host.sh" "$SHARD" "$S/rid-probe" --release-hold >/dev/null 2>&1
