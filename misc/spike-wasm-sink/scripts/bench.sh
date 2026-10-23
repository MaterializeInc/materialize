#!/usr/bin/env bash
# THROWAWAY: snapshot benchmark arms against a dedicated Valkey (6391).
# Usage: bench.sh SHARD COLUMNS REPS
S=$(dirname "$(readlink -f "$0")")
SHARD=$1; REPS=${2:-3}
W="$S/guest/target/wasm32-wasip2/release/wasm_sink_guest.wasm"
export VALKEY=127.0.0.1:6391
for rep in $(seq 1 "$REPS"); do
  for arm in null native wasm wasmfuel; do
    docker exec spike-valkey2 valkey-cli FLUSHALL >/dev/null
    case $arm in
      null) extra=(--null-guest) ;;
      native) extra=() ;;
      wasm) extra=(--wasm "$W") ;;
      wasmfuel) extra=(--wasm "$W" --fuel-base 1000000000000) ;;
    esac
    out=$("$S/host.sh" "$SHARD" "$S/rid-bench" --prefix b --exit-after-snapshot "${extra[@]}" 2>&1)
    secs=$(echo "$out" | grep -o "SNAPSHOT_DONE.*" | grep -o "secs=[0-9.]*")
    stats=$(echo "$out" | grep STATS | tail -1 | grep -oE "cpu_s=[0-9.]+|updates=[0-9]+|cpu_us_per_upd=[0-9.]+|rss_mib=[0-9.]+" | tr '\n' ' ')
    echo "BENCH rep=$rep arm=$arm $secs $stats"
  done
done
"$S/host.sh" "$SHARD" "$S/rid-bench" --release-hold >/dev/null 2>&1
