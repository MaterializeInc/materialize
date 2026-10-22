#!/usr/bin/env bash
# THROWAWAY: steady-state throughput. Standalone host (ARM: native|wasm) listens
# on mv while UPDATEs touch 100k rows each (200k diffs).
S=$(dirname "$(readlink -f "$0")")
ARM=${1:-wasm}; DUR=${2:-60}
W="$S/guest/target/wasm32-wasip2/release/wasm_sink_guest.wasm"
export VALKEY=127.0.0.1:6391
docker exec spike-valkey2 valkey-cli FLUSHALL >/dev/null
extra=(); [ "$ARM" = wasm ] && extra=(--wasm "$W")
timeout $((DUR + 40)) "$S/host.sh" sb2116565-c51f-4b9d-8070-3aab46e01d48 "$S/rid-steady-$ARM" --prefix s "${extra[@]}" > "$S/steady-$ARM.log" 2>&1 &
host=$!
# Wait for the snapshot to finish before churning.
for i in $(seq 1 60); do grep -q SNAPSHOT_DONE "$S/steady-$ARM.log" && break; sleep 1; done
grep STATS "$S/steady-$ARM.log" | tail -1 | grep -oE "cpu_s=[0-9.]+|updates=[0-9]+" | tr '\n' ' '; echo "<- after snapshot"
end=$((SECONDS + DUR)); n=0; t0=$(date +%s%3N)
while [ $SECONDS -lt $end ]; do
  psql -q -h localhost -p 6875 -U materialize -c "UPDATE t SET n = n + 1 WHERE id % 10 = $((n % 10))" >/dev/null 2>&1
  n=$((n + 1))
done
echo "churn statements=$n diffs=$((n * 200000)) secs=$(( ($(date +%s%3N) - t0) / 1000 ))"
sleep 20
grep STATS "$S/steady-$ARM.log" | tail -8 | grep -oE "phase=[a-z]+|wall_s=[0-9.]+|cpu_s=[0-9.]+|updates=[0-9]+|lag_ms=[0-9]+|rss_mib=[0-9.]+" | paste -sd' ' | fold -w 400
kill $host 2>/dev/null; wait $host 2>/dev/null
"$S/host.sh" sb2116565-c51f-4b9d-8070-3aab46e01d48 "$S/rid-steady-$ARM" --release-hold >/dev/null 2>&1
