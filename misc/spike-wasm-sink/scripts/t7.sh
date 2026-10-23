#!/usr/bin/env bash
# THROWAWAY T7: retention cap. A supervisor restarts the standalone host; Valkey
# pauses longer than max_lag, so the host must release its hold, and the next
# start must resnapshot instead of resuming into a gap.
S=$(dirname "$(readlink -f "$0")")
W="$S/guest/target/wasm32-wasip2/release/wasm_sink_guest.wasm"
export VALKEY=127.0.0.1:6391
docker exec spike-valkey2 valkey-cli FLUSHALL >/dev/null
rm -f "$S/rid-cap"
(
  end=$((SECONDS + 75))
  while [ $SECONDS -lt $end ]; do
    timeout 45 "$S/host.sh" sb2116565-c51f-4b9d-8070-3aab46e01d48 "$S/rid-cap" --prefix c --wasm "$W" \
      --call-deadline-ms 3000 --max-lag-ms 10000 2>&1 \
      | grep -vE "STATS|^ |^$|Caused" \
      | while IFS= read -r l; do echo "$(date +%s%3N) $l"; done | cut -c1-160
    sleep 1
  done
) &
sup=$!
"$S/churn.sh" 70 0.5 >/dev/null &
sleep 15
echo "$(date +%s%3N) EVENT pause valkey2"; docker pause spike-valkey2 >/dev/null
sleep 25
echo "$(date +%s%3N) EVENT unpause valkey2"; docker unpause spike-valkey2 >/dev/null
wait
sleep 2
python3 -I "$S/verify.py" 127.0.0.1 6391 c
"$S/host.sh" sb2116565-c51f-4b9d-8070-3aab46e01d48 "$S/rid-cap" --release-hold >/dev/null 2>&1
