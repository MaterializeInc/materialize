#!/usr/bin/env bash
# THROWAWAY T2: pause Valkey for PAUSE seconds during churn; sample frontiers.
S=$(dirname "$(readlink -f "$0")")
PAUSE=${1:-60}
"$S/churn.sh" $((PAUSE + 40)) 0 &
churn=$!
"$S/obs.sh" $((PAUSE + 70)) 5 &
obs=$!
sleep 15
echo "EVENT $(date +%s%3N) pause valkey"
docker pause spike-valkey >/dev/null
sleep "$PAUSE"
echo "EVENT $(date +%s%3N) unpause valkey"
docker unpause spike-valkey >/dev/null
wait $churn $obs
grep -E "wasm sink|u514" "$S/envd.log" | grep -iE "opened|halt|error|deadline" | tail -8 | cut -c1-260
