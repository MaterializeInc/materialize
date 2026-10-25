#!/usr/bin/env bash
# THROWAWAY T3: kill -9 the u1 clusterd N times at random 3-12s intervals during churn.
S=$(dirname "$(readlink -f "$0")")
N=${1:-20}
"$S/churn.sh" $((N * 8 + 20)) 0 &
churn=$!
for k in $(seq 1 "$N"); do
  sleep $((3 + RANDOM % 10))
  pid=$(ps -eo pid,args | grep "[c]lusterd" | grep "cluster-u1-replica-u1" | awk '{print $1}' | head -1)
  if [ -n "$pid" ]; then
    kill -9 "$pid"
    echo "KILL $k $(date +%s%3N) pid=$pid"
  else
    echo "KILL $k no clusterd"
  fi
done
wait $churn
echo "waiting for sink to catch up"
for i in $(seq 1 60); do
  lag=$("$S/obs.sh" 1 1 | grep -o "sink_lag=[0-9NA-]*")
  echo "$lag"
  case "$lag" in sink_lag=[0-9]|sink_lag=[0-9][0-9]|sink_lag=[0-9][0-9][0-9]|sink_lag=[0-9][0-9][0-9][0-9]) break ;; esac
  sleep 5
done
grep -c "wasm sink: opened" "$S/envd.log"
grep "wasm sink: opened" "$S/envd.log" | tail -3 | grep -o "resume=.*" | cut -c1-160
python3 -I "$S/verify.py" 127.0.0.1 6390 "{u514}"
