#!/usr/bin/env bash
# THROWAWAY T4: hold durability across an envd restart. Valkey is paused so
# the sink cannot progress; envd restarts; the MV since must stay pinned at
# the sink's progress, and the sink must resume without a gap after unpause.
S=$(dirname "$(readlink -f "$0")")
obs1() { "$S/obs.sh" 1 1 | grep OBS; }
"$S/churn.sh" 15 0 >/dev/null &
sleep 5
docker pause spike-valkey >/dev/null
echo "EVENT $(date +%s%3N) paused valkey"
wait
obs1
envd=$(ps -eo pid,args | grep "wasm-sink-spike/[m]zbuild/bin/environmentd" | awk '{print $1}' | head -1)
echo "EVENT $(date +%s%3N) kill envd pid=$envd"
kill "$envd"
for i in $(seq 1 60); do ss -ltn | grep -qE ':(6875|6879)\b' || break; sleep 1; done
setsid nohup "$S/envd.sh" </dev/null >/dev/null 2>&1 &
for i in $(seq 1 120); do psql -h localhost -p 6875 -U materialize -Atc "select 1" >/dev/null 2>&1 && break; sleep 1; done
echo "EVENT $(date +%s%3N) envd up"
"$S/churn.sh" 10 0 >/dev/null
sleep 5
obs1
"$S/vk.sh" GET '{u514}:progress' 2>/dev/null || echo "(valkey paused)"
docker unpause spike-valkey >/dev/null
echo "EVENT $(date +%s%3N) unpaused valkey"
for i in $(seq 1 24); do sleep 5; obs1; done
grep "wasm sink: opened" "$S/envd.log" | tail -2 | grep -o "resume=.*" | cut -c1-160
grep -E "gap|cannot serve" "$S/envd.log" | tail -3 | cut -c1-250
python3 -I "$S/verify.py" 127.0.0.1 6390 "{u514}"
