#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Parks TPC-H lineitem in a buffer that cannot drain, on a fresh replica, and
# records memory over time and buffer pool metrics.
#
# KIND=index: an index over a view whose temporal filter makes every row
# appear a day from now, so the arrangement's merge batcher holds all rows.
# KIND=mv: a materialized view with the same filter, so the MV sink's
# correction buffer holds all rows.
#
# Usage: park-arm.sh NAME KIND SPILL FILE_EXTENTS MEM_GIB [WORKERS]
set -uo pipefail
NAME=$1 KIND=$2 SPILL=$3 FILE=$4 MEM=$5 WORKERS=${6:-8}
OUT=~/envd-runs/$NAME
mkdir -p "$OUT"
SYS="psql -h localhost -p 6877 -U mz_system materialize -qAtX"
USR="psql -h localhost -p 6875 -U materialize materialize -qAtX"
CL="c_${NAME//[-.]/_}"
OBJ="o_${NAME//[-.]/_}"
# A day from now, in milliseconds since the epoch.
FUTURE=$(( ($(date +%s) + 86400) * 1000 ))

$SYS -c "ALTER SYSTEM SET enable_column_paged_batcher_spill = $SPILL"
$SYS -c "ALTER SYSTEM SET enable_compute_correction_v2_spill = $SPILL"
$SYS -c "ALTER SYSTEM SET enable_column_paged_batcher_file_extents = $FILE"
$SYS -c "ALTER SYSTEM SET enable_lgalloc = false"
$SYS -c "ALTER SYSTEM SET enable_columnation_lgalloc = false"
$USR -c "DROP CLUSTER IF EXISTS $CL CASCADE"
$USR -c "DROP VIEW IF EXISTS v_$OBJ CASCADE"
$USR -c "CREATE CLUSTER $CL SIZE 'scale=1,workers=$WORKERS,mem=${MEM}GiB'"
CID=$($USR -c "SELECT id FROM mz_clusters WHERE name = '$CL'")

PID=""
for _ in $(seq 120); do
  PID=$(pgrep -f -- "clusterd.*cluster-$CID-replica" | head -1)
  [ -n "$PID" ] && break
  sleep 1
done
[ -z "$PID" ] && { echo "no clusterd for $CID"; exit 1; }
CG=/sys/fs/cgroup$(cut -d: -f3 "/proc/$PID/cgroup")
ADDR=$(tr '\0' '\n' < "/proc/$PID/cmdline" | sed -n 's/^--internal-http-listen-addr=//p')
echo "cluster=$CL id=$CID pid=$PID kind=$KIND spill=$SPILL file=$FILE mem=${MEM}GiB future=$FUTURE" | tee "$OUT/info"
sleep 5

(
  while kill -0 "$PID" 2>/dev/null; do
    echo "$(date +%s) cur=$(cat "$CG/memory.current" 2>/dev/null) rss=$(awk '/VmRSS/ {print $2}' "/proc/$PID/status") file=$(awk '$1=="file" {print $2}' "$CG/memory.stat" 2>/dev/null) disk=$(df -m --output=used /scratch | tail -1)"
    sleep 1
  done
) > "$OUT/samples" &
SAMPLER=$!

START=$(date +%s.%N)
if [ "$KIND" = index ]; then
  $USR -c "CREATE VIEW v_$OBJ AS SELECT * FROM (SELECT * FROM lineitem UNION ALL SELECT * FROM li_empty) WHERE mz_now() >= ($FUTURE + l_orderkey % 2)::mz_timestamp"
  $USR -c "CREATE INDEX $OBJ IN CLUSTER $CL ON v_$OBJ (l_orderkey)"
else
  $USR -c "CREATE MATERIALIZED VIEW $OBJ IN CLUSTER $CL AS SELECT * FROM (SELECT * FROM lineitem UNION ALL SELECT * FROM li_empty) WHERE mz_now() >= ($FUTURE + l_orderkey % 2)::mz_timestamp"
fi
HYDRATED=""
for _ in $(seq 3600); do
  if ! kill -0 "$PID" 2>/dev/null; then HYDRATED="process exited (OOM?)"; break; fi
  H=$($USR -c "SELECT bool_and(h.hydrated) FROM mz_internal.mz_hydration_statuses h JOIN mz_objects o ON h.object_id = o.id WHERE o.name = '$OBJ'")
  [ "$H" = "t" ] && { HYDRATED=yes; break; }
  sleep 1
done
END=$(date +%s.%N)
echo "hydrated=$HYDRATED wall=$(echo "$END - $START" | bc)" | tee "$OUT/result"

# The rows never drain, so memory after a while is the parked footprint.
for wait in 30 90 180; do
  sleep $(( wait == 30 ? 30 : wait == 90 ? 60 : 90 ))
  kill -0 "$PID" 2>/dev/null || { echo "process exited before t+${wait}s" | tee -a "$OUT/result"; break; }
  echo "t+${wait}s VmRSS MiB=$(( $(awk '/VmRSS/ {print $2}' "/proc/$PID/status") / 1024 )) memory.current MiB=$(( $(cat "$CG/memory.current") / 1048576 )) scratch used MiB=$(df -m --output=used /scratch | tail -1) cgroup swap MiB=$(( $(cat "$CG/memory.swap.current" 2>/dev/null || echo 0) / 1048576 ))" | tee -a "$OUT/result"
done

curl -s --unix-socket "$ADDR" "http://localhost/metrics" > "$OUT/metrics" 2>/dev/null
grep -E '^mz_column_pool' "$OUT/metrics" > "$OUT/pool_metrics"
echo "memory.peak MiB=$(( $(cat "$CG/memory.peak" 2>/dev/null || echo 0) / 1048576 )) oom_kill=$(awk '$1=="oom_kill" {print $2}' "$CG/memory.events" 2>/dev/null)" | tee -a "$OUT/result"
kill "$SAMPLER" 2>/dev/null
awk '{for(i=2;i<=NF;i++){split($i,kv,"="); if(kv[1]=="rss" && kv[2]>m) m=kv[2]}} END {print "max VmRSS MiB=" int(m/1024)}' "$OUT/samples" | tee -a "$OUT/result"
grep -E 'mz_column_pool_(backend\{.*\} 1|resident_bytes|extent_resident_bytes|extent_file_(writes_total|reads_total|bytes|full_total|write_errors_total)|extent_demotions_elided_total|extent_pageouts_total|evictions_compress_total|live_chunks)' "$OUT/pool_metrics" | tee -a "$OUT/result"
$USR -c "DROP CLUSTER $CL CASCADE"
$USR -c "DROP VIEW IF EXISTS v_$OBJ CASCADE"
