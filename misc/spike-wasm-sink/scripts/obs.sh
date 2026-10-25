#!/usr/bin/env bash
# THROWAWAY: sample frontiers every INTERVAL seconds for SECONDS.
# Columns: wall_ms, mv_since_lag_ms, mv_upper_lag_ms, sink_upper_lag_ms, valkey_progress_lag_ms, sink_status
# Usage: obs.sh SECONDS [INTERVAL]
S=$(dirname "$(readlink -f "$0")")
end=$((SECONDS + $1))
while [ $SECONDS -lt $end ]; do
  now=$(date +%s%3N)
  row=$(psql -q -h localhost -p 6875 -U materialize -At -F ' ' -c "
    SELECT
      (SELECT read_frontier FROM mz_internal.mz_frontiers f JOIN mz_materialized_views m ON m.id = f.object_id WHERE m.name = 'mv'),
      (SELECT write_frontier FROM mz_internal.mz_frontiers f JOIN mz_materialized_views m ON m.id = f.object_id WHERE m.name = 'mv'),
      (SELECT write_frontier FROM mz_internal.mz_frontiers f JOIN mz_sinks s ON s.id = f.object_id WHERE s.name = 'ws'),
      (SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'ws')" 2>/dev/null)
  read -r since upper sinkup status <<<"$row"
  prog=$("$S/vk.sh" GET '{u514}:progress' 2>/dev/null)
  lag() { [ -n "$1" ] && echo $((now - $1)) || echo NA; }
  echo "OBS $now since_lag=$(lag "$since") upper_lag=$(lag "$upper") sink_lag=$(lag "$sinkup") valkey_lag=$(lag "$prog") status=$status"
  sleep "${2:-2}"
done
