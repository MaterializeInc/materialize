# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the wallclock freshness histograms of storage-owned collections and of
# an index that reads one of them.
#
# Every virtual arrival is counted exactly once, so a full one-minute period
# holds 60000 arrivals of one millisecond. The first period of an object starts
# at its creation and is partial. Periods close once the frontier passes their
# end, so the test relies on testdrive's retry feature.

$ set-sql-timeout duration=300s

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_wallclock_freshness_recording = true
ALTER SYSTEM SET wallclock_freshness_period_interval = '1min'
ALTER SYSTEM SET wallclock_freshness_virtual_arrival_interval = '1ms'

> CREATE CLUSTER freshness_storage SIZE 'scale=1,workers=1'

> CREATE TABLE tbl (a int)

> CREATE SOURCE lg
  IN CLUSTER freshness_storage
  FROM LOAD GENERATOR COUNTER (TICK INTERVAL '100ms')
  WITH (TIMESTAMP INTERVAL '1s')

# The ingestion only runs once it has a table to write to.
> CREATE TABLE lg_tbl FROM SOURCE lg

> CREATE CLUSTER freshness_compute SIZE 'scale=1,workers=1'

> CREATE VIEW lg_view AS SELECT counter FROM lg_tbl

> CREATE INDEX lg_idx IN CLUSTER freshness_compute ON lg_view (counter)

# An index with two inputs, the only object here that records skew.
> CREATE VIEW lg_join AS SELECT l.counter FROM lg_tbl l, tbl t

> CREATE INDEX lg_join_idx IN CLUSTER freshness_compute ON lg_join (counter)

# Every period after the first holds exactly one period of arrivals, in both
# measures. Waits until two full periods have closed.
> SELECT o.name, p.measure, count(*) >= 2, bool_and(p.arrivals = 60000)
  FROM (
    SELECT object_id, measure, period_start, period_end, sum(count) AS arrivals
    FROM mz_internal.mz_wallclock_global_freshness_histogram
    GROUP BY object_id, measure, period_start, period_end
  ) p
  JOIN mz_objects o ON o.id = p.object_id
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl', 'lg_idx')
    AND p.period_start > (
      SELECT min(f.period_start)
      FROM mz_internal.mz_wallclock_global_freshness_histogram f
      WHERE f.object_id = p.object_id AND f.measure = p.measure
    )
  GROUP BY o.name, p.measure
  HAVING count(*) >= 2
lg      end_to_end  true  true
lg      local       true  true
lg_idx  end_to_end  true  true
lg_idx  local       true  true
lg_tbl  end_to_end  true  true
lg_tbl  local       true  true
tbl     end_to_end  true  true
tbl     local       true  true

# Every period after the first, which starts at the object's creation, is one
# minute long and aligned to the minute.
> SELECT DISTINCT
    o.name,
    h.period_end - h.period_start,
    date_trunc('minute', h.period_start) = h.period_start
  FROM mz_internal.mz_wallclock_global_freshness_histogram h
  JOIN mz_objects o ON o.id = h.object_id
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl', 'lg_idx')
    AND h.period_start > (
      SELECT min(f.period_start)
      FROM mz_internal.mz_wallclock_global_freshness_histogram f
      WHERE f.object_id = h.object_id
    )
lg      00:01:00  true
lg_idx  00:01:00  true
lg_tbl  00:01:00  true
tbl     00:01:00  true

# Per period, both measures count the same arrivals of each object.
> SELECT o.name, bool_and(e.arrivals IS NOT DISTINCT FROM l.arrivals)
  FROM (
    SELECT object_id, period_start, sum(count) AS arrivals
    FROM mz_internal.mz_wallclock_global_freshness_histogram
    WHERE measure = 'end_to_end'
    GROUP BY object_id, period_start
  ) e
  FULL OUTER JOIN (
    SELECT object_id, period_start, sum(count) AS arrivals
    FROM mz_internal.mz_wallclock_global_freshness_histogram
    WHERE measure = 'local'
    GROUP BY object_id, period_start
  ) l ON e.object_id = l.object_id AND e.period_start = l.period_start
  JOIN mz_objects o ON o.id = coalesce(e.object_id, l.object_id)
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl', 'lg_idx')
  GROUP BY o.name
lg      true
lg_idx  true
lg_tbl  true
tbl     true

# Only the object with two inputs records skew, and per period it counts the
# same arrivals as the end-to-end latency.
> SELECT o.name, bool_and(e.arrivals = k.arrivals)
  FROM (
    SELECT object_id, period_start, sum(count) AS arrivals
    FROM mz_internal.mz_wallclock_global_freshness_histogram
    WHERE measure = 'end_to_end'
    GROUP BY object_id, period_start
  ) e
  JOIN (
    SELECT object_id, period_start, sum(count) AS arrivals
    FROM mz_internal.mz_wallclock_global_freshness_histogram
    WHERE measure = 'skew'
    GROUP BY object_id, period_start
  ) k ON e.object_id = k.object_id AND e.period_start = k.period_start
  JOIN mz_objects o ON o.id = e.object_id
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl', 'lg_idx', 'lg_join_idx')
  GROUP BY o.name
lg_join_idx  true

# Storage-owned collections have no inputs, so their local delay is their
# end-to-end latency, bucket by bucket.
> SELECT o.name, d.measure, d.period_start, d.bucket, d.count
  FROM (
    (
      SELECT object_id, 'end_to_end' AS measure, period_start, bucket, count
      FROM mz_internal.mz_wallclock_global_freshness_histogram
      WHERE measure = 'end_to_end'
      EXCEPT ALL
      SELECT object_id, 'end_to_end', period_start, bucket, count
      FROM mz_internal.mz_wallclock_global_freshness_histogram
      WHERE measure = 'local'
    )
    UNION ALL
    (
      SELECT object_id, 'local', period_start, bucket, count
      FROM mz_internal.mz_wallclock_global_freshness_histogram
      WHERE measure = 'local'
      EXCEPT ALL
      SELECT object_id, 'local', period_start, bucket, count
      FROM mz_internal.mz_wallclock_global_freshness_histogram
      WHERE measure = 'end_to_end'
    )
  ) d
  JOIN mz_objects o ON o.id = d.object_id
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl')

# The index only adds the time it takes to reflect its input, well below the
# 1 s tick that dominates its end-to-end latency. The first period includes
# hydration and is excluded.
> SELECT bool_and(pow(2, h.bucket::float8 / h.buckets_per_octave::float8) < 1000)
  FROM mz_internal.mz_wallclock_global_freshness_histogram h
  JOIN mz_objects o ON o.id = h.object_id
  WHERE o.name = 'lg_idx' AND h.measure = 'local'
    AND h.period_start > (
      SELECT min(f.period_start)
      FROM mz_internal.mz_wallclock_global_freshness_histogram f
      WHERE f.object_id = h.object_id AND f.measure = 'local'
    )
true

# Its end-to-end latency includes waiting for the tick.
> SELECT bool_or(pow(2, h.bucket::float8 / h.buckets_per_octave::float8) >= 1000)
  FROM mz_internal.mz_wallclock_global_freshness_histogram h
  JOIN mz_objects o ON o.id = h.object_id
  WHERE o.name = 'lg_idx' AND h.measure = 'end_to_end'
true

# The progress view advances.
$ set-from-sql var=lg-covered-until
SELECT p.covered_until::text
FROM mz_internal.mz_wallclock_freshness_progress p
JOIN mz_objects o ON o.id = p.object_id
WHERE o.name = 'lg' AND p.replica_id IS NULL

$ set-from-sql var=tbl-covered-until
SELECT p.covered_until::text
FROM mz_internal.mz_wallclock_freshness_progress p
JOIN mz_objects o ON o.id = p.object_id
WHERE o.name = 'tbl' AND p.replica_id IS NULL

> SELECT o.name, p.covered_until > '${lg-covered-until}'::timestamptz
  FROM mz_internal.mz_wallclock_freshness_progress p
  JOIN mz_objects o ON o.id = p.object_id
  WHERE o.name = 'lg' AND p.replica_id IS NULL
lg  true

> SELECT o.name, p.covered_until > '${tbl-covered-until}'::timestamptz
  FROM mz_internal.mz_wallclock_freshness_progress p
  JOIN mz_objects o ON o.id = p.object_id
  WHERE o.name = 'tbl' AND p.replica_id IS NULL
tbl  true

> DROP SOURCE lg CASCADE
> DROP TABLE tbl
> DROP CLUSTER freshness_storage
> DROP CLUSTER freshness_compute
