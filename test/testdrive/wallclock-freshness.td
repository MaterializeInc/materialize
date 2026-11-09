# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the wallclock freshness histograms of a table, a load generator source,
# an index on it and a materialized view that refreshes less often than the
# period.
#
# The count of a bucket is the milliseconds of wall-clock time the object spent
# at that staleness, so every full period sums to the period length. The first
# period of an object starts at its creation and is partial. A period is
# written once the controller's clock passes its end, so the test waits for
# two full periods through testdrive's retry feature. Tables and
# storage-managed collections are observed at the 1 s maintenance tick, so the
# test asserts no bucket values for them.

$ set-sql-timeout duration=300s

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_wallclock_freshness_recording = true
ALTER SYSTEM SET wallclock_freshness_period_interval = '1min'

> CREATE CLUSTER freshness_storage SIZE 'scale=1,workers=1'

> CREATE TABLE tbl (a int)

> CREATE SOURCE lg
  IN CLUSTER freshness_storage
  FROM LOAD GENERATOR COUNTER (TICK INTERVAL '100ms')
  WITH (TIMESTAMP INTERVAL '1s')

# The ingestion only runs once it has a table to write to.
> CREATE TABLE lg_tbl FROM SOURCE lg

> CREATE CLUSTER freshness_compute SIZE 'scale=1,workers=1'

> CREATE INDEX lg_idx IN CLUSTER freshness_compute ON lg_tbl (counter)

# Its frontier sits up to an hour ahead of the controller's clock, which must
# read as zero staleness and not as an error.
> CREATE MATERIALIZED VIEW refresh_mv
  IN CLUSTER freshness_compute
  WITH (REFRESH EVERY '1 hour')
  AS SELECT count(*) FROM tbl

# Every period after the first holds exactly one period of wall-clock time.
# Waits until two full periods have closed.
> SELECT o.name, count(*) >= 2, bool_and(p.total = 60000)
  FROM (
    SELECT object_id, period_start, period_end, sum(count) AS total
    FROM mz_internal.mz_wallclock_global_freshness_histogram
    GROUP BY object_id, period_start, period_end
  ) p
  JOIN mz_internal.mz_object_global_ids g ON g.global_id = p.object_id
  JOIN mz_objects o ON o.id = g.id
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl', 'lg_idx', 'refresh_mv')
    AND p.period_start > (
      SELECT min(f.period_start)
      FROM mz_internal.mz_wallclock_global_freshness_histogram f
      WHERE f.object_id = p.object_id
    )
  GROUP BY o.name
  HAVING count(*) >= 2
lg          true  true
lg_idx      true  true
lg_tbl      true  true
refresh_mv  true  true
tbl         true  true

# Every period after the first is one minute long and aligned to the minute.
> SELECT DISTINCT
    o.name,
    h.period_end - h.period_start,
    date_trunc('minute', h.period_start) = h.period_start
  FROM mz_internal.mz_wallclock_global_freshness_histogram h
  JOIN mz_internal.mz_object_global_ids g ON g.global_id = h.object_id
  JOIN mz_objects o ON o.id = g.id
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl', 'lg_idx', 'refresh_mv')
    AND h.period_start > (
      SELECT min(f.period_start)
      FROM mz_internal.mz_wallclock_global_freshness_histogram f
      WHERE f.object_id = h.object_id
    )
lg          00:01:00  true
lg_idx      00:01:00  true
lg_tbl      00:01:00  true
refresh_mv  00:01:00  true
tbl         00:01:00  true

# Rows carry the end-to-end measure only.
> SELECT DISTINCT measure
  FROM mz_internal.mz_wallclock_global_freshness_histogram
end_to_end

# The index runs on a replica, so it has replica histograms that also sum to
# one period.
> SELECT count(*) >= 2, bool_and(p.total = 60000)
  FROM (
    SELECT replica_id, period_start, period_end, sum(count) AS total
    FROM mz_internal.mz_wallclock_replica_freshness_histogram h
    JOIN mz_internal.mz_object_global_ids g ON g.global_id = h.object_id
    JOIN mz_objects o ON o.id = g.id
    WHERE o.name = 'lg_idx'
    GROUP BY replica_id, period_start, period_end
  ) p
  WHERE p.period_end - p.period_start = '1 minute'
true  true

# The load generator source records, through its freshness dependence.
> SELECT count(*) > 0
  FROM mz_internal.mz_wallclock_global_freshness_histogram h
  JOIN mz_internal.mz_object_global_ids g ON g.global_id = h.object_id
  JOIN mz_objects o ON o.id = g.id
  WHERE o.name = 'lg'
true

# The progress view advances.
$ set-from-sql var=lg-covered-until
SELECT p.covered_until::text
FROM mz_internal.mz_wallclock_freshness_progress p
JOIN mz_internal.mz_object_global_ids g ON g.global_id = p.object_id
JOIN mz_objects o ON o.id = g.id
WHERE o.name = 'lg' AND p.replica_id IS NULL

$ set-from-sql var=tbl-covered-until
SELECT p.covered_until::text
FROM mz_internal.mz_wallclock_freshness_progress p
JOIN mz_internal.mz_object_global_ids g ON g.global_id = p.object_id
JOIN mz_objects o ON o.id = g.id
WHERE o.name = 'tbl' AND p.replica_id IS NULL

> SELECT o.name, p.covered_until > '${lg-covered-until}'::timestamptz
  FROM mz_internal.mz_wallclock_freshness_progress p
  JOIN mz_internal.mz_object_global_ids g ON g.global_id = p.object_id
  JOIN mz_objects o ON o.id = g.id
  WHERE o.name = 'lg' AND p.replica_id IS NULL
lg  true

> SELECT o.name, p.covered_until > '${tbl-covered-until}'::timestamptz
  FROM mz_internal.mz_wallclock_freshness_progress p
  JOIN mz_internal.mz_object_global_ids g ON g.global_id = p.object_id
  JOIN mz_objects o ON o.id = g.id
  WHERE o.name = 'tbl' AND p.replica_id IS NULL
tbl  true

> DROP SOURCE lg CASCADE
> DROP MATERIALIZED VIEW refresh_mv
> DROP TABLE tbl
> DROP CLUSTER freshness_storage
> DROP CLUSTER freshness_compute
