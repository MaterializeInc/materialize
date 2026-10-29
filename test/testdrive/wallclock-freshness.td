# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the wallclock freshness histograms of storage-owned collections.
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

# Every period after the first holds exactly one period of arrivals. Waits
# until two full periods have closed.
> SELECT o.name, count(*) >= 2, bool_and(p.arrivals = 60000)
  FROM (
    SELECT object_id, period_start, period_end, sum(count) AS arrivals
    FROM mz_internal.mz_wallclock_global_freshness_histogram
    WHERE measure = 'end_to_end'
    GROUP BY object_id, period_start, period_end
  ) p
  JOIN mz_objects o ON o.id = p.object_id
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl')
    AND p.period_start > (
      SELECT min(f.period_start)
      FROM mz_internal.mz_wallclock_global_freshness_histogram f
      WHERE f.object_id = p.object_id AND f.measure = 'end_to_end'
    )
  GROUP BY o.name
  HAVING count(*) >= 2
lg      true  true
lg_tbl  true  true
tbl     true  true

# Every period after the first, which starts at the object's creation, is one
# minute long and aligned to the minute.
> SELECT DISTINCT
    o.name,
    h.period_end - h.period_start,
    date_trunc('minute', h.period_start) = h.period_start
  FROM mz_internal.mz_wallclock_global_freshness_histogram h
  JOIN mz_objects o ON o.id = h.object_id
  WHERE o.name IN ('tbl', 'lg', 'lg_tbl')
    AND h.period_start > (
      SELECT min(f.period_start)
      FROM mz_internal.mz_wallclock_global_freshness_histogram f
      WHERE f.object_id = h.object_id
    )
lg      00:01:00  true
lg_tbl  00:01:00  true
tbl     00:01:00  true

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
