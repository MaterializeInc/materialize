#!/usr/bin/env bash
# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# Create the views required by the sql metric exporter
#
# This can be run via docker compose, or locally on your computer. Inside docker compose
# it looks like:
#
#    docker-compose run prometheus_sql_exporter_setup
#
# Development: All perf views should be prefixed with `mz_perf_*`

exec_sql() {
    echo -n "$(date "+%Y-%m-%d %H:%M:%S") | $1: "
    psql -h materialized -p 6875 sslmode=disable <<<"$2"
}

exec_sql mz_perf_dependency_frontiers 'CREATE VIEW mz_perf_dependency_frontiers AS
SELECT DISTINCT
       ldd.dataflow as dataflow, lf_source.name as source,
       lf_source.time - lf_df.time as lag_ms
  FROM
       mz_view_dependencies ldd
  JOIN mz_view_frontiers lf_source ON ldd.source = lf_source.name
  JOIN mz_view_frontiers lf_df ON ldd.dataflow=lf_df.name;'

# operator operator is due to issue #1217
exec_sql mz_perf_arrangement_records 'CREATE VIEW mz_perf_arrangement_records AS
 SELECT mas.worker, name, records, operator operator
 FROM mz_arrangement_sizes mas
 JOIN mz_dataflow_operators mdo ON mdo.id = mas.operator'

# There are three steps required for a prometheus histogram from the mz_peek_durations
# logs:
#
#   1. Create some values that all represent everything in them and below (_core)
#   2. Find the max value and alias that with +Inf (_bucket)
#   3. calculate a couple aggregates (_aggregates)
exec_sql mz_perf_peek_durations_core 'CREATE VIEW mz_perf_peek_durations_core AS
SELECT
    d_upper.worker,
    CAST(d_upper.duration_ns AS TEXT) AS le,
    sum(d_summed.count) AS count
FROM
    mz_peek_durations AS d_upper,
    mz_peek_durations AS d_summed
WHERE
    d_upper.worker = d_summed.worker
  AND
    d_upper.duration_ns >= d_summed.duration_ns
GROUP BY d_upper.worker, d_upper.duration_ns'

exec_sql mz_perf_peek_durations_bucket "CREATE VIEW mz_perf_peek_durations_bucket AS
(
    SELECT * FROM mz_perf_peek_durations_core
) UNION (
    SELECT worker, '+Inf', max(count) AS count FROM mz_perf_peek_durations_core
    GROUP BY worker
)"
exec_sql mz_perf_peek_durations_aggregates 'CREATE VIEW mz_perf_peek_durations_aggregates AS
SELECT worker, sum(duration_ns * count) AS sum, sum(count) AS count
FROM mz_peek_durations lpd
GROUP BY worker'
