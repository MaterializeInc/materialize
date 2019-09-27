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
    psql -h materialized -p 6875 sslmode=disable <<<"$1"
}

exec_sql 'CREATE VIEW mz_perf_dependency_frontiers AS
SELECT DISTINCT
       ldd.dataflow as dataflow, lf_source.name as source,
       lf_source.time - lf_df.time as lag_ms
  FROM
       logs_dataflow_dependency ldd
  JOIN logs_frontiers lf_source ON ldd.source = lf_source.name
  JOIN logs_frontiers lf_df ON ldd.dataflow=lf_df.name;'

exec_sql 'CREATE VIEW mz_perf_peek_durations AS
SELECT
    lpd.worker,
    lpd.duration_ns,
    CAST(lpd.count AS float) / total.val * 100 AS percent
FROM
    logs_peek_durations AS lpd,
    (SELECT SUM(count) AS val FROM logs_peek_durations) AS total'
