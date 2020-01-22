-- Copyright 2019 Materialize, Inc. All rights reserved.
--
-- This file is part of Materialize. Materialize may not be used or
-- distributed without the express permission of Materialize, Inc.

-- Stores all addresses that only have one slot (0) in mz_dataflow_operator_addresses
-- The resulting addresses are either channels or dataflows
CREATE VIEW mz_addresses_with_unit_length as
SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
FROM
    mz_dataflow_operator_addresses
GROUP BY
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
HAVING count(*) = 1;

-- Maintains a list of the current dataflow operator ids, and their corresponding
-- operator names and local ids (per worker)
CREATE VIEW mz_dataflow_names AS
SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker,
    mz_dataflow_operator_addresses.value as local_id,
    mz_dataflow_operators.name
FROM
    mz_dataflow_operator_addresses,
    mz_dataflow_operators,
    mz_addresses_with_unit_length
WHERE
    mz_dataflow_operator_addresses.id = mz_dataflow_operators.id AND
    mz_dataflow_operator_addresses.worker = mz_dataflow_operators.worker AND
    mz_dataflow_operator_addresses.id = mz_addresses_with_unit_length.id AND
    mz_dataflow_operator_addresses.worker = mz_addresses_with_unit_length.worker AND
    mz_dataflow_operator_addresses.slot = 0;

-- Maintains a list of all operators bound to a dataflow and their corresponding names
-- and dataflow names and ids (per worker)
--
-- Keeping this as a separate view instead of rolling it into
-- mz_records_per_dataflow_operator to simplify logic
CREATE VIEW mz_dataflow_operator_dataflows AS
SELECT
    mz_dataflow_operators.id,
    mz_dataflow_operators.name,
    mz_dataflow_operators.worker,
    mz_dataflow_names.id as dataflow_id,
    mz_dataflow_names.name as dataflow_name
FROM
    mz_dataflow_operators,
    mz_dataflow_operator_addresses,
    mz_dataflow_names
WHERE
    mz_dataflow_operators.id = mz_dataflow_operator_addresses.id AND
    mz_dataflow_operators.worker = mz_dataflow_operator_addresses.worker AND
    mz_dataflow_operator_addresses.slot = 0 AND
    mz_dataflow_names.local_id = mz_dataflow_operator_addresses.value AND
    mz_dataflow_names.worker = mz_dataflow_operator_addresses.worker;

-- Maintains the number of records used by each operator in a dataflow (per worker)
-- Operators not using any records are not shown
CREATE VIEW mz_records_per_dataflow_operator AS
SELECT
    mz_dataflow_operator_dataflows.id,
    mz_dataflow_operator_dataflows.name,
    mz_dataflow_operator_dataflows.worker,
    mz_dataflow_operator_dataflows.dataflow_id,
    mz_arrangement_sizes.records
FROM
    mz_arrangement_sizes,
    mz_dataflow_operator_dataflows
WHERE
    mz_dataflow_operator_dataflows.id = mz_arrangement_sizes.operator AND
    mz_dataflow_operator_dataflows.worker = mz_arrangement_sizes.worker;

-- Maintains the number of records used by each dataflow (per worker)
CREATE VIEW mz_records_per_dataflow AS
SELECT
    mz_records_per_dataflow_operator.dataflow_id as id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker,
    SUM(mz_records_per_dataflow_operator.records) as records
FROM
    mz_records_per_dataflow_operator,
    mz_dataflow_names
WHERE
    mz_records_per_dataflow_operator.dataflow_id = mz_dataflow_names.id AND
    mz_records_per_dataflow_operator.worker = mz_dataflow_names.worker
GROUP BY
    mz_records_per_dataflow_operator.dataflow_id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker;

-- Maintains the number of records used by each dataflow (across all workers)
CREATE VIEW mz_records_per_dataflow_global AS
SELECT
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name,
    SUM(mz_records_per_dataflow.records) as records
FROM
    mz_records_per_dataflow
GROUP BY
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name;

--- Performance-rlated tables, used by prometheus

--
CREATE VIEW mz_perf_dependency_frontiers AS
SELECT DISTINCT
     coalesce(mcn.name, index_deps.dataflow) as dataflow,
     coalesce(mcn_source.name, frontier_source.global_id) as source,
     frontier_source.time - frontier_df.time as lag_ms
FROM
     mz_materialization_dependencies index_deps
JOIN mz_materialization_frontiers frontier_source ON index_deps.source = frontier_source.global_id
JOIN mz_materialization_frontiers frontier_df ON index_deps.dataflow = frontier_df.global_id
LEFT JOIN mz_catalog_names mcn ON mcn.global_id = index_deps.dataflow
LEFT JOIN mz_catalog_names mcn_source ON mcn_source.global_id = frontier_source.global_id;

-- operator operator is due to issue #1217
CREATE VIEW mz_perf_arrangement_records AS
 SELECT mas.worker, name, records, operator operator
 FROM mz_arrangement_sizes mas
 JOIN mz_dataflow_operators mdo ON mdo.id = mas.operator;

-- There are three steps required for a prometheus histogram from the mz_peek_durations
-- logs:
--
--   1. Create some values that all represent everything in them and below (_core)
--   2. Find the max value and alias that with +Inf (_bucket)
--   3. calculate a couple aggregates (_aggregates)
CREATE VIEW mz_perf_peek_durations_core AS
SELECT
    d_upper.worker,
    CAST(d_upper.duration_ns AS TEXT) AS le,
    sum(d_summed.count) AS count
FROM
    mz_peek_durations AS d_upper,
    mz_peek_durations AS d_summed
WHERE
    d_upper.worker = d_summed.worker AND
    d_upper.duration_ns >= d_summed.duration_ns
GROUP BY d_upper.worker, d_upper.duration_ns;

CREATE VIEW mz_perf_peek_durations_bucket AS (
    SELECT * FROM mz_perf_peek_durations_core
) UNION (
    SELECT worker, '+Inf', max(count) AS count FROM mz_perf_peek_durations_core
    GROUP BY worker
);

CREATE VIEW mz_perf_peek_durations_aggregates AS
SELECT worker, sum(duration_ns * count) AS sum, sum(count) AS count
FROM mz_peek_durations lpd
GROUP BY worker;
