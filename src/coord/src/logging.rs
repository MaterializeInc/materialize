// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dataflow_types::logging::{DifferentialLog, LogVariant, MaterializedLog, TimelyLog};
use expr::GlobalId;

pub struct LogSource {
    pub variant: LogVariant,
    pub name: &'static str,
    pub id: GlobalId,
    pub index_id: GlobalId,
}

pub const LOG_SOURCES: &[LogSource] = &[
    LogSource {
        name: "mz_dataflow_operators",
        variant: LogVariant::Timely(TimelyLog::Operates),
        id: GlobalId::System(1),
        index_id: GlobalId::System(2),
    },
    LogSource {
        name: "mz_dataflow_operator_addresses",
        variant: LogVariant::Timely(TimelyLog::Addresses),
        id: GlobalId::System(3),
        index_id: GlobalId::System(4),
    },
    LogSource {
        name: "mz_dataflow_channels",
        variant: LogVariant::Timely(TimelyLog::Channels),
        id: GlobalId::System(5),
        index_id: GlobalId::System(6),
    },
    LogSource {
        name: "mz_scheduling_elapsed",
        variant: LogVariant::Timely(TimelyLog::Elapsed),
        id: GlobalId::System(7),
        index_id: GlobalId::System(8),
    },
    LogSource {
        name: "mz_scheduling_histogram",
        variant: LogVariant::Timely(TimelyLog::Histogram),
        id: GlobalId::System(9),
        index_id: GlobalId::System(10),
    },
    LogSource {
        name: "mz_scheduling_parks",
        variant: LogVariant::Timely(TimelyLog::Parks),
        id: GlobalId::System(11),
        index_id: GlobalId::System(12),
    },
    LogSource {
        name: "mz_arrangement_sizes",
        variant: LogVariant::Differential(DifferentialLog::Arrangement),
        id: GlobalId::System(13),
        index_id: GlobalId::System(14),
    },
    LogSource {
        name: "mz_arrangement_sharing",
        variant: LogVariant::Differential(DifferentialLog::Sharing),
        id: GlobalId::System(15),
        index_id: GlobalId::System(16),
    },
    LogSource {
        name: "mz_materializations",
        variant: LogVariant::Materialized(MaterializedLog::DataflowCurrent),
        id: GlobalId::System(17),
        index_id: GlobalId::System(18),
    },
    LogSource {
        name: "mz_materialization_dependencies",
        variant: LogVariant::Materialized(MaterializedLog::DataflowDependency),
        id: GlobalId::System(19),
        index_id: GlobalId::System(20),
    },
    LogSource {
        name: "mz_worker_materialization_frontiers",
        variant: LogVariant::Materialized(MaterializedLog::FrontierCurrent),
        id: GlobalId::System(21),
        index_id: GlobalId::System(22),
    },
    LogSource {
        name: "mz_peek_active",
        variant: LogVariant::Materialized(MaterializedLog::PeekCurrent),
        id: GlobalId::System(23),
        index_id: GlobalId::System(24),
    },
    LogSource {
        name: "mz_peek_durations",
        variant: LogVariant::Materialized(MaterializedLog::PeekDuration),
        id: GlobalId::System(25),
        index_id: GlobalId::System(26),
    },
];

pub struct LogView {
    pub name: &'static str,
    pub sql: &'static str,
    pub id: GlobalId,
}

// Stores all addresses that only have one slot (0) in
// mz_dataflow_operator_addresses. The resulting addresses are either channels
// or dataflows.
const VIEW_ADDRESSES_WITH_UNIT_LENGTH: LogView = LogView {
    name: "mz_addresses_with_unit_length",
    sql: "CREATE VIEW mz_addresses_with_unit_length AS SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
FROM
    mz_catalog.mz_dataflow_operator_addresses
GROUP BY
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
HAVING count(*) = 1",
    id: GlobalId::System(33),
};

/// Maintains a list of the current dataflow operator ids, and their
/// corresponding operator names and local ids (per worker).
const VIEW_DATAFLOW_NAMES: LogView = LogView {
    name: "mz_dataflow_names",
    sql: "CREATE VIEW mz_dataflow_names AS SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker,
    mz_dataflow_operator_addresses.value as local_id,
    mz_dataflow_operators.name
FROM
    mz_catalog.mz_dataflow_operator_addresses,
    mz_catalog.mz_dataflow_operators,
    mz_catalog.mz_addresses_with_unit_length
WHERE
    mz_dataflow_operator_addresses.id = mz_dataflow_operators.id AND
    mz_dataflow_operator_addresses.worker = mz_dataflow_operators.worker AND
    mz_dataflow_operator_addresses.id = mz_addresses_with_unit_length.id AND
    mz_dataflow_operator_addresses.worker = mz_addresses_with_unit_length.worker AND
    mz_dataflow_operator_addresses.slot = 0",
    id: GlobalId::System(35),
};

/// Maintains a list of all operators bound to a dataflow and their
/// corresponding names and dataflow names and ids (per worker).
const VIEW_DATAFLOW_OPERATOR_DATAFLOWS: LogView = LogView {
    name: "mz_dataflow_operator_dataflows",
    sql: "CREATE VIEW mz_dataflow_operator_dataflows AS SELECT
    mz_dataflow_operators.id,
    mz_dataflow_operators.name,
    mz_dataflow_operators.worker,
    mz_dataflow_names.id as dataflow_id,
    mz_dataflow_names.name as dataflow_name
FROM
    mz_catalog.mz_dataflow_operators,
    mz_catalog.mz_dataflow_operator_addresses,
    mz_catalog.mz_dataflow_names
WHERE
    mz_dataflow_operators.id = mz_dataflow_operator_addresses.id AND
    mz_dataflow_operators.worker = mz_dataflow_operator_addresses.worker AND
    mz_dataflow_operator_addresses.slot = 0 AND
    mz_dataflow_names.local_id = mz_dataflow_operator_addresses.value AND
    mz_dataflow_names.worker = mz_dataflow_operator_addresses.worker",
    id: GlobalId::System(37),
};

/// Computes the minimum materialization frontiers across all workers.
const VIEW_MATERIALIZATION_FRONTIERS: LogView = LogView {
    name: "mz_materialization_frontiers",
    sql: "CREATE VIEW mz_materialization_frontiers AS SELECT
    global_id, min(time) AS time
FROM mz_catalog.mz_worker_materialization_frontiers
GROUP BY global_id",
    id: GlobalId::System(54),
};

/// Maintains the number of records used by each operator in a dataflow (per
/// worker). Operators not using any records are not shown.
const VIEW_RECORDS_PER_DATAFLOW_OPERATOR: LogView = LogView {
    name: "mz_records_per_dataflow_operator",
    sql: "CREATE VIEW mz_records_per_dataflow_operator AS SELECT
    mz_dataflow_operator_dataflows.id,
    mz_dataflow_operator_dataflows.name,
    mz_dataflow_operator_dataflows.worker,
    mz_dataflow_operator_dataflows.dataflow_id,
    mz_arrangement_sizes.records
FROM
    mz_catalog.mz_arrangement_sizes,
    mz_catalog.mz_dataflow_operator_dataflows
WHERE
    mz_dataflow_operator_dataflows.id = mz_arrangement_sizes.operator AND
    mz_dataflow_operator_dataflows.worker = mz_arrangement_sizes.worker",
    id: GlobalId::System(39),
};

/// Maintains the number of records used by each dataflow (per worker).
const VIEW_RECORDS_PER_DATAFLOW: LogView = LogView {
    name: "mz_records_per_dataflow",
    sql: "CREATE VIEW mz_records_per_dataflow AS SELECT
    mz_records_per_dataflow_operator.dataflow_id as id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker,
    SUM(mz_records_per_dataflow_operator.records) as records
FROM
    mz_catalog.mz_records_per_dataflow_operator,
    mz_catalog.mz_dataflow_names
WHERE
    mz_records_per_dataflow_operator.dataflow_id = mz_dataflow_names.id AND
    mz_records_per_dataflow_operator.worker = mz_dataflow_names.worker
GROUP BY
    mz_records_per_dataflow_operator.dataflow_id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker",
    id: GlobalId::System(41),
};

/// Maintains the number of records used by each dataflow (across all workers).
const VIEW_RECORDS_PER_DATAFLOW_GLOBAL: LogView = LogView {
    name: "mz_records_per_dataflow_global",
    sql: "CREATE VIEW mz_records_per_dataflow_global AS SELECT
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name,
    SUM(mz_records_per_dataflow.records) as records
FROM
    mz_catalog.mz_records_per_dataflow
GROUP BY
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name",
    id: GlobalId::System(43),
};

// Excludes materializations not in the catalog
// Only known materializations not in the catalog are ones from temporary
// dataflows created to serve select queries
const VIEW_PERF_DEPENDENCY_FRONTIERS: LogView = LogView {
    name: "mz_perf_dependency_frontiers",
    sql: "CREATE VIEW mz_perf_dependency_frontiers AS SELECT DISTINCT
    mcn.name AS dataflow,
    mcn_source.name AS source,
    frontier_source.time - frontier_df.time as lag_ms
FROM mz_catalog.mz_materialization_dependencies index_deps
JOIN mz_catalog.mz_materialization_frontiers frontier_source ON index_deps.source = frontier_source.global_id
JOIN mz_catalog.mz_materialization_frontiers frontier_df ON index_deps.dataflow = frontier_df.global_id
JOIN mz_catalog.mz_catalog_names mcn ON mcn.global_id = index_deps.dataflow
JOIN mz_catalog.mz_catalog_names mcn_source ON mcn_source.global_id = frontier_source.global_id",
    id: GlobalId::System(59),
};

const VIEW_PERF_ARRANGEMENT_RECORDS: LogView = LogView {
    name: "mz_perf_arrangement_records",
    sql:
        "CREATE VIEW mz_perf_arrangement_records AS SELECT mas.worker, name, records, operator
FROM mz_catalog.mz_arrangement_sizes mas
LEFT JOIN mz_catalog.mz_dataflow_operators mdo ON mdo.id = mas.operator AND mdo.worker = mas.worker",
    id: GlobalId::System(47),
};

const VIEW_PERF_PEEK_DURATIONS_CORE: LogView = LogView {
    name: "mz_perf_peek_durations_core",
    sql: "CREATE VIEW mz_perf_peek_durations_core AS SELECT
    d_upper.worker,
    CAST(d_upper.duration_ns AS TEXT) AS le,
    sum(d_summed.count) AS count
FROM
    mz_catalog.mz_peek_durations AS d_upper,
    mz_catalog.mz_peek_durations AS d_summed
WHERE
    d_upper.worker = d_summed.worker AND
    d_upper.duration_ns >= d_summed.duration_ns
GROUP BY d_upper.worker, d_upper.duration_ns",
    id: GlobalId::System(49),
};

const VIEW_PERF_PEEK_DURATIONS_BUCKET: LogView = LogView {
    name: "mz_perf_peek_durations_bucket",
    sql: "CREATE VIEW mz_perf_peek_durations_bucket AS
(
    SELECT * FROM mz_catalog.mz_perf_peek_durations_core
) UNION (
    SELECT worker, '+Inf', max(count) AS count FROM mz_catalog.mz_perf_peek_durations_core
    GROUP BY worker
)",
    id: GlobalId::System(51),
};

const VIEW_PERF_PEEK_DURATIONS_AGGREGATES: LogView = LogView {
    name: "mz_perf_peek_durations_aggregates",
    sql: "CREATE VIEW mz_perf_peek_durations_aggregates AS SELECT worker, sum(duration_ns * count) AS sum, sum(count) AS count
FROM mz_catalog.mz_peek_durations lpd
GROUP BY worker",
    id: GlobalId::System(53),
};

pub const LOG_VIEWS: &[LogView] = &[
    VIEW_ADDRESSES_WITH_UNIT_LENGTH,
    VIEW_DATAFLOW_NAMES,
    VIEW_DATAFLOW_OPERATOR_DATAFLOWS,
    VIEW_MATERIALIZATION_FRONTIERS,
    VIEW_RECORDS_PER_DATAFLOW_OPERATOR,
    VIEW_RECORDS_PER_DATAFLOW,
    VIEW_RECORDS_PER_DATAFLOW_GLOBAL,
    VIEW_PERF_DEPENDENCY_FRONTIERS,
    VIEW_PERF_ARRANGEMENT_RECORDS,
    VIEW_PERF_PEEK_DURATIONS_CORE,
    VIEW_PERF_PEEK_DURATIONS_BUCKET,
    VIEW_PERF_PEEK_DURATIONS_AGGREGATES,
];
