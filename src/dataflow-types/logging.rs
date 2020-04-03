// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::time::Duration;

use expr::GlobalId;
use repr::{RelationDesc, ScalarType};

/// Logging configuration.
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    granularity_ns: u128,
    active_logs: HashSet<LogVariant>,
}

impl LoggingConfig {
    pub fn new(granularity: Duration) -> LoggingConfig {
        Self {
            granularity_ns: granularity.as_nanos(),
            active_logs: LogVariant::default_logs().into_iter().collect(),
        }
    }

    pub fn granularity_ns(&self) -> u128 {
        self.granularity_ns
    }

    pub fn active_logs(&self) -> &HashSet<LogVariant> {
        &self.active_logs
    }

    pub fn active_views(&self) -> Vec<LogView> {
        vec![
            VIEW_ADDRESSES_WITH_UNIT_LENGTH,
            VIEW_DATAFLOW_NAMES,
            VIEW_DATAFLOW_OPERATOR_DATAFLOWS,
            VIEW_RECORDS_PER_DATAFLOW_OPERATOR,
            VIEW_RECORDS_PER_DATAFLOW,
            VIEW_RECORDS_PER_DATAFLOW_GLOBAL,
            VIEW_PERF_DEPENDENCY_FRONTIERS,
            VIEW_PERF_ARRANGEMENT_RECORDS,
            VIEW_PERF_PEEK_DURATIONS_CORE,
            VIEW_PERF_PEEK_DURATIONS_BUCKET,
            VIEW_PERF_PEEK_DURATIONS_AGGREGATES,
        ]
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum LogVariant {
    Timely(TimelyLog),
    Differential(DifferentialLog),
    Materialized(MaterializedLog),
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum TimelyLog {
    /// The operators and their names
    Operates,
    Channels,
    Elapsed,
    /// Histogram of operator execution durations
    Histogram,
    Addresses,
    Parks,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum DifferentialLog {
    Arrangement,
    Sharing,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum MaterializedLog {
    DataflowCurrent,
    DataflowDependency,
    FrontierCurrent,
    PeekCurrent,
    PeekDuration,
    PrimaryKeys,
    ForeignKeys,
    Catalog,
    KafkaSinks,
    AvroOcfSinks,
}

impl LogVariant {
    pub fn default_logs() -> Vec<LogVariant> {
        vec![
            LogVariant::Timely(TimelyLog::Operates),
            LogVariant::Timely(TimelyLog::Channels),
            LogVariant::Timely(TimelyLog::Elapsed),
            LogVariant::Timely(TimelyLog::Histogram),
            LogVariant::Timely(TimelyLog::Addresses),
            LogVariant::Timely(TimelyLog::Parks),
            LogVariant::Differential(DifferentialLog::Arrangement),
            LogVariant::Differential(DifferentialLog::Sharing),
            LogVariant::Materialized(MaterializedLog::DataflowCurrent),
            LogVariant::Materialized(MaterializedLog::DataflowDependency),
            LogVariant::Materialized(MaterializedLog::FrontierCurrent),
            LogVariant::Materialized(MaterializedLog::PeekCurrent),
            LogVariant::Materialized(MaterializedLog::PeekDuration),
            LogVariant::Materialized(MaterializedLog::PrimaryKeys),
            LogVariant::Materialized(MaterializedLog::ForeignKeys),
            LogVariant::Materialized(MaterializedLog::Catalog),
            LogVariant::Materialized(MaterializedLog::KafkaSinks),
            LogVariant::Materialized(MaterializedLog::AvroOcfSinks),
        ]
    }

    pub fn name(&self) -> &'static str {
        // Bind all names in one place to avoid accidental clashes.
        match self {
            LogVariant::Timely(TimelyLog::Operates) => "mz_dataflow_operators",
            LogVariant::Timely(TimelyLog::Addresses) => "mz_dataflow_operator_addresses",
            LogVariant::Timely(TimelyLog::Channels) => "mz_dataflow_channels",
            LogVariant::Timely(TimelyLog::Elapsed) => "mz_scheduling_elapsed",
            LogVariant::Timely(TimelyLog::Histogram) => "mz_scheduling_histogram",
            LogVariant::Timely(TimelyLog::Parks) => "mz_scheduling_parks",
            LogVariant::Differential(DifferentialLog::Arrangement) => "mz_arrangement_sizes",
            LogVariant::Differential(DifferentialLog::Sharing) => "mz_arrangement_sharing",
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => "mz_materializations",
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => {
                "mz_materialization_dependencies"
            }
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => {
                "mz_materialization_frontiers"
            }
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => "mz_peek_active",
            LogVariant::Materialized(MaterializedLog::PeekDuration) => "mz_peek_durations",
            LogVariant::Materialized(MaterializedLog::PrimaryKeys) => "mz_view_keys",
            LogVariant::Materialized(MaterializedLog::ForeignKeys) => "mz_view_foreign_keys",
            LogVariant::Materialized(MaterializedLog::Catalog) => "mz_catalog_names",
            LogVariant::Materialized(MaterializedLog::KafkaSinks) => "mz_kafka_sinks",
            LogVariant::Materialized(MaterializedLog::AvroOcfSinks) => "mz_avro_ocf_sinks",
        }
    }

    pub fn id(&self) -> GlobalId {
        // Bind all identifiers in one place to avoid accidental clashes.
        match self {
            LogVariant::Timely(TimelyLog::Operates) => GlobalId::system(1),
            LogVariant::Timely(TimelyLog::Channels) => GlobalId::system(3),
            LogVariant::Timely(TimelyLog::Elapsed) => GlobalId::system(5),
            LogVariant::Timely(TimelyLog::Histogram) => GlobalId::system(7),
            LogVariant::Timely(TimelyLog::Addresses) => GlobalId::system(9),
            LogVariant::Timely(TimelyLog::Parks) => GlobalId::system(11),
            LogVariant::Differential(DifferentialLog::Arrangement) => GlobalId::system(13),
            LogVariant::Differential(DifferentialLog::Sharing) => GlobalId::system(15),
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => GlobalId::system(17),
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => GlobalId::system(19),
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => GlobalId::system(21),
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => GlobalId::system(23),
            LogVariant::Materialized(MaterializedLog::PeekDuration) => GlobalId::system(25),
            LogVariant::Materialized(MaterializedLog::PrimaryKeys) => GlobalId::system(27),
            LogVariant::Materialized(MaterializedLog::ForeignKeys) => GlobalId::system(29),
            LogVariant::Materialized(MaterializedLog::Catalog) => GlobalId::system(31),
            LogVariant::Materialized(MaterializedLog::KafkaSinks) => GlobalId::system(55),
            LogVariant::Materialized(MaterializedLog::AvroOcfSinks) => GlobalId::system(57),
        }
    }

    pub fn index_id(&self) -> GlobalId {
        // Bind all identifiers in one place to avoid accidental clashes.
        match self {
            LogVariant::Timely(TimelyLog::Operates) => GlobalId::system(2),
            LogVariant::Timely(TimelyLog::Channels) => GlobalId::system(4),
            LogVariant::Timely(TimelyLog::Elapsed) => GlobalId::system(6),
            LogVariant::Timely(TimelyLog::Histogram) => GlobalId::system(8),
            LogVariant::Timely(TimelyLog::Addresses) => GlobalId::system(10),
            LogVariant::Timely(TimelyLog::Parks) => GlobalId::system(12),
            LogVariant::Differential(DifferentialLog::Arrangement) => GlobalId::system(14),
            LogVariant::Differential(DifferentialLog::Sharing) => GlobalId::system(16),
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => GlobalId::system(18),
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => GlobalId::system(20),
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => GlobalId::system(22),
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => GlobalId::system(24),
            LogVariant::Materialized(MaterializedLog::PeekDuration) => GlobalId::system(26),
            LogVariant::Materialized(MaterializedLog::PrimaryKeys) => GlobalId::system(28),
            LogVariant::Materialized(MaterializedLog::ForeignKeys) => GlobalId::system(30),
            LogVariant::Materialized(MaterializedLog::Catalog) => GlobalId::system(32),
            LogVariant::Materialized(MaterializedLog::KafkaSinks) => GlobalId::system(56),
            LogVariant::Materialized(MaterializedLog::AvroOcfSinks) => GlobalId::system(58),
        }
    }

    /// By which columns should the logs be indexed.
    ///
    /// This is distinct from the `keys` property of the type, which indicates uniqueness.
    /// When keys exist these are good choices for indexing, but when they do not we still
    /// require index guidance.
    pub fn index_by(&self) -> Vec<usize> {
        let typ = self.schema().typ().clone();
        typ.keys
            .get(0)
            .cloned()
            .unwrap_or_else(|| (0..typ.column_types.len()).collect::<Vec<_>>())
    }

    pub fn schema(&self) -> RelationDesc {
        match self {
            LogVariant::Timely(TimelyLog::Operates) => RelationDesc::empty()
                .add_column("id", ScalarType::Int64)
                .add_column("worker", ScalarType::Int64)
                .add_column("name", ScalarType::String)
                .add_keys(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Channels) => RelationDesc::empty()
                .add_column("id", ScalarType::Int64)
                .add_column("worker", ScalarType::Int64)
                .add_column("source_node", ScalarType::Int64)
                .add_column("source_port", ScalarType::Int64)
                .add_column("target_node", ScalarType::Int64)
                .add_column("target_port", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Elapsed) => RelationDesc::empty()
                .add_column("id", ScalarType::Int64)
                .add_column("worker", ScalarType::Int64)
                .add_column("elapsed_ns", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Histogram) => RelationDesc::empty()
                .add_column("id", ScalarType::Int64)
                .add_column("worker", ScalarType::Int64)
                .add_column("duration_ns", ScalarType::Int64)
                .add_column("count", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Addresses) => RelationDesc::empty()
                .add_column("id", ScalarType::Int64)
                .add_column("worker", ScalarType::Int64)
                .add_column("slot", ScalarType::Int64)
                .add_column("value", ScalarType::Int64)
                .add_keys(vec![0, 1, 2]),

            LogVariant::Timely(TimelyLog::Parks) => RelationDesc::empty()
                .add_column("worker", ScalarType::Int64)
                .add_column("slept_for", ScalarType::Int64)
                .add_column("requested", ScalarType::Int64)
                .add_column("count", ScalarType::Int64)
                .add_keys(vec![0, 1, 2]),

            LogVariant::Differential(DifferentialLog::Arrangement) => RelationDesc::empty()
                .add_column("operator", ScalarType::Int64)
                .add_column("worker", ScalarType::Int64)
                .add_column("records", ScalarType::Int64)
                .add_column("batches", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Differential(DifferentialLog::Sharing) => RelationDesc::empty()
                .add_column("operator", ScalarType::Int64)
                .add_column("worker", ScalarType::Int64)
                .add_column("count", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => RelationDesc::empty()
                .add_column("name", ScalarType::String)
                .add_column("worker", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::DataflowDependency) => RelationDesc::empty()
                .add_column("dataflow", ScalarType::String)
                .add_column("source", ScalarType::String)
                .add_column("worker", ScalarType::Int64),

            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => RelationDesc::empty()
                .add_column("global_id", ScalarType::String)
                .add_column("time", ScalarType::Int64),

            LogVariant::Materialized(MaterializedLog::PeekCurrent) => RelationDesc::empty()
                .add_column("uuid", ScalarType::String)
                .add_column("worker", ScalarType::Int64)
                .add_column("id", ScalarType::String)
                .add_column("time", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::PeekDuration) => RelationDesc::empty()
                .add_column("worker", ScalarType::Int64)
                .add_column("duration_ns", ScalarType::Int64)
                .add_column("count", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::PrimaryKeys) => RelationDesc::empty()
                .add_column("global_id", ScalarType::String)
                .add_column("column", ScalarType::Int64)
                .add_column("key_group", ScalarType::Int64),

            LogVariant::Materialized(MaterializedLog::ForeignKeys) => RelationDesc::empty()
                .add_column("child_id", ScalarType::String)
                .add_column("child_column", ScalarType::Int64)
                .add_column("parent_id", ScalarType::String)
                .add_column("parent_column", ScalarType::Int64)
                .add_column("key_group", ScalarType::Int64)
                .add_keys(vec![0, 1, 4]),

            LogVariant::Materialized(MaterializedLog::Catalog) => RelationDesc::empty()
                .add_column("global_id", ScalarType::String)
                .add_column("name", ScalarType::String)
                .add_keys(vec![0]),

            LogVariant::Materialized(MaterializedLog::KafkaSinks) => RelationDesc::empty()
                .add_column("global_id", ScalarType::String)
                .add_column("topic", ScalarType::String)
                .add_keys(vec![0]),

            LogVariant::Materialized(MaterializedLog::AvroOcfSinks) => RelationDesc::empty()
                .add_column("global_id", ScalarType::String)
                .add_column("path", ScalarType::String)
                .add_keys(vec![0]),
        }
    }

    /// Foreign key relations from the log variant to other log collections.
    ///
    /// The result is a list of other variants, and for each a list of local
    /// and other column identifiers that can be equated.
    pub fn foreign_keys(&self) -> Vec<(GlobalId, Vec<(usize, usize)>)> {
        match self {
            LogVariant::Timely(TimelyLog::Operates) => vec![],
            LogVariant::Timely(TimelyLog::Channels) => vec![],
            LogVariant::Timely(TimelyLog::Elapsed) => vec![(
                LogVariant::Timely(TimelyLog::Operates).id(),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Timely(TimelyLog::Histogram) => vec![(
                LogVariant::Timely(TimelyLog::Operates).id(),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Timely(TimelyLog::Addresses) => vec![(
                LogVariant::Timely(TimelyLog::Operates).id(),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Timely(TimelyLog::Parks) => vec![],
            LogVariant::Differential(DifferentialLog::Arrangement) => vec![(
                LogVariant::Timely(TimelyLog::Operates).id(),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Differential(DifferentialLog::Sharing) => vec![(
                LogVariant::Timely(TimelyLog::Operates).id(),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => vec![],
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::PeekDuration) => vec![],
            LogVariant::Materialized(MaterializedLog::PrimaryKeys) => vec![],
            LogVariant::Materialized(MaterializedLog::ForeignKeys) => vec![
                (
                    LogVariant::Materialized(MaterializedLog::PrimaryKeys).id(),
                    vec![(2, 0), (3, 1)],
                ),
                (
                    LogVariant::Materialized(MaterializedLog::Catalog).id(),
                    vec![(0, 0)],
                ),
                (
                    LogVariant::Materialized(MaterializedLog::Catalog).id(),
                    vec![(2, 0)],
                ),
            ],
            LogVariant::Materialized(MaterializedLog::Catalog) => vec![],
            LogVariant::Materialized(MaterializedLog::KafkaSinks) => vec![],
            LogVariant::Materialized(MaterializedLog::AvroOcfSinks) => vec![],
        }
    }
}

pub struct LogView {
    pub name: &'static str,
    pub sql: &'static str,
    pub id: GlobalId,
    pub index_id: GlobalId,
}

// Stores all addresses that only have one slot (0) in
// mz_dataflow_operator_addresses. The resulting addresses are either channels
// or dataflows.
const VIEW_ADDRESSES_WITH_UNIT_LENGTH: LogView = LogView {
    name: "mz_addresses_with_unit_length",
    sql: "CREATE MATERIALIZED VIEW mz_addresses_with_unit_length AS SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
FROM
    mz_catalog.mz_dataflow_operator_addresses
GROUP BY
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
HAVING count(*) = 1",
    id: GlobalId::System(33),
    index_id: GlobalId::System(34),
};

/// Maintains a list of the current dataflow operator ids, and their
/// corresponding operator names and local ids (per worker).
const VIEW_DATAFLOW_NAMES: LogView = LogView {
    name: "mz_dataflow_names",
    sql: "CREATE MATERIALIZED VIEW mz_dataflow_names AS SELECT
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
    index_id: GlobalId::System(36),
};

/// Maintains a list of all operators bound to a dataflow and their
/// corresponding names and dataflow names and ids (per worker).
const VIEW_DATAFLOW_OPERATOR_DATAFLOWS: LogView = LogView {
    name: "mz_dataflow_operator_dataflows",
    sql: "CREATE MATERIALIZED VIEW mz_dataflow_operator_dataflows AS SELECT
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
    index_id: GlobalId::System(38),
};

/// Maintains the number of records used by each operator in a dataflow (per
/// worker). Operators not using any records are not shown.
const VIEW_RECORDS_PER_DATAFLOW_OPERATOR: LogView = LogView {
    name: "mz_records_per_dataflow_operator",
    sql: "CREATE MATERIALIZED VIEW mz_records_per_dataflow_operator AS SELECT
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
    index_id: GlobalId::System(40),
};

/// Maintains the number of records used by each dataflow (per worker).
const VIEW_RECORDS_PER_DATAFLOW: LogView = LogView {
    name: "mz_records_per_dataflow",
    sql: "CREATE MATERIALIZED VIEW mz_records_per_dataflow AS SELECT
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
    index_id: GlobalId::System(42),
};

/// Maintains the number of records used by each dataflow (across all workers).
const VIEW_RECORDS_PER_DATAFLOW_GLOBAL: LogView = LogView {
    name: "mz_records_per_dataflow_global",
    sql: "CREATE MATERIALIZED VIEW mz_records_per_dataflow_global AS SELECT
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name,
    SUM(mz_records_per_dataflow.records) as records
FROM
    mz_catalog.mz_records_per_dataflow
GROUP BY
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name",
    id: GlobalId::System(43),
    index_id: GlobalId::System(44),
};

const VIEW_PERF_DEPENDENCY_FRONTIERS: LogView = LogView {
    name: "mz_perf_dependency_frontiers",
    sql: "CREATE MATERIALIZED VIEW mz_perf_dependency_frontiers AS SELECT DISTINCT
        coalesce(mcn.name, index_deps.dataflow) as dataflow,
        coalesce(mcn_source.name, frontier_source.global_id) as source,
        frontier_source.time - frontier_df.time as lag_ms
FROM
        mz_catalog.mz_materialization_dependencies index_deps
JOIN mz_catalog.mz_materialization_frontiers frontier_source ON index_deps.source = frontier_source.global_id
JOIN mz_catalog.mz_materialization_frontiers frontier_df ON index_deps.dataflow = frontier_df.global_id
LEFT JOIN mz_catalog.mz_catalog_names mcn ON mcn.global_id = index_deps.dataflow
LEFT JOIN mz_catalog.mz_catalog_names mcn_source ON mcn_source.global_id = frontier_source.global_id",
    id: GlobalId::System(45),
    index_id: GlobalId::System(46),
};

const VIEW_PERF_ARRANGEMENT_RECORDS: LogView = LogView {
    name: "mz_perf_arrangement_records",
    sql: "CREATE MATERIALIZED VIEW mz_perf_arrangement_records AS SELECT mas.worker, name, records, operator
FROM mz_catalog.mz_arrangement_sizes mas
JOIN mz_catalog.mz_dataflow_operators mdo ON mdo.id = mas.operator",
    id: GlobalId::System(47),
    index_id: GlobalId::System(48),
};

const VIEW_PERF_PEEK_DURATIONS_CORE: LogView = LogView {
    name: "mz_perf_peek_durations_core",
    sql: "CREATE MATERIALIZED VIEW mz_perf_peek_durations_core AS SELECT
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
    index_id: GlobalId::System(50),
};

const VIEW_PERF_PEEK_DURATIONS_BUCKET: LogView = LogView {
    name: "mz_perf_peek_durations_bucket",
    sql: "CREATE MATERIALIZED VIEW mz_perf_peek_durations_bucket AS
(
    SELECT * FROM mz_catalog.mz_perf_peek_durations_core
) UNION (
    SELECT worker, '+Inf', max(count) AS count FROM mz_catalog.mz_perf_peek_durations_core
    GROUP BY worker
)",
    id: GlobalId::System(51),
    index_id: GlobalId::System(52),
};

const VIEW_PERF_PEEK_DURATIONS_AGGREGATES: LogView = LogView {
    name: "mz_perf_peek_durations_aggregates",
    sql: "CREATE MATERIALIZED VIEW mz_perf_peek_durations_aggregates AS SELECT worker, sum(duration_ns * count) AS sum, sum(count) AS count
FROM mz_catalog.mz_peek_durations lpd
GROUP BY worker",
    id: GlobalId::System(53),
    index_id: GlobalId::System(54),
};
