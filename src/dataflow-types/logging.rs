// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashSet;
use std::time::Duration;

use expr::GlobalId;
use repr::{LiteralName, QualName, RelationDesc, ScalarType};

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
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum LogVariant {
    Timely(TimelyLog),
    Differential(DifferentialLog),
    Materialized(MaterializedLog),
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum TimelyLog {
    Operates,
    Channels,
    Elapsed,
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
        ]
    }

    pub fn name(&self) -> QualName {
        // Bind all names in one place to avoid accidental clashes.
        match self {
            LogVariant::Timely(TimelyLog::Operates) => "logs_operates".lit(),
            LogVariant::Timely(TimelyLog::Channels) => "logs_channels".lit(),
            LogVariant::Timely(TimelyLog::Elapsed) => "logs_elapsed".lit(),
            LogVariant::Timely(TimelyLog::Histogram) => "logs_histogram".lit(),
            LogVariant::Timely(TimelyLog::Addresses) => "logs_addresses".lit(),
            LogVariant::Timely(TimelyLog::Parks) => "logs_parks".lit(),
            LogVariant::Differential(DifferentialLog::Arrangement) => "logs_arrangement".lit(),
            LogVariant::Differential(DifferentialLog::Sharing) => "logs_sharing".lit(),
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => "logs_dataflows".lit(),
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => {
                "logs_dataflow_dependency".lit()
            }
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => "logs_frontiers".lit(),
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => "logs_peeks".lit(),
            LogVariant::Materialized(MaterializedLog::PeekDuration) => "logs_peek_durations".lit(),
            LogVariant::Materialized(MaterializedLog::PrimaryKeys) => "logs_primary_keys".lit(),
            LogVariant::Materialized(MaterializedLog::ForeignKeys) => "logs_foreign_keys".lit(),
        }
    }

    pub fn id(&self) -> GlobalId {
        // Bind all identifiers in one place to avoid accidental clashes.
        match self {
            LogVariant::Timely(TimelyLog::Operates) => GlobalId::system(1),
            LogVariant::Timely(TimelyLog::Channels) => GlobalId::system(2),
            LogVariant::Timely(TimelyLog::Elapsed) => GlobalId::system(3),
            LogVariant::Timely(TimelyLog::Histogram) => GlobalId::system(4),
            LogVariant::Timely(TimelyLog::Addresses) => GlobalId::system(5),
            LogVariant::Timely(TimelyLog::Parks) => GlobalId::system(6),
            LogVariant::Differential(DifferentialLog::Arrangement) => GlobalId::system(7),
            LogVariant::Differential(DifferentialLog::Sharing) => GlobalId::system(8),
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => GlobalId::system(9),
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => GlobalId::system(10),
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => GlobalId::system(11),
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => GlobalId::system(12),
            LogVariant::Materialized(MaterializedLog::PeekDuration) => GlobalId::system(13),
            LogVariant::Materialized(MaterializedLog::PrimaryKeys) => GlobalId::system(14),
            LogVariant::Materialized(MaterializedLog::ForeignKeys) => GlobalId::system(15),
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
                .add_column("elapsed_ns", ScalarType::Int64)
                .add_keys(vec![0]),

            LogVariant::Timely(TimelyLog::Histogram) => RelationDesc::empty()
                .add_column("id", ScalarType::Int64)
                .add_column("duration_ns", ScalarType::Int64)
                .add_column("count", ScalarType::Int64)
                .add_keys(vec![0]),

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
                .add_column("name", ScalarType::String)
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
                .add_column("name", ScalarType::String)
                .add_column("column", ScalarType::Int64)
                .add_column("key_group", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::ForeignKeys) => RelationDesc::empty()
                .add_column("child_name", ScalarType::String)
                .add_column("child_column", ScalarType::Int64)
                .add_column("parent_name", ScalarType::String)
                .add_column("parent_column", ScalarType::Int64)
                .add_column("key_group", ScalarType::Int64)
                .add_keys(vec![0, 1, 2, 3]),
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
            LogVariant::Timely(TimelyLog::Elapsed) => vec![],
            LogVariant::Timely(TimelyLog::Histogram) => vec![],
            LogVariant::Timely(TimelyLog::Addresses) => vec![(
                LogVariant::Timely(TimelyLog::Operates).id(),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Timely(TimelyLog::Parks) => vec![],
            LogVariant::Differential(DifferentialLog::Arrangement) => vec![],
            LogVariant::Differential(DifferentialLog::Sharing) => vec![],
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => vec![],
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::PeekDuration) => vec![],
            LogVariant::Materialized(MaterializedLog::PrimaryKeys) => vec![],
            LogVariant::Materialized(MaterializedLog::ForeignKeys) => vec![(
                LogVariant::Materialized(MaterializedLog::PrimaryKeys).id(),
                vec![(2, 0), (3, 1)],
            )],
        }
    }
}
