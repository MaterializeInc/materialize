// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use repr::{RelationDesc, ScalarType};
use std::collections::HashSet;
use std::time::Duration;

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
}

impl LogVariant {
    pub fn default_logs() -> Vec<LogVariant> {
        vec![
            LogVariant::Timely(TimelyLog::Operates),
            LogVariant::Timely(TimelyLog::Channels),
            LogVariant::Timely(TimelyLog::Elapsed),
            LogVariant::Timely(TimelyLog::Histogram),
            LogVariant::Timely(TimelyLog::Addresses),
            LogVariant::Differential(DifferentialLog::Arrangement),
            LogVariant::Differential(DifferentialLog::Sharing),
            LogVariant::Materialized(MaterializedLog::DataflowCurrent),
            LogVariant::Materialized(MaterializedLog::DataflowDependency),
            LogVariant::Materialized(MaterializedLog::FrontierCurrent),
            LogVariant::Materialized(MaterializedLog::PeekCurrent),
            LogVariant::Materialized(MaterializedLog::PeekDuration),
        ]
    }

    pub fn name(&self) -> &'static str {
        // Bind all names in one place to avoid accidental clashes.
        match self {
            LogVariant::Timely(TimelyLog::Operates) => "logs_operates",
            LogVariant::Timely(TimelyLog::Channels) => "logs_channels",
            LogVariant::Timely(TimelyLog::Elapsed) => "logs_elapsed",
            LogVariant::Timely(TimelyLog::Histogram) => "logs_histogram",
            LogVariant::Timely(TimelyLog::Addresses) => "logs_addresses",
            LogVariant::Differential(DifferentialLog::Arrangement) => "logs_arrangement",
            LogVariant::Differential(DifferentialLog::Sharing) => "logs_sharing",
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => "logs_dataflows",
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => {
                "logs_dataflow_dependency"
            }
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => "logs_frontiers",
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => "logs_peeks",
            LogVariant::Materialized(MaterializedLog::PeekDuration) => "logs_peek_durations",
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
                .add_column("address_slot", ScalarType::Int64)
                .add_column("address_value", ScalarType::Int64)
                .add_keys(vec![0, 1]),

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
                .add_column("name", ScalarType::String)
                .add_column("time", ScalarType::Int64)
                .add_keys(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::PeekDuration) => RelationDesc::empty()
                .add_column("worker", ScalarType::Int64)
                .add_column("duration_ns", ScalarType::Int64)
                .add_column("count", ScalarType::Int64)
                .add_keys(vec![0, 1]),
        }
    }
}
