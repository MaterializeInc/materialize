// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use repr::{ColumnType, RelationType, ScalarType};
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
    Messages,
    Shutdown,
    Text,
    Elapsed,
    Histogram,
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
            LogVariant::Timely(TimelyLog::Messages),
            LogVariant::Timely(TimelyLog::Shutdown),
            LogVariant::Timely(TimelyLog::Text),
            LogVariant::Timely(TimelyLog::Elapsed),
            LogVariant::Timely(TimelyLog::Histogram),
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
            LogVariant::Timely(TimelyLog::Messages) => "logs_messages",
            LogVariant::Timely(TimelyLog::Shutdown) => "logs_shutdown",
            LogVariant::Timely(TimelyLog::Text) => "logs_text",
            LogVariant::Timely(TimelyLog::Elapsed) => "logs_elapsed",
            LogVariant::Timely(TimelyLog::Histogram) => "logs_histogram",
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
    pub fn schema(&self) -> RelationType {
        match self {
            LogVariant::Timely(TimelyLog::Operates) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("id"),
                ColumnType::new(ScalarType::Int64).name("worker"),
                ColumnType::new(ScalarType::Int64).name("address_slot"),
                ColumnType::new(ScalarType::Int64).name("address_value"),
                ColumnType::new(ScalarType::String).name("name"),
            ])
            .add_keys(vec![0, 1]),
            LogVariant::Timely(TimelyLog::Channels) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("id"),
                ColumnType::new(ScalarType::Int64).name("worker"),
                ColumnType::new(ScalarType::String).name("scope"),
                ColumnType::new(ScalarType::Int64).name("source_node"),
                ColumnType::new(ScalarType::Int64).name("source_port"),
                ColumnType::new(ScalarType::Int64).name("target_node"),
                ColumnType::new(ScalarType::Int64).name("target_port"),
            ])
            .add_keys(vec![0, 1]),
            LogVariant::Timely(TimelyLog::Messages) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("channel"),
                ColumnType::new(ScalarType::Int64).name("count"),
            ])
            .add_keys(vec![0]),
            LogVariant::Timely(TimelyLog::Shutdown) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("id"),
                ColumnType::new(ScalarType::Int64).name("worker"),
            ])
            .add_keys(vec![0, 1]),
            LogVariant::Timely(TimelyLog::Text) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("text"),
                ColumnType::new(ScalarType::Int64).name("worker"),
            ]),
            LogVariant::Timely(TimelyLog::Elapsed) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("id"),
                ColumnType::new(ScalarType::Int64).name("elapsed_ns"),
            ])
            .add_keys(vec![0]),
            LogVariant::Timely(TimelyLog::Histogram) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("id"),
                ColumnType::new(ScalarType::Int64).name("duration_ns"),
                ColumnType::new(ScalarType::Int64).name("count"),
            ])
            .add_keys(vec![0]),
            LogVariant::Differential(DifferentialLog::Arrangement) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("operator"),
                ColumnType::new(ScalarType::Int64).name("worker"),
                ColumnType::new(ScalarType::Int64).name("records"),
                ColumnType::new(ScalarType::Int64).name("batches"),
            ])
            .add_keys(vec![0, 1]),
            LogVariant::Differential(DifferentialLog::Sharing) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("operator"),
                ColumnType::new(ScalarType::Int64).name("worker"),
                ColumnType::new(ScalarType::Int64).name("count"),
            ])
            .add_keys(vec![0, 1]),
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => RelationType::new(vec![
                ColumnType::new(ScalarType::String).name("name"),
                ColumnType::new(ScalarType::Int64).name("worker"),
            ])
            .add_keys(vec![0, 1]),
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => {
                RelationType::new(vec![
                    ColumnType::new(ScalarType::String).name("dataflow"),
                    ColumnType::new(ScalarType::String).name("source"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                ])
            }
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => RelationType::new(vec![
                ColumnType::new(ScalarType::String).name("name"),
                ColumnType::new(ScalarType::Int64).name("time"),
            ]),
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => RelationType::new(vec![
                ColumnType::new(ScalarType::String).name("uuid"),
                ColumnType::new(ScalarType::Int64).name("worker"),
                ColumnType::new(ScalarType::String).name("name"),
                ColumnType::new(ScalarType::Int64).name("time"),
            ])
            .add_keys(vec![0, 1]),
            LogVariant::Materialized(MaterializedLog::PeekDuration) => RelationType::new(vec![
                ColumnType::new(ScalarType::Int64).name("worker"),
                ColumnType::new(ScalarType::Int64).name("duration_ns"),
                ColumnType::new(ScalarType::Int64).name("count"),
            ])
            .add_keys(vec![0, 1]),
        }
    }
}
