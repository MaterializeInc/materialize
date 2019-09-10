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
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => "logs_frontiers",
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => "logs_peeks",
            LogVariant::Materialized(MaterializedLog::PeekDuration) => "logs_peek_durations",
        }
    }
    pub fn schema(&self) -> RelationType {
        match self {
            LogVariant::Timely(TimelyLog::Operates) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::String).name("address"),
                    ColumnType::new(ScalarType::String).name("name"),
                ],
            },
            LogVariant::Timely(TimelyLog::Channels) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::String).name("scope"),
                    ColumnType::new(ScalarType::Int64).name("source node"),
                    ColumnType::new(ScalarType::Int64).name("source port"),
                    ColumnType::new(ScalarType::Int64).name("target node"),
                    ColumnType::new(ScalarType::Int64).name("target port"),
                ],
            },
            LogVariant::Timely(TimelyLog::Messages) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("channel"),
                    ColumnType::new(ScalarType::Int64).name("count"),
                ],
            },
            LogVariant::Timely(TimelyLog::Shutdown) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                ],
            },
            LogVariant::Timely(TimelyLog::Text) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("text"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                ],
            },
            LogVariant::Timely(TimelyLog::Elapsed) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("elapsed_ns"),
                ],
            },
            LogVariant::Timely(TimelyLog::Histogram) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("duration_ns"),
                    ColumnType::new(ScalarType::Int64).name("count"),
                ],
            },
            LogVariant::Differential(DifferentialLog::Arrangement) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("operator"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::Int64).name("records"),
                    ColumnType::new(ScalarType::Int64).name("batches"),
                ],
            },
            LogVariant::Differential(DifferentialLog::Sharing) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("operator"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::Int64).name("count"),
                ],
            },
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::String).name("name"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                ],
            },
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::String).name("name"),
                    ColumnType::new(ScalarType::Int64).name("time"),
                ],
            },
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::String).name("uuid"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::String).name("name"),
                    ColumnType::new(ScalarType::Int64).name("time"),
                ],
            },
            LogVariant::Materialized(MaterializedLog::PeekDuration) => RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::Int64).name("duration_ns"),
                    ColumnType::new(ScalarType::Int64).name("count"),
                ],
            },
        }
    }
}
