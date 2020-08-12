// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use expr::GlobalId;
use repr::{RelationDesc, ScalarType};

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub granularity_ns: u128,
    pub active_logs: HashMap<LogVariant, GlobalId>,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum LogVariant {
    Timely(TimelyLog),
    Differential(DifferentialLog),
    Materialized(MaterializedLog),
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum TimelyLog {
    Operates,
    Channels,
    Elapsed,
    Histogram,
    Addresses,
    Parks,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum DifferentialLog {
    Arrangement,
    Sharing,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum MaterializedLog {
    DataflowCurrent,
    DataflowDependency,
    FrontierCurrent,
    PeekCurrent,
    PeekDuration,
}

impl LogVariant {
    /// By which columns should the logs be indexed.
    ///
    /// This is distinct from the `keys` property of the type, which indicates uniqueness.
    /// When keys exist these are good choices for indexing, but when they do not we still
    /// require index guidance.
    pub fn index_by(&self) -> Vec<usize> {
        let desc = self.desc();
        let arity = desc.arity();
        desc.typ()
            .keys
            .get(0)
            .cloned()
            .unwrap_or_else(|| (0..arity).collect())
    }

    pub fn desc(&self) -> RelationDesc {
        match self {
            LogVariant::Timely(TimelyLog::Operates) => RelationDesc::empty()
                .with_column("id", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("name", ScalarType::String.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Channels) => RelationDesc::empty()
                .with_column("id", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("source_node", ScalarType::Int64.nullable(false))
                .with_column("source_port", ScalarType::Int64.nullable(false))
                .with_column("target_node", ScalarType::Int64.nullable(false))
                .with_column("target_port", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Elapsed) => RelationDesc::empty()
                .with_column("id", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("elapsed_ns", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Histogram) => RelationDesc::empty()
                .with_column("id", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("duration_ns", ScalarType::Int64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Addresses) => RelationDesc::empty()
                .with_column("id", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("slot", ScalarType::Int64.nullable(false))
                .with_column("value", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1, 2]),

            LogVariant::Timely(TimelyLog::Parks) => RelationDesc::empty()
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("slept_for", ScalarType::Int64.nullable(false))
                .with_column("requested", ScalarType::Int64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1, 2]),

            LogVariant::Differential(DifferentialLog::Arrangement) => RelationDesc::empty()
                .with_column("operator", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("records", ScalarType::Int64.nullable(false))
                .with_column("batches", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Differential(DifferentialLog::Sharing) => RelationDesc::empty()
                .with_column("operator", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => RelationDesc::empty()
                .with_column("name", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::DataflowDependency) => RelationDesc::empty()
                .with_column("dataflow", ScalarType::String.nullable(false))
                .with_column("source", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false)),

            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => RelationDesc::empty()
                .with_column("global_id", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("time", ScalarType::Int64.nullable(false)),

            LogVariant::Materialized(MaterializedLog::PeekCurrent) => RelationDesc::empty()
                .with_column("uuid", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("id", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Materialized(MaterializedLog::PeekDuration) => RelationDesc::empty()
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("duration_ns", ScalarType::Int64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),
        }
    }

    /// Foreign key relations from the log variant to other log collections.
    ///
    /// The result is a list of other variants, and for each a list of local
    /// and other column identifiers that can be equated.
    pub fn foreign_keys(&self) -> Vec<(LogVariant, Vec<(usize, usize)>)> {
        match self {
            LogVariant::Timely(TimelyLog::Operates) => vec![],
            LogVariant::Timely(TimelyLog::Channels) => vec![],
            LogVariant::Timely(TimelyLog::Elapsed) => vec![(
                LogVariant::Timely(TimelyLog::Operates),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Timely(TimelyLog::Histogram) => vec![(
                LogVariant::Timely(TimelyLog::Operates),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Timely(TimelyLog::Addresses) => vec![(
                LogVariant::Timely(TimelyLog::Operates),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Timely(TimelyLog::Parks) => vec![],
            LogVariant::Differential(DifferentialLog::Arrangement) => vec![(
                LogVariant::Timely(TimelyLog::Operates),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Differential(DifferentialLog::Sharing) => vec![(
                LogVariant::Timely(TimelyLog::Operates),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Materialized(MaterializedLog::DataflowCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::DataflowDependency) => vec![],
            LogVariant::Materialized(MaterializedLog::FrontierCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::PeekCurrent) => vec![],
            LogVariant::Materialized(MaterializedLog::PeekDuration) => vec![],
        }
    }
}
