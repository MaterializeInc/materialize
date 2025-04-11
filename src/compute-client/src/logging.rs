// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute layer logging configuration.

use std::collections::BTreeMap;
use std::time::Duration;

use mz_repr::{GlobalId, RelationDesc, ScalarType};
use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy, any, prop};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// Logging configuration.
///
/// Setting `enable_logging` to `false` specifies that logging is disabled.
//
// Ideally we'd want to instead signal disabled logging by leaving `index_logs`
// empty. Unfortunately, we have to always provide `index_logs`, because we must
// install the logging dataflows even on replicas that have logging disabled. See database-issues#4545.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// The logging interval
    pub interval: Duration,
    /// Whether logging is enabled
    pub enable_logging: bool,
    /// Whether we should report logs for the log-processing dataflows
    pub log_logging: bool,
    /// Logs to keep in an arrangement
    pub index_logs: BTreeMap<LogVariant, GlobalId>,
}

impl Arbitrary for LoggingConfig {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<Duration>(),
            any::<bool>(),
            any::<bool>(),
            prop::collection::btree_map(any::<LogVariant>(), any::<GlobalId>(), 0..2),
        )
            .prop_map(
                |(interval, enable_logging, log_logging, index_logs)| LoggingConfig {
                    interval,
                    enable_logging,
                    log_logging,
                    index_logs,
                },
            )
            .boxed()
    }
}

/// TODO(database-issues#7533): Add documentation.
#[derive(
    Arbitrary, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Copy, Serialize, Deserialize,
)]
pub enum LogVariant {
    /// TODO(database-issues#7533): Add documentation.
    Timely(TimelyLog),
    /// TODO(database-issues#7533): Add documentation.
    Differential(DifferentialLog),
    /// TODO(database-issues#7533): Add documentation.
    Compute(ComputeLog),
}

impl From<TimelyLog> for LogVariant {
    fn from(value: TimelyLog) -> Self {
        Self::Timely(value)
    }
}

impl From<DifferentialLog> for LogVariant {
    fn from(value: DifferentialLog) -> Self {
        Self::Differential(value)
    }
}

impl From<ComputeLog> for LogVariant {
    fn from(value: ComputeLog) -> Self {
        Self::Compute(value)
    }
}

/// TODO(database-issues#7533): Add documentation.
#[derive(
    Arbitrary, Hash, Eq, Ord, PartialEq, PartialOrd, Debug, Clone, Copy, Serialize, Deserialize,
)]
pub enum TimelyLog {
    /// TODO(database-issues#7533): Add documentation.
    Operates,
    /// TODO(database-issues#7533): Add documentation.
    Channels,
    /// TODO(database-issues#7533): Add documentation.
    Elapsed,
    /// TODO(database-issues#7533): Add documentation.
    Histogram,
    /// TODO(database-issues#7533): Add documentation.
    Addresses,
    /// TODO(database-issues#7533): Add documentation.
    Parks,
    /// TODO(database-issues#7533): Add documentation.
    MessagesSent,
    /// TODO(database-issues#7533): Add documentation.
    MessagesReceived,
    /// TODO(database-issues#7533): Add documentation.
    Reachability,
    /// TODO(database-issues#7533): Add documentation.
    BatchesSent,
    /// TODO(database-issues#7533): Add documentation.
    BatchesReceived,
}

/// TODO(database-issues#7533): Add documentation.
#[derive(
    Arbitrary, Hash, Eq, Ord, PartialEq, PartialOrd, Debug, Clone, Copy, Serialize, Deserialize,
)]
pub enum DifferentialLog {
    /// TODO(database-issues#7533): Add documentation.
    ArrangementBatches,
    /// TODO(database-issues#7533): Add documentation.
    ArrangementRecords,
    /// TODO(database-issues#7533): Add documentation.
    Sharing,
    /// TODO(database-issues#7533): Add documentation.
    BatcherRecords,
    /// TODO(database-issues#7533): Add documentation.
    BatcherSize,
    /// TODO(database-issues#7533): Add documentation.
    BatcherCapacity,
    /// TODO(database-issues#7533): Add documentation.
    BatcherAllocations,
}

/// Variants of compute introspection sources.
#[derive(
    Arbitrary, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Copy, Serialize, Deserialize,
)]
pub enum ComputeLog {
    /// Installed dataflow exports.
    DataflowCurrent,
    /// Dataflow write frontiers.
    FrontierCurrent,
    /// Pending peeks.
    PeekCurrent,
    /// A histogram over peek durations.
    PeekDuration,
    /// Dataflow import frontiers.
    ImportFrontierCurrent,
    /// Arrangement heap sizes.
    ArrangementHeapSize,
    /// Arrangement heap capacities.
    ArrangementHeapCapacity,
    /// Arrangement heap allocations.
    ArrangementHeapAllocations,
    /// A histogram over dataflow shutdown durations.
    ShutdownDuration,
    /// Counts of errors in exported collections.
    ErrorCount,
    /// Hydration times of exported collections.
    HydrationTime,
    /// Mappings from `GlobalId`/`LirId`` pairs to dataflow addresses.
    LirMapping,
    /// Mappings from dataflows to `GlobalId`s.
    DataflowGlobal,
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

    /// Relation schemas for the logs.
    ///
    /// This types need to agree with the values that are produced
    /// in `logging::compute::construct` and with the description in
    /// `catalog/src/builtin.rs`.
    pub fn desc(&self) -> RelationDesc {
        match self {
            LogVariant::Timely(TimelyLog::Operates) => RelationDesc::builder()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("name", ScalarType::String.nullable(false))
                .with_key(vec![0, 1])
                .finish(),

            LogVariant::Timely(TimelyLog::Channels) => RelationDesc::builder()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("from_index", ScalarType::UInt64.nullable(false))
                .with_column("from_port", ScalarType::UInt64.nullable(false))
                .with_column("to_index", ScalarType::UInt64.nullable(false))
                .with_column("to_port", ScalarType::UInt64.nullable(false))
                .with_key(vec![0, 1])
                .finish(),

            LogVariant::Timely(TimelyLog::Elapsed) => RelationDesc::builder()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Timely(TimelyLog::Histogram) => RelationDesc::builder()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("duration_ns", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Timely(TimelyLog::Addresses) => RelationDesc::builder()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column(
                    "address",
                    ScalarType::List {
                        element_type: Box::new(ScalarType::UInt64),
                        custom_id: None,
                    }
                    .nullable(false),
                )
                .with_key(vec![0, 1])
                .finish(),

            LogVariant::Timely(TimelyLog::Parks) => RelationDesc::builder()
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("slept_for_ns", ScalarType::UInt64.nullable(false))
                .with_column("requested_ns", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Timely(TimelyLog::BatchesReceived) => RelationDesc::builder()
                .with_column("channel_id", ScalarType::UInt64.nullable(false))
                .with_column("from_worker_id", ScalarType::UInt64.nullable(false))
                .with_column("to_worker_id", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Timely(TimelyLog::BatchesSent) => RelationDesc::builder()
                .with_column("channel_id", ScalarType::UInt64.nullable(false))
                .with_column("from_worker_id", ScalarType::UInt64.nullable(false))
                .with_column("to_worker_id", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Timely(TimelyLog::MessagesReceived) => RelationDesc::builder()
                .with_column("channel_id", ScalarType::UInt64.nullable(false))
                .with_column("from_worker_id", ScalarType::UInt64.nullable(false))
                .with_column("to_worker_id", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Timely(TimelyLog::MessagesSent) => RelationDesc::builder()
                .with_column("channel_id", ScalarType::UInt64.nullable(false))
                .with_column("from_worker_id", ScalarType::UInt64.nullable(false))
                .with_column("to_worker_id", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Timely(TimelyLog::Reachability) => RelationDesc::builder()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("source", ScalarType::UInt64.nullable(false))
                .with_column("port", ScalarType::UInt64.nullable(false))
                .with_column("update_type", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(true))
                .finish(),

            LogVariant::Differential(DifferentialLog::ArrangementBatches)
            | LogVariant::Differential(DifferentialLog::ArrangementRecords)
            | LogVariant::Differential(DifferentialLog::Sharing)
            | LogVariant::Differential(DifferentialLog::BatcherRecords)
            | LogVariant::Differential(DifferentialLog::BatcherSize)
            | LogVariant::Differential(DifferentialLog::BatcherCapacity)
            | LogVariant::Differential(DifferentialLog::BatcherAllocations)
            | LogVariant::Compute(ComputeLog::ArrangementHeapSize)
            | LogVariant::Compute(ComputeLog::ArrangementHeapCapacity)
            | LogVariant::Compute(ComputeLog::ArrangementHeapAllocations) => {
                RelationDesc::builder()
                    .with_column("operator_id", ScalarType::UInt64.nullable(false))
                    .with_column("worker_id", ScalarType::UInt64.nullable(false))
                    .finish()
            }

            LogVariant::Compute(ComputeLog::DataflowCurrent) => RelationDesc::builder()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("dataflow_id", ScalarType::UInt64.nullable(false))
                .with_key(vec![0, 1])
                .finish(),

            LogVariant::Compute(ComputeLog::FrontierCurrent) => RelationDesc::builder()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(false))
                .with_key(vec![0, 1])
                .finish(),

            LogVariant::Compute(ComputeLog::ImportFrontierCurrent) => RelationDesc::builder()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("import_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(false))
                .with_key(vec![0, 1, 2])
                .finish(),

            LogVariant::Compute(ComputeLog::PeekCurrent) => RelationDesc::builder()
                .with_column("id", ScalarType::Uuid.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("object_id", ScalarType::String.nullable(false))
                .with_column("type", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(false))
                .with_key(vec![0, 1])
                .finish(),

            LogVariant::Compute(ComputeLog::PeekDuration) => RelationDesc::builder()
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("type", ScalarType::String.nullable(false))
                .with_column("duration_ns", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Compute(ComputeLog::ShutdownDuration) => RelationDesc::builder()
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("duration_ns", ScalarType::UInt64.nullable(false))
                .finish(),

            LogVariant::Compute(ComputeLog::ErrorCount) => RelationDesc::builder()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1])
                .finish(),

            LogVariant::Compute(ComputeLog::HydrationTime) => RelationDesc::builder()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("time_ns", ScalarType::UInt64.nullable(true))
                .with_key(vec![0, 1])
                .finish(),

            LogVariant::Compute(ComputeLog::LirMapping) => RelationDesc::builder()
                .with_column("global_id", ScalarType::String.nullable(false))
                .with_column("lir_id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("operator", ScalarType::String.nullable(false))
                .with_column("parent_lir_id", ScalarType::UInt64.nullable(true))
                .with_column("nesting", ScalarType::UInt16.nullable(false))
                .with_column("operator_id_start", ScalarType::UInt64.nullable(false))
                .with_column("operator_id_end", ScalarType::UInt64.nullable(false))
                .with_key(vec![0, 1, 2])
                .finish(),

            LogVariant::Compute(ComputeLog::DataflowGlobal) => RelationDesc::builder()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("global_id", ScalarType::String.nullable(false))
                .with_key(vec![0, 1])
                .finish(),
        }
    }
}
