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

use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc, ScalarType};
use proptest::prelude::{any, prop, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.logging.rs"));

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

impl RustType<ProtoLoggingConfig> for LoggingConfig {
    fn into_proto(&self) -> ProtoLoggingConfig {
        ProtoLoggingConfig {
            interval: Some(self.interval.into_proto()),
            enable_logging: self.enable_logging,
            log_logging: self.log_logging,
            index_logs: self.index_logs.into_proto(),
        }
    }

    fn from_proto(proto: ProtoLoggingConfig) -> Result<Self, TryFromProtoError> {
        Ok(LoggingConfig {
            interval: proto
                .interval
                .into_rust_if_some("ProtoLoggingConfig::interval")?,
            enable_logging: proto.enable_logging,
            log_logging: proto.log_logging,
            index_logs: proto.index_logs.into_rust()?,
        })
    }
}

impl ProtoMapEntry<LogVariant, GlobalId> for ProtoIndexLog {
    fn from_rust<'a>(entry: (&'a LogVariant, &'a GlobalId)) -> Self {
        ProtoIndexLog {
            key: Some(entry.0.into_proto()),
            value: Some(entry.1.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(LogVariant, GlobalId), TryFromProtoError> {
        Ok((
            self.key.into_rust_if_some("ProtoIndexLog::key")?,
            self.value.into_rust_if_some("ProtoIndexLog::value")?,
        ))
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

impl RustType<ProtoLogVariant> for LogVariant {
    fn into_proto(&self) -> ProtoLogVariant {
        use proto_log_variant::Kind::*;
        ProtoLogVariant {
            kind: Some(match self {
                LogVariant::Timely(x) => Timely(x.into_proto()),
                LogVariant::Differential(x) => Differential(x.into_proto()),
                LogVariant::Compute(x) => Compute(x.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoLogVariant) -> Result<Self, TryFromProtoError> {
        use proto_log_variant::Kind::*;
        match proto.kind {
            Some(Timely(x)) => Ok(LogVariant::Timely(x.into_rust()?)),
            Some(Differential(x)) => Ok(LogVariant::Differential(x.into_rust()?)),
            Some(Compute(x)) => Ok(LogVariant::Compute(x.into_rust()?)),
            None => Err(TryFromProtoError::missing_field("ProtoLogVariant::kind")),
        }
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

impl RustType<ProtoTimelyLog> for TimelyLog {
    fn into_proto(&self) -> ProtoTimelyLog {
        use proto_timely_log::Kind::*;
        ProtoTimelyLog {
            kind: Some(match self {
                TimelyLog::Operates => Operates(()),
                TimelyLog::Channels => Channels(()),
                TimelyLog::Elapsed => Elapsed(()),
                TimelyLog::Histogram => Histogram(()),
                TimelyLog::Addresses => Addresses(()),
                TimelyLog::Parks => Parks(()),
                TimelyLog::MessagesSent => MessagesSent(()),
                TimelyLog::MessagesReceived => MessagesReceived(()),
                TimelyLog::Reachability => Reachability(()),
                TimelyLog::BatchesSent => BatchesSent(()),
                TimelyLog::BatchesReceived => BatchesReceived(()),
            }),
        }
    }

    fn from_proto(proto: ProtoTimelyLog) -> Result<Self, TryFromProtoError> {
        use proto_timely_log::Kind::*;
        match proto.kind {
            Some(Operates(())) => Ok(TimelyLog::Operates),
            Some(Channels(())) => Ok(TimelyLog::Channels),
            Some(Elapsed(())) => Ok(TimelyLog::Elapsed),
            Some(Histogram(())) => Ok(TimelyLog::Histogram),
            Some(Addresses(())) => Ok(TimelyLog::Addresses),
            Some(Parks(())) => Ok(TimelyLog::Parks),
            Some(MessagesSent(())) => Ok(TimelyLog::MessagesSent),
            Some(MessagesReceived(())) => Ok(TimelyLog::MessagesReceived),
            Some(Reachability(())) => Ok(TimelyLog::Reachability),
            Some(BatchesSent(())) => Ok(TimelyLog::BatchesSent),
            Some(BatchesReceived(())) => Ok(TimelyLog::BatchesReceived),
            None => Err(TryFromProtoError::missing_field("ProtoTimelyLog::kind")),
        }
    }
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

impl RustType<ProtoDifferentialLog> for DifferentialLog {
    fn into_proto(&self) -> ProtoDifferentialLog {
        use proto_differential_log::Kind::*;
        ProtoDifferentialLog {
            kind: Some(match self {
                DifferentialLog::ArrangementBatches => ArrangementBatches(()),
                DifferentialLog::ArrangementRecords => ArrangementRecords(()),
                DifferentialLog::Sharing => Sharing(()),
                DifferentialLog::BatcherRecords => BatcherRecords(()),
                DifferentialLog::BatcherSize => BatcherSize(()),
                DifferentialLog::BatcherCapacity => BatcherCapacity(()),
                DifferentialLog::BatcherAllocations => BatcherAllocations(()),
            }),
        }
    }

    fn from_proto(proto: ProtoDifferentialLog) -> Result<Self, TryFromProtoError> {
        use proto_differential_log::Kind::*;
        match proto.kind {
            Some(ArrangementBatches(())) => Ok(DifferentialLog::ArrangementBatches),
            Some(ArrangementRecords(())) => Ok(DifferentialLog::ArrangementRecords),
            Some(Sharing(())) => Ok(DifferentialLog::Sharing),
            Some(BatcherRecords(())) => Ok(DifferentialLog::BatcherRecords),
            Some(BatcherSize(())) => Ok(DifferentialLog::BatcherSize),
            Some(BatcherCapacity(())) => Ok(DifferentialLog::BatcherCapacity),
            Some(BatcherAllocations(())) => Ok(DifferentialLog::BatcherAllocations),
            None => Err(TryFromProtoError::missing_field(
                "ProtoDifferentialLog::kind",
            )),
        }
    }
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

impl RustType<ProtoComputeLog> for ComputeLog {
    fn into_proto(&self) -> ProtoComputeLog {
        use proto_compute_log::Kind::*;
        ProtoComputeLog {
            kind: Some(match self {
                ComputeLog::DataflowCurrent => DataflowCurrent(()),
                ComputeLog::FrontierCurrent => FrontierCurrent(()),
                ComputeLog::PeekCurrent => PeekCurrent(()),
                ComputeLog::PeekDuration => PeekDuration(()),
                ComputeLog::ImportFrontierCurrent => ImportFrontierCurrent(()),
                ComputeLog::ArrangementHeapSize => ArrangementHeapSize(()),
                ComputeLog::ArrangementHeapCapacity => ArrangementHeapCapacity(()),
                ComputeLog::ArrangementHeapAllocations => ArrangementHeapAllocations(()),
                ComputeLog::ShutdownDuration => ShutdownDuration(()),
                ComputeLog::ErrorCount => ErrorCount(()),
                ComputeLog::HydrationTime => HydrationTime(()),
                ComputeLog::LirMapping => LirMapping(()),
                ComputeLog::DataflowGlobal => DataflowGlobal(()),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeLog) -> Result<Self, TryFromProtoError> {
        use proto_compute_log::Kind::*;
        match proto.kind {
            Some(DataflowCurrent(())) => Ok(ComputeLog::DataflowCurrent),
            Some(FrontierCurrent(())) => Ok(ComputeLog::FrontierCurrent),
            Some(PeekCurrent(())) => Ok(ComputeLog::PeekCurrent),
            Some(PeekDuration(())) => Ok(ComputeLog::PeekDuration),
            Some(ImportFrontierCurrent(())) => Ok(ComputeLog::ImportFrontierCurrent),
            Some(ArrangementHeapSize(())) => Ok(ComputeLog::ArrangementHeapSize),
            Some(ArrangementHeapCapacity(())) => Ok(ComputeLog::ArrangementHeapCapacity),
            Some(ArrangementHeapAllocations(())) => Ok(ComputeLog::ArrangementHeapAllocations),
            Some(ShutdownDuration(())) => Ok(ComputeLog::ShutdownDuration),
            Some(ErrorCount(())) => Ok(ComputeLog::ErrorCount),
            Some(HydrationTime(())) => Ok(ComputeLog::HydrationTime),
            Some(LirMapping(())) => Ok(ComputeLog::LirMapping),
            Some(DataflowGlobal(())) => Ok(ComputeLog::DataflowGlobal),
            None => Err(TryFromProtoError::missing_field("ProtoComputeLog::kind")),
        }
    }
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

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[mz_ore::test]
        fn logging_config_protobuf_roundtrip(expect in any::<LoggingConfig>()) {
            let actual = protobuf_roundtrip::<_, ProtoLoggingConfig>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
