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
use once_cell::sync::Lazy;
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
// install the logging dataflows even on replicas that have logging disabled. See #15799.
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

#[derive(Arbitrary, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Serialize, Deserialize)]
pub enum LogVariant {
    Timely(TimelyLog),
    Differential(DifferentialLog),
    Compute(ComputeLog),
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

#[derive(Arbitrary, Hash, Eq, Ord, PartialEq, PartialOrd, Debug, Clone, Serialize, Deserialize)]
pub enum TimelyLog {
    Operates,
    Channels,
    Elapsed,
    Histogram,
    Addresses,
    Parks,
    MessagesSent,
    MessagesReceived,
    Reachability,
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
            None => Err(TryFromProtoError::missing_field("ProtoTimelyLog::kind")),
        }
    }
}

#[derive(Arbitrary, Hash, Eq, Ord, PartialEq, PartialOrd, Debug, Clone, Serialize, Deserialize)]
pub enum DifferentialLog {
    ArrangementBatches,
    ArrangementRecords,
    Sharing,
}

impl RustType<ProtoDifferentialLog> for DifferentialLog {
    fn into_proto(&self) -> ProtoDifferentialLog {
        use proto_differential_log::Kind::*;
        ProtoDifferentialLog {
            kind: Some(match self {
                DifferentialLog::ArrangementBatches => ArrangementBatches(()),
                DifferentialLog::ArrangementRecords => ArrangementRecords(()),
                DifferentialLog::Sharing => Sharing(()),
            }),
        }
    }

    fn from_proto(proto: ProtoDifferentialLog) -> Result<Self, TryFromProtoError> {
        use proto_differential_log::Kind::*;
        match proto.kind {
            Some(ArrangementBatches(())) => Ok(DifferentialLog::ArrangementBatches),
            Some(ArrangementRecords(())) => Ok(DifferentialLog::ArrangementRecords),
            Some(Sharing(())) => Ok(DifferentialLog::Sharing),
            None => Err(TryFromProtoError::missing_field(
                "ProtoDifferentialLog::kind",
            )),
        }
    }
}

#[derive(Arbitrary, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Serialize, Deserialize)]
pub enum ComputeLog {
    DataflowCurrent,
    DataflowDependency,
    FrontierCurrent,
    PeekCurrent,
    PeekDuration,
    FrontierDelay,
    ImportFrontierCurrent,
    ArrangementHeapSize,
    ArrangementHeapCapacity,
    ArrangementHeapAllocations,
}

impl RustType<ProtoComputeLog> for ComputeLog {
    fn into_proto(&self) -> ProtoComputeLog {
        use proto_compute_log::Kind::*;
        ProtoComputeLog {
            kind: Some(match self {
                ComputeLog::DataflowCurrent => DataflowCurrent(()),
                ComputeLog::DataflowDependency => DataflowDependency(()),
                ComputeLog::FrontierCurrent => FrontierCurrent(()),
                ComputeLog::PeekCurrent => PeekCurrent(()),
                ComputeLog::PeekDuration => PeekDuration(()),
                ComputeLog::FrontierDelay => FrontierDelay(()),
                ComputeLog::ImportFrontierCurrent => ImportFrontierCurrent(()),
                ComputeLog::ArrangementHeapSize => ArrangementHeapSize(()),
                ComputeLog::ArrangementHeapCapacity => ArrangementHeapCapacity(()),
                ComputeLog::ArrangementHeapAllocations => ArrangementHeapAllocations(()),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeLog) -> Result<Self, TryFromProtoError> {
        use proto_compute_log::Kind::*;
        match proto.kind {
            Some(DataflowCurrent(())) => Ok(ComputeLog::DataflowCurrent),
            Some(DataflowDependency(())) => Ok(ComputeLog::DataflowDependency),
            Some(FrontierCurrent(())) => Ok(ComputeLog::FrontierCurrent),
            Some(PeekCurrent(())) => Ok(ComputeLog::PeekCurrent),
            Some(PeekDuration(())) => Ok(ComputeLog::PeekDuration),
            Some(FrontierDelay(())) => Ok(ComputeLog::FrontierDelay),
            Some(ImportFrontierCurrent(())) => Ok(ComputeLog::ImportFrontierCurrent),
            Some(ArrangementHeapSize(())) => Ok(ComputeLog::ArrangementHeapSize),
            Some(ArrangementHeapCapacity(())) => Ok(ComputeLog::ArrangementHeapCapacity),
            Some(ArrangementHeapAllocations(())) => Ok(ComputeLog::ArrangementHeapAllocations),
            None => Err(TryFromProtoError::missing_field("ProtoComputeLog::kind")),
        }
    }
}

pub static DEFAULT_LOG_VARIANTS: Lazy<Vec<LogVariant>> = Lazy::new(|| {
    let default_logs = vec![
        LogVariant::Timely(TimelyLog::Operates),
        LogVariant::Timely(TimelyLog::Channels),
        LogVariant::Timely(TimelyLog::Elapsed),
        LogVariant::Timely(TimelyLog::Histogram),
        LogVariant::Timely(TimelyLog::Addresses),
        LogVariant::Timely(TimelyLog::Parks),
        LogVariant::Timely(TimelyLog::MessagesSent),
        LogVariant::Timely(TimelyLog::MessagesReceived),
        LogVariant::Timely(TimelyLog::Reachability),
        LogVariant::Differential(DifferentialLog::ArrangementBatches),
        LogVariant::Differential(DifferentialLog::ArrangementRecords),
        LogVariant::Differential(DifferentialLog::Sharing),
        LogVariant::Compute(ComputeLog::DataflowCurrent),
        LogVariant::Compute(ComputeLog::DataflowDependency),
        LogVariant::Compute(ComputeLog::FrontierCurrent),
        LogVariant::Compute(ComputeLog::ImportFrontierCurrent),
        LogVariant::Compute(ComputeLog::FrontierDelay),
        LogVariant::Compute(ComputeLog::PeekCurrent),
        LogVariant::Compute(ComputeLog::PeekDuration),
    ];

    default_logs
});

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
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("name", ScalarType::String.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Channels) => RelationDesc::empty()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("from_index", ScalarType::UInt64.nullable(false))
                .with_column("from_port", ScalarType::UInt64.nullable(false))
                .with_column("to_index", ScalarType::UInt64.nullable(false))
                .with_column("to_port", ScalarType::UInt64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Elapsed) => RelationDesc::empty()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false)),

            LogVariant::Timely(TimelyLog::Histogram) => RelationDesc::empty()
                .with_column("id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("duration_ns", ScalarType::UInt64.nullable(false)),

            LogVariant::Timely(TimelyLog::Addresses) => RelationDesc::empty()
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
                .with_key(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Parks) => RelationDesc::empty()
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("slept_for_ns", ScalarType::UInt64.nullable(false))
                .with_column("requested_ns", ScalarType::UInt64.nullable(false)),

            LogVariant::Timely(TimelyLog::MessagesReceived) => RelationDesc::empty()
                .with_column("channel_id", ScalarType::UInt64.nullable(false))
                .with_column("from_worker_id", ScalarType::UInt64.nullable(false))
                .with_column("to_worker_id", ScalarType::UInt64.nullable(false)),

            LogVariant::Timely(TimelyLog::MessagesSent) => RelationDesc::empty()
                .with_column("channel_id", ScalarType::UInt64.nullable(false))
                .with_column("from_worker_id", ScalarType::UInt64.nullable(false))
                .with_column("to_worker_id", ScalarType::UInt64.nullable(false)),

            LogVariant::Timely(TimelyLog::Reachability) => RelationDesc::empty()
                .with_column(
                    "address",
                    ScalarType::List {
                        element_type: Box::new(ScalarType::UInt64),
                        custom_id: None,
                    }
                    .nullable(false),
                )
                .with_column("port", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("update_type", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(true)),

            LogVariant::Differential(DifferentialLog::ArrangementBatches)
            | LogVariant::Differential(DifferentialLog::ArrangementRecords)
            | LogVariant::Differential(DifferentialLog::Sharing)
            | LogVariant::Compute(ComputeLog::ArrangementHeapSize)
            | LogVariant::Compute(ComputeLog::ArrangementHeapCapacity)
            | LogVariant::Compute(ComputeLog::ArrangementHeapAllocations) => RelationDesc::empty()
                .with_column("operator_id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false)),

            LogVariant::Compute(ComputeLog::DataflowCurrent) => RelationDesc::empty()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("dataflow_id", ScalarType::UInt64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Compute(ComputeLog::DataflowDependency) => RelationDesc::empty()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("import_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false)),

            LogVariant::Compute(ComputeLog::FrontierCurrent) => RelationDesc::empty()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(false)),

            LogVariant::Compute(ComputeLog::ImportFrontierCurrent) => RelationDesc::empty()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("import_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(false)),

            LogVariant::Compute(ComputeLog::FrontierDelay) => RelationDesc::empty()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("import_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("delay_ns", ScalarType::UInt64.nullable(false)),

            LogVariant::Compute(ComputeLog::PeekCurrent) => RelationDesc::empty()
                .with_column("id", ScalarType::Uuid.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("index_id", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Compute(ComputeLog::PeekDuration) => RelationDesc::empty()
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("duration_ns", ScalarType::UInt64.nullable(false)),
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
            LogVariant::Timely(TimelyLog::MessagesReceived)
            | LogVariant::Timely(TimelyLog::MessagesSent) => vec![
                (
                    LogVariant::Timely(TimelyLog::Channels),
                    vec![(0, 0), (1, 1)],
                ),
                (
                    LogVariant::Timely(TimelyLog::Channels),
                    vec![(0, 0), (2, 2)],
                ),
            ],
            LogVariant::Timely(TimelyLog::Reachability) => vec![],
            LogVariant::Differential(DifferentialLog::ArrangementBatches)
            | LogVariant::Differential(DifferentialLog::ArrangementRecords)
            | LogVariant::Differential(DifferentialLog::Sharing)
            | LogVariant::Compute(ComputeLog::ArrangementHeapSize)
            | LogVariant::Compute(ComputeLog::ArrangementHeapCapacity)
            | LogVariant::Compute(ComputeLog::ArrangementHeapAllocations) => vec![(
                LogVariant::Timely(TimelyLog::Operates),
                vec![(0, 0), (1, 1)],
            )],
            LogVariant::Compute(ComputeLog::DataflowCurrent) => vec![],
            LogVariant::Compute(ComputeLog::DataflowDependency) => vec![],
            LogVariant::Compute(ComputeLog::FrontierCurrent) => vec![],
            LogVariant::Compute(ComputeLog::ImportFrontierCurrent) => vec![],
            LogVariant::Compute(ComputeLog::FrontierDelay) => vec![],
            LogVariant::Compute(ComputeLog::PeekCurrent) => vec![],
            LogVariant::Compute(ComputeLog::PeekDuration) => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[mz_ore::test]
        fn logging_config_protobuf_roundtrip(expect in any::<LoggingConfig>()) {
            let actual = protobuf_roundtrip::<_, ProtoLoggingConfig>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
