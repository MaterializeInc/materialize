// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use mz_repr::proto::newapi::{IntoRustIfSome, RustType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_repr::proto::{FromProtoIfSome, ProtoRepr, TryFromProtoError, TryIntoIfSome};
use mz_repr::{GlobalId, RelationDesc, ScalarType};

include!(concat!(env!("OUT_DIR"), "/mz_dataflow_types.logging.rs"));

/// Logging configuration.
#[derive(Arbitrary, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub granularity_ns: u128,
    pub active_logs: HashMap<LogVariant, GlobalId>,
    // Whether we should report logs for the log-processing dataflows
    pub log_logging: bool,
}

impl LoggingConfig {
    /// Announce the identifiers the logging config will populate.
    pub fn log_identifiers<'a>(&'a self) -> impl Iterator<Item = GlobalId> + 'a {
        self.active_logs.values().cloned()
    }
}

impl From<&LoggingConfig> for ProtoLoggingConfig {
    fn from(x: &LoggingConfig) -> Self {
        ProtoLoggingConfig {
            granularity_ns: Some(x.granularity_ns.into_proto()),
            active_logs: x.active_logs.iter().map(Into::into).collect(),
            log_logging: x.log_logging,
        }
    }
}

impl TryFrom<ProtoLoggingConfig> for LoggingConfig {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoLoggingConfig) -> Result<Self, Self::Error> {
        Ok(LoggingConfig {
            granularity_ns: x
                .granularity_ns
                .from_proto_if_some("ProtoLoggingConfig::granularity_ns")?,
            active_logs: x
                .active_logs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<HashMap<_, _>, _>>()?,
            log_logging: x.log_logging,
        })
    }
}

impl From<(&LogVariant, &GlobalId)> for ProtoActiveLog {
    fn from(x: (&LogVariant, &GlobalId)) -> Self {
        ProtoActiveLog {
            key: Some(x.0.into()),
            value: Some(x.1.into_proto()),
        }
    }
}

impl TryFrom<ProtoActiveLog> for (LogVariant, GlobalId) {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoActiveLog) -> Result<Self, Self::Error> {
        Ok((
            x.key.try_into_if_some("ProtoActiveLog::key")?,
            x.value.into_rust_if_some("ProtoActiveLog::value")?,
        ))
    }
}

#[derive(Arbitrary, Hash, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum LogVariant {
    Timely(TimelyLog),
    Differential(DifferentialLog),
    Materialized(MaterializedLog),
}

impl From<&LogVariant> for ProtoLogVariant {
    fn from(x: &LogVariant) -> Self {
        ProtoLogVariant {
            kind: Some(match x {
                LogVariant::Timely(timely) => proto_log_variant::Kind::Timely(timely.into()),
                LogVariant::Differential(differential) => {
                    proto_log_variant::Kind::Differential(differential.into())
                }
                LogVariant::Materialized(materialize) => {
                    proto_log_variant::Kind::Materialized(materialize.into())
                }
            }),
        }
    }
}

impl TryFrom<ProtoLogVariant> for LogVariant {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoLogVariant) -> Result<Self, Self::Error> {
        match x.kind {
            Some(proto_log_variant::Kind::Timely(timely)) => {
                Ok(LogVariant::Timely(timely.try_into()?))
            }
            Some(proto_log_variant::Kind::Differential(differential)) => {
                Ok(LogVariant::Differential(differential.try_into()?))
            }
            Some(proto_log_variant::Kind::Materialized(materialized)) => {
                Ok(LogVariant::Materialized(materialized.try_into()?))
            }
            None => Err(TryFromProtoError::missing_field("ProtoLogVariant::kind")),
        }
    }
}

#[derive(Arbitrary, Hash, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
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

impl From<&TimelyLog> for ProtoTimelyLog {
    fn from(x: &TimelyLog) -> Self {
        use proto_timely_log::Kind::*;
        ProtoTimelyLog {
            kind: Some(match x {
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
}

impl TryFrom<ProtoTimelyLog> for TimelyLog {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoTimelyLog) -> Result<Self, Self::Error> {
        use proto_timely_log::Kind::*;
        match x.kind {
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

#[derive(Arbitrary, Hash, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum DifferentialLog {
    ArrangementBatches,
    ArrangementRecords,
    Sharing,
}

impl From<&DifferentialLog> for ProtoDifferentialLog {
    fn from(x: &DifferentialLog) -> Self {
        use proto_differential_log::Kind::*;
        ProtoDifferentialLog {
            kind: Some(match x {
                DifferentialLog::ArrangementBatches => ArrangementBatches(()),
                DifferentialLog::ArrangementRecords => ArrangementRecords(()),
                DifferentialLog::Sharing => Sharing(()),
            }),
        }
    }
}

impl TryFrom<ProtoDifferentialLog> for DifferentialLog {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoDifferentialLog) -> Result<Self, Self::Error> {
        use proto_differential_log::Kind::*;
        match x.kind {
            Some(ArrangementBatches(())) => Ok(DifferentialLog::ArrangementBatches),
            Some(ArrangementRecords(())) => Ok(DifferentialLog::ArrangementRecords),
            Some(Sharing(())) => Ok(DifferentialLog::Sharing),
            None => Err(TryFromProtoError::missing_field(
                "ProtoDifferentialLog::kind",
            )),
        }
    }
}

#[derive(Arbitrary, Hash, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum MaterializedLog {
    DataflowCurrent,
    DataflowDependency,
    FrontierCurrent,
    PeekCurrent,
    PeekDuration,
}

impl From<&MaterializedLog> for ProtoMaterializedLog {
    fn from(x: &MaterializedLog) -> Self {
        use proto_materialized_log::Kind::*;
        ProtoMaterializedLog {
            kind: Some(match x {
                MaterializedLog::DataflowCurrent => DataflowCurrent(()),
                MaterializedLog::DataflowDependency => DataflowDependency(()),
                MaterializedLog::FrontierCurrent => FrontierCurrent(()),
                MaterializedLog::PeekCurrent => PeekCurrent(()),
                MaterializedLog::PeekDuration => PeekDuration(()),
            }),
        }
    }
}

impl TryFrom<ProtoMaterializedLog> for MaterializedLog {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoMaterializedLog) -> Result<Self, Self::Error> {
        use proto_materialized_log::Kind::*;
        match x.kind {
            Some(DataflowCurrent(())) => Ok(MaterializedLog::DataflowCurrent),
            Some(DataflowDependency(())) => Ok(MaterializedLog::DataflowDependency),
            Some(FrontierCurrent(())) => Ok(MaterializedLog::FrontierCurrent),
            Some(PeekCurrent(())) => Ok(MaterializedLog::PeekCurrent),
            Some(PeekDuration(())) => Ok(MaterializedLog::PeekDuration),
            None => Err(TryFromProtoError::missing_field(
                "ProtoMaterializedLog::kind",
            )),
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
                .with_column("worker", ScalarType::Int64.nullable(false)),

            LogVariant::Timely(TimelyLog::Histogram) => RelationDesc::empty()
                .with_column("id", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("duration_ns", ScalarType::Int64.nullable(false)),

            LogVariant::Timely(TimelyLog::Addresses) => RelationDesc::empty()
                .with_column("id", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column(
                    "address",
                    ScalarType::List {
                        element_type: Box::new(ScalarType::Int64),
                        custom_id: None,
                    }
                    .nullable(false),
                )
                .with_key(vec![0, 1]),

            LogVariant::Timely(TimelyLog::Parks) => RelationDesc::empty()
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("slept_for", ScalarType::Int64.nullable(false))
                .with_column("requested", ScalarType::Int64.nullable(false)),

            LogVariant::Timely(TimelyLog::MessagesReceived) => RelationDesc::empty()
                .with_column("channel", ScalarType::Int64.nullable(false))
                .with_column("source_worker", ScalarType::Int64.nullable(false))
                .with_column("target_worker", ScalarType::Int64.nullable(false)),

            LogVariant::Timely(TimelyLog::MessagesSent) => RelationDesc::empty()
                .with_column("channel", ScalarType::Int64.nullable(false))
                .with_column("source_worker", ScalarType::Int64.nullable(false))
                .with_column("target_worker", ScalarType::Int64.nullable(false)),

            LogVariant::Timely(TimelyLog::Reachability) => RelationDesc::empty()
                .with_column(
                    "address",
                    ScalarType::List {
                        element_type: Box::new(ScalarType::Int64),
                        custom_id: None,
                    }
                    .nullable(false),
                )
                .with_column("port", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("update_type", ScalarType::String.nullable(false))
                .with_column("timestamp", ScalarType::Int64.nullable(true)),

            LogVariant::Differential(DifferentialLog::ArrangementBatches)
            | LogVariant::Differential(DifferentialLog::ArrangementRecords)
            | LogVariant::Differential(DifferentialLog::Sharing) => RelationDesc::empty()
                .with_column("operator", ScalarType::Int64.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false)),

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
                .with_column("id", ScalarType::Uuid.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("index_id", ScalarType::String.nullable(false))
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
            | LogVariant::Differential(DifferentialLog::Sharing) => vec![(
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

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn logging_config_protobuf_roundtrip(expect in any::<LoggingConfig>()) {
            let actual = protobuf_roundtrip::<_, ProtoLoggingConfig>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
