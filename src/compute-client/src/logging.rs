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

use once_cell::sync::Lazy;
use proptest::prelude::{any, prop, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc, ScalarType};
use mz_storage_client::controller::CollectionMetadata;

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.logging.rs"));

/// Logging configuration.
///
/// Setting `enable_logging` to `false` specifies that logging is disabled.
//
// Ideally we'd want to instead signal disabled logging by leaving both `index_logs` and
// `sink_logs` empty. Unfortunately, we have to always provide `index_logs`, because we must
// install the logging dataflows even on replicas that have logging disabled. See #15799.
// TODO(teskje): Clean this up once we remove the arranged introspection sources.
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
    /// Logs to be written to persist
    pub sink_logs: BTreeMap<LogVariant, (GlobalId, CollectionMetadata)>,
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
            prop::collection::btree_map(
                any::<LogVariant>(),
                any::<(GlobalId, CollectionMetadata)>(),
                0..2,
            ),
        )
            .prop_map(
                |(interval, enable_logging, log_logging, index_logs, sink_logs)| LoggingConfig {
                    interval,
                    enable_logging,
                    log_logging,
                    index_logs,
                    sink_logs,
                },
            )
            .boxed()
    }
}

impl LoggingConfig {
    /// Announce the identifiers the logging config will populate.
    pub fn log_identifiers<'a>(&'a self) -> impl Iterator<Item = GlobalId> + 'a {
        let it1 = self.index_logs.values().cloned();
        let it2 = self.sink_logs.values().map(|(id, _)| *id);
        it1.chain(it2)
    }
}

impl RustType<ProtoLoggingConfig> for LoggingConfig {
    fn into_proto(&self) -> ProtoLoggingConfig {
        ProtoLoggingConfig {
            interval: Some(self.interval.into_proto()),
            enable_logging: self.enable_logging,
            log_logging: self.log_logging,
            index_logs: self.index_logs.into_proto(),
            sink_logs: self.sink_logs.into_proto(),
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
            sink_logs: proto.sink_logs.into_rust()?,
        })
    }
}

impl ProtoMapEntry<LogVariant, (GlobalId, CollectionMetadata)> for ProtoSinkLog {
    fn from_rust<'a>(
        (variant, (id, meta)): (&'a LogVariant, &'a (GlobalId, CollectionMetadata)),
    ) -> Self {
        Self {
            key: Some(variant.into_proto()),
            value_id: Some(id.into_proto()),
            value_meta: Some(meta.into_proto()),
        }
    }

    fn into_rust(
        self,
    ) -> std::result::Result<(LogVariant, (GlobalId, CollectionMetadata)), TryFromProtoError> {
        Ok((
            self.key.into_rust_if_some("ProtoSinkLog::key")?,
            (
                self.value_id.into_rust_if_some("ProtoSinkLog::value_id")?,
                self.value_meta
                    .into_rust_if_some("ProtoSinkLog::value_meta")?,
            ),
        ))
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

/// Create a VIEW over the postfixed introspection sources. These views are created and torn down
/// with replicas.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum LogView {
    MzArrangementSharing,
    MzArrangementSizes,
    MzComputeOperatorDurations,
    MzDataflowNames,
    MzDataflowOperatorDataflows,
    MzDataflowOperatorReachability,
    MzComputeFrontiers,
    MzComputeImportFrontiers,
    MzMessageCounts,
    MzPeekDurations,
    MzRawComputeOperatorDurations,
    MzRecordsPerDataflowOperator,
    MzRecordsPerDataflow,
    MzRecordsPerDataflowGlobal,
    MzSchedulingElapsed,
    MzSchedulingParks,
    MzWorkerComputeDelays,
}

pub static DEFAULT_LOG_VIEWS: Lazy<Vec<LogView>> = Lazy::new(|| {
    // Order matters, if view A depends on view B, A must be listed before B
    vec![
        LogView::MzArrangementSharing,
        LogView::MzArrangementSizes,
        LogView::MzDataflowNames,
        LogView::MzDataflowOperatorDataflows,
        LogView::MzDataflowOperatorReachability,
        LogView::MzComputeFrontiers,
        LogView::MzComputeImportFrontiers,
        LogView::MzMessageCounts,
        LogView::MzPeekDurations,
        LogView::MzRecordsPerDataflowOperator,
        LogView::MzRecordsPerDataflow,
        LogView::MzRecordsPerDataflowGlobal,
        LogView::MzSchedulingElapsed,
        LogView::MzRawComputeOperatorDurations,
        LogView::MzComputeOperatorDurations,
        LogView::MzSchedulingParks,
        LogView::MzWorkerComputeDelays,
    ]
});

impl LogView {
    pub fn get_template(&self) -> (&str, &str) {
        match self {
            LogView::MzArrangementSharing => (
                "SELECT
                    operator_id,
                    worker_id,
                    pg_catalog.count(*) AS count
                FROM mz_internal.mz_arrangement_sharing_internal_{}
                GROUP BY operator_id, worker_id",
                "mz_arrangement_sharing_{}",
            ),

            LogView::MzArrangementSizes => (
                "WITH batches_cte AS (
                    SELECT
                        operator_id,
                        worker_id,
                        pg_catalog.count(*) AS batches
                    FROM
                        mz_internal.mz_arrangement_batches_internal_{}
                    GROUP BY
                        operator_id, worker_id
                ),
                records_cte AS (
                    SELECT
                        operator_id,
                        worker_id,
                        pg_catalog.count(*) AS records
                    FROM
                        mz_internal.mz_arrangement_records_internal_{}
                    GROUP BY
                        operator_id, worker_id
                )
                SELECT
                    batches_cte.operator_id,
                    batches_cte.worker_id,
                    records_cte.records,
                    batches_cte.batches
                FROM batches_cte JOIN records_cte USING (operator_id, worker_id)",
                "mz_arrangement_sizes_{}",
            ),

            LogView::MzDataflowNames => (
                "SELECT mz_dataflow_addresses_{}.id,
                        mz_dataflow_addresses_{}.worker_id,
                        mz_dataflow_addresses_{}.address[1] AS local_id,
                        mz_dataflow_operators_{}.name
                 FROM
                        mz_internal.mz_dataflow_addresses_{},
                        mz_internal.mz_dataflow_operators_{}
                 WHERE
                        mz_dataflow_addresses_{}.id = mz_dataflow_operators_{}.id AND
                        mz_dataflow_addresses_{}.worker_id = mz_dataflow_operators_{}.worker_id AND
                        mz_catalog.list_length(mz_dataflow_addresses_{}.address) = 1",
                "mz_dataflows_{}",
            ),

            LogView::MzDataflowOperatorDataflows => (
                "SELECT
                    mz_dataflow_operators_{}.id,
                    mz_dataflow_operators_{}.name,
                    mz_dataflow_operators_{}.worker_id,
                    mz_dataflows_{}.id as dataflow_id,
                    mz_dataflows_{}.name as dataflow_name
                FROM
                    mz_internal.mz_dataflow_operators_{},
                    mz_internal.mz_dataflow_addresses_{},
                    mz_internal.mz_dataflows_{}
                WHERE
                    mz_dataflow_operators_{}.id = mz_dataflow_addresses_{}.id AND
                    mz_dataflow_operators_{}.worker_id = mz_dataflow_addresses_{}.worker_id AND
                    mz_dataflows_{}.local_id = mz_dataflow_addresses_{}.address[1] AND
                    mz_dataflows_{}.worker_id = mz_dataflow_addresses_{}.worker_id",
                "mz_dataflow_operator_dataflows_{}",
            ),

            LogView::MzDataflowOperatorReachability => (
                "SELECT
                    address,
                    port,
                    worker_id,
                    update_type,
                    time,
                    pg_catalog.count(*) as count
                 FROM
                    mz_internal.mz_dataflow_operator_reachability_internal_{}
                 GROUP BY address, port, worker_id, update_type, time",
                "mz_dataflow_operator_reachability_{}",
            ),

            LogView::MzComputeFrontiers => (
                "SELECT
                    export_id, pg_catalog.min(time) AS time
                FROM mz_internal.mz_worker_compute_frontiers_{}
                GROUP BY export_id",
                "mz_compute_frontiers_{}",
            ),

            LogView::MzComputeImportFrontiers => (
                "SELECT
                    export_id, import_id, pg_catalog.min(time) AS time
                FROM mz_internal.mz_worker_compute_import_frontiers_{}
                GROUP BY export_id, import_id",
                "mz_compute_import_frontiers_{}",
            ),

            LogView::MzMessageCounts => (
                "WITH sent_cte AS (
                    SELECT
                        channel_id,
                        from_worker_id,
                        to_worker_id,
                        pg_catalog.count(*) AS sent
                    FROM
                        mz_internal.mz_message_counts_sent_internal_{}
                    GROUP BY
                        channel_id, from_worker_id, to_worker_id
                ),
                received_cte AS (
                    SELECT
                        channel_id,
                        from_worker_id,
                        to_worker_id,
                        pg_catalog.count(*) AS received
                    FROM
                        mz_internal.mz_message_counts_received_internal_{}
                    GROUP BY
                        channel_id, from_worker_id, to_worker_id
                )
                SELECT
                    sent_cte.channel_id,
                    sent_cte.from_worker_id,
                    sent_cte.to_worker_id,
                    sent_cte.sent,
                    received_cte.received
                FROM sent_cte JOIN received_cte USING (channel_id, from_worker_id, to_worker_id)",
                "mz_message_counts_{}",
            ),

            LogView::MzPeekDurations => (
                "SELECT
                    worker_id,
                    duration_ns/1000 * '1 microsecond'::interval AS duration,
                    count
                FROM
                    mz_internal.mz_raw_peek_durations_{}",
                "mz_peek_durations_{}",
            ),

            LogView::MzRecordsPerDataflowOperator => (
                "WITH records_cte AS (
                    SELECT
                        operator_id,
                        worker_id,
                        pg_catalog.count(*) AS records
                    FROM
                        mz_internal.mz_arrangement_records_internal_{}
                    GROUP BY
                        operator_id, worker_id
                )
                SELECT
                    mz_dataflow_operator_dataflows_{}.id,
                    mz_dataflow_operator_dataflows_{}.name,
                    mz_dataflow_operator_dataflows_{}.worker_id,
                    mz_dataflow_operator_dataflows_{}.dataflow_id,
                    records_cte.records
                FROM
                    records_cte,
                    mz_internal.mz_dataflow_operator_dataflows_{}
                WHERE
                    mz_dataflow_operator_dataflows_{}.id = records_cte.operator_id AND
                    mz_dataflow_operator_dataflows_{}.worker_id = records_cte.worker_id",
                "mz_records_per_dataflow_operator_{}",
            ),

            LogView::MzRecordsPerDataflow => (
                "SELECT
                    mz_records_per_dataflow_operator_{}.dataflow_id as id,
                    mz_dataflows_{}.name,
                    mz_records_per_dataflow_operator_{}.worker_id,
                    pg_catalog.SUM(mz_records_per_dataflow_operator_{}.records) as records
                FROM
                    mz_internal.mz_records_per_dataflow_operator_{},
                    mz_internal.mz_dataflows_{}
                WHERE
                    mz_records_per_dataflow_operator_{}.dataflow_id = mz_dataflows_{}.id AND
                    mz_records_per_dataflow_operator_{}.worker_id = mz_dataflows_{}.worker_id
                GROUP BY
                    mz_records_per_dataflow_operator_{}.dataflow_id,
                    mz_dataflows_{}.name,
                    mz_records_per_dataflow_operator_{}.worker_id",
                "mz_records_per_dataflow_{}",
            ),

            LogView::MzRecordsPerDataflowGlobal => (
                "SELECT
                    mz_records_per_dataflow_{}.id,
                    mz_records_per_dataflow_{}.name,
                    pg_catalog.SUM(mz_records_per_dataflow_{}.records) as records
                FROM
                    mz_internal.mz_records_per_dataflow_{}
                GROUP BY
                    mz_records_per_dataflow_{}.id,
                    mz_records_per_dataflow_{}.name",
                "mz_records_per_dataflow_global_{}",
            ),

            LogView::MzSchedulingElapsed => (
                "SELECT
                    id, worker_id, pg_catalog.count(*) AS elapsed_ns
                FROM
                    mz_internal.mz_scheduling_elapsed_internal_{}
                GROUP BY
                    id, worker_id",
                "mz_scheduling_elapsed_{}",
            ),

            LogView::MzRawComputeOperatorDurations => (
                "SELECT
                    id, worker_id, duration_ns, pg_catalog.count(*) AS count
                FROM
                    mz_internal.mz_raw_compute_operator_durations_internal_{}
                GROUP BY
                    id, worker_id, duration_ns",
                "mz_raw_compute_operator_durations_{}",
            ),

            LogView::MzComputeOperatorDurations => (
                "SELECT
                    id,
                    worker_id,
                    duration_ns/1000 * '1 microsecond'::interval AS duration,
                    count
                FROM
                    mz_internal.mz_raw_compute_operator_durations_{}",
                "mz_compute_operator_durations_{}",
            ),

            LogView::MzSchedulingParks => (
                "SELECT
                    worker_id, slept_for, requested, pg_catalog.count(*) AS count
                FROM
                    mz_internal.mz_scheduling_parks_internal_{}
                GROUP BY
                    worker_id, slept_for, requested",
                "mz_scheduling_parks_{}",
            ),

            LogView::MzWorkerComputeDelays => (
                "SELECT
                    export_id,
                    import_id,
                    worker_id,
                    delay_ns/1000 * '1 microsecond'::interval AS delay,
                    count
                FROM
                    mz_internal.mz_raw_worker_compute_delays_{}",
                "mz_worker_compute_delays_{}",
            ),
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
                .with_column("slept_for", ScalarType::UInt64.nullable(false))
                .with_column("requested", ScalarType::UInt64.nullable(false)),

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
            | LogVariant::Differential(DifferentialLog::Sharing) => RelationDesc::empty()
                .with_column("operator_id", ScalarType::UInt64.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false)),

            LogVariant::Compute(ComputeLog::DataflowCurrent) => RelationDesc::empty()
                .with_column("export_id", ScalarType::String.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
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
                .with_column("delay_ns", ScalarType::UInt64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_column("sum", ScalarType::UInt64.nullable(true))
                .with_key(vec![0, 1, 2, 3]),

            LogVariant::Compute(ComputeLog::PeekCurrent) => RelationDesc::empty()
                .with_column("id", ScalarType::Uuid.nullable(false))
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("index_id", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::MzTimestamp.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Compute(ComputeLog::PeekDuration) => RelationDesc::empty()
                .with_column("worker_id", ScalarType::UInt64.nullable(false))
                .with_column("duration_ns", ScalarType::UInt64.nullable(false))
                .with_column("count", ScalarType::UInt64.nullable(false))
                .with_column("sum", ScalarType::UInt64.nullable(true))
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
    use super::*;
    use mz_proto::protobuf_roundtrip;
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
