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

use once_cell::sync::Lazy;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc, ScalarType};
use mz_storage::controller::CollectionMetadata;

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.logging.rs"));

/// Logging configuration.
#[derive(Arbitrary, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub granularity_ns: u128,
    /// Logs to keep in an arrangement
    pub active_logs: BTreeMap<LogVariant, GlobalId>,
    /// Whether we should report logs for the log-processing dataflows
    pub log_logging: bool,
    /// Logs to be written to persist
    pub sink_logs: BTreeMap<LogVariant, (GlobalId, CollectionMetadata)>,
}

impl LoggingConfig {
    /// Announce the identifiers the logging config will populate.
    pub fn log_identifiers<'a>(&'a self) -> impl Iterator<Item = GlobalId> + 'a {
        let it1 = self.active_logs.values().cloned();
        let it2 = self.sink_logs.values().map(|(id, _)| *id);
        it1.chain(it2)
    }
}

impl RustType<ProtoLoggingConfig> for LoggingConfig {
    fn into_proto(&self) -> ProtoLoggingConfig {
        ProtoLoggingConfig {
            granularity_ns: Some(self.granularity_ns.into_proto()),
            active_logs: self.active_logs.into_proto(),
            log_logging: self.log_logging,
            sink_logs: self.sink_logs.into_proto(),
        }
    }

    fn from_proto(proto: ProtoLoggingConfig) -> Result<Self, TryFromProtoError> {
        Ok(LoggingConfig {
            granularity_ns: proto
                .granularity_ns
                .into_rust_if_some("ProtoLoggingConfig::granularity_ns")?,
            active_logs: proto.active_logs.into_rust()?,
            log_logging: proto.log_logging,
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

impl ProtoMapEntry<LogVariant, GlobalId> for ProtoActiveLog {
    fn from_rust<'a>(entry: (&'a LogVariant, &'a GlobalId)) -> Self {
        ProtoActiveLog {
            key: Some(entry.0.into_proto()),
            value: Some(entry.1.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(LogVariant, GlobalId), TryFromProtoError> {
        Ok((
            self.key.into_rust_if_some("ProtoActiveLog::key")?,
            self.value.into_rust_if_some("ProtoActiveLog::value")?,
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
    SourceFrontierCurrent,
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
                ComputeLog::SourceFrontierCurrent => SourceFrontierCurrent(()),
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
            Some(SourceFrontierCurrent(())) => Ok(ComputeLog::SourceFrontierCurrent),
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
        LogVariant::Compute(ComputeLog::SourceFrontierCurrent),
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
    MzDataflowNames,
    MzDataflowOperatorDataflows,
    MzDataflowOperatorReachability,
    MzMaterializationFrontiers,
    MzMessageCounts,
    MzRecordsPerDataflowOperator,
    MzRecordsPerDataflow,
    MzRecordsPerDataflowGlobal,
    MzSchedulingElapsed,
    MzSchedulingHistogram,
    MzSchedulingParks,
}

pub static DEFAULT_LOG_VIEWS: Lazy<Vec<LogView>> = Lazy::new(|| {
    // Order matters, if view A depends on view B, A must be listed before B
    vec![
        LogView::MzArrangementSharing,
        LogView::MzArrangementSizes,
        LogView::MzDataflowNames,
        LogView::MzDataflowOperatorDataflows,
        LogView::MzDataflowOperatorReachability,
        LogView::MzMaterializationFrontiers,
        LogView::MzMessageCounts,
        LogView::MzRecordsPerDataflowOperator,
        LogView::MzRecordsPerDataflow,
        LogView::MzRecordsPerDataflowGlobal,
        LogView::MzSchedulingElapsed,
        LogView::MzSchedulingHistogram,
        LogView::MzSchedulingParks,
    ]
});

impl LogView {
    pub fn get_template(&self) -> (&str, &str) {
        match self {
            LogView::MzArrangementSharing => (
                "SELECT
                    operator,
                    worker,
                    pg_catalog.count(*) AS count
                FROM mz_catalog.mz_arrangement_sharing_internal_{}
                GROUP BY operator, worker",
                "mz_arrangement_sharing_{}",
            ),

            LogView::MzArrangementSizes => (
                "WITH batches_cte AS (
                    SELECT
                        operator,
                        worker,
                        pg_catalog.count(*) AS batches
                    FROM
                        mz_catalog.mz_arrangement_batches_internal_{}
                    GROUP BY
                        operator, worker
                ),
                records_cte AS (
                    SELECT
                        operator,
                        worker,
                        pg_catalog.count(*) AS records
                    FROM
                        mz_catalog.mz_arrangement_records_internal_{}
                    GROUP BY
                        operator, worker
                )
                SELECT
                    batches_cte.operator,
                    batches_cte.worker,
                    records_cte.records,
                    batches_cte.batches
                FROM batches_cte JOIN records_cte USING (operator, worker)",
                "mz_arrangement_sizes_{}",
            ),

            LogView::MzDataflowNames => (
                "SELECT mz_dataflow_addresses_{}.id,
                        mz_dataflow_addresses_{}.worker,
                        mz_dataflow_addresses_{}.address[1] AS local_id,
                        mz_dataflow_operators_{}.name
                 FROM
                        mz_catalog.mz_dataflow_addresses_{},
                        mz_catalog.mz_dataflow_operators_{}
                 WHERE
                        mz_dataflow_addresses_{}.id = mz_dataflow_operators_{}.id AND
                        mz_dataflow_addresses_{}.worker = mz_dataflow_operators_{}.worker AND
                        mz_catalog.list_length(mz_dataflow_addresses_{}.address) = 1",
                "mz_dataflow_names_{}",
            ),

            LogView::MzDataflowOperatorDataflows => (
                "SELECT
                    mz_dataflow_operators_{}.id,
                    mz_dataflow_operators_{}.name,
                    mz_dataflow_operators_{}.worker,
                    mz_dataflow_names_{}.id as dataflow_id,
                    mz_dataflow_names_{}.name as dataflow_name
                FROM
                    mz_catalog.mz_dataflow_operators_{},
                    mz_catalog.mz_dataflow_addresses_{},
                    mz_catalog.mz_dataflow_names_{}
                WHERE
                    mz_dataflow_operators_{}.id = mz_dataflow_addresses_{}.id AND
                    mz_dataflow_operators_{}.worker = mz_dataflow_addresses_{}.worker AND
                    mz_dataflow_names_{}.local_id = mz_dataflow_addresses_{}.address[1] AND
                    mz_dataflow_names_{}.worker = mz_dataflow_addresses_{}.worker",
                "mz_dataflow_operator_dataflows_{}",
            ),

            LogView::MzDataflowOperatorReachability => (
                "SELECT
                    address,
                    port,
                    worker,
                    update_type,
                    timestamp,
                    pg_catalog.count(*) as count
                 FROM
                    mz_catalog.mz_dataflow_operator_reachability_internal_{}
                 GROUP BY address, port, worker, update_type, timestamp",
                "mz_dataflow_operator_reachability_{}",
            ),

            LogView::MzMaterializationFrontiers => (
                "SELECT
                    global_id, pg_catalog.min(time) AS time
                FROM mz_catalog.mz_worker_materialization_frontiers_{}
                GROUP BY global_id",
                "mz_materialization_frontiers_{}",
            ),

            LogView::MzMessageCounts => (
                "WITH sent_cte AS (
                    SELECT
                        channel,
                        source_worker,
                        target_worker,
                        pg_catalog.count(*) AS sent
                    FROM
                        mz_catalog.mz_message_counts_sent_internal_{}
                    GROUP BY
                        channel, source_worker, target_worker
                ),
                received_cte AS (
                    SELECT
                        channel,
                        source_worker,
                        target_worker,
                        pg_catalog.count(*) AS received
                    FROM
                        mz_catalog.mz_message_counts_received_internal_{}
                    GROUP BY
                        channel, source_worker, target_worker
                )
                SELECT
                    sent_cte.channel,
                    sent_cte.source_worker,
                    sent_cte.target_worker,
                    sent_cte.sent,
                    received_cte.received
                FROM sent_cte JOIN received_cte USING (channel, source_worker, target_worker)",
                "mz_message_counts_{}",
            ),

            LogView::MzRecordsPerDataflowOperator => (
                "WITH records_cte AS (
                    SELECT
                        operator,
                        worker,
                        pg_catalog.count(*) AS records
                    FROM
                        mz_catalog.mz_arrangement_records_internal_{}
                    GROUP BY
                        operator, worker
                )
                SELECT
                    mz_dataflow_operator_dataflows_{}.id,
                    mz_dataflow_operator_dataflows_{}.name,
                    mz_dataflow_operator_dataflows_{}.worker,
                    mz_dataflow_operator_dataflows_{}.dataflow_id,
                    records_cte.records
                FROM
                    records_cte,
                    mz_catalog.mz_dataflow_operator_dataflows_{}
                WHERE
                    mz_dataflow_operator_dataflows_{}.id = records_cte.operator AND
                    mz_dataflow_operator_dataflows_{}.worker = records_cte.worker",
                "mz_records_per_dataflow_operator_{}",
            ),

            LogView::MzRecordsPerDataflow => (
                "SELECT
                    mz_records_per_dataflow_operator_{}.dataflow_id as id,
                    mz_dataflow_names_{}.name,
                    mz_records_per_dataflow_operator_{}.worker,
                    pg_catalog.SUM(mz_records_per_dataflow_operator_{}.records) as records
                FROM
                    mz_catalog.mz_records_per_dataflow_operator_{},
                    mz_catalog.mz_dataflow_names_{}
                WHERE
                    mz_records_per_dataflow_operator_{}.dataflow_id = mz_dataflow_names_{}.id AND
                    mz_records_per_dataflow_operator_{}.worker = mz_dataflow_names_{}.worker
                GROUP BY
                    mz_records_per_dataflow_operator_{}.dataflow_id,
                    mz_dataflow_names_{}.name,
                    mz_records_per_dataflow_operator_{}.worker",
                "mz_records_per_dataflow_{}",
            ),

            LogView::MzRecordsPerDataflowGlobal => (
                "SELECT
                    mz_records_per_dataflow_{}.id,
                    mz_records_per_dataflow_{}.name,
                    pg_catalog.SUM(mz_records_per_dataflow_{}.records) as records
                FROM
                    mz_catalog.mz_records_per_dataflow_{}
                GROUP BY
                    mz_records_per_dataflow_{}.id,
                    mz_records_per_dataflow_{}.name",
                "mz_records_per_dataflow_global_{}",
            ),

            LogView::MzSchedulingElapsed => (
                "SELECT
                    id, worker, pg_catalog.count(*) AS elapsed_ns
                FROM
                    mz_catalog.mz_scheduling_elapsed_internal_{}
                GROUP BY
                    id, worker",
                "mz_scheduling_elapsed_{}",
            ),

            LogView::MzSchedulingHistogram => (
                "SELECT
                    id, worker, duration_ns, pg_catalog.count(*) AS count
                FROM
                    mz_catalog.mz_scheduling_histogram_internal_{}
                GROUP BY
                    id, worker, duration_ns",
                "mz_scheduling_histogram_{}",
            ),

            LogView::MzSchedulingParks => (
                "SELECT
                    worker, slept_for, requested, pg_catalog.count(*) AS count
                FROM
                    mz_catalog.mz_scheduling_parks_internal_{}
                GROUP BY
                    worker, slept_for, requested",
                "mz_scheduling_parks_{}",
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

            LogVariant::Compute(ComputeLog::DataflowCurrent) => RelationDesc::empty()
                .with_column("global_id", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Compute(ComputeLog::DataflowDependency) => RelationDesc::empty()
                .with_column("dataflow", ScalarType::String.nullable(false))
                .with_column("source", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false)),

            LogVariant::Compute(ComputeLog::FrontierCurrent) => RelationDesc::empty()
                .with_column("global_id", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("time", ScalarType::Int64.nullable(false)),

            LogVariant::Compute(ComputeLog::SourceFrontierCurrent) => RelationDesc::empty()
                .with_column("global_id", ScalarType::String.nullable(false))
                .with_column("source", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("time", ScalarType::Int64.nullable(false)),

            LogVariant::Compute(ComputeLog::FrontierDelay) => RelationDesc::empty()
                .with_column("global_id", ScalarType::String.nullable(false))
                .with_column("source", ScalarType::String.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("delay_ns", ScalarType::Int64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1, 2, 3]),

            LogVariant::Compute(ComputeLog::PeekCurrent) => RelationDesc::empty()
                .with_column("id", ScalarType::Uuid.nullable(false))
                .with_column("worker", ScalarType::Int64.nullable(false))
                .with_column("index_id", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1]),

            LogVariant::Compute(ComputeLog::PeekDuration) => RelationDesc::empty()
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
            LogVariant::Compute(ComputeLog::DataflowCurrent) => vec![],
            LogVariant::Compute(ComputeLog::DataflowDependency) => vec![],
            LogVariant::Compute(ComputeLog::FrontierCurrent) => vec![],
            LogVariant::Compute(ComputeLog::SourceFrontierCurrent) => vec![],
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
