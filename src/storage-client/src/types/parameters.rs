// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configuration parameter types.

use mz_ore::cast::CastFrom;
use mz_persist_client::cfg::PersistParameters;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_tracing::params::TracingParameters;
use serde::{Deserialize, Serialize};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.parameters.rs"
));

/// Storage instance configuration parameters.
///
/// Parameters can be set (`Some`) or unset (`None`).
/// Unset parameters should be interpreted to mean "use the previous value".
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageParameters {
    /// Persist client configuration.
    pub persist: PersistParameters,
    pub pg_replication_timeouts: mz_postgres_util::ReplicationTimeouts,
    pub keep_n_source_status_history_entries: usize,
    pub keep_n_sink_status_history_entries: usize,
    /// A set of parameters used to tune RocksDB when used with `UPSERT` sources.
    pub upsert_rocksdb_tuning_config: mz_rocksdb_types::RocksDBTuningParameters,
    /// Whether or not to allow shard finalization to occur. Note that this will
    /// only disable the actual finalization of shards, not registering them for
    /// finalization.
    pub finalize_shards: bool,
    pub tracing: TracingParameters,
    /// A set of parameters used configure auto spill behaviour if disk is used.
    pub upsert_auto_spill_config: UpsertAutoSpillConfig,
    pub storage_dataflow_max_inflight_bytes: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct UpsertAutoSpillConfig {
    /// A flag to whether allow automatically spilling to disk or not
    pub allow_spilling_to_disk: bool,
    /// The size in bytes of the upsert state after which rocksdb will be used
    /// instead of in memory hashmap
    pub spill_to_disk_threshold_bytes: usize,
}

impl RustType<ProtoUpsertAutoSpillConfig> for UpsertAutoSpillConfig {
    fn into_proto(&self) -> ProtoUpsertAutoSpillConfig {
        ProtoUpsertAutoSpillConfig {
            allow_spilling_to_disk: self.allow_spilling_to_disk,
            spill_to_disk_threshold_bytes: u64::cast_from(self.spill_to_disk_threshold_bytes),
        }
    }
    fn from_proto(proto: ProtoUpsertAutoSpillConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            allow_spilling_to_disk: proto.allow_spilling_to_disk,
            spill_to_disk_threshold_bytes: usize::cast_from(proto.spill_to_disk_threshold_bytes),
        })
    }
}

impl StorageParameters {
    /// Update the parameter values with the set ones from `other`.
    pub fn update(
        &mut self,
        StorageParameters {
            persist,
            pg_replication_timeouts,
            keep_n_source_status_history_entries,
            keep_n_sink_status_history_entries,
            upsert_rocksdb_tuning_config,
            finalize_shards,
            tracing,
            upsert_auto_spill_config,
            storage_dataflow_max_inflight_bytes,
        }: StorageParameters,
    ) {
        self.persist.update(persist);
        self.pg_replication_timeouts = pg_replication_timeouts;
        self.keep_n_source_status_history_entries = keep_n_source_status_history_entries;
        self.keep_n_sink_status_history_entries = keep_n_sink_status_history_entries;
        self.upsert_rocksdb_tuning_config = upsert_rocksdb_tuning_config;
        self.finalize_shards = finalize_shards;
        self.tracing.update(tracing);
        self.finalize_shards = finalize_shards;
        self.upsert_auto_spill_config = upsert_auto_spill_config;
        self.storage_dataflow_max_inflight_bytes = storage_dataflow_max_inflight_bytes;
    }
}

impl RustType<ProtoStorageParameters> for StorageParameters {
    fn into_proto(&self) -> ProtoStorageParameters {
        ProtoStorageParameters {
            persist: Some(self.persist.into_proto()),
            pg_replication_timeouts: Some(self.pg_replication_timeouts.into_proto()),
            keep_n_source_status_history_entries: u64::cast_from(
                self.keep_n_source_status_history_entries,
            ),
            keep_n_sink_status_history_entries: u64::cast_from(
                self.keep_n_sink_status_history_entries,
            ),
            upsert_rocksdb_tuning_config: Some(self.upsert_rocksdb_tuning_config.into_proto()),
            finalize_shards: self.finalize_shards,
            tracing: Some(self.tracing.into_proto()),
            upsert_auto_spill_config: Some(self.upsert_auto_spill_config.into_proto()),
            storage_dataflow_max_inflight_bytes: self
                .storage_dataflow_max_inflight_bytes
                .map(u64::cast_from),
        }
    }

    fn from_proto(proto: ProtoStorageParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            persist: proto
                .persist
                .into_rust_if_some("ProtoStorageParameters::persist")?,
            pg_replication_timeouts: proto
                .pg_replication_timeouts
                .into_rust_if_some("ProtoStorageParameters::pg_replication_timeouts")?,
            keep_n_source_status_history_entries: usize::cast_from(
                proto.keep_n_source_status_history_entries,
            ),
            keep_n_sink_status_history_entries: usize::cast_from(
                proto.keep_n_sink_status_history_entries,
            ),
            upsert_rocksdb_tuning_config: proto
                .upsert_rocksdb_tuning_config
                .into_rust_if_some("ProtoStorageParameters::upsert_rocksdb_tuning_config")?,
            finalize_shards: proto.finalize_shards,
            tracing: proto
                .tracing
                .into_rust_if_some("ProtoStorageParameters::tracing")?,
            upsert_auto_spill_config: proto
                .upsert_auto_spill_config
                .into_rust_if_some("ProtoStorageParameters::upsert_auto_spill_config")?,
            storage_dataflow_max_inflight_bytes: proto
                .storage_dataflow_max_inflight_bytes
                .map(usize::cast_from),
        })
    }
}

impl RustType<ProtoPgReplicationTimeouts> for mz_postgres_util::ReplicationTimeouts {
    fn into_proto(&self) -> ProtoPgReplicationTimeouts {
        ProtoPgReplicationTimeouts {
            connect_timeout: self.connect_timeout.into_proto(),
            keepalives_retries: self.keepalives_retries,
            keepalives_idle: self.keepalives_idle.into_proto(),
            keepalives_interval: self.keepalives_interval.into_proto(),
            tcp_user_timeout: self.tcp_user_timeout.into_proto(),
        }
    }

    fn from_proto(proto: ProtoPgReplicationTimeouts) -> Result<Self, TryFromProtoError> {
        Ok(mz_postgres_util::ReplicationTimeouts {
            connect_timeout: proto.connect_timeout.into_rust()?,
            keepalives_retries: proto.keepalives_retries,
            keepalives_idle: proto.keepalives_idle.into_rust()?,
            keepalives_interval: proto.keepalives_interval.into_rust()?,
            tcp_user_timeout: proto.tcp_user_timeout.into_rust()?,
        })
    }
}
