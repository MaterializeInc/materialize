// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configuration parameter types.

use serde::{Deserialize, Serialize};

use mz_ore::cast::CastFrom;
use mz_persist_client::cfg::PersistParameters;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.parameters.rs"
));

/// Storage instance configuration parameters.
///
/// Parameters can be set (`Some`) or unset (`None`).
/// Unset parameters should be interpreted to mean "use the previous value".
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageParameters {
    /// Controls whether or not to use the new storage `persist_sink` implementation in storage
    /// ingestions.
    pub enable_multi_worker_storage_persist_sink: bool,
    /// Persist client configuration.
    pub persist: PersistParameters,
    pub pg_replication_timeouts: mz_postgres_util::ReplicationTimeouts,
    pub keep_n_source_status_history_entries: usize,
}

impl StorageParameters {
    /// Update the parameter values with the set ones from `other`.
    pub fn update(
        &mut self,
        StorageParameters {
            enable_multi_worker_storage_persist_sink,
            persist,
            pg_replication_timeouts,
            keep_n_source_status_history_entries,
        }: StorageParameters,
    ) {
        self.enable_multi_worker_storage_persist_sink = enable_multi_worker_storage_persist_sink;
        self.persist.update(persist);
        self.pg_replication_timeouts = pg_replication_timeouts;
        self.keep_n_source_status_history_entries = keep_n_source_status_history_entries;
    }
}

impl RustType<ProtoStorageParameters> for StorageParameters {
    fn into_proto(&self) -> ProtoStorageParameters {
        ProtoStorageParameters {
            enable_multi_worker_storage_persist_sink: self.enable_multi_worker_storage_persist_sink,
            persist: Some(self.persist.into_proto()),
            pg_replication_timeouts: Some(self.pg_replication_timeouts.into_proto()),
            keep_n_source_status_history_entries: u64::cast_from(
                self.keep_n_source_status_history_entries,
            ),
        }
    }

    fn from_proto(proto: ProtoStorageParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            enable_multi_worker_storage_persist_sink: proto
                .enable_multi_worker_storage_persist_sink,
            persist: proto
                .persist
                .into_rust_if_some("ProtoStorageParameters::persist")?,
            pg_replication_timeouts: proto
                .pg_replication_timeouts
                .into_rust_if_some("ProtoStorageParameters::pg_replication_timeouts")?,
            keep_n_source_status_history_entries: usize::cast_from(
                proto.keep_n_source_status_history_entries,
            ),
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
