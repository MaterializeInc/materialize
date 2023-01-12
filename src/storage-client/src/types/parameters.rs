// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configuration parameter types.

use std::time::Duration;

use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

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
    /// Persist client configuration.
    pub persist: PersistParameters,
}

impl StorageParameters {
    /// Update the parameter values with the set ones from `other`.
    pub fn update(&mut self, other: StorageParameters) {
        self.persist.update(other.persist);
    }
}

impl RustType<ProtoStorageParameters> for StorageParameters {
    fn into_proto(&self) -> ProtoStorageParameters {
        ProtoStorageParameters {
            persist: Some(self.persist.into_proto()),
        }
    }

    fn from_proto(proto: ProtoStorageParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            persist: proto
                .persist
                .into_rust_if_some("ProtoStorageParameters::persist")?,
        })
    }
}

/// Persist configuration parameters.
///
/// Parameters can be set (`Some`) or unset (`None`).
/// Unset parameters should be interpreted to mean "use the previous value".
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct PersistParameters {
    /// Configures [`mz_persist_client::PersistConfig::blob_target_size`].
    pub blob_target_size: Option<usize>,
    /// Configures [`mz_persist_client::PersistConfig::compaction_minimum_timeout`].
    pub compaction_minimum_timeout: Option<Duration>,
}

impl PersistParameters {
    /// Update the parameter values with the set ones from `other`.
    pub fn update(&mut self, other: PersistParameters) {
        if let Some(v) = other.blob_target_size {
            self.blob_target_size = Some(v);
        }
        if let Some(v) = other.compaction_minimum_timeout {
            self.compaction_minimum_timeout = Some(v);
        }
    }

    /// Return whether all parameters are unset.
    pub fn all_unset(&self) -> bool {
        self.blob_target_size.is_none() && self.compaction_minimum_timeout.is_none()
    }
}

impl RustType<ProtoPersistParameters> for PersistParameters {
    fn into_proto(&self) -> ProtoPersistParameters {
        ProtoPersistParameters {
            blob_target_size: self.blob_target_size.into_proto(),
            compaction_minimum_timeout: self.compaction_minimum_timeout.into_proto(),
        }
    }

    fn from_proto(proto: ProtoPersistParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            blob_target_size: proto.blob_target_size.into_rust()?,
            compaction_minimum_timeout: proto.compaction_minimum_timeout.into_rust()?,
        })
    }
}
