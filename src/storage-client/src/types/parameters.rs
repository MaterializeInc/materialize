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

use mz_persist_client::cfg::PersistParameters;
use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};

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
