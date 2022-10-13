// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to storage hosts.

use std::num::NonZeroUsize;

use proptest::prelude::any;
use proptest::prelude::Arbitrary;
use proptest::strategy::BoxedStrategy;
use proptest::strategy::Strategy;
use serde::{Deserialize, Serialize};

use mz_orchestrator::{CpuLimit, MemoryLimit};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_storage.types.hosts.rs"));

/// Resource allocations for a storage host.
///
/// Has some overlap with mz_compute_client::controller::ComputeReplicaAllocation,
/// but keeping it separate for now due slightly different semantics.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct StorageHostResourceAllocation {
    /// The memory limit for each process in the replica.
    pub memory_limit: Option<MemoryLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of worker threads in the replica.
    pub workers: NonZeroUsize,
}

impl RustType<ProtoStorageHostResourceAllocation> for StorageHostResourceAllocation {
    fn into_proto(&self) -> ProtoStorageHostResourceAllocation {
        ProtoStorageHostResourceAllocation {
            memory_limit: self.memory_limit.into_proto(),
            cpu_limit: self.cpu_limit.into_proto(),
            workers: self.workers.into_proto(),
        }
    }

    fn from_proto(proto: ProtoStorageHostResourceAllocation) -> Result<Self, TryFromProtoError> {
        Ok(StorageHostResourceAllocation {
            memory_limit: proto.memory_limit.into_rust()?,
            cpu_limit: proto.cpu_limit.into_rust()?,
            workers: proto.workers.into_rust()?,
        })
    }
}

/// Size or address of a storage instance
///
/// This represents how resources for a storage instance are going to be
/// provisioned.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageHostConfig {
    /// Remote unmanaged storage
    Remote {
        /// The network addresses of the storaged process.
        addr: String,
    },
    /// A remote but managed replica
    Managed {
        /// The resource allocation for the replica.
        allocation: StorageHostResourceAllocation,
        /// SQL size parameter used for allocation
        size: String,
    },
}

impl RustType<ProtoStorageHostConfig> for StorageHostConfig {
    fn into_proto(&self) -> ProtoStorageHostConfig {
        use proto_storage_host_config::*;
        ProtoStorageHostConfig {
            kind: Some(match self {
                StorageHostConfig::Remote { addr } => Kind::Remote(ProtoStorageHostConfigRemote {
                    addr: addr.into_proto(),
                }),
                StorageHostConfig::Managed { allocation, size } => {
                    Kind::Managed(ProtoStorageHostConfigManaged {
                        allocation: Some(allocation.into_proto()),
                        size: size.into_proto(),
                    })
                }
            }),
        }
    }

    fn from_proto(proto: ProtoStorageHostConfig) -> Result<Self, TryFromProtoError> {
        use proto_storage_host_config::*;
        Ok(
            match proto
                .kind
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoStorageHostConfig::kind"))?
            {
                Kind::Remote(ProtoStorageHostConfigRemote { addr }) => {
                    StorageHostConfig::Remote { addr }
                }
                Kind::Managed(ProtoStorageHostConfigManaged { allocation, size }) => {
                    StorageHostConfig::Managed {
                        allocation: allocation
                            .into_rust_if_some("ProtoStorageHostConfigManaged::allocation")?,
                        size,
                    }
                }
            },
        )
    }
}

impl Arbitrary for StorageHostConfig {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (any::<String>())
            .prop_map(|addr| Self::Remote { addr })
            .boxed()
    }
}

impl StorageHostConfig {
    /// Returns the size specified by the storage host configuration, if
    /// the storage host is a managed storage host.
    pub fn size(&self) -> Option<&str> {
        match self {
            StorageHostConfig::Remote { .. } => None,
            StorageHostConfig::Managed { size, .. } => Some(size),
        }
    }
}
