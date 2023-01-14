// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to storage Clusters.

use std::num::NonZeroUsize;

use proptest::prelude::any;
use proptest::prelude::Arbitrary;
use proptest::strategy::BoxedStrategy;
use proptest::strategy::Strategy;
use serde::{Deserialize, Serialize};

use mz_orchestrator::{CpuLimit, MemoryLimit};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.clusters.rs"
));

/// Resource allocations for a storage cluster.
///
/// Has some overlap with mz_compute_client::controller::ComputeReplicaAllocation,
/// but keeping it separate for now due slightly different semantics.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct StorageClusterResourceAllocation {
    /// The memory limit for each process in the replica.
    pub memory_limit: Option<MemoryLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of worker threads in the replica.
    pub workers: NonZeroUsize,
}

impl RustType<ProtoStorageClusterResourceAllocation> for StorageClusterResourceAllocation {
    fn into_proto(&self) -> ProtoStorageClusterResourceAllocation {
        ProtoStorageClusterResourceAllocation {
            memory_limit: self.memory_limit.into_proto(),
            cpu_limit: self.cpu_limit.into_proto(),
            workers: self.workers.into_proto(),
        }
    }

    fn from_proto(proto: ProtoStorageClusterResourceAllocation) -> Result<Self, TryFromProtoError> {
        Ok(StorageClusterResourceAllocation {
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
pub enum StorageClusterConfig {
    /// Remote unmanaged storage
    Remote {
        /// The network address of the clusterd process.
        addr: String,
    },
    /// A remote but managed replica
    Managed {
        /// The resource allocation for the replica.
        allocation: StorageClusterResourceAllocation,
        /// SQL size parameter used for allocation
        size: String,
    },
}

impl RustType<ProtoStorageClusterConfig> for StorageClusterConfig {
    fn into_proto(&self) -> ProtoStorageClusterConfig {
        use proto_storage_cluster_config::*;
        ProtoStorageClusterConfig {
            kind: Some(match self {
                StorageClusterConfig::Remote { addr } => {
                    Kind::Remote(ProtoStorageClusterConfigRemote {
                        addr: addr.into_proto(),
                    })
                }
                StorageClusterConfig::Managed { allocation, size } => {
                    Kind::Managed(ProtoStorageClusterConfigManaged {
                        allocation: Some(allocation.into_proto()),
                        size: size.into_proto(),
                    })
                }
            }),
        }
    }

    fn from_proto(proto: ProtoStorageClusterConfig) -> Result<Self, TryFromProtoError> {
        use proto_storage_cluster_config::*;
        Ok(
            match proto.kind.ok_or_else(|| {
                TryFromProtoError::missing_field("ProtoStorageClusterConfig::kind")
            })? {
                Kind::Remote(ProtoStorageClusterConfigRemote { addr }) => {
                    StorageClusterConfig::Remote { addr }
                }
                Kind::Managed(ProtoStorageClusterConfigManaged { allocation, size }) => {
                    StorageClusterConfig::Managed {
                        allocation: allocation
                            .into_rust_if_some("ProtoStorageClusterConfigManaged::allocation")?,
                        size,
                    }
                }
            },
        )
    }
}

impl Arbitrary for StorageClusterConfig {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (any::<String>())
            .prop_map(|addr| Self::Remote { addr })
            .boxed()
    }
}

impl StorageClusterConfig {
    /// Returns the size specified by the storage cluster configuration, if
    /// the storage cluster is a managed storage cluster.
    pub fn size(&self) -> Option<&str> {
        match self {
            StorageClusterConfig::Remote { .. } => None,
            StorageClusterConfig::Managed { size, .. } => Some(size),
        }
    }
}
