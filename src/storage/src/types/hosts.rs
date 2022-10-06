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

use serde::{Deserialize, Serialize};

use mz_orchestrator::{CpuLimit, MemoryLimit};

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
