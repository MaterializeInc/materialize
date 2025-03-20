// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared types for the `mz-controller` crate

pub mod dyncfgs;

/// Identifies a cluster.
pub type ClusterId = mz_compute_types::ComputeInstanceId;

/// Identifies a cluster replica.
pub type ReplicaId = mz_cluster_client::ReplicaId;

/// Identifies a watch set.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct WatchSetId(u64);

impl From<u64> for WatchSetId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

pub use mz_compute_types::DEFAULT_COMPUTE_REPLICA_LOGGING_INTERVAL as DEFAULT_REPLICA_LOGGING_INTERVAL;
