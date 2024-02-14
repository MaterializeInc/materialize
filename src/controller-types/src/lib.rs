// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared types for the `mz-controller` crate

/// Identifies a cluster.
pub type ClusterId = mz_compute_types::ComputeInstanceId;

/// Identifies a cluster replica.
pub type ReplicaId = mz_cluster_client::ReplicaId;

pub use mz_compute_types::DEFAULT_COMPUTE_REPLICA_LOGGING_INTERVAL as DEFAULT_REPLICA_LOGGING_INTERVAL;

/// Reports whether a given size name is a "v2" cluster size--i.e., a cluster
/// size that ends in "cc" or "C".
pub fn is_cluster_size_v2(size: &str) -> bool {
    size.ends_with("cc") || size.ends_with('C')
}
