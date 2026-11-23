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
pub type ClusterId = mz_cluster_client::instances::StorageInstanceId;

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

/// Default logging interval for replicas.
/// How a cluster's replicas share the queries sent to the cluster.
///
/// Maintained dataflows (indexes, materialized views, sinks) run on every replica in both modes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize
)]
pub enum QueryRouting {
    /// Every replica runs every query, and the first response wins.
    #[default]
    Broadcast,
    /// Each query runs on one replica that has caught up to the query's timestamp, chosen
    /// round-robin.
    Balanced,
}

impl std::fmt::Display for QueryRouting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryRouting::Broadcast => f.write_str("BROADCAST"),
            QueryRouting::Balanced => f.write_str("BALANCED"),
        }
    }
}

pub const DEFAULT_REPLICA_LOGGING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
