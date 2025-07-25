// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for commands to clusters.

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Configuration of the cluster we will spin up
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TimelyConfig {
    /// Number of per-process worker threads
    pub workers: usize,
    /// Identity of this process
    pub process: usize,
    /// Addresses of all processes
    pub addresses: Vec<String>,
    /// Proportionality value that decides whether to exert additional arrangement merge effort.
    ///
    /// Specifically, additional merge effort is exerted when the size of the second-largest batch
    /// in an arrangement is within a factor of `arrangement_exert_proportionality` of the size of
    /// the largest batch, or when a merge is already in progress.
    ///
    /// The higher the proportionality value, the more eagerly arrangement batches are merged. A
    /// value of `0` (or `1`) disables eager merging.
    pub arrangement_exert_proportionality: u32,
    /// Whether to use the zero copy allocator.
    pub enable_zero_copy: bool,
    /// Whether to use lgalloc to back the zero copy allocator.
    pub enable_zero_copy_lgalloc: bool,
    /// Optional limit on the number of empty buffers retained by the zero copy allocator.
    pub zero_copy_limit: Option<usize>,
}

impl ToString for TimelyConfig {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl FromStr for TimelyConfig {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }
}

/// A trait for cluster commands that provide a protocol nonce.
pub trait TryIntoProtocolNonce {
    /// Attempt to unpack `self` into a nonce. Otherwise, fail and return `self` back.
    fn try_into_protocol_nonce(self) -> Result<Uuid, Self>
    where
        Self: Sized;
}

/// Specifies the location of a cluster replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterReplicaLocation {
    /// The network addresses of the cluster control endpoints for each process in
    /// the replica. Connections from the controller to these addresses
    /// are sent commands, and send responses back.
    pub ctl_addrs: Vec<String>,
}
