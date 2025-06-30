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

use mz_proto::{ProtoType, RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/mz_cluster_client.client.rs"));

/// Configuration of the cluster we will spin up
#[derive(Arbitrary, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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

impl RustType<ProtoTimelyConfig> for TimelyConfig {
    fn into_proto(&self) -> ProtoTimelyConfig {
        ProtoTimelyConfig {
            workers: self.workers.into_proto(),
            addresses: self.addresses.into_proto(),
            process: self.process.into_proto(),
            arrangement_exert_proportionality: self.arrangement_exert_proportionality,
            enable_zero_copy: self.enable_zero_copy,
            enable_zero_copy_lgalloc: self.enable_zero_copy_lgalloc,
            zero_copy_limit: self.zero_copy_limit.into_proto(),
        }
    }

    fn from_proto(proto: ProtoTimelyConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            process: proto.process.into_rust()?,
            workers: proto.workers.into_rust()?,
            addresses: proto.addresses.into_rust()?,
            arrangement_exert_proportionality: proto.arrangement_exert_proportionality,
            enable_zero_copy: proto.enable_zero_copy,
            enable_zero_copy_lgalloc: proto.enable_zero_copy_lgalloc,
            zero_copy_limit: proto.zero_copy_limit.into_rust()?,
        })
    }
}

impl TimelyConfig {
    /// Split the timely configuration into `parts` pieces, each with a different `process` number.
    pub fn split_command(&self, parts: usize) -> Vec<Self> {
        (0..parts)
            .map(|part| TimelyConfig {
                process: part,
                ..self.clone()
            })
            .collect()
    }
}

/// A trait for specific cluster commands that can be unpacked into
/// `CreateTimely` variants.
pub trait TryIntoTimelyConfig {
    /// Attempt to unpack `self` into a `(TimelyConfig, Uuid)`. Otherwise,
    /// fail and return `self` back.
    fn try_into_timely_config(self) -> Result<(TimelyConfig, Uuid), Self>
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
    /// The network addresses of the dataflow (Timely) endpoints for
    /// each process in the replica. These are used for _internal_
    /// networking, that is, timely worker communicating messages
    /// between themselves.
    pub dataflow_addrs: Vec<String>,
    /// The workers per process in the replica.
    pub workers: usize,
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::{ProptestConfig, any};
    use proptest::proptest;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn timely_config_protobuf_roundtrip(expect in any::<TimelyConfig>() ) {
            let actual = protobuf_roundtrip::<_, ProtoTimelyConfig>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
