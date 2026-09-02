// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute configuration types.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Replica configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComputeReplicaConfig {
    /// TODO(database-issues#7533): Add documentation.
    pub logging: ComputeReplicaLogging,
    /// Whether arrangements on this replica request dictionary compression.
    ///
    /// This carries the configured value. Whether the replica honors it is
    /// gated at replica creation by the `enable_arrangement_dictionary_compression_alpha`
    /// feature flag.
    pub arrangement_compression: bool,
}

/// Logging configuration of a replica.
#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize
)]
pub struct ComputeReplicaLogging {
    /// Whether to enable logging for the logging dataflows.
    pub log_logging: bool,
    /// The interval at which to log.
    ///
    /// A `None` value indicates that logging is disabled.
    pub interval: Option<Duration>,
}

impl ComputeReplicaLogging {
    /// Return whether logging is enabled.
    pub fn enabled(&self) -> bool {
        self.interval.is_some()
    }
}
