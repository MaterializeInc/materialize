// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for bootstrap builtin cluster configuration.

#[derive(Debug, Clone)]
pub struct BootstrapBuiltinClusterConfig {
    pub size: String,
    pub replication_factor: u32,
}

pub const DEFAULT_REPLICATION_FACTOR: u32 = 1;
pub const SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR: u32 = 1;
pub const CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR: u32 = 1;
pub const PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR: u32 = 1;
// Support and analytics clusters are ephemeral - they are only spun up temporarily when needed.
// Since they are short-lived, they don't need replication by default.
pub const SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR: u32 = 0;
pub const ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR: u32 = 0;
