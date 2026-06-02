// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC transport for the [client](crate::client) module.
//!
//! Note: gRPC has been replaced by [CTP](crate::transport). Most of the gRPC code has been
//! removed, but some vestiges remain until we find time to move them to more appropriate places.

use std::time::UNIX_EPOCH;

use mz_ore::metric;
use mz_ore::metrics::{DeleteOnDropGauge, MetricsRegistry, UIntGaugeVec};
use prometheus::core::AtomicU64;
use tracing::error;

use crate::transport;

/// Metrics for a cluster (CTP) server.
pub struct GrpcServerMetrics {
    last_command_received: UIntGaugeVec,
    /// Deprecated alias of `last_command_received`, emitted under the old `mz_grpc_server_*` name.
    ///
    /// Retained so the `clusterd-not-receiving-commands` alert keeps working until the monitoring
    /// configuration migrates to `mz_cluster_server_last_command_received`. Remove once it has.
    last_command_received_grpc_alias: UIntGaugeVec,
}

impl GrpcServerMetrics {
    /// Registers the cluster server metrics into a `registry`.
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        let help = "The time (in seconds since the Unix epoch) at which the server last \
                    received data from the controller, including CTP keepalives. Used to \
                    detect controller connections that are no longer reachable.";
        Self {
            last_command_received: registry.register(metric!(
                name: "mz_cluster_server_last_command_received",
                help: help,
                var_labels: ["server_name"],
            )),
            last_command_received_grpc_alias: registry.register(metric!(
                name: "mz_grpc_server_last_command_received",
                help: help,
                var_labels: ["server_name"],
            )),
        }
    }

    pub fn for_server(&self, name: &'static str) -> PerGrpcServerMetrics {
        PerGrpcServerMetrics {
            last_command_received: self
                .last_command_received
                .get_delete_on_drop_metric(vec![name]),
            last_command_received_grpc_alias: self
                .last_command_received_grpc_alias
                .get_delete_on_drop_metric(vec![name]),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PerGrpcServerMetrics {
    last_command_received: DeleteOnDropGauge<AtomicU64, Vec<&'static str>>,
    last_command_received_grpc_alias: DeleteOnDropGauge<AtomicU64, Vec<&'static str>>,
}

impl<C, R> transport::Metrics<C, R> for PerGrpcServerMetrics {
    fn bytes_sent(&mut self, _len: usize) {}

    fn bytes_received(&mut self, _len: usize) {
        // We bump the "last command received" timestamp on any inbound bytes, not just on fully
        // decoded command messages (`message_received`). CTP exchanges keepalives on otherwise
        // idle connections, and those show up here but never as messages (empty keepalive frames
        // are dropped before decoding). Counting them as activity makes this metric a
        // connection-liveness signal: it stays fresh as long as the controller connection is
        // healthy and only goes stale once the connection is actually broken. Bumping it only on
        // command messages instead produces false positives on healthy-but-idle clusters that
        // don't receive a command for a while.
        match UNIX_EPOCH.elapsed() {
            Ok(ts) => {
                self.last_command_received.set(ts.as_secs());
                self.last_command_received_grpc_alias.set(ts.as_secs());
            }
            Err(e) => error!("failed to get system time: {e}"),
        }
    }

    fn message_sent(&mut self, _msg: &C) {}
    fn message_received(&mut self, _msg: &R) {}
}
