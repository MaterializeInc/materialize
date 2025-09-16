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

/// Metrics for a gRPC server.
pub struct GrpcServerMetrics {
    last_command_received: UIntGaugeVec,
}

impl GrpcServerMetrics {
    /// Registers the GRPC server metrics into a `registry`.
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            last_command_received: registry.register(metric!(
                name: "mz_grpc_server_last_command_received",
                help: "The time at which the server received its last command.",
                var_labels: ["server_name"],
            )),
        }
    }

    pub fn for_server(&self, name: &'static str) -> PerGrpcServerMetrics {
        PerGrpcServerMetrics {
            last_command_received: self
                .last_command_received
                .get_delete_on_drop_metric(vec![name]),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PerGrpcServerMetrics {
    last_command_received: DeleteOnDropGauge<AtomicU64, Vec<&'static str>>,
}

impl<C, R> transport::Metrics<C, R> for PerGrpcServerMetrics {
    fn bytes_sent(&mut self, _len: usize) {}
    fn bytes_received(&mut self, _len: usize) {}
    fn message_sent(&mut self, _msg: &C) {}

    fn message_received(&mut self, _msg: &R) {
        match UNIX_EPOCH.elapsed() {
            Ok(ts) => self.last_command_received.set(ts.as_secs()),
            Err(e) => error!("failed to get system time: {e}"),
        }
    }
}
