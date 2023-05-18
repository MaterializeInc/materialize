// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for the compute controller components

use std::sync::Arc;

use mz_ore::cast::{CastFrom, TryCastFrom};
use mz_ore::metric;
use mz_ore::metrics::{DeleteOnDropHistogram, HistogramVecExt, MetricsRegistry};
use mz_ore::stats::HISTOGRAM_BYTE_BUCKETS;
use mz_service::codec::StatsCollector;

use crate::controller::{ComputeInstanceId, ReplicaId};
use crate::protocol::command::ProtoComputeCommand;
use crate::protocol::response::ProtoComputeResponse;

/// Compute controller metrics
#[derive(Debug, Clone)]
pub struct ComputeControllerMetrics {
    messages_sent_bytes: prometheus::HistogramVec,
    messages_received_bytes: prometheus::HistogramVec,
}

impl ComputeControllerMetrics {
    pub fn new(metrics_registry: MetricsRegistry) -> Self {
        ComputeControllerMetrics {
            messages_sent_bytes: metrics_registry.register(metric!(
                name: "mz_compute_messages_sent_bytes",
                help: "size of compute messages sent",
                var_labels: ["instance", "replica"],
                buckets: HISTOGRAM_BYTE_BUCKETS.to_vec()
            )),
            messages_received_bytes: metrics_registry.register(metric!(
                name: "mz_compute_messages_received_bytes",
                help: "size of compute messages received",
                var_labels: ["instance", "replica"],
                buckets: HISTOGRAM_BYTE_BUCKETS.to_vec()
            )),
        }
    }

    pub fn for_instance(&self, instance_id: ComputeInstanceId) -> InstanceMetrics {
        InstanceMetrics {
            messages_sent_bytes: self.messages_sent_bytes.clone(),
            messages_received_bytes: self.messages_received_bytes.clone(),
            instance_id,
        }
    }
}

/// Per-instance metrics
#[derive(Debug, Clone)]
pub struct InstanceMetrics {
    messages_sent_bytes: prometheus::HistogramVec,
    messages_received_bytes: prometheus::HistogramVec,
    instance_id: ComputeInstanceId,
}

impl InstanceMetrics {
    pub fn for_replica(&self, replica_id: ReplicaId) -> ReplicaMetrics {
        let labels = vec![self.instance_id.to_string(), replica_id.to_string()];
        let messages_sent_bytes = self
            .messages_sent_bytes
            .get_delete_on_drop_histogram(labels.clone());
        let messages_received_bytes = self
            .messages_received_bytes
            .get_delete_on_drop_histogram(labels);

        ReplicaMetrics {
            inner: Arc::new(ReplicaMetricsInner {
                messages_sent_bytes,
                messages_received_bytes,
            }),
        }
    }
}

#[derive(Debug)]
struct ReplicaMetricsInner {
    messages_sent_bytes: DeleteOnDropHistogram<'static, Vec<String>>,
    messages_received_bytes: DeleteOnDropHistogram<'static, Vec<String>>,
}

/// Per-replica metrics.
#[derive(Debug, Clone)]
pub struct ReplicaMetrics {
    inner: Arc<ReplicaMetricsInner>,
}

/// Make ReplicaConnectionMetric pluggable into the gRPC connection.
impl StatsCollector<ProtoComputeCommand, ProtoComputeResponse> for ReplicaMetrics {
    fn send_event(&self, _item: &ProtoComputeCommand, size: usize) {
        match f64::try_cast_from(u64::cast_from(size)) {
            Some(x) => self.inner.messages_sent_bytes.observe(x),
            None => tracing::warn!(
                "{} has no precise representation as f64, ignoring message",
                size
            ),
        }
    }

    fn receive_event(&self, _item: &ProtoComputeResponse, size: usize) {
        match f64::try_cast_from(u64::cast_from(size)) {
            Some(x) => self.inner.messages_received_bytes.observe(x),
            None => tracing::warn!(
                "{} has no precise representation as f64, ignoring message",
                size
            ),
        }
    }
}
