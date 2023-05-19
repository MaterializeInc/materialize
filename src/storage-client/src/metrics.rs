// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for the storage controller components

use std::sync::Arc;

use mz_ore::cast::{CastFrom, TryCastFrom};
use mz_ore::metric;
use mz_ore::metrics::{DeleteOnDropHistogram, HistogramVecExt, MetricsRegistry};
use mz_ore::stats::HISTOGRAM_BYTE_BUCKETS;
use mz_service::codec::StatsCollector;

use crate::client::{ProtoStorageCommand, ProtoStorageResponse};
use crate::types::instances::StorageInstanceId;

/// Storage controller metrics
#[derive(Debug, Clone)]
pub struct StorageControllerMetrics {
    messages_sent_bytes: prometheus::HistogramVec,
    messages_received_bytes: prometheus::HistogramVec,
}

impl StorageControllerMetrics {
    pub fn new(metrics_registry: MetricsRegistry) -> Self {
        Self {
            messages_sent_bytes: metrics_registry.register(metric!(
                name: "mz_storage_messages_sent_bytes",
                help: "size of storage messages sent",
                var_labels: ["instance"],
                buckets: HISTOGRAM_BYTE_BUCKETS.to_vec()
            )),

            messages_received_bytes: metrics_registry.register(metric!(
                name: "mz_storage_messages_received_bytes",
                help: "size of storage messages received",
                var_labels: ["instance"],
                buckets: HISTOGRAM_BYTE_BUCKETS.to_vec()
            )),
        }
    }

    pub fn for_instance(&self, id: StorageInstanceId) -> RehydratingStorageClientMetrics {
        let labels = vec![id.to_string()];
        RehydratingStorageClientMetrics {
            inner: Arc::new(RehydratingStorageClientMetricsInner {
                messages_sent_bytes: self
                    .messages_sent_bytes
                    .get_delete_on_drop_histogram(labels.clone()),
                messages_received_bytes: self
                    .messages_received_bytes
                    .get_delete_on_drop_histogram(labels),
            }),
        }
    }
}

#[derive(Debug)]
struct RehydratingStorageClientMetricsInner {
    messages_sent_bytes: DeleteOnDropHistogram<'static, Vec<String>>,
    messages_received_bytes: DeleteOnDropHistogram<'static, Vec<String>>,
}

/// Per-instance metrics
#[derive(Debug, Clone)]
pub struct RehydratingStorageClientMetrics {
    inner: Arc<RehydratingStorageClientMetricsInner>,
}

/// Make ReplicaConnectionMetric pluggable into the gRPC connection.
impl StatsCollector<ProtoStorageCommand, ProtoStorageResponse> for RehydratingStorageClientMetrics {
    fn send_event(&self, _item: &ProtoStorageCommand, size: usize) {
        match f64::try_cast_from(u64::cast_from(size)) {
            Some(x) => self.inner.messages_sent_bytes.observe(x),
            None => tracing::warn!(
                "{} has no precise representation as f64, ignoring message",
                size
            ),
        }
    }

    fn receive_event(&self, _item: &ProtoStorageResponse, size: usize) {
        match f64::try_cast_from(u64::cast_from(size)) {
            Some(x) => self.inner.messages_received_bytes.observe(x),
            None => tracing::warn!(
                "{} has no precise representation as f64, ignoring message",
                size
            ),
        }
    }
}
