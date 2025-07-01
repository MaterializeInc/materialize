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
use std::time::Duration;

use mz_cluster_client::ReplicaId;
use mz_cluster_client::metrics::{ControllerMetrics, WallclockLagMetrics};
use mz_ore::cast::{CastFrom, TryCastFrom};
use mz_ore::metric;
use mz_ore::metrics::{
    CounterVec, DeleteOnDropCounter, DeleteOnDropGauge, DeleteOnDropHistogram, IntCounterVec,
    MetricVecExt, MetricsRegistry, UIntGaugeVec,
};
use mz_ore::stats::HISTOGRAM_BYTE_BUCKETS;
use mz_repr::GlobalId;
use mz_service::codec::StatsCollector;
use mz_storage_types::instances::StorageInstanceId;
use prometheus::core::{AtomicF64, AtomicU64};

use crate::client::{ProtoStorageCommand, ProtoStorageResponse};

pub type UIntGauge = DeleteOnDropGauge<AtomicU64, Vec<String>>;

/// Storage controller metrics
#[derive(Debug, Clone)]
pub struct StorageControllerMetrics {
    messages_sent_bytes: prometheus::HistogramVec,
    messages_received_bytes: prometheus::HistogramVec,
    regressed_offset_known: IntCounterVec,
    history_command_count: UIntGaugeVec,

    // replica connections
    connected_replica_count: UIntGaugeVec,
    replica_connects_total: IntCounterVec,
    replica_connect_wait_time_seconds_total: CounterVec,

    /// Metrics shared with the compute controller.
    shared: ControllerMetrics,
}

impl StorageControllerMetrics {
    pub fn new(metrics_registry: &MetricsRegistry, shared: ControllerMetrics) -> Self {
        Self {
            messages_sent_bytes: metrics_registry.register(metric!(
                name: "mz_storage_messages_sent_bytes",
                help: "size of storage messages sent",
                var_labels: ["instance_id", "replica_id"],
                buckets: HISTOGRAM_BYTE_BUCKETS.to_vec()
            )),
            messages_received_bytes: metrics_registry.register(metric!(
                name: "mz_storage_messages_received_bytes",
                help: "size of storage messages received",
                var_labels: ["instance_id", "replica_id"],
                buckets: HISTOGRAM_BYTE_BUCKETS.to_vec()
            )),
            regressed_offset_known: metrics_registry.register(metric!(
                name: "mz_storage_regressed_offset_known",
                help: "number of regressed offset_known stats for this id",
                var_labels: ["id"],
            )),
            history_command_count: metrics_registry.register(metric!(
                name: "mz_storage_controller_history_command_count",
                help: "The number of commands in the controller's command history.",
                var_labels: ["instance_id", "command_type"],
            )),
            connected_replica_count: metrics_registry.register(metric!(
                name: "mz_storage_controller_connected_replica_count",
                help: "The number of replicas successfully connected to the storage controller.",
                var_labels: ["instance_id"],
            )),
            replica_connects_total: metrics_registry.register(metric!(
                name: "mz_storage_controller_replica_connects_total",
                help: "The total number of replica (re-)connections made by the storage controller.",
                var_labels: ["instance_id", "replica_id"],
            )),
            replica_connect_wait_time_seconds_total: metrics_registry.register(metric!(
                name: "mz_storage_controller_replica_connect_wait_time_seconds_total",
                help: "The total time the storage controller spent waiting for replica (re-)connection.",
                var_labels: ["instance_id", "replica_id"],
            )),

            shared,
        }
    }

    pub fn regressed_offset_known(
        &self,
        id: mz_repr::GlobalId,
    ) -> DeleteOnDropCounter<prometheus::core::AtomicU64, Vec<String>> {
        self.regressed_offset_known
            .get_delete_on_drop_metric(vec![id.to_string()])
    }

    pub fn wallclock_lag_metrics(
        &self,
        id: GlobalId,
        instance_id: Option<StorageInstanceId>,
    ) -> WallclockLagMetrics {
        self.shared
            .wallclock_lag_metrics(id.to_string(), instance_id.map(|x| x.to_string()), None)
    }

    pub fn for_instance(&self, id: StorageInstanceId) -> InstanceMetrics {
        let connected_replica_count = self
            .connected_replica_count
            .get_delete_on_drop_metric(vec![id.to_string()]);

        InstanceMetrics {
            instance_id: id,
            metrics: self.clone(),
            connected_replica_count,
        }
    }
}

#[derive(Debug)]
pub struct InstanceMetrics {
    instance_id: StorageInstanceId,
    metrics: StorageControllerMetrics,
    /// Gauge tracking the number of connected replicas.
    pub connected_replica_count: UIntGauge,
}

impl InstanceMetrics {
    pub fn for_replica(&self, id: ReplicaId) -> ReplicaMetrics {
        let labels = vec![self.instance_id.to_string(), id.to_string()];
        ReplicaMetrics {
            inner: Arc::new(ReplicaMetricsInner {
                messages_sent_bytes: self
                    .metrics
                    .messages_sent_bytes
                    .get_delete_on_drop_metric(labels.clone()),
                messages_received_bytes: self
                    .metrics
                    .messages_received_bytes
                    .get_delete_on_drop_metric(labels.clone()),
                replica_connects_total: self
                    .metrics
                    .replica_connects_total
                    .get_delete_on_drop_metric(labels.clone()),
                replica_connect_wait_time_seconds_total: self
                    .metrics
                    .replica_connect_wait_time_seconds_total
                    .get_delete_on_drop_metric(labels),
            }),
        }
    }

    pub fn for_history(&self) -> HistoryMetrics {
        let command_gauge = |name: &str| {
            let labels = vec![self.instance_id.to_string(), name.to_string()];
            self.metrics
                .history_command_count
                .get_delete_on_drop_metric(labels)
        };

        HistoryMetrics {
            hello_count: command_gauge("hello"),
            run_ingestions_count: command_gauge("run_ingestions"),
            run_sinks_count: command_gauge("run_sinks"),
            allow_compaction_count: command_gauge("allow_compaction"),
            initialization_complete_count: command_gauge("initialization_complete"),
            allow_writes_count: command_gauge("allow_writes"),
            update_configuration_count: command_gauge("update_configuration"),
        }
    }
}

#[derive(Debug)]
struct ReplicaMetricsInner {
    messages_sent_bytes: DeleteOnDropHistogram<Vec<String>>,
    messages_received_bytes: DeleteOnDropHistogram<Vec<String>>,
    /// Counter tracking the total number of (re-)connects.
    replica_connects_total: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    /// Counter tracking the total time spent waiting for (re-)connects.
    replica_connect_wait_time_seconds_total: DeleteOnDropCounter<AtomicF64, Vec<String>>,
}

/// Per-instance metrics
#[derive(Debug, Clone)]
pub struct ReplicaMetrics {
    inner: Arc<ReplicaMetricsInner>,
}

impl ReplicaMetrics {
    /// Observe a successful replica connection.
    pub fn observe_connect(&self) {
        self.inner.replica_connects_total.inc();
    }

    /// Observe time spent waiting for a replica connection.
    pub fn observe_connect_time(&self, wait_time: Duration) {
        self.inner
            .replica_connect_wait_time_seconds_total
            .inc_by(wait_time.as_secs_f64());
    }
}

/// Make [`ReplicaMetrics`] pluggable into the gRPC connection.
impl StatsCollector<ProtoStorageCommand, ProtoStorageResponse> for ReplicaMetrics {
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

/// Metrics tracked by the command history.
#[derive(Debug)]
pub struct HistoryMetrics {
    /// Number of `Hello` commands.
    pub hello_count: UIntGauge,
    /// Number of `RunIngestion` commands.
    pub run_ingestions_count: UIntGauge,
    /// Number of `RunSink` commands.
    pub run_sinks_count: UIntGauge,
    /// Number of `AllowCompaction` commands.
    pub allow_compaction_count: UIntGauge,
    /// Number of `InitializationComplete` commands.
    pub initialization_complete_count: UIntGauge,
    /// Number of `AllowWrites` commands.
    pub allow_writes_count: UIntGauge,
    /// Number of `UpdateConfiguration` commands.
    pub update_configuration_count: UIntGauge,
}

impl HistoryMetrics {
    pub fn reset(&self) {
        self.hello_count.set(0);
        self.run_ingestions_count.set(0);
        self.run_sinks_count.set(0);
        self.allow_compaction_count.set(0);
        self.initialization_complete_count.set(0);
        self.allow_writes_count.set(0);
        self.update_configuration_count.set(0);
    }
}
