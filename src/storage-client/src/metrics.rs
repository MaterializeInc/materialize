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

use mz_cluster_client::metrics::{ControllerMetrics, WallclockLagMetrics};
use mz_cluster_client::ReplicaId;
use mz_ore::cast::{CastFrom, TryCastFrom};
use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropCounter, DeleteOnDropGauge, DeleteOnDropHistogram, IntCounterVec, MetricVecExt,
    MetricsRegistry, UIntGaugeVec,
};
use mz_ore::stats::HISTOGRAM_BYTE_BUCKETS;
use mz_repr::GlobalId;
use mz_service::codec::StatsCollector;
use mz_storage_types::instances::StorageInstanceId;
use prometheus::core::AtomicU64;

use crate::client::{ProtoStorageCommand, ProtoStorageResponse};

pub type UIntGauge = DeleteOnDropGauge<'static, AtomicU64, Vec<String>>;

/// Storage controller metrics
#[derive(Debug, Clone)]
pub struct StorageControllerMetrics {
    messages_sent_bytes: prometheus::HistogramVec,
    messages_received_bytes: prometheus::HistogramVec,
    startup_prepared_statements_kept: prometheus::IntGauge,
    regressed_offset_known: IntCounterVec,
    history_command_count: UIntGaugeVec,

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
            startup_prepared_statements_kept: metrics_registry.register(metric!(
                name: "mz_storage_startup_prepared_statements_kept",
                help: "number of prepared statements kept on startup",
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

            shared,
        }
    }

    pub fn regressed_offset_known(
        &self,
        id: mz_repr::GlobalId,
    ) -> DeleteOnDropCounter<'static, prometheus::core::AtomicU64, Vec<String>> {
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
        InstanceMetrics {
            instance_id: id,
            metrics: self.clone(),
        }
    }

    pub fn set_startup_prepared_statements_kept(&self, n: u64) {
        let n: i64 = n.try_into().expect("realistic number");
        self.startup_prepared_statements_kept.set(n);
    }
}

#[derive(Debug)]
pub struct InstanceMetrics {
    instance_id: StorageInstanceId,
    metrics: StorageControllerMetrics,
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
            create_timely_count: command_gauge("create_timely"),
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
    messages_sent_bytes: DeleteOnDropHistogram<'static, Vec<String>>,
    messages_received_bytes: DeleteOnDropHistogram<'static, Vec<String>>,
}

/// Per-instance metrics
#[derive(Debug, Clone)]
pub struct ReplicaMetrics {
    inner: Arc<ReplicaMetricsInner>,
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
    /// Number of `CreateTimely` commands.
    pub create_timely_count: UIntGauge,
    /// Number of `RunIngestions` commands.
    pub run_ingestions_count: UIntGauge,
    /// Number of `RunSinks` commands.
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
        self.create_timely_count.set(0);
        self.run_ingestions_count.set(0);
        self.run_sinks_count.set(0);
        self.allow_compaction_count.set(0);
        self.initialization_complete_count.set(0);
        self.allow_writes_count.set(0);
        self.update_configuration_count.set(0);
    }
}
