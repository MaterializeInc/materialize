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
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{
    CounterVec, DeleteOnDropCounter, DeleteOnDropGauge, IntCounterVec, MetricsRegistry,
    UIntGaugeVec,
};
use mz_repr::GlobalId;
use mz_service::transport;
use mz_storage_types::instances::StorageInstanceId;
use prometheus::core::{AtomicF64, AtomicU64};

use crate::client::{StorageCommand, StorageResponse};

type IntCounter = DeleteOnDropCounter<AtomicU64, Vec<String>>;
pub type UIntGauge = DeleteOnDropGauge<AtomicU64, Vec<String>>;

/// Storage controller metrics
#[derive(Debug, Clone)]
pub struct StorageControllerMetrics {
    // storage protocol
    commands_total: IntCounterVec,
    command_message_bytes_total: IntCounterVec,
    responses_total: IntCounterVec,
    response_message_bytes_total: IntCounterVec,

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
            commands_total: metrics_registry.register(metric!(
                name: "mz_storage_commands_total",
                help: "The total number of storage commands sent.",
                var_labels: ["instance_id", "replica_id", "command_type"],
            )),
            command_message_bytes_total: metrics_registry.register(metric!(
                name: "mz_storage_command_message_bytes_total",
                help: "The total number of bytes sent in storage command messages.",
                var_labels: ["instance_id", "replica_id"],
            )),
            responses_total: metrics_registry.register(metric!(
                name: "mz_storage_responses_total",
                help: "The total number of storage responses received.",
                var_labels: ["instance_id", "replica_id", "response_type"],
            )),
            response_message_bytes_total: metrics_registry.register(metric!(
                name: "mz_storage_response_message_bytes_total",
                help: "The total number of bytes received in storage response messages.",
                var_labels: ["instance_id", "replica_id"],
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
        let extended_labels = |extra: &str| {
            labels
                .iter()
                .cloned()
                .chain([extra.into()])
                .collect::<Vec<_>>()
        };

        ReplicaMetrics {
            inner: Arc::new(ReplicaMetricsInner {
                commands_total: CommandMetrics::build(|typ| {
                    let labels = extended_labels(typ);
                    self.metrics
                        .commands_total
                        .get_delete_on_drop_metric(labels)
                }),
                responses_total: ResponseMetrics::build(|typ| {
                    let labels = extended_labels(typ);
                    self.metrics
                        .responses_total
                        .get_delete_on_drop_metric(labels)
                }),
                command_message_bytes_total: self
                    .metrics
                    .command_message_bytes_total
                    .get_delete_on_drop_metric(labels.clone()),
                response_message_bytes_total: self
                    .metrics
                    .response_message_bytes_total
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
        let command_counts = CommandMetrics::build(|typ| {
            let labels = vec![self.instance_id.to_string(), typ.to_string()];
            self.metrics
                .history_command_count
                .get_delete_on_drop_metric(labels)
        });

        HistoryMetrics { command_counts }
    }
}

#[derive(Debug)]
struct ReplicaMetricsInner {
    commands_total: CommandMetrics<IntCounter>,
    command_message_bytes_total: IntCounter,
    responses_total: ResponseMetrics<IntCounter>,
    response_message_bytes_total: IntCounter,
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

impl<T> transport::Metrics<StorageCommand<T>, StorageResponse<T>> for ReplicaMetrics {
    fn bytes_sent(&mut self, len: usize) {
        self.inner
            .command_message_bytes_total
            .inc_by(u64::cast_from(len));
    }

    fn bytes_received(&mut self, len: usize) {
        self.inner
            .response_message_bytes_total
            .inc_by(u64::cast_from(len));
    }

    fn message_sent(&mut self, msg: &StorageCommand<T>) {
        self.inner.commands_total.for_command(msg).inc();
    }

    fn message_received(&mut self, msg: &StorageResponse<T>) {
        self.inner.responses_total.for_response(msg).inc();
    }
}

/// Metrics keyed by `StorageCommand` type.
#[derive(Clone, Debug)]
pub struct CommandMetrics<M> {
    /// Metrics for `Hello`.
    pub hello: M,
    /// Metrics for `InitializationComplete`.
    pub initialization_complete: M,
    /// Metrics for `AllowWrites`.
    pub allow_writes: M,
    /// Metrics for `UpdateConfiguration`.
    pub update_configuration: M,
    /// Metrics for `RunIngestion`.
    pub run_ingestion: M,
    /// Metrics for `AllowCompaction`.
    pub allow_compaction: M,
    /// Metrics for `RunSink`.
    pub run_sink: M,
    /// Metrics for `RunOneshotIngestion`.
    pub run_oneshot_ingestion: M,
    /// Metrics for `CancelOneshotIngestion`.
    pub cancel_oneshot_ingestion: M,
}

impl<M> CommandMetrics<M> {
    fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            hello: build_metric("hello"),
            initialization_complete: build_metric("initialization_complete"),
            allow_writes: build_metric("allow_writes"),
            update_configuration: build_metric("update_configuration"),
            run_ingestion: build_metric("run_ingestion"),
            allow_compaction: build_metric("allow_compaction"),
            run_sink: build_metric("run_sink"),
            run_oneshot_ingestion: build_metric("run_oneshot_ingestion"),
            cancel_oneshot_ingestion: build_metric("cancel_oneshot_ingestion"),
        }
    }

    fn for_all<F>(&self, f: F)
    where
        F: Fn(&M),
    {
        f(&self.hello);
        f(&self.initialization_complete);
        f(&self.allow_writes);
        f(&self.update_configuration);
        f(&self.run_ingestion);
        f(&self.allow_compaction);
        f(&self.run_sink);
        f(&self.run_oneshot_ingestion);
        f(&self.cancel_oneshot_ingestion);
    }

    pub fn for_command<T>(&self, command: &StorageCommand<T>) -> &M {
        use StorageCommand::*;

        match command {
            Hello { .. } => &self.hello,
            InitializationComplete => &self.initialization_complete,
            AllowWrites => &self.allow_writes,
            UpdateConfiguration(..) => &self.update_configuration,
            RunIngestion(..) => &self.run_ingestion,
            AllowCompaction(..) => &self.allow_compaction,
            RunSink(..) => &self.run_sink,
            RunOneshotIngestion(..) => &self.run_oneshot_ingestion,
            CancelOneshotIngestion(..) => &self.cancel_oneshot_ingestion,
        }
    }
}

/// Metrics keyed by `StorageResponse` type.
#[derive(Debug)]
struct ResponseMetrics<M> {
    frontier_upper: M,
    dropped_id: M,
    staged_batches: M,
    statistics_updates: M,
    status_update: M,
}

impl<M> ResponseMetrics<M> {
    fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            frontier_upper: build_metric("frontier_upper"),
            dropped_id: build_metric("dropped_id"),
            staged_batches: build_metric("staged_batches"),
            statistics_updates: build_metric("statistics_updates"),
            status_update: build_metric("status_update"),
        }
    }

    fn for_response<T>(&self, response: &StorageResponse<T>) -> &M {
        use StorageResponse::*;

        match response {
            FrontierUpper(..) => &self.frontier_upper,
            DroppedId(..) => &self.dropped_id,
            StagedBatches(..) => &self.staged_batches,
            StatisticsUpdates(..) => &self.statistics_updates,
            StatusUpdate(..) => &self.status_update,
        }
    }
}

/// Metrics tracked by the command history.
#[derive(Debug)]
pub struct HistoryMetrics {
    /// Metrics tracking command counts.
    pub command_counts: CommandMetrics<UIntGauge>,
}

impl HistoryMetrics {
    pub fn reset(&self) {
        self.command_counts.for_all(|m| m.set(0));
    }
}
