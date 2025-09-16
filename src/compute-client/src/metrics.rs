// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for the compute controller components

use std::borrow::Borrow;
use std::sync::Arc;
use std::time::Duration;

use mz_cluster_client::ReplicaId;
use mz_cluster_client::metrics::{ControllerMetrics, WallclockLagMetrics};
use mz_compute_types::ComputeInstanceId;
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::raw::UIntGaugeVec;
use mz_ore::metrics::{
    CounterVec, DeleteOnDropCounter, DeleteOnDropGauge, DeleteOnDropHistogram, HistogramVec,
    IntCounterVec, MetricVecExt, MetricsRegistry,
};
use mz_ore::stats::histogram_seconds_buckets;
use mz_repr::GlobalId;
use mz_service::transport;
use prometheus::core::{AtomicF64, AtomicU64};

use crate::protocol::command::ComputeCommand;
use crate::protocol::response::{ComputeResponse, PeekResponse};

pub(crate) type Counter = DeleteOnDropCounter<AtomicF64, Vec<String>>;
pub(crate) type IntCounter = DeleteOnDropCounter<AtomicU64, Vec<String>>;
pub(crate) type UIntGauge = DeleteOnDropGauge<AtomicU64, Vec<String>>;
type Histogram = DeleteOnDropHistogram<Vec<String>>;

/// Compute controller metrics.
#[derive(Debug, Clone)]
pub struct ComputeControllerMetrics {
    // compute protocol
    commands_total: IntCounterVec,
    command_message_bytes_total: IntCounterVec,
    responses_total: IntCounterVec,
    response_message_bytes_total: IntCounterVec,

    // controller state
    replica_count: UIntGaugeVec,
    collection_count: UIntGaugeVec,
    collection_unscheduled_count: UIntGaugeVec,
    peek_count: UIntGaugeVec,
    subscribe_count: UIntGaugeVec,
    copy_to_count: UIntGaugeVec,
    command_queue_size: UIntGaugeVec,
    response_send_count: IntCounterVec,
    response_recv_count: IntCounterVec,
    hydration_queue_size: UIntGaugeVec,

    // command history
    history_command_count: UIntGaugeVec,
    history_dataflow_count: UIntGaugeVec,

    // peeks
    peeks_total: IntCounterVec,
    peek_duration_seconds: HistogramVec,

    // replica connections
    connected_replica_count: UIntGaugeVec,
    replica_connects_total: IntCounterVec,
    replica_connect_wait_time_seconds_total: CounterVec,

    /// Metrics shared with the storage controller.
    shared: ControllerMetrics,
}

impl ComputeControllerMetrics {
    /// Create a metrics instance registered into the given registry.
    pub fn new(metrics_registry: &MetricsRegistry, shared: ControllerMetrics) -> Self {
        ComputeControllerMetrics {
            commands_total: metrics_registry.register(metric!(
                name: "mz_compute_commands_total",
                help: "The total number of compute commands sent.",
                var_labels: ["instance_id", "replica_id", "command_type"],
            )),
            command_message_bytes_total: metrics_registry.register(metric!(
                name: "mz_compute_command_message_bytes_total",
                help: "The total number of bytes sent in compute command messages.",
                var_labels: ["instance_id", "replica_id"],
            )),
            responses_total: metrics_registry.register(metric!(
                name: "mz_compute_responses_total",
                help: "The total number of compute responses sent.",
                var_labels: ["instance_id", "replica_id", "response_type"],
            )),
            response_message_bytes_total: metrics_registry.register(metric!(
                name: "mz_compute_response_message_bytes_total",
                help: "The total number of bytes sent in compute response messages.",
                var_labels: ["instance_id", "replica_id"],
            )),
            replica_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_replica_count",
                help: "The number of replicas.",
                var_labels: ["instance_id"],
            )),
            collection_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_collection_count",
                help: "The number of installed compute collections.",
                var_labels: ["instance_id"],
            )),
            collection_unscheduled_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_collection_unscheduled_count",
                help: "The number of installed but unscheduled compute collections.",
                var_labels: ["instance_id"],
            )),
            peek_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_peek_count",
                help: "The number of pending peeks.",
                var_labels: ["instance_id"],
            )),
            subscribe_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_subscribe_count",
                help: "The number of active subscribes.",
                var_labels: ["instance_id"],
            )),
            copy_to_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_copy_to_count",
                help: "The number of active copy tos.",
                var_labels: ["instance_id"],
            )),
            command_queue_size: metrics_registry.register(metric!(
                name: "mz_compute_controller_command_queue_size",
                help: "The size of the compute command queue.",
                var_labels: ["instance_id", "replica_id"],
            )),
            response_send_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_response_send_count",
                help: "The number of sends on the compute response queue.",
                var_labels: ["instance_id"],
            )),
            response_recv_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_response_recv_count",
                help: "The number of receives on the compute response queue.",
                var_labels: ["instance_id"],
            )),
            hydration_queue_size: metrics_registry.register(metric!(
                name: "mz_compute_controller_hydration_queue_size",
                help: "The size of the compute hydration queue.",
                var_labels: ["instance_id", "replica_id"],
            )),
            history_command_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_history_command_count",
                help: "The number of commands in the controller's command history.",
                var_labels: ["instance_id", "command_type"],
            )),
            history_dataflow_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_history_dataflow_count",
                help: "The number of dataflows in the controller's command history.",
                var_labels: ["instance_id"],
            )),
            peeks_total: metrics_registry.register(metric!(
                name: "mz_compute_peeks_total",
                help: "The total number of peeks served.",
                var_labels: ["instance_id", "result"],
            )),
            peek_duration_seconds: metrics_registry.register(metric!(
                name: "mz_compute_peek_duration_seconds",
                help: "A histogram of peek durations since restart.",
                var_labels: ["instance_id", "result"],
                buckets: histogram_seconds_buckets(0.000_500, 32.),
            )),
            connected_replica_count: metrics_registry.register(metric!(
                name: "mz_compute_controller_connected_replica_count",
                help: "The number of replicas successfully connected to the compute controller.",
                var_labels: ["instance_id"],
            )),
            replica_connects_total: metrics_registry.register(metric!(
                name: "mz_compute_controller_replica_connects_total",
                help: "The total number of replica (re-)connections made by the compute controller.",
                var_labels: ["instance_id", "replica_id"],
            )),
            replica_connect_wait_time_seconds_total: metrics_registry.register(metric!(
                name: "mz_compute_controller_replica_connect_wait_time_seconds_total",
                help: "The total time the compute controller spent waiting for replica (re-)connection.",
                var_labels: ["instance_id", "replica_id"],
            )),

            shared,
        }
    }

    /// Return an object suitable for tracking metrics for the given compute instance.
    pub fn for_instance(&self, instance_id: ComputeInstanceId) -> InstanceMetrics {
        let labels = vec![instance_id.to_string()];
        let replica_count = self.replica_count.get_delete_on_drop_metric(labels.clone());
        let collection_count = self
            .collection_count
            .get_delete_on_drop_metric(labels.clone());
        let collection_unscheduled_count = self
            .collection_unscheduled_count
            .get_delete_on_drop_metric(labels.clone());
        let peek_count = self.peek_count.get_delete_on_drop_metric(labels.clone());
        let subscribe_count = self
            .subscribe_count
            .get_delete_on_drop_metric(labels.clone());
        let copy_to_count = self.copy_to_count.get_delete_on_drop_metric(labels.clone());
        let history_command_count = CommandMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.history_command_count.get_delete_on_drop_metric(labels)
        });
        let history_dataflow_count = self
            .history_dataflow_count
            .get_delete_on_drop_metric(labels.clone());
        let peeks_total = PeekMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.peeks_total.get_delete_on_drop_metric(labels)
        });
        let peek_duration_seconds = PeekMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.peek_duration_seconds.get_delete_on_drop_metric(labels)
        });
        let response_send_count = self
            .response_send_count
            .get_delete_on_drop_metric(labels.clone());
        let response_recv_count = self
            .response_recv_count
            .get_delete_on_drop_metric(labels.clone());
        let connected_replica_count = self
            .connected_replica_count
            .get_delete_on_drop_metric(labels);

        InstanceMetrics {
            instance_id,
            metrics: self.clone(),
            replica_count,
            collection_count,
            collection_unscheduled_count,
            copy_to_count,
            peek_count,
            subscribe_count,
            history_command_count,
            history_dataflow_count,
            peeks_total,
            peek_duration_seconds,
            response_send_count,
            response_recv_count,
            connected_replica_count,
        }
    }
}

/// Per-instance metrics
#[derive(Debug)]
pub struct InstanceMetrics {
    instance_id: ComputeInstanceId,
    metrics: ComputeControllerMetrics,

    /// Gauge tracking the number of replicas.
    pub replica_count: UIntGauge,
    /// Gauge tracking the number of installed compute collections.
    pub collection_count: UIntGauge,
    /// Gauge tracking the number of installed but unscheduled compute collections.
    pub collection_unscheduled_count: UIntGauge,
    /// Gauge tracking the number of pending peeks.
    pub peek_count: UIntGauge,
    /// Gauge tracking the number of active subscribes.
    pub subscribe_count: UIntGauge,
    /// Gauge tracking the number of active COPY TO queries.
    pub copy_to_count: UIntGauge,
    /// Gauges tracking the number of commands in the command history.
    pub history_command_count: CommandMetrics<UIntGauge>,
    /// Gauge tracking the number of dataflows in the command history.
    pub history_dataflow_count: UIntGauge,
    /// Counter tracking the total number of peeks served.
    pub peeks_total: PeekMetrics<IntCounter>,
    /// Histogram tracking peek durations.
    pub peek_duration_seconds: PeekMetrics<Histogram>,
    /// Counter tracking the number of sends on the compute response queue.
    pub response_send_count: IntCounter,
    /// Counter tracking the number of receives on the compute response queue.
    pub response_recv_count: IntCounter,
    /// Gauge tracking the number of connected replicas.
    pub connected_replica_count: UIntGauge,
}

impl InstanceMetrics {
    /// TODO(database-issues#7533): Add documentation.
    pub fn for_replica(&self, replica_id: ReplicaId) -> ReplicaMetrics {
        let labels = vec![self.instance_id.to_string(), replica_id.to_string()];
        let extended_labels = |extra: &str| {
            labels
                .iter()
                .cloned()
                .chain([extra.into()])
                .collect::<Vec<_>>()
        };

        let commands_total = CommandMetrics::build(|typ| {
            let labels = extended_labels(typ);
            self.metrics
                .commands_total
                .get_delete_on_drop_metric(labels)
        });
        let responses_total = ResponseMetrics::build(|typ| {
            let labels = extended_labels(typ);
            self.metrics
                .responses_total
                .get_delete_on_drop_metric(labels)
        });

        let command_message_bytes_total = self
            .metrics
            .command_message_bytes_total
            .get_delete_on_drop_metric(labels.clone());
        let response_message_bytes_total = self
            .metrics
            .response_message_bytes_total
            .get_delete_on_drop_metric(labels.clone());

        let command_queue_size = self
            .metrics
            .command_queue_size
            .get_delete_on_drop_metric(labels.clone());
        let hydration_queue_size = self
            .metrics
            .hydration_queue_size
            .get_delete_on_drop_metric(labels.clone());

        let replica_connects_total = self
            .metrics
            .replica_connects_total
            .get_delete_on_drop_metric(labels.clone());
        let replica_connect_wait_time_seconds_total = self
            .metrics
            .replica_connect_wait_time_seconds_total
            .get_delete_on_drop_metric(labels);

        ReplicaMetrics {
            instance_id: self.instance_id,
            replica_id,
            metrics: self.metrics.clone(),
            inner: Arc::new(ReplicaMetricsInner {
                commands_total,
                command_message_bytes_total,
                responses_total,
                response_message_bytes_total,
                command_queue_size,
                hydration_queue_size,
                replica_connects_total,
                replica_connect_wait_time_seconds_total,
            }),
        }
    }

    /// TODO(database-issues#7533): Add documentation.
    pub fn for_history(&self) -> HistoryMetrics<UIntGauge> {
        let labels = vec![self.instance_id.to_string()];
        let command_counts = CommandMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.metrics
                .history_command_count
                .get_delete_on_drop_metric(labels)
        });
        let dataflow_count = self
            .metrics
            .history_dataflow_count
            .get_delete_on_drop_metric(labels);

        HistoryMetrics {
            command_counts,
            dataflow_count,
        }
    }

    /// Reflect the given peek response in the metrics.
    pub fn observe_peek_response(&self, response: &PeekResponse, duration: Duration) {
        self.peeks_total.for_peek_response(response).inc();
        self.peek_duration_seconds
            .for_peek_response(response)
            .observe(duration.as_secs_f64());
    }
}

/// Per-replica metrics.
#[derive(Debug, Clone)]
pub struct ReplicaMetrics {
    instance_id: ComputeInstanceId,
    replica_id: ReplicaId,
    metrics: ComputeControllerMetrics,

    /// Metrics counters, wrapped in an `Arc` to be shareable between threads.
    pub inner: Arc<ReplicaMetricsInner>,
}

/// Per-replica metrics counters.
#[derive(Debug)]
pub struct ReplicaMetricsInner {
    commands_total: CommandMetrics<IntCounter>,
    command_message_bytes_total: IntCounter,
    responses_total: ResponseMetrics<IntCounter>,
    response_message_bytes_total: IntCounter,

    /// Gauge tracking the size of the compute command queue.
    pub command_queue_size: UIntGauge,
    /// Gauge tracking the size of the hydration queue.
    pub hydration_queue_size: UIntGauge,

    /// Counter tracking the total number of (re-)connects.
    replica_connects_total: IntCounter,
    /// Counter tracking the total time spent waiting for (re-)connects.
    replica_connect_wait_time_seconds_total: Counter,
}

impl ReplicaMetrics {
    pub(crate) fn for_collection(
        &self,
        collection_id: GlobalId,
    ) -> Option<ReplicaCollectionMetrics> {
        // In an effort to reduce the cardinality of timeseries created, we collect metrics only
        // for non-transient collections. This is roughly equivalent to "long-lived" collections,
        // with the exception of subscribes which may or may not be long-lived. We might want to
        // change this policy in the future to track subscribes as well.
        if collection_id.is_transient() {
            return None;
        }

        let wallclock_lag = self.metrics.shared.wallclock_lag_metrics(
            collection_id.to_string(),
            Some(self.instance_id.to_string()),
            Some(self.replica_id.to_string()),
        );

        Some(ReplicaCollectionMetrics { wallclock_lag })
    }

    /// Observe a successful replica connection.
    pub(crate) fn observe_connect(&self) {
        self.inner.replica_connects_total.inc();
    }

    /// Observe time spent waiting for a replica connection.
    pub(crate) fn observe_connect_time(&self, wait_time: Duration) {
        self.inner
            .replica_connect_wait_time_seconds_total
            .inc_by(wait_time.as_secs_f64());
    }
}

impl<T> transport::Metrics<ComputeCommand<T>, ComputeResponse<T>> for ReplicaMetrics {
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

    fn message_sent(&mut self, msg: &ComputeCommand<T>) {
        self.inner.commands_total.for_command(msg).inc();
    }

    fn message_received(&mut self, msg: &ComputeResponse<T>) {
        self.inner.responses_total.for_response(msg).inc();
    }
}

/// Per-replica-and-collection metrics.
#[derive(Debug)]
pub(crate) struct ReplicaCollectionMetrics {
    /// Metrics tracking dataflow wallclock lag.
    pub wallclock_lag: WallclockLagMetrics,
}

/// Metrics keyed by `ComputeCommand` type.
#[derive(Clone, Debug)]
pub struct CommandMetrics<M> {
    /// Metrics for `Hello`.
    pub hello: M,
    /// Metrics for `CreateInstance`.
    pub create_instance: M,
    /// Metrics for `CreateDataflow`.
    pub create_dataflow: M,
    /// Metrics for `Schedule`.
    pub schedule: M,
    /// Metrics for `AllowCompaction`.
    pub allow_compaction: M,
    /// Metrics for `Peek`.
    pub peek: M,
    /// Metrics for `CancelPeek`.
    pub cancel_peek: M,
    /// Metrics for `InitializationComplete`.
    pub initialization_complete: M,
    /// Metrics for `UpdateConfiguration`.
    pub update_configuration: M,
    /// Metrics for `AllowWrites`.
    pub allow_writes: M,
}

impl<M> CommandMetrics<M> {
    /// TODO(database-issues#7533): Add documentation.
    pub fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            hello: build_metric("hello"),
            create_instance: build_metric("create_instance"),
            create_dataflow: build_metric("create_dataflow"),
            schedule: build_metric("schedule"),
            allow_compaction: build_metric("allow_compaction"),
            peek: build_metric("peek"),
            cancel_peek: build_metric("cancel_peek"),
            initialization_complete: build_metric("initialization_complete"),
            update_configuration: build_metric("update_configuration"),
            allow_writes: build_metric("allow_writes"),
        }
    }

    fn for_all<F>(&self, f: F)
    where
        F: Fn(&M),
    {
        f(&self.hello);
        f(&self.create_instance);
        f(&self.initialization_complete);
        f(&self.update_configuration);
        f(&self.create_dataflow);
        f(&self.schedule);
        f(&self.allow_compaction);
        f(&self.peek);
        f(&self.cancel_peek);
        f(&self.allow_writes);
    }

    /// TODO(database-issues#7533): Add documentation.
    pub fn for_command<T>(&self, command: &ComputeCommand<T>) -> &M {
        use ComputeCommand::*;

        match command {
            Hello { .. } => &self.hello,
            CreateInstance(_) => &self.create_instance,
            InitializationComplete => &self.initialization_complete,
            UpdateConfiguration(_) => &self.update_configuration,
            CreateDataflow(_) => &self.create_dataflow,
            Schedule(_) => &self.schedule,
            AllowCompaction { .. } => &self.allow_compaction,
            Peek(_) => &self.peek,
            CancelPeek { .. } => &self.cancel_peek,
            AllowWrites { .. } => &self.allow_writes,
        }
    }
}

/// Metrics keyed by `ComputeResponse` type.
#[derive(Debug)]
struct ResponseMetrics<M> {
    frontiers: M,
    peek_response: M,
    subscribe_response: M,
    copy_to_response: M,
    status: M,
}

impl<M> ResponseMetrics<M> {
    fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            frontiers: build_metric("frontiers"),
            peek_response: build_metric("peek_response"),
            subscribe_response: build_metric("subscribe_response"),
            copy_to_response: build_metric("copy_to_response"),
            status: build_metric("status"),
        }
    }

    fn for_response<T>(&self, response: &ComputeResponse<T>) -> &M {
        use ComputeResponse::*;

        match response {
            Frontiers(..) => &self.frontiers,
            PeekResponse(..) => &self.peek_response,
            SubscribeResponse(..) => &self.subscribe_response,
            CopyToResponse(..) => &self.copy_to_response,
            Status(..) => &self.status,
        }
    }
}

/// Metrics tracked by the command history.
#[derive(Debug)]
pub struct HistoryMetrics<G> {
    /// Metrics tracking command counts.
    pub command_counts: CommandMetrics<G>,
    /// Metric tracking the dataflow count.
    pub dataflow_count: G,
}

impl<G> HistoryMetrics<G>
where
    G: Borrow<mz_ore::metrics::UIntGauge>,
{
    /// Reset all tracked counts to 0.
    pub fn reset(&self) {
        self.command_counts.for_all(|m| m.borrow().set(0));
        self.dataflow_count.borrow().set(0);
    }
}

/// Metrics for finished peeks, keyed by peek result.
#[derive(Debug)]
pub struct PeekMetrics<M> {
    rows: M,
    rows_stashed: M,
    error: M,
    canceled: M,
}

impl<M> PeekMetrics<M> {
    fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            rows: build_metric("rows"),
            rows_stashed: build_metric("rows_stashed"),
            error: build_metric("error"),
            canceled: build_metric("canceled"),
        }
    }

    fn for_peek_response(&self, response: &PeekResponse) -> &M {
        use PeekResponse::*;

        match response {
            Rows(_) => &self.rows,
            Stashed(_) => &self.rows_stashed,
            Error(_) => &self.error,
            Canceled => &self.canceled,
        }
    }
}
