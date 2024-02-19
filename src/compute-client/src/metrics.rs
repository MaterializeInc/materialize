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
use mz_compute_types::ComputeInstanceId;
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{
    CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, DeleteOnDropHistogram, GaugeVec,
    GaugeVecExt, HistogramVec, HistogramVecExt, IntCounterVec, MetricsRegistry, UIntGaugeVec,
};
use mz_ore::stats::histogram_seconds_buckets;
use mz_repr::GlobalId;
use mz_service::codec::StatsCollector;
use prometheus::core::{AtomicF64, AtomicU64};

use crate::protocol::command::{ComputeCommand, ProtoComputeCommand};
use crate::protocol::response::{PeekResponse, ProtoComputeResponse};

type IntCounter = DeleteOnDropCounter<'static, AtomicU64, Vec<String>>;
type Gauge = DeleteOnDropGauge<'static, AtomicF64, Vec<String>>;
/// TODO(#25239): Add documentation.
pub type UIntGauge = DeleteOnDropGauge<'static, AtomicU64, Vec<String>>;
type Histogram = DeleteOnDropHistogram<'static, Vec<String>>;

/// Compute controller metrics
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
    peek_count: UIntGaugeVec,
    subscribe_count: UIntGaugeVec,
    copy_to_count: UIntGaugeVec,
    command_queue_size: UIntGaugeVec,
    response_queue_size: UIntGaugeVec,

    // command history
    history_command_count: UIntGaugeVec,
    history_dataflow_count: UIntGaugeVec,

    // peeks
    peeks_total: IntCounterVec,
    peek_duration_seconds: HistogramVec,

    // dataflows
    dataflow_initial_output_duration_seconds: GaugeVec,
}

impl ComputeControllerMetrics {
    /// TODO(#25239): Add documentation.
    pub fn new(metrics_registry: MetricsRegistry) -> Self {
        ComputeControllerMetrics {
            commands_total: metrics_registry.register(metric!(
                name: "mz_compute_commands_total",
                help: "The total number of compute commands sent.",
                var_labels: ["instance_id", "replica_id", "command_type"],
            )),
            command_message_bytes_total: metrics_registry.register(metric!(
                name: "mz_compute_command_message_bytes_total",
                help: "The total number of bytes sent in compute command messages.",
                var_labels: ["instance_id", "replica_id", "command_type"],
            )),
            responses_total: metrics_registry.register(metric!(
                name: "mz_compute_responses_total",
                help: "The total number of compute responses sent.",
                var_labels: ["instance_id", "replica_id", "response_type"],
            )),
            response_message_bytes_total: metrics_registry.register(metric!(
                name: "mz_compute_response_message_bytes_total",
                help: "The total number of bytes sent in compute response messages.",
                var_labels: ["instance_id", "replica_id", "response_type"],
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
            response_queue_size: metrics_registry.register(metric!(
                name: "mz_compute_controller_response_queue_size",
                help: "The size of the compute response queue.",
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
            dataflow_initial_output_duration_seconds: metrics_registry.register(metric!(
                name: "mz_dataflow_initial_output_duration_seconds",
                help: "The time from dataflow creation up to when the first output was produced.",
                var_labels: ["instance_id", "replica_id", "collection_id"],
            )),
        }
    }

    /// TODO(#25239): Add documentation.
    pub fn for_instance(&self, instance_id: ComputeInstanceId) -> InstanceMetrics {
        let labels = vec![instance_id.to_string()];
        let replica_count = self.replica_count.get_delete_on_drop_gauge(labels.clone());
        let collection_count = self
            .collection_count
            .get_delete_on_drop_gauge(labels.clone());
        let peek_count = self.peek_count.get_delete_on_drop_gauge(labels.clone());
        let subscribe_count = self
            .subscribe_count
            .get_delete_on_drop_gauge(labels.clone());
        let copy_to_count = self.copy_to_count.get_delete_on_drop_gauge(labels.clone());
        let history_command_count = CommandMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.history_command_count.get_delete_on_drop_gauge(labels)
        });
        let history_dataflow_count = self
            .history_dataflow_count
            .get_delete_on_drop_gauge(labels.clone());
        let peeks_total = PeekMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.peeks_total.get_delete_on_drop_counter(labels)
        });
        let peek_duration_seconds = PeekMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.peek_duration_seconds
                .get_delete_on_drop_histogram(labels)
        });

        InstanceMetrics {
            instance_id,
            metrics: self.clone(),
            replica_count,
            collection_count,
            copy_to_count,
            peek_count,
            subscribe_count,
            history_command_count,
            history_dataflow_count,
            peeks_total,
            peek_duration_seconds,
        }
    }
}

/// Per-instance metrics
#[derive(Debug)]
pub struct InstanceMetrics {
    instance_id: ComputeInstanceId,
    metrics: ComputeControllerMetrics,

    /// TODO(#25239): Add documentation.
    pub replica_count: UIntGauge,
    /// TODO(#25239): Add documentation.
    pub collection_count: UIntGauge,
    /// TODO(#25239): Add documentation.
    pub peek_count: UIntGauge,
    /// TODO(#25239): Add documentation.
    pub subscribe_count: UIntGauge,
    /// A counter to keep track of the number of active COPY TO queries in progress.
    pub copy_to_count: UIntGauge,
    /// TODO(#25239): Add documentation.
    pub history_command_count: CommandMetrics<UIntGauge>,
    /// TODO(#25239): Add documentation.
    pub history_dataflow_count: UIntGauge,
    /// TODO(#25239): Add documentation.
    pub peeks_total: PeekMetrics<IntCounter>,
    /// TODO(#25239): Add documentation.
    pub peek_duration_seconds: PeekMetrics<Histogram>,
}

impl InstanceMetrics {
    /// TODO(#25239): Add documentation.
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
                .get_delete_on_drop_counter(labels)
        });
        let command_message_bytes_total = CommandMetrics::build(|typ| {
            let labels = extended_labels(typ);
            self.metrics
                .command_message_bytes_total
                .get_delete_on_drop_counter(labels)
        });
        let responses_total = ResponseMetrics::build(|typ| {
            let labels = extended_labels(typ);
            self.metrics
                .responses_total
                .get_delete_on_drop_counter(labels)
        });
        let response_message_bytes_total = ResponseMetrics::build(|typ| {
            let labels = extended_labels(typ);
            self.metrics
                .response_message_bytes_total
                .get_delete_on_drop_counter(labels)
        });

        let command_queue_size = self
            .metrics
            .command_queue_size
            .get_delete_on_drop_gauge(labels.clone());
        let response_queue_size = self
            .metrics
            .response_queue_size
            .get_delete_on_drop_gauge(labels.clone());

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
                response_queue_size,
            }),
        }
    }

    /// TODO(#25239): Add documentation.
    pub fn for_history(&self) -> HistoryMetrics<UIntGauge> {
        let labels = vec![self.instance_id.to_string()];
        let command_counts = CommandMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.metrics
                .history_command_count
                .get_delete_on_drop_gauge(labels)
        });
        let dataflow_count = self
            .metrics
            .history_dataflow_count
            .get_delete_on_drop_gauge(labels);

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

    /// TODO(#25239): Add documentation.
    pub inner: Arc<ReplicaMetricsInner>,
}

/// TODO(#25239): Add documentation.
#[derive(Debug)]
pub struct ReplicaMetricsInner {
    commands_total: CommandMetrics<IntCounter>,
    command_message_bytes_total: CommandMetrics<IntCounter>,
    responses_total: ResponseMetrics<IntCounter>,
    response_message_bytes_total: ResponseMetrics<IntCounter>,

    /// TODO(#25239): Add documentation.
    pub command_queue_size: UIntGauge,
    /// TODO(#25239): Add documentation.
    pub response_queue_size: UIntGauge,
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

        let labels = vec![
            self.instance_id.to_string(),
            self.replica_id.to_string(),
            collection_id.to_string(),
        ];
        let initial_output_duration_seconds = self
            .metrics
            .dataflow_initial_output_duration_seconds
            .get_delete_on_drop_gauge(labels);

        Some(ReplicaCollectionMetrics {
            initial_output_duration_seconds,
        })
    }
}

/// Make [`ReplicaMetrics`] pluggable into the gRPC connection.
impl StatsCollector<ProtoComputeCommand, ProtoComputeResponse> for ReplicaMetrics {
    fn send_event(&self, item: &ProtoComputeCommand, size: usize) {
        self.inner.commands_total.for_proto_command(item).inc();
        self.inner
            .command_message_bytes_total
            .for_proto_command(item)
            .inc_by(u64::cast_from(size));
    }

    fn receive_event(&self, item: &ProtoComputeResponse, size: usize) {
        self.inner.responses_total.for_proto_response(item).inc();
        self.inner
            .response_message_bytes_total
            .for_proto_response(item)
            .inc_by(u64::cast_from(size));
    }
}

/// Per-replica-and-collection metrics.
#[derive(Debug)]
pub(crate) struct ReplicaCollectionMetrics {
    /// TODO(#25239): Add documentation.
    pub initial_output_duration_seconds: Gauge,
}

/// Metrics keyed by `ComputeCommand` type.
#[derive(Debug)]
pub struct CommandMetrics<M> {
    /// TODO(#25239): Add documentation.
    pub create_timely: M,
    /// TODO(#25239): Add documentation.
    pub create_instance: M,
    /// TODO(#25239): Add documentation.
    pub create_dataflow: M,
    /// TODO(#25239): Add documentation.
    pub allow_compaction: M,
    /// TODO(#25239): Add documentation.
    pub peek: M,
    /// TODO(#25239): Add documentation.
    pub cancel_peek: M,
    /// TODO(#25239): Add documentation.
    pub initialization_complete: M,
    /// TODO(#25239): Add documentation.
    pub update_configuration: M,
}

impl<M> CommandMetrics<M> {
    /// TODO(#25239): Add documentation.
    pub fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            create_timely: build_metric("create_timely"),
            create_instance: build_metric("create_instance"),
            create_dataflow: build_metric("create_dataflow"),
            allow_compaction: build_metric("allow_compaction"),
            peek: build_metric("peek"),
            cancel_peek: build_metric("cancel_peek"),
            initialization_complete: build_metric("initialization_complete"),
            update_configuration: build_metric("update_configuration"),
        }
    }

    fn for_all<F>(&self, f: F)
    where
        F: Fn(&M),
    {
        f(&self.create_timely);
        f(&self.create_instance);
        f(&self.initialization_complete);
        f(&self.update_configuration);
        f(&self.create_dataflow);
        f(&self.allow_compaction);
        f(&self.peek);
        f(&self.cancel_peek);
    }

    /// TODO(#25239): Add documentation.
    pub fn for_command<T>(&self, command: &ComputeCommand<T>) -> &M {
        use ComputeCommand::*;

        match command {
            CreateTimely { .. } => &self.create_timely,
            CreateInstance(_) => &self.create_instance,
            InitializationComplete => &self.initialization_complete,
            UpdateConfiguration(_) => &self.update_configuration,
            CreateDataflow(_) => &self.create_dataflow,
            AllowCompaction { .. } => &self.allow_compaction,
            Peek(_) => &self.peek,
            CancelPeek { .. } => &self.cancel_peek,
        }
    }

    fn for_proto_command(&self, proto: &ProtoComputeCommand) -> &M {
        use crate::protocol::command::proto_compute_command::Kind::*;

        match proto.kind.as_ref().unwrap() {
            CreateTimely(_) => &self.create_timely,
            CreateInstance(_) => &self.create_instance,
            CreateDataflow(_) => &self.create_dataflow,
            AllowCompaction(_) => &self.allow_compaction,
            Peek(_) => &self.peek,
            CancelPeek(_) => &self.cancel_peek,
            InitializationComplete(_) => &self.initialization_complete,
            UpdateConfiguration(_) => &self.update_configuration,
        }
    }
}

/// Metrics keyed by `ComputeResponse` type.
#[derive(Debug)]
struct ResponseMetrics<M> {
    frontier_upper: M,
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
            frontier_upper: build_metric("frontier_upper"),
            peek_response: build_metric("peek_response"),
            subscribe_response: build_metric("subscribe_response"),
            copy_to_response: build_metric("copy_to_response"),
            status: build_metric("status"),
        }
    }

    fn for_proto_response(&self, proto: &ProtoComputeResponse) -> &M {
        use crate::protocol::response::proto_compute_response::Kind::*;

        match proto.kind.as_ref().unwrap() {
            FrontierUpper(_) => &self.frontier_upper,
            PeekResponse(_) => &self.peek_response,
            SubscribeResponse(_) => &self.subscribe_response,
            CopyToResponse(_) => &self.copy_to_response,
            Status(_) => &self.status,
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
            error: build_metric("error"),
            canceled: build_metric("canceled"),
        }
    }

    fn for_peek_response(&self, response: &PeekResponse) -> &M {
        use PeekResponse::*;

        match response {
            Rows(_) => &self.rows,
            Error(_) => &self.error,
            Canceled => &self.canceled,
        }
    }
}
