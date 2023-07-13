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

use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{
    CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt, IntCounterVec,
    MetricsRegistry, UIntGaugeVec,
};
use mz_service::codec::StatsCollector;
use prometheus::core::AtomicU64;

use crate::controller::{ComputeInstanceId, ReplicaId};
use crate::protocol::command::{ComputeCommand, ProtoComputeCommand};
use crate::protocol::response::ProtoComputeResponse;

type IntCounter = DeleteOnDropCounter<'static, AtomicU64, Vec<String>>;
pub type UIntGauge = DeleteOnDropGauge<'static, AtomicU64, Vec<String>>;

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
    command_queue_size: UIntGaugeVec,
    response_queue_size: UIntGaugeVec,

    // command history
    history_command_count: UIntGaugeVec,
    history_dataflow_count: UIntGaugeVec,
}

impl ComputeControllerMetrics {
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
        }
    }

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
        let history_command_count = CommandMetrics::build(|typ| {
            let labels = labels.iter().cloned().chain([typ.into()]).collect();
            self.history_command_count.get_delete_on_drop_gauge(labels)
        });
        let history_dataflow_count = self.history_dataflow_count.get_delete_on_drop_gauge(labels);

        InstanceMetrics {
            instance_id,
            metrics: self.clone(),
            replica_count,
            collection_count,
            peek_count,
            subscribe_count,
            history_command_count,
            history_dataflow_count,
        }
    }
}

/// Per-instance metrics
#[derive(Debug)]
pub struct InstanceMetrics {
    instance_id: ComputeInstanceId,
    metrics: ComputeControllerMetrics,

    pub replica_count: UIntGauge,
    pub collection_count: UIntGauge,
    pub peek_count: UIntGauge,
    pub subscribe_count: UIntGauge,
    pub history_command_count: CommandMetrics<UIntGauge>,
    pub history_dataflow_count: UIntGauge,
}

impl InstanceMetrics {
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
}

/// Per-replica metrics.
#[derive(Debug, Clone)]
pub struct ReplicaMetrics {
    pub inner: Arc<ReplicaMetricsInner>,
}

#[derive(Debug)]
pub struct ReplicaMetricsInner {
    commands_total: CommandMetrics<IntCounter>,
    command_message_bytes_total: CommandMetrics<IntCounter>,
    responses_total: ResponseMetrics<IntCounter>,
    response_message_bytes_total: ResponseMetrics<IntCounter>,

    pub command_queue_size: UIntGauge,
    pub response_queue_size: UIntGauge,
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

/// Metrics keyed by `ComputeCommand` type.
#[derive(Debug)]
pub struct CommandMetrics<M> {
    create_timely: M,
    create_instance: M,
    create_dataflows: M,
    allow_compaction: M,
    peek: M,
    cancel_peeks: M,
    initialization_complete: M,
    update_configuration: M,
}

impl<M> CommandMetrics<M> {
    pub fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            create_timely: build_metric("create_timely"),
            create_instance: build_metric("create_instance"),
            create_dataflows: build_metric("create_dataflows"),
            allow_compaction: build_metric("allow_compaction"),
            peek: build_metric("peek"),
            cancel_peeks: build_metric("cancel_peeks"),
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
        f(&self.create_dataflows);
        f(&self.allow_compaction);
        f(&self.peek);
        f(&self.cancel_peeks);
    }

    pub fn for_command<T>(&self, command: &ComputeCommand<T>) -> &M {
        use ComputeCommand::*;

        match command {
            CreateTimely { .. } => &self.create_timely,
            CreateInstance(_) => &self.create_instance,
            InitializationComplete => &self.initialization_complete,
            UpdateConfiguration(_) => &self.update_configuration,
            CreateDataflows(_) => &self.create_dataflows,
            AllowCompaction(_) => &self.allow_compaction,
            Peek(_) => &self.peek,
            CancelPeeks { .. } => &self.cancel_peeks,
        }
    }

    fn for_proto_command(&self, proto: &ProtoComputeCommand) -> &M {
        use crate::protocol::command::proto_compute_command::Kind::*;

        match proto.kind.as_ref().unwrap() {
            CreateTimely(_) => &self.create_timely,
            CreateInstance(_) => &self.create_instance,
            CreateDataflows(_) => &self.create_dataflows,
            AllowCompaction(_) => &self.allow_compaction,
            Peek(_) => &self.peek,
            CancelPeeks(_) => &self.cancel_peeks,
            InitializationComplete(_) => &self.initialization_complete,
            UpdateConfiguration(_) => &self.update_configuration,
        }
    }
}

/// Metrics keyed by `ComputeResponse` type.
#[derive(Debug)]
struct ResponseMetrics<M> {
    frontier_uppers: M,
    peek_response: M,
    subscribe_response: M,
}

impl<M> ResponseMetrics<M> {
    fn build<F>(build_metric: F) -> Self
    where
        F: Fn(&str) -> M,
    {
        Self {
            frontier_uppers: build_metric("frontier_uppers"),
            peek_response: build_metric("peek_response"),
            subscribe_response: build_metric("subscribe_response"),
        }
    }

    fn for_proto_response(&self, proto: &ProtoComputeResponse) -> &M {
        use crate::protocol::response::proto_compute_response::Kind::*;

        match proto.kind.as_ref().unwrap() {
            FrontierUppers(_) => &self.frontier_uppers,
            PeekResponse(_) => &self.peek_response,
            SubscribeResponse(_) => &self.subscribe_response,
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
