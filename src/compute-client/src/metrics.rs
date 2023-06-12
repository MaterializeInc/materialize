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

use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, IntCounterVec, MetricsRegistry};
use mz_service::codec::StatsCollector;
use prometheus::core::AtomicU64;

use crate::controller::{ComputeInstanceId, ReplicaId};
use crate::protocol::command::ProtoComputeCommand;
use crate::protocol::response::ProtoComputeResponse;

/// Compute controller metrics
#[derive(Debug, Clone)]
pub struct ComputeControllerMetrics {
    command_message_bytes_total: IntCounterVec,
    response_message_bytes_total: IntCounterVec,
}

impl ComputeControllerMetrics {
    pub fn new(metrics_registry: MetricsRegistry) -> Self {
        ComputeControllerMetrics {
            command_message_bytes_total: metrics_registry.register(metric!(
                name: "mz_compute_command_message_bytes_total",
                help: "The total number of bytes sent in compute command messages.",
                var_labels: ["instance_id", "replica_id", "command_type"],
            )),
            response_message_bytes_total: metrics_registry.register(metric!(
                name: "mz_compute_response_message_bytes_total",
                help: "The total number of bytes sent in compute response messages.",
                var_labels: ["instance_id", "replica_id", "response_type"],
            )),
        }
    }

    pub fn for_instance(&self, instance_id: ComputeInstanceId) -> InstanceMetrics {
        InstanceMetrics {
            instance_id,
            metrics: self.clone(),
        }
    }
}

/// Per-instance metrics
#[derive(Debug, Clone)]
pub struct InstanceMetrics {
    instance_id: ComputeInstanceId,
    metrics: ComputeControllerMetrics,
}

impl InstanceMetrics {
    pub fn for_replica(&self, replica_id: ReplicaId) -> ReplicaMetrics {
        let instance_id = self.instance_id.to_string();
        let replica_id = replica_id.to_string();

        let command_message_bytes_total = CommandMetrics::build(|typ| {
            let labels = vec![instance_id.clone(), replica_id.clone(), typ.into()];
            self.metrics
                .command_message_bytes_total
                .get_delete_on_drop_counter(labels)
        });
        let response_message_bytes_total = ResponseMetrics::build(|typ| {
            let labels = vec![instance_id.clone(), replica_id.clone(), typ.into()];
            self.metrics
                .response_message_bytes_total
                .get_delete_on_drop_counter(labels)
        });
        ReplicaMetrics {
            inner: Arc::new(ReplicaMetricsInner {
                command_message_bytes_total,
                response_message_bytes_total,
            }),
        }
    }
}

/// Per-replica metrics.
#[derive(Debug, Clone)]
pub struct ReplicaMetrics {
    inner: Arc<ReplicaMetricsInner>,
}

type IntCounter = DeleteOnDropCounter<'static, AtomicU64, Vec<String>>;

#[derive(Debug)]
struct ReplicaMetricsInner {
    command_message_bytes_total: CommandMetrics<IntCounter>,
    response_message_bytes_total: ResponseMetrics<IntCounter>,
}

/// Make [`ReplicaMetrics`] pluggable into the gRPC connection.
impl StatsCollector<ProtoComputeCommand, ProtoComputeResponse> for ReplicaMetrics {
    fn send_event(&self, item: &ProtoComputeCommand, size: usize) {
        self.inner
            .command_message_bytes_total
            .for_proto_command(item)
            .inc_by(u64::cast_from(size));
    }

    fn receive_event(&self, item: &ProtoComputeResponse, size: usize) {
        self.inner
            .response_message_bytes_total
            .for_proto_response(item)
            .inc_by(u64::cast_from(size));
    }
}

/// Metrics keyed by `ComputeCommand` type.
#[derive(Debug)]
struct CommandMetrics<M> {
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
    fn build<F>(build_metric: F) -> Self
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
