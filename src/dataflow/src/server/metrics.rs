// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics for our interactive dataflow server

use enum_iterator::IntoEnumIterator;
use ore::metrics::{
    CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt, MetricsRegistry,
};
use ore::{
    metric,
    metrics::{IntCounterVec, IntGaugeVec},
};

use super::PendingPeek;
use dataflow_types::client::{Command, CommandKind};
use prometheus::core::AtomicI64;

#[derive(Clone)]
pub(super) struct ServerMetrics {
    command_queue: IntGaugeVec,
    pending_peeks: IntGaugeVec,
    commands_processed_metric: IntCounterVec,
}

impl ServerMetrics {
    pub(super) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            command_queue: registry.register(metric!(
                name: "mz_worker_command_queue_size",
                help: "the number of commands we would like to handle",
                var_labels: ["worker"],
            )),
            pending_peeks: registry.register(metric!(
                name: "mz_worker_pending_peeks_queue_size",
                help: "the number of peeks remaining to be processed",
                var_labels: ["worker"],
            )),
            commands_processed_metric: registry.register(metric!(
                name: "mz_worker_commands_processed_total",
                help: "How many commands of various kinds we have processed",
                var_labels: ["worker", "command"],
            )),
        }
    }

    pub(super) fn for_worker_id(&self, id: usize) -> WorkerMetrics {
        let worker_id = id.to_string();

        WorkerMetrics {
            command_queue: self
                .command_queue
                .get_delete_on_drop_gauge(vec![worker_id.clone()]),
            pending_peeks: self
                .pending_peeks
                .get_delete_on_drop_gauge(vec![worker_id.clone()]),
            commands_processed: CommandsProcessedMetrics::new(
                &worker_id,
                &self.commands_processed_metric,
            ),
        }
    }
}

/// Prometheus metrics that we would like to easily export
pub(super) struct WorkerMetrics {
    /// The size of the command queue
    ///
    /// Updated every time we decide to handle some
    command_queue: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The number of pending peeks
    ///
    /// Updated every time we successfully fulfill a peek
    pending_peeks: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// Total number of commands of each type processed
    commands_processed: CommandsProcessedMetrics,
}

impl WorkerMetrics {
    pub(super) fn observe_command_queue(&self, commands: &[Command]) {
        self.command_queue.set(commands.len() as i64);
    }

    pub(super) fn observe_pending_peeks(&self, pending_peeks: &[PendingPeek]) {
        self.pending_peeks.set(pending_peeks.len() as i64);
    }

    /// Observe that we have executed a command. Must be paired with [`WorkerMetrics::observe_command_finish`]
    pub(super) fn observe_command(&mut self, command: &Command) {
        self.commands_processed.observe(command.into());
    }

    /// Observe that we have executed a command
    pub(super) fn observe_command_finish(&mut self) {
        self.commands_processed.finish()
    }
}

/// Count of how many metrics we have processed for a given command type
#[derive(Debug)]
struct CommandsProcessedMetrics {
    cache: Vec<i64>,
    counters: Vec<DeleteOnDropCounter<'static, AtomicI64, Vec<String>>>,
}

impl CommandsProcessedMetrics {
    fn new(worker: &str, commands_processed_metric: &IntCounterVec) -> CommandsProcessedMetrics {
        CommandsProcessedMetrics {
            cache: CommandKind::into_enum_iter().map(|_| 0).collect(),
            counters: CommandKind::into_enum_iter()
                .map(|kind| {
                    commands_processed_metric.get_delete_on_drop_counter(vec![
                        worker.to_string(),
                        kind.name().to_string(),
                    ])
                })
                .collect(),
        }
    }

    fn observe(&mut self, command: CommandKind) {
        let idx: usize = command.into();
        self.cache[idx] += 1;
    }

    fn finish(&mut self) {
        for (cache_entry, counter) in self.cache.iter_mut().zip(self.counters.iter()) {
            if *cache_entry != 0 {
                counter.inc_by(*cache_entry);
            }
            *cache_entry = 0;
        }
    }
}
