// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics for our interactive dataflow server

use ore::metrics::MetricsRegistry;
use ore::{
    metric,
    metrics::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec},
};

use super::{PendingPeek, SequencedCommand};

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
            command_queue: self.command_queue.with_label_values(&[&worker_id]),
            pending_peeks: self.pending_peeks.with_label_values(&[&worker_id]),
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
    command_queue: IntGauge,
    /// The number of pending peeks
    ///
    /// Updated every time we successfully fulfill a peek
    pending_peeks: IntGauge,
    /// Total number of commands of each type processed
    commands_processed: CommandsProcessedMetrics,
}

impl WorkerMetrics {
    pub(super) fn observe_command_queue(&self, commands: &[SequencedCommand]) {
        self.command_queue.set(commands.len() as i64);
    }

    pub(super) fn observe_pending_peeks(&self, pending_peeks: &[PendingPeek]) {
        self.pending_peeks.set(pending_peeks.len() as i64);
    }

    /// Observe that we have executed a command. Must be paired with [`WorkerMetrics::observe_command_finish`]
    pub(super) fn observe_command(&mut self, command: &SequencedCommand) {
        self.commands_processed.observe(command)
    }

    /// Observe that we have executed a command
    pub(super) fn observe_command_finish(&mut self) {
        self.commands_processed.finish()
    }
}

/// Count of how many metrics we have processed for a given command type
#[derive(Debug)]
struct CommandsProcessedMetrics {
    create_dataflows_int: i32,
    create_dataflows: IntCounter,
    drop_sources_int: i32,
    drop_sources: IntCounter,
    drop_sinks_int: i32,
    drop_sinks: IntCounter,
    drop_indexes_int: i32,
    drop_indexes: IntCounter,
    peek_int: i32,
    peek: IntCounter,
    cancel_peek_int: i32,
    cancel_peek: IntCounter,
    create_local_input_int: i32,
    create_local_input: IntCounter,
    insert_int: i32,
    insert: IntCounter,
    allow_compaction_int: i32,
    allow_compaction: IntCounter,
    durability_frontier_updates_int: i32,
    durability_frontier_updates: IntCounter,
    append_log_int: i32,
    append_log: IntCounter,
    add_source_timestamping_int: i32,
    add_source_timestamping: IntCounter,
    advance_source_timestamp_int: i32,
    advance_source_timestamp: IntCounter,
    drop_source_timestamping_int: i32,
    drop_source_timestamping: IntCounter,
    enable_feedback_int: i32,
    enable_feedback: IntCounter,
    enable_logging_int: i32,
    enable_logging: IntCounter,
    shutdown_int: i32,
    shutdown: IntCounter,
    advance_all_local_inputs_int: i32,
    advance_all_local_inputs: IntCounter,
}

impl CommandsProcessedMetrics {
    fn new(worker: &str, commands_processed_metric: &IntCounterVec) -> CommandsProcessedMetrics {
        CommandsProcessedMetrics {
            create_dataflows_int: 0,
            create_dataflows: commands_processed_metric
                .with_label_values(&[worker, "create_dataflows"]),
            drop_sources_int: 0,
            drop_sources: commands_processed_metric.with_label_values(&[worker, "drop_sources"]),
            drop_sinks_int: 0,
            drop_sinks: commands_processed_metric.with_label_values(&[worker, "drop_sinks"]),
            drop_indexes_int: 0,
            drop_indexes: commands_processed_metric.with_label_values(&[worker, "drop_indexes"]),
            peek_int: 0,
            peek: commands_processed_metric.with_label_values(&[worker, "peek"]),
            cancel_peek_int: 0,
            cancel_peek: commands_processed_metric.with_label_values(&[worker, "cancel_peek"]),
            create_local_input_int: 0,
            create_local_input: commands_processed_metric
                .with_label_values(&[worker, "create_local_input"]),
            insert_int: 0,
            insert: commands_processed_metric.with_label_values(&[worker, "insert"]),
            allow_compaction_int: 0,
            allow_compaction: commands_processed_metric
                .with_label_values(&[worker, "allow_compaction"]),
            durability_frontier_updates_int: 0,
            durability_frontier_updates: commands_processed_metric
                .with_label_values(&[worker, "durability_frontier_updates"]),
            append_log_int: 0,
            append_log: commands_processed_metric.with_label_values(&[worker, "append_log"]),
            add_source_timestamping_int: 0,
            add_source_timestamping: commands_processed_metric
                .with_label_values(&[worker, "add_source_timestamping"]),
            advance_source_timestamp_int: 0,
            advance_source_timestamp: commands_processed_metric
                .with_label_values(&[worker, "advance_source_timestamp"]),
            drop_source_timestamping_int: 0,
            drop_source_timestamping: commands_processed_metric
                .with_label_values(&[worker, "drop_source_timestamping"]),
            enable_feedback_int: 0,
            enable_feedback: commands_processed_metric
                .with_label_values(&[worker, "enable_feedback"]),
            enable_logging_int: 0,
            enable_logging: commands_processed_metric
                .with_label_values(&[worker, "enable_logging"]),
            shutdown_int: 0,
            shutdown: commands_processed_metric.with_label_values(&[worker, "shutdown"]),
            advance_all_local_inputs_int: 0,
            advance_all_local_inputs: commands_processed_metric
                .with_label_values(&[worker, "advance_all_local_inputs"]),
        }
    }

    fn observe(&mut self, command: &SequencedCommand) {
        match command {
            SequencedCommand::CreateDataflows(..) => self.create_dataflows_int += 1,
            SequencedCommand::DropSources(..) => self.drop_sources_int += 1,
            SequencedCommand::DropSinks(..) => self.drop_sinks_int += 1,
            SequencedCommand::DropIndexes(..) => self.drop_indexes_int += 1,
            SequencedCommand::Peek { .. } => self.peek_int += 1,
            SequencedCommand::CancelPeek { .. } => self.cancel_peek_int += 1,
            SequencedCommand::Insert { .. } => self.insert_int += 1,
            SequencedCommand::AllowCompaction(..) => self.allow_compaction_int += 1,
            SequencedCommand::DurabilityFrontierUpdates(..) => {
                self.durability_frontier_updates_int += 1
            }
            SequencedCommand::AddSourceTimestamping { .. } => self.add_source_timestamping_int += 1,
            SequencedCommand::AdvanceSourceTimestamp { .. } => {
                self.advance_source_timestamp_int += 1
            }
            SequencedCommand::DropSourceTimestamping { .. } => {
                self.drop_source_timestamping_int += 1
            }
            SequencedCommand::EnableFeedback(..) => self.enable_feedback_int += 1,
            SequencedCommand::EnableLogging(_) => self.enable_logging_int += 1,
            SequencedCommand::Shutdown { .. } => self.shutdown_int += 1,
            SequencedCommand::AdvanceAllLocalInputs { .. } => {
                self.advance_all_local_inputs_int += 1
            }
        }
    }

    fn finish(&mut self) {
        if self.create_dataflows_int > 0 {
            self.create_dataflows
                .inc_by(self.create_dataflows_int as i64);
            self.create_dataflows_int = 0;
        }
        if self.drop_sources_int > 0 {
            self.drop_sources.inc_by(self.drop_sources_int as i64);
            self.drop_sources_int = 0;
        }
        if self.drop_sinks_int > 0 {
            self.drop_sinks.inc_by(self.drop_sinks_int as i64);
            self.drop_sinks_int = 0;
        }
        if self.drop_indexes_int > 0 {
            self.drop_indexes.inc_by(self.drop_indexes_int as i64);
            self.drop_indexes_int = 0;
        }
        if self.peek_int > 0 {
            self.peek.inc_by(self.peek_int as i64);
            self.peek_int = 0;
        }
        if self.cancel_peek_int > 0 {
            self.cancel_peek.inc_by(self.cancel_peek_int as i64);
            self.cancel_peek_int = 0;
        }
        if self.insert_int > 0 {
            self.insert.inc_by(self.insert_int as i64);
            self.insert_int = 0;
        }
        if self.allow_compaction_int > 0 {
            self.allow_compaction
                .inc_by(self.allow_compaction_int as i64);
            self.allow_compaction_int = 0;
        }
        if self.durability_frontier_updates_int > 0 {
            self.durability_frontier_updates
                .inc_by(self.durability_frontier_updates_int as i64);
            self.durability_frontier_updates_int = 0;
        }
        if self.add_source_timestamping_int > 0 {
            self.add_source_timestamping
                .inc_by(self.add_source_timestamping_int as i64);
            self.add_source_timestamping_int = 0;
        }
        if self.advance_source_timestamp_int > 0 {
            self.advance_source_timestamp
                .inc_by(self.advance_source_timestamp_int as i64);
            self.advance_source_timestamp_int = 0;
        }
        if self.drop_source_timestamping_int > 0 {
            self.drop_source_timestamping
                .inc_by(self.drop_source_timestamping_int as i64);
            self.drop_source_timestamping_int = 0;
        }
        if self.enable_feedback_int > 0 {
            self.enable_feedback.inc_by(self.enable_feedback_int as i64);
            self.enable_feedback_int = 0;
        }
        if self.shutdown_int > 0 {
            self.shutdown.inc_by(self.shutdown_int as i64);
            self.shutdown_int = 0;
        }
        if self.advance_all_local_inputs_int > 0 {
            self.advance_all_local_inputs
                .inc_by(self.advance_all_local_inputs_int as i64);
            self.advance_all_local_inputs_int = 0;
        }
    }
}
