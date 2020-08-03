// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics for our interactive dataflow server

use lazy_static::lazy_static;
use prometheus::{
    register_int_counter_vec, register_int_gauge_vec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec,
};

use super::{PendingPeek, SequencedCommand};

lazy_static! {
    pub(super) static ref COMMAND_QUEUE_RAW: IntGaugeVec = register_int_gauge_vec!(
        "mz_worker_command_queue_size",
        "the number of commands we would like to handle",
        &["worker"]
    )
    .unwrap();
    pub(super) static ref PENDING_PEEKS_RAW: IntGaugeVec = register_int_gauge_vec!(
        "mz_worker_pending_peeks_queue_size",
        "the number of peeks remaining to be processed",
        &["worker"]
    )
    .unwrap();
    pub(super) static ref COMMANDS_PROCESSED_RAW: IntCounterVec = register_int_counter_vec!(
        "mz_worker_commands_processed_total",
        "How many commands of various kinds we have processed",
        &["worker", "command"]
    )
    .unwrap();
}

/// Prometheus metrics that we would like to easily export
pub(super) struct Metrics {
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

impl Metrics {
    pub(super) fn for_worker_id(id: usize) -> Metrics {
        let worker_id = id.to_string();

        Metrics {
            command_queue: COMMAND_QUEUE_RAW.with_label_values(&[&worker_id]),
            pending_peeks: PENDING_PEEKS_RAW.with_label_values(&[&worker_id]),
            commands_processed: CommandsProcessedMetrics::new(&worker_id),
        }
    }

    pub(super) fn observe_command_queue(&self, commands: &[SequencedCommand]) {
        self.command_queue.set(commands.len() as i64);
    }

    pub(super) fn observe_pending_peeks(&self, pending_peeks: &[PendingPeek]) {
        self.pending_peeks.set(pending_peeks.len() as i64);
    }

    /// Observe that we have executed a command. Must be paired with [`observe_command_finish`]
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
    append_log_int: i32,
    append_log: IntCounter,
    advance_source_timestamp_int: i32,
    advance_source_timestamp: IntCounter,
    enable_feedback_int: i32,
    enable_feedback: IntCounter,
    enable_persistence_int: i32,
    enable_persistence: IntCounter,
    shutdown_int: i32,
    shutdown: IntCounter,
    advance_all_local_inputs_int: i32,
    advance_all_local_inputs: IntCounter,
}

impl CommandsProcessedMetrics {
    fn new(worker: &str) -> CommandsProcessedMetrics {
        CommandsProcessedMetrics {
            create_dataflows_int: 0,
            create_dataflows: COMMANDS_PROCESSED_RAW
                .with_label_values(&[worker, "create_dataflows"]),
            drop_sources_int: 0,
            drop_sources: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "drop_sources"]),
            drop_sinks_int: 0,
            drop_sinks: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "drop_sinks"]),
            drop_indexes_int: 0,
            drop_indexes: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "drop_indexes"]),
            peek_int: 0,
            peek: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "peek"]),
            cancel_peek_int: 0,
            cancel_peek: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "cancel_peek"]),
            create_local_input_int: 0,
            create_local_input: COMMANDS_PROCESSED_RAW
                .with_label_values(&[worker, "create_local_input"]),
            insert_int: 0,
            insert: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "insert"]),
            allow_compaction_int: 0,
            allow_compaction: COMMANDS_PROCESSED_RAW
                .with_label_values(&[worker, "allow_compaction"]),
            append_log_int: 0,
            append_log: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "append_log"]),
            advance_source_timestamp_int: 0,
            advance_source_timestamp: COMMANDS_PROCESSED_RAW
                .with_label_values(&[worker, "advance_source_timestamp"]),
            enable_feedback_int: 0,
            enable_feedback: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "enable_feedback"]),
            enable_persistence_int: 0,
            enable_persistence: COMMANDS_PROCESSED_RAW
                .with_label_values(&[worker, "enable_persistence"]),
            shutdown_int: 0,
            shutdown: COMMANDS_PROCESSED_RAW.with_label_values(&[worker, "shutdown"]),
            advance_all_local_inputs_int: 0,
            advance_all_local_inputs: COMMANDS_PROCESSED_RAW
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
            SequencedCommand::CreateLocalInput { .. } => self.create_local_input_int += 1,
            SequencedCommand::Insert { .. } => self.insert_int += 1,
            SequencedCommand::AllowCompaction(..) => self.allow_compaction_int += 1,
            SequencedCommand::AppendLog(..) => self.append_log_int += 1,
            SequencedCommand::AdvanceSourceTimestamp { .. } => {
                self.advance_source_timestamp_int += 1
            }
            SequencedCommand::EnableFeedback(..) => self.enable_feedback_int += 1,
            SequencedCommand::EnablePersistence(..) => self.enable_persistence_int += 1,
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
        if self.create_local_input_int > 0 {
            self.create_local_input
                .inc_by(self.create_local_input_int as i64);
            self.create_local_input_int = 0;
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
        if self.append_log_int > 0 {
            self.append_log.inc_by(self.append_log_int as i64);
            self.append_log_int = 0;
        }
        if self.advance_source_timestamp_int > 0 {
            self.advance_source_timestamp
                .inc_by(self.advance_source_timestamp_int as i64);
            self.advance_source_timestamp_int = 0;
        }
        if self.enable_feedback_int > 0 {
            self.enable_feedback.inc_by(self.enable_feedback_int as i64);
            self.enable_feedback_int = 0;
        }
        if self.enable_persistence_int > 0 {
            self.enable_persistence
                .inc_by(self.enable_persistence_int as i64);
            self.enable_persistence_int = 0;
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
