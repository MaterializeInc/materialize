// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::stats::{histogram_milliseconds_buckets, histogram_seconds_buckets};
use mz_sql::ast::{AstInfo, Statement, StatementKind, SubscribeOutput};
use mz_sql::session::user::User;
use mz_sql_parser::ast::statement_kind_label_value;
use prometheus::{Histogram, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub query_total: IntCounterVec,
    pub active_sessions: IntGaugeVec,
    pub active_subscribes: IntGaugeVec,
    pub active_copy_tos: IntGaugeVec,
    pub queue_busy_seconds: HistogramVec,
    pub determine_timestamp: IntCounterVec,
    pub timestamp_difference_for_strict_serializable_ms: HistogramVec,
    pub commands: IntCounterVec,
    pub storage_usage_collection_time_seconds: HistogramVec,
    pub subscribe_outputs: IntCounterVec,
    pub canceled_peeks: IntCounterVec,
    pub linearize_message_seconds: HistogramVec,
    pub time_to_first_row_seconds: HistogramVec,
    pub statement_logging_records: IntCounterVec,
    pub statement_logging_unsampled_bytes: IntCounterVec,
    pub statement_logging_actual_bytes: IntCounterVec,
    pub message_batch: HistogramVec,
    pub message_handling: HistogramVec,
    pub optimization_notices: IntCounterVec,
    pub append_table_duration_seconds: HistogramVec,
    pub webhook_validation_reduce_failures: IntCounterVec,
    pub webhook_get_appender: IntCounter,
    pub check_scheduling_policies_seconds: HistogramVec,
    pub handle_scheduling_decisions_seconds: HistogramVec,
    pub row_set_finishing_seconds: HistogramVec,
    pub session_startup_table_writes_seconds: HistogramVec,
    pub parse_seconds: HistogramVec,
    pub pgwire_message_processing_seconds: HistogramVec,
    pub result_rows_first_to_last_byte_seconds: HistogramVec,
    pub pgwire_ensure_transaction_seconds: HistogramVec,
    pub catalog_snapshot_seconds: HistogramVec,
    pub pgwire_recv_scheduling_delay_ms: HistogramVec,
}

impl Metrics {
    pub(crate) fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            query_total: registry.register(metric!(
                name: "mz_query_total",
                help: "The total number of queries issued of the given type since process start.",
                var_labels: ["session_type", "statement_type"],
            )),
            active_sessions: registry.register(metric!(
                name: "mz_active_sessions",
                help: "The number of active coordinator sessions.",
                var_labels: ["session_type"],
            )),
            active_subscribes: registry.register(metric!(
                name: "mz_active_subscribes",
                help: "The number of active SUBSCRIBE queries.",
                var_labels: ["session_type"],
            )),
            active_copy_tos: registry.register(metric!(
                name: "mz_active_copy_tos",
                help: "The number of active COPY TO queries.",
                var_labels: ["session_type"],
            )),
            queue_busy_seconds: registry.register(metric!(
                name: "mz_coord_queue_busy_seconds",
                help: "The number of seconds the coord queue was processing before it was empty. This is a sampled metric and does not measure the full coord queue wait/idle times.",
                buckets: histogram_seconds_buckets(0.000_128, 32.0)
            )),
            determine_timestamp: registry.register(metric!(
                name: "mz_determine_timestamp",
                help: "The total number of calls to determine_timestamp.",
                var_labels:["respond_immediately", "isolation_level", "compute_instance", "determination_method"],
            )),
            timestamp_difference_for_strict_serializable_ms: registry.register(metric!(
                name: "mz_timestamp_difference_for_strict_serializable_ms",
                help: "Difference in timestamp in milliseconds for running in strict serializable vs serializable isolation level.",
                var_labels:["compute_instance", "determination_method"],
                buckets: histogram_milliseconds_buckets(1., 8000.),
            )),
            commands: registry.register(metric!(
                name: "mz_adapter_commands",
                help: "The total number of adapter commands issued of the given type since process start.",
                var_labels: ["command_type", "status", "application_name"],
            )),
            storage_usage_collection_time_seconds: registry.register(metric!(
                name: "mz_storage_usage_collection_time_seconds",
                help: "The number of seconds the coord spends collecting usage metrics from storage.",
                buckets: histogram_seconds_buckets(0.000_128, 8.0)
            )),
            subscribe_outputs: registry.register(metric!(
                name: "mz_subscribe_outputs",
                help: "The total number of different subscribe outputs used",
                var_labels: ["session_type", "subscribe_output"],
            )),
            canceled_peeks: registry.register(metric!(
                name: "mz_canceled_peeks_total",
                help: "The total number of canceled peeks since process start.",
            )),
            linearize_message_seconds: registry.register(metric!(
                name: "mz_linearize_message_seconds",
                help: "The number of seconds it takes to linearize strict serializable messages",
                var_labels: ["type", "immediately_handled"],
                buckets: histogram_seconds_buckets(0.000_128, 8.0),
            )),
            time_to_first_row_seconds: registry.register(metric! {
                name: "mz_time_to_first_row_seconds",
                help: "Latency of an execute for a successful query from pgwire's perspective",
                var_labels: ["instance_id", "isolation_level", "strategy"],
                buckets: histogram_seconds_buckets(0.000_128, 32.0)
            }),
            statement_logging_records: registry.register(metric! {
                name: "mz_statement_logging_record_count",
                help: "The total number of SQL statements tagged with whether or not they were recorded.",
                var_labels: ["sample"],
            }),
            statement_logging_unsampled_bytes: registry.register(metric!(
                name: "mz_statement_logging_unsampled_bytes",
                help: "The total amount of SQL text that would have been logged if statement logging were unsampled.",
            )),
            statement_logging_actual_bytes: registry.register(metric!(
                name: "mz_statement_logging_actual_bytes",
                help: "The total amount of SQL text that was logged by statement logging.",
            )),
            message_batch: registry.register(metric!(
                name: "mz_coordinator_message_batch_size",
                help: "Message batch size handled by the coordinator.",
                buckets: vec![0., 1., 2., 3., 4., 6., 8., 12., 16., 24., 32., 48., 64.],
            )),
            message_handling: registry.register(metric!(
                name: "mz_slow_message_handling",
                help: "Latency for ALL coordinator messages. 'slow' is in the name for legacy reasons, but is not accurate.",
                var_labels: ["message_kind"],
                buckets: histogram_seconds_buckets(0.000_128, 512.0),
            )),
            optimization_notices: registry.register(metric!(
                name: "mz_optimization_notices",
                help: "Number of optimization notices per notice type.",
                var_labels: ["notice_type"],
            )),
            append_table_duration_seconds: registry.register(metric!(
                name: "mz_append_table_duration_seconds",
                help: "Latency for appending to any (user or system) table.",
                buckets: histogram_seconds_buckets(0.128, 32.0),
            )),
            webhook_validation_reduce_failures: registry.register(metric!(
                name: "mz_webhook_validation_reduce_failures",
                help: "Count of how many times we've failed to reduce a webhook source's CHECK statement.",
                var_labels: ["reason"],
            )),
            webhook_get_appender: registry.register(metric!(
                name: "mz_webhook_get_appender_count",
                help: "Count of getting a webhook appender from the Coordinator.",
            )),
            check_scheduling_policies_seconds: registry.register(metric!(
                name: "mz_check_scheduling_policies_seconds",
                help: "The time each policy in `check_scheduling_policies` takes.",
                var_labels: ["policy", "thread"],
                buckets: histogram_seconds_buckets(0.000_128, 8.0),
            )),
            handle_scheduling_decisions_seconds: registry.register(metric!(
                name: "mz_handle_scheduling_decisions_seconds",
                help: "The time `handle_scheduling_decisions` takes.",
                var_labels: ["altered_a_cluster"],
                buckets: histogram_seconds_buckets(0.000_128, 8.0),
            )),
            row_set_finishing_seconds: registry.register(metric!(
                name: "mz_row_set_finishing_seconds",
                help: "The time it takes to run RowSetFinishing::finish.",
                buckets: histogram_seconds_buckets(0.000_128, 16.0),
            )),
            session_startup_table_writes_seconds: registry.register(metric!(
                name: "mz_session_startup_table_writes_seconds",
                help: "If we had to wait for builtin table writes before processing a query, how long did we wait for.",
                buckets: histogram_seconds_buckets(0.000_008, 4.0),
            )),
            parse_seconds: registry.register(metric!(
                name: "mz_parse_seconds",
                help: "The time it takes to parse a SQL statement. (Works for both Simple Queries and the Extended Query protocol.)",
                buckets: histogram_seconds_buckets(0.001, 8.0),
            )),
            pgwire_message_processing_seconds: registry.register(metric!(
                name: "mz_pgwire_message_processing_seconds",
                help: "The time it takes to process each of the pgwire message types, measured in the Adapter frontend",
                var_labels: ["message_type"],
                buckets: histogram_seconds_buckets(0.001, 512.0),
            )),
            result_rows_first_to_last_byte_seconds: registry.register(metric!(
                name: "mz_result_rows_first_to_last_byte_seconds",
                help: "The time from just before sending the first result row to sending a final response message after having successfully flushed the last result row to the connection. (This can span multiple FETCH statements.) (This is never observed for unbounded SUBSCRIBEs, i.e., which have no last result row.)",
                var_labels: ["statement_type"],
                buckets: histogram_seconds_buckets(0.001, 8192.0),
            )),
            pgwire_ensure_transaction_seconds: registry.register(metric!(
                name: "mz_pgwire_ensure_transaction_seconds",
                help: "The time it takes to run `ensure_transactions` when processing pgwire messages.",
                var_labels: ["message_type"],
                buckets: histogram_seconds_buckets(0.001, 512.0),
            )),
            catalog_snapshot_seconds: registry.register(metric!(
                name: "mz_catalog_snapshot_seconds",
                help: "The time it takes to run `catalog_snapshot` when fetching the catalog.",
                var_labels: ["context"],
                buckets: histogram_seconds_buckets(0.001, 512.0),
            )),
            pgwire_recv_scheduling_delay_ms: registry.register(metric!(
                name: "mz_pgwire_recv_scheduling_delay_ms",
                help: "The time between a pgwire connection's receiver task being woken up by incoming data and getting polled.",
                var_labels: ["message_type"],
                buckets: histogram_milliseconds_buckets(0.128, 512000.),
            ))
        }
    }

    pub(crate) fn row_set_finishing_seconds(&self) -> Histogram {
        self.row_set_finishing_seconds.with_label_values(&[])
    }

    pub(crate) fn session_metrics(&self) -> SessionMetrics {
        SessionMetrics {
            row_set_finishing_seconds: self.row_set_finishing_seconds(),
            session_startup_table_writes_seconds: self
                .session_startup_table_writes_seconds
                .with_label_values(&[]),
        }
    }
}

/// Metrics associated with a [`crate::session::Session`].
#[derive(Debug, Clone)]
pub struct SessionMetrics {
    row_set_finishing_seconds: Histogram,
    session_startup_table_writes_seconds: Histogram,
}

impl SessionMetrics {
    pub(crate) fn row_set_finishing_seconds(&self) -> &Histogram {
        &self.row_set_finishing_seconds
    }

    pub(crate) fn session_startup_table_writes_seconds(&self) -> &Histogram {
        &self.session_startup_table_writes_seconds
    }
}

pub(crate) fn session_type_label_value(user: &User) -> &'static str {
    match user.is_internal() {
        true => "system",
        false => "user",
    }
}

pub fn statement_type_label_value<T>(stmt: &Statement<T>) -> &'static str
where
    T: AstInfo,
{
    statement_kind_label_value(StatementKind::from(stmt))
}

pub(crate) fn subscribe_output_label_value<T>(output: &SubscribeOutput<T>) -> &'static str
where
    T: AstInfo,
{
    match output {
        SubscribeOutput::Diffs => "diffs",
        SubscribeOutput::WithinTimestampOrderBy { .. } => "within_timestamp_order_by",
        SubscribeOutput::EnvelopeUpsert { .. } => "envelope_upsert",
        SubscribeOutput::EnvelopeDebezium { .. } => "envelope_debezium",
    }
}
