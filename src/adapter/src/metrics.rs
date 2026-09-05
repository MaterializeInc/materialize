// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use mz_controller_types::ClusterId;
use mz_ore::metric;
use mz_ore::metrics::{
    MakeCollector, MakeCollectorOpts, MetricTag, MetricVisibility, MetricsRegistry, UIntGauge,
    remove_children_with_label,
};
use mz_ore::stats::{histogram_milliseconds_buckets, histogram_seconds_buckets};
use mz_sql::ast::{AstInfo, Statement, StatementKind, SubscribeOutput};
use mz_sql::session::hint::ApplicationNameHint;
use mz_sql::session::user::User;
use mz_sql::session::vars::IsolationLevel;
use mz_sql_parser::ast::statement_kind_label_value;
use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{Histogram, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec};

use crate::statement_logging::StatementExecutionStrategy;

pub(crate) const OCC_CALLER_SESSION: &str = "session";
pub(crate) const OCC_CALLER_BACKGROUND: &str = "background";

#[derive(Debug, Clone)]
pub struct Metrics {
    pub query_total: IntCounterVec,
    pub active_sessions: IntGaugeVec,
    pub active_subscribes: IntGaugeVec,
    pub active_internal_subscribes: IntGaugeVec,
    pub active_copy_tos: IntGaugeVec,
    pub queue_busy_seconds: Histogram,
    pub commands: IntCounterVec,
    pub storage_usage_collection_time_seconds: Histogram,
    pub arrangement_sizes_collection_time_seconds: Histogram,
    pub arrangement_sizes_rows_written: IntCounter,
    pub hydration_history_mutations: IntCounterVec,
    pub hydration_history_retention_batch_full: IntCounter,
    pub hydration_history_rows_affected: IntCounterVec,
    pub hydration_history_sweep_duration_seconds: Histogram,
    pub subscribe_outputs: IntCounterVec,
    pub canceled_peeks: IntCounter,
    pub linearize_message_seconds: HistogramVec,
    pub statement_logging_records: IntCounterVec,
    pub statement_logging_unsampled_bytes: IntCounter,
    pub statement_logging_actual_bytes: IntCounter,
    pub message_batch: Histogram,
    pub message_handling: HistogramVec,
    pub optimization_notices: IntCounterVec,
    pub append_table_duration_seconds: Histogram,
    pub webhook_validation_reduce_failures: IntCounterVec,
    pub webhook_get_appender: IntCounter,
    pub row_set_finishing_seconds: Histogram,
    pub session_startup_table_writes_seconds: Histogram,
    pub parse_seconds: Histogram,
    pub pgwire_message_processing_seconds: HistogramVec,
    pub result_rows_first_to_last_byte_seconds: HistogramVec,
    pub pgwire_ensure_transaction_seconds: HistogramVec,
    pub catalog_snapshot_seconds: HistogramVec,
    pub catalog_snapshot_cache: IntCounterVec,
    pub catalog_arc_strong_count: UIntGauge,
    pub catalog_arc_weak_count: UIntGauge,
    pub pgwire_recv_scheduling_delay_ms: HistogramVec,
    pub catalog_transact_seconds: HistogramVec,
    pub catalog_transact_phase_seconds: HistogramVec,
    pub apply_catalog_implications_seconds: Histogram,
    pub group_commit_catalog_upper_seconds: Histogram,
    pub occ_retry_count: HistogramVec,
    pub by_cluster: ClusterLabeledMetrics,
}

impl Metrics {
    pub(crate) fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            query_total: registry.register(metric!(
                name: "mz_query_total",
                help: "The total number of queries issued of the given type since process start.",
                var_labels: ["session_type", "statement_type"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Environment],
            )),
            active_sessions: registry.register(metric!(
                name: "mz_active_sessions",
                help: "The number of active coordinator sessions.",
                var_labels: ["session_type"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Environment],
            )),
            active_subscribes: registry.register(metric!(
                name: "mz_active_subscribes",
                help: "The number of active SUBSCRIBE queries.",
                var_labels: ["session_type"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Environment],
            )),
            active_internal_subscribes: registry.register(metric!(
                name: "mz_active_internal_subscribes",
                help: "The number of active internal subscribes used by read-then-write operations and background maintenance.",
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
            commands: registry.register(metric!(
                name: "mz_adapter_commands",
                help: "The total number of adapter commands issued of the given type since process start.",
                var_labels: ["command_type", "status", "application_name"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Environment],
            )),
            storage_usage_collection_time_seconds: registry.register(metric!(
                name: "mz_storage_usage_collection_time_seconds",
                help: "The number of seconds the coord spends collecting usage metrics from storage.",
                buckets: histogram_seconds_buckets(0.000_128, 8.0)
            )),
            arrangement_sizes_collection_time_seconds: registry.register(metric!(
                name: "mz_arrangement_sizes_collection_time_seconds",
                help: "Seconds to read mz_object_arrangement_sizes and prepare history records for one snapshot.",
                buckets: histogram_seconds_buckets(0.000_128, 8.0)
            )),
            arrangement_sizes_rows_written: registry.register(metric!(
                name: "mz_arrangement_sizes_rows_written_total",
                help: "Total rows appended to mz_object_arrangement_size_history since process start.",
            )),
            hydration_history_mutations: registry.register(metric!(
                name: "mz_hydration_history_mutations_total",
                help: "Total hydration-history collection and retention mutations since process start.",
                var_labels: ["operation", "outcome"],
            )),
            hydration_history_retention_batch_full: registry.register(metric!(
                name: "mz_hydration_history_retention_batch_full_total",
                help: "Total hydration-history retention batches that were full. Repeated increments mean retention may not be keeping up with its schedule.",
            )),
            hydration_history_rows_affected: registry.register(metric!(
                name: "mz_hydration_history_rows_affected_total",
                help: "Total rows changed by hydration-history maintenance since process start.",
                var_labels: ["action"],
            )),
            hydration_history_sweep_duration_seconds: registry.register(metric!(
                name: "mz_hydration_history_sweep_duration_seconds",
                help: "Wall time of a complete hydration-history collection and retention sweep.",
                buckets: histogram_seconds_buckets(0.128, 1024.0),
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
                help: "The time it takes to fetch a catalog snapshot from the Coordinator. \
                       Only observed on session snapshot cache misses.",
                var_labels: ["context"],
                buckets: histogram_seconds_buckets(0.001, 512.0),
            )),
            catalog_snapshot_cache: registry.register(metric!(
                name: "mz_catalog_snapshot_cache",
                help: "Hits and misses of the session-side catalog snapshot cache. A miss \
                       costs a Coordinator round-trip.",
                var_labels: ["context", "result"],
            )),
            catalog_arc_strong_count: registry.register(metric!(
                name: "mz_catalog_arc_strong_count",
                help: "The number of strong references to the current catalog snapshot: roughly, \
                       in-flight users plus a small constant baseline.",
            )),
            catalog_arc_weak_count: registry.register(metric!(
                name: "mz_catalog_arc_weak_count",
                help: "The number of weak references to the current catalog snapshot: sessions \
                       whose snapshot cache points at the current catalog version (older \
                       versions are not counted). Drops on catalog changes and recovers as \
                       session caches repopulate.",
            )),
            pgwire_recv_scheduling_delay_ms: registry.register(metric!(
                name: "mz_pgwire_recv_scheduling_delay_ms",
                help: "The time between a pgwire connection's receiver task being woken up by incoming data and getting polled.",
                var_labels: ["message_type"],
                buckets: histogram_milliseconds_buckets(0.128, 512000.),
            )),
            catalog_transact_seconds: registry.register(metric!(
                name: "mz_catalog_transact_seconds",
                help: "The time it takes to run various catalog transact methods.",
                var_labels: ["method"],
                buckets: histogram_seconds_buckets(0.001, 32.0),
            )),
            catalog_transact_phase_seconds: registry.register(metric!(
                name: "mz_catalog_transact_phase_seconds",
                help: "Wall time of the individual phases of a coordinator catalog transaction, to attribute where transact time is spent. Phases overlap and do not sum to mz_catalog_transact_seconds. The transact phase includes the durable catalog sync and commit.",
                var_labels: ["phase"],
                buckets: histogram_seconds_buckets(0.000_128, 32.0),
            )),
            apply_catalog_implications_seconds: registry.register(metric!(
                name: "mz_apply_catalog_implications_seconds",
                help: "The time it takes to apply catalog implications.",
                buckets: histogram_seconds_buckets(0.001, 32.0),
            )),
            group_commit_catalog_upper_seconds: registry.register(metric!(
                name: "mz_group_commit_catalog_upper_seconds",
                help: "The time it takes to advance the catalog shard upper for a txns-shard write (group commits and table register/forget).",
                buckets: histogram_seconds_buckets(0.001, 32.0),
            )),
            occ_retry_count: registry.register(metric!(
                name: "mz_occ_read_then_write_retry_count",
                help: "Number of OCC retries per read-then-write operation.",
                var_labels: ["caller"],
                buckets: vec![
                    0., 1., 2., 3., 5., 10., 25., 50., 100., 200., 300., 500., 750., 1000.,
                ],
            )),
            by_cluster: ClusterLabeledMetrics::register_into(registry),
        }
    }

    pub(crate) fn row_set_finishing_seconds(&self) -> Histogram {
        self.row_set_finishing_seconds.clone()
    }

    pub(crate) fn session_metrics(&self) -> SessionMetrics {
        SessionMetrics {
            row_set_finishing_seconds: self.row_set_finishing_seconds(),
            session_startup_table_writes_seconds: self.session_startup_table_writes_seconds.clone(),
            query_total: self.query_total.clone(),
            subscribe_outputs: self.subscribe_outputs.clone(),
            by_cluster: self.by_cluster.clone(),
            optimization_notices: self.optimization_notices.clone(),
            statement_logging_records: self.statement_logging_records.clone(),
            statement_logging_unsampled_bytes: self.statement_logging_unsampled_bytes.clone(),
            statement_logging_actual_bytes: self.statement_logging_actual_bytes.clone(),
        }
    }
}

/// Metrics to be accessed from a [`crate::session::Session`].
#[derive(Debug, Clone)]
pub struct SessionMetrics {
    row_set_finishing_seconds: Histogram,
    session_startup_table_writes_seconds: Histogram,
    query_total: IntCounterVec,
    subscribe_outputs: IntCounterVec,
    by_cluster: ClusterLabeledMetrics,
    optimization_notices: IntCounterVec,
    statement_logging_records: IntCounterVec,
    statement_logging_unsampled_bytes: IntCounter,
    statement_logging_actual_bytes: IntCounter,
}

impl SessionMetrics {
    pub(crate) fn row_set_finishing_seconds(&self) -> &Histogram {
        &self.row_set_finishing_seconds
    }

    pub(crate) fn session_startup_table_writes_seconds(&self) -> &Histogram {
        &self.session_startup_table_writes_seconds
    }

    pub(crate) fn query_total(&self, label_values: &[&str]) -> GenericCounter<AtomicU64> {
        self.query_total.with_label_values(label_values)
    }

    pub(crate) fn subscribe_outputs(&self, label_values: &[&str]) -> GenericCounter<AtomicU64> {
        self.subscribe_outputs.with_label_values(label_values)
    }

    pub(crate) fn by_cluster(&self) -> &ClusterLabeledMetrics {
        &self.by_cluster
    }

    pub(crate) fn optimization_notices(&self, label_values: &[&str]) -> GenericCounter<AtomicU64> {
        self.optimization_notices.with_label_values(label_values)
    }

    pub(crate) fn statement_logging_records(
        &self,
        label_values: &[&str],
    ) -> GenericCounter<AtomicU64> {
        self.statement_logging_records
            .with_label_values(label_values)
    }

    pub(crate) fn statement_logging_unsampled_bytes(&self) -> &IntCounter {
        &self.statement_logging_unsampled_bytes
    }

    pub(crate) fn statement_logging_actual_bytes(&self) -> &IntCounter {
        &self.statement_logging_actual_bytes
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

/// Adapter metrics whose series carry a cluster id label.
///
/// Series are created on first observation, as for any labeled metric, and
/// `remove_cluster` deletes a dropped cluster's series from every vec that
/// `register` put here.
#[derive(Debug, Clone)]
pub struct ClusterLabeledMetrics {
    time_to_first_row_seconds: HistogramVec,
    determine_timestamp: IntCounterVec,
    timestamp_difference_for_bounded_staleness_ms: HistogramVec,
    /// Every vec above with the label that carries its cluster id. `register`
    /// is the only way in, so a vec registered any other way is not swept.
    delete_on_cluster_drop: Vec<ClusterLabeledVec>,
}

/// A metric vec with the name of the label that carries the cluster id.
#[derive(Debug, Clone)]
enum ClusterLabeledVec {
    Histogram(HistogramVec, &'static str),
    Counter(IntCounterVec, &'static str),
}

impl ClusterLabeledVec {
    fn delete_cluster(&self, cluster: &str) {
        match self {
            Self::Histogram(vec, label) => remove_children_with_label(vec, label, cluster),
            Self::Counter(vec, label) => remove_children_with_label(vec, label, cluster),
        }
    }
}

impl ClusterLabeledMetrics {
    fn register_into(registry: &MetricsRegistry) -> Self {
        let mut vecs = Vec::new();
        Self {
            time_to_first_row_seconds: Self::register(
                registry,
                &mut vecs,
                ClusterLabeledVec::Histogram,
                "instance_id",
                metric! {
                    name: "mz_time_to_first_row_seconds",
                    help: "Latency of an execute for a successful query from pgwire's perspective",
                    var_labels: ["instance_id", "isolation_level", "strategy", "application_name"],
                    // NOTE: Measurements below 512 microseconds are negligible, so omit those buckets.
                    buckets: histogram_seconds_buckets(0.000_512, 32.0)
                },
            ),
            determine_timestamp: Self::register(
                registry,
                &mut vecs,
                ClusterLabeledVec::Counter,
                "compute_instance",
                metric!(
                    name: "mz_determine_timestamp",
                    help: "The total number of calls to determine_timestamp.",
                    var_labels: ["respond_immediately", "isolation_level", "compute_instance"],
                ),
            ),
            timestamp_difference_for_bounded_staleness_ms: Self::register(
                registry,
                &mut vecs,
                ClusterLabeledVec::Histogram,
                "compute_instance",
                metric!(
                    name: "mz_timestamp_difference_for_bounded_staleness_ms",
                    help: "How much older bounded-staleness timestamps are compared to serializable, in milliseconds. Measures the actual staleness incurred.",
                    var_labels: ["compute_instance"],
                    buckets: histogram_milliseconds_buckets(1., 8000.),
                ),
            ),
            delete_on_cluster_drop: vecs,
        }
    }

    /// Registers `opts` and records it, wrapped by `wrap`, with `cluster_label`
    /// as the label carrying its cluster id, so `remove_cluster` covers it.
    ///
    /// Panics if `cluster_label` is not a variable label of `opts`.
    fn register<M: MakeCollector>(
        registry: &MetricsRegistry,
        vecs: &mut Vec<ClusterLabeledVec>,
        wrap: fn(M, &'static str) -> ClusterLabeledVec,
        cluster_label: &'static str,
        opts: MakeCollectorOpts,
    ) -> M {
        assert!(
            opts.opts
                .variable_labels
                .iter()
                .any(|label| label == cluster_label),
            "{cluster_label} is not a label of {}",
            opts.opts.name
        );
        let vec: M = registry.register(opts);
        // A metric vec is a handle onto shared state, so the clone sees the
        // same children as the field.
        vecs.push(wrap(vec.clone(), cluster_label));
        vec
    }

    /// Deletes the series of `cluster_id`.
    pub(crate) fn remove_cluster(&self, cluster_id: ClusterId) {
        let cluster = cluster_id.to_string();
        for vec in &self.delete_on_cluster_drop {
            vec.delete_cluster(&cluster);
        }
    }

    /// Statements without a cluster or strategy record under the "none" label value.
    pub(crate) fn time_to_first_row_seconds(
        &self,
        cluster_id: Option<ClusterId>,
        isolation_level: IsolationLevel,
        strategy: Option<StatementExecutionStrategy>,
        application_name: ApplicationNameHint,
    ) -> Histogram {
        let instance = match cluster_id {
            Some(id) => Cow::Owned(id.to_string()),
            None => Cow::Borrowed("none"),
        };
        self.time_to_first_row_seconds.with_label_values(&[
            instance.as_ref(),
            isolation_level.as_variant_str(),
            strategy.map_or("none", |strategy| strategy.name()),
            application_name.as_str(),
        ])
    }

    pub(crate) fn determine_timestamp(
        &self,
        cluster_id: ClusterId,
        respond_immediately: bool,
        isolation_level: IsolationLevel,
    ) -> GenericCounter<AtomicU64> {
        self.determine_timestamp.with_label_values(&[
            if respond_immediately { "true" } else { "false" },
            isolation_level.as_variant_str(),
            &cluster_id.to_string(),
        ])
    }

    pub(crate) fn timestamp_difference_for_bounded_staleness_ms(
        &self,
        cluster_id: ClusterId,
    ) -> Histogram {
        self.timestamp_difference_for_bounded_staleness_ms
            .with_label_values(&[&cluster_id.to_string()])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const U1: ClusterId = ClusterId::User(1);
    const U2: ClusterId = ClusterId::User(2);

    fn families(registry: &MetricsRegistry) -> BTreeSet<String> {
        registry
            .gather()
            .into_iter()
            .map(|family| family.name().to_string())
            .collect()
    }

    /// Names of the families with a series carrying `value` under any label.
    fn families_with_label_value(registry: &MetricsRegistry, value: &str) -> BTreeSet<String> {
        registry
            .gather()
            .into_iter()
            .filter(|family| {
                family
                    .get_metric()
                    .iter()
                    .flat_map(|metric| metric.get_label())
                    .any(|label| label.value() == value)
            })
            .map(|family| family.name().to_string())
            .collect()
    }

    fn observe_first_row(
        metrics: &ClusterLabeledMetrics,
        cluster_id: Option<ClusterId>,
        strategy: Option<StatementExecutionStrategy>,
    ) {
        metrics
            .time_to_first_row_seconds(
                cluster_id,
                IsolationLevel::StrictSerializable,
                strategy,
                ApplicationNameHint::from_str("psql"),
            )
            .observe(0.1);
    }

    #[mz_ore::test]
    fn dropping_a_cluster_removes_its_series() {
        let registry = MetricsRegistry::new();
        let metrics = ClusterLabeledMetrics::register_into(&registry);

        observe_first_row(&metrics, Some(U1), None);
        observe_first_row(
            &metrics,
            Some(U1),
            Some(StatementExecutionStrategy::FastPath),
        );
        observe_first_row(&metrics, Some(U2), None);
        metrics
            .determine_timestamp(U1, true, IsolationLevel::Serializable)
            .inc();
        metrics
            .timestamp_difference_for_bounded_staleness_ms(U1)
            .observe(5.0);
        assert_eq!(
            families_with_label_value(&registry, "u1"),
            families(&registry),
            "every registered family needs a u1 series for the drop to be exercised"
        );

        metrics.remove_cluster(U1);

        assert_eq!(
            families_with_label_value(&registry, "u1"),
            BTreeSet::new(),
            "u1 series survived the drop"
        );
        assert_eq!(
            families_with_label_value(&registry, "u2"),
            BTreeSet::from(["mz_time_to_first_row_seconds".to_string()]),
            "the other cluster's series must be untouched"
        );
    }

    #[mz_ore::test]
    fn statements_without_a_cluster_are_unaffected_by_drops() {
        let registry = MetricsRegistry::new();
        let metrics = ClusterLabeledMetrics::register_into(&registry);
        observe_first_row(&metrics, None, Some(StatementExecutionStrategy::Constant));
        observe_first_row(&metrics, Some(U1), None);
        metrics.remove_cluster(U1);
        assert_eq!(families_with_label_value(&registry, "u1"), BTreeSet::new());
        assert_eq!(
            families_with_label_value(&registry, "none"),
            BTreeSet::from(["mz_time_to_first_row_seconds".to_string()])
        );
    }

    #[mz_ore::test]
    #[should_panic(expected = "is not a label of mz_test")]
    fn registering_under_a_label_the_metric_lacks_panics() {
        let registry = MetricsRegistry::new();
        let _: IntCounterVec = ClusterLabeledMetrics::register(
            &registry,
            &mut Vec::new(),
            ClusterLabeledVec::Counter,
            "cluster_id",
            metric!(
                name: "mz_test",
                help: "test",
                var_labels: ["compute_instance"],
            ),
        );
    }
}
