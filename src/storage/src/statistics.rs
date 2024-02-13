// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for managing storage statistics.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

use mz_ore::metric;
use mz_ore::metrics::{
    CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt, IntCounterVec, IntGaugeVec,
    MetricsRegistry, UIntGaugeVec,
};
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::client::{SinkStatisticsUpdate, SourceStatisticsUpdate};
use mz_storage_types::sources::SourceEnvelope;
use prometheus::core::{AtomicI64, AtomicU64};
use timely::progress::frontier::Antichain;
use timely::PartialOrder;

// Note(guswynn): ordinarily these metric structs would be in the `metrics` modules, but we
// put them here so they can be near the user-facing definitions as well.

#[derive(Clone, Debug)]
pub(crate) struct SourceStatisticsMetricDefs {
    // Counters
    pub(crate) messages_received: IntCounterVec,
    pub(crate) updates_staged: IntCounterVec,
    pub(crate) updates_committed: IntCounterVec,
    pub(crate) bytes_received: IntCounterVec,

    // Gauges
    pub(crate) snapshot_committed: UIntGaugeVec,
    pub(crate) envelope_state_bytes: UIntGaugeVec,
    pub(crate) envelope_state_records: UIntGaugeVec,
    pub(crate) rehydration_latency_ms: IntGaugeVec,

    // statistics that are not yet exposed to users.
    pub(crate) upstream_values: UIntGaugeVec,
    pub(crate) committed_values: UIntGaugeVec,
}

impl SourceStatisticsMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            snapshot_committed: registry.register(metric!(
                name: "mz_source_snapshot_committed",
                help: "Whether or not the worker has committed the initial snapshot for a source.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            messages_received: registry.register(metric!(
                name: "mz_source_messages_received",
                help: "The number of raw messages the worker has received from upstream.",
                var_labels: ["source_id", "worker_id", "parent_source_id"],
            )),
            updates_staged: registry.register(metric!(
                name: "mz_source_updates_staged",
                help: "The number of updates (inserts + deletes) the worker has written but not yet committed to the storage layer.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            updates_committed: registry.register(metric!(
                name: "mz_source_updates_committed",
                help: "The number of updates (inserts + deletes) the worker has committed into the storage layer.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            bytes_received: registry.register(metric!(
                name: "mz_source_bytes_received",
                help: "The number of bytes worth of messages the worker has received from upstream. The way the bytes are counted is source-specific.",
                var_labels: ["source_id", "worker_id", "parent_source_id"],
            )),
            envelope_state_bytes: registry.register(metric!(
                name: "mz_source_envelope_state_bytes",
                help: "The number of bytes of the source envelope state kept. This will be specific to the envelope in use.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            envelope_state_records: registry.register(metric!(
                name: "mz_source_envelope_state_records",
                help: "The number of records in the source envelope state. This will be specific to the envelope in use",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            rehydration_latency_ms: registry.register(metric!(
                name: "mz_source_rehydration_latency_ms",
                help: "The amount of time in milliseconds it took for the worker to rehydrate the source envelope state. This will be specific to the envelope in use.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id", "envelope"],
            )),
            upstream_values: registry.register(metric!(
                name: "mz_source_upstream_values",
                help: "The total number of _values_ (source-defined unit) present in upstream.",
                var_labels: ["source_id", "worker_id", "shard_id"],
            )),
            committed_values: registry.register(metric!(
                name: "mz_source_committed_values",
                help: "The total number of _values_ (source-defined unit) we have fully processed, and storage and committed.",
                var_labels: ["source_id", "worker_id", "shard_id"],
            )),
        }
    }
}

/// Prometheus metrics for user-facing source metrics.
#[derive(Debug)]
pub struct SourceStatisticsMetrics {
    // Counters
    pub(crate) messages_received: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) updates_staged: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) updates_committed: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) bytes_received: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,

    // Gauges
    pub(crate) snapshot_committed: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub(crate) envelope_state_bytes: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub(crate) envelope_state_records: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub(crate) rehydration_latency_ms: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,

    // statistics that are not yet exposed to users.
    pub(crate) upstream_values: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub(crate) committed_values: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl SourceStatisticsMetrics {
    pub(crate) fn new(
        defs: &SourceStatisticsMetricDefs,
        id: GlobalId,
        worker_id: usize,
        parent_source_id: GlobalId,
        shard_id: &mz_persist_client::ShardId,
        envelope: SourceEnvelope,
    ) -> SourceStatisticsMetrics {
        let shard = shard_id.to_string();
        let envelope = match envelope {
            SourceEnvelope::None(_) => "none",
            SourceEnvelope::Upsert(_) => "upsert",
            SourceEnvelope::CdcV2 => "cdcv2",
        };

        SourceStatisticsMetrics {
            snapshot_committed: defs.snapshot_committed.get_delete_on_drop_gauge(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            messages_received: defs.messages_received.get_delete_on_drop_counter(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
            ]),
            updates_staged: defs.updates_staged.get_delete_on_drop_counter(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            updates_committed: defs.updates_committed.get_delete_on_drop_counter(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            bytes_received: defs.bytes_received.get_delete_on_drop_counter(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
            ]),
            envelope_state_bytes: defs.envelope_state_bytes.get_delete_on_drop_gauge(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            envelope_state_records: defs.envelope_state_records.get_delete_on_drop_gauge(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            rehydration_latency_ms: defs.rehydration_latency_ms.get_delete_on_drop_gauge(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
                envelope.to_string(),
            ]),
            upstream_values: defs.upstream_values.get_delete_on_drop_gauge(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
            ]),
            committed_values: defs.committed_values.get_delete_on_drop_gauge(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
            ]),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SinkStatisticsMetricDefs {
    // Counters
    pub(crate) messages_staged: IntCounterVec,
    pub(crate) messages_committed: IntCounterVec,
    pub(crate) bytes_staged: IntCounterVec,
    pub(crate) bytes_committed: IntCounterVec,
}

impl SinkStatisticsMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            messages_staged: registry.register(metric!(
                name: "mz_sink_messages_staged",
                help: "The number of messages staged but possibly not committed to the sink.",
                var_labels: ["sink_id", "worker_id"],
            )),
            messages_committed: registry.register(metric!(
                name: "mz_sink_messages_committed",
                help: "The number of messages committed to the sink.",
                var_labels: ["sink_id", "worker_id"],
            )),
            bytes_staged: registry.register(metric!(
                name: "mz_sink_bytes_staged",
                help: "The number of bytes staged but possibly not committed to the sink.",
                var_labels: ["sink_id", "worker_id"],
            )),
            bytes_committed: registry.register(metric!(
                name: "mz_sink_bytes_committed",
                help: "The number of bytes committed to the sink.",
                var_labels: ["sink_id", "worker_id"],
            )),
        }
    }
}

/// Prometheus metrics for user-facing sink metrics.
#[derive(Debug)]
pub struct SinkStatisticsMetrics {
    // Counters
    pub(crate) messages_staged: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) messages_committed: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) bytes_staged: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) bytes_committed: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl SinkStatisticsMetrics {
    pub(crate) fn new(
        defs: &SinkStatisticsMetricDefs,
        id: GlobalId,
        worker_id: usize,
    ) -> SinkStatisticsMetrics {
        SinkStatisticsMetrics {
            messages_staged: defs
                .messages_staged
                .get_delete_on_drop_counter(vec![id.to_string(), worker_id.to_string()]),
            messages_committed: defs
                .messages_committed
                .get_delete_on_drop_counter(vec![id.to_string(), worker_id.to_string()]),
            bytes_staged: defs
                .bytes_staged
                .get_delete_on_drop_counter(vec![id.to_string(), worker_id.to_string()]),
            bytes_committed: defs
                .bytes_committed
                .get_delete_on_drop_counter(vec![id.to_string(), worker_id.to_string()]),
        }
    }
}

/// Meta data needed to maintain source statistics.
#[derive(Debug, Clone)]
pub struct SourceStatisticsMetadata {
    /// The resumption upper of the source.
    resume_upper: Antichain<Timestamp>,
    /// Time at which the source (more precisely: this metadata object) was created.
    created_at: Instant,
}

impl SourceStatisticsMetadata {
    /// Create a new `SourceStatisticsMetadata` object.
    pub fn new(resume_upper: Antichain<Timestamp>) -> Self {
        Self {
            resume_upper,
            created_at: Instant::now(),
        }
    }
}

#[derive(Debug)]
struct StatsInner<Stats, Metrics> {
    stats: Stats,
    prom: Metrics,
}

/// A helper struct designed to make it easy for operators to update user-facing metrics.
/// This struct also ensures that each stack is also incremented in prometheus.
///
/// Caveats:
/// - There is one Prometheus timeseries-per-worker, and we label it with the source id and the
/// source id of the parent source (if there is no parent, these labels have the same value).
///     - Some metrics also have the shard id we are writing metrics for.
/// - The prometheus metrics do not have the same timestamps as the ones exposed in sql, because
/// they are written at different times.
///     - This may be fixed in the future when we write the metrics from storaged directly.
///     - The value also eventually converge to the same value.
#[derive(Debug)]
pub struct StorageStatistics<Stats, Metrics, Meta> {
    // Note also that the `DeleteOnDropCounter`'s in the `SourceStatisticsMetrics`
    // already are in an `Arc`, so this is a bit of extra wrapping, but the cost
    // shouldn't cost too much.
    stats: Rc<RefCell<StatsInner<Stats, Metrics>>>,
    /// Meta data needed to maintain statistics.
    meta: Meta,
}

impl<Stats, Metrics, Meta: Clone> Clone for StorageStatistics<Stats, Metrics, Meta> {
    fn clone(&self) -> Self {
        Self {
            stats: Rc::clone(&self.stats),
            meta: self.meta.clone(),
        }
    }
}

/// Statistics maintained for sources. Gauges can be uninitialized.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceStatisticsRecord {
    id: GlobalId,
    worker_id: usize,
    // Counters
    messages_received: u64,
    bytes_received: u64,
    updates_staged: u64,
    updates_committed: u64,
    // Gauges
    snapshot_committed: Option<bool>,
    envelope_state_bytes: Option<u64>,
    envelope_state_records: Option<u64>,
    rehydration_latency_ms: Option<Option<i64>>,
}

impl SourceStatisticsRecord {
    fn clear(&mut self) {
        self.messages_received = 0;
        self.bytes_received = 0;
        self.updates_staged = 0;
        self.updates_committed = 0;
        self.snapshot_committed = None;
        // These 2 are gauges but we WANT them to be reset, so we
        // consider them instantly initialized.
        self.envelope_state_bytes = Some(0);
        self.envelope_state_records = Some(0);
        self.rehydration_latency_ms = None;
    }
}

/// Statistics maintained for sinks. Gauges can be uninitialized.
#[derive(Clone, Debug, PartialEq)]
pub struct SinkStatisticsRecord {
    id: GlobalId,
    worker_id: usize,
    // Counters
    messages_staged: u64,
    messages_committed: u64,
    bytes_staged: u64,
    bytes_committed: u64,
}

impl SinkStatisticsRecord {
    fn clear(&mut self) {
        self.messages_staged = 0;
        self.messages_committed = 0;
        self.bytes_staged = 0;
        self.bytes_committed = 0;
    }
}

/// Statistics maintained for sources.
pub type SourceStatistics =
    StorageStatistics<SourceStatisticsRecord, SourceStatisticsMetrics, SourceStatisticsMetadata>;

/// Statistics maintained for sinks.
pub type SinkStatistics = StorageStatistics<SinkStatisticsRecord, SinkStatisticsMetrics, ()>;

impl SourceStatistics {
    pub(crate) fn new(
        id: GlobalId,
        worker_id: usize,
        metrics: &SourceStatisticsMetricDefs,
        parent_source_id: GlobalId,
        shard_id: &mz_persist_client::ShardId,
        envelope: SourceEnvelope,
        resume_upper: Antichain<Timestamp>,
    ) -> Self {
        Self {
            stats: Rc::new(RefCell::new(StatsInner {
                stats: SourceStatisticsRecord {
                    id,
                    worker_id,
                    messages_received: 0,
                    updates_staged: 0,
                    updates_committed: 0,
                    bytes_received: 0,
                    snapshot_committed: None,
                    // These 2 are gauges but we WANT them to be reset, so we
                    // consider them instantly initialized.
                    envelope_state_bytes: Some(0),
                    envelope_state_records: Some(0),
                    rehydration_latency_ms: None,
                },
                prom: SourceStatisticsMetrics::new(
                    metrics,
                    id,
                    worker_id,
                    parent_source_id,
                    shard_id,
                    envelope,
                ),
            })),
            meta: SourceStatisticsMetadata::new(resume_upper),
        }
    }

    /// Clear the data, resetting gauges to uninitialized.
    /// This does not reset prometheus gauges, which will be overwritten with new values.
    pub fn clear(&self) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.clear();
    }

    /// Get a snapshot of the data, returning `None` if all gauges are not initialized.
    pub fn snapshot(&self) -> Option<SourceStatisticsUpdate> {
        let cur = self.stats.borrow_mut();

        Some(SourceStatisticsUpdate {
            id: cur.stats.id,
            worker_id: cur.stats.worker_id,
            messages_received: cur.stats.messages_received,
            bytes_received: cur.stats.bytes_received,
            updates_staged: cur.stats.updates_staged,
            updates_committed: cur.stats.updates_committed,
            snapshot_committed: cur.stats.snapshot_committed?,
            envelope_state_bytes: cur.stats.envelope_state_bytes?,
            envelope_state_records: cur.stats.envelope_state_records?,
            rehydration_latency_ms: cur.stats.rehydration_latency_ms?,
        })
    }

    /// Set the `snapshot_committed` stat based on the reported upper, and
    /// mark the stats as initialized.
    ///
    /// - In sql, we ensure that we _never_ reset `snapshot_committed` to `false`, but gauges and
    /// counters are ordinarily reset to 0 in Prometheus, so on restarts this value may be inconsistent.
    // TODO(guswynn): Actually test that this initialization logic works.
    pub fn initialize_snapshot_committed(&self, upper: &Antichain<Timestamp>) {
        self.update_snapshot_committed(upper);
    }

    /// Set the `snapshot_committed` stat based on the reported upper.
    pub fn update_snapshot_committed(&self, upper: &Antichain<Timestamp>) {
        let value = *upper != Antichain::from_elem(Timestamp::MIN);
        let mut cur = self.stats.borrow_mut();
        cur.stats.snapshot_committed = Some(value);
        cur.prom.snapshot_committed.set(if value { 1 } else { 0 });
    }

    /// Increment the `messages_received` stat.
    pub fn inc_messages_received_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.messages_received = cur.stats.messages_received + value;
        cur.prom.messages_received.inc_by(value);
    }

    /// Increment the `updates` stat.
    pub fn inc_updates_staged_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.updates_staged = cur.stats.updates_staged + value;
        cur.prom.updates_staged.inc_by(value);
    }

    /// Increment the `messages_committed` stat.
    pub fn inc_updates_committed_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.updates_committed = cur.stats.updates_committed + value;
        cur.prom.updates_committed.inc_by(value);
    }

    /// Increment the `bytes_received` stat.
    pub fn inc_bytes_received_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.bytes_received = cur.stats.bytes_received + value;
        cur.prom.bytes_received.inc_by(value);
    }

    /// Update the `envelope_state_bytes` stat.
    /// A positive value will add and a negative value will subtract.
    pub fn update_envelope_state_bytes_by(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        if let Some(updated) = cur
            .stats
            .envelope_state_bytes
            .unwrap_or(0)
            .checked_add_signed(value)
        {
            cur.stats.envelope_state_bytes = Some(updated);
            cur.prom.envelope_state_bytes.set(updated);
        } else {
            let envelope_state_bytes = cur.stats.envelope_state_bytes.unwrap_or(0);
            tracing::warn!(
                "Unexpected u64 overflow while updating envelope_state_bytes value {} with {}",
                envelope_state_bytes,
                value
            );
            cur.stats.envelope_state_bytes = Some(0);
            cur.prom.envelope_state_bytes.set(0);
        }
    }

    /// Set the `envelope_state_bytes` to the given value
    pub fn set_envelope_state_bytes(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        let value = if value < 0 {
            tracing::warn!(
                "Unexpected negative value for envelope_state_bytes {}",
                value
            );
            0
        } else {
            value.unsigned_abs()
        };
        cur.stats.envelope_state_bytes = Some(value);
        cur.prom.envelope_state_bytes.set(value);
    }

    /// Update the `envelope_state_records` stat.
    /// A positive value will add and a negative value will subtract.
    pub fn update_envelope_state_records_by(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        if let Some(updated) = cur
            .stats
            .envelope_state_records
            .unwrap_or(0)
            .checked_add_signed(value)
        {
            cur.stats.envelope_state_records = Some(updated);
            cur.prom.envelope_state_records.set(updated);
        } else {
            let envelope_state_records = cur.stats.envelope_state_records.unwrap_or(0);
            tracing::warn!(
                "Unexpected u64 overflow while updating envelope_state_records value {} with {}",
                envelope_state_records,
                value
            );
            cur.stats.envelope_state_records = Some(0);
            cur.prom.envelope_state_records.set(0);
        }
    }

    /// Set the `envelope_state_records` to the given value
    pub fn set_envelope_state_records(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        let value = if value < 0 {
            tracing::warn!(
                "Unexpected negative value for envelope_state_records {}",
                value
            );
            0
        } else {
            value.unsigned_abs()
        };
        cur.stats.envelope_state_records = Some(value);
        cur.prom.envelope_state_records.set(value);
    }

    /// Initialize the `rehydration_latency_ms` stat as `NULL`.
    pub fn initialize_rehydration_latency_ms(&self) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.rehydration_latency_ms = Some(None);
    }

    /// Set the `rehydration_latency_ms` stat based on the reported upper.
    pub fn update_rehydration_latency_ms(&self, upper: &Antichain<Timestamp>) {
        let mut cur = self.stats.borrow_mut();

        if matches!(cur.stats.rehydration_latency_ms, Some(Some(_))) {
            return; // source was already hydrated before
        }
        if !PartialOrder::less_than(&self.meta.resume_upper, upper) {
            return; // source is not yet hydrated
        }

        let elapsed = self.meta.created_at.elapsed();
        let value = elapsed
            .as_millis()
            .try_into()
            .expect("Rehydration took more than ~584 million years!");
        cur.stats.rehydration_latency_ms = Some(Some(value));
        cur.prom.rehydration_latency_ms.set(value);
    }

    /// Set the `upstream_values` stat to the given value.
    pub fn set_upstream_values(&self, value: u64) {
        let cur = self.stats.borrow_mut();
        // Not yet exposed to users.
        // cur.prom.upstream_values = value;
        cur.prom.upstream_values.set(value);
    }

    /// Set the `committed_values` stat to the given value.
    pub fn set_committed_values(&self, value: u64) {
        let cur = self.stats.borrow_mut();
        // Not yet exposed to users.
        // cur.prom.committed_values = value;
        cur.prom.committed_values.set(value);
    }
}

impl SinkStatistics {
    pub(crate) fn new(id: GlobalId, worker_id: usize, metrics: &SinkStatisticsMetricDefs) -> Self {
        Self {
            stats: Rc::new(RefCell::new(StatsInner {
                stats: SinkStatisticsRecord {
                    id,
                    worker_id,
                    messages_staged: 0,
                    messages_committed: 0,
                    bytes_staged: 0,
                    bytes_committed: 0,
                },
                prom: SinkStatisticsMetrics::new(metrics, id, worker_id),
            })),
            meta: (),
        }
    }

    /// Clear the data, resetting gauges to uninitialized.
    /// This does not reset prometheus gauges, which will be overwritten with new values.
    pub fn clear(&self) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.clear()
    }

    /// Get a snapshot of the data, returning `None` if all gauges are not initialized.
    pub fn snapshot(&self) -> Option<SinkStatisticsUpdate> {
        let cur = self.stats.borrow_mut();

        Some(SinkStatisticsUpdate {
            id: cur.stats.id,
            worker_id: cur.stats.worker_id,
            messages_staged: cur.stats.messages_staged,
            messages_committed: cur.stats.messages_committed,
            bytes_staged: cur.stats.bytes_staged,
            bytes_committed: cur.stats.bytes_committed,
        })
    }

    /// Increment the `messages_staged` stat.
    pub fn inc_messages_staged_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.messages_staged = cur.stats.messages_staged + value;
        cur.prom.messages_staged.inc_by(value);
    }

    /// Increment the `bytes_received` stat.
    pub fn inc_bytes_staged_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.bytes_staged = cur.stats.bytes_staged + value;
        cur.prom.bytes_staged.inc_by(value);
    }

    /// Increment the `messages_committed` stat.
    pub fn inc_messages_committed_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.messages_committed = cur.stats.messages_committed + value;
        cur.prom.messages_committed.inc_by(value);
    }

    /// Increment the `bytes_committed` stat.
    pub fn inc_bytes_committed_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.bytes_committed = cur.stats.bytes_committed + value;
        cur.prom.bytes_committed.inc_by(value);
    }
}

/// A structure that keeps track of _local_ statistics, as well as aggregating
/// statistics into a single worker (currently worker 0).
///
/// This is because we ONLY want to emit statistics for _gauge-style-statistics_ when
/// ALL workers have caught up to the _currently running instance of a source/sink_ and have
/// reported an accurate gauge. This prevents issues like 1 worker reporting
/// `snapshot_committed=true` before the others workers have caught up to report that they haven't
/// finished snapshotting.
///
/// The API flow is:
/// - Initialize sources/sinks with `initialize_source` or `initialize_sink`. This advances the
/// local epoch.
/// - Advance the _global epoch_ with `advance_global_epoch`. This should always be called strictly
/// before reinitializing objects.
/// - Emit a snapshot of local data with `emit_local`, which can be broadcasted to other workers.
/// - Ingest data from other workers with `ingest`.
/// - Emit a `snapshot` of _global data_ (i.e. skipping objects that don't have all workers caught
/// up) with `snapshot`.
///
/// All functions can and should be called from ALL workers.
///
/// Note that we hand-roll a statistics epoch here, but in the future, when the storage
/// _controller_ has top-level support for _dataflow epochs_, this logic can be entirely moved
/// there.
pub struct AggregatedStatistics {
    worker_id: usize,
    worker_count: usize,
    local_source_statistics: BTreeMap<GlobalId, (usize, SourceStatistics)>,
    local_sink_statistics: BTreeMap<GlobalId, (usize, SinkStatistics)>,

    global_source_statistics: BTreeMap<GlobalId, (usize, BTreeMap<usize, SourceStatisticsUpdate>)>,
    global_sink_statistics: BTreeMap<GlobalId, (usize, BTreeMap<usize, SinkStatisticsUpdate>)>,
}

impl AggregatedStatistics {
    /// Initialize a new `AggregatedStatistics`.
    pub fn new(worker_id: usize, worker_count: usize) -> Self {
        AggregatedStatistics {
            worker_id,
            worker_count,
            local_source_statistics: Default::default(),
            local_sink_statistics: Default::default(),
            global_source_statistics: Default::default(),
            global_sink_statistics: Default::default(),
        }
    }

    /// Get a `SourceStatistics` for an id, if it exists.
    pub fn get_source(&self, id: &GlobalId) -> Option<&SourceStatistics> {
        self.local_source_statistics.get(id).map(|(_, s)| s)
    }

    /// Get a `SinkStatistics` for an id, if it exists.
    pub fn get_sink(&self, id: &GlobalId) -> Option<&SinkStatistics> {
        self.local_sink_statistics.get(id).map(|(_, s)| s)
    }

    /// Deinitialize an object. Other methods other than `initialize_source` and `initialize_sink`
    /// will never overwrite this.
    pub fn deinitialize(&mut self, id: GlobalId) {
        self.local_source_statistics.remove(&id);
        self.local_sink_statistics.remove(&id);
        self.global_source_statistics.remove(&id);
        self.global_sink_statistics.remove(&id);
    }

    /// Advance the _global epoch_ for statistics. Local statistics before this epoch will be
    /// ignored.
    pub fn advance_global_epoch(&mut self, id: GlobalId) {
        if self.worker_id == 0 {
            self.global_source_statistics
                .entry(id)
                .and_modify(|(ref mut epoch, ref mut stats)| {
                    *epoch += 1;
                    stats.clear()
                });
            self.global_sink_statistics
                .entry(id)
                .and_modify(|(ref mut epoch, ref mut stats)| {
                    *epoch += 1;
                    stats.clear()
                });
        }
    }

    /// Re-initialize a source. If it already exists, then its local epoch is advanced.
    pub fn initialize_source<F: FnOnce() -> SourceStatistics>(&mut self, id: GlobalId, stats: F) {
        self.local_source_statistics
            .entry(id)
            .and_modify(|(ref mut epoch, ref mut stats)| {
                *epoch += 1;
                stats.clear()
            })
            .or_insert_with(|| (0, stats()));

        if self.worker_id == 0 {
            self.global_source_statistics
                .entry(id)
                .or_insert_with(|| (0, Default::default()));
        }
    }

    /// Re-initialize a sink. If it already exists, then its local epoch is advanced.
    pub fn initialize_sink<F: FnOnce() -> SinkStatistics>(&mut self, id: GlobalId, stats: F) {
        self.local_sink_statistics
            .entry(id)
            .and_modify(|(ref mut epoch, ref mut stats)| {
                *epoch += 1;
                stats.clear()
            })
            .or_insert_with(|| (0, stats()));
        if self.worker_id == 0 {
            self.global_sink_statistics
                .entry(id)
                .or_insert_with(|| (0, Default::default()));
        }
    }

    /// Ingest data from other workers.
    pub fn ingest(
        &mut self,
        source_statistics: Vec<(usize, SourceStatisticsUpdate)>,
        sink_statistics: Vec<(usize, SinkStatisticsUpdate)>,
    ) {
        // Currently, only worker 0 ingest data from other workers.
        if self.worker_id == 0 {
            for (epoch, stat) in source_statistics {
                self.global_source_statistics.entry(stat.id).and_modify(
                    |(global_epoch, ref mut stats)| {
                        if epoch >= *global_epoch {
                            stats.insert(stat.worker_id, stat);
                        }
                    },
                );
            }

            for (epoch, stat) in sink_statistics {
                self.global_sink_statistics.entry(stat.id).and_modify(
                    |(global_epoch, ref mut stats)| {
                        if epoch >= *global_epoch {
                            stats.insert(stat.worker_id, stat);
                        }
                    },
                );
            }
        }
    }

    fn _emit_local(
        &self,
    ) -> (
        Vec<(usize, SourceStatisticsUpdate)>,
        Vec<(usize, SinkStatisticsUpdate)>,
    ) {
        let sources = self
            .local_source_statistics
            .values()
            .flat_map(|(epoch, s)| s.snapshot().map(|v| (*epoch, v)))
            .collect();

        let sinks = self
            .local_sink_statistics
            .values()
            .flat_map(|(epoch, s)| s.snapshot().map(|v| (*epoch, v)))
            .collect();

        (sources, sinks)
    }

    /// Emit a snapshot of this workers local data.
    pub fn emit_local(
        &self,
    ) -> (
        Vec<(usize, SourceStatisticsUpdate)>,
        Vec<(usize, SinkStatisticsUpdate)>,
    ) {
        // As an optimization, worker 0 does not broadcast it data. It ingests
        // it in `snapshot`.
        if self.worker_id == 0 {
            return (Vec::new(), Vec::new());
        }

        let sources = self
            .local_source_statistics
            .values()
            .flat_map(|(epoch, s)| s.snapshot().map(|v| (*epoch, v)))
            .collect();

        let sinks = self
            .local_sink_statistics
            .values()
            .flat_map(|(epoch, s)| s.snapshot().map(|v| (*epoch, v)))
            .collect();

        (sources, sinks)
    }

    /// Emit a _global_ snapshot of data. This does not include objects whose workers have not
    /// initialized gauges for the current epoch.
    pub fn snapshot(&mut self) -> (Vec<SourceStatisticsUpdate>, Vec<SinkStatisticsUpdate>) {
        if !self.worker_id == 0 {
            return (Vec::new(), Vec::new());
        }

        let (sources, sinks) = self._emit_local();
        self.ingest(sources, sinks);

        let sources = self
            .global_source_statistics
            .values()
            .filter_map(|(_, s)| {
                if s.len() >= self.worker_count {
                    Some(s.values().cloned())
                } else {
                    None
                }
            })
            .flatten()
            .collect();

        let sinks = self
            .global_sink_statistics
            .values()
            .filter_map(|(_, s)| {
                if s.len() >= self.worker_count {
                    Some(s.values().cloned())
                } else {
                    None
                }
            })
            .flatten()
            .collect();

        (sources, sinks)
    }
}
