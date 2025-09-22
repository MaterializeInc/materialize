// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for managing storage statistics.
//!
//!
//! This module collects statistics related to sources and sinks. Statistics, as exposed
//! to their respective system tables have strong semantics, defined within the
//! `mz_storage_types::statistics` module. This module collects and aggregates metrics
//! across workers according to those semantics.
//!
//! Note that it _simultaneously_ collect prometheus metrics for the given statistics. Those
//! metrics _do not have the same strong semantics_, which is _by design_ to ensure we
//! are able to categorically debug sources and sinks during complex failures (or bugs
//! with statistics collection itself). Prometheus metrics are
//! - Never dropped or reset until a source/sink is dropped.
//! - Entirely independent across workers.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropCounter, DeleteOnDropGauge, IntCounterVec, IntGaugeVec, MetricsRegistry,
    UIntGaugeVec,
};
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::statistics::{Gauge, SinkStatisticsUpdate, SourceStatisticsUpdate};
use mz_storage_types::sources::SourceEnvelope;
use prometheus::core::{AtomicI64, AtomicU64};
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::progress::frontier::Antichain;

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
    pub(crate) bytes_indexed: UIntGaugeVec,
    pub(crate) records_indexed: UIntGaugeVec,
    pub(crate) rehydration_latency_ms: IntGaugeVec,

    pub(crate) offset_known: UIntGaugeVec,
    pub(crate) offset_committed: UIntGaugeVec,
    pub(crate) snapshot_records_known: UIntGaugeVec,
    pub(crate) snapshot_records_staged: UIntGaugeVec,

    // Just prometheus.
    pub(crate) envelope_state_tombstones: UIntGaugeVec,
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
            bytes_indexed: registry.register(metric!(
                name: "mz_source_bytes_indexed",
                help: "The number of bytes of the source envelope state kept. This will be specific to the envelope in use.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            records_indexed: registry.register(metric!(
                name: "mz_source_records_indexed",
                help: "The number of records in the source envelope state. This will be specific to the envelope in use",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            envelope_state_tombstones: registry.register(metric!(
                name: "mz_source_envelope_state_tombstones",
                help: "The number of outstanding tombstones in the source envelope state. This will be specific to the envelope in use",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            rehydration_latency_ms: registry.register(metric!(
                name: "mz_source_rehydration_latency_ms",
                help: "The amount of time in milliseconds it took for the worker to rehydrate the source envelope state. This will be specific to the envelope in use.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id", "envelope"],
            )),
            offset_known: registry.register(metric!(
                name: "mz_source_offset_known",
                help: "The total number of _values_ (source-defined unit) present in upstream.",
                var_labels: ["source_id", "worker_id", "shard_id"],
            )),
            offset_committed: registry.register(metric!(
                name: "mz_source_offset_committed",
                help: "The total number of _values_ (source-defined unit) we have fully processed, and storage and committed.",
                var_labels: ["source_id", "worker_id", "shard_id"],
            )),
            snapshot_records_known: registry.register(metric!(
                name: "mz_source_snapshot_records_known",
                help: "The total number of records in the source's snapshot",
                var_labels: ["source_id", "worker_id", "shard_id", "parent_source_id"],
            )),
            snapshot_records_staged: registry.register(metric!(
                name: "mz_source_snapshot_records_staged",
                help: "The total number of records read from the source's snapshot",
                var_labels: ["source_id", "worker_id", "shard_id", "parent_source_id"],
            )),
        }
    }
}

/// Prometheus metrics for user-facing source metrics.
#[derive(Debug)]
pub struct SourceStatisticsMetrics {
    // Counters
    pub(crate) messages_received: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) updates_staged: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) updates_committed: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) bytes_received: DeleteOnDropCounter<AtomicU64, Vec<String>>,

    // Gauges
    pub(crate) snapshot_committed: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    pub(crate) bytes_indexed: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    pub(crate) records_indexed: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    pub(crate) rehydration_latency_ms: DeleteOnDropGauge<AtomicI64, Vec<String>>,

    pub(crate) offset_known: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    pub(crate) offset_committed: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    pub(crate) snapshot_records_known: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    pub(crate) snapshot_records_staged: DeleteOnDropGauge<AtomicU64, Vec<String>>,

    // Just prometheus.
    pub(crate) envelope_state_tombstones: DeleteOnDropGauge<AtomicU64, Vec<String>>,
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
            snapshot_committed: defs.snapshot_committed.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            messages_received: defs.messages_received.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
            ]),
            updates_staged: defs.updates_staged.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            updates_committed: defs.updates_committed.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            bytes_received: defs.bytes_received.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
            ]),
            bytes_indexed: defs.bytes_indexed.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            records_indexed: defs.records_indexed.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
            ]),
            envelope_state_tombstones: defs.envelope_state_tombstones.get_delete_on_drop_metric(
                vec![
                    id.to_string(),
                    worker_id.to_string(),
                    parent_source_id.to_string(),
                    shard.clone(),
                ],
            ),
            rehydration_latency_ms: defs.rehydration_latency_ms.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                parent_source_id.to_string(),
                shard.clone(),
                envelope.to_string(),
            ]),
            offset_known: defs.offset_known.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                shard.clone(),
            ]),
            offset_committed: defs.offset_committed.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                shard.clone(),
            ]),
            snapshot_records_known: defs.snapshot_records_known.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                shard.clone(),
                parent_source_id.to_string(),
            ]),
            snapshot_records_staged: defs.snapshot_records_staged.get_delete_on_drop_metric(vec![
                id.to_string(),
                worker_id.to_string(),
                shard.clone(),
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
    pub(crate) messages_staged: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) messages_committed: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) bytes_staged: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) bytes_committed: DeleteOnDropCounter<AtomicU64, Vec<String>>,
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
                .get_delete_on_drop_metric(vec![id.to_string(), worker_id.to_string()]),
            messages_committed: defs
                .messages_committed
                .get_delete_on_drop_metric(vec![id.to_string(), worker_id.to_string()]),
            bytes_staged: defs
                .bytes_staged
                .get_delete_on_drop_metric(vec![id.to_string(), worker_id.to_string()]),
            bytes_committed: defs
                .bytes_committed
                .get_delete_on_drop_metric(vec![id.to_string(), worker_id.to_string()]),
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SourceStatisticsRecord {
    id: GlobalId,
    worker_id: usize,
    // Counters
    messages_received: u64,
    bytes_received: u64,
    updates_staged: u64,
    updates_committed: u64,

    // Gauges are always wrapped in an `Option` that represents if that gauge has been
    // initialized by that worker.
    records_indexed: Option<u64>,
    bytes_indexed: Option<u64>,

    // This field is nullable, so its value is an `Option`.
    rehydration_latency_ms: Option<Option<i64>>,

    // The following fields are able to be unset when shipped to the controller, so their
    // values are `Option`'s
    snapshot_records_known: Option<Option<u64>>,
    snapshot_records_staged: Option<Option<u64>>,
    snapshot_committed: Option<bool>,
    offset_known: Option<Option<u64>>,
    offset_committed: Option<Option<u64>>,

    // Just prometheus.
    envelope_state_tombstones: u64,
}

impl SourceStatisticsRecord {
    fn reset_gauges(&mut self) {
        // These gauges MUST be initialized across all workers before we aggregate and
        // report statistics.
        self.rehydration_latency_ms = None;
        self.snapshot_committed = None;

        // We consider these gauges always initialized
        self.bytes_indexed = Some(0);
        self.records_indexed = Some(0);

        // We don't yet populate these, so we consider the initialized (with an empty value).
        self.snapshot_records_known = Some(None);
        self.snapshot_records_staged = Some(None);
        self.offset_known = Some(None);
        self.offset_committed = Some(None);

        self.envelope_state_tombstones = 0;
    }

    /// Reset counters so that we continue to ship diffs to the controller.
    fn reset_counters(&mut self) {
        self.messages_received = 0;
        self.bytes_received = 0;
        self.updates_staged = 0;
        self.updates_committed = 0;
    }

    /// Convert this record into an `SourceStatisticsUpdate` to be merged
    /// across workers. Gauges must be initialized.
    fn as_update(&self) -> SourceStatisticsUpdate {
        let SourceStatisticsRecord {
            id,
            worker_id: _,
            messages_received,
            bytes_received,
            updates_staged,
            updates_committed,
            records_indexed,
            bytes_indexed,
            rehydration_latency_ms,
            snapshot_records_known,
            snapshot_records_staged,
            snapshot_committed,
            offset_known,
            offset_committed,
            envelope_state_tombstones: _,
        } = self.clone();

        SourceStatisticsUpdate {
            id,
            messages_received: messages_received.into(),
            bytes_received: bytes_received.into(),
            updates_staged: updates_staged.into(),
            updates_committed: updates_committed.into(),
            records_indexed: Gauge::gauge(records_indexed.unwrap()),
            bytes_indexed: Gauge::gauge(bytes_indexed.unwrap()),
            rehydration_latency_ms: Gauge::gauge(rehydration_latency_ms.unwrap()),
            snapshot_records_known: Gauge::gauge(snapshot_records_known.unwrap()),
            snapshot_records_staged: Gauge::gauge(snapshot_records_staged.unwrap()),
            snapshot_committed: Gauge::gauge(snapshot_committed.unwrap()),
            offset_known: Gauge::gauge(offset_known.unwrap()),
            offset_committed: Gauge::gauge(offset_committed.unwrap()),
        }
    }
}

/// Statistics maintained for sinks. Gauges can be uninitialized.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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
    fn reset_gauges(&self) {}

    /// Reset counters so that we continue to ship diffs to the controller.
    fn reset_counters(&mut self) {
        self.messages_staged = 0;
        self.messages_committed = 0;
        self.bytes_staged = 0;
        self.bytes_committed = 0;
    }

    /// Convert this record into an `SinkStatisticsUpdate` to be merged
    /// across workers. Gauges must be initialized.
    fn as_update(&self) -> SinkStatisticsUpdate {
        let SinkStatisticsRecord {
            id,
            worker_id: _,
            messages_staged,
            messages_committed,
            bytes_staged,
            bytes_committed,
        } = self.clone();

        SinkStatisticsUpdate {
            id,
            messages_staged: messages_staged.into(),
            messages_committed: messages_committed.into(),
            bytes_staged: bytes_staged.into(),
            bytes_committed: bytes_committed.into(),
        }
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
                    records_indexed: Some(0),
                    bytes_indexed: Some(0),
                    rehydration_latency_ms: None,
                    snapshot_records_staged: Some(None),
                    snapshot_records_known: Some(None),
                    snapshot_committed: None,
                    offset_known: Some(None),
                    offset_committed: Some(None),
                    envelope_state_tombstones: 0,
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

    /// Reset gauges to uninitialized state.
    /// This does not reset prometheus gauges, which will be overwritten with new values.
    pub fn reset_gauges(&self) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.reset_gauges();
    }

    /// Get a snapshot of the data, returning `None` if all gauges are not initialized.
    ///
    /// This also resets counters, so that we continue to move diffs around.
    pub fn snapshot(&self) -> Option<SourceStatisticsRecord> {
        let mut cur = self.stats.borrow_mut();

        match &cur.stats {
            SourceStatisticsRecord {
                records_indexed: Some(_),
                bytes_indexed: Some(_),
                rehydration_latency_ms: Some(_),
                snapshot_records_known: Some(_),
                snapshot_records_staged: Some(_),
                snapshot_committed: Some(_),
                offset_known: Some(_),
                offset_committed: Some(_),
                ..
            } => {
                let ret = Some(cur.stats.clone());
                cur.stats.reset_counters();
                ret
            }
            _ => None,
        }
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

    /// Update the `bytes_indexed` stat.
    /// A positive value will add and a negative value will subtract.
    pub fn update_bytes_indexed_by(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        if let Some(updated) = cur
            .stats
            .bytes_indexed
            .unwrap_or(0)
            .checked_add_signed(value)
        {
            cur.stats.bytes_indexed = Some(updated);
            cur.prom.bytes_indexed.set(updated);
        } else {
            let bytes_indexed = cur.stats.bytes_indexed.unwrap_or(0);
            tracing::warn!(
                "Unexpected u64 overflow while updating bytes_indexed value {} with {}",
                bytes_indexed,
                value
            );
            cur.stats.bytes_indexed = Some(0);
            cur.prom.bytes_indexed.set(0);
        }
    }

    /// Set the `bytes_indexed` to the given value
    pub fn set_bytes_indexed(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        let value = if value < 0 {
            tracing::warn!("Unexpected negative value for bytes_indexed {}", value);
            0
        } else {
            value.unsigned_abs()
        };
        cur.stats.bytes_indexed = Some(value);
        cur.prom.bytes_indexed.set(value);
    }

    /// Update the `records_indexed` stat.
    /// A positive value will add and a negative value will subtract.
    pub fn update_records_indexed_by(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        if let Some(updated) = cur
            .stats
            .records_indexed
            .unwrap_or(0)
            .checked_add_signed(value)
        {
            cur.stats.records_indexed = Some(updated);
            cur.prom.records_indexed.set(updated);
        } else {
            let records_indexed = cur.stats.records_indexed.unwrap_or(0);
            tracing::warn!(
                "Unexpected u64 overflow while updating records_indexed value {} with {}",
                records_indexed,
                value
            );
            cur.stats.records_indexed = Some(0);
            cur.prom.records_indexed.set(0);
        }
    }

    /// Set the `records_indexed` to the given value
    pub fn set_records_indexed(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        let value = if value < 0 {
            tracing::warn!("Unexpected negative value for records_indexed {}", value);
            0
        } else {
            value.unsigned_abs()
        };
        cur.stats.records_indexed = Some(value);
        cur.prom.records_indexed.set(value);
    }

    /// Initialize the `rehydration_latency_ms` stat as `NULL`.
    pub fn initialize_rehydration_latency_ms(&self) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.rehydration_latency_ms = Some(None);
    }

    /// Update the `envelope_state_tombstones` stat.
    /// A positive value will add and a negative value will subtract.
    // TODO(guswynn): consider exposing this to users
    pub fn update_envelope_state_tombstones_by(&self, value: i64) {
        let mut cur = self.stats.borrow_mut();
        if let Some(updated) = cur
            .stats
            .envelope_state_tombstones
            .checked_add_signed(value)
        {
            cur.stats.envelope_state_tombstones = updated;
            cur.prom.envelope_state_tombstones.set(updated);
        } else {
            let envelope_state_tombstones = cur.stats.envelope_state_tombstones;
            tracing::warn!(
                "Unexpected u64 overflow while updating envelope_state_tombstones value {} with {}",
                envelope_state_tombstones,
                value
            );
            cur.stats.envelope_state_tombstones = 0;
            cur.prom.envelope_state_tombstones.set(0);
        }
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

    /// Set the `offset_known` stat to the given value.
    pub fn set_offset_known(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.offset_known = Some(Some(value));
        cur.prom.offset_known.set(value);
    }

    /// Set the `offset_committed` stat to the given value.
    pub fn set_offset_committed(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.offset_committed = Some(Some(value));
        cur.prom.offset_committed.set(value);
    }

    /// Set the `snapshot_records_known` stat to the given value.
    pub fn set_snapshot_records_known(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.snapshot_records_known = Some(Some(value));
        cur.prom.snapshot_records_known.set(value);
    }

    /// Set the `snapshot_records_known` stat to the given value.
    pub fn set_snapshot_records_staged(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.stats.snapshot_records_staged = Some(Some(value));
        cur.prom.snapshot_records_staged.set(value);
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

    /// Reset gauges to uninitialized state.
    /// This does not reset prometheus gauges, which will be overwritten with new values.
    pub fn reset_gauges(&self) {
        let cur = self.stats.borrow_mut();
        cur.stats.reset_gauges();
    }

    /// Get a snapshot of the data, returning `None` if all gauges are not initialized.
    ///
    /// This also resets counters, so that we continue to move diffs around.
    pub fn snapshot(&self) -> Option<SinkStatisticsRecord> {
        let mut cur = self.stats.borrow_mut();

        match &cur.stats {
            SinkStatisticsRecord { .. } => {
                let ret = Some(cur.stats.clone());
                cur.stats.reset_counters();
                ret
            }
        }
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
    local_source_statistics: BTreeMap<GlobalId, (usize, GlobalId, SourceStatistics)>,
    local_sink_statistics: BTreeMap<GlobalId, (usize, SinkStatistics)>,

    global_source_statistics:
        BTreeMap<GlobalId, (usize, GlobalId, Vec<Option<SourceStatisticsUpdate>>)>,
    global_sink_statistics: BTreeMap<GlobalId, (usize, Vec<Option<SinkStatisticsUpdate>>)>,
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

    /// Get a collection of `SourceStatistics` for the ingestion `ingestion_id`.
    pub fn get_ingestion_stats(
        &self,
        ingestion_id: &GlobalId,
    ) -> BTreeMap<GlobalId, SourceStatistics> {
        let mut ingestion_stats = BTreeMap::new();
        for (id, (_epoch, ingestion_id2, stats)) in self.local_source_statistics.iter() {
            if ingestion_id == ingestion_id2 {
                ingestion_stats.insert(id.clone(), stats.clone());
            }
        }
        ingestion_stats
    }

    /// Get a `SourceStatistics` for an id, if it exists.
    pub fn get_source(&self, id: &GlobalId) -> Option<&SourceStatistics> {
        self.local_source_statistics.get(id).map(|(_, _, s)| s)
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

    /// Advance the _global epoch_ for statistics.
    ///
    /// Gauge values from previous epochs will be ignored. Counter values from previous epochs will
    /// still be applied as usual.
    pub fn advance_global_epoch(&mut self, id: GlobalId) {
        if let Some((epoch, _ingestion_id, stats)) = self.global_source_statistics.get_mut(&id) {
            *epoch += 1;
            for worker_stats in stats {
                if let Some(update) = worker_stats {
                    update.reset_gauges();
                }
            }
        }
        if let Some((epoch, stats)) = self.global_sink_statistics.get_mut(&id) {
            *epoch += 1;
            for worker_stats in stats {
                if let Some(update) = worker_stats {
                    update.reset_gauges();
                }
            }
        }
    }

    /// Re-initialize a source. If it already exists, then its local epoch is advanced.
    pub fn initialize_source<F: FnOnce() -> SourceStatistics>(
        &mut self,
        id: GlobalId,
        ingestion_id: GlobalId,
        resume_upper: Antichain<Timestamp>,
        stats: F,
    ) {
        self.local_source_statistics
            .entry(id)
            .and_modify(|(epoch, ingestion_id2, stats)| {
                assert_eq!(ingestion_id, *ingestion_id2);
                *epoch += 1;
                stats.reset_gauges();
                stats.meta = SourceStatisticsMetadata::new(resume_upper);
            })
            .or_insert_with(|| (0, ingestion_id, stats()));

        if self.worker_id == 0 {
            self.global_source_statistics
                .entry(id)
                .or_insert_with(|| (0, ingestion_id, vec![None; self.worker_count]));
        }
    }

    /// Re-initialize a sink. If it already exists, then its local epoch is advanced.
    pub fn initialize_sink<F: FnOnce() -> SinkStatistics>(&mut self, id: GlobalId, stats: F) {
        self.local_sink_statistics
            .entry(id)
            .and_modify(|(epoch, stats)| {
                *epoch += 1;
                stats.reset_gauges();
            })
            .or_insert_with(|| (0, stats()));
        if self.worker_id == 0 {
            self.global_sink_statistics
                .entry(id)
                .or_insert_with(|| (0, vec![None; self.worker_count]));
        }
    }

    /// Ingest data from other workers.
    pub fn ingest(
        &mut self,
        source_statistics: Vec<(usize, SourceStatisticsRecord)>,
        sink_statistics: Vec<(usize, SinkStatisticsRecord)>,
    ) {
        // Currently, only worker 0 ingest data from other workers.
        if self.worker_id != 0 {
            return;
        }

        for (epoch, stat) in source_statistics {
            if let Some((global_epoch, _, stats)) = self.global_source_statistics.get_mut(&stat.id)
            {
                // The record might be from a previous incarnation of the source. If that's the
                // case, we only incorporate its counter values and ignore its gauge values.
                let epoch_match = epoch >= *global_epoch;
                let mut update = stat.as_update();
                match (&mut stats[stat.worker_id], epoch_match) {
                    (None, true) => stats[stat.worker_id] = Some(update),
                    (None, false) => {
                        update.reset_gauges();
                        stats[stat.worker_id] = Some(update);
                    }
                    (Some(occupied), true) => occupied.incorporate(update),
                    (Some(occupied), false) => occupied.incorporate_counters(update),
                }
            }
        }

        for (epoch, stat) in sink_statistics {
            if let Some((global_epoch, stats)) = self.global_sink_statistics.get_mut(&stat.id) {
                // The record might be from a previous incarnation of the sink. If that's the
                // case, we only incorporate its counter values and ignore its gauge values.
                let epoch_match = epoch >= *global_epoch;
                let update = stat.as_update();
                match (&mut stats[stat.worker_id], epoch_match) {
                    (None, true) => stats[stat.worker_id] = Some(update),
                    (None, false) => {
                        update.reset_gauges();
                        stats[stat.worker_id] = Some(update);
                    }
                    (Some(occupied), true) => occupied.incorporate(update),
                    (Some(occupied), false) => occupied.incorporate_counters(update),
                }
            }
        }
    }

    fn _emit_local(
        &mut self,
    ) -> (
        Vec<(usize, SourceStatisticsRecord)>,
        Vec<(usize, SinkStatisticsRecord)>,
    ) {
        let sources = self
            .local_source_statistics
            .values_mut()
            .flat_map(|(epoch, _, s)| s.snapshot().map(|v| (*epoch, v)))
            .collect();

        let sinks = self
            .local_sink_statistics
            .values_mut()
            .flat_map(|(epoch, s)| s.snapshot().map(|v| (*epoch, v)))
            .collect();

        (sources, sinks)
    }

    /// Emit a snapshot of this workers local data.
    pub fn emit_local(
        &mut self,
    ) -> (
        Vec<(usize, SourceStatisticsRecord)>,
        Vec<(usize, SinkStatisticsRecord)>,
    ) {
        // As an optimization, worker 0 does not broadcast it data. It ingests
        // it in `snapshot`.
        if self.worker_id == 0 {
            return (Vec::new(), Vec::new());
        }

        self._emit_local()
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
            .iter_mut()
            .filter_map(|(_, (_, _, s))| {
                if s.iter().all(|s| s.is_some()) {
                    let ret = Some(SourceStatisticsUpdate::summarize(|| {
                        s.iter().filter_map(Option::as_ref)
                    }));

                    // Reset the counters so we only report diffs.
                    s.iter_mut().for_each(|s| {
                        if let Some(s) = s {
                            s.reset_counters();
                        }
                    });
                    ret
                } else {
                    None
                }
            })
            .collect();

        let sinks = self
            .global_sink_statistics
            .iter_mut()
            .filter_map(|(_, (_, s))| {
                if s.iter().all(|s| s.is_some()) {
                    let ret = Some(SinkStatisticsUpdate::summarize(|| {
                        s.iter().filter_map(Option::as_ref)
                    }));

                    // Reset the counters so we only report diffs.
                    s.iter_mut().for_each(|s| {
                        if let Some(s) = s {
                            s.reset_counters();
                        }
                    });
                    ret
                } else {
                    None
                }
            })
            .collect();

        (sources, sinks)
    }
}
