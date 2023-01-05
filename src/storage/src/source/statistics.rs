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
use std::rc::Rc;

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;

use mz_ore::metric;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::metrics::{IntCounterVec, MetricsRegistry, UIntGaugeVec};
use mz_repr::GlobalId;
use mz_storage_client::client::SourceStatisticsUpdate;
use prometheus::core::AtomicU64;

use crate::source::metrics::SourceBaseMetrics;

#[derive(Clone, Debug)]
pub(crate) struct SourceStatisticsMetricsDefinitions {
    pub(crate) snapshot_committed: UIntGaugeVec,
    pub(crate) messages_received: IntCounterVec,
    pub(crate) updates_staged: IntCounterVec,
    pub(crate) updates_committed: IntCounterVec,
    pub(crate) bytes_received: IntCounterVec,
}

impl SourceStatisticsMetricsDefinitions {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            snapshot_committed: registry.register(metric!(
                name: "mz_snapshot_committed",
                help: "Whether or not the worker has committed the initial snapshot for a source.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            messages_received: registry.register(metric!(
                name: "mz_messages_received",
                help: "The number of raw messages the worker has received from upstream.",
                var_labels: ["source_id", "worker_id", "parent_source_id"],
            )),
            updates_staged: registry.register(metric!(
                name: "mz_updates_staged",
                help: "The number of updates (inserts + deletes) the worker has written but not yet committed to the storage layer.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            updates_committed: registry.register(metric!(
                name: "mz_updates_committed",
                help: "The number of updates (inserts + deletes) the worker has committed into the storage layer.",
                var_labels: ["source_id", "worker_id", "parent_source_id", "shard_id"],
            )),
            bytes_received: registry.register(metric!(
                name: "mz_bytes_received",
                help: "The number of bytes worth of messages the worker has received from upstream. The way the bytes are counted is source-specific.",
                var_labels: ["source_id", "worker_id", "parent_source_id"],
            )),
        }
    }
}

/// Prometheus metrics for user-facing source metrics.
pub struct SourceStatisticsMetrics {
    pub(crate) snapshot_committed: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub(crate) messages_received: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) updates_staged: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) updates_committed: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) bytes_received: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl SourceStatisticsMetrics {
    pub fn new(
        id: GlobalId,
        worker_id: usize,
        metrics: &SourceBaseMetrics,
        parent_source_id: GlobalId,
        shard_id: &mz_persist_client::ShardId,
    ) -> SourceStatisticsMetrics {
        let shard = shard_id.to_string();

        SourceStatisticsMetrics {
            snapshot_committed: metrics
                .source_statistics
                .snapshot_committed
                .get_delete_on_drop_gauge(vec![
                    id.to_string(),
                    worker_id.to_string(),
                    parent_source_id.to_string(),
                    shard.clone(),
                ]),
            messages_received: metrics
                .source_statistics
                .messages_received
                .get_delete_on_drop_counter(vec![
                    id.to_string(),
                    worker_id.to_string(),
                    parent_source_id.to_string(),
                ]),
            updates_staged: metrics
                .source_statistics
                .updates_staged
                .get_delete_on_drop_counter(vec![
                    id.to_string(),
                    worker_id.to_string(),
                    parent_source_id.to_string(),
                    shard.clone(),
                ]),
            updates_committed: metrics
                .source_statistics
                .updates_committed
                .get_delete_on_drop_counter(vec![
                    id.to_string(),
                    worker_id.to_string(),
                    parent_source_id.to_string(),
                    shard,
                ]),
            bytes_received: metrics
                .source_statistics
                .bytes_received
                .get_delete_on_drop_counter(vec![
                    id.to_string(),
                    worker_id.to_string(),
                    parent_source_id.to_string(),
                ]),
        }
    }
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
/// - In sql, we ensure that we _never_ reset `snapshot_committed` to `false`, but gauges and
/// counters are ordinarily reset to 0 in Prometheus, so on restarts this value may be inconsistent.
#[derive(Clone)]
pub struct SourceStatistics {
    // We just use `SourceStatisticsUpdate` for convenience here!
    // The boolean its an initialization flag, to ensure we
    // don't report a false `snapshot_committed` value before
    // the `persist_sink` reports the current shard upper.
    // TODO(guswynn): this boolean is kindof gross, it should
    // probably be cleaned up.
    //
    // Note also that the `DeleteOnDropCounter`'s in the `SourceStatisticsMetrics`
    // already are in an `Arc`, so this is a bit of extra wrapping, but the cost
    // shouldn't cost too much.
    stats: Rc<RefCell<(bool, SourceStatisticsUpdate, SourceStatisticsMetrics)>>,
}

impl SourceStatistics {
    pub fn new(
        id: GlobalId,
        worker_id: usize,
        metrics: &SourceBaseMetrics,
        parent_source_id: GlobalId,
        shard_id: &mz_persist_client::ShardId,
    ) -> Self {
        Self {
            stats: Rc::new(RefCell::new((
                false,
                SourceStatisticsUpdate {
                    id,
                    worker_id,
                    snapshot_committed: false,
                    messages_received: 0,
                    updates_staged: 0,
                    updates_committed: 0,
                    bytes_received: 0,
                },
                SourceStatisticsMetrics::new(id, worker_id, metrics, parent_source_id, shard_id),
            ))),
        }
    }

    /// Return a snapshot of the stats data, if its been initialized.
    pub fn snapshot(&self) -> Option<SourceStatisticsUpdate> {
        let inner = self.stats.borrow();
        inner.0.then(|| inner.1.clone())
    }

    /// Set the `snapshot_committed` stat based on the reported upper, and
    /// mark the stats as initialized.
    // TODO(guswynn): Actually test that this initialization logic works.
    pub fn initialize_snapshot_committed<T: Timestamp>(&self, upper: &Antichain<T>) {
        self.update_snapshot_committed(upper);
        self.stats.borrow_mut().0 = true;
    }

    /// Set the `snapshot_committed` stat based on the reported upper.
    pub fn update_snapshot_committed<T: Timestamp>(&self, upper: &Antichain<T>) {
        let value = *upper != Antichain::from_elem(T::minimum());
        let mut cur = self.stats.borrow_mut();
        cur.1.snapshot_committed = value;
        cur.2.snapshot_committed.set(if value { 1 } else { 0 });
    }

    /// Increment the `messages_received` stat.
    pub fn inc_messages_received_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.1.messages_received = cur.1.messages_received + value;
        cur.2.messages_received.inc_by(value);
    }

    /// Increment the `updates` stat.
    pub fn inc_updates_staged_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.1.updates_staged = cur.1.updates_staged + value;
        cur.2.updates_staged.inc_by(value);
    }

    /// Increment the `messages_committed` stat.
    pub fn inc_updates_committed_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.1.updates_committed = cur.1.updates_committed + value;
        cur.2.updates_committed.inc_by(value);
    }

    /// Increment the `bytes_received` stat.
    pub fn inc_bytes_received_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.1.bytes_received = cur.1.bytes_received + value;
        cur.2.bytes_received.inc_by(value);
    }
}
