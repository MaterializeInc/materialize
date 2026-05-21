// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, MutexGuard};

use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
use mz_ore::stats::histogram_seconds_buckets;
use mz_persist_types::ShardId;
use prometheus::{Counter, HistogramVec};

#[derive(Debug)]
pub struct StorageCollectionsMetrics {
    pub finalization_outstanding: UIntGauge,
    pub finalization_pending_commit: UIntGauge,
    pub finalization_started: Counter,
    pub finalization_succeeded: Counter,
    pub finalization_failed: Counter,
    pub create_collections_phase_seconds: HistogramVec,
    pub prepare_state_phase_seconds: HistogramVec,
    /// Number of observed advances of the txns shard upper. Incremented once
    /// each time the `BackgroundTask` learns of a new txns upper. Useful for
    /// telling apart "txns shard genuinely commits at rate X" from
    /// "BackgroundTask is doing O(N) work at rate X for some other reason".
    pub txns_upper_advances: Counter,
    /// Number of times the periodic since-downgrade sweep for txns-backed
    /// collections has run. Compare with `txns_upper_advances` to see how
    /// much fanout work is being coalesced by the sweep.
    pub txns_since_sweeps: Counter,
}

impl StorageCollectionsMetrics {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        StorageCollectionsMetrics {
            finalization_outstanding: registry.register(metric!(
                name: "mz_shard_finalization_outstanding",
                help: "count of shards in need of finalization",
            )),
            finalization_pending_commit: registry.register(metric!(
                name: "mz_shard_finalization_pending_commit",
                help: "count of shards for which finalization has completed but has not yet been durably recorded",
            )),
            finalization_started: registry.register(metric!(
                name: "mz_shard_finalization_op_started",
                help: "count of shard finalization operations that have started",
            )),
            finalization_succeeded: registry.register(metric!(
                name: "mz_shard_finalization_op_succeeded",
                help: "count of shard finalization operations that succeeded",
            )),
            finalization_failed: registry.register(metric!(
                name: "mz_shard_finalization_op_failed",
                help: "count of shard finalization operations that failed",
            )),
            create_collections_phase_seconds: registry.register(metric!(
                name: "mz_storage_collections_create_collections_phase_seconds",
                help: "The time spent in each phase of a single \
                       StorageCollections::create_collections_for_bootstrap call.",
                var_labels: ["phase"],
                buckets: histogram_seconds_buckets(0.0001, 32.0),
            )),
            prepare_state_phase_seconds: registry.register(metric!(
                name: "mz_storage_collections_prepare_state_phase_seconds",
                help: "The time spent in each phase of a single \
                       StorageCollections::prepare_state call.",
                var_labels: ["phase"],
                buckets: histogram_seconds_buckets(0.000_01, 32.0),
            )),
            txns_upper_advances: registry.register(metric!(
                name: "mz_storage_collections_txns_upper_advances_total",
                help: "Count of observed advances of the txns shard upper, as \
                       observed by the StorageCollections BackgroundTask.",
            )),
            txns_since_sweeps: registry.register(metric!(
                name: "mz_storage_collections_txns_since_sweeps_total",
                help: "Count of periodic since-downgrade sweeps over txns-backed \
                       collections performed by the StorageCollections BackgroundTask.",
            )),
        }
    }
}

/// A set of shard IDs that maintains a gauge containing the set's size.
#[derive(Debug)]
pub struct ShardIdSet {
    set: Mutex<BTreeSet<ShardId>>,
    gauge: UIntGauge,
}

impl ShardIdSet {
    pub fn new(gauge: UIntGauge) -> ShardIdSet {
        ShardIdSet {
            set: Mutex::new(BTreeSet::new()),
            gauge,
        }
    }

    pub fn lock(&self) -> ShardIdSetGuard<'_> {
        ShardIdSetGuard {
            set: self.set.lock().expect("lock poisoned"),
            gauge: &self.gauge,
        }
    }
}

#[derive(Debug)]
pub struct ShardIdSetGuard<'a> {
    set: MutexGuard<'a, BTreeSet<ShardId>>,
    gauge: &'a UIntGauge,
}

impl Drop for ShardIdSetGuard<'_> {
    fn drop(&mut self) {
        self.gauge.set(u64::cast_from(self.set.len()));
    }
}

impl<'a> Deref for ShardIdSetGuard<'a> {
    type Target = BTreeSet<ShardId>;

    fn deref(&self) -> &Self::Target {
        self.set.deref()
    }
}

impl DerefMut for ShardIdSetGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.set.deref_mut()
    }
}
