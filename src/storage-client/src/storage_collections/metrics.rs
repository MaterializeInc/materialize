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
use mz_persist_types::ShardId;
use prometheus::Counter;

#[derive(Debug)]
pub struct StorageCollectionsMetrics {
    pub finalization_outstanding: UIntGauge,
    pub finalization_pending_commit: UIntGauge,
    pub finalization_started: Counter,
    pub finalization_succeeded: Counter,
    pub finalization_failed: Counter,
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
