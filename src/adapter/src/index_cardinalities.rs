// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Record counts of index arrangements, maintained from replica introspection.
//!
//! Compute reports how many records each index arrangement holds. Those reports
//! reach the coordinator through a per-replica introspection subscribe and land
//! here, where the optimizer can read them without awaiting anything.
//!
//! The authoritative state is per replica, because replicas disagree: one may
//! still be hydrating while another is caught up. Readers see a snapshot that
//! takes the maximum across replicas, since the dominant error is a hydrating
//! replica reporting a partial count, and under-estimating is the dangerous
//! direction for a join orderer.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use mz_cluster_client::ReplicaId;
use mz_repr::{Diff, GlobalId, Row, Timestamp};

/// Record counts of index arrangements, keyed by index [`GlobalId`].
///
/// Writes come from the coordinator's main loop. Reads come from session threads
/// running the optimizer, and take a cheap `Arc` clone of a precomputed snapshot
/// rather than walking the per-replica state.
#[derive(Debug)]
pub struct IndexCardinalities {
    inner: RwLock<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// What each replica last reported for each index.
    per_replica: BTreeMap<(ReplicaId, GlobalId), u64>,
    /// Maximum across replicas, rebuilt whenever `per_replica` changes.
    snapshot: Arc<BTreeMap<GlobalId, u64>>,
}

impl Default for IndexCardinalities {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexCardinalities {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                per_replica: BTreeMap::new(),
                snapshot: Arc::new(BTreeMap::new()),
            }),
        }
    }

    /// Applies a batch of subscribe updates reported by `replica_id`.
    ///
    /// Updates are applied in time order. A subscribe batch is sorted by time but
    /// carries no ordering between its collections, and one index's count can
    /// change more than once within a single batch. Applying every retraction
    /// before every insertion would leave the final value dependent on iteration
    /// order rather than on time.
    pub fn apply_batch<I>(&self, replica_id: ReplicaId, updates: I)
    where
        I: IntoIterator<Item = (Row, Timestamp, Diff)>,
    {
        let mut updates: Vec<_> = updates
            .into_iter()
            .filter_map(|(row, ts, diff)| {
                let (index_id, records) = decode_row(&row)?;
                Some((ts, index_id, records, diff))
            })
            .collect();
        if updates.is_empty() {
            return;
        }
        updates.sort_by_key(|(ts, ..)| *ts);

        let mut inner = self.inner.write().expect("lock poisoned");
        for (_ts, index_id, records, diff) in updates {
            let key = (replica_id, index_id);
            if diff.is_positive() {
                inner.per_replica.insert(key, records);
            } else {
                // Only retract the value we currently hold. A retraction of a
                // superseded value would otherwise drop a newer insertion that
                // shares the batch.
                if inner.per_replica.get(&key) == Some(&records) {
                    inner.per_replica.remove(&key);
                }
            }
        }
        inner.rebuild_snapshot();
    }

    /// Drops everything `replica_id` reported.
    ///
    /// Both retraction paths for an introspection subscribe must call this, the
    /// reinstall path and the drop path. Leaking a dropped replica's entries pins
    /// the estimate to that replica's last count, because reads take a maximum.
    pub fn evict_replica(&self, replica_id: ReplicaId) {
        let mut inner = self.inner.write().expect("lock poisoned");
        let before = inner.per_replica.len();
        inner.per_replica.retain(|(r, _), _| *r != replica_id);
        if inner.per_replica.len() != before {
            inner.rebuild_snapshot();
        }
    }

    /// Returns the current maximum-across-replicas view, keyed by index ID.
    pub fn snapshot(&self) -> Arc<BTreeMap<GlobalId, u64>> {
        Arc::clone(&self.inner.read().expect("lock poisoned").snapshot)
    }
}

impl Inner {
    fn rebuild_snapshot(&mut self) {
        let mut snapshot: BTreeMap<GlobalId, u64> = BTreeMap::new();
        for ((_replica, index_id), records) in &self.per_replica {
            let entry = snapshot.entry(*index_id).or_default();
            *entry = (*entry).max(*records);
        }
        self.snapshot = Arc::new(snapshot);
    }
}

/// Decodes a `(index_id, records)` row as produced by the index cardinality
/// subscribe. Malformed rows are dropped rather than panicking, because this runs
/// on the coordinator's main loop and a replica should not be able to wedge it.
fn decode_row(row: &Row) -> Option<(GlobalId, u64)> {
    let mut datums = row.iter();
    let index_id: GlobalId = datums.next()?.unwrap_str().parse().ok()?;
    let records = datums.next()?.unwrap_int64();
    let records = u64::try_from(records).ok()?;
    Some((index_id, records))
}

#[cfg(test)]
mod tests {
    use mz_repr::Datum;

    use super::*;

    fn row(index_id: &str, records: i64) -> Row {
        Row::pack_slice(&[Datum::String(index_id), Datum::Int64(records)])
    }

    fn replica(id: u64) -> ReplicaId {
        ReplicaId::User(id)
    }

    fn gid(id: &str) -> GlobalId {
        id.parse().expect("valid GlobalId")
    }

    #[mz_ore::test]
    fn insert_and_read() {
        let cards = IndexCardinalities::new();
        cards.apply_batch(replica(1), [(row("u1", 42), Timestamp::new(0), Diff::ONE)]);
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&42));
    }

    #[mz_ore::test]
    fn max_across_replicas() {
        let cards = IndexCardinalities::new();
        cards.apply_batch(replica(1), [(row("u1", 10), Timestamp::new(0), Diff::ONE)]);
        cards.apply_batch(replica(2), [(row("u1", 99), Timestamp::new(0), Diff::ONE)]);
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&99));
    }

    /// A key changing twice in one batch must land on the value with the highest
    /// time, not on whichever insertion happens to be applied last.
    #[mz_ore::test]
    fn applies_in_time_order() {
        let cards = IndexCardinalities::new();
        cards.apply_batch(
            replica(1),
            [
                (row("u1", 10), Timestamp::new(2), Diff::MINUS_ONE),
                (row("u1", 12), Timestamp::new(2), Diff::ONE),
                (row("u1", 4), Timestamp::new(1), Diff::MINUS_ONE),
                (row("u1", 10), Timestamp::new(1), Diff::ONE),
            ],
        );
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&12));
    }

    #[mz_ore::test]
    fn evict_replica_drops_only_that_replica() {
        let cards = IndexCardinalities::new();
        cards.apply_batch(replica(1), [(row("u1", 10), Timestamp::new(0), Diff::ONE)]);
        cards.apply_batch(replica(2), [(row("u1", 7), Timestamp::new(0), Diff::ONE)]);
        cards.evict_replica(replica(1));
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&7));
        cards.evict_replica(replica(2));
        assert_eq!(cards.snapshot().get(&gid("u1")), None);
    }

    /// A dropped replica that keeps its entries would pin the maximum forever.
    #[mz_ore::test]
    fn eviction_releases_a_stale_maximum() {
        let cards = IndexCardinalities::new();
        cards.apply_batch(
            replica(1),
            [(row("u1", 10_000_000), Timestamp::new(0), Diff::ONE)],
        );
        cards.apply_batch(
            replica(2),
            [(row("u1", 1000), Timestamp::new(0), Diff::ONE)],
        );
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&10_000_000));
        cards.evict_replica(replica(1));
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&1000));
    }

    #[mz_ore::test]
    fn malformed_rows_are_dropped() {
        let cards = IndexCardinalities::new();
        let bad = Row::pack_slice(&[Datum::String("not-a-global-id"), Datum::Int64(1)]);
        cards.apply_batch(replica(1), [(bad, Timestamp::new(0), Diff::ONE)]);
        assert!(cards.snapshot().is_empty());
    }
}
