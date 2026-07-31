// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Statistics about index arrangements, maintained from replica introspection.
//!
//! Compute reports what each index arrangement holds. Those reports reach the
//! coordinator through a per-replica introspection subscribe and land here, where
//! the optimizer can read them without awaiting anything.
//!
//! The authoritative state is per replica, because replicas disagree: one may still
//! be hydrating while another is caught up. Readers see a snapshot that takes the
//! maximum across replicas, since the dominant error is a hydrating replica reporting
//! a partial count, and under-estimating is the dangerous direction for both
//! consumers. A join orderer that under-estimates picks a bad order, and a memory
//! bound that under-estimates understates the memory a plan needs.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use mz_cluster_client::ReplicaId;
use mz_repr::{Diff, GlobalId, Row, Timestamp};

/// What one index's arrangement holds, as reported by a replica.
///
/// Every field is summed across the replica's workers and over-counts rather than
/// measuring exactly, so each is usable as an upper bound and none should be
/// presented as a measurement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArrangementStats {
    /// Updates the arrangement holds.
    ///
    /// Counts retractions, so it bounds distinct rows and it bounds updates. It does
    /// *not* bound multiset rows, because persist consolidates duplicates into a
    /// single `(row, diff = N)` update.
    pub records: u64,
    /// Distinct keys the arrangement holds.
    ///
    /// Over-counts, because a spine partitions by time and one key can occupy a slot
    /// in several live batches at once. Under retractions a key can be logically
    /// absent while still holding a slot in two batches, so the ratio is unbounded in
    /// general until compaction advances past it. Sound only as an upper bound.
    pub distinct_keys: u64,
}

/// Arrangement statistics keyed by index [`GlobalId`].
///
/// Writes come from the coordinator's main loop. Reads come from session threads
/// running the optimizer, and take a cheap `Arc` clone of a precomputed snapshot
/// rather than walking the per-replica state.
#[derive(Debug)]
pub struct IndexArrangementStats {
    inner: RwLock<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// What each replica last reported for each index.
    per_replica: BTreeMap<(ReplicaId, GlobalId), ArrangementStats>,
    /// Maximum across replicas, rebuilt whenever `per_replica` changes.
    snapshot: Arc<BTreeMap<GlobalId, ArrangementStats>>,
}

impl Default for IndexArrangementStats {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexArrangementStats {
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
    /// carries no ordering between its collections, and one index's statistics can
    /// change more than once within a single batch. Applying every retraction before
    /// every insertion would leave the final value dependent on iteration order
    /// rather than on time.
    pub fn apply_batch<I>(&self, replica_id: ReplicaId, updates: I)
    where
        I: IntoIterator<Item = (Row, Timestamp, Diff)>,
    {
        let mut updates: Vec<_> = updates
            .into_iter()
            .filter_map(|(row, ts, diff)| {
                let (index_id, stats) = decode_row(&row)?;
                Some((ts, index_id, stats, diff))
            })
            .collect();
        if updates.is_empty() {
            return;
        }
        updates.sort_by_key(|(ts, ..)| *ts);

        let mut inner = self.inner.write().expect("lock poisoned");
        for (_ts, index_id, stats, diff) in updates {
            let key = (replica_id, index_id);
            if diff.is_positive() {
                inner.per_replica.insert(key, stats);
            } else {
                // Only retract the value we currently hold. A retraction of a
                // superseded value would otherwise drop a newer insertion that
                // shares the batch.
                if inner.per_replica.get(&key) == Some(&stats) {
                    inner.per_replica.remove(&key);
                }
            }
        }
        inner.rebuild_snapshot();
    }

    /// Drops everything `replica_id` reported.
    ///
    /// Both retraction paths for an introspection subscribe must call this, the
    /// reinstall path and the drop path. Leaking a dropped replica's entries pins the
    /// estimate to that replica's last report, because reads take a maximum.
    pub fn evict_replica(&self, replica_id: ReplicaId) {
        let mut inner = self.inner.write().expect("lock poisoned");
        let before = inner.per_replica.len();
        inner.per_replica.retain(|(r, _), _| *r != replica_id);
        if inner.per_replica.len() != before {
            inner.rebuild_snapshot();
        }
    }

    /// Returns the current maximum-across-replicas view, keyed by index ID.
    pub fn snapshot(&self) -> Arc<BTreeMap<GlobalId, ArrangementStats>> {
        Arc::clone(&self.inner.read().expect("lock poisoned").snapshot)
    }
}

impl Inner {
    fn rebuild_snapshot(&mut self) {
        let mut snapshot: BTreeMap<GlobalId, ArrangementStats> = BTreeMap::new();
        for ((_replica, index_id), stats) in &self.per_replica {
            // Each field takes its own maximum, so the result can pair one replica's
            // record count with another's key count. That is deliberate: both are
            // consumed independently as upper bounds, and a per-field maximum is
            // tighter than picking whichever single replica leads on one field.
            let entry = snapshot.entry(*index_id).or_default();
            entry.records = entry.records.max(stats.records);
            entry.distinct_keys = entry.distinct_keys.max(stats.distinct_keys);
        }
        self.snapshot = Arc::new(snapshot);
    }
}

/// Decodes an `(index_id, records, distinct_keys)` row as produced by the arrangement
/// statistics subscribe. Malformed rows are dropped rather than panicking, because this
/// runs on the coordinator's main loop and a replica should not be able to wedge it.
fn decode_row(row: &Row) -> Option<(GlobalId, ArrangementStats)> {
    let mut datums = row.iter();
    let index_id: GlobalId = datums.next()?.unwrap_str().parse().ok()?;
    let records = u64::try_from(datums.next()?.unwrap_int64()).ok()?;
    let distinct_keys = u64::try_from(datums.next()?.unwrap_int64()).ok()?;
    Some((
        index_id,
        ArrangementStats {
            records,
            distinct_keys,
        },
    ))
}

#[cfg(test)]
mod tests {
    use mz_repr::Datum;

    use super::*;

    fn row(index_id: &str, records: i64, distinct_keys: i64) -> Row {
        Row::pack_slice(&[
            Datum::String(index_id),
            Datum::Int64(records),
            Datum::Int64(distinct_keys),
        ])
    }

    fn replica(id: u64) -> ReplicaId {
        ReplicaId::User(id)
    }

    fn gid(id: &str) -> GlobalId {
        id.parse().expect("valid GlobalId")
    }

    fn stats(records: u64, distinct_keys: u64) -> ArrangementStats {
        ArrangementStats {
            records,
            distinct_keys,
        }
    }

    #[mz_ore::test]
    fn insert_and_read() {
        let cards = IndexArrangementStats::new();
        cards.apply_batch(
            replica(1),
            [(row("u1", 42, 7), Timestamp::new(0), Diff::ONE)],
        );
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&stats(42, 7)));
    }

    #[mz_ore::test]
    fn max_across_replicas() {
        let cards = IndexArrangementStats::new();
        cards.apply_batch(
            replica(1),
            [(row("u1", 10, 5), Timestamp::new(0), Diff::ONE)],
        );
        cards.apply_batch(
            replica(2),
            [(row("u1", 99, 9), Timestamp::new(0), Diff::ONE)],
        );
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&stats(99, 9)));
    }

    /// Each field maximises independently, so a replica leading on records does not
    /// drag its lower key count along.
    #[mz_ore::test]
    fn fields_maximise_independently() {
        let cards = IndexArrangementStats::new();
        cards.apply_batch(
            replica(1),
            [(row("u1", 100, 3), Timestamp::new(0), Diff::ONE)],
        );
        cards.apply_batch(
            replica(2),
            [(row("u1", 20, 40), Timestamp::new(0), Diff::ONE)],
        );
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&stats(100, 40)));
    }

    /// A key changing twice in one batch must land on the value with the highest
    /// time, not on whichever insertion happens to be applied last.
    #[mz_ore::test]
    fn applies_in_time_order() {
        let cards = IndexArrangementStats::new();
        cards.apply_batch(
            replica(1),
            [
                (row("u1", 10, 2), Timestamp::new(2), Diff::MINUS_ONE),
                (row("u1", 12, 3), Timestamp::new(2), Diff::ONE),
                (row("u1", 4, 1), Timestamp::new(1), Diff::MINUS_ONE),
                (row("u1", 10, 2), Timestamp::new(1), Diff::ONE),
            ],
        );
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&stats(12, 3)));
    }

    #[mz_ore::test]
    fn evict_replica_drops_only_that_replica() {
        let cards = IndexArrangementStats::new();
        cards.apply_batch(
            replica(1),
            [(row("u1", 10, 4), Timestamp::new(0), Diff::ONE)],
        );
        cards.apply_batch(
            replica(2),
            [(row("u1", 7, 2), Timestamp::new(0), Diff::ONE)],
        );
        cards.evict_replica(replica(1));
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&stats(7, 2)));
        cards.evict_replica(replica(2));
        assert_eq!(cards.snapshot().get(&gid("u1")), None);
    }

    /// A dropped replica that keeps its entries would pin the maximum forever.
    #[mz_ore::test]
    fn eviction_releases_a_stale_maximum() {
        let cards = IndexArrangementStats::new();
        cards.apply_batch(
            replica(1),
            [(row("u1", 10_000_000, 500), Timestamp::new(0), Diff::ONE)],
        );
        cards.apply_batch(
            replica(2),
            [(row("u1", 1000, 10), Timestamp::new(0), Diff::ONE)],
        );
        assert_eq!(
            cards.snapshot().get(&gid("u1")),
            Some(&stats(10_000_000, 500))
        );
        cards.evict_replica(replica(1));
        assert_eq!(cards.snapshot().get(&gid("u1")), Some(&stats(1000, 10)));
    }

    #[mz_ore::test]
    fn malformed_rows_are_dropped() {
        let cards = IndexArrangementStats::new();
        let bad = Row::pack_slice(&[
            Datum::String("not-a-global-id"),
            Datum::Int64(1),
            Datum::Int64(1),
        ]);
        cards.apply_batch(replica(1), [(bad, Timestamp::new(0), Diff::ONE)]);
        assert!(cards.snapshot().is_empty());
    }

    /// A row missing the distinct-keys column is malformed, not a zero. Treating a
    /// short row as zero keys would hand a consumer a bound of zero, which is the
    /// unsound direction.
    #[mz_ore::test]
    fn short_rows_are_dropped() {
        let cards = IndexArrangementStats::new();
        let short = Row::pack_slice(&[Datum::String("u1"), Datum::Int64(5)]);
        cards.apply_batch(replica(1), [(short, Timestamp::new(0), Diff::ONE)]);
        assert!(cards.snapshot().is_empty());
    }
}
