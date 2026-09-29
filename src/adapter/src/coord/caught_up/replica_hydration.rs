// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An in-memory mirror of when compute collections hydrated on each replica.
//!
//! Replicas record hydration times in their `mz_compute_hydration_times_per_worker`
//! log. That log lives as long as the replica process, and a dataflow that
//! survives reconciliation keeps its original hydration time. So replicas can
//! tell a restarted environmentd when its collections hydrated, without
//! environmentd storing anything durably. A replica restart or a dataflow that
//! reconciliation replaces starts over, which is correct because hydration
//! then really happens again.
//!
//! A replica-targeted introspection subscribe feeds the mirror, see
//! `coord::introspection`.

use std::collections::{BTreeMap, BTreeSet};

use mz_controller_types::ReplicaId;
use mz_ore::now::EpochMillis;
use mz_repr::GlobalId;

/// How long a tracked replica may go without delivering its snapshot before
/// we stop waiting for it.
///
/// A snapshot normally arrives within seconds of installing the subscribe.
/// Waiting longer, for example for a subscribe that failed to install, only
/// delays the fallback to local observation, which is always safe.
const UNSYNCED_TIMEOUT_MS: EpochMillis = 60_000;

/// Per-replica hydration times of compute collections.
#[derive(Debug, Default)]
pub struct ReplicaHydrationTimes {
    /// Replicas without an entry have no hydration subscribe, for example
    /// because their introspection is disabled.
    replicas: BTreeMap<ReplicaId, ReplicaState>,
}

#[derive(Debug)]
struct ReplicaState {
    reset_at: EpochMillis,
    /// Whether the subscribe has delivered its snapshot.
    synced: bool,
    /// Consolidated subscribe output: hydration times of hydrated collections.
    ///
    /// We keep counts rather than a plain map because a batch can carry an
    /// insertion before the retraction it replaces. Between batches every
    /// collection has at most one row with count one.
    rows: BTreeMap<(GlobalId, EpochMillis), i64>,
}

/// When a set of collections had all hydrated, according to replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydratedSince {
    /// A tracked replica has not delivered its snapshot yet.
    Pending,
    /// Some collection has no reported hydration time on any replica.
    Unknown,
    /// All collections had hydrated at this time. Zero if there are none.
    Known(EpochMillis),
}

impl ReplicaHydrationTimes {
    /// Starts tracking `replica_id` from scratch at time `now`.
    ///
    /// Call this whenever a hydration subscribe is installed on the replica.
    pub fn reset(&mut self, replica_id: ReplicaId, now: EpochMillis) {
        let state = ReplicaState {
            reset_at: now,
            synced: false,
            rows: BTreeMap::new(),
        };
        self.replicas.insert(replica_id, state);
    }

    /// Stops tracking `replica_id`.
    pub fn remove(&mut self, replica_id: ReplicaId) {
        self.replicas.remove(&replica_id);
    }

    /// Applies a batch of `(collection, hydrated_at, diff)` subscribe updates
    /// for `replica_id`.
    ///
    /// `snapshot_complete` must be true once the updates received so far
    /// include the subscribe's full snapshot. Batches for untracked replicas
    /// are ignored.
    pub fn apply(
        &mut self,
        replica_id: ReplicaId,
        snapshot_complete: bool,
        updates: impl IntoIterator<Item = (GlobalId, EpochMillis, i64)>,
    ) {
        let Some(state) = self.replicas.get_mut(&replica_id) else {
            return;
        };
        for (id, hydrated_at, diff) in updates {
            let count = state.rows.entry((id, hydrated_at)).or_default();
            *count += diff;
            if *count == 0 {
                state.rows.remove(&(id, hydrated_at));
            }
        }
        state.synced |= snapshot_complete;
    }

    /// Returns the latest, over `collections`, of the earliest hydration time
    /// on any tracked replica in `replicas`.
    ///
    /// This mirrors the caught-up rule that a collection needs to be hydrated
    /// on some replica. Untracked replicas contribute nothing, and neither do
    /// replicas that have not synced within [`UNSYNCED_TIMEOUT_MS`] of their
    /// reset.
    pub fn hydrated_since(
        &self,
        replicas: &BTreeSet<ReplicaId>,
        collections: &BTreeSet<GlobalId>,
        now: EpochMillis,
    ) -> HydratedSince {
        let mut tracked = Vec::new();
        for state in replicas.iter().filter_map(|id| self.replicas.get(id)) {
            if state.synced {
                tracked.push(state);
            } else if now.saturating_sub(state.reset_at) < UNSYNCED_TIMEOUT_MS {
                return HydratedSince::Pending;
            }
        }

        let mut since = 0;
        for id in collections {
            let earliest = tracked
                .iter()
                .filter_map(|state| state.hydrated_at(*id))
                .min();
            match earliest {
                Some(hydrated_at) => since = since.max(hydrated_at),
                None => return HydratedSince::Unknown,
            }
        }
        HydratedSince::Known(since)
    }
}

impl ReplicaState {
    fn hydrated_at(&self, id: GlobalId) -> Option<EpochMillis> {
        self.rows
            .range((id, EpochMillis::MIN)..=(id, EpochMillis::MAX))
            .find(|(_, count)| **count > 0)
            .map(|((_, hydrated_at), _)| *hydrated_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const R1: ReplicaId = ReplicaId::User(1);
    const R2: ReplicaId = ReplicaId::User(2);
    const A: GlobalId = GlobalId::User(1);
    const B: GlobalId = GlobalId::User(2);

    fn set<T: Ord + Copy>(items: &[T]) -> BTreeSet<T> {
        items.iter().copied().collect()
    }

    #[mz_ore::test]
    fn untracked_replicas_give_no_evidence() {
        let times = ReplicaHydrationTimes::default();
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Unknown
        );
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[]), 0),
            HydratedSince::Known(0)
        );
    }

    #[mz_ore::test]
    fn apply_to_untracked_replica_is_ignored() {
        let mut times = ReplicaHydrationTimes::default();
        times.apply(R1, true, [(A, 100, 1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Unknown
        );
    }

    #[mz_ore::test]
    fn remove_stops_tracking() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1, 0);
        times.apply(R1, true, [(A, 100, 1)]);
        times.remove(R1);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Unknown
        );
        times.apply(R1, true, [(A, 100, 1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Unknown
        );
    }

    #[mz_ore::test]
    fn pending_until_snapshot_complete() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1, 0);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Pending
        );
        times.apply(R1, false, []);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Pending
        );
        times.apply(R1, false, [(A, 100, 1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Pending
        );
        times.apply(R1, true, []);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Known(100)
        );
    }

    #[mz_ore::test]
    fn unsynced_replica_is_untracked_after_timeout() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1, 1_000);
        times.reset(R2, 1_000);
        times.apply(R2, true, [(A, 100, 1)]);
        let before = 1_000 + UNSYNCED_TIMEOUT_MS - 1;
        assert_eq!(
            times.hydrated_since(&set(&[R1, R2]), &set(&[A]), before),
            HydratedSince::Pending
        );
        let after = 1_000 + UNSYNCED_TIMEOUT_MS;
        assert_eq!(
            times.hydrated_since(&set(&[R1, R2]), &set(&[A]), after),
            HydratedSince::Known(100)
        );
    }

    #[mz_ore::test]
    fn latest_collection_and_earliest_replica() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1, 0);
        times.reset(R2, 0);
        times.apply(R1, true, [(A, 100, 1), (B, 300, 1)]);
        times.apply(R2, true, [(A, 50, 1), (B, 400, 1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1, R2]), &set(&[A, B]), 0),
            HydratedSince::Known(300)
        );
        // B is missing on R2, but R1 has it.
        times.apply(R2, true, [(B, 400, -1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1, R2]), &set(&[A, B]), 0),
            HydratedSince::Known(300)
        );
    }

    #[mz_ore::test]
    fn out_of_order_updates_consolidate() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1, 0);
        times.apply(R1, true, [(A, 100, 1)]);
        times.apply(R1, true, [(A, 200, 1), (A, 100, -1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Known(200)
        );
    }

    #[mz_ore::test]
    fn reset_discards_previous_incarnation() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1, 0);
        times.apply(R1, true, [(A, 100, 1)]);
        times.reset(R1, 0);
        times.apply(R1, true, [(A, 500, 1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A]), 0),
            HydratedSince::Known(500)
        );
    }
}
