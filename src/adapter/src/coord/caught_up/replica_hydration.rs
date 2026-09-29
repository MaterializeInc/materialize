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

/// Per-replica hydration times of compute collections.
#[derive(Debug, Default)]
pub struct ReplicaHydrationTimes {
    /// Replicas without an entry have no hydration subscribe, for example
    /// because their introspection is disabled.
    replicas: BTreeMap<ReplicaId, ReplicaState>,
}

#[derive(Debug, Default)]
struct ReplicaState {
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
    /// A replica has not delivered its hydration times yet, so the answer can
    /// still change.
    Pending,
    /// Some collection has no reported hydration time on any replica.
    Unknown,
    /// All collections had hydrated at this time. Zero if there are none.
    Known(EpochMillis),
}

impl ReplicaHydrationTimes {
    /// Starts tracking `replica_id` from scratch.
    ///
    /// Call this whenever a hydration subscribe is installed on the replica.
    /// The subscribe's first batch then carries the full snapshot.
    pub fn reset(&mut self, replica_id: ReplicaId) {
        self.replicas.insert(replica_id, ReplicaState::default());
    }

    /// Stops tracking `replica_id`.
    pub fn remove(&mut self, replica_id: ReplicaId) {
        self.replicas.remove(&replica_id);
    }

    /// Applies a batch of `(collection, hydrated_at, diff)` subscribe updates
    /// for `replica_id`.
    ///
    /// Batches for untracked replicas are ignored.
    pub fn apply(
        &mut self,
        replica_id: ReplicaId,
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
        state.synced = true;
    }

    /// Returns when each of `collections` had hydrated on at least one of
    /// `replicas`, mirroring the caught-up rule that a collection needs to be
    /// hydrated on some replica.
    ///
    /// Replicas that are not tracked contribute nothing.
    pub fn hydrated_since(
        &self,
        replicas: &BTreeSet<ReplicaId>,
        collections: &BTreeSet<GlobalId>,
    ) -> HydratedSince {
        let tracked: Vec<_> = replicas
            .iter()
            .filter_map(|replica_id| self.replicas.get(replica_id))
            .collect();
        if tracked.iter().any(|state| !state.synced) {
            return HydratedSince::Pending;
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
            times.hydrated_since(&set(&[R1]), &set(&[A])),
            HydratedSince::Unknown
        );
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[])),
            HydratedSince::Known(0)
        );
    }

    #[mz_ore::test]
    fn pending_until_first_batch() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A])),
            HydratedSince::Pending
        );
        times.apply(R1, []);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A])),
            HydratedSince::Unknown
        );
    }

    #[mz_ore::test]
    fn latest_collection_and_earliest_replica() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1);
        times.reset(R2);
        times.apply(R1, [(A, 100, 1), (B, 300, 1)]);
        times.apply(R2, [(A, 50, 1), (B, 400, 1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1, R2]), &set(&[A, B])),
            HydratedSince::Known(300)
        );
        // B is missing on R2, but R1 has it.
        times.apply(R2, [(B, 400, -1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1, R2]), &set(&[A, B])),
            HydratedSince::Known(300)
        );
    }

    #[mz_ore::test]
    fn out_of_order_updates_consolidate() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1);
        times.apply(R1, [(A, 100, 1)]);
        times.apply(R1, [(A, 200, 1), (A, 100, -1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A])),
            HydratedSince::Known(200)
        );
    }

    #[mz_ore::test]
    fn reset_discards_previous_incarnation() {
        let mut times = ReplicaHydrationTimes::default();
        times.reset(R1);
        times.apply(R1, [(A, 100, 1)]);
        times.reset(R1);
        times.apply(R1, [(A, 500, 1)]);
        assert_eq!(
            times.hydrated_since(&set(&[R1]), &set(&[A])),
            HydratedSince::Known(500)
        );
    }
}
