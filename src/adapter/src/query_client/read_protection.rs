// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adapter bundle API for catalog-backed client read protection.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_catalog::read_protection;
pub(crate) use mz_catalog::read_protection::{
    CLIENT_PROTECTION_HEARTBEAT_INTERVAL, ClientProtectionReclaimer, ClientReadProtectionClosed,
};
use mz_repr::{GlobalId, Timestamp};

use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_policy::ReadHolds;

/// Adapts per-collection grant tokens to adapter storage and compute bundles.
#[derive(Debug)]
pub(crate) struct ClientReadProtection {
    inner: read_protection::ClientReadProtection,
}

impl ClientReadProtection {
    pub(crate) fn new(incarnation: u64) -> Self {
        Self {
            inner: read_protection::ClientReadProtection::new(incarnation),
        }
    }

    pub(crate) fn incarnation(&self) -> u64 {
        self.inner.incarnation()
    }

    pub(crate) fn active_frontiers(&self) -> BTreeMap<GlobalId, Timestamp> {
        self.inner.active_frontiers()
    }

    pub(crate) fn publication_pending(&self) -> bool {
        self.inner.publication_pending()
    }

    pub(crate) fn granted_frontier(&self, id: GlobalId) -> Option<Timestamp> {
        self.inner.granted_frontier(id)
    }

    pub(crate) fn reusable_frontier(
        &self,
        id: GlobalId,
        read_ts: Option<Timestamp>,
    ) -> Option<Timestamp> {
        self.inner.reusable_frontier(id, read_ts)
    }

    /// Acquire the entire bundle atomically against acknowledged and pending grants.
    pub(crate) fn try_acquire(
        &self,
        bundle: &CollectionIdBundle,
        requested: &BTreeMap<GlobalId, Timestamp>,
        dependencies: &BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    ) -> Result<Option<ReadHolds>, ClientReadProtectionClosed> {
        let compute_ids = bundle.compute_ids.values().flatten().copied().collect();
        let Some(tokens) =
            self.inner
                .try_acquire(&bundle.storage_ids, &compute_ids, requested, dependencies)?
        else {
            return Ok(None);
        };
        // Each bundle entry owns an independent token, even if the same GlobalId
        // occurs in multiple instances or in both storage and compute. The base
        // tokens keep the entire acquisition protected while assembling the bundle.
        let mut holds = ReadHolds::new();
        for id in &bundle.storage_ids {
            holds.storage_holds.insert(*id, tokens[id].clone());
        }
        for (instance, ids) in &bundle.compute_ids {
            for id in ids {
                holds
                    .compute_holds
                    .insert((*instance, *id), tokens[id].clone());
            }
        }
        Ok(Some(holds))
    }

    pub(crate) fn prepare_publication(
        &self,
        extra: BTreeMap<GlobalId, Timestamp>,
    ) -> BTreeMap<GlobalId, Timestamp> {
        self.inner.prepare_publication(extra)
    }

    pub(crate) fn prepare_publication_if_needed(
        &self,
        elapsed: Duration,
    ) -> Option<BTreeMap<GlobalId, Timestamp>> {
        self.inner.prepare_publication_if_needed(elapsed)
    }

    pub(crate) fn finish_publication(&self, committed: bool) {
        self.inner.finish_publication(committed)
    }

    pub(crate) fn mark_closed(&self) {
        self.inner.mark_closed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use mz_compute_types::ComputeInstanceId;
    use timely::progress::Antichain;

    use super::*;

    fn requirements(values: &[(u64, u64)]) -> BTreeMap<GlobalId, Timestamp> {
        values
            .iter()
            .map(|(id, time)| (GlobalId::User(*id), Timestamp::from(*time)))
            .collect()
    }

    fn bundle() -> CollectionIdBundle {
        CollectionIdBundle {
            storage_ids: [GlobalId::User(1)].into(),
            compute_ids: [(ComputeInstanceId::User(1), [GlobalId::User(2)].into())].into(),
        }
    }

    fn dependencies() -> BTreeMap<GlobalId, BTreeSet<GlobalId>> {
        [(GlobalId::User(2), [GlobalId::User(1)].into())].into()
    }

    fn acquire(client: &ClientReadProtection, time: u64) -> ReadHolds {
        client
            .try_acquire(
                &bundle(),
                &requirements(&[(1, time), (2, time)]),
                &dependencies(),
            )
            .expect("client open")
            .expect("grant committed")
    }

    fn publish(client: &ClientReadProtection, extra: BTreeMap<GlobalId, Timestamp>) {
        client.prepare_publication(extra);
        client.finish_publication(true);
    }

    #[mz_ore::test]
    fn observing_active_minima_does_not_retain_or_publish_protection() {
        let client = ClientReadProtection::new(1);
        let granted = requirements(&[(1, 10), (2, 10)]);
        publish(&client, granted.clone());
        assert!(client.active_frontiers().is_empty());
        let holds = acquire(&client, 10);
        let observed = client.active_frontiers();
        drop(holds);
        assert!(client.active_frontiers().is_empty());
        assert_eq!(observed, granted);
        assert_eq!(
            client.granted_frontier(GlobalId::User(1)),
            Some(Timestamp::from(10))
        );
        assert!(!client.publication_pending());
    }

    #[mz_ore::test]
    fn coalesced_advancement_and_idle_renewal_preserve_live_tokens() {
        let client = ClientReadProtection::new(1);
        let cadence = Duration::from_secs(1);
        publish(&client, requirements(&[(1, 10), (2, 10)]));
        let mut window = acquire(&client, 10);
        let old_query = window.clone();
        window.downgrade(Timestamp::from(20));
        window.downgrade(Timestamp::from(30));
        // Local changes alone are not dirtiness: a live reader still needs 10.
        assert_eq!(client.prepare_publication_if_needed(cadence), None);
        assert!(!client.publication_pending());
        drop(old_query);
        let advanced = requirements(&[(1, 30), (2, 30)]);
        assert_eq!(
            client.prepare_publication_if_needed(cadence),
            Some(advanced.clone())
        );
        assert!(client.publication_pending());
        assert!(
            client
                .try_acquire(
                    &bundle(),
                    &requirements(&[(1, 10), (2, 10)]),
                    &dependencies()
                )
                .expect("open")
                .is_none()
        );
        drop(acquire(&client, 30));
        client.finish_publication(false);
        assert_eq!(client.granted_frontier(GlobalId::User(1)), Some(10.into()));
        assert_eq!(
            client.prepare_publication_if_needed(cadence),
            Some(advanced.clone()),
            "failed publication must be retried without waiting for heartbeat"
        );
        // Changes during the commit remain eligible for the next cadence.
        window.downgrade(Timestamp::from(40));
        client.finish_publication(true);
        assert_eq!(client.granted_frontier(GlobalId::User(1)), Some(30.into()));
        let current = requirements(&[(1, 40), (2, 40)]);
        assert_eq!(
            client.prepare_publication_if_needed(cadence),
            Some(current.clone())
        );
        client.finish_publication(true);
        assert_eq!(
            client.prepare_publication_if_needed(CLIENT_PROTECTION_HEARTBEAT_INTERVAL - cadence),
            None
        );
        assert_eq!(
            client.prepare_publication_if_needed(CLIENT_PROTECTION_HEARTBEAT_INTERVAL),
            Some(current)
        );
        client.finish_publication(true);
        drop(window);
        assert_eq!(
            client.prepare_publication_if_needed(cadence),
            Some(BTreeMap::new())
        );
        client.finish_publication(true);
        assert_eq!(client.prepare_publication_if_needed(cadence), None);
        assert_eq!(
            client.prepare_publication_if_needed(CLIENT_PROTECTION_HEARTBEAT_INTERVAL),
            Some(BTreeMap::new())
        );
        client.finish_publication(true);
    }

    #[mz_ore::test]
    fn historical_grant_expansion_requires_commit() {
        let client = ClientReadProtection::new(1);
        publish(&client, requirements(&[(1, 100), (2, 100)]));
        let ordinary = acquire(&client, 120);
        for id in bundle().iter() {
            for target in [None, Some(Timestamp::from(100)), Some(Timestamp::from(120))] {
                assert_eq!(
                    client.reusable_frontier(id, target),
                    Some(Timestamp::from(100))
                );
            }
            assert_eq!(
                client.reusable_frontier(id, Some(Timestamp::from(50))),
                None
            );
        }

        let historical = requirements(&[(1, 50), (2, 50)]);
        let try_historical = || {
            client
                .try_acquire(&bundle(), &historical, &dependencies())
                .expect("client remains open")
        };
        assert!(try_historical().is_none());
        client.prepare_publication(historical.clone());
        assert!(try_historical().is_none());
        // An expansion must not interrupt reads already covered by both grants.
        drop(acquire(&client, 120));
        client.finish_publication(false);
        assert!(try_historical().is_none());
        publish(&client, historical.clone());
        let held = try_historical().expect("committed historical coverage");
        assert_eq!(
            held.least_valid_read(),
            Antichain::from_elem(Timestamp::from(50))
        );
        assert_eq!(client.prepare_publication(BTreeMap::new()), historical);
        client.finish_publication(true);
        drop((ordinary, held));
    }

    #[mz_ore::test]
    fn compute_only_subset_retains_leaves_after_catalog_drop_and_closure() {
        let client = ClientReadProtection::new(1);
        let deps = [(
            GlobalId::User(2),
            [GlobalId::User(1), GlobalId::User(3)].into(),
        )]
        .into();
        publish(&client, requirements(&[(1, 10), (2, 10), (3, 10)]));
        let holds = client
            .try_acquire(&bundle(), &requirements(&[(1, 10), (2, 10)]), &deps)
            .expect("open")
            .expect("covered");
        let mut index_bundle = bundle();
        index_bundle.storage_ids.clear();
        let mut subset = holds.subset(&index_bundle);
        assert!(subset.storage_holds.is_empty());
        drop(holds);
        // Neither catalog knowledge nor direct leaf tokens participate in
        // subsequent accounting, including once the client is closed.
        drop(deps);
        client.mark_closed();
        assert_eq!(
            client.prepare_publication(BTreeMap::new()),
            requirements(&[(1, 10), (2, 10), (3, 10)])
        );
        client.finish_publication(true);
        let clone = subset.clone();
        subset.downgrade(Timestamp::from(20));
        for (id, hold) in clone.compute_holds {
            subset
                .compute_holds
                .get_mut(&id)
                .expect("index retained")
                .merge_assign(hold);
        }
        assert_eq!(
            client.prepare_publication(BTreeMap::new()),
            requirements(&[(1, 10), (2, 10), (3, 10)])
        );
        client.finish_publication(true);
        subset.downgrade(Timestamp::from(30));
        assert_eq!(
            client.prepare_publication(BTreeMap::new()),
            requirements(&[(1, 30), (2, 30), (3, 30)])
        );
        client.finish_publication(true);
        drop(subset);
        assert!(client.prepare_publication(BTreeMap::new()).is_empty());
        client.finish_publication(true);
    }

    #[mz_ore::test]
    fn direct_and_derived_holds_have_independent_timestamps() {
        let client = ClientReadProtection::new(1);
        publish(&client, requirements(&[(1, 5), (2, 10)]));
        let mut holds = client
            .try_acquire(
                &bundle(),
                &requirements(&[(1, 5), (2, 10)]),
                &dependencies(),
            )
            .expect("open")
            .expect("covered");
        assert_eq!(
            client.prepare_publication(BTreeMap::new()),
            requirements(&[(1, 5), (2, 10)])
        );
        client.finish_publication(true);
        holds.remove_storage_collection(GlobalId::User(1));
        assert_eq!(
            client.prepare_publication(BTreeMap::new()),
            requirements(&[(1, 10), (2, 10)])
        );
        client.finish_publication(true);
        drop(holds);
        assert!(client.prepare_publication(BTreeMap::new()).is_empty());
        client.finish_publication(true);
    }

    #[mz_ore::test]
    fn leaf_coverage_and_publication_barrier() {
        let client = ClientReadProtection::new(1);
        let mut index_bundle = bundle();
        index_bundle.storage_ids.clear();
        let request = requirements(&[(2, 10)]);
        let try_index = || {
            client
                .try_acquire(&index_bundle, &request, &dependencies())
                .expect("open")
        };
        publish(&client, request.clone());
        assert!(try_index().is_none());
        client.prepare_publication(requirements(&[(1, 10), (2, 10)]));
        assert!(try_index().is_none());
        client.finish_publication(true);
        drop(try_index().expect("leaf grant committed"));
        // The index grant itself stays unchanged. Both leaf advancement and
        // removal must bar acquisition at the old index timestamp.
        for pending in [requirements(&[(1, 20), (2, 10)]), request.clone()] {
            client.prepare_publication(pending);
            assert!(try_index().is_none());
            client.finish_publication(false);
            drop(try_index().expect("failed publication preserves grant"));
        }
        publish(&client, requirements(&[(1, 20), (2, 10)]));
        assert!(try_index().is_none());
    }

    #[mz_ore::test]
    fn dependency_registration_is_immutable() {
        let client = ClientReadProtection::new(1);
        publish(&client, requirements(&[(1, 10), (2, 10), (3, 10)]));
        let holds = acquire(&client, 10);
        let request = requirements(&[(1, 10), (2, 10)]);
        for deps in [
            BTreeMap::new(),
            [(GlobalId::User(2), BTreeSet::new())].into(),
            [(GlobalId::User(2), [GlobalId::User(3)].into())].into(),
            [(
                GlobalId::User(2),
                [GlobalId::User(1), GlobalId::User(3)].into(),
            )]
            .into(),
        ] {
            assert!(
                client
                    .try_acquire(&bundle(), &request, &deps)
                    .expect("open")
                    .is_none()
            );
        }
        drop(holds);
        // A committed root retains its definition even without live tokens.
        assert!(
            client
                .try_acquire(
                    &bundle(),
                    &request,
                    &[(GlobalId::User(2), BTreeSet::new())].into()
                )
                .expect("open")
                .is_none()
        );
        assert!(client.prepare_publication(BTreeMap::new()).is_empty());
        client.finish_publication(true);
    }

    #[mz_ore::test]
    fn durable_coverage_is_atomic_for_expanded_scope() {
        let client = ClientReadProtection::new(7);
        assert_eq!(client.incarnation(), 7);
        let request = requirements(&[(1, 10), (2, 10)]);
        assert!(
            client
                .try_acquire(&bundle(), &request, &dependencies())
                .expect("open")
                .is_none()
        );
        client.prepare_publication(request.clone());
        assert!(
            client
                .try_acquire(&bundle(), &request, &dependencies())
                .expect("open")
                .is_none()
        );
        client.finish_publication(false);
        publish(&client, requirements(&[(1, 10)]));
        assert!(
            client
                .try_acquire(&bundle(), &request, &dependencies())
                .expect("open")
                .is_none()
        );
        // A failed acquisition must not pin even the covered storage leaf.
        assert!(client.prepare_publication(BTreeMap::new()).is_empty());
        client.finish_publication(false);
        publish(&client, request.clone());
        assert!(
            client
                .try_acquire(&bundle(), &requirements(&[(1, 10)]), &dependencies())
                .expect("open")
                .is_none()
        );
        assert!(
            client
                .try_acquire(
                    &bundle(),
                    &requirements(&[(1, 9), (2, 10)]),
                    &dependencies()
                )
                .expect("open")
                .is_none()
        );
        let holds = acquire(&client, 10);
        assert_eq!(
            holds.storage_ids().collect::<Vec<_>>(),
            vec![GlobalId::User(1)]
        );
        assert_eq!(
            holds.compute_ids().collect::<Vec<_>>(),
            vec![(ComputeInstanceId::User(1), GlobalId::User(2))]
        );
    }

    #[mz_ore::test]
    fn clone_downgrade_release_minima() {
        let client = ClientReadProtection::new(1);
        publish(&client, requirements(&[(1, 10), (2, 10)]));
        let mut holds = acquire(&client, 10);
        let mut clone = holds.clone();
        holds.downgrade(Timestamp::from(20));
        assert_eq!(
            client.prepare_publication(requirements(&[(1, 5)])),
            requirements(&[(1, 5), (2, 10)])
        );
        client.finish_publication(true);
        clone
            .storage_holds
            .get_mut(&GlobalId::User(1))
            .expect("leaf")
            .release();
        assert_eq!(
            client.prepare_publication(BTreeMap::new()),
            requirements(&[(1, 10), (2, 10)])
        );
        client.finish_publication(true);
        drop(clone);
        assert_eq!(
            client.prepare_publication(BTreeMap::new()),
            requirements(&[(1, 20), (2, 20)])
        );
        client.finish_publication(true);
        drop(holds);
        assert!(client.prepare_publication(BTreeMap::new()).is_empty());
        client.finish_publication(true);
    }

    #[mz_ore::test]
    fn publication_barrier_and_failure() {
        let client = Arc::new(ClientReadProtection::new(1));
        publish(&client, requirements(&[(1, 10), (2, 10)]));
        let request = requirements(&[(1, 10), (2, 10)]);
        assert!(!client.publication_pending());
        // Exercise both lock orderings: an acquisition before prepare is in its
        // snapshot, while one on another thread after prepare sees the barrier.
        let holds = acquire(&client, 10);
        assert_eq!(client.prepare_publication(BTreeMap::new()), request);
        assert!(client.publication_pending());
        let clone = holds.clone();
        drop(holds);
        client.finish_publication(true);
        drop(clone);
        assert!(!client.publication_pending());
        client.prepare_publication(requirements(&[(1, 20), (2, 20)]));
        let other = Arc::clone(&client);
        std::thread::spawn(move || {
            assert!(
                other
                    .try_acquire(&bundle(), &request, &dependencies())
                    .expect("open")
                    .is_none()
            );
            drop(acquire(&other, 20));
        })
        .join()
        .expect("acquisition thread");
        client.finish_publication(false);
        assert!(!client.publication_pending());
        drop(acquire(&client, 10));
        publish(&client, requirements(&[(1, 20), (2, 20)]));
        assert!(
            client
                .try_acquire(
                    &bundle(),
                    &requirements(&[(1, 10), (2, 10)]),
                    &dependencies()
                )
                .expect("open")
                .is_none()
        );
        drop(acquire(&client, 20));
        client.prepare_publication(BTreeMap::new());
        assert!(
            client
                .try_acquire(
                    &bundle(),
                    &requirements(&[(1, 20), (2, 20)]),
                    &dependencies()
                )
                .expect("open")
                .is_none()
        );
        client.finish_publication(true);
        assert!(
            client
                .try_acquire(
                    &bundle(),
                    &requirements(&[(1, 20), (2, 20)]),
                    &dependencies()
                )
                .expect("open")
                .is_none()
        );
    }

    #[mz_ore::test]
    fn closure_and_client_isolation() {
        let client = ClientReadProtection::new(1);
        let other = ClientReadProtection::new(2);
        publish(&client, requirements(&[(1, 10), (2, 10)]));
        publish(&other, requirements(&[(1, 10), (2, 10)]));
        let holds = acquire(&client, 10);
        client.mark_closed();
        assert!(
            client
                .try_acquire(
                    &bundle(),
                    &requirements(&[(1, 10), (2, 10)]),
                    &dependencies()
                )
                .is_err()
        );
        let mut clone = holds.clone();
        drop(holds);
        clone.downgrade(Timestamp::from(20));
        assert_eq!(
            client.prepare_publication(BTreeMap::new()),
            requirements(&[(1, 20), (2, 20)])
        );
        client.finish_publication(true);
        assert!(other.prepare_publication(BTreeMap::new()).is_empty());
        other.finish_publication(false);
        drop(acquire(&other, 10));
        drop(client);
        drop(clone.clone());
        drop(clone);
    }

    #[mz_ore::test]
    fn reclaimer_renewal_absence_restart_and_isolation() {
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        let mut reclaimer = ClientProtectionReclaimer::default();
        assert!(reclaimer.observe([(1, 8), (2, 9)], start).is_empty());
        assert!(reclaimer.observe([(1, 8), (2, 10)], at(299)).is_empty());
        assert_eq!(reclaimer.observe([(1, 8), (2, 10)], at(300)), vec![(1, 8)]);
        assert!(reclaimer.observe([(2, 10)], at(301)).is_empty());
        assert_eq!(reclaimer.observe([(1, 8), (2, 10)], at(599)), vec![(2, 10)]);
        assert_eq!(reclaimer.observe([(1, 8), (2, 11)], at(899)), vec![(1, 8)]);
        let mut restarted = ClientProtectionReclaimer::default();
        assert!(restarted.observe([(1, 8)], at(899)).is_empty());
        assert!(restarted.observe([(1, 8)], at(1198)).is_empty());
        assert_eq!(restarted.observe([(1, 8)], at(1199)), vec![(1, 8)]);
    }
}
