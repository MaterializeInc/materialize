// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Local tokens backed exclusively by acknowledged durable client grants.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::read_holds::{ChangeTx, ReadHold};
use timely::progress::Antichain;
use timely::progress::frontier::MutableAntichain;

use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_policy::ReadHolds;

/// Publish heartbeat and requirements together at this cadence.
pub(crate) const CLIENT_PROTECTION_PUBLICATION_INTERVAL: Duration = Duration::from_secs(60);
/// Observe an unchanged heartbeat locally for this long before attempting closure.
pub(crate) const CLIENT_PROTECTION_UNCHANGED_GRACE: Duration = Duration::from_secs(300);

#[derive(Debug, thiserror::Error)]
#[error("query client read protection is closed")]
pub(crate) struct ClientReadProtectionClosed;

/// The coordinator serializes publications for this incarnation. Tokens only
/// update local counters, never storage or compute controllers.
#[derive(Debug)]
pub(crate) struct ClientReadProtection {
    incarnation: u64,
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    committed: BTreeMap<GlobalId, Timestamp>,
    active: BTreeMap<GlobalId, MutableAntichain<Timestamp>>,
    // Catalog-derived logical leaves, never durable bindings. A token must
    // retain this knowledge even when its index disappears from the catalog.
    dependencies: BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    pending: Option<BTreeMap<GlobalId, Timestamp>>,
    closed: bool,
}

impl State {
    fn update(&mut self, id: GlobalId, changes: &[(Timestamp, i64)]) {
        for id in
            std::iter::once(id).chain(self.dependencies.get(&id).into_iter().flatten().copied())
        {
            let active = self.active.entry(id).or_default();
            active.update_iter(changes.iter().copied());
            if active.frontier().is_empty() {
                self.active.remove(&id);
            }
        }
    }

    fn prune_dependencies(&mut self) {
        self.dependencies.retain(|id, _| {
            self.active.contains_key(id)
                || self.committed.contains_key(id)
                || self.pending.as_ref().is_some_and(|p| p.contains_key(id))
        });
    }
}

impl ClientReadProtection {
    pub(crate) fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    pub(crate) fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Whether the writer has a publication in flight. Committed-update
    /// callbacks must defer further publication until that attempt finishes.
    pub(crate) fn publication_pending(&self) -> bool {
        self.state
            .lock()
            .expect("read protection mutex poisoned")
            .pending
            .is_some()
    }

    /// Returns a cached grant floor, not a new read hold. Acquisition must still
    /// validate against the current and pending publications under the mutex.
    pub(crate) fn granted_frontier(&self, id: GlobalId) -> Option<Timestamp> {
        let state = self.state.lock().expect("read protection mutex poisoned");
        if state.closed {
            None
        } else {
            state.committed.get(&id).copied()
        }
    }

    /// A grant can supply the acquisition floor only if it covers the desired
    /// timestamp. A later grant says nothing about older retained history, which
    /// the caller must observe and protect through a committed publication.
    /// With no timestamp preference, reuse the established window.
    pub(crate) fn reusable_frontier(
        &self,
        id: GlobalId,
        read_ts: Option<Timestamp>,
    ) -> Option<Timestamp> {
        self.granted_frontier(id)
            .filter(|grant| read_ts.is_none_or(|time| *grant <= time))
    }

    /// Acquire the bundle at the requested finite frontiers.
    /// Every bundle ID must have a requested frontier. Entries outside the
    /// bundle are ignored. Every compute index must have an explicit, immutable
    /// catalog-derived set of persisted logical inputs in `dependencies`.
    /// Each index token protects these leaves at its own requested timestamp,
    /// independently of any direct storage tokens in the bundle.
    /// Returns `None` without installing holds if coverage or dependencies are
    /// missing, or a dependency definition disagrees with a retained definition.
    /// The caller must publish expanded leaf requirements before acquisition.
    pub(crate) fn try_acquire(
        &self,
        bundle: &CollectionIdBundle,
        requested: &BTreeMap<GlobalId, Timestamp>,
        dependencies: &BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    ) -> Result<Option<ReadHolds>, ClientReadProtectionClosed> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        if state.closed {
            return Err(ClientReadProtectionClosed);
        }
        let covered = |grants: &BTreeMap<GlobalId, Timestamp>, id, time| {
            grants.get(&id).is_some_and(|grant| grant <= time)
        };
        for id in bundle.compute_ids.values().flatten() {
            let Some(leaves) = dependencies.get(id) else {
                return Ok(None);
            };
            if state.dependencies.get(id).is_some_and(|old| old != leaves) {
                return Ok(None);
            }
            let Some(time) = requested.get(id) else {
                return Ok(None);
            };
            for leaf in leaves {
                if !covered(&state.committed, *leaf, time)
                    || state
                        .pending
                        .as_ref()
                        .is_some_and(|pending| !covered(pending, *leaf, time))
                {
                    return Ok(None);
                }
            }
        }
        for id in bundle.iter() {
            let Some(time) = requested.get(&id) else {
                return Ok(None);
            };
            if !covered(&state.committed, id, time)
                || state
                    .pending
                    .as_ref()
                    .is_some_and(|pending| !covered(pending, id, time))
            {
                return Ok(None);
            }
        }

        // Keep the counters alive as long as any token exists, including after
        // closure or owner drop. Cloning an existing token must not hang up.
        let local = Arc::clone(&self.state);
        let change_tx: ChangeTx = Arc::new(move |id, mut changes| {
            let mut state = local.lock().expect("read protection mutex poisoned");
            state.update(id, &changes.drain().collect::<Vec<_>>());
            state.prune_dependencies();
            Ok(())
        });
        for id in bundle.compute_ids.values().flatten() {
            state
                .dependencies
                .entry(*id)
                .or_insert_with(|| dependencies[id].clone());
        }
        let mut issue = |id| {
            let time = requested[&id];
            state.update(id, &[(time, 1)]);
            ReadHold::new(id, Antichain::from_elem(time), Arc::clone(&change_tx))
        };
        // Validation and installation share the publication mutex. No callback
        // runs during construction, because ReadHold::new does not issue +1.
        let mut holds = ReadHolds::new();
        for id in &bundle.storage_ids {
            holds.storage_holds.insert(*id, issue(*id));
        }
        for (instance, ids) in &bundle.compute_ids {
            for id in ids {
                holds.compute_holds.insert((*instance, *id), issue(*id));
            }
        }
        Ok(Some(holds))
    }

    /// Snapshot active minima plus extra requirements for a durable publication.
    /// The returned map replaces all requirements for this incarnation.
    ///
    /// Until `finish_publication`, acquisitions must satisfy both the committed
    /// and pending maps. This prevents borrowing a grant that the writer is
    /// advancing or removing. Clone, downgrade, and drop cannot lower the
    /// snapshot's minima, so they may continue throughout publication.
    ///
    /// Panics if another publication is pending. The coordinator owns writer
    /// serialization and must finish this publication before starting another.
    pub(crate) fn prepare_publication(
        &self,
        mut extra: BTreeMap<GlobalId, Timestamp>,
    ) -> BTreeMap<GlobalId, Timestamp> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        assert!(state.pending.is_none(), "publication already pending");
        for (id, active) in &state.active {
            if let Some(time) = active.frontier().iter().next() {
                extra
                    .entry(*id)
                    .and_modify(|extra| *extra = (*extra).min(*time))
                    .or_insert(*time);
            }
        }
        state.pending = Some(extra.clone());
        extra
    }

    /// Acknowledge durable commit, or discard a definitively failed publication.
    /// An unknown commit outcome must not be reported as failure. Keep the
    /// barrier until the outcome is resolved, or close the client.
    /// Panics if no publication is pending.
    pub(crate) fn finish_publication(&self, committed: bool) {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        let pending = state.pending.take().expect("publication must be pending");
        if committed {
            state.committed = pending;
        }
        state.prune_dependencies();
    }

    /// Stop new acquisitions without invalidating token bookkeeping. Existing
    /// tokens are not proof of liveness. Closure is enforced at the read boundary.
    pub(crate) fn mark_closed(&self) {
        self.state
            .lock()
            .expect("read protection mutex poisoned")
            .closed = true;
    }
}

/// Process-local unchanged-heartbeat observations. Restarting this observer
/// starts a full grace window, regardless of the persisted heartbeat value.
#[derive(Debug, Default)]
pub(crate) struct ClientProtectionReclaimer {
    observations: BTreeMap<u64, (u64, Instant)>,
}

impl ClientProtectionReclaimer {
    /// Observe a complete snapshot of open incarnations and their heartbeats.
    /// Each ID appears once. Returned candidates must be closed by a durable CAS
    /// against the returned heartbeat, not by trusting this observation alone.
    pub(crate) fn observe(
        &mut self,
        clients: impl IntoIterator<Item = (u64, u64)>,
        now: Instant,
    ) -> Vec<(u64, u64)> {
        let mut present = BTreeSet::new();
        let mut candidates = Vec::new();
        for (id, heartbeat) in clients {
            present.insert(id);
            let observation = self.observations.entry(id).or_insert((heartbeat, now));
            if observation.0 != heartbeat {
                *observation = (heartbeat, now);
            }
            if now.saturating_duration_since(observation.1) >= CLIENT_PROTECTION_UNCHANGED_GRACE {
                candidates.push((id, heartbeat));
            }
        }
        self.observations.retain(|id, _| present.contains(id));
        candidates
    }
}

#[cfg(test)]
mod tests {
    use mz_compute_types::ComputeInstanceId;

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
