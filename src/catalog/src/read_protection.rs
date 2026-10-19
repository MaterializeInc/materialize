// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Local tokens backed exclusively by acknowledged durable client grants.

pub mod publication;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::read_holds::{ChangeTx, ReadHold};
use timely::progress::Antichain;
use timely::progress::frontier::MutableAntichain;

/// New read holds cannot be acquired because this client has been closed.
#[derive(Debug, thiserror::Error)]
#[error("query client read protection is closed")]
pub struct ClientReadProtectionClosed;

/// The caller serializes publications for this incarnation. Tokens only
/// update local counters, never storage or compute controllers.
#[derive(Debug)]
pub struct ClientReadProtection {
    incarnation: u64,
    state: Arc<Mutex<State>>,
}

/// Retains transferred history until successor reconstruction establishes its
/// own requirements. These flat holds cannot be merged with ordinary index holds,
/// whose accounting also expands through logical dependencies.
#[derive(Clone, Debug)]
pub struct RecoveryReadHolds {
    _holds: Vec<ReadHold>,
}

#[derive(Debug, Default)]
struct State {
    committed: BTreeMap<GlobalId, Timestamp>,
    active: BTreeMap<GlobalId, MutableAntichain<Timestamp>>,
    // Catalog-derived logical leaves, never durable bindings. A token must
    // retain this knowledge even when its index disappears from the catalog.
    dependencies: BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    // Omitted keys retain committed authority. None revokes a grant only after
    // commit, but blocks new acquisitions immediately while the write is pending.
    pending: Option<BTreeMap<GlobalId, Option<Timestamp>>>,
    changed: BTreeSet<GlobalId>,
    closed: bool,
}

impl State {
    fn resample(&mut self, id: GlobalId) {
        let frontier = self
            .active
            .get(&id)
            .and_then(|active| active.frontier().iter().next().copied());
        if frontier == self.committed.get(&id).copied() {
            self.changed.remove(&id);
        } else {
            self.changed.insert(id);
        }
    }

    fn update_direct(
        active: &mut BTreeMap<GlobalId, MutableAntichain<Timestamp>>,
        changed: &mut BTreeSet<GlobalId>,
        id: GlobalId,
        changes: &[(Timestamp, i64)],
    ) {
        let frontier = active.entry(id).or_default();
        let before = frontier.frontier().iter().next().copied();
        frontier.update_iter(changes.iter().copied());
        if before != frontier.frontier().iter().next().copied() {
            changed.insert(id);
        }
        if frontier.frontier().is_empty() {
            active.remove(&id);
        }
    }

    fn update(&mut self, id: GlobalId, changes: &[(Timestamp, i64)]) {
        for id in
            std::iter::once(id).chain(self.dependencies.get(&id).into_iter().flatten().copied())
        {
            Self::update_direct(&mut self.active, &mut self.changed, id, changes);
        }
    }

    // Definitions are registered under committed grants, which token changes
    // cannot remove. Only publication completion can release those grants and
    // make definitions eligible for pruning.
    fn prune_dependencies(&mut self, ids: impl IntoIterator<Item = GlobalId>) {
        for id in ids {
            if !self.active.contains_key(&id) && !self.committed.contains_key(&id) {
                self.dependencies.remove(&id);
            }
        }
    }

    fn aggregate(&self, mut extra: BTreeMap<GlobalId, Timestamp>) -> BTreeMap<GlobalId, Timestamp> {
        for (id, active) in &self.active {
            if let Some(time) = active.frontier().iter().next() {
                extra
                    .entry(*id)
                    .and_modify(|extra| *extra = (*extra).min(*time))
                    .or_insert(*time);
            }
        }
        extra
    }
}

impl ClientReadProtection {
    /// Create an open client with no acknowledged grants for this incarnation.
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Adopt requirements transferred by a committed execution replacement.
    ///
    /// The returned holds retain that history until successor reconstruction has
    /// acquired its own protection. They are not query-admission tokens: index
    /// acquisition must still register and validate its logical inputs.
    pub fn from_committed(
        incarnation: u64,
        requirements: BTreeMap<GlobalId, Timestamp>,
    ) -> (Self, RecoveryReadHolds) {
        let protection = Self::new(incarnation);
        let local = Arc::clone(&protection.state);
        // The transferred map already includes independent logical-leaf grants.
        // Expanding these callbacks through subsequently registered dependencies
        // would subtract leaf counts that these recovery holds never installed.
        let change_tx: ChangeTx = Arc::new(move |id, mut changes| {
            let mut state = local.lock().expect("read protection mutex poisoned");
            let state = &mut *state;
            State::update_direct(
                &mut state.active,
                &mut state.changed,
                id,
                &changes.drain().collect::<Vec<_>>(),
            );
            Ok(())
        });
        let holds = {
            let mut state = protection
                .state
                .lock()
                .expect("read protection mutex poisoned");
            state.committed = requirements.clone();
            requirements
                .into_iter()
                .map(|(id, time)| {
                    let state = &mut *state;
                    State::update_direct(&mut state.active, &mut state.changed, id, &[(time, 1)]);
                    ReadHold::new(id, Antichain::from_elem(time), Arc::clone(&change_tx))
                })
                .collect()
        };
        (protection, RecoveryReadHolds { _holds: holds })
    }

    /// Return the durable client incarnation whose grants back these tokens.
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Copies local active-token minima for diagnostics without publishing,
    /// acquiring or extending protection. These are not a liveness certificate.
    pub fn active_frontiers(&self) -> BTreeMap<GlobalId, Timestamp> {
        self.state
            .lock()
            .expect("read protection mutex poisoned")
            .aggregate(BTreeMap::new())
    }

    /// Whether the writer has a publication in flight. Committed-update
    /// callbacks must defer further publication until that attempt finishes.
    pub fn publication_pending(&self) -> bool {
        self.state
            .lock()
            .expect("read protection mutex poisoned")
            .pending
            .is_some()
    }

    /// Returns a cached grant floor, not a new read hold. Acquisition must still
    /// validate against the current and pending publications under the mutex.
    pub fn granted_frontier(&self, id: GlobalId) -> Option<Timestamp> {
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
    /// With no timestamp preference, reuse the established window. A pending
    /// publication can restrict that window but cannot expand its authority.
    /// Acquisition must still validate the returned hint atomically.
    pub fn reusable_frontier(&self, id: GlobalId, read_ts: Option<Timestamp>) -> Option<Timestamp> {
        let state = self.state.lock().expect("read protection mutex poisoned");
        if state.closed {
            return None;
        }
        let mut frontier = *state.committed.get(&id)?;
        if let Some(pending) = state.pending.as_ref().and_then(|pending| pending.get(&id)) {
            frontier = frontier.max((*pending)?);
        }
        read_ts
            .is_none_or(|time| frontier <= time)
            .then_some(frontier)
    }

    /// Acquire one token per ID in the union of the storage and compute sets
    /// at the requested finite frontiers. Every ID must have a requested
    /// frontier. Entries outside the sets are ignored. Every compute index must
    /// have an explicit, immutable catalog-derived set of persisted logical
    /// inputs in `dependencies`, including when that set is empty.
    /// Each index token protects these leaves at its own requested timestamp,
    /// independently of any direct storage tokens.
    /// Returns `None` without installing holds if coverage or dependencies are
    /// missing, or a dependency definition disagrees with a retained definition.
    /// Returns an error if the client is closed.
    /// The caller must publish expanded leaf requirements before acquisition.
    pub fn try_acquire(
        &self,
        storage_ids: &BTreeSet<GlobalId>,
        compute_ids: &BTreeSet<GlobalId>,
        requested: &BTreeMap<GlobalId, Timestamp>,
        dependencies: &BTreeMap<GlobalId, BTreeSet<GlobalId>>,
    ) -> Result<Option<BTreeMap<GlobalId, ReadHold>>, ClientReadProtectionClosed> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        if state.closed {
            return Err(ClientReadProtectionClosed);
        }
        let covered = |grants: &BTreeMap<GlobalId, Timestamp>, id, time| {
            grants.get(&id).is_some_and(|grant| grant <= time)
        };
        for id in compute_ids {
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
                    || state.pending.as_ref().is_some_and(|pending| {
                        pending
                            .get(leaf)
                            .is_some_and(|grant| grant.is_none_or(|grant| grant > *time))
                    })
                {
                    return Ok(None);
                }
            }
        }
        for id in storage_ids.union(compute_ids).copied() {
            let Some(time) = requested.get(&id) else {
                return Ok(None);
            };
            if !covered(&state.committed, id, time)
                || state.pending.as_ref().is_some_and(|pending| {
                    pending
                        .get(&id)
                        .is_some_and(|grant| grant.is_none_or(|grant| grant > *time))
                })
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
            Ok(())
        });
        for id in compute_ids {
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
        let holds = storage_ids
            .union(compute_ids)
            .map(|id| (*id, issue(*id)))
            .collect();
        Ok(Some(holds))
    }

    /// Prepare new or stronger grants without advancing or releasing committed protection.
    /// The returned map contains only requested collections, clamped to their
    /// existing stronger grants.
    /// Active tokens are already covered by committed grants and only downgrade
    /// forward, so retaining those grants also preserves every active token.
    /// Advancement and release remain eligible for the next aggregate publication.
    ///
    /// Acquisitions remain bounded by committed and pending grants until the
    /// definitive outcome. Panics if another publication is pending.
    pub fn prepare_grant_publication(
        &self,
        extra: BTreeMap<GlobalId, Timestamp>,
    ) -> BTreeMap<GlobalId, Timestamp> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        assert!(state.pending.is_none(), "publication already pending");
        let requirements: BTreeMap<_, _> = extra
            .into_iter()
            .map(|(id, frontier)| {
                let frontier = state
                    .committed
                    .get(&id)
                    .map_or(frontier, |held| (*held).min(frontier));
                (id, frontier)
            })
            .collect();
        state.pending = Some(
            requirements
                .iter()
                .map(|(id, time)| (*id, Some(*time)))
                .collect(),
        );
        requirements
    }

    /// Snapshot only changed per-collection minima. `None` releases a grant,
    /// while an omitted collection retains its committed protection.
    /// Pending updates restrict acquisition until their definitive outcome.
    pub fn prepare_updates_if_needed(&self) -> Option<BTreeMap<GlobalId, Option<Timestamp>>> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        assert!(state.pending.is_none(), "publication already pending");
        let updates: BTreeMap<_, _> = std::mem::take(&mut state.changed)
            .into_iter()
            .filter_map(|id| {
                let frontier = state
                    .active
                    .get(&id)
                    .and_then(|active| active.frontier().iter().next().copied());
                (frontier != state.committed.get(&id).copied()).then_some((id, frontier))
            })
            .collect();
        if updates.is_empty() {
            return None;
        }
        state.pending = Some(updates.clone());
        Some(updates)
    }

    /// Acknowledge durable commit, or discard a definitively failed publication.
    /// An unknown commit outcome must not be reported as failure. Keep the
    /// barrier until the outcome is resolved, or close the client.
    /// Panics if no publication is pending.
    pub fn finish_publication(&self, committed: bool) {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        let pending = state.pending.take().expect("publication must be pending");
        // Resample published keys after either outcome. Tokens may have moved
        // while the write was pending, and a failed write changed no authority.
        if committed {
            for (id, frontier) in &pending {
                match frontier {
                    Some(frontier) => {
                        state.committed.insert(*id, *frontier);
                    }
                    None => {
                        state.committed.remove(id);
                    }
                }
            }
        }
        for id in pending.keys() {
            state.resample(*id);
        }
        state.prune_dependencies(pending.into_keys());
    }

    /// Acknowledge a definitive successful catalog-derived grant publication.
    /// The supplied map must cover every pending requirement without advancement
    /// or release. Unmentioned committed grants remain unchanged.
    /// Failed publications use `finish_publication(false)` instead.
    /// Panics if no publication is pending or a pending requirement is not covered.
    pub fn finish_grant_publication(&self, requirements: BTreeMap<GlobalId, Timestamp>) {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        let pending = state.pending.take().expect("publication must be pending");
        assert!(
            pending.iter().all(
                |(id, frontier)| frontier.is_some_and(|frontier| requirements
                    .get(id)
                    .is_some_and(|held| *held <= frontier))
            ),
            "catalog grant must cover every pending requirement"
        );
        for (id, frontier) in requirements {
            state.committed.insert(id, frontier);
            state.resample(id);
        }
        state.prune_dependencies(pending.into_keys());
    }

    /// Stop new acquisitions without invalidating token bookkeeping. Existing
    /// tokens are not proof of liveness. Closure is enforced at the read boundary.
    pub fn mark_closed(&self) {
        self.state
            .lock()
            .expect("read protection mutex poisoned")
            .closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn sparse_publication_preserves_unmentioned_grants_and_resamples_after_failure() {
        let a = GlobalId::User(1);
        let b = GlobalId::User(2);
        let protection = ClientReadProtection::new(1);
        let requirements = BTreeMap::from([(a, 10.into()), (b, 10.into())]);
        protection.prepare_grant_publication(requirements.clone());
        protection.finish_grant_publication(requirements.clone());
        let mut holds = protection
            .try_acquire(
                &BTreeSet::from([a, b]),
                &BTreeSet::new(),
                &requirements,
                &BTreeMap::new(),
            )
            .expect("client is open")
            .expect("committed grants cover both collections");
        assert_eq!(protection.prepare_updates_if_needed(), None);
        holds
            .get_mut(&a)
            .expect("hold for a")
            .try_downgrade(Antichain::from_elem(20.into()))
            .expect("advancing the hold succeeds");
        // Extra protection does not consume the local frontier change, even
        // when publishing it leaves the durable grants unchanged.
        assert_eq!(
            protection.prepare_grant_publication(requirements.clone()),
            requirements
        );
        protection.finish_grant_publication(requirements.clone());
        let update = BTreeMap::from([(a, Some(20.into()))]);
        assert_eq!(protection.prepare_updates_if_needed(), Some(update.clone()));
        assert_eq!(protection.reusable_frontier(a, Some(15.into())), None);
        assert_eq!(
            protection.reusable_frontier(b, Some(10.into())),
            Some(10.into())
        );

        protection.finish_publication(false);
        assert_eq!(protection.granted_frontier(a), Some(10.into()));
        assert_eq!(protection.prepare_updates_if_needed(), Some(update));
        // A local release during the write must be included in the next update.
        holds.remove(&b);
        protection.finish_publication(true);
        assert_eq!(protection.granted_frontier(b), Some(10.into()));
        assert_eq!(
            protection.prepare_updates_if_needed(),
            Some(BTreeMap::from([(b, None)])),
        );
        assert_eq!(protection.reusable_frontier(b, None), None);
        protection.finish_publication(true);
        assert_eq!(protection.granted_frontier(b), None);
        assert_eq!(protection.prepare_updates_if_needed(), None);
    }

    #[mz_ore::test]
    fn reconstruction_holds_preserve_and_release_transferred_grants() {
        let index = GlobalId::User(1);
        let leaf = GlobalId::User(2);
        let inherited = BTreeMap::from([(index, 10.into()), (leaf, 10.into())]);
        let (protection, recovery) = ClientReadProtection::from_committed(2, inherited.clone());
        assert_eq!(protection.prepare_updates_if_needed(), None);

        let successor = protection
            .try_acquire(
                &BTreeSet::new(),
                &BTreeSet::from([index]),
                &BTreeMap::from([(index, 20.into())]),
                &BTreeMap::from([(index, BTreeSet::from([leaf]))]),
            )
            .expect("successor is open")
            .expect("transferred grants cover the reconstructed index");
        let recovery_clone = recovery.clone();
        drop(recovery);
        assert_eq!(protection.prepare_updates_if_needed(), None);
        drop(recovery_clone);
        assert_eq!(
            protection.prepare_updates_if_needed(),
            Some(BTreeMap::from([
                (index, Some(20.into())),
                (leaf, Some(20.into()))
            ])),
        );
        protection.finish_publication(true);
        drop(successor);
        assert_eq!(
            protection.prepare_updates_if_needed(),
            Some(BTreeMap::from([(index, None), (leaf, None)]))
        );
        protection.finish_publication(true);
    }
}
