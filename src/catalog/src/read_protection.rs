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
    pending: Option<BTreeMap<GlobalId, Timestamp>>,
    closed: bool,
}

impl State {
    fn update_direct(
        active: &mut BTreeMap<GlobalId, MutableAntichain<Timestamp>>,
        id: GlobalId,
        changes: &[(Timestamp, i64)],
    ) {
        let frontier = active.entry(id).or_default();
        frontier.update_iter(changes.iter().copied());
        if frontier.frontier().is_empty() {
            active.remove(&id);
        }
    }

    fn update(&mut self, id: GlobalId, changes: &[(Timestamp, i64)]) {
        for id in
            std::iter::once(id).chain(self.dependencies.get(&id).into_iter().flatten().copied())
        {
            Self::update_direct(&mut self.active, id, changes);
        }
    }

    // Definitions are registered under committed grants, which token changes
    // cannot remove. Only publication completion can release those grants and
    // make definitions eligible for pruning.
    fn prune_dependencies(&mut self) {
        self.dependencies.retain(|id, _| {
            self.active.contains_key(id)
                || self.committed.contains_key(id)
                || self.pending.as_ref().is_some_and(|p| p.contains_key(id))
        });
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
            State::update_direct(&mut state.active, id, &changes.drain().collect::<Vec<_>>());
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
                    State::update_direct(&mut state.active, id, &[(time, 1)]);
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
        if let Some(pending) = &state.pending {
            frontier = frontier.max(*pending.get(&id)?);
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
                    || state
                        .pending
                        .as_ref()
                        .is_some_and(|pending| !covered(pending, *leaf, time))
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

    /// Snapshot active minima plus extra requirements for a durable publication.
    /// The returned map replaces all requirements for this incarnation.
    ///
    /// Until `finish_publication`, acquisitions must satisfy both the committed
    /// and pending maps. This prevents borrowing a grant that the writer is
    /// advancing or removing. Clone, downgrade, and drop cannot lower the
    /// snapshot's minima, so they may continue throughout publication.
    ///
    /// Panics if another publication is pending. The caller owns writer
    /// serialization and must finish this publication before starting another.
    pub fn prepare_publication(
        &self,
        extra: BTreeMap<GlobalId, Timestamp>,
    ) -> BTreeMap<GlobalId, Timestamp> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        assert!(state.pending.is_none(), "publication already pending");
        let extra = state.aggregate(extra);
        state.pending = Some(extra.clone());
        extra
    }

    /// Prepare new or stronger grants without advancing or releasing committed protection.
    /// Active tokens are already covered by committed grants and only downgrade
    /// forward, so retaining those grants also preserves every active token.
    /// Advancement and release remain eligible for the next aggregate publication.
    ///
    /// Uses the same acquisition barrier and definitive completion contract as
    /// `prepare_publication`. Panics if another publication is pending.
    pub fn prepare_grant_publication(
        &self,
        extra: BTreeMap<GlobalId, Timestamp>,
    ) -> BTreeMap<GlobalId, Timestamp> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        assert!(state.pending.is_none(), "publication already pending");
        let mut requirements = state.committed.clone();
        for (id, frontier) in extra {
            requirements
                .entry(id)
                .and_modify(|held| *held = (*held).min(frontier))
                .or_insert(frontier);
        }
        state.pending = Some(requirements.clone());
        requirements
    }

    /// Prepare changed aggregate requirements at the coalesced publication cadence.
    /// Unchanged protection needs no write. The snapshot installs the same
    /// acquisition barrier as an explicit grant expansion.
    pub fn prepare_publication_if_needed(&self) -> Option<BTreeMap<GlobalId, Timestamp>> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        assert!(state.pending.is_none(), "publication already pending");
        let requirements = state.aggregate(BTreeMap::new());
        if requirements == state.committed {
            return None;
        }
        state.pending = Some(requirements.clone());
        Some(requirements)
    }

    /// Acknowledge durable commit, or discard a definitively failed publication.
    /// An unknown commit outcome must not be reported as failure. Keep the
    /// barrier until the outcome is resolved, or close the client.
    /// Panics if no publication is pending.
    pub fn finish_publication(&self, committed: bool) {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        let pending = state.pending.take().expect("publication must be pending");
        if committed {
            state.committed = pending;
        }
        state.prune_dependencies();
    }

    /// Acknowledge a definitive successful catalog-derived grant publication.
    /// The supplied map must cover every pending requirement without advancement
    /// or release. Failed publications use `finish_publication(false)` instead.
    /// Panics if no publication is pending or a pending requirement is not covered.
    pub fn finish_grant_publication(&self, requirements: BTreeMap<GlobalId, Timestamp>) {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        let pending = state.pending.take().expect("publication must be pending");
        assert!(
            pending
                .iter()
                .all(|(id, frontier)| requirements.get(id).is_some_and(|held| held <= frontier)),
            "catalog grant must cover every pending requirement"
        );
        state.committed = requirements;
        state.prune_dependencies();
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
    fn reconstruction_holds_preserve_and_release_transferred_grants() {
        let index = GlobalId::User(1);
        let leaf = GlobalId::User(2);
        let inherited = BTreeMap::from([(index, 10.into()), (leaf, 10.into())]);
        let (protection, recovery) = ClientReadProtection::from_committed(2, inherited.clone());
        assert_eq!(protection.prepare_publication(BTreeMap::new()), inherited);
        protection.finish_publication(true);

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
        assert_eq!(protection.prepare_publication(BTreeMap::new()), inherited,);
        protection.finish_publication(true);
        drop(recovery_clone);
        assert_eq!(
            protection.prepare_publication(BTreeMap::new()),
            BTreeMap::from([(index, 20.into()), (leaf, 20.into())]),
        );
        protection.finish_publication(true);
        drop(successor);
        assert!(protection.prepare_publication(BTreeMap::new()).is_empty());
        protection.finish_publication(true);
    }
}
