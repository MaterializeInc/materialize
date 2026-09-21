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

/// Renew the heartbeat at this interval when no requirements need publication.
pub const CLIENT_PROTECTION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
/// Observe an unchanged heartbeat locally for this long before attempting closure.
pub const CLIENT_PROTECTION_UNCHANGED_GRACE: Duration = Duration::from_secs(300);

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

    /// Return the durable client incarnation whose grants back these tokens.
    pub fn incarnation(&self) -> u64 {
        self.incarnation
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
    /// With no timestamp preference, reuse the established window.
    pub fn reusable_frontier(&self, id: GlobalId, read_ts: Option<Timestamp>) -> Option<Timestamp> {
        self.granted_frontier(id)
            .filter(|grant| read_ts.is_none_or(|time| *grant <= time))
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
            state.prune_dependencies();
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

    /// Prepare changed aggregate requirements, or an idle heartbeat renewal.
    /// Called on the coalesced publication cadence. An unchanged aggregate needs
    /// no write until renewal is due. The snapshot installs the same acquisition
    /// barrier as an explicit grant expansion.
    pub fn prepare_publication_if_needed(
        &self,
        elapsed: Duration,
    ) -> Option<BTreeMap<GlobalId, Timestamp>> {
        let mut state = self.state.lock().expect("read protection mutex poisoned");
        assert!(state.pending.is_none(), "publication already pending");
        let requirements = state.aggregate(BTreeMap::new());
        if requirements == state.committed && elapsed < CLIENT_PROTECTION_HEARTBEAT_INTERVAL {
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

    /// Stop new acquisitions without invalidating token bookkeeping. Existing
    /// tokens are not proof of liveness. Closure is enforced at the read boundary.
    pub fn mark_closed(&self) {
        self.state
            .lock()
            .expect("read protection mutex poisoned")
            .closed = true;
    }
}

/// Process-local unchanged-heartbeat observations. Restarting this observer
/// starts a full grace window, regardless of the persisted heartbeat value.
#[derive(Debug, Default)]
pub struct ClientProtectionReclaimer {
    observations: BTreeMap<u64, (u64, Instant)>,
}

impl ClientProtectionReclaimer {
    /// Observe a complete snapshot of open incarnations and their heartbeats.
    /// Each ID appears once. Returned candidates must be closed by a durable CAS
    /// against the returned heartbeat, not by trusting this observation alone.
    pub fn observe(
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
