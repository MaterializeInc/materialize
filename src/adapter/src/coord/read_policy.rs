// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and methods related to initializing, updating, and removing read policies
//! on collections.
//!
//! This module contains the API for read holds on collections. A "read hold" prevents
//! the controller from compacting the associated collections, and ensures that they
//! remain "readable" at a specific time, as long as the hold is held.

//! Allow usage of `std::collections::HashMap`.
//! The code in this module deals with `Antichain`-keyed maps. `Antichain` does not implement
//! `Ord`, so we cannot use `BTreeMap`s. We need to iterate through the maps, so we cannot use the
//! `mz_ore` wrapper either.
#![allow(clippy::disallowed_types)]

use std::collections::{hash_map, BTreeMap, BTreeSet, HashMap};

use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_adapter_types::compaction::{CompactionWindow, ReadCapability};
use mz_compute_types::ComputeInstanceId;
use mz_ore::instrument;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use serde::Serialize;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timeline::{TimelineContext, TimelineState};
use crate::coord::Coordinator;
use crate::session::Session;
use crate::util::ResultExt;

/// Relevant information for acquiring or releasing a bundle of read holds.
#[derive(Debug, Serialize)]
pub struct ReadHolds<T: TimelyTimestamp> {
    pub holds: HashMap<Antichain<T>, CollectionIdBundle>,
    pub storage_holds: HashMap<GlobalId, ReadHold<T>>,
}

impl<T: TimelyTimestamp> ReadHolds<T> {
    /// Return empty `ReadHolds`.
    pub fn new() -> Self {
        ReadHolds {
            holds: HashMap::new(),
            storage_holds: HashMap::new(),
        }
    }

    /// Returns whether the `ReadHolds` is empty.
    pub fn is_empty(&self) -> bool {
        self.holds.is_empty()
    }

    /// Returns an iterator over all times at which a read hold exists.
    pub fn times(&self) -> impl Iterator<Item = &Antichain<T>> {
        self.holds.keys()
    }

    /// Return a `CollectionIdBundle` containing all the IDs in the `ReadHolds`.
    pub fn id_bundle(&self) -> CollectionIdBundle {
        self.holds
            .values()
            .fold(CollectionIdBundle::default(), |mut accum, id_bundle| {
                accum.extend(id_bundle);
                accum
            })
    }

    /// Returns an iterator over all compute ids by compute instance and the time at which their
    /// read hold exists.
    pub fn compute_ids(
        &self,
    ) -> impl Iterator<
        Item = (
            &ComputeInstanceId,
            impl Iterator<Item = (&Antichain<T>, &GlobalId)>,
        ),
    > {
        let compute_instances: BTreeSet<_> = self
            .holds
            .iter()
            .flat_map(|(_, id_bundle)| id_bundle.compute_ids.keys())
            .collect();

        compute_instances.into_iter().map(|compute_instance| {
            let inner_iter = self
                .holds
                .iter()
                .filter_map(|(time, id_bundle)| {
                    id_bundle
                        .compute_ids
                        .get(compute_instance)
                        .map(|ids| std::iter::repeat(time).zip(ids.iter()))
                })
                .flatten();
            (compute_instance, inner_iter)
        })
    }

    /// Extends a `ReadHolds` with the contents of another `ReadHolds`.
    /// Asserts that the newly added read holds don't coincide with any of the existing read holds in self.
    pub fn extend_with_new(&mut self, other: ReadHolds<T>) {
        for (time, other_id_bundle) in other.holds {
            let self_id_bundle = self.holds.entry(time).or_default();
            assert!(
                self_id_bundle.intersection(&other_id_bundle).is_empty(),
                "extend_with_new encountered duplicate read holds",
            );
            self_id_bundle.extend(&other_id_bundle);
        }

        for (id, other_storage_hold) in other.storage_holds.into_iter() {
            let self_entry = self.storage_holds.entry(id);

            match self_entry {
                hash_map::Entry::Occupied(mut e) => {
                    e.get_mut().merge_assign(other_storage_hold);
                }
                hash_map::Entry::Vacant(e) => {
                    e.insert(other_storage_hold);
                }
            }
        }
    }

    /// If the read hold contains a storage ID equal to `id`, removes it from the read hold.
    pub fn remove_storage_id(&mut self, id: &GlobalId) {
        for (_since, id_bundle) in &mut self.holds {
            let removed_id = id_bundle.storage_ids.remove(id);
            tracing::info!(%id, removed = %removed_id, "removed storage read holds from adapter read holds!");
        }
        self.holds.retain(|_, id_bundle| !id_bundle.is_empty());
        self.storage_holds.remove(id);
    }

    /// If the read hold contains a compute ID equal to `id` in `compute_instance`, removes it from
    /// the read hold and drops it.
    pub fn remove_compute_id(&mut self, compute_instance: &ComputeInstanceId, id: &GlobalId) {
        for (_, id_bundle) in &mut self.holds {
            if let Some(compute_ids) = id_bundle.compute_ids.get_mut(compute_instance) {
                compute_ids.remove(id);
                if compute_ids.is_empty() {
                    id_bundle.compute_ids.remove(compute_instance);
                }
            }
        }
        self.holds.retain(|_, id_bundle| !id_bundle.is_empty());
    }
}

/// A drop guard that prevents read holds from being leaked by releasing them
/// when this guard is dropped.
///
/// This holds a mutable reference to the [Coordinator] that the read holds were
/// acquired from, so that they can be released on drop. While the guard is is
/// in scope, you have to use its `coordinator` field when you want to access
/// the coordinator.
///
/// A note on why we have this clunky guard: The alternative is to audit all
/// call-sites of [Coordinator::acquire_read_holds] to make sure that acquired
/// holds are eventually released. With early returns and returns on `?`, this
/// seemed daunting, and it would be too easy to accidentally introduce early
/// returns in code that happens to have a [ReadHolds] in scope, which would
/// make them leak.
///
/// A note on types: This is special-cased to `ReadHolds<mz_repr::Timestamp>`
/// because that is what [Coordinator::acquire_read_holds] and
/// [Coordinator::release_read_holds] can work with.
///
/// TODO: Once we make [ReadHolds] themselves droppable, that is make them
/// release their read holds when dropped, we can remove this special guard and
/// simplify call-sites again.
pub struct ReadHoldsGuard<'a> {
    pub coordinator: &'a mut Coordinator,
    read_holds: Option<ReadHolds<mz_repr::Timestamp>>,
}

impl<'a> ReadHoldsGuard<'a> {
    pub fn new(
        coordinator: &'a mut Coordinator,
        read_holds: ReadHolds<mz_repr::Timestamp>,
    ) -> Self {
        ReadHoldsGuard {
            coordinator,
            read_holds: Some(read_holds),
        }
    }

    /// Take the [ReadHolds] out of the guard. After this call, you are
    /// responsible for eventually releasing the read holds!
    pub fn take(mut self) -> ReadHolds<mz_repr::Timestamp> {
        let read_holds = self.read_holds.take().expect("missing read holds");

        read_holds
    }
}

impl Drop for ReadHoldsGuard<'_> {
    fn drop(&mut self) {
        if let Some(read_holds) = self.read_holds.take() {
            tracing::debug!(?read_holds, "ReadHoldsGuard, dropping read holds");
            self.coordinator.release_read_holds(vec![read_holds])
        }
    }
}

impl crate::coord::Coordinator {
    /// Initialize the storage read policies.
    ///
    /// This should be called only after a storage collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    pub(crate) async fn initialize_storage_read_policies(
        &mut self,
        ids: Vec<GlobalId>,
        compaction_window: CompactionWindow,
    ) {
        self.initialize_read_policies(
            &CollectionIdBundle {
                storage_ids: ids.into_iter().collect(),
                compute_ids: BTreeMap::new(),
            },
            compaction_window,
        )
        .await;
    }

    /// Initialize the compute read policies.
    ///
    /// This should be called only after a compute collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    pub(crate) async fn initialize_compute_read_policies(
        &mut self,
        ids: Vec<GlobalId>,
        instance: ComputeInstanceId,
        compaction_window: CompactionWindow,
    ) {
        let mut compute_ids: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        compute_ids.insert(instance, ids.into_iter().collect());
        self.initialize_read_policies(
            &CollectionIdBundle {
                storage_ids: BTreeSet::new(),
                compute_ids,
            },
            compaction_window,
        )
        .await;
    }

    /// Initialize the storage and compute read policies.
    ///
    /// This should be called only after a collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    #[instrument(name = "coord::initialize_read_policies")]
    pub(crate) async fn initialize_read_policies(
        &mut self,
        id_bundle: &CollectionIdBundle,
        compaction_window: CompactionWindow,
    ) {
        tracing::info!(id_bundle = ?id_bundle, "read_policy::initialize_read_policies");

        let mut compute_policy_updates: BTreeMap<ComputeInstanceId, Vec<_>> = BTreeMap::new();
        let mut storage_policy_updates: Vec<(GlobalId, ReadPolicy<mz_repr::Timestamp>)> =
            Vec::new();

        let mut compute_id_bundles: HashMap<_, CollectionIdBundle> = HashMap::new();

        // Update the Coordinator's timeline read hold state and organize all id bundles by time.
        for (timeline_context, id_bundle) in self.partition_ids_by_timeline_context(id_bundle) {
            match timeline_context {
                TimelineContext::TimelineDependent(timeline) => {
                    let TimelineState { oracle, .. } = self.ensure_timeline_state(&timeline).await;

                    let read_ts = oracle.read_ts().await;
                    // SUBTLE: This does NOT yet acquire the read holds. That
                    // only happens after this loop.
                    let new_compute_read_holds =
                        self.initialize_compute_read_holds(read_ts, &id_bundle);

                    for (time, id_bundle) in &new_compute_read_holds.holds {
                        compute_id_bundles
                            .entry(Some(time.clone()))
                            .or_default()
                            .extend(id_bundle);
                    }

                    // Acquire the earliest possible STORAGE read holds.
                    let new_storage_read_holds = self.acquire_storage_read_holds(
                        mz_repr::Timestamp::minimum(),
                        id_bundle.storage_ids.into_iter(),
                    );

                    for (id, _storage_hold) in new_storage_read_holds.storage_holds.iter() {
                        storage_policy_updates.push((*id, compaction_window.into()));
                    }

                    let TimelineState { read_holds, .. } =
                        self.ensure_timeline_state(&timeline).await;

                    read_holds.extend_with_new(new_compute_read_holds);
                    read_holds.extend_with_new(new_storage_read_holds);
                }
                TimelineContext::TimestampIndependent | TimelineContext::TimestampDependent => {
                    compute_id_bundles
                        .entry(None)
                        .or_default()
                        .extend(&id_bundle);
                }
            }
        }

        // Create read capabilities for all compute objects.
        for (time, id_bundle) in compute_id_bundles {
            for (compute_instance, compute_ids) in id_bundle.compute_ids {
                for id in compute_ids {
                    let read_capability = self.ensure_compute_capability(
                        &compute_instance,
                        &id,
                        Some(compaction_window.clone()),
                    );
                    if let Some(time) = &time {
                        read_capability
                            .holds
                            .update_iter(time.iter().map(|t| (*t, 1)));
                    }
                    compute_policy_updates
                        .entry(compute_instance)
                        .or_default()
                        .push((id, read_capability.policy()));
                }
            }
        }

        // Apply read capabilities.
        for (compute_instance, compute_policy_updates) in compute_policy_updates {
            self.controller
                .active_compute()
                .set_read_policy(compute_instance, compute_policy_updates)
                .unwrap_or_terminate("cannot fail to set read policy");
        }

        for (id, policy) in storage_policy_updates.iter() {
            let prev = self.storage_read_policies.insert(*id, policy.clone());
            assert!(
                prev.is_none(),
                "already have a read policy for {}: {:?}",
                id,
                policy
            );
        }
        if !storage_policy_updates.is_empty() {
            self.controller
                .collections
                .set_read_policies(storage_policy_updates);
        }
    }

    // If there is not capability for the given object, initialize one at the
    // earliest possible since. Return the capability.
    //
    // When a `compaction_window` is given, this is installed as the policy of
    // the collection, regardless if a capability existed before or not.
    fn ensure_compute_capability(
        &mut self,
        instance_id: &ComputeInstanceId,
        id: &GlobalId,
        compaction_window: Option<CompactionWindow>,
    ) -> &mut ReadCapability<mz_repr::Timestamp> {
        let entry = self
            .compute_read_capabilities
            .entry(*id)
            .and_modify(|capability| {
                // If we explicitly got a compaction window, override any existing
                // one.
                if let Some(compaction_window) = compaction_window {
                    capability.base_policy = compaction_window.into();
                }
            })
            .or_insert_with(|| {
                let policy: ReadPolicy<Timestamp> = match compaction_window {
                    Some(compaction_window) => compaction_window.into(),
                    None => {
                        // We didn't get an initial policy, so set the current
                        // since as a static policy.
                        let compute = self.controller.active_compute();
                        let collection = compute
                            .collection(*instance_id, *id)
                            .expect("collection does not exist");
                        let read_frontier = collection.read_capability().clone();
                        ReadPolicy::NoPolicy {
                            initial_since: read_frontier,
                        }
                    }
                };

                ReadCapability::from(policy)
            });

        entry
    }

    pub(crate) fn update_storage_base_read_policies(
        &mut self,
        base_policies: Vec<(GlobalId, ReadPolicy<mz_repr::Timestamp>)>,
    ) {
        let mut policies = Vec::with_capacity(base_policies.len());
        for (id, base_policy) in base_policies {
            let current_policy = self.storage_read_policies.get_mut(&id).expect(
                "coord out of sync, expected to have a base policy set for known collection",
            );
            *current_policy = base_policy.clone();
            policies.push((id, base_policy))
        }
        self.controller.collections.set_read_policies(policies);
    }

    pub(crate) fn update_compute_base_read_policies(
        &mut self,
        mut base_policies: Vec<(ComputeInstanceId, GlobalId, ReadPolicy<mz_repr::Timestamp>)>,
    ) {
        base_policies.sort_by_key(|&(cluster_id, _, _)| cluster_id);
        for (cluster_id, group) in &base_policies
            .into_iter()
            .group_by(|&(cluster_id, _, _)| cluster_id)
        {
            let group = group
                .map(|(_, id, base_policy)| {
                    let capability = self
                        .compute_read_capabilities
                        .get_mut(&id)
                        .expect("coord out of sync");
                    capability.base_policy = base_policy;
                    (id, capability.policy())
                })
                .collect::<Vec<_>>();
            self.controller
                .active_compute()
                .set_read_policy(cluster_id, group)
                .unwrap_or_terminate("cannot fail to set read policy");
        }
    }

    pub(crate) fn update_compute_base_read_policy(
        &mut self,
        compute_instance: ComputeInstanceId,
        id: GlobalId,
        base_policy: ReadPolicy<mz_repr::Timestamp>,
    ) {
        self.update_compute_base_read_policies(vec![(compute_instance, id, base_policy)])
    }

    /// Drop read policy in STORAGE for `id`.
    ///
    /// Returns true if `id` had a read policy and false otherwise.
    pub(crate) fn drop_storage_read_policy(&mut self, id: &GlobalId) -> bool {
        self.storage_read_policies.remove(id).is_some()
    }

    /// Drop read policy in COMPUTE for `id`.
    ///
    /// Returns true if `id` had a read policy and false otherwise.
    pub(crate) fn drop_compute_read_policy(&mut self, id: &GlobalId) -> bool {
        self.compute_read_capabilities.remove(id).is_some()
    }

    /// Creates a `ReadHolds` struct that creates a read hold for each id in
    /// `id_bundle`. The time of each read holds is at `time`, if possible
    /// otherwise it is at the lowest possible time.
    ///
    /// This does not apply the read holds in STORAGE or COMPUTE. It is up
    /// to the caller to apply the read holds.
    fn initialize_compute_read_holds(
        &mut self,
        time: mz_repr::Timestamp,
        id_bundle: &CollectionIdBundle,
    ) -> ReadHolds<mz_repr::Timestamp> {
        let mut read_holds = ReadHolds::new();
        let time = Antichain::from_elem(time);

        for (compute_instance, compute_ids) in id_bundle.compute_ids.iter() {
            let compute = self.controller.active_compute();
            for id in compute_ids.iter() {
                let collection = compute
                    .collection(*compute_instance, *id)
                    .expect("collection does not exist");
                let read_frontier = collection.read_capability().clone();
                let time = time.join(&read_frontier);
                read_holds
                    .holds
                    .entry(time)
                    .or_default()
                    .compute_ids
                    .entry(*compute_instance)
                    .or_default()
                    .insert(*id);
            }
        }

        read_holds
    }

    /// Acquires and returns read holds for the storage collections identified
    /// by `ids` at the given `time`. If that is not possible, for each
    /// collection, the earliest possible read hold will be acquired.
    fn acquire_storage_read_holds<I>(
        &mut self,
        time: mz_repr::Timestamp,
        ids: I,
    ) -> ReadHolds<mz_repr::Timestamp>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let mut read_holds = ReadHolds::new();

        let since = Antichain::from_elem(time);

        // Acquire the earliest possible read holds.
        let desired_holds = ids.into_iter().map(|id| (id, since.clone())).collect_vec();

        let storage_read_holds = self
            .controller
            .collections
            .acquire_read_holds(desired_holds);

        for mut storage_hold in storage_read_holds {
            // We try and downgrade, but if it's already further ahead, that's
            // also fine.
            let _ = storage_hold.try_downgrade(since.clone());

            read_holds
                .holds
                .entry(storage_hold.since().to_owned())
                .or_default()
                .storage_ids
                .insert(*storage_hold.id());

            read_holds
                .storage_holds
                .insert(*storage_hold.id(), storage_hold);
        }

        read_holds
    }

    /// Attempt to acquire read holds on the indicated collections at the indicated `time`.
    ///
    /// If we are unable to acquire a read hold at the provided `time` for a specific id, then
    /// depending on the `precise` argument, we either fall back to acquiring a read hold at
    /// the lowest possible time for that id, or return an error. The returned error contains
    /// those collection sinces that were later than the specified time.
    pub(crate) fn acquire_read_holds(
        &mut self,
        time: Timestamp,
        id_bundle: &CollectionIdBundle,
        precise: bool,
    ) -> Result<ReadHoldsGuard, Vec<(Antichain<Timestamp>, CollectionIdBundle)>> {
        let mut read_holds = self.initialize_compute_read_holds(time, id_bundle);

        // Update COMPUTE read policies
        for (compute_instance, compute_ids) in read_holds.compute_ids() {
            let mut policy_changes = Vec::new();
            for (time, id) in compute_ids {
                let read_needs = self.ensure_compute_capability(compute_instance, id, None);
                read_needs.holds.update_iter(time.iter().map(|t| (*t, 1)));
                policy_changes.push((*id, read_needs.policy()));
            }
            let mut compute = self.controller.active_compute();
            compute
                .set_read_policy(*compute_instance, policy_changes)
                .unwrap_or_terminate("cannot fail to set read policy");
        }

        // Acquire the earliest possible STORAGE read holds.
        let storage_read_holds =
            self.acquire_storage_read_holds(time, id_bundle.storage_ids.iter().cloned());
        read_holds.extend_with_new(storage_read_holds);

        if precise {
            // If we are not able to acquire read holds precisely at the specified time (only later), then error out.
            let too_late = read_holds
                .holds
                .iter()
                .filter_map(|(antichain, ids)| {
                    if antichain.iter().all(|hold_time| *hold_time == time) {
                        None
                    } else {
                        Some((antichain.clone(), ids.clone()))
                    }
                })
                .collect_vec();
            if !too_late.is_empty() {
                // Make sure we release what we already acquired!
                self.release_read_holds(vec![read_holds]);
                return Err(too_late);
            }
        }

        tracing::debug!(
            ?time,
            ?id_bundle,
            ?read_holds,
            "adapter::acquire_read_holds"
        );

        Ok(ReadHoldsGuard::new(self, read_holds))
    }

    /// Attempt to acquire read holds on the indicated collections at the indicated `time`.
    /// This is similar to [Self::acquire_read_holds], but instead of returning the read holds,
    /// it arranges for them to be automatically released at the end of the transaction.
    ///
    /// If we are unable to acquire a read hold at the provided `time` for a specific id, then
    /// depending on the `precise` argument, we either fall back to acquiring a read hold at
    /// the lowest possible time for that id, or return an error. The returned error contains
    /// those collection sinces that were later than the specified time.
    pub(crate) fn acquire_read_holds_auto_cleanup(
        &mut self,
        session: &Session,
        time: Timestamp,
        id_bundle: &CollectionIdBundle,
        precise: bool,
    ) -> Result<(), Vec<(Antichain<Timestamp>, CollectionIdBundle)>> {
        tracing::debug!(session = %session.conn_id(), ?time, ?id_bundle, "acquire read holds auto cleanup");
        let read_holds = self.acquire_read_holds(time, id_bundle, precise)?.take();
        self.txn_read_holds
            .entry(session.conn_id().clone())
            .or_insert_with(Vec::new)
            .push(read_holds);
        Ok(())
    }

    /// Attempt to update the timestamp of the read holds on the indicated collections from the
    /// indicated times within `read_holds` to `new_time`.
    ///
    /// If we are unable to update a read hold at the provided `time` for a specific id, then we
    /// leave it unchanged.
    ///
    /// This method relies on a previous call to
    /// `initialize_read_holds`, `acquire_read_holds`, or `update_read_hold` that returned
    /// `read_holds`, and its behavior will be erratic if called on anything else.
    pub(super) fn update_read_holds(
        &mut self,
        mut read_holds: ReadHolds<mz_repr::Timestamp>,
        new_time: mz_repr::Timestamp,
    ) -> ReadHolds<mz_repr::Timestamp> {
        let mut new_read_holds = ReadHolds::new();
        let mut storage_changes = BTreeMap::new();
        let mut compute_policy_changes: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let new_time = Antichain::from_elem(new_time);

        for (old_time, id_bundle) in read_holds.holds {
            let new_time = old_time.join(&new_time);
            if old_time != new_time {
                new_read_holds
                    .holds
                    .entry(new_time.clone())
                    .or_default()
                    .extend(&id_bundle);

                for id in id_bundle.storage_ids {
                    let storage_change = storage_changes
                        .entry(id.clone())
                        .or_insert_with(|| new_time.clone());
                    storage_change.meet_assign(&new_time);
                }

                for (compute_instance, compute_ids) in id_bundle.compute_ids {
                    let compute = self.controller.active_compute();
                    for id in compute_ids {
                        let collection = compute
                            .collection(compute_instance, id)
                            .expect("id does not exist");
                        assert!(collection.read_capability().le(&new_time.borrow()),
                                "Compute collection {:?} (instance {:?}) has read frontier {:?} not less-equal new time {:?}; old time: {:?}",
                                id,
                                compute_instance,
                                collection.read_capability(),
                                new_time,
                                old_time,
                        );
                        let read_needs = self
                            .compute_read_capabilities
                            .get_mut(&id)
                            .expect("id does not exist");
                        read_needs
                            .holds
                            .update_iter(new_time.iter().map(|t| (*t, 1)));
                        read_needs
                            .holds
                            .update_iter(old_time.iter().map(|t| (*t, -1)));
                        compute_policy_changes
                            .entry(compute_instance)
                            .or_default()
                            .push((id, read_needs.policy()));
                    }
                }
            } else {
                new_read_holds
                    .holds
                    .entry(old_time)
                    .or_default()
                    .extend(&id_bundle);
            }
        }

        // Update STORAGE holds in place...

        for (id, frontier) in storage_changes {
            let hold = read_holds
                .storage_holds
                .get_mut(&id)
                .expect("known to exist");
            hold.try_downgrade(frontier)
                .expect("we only advance the frontier");
        }

        // And then move over STORAGE holds.
        new_read_holds
            .storage_holds
            .extend(read_holds.storage_holds.into_iter());

        // Update COMPUTE read policies
        let mut compute = self.controller.active_compute();
        for (compute_instance, compute_policy_changes) in compute_policy_changes {
            compute
                .set_read_policy(compute_instance, compute_policy_changes)
                .unwrap_or_terminate("cannot fail to set read policy");
        }

        new_read_holds
    }

    /// Release the given read holds.
    ///
    /// This method relies on a previous call to
    /// `initialize_read_holds`, `acquire_read_holds`, or `update_read_hold` that returned
    /// `ReadHolds`, and its behavior will be erratic if called on anything else,
    /// or if called more than once on the same bundle of read holds.
    pub(super) fn release_read_holds(&mut self, read_holdses: Vec<ReadHolds<Timestamp>>) {
        tracing::debug!(?read_holdses, "adapter::release_read_holds");
        // Update COMPUTE read policies
        let mut compute = self.controller.active_compute();
        let mut policy_changes_per_instance = BTreeMap::new();
        for read_holds in read_holdses.iter() {
            for (compute_instance, compute_ids) in read_holds.compute_ids() {
                let policy_changes = policy_changes_per_instance
                    .entry(compute_instance)
                    .or_insert_with(Vec::new);
                for (time, id) in compute_ids {
                    // It's possible that a concurrent DDL statement has already dropped this GlobalId
                    if let Some(read_needs) = self.compute_read_capabilities.get_mut(id) {
                        read_needs.holds.update_iter(time.iter().map(|t| (*t, -1)));
                        policy_changes.push((*id, read_needs.policy()));
                    }
                }
            }
        }
        for (compute_instance, policy_changes) in policy_changes_per_instance {
            if compute.instance_exists(*compute_instance) {
                compute
                    .set_read_policy(*compute_instance, policy_changes)
                    .unwrap_or_terminate("cannot fail to set read policy");
            }
        }

        // Release STORAGE read holds.
        //
        // NOTE: This happens implicitly by the Drop implementation of ReadHold.
    }
}
