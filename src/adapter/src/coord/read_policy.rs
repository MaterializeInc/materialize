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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;

use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_adapter_types::compaction::{CompactionWindow, ReadCapability};
use mz_compute_types::ComputeInstanceId;
use mz_ore::instrument;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::read_policy::ReadPolicy;
use timely::progress::Antichain;

use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timeline::{TimelineContext, TimelineState};
use crate::session::Session;
use crate::util::ResultExt;

/// Relevant information for acquiring or releasing a bundle of read holds.
#[derive(Clone, Debug)]
pub(crate) struct ReadHolds<T> {
    holds: HashMap<Antichain<T>, CollectionIdBundle>,
}

impl<T: Eq + Hash + Ord> ReadHolds<T> {
    /// Return empty `ReadHolds`.
    pub fn new() -> Self {
        ReadHolds {
            holds: HashMap::new(),
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

    /// Returns an iterator over all storage ids and the time at which their read hold exists.
    pub fn storage_ids(&self) -> impl Iterator<Item = (&Antichain<T>, &GlobalId)> {
        self.holds
            .iter()
            .flat_map(|(time, id_bundle)| std::iter::repeat(time).zip(id_bundle.storage_ids.iter()))
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
    }

    /// If the read hold contains a storage ID equal to `id`, removes it from the read hold and
    /// drops it.
    pub fn remove_storage_id(&mut self, id: &GlobalId) {
        for (_, id_bundle) in &mut self.holds {
            id_bundle.storage_ids.remove(id);
        }
        self.holds.retain(|_, id_bundle| !id_bundle.is_empty());
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
        let mut compute_policy_updates: BTreeMap<ComputeInstanceId, Vec<_>> = BTreeMap::new();
        let mut storage_policy_updates = Vec::new();
        let mut id_bundles: HashMap<_, CollectionIdBundle> = HashMap::new();

        // Update the Coordinator's timeline read hold state and organize all id bundles by time.
        for (timeline_context, id_bundle) in self.partition_ids_by_timeline_context(id_bundle) {
            match timeline_context {
                TimelineContext::TimelineDependent(timeline) => {
                    let TimelineState { oracle, .. } = self.ensure_timeline_state(&timeline).await;
                    let read_ts = oracle.read_ts().await;
                    let new_read_holds = self.initialize_read_holds(read_ts, &id_bundle);
                    let TimelineState { read_holds, .. } =
                        self.ensure_timeline_state(&timeline).await;
                    for (time, id_bundle) in &new_read_holds.holds {
                        id_bundles
                            .entry(Some(time.clone()))
                            .or_default()
                            .extend(id_bundle);
                    }
                    read_holds.extend_with_new(new_read_holds);
                }
                TimelineContext::TimestampIndependent | TimelineContext::TimestampDependent => {
                    id_bundles.entry(None).or_default().extend(&id_bundle);
                }
            }
        }

        // Create read capabilities for all objects.
        for (time, id_bundle) in id_bundles {
            for (compute_instance, compute_ids) in id_bundle.compute_ids {
                for id in compute_ids {
                    let mut read_capability: ReadCapability<Timestamp> = compaction_window.into();
                    if let Some(time) = &time {
                        read_capability
                            .holds
                            .update_iter(time.iter().map(|t| (*t, 1)));
                    }
                    self.compute_read_capabilities.insert(id, read_capability);
                    compute_policy_updates
                        .entry(compute_instance)
                        .or_default()
                        .push((id, self.compute_read_capabilities[&id].policy()));
                }
            }

            for id in id_bundle.storage_ids {
                let mut read_capability: ReadCapability<Timestamp> = compaction_window.into();
                if let Some(time) = &time {
                    read_capability
                        .holds
                        .update_iter(time.iter().map(|t| (*t, 1)));
                }
                self.storage_read_capabilities.insert(id, read_capability);
                storage_policy_updates.push((id, self.storage_read_capabilities[&id].policy()));
            }
        }

        // Apply read capabilities.
        for (compute_instance, compute_policy_updates) in compute_policy_updates {
            self.controller
                .active_compute()
                .set_read_policy(compute_instance, compute_policy_updates)
                .unwrap_or_terminate("cannot fail to set read policy");
        }
        self.controller
            .storage
            .set_read_policy(storage_policy_updates);
    }

    pub(crate) fn update_storage_base_read_policies(
        &mut self,
        base_policies: Vec<(GlobalId, ReadPolicy<mz_repr::Timestamp>)>,
    ) {
        let mut policies = Vec::with_capacity(base_policies.len());
        for (id, base_policy) in base_policies {
            let capability = self
                .storage_read_capabilities
                .get_mut(&id)
                .expect("coord out of sync");
            capability.base_policy = base_policy;
            policies.push((id, capability.policy()))
        }
        self.controller.storage.set_read_policy(policies)
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
        self.storage_read_capabilities.remove(id).is_some()
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
    fn initialize_read_holds(
        &mut self,
        time: mz_repr::Timestamp,
        id_bundle: &CollectionIdBundle,
    ) -> ReadHolds<mz_repr::Timestamp> {
        let mut read_holds = ReadHolds::new();
        let time = Antichain::from_elem(time);

        for id in id_bundle.storage_ids.iter() {
            let collection = self
                .controller
                .storage
                .collection(*id)
                .expect("collection does not exist");
            let read_frontier = collection.implied_capability.clone();
            let time = time.join(&read_frontier);
            read_holds
                .holds
                .entry(time)
                .or_default()
                .storage_ids
                .insert(*id);
        }
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
    ) -> Result<ReadHolds<Timestamp>, Vec<(Antichain<Timestamp>, CollectionIdBundle)>> {
        let read_holds = self.initialize_read_holds(time, id_bundle);
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
                return Err(too_late);
            }
        }
        // Update STORAGE read policies.
        let mut policy_changes = Vec::new();
        for (time, id) in read_holds.storage_ids() {
            let read_needs = self
                .storage_read_capabilities
                .get_mut(id)
                .expect("id does not exist");
            read_needs.holds.update_iter(time.iter().map(|t| (*t, 1)));
            policy_changes.push((*id, read_needs.policy()));
        }
        self.controller.storage.set_read_policy(policy_changes);
        // Update COMPUTE read policies
        for (compute_instance, compute_ids) in read_holds.compute_ids() {
            let mut policy_changes = Vec::new();
            let mut compute = self.controller.active_compute();
            for (time, id) in compute_ids {
                let read_needs = self
                    .compute_read_capabilities
                    .get_mut(id)
                    .expect("id does not exist");
                read_needs.holds.update_iter(time.iter().map(|t| (*t, 1)));
                policy_changes.push((*id, read_needs.policy()));
            }
            compute
                .set_read_policy(*compute_instance, policy_changes)
                .unwrap_or_terminate("cannot fail to set read policy");
        }

        Ok(read_holds)
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
        let read_holds = self.acquire_read_holds(time, id_bundle, precise)?;
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
        read_holds: ReadHolds<mz_repr::Timestamp>,
        new_time: mz_repr::Timestamp,
    ) -> ReadHolds<mz_repr::Timestamp> {
        let mut new_read_holds = ReadHolds::new();
        let mut storage_policy_changes = Vec::new();
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
                    let collection = self
                        .controller
                        .storage
                        .collection(id)
                        .expect("id does not exist");
                    assert!(collection.implied_capability.le(&new_time.borrow()),
                            "Storage collection {:?} has read frontier {:?} not less-equal new time {:?}; old time: {:?}",
                            id,
                            collection.implied_capability,
                            new_time,
                            old_time,
                    );
                    let read_needs = self
                        .storage_read_capabilities
                        .get_mut(&id)
                        .expect("id does not exist");
                    read_needs
                        .holds
                        .update_iter(new_time.iter().map(|t| (*t, 1)));
                    read_needs
                        .holds
                        .update_iter(old_time.iter().map(|t| (*t, -1)));
                    storage_policy_changes.push((id, read_needs.policy()));
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

        // Update STORAGE read policies.
        self.controller
            .storage
            .set_read_policy(storage_policy_changes);

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
        // Update STORAGE read policies.
        let mut storage_policy_changes = Vec::new();
        for read_holds in read_holdses.iter() {
            for (time, id) in read_holds.storage_ids() {
                // It's possible that a concurrent DDL statement has already dropped this GlobalId
                if let Some(read_needs) = self.storage_read_capabilities.get_mut(id) {
                    read_needs.holds.update_iter(time.iter().map(|t| (*t, -1)));
                    storage_policy_changes.push((*id, read_needs.policy()));
                }
            }
        }
        self.controller
            .storage
            .set_read_policy(storage_policy_changes);
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
    }
}
