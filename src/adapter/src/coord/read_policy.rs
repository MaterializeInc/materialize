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

use std::collections::BTreeMap;
use std::fmt::Debug;

use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_adapter_types::compaction::CompactionWindow;
use mz_compute_types::ComputeInstanceId;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timeline::{TimelineContext, TimelineState};
use crate::session::Session;
use crate::util::ResultExt;

/// Read holds kept to ensure a set of collections remains readable at some
/// time.
///
/// This is a collection of [`ReadHold`] objects, which act as tokens ensuring
/// that read frontiers cannot advance past the held time as long as they exist.
/// Dropping a [`ReadHolds`] also drops the [`ReadHold`] tokens within and
/// relinquishes the associated read capabilities.
#[derive(Debug)]
pub struct ReadHolds<T: TimelyTimestamp> {
    pub storage_holds: BTreeMap<GlobalId, ReadHold<T>>,
    pub compute_holds: BTreeMap<(ComputeInstanceId, GlobalId), ReadHold<T>>,
}

impl<T: TimelyTimestamp> ReadHolds<T> {
    /// Return empty `ReadHolds`.
    pub fn new() -> Self {
        ReadHolds {
            storage_holds: BTreeMap::new(),
            compute_holds: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.storage_holds.is_empty() && self.compute_holds.is_empty()
    }

    /// Return the IDs of the contained storage collections.
    pub fn storage_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.storage_holds.keys().copied()
    }

    /// Return the IDs of the contained compute collections.
    pub fn compute_ids(&self) -> impl Iterator<Item = (ComputeInstanceId, GlobalId)> + '_ {
        self.compute_holds.keys().copied()
    }

    /// Return a `CollectionIdBundle` containing all the IDs in the
    /// [ReadHolds].
    pub fn id_bundle(&self) -> CollectionIdBundle {
        let mut res = CollectionIdBundle::default();
        for id in self.storage_ids() {
            res.storage_ids.insert(id);
        }
        for (instance_id, id) in self.compute_ids() {
            res.compute_ids.entry(instance_id).or_default().insert(id);
        }

        res
    }

    /// Downgrade the contained [`ReadHold`]s to the given time.
    pub fn downgrade(&mut self, time: T) {
        let frontier = Antichain::from_elem(time);
        for hold in self.storage_holds.values_mut() {
            let _ = hold.try_downgrade(frontier.clone());
        }
        for hold in self.compute_holds.values_mut() {
            let _ = hold.try_downgrade(frontier.clone());
        }
    }

    fn insert_storage_collection(&mut self, id: GlobalId, hold: ReadHold<T>) {
        let prev = self.storage_holds.insert(id, hold);
        assert!(prev.is_none(), "duplicate storage read hold: {id}");
    }

    fn insert_compute_collection(&mut self, id: (ComputeInstanceId, GlobalId), hold: ReadHold<T>) {
        let prev = self.compute_holds.insert(id, hold);
        assert!(prev.is_none(), "duplicate compute read hold: {id:?}");
    }

    pub fn remove_storage_collection(&mut self, id: GlobalId) {
        self.storage_holds.remove(&id);
    }

    pub fn remove_compute_collection(&mut self, instance_id: ComputeInstanceId, id: GlobalId) {
        self.compute_holds.remove(&(instance_id, id));
    }

    /// Return copies of a subset of the contained read holds, as specified by the given `ids`.
    pub fn clone_for(&self, ids: &CollectionIdBundle) -> Self {
        let mut storage_holds = BTreeMap::new();
        let mut compute_holds = BTreeMap::new();

        for &id in &ids.storage_ids {
            let hold = self.storage_holds[&id].clone();
            storage_holds.insert(id, hold);
        }
        for (&instance_id, compute_ids) in &ids.compute_ids {
            for &id in compute_ids {
                let hold = self.compute_holds[&(instance_id, id)].clone();
                compute_holds.insert((instance_id, id), hold);
            }
        }

        Self {
            storage_holds,
            compute_holds,
        }
    }
}

impl<T: TimelyTimestamp + Lattice> ReadHolds<T> {
    pub fn least_valid_read(&self) -> Antichain<T> {
        let mut since = Antichain::from_elem(T::minimum());
        for hold in self.storage_holds.values() {
            since.join_assign(hold.since());
        }

        for hold in self.compute_holds.values() {
            since.join_assign(hold.since());
        }

        since
    }

    /// Returns the frontier at which this [ReadHolds] is holding back the
    /// since of the collection identified by `id`. This does not mean that the
    /// overall since of the collection is what we report here. Only that it is
    /// _at least_ held back to the reported frontier by this read hold.
    ///
    /// This method is not meant to be fast, use wisely!
    pub fn since(&self, desired_id: &GlobalId) -> Antichain<T> {
        let mut since = Antichain::new();

        if let Some(hold) = self.storage_holds.get(desired_id) {
            since.extend(hold.since().iter().cloned());
        }

        for ((_instance, id), hold) in self.compute_holds.iter() {
            if id != desired_id {
                continue;
            }
            since.extend(hold.since().iter().cloned());
        }

        since
    }

    /// Merge the read holds in `other` into the contained read holds.
    fn merge(&mut self, other: Self) {
        use std::collections::btree_map::Entry;

        for (id, other_hold) in other.storage_holds {
            match self.storage_holds.entry(id) {
                Entry::Occupied(mut o) => {
                    o.get_mut().merge_assign(other_hold);
                }
                Entry::Vacant(v) => {
                    v.insert(other_hold);
                }
            }
        }
        for (id, other_hold) in other.compute_holds {
            match self.compute_holds.entry(id) {
                Entry::Occupied(mut o) => {
                    o.get_mut().merge_assign(other_hold);
                }
                Entry::Vacant(v) => {
                    v.insert(other_hold);
                }
            }
        }
    }
}

impl<T: TimelyTimestamp> Default for ReadHolds<T> {
    fn default() -> Self {
        ReadHolds::new()
    }
}

impl<T: TimelyTimestamp> From<ReadHolds<T>> for Vec<ReadHold<T>> {
    fn from(holds: ReadHolds<T>) -> Self {
        let storage = holds.storage_holds.into_values();
        let compute = holds.compute_holds.into_values();
        storage.chain(compute).collect()
    }
}

impl crate::coord::Coordinator {
    /// Initialize the read policy for a storage collection.
    ///
    /// This should be called only after a storage collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    pub(crate) async fn initialize_storage_read_policy(
        &mut self,
        mut read_hold: ReadHold<Timestamp>,
        compaction_window: CompactionWindow,
    ) {
        let id = read_hold.id();

        // Install read hold in the Coordinator's timeline state.
        if let TimelineContext::TimelineDependent(timeline) = self.get_timeline_context(id) {
            let TimelineState { oracle, read_holds } = self.ensure_timeline_state(&timeline).await;
            let read_ts = oracle.read_ts().await;
            let _ = read_hold.try_downgrade(Antichain::from_elem(read_ts));
            read_holds.insert_storage_collection(id, read_hold);
        };

        // Install read policy.
        self.controller
            .storage
            .set_read_policy(vec![(id, compaction_window.into())]);
    }

    pub(crate) async fn initialize_storage_read_policies(
        &mut self,
        read_holds: impl IntoIterator<Item = ReadHold<Timestamp>>,
        compaction_window: CompactionWindow,
    ) {
        for hold in read_holds {
            self.initialize_storage_read_policy(hold, compaction_window)
                .await;
        }
    }

    /// Initialize the read policy for a compute collection.
    ///
    /// This should be called only after a compute collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    pub(crate) async fn initialize_compute_read_policy(
        &mut self,
        mut read_hold: ReadHold<Timestamp>,
        instance: ComputeInstanceId,
        compaction_window: CompactionWindow,
    ) {
        let id = read_hold.id();

        // Install read hold in the Coordinator's timeline state.
        if let TimelineContext::TimelineDependent(timeline) = self.get_timeline_context(id) {
            let TimelineState { oracle, read_holds } = self.ensure_timeline_state(&timeline).await;
            let read_ts = oracle.read_ts().await;
            let _ = read_hold.try_downgrade(Antichain::from_elem(read_ts));
            read_holds.insert_compute_collection((instance, id), read_hold);
        };

        // Install read policy.
        self.controller
            .compute
            .set_read_policy(instance, vec![(id, compaction_window.into())])
            .expect("cannot fail to set read policy");
    }

    pub(crate) fn update_storage_read_policies(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<Timestamp>)>,
    ) {
        self.controller.storage.set_read_policy(policies);
    }

    pub(crate) fn update_compute_read_policies(
        &mut self,
        mut policies: Vec<(ComputeInstanceId, GlobalId, ReadPolicy<Timestamp>)>,
    ) {
        policies.sort_by_key(|&(cluster_id, _, _)| cluster_id);
        for (cluster_id, group) in &policies
            .into_iter()
            .group_by(|&(cluster_id, _, _)| cluster_id)
        {
            let group = group.map(|(_, id, policy)| (id, policy)).collect();
            self.controller
                .compute
                .set_read_policy(cluster_id, group)
                .unwrap_or_terminate("cannot fail to set read policy");
        }
    }

    pub(crate) fn update_compute_read_policy(
        &mut self,
        compute_instance: ComputeInstanceId,
        id: GlobalId,
        base_policy: ReadPolicy<Timestamp>,
    ) {
        self.update_compute_read_policies(vec![(compute_instance, id, base_policy)])
    }

    /// Attempt to acquire read holds on the indicated collections at the
    /// earliest available time.
    ///
    /// # Panics
    ///
    /// Will panic if any of the referenced collections in `id_bundle` don't
    /// exist.
    pub(crate) fn acquire_read_holds(
        &mut self,
        id_bundle: &CollectionIdBundle,
    ) -> ReadHolds<Timestamp> {
        let mut read_holds = ReadHolds::new();

        let desired_storage_holds = id_bundle.storage_ids.iter().map(|id| *id).collect_vec();
        let storage_read_holds = self
            .controller
            .storage
            .acquire_read_holds(desired_storage_holds)
            .expect("missing storage collections");
        read_holds.storage_holds = storage_read_holds
            .into_iter()
            .map(|hold| (hold.id(), hold))
            .collect();

        for (&instance_id, collection_ids) in &id_bundle.compute_ids {
            for &id in collection_ids {
                let hold = self
                    .controller
                    .compute
                    .acquire_read_hold(instance_id, id)
                    .expect("missing compute collection");

                let prev = read_holds.compute_holds.insert((instance_id, id), hold);
                assert!(
                    prev.is_none(),
                    "duplicate compute ID in id_bundle {id_bundle:?}"
                );
            }
        }

        tracing::debug!(?read_holds, "acquire_read_holds");
        read_holds
    }

    /// Stash transaction read holds. They will be released when the transaction
    /// is cleaned up.
    pub(crate) fn store_transaction_read_holds(
        &mut self,
        session: &Session,
        read_holds: ReadHolds<Timestamp>,
    ) {
        use std::collections::btree_map::Entry;

        let conn_id = session.conn_id().clone();
        match self.txn_read_holds.entry(conn_id) {
            Entry::Vacant(v) => {
                v.insert(read_holds);
            }
            Entry::Occupied(mut o) => {
                o.get_mut().merge(read_holds);
            }
        }
    }
}
