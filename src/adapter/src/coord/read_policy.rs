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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use differential_dataflow::lattice::Lattice;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_types::ComputeInstanceId;
use mz_ore::instrument;
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use timely::progress::Antichain;
use timely::progress::Timestamp as _;

use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timeline::TimelineContext;

/// Read holds kept to ensure a set of collections remains readable at some
/// time.
///
/// This is a collection of [`ReadHold`] objects, which act as tokens ensuring
/// that read frontiers cannot advance past the held time as long as they exist.
/// Dropping a [`ReadHolds`] also drops the [`ReadHold`] tokens within and
/// relinquishes the associated read capabilities.
#[derive(Debug, Default, Clone)]
pub struct ReadHolds {
    pub storage_holds: BTreeMap<GlobalId, ReadHold>,
    pub compute_holds: BTreeMap<(ComputeInstanceId, GlobalId), ReadHold>,
}

impl ReadHolds {
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
    pub fn downgrade(&mut self, time: Timestamp) {
        let frontier = Antichain::from_elem(time);
        for hold in self.storage_holds.values_mut() {
            let _ = hold.try_downgrade(frontier.clone());
        }
        for hold in self.compute_holds.values_mut() {
            let _ = hold.try_downgrade(frontier.clone());
        }
    }

    pub fn remove_storage_collection(&mut self, id: GlobalId) {
        self.storage_holds.remove(&id);
    }

    pub fn remove_compute_collection(&mut self, instance_id: ComputeInstanceId, id: GlobalId) {
        self.compute_holds.remove(&(instance_id, id));
    }

    /// Returns a new ReadHolds containing only the holds for collections in `id_bundle`.
    pub fn subset(&self, id_bundle: &CollectionIdBundle) -> ReadHolds {
        let mut result = ReadHolds::new();

        for id in &id_bundle.storage_ids {
            if let Some(hold) = self.storage_holds.get(id) {
                result.storage_holds.insert(*id, hold.clone());
            }
        }

        for (instance_id, ids) in &id_bundle.compute_ids {
            for id in ids {
                if let Some(hold) = self.compute_holds.get(&(*instance_id, *id)) {
                    result
                        .compute_holds
                        .insert((*instance_id, *id), hold.clone());
                }
            }
        }

        result
    }
}

impl ReadHolds {
    pub fn least_valid_read(&self) -> Antichain<Timestamp> {
        let mut since = Antichain::from_elem(Timestamp::minimum());
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
    pub fn since(&self, desired_id: &GlobalId) -> Antichain<Timestamp> {
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

    /// Extend the contained read holds with those in `other`.
    ///
    ///
    /// # Panics
    ///
    /// In contrast to [`ReadHolds::merge`], this method expects the collection
    /// IDs in `self` and `other` to be distinct and panics otherwise.
    pub(super) fn extend(&mut self, other: Self) {
        for (id, other_hold) in other.storage_holds {
            let prev = self.storage_holds.insert(id, other_hold);
            assert!(prev.is_none(), "duplicate storage read hold: {id}");
        }
        for (id, other_hold) in other.compute_holds {
            let prev = self.compute_holds.insert(id, other_hold);
            assert!(prev.is_none(), "duplicate compute read hold: {id:?}");
        }
    }
}

impl crate::coord::Coordinator {
    /// Retain serving-timeline windows for newly committed compute collections.
    /// Replica followers apply their physical compaction policies.
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

    /// Queue durable serving-timeline protection and install storage read policies.
    /// Compute compaction policies belong to replica followers.
    #[instrument(name = "coord::initialize_read_policies")]
    pub(crate) async fn initialize_read_policies(
        &mut self,
        id_bundle: &CollectionIdBundle,
        compaction_window: CompactionWindow,
    ) {
        // Lifecycle policy installation must not wait for query connections or
        // durable client publication. Protected windows are acquired after the
        // installation batch, when readable query replicas can be observed.
        for (timeline_context, id_bundle) in
            self.catalog().partition_ids_by_timeline_context(id_bundle)
        {
            if let TimelineContext::TimelineDependent(timeline) = timeline_context {
                self.ensure_timeline_state(&timeline)
                    .await
                    .pending_read_holds
                    .extend(&id_bundle);
            }
        }

        // Install read policies.
        let read_policy = ReadPolicy::from(compaction_window);

        let storage_policies = id_bundle
            .storage_ids
            .iter()
            .map(|id| (*id, read_policy.clone()))
            .collect();
        self.controller
            .storage_collections
            .set_read_policies(storage_policies);
    }

    pub(crate) fn update_storage_read_policies(&self, policies: Vec<(CatalogItemId, ReadPolicy)>) {
        let policies = policies
            .into_iter()
            .map(|(item_id, policy)| {
                // Set the read policy for all GlobalIds associated with an item.
                self.catalog()
                    .get_entry(&item_id)
                    .global_ids()
                    .map(move |gid| (gid, policy.clone()))
            })
            .flatten()
            .collect();
        self.controller
            .storage_collections
            .set_read_policies(policies);
    }

    /// Acquire request-scoped durable protection at the earliest readable time.
    pub(crate) async fn acquire_query_read_holds(
        &mut self,
        id_bundle: &CollectionIdBundle,
    ) -> Result<ReadHolds, crate::AdapterError> {
        let client = self.query_client.as_ref().ok_or_else(|| {
            crate::AdapterError::Internal("durable query read protection is not initialized".into())
        })?;
        let incarnation = client.protection.incarnation();
        let (holds, _) = self
            .acquire_client_read_protection(incarnation, id_bundle.clone(), |_| Ok(None))
            .await?;
        Ok(holds)
    }

    /// Stash transaction read holds. They will be released when the transaction
    /// is cleaned up.
    pub(crate) fn store_transaction_read_holds(
        &mut self,
        conn_id: ConnectionId,
        read_holds: ReadHolds,
    ) {
        use std::collections::btree_map::Entry;

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
