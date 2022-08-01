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

use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};

use mz_compute_client::controller::ComputeInstanceId;
use mz_repr::{GlobalId, Timestamp};
use mz_stash::Append;
use mz_storage::controller::ReadPolicy;

use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::timeline::TimelineState;

/// Information about the read capability requirements of a collection.
///
/// This type tracks both a default policy, as well as various holds that may
/// be expressed, as by transactions to ensure collections remain readable.
pub(crate) struct ReadCapability<T = mz_repr::Timestamp>
where
    T: timely::progress::Timestamp,
{
    /// The default read policy for the collection when no holds are present.
    pub(crate) base_policy: ReadPolicy<T>,
    /// Holds expressed by transactions, that should prevent compaction.
    pub(crate) holds: MutableAntichain<T>,
}

impl<T: timely::progress::Timestamp> From<ReadPolicy<T>> for ReadCapability<T> {
    fn from(base_policy: ReadPolicy<T>) -> Self {
        Self {
            base_policy,
            holds: MutableAntichain::new(),
        }
    }
}

impl<T: timely::progress::Timestamp> ReadCapability<T> {
    /// Acquires the effective read policy, reflecting both the base policy and any holds.
    pub(crate) fn policy(&self) -> ReadPolicy<T> {
        // TODO: This could be "optimized" when `self.holds.frontier` is empty.
        ReadPolicy::Multiple(vec![
            ReadPolicy::ValidFrom(self.holds.frontier().to_owned()),
            self.base_policy.clone(),
        ])
    }
}

/// Relevant information for acquiring or releasing a bundle of read holds.
#[derive(Clone)]
pub(crate) struct ReadHolds<T> {
    pub(crate) time: T,
    pub(crate) id_bundle: CollectionIdBundle,
}

impl<T> ReadHolds<T> {
    /// Return empty `ReadHolds` at `time`.
    pub fn new(time: T) -> Self {
        ReadHolds {
            time,
            id_bundle: CollectionIdBundle::default(),
        }
    }
}

impl<S: Append + 'static> crate::coord::Coordinator<S> {
    /// Initialize the storage read policies.
    ///
    /// This should be called only after a storage collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    pub(crate) async fn initialize_storage_read_policies(
        &mut self,
        ids: Vec<GlobalId>,
        compaction_window_ms: Option<Timestamp>,
    ) {
        self.initialize_read_policies(
            CollectionIdBundle {
                storage_ids: ids.into_iter().collect(),
                compute_ids: BTreeMap::new(),
            },
            compaction_window_ms,
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
        compaction_window_ms: Option<Timestamp>,
    ) {
        let mut compute_ids: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        compute_ids.insert(instance, ids.into_iter().collect());
        self.initialize_read_policies(
            CollectionIdBundle {
                storage_ids: BTreeSet::new(),
                compute_ids,
            },
            compaction_window_ms,
        )
        .await;
    }

    /// Initialize the storage and compute read policies.
    ///
    /// This should be called only after a collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn initialize_read_policies(
        &mut self,
        id_bundle: CollectionIdBundle,
        compaction_window_ms: Option<Timestamp>,
    ) {
        // We do compute first, and they may result in additional storage policy effects.
        for (compute_instance, compute_ids) in id_bundle.compute_ids.iter() {
            let mut compute_policy_updates = Vec::new();
            for id in compute_ids.iter() {
                let policy = match compaction_window_ms {
                    Some(time) => ReadPolicy::lag_writes_by(time),
                    None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
                };

                let mut read_capability: ReadCapability<_> = policy.into();

                if let Some(timeline) = self.get_timeline(*id) {
                    let TimelineState { read_holds, .. } =
                        self.ensure_timeline_state(timeline).await;
                    read_capability
                        .holds
                        .update_iter(Some((read_holds.time, 1)));
                    read_holds
                        .id_bundle
                        .compute_ids
                        .entry(*compute_instance)
                        .or_default()
                        .insert(*id);
                }

                self.read_capability.insert(*id, read_capability);
                compute_policy_updates.push((*id, self.read_capability[&id].policy()));
            }
            self.controller
                .compute_mut(*compute_instance)
                .unwrap()
                .set_read_policy(compute_policy_updates)
                .await
                .unwrap();
        }

        let mut storage_policy_updates = Vec::new();
        for id in id_bundle.storage_ids.iter() {
            let policy = match compaction_window_ms {
                Some(time) => ReadPolicy::lag_writes_by(time),
                None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
            };

            let mut read_capability: ReadCapability<_> = policy.into();

            if let Some(timeline) = self.get_timeline(*id) {
                let TimelineState { read_holds, .. } = self.ensure_timeline_state(timeline).await;
                read_capability
                    .holds
                    .update_iter(Some((read_holds.time, 1)));
                read_holds.id_bundle.storage_ids.insert(*id);
            }

            self.read_capability.insert(*id, read_capability);
            storage_policy_updates.push((*id, self.read_capability[&id].policy()));
        }
        self.controller
            .storage_mut()
            .set_read_policy(storage_policy_updates)
            .await
            .unwrap();
    }

    pub(crate) async fn update_compute_base_read_policy(
        &mut self,
        compute_instance: ComputeInstanceId,
        id: GlobalId,
        base_policy: ReadPolicy<mz_repr::Timestamp>,
    ) {
        let capability = self
            .read_capability
            .get_mut(&id)
            .expect("coord out of sync");
        capability.base_policy = base_policy;
        self.controller
            .compute_mut(compute_instance)
            .unwrap()
            .set_read_policy(vec![(id, capability.policy())])
            .await
            .unwrap();
    }

    /// Drop read policy for `id`.
    ///
    /// Returns true if `id` had a read policy and false otherwise
    pub(crate) fn drop_read_policy(&mut self, id: &GlobalId) -> bool {
        self.read_capability.remove(id).is_some()
    }

    /// Acquire read holds on the indicated collections at the indicated time.
    ///
    /// This method will panic if the holds cannot be acquired. In the future,
    /// it would be polite to have it error instead, as it is not unrecoverable.
    pub(crate) async fn acquire_read_holds(&mut self, read_holds: &ReadHolds<mz_repr::Timestamp>) {
        // Update STORAGE read policies.
        let mut policy_changes = Vec::new();
        let storage = self.controller.storage_mut();
        for id in read_holds.id_bundle.storage_ids.iter() {
            let collection = storage.collection(*id).unwrap();
            assert!(
                collection
                    .read_capabilities
                    .frontier()
                    .less_equal(&read_holds.time),
                "Storage collection {:?} has read frontier {:?} not less-equal desired time {:?}",
                id,
                collection.read_capabilities.frontier(),
                read_holds.time,
            );
            let read_needs = self.read_capability.get_mut(id).unwrap();
            read_needs.holds.update_iter(Some((read_holds.time, 1)));
            policy_changes.push((*id, read_needs.policy()));
        }
        storage.set_read_policy(policy_changes).await.unwrap();
        // Update COMPUTE read policies
        for (compute_instance, compute_ids) in read_holds.id_bundle.compute_ids.iter() {
            let mut policy_changes = Vec::new();
            let mut compute = self.controller.compute_mut(*compute_instance).unwrap();
            for id in compute_ids.iter() {
                let collection = compute.as_ref().collection(*id).unwrap();
                assert!(collection
                    .read_capabilities
                    .frontier()
                    .less_equal(&read_holds.time),
                    "Compute collection {:?} (instance {:?}) has read frontier {:?} not less-equal desired time {:?}",
                    id,
                    compute_instance,
                    collection.read_capabilities.frontier(),
                    read_holds.time,
                );
                let read_needs = self.read_capability.get_mut(id).unwrap();
                read_needs.holds.update_iter(Some((read_holds.time, 1)));
                policy_changes.push((*id, read_needs.policy()));
            }
            compute.set_read_policy(policy_changes).await.unwrap();
        }
    }
    /// Update the timestamp of the read holds on the indicated collections from the
    /// indicated time within `read_holds` to `new_time`.
    ///
    /// This method relies on a previous call to `acquire_read_holds` with the same
    /// `read_holds` argument or a previous call to `update_read_hold` that returned
    /// `read_holds`, and its behavior will be erratic if called on anything else.
    pub(super) async fn update_read_hold(
        &mut self,
        mut read_holds: ReadHolds<mz_repr::Timestamp>,
        new_time: mz_repr::Timestamp,
    ) -> ReadHolds<mz_repr::Timestamp> {
        let ReadHolds {
            time: old_time,
            id_bundle:
                CollectionIdBundle {
                    storage_ids,
                    compute_ids,
                },
        } = &read_holds;

        // Update STORAGE read policies.
        let mut policy_changes = Vec::new();
        let storage = self.controller.storage_mut();
        for id in storage_ids.iter() {
            let collection = storage.collection(*id).unwrap();
            assert!(collection
                .read_capabilities
                .frontier()
                .less_equal(&new_time),
                "Storage collection {:?} has read frontier {:?} not less-equal new time {:?}; old time: {:?}",
                id,
                collection.read_capabilities.frontier(),
                new_time,
                old_time,
            );
            let read_needs = self.read_capability.get_mut(id).unwrap();
            read_needs.holds.update_iter(Some((new_time, 1)));
            read_needs.holds.update_iter(Some((*old_time, -1)));
            policy_changes.push((*id, read_needs.policy()));
        }
        storage.set_read_policy(policy_changes).await.unwrap();
        // Update COMPUTE read policies
        for (compute_instance, compute_ids) in compute_ids.iter() {
            let mut policy_changes = Vec::new();
            let mut compute = self.controller.compute_mut(*compute_instance).unwrap();
            for id in compute_ids.iter() {
                let collection = compute.as_ref().collection(*id).unwrap();
                assert!(collection
                    .read_capabilities
                    .frontier()
                    .less_equal(&new_time),
                    "Compute collection {:?} (instance {:?}) has read frontier {:?} not less-equal new time {:?}; old time: {:?}",
                    id,
                    compute_instance,
                    collection.read_capabilities.frontier(),
                    new_time,
                    old_time,
                );
                let read_needs = self.read_capability.get_mut(id).unwrap();
                read_needs.holds.update_iter(Some((new_time, 1)));
                read_needs.holds.update_iter(Some((*old_time, -1)));
                policy_changes.push((*id, read_needs.policy()));
            }
            compute.set_read_policy(policy_changes).await.unwrap();
        }

        read_holds.time = new_time;
        read_holds
    }
    /// Release read holds on the indicated collections at the indicated time.
    ///
    /// This method relies on a previous call to `acquire_read_holds` with the same
    /// argument, and its behavior will be erratic if called on anything else, or if
    /// called more than once on the same bundle of read holds.
    pub(super) async fn release_read_hold(&mut self, read_holds: &ReadHolds<mz_repr::Timestamp>) {
        let ReadHolds {
            time,
            id_bundle:
                CollectionIdBundle {
                    storage_ids,
                    compute_ids,
                },
        } = read_holds;

        // Update STORAGE read policies.
        let mut policy_changes = Vec::new();
        for id in storage_ids.iter() {
            // It's possible that a concurrent DDL statement has already dropped this GlobalId
            if let Some(read_needs) = self.read_capability.get_mut(id) {
                read_needs.holds.update_iter(Some((*time, -1)));
                policy_changes.push((*id, read_needs.policy()));
            }
        }
        self.controller
            .storage_mut()
            .set_read_policy(policy_changes)
            .await
            .unwrap();
        // Update COMPUTE read policies
        for (compute_instance, compute_ids) in compute_ids.iter() {
            let mut policy_changes = Vec::new();
            for id in compute_ids.iter() {
                // It's possible that a concurrent DDL statement has already dropped this GlobalId
                if let Some(read_needs) = self.read_capability.get_mut(id) {
                    read_needs.holds.update_iter(Some((*time, -1)));
                    policy_changes.push((*id, read_needs.policy()));
                }
            }
            if let Some(mut compute) = self.controller.compute_mut(*compute_instance) {
                compute.set_read_policy(policy_changes).await.unwrap();
            }
        }
    }
}
