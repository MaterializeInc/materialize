// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller that provides an interface to a compute instance, and the storage layer below it.
//!
//! The compute controller curates the creation of indexes and sinks, the progress of readers through
//! these collections, and their eventual dropping and resource reclamation.
//!
//! The compute controller can be viewed as a partial map from `GlobalId` to collection. It is an error to
//! use an identifier before it has been "created" with `create_dataflows()`. Once created, the controller holds
//! a read capability for each output collection of a dataflow, which is manipulated with `allow_compaction()`.
//! Eventually, a collecction is dropped with either `drop_sources()` or by allowing compaction to the empty frontier.
//!
//! Created dataflows will prevent the compaction of their inputs, including other compute collections but also
//! collections managed by the storage layer. Each dataflow input is prevented from compacting beyond the allowed
//! compaction of each of its outputs, ensuring that we can recover each dataflow to its current state in case of
//! failure or other reconfiguration.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use uuid::Uuid;

use crate::client::controller::storage::StorageController;
use crate::client::replicated::ActiveReplication;
use crate::client::{ComputeClient, ComputeCommand, ComputeInstanceId};
use crate::client::{GenericClient, Peek};
use crate::logging::LoggingConfig;
use crate::DataflowDescription;
use mz_expr::GlobalId;
use mz_expr::RowSetFinishing;
use mz_repr::Row;

use super::ReadPolicy;

/// Controller state maintained for each compute instance.
#[derive(Debug)]
pub(super) struct ComputeControllerState<T> {
    pub(super) client: ActiveReplication<Box<dyn ComputeClient<T>>, T>,
    /// Tracks expressed `since` and received `upper` frontiers for indexes and sinks.
    pub(super) collections: BTreeMap<GlobalId, CollectionState<T>>,
    /// Currently outstanding peeks: identifiers and timestamps.
    pub(super) peeks: BTreeMap<uuid::Uuid, (GlobalId, T)>,
}

/// An immutable controller for a compute instance.
#[derive(Debug, Copy, Clone)]
pub struct ComputeController<'a, T> {
    pub(super) _instance: ComputeInstanceId, // likely to be needed soon
    pub(super) compute: &'a ComputeControllerState<T>,
    pub(super) storage_controller: &'a dyn StorageController<Timestamp = T>,
}

/// A mutable controller for a compute instance.
#[derive(Debug)]
pub struct ComputeControllerMut<'a, T> {
    pub(super) instance: ComputeInstanceId,
    pub(super) compute: &'a mut ComputeControllerState<T>,
    pub(super) storage_controller: &'a mut dyn StorageController<Timestamp = T>,
}

/// Errors arising from compute commands.
#[derive(Debug)]
pub enum ComputeError {
    /// Command referenced an instance that was not present.
    InstanceMissing(ComputeInstanceId),
    /// Command referenced an identifier that was not present.
    IdentifierMissing(GlobalId),
    /// Dataflow was malformed (e.g. missing `as_of`).
    DataflowMalformed,
    /// The dataflow `as_of` was not greater than the `since` of the identifier.
    DataflowSinceViolation(GlobalId),
    /// The peek `timestamp` was not greater than the `since` of the identifier.
    PeekSinceViolation(GlobalId),
    /// An error from the underlying client.
    ClientError(anyhow::Error),
}

impl From<anyhow::Error> for ComputeError {
    fn from(error: anyhow::Error) -> Self {
        Self::ClientError(error)
    }
}

impl<T> ComputeControllerState<T>
where
    T: Timestamp + Lattice,
{
    pub(super) async fn new(
        // client: ActiveReplication<Box<dyn ComputeClient<T>>, T>,
        logging: &Option<LoggingConfig>,
    ) -> Result<Self, anyhow::Error> {
        let mut collections = BTreeMap::default();
        if let Some(logging_config) = logging.as_ref() {
            for id in logging_config.log_identifiers() {
                collections.insert(
                    id,
                    CollectionState::new(
                        Antichain::from_elem(T::minimum()),
                        Vec::new(),
                        Vec::new(),
                    ),
                );
            }
        }
        let mut client = crate::client::replicated::ActiveReplication::default();
        client
            .send(ComputeCommand::CreateInstance(logging.clone()))
            .await?;

        Ok(Self {
            client,
            collections,
            peeks: Default::default(),
        })
    }
}

// Public interface
impl<'a, T> ComputeController<'a, T>
where
    T: Timestamp + Lattice,
{
    /// Acquires an immutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage(&self) -> &dyn crate::client::controller::StorageController<Timestamp = T> {
        self.storage_controller
    }

    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&'a CollectionState<T>, ComputeError> {
        self.compute
            .collections
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }
}

impl<'a, T> ComputeControllerMut<'a, T>
where
    T: Timestamp + Lattice,
{
    /// Constructs an immutable handle from this mutable handle.
    pub fn as_ref<'b>(&'b self) -> ComputeController<'b, T> {
        ComputeController {
            _instance: self.instance,
            storage_controller: self.storage_controller,
            compute: &self.compute,
        }
    }

    /// Acquires a mutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage_mut(
        &mut self,
    ) -> &mut dyn crate::client::controller::StorageController<Timestamp = T> {
        self.storage_controller
    }

    /// Adds a new instance replica, by name.
    pub async fn add_replica(&mut self, id: String, client: Box<dyn ComputeClient<T>>) {
        self.compute.client.add_replica(id, client).await;
    }
    /// Removes an existing instance replica, by name.
    pub fn remove_replica(&mut self, id: &str) {
        self.compute.client.remove_replica(id);
    }

    /// Creates and maintains the described dataflows, and initializes state for their output.
    ///
    /// This method creates dataflows whose inputs are still readable at the dataflow `as_of`
    /// frontier, and initializes the outputs as readable from that frontier onward.
    /// It installs read dependencies from the outputs to the inputs, so that the input read
    /// capabilities will be held back to the output read capabilities, ensuring that we are
    /// always able to return to a state that can serve the output read capabilities.
    pub async fn create_dataflows(
        &mut self,
        dataflows: Vec<DataflowDescription<crate::plan::Plan<T>, T>>,
    ) -> Result<(), ComputeError> {
        // Validate dataflows as having inputs whose `since` is less or equal to the dataflow's `as_of`.
        // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.
        for dataflow in dataflows.iter() {
            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or(ComputeError::DataflowMalformed)?;

            // Record all transitive dependencies of the outputs.
            let mut storage_dependencies = Vec::new();
            let mut compute_dependencies = Vec::new();

            // Validate sources have `since.less_equal(as_of)`.
            for (source_id, _) in dataflow.source_imports.iter() {
                let since = &self
                    .storage_controller
                    .collection(*source_id)
                    .or(Err(ComputeError::IdentifierMissing(*source_id)))?
                    .read_capabilities
                    .frontier();
                if !(<_ as timely::order::PartialOrder>::less_equal(since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }

                storage_dependencies.push(*source_id);
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for (index_id, _) in dataflow.index_imports.iter() {
                let collection = self.as_ref().collection(*index_id)?;
                let since = collection.read_capabilities.frontier();
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*index_id))?;
                } else {
                    compute_dependencies.push(*index_id);
                }
            }

            // Canonicalize depedencies.
            // Probably redundant based on key structure, but doing for sanity.
            storage_dependencies.sort();
            storage_dependencies.dedup();
            compute_dependencies.sort();
            compute_dependencies.dedup();

            // We will bump the internals of each input by the number of dependents (outputs).
            let outputs = dataflow.sink_exports.len() + dataflow.index_exports.len();
            let mut changes = ChangeBatch::new();
            for time in as_of.iter() {
                changes.update(time.clone(), outputs as i64);
            }
            // Update storage read capabilities for inputs.
            let mut storage_read_updates = storage_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.storage_controller
                .update_read_capabilities(&mut storage_read_updates)
                .await;
            // Update compute read capabilities for inputs.
            let mut compute_read_updates = compute_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.update_read_capabilities(&mut compute_read_updates)
                .await;

            // Install collection state for each of the exports.
            for (sink_id, _) in dataflow.sink_exports.iter() {
                self.compute.collections.insert(
                    *sink_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
            }
            for (index_id, _) in dataflow.index_exports.iter() {
                self.compute.collections.insert(
                    *index_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
            }
        }

        self.compute
            .client
            .send(ComputeCommand::CreateDataflows(dataflows))
            .await
            .expect("Compute command failed; unrecoverable");

        Ok(())
    }
    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    pub async fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.as_ref().validate_ids(identifiers.iter().cloned())?;

        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction(compaction_commands).await?;
        Ok(())
    }
    /// Drops the read capability for the indexes and allows their resources to be reclaimed.
    pub async fn drop_indexes(&mut self, identifiers: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.as_ref().validate_ids(identifiers.iter().cloned())?;

        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction(compaction_commands).await?;
        Ok(())
    }
    /// Initiate a peek request for the contents of `id` at `timestamp`.
    pub async fn peek(
        &mut self,
        id: GlobalId,
        key: Option<Row>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
    ) -> Result<(), ComputeError> {
        let since = self.as_ref().collection(id)?.read_capabilities.frontier();

        if !since.less_equal(&timestamp) {
            Err(ComputeError::PeekSinceViolation(id))?;
        }

        // Install a compaction hold on `id` at `timestamp`.
        let mut updates = BTreeMap::new();
        updates.insert(id, ChangeBatch::new_from(timestamp.clone(), 1));
        self.update_read_capabilities(&mut updates).await;
        self.compute.peeks.insert(uuid, (id, timestamp.clone()));

        self.compute
            .client
            .send(ComputeCommand::Peek(Peek {
                id,
                key,
                uuid,
                timestamp,
                finishing,
                map_filter_project,
            }))
            .await
            .map_err(ComputeError::from)
    }
    /// Cancels existing peek requests.
    pub async fn cancel_peeks(&mut self, uuids: &BTreeSet<Uuid>) -> Result<(), ComputeError> {
        self.remove_peeks(uuids.iter().cloned()).await;
        self.compute
            .client
            .send(ComputeCommand::CancelPeeks {
                uuids: uuids.clone(),
            })
            .await
            .map_err(ComputeError::from)
    }

    /// Downgrade the read capabilities of specific identifiers to specific frontiers.
    ///
    /// Downgrading any read capability to the empty frontier will drop the item and eventually reclaim its resources.
    async fn allow_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.as_ref()
            .validate_ids(frontiers.iter().map(|(id, _)| *id))?;
        let policies = frontiers
            .into_iter()
            .map(|(id, frontier)| (id, ReadPolicy::ValidFrom(frontier)));
        self.set_read_policy(policies.collect()).await;
        Ok(())
    }

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    pub async fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<T>)>) {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            if let Ok(collection) = self.collection_mut(id) {
                let mut new_read_capability = policy.frontier(collection.write_frontier.frontier());

                if <_ as timely::order::PartialOrder>::less_equal(
                    &collection.implied_capability,
                    &new_read_capability,
                ) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                    if !update.is_empty() {
                        read_capability_changes.insert(id, update);
                    }
                }

                collection.read_policy = policy;
            } else {
                tracing::error!("Reference to unregistered id: {:?}", id);
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes)
                .await;
        }
    }
}

// Internal interface
impl<'a, T> ComputeController<'a, T>
where
    T: Timestamp + Lattice,
{
    /// Validate that a collection exists for all identifiers, and error if any do not.
    pub fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), ComputeError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }
}

impl<'a, T> ComputeControllerMut<'a, T>
where
    T: Timestamp + Lattice,
{
    /// Acquire a mutable reference to the collection state, should it exist.
    pub(super) fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<T>, ComputeError> {
        self.compute
            .collections
            .get_mut(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }

    /// Accept write frontier updates from the compute layer.
    pub(super) async fn update_write_frontiers(&mut self, updates: &[(GlobalId, ChangeBatch<T>)]) {
        let mut read_capability_changes = BTreeMap::default();
        for (id, changes) in updates.iter() {
            let collection = self
                .collection_mut(*id)
                .expect("Reference to absent collection");

            collection
                .write_frontier
                .update_iter(changes.clone().drain());

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.frontier());
            if <_ as timely::order::PartialOrder>::less_equal(
                &collection.implied_capability,
                &new_read_capability,
            ) {
                // TODO: reuse change batch above?
                let mut update = ChangeBatch::new();
                update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                if !update.is_empty() {
                    read_capability_changes.insert(*id, update);
                }
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes)
                .await;
        }
    }

    /// Applies `updates`, propagates consequences through other read capabilities, and sends an appropriate compaction command.
    pub(super) async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) {
        // Locations to record consequences that we need to act on.
        let mut storage_todo = BTreeMap::default();
        let mut compute_net = Vec::default();
        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.collection_mut(key) {
                let changes = collection.read_capabilities.update_iter(update.drain());
                update.extend(changes);
                for id in collection.storage_dependencies.iter() {
                    storage_todo
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }
                for id in collection.compute_dependencies.iter() {
                    updates
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }
                compute_net.push((key, update));
            } else {
                // Storage presumably, but verify.
                storage_todo
                    .entry(key)
                    .or_insert_with(ChangeBatch::new)
                    .extend(update.drain())
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands.
        let mut compaction_commands = Vec::new();
        for (id, change) in compute_net.iter_mut() {
            if !change.is_empty() {
                let frontier = self
                    .as_ref()
                    .collection(*id)
                    .unwrap()
                    .read_capabilities
                    .frontier()
                    .to_owned();
                compaction_commands.push((*id, frontier));
            }
        }
        if !compaction_commands.is_empty() {
            self.compute
                .client
                .send(ComputeCommand::AllowCompaction(compaction_commands))
                .await
                .expect("Compute instance command failed; unrecoverable");
        }

        // We may have storage consequences to process.
        if !storage_todo.is_empty() {
            self.storage_controller
                .update_read_capabilities(&mut storage_todo)
                .await;
        }
    }
    /// Removes a registered peek, unblocking compaction that might have waited on it.
    pub(super) async fn remove_peeks(&mut self, peek_ids: impl IntoIterator<Item = uuid::Uuid>) {
        let mut updates = peek_ids
            .into_iter()
            .flat_map(|uuid| {
                self.compute
                    .peeks
                    .remove(&uuid)
                    .map(|(id, time)| (id, ChangeBatch::new_from(time, -1)))
            })
            .collect();
        self.update_read_capabilities(&mut updates).await;
    }
}

/// State maintained about individual collections.
#[derive(Debug)]
pub struct CollectionState<T> {
    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may also contain
    /// capabilities held by others who have read dependencies on this collection.
    pub read_capabilities: MutableAntichain<T>,
    /// The implicit capability associated with collection creation.
    pub implied_capability: Antichain<T>,
    /// The policy to use to downgrade `self.implied_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Storage identifiers on which this collection depends.
    pub storage_dependencies: Vec<GlobalId>,
    /// Compute identifiers on which this collection depends.
    pub compute_dependencies: Vec<GlobalId>,

    /// Reported progress in the write capabilities.
    ///
    /// Importantly, this is not a write capability, but what we have heard about the
    /// write capabilities of others. All future writes will have times greater than or
    /// equal to `upper_frontier.frontier()`.
    pub write_frontier: MutableAntichain<T>,
}

impl<T: Timestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
    pub fn new(
        since: Antichain<T>,
        storage_dependencies: Vec<GlobalId>,
        compute_dependencies: Vec<GlobalId>,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::ValidFrom(since),
            storage_dependencies,
            compute_dependencies,
            write_frontier: MutableAntichain::new_bottom(Timestamp::minimum()),
        }
    }

    /// Reports the current read capability.
    pub fn read_capability(&self) -> &Antichain<T> {
        &self.implied_capability
    }
}
