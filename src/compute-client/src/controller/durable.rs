// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TODO(teske): Document this module.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use differential_dataflow::lattice::Lattice;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_expr::RowSetFinishing;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Row};
use mz_storage::controller::{ReadPolicy, StorageController};
use mz_storage::types::sinks::{ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection};

use crate::command::{
    ComputeCommand, DataflowDescription, InstanceConfig, Peek, ReplicaId, SourceInstanceDesc,
};
use crate::logging::{LogVariant, LoggingConfig};
use crate::service::{ComputeClient, ComputeGrpcClient};

use super::replicated::{ActiveReplication, ActiveReplicationResponse};
use super::{CollectionState, ComputeError};

#[derive(Debug)]
pub(super) struct DurableState<T> {
    /// The replicas of this compute instance.
    replicas: ActiveReplication<T>,
    /// Tracks expressed `since` and received `upper` frontiers for indexes and sinks.
    collections: BTreeMap<GlobalId, CollectionState<T>>,
    /// Currently outstanding peeks: identifiers and timestamps.
    peeks: BTreeMap<Uuid, (GlobalId, T)>,
}

#[derive(Debug)]
pub(super) struct ActiveDurableState<'a, T> {
    state: &'a mut DurableState<T>,
    storage_controller: &'a mut dyn StorageController<Timestamp = T>,
}

// Public interface

impl<T> DurableState<T> {
    /// Acquire a handle to the collection state associated with `id`.
    pub(super) fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.collections
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }

    pub(super) fn activate<'a>(
        &'a mut self,
        storage_controller: &'a mut dyn StorageController<Timestamp = T>,
    ) -> ActiveDurableState<'a, T> {
        ActiveDurableState {
            state: self,
            storage_controller,
        }
    }
}

impl<T> DurableState<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub(super) fn new(build_info: &'static BuildInfo, logging: &Option<LoggingConfig>) -> Self {
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
        let mut replicas = ActiveReplication::new(build_info);
        replicas.send(ComputeCommand::CreateInstance(InstanceConfig {
            replica_id: Default::default(),
            logging: logging.clone(),
        }));

        Self {
            replicas,
            collections,
            peeks: BTreeMap::new(),
        }
    }

    pub(super) fn initialization_complete(&mut self) {
        self.replicas.send(ComputeCommand::InitializationComplete);
    }

    pub(super) async fn receive_response(&mut self) -> ActiveReplicationResponse<T> {
        self.replicas.recv().await
    }

    pub(super) fn drop(mut self) {
        assert!(
            self.replicas.get_replica_ids().next().is_none(),
            "cannot drop instances with provisioned replicas"
        );

        self.replicas.send(ComputeCommand::DropInstance);
    }
}

impl<T> ActiveDurableState<'_, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub(super) fn add_replica(
        &mut self,
        id: ReplicaId,
        addrs: Vec<String>,
        persisted_logs: HashMap<LogVariant, GlobalId>,
    ) {
        // Create ComputeState entries in ComputeController
        for id in persisted_logs.values() {
            self.state.collections.insert(
                *id,
                CollectionState::new(Antichain::from_elem(T::minimum()), Vec::new(), Vec::new()),
            );
        }

        // Enrich log collections with metadata such that they can be sent over to computed
        let persisted_logs = persisted_logs
            .into_iter()
            .map(|(variant, id)| {
                let meta = self
                    .storage_controller
                    .collection(id)
                    .expect("cannot get collection metadata")
                    .collection_metadata
                    .clone();
                (variant, (id, meta))
            })
            .collect();

        // Add the replica
        self.state.replicas.add_replica(id, addrs, persisted_logs);
    }

    pub(super) fn remove_replica(&mut self, id: ReplicaId) {
        self.state.replicas.remove_replica(id);
    }

    pub(super) async fn create_dataflows(
        &mut self,
        dataflows: Vec<DataflowDescription<crate::plan::Plan<T>, (), T>>,
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
            for source_id in dataflow.source_imports.keys() {
                let since = &self
                    .storage_controller
                    .collection(*source_id)
                    .or(Err(ComputeError::IdentifierMissing(*source_id)))?
                    .read_capabilities
                    .frontier();
                if !(timely::order::PartialOrder::less_equal(since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }

                storage_dependencies.push(*source_id);
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for index_id in dataflow.index_imports.keys() {
                let collection = self.state.collection(*index_id)?;
                let since = collection.read_capabilities.frontier();
                if !(timely::order::PartialOrder::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*index_id))?;
                } else {
                    compute_dependencies.push(*index_id);
                }
            }

            // Canonicalize dependencies.
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
                .await?;
            // Update compute read capabilities for inputs.
            let mut compute_read_updates = compute_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.update_read_capabilities(&mut compute_read_updates)
                .await?;

            // Install collection state for each of the exports.
            for sink_id in dataflow.sink_exports.keys() {
                self.state.collections.insert(
                    *sink_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
            }
            for index_id in dataflow.index_exports.keys() {
                self.state.collections.insert(
                    *index_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
            }
        }

        // Here we augment all imported sources and all exported sinks with with the appropriate
        // storage metadata needed by the compute instance.
        let mut augmented_dataflows = Vec::with_capacity(dataflows.len());
        for d in dataflows {
            let mut source_imports = BTreeMap::new();
            for (id, (si, monotonic)) in d.source_imports {
                let collection = self.storage_controller.collection(id)?;
                let desc = SourceInstanceDesc {
                    storage_metadata: collection.collection_metadata.clone(),
                    arguments: si.arguments,
                    typ: collection.description.desc.typ().clone(),
                };
                source_imports.insert(id, (desc, monotonic));
            }

            let mut sink_exports = BTreeMap::new();
            for (id, se) in d.sink_exports {
                let connection = match se.connection {
                    ComputeSinkConnection::Persist(conn) => {
                        let metadata = self
                            .storage_controller
                            .collection(id)?
                            .collection_metadata
                            .clone();
                        let conn = PersistSinkConnection {
                            value_desc: conn.value_desc,
                            storage_metadata: metadata,
                        };
                        ComputeSinkConnection::Persist(conn)
                    }
                    ComputeSinkConnection::Tail(conn) => ComputeSinkConnection::Tail(conn),
                };
                let desc = ComputeSinkDesc {
                    from: se.from,
                    from_desc: se.from_desc,
                    connection,
                    as_of: se.as_of,
                };
                sink_exports.insert(id, desc);
            }

            augmented_dataflows.push(DataflowDescription {
                source_imports,
                sink_exports,
                // The rest of the fields are identical
                index_imports: d.index_imports,
                objects_to_build: d.objects_to_build,
                index_exports: d.index_exports,
                as_of: d.as_of,
                until: d.until,
                debug_name: d.debug_name,
            });
        }

        self.state
            .replicas
            .send(ComputeCommand::CreateDataflows(augmented_dataflows));

        Ok(())
    }

    pub(super) async fn drop_collections(
        &mut self,
        ids: Vec<GlobalId>,
    ) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(ids.iter().cloned())?;

        let compaction_commands = ids.into_iter().map(|id| (id, Antichain::new())).collect();
        self.allow_compaction(compaction_commands).await?;
        Ok(())
    }

    pub(super) async fn peek(
        &mut self,
        id: GlobalId,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
    ) -> Result<(), ComputeError> {
        let since = self.state.collection(id)?.read_capabilities.frontier();

        if !since.less_equal(&timestamp) {
            Err(ComputeError::PeekSinceViolation(id))?;
        }

        // Install a compaction hold on `id` at `timestamp`.
        let mut updates = BTreeMap::new();
        updates.insert(id, ChangeBatch::new_from(timestamp.clone(), 1));
        self.update_read_capabilities(&mut updates).await?;
        self.state.peeks.insert(uuid, (id, timestamp.clone()));

        self.state.replicas.send(ComputeCommand::Peek(Peek {
            id,
            literal_constraints,
            uuid,
            timestamp,
            finishing,
            map_filter_project,
            target_replica,
            // Obtain an `OpenTelemetryContext` from the thread-local tracing
            // tree to forward it on to the compute worker.
            otel_ctx: OpenTelemetryContext::obtain(),
        }));

        Ok(())
    }

    pub(super) async fn cancel_peeks(
        &mut self,
        uuids: &BTreeSet<Uuid>,
    ) -> Result<(), ComputeError> {
        self.remove_peeks(uuids.iter().cloned()).await?;
        self.state.replicas.send(ComputeCommand::CancelPeeks {
            uuids: uuids.clone(),
        });
        Ok(())
    }

    pub(super) async fn set_read_policy(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), ComputeError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            if let Ok(collection) = self.state.collection_mut(id) {
                let mut new_read_capability = policy.frontier(collection.write_frontier.borrow());

                if timely::order::PartialOrder::less_equal(
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
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn update_write_frontiers(
        &mut self,
        updates: &[(GlobalId, Antichain<T>)],
    ) -> Result<(), ComputeError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, new_upper) in updates.iter() {
            let collection = self
                .state
                .collection_mut(*id)
                .expect("Reference to absent collection");

            collection.write_frontier.join_assign(new_upper);

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.borrow());
            if timely::order::PartialOrder::less_equal(
                &collection.implied_capability,
                &new_read_capability,
            ) {
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
                .await?;
        }

        // Tell the storage controller about new write frontiers for storage
        // collections that are advanced by compute sinks.
        // TODO(teskje): The storage controller should have a task to directly
        // keep track of the frontiers of storage collections, instead of
        // relying on others for that information.
        let storage_updates: Vec<_> = updates
            .iter()
            .filter(|(id, _)| self.storage_controller.collection(*id).is_ok())
            .cloned()
            .collect();
        self.storage_controller
            .update_write_frontiers(&storage_updates)
            .await?;

        Ok(())
    }

    pub(super) async fn remove_peeks(
        &mut self,
        peek_ids: impl IntoIterator<Item = uuid::Uuid>,
    ) -> Result<(), ComputeError> {
        let mut updates = peek_ids
            .into_iter()
            .flat_map(|uuid| {
                self.state
                    .peeks
                    .remove(&uuid)
                    .map(|(id, time)| (id, ChangeBatch::new_from(time, -1)))
            })
            .collect();
        self.update_read_capabilities(&mut updates).await?;
        Ok(())
    }
}

// Internal interface

impl<T> DurableState<T> {
    /// Acquire a mutable handle to the collection state associated with `id`.
    fn collection_mut(&mut self, id: GlobalId) -> Result<&mut CollectionState<T>, ComputeError> {
        self.collections
            .get_mut(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }
}

impl<'a, T> ActiveDurableState<'a, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), ComputeError> {
        for id in ids {
            self.state.collection(id)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) -> Result<(), ComputeError> {
        // Locations to record consequences that we need to act on.
        let mut storage_todo = BTreeMap::default();
        let mut compute_net = Vec::default();
        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.state.collection_mut(key) {
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
                    .state
                    .collection(*id)
                    .unwrap()
                    .read_capabilities
                    .frontier()
                    .to_owned();
                compaction_commands.push((*id, frontier));
            }
        }
        if !compaction_commands.is_empty() {
            self.state
                .replicas
                .send(ComputeCommand::AllowCompaction(compaction_commands));
        }

        // We may have storage consequences to process.
        if !storage_todo.is_empty() {
            self.storage_controller
                .update_read_capabilities(&mut storage_todo)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn allow_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(frontiers.iter().map(|(id, _)| *id))?;
        let policies = frontiers
            .into_iter()
            .map(|(id, frontier)| (id, ReadPolicy::ValidFrom(frontier)));
        self.set_read_policy(policies.collect()).await?;
        Ok(())
    }
}
