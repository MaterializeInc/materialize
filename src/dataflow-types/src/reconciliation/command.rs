// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Functionality to reconcile commands between a COMPUTE controller and a COMPUTE instance.
//!
//! The [ComputeCommandReconcile] and [StorageCommandReconcile] structs implement [GenericClient]
//! the controller to reconnect after restarts. It maintains enough state to
//! get a newly connected instance up-to-date and matches existing installed objects with
//! what the controller wants to provide.
//!
//! [ComputeCommandReconcile] is designed to live in a COMPUTE instance and liberally uses `assert` to
//! validate the correctness of commands. It is not intended to be part of a COMPUTE controller as
//! it might have correctness issues.
//!
//! The contract between this and the COMPUTE controller is that identifiers are not re-used and
//! describe the same object after restarts. Failure to do so will result in undefined behavior.
//!
//! The reconciliation presents to a restarted COMPUTE controller as if the COMPUTE instance was
//! restarted as well. It responds with the expected replies after a `CreateInstance` command and
//! brings the controller  up-to-date by notifying it about the current upper frontiers.
//!
//! Controllers should ignore all responses received before `CreateInstance` as those were intended
//! for the previous instance. The implementation currently does not distinguish between buffering
//! messages for a disconnected controller and talking to a live controller.
//!
//! [StorageCommandReconcile] is designed to live in a STORAGE instance.

use crate::client::{
    Command, ComputeCommand, ComputeInstanceId, ComputeResponse, GenericClient, Response,
    StorageCommand, StorageResponse, TimestampBindingFeedback,
};
use crate::sources::SourceConnector;
use crate::{DataflowDescription, Plan};
use async_trait::async_trait;
use mz_expr::GlobalId;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use timely::progress::frontier::MutableAntichain;
use timely::progress::ChangeBatch;
use tracing::warn;

/// Reconcile commands targeted at a COMPUTE instance.
///
/// See the module-level documentation for details.
#[derive(Debug)]
pub struct ComputeCommandReconcile<T, C> {
    /// The client wrapped by this struct.
    client: C,
    /// The known compute instances we're responsible for.
    created: HashSet<ComputeInstanceId>,
    /// Dataflows by ID.
    dataflows: HashMap<GlobalId, DataflowDescription<Plan>>,
    /// Outstanding peek identifiers, to guide responses (and which to suppress).
    peeks: HashSet<uuid::Uuid>,
    /// Stash of responses to send back to the controller.
    responses: VecDeque<Response>,
    /// Upper frontiers for indexes, sources, and sinks.
    ///
    /// The `Option<ComputeInstanceId>` uses `None` to represent Storage.
    uppers: HashMap<(GlobalId, ComputeInstanceId), MutableAntichain<T>>,
}

#[async_trait]
impl<C: GenericClient<Command<mz_repr::Timestamp>, Response<mz_repr::Timestamp>> + 'static>
    GenericClient<Command<mz_repr::Timestamp>, Response<mz_repr::Timestamp>>
    for ComputeCommandReconcile<mz_repr::Timestamp, C>
{
    async fn send(&mut self, cmd: Command<mz_repr::Timestamp>) -> Result<(), anyhow::Error> {
        self.absorb_command(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<Response<mz_repr::Timestamp>>, anyhow::Error> {
        if let Some(response) = self.responses.pop_front() {
            Ok(Some(response))
        } else {
            let response = self.client.recv().await;
            if let Ok(Some(response)) = response {
                self.absorb_response(response)
            }
            Ok(self.responses.pop_front())
        }
    }
}

impl<C: GenericClient<Command<mz_repr::Timestamp>, Response<mz_repr::Timestamp>>>
    ComputeCommandReconcile<mz_repr::Timestamp, C>
{
    /// Construct a new [ComputeCommandReconcile].
    ///
    /// * `client`: The client wrapped by this struct.
    pub fn new(client: C) -> Self {
        Self {
            client,
            created: Default::default(),
            dataflows: Default::default(),
            peeks: Default::default(),
            responses: Default::default(),
            uppers: Default::default(),
        }
    }

    /// Start tracking of a id within an instance.
    ///
    /// If we're already tracking this ID, it means that the controller lost connection and
    /// reconnected (or has a bug). We're updating the controller's upper to match the local state.
    fn start_tracking(&mut self, id: GlobalId, instance: ComputeInstanceId) {
        let frontier = timely::progress::frontier::MutableAntichain::new_bottom(
            <mz_repr::Timestamp as timely::progress::Timestamp>::minimum(),
        );
        match self.uppers.entry((id, instance)) {
            Entry::Occupied(entry) => {
                // We're about to start tracking an already-bound ID. This means that the controller
                // needs to be informed about the `upper`.
                let mut change_batch = ChangeBatch::new_from(
                    <mz_repr::Timestamp as timely::progress::Timestamp>::minimum(),
                    -1,
                );
                change_batch.extend(entry.get().frontier().iter().copied().map(|t| (t, 1)));
                self.responses.push_back(Response::Compute(
                    ComputeResponse::FrontierUppers(vec![(id, change_batch)]),
                    instance,
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(frontier);
            }
        }
    }

    /// Stop tracking the id within an instance.
    fn stop_tracking(&mut self, id: GlobalId, instance: ComputeInstanceId) {
        let previous = self.uppers.remove(&(id, instance));
        if previous.is_none() {
            warn!("Protocol error: ceasing frontier tracking for absent identifier {id:?}");
        }
        // Remove dataflow export information.
        self.dataflows.remove(&id);
    }

    async fn absorb_command(&mut self, command: Command) -> Result<(), anyhow::Error> {
        use Command::*;
        match command {
            Compute(command, instance) => self.absorb_compute_command(command, instance).await,
            Storage(_) => panic!("ComputeCommandReconcile cannot handle Storage commands"),
        }
    }

    /// Absorbs a response, and produces response that should be emitted.
    pub fn absorb_response(&mut self, message: Response) {
        match message {
            Response::Compute(ComputeResponse::FrontierUppers(mut list), instance) => {
                for (id, changes) in list.iter_mut() {
                    if let Some(frontier) = self.uppers.get_mut(&(*id, instance)) {
                        let iter = frontier.update_iter(changes.drain());
                        changes.extend(iter);
                    } else {
                        changes.clear();
                    }
                }

                self.responses.push_back(Response::Compute(
                    ComputeResponse::FrontierUppers(list),
                    instance,
                ));
            }
            Response::Compute(ComputeResponse::PeekResponse(uuid, response), instance) => {
                if self.peeks.remove(&uuid) {
                    self.responses.push_back(Response::Compute(
                        ComputeResponse::PeekResponse(uuid, response),
                        instance,
                    ));
                }
            }
            Response::Compute(ComputeResponse::TailResponse(id, response), instance) => {
                self.responses.push_back(Response::Compute(
                    ComputeResponse::TailResponse(id, response),
                    instance,
                ));
            }
            Response::Storage(_) => {
                panic!("ComputeCommandReconcile cannot handle Storage responses")
            }
        }
    }

    async fn absorb_compute_command(
        &mut self,
        command: ComputeCommand,
        instance: ComputeInstanceId,
    ) -> Result<(), anyhow::Error> {
        use Command::Compute;
        use ComputeCommand::*;
        match command {
            CreateInstance(config) => {
                // TODO: Handle `logging` correctly when reconnecting. We currently assume that the
                // logging config stays the same.
                if self.created.insert(instance) {
                    if let Some(logging) = &config {
                        for id in logging.log_identifiers() {
                            if !self.uppers.contains_key(&(id, instance)) {
                                self.start_tracking(id, instance);
                            }
                        }
                    }
                    self.client
                        .send(Compute(CreateInstance(config), instance))
                        .await?;
                }
                Ok(())
            }
            cmd @ DropInstance => {
                if self.created.remove(&instance) {
                    self.uppers.retain(|(_, i), _| i != &instance);
                    self.client.send(Compute(cmd, instance)).await
                } else {
                    Ok(())
                }
            }
            CreateDataflows(dataflows) => {
                let mut create = Vec::new();
                for dataflow in dataflows {
                    for id in dataflow.export_ids() {
                        self.start_tracking(id, instance);
                    }
                    match self.dataflows.entry(dataflow.global_id().unwrap()) {
                        Entry::Vacant(entry) => {
                            entry.insert(dataflow.clone());
                            create.push(dataflow);
                        }
                        Entry::Occupied(entry) => {
                            assert!(
                                entry.get().compatible_with(&dataflow),
                                "New dataflow with same ID {:?}",
                                dataflow.id
                            );
                        }
                    }
                }
                if !create.is_empty() {
                    self.client
                        .send(Compute(CreateDataflows(create), instance))
                        .await?
                }
                Ok(())
            }
            AllowCompaction(frontiers) => {
                for (id, frontier) in &frontiers {
                    if frontier.is_empty() {
                        self.stop_tracking(*id, instance);
                    }
                }
                self.client
                    .send(Compute(AllowCompaction(frontiers), instance))
                    .await
            }
            Peek {
                id,
                timestamp,
                map_filter_project,
                uuid,
                finishing,
                key,
            } => {
                self.peeks.insert(uuid.clone());
                self.client
                    .send(Compute(
                        ComputeCommand::Peek {
                            id,
                            timestamp,
                            map_filter_project,
                            uuid,
                            finishing,
                            key,
                        },
                        instance,
                    ))
                    .await
            }
            CancelPeeks { uuids } => {
                for uuid in &uuids {
                    self.peeks.remove(uuid);
                }
                self.client
                    .send(Compute(CancelPeeks { uuids }, instance))
                    .await
            }
        }
    }
}

/// Reconcile commands targeted at a STORAGE instance.
///
/// Warning: This is not yet ready for production and is known to be incorrect!
///
/// See the module-level documentation for details.
#[derive(Debug)]
pub struct StorageCommandReconcile<T, C> {
    /// The client wrapped by this struct.
    client: C,
    /// Has the storage instance been initialized?
    created: bool,
    /// Stash of responses to send back to the controller.
    responses: VecDeque<Response>,
    /// Tables to clear
    tables: HashSet<GlobalId>,
    /// Upper frontiers for indexes, sources, and sinks.
    ///
    /// The `Option<ComputeInstanceId>` uses `None` to represent Storage.
    uppers: HashMap<GlobalId, MutableAntichain<T>>,
}

#[async_trait]
impl<C: GenericClient<Command<mz_repr::Timestamp>, Response<mz_repr::Timestamp>> + 'static>
    GenericClient<Command<mz_repr::Timestamp>, Response<mz_repr::Timestamp>>
    for StorageCommandReconcile<mz_repr::Timestamp, C>
{
    async fn send(&mut self, cmd: Command<mz_repr::Timestamp>) -> Result<(), anyhow::Error> {
        self.absorb_command(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<Response<mz_repr::Timestamp>>, anyhow::Error> {
        // TODO: This is implementation matches the one for ComputeCommandReconcile
        if let Some(response) = self.responses.pop_front() {
            Ok(Some(response))
        } else {
            let response = self.client.recv().await;
            if let Ok(Some(response)) = response {
                self.absorb_response(response)
            }
            Ok(self.responses.pop_front())
        }
    }
}

impl<C: GenericClient<Command<mz_repr::Timestamp>, Response<mz_repr::Timestamp>>>
    StorageCommandReconcile<mz_repr::Timestamp, C>
{
    /// Construct a new [StorageCommandReconcile].
    ///
    /// * `client`: The client wrapped by this struct.
    pub fn new(client: C) -> Self {
        warn!("StorageCommandReconcile is not yet fully implemented, use with caution.");
        Self {
            client,
            created: Default::default(),
            responses: Default::default(),
            tables: Default::default(),
            uppers: Default::default(),
        }
    }

    /// Start tracking of a id within an instance.
    ///
    /// If we're already tracking this ID, it means that the controller lost connection and
    /// reconnected (or has a bug). We're updating the controller's upper to match the local state.
    fn start_tracking(&mut self, id: GlobalId) {
        let frontier = timely::progress::frontier::MutableAntichain::new_bottom(
            <mz_repr::Timestamp as timely::progress::Timestamp>::minimum(),
        );
        match self.uppers.entry(id) {
            Entry::Occupied(entry) => {
                // We're about to start tracking an already-bound ID. This means that the controller
                // needs to be informed about the `upper`.
                let mut change_batch = ChangeBatch::new_from(
                    <mz_repr::Timestamp as timely::progress::Timestamp>::minimum(),
                    -1,
                );
                change_batch.extend(entry.get().frontier().iter().copied().map(|t| (t, 1)));
                // TODO: This is a gross hack to make things work. In practise, we should
                // keep note of all timestamp bindings and release them to the controller
                // once it (re-)connects.
                let feedback = TimestampBindingFeedback {
                    changes: vec![(id, change_batch)],
                    bindings: vec![],
                };
                self.responses
                    .push_back(Response::Storage(StorageResponse::TimestampBindings(
                        feedback,
                    )));
            }
            Entry::Vacant(entry) => {
                entry.insert(frontier);
            }
        }
    }

    /// Stop tracking the id within the instance.
    fn stop_tracking(&mut self, id: GlobalId) {
        let previous = self.uppers.remove(&id);
        if previous.is_none() {
            warn!("Protocol error: ceasing frontier tracking for absent identifier {id:?}");
        }
        self.tables.remove(&id);
    }

    async fn absorb_command(&mut self, command: Command) -> Result<(), anyhow::Error> {
        use Command::*;
        match command {
            Compute(_, _) => panic!("StorageCommandReconcile cannot handle Compute commands"),
            Storage(command) => self.absorb_storage_command(command).await,
        }
    }

    /// Absorbs a response, and produces response that should be emitted.
    pub fn absorb_response(&mut self, message: Response) {
        match message {
            Response::Compute(_, _) => {
                panic!("StorageCommandReconcile cannot handle Compute responses")
            }
            Response::Storage(StorageResponse::TimestampBindings(mut feedback)) => {
                for (id, changes) in feedback.changes.iter_mut() {
                    if let Some(frontier) = self.uppers.get_mut(id) {
                        let iter = frontier.update_iter(changes.drain());
                        changes.extend(iter);
                    } else {
                        changes.clear();
                    }
                }

                self.responses
                    .push_back(Response::Storage(StorageResponse::TimestampBindings(
                        feedback,
                    )));
            }
            Response::Storage(StorageResponse::LinearizedTimestamps(feedback)) => self
                .responses
                .push_back(Response::Storage(StorageResponse::LinearizedTimestamps(
                    feedback,
                ))),
        }
    }

    async fn absorb_storage_command(
        &mut self,
        command: StorageCommand,
    ) -> Result<(), anyhow::Error> {
        use Command::Storage;
        use StorageCommand::*;
        match command {
            CreateInstance() => {
                if !self.created {
                    self.created = true;
                    self.client
                        .send(Command::Storage(StorageCommand::CreateInstance()))
                        .await?;
                } else {
                    let ids = self.tables.drain().collect();
                    self.client
                        .send(Command::Storage(StorageCommand::Truncate(ids)))
                        .await?;
                }
                Ok(())
            }
            DropInstance() => {
                if self.created {
                    self.created = false;
                    self.uppers.clear();
                    self.client
                        .send(Command::Storage(StorageCommand::DropInstance()))
                        .await?;
                }
                Ok(())
            }
            CreateSources(sources) => {
                let mut create = Vec::new();
                for source in sources {
                    let id = source.id;
                    if !self.uppers.contains_key(&id) {
                        if let SourceConnector::Local { .. } = &source.desc.connector {
                            self.tables.insert(id);
                        }
                        create.push(source);
                        self.start_tracking(id);
                    }
                    // TODO: Validate the `source` == `previous source`
                }
                if !create.is_empty() {
                    self.client.send(Storage(CreateSources(create))).await?;
                }
                Ok(())
            }
            RenderSources(sources) => {
                // let mut filtered_sources = Vec::with_capacity(sources.len());
                // for (name, id, as_of, imports) in sources {
                //     if !self.uppers.contains_key(&(id, None)) {
                //         filtered_sources.push((name, id, as_of, imports));
                //     }
                //     // TODO: Validate the `source` == `previous source`
                // }
                self.client.send(Storage(RenderSources(sources))).await
            }
            AllowCompaction(frontiers) => {
                for (id, frontier) in &frontiers {
                    if frontier.is_empty() {
                        self.stop_tracking(*id);
                    }
                }
                self.client.send(Storage(AllowCompaction(frontiers))).await
            }
            Insert { id, updates } => {
                self.tables.insert(id);
                let frontier = self.uppers.get(&id).expect("Insert into unknown source");
                let mut filtered_updates = Vec::with_capacity(updates.len());
                for update in updates {
                    if frontier.less_than(&update.timestamp) {
                        filtered_updates.push(update);
                    }
                }
                self.client
                    .send(Storage(Insert {
                        id,
                        updates: filtered_updates,
                    }))
                    .await
            }
            cmd @ Truncate { .. } => self.client.send(Storage(cmd)).await,
            DurabilityFrontierUpdates(frontiers) => {
                self.client
                    .send(Storage(DurabilityFrontierUpdates(frontiers)))
                    .await
            }
            AdvanceAllLocalInputs { advance_to } => {
                self.client
                    .send(Storage(AdvanceAllLocalInputs { advance_to }))
                    .await
            }
        }
    }
}
