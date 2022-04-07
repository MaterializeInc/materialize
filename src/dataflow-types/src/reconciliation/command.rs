// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Functionality to reconcile commands between a COMPUTE controller and a COMPUTE instance.
//!
//! The [ComputeCommandReconcile] and [StorageCommandReconcile] struct implements [GenericClient],
//! which allow the controller to reconnect after restarts. It maintains enough state to
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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use timely::progress::frontier::MutableAntichain;
use timely::progress::ChangeBatch;
use tracing::warn;

use mz_expr::GlobalId;

use crate::client::{
    ComputeClient, ComputeCommand, ComputeResponse, GenericClient, StorageClient, StorageCommand,
    StorageResponse, TimestampBindingFeedback,
};
use crate::sources::SourceConnector;
use crate::{DataflowDescription, Plan};

/// Reconcile commands targeted at a COMPUTE instance.
///
/// See the module-level documentation for details.
#[derive(Debug)]
pub struct ComputeCommandReconcile<T, C> {
    /// The client wrapped by this struct.
    client: C,
    /// Whether we've seen a `CreateInstance` command without a corresponding
    /// `DropInstance` command.
    created: bool,
    /// Dataflows by ID.
    dataflows: HashMap<GlobalId, DataflowDescription<Plan<T>, T>>,
    /// Outstanding peek identifiers, to guide responses (and which to suppress).
    peeks: HashSet<uuid::Uuid>,
    /// Stash of responses to send back to the controller.
    responses: VecDeque<ComputeResponse<T>>,
    /// Upper frontiers for indexes, sources, and sinks.
    uppers: HashMap<GlobalId, MutableAntichain<T>>,
}

#[async_trait]
impl<T, C> GenericClient<ComputeCommand<T>, ComputeResponse<T>> for ComputeCommandReconcile<T, C>
where
    C: ComputeClient<T>,
    T: timely::progress::Timestamp + Copy,
{
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        self.absorb_command(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
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

impl<T, C> ComputeCommandReconcile<T, C>
where
    C: ComputeClient<T>,
    T: timely::progress::Timestamp + Copy,
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
    fn start_tracking(&mut self, id: GlobalId) {
        let frontier = timely::progress::frontier::MutableAntichain::new_bottom(T::minimum());
        match self.uppers.entry(id) {
            Entry::Occupied(entry) => {
                // We're about to start tracking an already-bound ID. This means that the controller
                // needs to be informed about the `upper`.
                let mut change_batch = ChangeBatch::new_from(T::minimum(), -1);
                change_batch.extend(entry.get().frontier().iter().copied().map(|t| (t, 1)));
                self.responses
                    .push_back(ComputeResponse::FrontierUppers(vec![(id, change_batch)]));
            }
            Entry::Vacant(entry) => {
                entry.insert(frontier);
            }
        }
    }

    /// Stop tracking the id within an instance.
    fn stop_tracking(&mut self, id: GlobalId) {
        let previous = self.uppers.remove(&id);
        if previous.is_none() {
            warn!("Protocol error: ceasing frontier tracking for absent identifier {id:?}");
        }
        // Remove dataflow export information.
        self.dataflows.remove(&id);
    }

    /// Absorbs a response, and produces response that should be emitted.
    pub fn absorb_response(&mut self, message: ComputeResponse<T>) {
        match message {
            ComputeResponse::FrontierUppers(mut list) => {
                for (id, changes) in list.iter_mut() {
                    if let Some(frontier) = self.uppers.get_mut(id) {
                        let iter = frontier.update_iter(changes.drain());
                        changes.extend(iter);
                    } else {
                        changes.clear();
                    }
                }

                self.responses
                    .push_back(ComputeResponse::FrontierUppers(list));
            }
            ComputeResponse::PeekResponse(uuid, response) => {
                if self.peeks.remove(&uuid) {
                    self.responses
                        .push_back(ComputeResponse::PeekResponse(uuid, response));
                }
            }
            ComputeResponse::TailResponse(id, response) => {
                self.responses
                    .push_back(ComputeResponse::TailResponse(id, response));
            }
        }
    }

    async fn absorb_command(&mut self, command: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        use ComputeCommand::*;
        match command {
            CreateInstance(config) => {
                // TODO: Handle `logging` correctly when reconnecting. We currently assume that the
                // logging config stays the same.
                if !self.created {
                    if let Some(logging) = &config {
                        for id in logging.log_identifiers() {
                            if !self.uppers.contains_key(&id) {
                                self.start_tracking(id);
                            }
                        }
                    }
                    self.client.send(CreateInstance(config)).await?;
                    self.created = true;
                }
                Ok(())
            }
            cmd @ DropInstance => {
                if self.created {
                    self.created = false;
                    self.uppers.clear();
                    self.client.send(cmd).await
                } else {
                    Ok(())
                }
            }
            CreateDataflows(dataflows) => {
                let mut create = Vec::new();
                for dataflow in dataflows {
                    for id in dataflow.export_ids() {
                        self.start_tracking(id);
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
                    self.client.send(CreateDataflows(create)).await?
                }
                Ok(())
            }
            AllowCompaction(frontiers) => {
                for (id, frontier) in &frontiers {
                    if frontier.is_empty() {
                        self.stop_tracking(*id);
                    }
                }
                self.client.send(AllowCompaction(frontiers)).await
            }
            Peek(peek) => {
                self.peeks.insert(peek.uuid);
                self.client.send(ComputeCommand::Peek(peek)).await
            }
            CancelPeeks { uuids } => {
                for uuid in &uuids {
                    self.peeks.remove(uuid);
                }
                self.client.send(CancelPeeks { uuids }).await
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
    responses: VecDeque<StorageResponse<T>>,
    /// Tables to clear
    tables: HashSet<GlobalId>,
    /// Upper frontiers for indexes, sources, and sinks.
    ///
    /// The `Option<ComputeInstanceId>` uses `None` to represent Storage.
    uppers: HashMap<GlobalId, MutableAntichain<T>>,
}

#[async_trait]
impl<T, C> GenericClient<StorageCommand<T>, StorageResponse<T>> for StorageCommandReconcile<T, C>
where
    C: StorageClient<T>,
    T: timely::progress::Timestamp + Copy,
{
    async fn send(&mut self, cmd: StorageCommand<T>) -> Result<(), anyhow::Error> {
        self.absorb_command(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<StorageResponse<T>>, anyhow::Error> {
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

impl<T, C> StorageCommandReconcile<T, C>
where
    C: StorageClient<T>,
    T: timely::progress::Timestamp + Copy,
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
        let frontier = timely::progress::frontier::MutableAntichain::new_bottom(T::minimum());
        match self.uppers.entry(id) {
            Entry::Occupied(entry) => {
                // We're about to start tracking an already-bound ID. This means that the controller
                // needs to be informed about the `upper`.
                let mut change_batch = ChangeBatch::new_from(T::minimum(), -1);
                change_batch.extend(entry.get().frontier().iter().copied().map(|t| (t, 1)));
                // TODO: This is a gross hack to make things work. In practise, we should
                // keep note of all timestamp bindings and release them to the controller
                // once it (re-)connects.
                let feedback = TimestampBindingFeedback {
                    changes: vec![(id, change_batch)],
                    bindings: vec![],
                };
                self.responses
                    .push_back(StorageResponse::TimestampBindings(feedback));
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

    /// Absorbs a response, and produces response that should be emitted.
    pub fn absorb_response(&mut self, message: StorageResponse<T>) {
        match message {
            StorageResponse::TimestampBindings(mut feedback) => {
                for (id, changes) in feedback.changes.iter_mut() {
                    if let Some(frontier) = self.uppers.get_mut(id) {
                        let iter = frontier.update_iter(changes.drain());
                        changes.extend(iter);
                    } else {
                        changes.clear();
                    }
                }

                self.responses
                    .push_back(StorageResponse::TimestampBindings(feedback));
            }
            StorageResponse::LinearizedTimestamps(feedback) => self
                .responses
                .push_back(StorageResponse::LinearizedTimestamps(feedback)),
        }
    }

    async fn absorb_command(&mut self, command: StorageCommand<T>) -> Result<(), anyhow::Error> {
        use StorageCommand::*;
        match command {
            CreateInstance() => {
                if !self.created {
                    self.created = true;
                    self.client.send(StorageCommand::CreateInstance()).await?;
                } else {
                    let ids = self.tables.drain().collect();
                    self.client.send(StorageCommand::Truncate(ids)).await?;
                }
                Ok(())
            }
            DropInstance() => {
                if self.created {
                    self.created = false;
                    self.uppers.clear();
                    self.client.send(StorageCommand::DropInstance()).await?;
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
                    self.client.send(CreateSources(create)).await?;
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
                self.client.send(RenderSources(sources)).await
            }
            AllowCompaction(frontiers) => {
                for (id, frontier) in &frontiers {
                    if frontier.is_empty() {
                        self.stop_tracking(*id);
                    }
                }
                self.client.send(AllowCompaction(frontiers)).await
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
                    .send(Insert {
                        id,
                        updates: filtered_updates,
                    })
                    .await
            }
            cmd @ Truncate { .. } => self.client.send(cmd).await,
            DurabilityFrontierUpdates(frontiers) => {
                self.client.send(DurabilityFrontierUpdates(frontiers)).await
            }
            AdvanceAllLocalInputs { advance_to } => {
                self.client.send(AdvanceAllLocalInputs { advance_to }).await
            }
        }
    }
}
