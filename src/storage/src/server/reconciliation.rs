// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Functionality to reconcile commands between a storage controller and a
//! storage server.
//!
//! The [`StorageCommandReconcile`] struct implements [`GenericClient`], which
//! allows the controller to reconnect after restarts. It maintains enough state
//! to get a newly connected instance up-to-date and matches existing installed
//! objects with what the controller wants to provide.
//!
//! [`StorageCommandReconcile`] is designed to live in a `storaged` process and
//! liberally uses `assert` to validate the correctness of commands.
//!
//! The contract between this and the storage controller is that identifiers are
//! not re-used and describe the same object after restarts. Failure to do so
//! will result in undefined behavior.
//!
//! The reconciliation presents to a restarted storage controller as if the
//! storage process was restarted as well. It brings the controller up-to-date
//! by notifying it about the current upper frontiers.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use timely::progress::Timestamp;
use tracing::warn;

use mz_repr::GlobalId;
use mz_service::client::GenericClient;
use mz_service::frontiers::FrontierReconcile;
use mz_service::grpc::GrpcServerCommand;

use crate::client::controller::CollectionMetadata;
use crate::client::sources::IngestionDescription;
use crate::client::{StorageClient, StorageCommand, StorageResponse};

/// Reconcile commands targeted at a storage host.
///
/// See the module-level documentation for details.
#[derive(Debug)]
pub struct StorageCommandReconcile<T, C> {
    /// The client wrapped by this struct.
    client: C,
    /// Ingestions by ID.
    ingestions: HashMap<GlobalId, IngestionDescription<CollectionMetadata>>,
    /// Stash of responses to send back to the controller.
    responses: VecDeque<StorageResponse<T>>,
    /// Upper frontiers for ingestions.
    uppers: FrontierReconcile<T>,
}

#[async_trait]
impl<T, C> GenericClient<GrpcServerCommand<StorageCommand<T>>, StorageResponse<T>>
    for StorageCommandReconcile<T, C>
where
    C: StorageClient<T>,
    T: Timestamp + Copy,
{
    async fn send(
        &mut self,
        cmd: GrpcServerCommand<StorageCommand<T>>,
    ) -> Result<(), anyhow::Error> {
        self.absorb_command(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<StorageResponse<T>>, anyhow::Error> {
        loop {
            if let Some(response) = self.responses.pop_front() {
                return Ok(Some(response));
            }
            match self.client.recv().await? {
                None => return Ok(None),
                Some(response) => self.absorb_response(response),
            }
        }
    }
}

impl<T, C> StorageCommandReconcile<T, C>
where
    C: StorageClient<T>,
    T: Timestamp + Copy,
{
    /// Construct a new [`StorageCommandReconcile`] that wraps a client.
    pub fn new(client: C) -> Self {
        Self {
            client,
            ingestions: HashMap::default(),
            responses: VecDeque::default(),
            uppers: FrontierReconcile::default(),
        }
    }

    /// Start tracking of a id within an instance.
    ///
    /// If we're already tracking this ID, it means that the controller lost
    /// connection and reconnected (or has a bug). We're updating the
    /// controller's upper to match the local state.
    fn start_tracking(&mut self, id: GlobalId) {
        let mut correction = self.uppers.start_tracking(id);
        if !correction.is_empty() {
            self.responses
                .push_back(StorageResponse::FrontierUppers(vec![(id, correction)]));
        }
    }

    /// Stop tracking the id within an instance.
    fn stop_tracking(&mut self, id: GlobalId) {
        let previous = self.uppers.stop_tracking(id);
        if previous.is_none() {
            warn!("Protocol error: ceasing frontier tracking for absent identifier {id:?}");
        }
        self.ingestions.remove(&id);
    }

    /// Absorbs a response, and produces response that should be emitted.
    pub fn absorb_response(&mut self, message: StorageResponse<T>) {
        match message {
            StorageResponse::FrontierUppers(mut list) => {
                self.uppers.absorb(&mut list);
                self.responses
                    .push_back(StorageResponse::FrontierUppers(list));
            }
            StorageResponse::LinearizedTimestamps(_) => {
                unreachable!("storaged processes never produce this response type");
            }
        }
    }

    async fn absorb_command(
        &mut self,
        command: GrpcServerCommand<StorageCommand<T>>,
    ) -> Result<(), anyhow::Error> {
        match command {
            GrpcServerCommand::Reconnected => {
                self.uppers.bump_epoch();
                Ok(())
            }
            GrpcServerCommand::Client(StorageCommand::IngestSources(ingestions)) => {
                let mut create = Vec::new();
                for ingestion in ingestions {
                    self.start_tracking(ingestion.id);
                    match self.ingestions.entry(ingestion.id) {
                        Entry::Vacant(entry) => {
                            entry.insert(ingestion.description.clone());
                            create.push(ingestion);
                        }
                        Entry::Occupied(entry) => {
                            assert_eq!(
                                *entry.get(),
                                ingestion.description,
                                "New ingestion with same ID {:?}",
                                ingestion.id
                            );
                        }
                    }
                }
                if !create.is_empty() {
                    self.client
                        .send(StorageCommand::IngestSources(create))
                        .await?
                }
                Ok(())
            }
            GrpcServerCommand::Client(StorageCommand::AllowCompaction(frontiers)) => {
                for (id, frontier) in &frontiers {
                    if frontier.is_empty() {
                        self.stop_tracking(*id);
                    }
                }
                self.client
                    .send(StorageCommand::AllowCompaction(frontiers))
                    .await
            }
        }
    }
}
