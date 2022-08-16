// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rehydration of storage hosts.
//!
//! Rehydration is the process of bringing a crashed `storaged` process back
//! up to date. The [`RehydratingStorageClient`] records all commands it
//! observes in a minimal form. If it observes a send or receive failure while
//! communicating with the underlying client, it will reconnect the client and
//! replay the command stream.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use differential_dataflow::lattice::Lattice;
use futures::Stream;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::Codec64;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::warn;

use mz_build_info::BuildInfo;
use mz_ore::retry::Retry;
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_repr::GlobalId;
use mz_service::client::GenericClient;

use crate::protocol::client::{
    ExportSinkCommand, IngestSourceCommand, StorageClient, StorageCommand, StorageGrpcClient,
    StorageResponse,
};

/// A storage client that replays the command stream on failure.
///
/// See the [module documentation](self) for details.
#[derive(Debug)]
pub struct RehydratingStorageClient<T> {
    command_tx: UnboundedSender<StorageCommand<T>>,
    response_rx: UnboundedReceiverStream<StorageResponse<T>>,
    _task: AbortOnDropHandle<()>,
}

impl<T> RehydratingStorageClient<T>
where
    T: Timestamp + Lattice + Codec64,
    StorageGrpcClient: StorageClient<T>,
{
    /// Creates a `RehydratingStorageClient` for a storage host with the given
    /// network address.
    pub fn new(
        addr: String,
        build_info: &'static BuildInfo,
        persist: Arc<Mutex<PersistClientCache>>,
    ) -> RehydratingStorageClient<T> {
        let (command_tx, command_rx) = unbounded_channel();
        let (response_tx, response_rx) = unbounded_channel();
        let mut task = RehydrationTask {
            addr,
            build_info,
            command_rx,
            response_tx,
            ingestions: BTreeMap::new(),
            exports: BTreeMap::new(),
            uppers: HashMap::new(),
            initialized: false,
            persist,
        };
        let task = mz_ore::task::spawn(|| "rehydration", async move { task.run().await });
        RehydratingStorageClient {
            command_tx,
            response_rx: UnboundedReceiverStream::new(response_rx),
            _task: task.abort_on_drop(),
        }
    }

    /// Sends a command to the underlying client.
    pub fn send(&mut self, cmd: StorageCommand<T>) {
        self.command_tx
            .send(cmd)
            .expect("rehydration task should not drop first");
    }

    /// Returns a stream that produces responses from the underlying client.
    pub fn response_stream(&mut self) -> impl Stream<Item = StorageResponse<T>> + '_ {
        &mut self.response_rx
    }
}

/// A task that manages rehydration.
struct RehydrationTask<T> {
    /// The network address of the storage host.
    addr: String,
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the storage host are delivered.
    command_rx: UnboundedReceiver<StorageCommand<T>>,
    /// A channel upon which responses from the storage host are delivered.
    response_tx: UnboundedSender<StorageResponse<T>>,
    /// The ingestions that have been observed.
    ingestions: BTreeMap<GlobalId, IngestSourceCommand<T>>,
    /// The exports that have been observed.
    exports: BTreeMap<GlobalId, ExportSinkCommand<T>>,
    /// The upper frontier information received.
    uppers: HashMap<GlobalId, (Antichain<T>, MutableAntichain<T>)>,
    /// Set to `true` once [`StorageCommand::InitializationComplete`] has been
    /// observed.
    initialized: bool,
    /// A handle to Persist
    persist: Arc<Mutex<PersistClientCache>>,
}

enum RehydrationTaskState {
    /// The storage host should be (re)hydrated.
    Rehydrate,
    /// Communication with the storage host is live. Commands and responses should
    /// be forwarded until an error occurs.
    Pump { client: StorageGrpcClient },
    /// The caller has asked us to shut down communication with this storage
    /// host.
    Done,
}

impl<T> RehydrationTask<T>
where
    T: Timestamp + Lattice + Codec64,
    StorageGrpcClient: StorageClient<T>,
{
    async fn run(&mut self) {
        let mut state = RehydrationTaskState::Rehydrate;
        loop {
            state = match state {
                RehydrationTaskState::Rehydrate => self.step_rehydrate().await,
                RehydrationTaskState::Pump { client } => self.step_pump(client).await,
                RehydrationTaskState::Done => break,
            }
        }
    }

    async fn step_rehydrate(&mut self) -> RehydrationTaskState {
        // Zero out frontiers.
        for (_id, (_, frontiers)) in self.uppers.iter_mut() {
            *frontiers = MutableAntichain::new_bottom(T::minimum());
        }

        // Reconnect to the storage host.
        let client = Retry::default()
            .clamp_backoff(Duration::from_secs(32))
            .retry_async(|_| {
                let addr = self.addr.clone();
                let version = self.build_info.semver_version();
                async move {
                    match StorageGrpcClient::connect(addr, version).await {
                        Ok(client) => Ok(client),
                        Err(e) => {
                            warn!("error connecting to storage host, retrying: {e}");
                            Err(e)
                        }
                    }
                }
            })
            .await
            .expect("retry retries forever");

        for ingest in self.ingestions.values_mut() {
            ingest.resume_upper = ingest
                .description
                .get_resume_upper::<T>(Arc::clone(&self.persist))
                .await;
        }

        // Rehydrate all commands.
        let mut commands = vec![
            StorageCommand::IngestSources(self.ingestions.values().cloned().collect()),
            StorageCommand::ExportSinks(self.exports.values().cloned().collect()),
        ];
        if self.initialized {
            commands.push(StorageCommand::InitializationComplete)
        }
        self.send_commands(client, commands).await
    }

    async fn step_pump(&mut self, mut client: StorageGrpcClient) -> RehydrationTaskState {
        select! {
            // Command from controller to forward to storage host.
            command = self.command_rx.recv() => match command {
                None => RehydrationTaskState::Done,
                Some(command) => {
                    self.absorb_command(&command);
                    self.send_commands(client, vec![command]).await
                }
            },
            // Response from storage host to forward to controller.
            response = client.recv() => {
                let response = match response.transpose() {
                    None => {
                        // In the future, if a storage host politely hangs up,
                        // we might want to take it as a signal that a new
                        // controller has taken over. For now we just try to
                        // reconnect.
                        Err(anyhow!("storage host unexpectedly gracefully terminated connection"))
                    }
                    Some(response) => response,
                };

                self.send_response(client, response).await
            }
        }
    }

    async fn send_commands(
        &mut self,
        mut client: StorageGrpcClient,
        commands: impl IntoIterator<Item = StorageCommand<T>>,
    ) -> RehydrationTaskState {
        for command in commands {
            if let Err(e) = client.send(command).await {
                return self.send_response(client, Err(e)).await;
            }
        }
        RehydrationTaskState::Pump { client }
    }

    async fn send_response(
        &mut self,
        client: StorageGrpcClient,
        response: Result<StorageResponse<T>, anyhow::Error>,
    ) -> RehydrationTaskState {
        match response {
            Ok(response) => {
                if let Some(response) = self.absorb_response(response) {
                    if self.response_tx.send(response).is_err() {
                        RehydrationTaskState::Done
                    } else {
                        RehydrationTaskState::Pump { client }
                    }
                } else {
                    RehydrationTaskState::Pump { client }
                }
            }
            Err(e) => {
                warn!("storage host produced error, reconnecting: {e}");
                RehydrationTaskState::Rehydrate
            }
        }
    }

    fn absorb_command(&mut self, command: &StorageCommand<T>) {
        match command {
            StorageCommand::InitializationComplete => self.initialized = true,
            StorageCommand::IngestSources(ingestions) => {
                for ingestion in ingestions {
                    self.ingestions.insert(ingestion.id, ingestion.clone());
                    // Initialize the uppers we are tracking
                    self.uppers.insert(
                        ingestion.id,
                        (
                            Antichain::from_elem(T::minimum()),
                            MutableAntichain::new_bottom(T::minimum()),
                        ),
                    );
                }
            }
            StorageCommand::ExportSinks(exports) => {
                for export in exports {
                    self.exports.insert(export.id, export.clone());
                    // Initialize the uppers we are tracking
                    self.uppers.insert(
                        export.id,
                        (
                            Antichain::from_elem(T::minimum()),
                            MutableAntichain::new_bottom(T::minimum()),
                        ),
                    );
                }
            }
            StorageCommand::AllowCompaction(frontiers) => {
                for (id, frontier) in frontiers {
                    if frontier.is_empty() {
                        self.ingestions.remove(id);
                        self.uppers.remove(id);
                    }
                }
            }
        }
    }

    fn absorb_response(&mut self, response: StorageResponse<T>) -> Option<StorageResponse<T>> {
        match response {
            StorageResponse::FrontierUppers(mut list) => {
                for (id, changes) in list.iter_mut() {
                    if let Some((reported, tracked)) = self.uppers.get_mut(id) {
                        // Apply changes to `tracked` frontier.
                        tracked.update_iter(changes.drain());
                        // We can swap `reported` into `changes`, negated, and then use that to repopulate `reported`.
                        changes.extend(reported.iter().map(|t| (t.clone(), -1)));
                        reported.clear();
                        for (time1, _neg_one) in changes.iter() {
                            for time2 in tracked.frontier().iter() {
                                reported.insert(time1.join(time2));
                            }
                        }
                        changes.extend(reported.iter().map(|t| (t.clone(), 1)));
                        changes.compact();
                    } else {
                        // We should have initialized the uppers when we first absorbed
                        // a command, if storaged has restarted since then.
                        //
                        // If the controller has restarted since then, we should have
                        // initialized them in the initial `step_rehydrate`.
                        panic!("RehydratingStorageClient received FrontierUppers response for absent identifier {id}");
                    }
                }
                if !list.is_empty() {
                    Some(StorageResponse::FrontierUppers(list))
                } else {
                    None
                }
            }
        }
    }
}
