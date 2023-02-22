// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A friendly companion async worker that can be used by a timely storage
//! worker to do work that requires async.
//!
//! CAUTION: This is not meant for high-throughput data processing but for
//! one-off requests that we need to do every now and then.

use std::marker::PhantomData;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc;
use tracing::Instrument;

use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec64;
use mz_repr::Diff;
use mz_repr::GlobalId;
use mz_service::local::Activatable;
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::controller::ResumptionFrontierCalculator;
use mz_storage_client::types::sinks::MetadataFilled;
use mz_storage_client::types::sinks::StorageSinkDesc;
use mz_storage_client::types::sources::IngestionDescription;
use mz_storage_client::types::sources::SourceData;

/// A worker that can execute commands that come in on a channel and returns
/// responses on another channel. This is useful in places where we can't
/// normally run async code, such as the timely main loop.
#[derive(Debug)]
pub struct AsyncStorageWorker<T: Timestamp + Lattice + Codec64> {
    tx: mpsc::UnboundedSender<(tracing::Span, AsyncStorageWorkerCommand<T>)>,
    rx: crossbeam_channel::Receiver<AsyncStorageWorkerResponse<T>>,
}

/// Commands for [AsyncStorageWorker].
#[derive(Debug)]
pub enum AsyncStorageWorkerCommand<T: Timestamp + Lattice + Codec64> {
    /// Calculate a recent resumption frontier for the ingestion.
    CalculateResumeFrontier(
        GlobalId,
        IngestionDescription<CollectionMetadata>,
        PhantomData<T>,
    ),
    /// Calculate a recent as_of for the export/sink.
    CalculateExportAsOf(GlobalId, StorageSinkDesc<MetadataFilled, T>),
}

/// Responses from [AsyncStorageWorker].
#[derive(Debug)]
pub enum AsyncStorageWorkerResponse<T: Timestamp + Lattice + Codec64> {
    /// An `IngestionDescription` with a calculated, recent resume upper.
    IngestDescriptionWithResumeUpper(
        GlobalId,
        IngestionDescription<CollectionMetadata>,
        Antichain<T>,
    ),
    /// A `StorageSinkDesc` with an updated, recent `as_of`.
    UpdatedSinkDesc(GlobalId, StorageSinkDesc<MetadataFilled, T>),
}

impl<T: Timestamp + Lattice + Codec64> AsyncStorageWorker<T> {
    /// Creates a new [`AsyncStorageWorker`].
    ///
    /// IMPORTANT: The passed in `activatable` is activated when new responses
    /// are added the response channel. It is important to not sleep the thread
    /// that is reading from this via [`try_recv`](Self::try_recv) when
    /// [`is_empty`](Self::is_empty) has returned `false`.
    pub fn new<A: Activatable + Send + 'static>(
        activatable: A,
        persist_clients: Arc<PersistClientCache>,
    ) -> Self {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<(tracing::Span, _)>();
        let (response_tx, response_rx) = crossbeam_channel::unbounded();

        let mut response_tx = ActivatingSender::new(response_tx, activatable);

        mz_ore::task::spawn(|| "AsyncStorageWorker", async move {
            while let Some((span, command)) = command_rx.recv().await {
                match command {
                    AsyncStorageWorkerCommand::CalculateResumeFrontier(
                        id,
                        ingestion_description,
                        _phantom_data,
                    ) => {
                        let mut state = ingestion_description
                            .initialize_state(&persist_clients)
                            .instrument(span.clone())
                            .await;
                        let resume_upper: Antichain<T> = ingestion_description
                            .calculate_resumption_frontier(&mut state)
                            .instrument(span)
                            .await;
                        let res = response_tx.send(
                            AsyncStorageWorkerResponse::IngestDescriptionWithResumeUpper(
                                id,
                                ingestion_description,
                                resume_upper,
                            ),
                        );

                        match res {
                            Ok(_) => {
                                // All's well!
                            }
                            Err(_err) => {
                                // Receiver must have hung up.
                                break;
                            }
                        }
                    }
                    AsyncStorageWorkerCommand::CalculateExportAsOf(id, mut sink_desc) => {
                        let persist_client = persist_clients
                            .open(sink_desc.from_storage_metadata.persist_location.clone())
                            .await
                            .expect("error creating persist client");

                        let from_read_handle = persist_client
                            .open_leased_reader::<SourceData, (), T, Diff>(
                                sink_desc.from_storage_metadata.data_shard,
                                "graceful restart since",
                                // This is also `from_desc`, but this would be
                                // the _only_ usage of `from_desc` in storage,
                                // and we try to be consistent about where we
                                // get `RelationDesc`s for perist clients
                                Arc::new(sink_desc.from_storage_metadata.relation_desc.clone()),
                                Arc::new(UnitSchema),
                            )
                            .await
                            .expect("from collection disappeared");

                        let cached_as_of = &sink_desc.as_of;

                        let from_since = from_read_handle.since();
                        sink_desc.as_of = cached_as_of.maybe_fast_forward(from_since);

                        let res = response_tx
                            .send(AsyncStorageWorkerResponse::UpdatedSinkDesc(id, sink_desc));

                        match res {
                            Ok(_) => {
                                // All's well!
                            }
                            Err(_err) => {
                                // Receiver must have hung up.
                                break;
                            }
                        }
                    }
                }
            }
            tracing::trace!("shutting down async storage worker task");
        });

        Self {
            tx: command_tx,
            rx: response_rx,
        }
    }

    /// Calculates a recent resume upper for the given `IngestionDescription`.
    pub fn calculate_resume_upper(
        &self,
        id: GlobalId,
        ingestion: IngestionDescription<CollectionMetadata>,
    ) {
        self.send(AsyncStorageWorkerCommand::CalculateResumeFrontier(
            id,
            ingestion,
            PhantomData,
        ))
    }

    /// Updates the given `sink_desc` with a recent `as_of` by advancing it to
    /// the `since` of the sinked persist shard.
    pub fn calculate_export_as_of(
        &self,
        id: GlobalId,
        sink_desc: StorageSinkDesc<MetadataFilled, T>,
    ) {
        self.send(AsyncStorageWorkerCommand::CalculateExportAsOf(
            id, sink_desc,
        ))
    }

    fn send(&self, cmd: AsyncStorageWorkerCommand<T>) {
        self.tx
            .send((tracing::Span::current(), cmd))
            .expect("persist worker exited while its handle was alive")
    }

    /// Attempts to receive a message from the worker without blocking.
    ///
    /// This internally does a `try_recv` on a channel.
    pub fn try_recv(
        &self,
    ) -> Result<AsyncStorageWorkerResponse<T>, crossbeam_channel::TryRecvError> {
        self.rx.try_recv()
    }

    /// Returns `true` if there are currently no responses.
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

/// Helper that makes sure that we always activate the target when we send a
/// message.
struct ActivatingSender<T, A: Activatable> {
    tx: crossbeam_channel::Sender<T>,
    activatable: A,
}

impl<T, A: Activatable> ActivatingSender<T, A> {
    fn new(tx: crossbeam_channel::Sender<T>, activatable: A) -> Self {
        Self { tx, activatable }
    }

    fn send(&mut self, message: T) -> Result<(), crossbeam_channel::SendError<T>> {
        let res = self.tx.send(message);
        self.activatable.activate();
        res
    }
}
