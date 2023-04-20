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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ListenEvent;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, Row};
use mz_service::local::Activatable;
use mz_storage_client::controller::{CollectionMetadata, CreateResumptionFrontierCalc};
use mz_storage_client::types::sources::{
    GenericSourceConnection, IngestionDescription, KafkaSourceConnection,
    LoadGeneratorSourceConnection, PostgresSourceConnection, SourceConnection, SourceData,
    SourceTimestamp, TestScriptSourceConnection,
};
use timely::order::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::source::reclock::{ReclockBatch, ReclockFollower};
use crate::source::types::SourceRender;

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
}

/// Responses from [AsyncStorageWorker].
#[derive(Debug)]
pub enum AsyncStorageWorkerResponse<T: Timestamp + Lattice + Codec64> {
    /// An `IngestionDescription` with a calculated, recent resume upper.
    IngestDescriptionWithResumeUpper(
        GlobalId,
        IngestionDescription<CollectionMetadata>,
        Antichain<T>,
        BTreeMap<GlobalId, Vec<Row>>,
    ),
}

async fn reclock_resume_frontier<C, IntoTime>(
    persist_clients: &PersistClientCache,
    ingestion_description: &IngestionDescription<CollectionMetadata>,
    resume_upper: &Antichain<IntoTime>,
    source_resume_uppers: &BTreeMap<GlobalId, Antichain<IntoTime>>,
) -> BTreeMap<GlobalId, Antichain<C::Time>>
where
    C: SourceConnection + SourceRender,
    IntoTime: Timestamp + Lattice + Codec64 + Display,
{
    if **resume_upper == [IntoTime::minimum()] {
        mz_ore::soft_assert!(
            source_resume_uppers
                .iter()
                .all(|(_, upper)| **upper == [IntoTime::minimum()]),
            "resumer upper is IntoTime::minimum(), but some collections have moved beyond it"
        );

        // Every ID's resume upper is min.
        return source_resume_uppers
            .keys()
            .map(|id| (*id, Antichain::from_elem(C::Time::minimum())))
            .collect();
    }

    let metadata = &ingestion_description.ingestion_metadata;

    let persist_client = persist_clients
        .open(metadata.persist_location.clone())
        .await
        .expect("location unavailable");

    let read_handle = persist_client
        .open_leased_reader::<SourceData, (), IntoTime, Diff>(
            metadata.remap_shard.clone().unwrap(),
            "reclock",
            Arc::new(ingestion_description.desc.connection.timestamp_desc()),
            Arc::new(UnitSchema),
        )
        .await
        .expect("shard unavailable");

    let as_of = read_handle.since().clone();

    let mut remap_updates = vec![];

    let mut subscription = read_handle
        .subscribe(as_of.clone())
        .await
        .expect("always valid to read at since");

    let mut upper = as_of.clone();
    while PartialOrder::less_than(&upper, resume_upper) {
        for event in subscription.fetch_next().await {
            match event {
                ListenEvent::Updates(updates) => {
                    for ((k, v), t, d) in updates {
                        let row: Row = k.expect("invalid binding").0.expect("invalid binding");
                        let _v: () = v.expect("invalid binding");
                        let from_ts = C::Time::decode_row(&row);
                        remap_updates.push((from_ts, t, d));
                    }
                }
                ListenEvent::Progress(f) => upper = f,
            }
        }
    }

    let reclock_batch = ReclockBatch {
        updates: remap_updates,
        upper,
    };

    // This cannot be instantiated earlier because `AsyncStorageWorker` then needs to be `+ Send +
    // Sync`.
    let mut timestamper = ReclockFollower::new(as_of);
    timestamper.push_trace_batch(reclock_batch);

    source_resume_uppers
        .into_iter()
        .map(|(id, source_resume_upper)| {
            (
                *id,
                timestamper
                    .source_upper_at_frontier(source_resume_upper.borrow())
                    .expect("enough data is loaded"),
            )
        })
        .collect()
}

impl<T: Timestamp + Lattice + Codec64 + Display> AsyncStorageWorker<T> {
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
                        let mut calc = ingestion_description
                            .create_calc(&persist_clients)
                            .instrument(span.clone())
                            .await;

                        let resume_upper =
                            calc.calculate_resumption_frontier().instrument(span).await;

                        let export_uppers = calc.get_uppers();

                        /// Convenience function to convert `BTreeMap<GlobalId, Antichain<C>>` to
                        /// `BTreeMap<GlobalId, Vec<Row>>`.
                        fn to_vec_row<T: SourceTimestamp>(
                            uppers: BTreeMap<GlobalId, Antichain<T>>,
                        ) -> BTreeMap<GlobalId, Vec<Row>> {
                            uppers
                                .into_iter()
                                .map(|(id, upper)| {
                                    (id, upper.into_iter().map(|ts| ts.encode_row()).collect())
                                })
                                .collect()
                        }

                        // Create a specialized description to be able to call the generic method
                        let source_resume_upper = match ingestion_description.desc.connection {
                            GenericSourceConnection::Kafka(_) => {
                                let uppers = reclock_resume_frontier::<KafkaSourceConnection, _>(
                                    &persist_clients,
                                    &ingestion_description,
                                    &resume_upper,
                                    &export_uppers,
                                )
                                .await;
                                to_vec_row(uppers)
                            }
                            GenericSourceConnection::Postgres(_) => {
                                let uppers =
                                    reclock_resume_frontier::<PostgresSourceConnection, _>(
                                        &persist_clients,
                                        &ingestion_description,
                                        &resume_upper,
                                        &export_uppers,
                                    )
                                    .await;
                                to_vec_row(uppers)
                            }
                            GenericSourceConnection::LoadGenerator(_) => {
                                let uppers =
                                    reclock_resume_frontier::<LoadGeneratorSourceConnection, _>(
                                        &persist_clients,
                                        &ingestion_description,
                                        &resume_upper,
                                        &export_uppers,
                                    )
                                    .await;
                                to_vec_row(uppers)
                            }
                            GenericSourceConnection::TestScript(_) => {
                                let uppers =
                                    reclock_resume_frontier::<TestScriptSourceConnection, _>(
                                        &persist_clients,
                                        &ingestion_description,
                                        &resume_upper,
                                        &export_uppers,
                                    )
                                    .await;
                                to_vec_row(uppers)
                            }
                        };

                        let res = response_tx.send(
                            AsyncStorageWorkerResponse::IngestDescriptionWithResumeUpper(
                                id,
                                ingestion_description,
                                resume_upper,
                                source_resume_upper,
                            ),
                        );

                        if let Err(_err) = res {
                            // Receiver must have hung up.
                            break;
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
