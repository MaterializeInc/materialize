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
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, Row};
use mz_service::local::Activatable;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, KafkaSourceConnection,
    LoadGeneratorSourceConnection, MySqlSourceConnection, PostgresSourceConnection,
    SourceConnection, SourceData, SourceEnvelope, SourceTimestamp,
};
use timely::order::PartialOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc;

use crate::source::types::SourceRender;

/// A worker that can execute commands that come in on a channel and returns
/// responses on another channel. This is useful in places where we can't
/// normally run async code, such as the timely main loop.
#[derive(Debug)]
pub struct AsyncStorageWorker<T: Timestamp + Lattice + Codec64> {
    tx: mpsc::UnboundedSender<AsyncStorageWorkerCommand>,
    rx: crossbeam_channel::Receiver<AsyncStorageWorkerResponse<T>>,
}

/// Commands for [AsyncStorageWorker].
#[derive(Debug)]
pub enum AsyncStorageWorkerCommand {
    /// Calculate a recent resumption frontier for the ingestion.
    UpdateFrontiers(GlobalId, IngestionDescription<CollectionMetadata>),
}

/// Responses from [AsyncStorageWorker].
#[derive(Debug)]
pub enum AsyncStorageWorkerResponse<T: Timestamp + Lattice + Codec64> {
    /// An `IngestionDescription` with recent as-of and resume upper frontiers.
    FrontiersUpdated {
        /// ID of the ingestion/source.
        id: GlobalId,
        /// The description of the ingestion/source.
        ingestion_description: IngestionDescription<CollectionMetadata>,
        /// The frontier beyond which ingested updates should be uncompacted. Inputs to the
        /// ingestion are guaranteed to be readable at this frontier.
        as_of: Antichain<T>,
        /// A frontier in the Materialize time domain with the property that all updates not beyond
        /// it have already been durably ingested.
        resume_uppers: BTreeMap<GlobalId, Antichain<T>>,
        /// A frontier in the source time domain with the property that all updates not beyond it
        /// have already been durably ingested.
        source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
    },
}

async fn reclock_resume_uppers<C, IntoTime>(
    id: &GlobalId,
    persist_clients: &PersistClientCache,
    ingestion_description: &IngestionDescription<CollectionMetadata>,
    as_of: Antichain<IntoTime>,
    resume_uppers: &BTreeMap<GlobalId, Antichain<IntoTime>>,
) -> BTreeMap<GlobalId, Antichain<C::Time>>
where
    C: SourceConnection + SourceRender,
    IntoTime: Timestamp + Lattice + Codec64 + Display + Sync,
{
    let metadata = &ingestion_description.ingestion_metadata;

    let persist_client = persist_clients
        .open(metadata.persist_location.clone())
        .await
        .expect("location unavailable");

    // We must load enough data in the timestamper to reclock all the requested frontiers
    let mut remap_updates = vec![];
    let mut remap_upper = as_of.clone();
    let mut subscription = None;
    for upper in resume_uppers.values() {
        // TODO(petrosagg): this feels icky, we shouldn't have exceptions in frontier reasoning
        // unless there is a good explanation as to why it is the case. It seems to me that this is
        // because in various moments in ingestion we mix uppers and sinces and try to derive one
        // from the other. Investigate if we could explicitly track natively timestamped
        // since/uppers in the controller.
        if upper.is_empty() {
            continue;
        }

        while PartialOrder::less_than(&remap_upper, upper) {
            let subscription = match subscription.as_mut() {
                Some(subscription) => subscription,
                None => {
                    let read_handle = persist_client
                        .open_leased_reader::<SourceData, (), IntoTime, Diff>(
                            metadata.remap_shard.clone().unwrap(),
                            Arc::new(ingestion_description.desc.connection.timestamp_desc()),
                            Arc::new(UnitSchema),
                            Diagnostics {
                                shard_name: ingestion_description.remap_collection_id.to_string(),
                                handle_purpose: format!("reclock for {}", id),
                            },
                            false,
                        )
                        .await
                        .expect("shard unavailable");

                    let sub = read_handle
                        .subscribe(as_of.clone())
                        .await
                        .expect("always valid to read at since");

                    subscription.insert(sub)
                }
            };
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
                    ListenEvent::Progress(f) => remap_upper = f,
                }
            }
        }
    }

    remap_updates.sort_unstable_by(|a, b| a.1.cmp(&b.1));

    // The conversion of an IntoTime frontier to a FromTime frontier has the property that all
    // messages that would be reclocked to times beyond the provided `IntoTime` frontier will be
    // beyond the returned `FromTime` frontier. This can be used to compute a safe starting point
    // to resume producing an `IntoTime` collection at a particular frontier.
    let mut source_upper = MutableAntichain::new();
    let mut source_upper_at_frontier = move |upper: &Antichain<IntoTime>| {
        if PartialOrder::less_equal(upper, &as_of) {
            Antichain::from_elem(Timestamp::minimum())
        } else {
            let idx = remap_updates.partition_point(|(_, t, _)| !upper.less_equal(t));
            source_upper.clear();
            source_upper.update_iter(
                remap_updates[0..idx]
                    .iter()
                    .map(|(from_time, _, diff)| (from_time.clone(), *diff)),
            );
            source_upper.frontier().to_owned()
        }
    };

    let mut source_resume_uppers = BTreeMap::new();
    for (id, upper) in resume_uppers {
        let source_upper = source_upper_at_frontier(upper);
        source_resume_uppers.insert(*id, source_upper);
    }
    source_resume_uppers
}

impl<T: Timestamp + Lattice + Codec64 + Display + Sync> AsyncStorageWorker<T> {
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
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = crossbeam_channel::unbounded();

        let response_tx = ActivatingSender::new(response_tx, activatable);

        mz_ore::task::spawn(|| "AsyncStorageWorker", async move {
            while let Some(command) = command_rx.recv().await {
                match command {
                    AsyncStorageWorkerCommand::UpdateFrontiers(id, ingestion_description) => {
                        // Here we update the as-of and upper(i.e resumption) frontiers of the
                        // ingestion.
                        //
                        // A good enough value for the as-of is the `meet({e.since for e in
                        // exports})` but this is not as tight as it could be because the since
                        // might be held back for unrelated to the ingestion reasons (e.g a user
                        // wanting to keep historical data). To make it tight we would need to find
                        // the maximum frontier at which all inputs to the ingestion are readable
                        // and start from there. We can find this by defining:
                        //
                        // max_readable(shard) = {(t - 1) for t in shard.upper}
                        // advanced_max_readable(shard) = advance_by(max_readable(shard), shard.since)
                        // as_of = meet({advanced_max_readable(e) for e in exports})
                        //
                        // We defer this optimization for when Materialize allows users to
                        // arbitrarily hold back collections to perform historical queries and when
                        // the storage command protocol is updated such that these calculations are
                        // performed by the controller and not here.
                        let mut resume_uppers = BTreeMap::new();

                        // TODO(petrosagg): The as_of of the ingestion should normally be based
                        // on the since frontiers of its outputs. Even though the storage
                        // controller makes sure to make downgrade decisions in an organized
                        // and ordered fashion, it then proceeds to persist them in an
                        // asynchronous and disorganized fashion to persist. The net effect is
                        // that upon restart, or upon observing the persist state like this
                        // function, one can see non-sensical results like the since of A be in
                        // advance of B even when B depends on A! This can happen because the
                        // downgrade of B gets reordered and lost. Here is our best attempt at
                        // playing detective of what the controller meant to do by blindly
                        // assuming that the since of the remap shard is a suitable since
                        // frontier without consulting the since frontier of the outputs. One
                        // day we will enforce order to chaos and this comment will be deleted.
                        let remap_shard = ingestion_description
                            .ingestion_metadata
                            .remap_shard
                            .expect("ingestions must have a remap shard");
                        let client = persist_clients
                            .open(
                                ingestion_description
                                    .ingestion_metadata
                                    .persist_location
                                    .clone(),
                            )
                            .await
                            .expect("error creating persist client");
                        let read_handle = client
                            .open_leased_reader::<SourceData, (), T, Diff>(
                                remap_shard,
                                Arc::new(ingestion_description.desc.connection.timestamp_desc()),
                                Arc::new(UnitSchema),
                                Diagnostics {
                                    shard_name: ingestion_description
                                        .remap_collection_id
                                        .to_string(),
                                    handle_purpose: format!("resumption data for {}", id),
                                },
                                false,
                            )
                            .await
                            .unwrap();
                        let as_of = read_handle.since().clone();
                        mz_ore::task::spawn(move || "deferred_expire", async move {
                            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                            read_handle.expire().await;
                        });
                        let seen_remap_shard = remap_shard.clone();

                        for (id, export) in ingestion_description.source_exports.iter() {
                            // Explicit destructuring to force a compile error when the metadata change
                            let CollectionMetadata {
                                persist_location,
                                remap_shard,
                                data_shard,
                                relation_desc,
                                txns_shard,
                            } = &export.storage_metadata;
                            assert_eq!(
                                txns_shard, &None,
                                "source {} unexpectedly using txn-wal",
                                id
                            );
                            let client = persist_clients
                                .open(persist_location.clone())
                                .await
                                .expect("error creating persist client");

                            let mut write_handle = client
                                .open_writer::<SourceData, (), T, Diff>(
                                    *data_shard,
                                    Arc::new(relation_desc.clone()),
                                    Arc::new(UnitSchema),
                                    Diagnostics {
                                        shard_name: id.to_string(),
                                        handle_purpose: format!("resumption data {}", id),
                                    },
                                )
                                .await
                                .unwrap();
                            let upper = write_handle.fetch_recent_upper().await;
                            let upper = match export.data_config.envelope {
                                // The CdcV2 envelope must re-ingest everything since the Mz frontier does not have a relation to upstream timestamps.
                                // TODO(petrosagg): move this reasoning to the controller
                                SourceEnvelope::CdcV2 if upper.is_empty() => Antichain::new(),
                                SourceEnvelope::CdcV2 => Antichain::from_elem(Timestamp::minimum()),
                                _ => upper.clone(),
                            };
                            resume_uppers.insert(*id, upper);
                            write_handle.expire().await;

                            if let Some(remap_shard) = remap_shard {
                                assert_eq!(
                                    seen_remap_shard, *remap_shard,
                                    "ingestion with multiple remap shards"
                                );
                            }
                        }

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
                        let source_resume_uppers = match ingestion_description.desc.connection {
                            GenericSourceConnection::Kafka(_) => {
                                let uppers = reclock_resume_uppers::<KafkaSourceConnection, _>(
                                    &id,
                                    &persist_clients,
                                    &ingestion_description,
                                    as_of.clone(),
                                    &resume_uppers,
                                )
                                .await;
                                to_vec_row(uppers)
                            }
                            GenericSourceConnection::Postgres(_) => {
                                let uppers = reclock_resume_uppers::<PostgresSourceConnection, _>(
                                    &id,
                                    &persist_clients,
                                    &ingestion_description,
                                    as_of.clone(),
                                    &resume_uppers,
                                )
                                .await;
                                to_vec_row(uppers)
                            }
                            GenericSourceConnection::MySql(_) => {
                                let uppers = reclock_resume_uppers::<MySqlSourceConnection, _>(
                                    &id,
                                    &persist_clients,
                                    &ingestion_description,
                                    as_of.clone(),
                                    &resume_uppers,
                                )
                                .await;
                                to_vec_row(uppers)
                            }
                            GenericSourceConnection::LoadGenerator(_) => {
                                let uppers =
                                    reclock_resume_uppers::<LoadGeneratorSourceConnection, _>(
                                        &id,
                                        &persist_clients,
                                        &ingestion_description,
                                        as_of.clone(),
                                        &resume_uppers,
                                    )
                                    .await;
                                to_vec_row(uppers)
                            }
                        };

                        let res = response_tx.send(AsyncStorageWorkerResponse::FrontiersUpdated {
                            id,
                            ingestion_description,
                            as_of,
                            resume_uppers,
                            source_resume_uppers,
                        });

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

    /// Updates the frontiers associated with the provided `IngestionDescription` to recent values.
    /// Currently this will calculate a fresh as-of for the ingestion and a fresh resumption
    /// frontier for each of the exports.
    pub fn update_frontiers(
        &self,
        id: GlobalId,
        ingestion: IngestionDescription<CollectionMetadata>,
    ) {
        self.send(AsyncStorageWorkerCommand::UpdateFrontiers(id, ingestion))
    }

    fn send(&self, cmd: AsyncStorageWorkerCommand) {
        self.tx
            .send(cmd)
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

    fn send(&self, message: T) -> Result<(), crossbeam_channel::SendError<T>> {
        let res = self.tx.send(message);
        self.activatable.activate();
        res
    }
}
