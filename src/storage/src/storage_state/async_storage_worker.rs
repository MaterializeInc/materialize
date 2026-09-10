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
use std::thread::Thread;

use differential_dataflow::lattice::Lattice;
use mz_persist_client::Diagnostics;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ListenEvent;
use mz_persist_types::Codec64;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{GlobalId, Row, TimestampManipulation};
use mz_storage_client::client::RunIngestionCommand;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sinks::StorageSinkDesc;
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, KafkaSourceConnection,
    LoadGeneratorSourceConnection, MySqlSourceConnection, PostgresSourceConnection,
    SourceConnection, SourceData, SourceEnvelope, SourceTimestamp, SqlServerSourceConnection,
};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc;

use crate::source::types::SourceRender;

/// A worker that can execute commands that come in on a channel and returns
/// responses on another channel. This is useful in places where we can't
/// normally run async code, such as the timely main loop.
#[derive(Debug)]
pub struct AsyncStorageWorker<T: Timestamp + Lattice + Codec64> {
    tx: mpsc::UnboundedSender<AsyncStorageWorkerCommand<T>>,
    rx: crossbeam_channel::Receiver<AsyncStorageWorkerResponse<T>>,
}

/// Commands for [AsyncStorageWorker].
#[derive(Debug)]
pub enum AsyncStorageWorkerCommand<T> {
    /// Calculate a recent resumption frontier for the ingestion.
    UpdateIngestionFrontiers(
        GlobalId,
        IngestionDescription<CollectionMetadata>,
        Option<Antichain<T>>,
    ),

    /// Calculate a recent resumption frontier for the Sink.
    UpdateSinkFrontiers(GlobalId, StorageSinkDesc<CollectionMetadata, T>),

    /// This command is used to properly order create and drop of dataflows.
    /// Currently, this is a no-op in AsyncStorageWorker.
    ForwardDropDataflow(GlobalId),
}

/// Responses from [AsyncStorageWorker].
#[derive(Debug)]
pub enum AsyncStorageWorkerResponse<T: Timestamp + Lattice + Codec64> {
    /// An `IngestionDescription` with recent as-of and resume upper frontiers.
    IngestionFrontiersUpdated {
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
    /// A `StorageSinkDesc` with recent as-of frontier.
    ExportFrontiersUpdated {
        /// ID of the sink.
        id: GlobalId,
        /// The updated description of the sink.
        description: StorageSinkDesc<CollectionMetadata, T>,
    },

    /// Indicates data flow can be dropped.
    DropDataflow(GlobalId),
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
    IntoTime: Timestamp + TotalOrder + Lattice + Codec64 + Display + Sync,
{
    let remap_metadata = &ingestion_description.remap_metadata;

    let persist_client = persist_clients
        .open(remap_metadata.persist_location.clone())
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
                        .open_leased_reader::<SourceData, (), IntoTime, StorageDiff>(
                            remap_metadata.data_shard.clone(),
                            Arc::new(remap_metadata.relation_desc.clone()),
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
                            let row: Row = k.0.expect("invalid binding");
                            let _v: () = v;
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

impl<T: Timestamp + TimestampManipulation + Lattice + Codec64 + Display + Sync>
    AsyncStorageWorker<T>
{
    /// Creates a new [`AsyncStorageWorker`].
    ///
    /// IMPORTANT: The passed in `thread` is unparked when new responses
    /// are added the response channel. It is important to not sleep the thread
    /// that is reading from this via [`try_recv`](Self::try_recv) when
    /// [`is_empty`](Self::is_empty) has returned `false`.
    pub fn new(thread: Thread, persist_clients: Arc<PersistClientCache>) -> Self {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = crossbeam_channel::unbounded();

        let response_tx = ActivatingSender::new(response_tx, thread);

        mz_ore::task::spawn(|| "AsyncStorageWorker", async move {
            while let Some(command) = command_rx.recv().await {
                match command {
                    AsyncStorageWorkerCommand::UpdateIngestionFrontiers(
                        id,
                        ingestion_description,
                        remap_compaction_bound,
                    ) => {
                        let mut resume_uppers = BTreeMap::new();

                        for (id, export) in ingestion_description.source_exports.iter() {
                            // Explicit destructuring to force a compile error when the metadata change
                            let CollectionMetadata {
                                persist_location,
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
                                .open_writer::<SourceData, (), T, StorageDiff>(
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
                                // CDCv2 replays all upstream data. Its decoder discards transport
                                // timestamps and reconstructs embedded output timestamps, so neither
                                // output progress nor the remap as-of can shorten upstream replay.
                                // TODO(petrosagg): move this reasoning to the controller
                                SourceEnvelope::CdcV2 if upper.is_empty() => Antichain::new(),
                                SourceEnvelope::CdcV2 => Antichain::from_elem(Timestamp::minimum()),
                                _ => upper.clone(),
                            };
                            resume_uppers.insert(*id, upper);
                            write_handle.expire().await;
                        }

                        let client = persist_clients
                            .open(
                                ingestion_description
                                    .remap_metadata
                                    .persist_location
                                    .clone(),
                            )
                            .await
                            .expect("error creating persist client");
                        let read_handle = client
                            .open_leased_reader::<SourceData, (), T, StorageDiff>(
                                ingestion_description.remap_metadata.data_shard,
                                Arc::new(
                                    ingestion_description.remap_metadata.relation_desc.clone(),
                                ),
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
                        let remap_since = read_handle.since().clone();
                        mz_ore::task::spawn(move || "deferred_expire", async move {
                            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                            read_handle.expire().await;
                        });
                        let as_of = ingestion_as_of(
                            &remap_since,
                            remap_compaction_bound.as_ref(),
                            resume_uppers.values(),
                        );

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
                            GenericSourceConnection::SqlServer(_) => {
                                let uppers = reclock_resume_uppers::<SqlServerSourceConnection, _>(
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

                        let res = response_tx.send(
                            AsyncStorageWorkerResponse::IngestionFrontiersUpdated {
                                id,
                                ingestion_description,
                                as_of,
                                resume_uppers,
                                source_resume_uppers,
                            },
                        );

                        if let Err(_err) = res {
                            // Receiver must have hung up.
                            break;
                        }
                    }
                    AsyncStorageWorkerCommand::UpdateSinkFrontiers(id, mut description) => {
                        let metadata = description.to_storage_metadata.clone();
                        let client = persist_clients
                            .open(metadata.persist_location.clone())
                            .await
                            .expect("error creating persist client");

                        let mut write_handle = client
                            .open_writer::<SourceData, (), T, StorageDiff>(
                                metadata.data_shard,
                                Arc::new(metadata.relation_desc),
                                Arc::new(UnitSchema),
                                Diagnostics {
                                    shard_name: id.to_string(),
                                    handle_purpose: format!("resumption data {}", id),
                                },
                            )
                            .await
                            .unwrap();
                        // Choose an as-of frontier for this execution of the sink. If the write
                        // frontier of the sink is strictly larger than its read hold, it must have
                        // at least written out its snapshot, and we can skip reading it; otherwise
                        // assume we may have to replay from the beginning.
                        let upper = write_handle.fetch_recent_upper().await;
                        let mut read_hold = Antichain::from_iter(
                            upper
                                .iter()
                                .map(|t| t.step_back().unwrap_or_else(T::minimum)),
                        );
                        read_hold.join_assign(&description.as_of);
                        description.with_snapshot = description.with_snapshot
                            && !PartialOrder::less_than(&description.as_of, upper);
                        description.as_of = read_hold;
                        let res =
                            response_tx.send(AsyncStorageWorkerResponse::ExportFrontiersUpdated {
                                id,
                                description,
                            });

                        if let Err(_err) = res {
                            // Receiver must have hung up.
                            break;
                        }
                    }
                    AsyncStorageWorkerCommand::ForwardDropDataflow(id) => {
                        if let Err(_) =
                            response_tx.send(AsyncStorageWorkerResponse::DropDataflow(id))
                        {
                            // Receiver hang up
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

    /// Calculates a readable ingestion as-of respecting the command's remap permission,
    /// and recent resumption frontiers for its exports.
    pub fn update_ingestion_frontiers(&self, ingestion: RunIngestionCommand)
    where
        T: From<mz_repr::Timestamp>,
    {
        let RunIngestionCommand {
            id,
            description,
            remap_compaction_bound,
        } = ingestion;
        let remap_compaction_bound =
            remap_compaction_bound.map(|bound| bound.into_iter().map(T::from).collect());
        self.send(AsyncStorageWorkerCommand::UpdateIngestionFrontiers(
            id,
            description,
            remap_compaction_bound,
        ))
    }

    /// Updates the frontiers associated with the provided `StorageSinkDesc` to recent values.
    /// Currently this will calculate a fresh as-of for the ingestion.
    pub fn update_sink_frontiers(
        &self,
        id: GlobalId,
        sink: StorageSinkDesc<CollectionMetadata, T>,
    ) {
        self.send(AsyncStorageWorkerCommand::UpdateSinkFrontiers(id, sink))
    }

    /// Enqueue a drop dataflow in the async storage worker channel to ensure proper
    /// ordering of creating and dropping data flows.
    pub fn drop_dataflow(&self, id: GlobalId) {
        self.send(AsyncStorageWorkerCommand::ForwardDropDataflow(id))
    }

    fn send(&self, cmd: AsyncStorageWorkerCommand<T>) {
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

/// Chooses the ingestion input as-of without advancing physical compaction.
fn ingestion_as_of<'a, T: TimestampManipulation>(
    remap_since: &Antichain<T>,
    remap_compaction_bound: Option<&Antichain<T>>,
    resume_uppers: impl IntoIterator<Item = &'a Antichain<T>>,
) -> Antichain<T> {
    let mut as_of: Antichain<T> = resume_uppers
        .into_iter()
        .flat_map(|upper| {
            upper
                .iter()
                .map(|t| t.step_back().unwrap_or_else(T::minimum))
        })
        .collect();
    as_of.join_assign(remap_since);
    // New exports at MIN must start at the committed floor even if a lease or
    // physical compaction still holds remap since behind it. The command's recovery
    // constraint makes this safe for existing exports too.
    if let Some(bound) = remap_compaction_bound {
        as_of.join_assign(bound);
    }
    as_of
}

/// Helper that makes sure that we always unpark the target thread when we send a
/// message.
struct ActivatingSender<T> {
    tx: crossbeam_channel::Sender<T>,
    thread: Thread,
}

impl<T> ActivatingSender<T> {
    fn new(tx: crossbeam_channel::Sender<T>, thread: Thread) -> Self {
        Self { tx, thread }
    }

    fn send(&self, message: T) -> Result<(), crossbeam_channel::SendError<T>> {
        let res = self.tx.send(message);
        self.thread.unpark();
        res
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mz_persist_client::ShardId;
    use mz_persist_types::PersistLocation;
    use mz_repr::{RelationDesc, Timestamp};
    use mz_storage_types::instances::StorageInstanceId;
    use mz_storage_types::sources::envelope::{KeyEnvelope, NoneEnvelope};
    use mz_storage_types::sources::load_generator::{
        LoadGenerator, LoadGeneratorOutput, LoadGeneratorSourceExportDetails,
    };
    use mz_storage_types::sources::{
        MzOffset, SourceDesc, SourceExport, SourceExportDataConfig, SourceExportDetails,
    };

    use super::*;

    fn frontier(t: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(t.into())
    }

    #[mz_ore::test]
    fn ingestion_as_of_respects_readability_and_completion() {
        assert_eq!(
            ingestion_as_of(&frontier(200), Some(&frontier(100)), &[frontier(0)]),
            frontier(200),
        );
        for (since, permission, uppers) in [
            (Antichain::new(), frontier(100), vec![frontier(0)]),
            (frontier(10), Antichain::new(), vec![frontier(0)]),
            (frontier(10), frontier(100), vec![Antichain::new()]),
        ] {
            assert!(ingestion_as_of(&since, Some(&permission), &uppers).is_empty());
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn ingestion_birth_floor_and_recovery() {
        let clients = Arc::new(PersistClientCache::new_no_metrics());
        let location = PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        };
        let client = clients.open(location.clone()).await.unwrap();
        let connection = LoadGeneratorSourceConnection {
            load_generator: LoadGenerator::Counter {
                max_cardinality: None,
            },
            tick_micros: None,
            as_of: 0,
            up_to: u64::MAX,
        };
        let remap_metadata = CollectionMetadata {
            persist_location: location.clone(),
            data_shard: ShardId::new(),
            relation_desc: connection.timestamp_desc(),
            txns_shard: None,
        };
        let (mut remap_writer, mut remap_reader) = client
            .open::<SourceData, (), Timestamp, StorageDiff>(
                remap_metadata.data_shard,
                Arc::new(remap_metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                false,
            )
            .await
            .unwrap();
        // Offset 7 is needed by existing exports, while new exports must replay from MIN.
        let bindings = [(0, 0, 1), (0, 50, -1), (7, 50, 1)].map(|(offset, time, diff)| {
            (
                (SourceData(Ok(MzOffset::from(offset).encode_row())), ()),
                Timestamp::from(time),
                StorageDiff::from(diff),
            )
        });
        remap_writer
            .compare_and_append(bindings, frontier(0), frontier(300))
            .await
            .unwrap()
            .unwrap();
        remap_reader.downgrade_since(&frontier(10)).await;

        let worker = AsyncStorageWorker::<Timestamp>::new(std::thread::current(), clients);
        let ordinary = SourceEnvelope::None(NoneEnvelope {
            key_envelope: KeyEnvelope::None,
            key_arity: 0,
        });
        for (name, permission, uppers, envelope, expected_as_of) in [
            (
                "new alongside older exports",
                Some(100),
                vec![0, 151, 201],
                ordinary.clone(),
                100,
            ),
            (
                "restart at permission",
                Some(100),
                vec![101, 151],
                ordinary.clone(),
                100,
            ),
            (
                "partial progress",
                Some(100),
                vec![151, 201],
                ordinary.clone(),
                150,
            ),
            (
                "legacy new export",
                None,
                vec![0, 151],
                ordinary.clone(),
                10,
            ),
            ("legacy restart", None, vec![151, 201], ordinary, 150),
            (
                "CDCv2 full replay",
                Some(100),
                vec![151],
                SourceEnvelope::CdcV2,
                100,
            ),
        ] {
            let mut source_exports = BTreeMap::new();
            for (index, upper) in uppers.iter().enumerate() {
                let id = GlobalId::User(u64::try_from(index).unwrap() + 1);
                let metadata = CollectionMetadata {
                    persist_location: location.clone(),
                    data_shard: ShardId::new(),
                    relation_desc: RelationDesc::empty(),
                    txns_shard: None,
                };
                let mut writer = client
                    .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                        metadata.data_shard,
                        Arc::new(metadata.relation_desc.clone()),
                        Arc::new(UnitSchema),
                        Diagnostics::for_tests(),
                    )
                    .await
                    .unwrap();
                let updates: Vec<((SourceData, ()), Timestamp, StorageDiff)> = Vec::new();
                writer
                    .compare_and_append(updates, frontier(0), frontier(*upper))
                    .await
                    .unwrap()
                    .unwrap();
                writer.expire().await;
                source_exports.insert(
                    id,
                    SourceExport {
                        storage_metadata: metadata,
                        details: SourceExportDetails::LoadGenerator(
                            LoadGeneratorSourceExportDetails {
                                output: LoadGeneratorOutput::Default,
                            },
                        ),
                        data_config: SourceExportDataConfig {
                            encoding: None,
                            envelope: envelope.clone(),
                        },
                    },
                );
            }
            worker.update_ingestion_frontiers(RunIngestionCommand {
                id: GlobalId::User(1),
                description: IngestionDescription {
                    desc: SourceDesc {
                        connection: GenericSourceConnection::LoadGenerator(connection.clone()),
                        timestamp_interval: Duration::from_secs(1),
                    },
                    source_exports,
                    instance_id: StorageInstanceId::system(0).unwrap(),
                    remap_collection_id: GlobalId::User(10),
                    remap_metadata: remap_metadata.clone(),
                },
                remap_compaction_bound: permission.map(frontier),
            });
            let response = tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    match worker.try_recv() {
                        Ok(response) => break response,
                        Err(crossbeam_channel::TryRecvError::Empty) => {
                            tokio::task::yield_now().await
                        }
                        Err(err) => panic!("{err}"),
                    }
                }
            })
            .await
            .expect(name);
            let AsyncStorageWorkerResponse::IngestionFrontiersUpdated {
                as_of,
                resume_uppers,
                source_resume_uppers,
                ..
            } = response
            else {
                panic!("unexpected response: {response:?}")
            };
            assert_eq!(as_of, frontier(expected_as_of), "{name}");
            for (index, upper) in uppers.into_iter().enumerate() {
                let id = GlobalId::User(u64::try_from(index).unwrap() + 1);
                let upper = if envelope == SourceEnvelope::CdcV2 {
                    0
                } else {
                    upper
                };
                assert_eq!(resume_uppers[&id], frontier(upper), "{name}");
                let offset = if upper == 0 { 0 } else { 7 };
                assert_eq!(
                    source_resume_uppers[&id],
                    [MzOffset::from(offset).encode_row()],
                    "{name}"
                );
            }
        }
        assert_eq!(remap_reader.since(), &frontier(10));
        remap_reader.expire().await;
        remap_writer.expire().await;
    }
}
