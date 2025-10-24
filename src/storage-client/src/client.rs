// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

//! The public API of the storage layer.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::iter;

use async_trait::async_trait;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_cluster_client::ReplicaId;
use mz_cluster_client::client::TryIntoProtocolNonce;
use mz_ore::assert_none;
use mz_persist_client::batch::{BatchBuilder, ProtoBatch};
use mz_persist_client::write::WriteHandle;
use mz_persist_types::{Codec, Codec64, StepForward};
use mz_repr::{Diff, GlobalId, Row, TimestampManipulation};
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::oneshot_sources::OneshotIngestionRequest;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::sinks::StorageSinkDesc;
use mz_storage_types::sources::IngestionDescription;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use timely::PartialOrder;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, MutableAntichain};
use uuid::Uuid;

use crate::statistics::{SinkStatisticsUpdate, SourceStatisticsUpdate};

/// A client to a storage server.
pub trait StorageClient<T = mz_repr::Timestamp>:
    GenericClient<StorageCommand<T>, StorageResponse<T>>
{
}

impl<C, T> StorageClient<T> for C where C: GenericClient<StorageCommand<T>, StorageResponse<T>> {}

#[async_trait]
impl<T: Send> GenericClient<StorageCommand<T>, StorageResponse<T>> for Box<dyn StorageClient<T>> {
    async fn send(&mut self, cmd: StorageCommand<T>) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> Result<Option<StorageResponse<T>>, anyhow::Error> {
        // `GenericClient::recv` is required to be cancel safe.
        (**self).recv().await
    }
}

/// Commands related to the ingress and egress of collections.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageCommand<T = mz_repr::Timestamp> {
    /// Transmits connection meta information, before other commands are sent.
    Hello {
        nonce: Uuid,
    },
    /// Indicates that the controller has sent all commands reflecting its
    /// initial state.
    InitializationComplete,
    /// `AllowWrites` informs the replica that it can transition out of the
    /// read-only stage and into the read-write computation stage.
    /// It is now allowed to affect changes to external systems (writes).
    ///
    /// See `ComputeCommand::AllowWrites` for details. This command works
    /// analogously to the compute version.
    AllowWrites,
    /// Update storage instance configuration.
    UpdateConfiguration(Box<StorageParameters>),
    /// Run the specified ingestion dataflow.
    RunIngestion(Box<RunIngestionCommand>),
    /// Enable compaction in storage-managed collections.
    ///
    /// A collection id and a frontier after which accumulations must be correct.
    AllowCompaction(GlobalId, Antichain<T>),
    RunSink(Box<RunSinkCommand<T>>),
    /// Run a dataflow which will ingest data from an external source and only __stage__ it in
    /// Persist.
    ///
    /// Unlike regular ingestions/sources, some other component (e.g. `environmentd`) is
    /// responsible for linking the staged data into a shard.
    RunOneshotIngestion(Box<RunOneshotIngestion>),
    /// `CancelOneshotIngestion` instructs the replica to cancel the identified oneshot ingestion.
    ///
    /// It is invalid to send a [`CancelOneshotIngestion`] command that references a oneshot
    /// ingestion that was not created by a corresponding [`RunOneshotIngestion`] command before.
    /// Doing so may cause the replica to exhibit undefined behavior.
    ///
    /// [`CancelOneshotIngestion`]: crate::client::StorageCommand::CancelOneshotIngestion
    /// [`RunOneshotIngestion`]: crate::client::StorageCommand::RunOneshotIngestion
    CancelOneshotIngestion(Uuid),
}

impl<T> StorageCommand<T> {
    /// Returns whether this command instructs the installation of storage objects.
    pub fn installs_objects(&self) -> bool {
        use StorageCommand::*;
        match self {
            Hello { .. }
            | InitializationComplete
            | AllowWrites
            | UpdateConfiguration(_)
            | AllowCompaction(_, _)
            | CancelOneshotIngestion { .. } => false,
            // TODO(cf2): multi-replica oneshot ingestions. At the moment returning
            // true here means we can't run `COPY FROM` on multi-replica clusters, this
            // should be easy enough to support though.
            RunIngestion(_) | RunSink(_) | RunOneshotIngestion(_) => true,
        }
    }
}

/// A command that starts ingesting the given ingestion description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunIngestionCommand {
    /// The id of the storage collection being ingested.
    pub id: GlobalId,
    /// The description of what source type should be ingested and what post-processing steps must
    /// be applied to the data before writing them down into the storage collection
    pub description: IngestionDescription<CollectionMetadata>,
}

/// A command that starts ingesting the given ingestion description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunOneshotIngestion {
    /// The ID of the ingestion dataflow.
    pub ingestion_id: uuid::Uuid,
    /// The ID of collection we'll stage batches for.
    pub collection_id: GlobalId,
    /// Metadata for the collection we'll stage batches for.
    pub collection_meta: CollectionMetadata,
    /// Details for the oneshot ingestion.
    pub request: OneshotIngestionRequest,
}

/// A command that starts exporting the given sink description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunSinkCommand<T> {
    pub id: GlobalId,
    pub description: StorageSinkDesc<CollectionMetadata, T>,
}

/// A "kind" enum for statuses tracked by the health operator
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    Starting,
    Running,
    Paused,
    Stalled,
    /// This status is currently unused.
    // re-design the ceased status
    Ceased,
    Dropped,
}

impl std::str::FromStr for Status {
    type Err = anyhow::Error;
    /// Keep in sync with [`Status::to_str`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "starting" => Status::Starting,
            "running" => Status::Running,
            "paused" => Status::Paused,
            "stalled" => Status::Stalled,
            "ceased" => Status::Ceased,
            "dropped" => Status::Dropped,
            s => return Err(anyhow::anyhow!("{} is not a valid status", s)),
        })
    }
}

impl Status {
    /// Keep in sync with `Status::from_str`.
    pub fn to_str(&self) -> &'static str {
        match self {
            Status::Starting => "starting",
            Status::Running => "running",
            Status::Paused => "paused",
            Status::Stalled => "stalled",
            Status::Ceased => "ceased",
            Status::Dropped => "dropped",
        }
    }

    /// Determines if a new status should be produced in context of a previous
    /// status.
    pub fn superseded_by(self, new: Status) -> bool {
        match (self, new) {
            (_, Status::Dropped) => true,
            (Status::Dropped, _) => false,
            // Don't re-mark that object as paused.
            (Status::Paused, Status::Paused) => false,
            // De-duplication of other statuses is currently managed by the
            // `health_operator`.
            _ => true,
        }
    }
}

/// A source or sink status update.
///
/// Represents a status update for a given object type. The inner value for each
/// variant should be able to be packed into a status row that conforms to the schema
/// for the object's status history relation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatusUpdate {
    pub id: GlobalId,
    pub status: Status,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub error: Option<String>,
    pub hints: BTreeSet<String>,
    pub namespaced_errors: BTreeMap<String, String>,
    pub replica_id: Option<ReplicaId>,
}

impl StatusUpdate {
    pub fn new(
        id: GlobalId,
        timestamp: chrono::DateTime<chrono::Utc>,
        status: Status,
    ) -> StatusUpdate {
        StatusUpdate {
            id,
            timestamp,
            status,
            error: None,
            hints: Default::default(),
            namespaced_errors: Default::default(),
            replica_id: None,
        }
    }
}

impl From<StatusUpdate> for Row {
    fn from(update: StatusUpdate) -> Self {
        use mz_repr::Datum;

        let timestamp = Datum::TimestampTz(update.timestamp.try_into().expect("must fit"));
        let id = update.id.to_string();
        let id = Datum::String(&id);
        let status = Datum::String(update.status.to_str());
        let error = update.error.as_deref().into();

        let mut row = Row::default();
        let mut packer = row.packer();
        packer.extend([timestamp, id, status, error]);

        if !update.hints.is_empty() || !update.namespaced_errors.is_empty() {
            packer.push_dict_with(|dict_packer| {
                // `hint` and `namespaced` are ordered,
                // as well as the BTree's they each contain.
                if !update.hints.is_empty() {
                    dict_packer.push(Datum::String("hints"));
                    dict_packer.push_list(update.hints.iter().map(|s| Datum::String(s)));
                }
                if !update.namespaced_errors.is_empty() {
                    dict_packer.push(Datum::String("namespaced"));
                    dict_packer.push_dict(
                        update
                            .namespaced_errors
                            .iter()
                            .map(|(k, v)| (k.as_str(), Datum::String(v))),
                    );
                }
            });
        } else {
            packer.push(Datum::Null);
        }

        match update.replica_id {
            Some(id) => packer.push(Datum::String(&id.to_string())),
            None => packer.push(Datum::Null),
        }

        row
    }
}

/// An update to an append only collection.
pub enum AppendOnlyUpdate {
    Row((Row, Diff)),
    Status(StatusUpdate),
}

impl AppendOnlyUpdate {
    pub fn into_row(self) -> (Row, Diff) {
        match self {
            AppendOnlyUpdate::Row((row, diff)) => (row, diff),
            AppendOnlyUpdate::Status(status) => (Row::from(status), Diff::ONE),
        }
    }
}

impl From<(Row, Diff)> for AppendOnlyUpdate {
    fn from((row, diff): (Row, Diff)) -> Self {
        Self::Row((row, diff))
    }
}

impl From<StatusUpdate> for AppendOnlyUpdate {
    fn from(update: StatusUpdate) -> Self {
        Self::Status(update)
    }
}

/// Responses that the storage nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageResponse<T = mz_repr::Timestamp> {
    /// A new upper frontier for the specified identifier.
    FrontierUpper(GlobalId, Antichain<T>),
    /// Punctuation indicates that no more responses will be transmitted for the specified id
    DroppedId(GlobalId),
    /// Batches that have been staged in Persist and maybe will be linked into a shard.
    StagedBatches(BTreeMap<uuid::Uuid, Vec<Result<ProtoBatch, String>>>),
    /// A list of statistics updates, currently only for sources.
    StatisticsUpdates(Vec<SourceStatisticsUpdate>, Vec<SinkStatisticsUpdate>),
    /// A status update for a source or a sink. Periodically sent from
    /// storage workers to convey the latest status information about an object.
    StatusUpdate(StatusUpdate),
}

/// Maintained state for partitioned storage clients.
///
/// This helper type unifies the responses of multiple partitioned
/// workers in order to present as a single worker.
#[derive(Debug)]
pub struct PartitionedStorageState<T> {
    /// Number of partitions the state machine represents.
    parts: usize,
    /// Upper frontiers for sources and sinks, both unioned across all partitions and from each
    /// individual partition.
    uppers: BTreeMap<GlobalId, (MutableAntichain<T>, Vec<Option<Antichain<T>>>)>,
    /// Staged batches from oneshot sources that will get appended by `environmentd`.
    oneshot_source_responses:
        BTreeMap<uuid::Uuid, BTreeMap<usize, Vec<Result<ProtoBatch, String>>>>,
}

impl<T> Partitionable<StorageCommand<T>, StorageResponse<T>>
    for (StorageCommand<T>, StorageResponse<T>)
where
    T: timely::progress::Timestamp + Lattice,
{
    type PartitionedState = PartitionedStorageState<T>;

    fn new(parts: usize) -> PartitionedStorageState<T> {
        PartitionedStorageState {
            parts,
            uppers: BTreeMap::new(),
            oneshot_source_responses: BTreeMap::new(),
        }
    }
}

impl<T> PartitionedStorageState<T>
where
    T: timely::progress::Timestamp,
{
    fn observe_command(&mut self, command: &StorageCommand<T>) {
        // Note that `observe_command` is quite different in `mz_compute_client`.
        // Compute (currently) only sends the command to 1 process,
        // but storage fans out to all workers, allowing the storage processes
        // to self-coordinate how commands and internal commands are ordered.
        //
        // TODO(guswynn): cluster-unification: consolidate this with compute.
        let _ = match command {
            StorageCommand::Hello { .. } => {}
            StorageCommand::RunIngestion(ingestion) => {
                self.insert_new_uppers(ingestion.description.collection_ids());
            }
            StorageCommand::RunSink(export) => {
                self.insert_new_uppers([export.id]);
            }
            StorageCommand::InitializationComplete
            | StorageCommand::AllowWrites
            | StorageCommand::UpdateConfiguration(_)
            | StorageCommand::AllowCompaction(_, _)
            | StorageCommand::RunOneshotIngestion(_)
            | StorageCommand::CancelOneshotIngestion { .. } => {}
        };
    }

    /// Shared implementation for commands that install uppers with controllable behavior with
    /// encountering existing uppers.
    ///
    /// If any ID was previously tracked in `self` and `skip_existing` is `false`, we return the ID
    /// as an error.
    fn insert_new_uppers<I: IntoIterator<Item = GlobalId>>(&mut self, ids: I) {
        for id in ids {
            self.uppers.entry(id).or_insert_with(|| {
                let mut frontier = MutableAntichain::new();
                // TODO(guswynn): cluster-unification: fix this dangerous use of `as`, by
                // merging the types that compute and storage use.
                #[allow(clippy::as_conversions)]
                frontier.update_iter(iter::once((T::minimum(), self.parts as i64)));
                let part_frontiers = vec![Some(Antichain::from_elem(T::minimum())); self.parts];

                (frontier, part_frontiers)
            });
        }
    }
}

impl<T> PartitionedState<StorageCommand<T>, StorageResponse<T>> for PartitionedStorageState<T>
where
    T: timely::progress::Timestamp + Lattice,
{
    fn split_command(&mut self, command: StorageCommand<T>) -> Vec<Option<StorageCommand<T>>> {
        self.observe_command(&command);

        // Fan out to all processes (which will fan out to all workers).
        // StorageState manages ordering of commands internally.
        vec![Some(command); self.parts]
    }

    fn absorb_response(
        &mut self,
        shard_id: usize,
        response: StorageResponse<T>,
    ) -> Option<Result<StorageResponse<T>, anyhow::Error>> {
        match response {
            // Avoid multiple retractions of minimum time, to present as updates from one worker.
            StorageResponse::FrontierUpper(id, new_shard_upper) => {
                let (frontier, shard_frontiers) = match self.uppers.get_mut(&id) {
                    Some(value) => value,
                    None => panic!("Reference to absent collection: {id}"),
                };
                let old_upper = frontier.frontier().to_owned();
                let shard_upper = match &mut shard_frontiers[shard_id] {
                    Some(shard_upper) => shard_upper,
                    None => panic!("Reference to absent shard {shard_id} for collection {id}"),
                };
                frontier.update_iter(shard_upper.iter().map(|t| (t.clone(), -1)));
                frontier.update_iter(new_shard_upper.iter().map(|t| (t.clone(), 1)));
                shard_upper.join_assign(&new_shard_upper);

                let new_upper = frontier.frontier();
                if PartialOrder::less_than(&old_upper.borrow(), &new_upper) {
                    Some(Ok(StorageResponse::FrontierUpper(id, new_upper.to_owned())))
                } else {
                    None
                }
            }
            StorageResponse::DroppedId(id) => {
                let (_, shard_frontiers) = match self.uppers.get_mut(&id) {
                    Some(value) => value,
                    None => panic!("Reference to absent collection: {id}"),
                };
                let prev = shard_frontiers[shard_id].take();
                assert!(
                    prev.is_some(),
                    "got double drop for {id} from shard {shard_id}"
                );

                if shard_frontiers.iter().all(Option::is_none) {
                    self.uppers.remove(&id);
                    Some(Ok(StorageResponse::DroppedId(id)))
                } else {
                    None
                }
            }
            StorageResponse::StatisticsUpdates(source_stats, sink_stats) => {
                // Just forward it along; the `worker_id` should have been set in `storage_state`.
                // We _could_ consolidate across worker_id's, here, but each worker only produces
                // responses periodically, so we avoid that complexity.
                Some(Ok(StorageResponse::StatisticsUpdates(
                    source_stats,
                    sink_stats,
                )))
            }
            StorageResponse::StatusUpdate(updates) => {
                Some(Ok(StorageResponse::StatusUpdate(updates)))
            }
            StorageResponse::StagedBatches(batches) => {
                let mut finished_batches = BTreeMap::new();

                for (collection_id, batches) in batches {
                    tracing::info!(%shard_id, %collection_id, "got batch");

                    let entry = self
                        .oneshot_source_responses
                        .entry(collection_id)
                        .or_default();
                    let novel = entry.insert(shard_id, batches);
                    assert_none!(novel, "Duplicate oneshot source response");

                    // Check if we've received responses from all shards.
                    if entry.len() == self.parts {
                        let entry = self
                            .oneshot_source_responses
                            .remove(&collection_id)
                            .expect("checked above");
                        let all_batches: Vec<_> = entry.into_values().flatten().collect();

                        finished_batches.insert(collection_id, all_batches);
                    }
                }

                if !finished_batches.is_empty() {
                    Some(Ok(StorageResponse::StagedBatches(finished_batches)))
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// A batch of updates to be fed to a local input
pub struct Update<T = mz_repr::Timestamp> {
    pub row: Row,
    pub timestamp: T,
    pub diff: Diff,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// A batch of updates to be fed to a local input; however, the input must
/// determine the most appropriate timestamps to use.
///
/// TODO(cf2): Can we remove this and use only on [`TableData`].
pub struct TimestamplessUpdate {
    pub row: Row,
    pub diff: Diff,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableData {
    /// Rows that still need to be persisted and appended.
    ///
    /// The contained [`Row`]s are _not_ consolidated.
    Rows(Vec<(Row, Diff)>),
    /// Batches already staged in Persist ready to be appended.
    Batches(SmallVec<[ProtoBatch; 1]>),
}

impl TableData {
    pub fn is_empty(&self) -> bool {
        match self {
            TableData::Rows(rows) => rows.is_empty(),
            TableData::Batches(batches) => batches.is_empty(),
        }
    }
}

/// A collection of timestamp-less updates. As updates are added to the builder
/// they are automatically spilled to blob storage.
pub struct TimestamplessUpdateBuilder<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    builder: BatchBuilder<K, V, T, D>,
    initial_ts: T,
}

impl<K, V, T, D> TimestamplessUpdateBuilder<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: TimestampManipulation + Lattice + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    /// Create a new [`TimestamplessUpdateBuilder`] for the shard associated
    /// with the provided [`WriteHandle`].
    pub fn new(handle: &WriteHandle<K, V, T, D>) -> Self {
        let initial_ts = T::minimum();
        let builder = handle.builder(Antichain::from_elem(initial_ts.clone()));
        TimestamplessUpdateBuilder {
            builder,
            initial_ts,
        }
    }

    /// Add a `(K, V, D)` to the staged batch.
    pub async fn add(&mut self, k: &K, v: &V, d: &D) {
        self.builder
            .add(k, v, &self.initial_ts, d)
            .await
            .expect("invalid Persist usage");
    }

    /// Finish the builder and return a [`ProtoBatch`] which can later be linked into a shard.
    ///
    /// The returned batch has nonsensical lower and upper bounds and must be re-written before
    /// appending into the destination shard.
    pub async fn finish(self) -> ProtoBatch {
        let finish_ts = StepForward::step_forward(&self.initial_ts);
        let batch = self
            .builder
            .finish(Antichain::from_elem(finish_ts))
            .await
            .expect("invalid Persist usage");

        batch.into_transmittable_batch()
    }
}

impl TryIntoProtocolNonce for StorageCommand {
    fn try_into_protocol_nonce(self) -> Result<Uuid, Self> {
        match self {
            StorageCommand::Hello { nonce } => Ok(nonce),
            cmd => Err(cmd),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test to ensure the size of the `StorageCommand` enum doesn't regress.
    #[mz_ore::test]
    fn test_storage_command_size() {
        assert_eq!(std::mem::size_of::<StorageCommand>(), 40);
    }

    /// Test to ensure the size of the `StorageResponse` enum doesn't regress.
    #[mz_ore::test]
    fn test_storage_response_size() {
        assert_eq!(std::mem::size_of::<StorageResponse>(), 120);
    }
}
