// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of the storage controller trait.

use std::any::Any;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt::Debug;
use std::num::NonZeroI64;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::FutureExt;
use itertools::Itertools;
use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_client::ReplicaId;

use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_txn::metrics::Metrics as TxnMetrics;
use mz_persist_txn::txn_read::TxnsRead;
use mz_persist_txn::txns::TxnsHandle;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{Codec64, Opaque};
use mz_proto::RustType;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{ColumnName, Datum, Diff, GlobalId, RelationDesc, Row, TimestampManipulation};
use mz_stash::{self, TypedCollection};
use mz_storage_client::client::{
    ProtoStorageCommand, ProtoStorageResponse, RunIngestionCommand, RunSinkCommand, Status,
    StatusUpdate, StorageCommand, StorageResponse, TimestamplessUpdate,
};
use mz_storage_client::controller::{
    CollectionDescription, CollectionState, DataSource, DataSourceOther, ExportDescription,
    ExportState, IntrospectionType, MonotonicAppender, Response, SnapshotCursor, StorageController,
    StorageTxn,
};
use mz_storage_client::metrics::StorageControllerMetrics;
use mz_storage_client::statistics::{
    SinkStatisticsUpdate, SourceStatisticsUpdate, WebhookStatistics,
};
use mz_storage_types::collections as proto;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::{
    CollectionMetadata, PersistTxnTablesImpl, StorageError, TxnsCodecRow,
};
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sinks::{StorageSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::{IngestionDescription, SourceData, SourceExport};
use mz_storage_types::AlterCompatible;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::oneshot;
use tokio::sync::watch::{channel, Sender};
use tokio_stream::StreamMap;
use tracing::{debug, info, warn};

use crate::persist_handles::SnapshotStatsAsOf;
use crate::rehydration::RehydratingStorageClient;
mod collection_mgmt;
mod collection_status;
mod persist_handles;
mod rehydration;
mod statistics;

pub static METADATA_COLLECTION: TypedCollection<proto::GlobalId, proto::DurableCollectionMetadata> =
    TypedCollection::new("storage-collection-metadata");

pub static PERSIST_TXNS_SHARD: TypedCollection<(), String> =
    TypedCollection::new("persist-txns-shard");

pub static SHARD_FINALIZATION: TypedCollection<String, ()> =
    TypedCollection::new("storage-shards-to-finalize");

pub static ALL_COLLECTIONS: &[&str] = &[
    METADATA_COLLECTION.name(),
    PERSIST_TXNS_SHARD.name(),
    SHARD_FINALIZATION.name(),
];

#[derive(Debug)]
enum PersistTxns<T> {
    EnabledEager {
        txns_id: ShardId,
        txns_client: PersistClient,
    },
    EnabledLazy {
        txns_read: TxnsRead<T>,
        txns_client: PersistClient,
    },
}

impl<T: Timestamp + Lattice + Codec64> PersistTxns<T> {
    fn expect_enabled_lazy(&self, txns_id: &ShardId) -> &TxnsRead<T> {
        match self {
            PersistTxns::EnabledLazy { txns_read, .. } => {
                assert_eq!(txns_id, txns_read.txns_id());
                txns_read
            }
            PersistTxns::EnabledEager { .. } => {
                panic!("set if txns are enabled and lazy")
            }
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct PendingCompactionCommand<T> {
    /// [`GlobalId`] of the collection we want to compact.
    id: GlobalId,
    /// [`Antichain`] representing the requested read frontier.
    read_frontier: Antichain<T>,
    /// Cluster associated with this collection, if any.
    cluster_id: Option<StorageInstanceId>,
    /// Future that returns if since of the collection has been downgraded.
    #[derivative(Debug = "ignore")]
    downgrade_notif: BoxFuture<'static, Result<(), ()>>,
}

/// A storage controller for a storage instance.
#[derive(Debug)]
pub struct Controller<T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation>
{
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A function that returns the current time.
    now: NowFn,
    /// The fencing token for this instance of the controller.
    envd_epoch: NonZeroI64,

    /// The set of shards provisionally allocated for collections to use in
    /// [`Self::create_collections`].
    ///
    /// To allocate these values, use [`Self::provisionally_synchronize_state`].
    provisional_shard_mappings: BTreeMap<GlobalId, ShardId>,

    /// The set of [`GlobalId`]s eligible to be dropped via
    /// [`Self::drop_sources`].
    ///
    /// To identify these collections, use
    /// [`Self::provisionally_synchronize_state`].
    provisional_dropped_collections: BTreeSet<GlobalId>,

    /// The set of [`ShardId`]s that we will attempt to finalize.
    finalizable_shards: BTreeSet<ShardId>,

    /// The set of [`ShardId`]s that we know that we have finalized and whose
    /// presence can be deleted from durable storage.
    ///
    /// Values from this field should only be cleared during
    /// [`Self::mark_state_synchronized`].
    finalized_shards: BTreeSet<ShardId>,

    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    pub(crate) collections: BTreeMap<GlobalId, CollectionState<T>>,
    pub(crate) exports: BTreeMap<GlobalId, ExportState<T>>,

    /// Write handle for table shards.
    pub(crate) persist_table_worker: persist_handles::PersistTableWriteWorker<T>,
    /// Write handle for monotonic shards.
    pub(crate) persist_monotonic_worker: persist_handles::PersistMonotonicWriteWorker<T>,
    /// Read handles for all shards.
    ///
    /// These handles are on the other end of a Tokio task, so that work can be done asynchronously
    /// without blocking the storage controller.
    persist_read_handles: persist_handles::PersistReadWorker<T>,
    /// Whether to use the new persist-txn tables implementation or the legacy
    /// one.
    txns: PersistTxns<T>,
    /// Whether we have run `txns_init` yet (required before create_collections
    /// and the various flavors of append).
    txns_init_run: bool,
    txns_metrics: Arc<TxnMetrics>,
    stashed_response: Option<StorageResponse<T>>,
    /// Compaction commands to send during the next call to
    /// `StorageController::process`.
    pending_compaction_commands: Vec<PendingCompactionCommand<T>>,

    /// Interface for managed collections
    pub(crate) collection_manager: collection_mgmt::CollectionManager<T>,

    /// Facility for appending status updates for sources/sinks
    pub(crate) collection_status_manager: collection_status::CollectionStatusManager<T>,
    /// Tracks which collection is responsible for which [`IntrospectionType`].
    pub(crate) introspection_ids: Arc<Mutex<BTreeMap<IntrospectionType, GlobalId>>>,
    /// Tokens for tasks that drive updating introspection collections. Dropping
    /// this will make sure that any tasks (or other resources) will stop when
    /// needed.
    // TODO(aljoscha): Should these live somewhere else?
    introspection_tokens: BTreeMap<GlobalId, Box<dyn Any + Send + Sync>>,

    // The following two fields must always be locked in order.
    /// Consolidated metrics updates to periodically write. We do not eagerly initialize this,
    /// and its contents are entirely driven by `StorageResponse::StatisticsUpdates`'s, as well
    /// as webhook statistics.
    source_statistics: Arc<Mutex<statistics::SourceStatistics>>,
    /// Consolidated metrics updates to periodically write. We do not eagerly initialize this,
    /// and its contents are entirely driven by `StorageResponse::StatisticsUpdates`'s.
    sink_statistics: Arc<Mutex<BTreeMap<GlobalId, Option<SinkStatisticsUpdate>>>>,
    /// A way to update the statistics interval in the statistics tasks.
    statistics_interval_sender: Sender<Duration>,

    /// Clients for all known storage instances.
    clients: BTreeMap<StorageInstanceId, RehydratingStorageClient<T>>,
    /// For each storage instance the ID of its replica, if any.
    replicas: BTreeMap<StorageInstanceId, ReplicaId>,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// Storage configuration to apply to newly provisioned instances, and use during purification.
    config: StorageConfiguration,
    /// Mechanism for returning frontier advancement for tables.
    internal_response_queue: tokio::sync::mpsc::UnboundedReceiver<StorageResponse<T>>,
    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,
    /// A persist client used to write to storage collections
    persist: Arc<PersistClientCache>,
    /// Metrics of the Storage controller
    metrics: StorageControllerMetrics,
    /// Mechanism for the storage controller to send itself feedback, potentially emulating the
    /// responses we expect from clusters.
    ///
    /// Note: This is used for finalizing shards of webhook sources, once webhook sources are
    /// installed on a `clusterd` this can likely be refactored away.
    internal_response_sender: tokio::sync::mpsc::UnboundedSender<StorageResponse<T>>,

    /// `(read, write)` frontiers that have been recorded in the `Frontiers` collection, kept to be
    /// able to retract old rows.
    recorded_frontiers: BTreeMap<GlobalId, (Antichain<T>, Antichain<T>)>,
    /// Write frontiers that have been recorded in the `ReplicaFrontiers` collection, kept to be
    /// able to retract old rows.
    recorded_replica_frontiers: BTreeMap<(GlobalId, ReplicaId), Antichain<T>>,
}

#[async_trait(?Send)]
impl<T> StorageController for Controller<T>
where
    T: Timestamp
        + Lattice
        + TotalOrder
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<mz_repr::Timestamp>,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    type Timestamp = T;

    fn initialization_complete(&mut self) {
        mz_ore::soft_assert_or_log!(
            self.provisional_shard_mappings.is_empty(),
            "dangling collections {:?}",
            self.provisional_shard_mappings
        );

        self.clear_provisional_state();

        self.reconcile_dangling_statistics();
        self.initialized = true;
        for client in self.clients.values_mut() {
            client.send(StorageCommand::InitializationComplete);
        }
    }

    fn update_parameters(&mut self, config_params: StorageParameters) {
        config_params.persist.apply(self.persist.cfg());

        for client in self.clients.values_mut() {
            client.send(StorageCommand::UpdateConfiguration(config_params.clone()));
        }
        self.config.update(config_params);
        self.statistics_interval_sender
            .send_replace(self.config.parameters.statistics_interval);
        self.collection_manager.update_user_batch_duration(
            self.config
                .parameters
                .user_storage_managed_collections_batch_duration,
        );
    }

    /// Get the current configuration
    fn config(&self) -> &StorageConfiguration {
        &self.config
    }

    fn collection(&self, id: GlobalId) -> Result<&CollectionState<Self::Timestamp>, StorageError> {
        self.collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<Self::Timestamp>, StorageError> {
        self.collections
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn collections(
        &self,
    ) -> Box<dyn Iterator<Item = (&GlobalId, &CollectionState<Self::Timestamp>)> + '_> {
        Box::new(self.collections.iter())
    }

    fn create_instance(&mut self, id: StorageInstanceId) {
        let mut client = RehydratingStorageClient::new(
            self.build_info,
            self.metrics.for_instance(id),
            self.envd_epoch,
            self.config.parameters.grpc_client.clone(),
            self.now.clone(),
        );
        if self.initialized {
            client.send(StorageCommand::InitializationComplete);
        }
        client.send(StorageCommand::UpdateConfiguration(
            self.config.parameters.clone(),
        ));
        let old_client = self.clients.insert(id, client);
        assert!(old_client.is_none(), "storage instance {id} already exists");
    }

    fn drop_instance(&mut self, id: StorageInstanceId) {
        let client = self.clients.remove(&id);
        assert!(client.is_some(), "storage instance {id} does not exist");
    }

    fn connect_replica(
        &mut self,
        instance_id: StorageInstanceId,
        replica_id: ReplicaId,
        location: ClusterReplicaLocation,
    ) {
        let client = self
            .clients
            .get_mut(&instance_id)
            .unwrap_or_else(|| panic!("instance {instance_id} does not exist"));
        client.connect(location);

        self.replicas.insert(instance_id, replica_id);
    }

    fn drop_replica(&mut self, instance_id: StorageInstanceId, _replica_id: ReplicaId) {
        let client = self
            .clients
            .get_mut(&instance_id)
            .unwrap_or_else(|| panic!("instance {instance_id} does not exist"));
        client.reset();

        self.replicas.remove(&instance_id);
    }

    // TODO(aljoscha): It would be swell if we could refactor this Leviathan of
    // a method/move individual parts to their own methods.
    #[instrument(name = "storage::create_collections")]
    async fn create_collections(
        &mut self,
        register_ts: Option<Self::Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError> {
        assert!(self.txns_init_run);
        // Validate first, to avoid corrupting state.
        // 1. create a dropped identifier, or
        // 2. create an existing identifier with a new description.
        // Make sure to check for errors within `ingestions` as well.
        collections.sort_by_key(|(id, _)| *id);
        collections.dedup();
        for pos in 1..collections.len() {
            if collections[pos - 1].0 == collections[pos].0 {
                return Err(StorageError::SourceIdReused(collections[pos].0));
            }
        }
        for (id, description) in collections.iter() {
            if let Ok(collection) = self.collection(*id) {
                if &collection.description != description {
                    return Err(StorageError::SourceIdReused(*id));
                }
            }
        }

        // We first enrich each collection description with some additional metadata...
        use futures::stream::{StreamExt, TryStreamExt};
        let enriched_with_metadata = collections
            .into_iter()
            .map(|(id, description)| {
                let data_shard = *self
                    .provisional_shard_mappings
                    .get(&id)
                    .unwrap_or_else(|| panic!("missing call to `prepare_collections` for {id}"));

                let get_shard = |id| -> Result<Option<ShardId>, StorageError> {
                    Ok(Some(match self.collections.get(&id) {
                        Some(col) => col.collection_metadata.data_shard,
                        None => self
                            .provisional_shard_mappings
                            .get(&id)
                            .cloned()
                            .ok_or(StorageError::IdentifierMissing(id))?,
                    }))
                };

                let status_shard = match description.status_collection_id {
                    Some(status_collection_id) => get_shard(status_collection_id)?,
                    None => None,
                };

                let remap_shard = match &description.data_source {
                    // Only ingestions can have remap shards.
                    DataSource::Ingestion(IngestionDescription {
                        remap_collection_id,
                        ..
                    }) => {
                        // Iff ingestion has a remap collection, its metadata must
                        // exist (and be correct) by this point.
                        get_shard(*remap_collection_id)?
                    }
                    _ => None,
                };

                // If the shard is being managed by persist-txn (initially, tables), then we need to
                // pass along the shard id for the txns shard to dataflow rendering.
                let txns_shard = match description.data_source {
                    DataSource::Other(DataSourceOther::TableWrites) => match &self.txns {
                        // If we're not using lazy persist-txn upper (i.e. we're
                        // using eager uppers) then all reads should be done
                        // normally.
                        PersistTxns::EnabledEager { .. } => None,
                        PersistTxns::EnabledLazy { txns_read, .. } => Some(*txns_read.txns_id()),
                    },
                    DataSource::Ingestion(_)
                    | DataSource::Introspection(_)
                    | DataSource::Progress
                    | DataSource::Webhook
                    | DataSource::Other(DataSourceOther::Compute)
                    | DataSource::Other(DataSourceOther::Source) => None,
                };

                let metadata = CollectionMetadata {
                    persist_location: self.persist_location.clone(),
                    remap_shard,
                    data_shard,
                    status_shard,
                    relation_desc: description.desc.clone(),
                    txns_shard,
                };

                Ok((id, description, metadata))
            })
            .collect_vec();

        // So that we can open `SinceHandle`s for each collections concurrently.
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();
        let persist_client = &persist_client;
        // Reborrow the `&mut self` as immutable, as all the concurrent work to be processed in
        // this stream cannot all have exclusive access.
        let this = &*self;
        let to_register: Vec<_> = futures::stream::iter(enriched_with_metadata)
            .map(|data: Result<_, StorageError>| {
                let register_ts = register_ts.clone();
                async move {
                let (id, description, metadata) = data?;

                // should be replaced with real introspection (https://github.com/MaterializeInc/materialize/issues/14266)
                // but for now, it's helpful to have this mapping written down somewhere
                debug!(
                    "mapping GlobalId={} to remap shard ({:?}), data shard ({}), status shard ({:?})",
                    id, metadata.remap_shard, metadata.data_shard, metadata.status_shard
                );

                let (write, mut since_handle) = this
                    .open_data_handles(
                        &id,
                        metadata.data_shard,
                        description.since.as_ref(),
                        metadata.relation_desc.clone(),
                        persist_client,
                    )
                    .await;

                // Present tables as springing into existence at the register_ts by advancing
                // the since. Otherwise, we could end up in a situation where a table with a
                // long compaction window appears to exist before the environment (and this the
                // table) existed.
                //
                // We could potentially also do the same thing for other sources, in particular
                // storage's internal sources and perhaps others, but leave them for now.
                match description.data_source {
                    DataSource::Introspection(_)
                    | DataSource::Webhook
                    | DataSource::Ingestion(_)
                    | DataSource::Progress
                    | DataSource::Other(DataSourceOther::Compute)
                    | DataSource::Other(DataSourceOther::Source) => {},
                    DataSource::Other(DataSourceOther::TableWrites) => {
                        let register_ts = register_ts.expect("caller should have provided a register_ts when creating a table");
                        if since_handle.since().elements() == &[T::minimum()] {
                            debug!("advancing {} to initial since of {:?}", id, register_ts);
                            let token = since_handle.opaque().clone();
                            let _ = since_handle.compare_and_downgrade_since(&token, (&token, &Antichain::from_elem(register_ts.clone()))).await;
                        }
                    }
                }

                Ok::<_, StorageError>((id, description, write, since_handle, metadata))
            }})
            // Poll each future for each collection concurrently, maximum of 50 at a time.
            .buffer_unordered(50)
            // HERE BE DRAGONS:
            //
            // There are at least 2 subtleties in using `FuturesUnordered` (which
            // `buffer_unordered` uses underneath:
            // - One is captured here <https://github.com/rust-lang/futures-rs/issues/2387>
            // - And the other is deadlocking if processing an OUTPUT of a `FuturesUnordered`
            // stream attempts to obtain an async mutex that is also obtained in the futures
            // being polled.
            //
            // Both of these could potentially be issues in all usages of `buffer_unordered` in
            // this method, so we stick the standard advice: only use `try_collect` or
            // `collect`!
            .try_collect()
            .await?;

        let mut to_create = Vec::with_capacity(to_register.len());
        let mut table_registers = Vec::with_capacity(to_register.len());
        // This work mutates the controller state, so must be done serially. Because there
        // is no io-bound work, its very fast.
        {
            // We hold this lock for a very short amount of time, just doing some hashmap inserts
            // and unbounded channel sends.
            let mut source_statistics = self.source_statistics.lock().expect("poisoned");
            for (id, description, write, since_handle, metadata) in to_register {
                let data_shard_since = since_handle.since().clone();

                let collection_state = CollectionState::new(
                    description.clone(),
                    data_shard_since,
                    write.upper().clone(),
                    vec![],
                    metadata.clone(),
                );

                match description.data_source {
                    DataSource::Introspection(_) => {
                        debug!(desc = ?description, meta = ?metadata, "registering {} with persist monotonic worker", id);
                        self.persist_monotonic_worker.register(id, write);
                        self.collections.insert(id, collection_state);
                    }
                    DataSource::Webhook => {
                        debug!(desc = ?description, meta = ?metadata, "registering {} with persist monotonic worker", id);
                        self.persist_monotonic_worker.register(id, write);
                        self.collections.insert(id, collection_state);

                        source_statistics.source_statistics.insert(id, None);
                        // This collection of statistics is periodically aggregated into
                        // `source_statistics`.
                        source_statistics
                            .webhook_statistics
                            .insert(id, Default::default());
                    }
                    DataSource::Other(DataSourceOther::TableWrites) => {
                        debug!(desc = ?description, meta = ?metadata, "registering {} with persist table worker", id);
                        table_registers.push((id, write, collection_state));
                    }
                    DataSource::Progress | DataSource::Other(DataSourceOther::Compute) => {
                        debug!(desc = ?description, meta = ?metadata, "not registering {} with a controller persist worker", id);
                        self.collections.insert(id, collection_state);
                    }
                    DataSource::Ingestion(_) | DataSource::Other(DataSourceOther::Source) => {
                        debug!(desc = ?description, meta = ?metadata, "not registering {} with a controller persist worker", id);
                        self.collections.insert(id, collection_state);
                        source_statistics.source_statistics.insert(id, None);
                    }
                }
                self.persist_read_handles.register(id, since_handle);

                to_create.push((id, description));
            }
        }

        // Register the tables all in one batch.
        if !table_registers.is_empty() {
            let register_ts = register_ts
                .expect("caller should have provided a register_ts when creating a table");
            let mut collection_states = Vec::with_capacity(table_registers.len());
            let table_registers = table_registers
                .into_iter()
                .map(|(id, write, collection_state)| {
                    collection_states.push((id, collection_state));
                    (id, write)
                })
                .collect();
            // This register call advances the logical upper of the table. The
            // register call eventually circles that info back to the
            // controller, but some tests fail if we don't synchronously update
            // it in create_collections, so just do that now.
            let advance_to = mz_persist_types::StepForward::step_forward(&register_ts);
            self.persist_table_worker
                .register(register_ts, table_registers);
            for (id, mut collection_state) in collection_states {
                if let PersistTxns::EnabledLazy { .. } = &self.txns {
                    if collection_state.write_frontier.less_than(&advance_to) {
                        collection_state.write_frontier = Antichain::from_elem(advance_to.clone());
                    }
                }
                self.collections.insert(id, collection_state);
            }
        }

        // Patch up the since of all subsources (which includes the "main"
        // collection) and install read holds from the subsources on the since
        // of the remap collection. We need to do this here because a) the since
        // of the remap collection might be in advance of the since of the data
        // collections because we lazily forward commands to downgrade the since
        // to persist, and b) at the time the subsources are created we know
        // close to nothing about them, not even that they are subsources.
        //
        // N.B. Patching up the since based on the since of the remap collection
        // is correct because the since of the remap collection can advance iff
        // the storage controller allowed it to, which it only does when it
        // would also allow the since of the data collections to advance. It's
        // just that we need to reconcile outselves to the outside world
        // (persist) here.
        //
        // TODO(aljoscha): We should find a way to put this information and the
        // read holds in place when we create the subsource collections. OR, we
        // could create the subsource collections only as part of creating the
        // main source/ingestion.
        for (_id, description) in to_create.iter() {
            match &description.data_source {
                DataSource::Ingestion(ingestion) => {
                    let storage_dependencies = description.get_storage_dependencies();

                    self.install_dependency_read_holds(
                        // N.B. The "main" collection of the source is included in
                        // `source_exports`.
                        ingestion.source_exports.keys().cloned(),
                        &storage_dependencies,
                    )?;
                }
                DataSource::Webhook
                | DataSource::Introspection(_)
                | DataSource::Progress
                | DataSource::Other(_) => {
                    // No since to patch up and no read holds to install on
                    // dependencies!
                }
            }
        }

        // Reborrow `&mut self` immutably, same reasoning as above.
        let this = &*self;

        this.append_shard_mappings(to_create.iter().map(|(id, _)| *id), 1)
            .await;

        // TODO(guswynn): perform the io in this final section concurrently.
        for (id, description) in to_create {
            // Remove the provisional shard mappings.
            self.provisional_shard_mappings.remove(&id);
            match description.data_source {
                DataSource::Ingestion(ingestion) => {
                    let description = self.enrich_ingestion(id, ingestion)?;

                    // Fetch the client for this ingestion's instance.
                    let client =
                        self.clients
                            .get_mut(&description.instance_id)
                            .ok_or_else(|| StorageError::IngestionInstanceMissing {
                                storage_instance_id: description.instance_id,
                                ingestion_id: id,
                            })?;
                    let augmented_ingestion = RunIngestionCommand { id, description };

                    client.send(StorageCommand::RunIngestions(vec![augmented_ingestion]));
                }
                DataSource::Introspection(i) => {
                    let prev = self
                        .introspection_ids
                        .lock()
                        .expect("poisoned lock")
                        .insert(i, id);
                    assert!(
                        prev.is_none(),
                        "cannot have multiple IDs for introspection type"
                    );

                    self.collection_manager.register_collection(id);

                    match i {
                        IntrospectionType::ShardMapping => {
                            self.initialize_shard_mapping().await;
                        }
                        IntrospectionType::Frontiers | IntrospectionType::ReplicaFrontiers => {
                            // Set the collection to empty.
                            self.reconcile_managed_collection(id, vec![]).await;
                        }
                        IntrospectionType::StorageSourceStatistics => {
                            let prev = self.snapshot_statistics(id).await;

                            let scraper_token = statistics::spawn_statistics_scraper::<
                                statistics::SourceStatistics,
                                SourceStatisticsUpdate,
                                _,
                            >(
                                id.clone(),
                                // These do a shallow copy.
                                self.collection_manager.clone(),
                                Arc::clone(&self.source_statistics),
                                prev,
                                self.config.parameters.statistics_interval,
                                self.statistics_interval_sender.subscribe(),
                            );
                            let web_token = statistics::spawn_webhook_statistics_scraper(
                                Arc::clone(&self.source_statistics),
                                self.config.parameters.statistics_interval,
                                self.statistics_interval_sender.subscribe(),
                            );

                            // Make sure these are dropped when the controller is
                            // dropped, so that the internal task will stop.
                            self.introspection_tokens
                                .insert(id, Box::new((scraper_token, web_token)));
                        }
                        IntrospectionType::StorageSinkStatistics => {
                            let prev = self.snapshot_statistics(id).await;

                            let scraper_token =
                                statistics::spawn_statistics_scraper::<_, SinkStatisticsUpdate, _>(
                                    id.clone(),
                                    // These do a shallow copy.
                                    self.collection_manager.clone(),
                                    Arc::clone(&self.sink_statistics),
                                    prev,
                                    self.config.parameters.statistics_interval,
                                    self.statistics_interval_sender.subscribe(),
                                );

                            // Make sure this is dropped when the controller is
                            // dropped, so that the internal task will stop.
                            self.introspection_tokens.insert(id, scraper_token);
                        }
                        IntrospectionType::SourceStatusHistory => {
                            let last_status_per_id = self
                                .partially_truncate_status_history(
                                    IntrospectionType::SourceStatusHistory,
                                )
                                .await;

                            let status_col = collection_status::MZ_SOURCE_STATUS_HISTORY_DESC
                                .get_by_name(&ColumnName::from("status"))
                                .expect("schema has not changed")
                                .0;

                            self.collection_status_manager.extend_previous_statuses(
                                last_status_per_id.into_iter().map(|(id, row)| {
                                    (
                                        id,
                                        Status::from_str(
                                            row.iter()
                                                .nth(status_col)
                                                .expect("schema has not changed")
                                                .unwrap_str(),
                                        )
                                        .expect("statuses must be uncorrupted"),
                                    )
                                }),
                            )
                        }
                        IntrospectionType::SinkStatusHistory => {
                            let last_status_per_id = self
                                .partially_truncate_status_history(
                                    IntrospectionType::SinkStatusHistory,
                                )
                                .await;

                            let status_col = collection_status::MZ_SINK_STATUS_HISTORY_DESC
                                .get_by_name(&ColumnName::from("status"))
                                .expect("schema has not changed")
                                .0;

                            self.collection_status_manager.extend_previous_statuses(
                                last_status_per_id.into_iter().map(|(id, row)| {
                                    (
                                        id,
                                        Status::from_str(
                                            row.iter()
                                                .nth(status_col)
                                                .expect("schema has not changed")
                                                .unwrap_str(),
                                        )
                                        .expect("statuses must be uncorrupted"),
                                    )
                                }),
                            )
                        }
                        IntrospectionType::PrivatelinkConnectionStatusHistory => {
                            self.partially_truncate_status_history(
                                IntrospectionType::PrivatelinkConnectionStatusHistory,
                            )
                            .await;
                        }

                        // Truncate compute-maintained collections.
                        IntrospectionType::ComputeDependencies
                        | IntrospectionType::ComputeReplicaHeartbeats
                        | IntrospectionType::ComputeHydrationStatus
                        | IntrospectionType::ComputeOperatorHydrationStatus => {
                            self.reconcile_managed_collection(id, vec![]).await;
                        }

                        // Note [btv] - we don't truncate these, because that uses
                        // a huge amount of memory on environmentd startup.
                        IntrospectionType::PreparedStatementHistory
                        | IntrospectionType::StatementExecutionHistory
                        | IntrospectionType::SessionHistory
                        | IntrospectionType::StatementLifecycleHistory
                        | IntrospectionType::SqlText => {
                            // do nothing.
                        }
                    }
                }
                DataSource::Webhook => {
                    // Register the collection so our manager knows about it.
                    self.collection_manager.register_collection(id);
                }
                DataSource::Progress | DataSource::Other(_) => {}
            }
        }

        Ok(())
    }

    fn check_alter_collection(
        &mut self,
        collections: &BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError> {
        for (id, ingestion) in collections {
            self.check_alter_collection_inner(*id, ingestion.clone())?;
        }
        Ok(())
    }

    async fn alter_collection(
        &mut self,
        collections: BTreeMap<GlobalId, IngestionDescription>,
    ) -> Result<(), StorageError> {
        self.check_alter_collection(&collections)
            .expect("error avoided by calling check_alter_collection first");

        for (id, ingestion) in collections {
            // Describe the ingestion in terms of collection metadata.
            let description = self
                .enrich_ingestion(id, ingestion.clone())
                .expect("verified valid in check_alter_collection_inner");

            let collection = self.collection_mut(id).expect("validated exists");
            let new_source_exports = match &mut collection.description.data_source {
                DataSource::Ingestion(active_ingestion) => {
                    // Determine which IDs we're adding.
                    let new_source_exports: Vec<_> = description
                        .source_exports
                        .keys()
                        .filter(|id| !active_ingestion.source_exports.contains_key(id))
                        .cloned()
                        .collect();
                    *active_ingestion = ingestion;

                    new_source_exports
                }
                _ => unreachable!("verified collection refers to ingestion"),
            };

            // Assess dependency since, which we have to fast-forward this
            // collection's since to.
            let storage_dependencies = collection.description.get_storage_dependencies();

            // Ensure this new collection's since is aligned with the dependencies.
            // This will likely place its since beyond its upper which is OK because
            // its snapshot will catch it up with the rest of the source, i.e. we
            // will never see its upper at a state beyond 0 and less than its since.
            self.install_dependency_read_holds(
                new_source_exports.into_iter(),
                &storage_dependencies,
            )?;

            // Fetch the client for this ingestion's instance.
            let client = self
                .clients
                .get_mut(&description.instance_id)
                .expect("verified exists");

            client.send(StorageCommand::RunIngestions(vec![RunIngestionCommand {
                id,
                description,
            }]));
        }

        Ok(())
    }

    fn export(&self, id: GlobalId) -> Result<&ExportState<Self::Timestamp>, StorageError> {
        self.exports
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn export_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut ExportState<Self::Timestamp>, StorageError> {
        self.exports
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    async fn create_exports(
        &mut self,
        exports: Vec<(GlobalId, ExportDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError> {
        // Validate first, to avoid corrupting state.
        let mut dedup = BTreeMap::new();
        for (id, desc) in exports.iter() {
            if dedup.insert(id, desc).is_some() {
                return Err(StorageError::SinkIdReused(*id));
            }
            if let Ok(export) = self.export(*id) {
                if &export.description != desc {
                    return Err(StorageError::SinkIdReused(*id));
                }
            }
        }

        for (id, description) in exports {
            let from_id = description.sink.from;

            let dependency_since = self.determine_collection_since_joins(&[from_id])?;
            self.install_read_capabilities(id, &[from_id], dependency_since.clone())?;

            info!(
                sink_id = id.to_string(),
                from_id = from_id.to_string(),
                acquired_since = ?dependency_since,
                "prepare_export: sink acquired read holds"
            );
            let read_policy = ReadPolicy::step_back();

            let from_collection = self.collection(from_id)?;
            let from_storage_metadata = from_collection.collection_metadata.clone();

            let storage_dependencies = vec![from_id];

            info!(
                sink_id = id.to_string(),
                from_id = from_id.to_string(),
                as_of = ?description.sink.as_of,
                "create_exports: creating sink"
            );

            self.exports.insert(
                id,
                ExportState::new(
                    description.clone(),
                    dependency_since,
                    read_policy,
                    storage_dependencies,
                ),
            );

            let status_id = if let Some(status_collection_id) = description.sink.status_id {
                Some(
                    self.collection(status_collection_id)?
                        .collection_metadata
                        .data_shard,
                )
            } else {
                None
            };

            let cmd = RunSinkCommand {
                id,
                description: StorageSinkDesc {
                    from: from_id,
                    from_desc: description.sink.from_desc,
                    connection: description.sink.connection,
                    envelope: description.sink.envelope,
                    as_of: description.sink.as_of,
                    status_id,
                    from_storage_metadata,
                    with_snapshot: description.sink.with_snapshot,
                },
            };

            // Fetch the client for this exports's cluster.
            let client = self
                .clients
                .get_mut(&description.instance_id)
                .ok_or_else(|| StorageError::ExportInstanceMissing {
                    storage_instance_id: description.instance_id,
                    export_id: id,
                })?;

            self.sink_statistics
                .lock()
                .expect("poisoned")
                .insert(id, None);

            client.send(StorageCommand::RunSinks(vec![cmd]));
        }
        Ok(())
    }

    /// Create the sinks described by the `ExportDescription`.
    async fn update_export_connection(
        &mut self,
        exports: BTreeMap<GlobalId, StorageSinkConnection>,
    ) -> Result<(), StorageError> {
        let mut updates_by_instance =
            BTreeMap::<StorageInstanceId, Vec<(RunSinkCommand<T>, ExportState<T>)>>::new();

        for (id, connection) in exports {
            let mut export = self.export(id).expect("export exists").clone();
            let current_sink = export.description.sink.clone();
            export.description.sink.connection = connection;

            // Ensure compatibility
            current_sink.alter_compatible(id, &export.description.sink)?;

            let from_collection = self.collection(export.description.sink.from)?;
            let from_storage_metadata = from_collection.collection_metadata.clone();

            let status_id = if let Some(status_collection_id) = export.description.sink.status_id {
                Some(
                    self.collection(status_collection_id)?
                        .collection_metadata
                        .data_shard,
                )
            } else {
                None
            };

            let cmd = RunSinkCommand {
                id,
                description: StorageSinkDesc {
                    from: export.description.sink.from,
                    from_desc: export.description.sink.from_desc.clone(),
                    connection: export.description.sink.connection.clone(),
                    envelope: export.description.sink.envelope,
                    with_snapshot: export.description.sink.with_snapshot,
                    // Here we are about to send a RunSinkCommand with the current read capaibility
                    // held by this sink. However, clusters are already running a version of the
                    // sink and nothing guarantees that by the time this command arrives at the
                    // clusters they won't have made additional progress such that this read
                    // capability is invalidated.
                    // The solution to this problem is for the controller to track specific
                    // executions of dataflows such that it can track the shutdown of the current
                    // instance and the initialization of the new instance separately and ensure
                    // read holds are held for the correct amount of time.
                    // TODO(petrosagg): change the controller to explicitly track dataflow executions
                    as_of: export.read_capability.clone(),
                    status_id,
                    from_storage_metadata,
                },
            };

            let update = updates_by_instance
                .entry(export.description.instance_id)
                .or_default();
            update.push((cmd, export));

            self.sink_statistics
                .lock()
                .expect("poisoned")
                .insert(id, None);
        }

        for (instance_id, updates) in updates_by_instance {
            let mut export_updates = BTreeMap::new();
            let mut cmds = Vec::with_capacity(updates.len());

            for (cmd, export_state) in updates {
                export_updates.insert(cmd.id, export_state);
                cmds.push(cmd);
            }

            // Fetch the client for this exports's cluster.
            let client = self.clients.get_mut(&instance_id).ok_or_else(|| {
                StorageError::ExportInstanceMissing {
                    storage_instance_id: instance_id,
                    export_id: *export_updates
                        .keys()
                        .next()
                        .expect("set of exports not empty"),
                }
            })?;

            client.send(StorageCommand::RunSinks(cmds));

            // Update state only after all possible errors have occurred.
            for (id, export_state) in export_updates {
                let export = self.export_mut(id).expect("export known to exist");
                *export = export_state;
            }
        }

        Ok(())
    }

    fn drop_sources(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.validate_collection_ids(identifiers.iter().cloned())?;
        self.drop_sources_unvalidated(identifiers);
        Ok(())
    }

    fn drop_sources_unvalidated(&mut self, identifiers: Vec<GlobalId>) {
        for id in &identifiers {
            let present = self.provisional_dropped_collections.remove(id);
            mz_ore::soft_assert_or_log!(
                present,
                "dropping {id}, but drop was not synchronized with storage \
                controller via `synchronzize_collections`"
            );
        }

        // We don't explicitly remove read capabilities! Downgrading the
        // frontier of the source to `[]` (the empty Antichain), will propagate
        // to the storage dependencies.
        let policies = identifiers
            .into_iter()
            .filter(|id| self.collection(*id).is_ok())
            .map(|id| (id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();
        self.set_read_policy(policies);
    }

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), StorageError> {
        self.validate_export_ids(identifiers.iter().cloned())?;
        self.drop_sinks_unvalidated(identifiers);
        Ok(())
    }

    fn drop_sinks_unvalidated(&mut self, identifiers: Vec<GlobalId>) {
        for id in identifiers {
            // Already removed.
            if self.export(id).is_err() {
                continue;
            }

            // We don't explicitly remove read capabilities! Downgrading the
            // frontier of the sink to `[]` (the empty Antichain), will
            // propagate to the storage dependencies.

            // Remove sink by removing its write frontier and arranging for deprovisioning.
            self.update_write_frontiers(&[(id, Antichain::new())]);
        }
    }

    #[instrument(level = "debug")]
    fn append_table(
        &mut self,
        write_ts: Self::Timestamp,
        advance_to: Self::Timestamp,
        commands: Vec<(GlobalId, Vec<TimestamplessUpdate>)>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), StorageError>>, StorageError> {
        assert!(self.txns_init_run);
        // TODO(petrosagg): validate appends against the expected RelationDesc of the collection
        for (id, updates) in commands.iter() {
            if !updates.is_empty() {
                if !write_ts.less_than(&advance_to) {
                    return Err(StorageError::UpdateBeyondUpper(*id));
                }
            }
        }

        Ok(self
            .persist_table_worker
            .append(write_ts, advance_to, commands))
    }

    fn monotonic_appender(&self, id: GlobalId) -> Result<MonotonicAppender, StorageError> {
        assert!(self.txns_init_run);
        self.collection_manager.monotonic_appender(id)
    }

    fn webhook_statistics(&self, id: GlobalId) -> Result<Arc<WebhookStatistics>, StorageError> {
        // Call to this method are usually cached so the lock is not in the critical path.
        let source_statistics = self.source_statistics.lock().expect("poisoned");
        source_statistics
            .webhook_statistics
            .get(&id)
            .cloned()
            .ok_or(StorageError::IdentifierMissing(id))
    }

    // TODO(petrosagg): This signature is not very useful in the context of partially ordered times
    // where the as_of frontier might have multiple elements. In the current form the mutually
    // incomparable updates will be accumulated together to a state of the collection that never
    // actually existed. We should include the original time in the updates advanced by the as_of
    // frontier in the result and let the caller decide what to do with the information.
    async fn snapshot(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<Vec<(Row, Diff)>, StorageError> {
        let metadata = &self.collection(id)?.collection_metadata;
        let contents = match metadata.txns_shard.as_ref() {
            None => {
                // We're not using persist-txn for tables, so we can take a snapshot directly.
                let mut read_handle = self.read_handle_for_snapshot(id).await?;
                read_handle
                    .snapshot_and_fetch(Antichain::from_elem(as_of))
                    .await
            }
            Some(txns_id) => {
                // We _are_ using persist-txn for tables. It advances the physical upper of the
                // shard lazily, so we need to ask it for the snapshot to ensure the read is
                // unblocked.
                //
                // Consider the following scenario:
                // - Table A is written to via txns at time 5
                // - Tables other than A are written to via txns consuming timestamps up to 10
                // - We'd like to read A at 7
                // - The application process of A's txn has advanced the upper to 5+1, but we need
                //   it to be past 7, but the txns shard knows that (5,10) is empty of writes to A
                // - This branch allows it to handle that advancing the physical upper of Table A to
                //   10 (NB but only once we see it get past the write at 5!)
                // - Then we can read it normally.
                let txns_read = self.txns.expect_enabled_lazy(txns_id);
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
                let mut read_handle = self.read_handle_for_snapshot(id).await?;
                data_snapshot.snapshot_and_fetch(&mut read_handle).await
            }
        };
        match contents {
            Ok(contents) => {
                let mut snapshot = Vec::with_capacity(contents.len());
                for ((data, _), _, diff) in contents {
                    // TODO(petrosagg): We should accumulate the errors too and let the user
                    // interprret the result
                    let row = data.expect("invalid protobuf data").0?;
                    snapshot.push((row, diff));
                }
                Ok(snapshot)
            }
            Err(_) => Err(StorageError::ReadBeforeSince(id)),
        }
    }

    async fn snapshot_cursor(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<SnapshotCursor<Self::Timestamp>, StorageError>
    where
        Self::Timestamp: Timestamp + Lattice + Codec64,
    {
        let metadata = &self.collection(id)?.collection_metadata;
        // See the comments in Self::snapshot for what's going on here.
        let cursor = match metadata.txns_shard.as_ref() {
            None => {
                let mut handle = self.read_handle_for_snapshot(id).await?;
                let cursor = handle
                    .snapshot_cursor(Antichain::from_elem(as_of), |_| true)
                    .await
                    .map_err(|_| StorageError::ReadBeforeSince(id))?;
                SnapshotCursor {
                    _read_handle: handle,
                    cursor,
                }
            }
            Some(txns_id) => {
                let txns_read = self.txns.expect_enabled_lazy(txns_id);
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
                let mut handle = self.read_handle_for_snapshot(id).await?;
                let cursor = data_snapshot
                    .snapshot_cursor(&mut handle, |_| true)
                    .await
                    .map_err(|_| StorageError::ReadBeforeSince(id))?;
                SnapshotCursor {
                    _read_handle: handle,
                    cursor,
                }
            }
        };

        Ok(cursor)
    }

    async fn snapshot_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotStats, StorageError> {
        let metadata = &self.collection(id)?.collection_metadata;
        // See the comments in Self::snapshot for what's going on here.
        let as_of = match metadata.txns_shard.as_ref() {
            None => SnapshotStatsAsOf::Direct(as_of),
            Some(txns_id) => {
                let txns_read = self.txns.expect_enabled_lazy(txns_id);
                let as_of = as_of
                    .into_option()
                    .expect("cannot read as_of the empty antichain");
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
                SnapshotStatsAsOf::Txns(data_snapshot)
            }
        };
        self.persist_read_handles.snapshot_stats(id, as_of).await
    }

    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> Result<SnapshotPartsStats, StorageError> {
        let metadata = &self.collection(id)?.collection_metadata;
        // See the comments in Self::snapshot for what's going on here.
        let result = match metadata.txns_shard.as_ref() {
            None => {
                let mut read_handle = self.read_handle_for_snapshot(id).await?;
                let result = read_handle.snapshot_parts_stats(as_of).await;
                read_handle.expire().await;
                result
            }
            Some(txns_id) => {
                let as_of = as_of
                    .into_option()
                    .expect("cannot read as_of the empty antichain");
                let txns_read = self.txns.expect_enabled_lazy(txns_id);
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
                let mut handle = self.read_handle_for_snapshot(id).await?;
                let result = data_snapshot.snapshot_parts_stats(&mut handle).await;
                handle.expire().await;
                result
            }
        };
        result.map_err(|_| StorageError::ReadBeforeSince(id))
    }

    #[instrument(level = "debug")]
    fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
        let mut read_capability_changes = BTreeMap::default();

        for (id, policy) in policies.into_iter() {
            let collection = self
                .collection_mut(id)
                .expect("Reference to absent collection");

            let mut new_read_capability = policy.frontier(collection.write_frontier.borrow());

            if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                let mut update = ChangeBatch::new();
                update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                if !update.is_empty() {
                    update.drain_into(
                        read_capability_changes
                            .entry(id)
                            .or_insert_with(ChangeBatch::new),
                    );
                }
            }

            collection.read_policy = policy;
        }

        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes);
        }
    }

    #[instrument(level = "debug", fields(updates))]
    fn update_write_frontiers(&mut self, updates: &[(GlobalId, Antichain<Self::Timestamp>)]) {
        let mut read_capability_changes = BTreeMap::default();

        for (id, new_upper) in updates.iter() {
            if let Ok(collection) = self.collection_mut(*id) {
                if PartialOrder::less_than(&collection.write_frontier, new_upper) {
                    collection.write_frontier = new_upper.clone();
                }

                let mut new_read_capability = collection
                    .read_policy
                    .frontier(collection.write_frontier.borrow());

                if PartialOrder::less_equal(&collection.implied_capability, &new_read_capability) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));

                    if !update.is_empty() {
                        read_capability_changes.insert(*id, update);
                    }
                }
            } else if let Ok(export) = self.export_mut(*id) {
                if PartialOrder::less_than(&export.write_frontier, new_upper) {
                    export.write_frontier = new_upper.clone();
                }

                // Ignore read policy for sinks whose write frontiers are closed, which identifies
                // the sink is being dropped; we need to advance the read frontier to the empty
                // chain to signal to the dataflow machinery that they should deprovision this
                // object.
                let mut new_read_capability = if export.write_frontier.is_empty() {
                    export.write_frontier.clone()
                } else {
                    export.read_policy.frontier(export.write_frontier.borrow())
                };

                if PartialOrder::less_equal(&export.read_capability, &new_read_capability) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut export.read_capability, &mut new_read_capability);
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));

                    if !update.is_empty() {
                        read_capability_changes.insert(*id, update);
                    }
                }
            } else {
                panic!("Reference to absent collection {id}");
            }
        }

        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes);
        }
    }

    #[instrument(level = "debug", fields(updates))]
    fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<Self::Timestamp>>,
    ) {
        // Location to record consequences that we need to act on.
        let mut collections_net = BTreeMap::new();
        let mut exports_net = BTreeMap::new();

        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.collection_mut(key) {
                let current_read_capabilities = collection.read_capabilities.frontier().to_owned();
                for (time, diff) in update.iter() {
                    assert!(
                        collection.read_capabilities.count_for(time) + diff >= 0,
                        "update {:?} for collection {key} would lead to negative \
                        read capabilities, read capabilities before applying: {:?}",
                        update,
                        collection.read_capabilities
                    );

                    if collection.read_capabilities.count_for(time) + diff > 0 {
                        assert!(
                            current_read_capabilities.less_equal(time),
                            "update {:?} for collection {key} is trying to \
                            install read capabilities before the current \
                            frontier of read capabilities, read capabilities before applying: {:?}",
                            update,
                            collection.read_capabilities
                        );
                    }
                }

                let changes = collection.read_capabilities.update_iter(update.drain());
                update.extend(changes);

                for id in collection.storage_dependencies.iter() {
                    updates
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }

                let (changes, frontier, _cluster_id) =
                    collections_net.entry(key).or_insert_with(|| {
                        (
                            ChangeBatch::new(),
                            Antichain::new(),
                            collection.cluster_id(),
                        )
                    });

                changes.extend(update.drain());
                *frontier = collection.read_capabilities.frontier().to_owned();
            } else if let Ok(export) = self.export_mut(key) {
                // Exports are not depended upon by other storage objects. We
                // only need to report changes in our own read_capability to our
                // dependencies.
                for id in export.storage_dependencies.iter() {
                    updates
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }

                // Make sure we also send `AllowCompaction` commands for sinks,
                // which drives updating the sink's `as_of`, among other things.
                let (changes, frontier, _cluster_id) =
                    exports_net.entry(key).or_insert_with(|| {
                        (
                            ChangeBatch::new(),
                            Antichain::new(),
                            Some(export.cluster_id()),
                        )
                    });

                changes.extend(update.drain());
                *frontier = export.read_capability.clone();
            } else {
                // This is confusing and we should probably error.
                panic!("Unknown collection identifier {}", key);
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands and
        // downgrade persist sinces. The actual downgrades are performed by a Tokio
        // task asynchorously.
        //
        // N.B. We only downgrade persist sinces for collections because
        // exports/sinks don't have an associated collection. We still _do_ want
        // to sent `AllowCompaction` commands to workers for them, though.
        let mut worker_compaction_commands = BTreeMap::default();
        let mut persist_compaction_commands = BTreeMap::default();
        for (key, (mut changes, frontier, cluster_id)) in collections_net {
            if !changes.is_empty() {
                let (tx, rx) = oneshot::channel();
                worker_compaction_commands.insert(key, (frontier.clone(), cluster_id, rx));
                persist_compaction_commands.insert(key, (frontier, tx));
            }
        }
        for (key, (mut changes, frontier, cluster_id)) in exports_net {
            if !changes.is_empty() {
                let (_tx, rx) = oneshot::channel();
                worker_compaction_commands.insert(key, (frontier, cluster_id, rx));
            }
        }

        self.persist_read_handles
            .downgrade(persist_compaction_commands);

        for (id, (read_frontier, cluster_id, rx)) in worker_compaction_commands {
            // Acquiring a client for a storage instance requires await, so we
            // instead stash these for later and process when we can.
            let downgrade_notif = rx
                .map(|res| match res {
                    Ok(res) => res,
                    Err(_) => Err(()),
                })
                .boxed();
            self.pending_compaction_commands
                .push(PendingCompactionCommand {
                    id,
                    read_frontier,
                    cluster_id,
                    downgrade_notif,
                });
        }
    }

    async fn ready(&mut self) {
        let mut clients = self
            .clients
            .values_mut()
            .map(|client| client.response_stream())
            .enumerate()
            .collect::<StreamMap<_, _>>();

        use tokio_stream::StreamExt;
        let msg = tokio::select! {
            // Order matters here. We want to process internal commands
            // before processing external commands.
            biased;

            Some(m) = self.internal_response_queue.recv() => m,
            Some((_id, m)) = clients.next() => m,
        };

        self.stashed_response = Some(msg);
    }

    #[instrument(level = "debug")]
    async fn process(&mut self) -> Result<Option<Response<T>>, anyhow::Error> {
        let mut updated_frontiers = None;
        match self.stashed_response.take() {
            None => (),
            Some(StorageResponse::FrontierUppers(updates)) => {
                self.update_write_frontiers(&updates);
                updated_frontiers = Some(Response::FrontierUpdates(updates));
            }
            Some(StorageResponse::DroppedIds(ids)) => {
                let shards_to_finalize = ids.iter().filter_map(|id| {
                    // Note: All handles to the id should be dropped by now and the since of
                    // the collection should be downgraded to the empty antichain. If handles
                    // to the shard still exist, then we will incorrectly report the shard as
                    // alive, and if the since of the shard has not been downgraded, then we
                    // will continuously fail to finalize it.
                    //
                    // TODO(parkmycar): Should we be asserting that .remove(...) is some? In
                    // other words that we know about the collection we're receiving an event
                    // for.
                    self.collections
                        .remove(id)
                        .map(|state| state.collection_metadata.data_shard)
                });

                self.finalizable_shards.extend(shards_to_finalize);

                if self.config.parameters.finalize_shards {
                    info!("triggering shard finalization due to dropped storage object");
                    self.finalize_shards().await;
                } else {
                    info!("not triggering shard finalization due to dropped storage object because enable_storage_shard_finalization parameter is false")
                }
            }
            Some(StorageResponse::StatisticsUpdates(source_stats, sink_stats)) => {
                // Note we only hold the locks while moving some plain-old-data around here.
                //
                // We just write the whole object, as the update from storage represents the
                // current values.
                //
                // We don't overwrite removed objects, as we may have received a late
                // `StatisticsUpdates` while we were shutting down the storage object.
                {
                    let mut shared_stats = self.source_statistics.lock().expect("poisoned");
                    for stat in source_stats {
                        // Don't override it if its been removed.
                        shared_stats
                            .source_statistics
                            .entry(stat.id)
                            .and_modify(|current| match current {
                                Some(ref mut current) => current.incorporate(stat),
                                None => *current = Some(stat),
                            });
                    }
                }

                {
                    let mut shared_stats = self.sink_statistics.lock().expect("poisoned");
                    for stat in sink_stats {
                        // Don't override it if its been removed.
                        shared_stats
                            .entry(stat.id)
                            .and_modify(|current| match current {
                                Some(ref mut current) => current.incorporate(stat),
                                None => *current = Some(stat),
                            });
                    }
                }
            }
            Some(StorageResponse::StatusUpdates(updates)) => {
                self.record_status_updates(updates).await;
            }
        }

        // IDs of sources that were dropped whose statuses should be updated.
        let mut pending_source_drops = vec![];

        // IDs of all collections that were dropped whose shard mappings should be deleted.
        let mut pending_collection_drops = vec![];

        // IDs of sinks that were dropped whose statuses should be updated (and statistics
        // cleared).
        let mut pending_sink_drops = vec![];

        // IDs of sources (and subsources) whose statistics should be cleared.
        let mut source_statistics_to_drop = vec![];

        // TODO(aljoscha): We could consolidate these before sending to
        // instances, but this seems fine for now.
        for compaction_command in self.pending_compaction_commands.drain(..) {
            let PendingCompactionCommand {
                id,
                read_frontier,
                cluster_id,
                downgrade_notif,
            } = compaction_command;

            // TODO(petrosagg): make this a strict check
            // TODO(aljoscha): What's up with this TODO?
            // Note that while collections are dropped, the `client` may already
            // be cleared out, before we do this post-processing!
            let client = cluster_id.and_then(|cluster_id| self.clients.get_mut(&cluster_id));

            if cluster_id.is_some() && read_frontier.is_empty() {
                if self.collections.get(&id).is_some() {
                    pending_source_drops.push(id);
                } else if self.exports.get(&id).is_some() {
                    pending_sink_drops.push(id);
                } else {
                    panic!("Reference to absent collection {id}");
                }
            }

            // Check if a collection is managed by the storage-controller itself.
            let collection = self.collections.get(&id);
            if let Some(CollectionState { description, .. }) = collection {
                // Normally `clusterd` will emit this StorageResponse when it knows we can drop an
                // ID, but some collections are managed by the storage controller itself, so we
                // must emit this event here.
                let drop_notif = match description.data_source {
                    DataSource::Other(DataSourceOther::TableWrites) if read_frontier.is_empty() => {
                        pending_collection_drops.push(id);
                        Some(self.persist_table_worker.drop_handle(id))
                    }
                    DataSource::Webhook | DataSource::Introspection(_)
                        if read_frontier.is_empty() =>
                    {
                        pending_collection_drops.push(id);
                        // TODO(parkmycar): The Collection Manager and PersistMonotonicWriter
                        // could probably use some love and maybe get merged together?
                        let unregister_notif = self.collection_manager.unregister_collection(id);
                        let monotonic_worker = self.persist_monotonic_worker.clone();
                        let drop_fut = async move {
                            // Wait for the collection manager to stop writing.
                            unregister_notif.await;
                            // Wait for the montonic worker to drop the handle.
                            monotonic_worker.drop_handle(id).await;
                        };
                        Some(drop_fut.boxed())
                    }
                    // These sources are manged by `clusterd`.
                    DataSource::Webhook
                    | DataSource::Introspection(_)
                    | DataSource::Other(_)
                    | DataSource::Progress
                    | DataSource::Ingestion(_) => None,
                };

                // Wait for all of the resources to get cleaned up, but emitting our event.
                if let Some(drop_notif) = drop_notif {
                    let internal_response_sender = self.internal_response_sender.clone();
                    mz_ore::task::spawn(|| format!("storage-cleanup-{id}"), async move {
                        // Wait for the relevant component to drop its resources and handles, this
                        // guarantees we won't see any more writes.
                        drop_notif.await;
                        // Wait to make sure the since of our collection has been downgraded, this
                        // guarantees we won't have any more readers.
                        //
                        // If we fail to downgrade the since the process will halt, but as a back
                        // stop we only notify the storage-controller if it succeeds.
                        if downgrade_notif.await.is_ok() {
                            // Notify that this ID has been dropped, which will start finalization of
                            // the shard.
                            let _ = internal_response_sender
                                .send(StorageResponse::DroppedIds([id].into()));
                        }
                    });
                }
            }

            // Sources can have subsources, which don't have associated clusters, which
            // is why this operates differently than sinks.
            if read_frontier.is_empty() {
                if self.collections.get(&id).is_some() {
                    source_statistics_to_drop.push(id);
                }
            }

            // Note that while collections are dropped, the `client` may already
            // be cleared out, before we do this post-processing!
            if let Some(client) = client {
                client.send(StorageCommand::AllowCompaction(vec![(
                    id,
                    read_frontier.clone(),
                )]));
            }
        }

        // Delete all collection->shard mappings, making sure to de-duplicate.
        let shards_to_update: BTreeSet<_> = pending_source_drops
            .iter()
            .chain(pending_collection_drops.iter())
            .cloned()
            .collect();
        self.append_shard_mappings(shards_to_update.into_iter(), -1)
            .await;

        // Record the drop status for all pending source and sink drops.
        //
        // We also delete the items' statistics objects.
        //
        // The locks are held for a short time, only while we do some hash map removals.

        let status_now = mz_ore::now::to_datetime((self.now)());

        let mut dropped_sources = vec![];
        for id in pending_source_drops.drain(..) {
            dropped_sources.push(StatusUpdate::new(id, status_now, Status::Dropped));
        }

        self.collection_status_manager
            .append_updates(dropped_sources, IntrospectionType::SourceStatusHistory)
            .await;

        {
            let mut source_statistics = self.source_statistics.lock().expect("poisoned");
            for id in source_statistics_to_drop {
                source_statistics.source_statistics.remove(&id);
                source_statistics.webhook_statistics.remove(&id);
            }
        }

        // Record the drop status for all pending sink drops.
        let mut dropped_sinks = vec![];
        {
            let mut sink_statistics = self.sink_statistics.lock().expect("poisoned");
            for id in pending_sink_drops.drain(..) {
                dropped_sinks.push(StatusUpdate::new(id, status_now, Status::Dropped));
                sink_statistics.remove(&id);
            }
        }
        self.collection_status_manager
            .append_updates(dropped_sinks, IntrospectionType::SinkStatusHistory)
            .await;

        Ok(updated_frontiers)
    }

    async fn reconcile_state(&mut self) {
        // TODO(remove?)
    }

    async fn inspect_persist_state(
        &self,
        id: GlobalId,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let collection = &self.collection(id)?.collection_metadata;
        let client = self
            .persist
            .open(collection.persist_location.clone())
            .await?;
        let shard_state = client
            .inspect_shard::<Self::Timestamp>(&collection.data_shard)
            .await?;
        let json_state = serde_json::to_value(shard_state)?;
        Ok(json_state)
    }

    async fn record_frontiers(
        &mut self,
        external_frontiers: BTreeMap<
            GlobalId,
            (Antichain<Self::Timestamp>, Antichain<Self::Timestamp>),
        >,
    ) {
        let mut frontiers = external_frontiers;

        // Enrich `frontiers` with storage frontiers.
        for (object_id, collection) in self.active_collections() {
            let since = collection.read_capabilities.frontier().to_owned();
            let upper = collection.write_frontier.clone();
            frontiers.insert(object_id, (since, upper));
        }
        for (object_id, export) in self.active_exports() {
            // Exports cannot be read from, so their `since` is always the empty frontier.
            let since = Antichain::new();
            let upper = export.write_frontier.clone();
            frontiers.insert(object_id, (since, upper));
        }

        let mut updates = Vec::new();
        let mut push_update =
            |object_id: GlobalId,
             (since, upper): (Antichain<Self::Timestamp>, Antichain<Self::Timestamp>),
             diff: Diff| {
                let read_frontier = since
                    .into_option()
                    .map_or(Datum::Null, |ts| Datum::MzTimestamp(ts.into()));
                let write_frontier = upper
                    .into_option()
                    .map_or(Datum::Null, |ts| Datum::MzTimestamp(ts.into()));
                let row = Row::pack_slice(&[
                    Datum::String(&object_id.to_string()),
                    read_frontier,
                    write_frontier,
                ]);
                updates.push((row, diff));
            };

        let mut old_frontiers = std::mem::replace(&mut self.recorded_frontiers, frontiers);
        for (&id, new) in &self.recorded_frontiers {
            match old_frontiers.remove(&id) {
                Some(old) if &old != new => {
                    push_update(id, new.clone(), 1);
                    push_update(id, old, -1);
                }
                Some(_) => (),
                None => push_update(id, new.clone(), 1),
            }
        }
        for (id, old) in old_frontiers {
            push_update(id, old, -1);
        }

        let id = self.introspection_ids.lock().expect("poisoned")[&IntrospectionType::Frontiers];
        self.append_to_managed_collection(id, updates).await;
    }

    async fn record_replica_frontiers(
        &mut self,
        external_frontiers: BTreeMap<(GlobalId, ReplicaId), Antichain<Self::Timestamp>>,
    ) {
        let mut frontiers = external_frontiers;

        // Enrich `frontiers` with storage frontiers.
        for (object_id, collection) in self.active_collections() {
            let replica_id = collection
                .cluster_id()
                .and_then(|c| self.replicas.get(&c))
                .copied();
            if let Some(replica_id) = replica_id {
                let upper = collection.write_frontier.clone();
                frontiers.insert((object_id, replica_id), upper);
            }
        }
        for (object_id, export) in self.active_exports() {
            let cluster_id = export.cluster_id();
            let replica_id = self.replicas.get(&cluster_id).copied();
            if let Some(replica_id) = replica_id {
                let upper = export.write_frontier.clone();
                frontiers.insert((object_id, replica_id), upper);
            }
        }

        let mut updates = Vec::new();
        let mut push_update = |(object_id, replica_id): (GlobalId, ReplicaId),
                               upper: Antichain<Self::Timestamp>,
                               diff: Diff| {
            let write_frontier = upper
                .into_option()
                .map_or(Datum::Null, |ts| Datum::MzTimestamp(ts.into()));
            let row = Row::pack_slice(&[
                Datum::String(&object_id.to_string()),
                Datum::String(&replica_id.to_string()),
                write_frontier,
            ]);
            updates.push((row, diff));
        };

        let mut old_frontiers = std::mem::replace(&mut self.recorded_replica_frontiers, frontiers);
        for (&key, new) in &self.recorded_replica_frontiers {
            match old_frontiers.remove(&key) {
                Some(old) if &old != new => {
                    push_update(key, new.clone(), 1);
                    push_update(key, old, -1);
                }
                Some(_) => (),
                None => push_update(key, new.clone(), 1),
            }
        }
        for (key, old) in old_frontiers {
            push_update(key, old, -1);
        }

        let id =
            self.introspection_ids.lock().expect("poisoned")[&IntrospectionType::ReplicaFrontiers];
        self.append_to_managed_collection(id, updates).await;
    }

    async fn record_introspection_updates(
        &mut self,
        type_: IntrospectionType,
        updates: Vec<(Row, Diff)>,
    ) {
        let id = self.introspection_ids.lock().expect("poisoned")[&type_];
        self.append_to_managed_collection(id, updates).await;
    }

    /// With the CRDB based timestamp oracle, there is no longer write timestamp
    /// fencing. As in, when a new Coordinator, `B`, starts up, there is nothing
    /// that prevents an old Coordinator, `A`, from getting a new write
    /// timestamp that is higher than `B`'s boot timestamp. Below is the
    /// implications for all persist transaction scenarios, `on` means a
    /// Coordinator turning the persist txn flag on, `off` means a Coordinator
    /// turning the persist txn flag off.
    ///
    /// The following series of events is a concern:
    ///   1. `A` writes at `t_0`, s.t. `t_0` > `B`'s boot timestamp.
    ///   2. `B` writes at `t_1`, s.t. `t_1` > `t_0`.
    ///   3. `A` writes at `t_2`, s.t. `t_2` > `t_1`.
    ///   4. etc.
    ///
    /// - `off` -> `off`: If `B`` manages to append `t_1` before A appends `t_0`
    ///    then the `t_0` append will panic and we won't acknowledge the write
    ///   to the user (or similarly `t_2` and `t_1`). Before persist-txn,
    ///   appends are not atomic, so we might get a partial append. This is fine
    ///   because we only support single table transactions.
    /// - `on` -> `on`: The txn-shard is meant to correctly handle two writers
    ///   so this should be fine. Note it's possible that we have two
    ///   Coordinators interleaving write transactions without the leadership
    ///   check described below, but that should be fine.
    /// - `off` -> `on`: If `A` gets a write timestamp higher than `B`'s boot
    ///   timestamp, then `A` can write directly to a data shard after it's been
    ///   registered with a txn-shard, breaking the invariant that no data shard
    ///   is written to directly while it's registered to a transaction shard.
    ///   To mitigate this, we must do a leadership check AFTER getting the
    ///   write timestamp. In order for `B` to register a data shard in the txn
    ///   shard, it must first become the leader then second get a register
    ///   timestamp. So if `A` gets a write timestamp higher than `B`'s register
    ///   timestamp, it will fail the leadership check before attempting the
    ///   append.
    /// - `on` -> `off`: If `A` tries to write to the txn-shard at a timestamp
    ///   higher than `B`'s boot timestamp, it will fail because the shards have
    ///   been forgotten. So everything should be ok.
    ///
    ///  In general, all transitions make the following steps:
    ///   1. Get write timestamp, `ts`.
    ///   2. Apply all transactions to all data shards up to `ts`.
    ///   3. Register/forget all data shards. So if we crash at any point in
    ///      these steps, for example after only applying some transactions,
    ///      then the next Coordinator can pick up where we left off and finish
    ///      whatever needs finishing.
    ///
    /// H/t jkosh44 for the above notes from the discussion in which we hashed
    /// this all out.
    async fn init_txns(&mut self, init_ts: T) -> Result<(), StorageError> {
        assert_eq!(self.txns_init_run, false);
        let (txns_id, txns_client) = match &self.txns {
            PersistTxns::EnabledEager {
                txns_id,
                txns_client,
            } => {
                info!(
                    "init_txns at {:?}: enabled lazy txns_id={}",
                    init_ts, txns_id
                );
                (txns_id, txns_client)
            }
            PersistTxns::EnabledLazy {
                txns_read,
                txns_client,
            } => {
                info!(
                    "init_txns at {:?}: enabled eager txns_id={}",
                    init_ts,
                    txns_read.txns_id()
                );
                (txns_read.txns_id(), txns_client)
            }
        };

        let mut txns = TxnsHandle::<SourceData, (), T, i64, PersistEpoch, TxnsCodecRow>::open(
            T::minimum(),
            txns_client.clone(),
            Arc::clone(&self.txns_metrics),
            *txns_id,
            Arc::new(RelationDesc::empty()),
            Arc::new(UnitSchema),
        )
        .await;

        // If successful, this forget_all call guarantees:
        // - That we were able to write to the txns shard at `init_ts` (a
        //   timestamp given to us by the coordinator).
        // - That no data shards are registered at `init_ts` and thus every
        //   table is now free to be written to directly at times greater than
        //   that. This is not necessary if the txns feature is enabled, we
        //   could instead commit an empty txn, but we need the apply guarantee
        //   that it has and it doesn't hurt to start everything from a clean
        //   slate on boot (register is idempotent and create_collections will
        //   be called shortly).
        // - That all txn writes through `init_ts` have been applied
        //   (materialized physically in the data shards).
        //
        // We don't have an extra timestamp here for the tidy, so for now ignore it and let the
        // next transaction perform any tidy needed.
        let (removed, _tidy) = txns
            .forget_all(init_ts.clone())
            .await
            .map_err(|_| StorageError::InvalidUppers(vec![]))?;
        info!("init_txns removed from txns shard: {:?}", removed);
        drop(txns);

        self.txns_init_run = true;
        Ok(())
    }

    async fn initialize_state(
        &mut self,
        txn: &mut dyn StorageTxn,
        init_ids: BTreeSet<GlobalId>,
        mut drop_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError> {
        mz_ore::soft_assert_or_log!(
            self.provisional_shard_mappings.is_empty(),
            "seeding collections should occur only on start up"
        );

        // Determine all of the existing metadata.
        let metadata = txn.get_storage_metadata();
        let processed_metadata: Result<Vec<_>, _> = metadata
            .into_iter()
            .map(|(id, shard)| ShardId::from_str(&shard).map(|shard| (id, shard)))
            .collect();
        let metadata = processed_metadata.map_err(|e| StorageError::Generic(anyhow::anyhow!(e)))?;
        let storage_metadata: BTreeMap<_, _> = metadata.into_iter().collect();

        // Determine which collections we do not yet have metadata for.
        let new_collections: BTreeSet<GlobalId> = init_ids
            .iter()
            .filter(|id| !storage_metadata.contains_key(id))
            .cloned()
            .collect();

        mz_ore::soft_assert_or_log!(
            new_collections.iter().all(|id| id.is_system()),
            "initializing collections should only be missing metadata for new system objects, but got {:?}",
            new_collections
        );

        // Ensure we don't double-drop IDs.
        drop_ids.retain(|id| storage_metadata.contains_key(id));

        self.provisionally_synchronize_state(txn, new_collections, drop_ids)
            .await?;

        // Add all previous metadata to the set of provisional metadata. We
        // expect to call `create_collections` for each of these.
        self.provisional_shard_mappings.extend(storage_metadata);

        // All shards that belong to collections dropped in the last epoch are
        // eligible for finalization. This intentionally includes any built-in
        // collections present in `drop_ids`.
        //
        // n.b. this introduces an unlikely race condition: if a collection is
        // dropped from the catalog, but the dataflow is still running on a
        // worker, assuming the shard is safe to finalize on reboot may cause
        // the cluster to panic.
        self.finalizable_shards.extend(
            txn.get_unfinalized_shards()
                .into_iter()
                .map(|shard| ShardId::from_str(&shard).expect("deserialization corrupted")),
        );

        // We will not call any additional function to drop built-in
        // collections.
        self.provisional_dropped_collections.clear();

        Ok(())
    }

    async fn provisionally_synchronize_state(
        &mut self,
        txn: &mut dyn StorageTxn,
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError> {
        use itertools::Either;
        if !self.provisional_shard_mappings.is_empty()
            || !self.provisional_dropped_collections.is_empty()
        {
            return Err(StorageError::DanglingProvisionalState);
        }

        self.provisional_shard_mappings = ids_to_add
            .into_iter()
            .map(|id| (id, ShardId::new()))
            .collect();

        let durably_recorded_mappings = self
            .provisional_shard_mappings
            .iter()
            .map(|(id, shard)| (*id, shard.to_string()))
            .collect();

        txn.insert_storage_metadata(durably_recorded_mappings)?;

        // Delete the metadata for any dropped collections.
        let dropped_mappings = txn.delete_storage_metadata(ids_to_drop);
        let (dropped_ids, dropped_shards) = dropped_mappings
            .into_iter()
            .flat_map(|(id, shard)| [Either::Left(id), Either::Right(shard)])
            .partition_map(|v| v);

        txn.insert_unfinalized_shards(dropped_shards)?;
        self.provisional_dropped_collections = dropped_ids;

        // Reconcile any shards we've successfully finalized with the shard
        // finalization collection.
        txn.mark_shards_as_finalized(
            self.finalized_shards
                .iter()
                .map(|v| v.to_string())
                .collect(),
        );

        Ok(())
    }

    fn clear_provisional_state(&mut self) {
        self.provisional_shard_mappings.clear();
        self.provisional_dropped_collections.clear();
    }

    fn mark_state_synchronized(&mut self, txn: &dyn StorageTxn) {
        let unfinalized_shards = txn.get_unfinalized_shards();
        // If the durable state believes the shard is unfinalized, retain it for
        // the next synchronization.
        self.finalized_shards
            .retain(|shard| unfinalized_shards.contains(&shard.to_string()));
    }
}

/// A wrapper struct that presents the adapter token to a format that is understandable by persist
/// and also allows us to differentiate between a token being present versus being set for the
/// first time.
// TODO(aljoscha): Make this crate-public again once the remap operator doesn't
// hold a critical handle anymore.
#[derive(PartialEq, Clone, Debug)]
pub struct PersistEpoch(Option<NonZeroI64>);

impl Opaque for PersistEpoch {
    fn initial() -> Self {
        PersistEpoch(None)
    }
}

impl Codec64 for PersistEpoch {
    fn codec_name() -> String {
        "PersistEpoch".to_owned()
    }

    fn encode(&self) -> [u8; 8] {
        self.0.map(NonZeroI64::get).unwrap_or(0).to_le_bytes()
    }

    fn decode(buf: [u8; 8]) -> Self {
        Self(NonZeroI64::new(i64::from_le_bytes(buf)))
    }
}

impl From<NonZeroI64> for PersistEpoch {
    fn from(epoch: NonZeroI64) -> Self {
        Self(Some(epoch))
    }
}

/// Seed [`StorageTxn`] with any state required to instantiate a
/// [`StorageController`].
///
/// This cannot be a member of [`StorageController`] because it cannot take a
/// `self` parameter.
///
pub fn prepare_initialization(txn: &mut dyn StorageTxn) -> Result<(), StorageError> {
    if txn.get_persist_txn_shard().is_none() {
        let txns_id = ShardId::new();
        txn.write_persist_txn_shard(txns_id.to_string())?;
    }

    Ok(())
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: StorageController<Timestamp = T>,
{
    /// Create a new storage controller from a client it should wrap.
    ///
    /// Note that when creating a new storage controller, you must also
    /// reconcile it with the previous state.
    ///
    /// # Panics
    /// If this function is called before [`prepare_initialization`].
    pub async fn new(
        build_info: &'static BuildInfo,
        persist_location: PersistLocation,
        persist_clients: Arc<PersistClientCache>,
        now: NowFn,
        envd_epoch: NonZeroI64,
        metrics_registry: MetricsRegistry,
        persist_txn_tables: PersistTxnTablesImpl,
        connection_context: ConnectionContext,
        txn: &dyn StorageTxn,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // This value must be already installed because we must ensure it's
        // durably recorded before it is used, otherwise we risk leaking persist
        // state.
        let txns_id = txn
            .get_persist_txn_shard()
            .expect("must call prepare initialization before creating storage controller");
        let txns_id = ShardId::from_str(txns_id.as_str()).expect("shard ID must be valid");

        let txns_client = persist_clients
            .open(persist_location.clone())
            .await
            .expect("location should be valid");
        let txns_metrics = Arc::new(TxnMetrics::new(&metrics_registry));
        let txns = TxnsHandle::open(
            T::minimum(),
            txns_client.clone(),
            Arc::clone(&txns_metrics),
            txns_id,
            Arc::new(RelationDesc::empty()),
            Arc::new(UnitSchema),
        )
        .await;
        let persist_table_worker = persist_handles::PersistTableWriteWorker::new_txns(
            tx.clone(),
            txns,
            persist_txn_tables,
        );
        let txns = match persist_txn_tables {
            PersistTxnTablesImpl::Lazy => {
                let txns_read = TxnsRead::start::<TxnsCodecRow>(txns_client.clone(), txns_id).await;
                PersistTxns::EnabledLazy {
                    txns_read,
                    txns_client,
                }
            }
            PersistTxnTablesImpl::Eager => PersistTxns::EnabledEager {
                txns_id,
                txns_client,
            },
        };
        let persist_monotonic_worker =
            persist_handles::PersistMonotonicWriteWorker::new(tx.clone());
        let collection_manager_write_handle = persist_monotonic_worker.clone();

        let collection_manager =
            collection_mgmt::CollectionManager::new(collection_manager_write_handle, now.clone());

        let introspection_ids = Arc::new(Mutex::new(BTreeMap::new()));

        let collection_status_manager = crate::collection_status::CollectionStatusManager::new(
            collection_manager.clone(),
            Arc::clone(&introspection_ids),
        );

        let (statistics_interval_sender, _) =
            channel(mz_storage_types::parameters::STATISTICS_INTERVAL_DEFAULT);

        Self {
            build_info,
            provisional_shard_mappings: BTreeMap::new(),
            provisional_dropped_collections: BTreeSet::new(),
            finalizable_shards: BTreeSet::new(),
            finalized_shards: BTreeSet::new(),
            collections: BTreeMap::default(),
            exports: BTreeMap::default(),
            persist_table_worker,
            persist_monotonic_worker,
            persist_read_handles: persist_handles::PersistReadWorker::new(),
            txns,
            txns_init_run: false,
            txns_metrics,
            stashed_response: None,
            pending_compaction_commands: vec![],
            collection_manager,
            collection_status_manager,
            introspection_ids,
            introspection_tokens: BTreeMap::new(),
            now,
            envd_epoch,
            source_statistics: Arc::new(Mutex::new(statistics::SourceStatistics {
                source_statistics: BTreeMap::new(),
                webhook_statistics: BTreeMap::new(),
            })),
            sink_statistics: Arc::new(Mutex::new(BTreeMap::new())),
            statistics_interval_sender,
            clients: BTreeMap::new(),
            replicas: BTreeMap::new(),
            initialized: false,
            config: StorageConfiguration::new(connection_context),
            internal_response_sender: tx,
            internal_response_queue: rx,
            persist_location,
            persist: persist_clients,
            metrics: StorageControllerMetrics::new(metrics_registry),
            recorded_frontiers: BTreeMap::new(),
            recorded_replica_frontiers: BTreeMap::new(),
        }
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_collection_ids(
        &self,
        ids: impl Iterator<Item = GlobalId>,
    ) -> Result<(), StorageError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_export_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), StorageError> {
        for id in ids {
            self.export(id)?;
        }
        Ok(())
    }

    /// Iterate over collections that have not been dropped.
    fn active_collections(&self) -> impl Iterator<Item = (GlobalId, &CollectionState<T>)> {
        self.collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| (*id, c))
    }

    /// Iterate over exports that have not been dropped.
    fn active_exports(&self) -> impl Iterator<Item = (GlobalId, &ExportState<T>)> {
        self.exports
            .iter()
            .filter(|(_id, e)| !e.is_dropped())
            .map(|(id, e)| (*id, e))
    }

    /// Return the since frontier at which we can read from all the given
    /// collections.
    ///
    /// The outer error is a potentially recoverable internal error, while the
    /// inner error is appropriate to return to the adapter.
    fn determine_collection_since_joins(
        &self,
        collections: &[GlobalId],
    ) -> Result<Antichain<T>, StorageError> {
        let mut joined_since = Antichain::from_elem(T::minimum());
        for id in collections {
            let collection = self.collection(*id)?;

            let since = collection.implied_capability.clone();
            joined_since.join_assign(&since);
        }

        Ok(joined_since)
    }

    /// Install read capabilities on the given `storage_dependencies`.
    #[instrument(level = "info", fields(from_id, storage_dependencies, read_capability))]
    fn install_read_capabilities(
        &mut self,
        _from_id: GlobalId,
        storage_dependencies: &[GlobalId],
        read_capability: Antichain<T>,
    ) -> Result<(), StorageError> {
        let mut changes = ChangeBatch::new();
        for time in read_capability.iter() {
            changes.update(time.clone(), 1);
        }

        let mut storage_read_updates = storage_dependencies
            .iter()
            .map(|id| (*id, changes.clone()))
            .collect();

        self.update_read_capabilities(&mut storage_read_updates);

        Ok(())
    }

    /// Opens a write and critical since handles for the given `shard`.
    ///
    /// `since` is an optional `since` that the read handle will be forwarded to if it is less than
    /// its current since.
    ///
    /// This will `halt!` the process if we cannot successfully acquire a critical handle with our
    /// current epoch.
    async fn open_data_handles(
        &self,
        id: &GlobalId,
        shard: ShardId,
        since: Option<&Antichain<T>>,
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> (
        WriteHandle<SourceData, (), T, Diff>,
        SinceHandle<SourceData, (), T, Diff, PersistEpoch>,
    ) {
        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };

        // Construct the handle in a separate block to ensure all error paths are diverging
        let since_handle = {
            // This block's aim is to ensure the handle is in terms of our epoch
            // by the time we return it.
            let mut handle: SinceHandle<_, _, _, _, PersistEpoch> = persist_client
                .open_critical_since(
                    shard,
                    PersistClient::CONTROLLER_CRITICAL_SINCE,
                    diagnostics.clone(),
                )
                .await
                .expect("invalid persist usage");

            // Take the join of the handle's since and the provided `since`; this lets materialized
            // views express the since at which their read handles "start."
            let since = handle
                .since()
                .join(since.unwrap_or(&Antichain::from_elem(T::minimum())));

            let our_epoch = self.envd_epoch;

            loop {
                let current_epoch: PersistEpoch = handle.opaque().clone();

                // Ensure the current epoch is <= our epoch.
                let unchecked_success = current_epoch.0.map(|e| e <= our_epoch).unwrap_or(true);

                if unchecked_success {
                    // Update the handle's state so that it is in terms of our epoch.
                    let checked_success = handle
                        .compare_and_downgrade_since(
                            &current_epoch,
                            (&PersistEpoch::from(our_epoch), &since),
                        )
                        .await
                        .is_ok();
                    if checked_success {
                        break handle;
                    }
                } else {
                    mz_ore::halt!("fenced by envd @ {current_epoch:?}. ours = {our_epoch}");
                }
            }
        };

        let mut write = persist_client
            .open_writer(
                shard,
                Arc::new(relation_desc),
                Arc::new(UnitSchema),
                diagnostics.clone(),
            )
            .await
            .expect("invalid persist usage");

        // N.B.
        // Fetch the most recent upper for the write handle. Otherwise, this may be behind
        // the since of the since handle. Its vital this happens AFTER we create
        // the since handle as it needs to be linearized with that operation. It may be true
        // that creating the write handle after the since handle already ensures this, but we
        // do this out of an abundance of caution.
        //
        // Note that this returns the upper, but also sets it on the handle to be fetched later.
        write.fetch_recent_upper().await;

        (write, since_handle)
    }

    /// Get the current rows in the given statistics table. This is used to bootstrap
    /// the statistics tasks.
    ///
    // TODO(guswynn): we need to be more careful about the update time we get here:
    // <https://github.com/MaterializeInc/materialize/issues/25349>
    async fn snapshot_statistics(&mut self, id: GlobalId) -> Vec<Row> {
        match self.collections[&id].write_frontier.as_option() {
            Some(f) if f > &T::minimum() => {
                let as_of = f.step_back().unwrap();

                let snapshot = self.snapshot(id, as_of).await.unwrap();
                snapshot
                    .into_iter()
                    .map(|(row, diff)| {
                        assert!(diff == 1);
                        row
                    })
                    .collect()
            }
            // If collection is closed or the frontier is the minimum, we cannot
            // or don't need to truncate (respectively).
            _ => Vec::new(),
        }
    }

    /// Remove statistics for sources/sinks that were dropped but still have statistics rows
    /// hanging around.
    fn reconcile_dangling_statistics(&mut self) {
        self.source_statistics
            .lock()
            .expect("poisoned")
            .source_statistics
            // collections should also contain subsources.
            .retain(|k, _| self.collections.contains_key(k));
        self.sink_statistics
            .lock()
            .expect("poisoned")
            .retain(|k, _| self.exports.contains_key(k));
    }

    /// Effectively truncates the `data_shard` associated with `global_id`
    /// effective as of the system time.
    ///
    /// # Panics
    /// - If `id` does not belong to a collection or is not registered as a
    ///   managed collection.
    async fn reconcile_managed_collection(&mut self, id: GlobalId, updates: Vec<(Row, Diff)>) {
        let mut reconciled_updates = BTreeMap::<Row, Diff>::new();

        for (row, diff) in updates.into_iter() {
            *reconciled_updates.entry(row).or_default() += diff;
        }

        match self.collections[&id].write_frontier.as_option() {
            Some(f) if f > &T::minimum() => {
                let as_of = f.step_back().unwrap();

                let negate = self.snapshot(id, as_of).await.unwrap();

                for (row, diff) in negate.into_iter() {
                    *reconciled_updates.entry(row).or_default() -= diff;
                }
            }
            // If collection is closed or the frontier is the minimum, we cannot
            // or don't need to truncate (respectively).
            _ => {}
        }

        let updates: Vec<_> = reconciled_updates
            .into_iter()
            .filter(|(_, diff)| *diff != 0)
            .collect();

        if !updates.is_empty() {
            self.append_to_managed_collection(id, updates).await;
        }
    }

    /// Append `updates` to the `data_shard` associated with `global_id`
    /// effective as of the system time.
    ///
    /// # Panics
    /// - If `id` is not registered as a managed collection.
    #[instrument(level = "debug", fields(id))]
    async fn append_to_managed_collection(&self, id: GlobalId, updates: Vec<(Row, Diff)>) {
        assert!(self.txns_init_run);
        self.collection_manager
            .append_to_collection(id, updates)
            .await;
    }

    /// Initializes the data expressing which global IDs correspond to which
    /// shards. Necessary because we cannot write any of these mappings that we
    /// discover before the shard mapping collection exists.
    ///
    /// # Panics
    /// - If `IntrospectionType::ShardMapping` is not associated with a
    /// `GlobalId` in `self.introspection_ids`.
    /// - If `IntrospectionType::ShardMapping`'s `GlobalId` is not registered as
    ///   a managed collection.
    async fn initialize_shard_mapping(&mut self) {
        let id =
            self.introspection_ids.lock().expect("poisoned lock")[&IntrospectionType::ShardMapping];

        let mut row_buf = Row::default();
        let mut updates = Vec::with_capacity(self.collections.len());
        for (
            global_id,
            CollectionState {
                collection_metadata: CollectionMetadata { data_shard, .. },
                ..
            },
        ) in self.collections.iter()
        {
            let mut packer = row_buf.packer();
            packer.push(Datum::from(global_id.to_string().as_str()));
            packer.push(Datum::from(data_shard.to_string().as_str()));
            updates.push((row_buf.clone(), 1));
        }

        self.reconcile_managed_collection(id, updates).await;
    }

    /// Effectively truncates the source status history shard except for the
    /// most recent updates from each ID.
    ///
    /// Returns a map with latest unpacked row per id.
    async fn partially_truncate_status_history(
        &mut self,
        collection: IntrospectionType,
    ) -> BTreeMap<GlobalId, Row> {
        let (keep_n, occurred_at_col, id_col) = match collection {
            IntrospectionType::SourceStatusHistory => (
                self.config.parameters.keep_n_source_status_history_entries,
                collection_status::MZ_SOURCE_STATUS_HISTORY_DESC
                    .get_by_name(&ColumnName::from("occurred_at"))
                    .expect("schema has not changed")
                    .0,
                collection_status::MZ_SOURCE_STATUS_HISTORY_DESC
                    .get_by_name(&ColumnName::from("source_id"))
                    .expect("schema has not changed")
                    .0,
            ),
            IntrospectionType::SinkStatusHistory => (
                self.config.parameters.keep_n_sink_status_history_entries,
                collection_status::MZ_SINK_STATUS_HISTORY_DESC
                    .get_by_name(&ColumnName::from("occurred_at"))
                    .expect("schema has not changed")
                    .0,
                collection_status::MZ_SINK_STATUS_HISTORY_DESC
                    .get_by_name(&ColumnName::from("sink_id"))
                    .expect("schema has not changed")
                    .0,
            ),
            IntrospectionType::PrivatelinkConnectionStatusHistory => (
                self.config
                    .parameters
                    .keep_n_privatelink_status_history_entries,
                collection_status::MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC
                    .get_by_name(&ColumnName::from("occurred_at"))
                    .expect("schema has not changed")
                    .0,
                collection_status::MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC
                    .get_by_name(&ColumnName::from("connection_id"))
                    .expect("schema has not changed")
                    .0,
            ),
            _ => unreachable!(),
        };

        let id = self.introspection_ids.lock().expect("poisoned")[&collection];

        let mut rows = match self.collections[&id].write_frontier.as_option() {
            Some(f) if f > &T::minimum() => {
                let as_of = f.step_back().unwrap();

                self.snapshot(id, as_of).await.expect("snapshot succeeds")
            }
            // If collection is closed or the frontier is the minimum, we cannot
            // or don't need to truncate (respectively).
            _ => return BTreeMap::new(),
        };

        // BTreeMap<Id, MinHeap<(OccurredAt, Row)>>, to track the
        // earliest events for each id.
        let mut last_n_entries_per_id: BTreeMap<Datum, BinaryHeap<Reverse<(Datum, Vec<Datum>)>>> =
            BTreeMap::new();

        // BTreeMap to keep track of the row with the latest timestamp for each id
        let mut latest_row_per_id: BTreeMap<Datum, (CheckedTimestamp<DateTime<Utc>>, Vec<Datum>)> =
            BTreeMap::new();

        // Consolidate the snapshot, so we can process it correctly below.
        differential_dataflow::consolidation::consolidate(&mut rows);

        let mut deletions = vec![];

        for (row, diff) in rows.iter() {
            let status_row = row.unpack();
            let id = status_row[id_col];
            let occurred_at = status_row[occurred_at_col];

            // Duplicate rows ARE possible if many status changes happen in VERY quick succession,
            // so we go ahead and handle them.
            assert!(
                *diff > 0,
                "only know how to operate over consolidated data with diffs > 0, \
                found diff {} for object {} in {:?}",
                diff,
                id,
                collection
            );

            // Keep track of the timestamp of the latest row per id
            let timestamp = occurred_at.unwrap_timestamptz();
            match latest_row_per_id.get(&id) {
                Some(existing) if &existing.0 > &timestamp => {}
                _ => {
                    latest_row_per_id.insert(id, (timestamp, status_row.clone()));
                }
            }

            // Consider duplicated rows separately.
            for _ in 0..*diff {
                let entries = last_n_entries_per_id.entry(id).or_default();

                // We CAN have multiple statuses (most likely Starting and Running) at the exact same
                // millisecond, depending on how the `health_operator` is scheduled.
                //
                // Note that these will be arbitrarily ordered, so a Starting event might
                // survive and a Running one won't. The next restart will remove the other,
                // so we don't bother being careful about it.
                //
                // TODO(guswynn): unpack these into health-status objects and use
                // their `Ord1 impl.
                entries.push(Reverse((occurred_at, status_row.clone())));

                // Retain some number of entries, using pop to mark the oldest entries for
                // deletion.
                while entries.len() > keep_n {
                    if let Some(Reverse((_, r))) = entries.pop() {
                        deletions.push(r);
                    }
                }
            }
        }

        let mut row_buf = Row::default();
        // Updates are only deletes because everything else is already in the shard.
        let updates = deletions
            .into_iter()
            .map(|unpacked_row| {
                // Re-pack all rows
                let mut packer = row_buf.packer();
                packer.extend(unpacked_row.into_iter());
                (row_buf.clone(), -1)
            })
            .collect();

        self.append_to_managed_collection(id, updates).await;

        latest_row_per_id
            .into_iter()
            .filter_map(|(key, (_, row_vec))| {
                match GlobalId::from_str(key.unwrap_str()) {
                    Ok(id) => {
                        let mut packer = row_buf.packer();
                        packer.extend(row_vec.into_iter());
                        Some((id, row_buf.clone()))
                    }
                    // Ignore any rows that can't be unwrapped correctly
                    Err(_) => None,
                }
            })
            .collect()
    }

    /// Appends a new global ID, shard ID pair to the appropriate collection.
    /// Use a `diff` of 1 to append a new entry; -1 to retract an existing
    /// entry.
    ///
    /// However, data is written iff we know of the `GlobalId` of the
    /// `IntrospectionType::ShardMapping` collection; in other cases, data is
    /// dropped on the floor. In these cases, the data is later written by
    /// [`Self::initialize_shard_mapping`].
    ///
    /// # Panics
    /// - If `self.collections` does not have an entry for `global_id`.
    /// - If `IntrospectionType::ShardMapping`'s `GlobalId` is not registered as
    ///   a managed collection.
    /// - If diff is any value other than `1` or `-1`.
    #[instrument(level = "debug")]
    async fn append_shard_mappings<I>(&self, global_ids: I, diff: i64)
    where
        I: Iterator<Item = GlobalId>,
    {
        assert!(self.txns_init_run);
        mz_ore::soft_assert_or_log!(diff == -1 || diff == 1, "use 1 for insert or -1 for delete");

        let id = match self
            .introspection_ids
            .lock()
            .expect("poisoned")
            .get(&IntrospectionType::ShardMapping)
        {
            Some(id) => *id,
            _ => return,
        };

        let mut updates = vec![];
        // Pack updates into rows
        let mut row_buf = Row::default();

        for global_id in global_ids {
            let shard_id = self.collections[&global_id].collection_metadata.data_shard;

            let mut packer = row_buf.packer();
            packer.push(Datum::from(global_id.to_string().as_str()));
            packer.push(Datum::from(shard_id.to_string().as_str()));
            updates.push((row_buf.clone(), diff));
        }

        self.append_to_managed_collection(id, updates).await;
    }

    /// Attempts to close all shards marked for finalization.
    #[allow(dead_code)]
    #[instrument(level = "debug")]
    async fn finalize_shards(&mut self) {
        // Open a persist client to delete unused shards.
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();

        let persist_client = &persist_client;
        let diagnostics = &Diagnostics::from_purpose("finalizing shards");

        use futures::stream::StreamExt;
        let finalized_shards: BTreeSet<ShardId> =
            futures::stream::iter(self.finalizable_shards.clone())
                .map(|shard_id| async move {
                    let persist_client = persist_client.clone();
                    let diagnostics = diagnostics.clone();

                    let is_finalized = persist_client
                        .is_finalized::<SourceData, (), T, Diff>(shard_id, diagnostics)
                        .await
                        .expect("invalid persist usage");

                    if is_finalized {
                        Some(shard_id)
                    } else {
                        // Finalizing a shard can take a long time cleaning up existing data.
                        // Spawning a task means that we can't proactively remove this shard
                        // from the finalization register, unfortunately... but a future run
                        // of `finalize_shards` should notice the shard has been finalized and tidy
                        // up.
                        mz_ore::task::spawn(|| format!("finalize_shard({shard_id})"), async move {
                            let finalize = || async move {
                                let empty_batch: Vec<((SourceData, ()), T, Diff)> = vec![];
                                let mut write_handle: WriteHandle<SourceData, (), T, Diff> =
                                    persist_client
                                        .open_writer(
                                            shard_id,
                                            Arc::new(RelationDesc::empty()),
                                            Arc::new(UnitSchema),
                                            // TODO: thread the global ID into the shard finalization WAL
                                            Diagnostics::from_purpose("finalizing shards"),
                                        )
                                        .await
                                        .expect("invalid persist usage");

                                let upper = write_handle.upper();
                                if !upper.is_empty() {
                                    let append = write_handle
                                        .append(empty_batch, upper.clone(), Antichain::new())
                                        .await?;

                                    if let Err(e) = append {
                                        warn!(
                                        "tried to finalize a shard with an advancing upper: {e:?}"
                                    );
                                        return Ok(());
                                    }
                                }
                                write_handle.expire().await;

                                persist_client
                                    .finalize_shard::<SourceData, (), T, Diff>(
                                        shard_id,
                                        Diagnostics::from_purpose("finalizing shards"),
                                    )
                                    .await
                            };

                            match finalize().await {
                                Err(e) => {
                                    // Rather than error, just leave this shard as one to finalize later.
                                    warn!("error during background finalization: {e:?}");
                                }
                                Ok(()) => {}
                            }
                        });
                        None
                    }
                })
                // Poll each future for each collection concurrently, maximum of 10 at a time.
                .buffer_unordered(10)
                // HERE BE DRAGONS: see warning on other uses of buffer_unordered
                // before any changes to `collect`
                .collect::<BTreeSet<Option<ShardId>>>()
                .await
                .into_iter()
                .filter_map(|shard| shard)
                .collect();

        for shard in finalized_shards {
            self.finalizable_shards.remove(&shard);
            self.finalized_shards.insert(shard);
        }
    }

    /// Determines if an `ALTER` is valid.
    fn check_alter_collection_inner(
        &self,
        id: GlobalId,
        mut ingestion: IngestionDescription,
    ) -> Result<(), StorageError> {
        // Check that the client exists.
        self.clients
            .get(&ingestion.instance_id)
            .ok_or(StorageError::IngestionInstanceMissing {
                storage_instance_id: ingestion.instance_id,
                ingestion_id: id,
            })?;

        // Take a cloned copy of the description because we are going to treat it as a "scratch
        // space".
        let mut collection_description = self.collection(id)?.description.clone();

        // Get the previous storage dependencies; we need these to understand if something has
        // changed in what we depend upon.
        let prev_storage_dependencies = collection_description.get_storage_dependencies();

        // We cannot know the metadata of exports yet to be created, so we have
        // to remove them. However, we know that adding source exports is
        // compatible, so still OK to proceed.
        ingestion
            .source_exports
            .retain(|id, _| self.collection(*id).is_ok());

        // Describe the ingestion in terms of collection metadata.
        let described_ingestion = self.enrich_ingestion(id, ingestion.clone())?;

        // Check compatibility between current and new ingestions and install new ingestion in
        // collection description.
        match &mut collection_description.data_source {
            DataSource::Ingestion(cur_ingestion) => {
                let prev_ingestion = self.enrich_ingestion(id, cur_ingestion.clone())?;
                prev_ingestion.alter_compatible(id, &described_ingestion)?;

                *cur_ingestion = ingestion;
            }
            o => {
                tracing::info!(
                    "{id:?} inalterable because its data source is {:?} and not an ingestion",
                    o
                );
                return Err(StorageError::InvalidAlter { id });
            }
        };

        let new_storage_dependencies = collection_description.get_storage_dependencies();

        if prev_storage_dependencies != new_storage_dependencies {
            tracing::info!(
                    "{id:?} inalterable because its storage dependencies have changed: were {:?} but are now {:?}",
                    prev_storage_dependencies,
                    new_storage_dependencies
                );
            return Err(StorageError::InvalidAlter { id });
        }

        Ok(())
    }

    /// For each element of `collections`, install a read hold on all of the
    /// `storage_dependencies`.
    ///
    /// Note that this adjustment is only guaranteed to be reflected in memory;
    /// downgrades to persist shards are not guaranteed to occur unless they
    /// close the shard.
    ///
    /// # Panics
    ///
    /// - If any identified collection's since is less than the dependency since
    ///   and:
    ///     - Its read policy is not `ReadPolicy::NoPolicy`
    ///     - Its read policy is `ReadPolicy::NoPolicy(f)` and the dependency
    ///       since is <= `f`.
    ///
    ///     - Its write frontier is neither `T::minimum` nor beyond the
    ///       dependency since.
    /// - If any identified collection's data source is not
    ///   [`DataSource::Ingestion] (primary source) or [`DataSource::Other`]
    ///   (subsources).
    fn install_dependency_read_holds<I: Iterator<Item = GlobalId>>(
        &mut self,
        collections: I,
        storage_dependencies: &[GlobalId],
    ) -> Result<(), StorageError> {
        let dependency_since = self.determine_collection_since_joins(storage_dependencies)?;
        let enable_asserts = self.config().parameters.enable_dependency_read_hold_asserts;

        for id in collections {
            let collection = self.collection(id).expect("known to exist");
            assert!(
                matches!(collection.description.data_source, DataSource::Other(_) | DataSource::Ingestion(_)),
                "only primary sources w/ subsources and subsources can have dependency read holds installed"
            );

            // Because of the "backward" dependency structure (primary sources
            // depend on subsources, rather than the other way around, which one
            // might expect), we do not know what the initial since of the
            // collection should be. We only find out that information once its
            // primary sources comes along and correlates the subsource to its
            // dependency sinces (e.g. remap shards).
            //
            // Once we find that out, we need ensure that the controller's
            // version of the since is sufficiently advanced so that we may
            // install the read hold.
            //
            // TODO: remove this if statement once we fix the inverse dependency
            // of subsources
            if PartialOrder::less_than(&collection.implied_capability, &dependency_since) {
                assert!(
                    match &collection.read_policy {
                        ReadPolicy::NoPolicy { initial_since } =>
                            PartialOrder::less_than(initial_since, &dependency_since),
                        _ => false,
                    } || !enable_asserts,
                    "subsources should not have external read holds installed until \
                                    their ingestion is created, but {:?} has read policy {:?}",
                    id,
                    collection.read_policy
                );

                // Patch up the implied capability + maybe the persist shard's
                // since.
                self.set_read_policy(vec![(
                    id,
                    ReadPolicy::NoPolicy {
                        initial_since: dependency_since.clone(),
                    },
                )]);

                // We have to re-borrow.
                let collection = self.collection(id).expect("known to exist");
                assert!(
                    collection.implied_capability == dependency_since || !enable_asserts,
                    "monkey patching the implied_capability to {:?} did not work, is still {:?}",
                    dependency_since,
                    collection.implied_capability,
                );
            }

            // Fill in the storage dependencies.
            let collection = self.collection_mut(id).expect("known to exist");

            assert!(
                PartialOrder::less_than(&collection.implied_capability, &collection.write_frontier)
                    // Whenever a collection is being initialized, this state is
                    // acceptable.
                    || *collection.write_frontier == [T::minimum()]
                    || !enable_asserts,
                "{id}:  the implied capability {:?} should be less than the write_frontier {:?}. Collection state dump: {:#?}",
                collection.implied_capability,
                collection.write_frontier,
                collection
            );

            collection
                .storage_dependencies
                .extend(storage_dependencies.iter().cloned());

            assert!(
                !PartialOrder::less_than(
                    &collection.read_capabilities.frontier(),
                    &collection.implied_capability.borrow()
                ) || !enable_asserts,
                "{id}: at this point, there can be no read holds for any time that is not \
                    beyond the implied capability  but we have implied_capability {:?}, \
                    read_capabilities {:?}",
                collection.implied_capability,
                collection.read_capabilities,
            );

            let read_hold = collection.implied_capability.clone();
            self.install_read_capabilities(id, storage_dependencies, read_hold)?;
        }

        Ok(())
    }

    /// Converts an `IngestionDescription<()>` into `IngestionDescription<CollectionMetadata>`.
    fn enrich_ingestion(
        &self,
        id: GlobalId,
        ingestion: IngestionDescription,
    ) -> Result<IngestionDescription<CollectionMetadata>, StorageError> {
        // The ingestion metadata is simply the collection metadata of the collection with
        // the associated ingestion
        let ingestion_metadata = self.collection(id)?.collection_metadata.clone();

        let mut source_exports = BTreeMap::new();
        for (id, export) in ingestion.source_exports {
            // Note that these metadata's have been previously enriched with the
            // required `RelationDesc` for each sub-source above!
            let storage_metadata = self.collection(id)?.collection_metadata.clone();
            source_exports.insert(
                id,
                SourceExport {
                    storage_metadata,
                    output_index: export.output_index,
                },
            );
        }

        Ok(IngestionDescription {
            source_exports,
            ingestion_metadata,
            // The rest of the fields are identical
            desc: ingestion.desc,
            instance_id: ingestion.instance_id,
            remap_collection_id: ingestion.remap_collection_id,
        })
    }

    async fn read_handle_for_snapshot(
        &self,
        id: GlobalId,
    ) -> Result<ReadHandle<SourceData, (), T, Diff>, StorageError> {
        let metadata = &self.collection(id)?.collection_metadata;

        let persist_client = self
            .persist
            .open(metadata.persist_location.clone())
            .await
            .unwrap();

        // We create a new read handle every time someone requests a snapshot and then immediately
        // expire it instead of keeping a read handle permanently in our state to avoid having it
        // heartbeat continously. The assumption is that calls to snapshot are rare and therefore
        // worth it to always create a new handle.
        let read_handle = persist_client
            .open_leased_reader::<SourceData, (), _, _>(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    handle_purpose: format!("snapshot {}", id),
                },
            )
            .await
            .expect("invalid persist usage");
        Ok(read_handle)
    }

    // TODO: This appears to have become unused at some point. Figure out if the
    // caller is coming back or if we should delete it.
    #[allow(dead_code)]
    async fn snapshot_and_stream(
        &self,
        id: GlobalId,
        as_of: T,
    ) -> Result<BoxStream<(SourceData, T, Diff)>, StorageError> {
        use futures::stream::StreamExt;

        let metadata = &self.collection(id)?.collection_metadata;
        // See the comments in Self::snapshot for what's going on here.
        match metadata.txns_shard.as_ref() {
            None => {
                let as_of = Antichain::from_elem(as_of);
                let mut read_handle = self.read_handle_for_snapshot(id).await?;
                let contents = read_handle.snapshot_and_stream(as_of).await;
                match contents {
                    Ok(contents) => {
                        Ok(Box::pin(contents.map(|((result_k, result_v), t, diff)| {
                            let () = result_v.expect("invalid empty value");
                            let data = result_k.expect("invalid key data");
                            (data, t, diff)
                        })))
                    }
                    Err(_) => Err(StorageError::ReadBeforeSince(id)),
                }
            }
            Some(txns_id) => {
                let txns_read = self.txns.expect_enabled_lazy(txns_id);
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
                let mut handle = self.read_handle_for_snapshot(id).await?;
                let contents = data_snapshot.snapshot_and_stream(&mut handle).await;
                match contents {
                    Ok(contents) => {
                        Ok(Box::pin(contents.map(|((result_k, result_v), t, diff)| {
                            let () = result_v.expect("invalid empty value");
                            let data = result_k.expect("invalid key data");
                            (data, t, diff)
                        })))
                    }
                    Err(_) => Err(StorageError::ReadBeforeSince(id)),
                }
            }
        }
    }

    /// Handles writing of status updates for sources/sinks to the appropriate
    /// status relation
    async fn record_status_updates(&mut self, updates: Vec<StatusUpdate>) {
        let mut sink_status_updates = vec![];
        let mut source_status_updates = vec![];

        for update in updates {
            let id = update.id;
            if self.exports.contains_key(&id) {
                sink_status_updates.push(update);
            } else if self.collections.contains_key(&id) {
                source_status_updates.push(update);
            }
        }

        self.collection_status_manager
            .append_updates(
                source_status_updates,
                IntrospectionType::SourceStatusHistory,
            )
            .await;
        self.collection_status_manager
            .append_updates(sink_status_updates, IntrospectionType::SinkStatusHistory)
            .await;
    }
}
