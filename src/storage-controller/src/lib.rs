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
use std::fmt::{Debug, Display};
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
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::critical::SinceHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_txn::metrics::Metrics as TxnMetrics;
use mz_persist_txn::txn_read::TxnsRead;
use mz_persist_txn::txns::TxnsHandle;
use mz_persist_txn::INIT_FORGET_ALL;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{Codec64, Opaque};
use mz_proto::RustType;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{ColumnName, Datum, Diff, GlobalId, RelationDesc, Row, TimestampManipulation};
use mz_storage_client::client::{
    ProtoStorageCommand, ProtoStorageResponse, RunIngestionCommand, RunSinkCommand, Status,
    StatusUpdate, StorageCommand, StorageResponse, TimestamplessUpdate,
};
use mz_storage_client::controller::{
    CollectionDescription, DataSource, DataSourceOther, ExportDescription, ExportState,
    IntrospectionType, MonotonicAppender, Response, SnapshotCursor, StorageController,
    StorageMetadata, StorageTxn,
};
use mz_storage_client::metrics::StorageControllerMetrics;
use mz_storage_client::statistics::{
    SinkStatisticsUpdate, SourceStatisticsUpdate, WebhookStatistics,
};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::{
    AlterError, CollectionMetadata, PersistTxnTablesImpl, StorageError, TxnsCodecRow,
};
use mz_storage_types::dyncfgs::STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION;
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::{ReadHold, ReadHoldError};
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sinks::{StorageSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, SourceConnection, SourceData, SourceDesc,
    SourceExport,
};
use mz_storage_types::AlterCompatible;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::watch::{channel, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamMap;
use tracing::{debug, info, warn};

use crate::persist_handles::SnapshotStatsAsOf;
use crate::rehydration::RehydratingStorageClient;
mod collection_mgmt;
mod collection_status;
mod persist_handles;
mod rehydration;
mod statistics;

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
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Controller<T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation>
{
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A function that returns the current time.
    now: NowFn,
    /// The fencing token for this instance of the controller.
    envd_epoch: NonZeroI64,

    /// The set of [`ShardId`]s to finalize.
    ///
    /// This is a separate set from `finalized_shards` because we know that
    /// some environments have many, many finalizable shards that we are
    /// struggling to finalize.
    finalizable_shards: BTreeSet<ShardId>,

    /// The set of [`ShardId`]s we have finalized.
    ///
    /// This is a separate set from `finalizable_shards` because we know that
    /// some environments have many, many finalizable shards that we are
    /// struggling to finalize.
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
    /// Channel for sending table handle drops.
    #[derivative(Debug = "ignore")]
    pending_table_handle_drops_tx: mpsc::UnboundedSender<GlobalId>,
    /// Channel for receiving table handle drops.
    #[derivative(Debug = "ignore")]
    pending_table_handle_drops_rx: mpsc::UnboundedReceiver<GlobalId>,

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

    /// Channel for sending changes to [ReadHolds](ReadHold) back to us, the
    /// issuer, sending side.
    read_holds_tx: tokio::sync::mpsc::UnboundedSender<(GlobalId, ChangeBatch<T>)>,

    /// Channel for sending changes to [ReadHolds](ReadHold) back to us, the
    /// issuer, receiving side.
    read_holds_rx: tokio::sync::mpsc::UnboundedReceiver<(GlobalId, ChangeBatch<T>)>,

    /// `(read, write)` frontiers that have been recorded in the `Frontiers` collection, kept to be
    /// able to retract old rows.
    recorded_frontiers: BTreeMap<GlobalId, (Antichain<T>, Antichain<T>)>,
    /// Write frontiers that have been recorded in the `ReplicaFrontiers` collection, kept to be
    /// able to retract old rows.
    recorded_replica_frontiers: BTreeMap<(GlobalId, ReplicaId), Antichain<T>>,
}

#[async_trait(?Send)]
impl<'a, T> StorageController for Controller<T>
where
    T: Timestamp
        + Lattice
        + TotalOrder
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<Datum<'a>>
        + Display,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    type Timestamp = T;

    fn initialization_complete(&mut self) {
        self.reconcile_dangling_statistics();
        self.initialized = true;
        for client in self.clients.values_mut() {
            client.send(StorageCommand::InitializationComplete);
        }
    }

    fn update_parameters(&mut self, config_params: StorageParameters) {
        // We serialize the dyncfg updates in StorageParameters, but configure
        // persist separately.
        self.persist.cfg().apply_from(&config_params.dyncfg_updates);

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

    fn collection_metadata(
        &self,
        id: GlobalId,
    ) -> Result<CollectionMetadata, StorageError<Self::Timestamp>> {
        let res = self.collection(id)?.collection_metadata.clone();

        Ok(res)
    }

    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<
        (Antichain<Self::Timestamp>, Antichain<Self::Timestamp>),
        StorageError<Self::Timestamp>,
    > {
        let collection = self.collection(id)?;

        let res = (
            collection.implied_capability.clone(),
            collection.write_frontier.clone(),
        );

        Ok(res)
    }

    fn collections_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Result<Vec<(GlobalId, Antichain<T>, Antichain<T>)>, StorageError<Self::Timestamp>> {
        let res = ids
            .into_iter()
            .map(|id| {
                self.collections
                    .get(&id)
                    .map(|c| {
                        (
                            id.clone(),
                            c.implied_capability.clone(),
                            c.write_frontier.clone(),
                        )
                    })
                    .ok_or(StorageError::IdentifierMissing(id))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(res)
    }

    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
        self.collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| (*id, c.collection_metadata.clone()))
            .collect()
    }

    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>> {
        let _collection = self.collection(id)?;
        Ok(())
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

    /// Create and "execute" the described collection.
    ///
    /// "Execute" is in scare quotes because what executing a collection means
    /// varies widely based on the type of collection you're creating.
    ///
    /// The general process creating a collection undergoes is:
    /// 1. Enrich the description we get from the user with the metadata only
    ///    the storage controller's metadata. This is mostly a matter of
    ///    separating concerns.
    /// 2. Generate write and read persist handles for the collection.
    /// 3. Store the collection's metadata in the appropriate field.
    /// 4. "Execte" the collection. What that means is contingent on the type of
    ///    collection. so consult the code for more details.
    ///
    // TODO(aljoscha): It would be swell if we could refactor this Leviathan of
    // a method/move individual parts to their own methods. @guswynn observes
    // that a number of these operations could be moved into fns on
    // `DataSource`.
    #[instrument(name = "storage::create_collections")]
    async fn create_collections(
        &mut self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
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
                let data_shard = storage_metadata.get_collection_shard::<T>(id)?;

                let get_shard = |id| -> Result<ShardId, StorageError<T>> {
                    let shard = match self.collections.get(&id) {
                        Some(col) => col.collection_metadata.data_shard,
                        None => storage_metadata.get_collection_shard::<T>(id)?,
                    };
                    Ok(shard)
                };

                let status_shard = match description.status_collection_id {
                    Some(status_collection_id) => Some(get_shard(status_collection_id)?),
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
                        Some(get_shard(*remap_collection_id)?)
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
                    | DataSource::IngestionExport { .. }
                    | DataSource::Introspection(_)
                    | DataSource::Progress
                    | DataSource::Webhook
                    | DataSource::Other(DataSourceOther::Compute) => None,
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
        let mut to_register: Vec<_> = futures::stream::iter(enriched_with_metadata)
            .map(|data: Result<_, StorageError<Self::Timestamp>>| {
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
                    | DataSource::IngestionExport { .. }
                    | DataSource::Webhook
                    | DataSource::Ingestion(_)
                    | DataSource::Progress
                    | DataSource::Other(DataSourceOther::Compute) => {},
                    DataSource::Other(DataSourceOther::TableWrites) => {
                        let register_ts = register_ts.expect("caller should have provided a register_ts when creating a table");
                        if since_handle.since().elements() == &[T::minimum()] {
                            debug!("advancing {} to initial since of {:?}", id, register_ts);
                            let token = since_handle.opaque().clone();
                            let _ = since_handle.compare_and_downgrade_since(&token, (&token, &Antichain::from_elem(register_ts.clone()))).await;
                        }
                    }
                }

                Ok::<_, StorageError<T>>((id, description, write, since_handle, metadata))
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

        // Reorder in dependency order.
        to_register.sort_by_key(|(id, ..)| *id);

        // The set of collections that we should render at the end of this
        // function.
        let mut to_execute = BTreeSet::new();
        // New collections that are being created; this is distinct from the set
        // of collections we plan to execute because
        // `DataSource::IngestionExport` is added as a new collection, but is
        // not executed directly.
        let mut new_collections = BTreeSet::new();
        let mut table_registers = Vec::with_capacity(to_register.len());

        // Statistics need a level of indirection so we can mutably borrow
        // `self` when registering collections and when we are inserting
        // statistics.
        let mut new_source_statistic_entries = BTreeSet::new();
        let mut new_webhook_statistic_entries = BTreeSet::new();

        for (id, mut description, write, since_handle, metadata) in to_register {
            to_execute.insert(id);
            new_collections.insert(id);

            // Ensure that the ingestion has an export for its primary source.
            // This is done in an akward spot to appease the borrow checker.
            if let DataSource::Ingestion(ingestion) = &mut description.data_source {
                ingestion.source_exports.insert(
                    id,
                    SourceExport {
                        output_index: 0,
                        storage_metadata: (),
                    },
                );
            }

            let write_frontier = write.upper();
            let data_shard_since = since_handle.since().clone();

            // Determine if this collection has another dependency.
            let storage_dependency =
                self.determine_collection_dependency(&description.data_source)?;

            // Determine the intial since of the collection.
            let initial_since = match storage_dependency {
                Some(dep) => {
                    let dependency_collection = self.collection(dep)?;
                    let dependency_since = dependency_collection.implied_capability.clone();

                    // If an item has a dependency, its initial since must be
                    // advanced as far as its dependency, i.e. a dependency's
                    // since may never be in advance of its dependents.
                    //
                    // We have to do this every time we initialize the
                    // collection, though––the invariant might have been upheld
                    // correctly in the previous epoch, but the
                    // `data_shard_since` might not have compacted and, on
                    // establishing a new persist connection, still have data we
                    // said _could_ be compacted.
                    if PartialOrder::less_than(&data_shard_since, &dependency_since) {
                        // The dependency since cannot be in advance of the
                        // dependent upper unless the collection is new. If the
                        // dependency since advanced past the dependent's upper,
                        // the dependent cannot read data from the dependency at
                        // its upper.
                        //
                        // Another way of understanding that this is a problem
                        // is that this means that the read hold installed on
                        // the dependency was probably not been upheld––if it
                        // were, the dependency's since could not have advanced
                        // as far the dependent's upper.
                        mz_ore::soft_assert_or_log!(
                            write_frontier.elements() == &[T::minimum()]
                                || PartialOrder::less_than(&dependency_since, write_frontier),
                            "dependency ({dep}) since has advanced past dependent ({id}) upper \n
                            dependent ({id}): since {:?}, upper {:?} \n
                            dependency ({dep}): since {:?}",
                            data_shard_since,
                            write_frontier,
                            dependency_since
                        );

                        dependency_since
                    } else {
                        data_shard_since
                    }
                }
                None => data_shard_since,
            };

            let collection_state = CollectionState::new(
                description,
                initial_since,
                write_frontier.clone(),
                storage_dependency,
                metadata.clone(),
            );

            // Install the collection state in the appropriate spot.
            match &collection_state.description.data_source {
                DataSource::Introspection(_) => {
                    debug!(desc = ?collection_state.description, meta = ?metadata, "registering {} with persist monotonic worker", id);
                    self.persist_monotonic_worker.register(id, write);
                    self.collections.insert(id, collection_state);
                }
                DataSource::Webhook => {
                    debug!(desc = ?collection_state.description, meta = ?metadata, "registering {} with persist monotonic worker", id);
                    self.persist_monotonic_worker.register(id, write);
                    self.collections.insert(id, collection_state);
                    new_source_statistic_entries.insert(id);
                    // This collection of statistics is periodically aggregated into
                    // `source_statistics`.
                    new_webhook_statistic_entries.insert(id);
                }
                DataSource::IngestionExport {
                    ingestion_id,
                    external_reference,
                } => {
                    debug!(desc = ?collection_state.description, meta = ?metadata, "not registering {} with a controller persist worker", id);
                    // Adjust the source to contain this export.
                    let source_collection = self
                        .collections
                        .get_mut(ingestion_id)
                        .expect("known to exist");
                    match &mut source_collection.description {
                        CollectionDescription {
                            data_source: DataSource::Ingestion(ingestion_desc),
                            ..
                        } => {
                            // DataSource::IngestionExport names the object it
                            // wants to export, so we look up the output index
                            // for that name.
                            let output_index = ingestion_desc
                                .desc
                                .connection
                                .output_idx_for_name(external_reference)
                                .ok_or(StorageError::MissingSubsourceReference {
                                    ingestion_id: *ingestion_id,
                                    reference: external_reference.clone(),
                                })?;

                            ingestion_desc.source_exports.insert(
                                id,
                                SourceExport {
                                    output_index,
                                    storage_metadata: (),
                                },
                            )
                        }
                        _ => unreachable!(
                            "SourceExport must only refer to primary sources that already exist"
                        ),
                    };

                    // Executing the source export doesn't do anything, ensure we execute the source instead.
                    to_execute.remove(&id);
                    to_execute.insert(*ingestion_id);

                    self.collections.insert(id, collection_state);
                    new_source_statistic_entries.insert(id);
                }
                DataSource::Other(DataSourceOther::TableWrites) => {
                    debug!(desc = ?collection_state.description, meta = ?metadata, "registering {} with persist table worker", id);
                    table_registers.push((id, write, collection_state));
                }
                DataSource::Progress | DataSource::Other(DataSourceOther::Compute) => {
                    debug!(desc = ?collection_state.description, meta = ?metadata, "not registering {} with a controller persist worker", id);
                    self.collections.insert(id, collection_state);
                }
                DataSource::Ingestion(_) => {
                    debug!(desc = ?collection_state.description, meta = ?metadata, "not registering {} with a controller persist worker", id);
                    self.collections.insert(id, collection_state);
                    new_source_statistic_entries.insert(id);
                }
            }

            self.persist_read_handles.register(id, since_handle);

            // If this collection has a dependency, install a read hold on it.
            self.install_collection_dependency_read_holds(id)?;
        }

        {
            // Enusre all sources are associated with the statistics.
            let mut source_statistics = self.source_statistics.lock().expect("poisoned");
            source_statistics.source_statistics.extend(
                new_source_statistic_entries
                    .into_iter()
                    .map(|id| (id, None)),
            );
            source_statistics.webhook_statistics.extend(
                new_webhook_statistic_entries
                    .into_iter()
                    .map(|id| (id, Default::default())),
            );
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
                .register(register_ts, table_registers)
                .await
                .expect("table worker unexpectedly shut down");
            for (id, mut collection_state) in collection_states {
                if let PersistTxns::EnabledLazy { .. } = &self.txns {
                    if collection_state.write_frontier.less_than(&advance_to) {
                        collection_state.write_frontier = Antichain::from_elem(advance_to.clone());
                    }
                }
                self.collections.insert(id, collection_state);
            }
        }

        self.append_shard_mappings(new_collections.into_iter(), 1)
            .await;

        self.synchronize_finalized_shards(storage_metadata);

        // TODO(guswynn): perform the io in this final section concurrently.
        for id in to_execute {
            let description = &self.collection(id)?.description;
            match &description.data_source {
                DataSource::Ingestion(_) => {
                    self.run_ingestion(id)?;
                }
                DataSource::IngestionExport { .. } => unreachable!(
                    "ingestion exports do not execute directly, but instead schedule their source to be re-executed"
                ),
                DataSource::Introspection(i) => {
                    let prev = self
                        .introspection_ids
                        .lock()
                        .expect("poisoned lock")
                        .insert(*i, id);
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
                                self.metrics.clone(),
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
                                    self.metrics.clone(),
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
                        | IntrospectionType::ComputeOperatorHydrationStatus
                        | IntrospectionType::ComputeMaterializedViewRefreshes => {
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

    fn check_alter_ingestion_source_desc(
        &mut self,
        ingestion_id: GlobalId,
        source_desc: &SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let data_source = &self.collection(ingestion_id)?.description.data_source;
        match &data_source {
            DataSource::Ingestion(cur_ingestion) => {
                cur_ingestion
                    .desc
                    .alter_compatible(ingestion_id, source_desc)?;

                // Ensure updated `SourceDesc` contains reference to all
                // current external references.
                for export_id in cur_ingestion
                    .source_exports
                    .keys()
                    .filter(|export| **export != ingestion_id)
                {
                    let collection = self
                        .collection(*export_id)
                        .map_err(|_| AlterError { id: ingestion_id })?;

                    let external_reference = match &collection.description.data_source {
                        DataSource::IngestionExport {
                            external_reference, ..
                        } => external_reference,
                        o => {
                            tracing::warn!(
                                "{export_id:?} not DataSource::IngestionExport but {o:#?}",
                            );
                            Err(AlterError { id: ingestion_id })?
                        }
                    };

                    if source_desc
                        .connection
                        .output_idx_for_name(external_reference)
                        .is_none()
                    {
                        tracing::warn!(
                            "subsource {export_id} of {ingestion_id} refers to \
                            {external_reference:?}, which is missing from \
                            updated SourceDesc \n{source_desc:#?}"
                        );
                        Err(AlterError { id: ingestion_id })?
                    }
                }
            }
            o => {
                tracing::info!(
                    "{ingestion_id} inalterable because its data source is {:?} and not an ingestion",
                    o
                );
                Err(AlterError { id: ingestion_id })?
            }
        }

        Ok(())
    }

    async fn alter_ingestion_source_desc(
        &mut self,
        ingestion_id: GlobalId,
        source_desc: SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        self.check_alter_ingestion_source_desc(ingestion_id, &source_desc)?;

        let collection = self.collection(ingestion_id).expect("validated exists");
        let curr_ingestion = match &collection.description.data_source {
            DataSource::Ingestion(active_ingestion) => active_ingestion,
            _ => unreachable!("verified collection refers to ingestion"),
        };

        mz_ore::soft_assert_ne_or_log!(
            curr_ingestion.desc,
            source_desc,
            "alter_ingestion_source_desc should only be called when producing new SourceDesc",
        );

        // Generate new source exports because they might have changed.
        let mut source_exports = BTreeMap::new();
        // Each source includes a `0` output index export "for the
        // primary source", whether it's used or not.
        source_exports.insert(
            ingestion_id,
            SourceExport {
                output_index: 0,
                storage_metadata: (),
            },
        );

        // Get the updated output indices for each source export.
        //
        // TODO(#26766): this could be simpler if the output indices
        // were determined in rendering, e.g. `SourceExport` had an
        // `Option<UnresolvedItemName>` instead of a `usize` and we
        // looked up its output index when we were aligning the
        // rendering outputs.
        for export_id in curr_ingestion.source_exports.keys() {
            if *export_id == ingestion_id {
                // Already inserted above
                continue;
            }

            let DataSource::IngestionExport {
                ingestion_id,
                external_reference,
            } = &self.collection(*export_id)?.description.data_source
            else {
                panic!("source exports must be DataSource::IngestionExport")
            };

            let output_index = source_desc
                .connection
                .output_idx_for_name(external_reference)
                .ok_or(StorageError::MissingSubsourceReference {
                    ingestion_id: *ingestion_id,
                    reference: external_reference.clone(),
                })?;

            source_exports.insert(
                *export_id,
                SourceExport {
                    output_index,
                    storage_metadata: (),
                },
            );
        }

        // Update the `SourceDesc` and the source exports
        // simultaneously.
        let collection = self
            .collections
            .get_mut(&ingestion_id)
            .expect("validated exists");
        let curr_ingestion = match &mut collection.description.data_source {
            DataSource::Ingestion(curr_ingestion) => curr_ingestion,
            _ => unreachable!("verified collection refers to ingestion"),
        };
        curr_ingestion.desc = source_desc;
        curr_ingestion.source_exports = source_exports;
        tracing::debug!("altered {ingestion_id}'s SourceDesc");

        // n.b. we do not re-run updated ingestions because updating the source
        // desc is only done in preparation for adding subsources, which will
        // then run the ingestion.
        //
        // If this expectation ever changes, we will almost certainly know
        // because failing to run an altered ingestion means that whatever
        // changes you expect to occur will not be reflected in the running
        // dataflow.

        Ok(())
    }

    async fn alter_ingestion_connections(
        &mut self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let mut ingestions_to_run = BTreeSet::new();

        for (id, conn) in source_connections {
            let collection = self
                .collections
                .get_mut(&id)
                .ok_or_else(|| StorageError::IdentifierMissing(id))?;

            match &mut collection.description.data_source {
                DataSource::Ingestion(ingestion) => {
                    // If the connection hasn't changed, there's no sense in
                    // re-rendering the dataflow.
                    if ingestion.desc.connection != conn {
                        ingestion.desc.connection = conn;
                        ingestions_to_run.insert(id);
                    } else {
                        tracing::debug!(
                            "update_source_connection called on {id} but the \
                            connection was the same"
                        );
                    }
                }
                o => {
                    tracing::warn!("update_source_connection called on {:?}", o);
                    Err(StorageError::IdentifierInvalid(id))?;
                }
            }
        }

        for id in ingestions_to_run {
            self.run_ingestion(id)?;
        }
        Ok(())
    }

    fn export(
        &self,
        id: GlobalId,
    ) -> Result<&ExportState<Self::Timestamp>, StorageError<Self::Timestamp>> {
        self.exports
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn export_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut ExportState<Self::Timestamp>, StorageError<Self::Timestamp>> {
        self.exports
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    async fn create_exports(
        &mut self,
        exports: Vec<(GlobalId, ExportDescription<Self::Timestamp>)>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
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
            let dep_collection = self.collection(from_id)?;
            let dependency_since = dep_collection.implied_capability.clone();
            self.install_read_capability(id, from_id, dependency_since.clone())?;

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
    async fn alter_export_connections(
        &mut self,
        exports: BTreeMap<GlobalId, StorageSinkConnection>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
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

    // Dropping a table takes roughly the following flow:
    //
    //   1. We remove the table from the persist table write worker.
    //   2. The table removal is awaited in an async task.
    //   3. A message is sent to the storage controller that the table has been removed from the
    //      table write worker.
    //   4. The controller drains all table drop messages during `process`.
    //   5. `process` calls `drop_sources` with the dropped tables.
    fn drop_tables(
        &mut self,
        identifiers: Vec<GlobalId>,
        ts: Self::Timestamp,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        assert!(
            identifiers
                .iter()
                .all(|id| self.collections[id].description.is_table()),
            "identifiers contain non-tables: {:?}",
            identifiers
                .iter()
                .filter(|id| !self.collections[id].description.is_table())
                .collect::<Vec<_>>()
        );
        let drop_notif = self
            .persist_table_worker
            .drop_handles(identifiers.clone(), ts);
        let tx = self.pending_table_handle_drops_tx.clone();
        mz_ore::task::spawn(|| "table-cleanup".to_string(), async move {
            drop_notif.await;
            for identifier in identifiers {
                let _ = tx.send(identifier);
            }
        });
        Ok(())
    }

    fn drop_sources(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        self.validate_collection_ids(identifiers.iter().cloned())?;
        self.drop_sources_unvalidated(storage_metadata, identifiers)
    }

    fn drop_sources_unvalidated(
        &mut self,
        storage_metadata: &StorageMetadata,
        ids: Vec<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let mut ingestions_to_execute = BTreeSet::new();
        let mut to_drop = BTreeSet::new();
        for id in ids {
            let metadata = storage_metadata.get_collection_shard::<T>(id);
            mz_ore::soft_assert_or_log!(
                matches!(metadata, Err(StorageError::IdentifierMissing(_))),
                "dropping {id}, but drop was not synchronized with storage \
                controller via `synchronize_collections`"
            );

            let dropped_data_source = match self.collection(id) {
                Ok(col) => col.description.data_source.clone(),
                Err(_) => continue,
            };

            to_drop.insert(id);

            // If we are dropping source exports, we need to modify the
            // ingestion that it runs on.
            if let DataSource::IngestionExport { ingestion_id, .. } = dropped_data_source {
                // If we remove this export, we need to stop producing data to
                // it, so plan to re-execute the ingestion with the amended
                // description.
                ingestions_to_execute.insert(ingestion_id);

                // Adjust the source to remove this export.
                let ingestion_collection = match self.collections.get_mut(&ingestion_id) {
                    Some(ingestion_collection) => ingestion_collection,
                    // Primary ingestion already dropped.
                    None => {
                        tracing::error!(
                            "primary source {ingestion_id} seemingly dropped before subsource {id}",
                        );
                        continue;
                    }
                };

                match &mut ingestion_collection.description {
                    CollectionDescription {
                        data_source: DataSource::Ingestion(ingestion_desc),
                        ..
                    } => {
                        let removed = ingestion_desc.source_exports.remove(&id);
                        mz_ore::soft_assert_or_log!(
                            removed.is_some(),
                            "dropped subsource {id} already removed from source exports"
                        );
                    }
                    _ => unreachable!(
                        "SourceExport must only refer to primary sources that already exist"
                    ),
                };
            }
        }

        // Do not bother re-executing ingestions we know we plan to drop.
        ingestions_to_execute.retain(|id| !to_drop.contains(id));
        for ingestion_id in ingestions_to_execute {
            self.run_ingestion(ingestion_id)?;
        }

        self.synchronize_finalized_shards(storage_metadata);

        // We don't explicitly remove read capabilities! Downgrading the
        // frontier of the source to `[]` (the empty Antichain), will propagate
        // to the storage dependencies.
        self.set_read_policy(
            to_drop
                .into_iter()
                .map(|id| (id, ReadPolicy::ValidFrom(Antichain::new())))
                .collect(),
        );
        Ok(())
    }

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    fn drop_sinks(
        &mut self,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
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
    ) -> Result<
        tokio::sync::oneshot::Receiver<Result<(), StorageError<Self::Timestamp>>>,
        StorageError<Self::Timestamp>,
    > {
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

    fn monotonic_appender(
        &self,
        id: GlobalId,
    ) -> Result<MonotonicAppender<Self::Timestamp>, StorageError<Self::Timestamp>> {
        assert!(self.txns_init_run);
        self.collection_manager.monotonic_appender(id)
    }

    fn webhook_statistics(
        &self,
        id: GlobalId,
    ) -> Result<Arc<WebhookStatistics>, StorageError<Self::Timestamp>> {
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
    ) -> Result<Vec<(Row, Diff)>, StorageError<Self::Timestamp>> {
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
    ) -> Result<SnapshotCursor<Self::Timestamp>, StorageError<Self::Timestamp>>
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
    ) -> Result<SnapshotStats, StorageError<Self::Timestamp>> {
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
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError<Self::Timestamp>>> {
        // See the comments in Self::snapshot for what's going on here.
        let read_handle = self.read_handle_for_snapshot(id).await;
        let data_snapshot = match self.collection(id) {
            Err(e) => Err(e),
            Ok(CollectionState {
                collection_metadata:
                    CollectionMetadata {
                        txns_shard: Some(txns_id),
                        data_shard,
                        ..
                    },
                ..
            }) => {
                let as_of = as_of
                    .as_option()
                    .expect("cannot read as_of the empty antichain");
                let txns_read = self.txns.expect_enabled_lazy(txns_id);
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read.data_snapshot(*data_shard, as_of.clone()).await;
                Ok(Some(data_snapshot))
            }
            Ok(_) => Ok(None),
        };

        Box::pin(async move {
            let mut read_handle = read_handle?;
            let result = match data_snapshot? {
                Some(data_snapshot) => data_snapshot.snapshot_parts_stats(&mut read_handle).await,
                None => read_handle.snapshot_parts_stats(as_of).await,
            };
            read_handle.expire().await;
            result.map_err(|_| StorageError::ReadBeforeSince(id))
        })
    }

    #[instrument(level = "debug")]
    fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
        let mut read_capability_changes = BTreeMap::default();

        for (id, policy) in policies.into_iter() {
            let collection = self
                .collections
                .get_mut(&id)
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

    fn acquire_read_holds(
        &mut self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, ReadHoldError> {
        let mut holds = Vec::new();
        // NOTE: We acquire read holds at the earliest possible time rather than
        // at the implied capability. This is so that, for example, adapter can
        // acquire a read hold to hold back the frontier, giving the COMPUTE
        // controller a chance to also acquire a read hold at that early
        // frontier. If/when we change the interplay between adapter and COMPUTE
        // to pass around ReadHold tokens, we might tighten this up and instead
        // acquire read holds at the implied capability.
        //
        // Context: the read capabilities include the implied capability, in
        // addition to all outstanding read holds. This means the frontier of
        // read capabilities will always be less_equal compared to the implied
        // capability, and it can never be true that the implied capability is
        // less_than the frontier of read capabilities.
        for id in desired_holds.iter() {
            let collection = self
                .collections
                .get(id)
                .ok_or(ReadHoldError::CollectionMissing(*id))?;
            let desired_hold = collection.read_capabilities.frontier().to_owned();
            holds.push((*id, desired_hold));
        }

        let mut updates = holds
            .iter()
            .map(|(id, hold)| {
                let mut changes = ChangeBatch::new();
                changes.extend(hold.iter().map(|time| (time.clone(), 1)));
                (*id, changes)
            })
            .collect::<BTreeMap<_, _>>();

        self.update_read_capabilities(&mut updates);

        let acquired_holds = holds
            .into_iter()
            .map(|(id, since)| ReadHold::new(id, since, self.read_holds_tx.clone()))
            .collect_vec();

        tracing::debug!(?desired_holds, ?acquired_holds, "acquire_read_holds");

        Ok(acquired_holds)
    }

    #[instrument(level = "debug", fields(updates))]
    fn update_write_frontiers(&mut self, updates: &[(GlobalId, Antichain<Self::Timestamp>)]) {
        let mut read_capability_changes = BTreeMap::default();

        for (id, new_upper) in updates.iter() {
            if let Some(collection) = self.collections.get_mut(id) {
                if PartialOrder::less_than(&collection.write_frontier, new_upper) {
                    collection.write_frontier.clone_from(new_upper);
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
                    export.write_frontier.clone_from(new_upper);
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
            if let Some(collection) = self.collections.get_mut(&key) {
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

                for id in collection.storage_dependency.iter() {
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
                frontier.clone_from(&export.read_capability);
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

        // We cannot check the channel without reading, so try and read, but
        // then put it back, so that `process()` can work it off.
        if let Ok(response) = self.read_holds_rx.try_recv() {
            // IMPORTANT: The consuming side makes sure to always read off all
            // updates in the channel, combining updates for the same ID, before
            // working them off. Without this we might get into bad situations
            // where we retract things to early or incorrectly.
            self.read_holds_tx
                .send(response)
                .expect("cannot fail to re-enqueue");

            // Return, to signal readyness for processing!
            return;
        }

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
    async fn process(
        &mut self,
        storage_metadata: &StorageMetadata,
    ) -> Result<Option<Response<T>>, anyhow::Error> {
        let mut updated_frontiers = None;
        match self.stashed_response.take() {
            None => (),
            Some(StorageResponse::FrontierUppers(updates)) => {
                self.update_write_frontiers(&updates);
                updated_frontiers = Some(Response::FrontierUpdates(updates));
            }
            Some(StorageResponse::DroppedIds(ids)) => {
                let shards_to_finalize = ids
                    .iter()
                    .filter_map(|id| {
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
                    })
                    .collect();

                self.finalize_shards(shards_to_finalize).await;
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
                                None => {
                                    *current = Some(stat.with_metrics(&self.metrics));
                                }
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

        if let Ok((id, read_hold_change)) = self.read_holds_rx.try_recv() {
            let mut read_hold_changes = BTreeMap::new();
            read_hold_changes.insert(id, read_hold_change);

            // IMPORTANT: We read off all changes, combine them, and then
            // process as a whole. We need this for correctness because of the
            // "stunt" we're pulling in `ready()` where we `try_recv()` and then
            // re-enqueue for determining whether there's any updates to be
            // worked off.
            while let Ok((id, read_hold_change)) = self.read_holds_rx.try_recv() {
                read_hold_changes
                    .entry(id)
                    .or_default()
                    .extend(read_hold_change.into_inner().into_iter());
            }

            tracing::debug!(?read_hold_changes, "changes to storage read holds");
            self.update_read_capabilities(&mut read_hold_changes);
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

        // Process dropped tables in a single batch.
        let mut dropped_table_ids = Vec::new();
        while let Ok(dropped_id) = self.pending_table_handle_drops_rx.try_recv() {
            dropped_table_ids.push(dropped_id);
        }
        if !dropped_table_ids.is_empty() {
            self.drop_sources(storage_metadata, dropped_table_ids)?;
        }

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
                if self.collections.contains_key(&id) {
                    pending_source_drops.push(id);
                } else if self.exports.contains_key(&id) {
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
                        // Hacky, return an empty future so the IDs are finalized below.
                        Some(async move {}.boxed())
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
                    DataSource::IngestionExport { .. } if read_frontier.is_empty() => {
                        // Dropping an ingestion is a form of dropping a source.
                        // This won't be handled above because ingestion exports
                        // do not yet track the cluster on pending compaction
                        // commands.
                        //
                        // TODO(#8185): place the cluster ID in the pending compaction
                        // commands of IngestionExports.
                        pending_source_drops.push(id);
                        None
                    }
                    // These sources are manged by `clusterd`.
                    DataSource::Webhook
                    | DataSource::Introspection(_)
                    | DataSource::Other(_)
                    | DataSource::Progress
                    | DataSource::Ingestion(_)
                    | DataSource::IngestionExport { .. } => None,
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
                if self.collections.contains_key(&id) {
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
        for (object_id, since, upper) in self.active_collection_frontiers() {
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
                let read_frontier = since.into_option().map_or(Datum::Null, |ts| ts.into());
                let write_frontier = upper.into_option().map_or(Datum::Null, |ts| ts.into());
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
        for (object_id, collection) in self.collections.iter().filter(|(_id, c)| !c.is_dropped()) {
            let replica_id = collection
                .cluster_id()
                .and_then(|c| self.replicas.get(&c))
                .copied();
            if let Some(replica_id) = replica_id {
                let upper = collection.write_frontier.clone();
                frontiers.insert((*object_id, replica_id), upper);
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
            let write_frontier = upper.into_option().map_or(Datum::Null, |ts| ts.into());
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
    async fn init_txns(&mut self, init_ts: T) -> Result<(), StorageError<Self::Timestamp>> {
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
        if INIT_FORGET_ALL.get(txns_client.dyncfgs()) {
            let (removed, _tidy) = txns
                .forget_all(init_ts.clone())
                .await
                .map_err(|_| StorageError::InvalidUppers(vec![]))?;
            info!("init_txns removed from txns shard: {:?}", removed);
        } else {
            // More limited version of the above to mitigate #25992. This is all
            // that should be necessary (and probably more than we need,
            // strictly) now that we've removed the old tables impl. Guarantees:
            // - That we were able to write to the txns shard at `init_ts` (a
            //   timestamp given to us by the coordinator).
            // - That all txn writes through `init_ts` have been applied
            //   (materialized physically in the data shards).
            let mut empty_txn = txns.begin();
            let apply = empty_txn
                .commit_at(&mut txns, init_ts.clone())
                .await
                .map_err(|_| StorageError::InvalidUppers(vec![]))?;
            let _tidy = apply.apply_eager(&mut txns).await;
            info!("init_txns committed at and applied through {:?}", init_ts);
        }

        drop(txns);

        self.txns_init_run = true;
        Ok(())
    }

    async fn initialize_state(
        &mut self,
        txn: &mut dyn StorageTxn<T>,
        init_ids: BTreeSet<GlobalId>,
        drop_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<T>> {
        let metadata = txn.get_collection_metadata();
        let processed_metadata: Result<Vec<_>, _> = metadata
            .into_iter()
            .map(|(id, shard)| ShardId::from_str(&shard).map(|shard| (id, shard)))
            .collect();
        let metadata = processed_metadata.map_err(|e| StorageError::Generic(anyhow::anyhow!(e)))?;
        let existing_metadata: BTreeSet<_> = metadata.into_iter().map(|(id, _)| id).collect();

        // Determine which collections we do not yet have metadata for.
        let new_collections: BTreeSet<GlobalId> = init_ids
            .iter()
            .filter(|id| !existing_metadata.contains(id))
            .cloned()
            .collect();

        mz_ore::soft_assert_or_log!(
            new_collections.iter().all(|id| id.is_system()),
            "initializing collections should only be missing metadata for new system objects, but got {:?}",
            new_collections
        );

        self.prepare_state(txn, new_collections, drop_ids).await?;

        // All shards that belong to collections dropped in the last epoch are
        // eligible for finalization. This intentionally includes any built-in
        // collections present in `drop_ids`.
        //
        // n.b. this introduces an unlikely race condition: if a collection is
        // dropped from the catalog, but the dataflow is still running on a
        // worker, assuming the shard is safe to finalize on reboot may cause
        // the cluster to panic.
        if self.config.parameters.finalize_shards {
            self.finalizable_shards.extend(
                txn.get_unfinalized_shards()
                    .into_iter()
                    .map(|shard| ShardId::from_str(&shard).expect("deserialization corrupted")),
            );
        }

        Ok(())
    }

    async fn prepare_state(
        &mut self,
        txn: &mut dyn StorageTxn<T>,
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<T>> {
        txn.insert_collection_metadata(
            ids_to_add
                .into_iter()
                .map(|id| (id, ShardId::new().to_string()))
                .collect(),
        )?;

        // Delete the metadata for any dropped collections.
        let dropped_mappings = txn.delete_collection_metadata(ids_to_drop);

        let dropped_shards = dropped_mappings
            .into_iter()
            .map(|(_id, shard)| shard)
            .collect();

        txn.insert_unfinalized_shards(dropped_shards)?;

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
pub fn prepare_initialization<T>(txn: &mut dyn StorageTxn<T>) -> Result<(), StorageError<T>> {
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
        txn: &dyn StorageTxn<T>,
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

        let (pending_table_handle_drops_tx, pending_table_handle_drops_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let (read_holds_tx, read_holds_rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            build_info,
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
            pending_table_handle_drops_tx,
            pending_table_handle_drops_rx,
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
            config: StorageConfiguration::new(connection_context, mz_dyncfgs::all_dyncfgs()),
            internal_response_sender: tx,
            internal_response_queue: rx,
            read_holds_tx,
            read_holds_rx,
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
    ) -> Result<(), StorageError<T>> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_export_ids(
        &self,
        ids: impl Iterator<Item = GlobalId>,
    ) -> Result<(), StorageError<T>> {
        for id in ids {
            self.export(id)?;
        }
        Ok(())
    }

    /// Iterate over exports that have not been dropped.
    fn active_exports(&self) -> impl Iterator<Item = (GlobalId, &ExportState<T>)> {
        self.exports
            .iter()
            .filter(|(_id, e)| !e.is_dropped())
            .map(|(id, e)| (*id, e))
    }

    /// Returns the frontier of read capabilities and the write frontier for all
    /// active collections.
    ///
    /// This is different from [StorageController::collection_frontiers] which
    /// returns the implied capability and the write frontier.
    ///
    /// A collection is "active" when it has a non empty frontier of read
    /// capabilties.
    fn active_collection_frontiers(&self) -> Vec<(GlobalId, Antichain<T>, Antichain<T>)> {
        let res = self
            .collections
            .iter()
            .filter(|(_id, c)| !c.is_dropped())
            .map(|(id, c)| {
                (
                    id.clone(),
                    c.read_capabilities.frontier().to_owned(),
                    c.write_frontier.clone(),
                )
            })
            .collect_vec();

        res
    }

    /// Install read capabilities on the given `storage_dependency`.
    fn install_read_capability(
        &mut self,
        _from_id: GlobalId,
        storage_dependency: GlobalId,
        read_capability: Antichain<T>,
    ) -> Result<(), StorageError<T>> {
        let mut changes = ChangeBatch::new();
        for time in read_capability.iter() {
            changes.update(time.clone(), 1);
        }

        let mut storage_read_updates = BTreeMap::from_iter([(storage_dependency, changes)]);
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

    /// Remove any shards that we know are finalized
    fn synchronize_finalized_shards(&mut self, storage_metadata: &StorageMetadata) {
        self.finalized_shards.retain(|shard| {
            storage_metadata
                .unfinalized_shards
                .contains(shard.to_string().as_str())
        });
    }

    /// Attempts to close all shards marked for finalization.
    #[allow(dead_code)]
    #[instrument(level = "debug")]
    async fn finalize_shards(&mut self, new_shards: Vec<ShardId>) {
        if !self.config.parameters.finalize_shards {
            info!("not triggering shard finalization due to dropped storage object because enable_storage_shard_finalization parameter is false");
            return;
        }

        info!("triggering shard finalization due to dropped storage object");

        self.finalizable_shards.extend(new_shards);
        if self.finalizable_shards.is_empty() {
            info!("no shards to finalize");
        }

        // Open a persist client to delete unused shards.
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();

        let persist_client = &persist_client;
        let diagnostics = &Diagnostics::from_purpose("finalizing shards");

        let force_downgrade_since =
            STORAGE_DOWNGRADE_SINCE_DURING_FINALIZATION.get(self.config.config_set());

        let epoch = &PersistEpoch::from(self.envd_epoch);

        use futures::stream::StreamExt;
        let finalized_shards: BTreeSet<ShardId> =
            futures::stream::iter(self.finalizable_shards.clone())
                .map(|shard_id| async move {
                    let persist_client = persist_client.clone();
                    let diagnostics = diagnostics.clone();
                    let epoch = epoch.clone();

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

                                if force_downgrade_since {
                                    let mut since_handle: SinceHandle<
                                        SourceData,
                                        (),
                                        T,
                                        Diff,
                                        PersistEpoch,
                                    > = persist_client
                                        .open_critical_since(
                                            shard_id,
                                            PersistClient::CONTROLLER_CRITICAL_SINCE,
                                            Diagnostics::from_purpose("finalizing shards"),
                                        )
                                        .await
                                        .expect("invalid persist usage");
                                    let epoch = epoch.clone();
                                    let new_since = Antichain::new();
                                    let downgrade = since_handle
                                        .compare_and_downgrade_since(&epoch, (&epoch, &new_since))
                                        .await;
                                    if let Err(e) = downgrade {
                                        warn!(
                                        "tried to finalize a shard with an advancing epoch: {e:?}"
                                    );
                                        return Ok(());
                                    }
                                }

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

        for id in finalized_shards {
            self.finalizable_shards.remove(&id);
            self.finalized_shards.insert(id);
        }
    }

    /// Determine if this collection has another dependency.
    ///
    /// Currently, collections have either 0 or 1 dependencies.
    fn determine_collection_dependency(
        &self,
        data_source: &DataSource,
    ) -> Result<Option<GlobalId>, StorageError<T>> {
        let dependency = match &data_source {
            DataSource::Introspection(_)
            | DataSource::Webhook
            | DataSource::Other(DataSourceOther::TableWrites)
            | DataSource::Progress
            | DataSource::Other(DataSourceOther::Compute) => None,
            DataSource::IngestionExport { ingestion_id, .. } => {
                // Ingestion exports depend on their primary source's remap
                // collection.
                let source_collection = self.collection(*ingestion_id)?;
                match &source_collection.description {
                    CollectionDescription {
                        data_source: DataSource::Ingestion(ingestion_desc),
                        ..
                    } => Some(ingestion_desc.remap_collection_id),
                    _ => unreachable!(
                        "SourceExport must only refer to primary sources that already exist"
                    ),
                }
            }
            // Ingestions depend on their remap collection.
            DataSource::Ingestion(ingestion) => Some(ingestion.remap_collection_id),
        };

        Ok(dependency)
    }

    /// If this identified collection has a dependency, install a read hold on
    /// it.
    ///
    /// This is necessary to ensure that the dependency's since does not advance
    /// beyond its dependents'.
    fn install_collection_dependency_read_holds(
        &mut self,
        id: GlobalId,
    ) -> Result<(), StorageError<T>> {
        let (dep, collection_implied_capability) = match self.collection(id) {
            Ok(CollectionState {
                storage_dependency: Some(dep),
                implied_capability,
                ..
            }) => (dep, implied_capability),
            _ => return Ok(()),
        };

        let dep_collection = self.collection(*dep)?;

        mz_ore::soft_assert_or_log!(
            PartialOrder::less_equal(
                &dep_collection.implied_capability,
                collection_implied_capability
            ),
            "dependency since cannot be in advance of dependent's since"
        );

        self.install_read_capability(id, *dep, collection_implied_capability.clone())
    }

    async fn read_handle_for_snapshot(
        &self,
        id: GlobalId,
    ) -> Result<ReadHandle<SourceData, (), T, Diff>, StorageError<T>> {
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
                USE_CRITICAL_SINCE_SNAPSHOT.get(persist_client.dyncfgs()),
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
    ) -> Result<BoxStream<(SourceData, T, Diff)>, StorageError<T>> {
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

    fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, StorageError<T>> {
        self.collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    /// Runs the identified ingestion using the current definition of the
    /// ingestion in-memory.
    fn run_ingestion(&mut self, id: GlobalId) -> Result<(), StorageError<T>> {
        let collection = self.collection(id)?;
        let ingestion_description = match &collection.description.data_source {
            DataSource::Ingestion(i) => i.clone(),
            _ => {
                tracing::warn!("run_ingestion called on non-ingestion ID {}", id);
                Err(StorageError::IdentifierInvalid(id))?
            }
        };

        // Enrich all of the exports with their metadata
        let mut source_exports = BTreeMap::new();
        for (
            export_id,
            SourceExport {
                output_index,
                storage_metadata: (),
            },
        ) in ingestion_description.source_exports
        {
            let export_storage_metadata = self.collection(export_id)?.collection_metadata.clone();
            source_exports.insert(
                export_id,
                SourceExport {
                    storage_metadata: export_storage_metadata,
                    output_index,
                },
            );
        }

        let description = IngestionDescription::<CollectionMetadata> {
            source_exports,
            // The ingestion metadata is simply the collection metadata of the collection with
            // the associated ingestion
            ingestion_metadata: collection.collection_metadata.clone(),
            // The rest of the fields are identical
            desc: ingestion_description.desc,
            instance_id: ingestion_description.instance_id,
            remap_collection_id: ingestion_description.remap_collection_id,
        };

        let storage_instance_id = description.instance_id;
        // Fetch the client for this ingestion's instance.
        let client = self.clients.get_mut(&storage_instance_id).ok_or_else(|| {
            StorageError::IngestionInstanceMissing {
                storage_instance_id,
                ingestion_id: id,
            }
        })?;

        let augmented_ingestion = RunIngestionCommand { id, description };
        client.send(StorageCommand::RunIngestions(vec![augmented_ingestion]));

        Ok(())
    }
}

/// State maintained about individual collections.
#[derive(Debug)]
pub struct CollectionState<T> {
    /// Description with which the collection was created
    pub description: CollectionDescription<T>,

    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may also contain
    /// capabilities held by others who have read dependencies on this collection.
    pub read_capabilities: MutableAntichain<T>,
    /// The implicit capability associated with collection creation.  This should never be less
    /// than the since of the associated persist collection.
    pub implied_capability: Antichain<T>,
    /// The policy to use to downgrade `self.implied_capability`.
    pub read_policy: ReadPolicy<T>,

    /// An optional storage identify that this collection may depend on.
    ///
    /// This could become a vec in the future, we just currently have either 0
    /// or 1 dependencies.
    pub storage_dependency: Option<GlobalId>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,

    pub collection_metadata: CollectionMetadata,
}

impl<T: Timestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
    pub fn new(
        description: CollectionDescription<T>,
        since: Antichain<T>,
        write_frontier: Antichain<T>,
        storage_dependency: Option<GlobalId>,
        metadata: CollectionMetadata,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            description,
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::NoPolicy {
                initial_since: since,
            },
            storage_dependency,
            write_frontier,
            collection_metadata: metadata,
        }
    }

    /// Returns the cluster to which the collection is bound, if applicable.
    pub fn cluster_id(&self) -> Option<StorageInstanceId> {
        match &self.description.data_source {
            DataSource::Ingestion(ingestion) => Some(ingestion.instance_id),
            DataSource::Webhook
            | DataSource::Introspection(_)
            | DataSource::Other(_)
            // TODO(#8185) This isn't quite right because a source export runs
            // on the ingestion's cluster, but we don't yet support announcing
            // that.
            | DataSource::IngestionExport { .. }
            | DataSource::Progress => None,
        }
    }

    /// Returns whether the collection was dropped.
    pub fn is_dropped(&self) -> bool {
        self.read_capabilities.is_empty()
    }
}
