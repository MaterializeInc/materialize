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
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};
use std::num::NonZeroI64;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::stream::BoxStream;
use futures::FutureExt;
use itertools::Itertools;
use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_client::{ReplicaId, WallclockLagFn};
use mz_controller_types::dyncfgs::{ENABLE_0DT_DEPLOYMENT_SOURCES, WALLCLOCK_LAG_REFRESH_INTERVAL};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::{assert_none, instrument, soft_panic_or_log};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::schema::CaESchema;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::schema::SchemaId;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{ColumnName, Datum, Diff, GlobalId, RelationDesc, Row, TimestampManipulation};
use mz_storage_client::client::{
    ProtoStorageCommand, ProtoStorageResponse, RunIngestionCommand, RunSinkCommand, Status,
    StatusUpdate, StorageCommand, StorageResponse, TimestamplessUpdate,
};
use mz_storage_client::controller::{
    BoxFuture, CollectionDescription, DataSource, DataSourceOther, ExportDescription, ExportState,
    IntrospectionType, MonotonicAppender, PersistEpoch, Response, SnapshotCursor,
    StorageController, StorageMetadata, StorageTxn, StorageWriteOp,
};
use mz_storage_client::metrics::StorageControllerMetrics;
use mz_storage_client::statistics::{
    SinkStatisticsUpdate, SourceStatisticsUpdate, WebhookStatistics,
};
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::{AlterError, CollectionMetadata, StorageError, TxnsCodecRow};
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
use mz_txn_wal::metrics::Metrics as TxnMetrics;
use mz_txn_wal::txn_read::TxnsRead;
use mz_txn_wal::txns::TxnsHandle;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::MutableAntichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::mpsc;
use tokio::sync::watch::{channel, Sender};
use tokio::time::error::Elapsed;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::collection_mgmt::{
    partially_truncate_status_history, AppendOnlyIntrospectionConfig, CollectionManagerKind,
    DifferentialIntrospectionConfig,
};
use crate::instance::{Instance, ReplicaConfig};
use crate::statistics::StatsState;

mod collection_mgmt;
mod collection_status;
mod history;
mod instance;
mod persist_handles;
mod rtr;
mod statistics;

#[derive(Derivative)]
#[derivative(Debug)]
struct PendingCompactionCommand<T> {
    /// [`GlobalId`] of the collection we want to compact.
    id: GlobalId,
    /// [`Antichain`] representing the requested read frontier.
    read_frontier: Antichain<T>,
    /// Cluster associated with this collection, if any.
    cluster_id: Option<StorageInstanceId>,
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

    /// Whether or not this controller is in read-only mode.
    ///
    /// When in read-only mode, neither this controller nor the instances
    /// controlled by it are allowed to affect changes to external systems
    /// (largely persist).
    read_only: bool,

    /// Collections maintained by the storage controller.
    ///
    /// This collection only grows, although individual collections may be rendered unusable.
    /// This is to prevent the re-binding of identifiers to other descriptions.
    pub(crate) collections: BTreeMap<GlobalId, CollectionState<T>>,

    pub(crate) exports: BTreeMap<GlobalId, ExportState<T>>,

    /// Write handle for table shards.
    pub(crate) persist_table_worker: persist_handles::PersistTableWriteWorker<T>,
    /// A shared TxnsCache running in a task and communicated with over a channel.
    txns_read: TxnsRead<T>,
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
    introspection_tokens: Arc<Mutex<BTreeMap<GlobalId, Box<dyn Any + Send + Sync>>>>,

    // The following two fields must always be locked in order.
    /// Consolidated metrics updates to periodically write. We do not eagerly initialize this,
    /// and its contents are entirely driven by `StorageResponse::StatisticsUpdates`'s, as well
    /// as webhook statistics.
    source_statistics: Arc<Mutex<statistics::SourceStatistics>>,
    /// Consolidated metrics updates to periodically write. We do not eagerly initialize this,
    /// and its contents are entirely driven by `StorageResponse::StatisticsUpdates`'s.
    sink_statistics: Arc<Mutex<BTreeMap<GlobalId, statistics::StatsState<SinkStatisticsUpdate>>>>,
    /// A way to update the statistics interval in the statistics tasks.
    statistics_interval_sender: Sender<Duration>,

    /// Clients for all known storage instances.
    instances: BTreeMap<StorageInstanceId, Instance<T>>,
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

    /// A function that computes the lag between the given time and wallclock time.
    #[derivative(Debug = "ignore")]
    wallclock_lag: WallclockLagFn<T>,
    /// The last time wallclock lag introspection was refreshed.
    wallclock_lag_last_refresh: Instant,

    /// Handle to a [StorageCollections].
    storage_collections: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
    /// Migrated storage collections that can be written even in read only mode.
    migrated_storage_collections: BTreeSet<GlobalId>,

    /// Ticker for scheduling periodic maintenance work.
    maintenance_ticker: tokio::time::Interval,
    /// Whether maintenance work was scheduled.
    maintenance_scheduled: bool,

    /// Shared transmit channel for replicas to send responses.
    instance_response_tx: mpsc::UnboundedSender<StorageResponse<T>>,
    /// Receive end for replica responses.
    instance_response_rx: mpsc::UnboundedReceiver<StorageResponse<T>>,
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
        + Into<Datum<'static>>
        + Display,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    type Timestamp = T;

    fn initialization_complete(&mut self) {
        self.reconcile_dangling_statistics();
        self.initialized = true;

        for instance in self.instances.values_mut() {
            instance.send(StorageCommand::InitializationComplete);
        }
    }

    fn update_parameters(&mut self, config_params: StorageParameters) {
        self.storage_collections
            .update_parameters(config_params.clone());

        // We serialize the dyncfg updates in StorageParameters, but configure
        // persist separately.
        self.persist.cfg().apply_from(&config_params.dyncfg_updates);

        for instance in self.instances.values_mut() {
            instance.send(StorageCommand::UpdateConfiguration(config_params.clone()));
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
        self.storage_collections.collection_metadata(id)
    }

    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<
        (Antichain<Self::Timestamp>, Antichain<Self::Timestamp>),
        StorageError<Self::Timestamp>,
    > {
        Ok(match self.export(id) {
            Ok(export) => (
                export.read_hold.since().clone(),
                export.write_frontier.clone(),
            ),
            Err(_) => {
                let frontiers = self.storage_collections.collection_frontiers(id)?;
                (frontiers.implied_capability, frontiers.write_frontier)
            }
        })
    }

    fn collections_frontiers(
        &self,
        mut ids: Vec<GlobalId>,
    ) -> Result<Vec<(GlobalId, Antichain<T>, Antichain<T>)>, StorageError<Self::Timestamp>> {
        // The ids might be either normal collections or exports. Both have frontiers that might be
        // interesting to external observers.
        let mut result = vec![];
        ids.retain(|&id| match self.export(id) {
            Ok(export) => {
                result.push((
                    id,
                    export.read_hold.since().clone(),
                    export.write_frontier.clone(),
                ));
                false
            }
            Err(_) => true,
        });

        result.extend(
            self.storage_collections
                .collections_frontiers(ids)?
                .into_iter()
                .map(|frontiers| {
                    (
                        frontiers.id,
                        frontiers.implied_capability,
                        frontiers.write_frontier,
                    )
                }),
        );

        Ok(result)
    }

    fn active_collection_metadatas(&self) -> Vec<(GlobalId, CollectionMetadata)> {
        self.storage_collections.active_collection_metadatas()
    }

    fn active_ingestions(&self, instance_id: StorageInstanceId) -> &BTreeSet<GlobalId> {
        self.instances[&instance_id].active_ingestions()
    }

    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>> {
        self.storage_collections.check_exists(id)
    }

    fn create_instance(&mut self, id: StorageInstanceId) {
        let metrics = self.metrics.for_instance(id);
        let mut instance = Instance::new(
            self.envd_epoch,
            metrics,
            self.now.clone(),
            self.instance_response_tx.clone(),
        );
        if self.initialized {
            instance.send(StorageCommand::InitializationComplete);
        }
        if !self.read_only {
            instance.send(StorageCommand::AllowWrites);
        }
        instance.send(StorageCommand::UpdateConfiguration(
            self.config.parameters.clone(),
        ));
        let old_instance = self.instances.insert(id, instance);
        assert_none!(old_instance, "storage instance {id} already exists");
    }

    fn drop_instance(&mut self, id: StorageInstanceId) {
        let instance = self.instances.remove(&id);
        assert!(instance.is_some(), "storage instance {id} does not exist");
    }

    fn connect_replica(
        &mut self,
        instance_id: StorageInstanceId,
        replica_id: ReplicaId,
        location: ClusterReplicaLocation,
    ) {
        let instance = self
            .instances
            .get_mut(&instance_id)
            .unwrap_or_else(|| panic!("instance {instance_id} does not exist"));

        let config = ReplicaConfig {
            build_info: self.build_info,
            location,
            grpc_client: self.config.parameters.grpc_client.clone(),
        };
        instance.add_replica(replica_id, config);
    }

    fn drop_replica(&mut self, instance_id: StorageInstanceId, replica_id: ReplicaId) {
        self.instances
            .get_mut(&instance_id)
            .unwrap_or_else(|| panic!("instance {instance_id} does not exist"))
            .drop_replica(replica_id);
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
    /// 4. "Execute" the collection. What that means is contingent on the type of
    ///    collection. so consult the code for more details.
    ///
    // TODO(aljoscha): It would be swell if we could refactor this Leviathan of
    // a method/move individual parts to their own methods. @guswynn observes
    // that a number of these operations could be moved into fns on
    // `DataSource`.
    #[instrument(name = "storage::create_collections")]
    async fn create_collections_for_bootstrap(
        &mut self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Self::Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription<Self::Timestamp>)>,
        migrated_storage_collections: &BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        self.migrated_storage_collections
            .clone_from(migrated_storage_collections);

        self.storage_collections
            .create_collections_for_bootstrap(
                storage_metadata,
                register_ts.clone(),
                collections.clone(),
                migrated_storage_collections,
            )
            .await?;

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

        // We first enrich each collection description with some additional metadata...
        let enriched_with_metadata = collections
            .into_iter()
            .map(|(id, description)| {
                let data_shard = storage_metadata.get_collection_shard::<T>(id)?;

                let get_shard = |id| -> Result<ShardId, StorageError<T>> {
                    let shard = storage_metadata.get_collection_shard::<T>(id)?;
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

                // If the shard is being managed by txn-wal (initially, tables), then we need to
                // pass along the shard id for the txns shard to dataflow rendering.
                let txns_shard = description
                    .data_source
                    .in_txns()
                    .then(|| *self.txns_read.txns_id());

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

        // So that we can open persist handles for each collections concurrently.
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();
        let persist_client = &persist_client;

        // Reborrow the `&mut self` as immutable, as all the concurrent work to be processed in
        // this stream cannot all have exclusive access.
        use futures::stream::{StreamExt, TryStreamExt};
        let this = &*self;
        let mut to_register: Vec<_> = futures::stream::iter(enriched_with_metadata)
            .map(|data: Result<_, StorageError<Self::Timestamp>>| {
                async move {
                let (id, description, metadata) = data?;

                // should be replaced with real introspection (https://github.com/MaterializeInc/database-issues/issues/4078)
                // but for now, it's helpful to have this mapping written down somewhere
                debug!(
                    "mapping GlobalId={} to remap shard ({:?}), data shard ({}), status shard ({:?})",
                    id, metadata.remap_shard, metadata.data_shard, metadata.status_shard
                );

                let write = this
                    .open_data_handles(
                        &id,
                        metadata.data_shard,
                        metadata.relation_desc.clone(),
                        persist_client,
                    )
                    .await;

                Ok::<_, StorageError<T>>((id, description, write, metadata))
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

        for (id, mut description, write, metadata) in to_register {
            to_execute.insert(id);
            new_collections.insert(id);

            // Ensure that the ingestion has an export for its primary source.
            // This is done in an awkward spot to appease the borrow checker.
            if let DataSource::Ingestion(ingestion) = &mut description.data_source {
                ingestion
                    .source_exports
                    .insert(id, ingestion.desc.primary_source_export());
            }

            let write_frontier = write.upper();

            // Determine if this collection has another dependency.
            let storage_dependencies =
                self.determine_collection_dependencies(id, &description.data_source)?;

            let dependency_read_holds = self
                .storage_collections
                .acquire_read_holds(storage_dependencies)
                .expect("can acquire read holds");

            let mut dependency_since = Antichain::from_elem(T::minimum());
            for read_hold in dependency_read_holds.iter() {
                dependency_since.join_assign(read_hold.since());
            }

            // Assert some invariants.
            if !dependency_read_holds.is_empty() {
                // The dependency since cannot be beyond the dependent (our)
                // upper unless the collection is new. In practice, the
                // depdenency is the remap shard of a source (export), and if
                // the since is allowed to "catch up" to the upper, that is
                // `upper <= since`, a restarting ingestion cannot differentiate
                // between updates that have already been written out to the
                // backing persist shard and updates that have yet to be
                // written. We would write duplicate updates.
                //
                // If this check fails, it means that the read hold installed on
                // the dependency was probably not upheld –– if it were, the
                // dependency's since could not have advanced as far the
                // dependent's upper.
                //
                // We don't care about the dependency since when the write
                // frontier is empty. In that case, no-one can write down any
                // more updates.
                mz_ore::soft_assert_or_log!(
                    write_frontier.elements() == &[T::minimum()]
                        || write_frontier.is_empty()
                        || PartialOrder::less_than(&dependency_since, write_frontier),
                    "dependency since has advanced past dependent ({id}) upper \n
                            dependent ({id}): upper {:?} \n
                            dependency since {:?} \n
                            dependency read holds: {:?}",
                    write_frontier,
                    dependency_since,
                    dependency_read_holds,
                );
            }

            let mut collection_state = CollectionState {
                data_source: description.data_source.clone(),
                collection_metadata: metadata.clone(),
                extra_state: CollectionStateExtra::None,
                wallclock_lag_max: Default::default(),
            };

            // Install the collection state in the appropriate spot.
            match &collection_state.data_source {
                DataSource::Introspection(typ) => {
                    debug!(data_source = ?collection_state.data_source, meta = ?metadata, "registering {} with persist monotonic worker", id);
                    // We always register the collection with the collection manager,
                    // regardless of read-only mode. The CollectionManager itself is
                    // aware of read-only mode and will not attempt to write before told
                    // to do so.
                    //
                    self.register_introspection_collection(id, *typ, write)
                        .await?;
                    self.collections.insert(id, collection_state);
                }
                DataSource::Webhook => {
                    debug!(data_source = ?collection_state.data_source, meta = ?metadata, "registering {} with persist monotonic worker", id);
                    self.collections.insert(id, collection_state);
                    new_source_statistic_entries.insert(id);
                    // This collection of statistics is periodically aggregated into
                    // `source_statistics`.
                    new_webhook_statistic_entries.insert(id);
                    // Register the collection so our manager knows about it.
                    //
                    // NOTE: Maybe this shouldn't be in the collection manager,
                    // and collection manager should only be responsible for
                    // built-in introspection collections?
                    self.collection_manager
                        .register_append_only_collection(id, write, false, None);
                }
                DataSource::IngestionExport {
                    ingestion_id,
                    details,
                    data_config,
                } => {
                    debug!(data_source = ?collection_state.data_source, meta = ?metadata, "not registering {} with a controller persist worker", id);
                    // Adjust the source to contain this export.
                    let ingestion_state = self
                        .collections
                        .get_mut(ingestion_id)
                        .expect("known to exist");

                    let instance_id = match &mut ingestion_state.data_source {
                        DataSource::Ingestion(ingestion_desc) => {
                            ingestion_desc.source_exports.insert(
                                id,
                                SourceExport {
                                    storage_metadata: (),
                                    details: details.clone(),
                                    data_config: data_config.clone(),
                                },
                            );

                            // Record the ingestion's cluster ID for the
                            // ingestion export. This way we always have a
                            // record of it, even if the ingestion's collection
                            // description disappears.
                            ingestion_desc.instance_id
                        }
                        _ => unreachable!(
                            "SourceExport must only refer to primary sources that already exist"
                        ),
                    };

                    // Executing the source export doesn't do anything, ensure we execute the source instead.
                    to_execute.remove(&id);
                    to_execute.insert(*ingestion_id);

                    let ingestion_state = IngestionState {
                        read_capabilities: MutableAntichain::from(dependency_since.clone()),
                        dependency_read_holds,
                        derived_since: dependency_since,
                        write_frontier: Antichain::from_elem(Self::Timestamp::minimum()),
                        hold_policy: ReadPolicy::step_back(),
                        instance_id,
                    };

                    collection_state.extra_state = CollectionStateExtra::Ingestion(ingestion_state);

                    self.collections.insert(id, collection_state);
                    new_source_statistic_entries.insert(id);
                }
                DataSource::Other(DataSourceOther::TableWrites) => {
                    debug!(data_source = ?collection_state.data_source, meta = ?metadata, "registering {} with persist table worker", id);
                    self.collections.insert(id, collection_state);
                    table_registers.push((id, write));
                }
                DataSource::Progress | DataSource::Other(DataSourceOther::Compute) => {
                    debug!(data_source = ?collection_state.data_source, meta = ?metadata, "not registering {} with a controller persist worker", id);
                    self.collections.insert(id, collection_state);
                }
                DataSource::Ingestion(ingestion_desc) => {
                    debug!(?ingestion_desc, meta = ?metadata, "not registering {} with a controller persist worker", id);

                    let mut dependency_since = Antichain::from_elem(T::minimum());
                    for read_hold in dependency_read_holds.iter() {
                        dependency_since.join_assign(read_hold.since());
                    }

                    let ingestion_state = IngestionState {
                        read_capabilities: MutableAntichain::from(dependency_since.clone()),
                        dependency_read_holds,
                        derived_since: dependency_since,
                        write_frontier: Antichain::from_elem(Self::Timestamp::minimum()),
                        hold_policy: ReadPolicy::step_back(),
                        instance_id: ingestion_desc.instance_id,
                    };

                    collection_state.extra_state = CollectionStateExtra::Ingestion(ingestion_state);

                    self.collections.insert(id, collection_state);
                    new_source_statistic_entries.insert(id);
                }
            }
        }

        {
            // Ensure all sources are associated with the statistics.
            //
            // We currently do not call `create_collections` after we have initialized the source
            // statistics scrapers, but in the interest of safety, avoid overriding existing
            // statistics values.
            let mut source_statistics = self.source_statistics.lock().expect("poisoned");

            for id in new_source_statistic_entries {
                source_statistics
                    .source_statistics
                    .entry(id)
                    .or_insert(StatsState::new(SourceStatisticsUpdate::new(id)));
            }
            for id in new_webhook_statistic_entries {
                source_statistics.webhook_statistics.entry(id).or_default();
            }
        }

        // Register the tables all in one batch.
        if !table_registers.is_empty() {
            let register_ts = register_ts
                .expect("caller should have provided a register_ts when creating a table");

            if self.read_only {
                // In read-only mode, we use a special read-only table worker
                // that allows writing to migrated tables and will continually
                // bump their shard upper so that it tracks the txn shard upper.
                // We do this, so that they remain readable at a recent
                // timestamp, which in turn allows dataflows that depend on them
                // to (re-)hydrate.
                //
                // We only want to register migrated tables, though, and leave
                // existing tables out/never write to them in read-only mode.
                table_registers
                    .retain(|(id, _write_handle)| migrated_storage_collections.contains(id));

                self.persist_table_worker
                    .register(register_ts, table_registers)
                    .await
                    .expect("table worker unexpectedly shut down");
            } else {
                self.persist_table_worker
                    .register(register_ts, table_registers)
                    .await
                    .expect("table worker unexpectedly shut down");
            }
        }

        self.append_shard_mappings(new_collections.into_iter(), 1);

        // TODO(guswynn): perform the io in this final section concurrently.
        for id in to_execute {
            match &self.collection(id)?.data_source {
                DataSource::Ingestion(ingestion) => {
                    if !self.read_only || (
                        ENABLE_0DT_DEPLOYMENT_SOURCES.get(self.config.config_set())
                        && ingestion.desc.connection.supports_read_only()
                    ) {
                        self.run_ingestion(id)?;
                    }
                }
                DataSource::IngestionExport { .. } => unreachable!(
                    "ingestion exports do not execute directly, but instead schedule their source to be re-executed"
                ),
                DataSource::Introspection(_) => {}
                DataSource::Webhook => {}
                DataSource::Progress | DataSource::Other(_) => {}
            };
        }

        Ok(())
    }

    fn check_alter_ingestion_source_desc(
        &mut self,
        ingestion_id: GlobalId,
        source_desc: &SourceDesc,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let source_collection = self.collection(ingestion_id)?;
        let data_source = &source_collection.data_source;
        match &data_source {
            DataSource::Ingestion(cur_ingestion) => {
                cur_ingestion
                    .desc
                    .alter_compatible(ingestion_id, source_desc)?;
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

        // Update the `SourceDesc` and the source exports
        // simultaneously.
        let collection = self
            .collections
            .get_mut(&ingestion_id)
            .expect("validated exists");
        let curr_ingestion = match &mut collection.data_source {
            DataSource::Ingestion(curr_ingestion) => curr_ingestion,
            _ => unreachable!("verified collection refers to ingestion"),
        };

        curr_ingestion.desc = source_desc;
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
        // Also have to let StorageCollections know!
        self.storage_collections
            .alter_ingestion_connections(source_connections.clone())
            .await?;

        let mut ingestions_to_run = BTreeSet::new();

        for (id, conn) in source_connections {
            let collection = self
                .collections
                .get_mut(&id)
                .ok_or_else(|| StorageError::IdentifierMissing(id))?;

            match &mut collection.data_source {
                DataSource::Ingestion(ingestion) => {
                    // If the connection hasn't changed, there's no sense in
                    // re-rendering the dataflow.
                    if ingestion.desc.connection != conn {
                        tracing::info!(from = ?ingestion.desc.connection, to = ?conn, "alter_ingestion_connections, updating");
                        ingestion.desc.connection = conn;
                        ingestions_to_run.insert(id);
                    } else {
                        tracing::warn!(
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

    async fn alter_table_desc(
        &mut self,
        table_id: GlobalId,
        new_desc: RelationDesc,
        expected_schema: SchemaId,
        forget_ts: Self::Timestamp,
        register_ts: Self::Timestamp,
    ) -> Result<SchemaId, StorageError<Self::Timestamp>> {
        let shard_id = {
            let Controller {
                collections,
                storage_collections,
                ..
            } = self;

            // TODO(parkmycar): We're threading a needle here to make sure we don't
            // leave either the Controller or StorageCollections in an inconsistent
            // state. We should refactor this to be more robust.

            // Before letting StorageCollections know, make sure we know about this
            // collection.
            let collection = collections
                .get_mut(&table_id)
                .ok_or(StorageError::IdentifierMissing(table_id))?;
            if !matches!(
                collection.data_source,
                DataSource::Other(DataSourceOther::TableWrites)
            ) {
                return Err(StorageError::IdentifierInvalid(table_id));
            }

            // Now also let StorageCollections know!
            storage_collections.alter_table_desc(table_id, new_desc.clone())?;
            // StorageCollections was successfully updated, now we can update our
            // in-memory state.
            collection.collection_metadata.relation_desc = new_desc.clone();

            collection.collection_metadata.data_shard
        };

        let diagnostics = Diagnostics {
            shard_name: table_id.to_string(),
            handle_purpose: "alter_table_desc".to_string(),
        };

        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .expect("invalid persist location");
        let schema_result = persist_client
            .compare_and_evolve_schema::<SourceData, (), T, Diff>(
                shard_id,
                expected_schema,
                &new_desc,
                &UnitSchema,
                diagnostics,
            )
            .await
            .map_err(|e| StorageError::InvalidUsage(e.to_string()))?;
        let schema_id = match schema_result {
            CaESchema::Ok(id) => id,
            // TODO(alter_table): Better handling of these errors.
            CaESchema::ExpectedMismatch { .. } => {
                return Err(StorageError::Generic(anyhow::anyhow!(
                    "schema expected mismatch, {table_id:?}",
                )))
            }
            CaESchema::Incompatible => {
                return Err(StorageError::Generic(anyhow::anyhow!(
                    "schema incompatible, {table_id:?}"
                )))
            }
        };

        let write_handle = self
            .open_data_handles(&table_id, shard_id, new_desc.clone(), &persist_client)
            .await;

        self.persist_table_worker
            .update(table_id, forget_ts, register_ts, write_handle);

        Ok(schema_id)
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

            // Acquire read holds at StorageCollections to ensure that the
            // sinked collection is not dropped while we're sinking it.
            let desired_read_holds = vec![from_id.clone()];
            let read_hold = self
                .storage_collections
                .acquire_read_holds(desired_read_holds)
                .expect("missing dependency")
                .into_element();

            info!(
                sink_id = id.to_string(),
                from_id = from_id.to_string(),
                acquired_read_hold = ?read_hold,
                "sink acquired read holds"
            );
            let read_policy = ReadPolicy::step_back();

            info!(
                sink_id = id.to_string(),
                from_id = from_id.to_string(),
                as_of = ?description.sink.as_of,
                "create_exports: creating sink"
            );

            self.exports.insert(
                id,
                ExportState::new(description.clone(), read_hold, read_policy),
            );

            // Just like with `new_source_statistic_entries`, we can probably
            // `insert` here, but in the interest of safety, never override
            // existing values.
            self.sink_statistics
                .lock()
                .expect("poisoned")
                .entry(id)
                .or_insert(StatsState::new(SinkStatisticsUpdate::new(id)));

            if !self.read_only {
                self.run_export(id)?;
            }
        }
        Ok(())
    }

    async fn alter_export(
        &mut self,
        id: GlobalId,
        new_description: ExportDescription<Self::Timestamp>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let from_id = new_description.sink.from;

        // Acquire read holds at StorageCollections to ensure that the
        // sinked collection is not dropped while we're sinking it.
        let desired_read_holds = vec![from_id.clone()];
        let read_hold = self
            .storage_collections
            .acquire_read_holds(desired_read_holds)
            .expect("missing dependency")
            .into_element();
        let from_storage_metadata = self.storage_collections.collection_metadata(from_id)?;

        // Check whether the sink's write frontier is beyond the read hold we got
        let cur_export = self
            .exports
            .get_mut(&id)
            .ok_or_else(|| StorageError::IdentifierMissing(id))?;
        let input_readable = cur_export
            .write_frontier
            .iter()
            .all(|t| read_hold.since().less_than(t));
        if !input_readable {
            return Err(StorageError::ReadBeforeSince(from_id));
        }

        let new_export = ExportState {
            description: new_description.clone(),
            read_hold,
            read_policy: cur_export.read_policy.clone(),
            write_frontier: cur_export.write_frontier.clone(),
            wallclock_lag_max: Default::default(),
        };
        *cur_export = new_export;

        let status_id = match new_description.sink.status_id.clone() {
            Some(id) => Some(self.storage_collections.collection_metadata(id)?.data_shard),
            None => None,
        };

        let cmd = RunSinkCommand {
            id,
            description: StorageSinkDesc {
                from: from_id,
                from_desc: new_description.sink.from_desc,
                connection: new_description.sink.connection,
                envelope: new_description.sink.envelope,
                as_of: new_description.sink.as_of,
                version: new_description.sink.version,
                partition_strategy: new_description.sink.partition_strategy,
                status_id,
                from_storage_metadata,
                with_snapshot: new_description.sink.with_snapshot,
            },
        };

        // Fetch the client for this exports's cluster.
        let instance = self
            .instances
            .get_mut(&new_description.instance_id)
            .ok_or_else(|| StorageError::ExportInstanceMissing {
                storage_instance_id: new_description.instance_id,
                export_id: id,
            })?;

        instance.send(StorageCommand::RunSinks(vec![cmd]));
        Ok(())
    }

    /// Create the sinks described by the `ExportDescription`.
    async fn alter_export_connections(
        &mut self,
        exports: BTreeMap<GlobalId, StorageSinkConnection>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let mut updates_by_instance =
            BTreeMap::<StorageInstanceId, Vec<(RunSinkCommand<T>, ExportDescription<T>)>>::new();

        for (id, connection) in exports {
            // We stage changes in new_export_description and then apply all
            // updates to exports at the end.
            //
            // We don't just go ahead and clone the `ExportState` itself and
            // update that because `ExportState` is not clone, because it holds
            // a `ReadHandle` and cloning that would cause additional work for
            // whoever guarantees those read holds.
            let (mut new_export_description, as_of) = {
                let export = self.export(id).expect("export exists");
                let export_description = export.description.clone();
                let as_of = export.read_hold.since().clone();

                (export_description, as_of)
            };
            let current_sink = new_export_description.sink.clone();

            new_export_description.sink.connection = connection;

            // Ensure compatibility
            current_sink.alter_compatible(id, &new_export_description.sink)?;

            let from_storage_metadata = self
                .storage_collections
                .collection_metadata(new_export_description.sink.from)?;

            let status_id =
                if let Some(status_collection_id) = new_export_description.sink.status_id {
                    Some(
                        self.storage_collections
                            .collection_metadata(status_collection_id)?
                            .data_shard,
                    )
                } else {
                    None
                };

            let cmd = RunSinkCommand {
                id,
                description: StorageSinkDesc {
                    from: new_export_description.sink.from,
                    from_desc: new_export_description.sink.from_desc.clone(),
                    connection: new_export_description.sink.connection.clone(),
                    envelope: new_export_description.sink.envelope,
                    with_snapshot: new_export_description.sink.with_snapshot,
                    partition_strategy: new_export_description.sink.partition_strategy.clone(),
                    version: new_export_description.sink.version,
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
                    as_of: as_of.to_owned(),
                    status_id,
                    from_storage_metadata,
                },
            };

            let update = updates_by_instance
                .entry(new_export_description.instance_id)
                .or_default();
            update.push((cmd, new_export_description));
        }

        for (instance_id, updates) in updates_by_instance {
            let mut export_updates = BTreeMap::new();
            let mut cmds = Vec::with_capacity(updates.len());

            for (cmd, export_state) in updates {
                export_updates.insert(cmd.id, export_state);
                cmds.push(cmd);
            }

            // Fetch the client for this exports's cluster.
            let instance = self.instances.get_mut(&instance_id).ok_or_else(|| {
                StorageError::ExportInstanceMissing {
                    storage_instance_id: instance_id,
                    export_id: *export_updates
                        .keys()
                        .next()
                        .expect("set of exports not empty"),
                }
            })?;

            instance.send(StorageCommand::RunSinks(cmds));

            // Update state only after all possible errors have occurred.
            for (id, new_export_description) in export_updates {
                let export = self.export_mut(id).expect("export known to exist");
                export.description = new_export_description;
            }
        }

        Ok(())
    }

    // Dropping a table takes roughly the following flow:
    //
    // First determine if this is a TableWrites table or a source-fed table (an IngestionExport):
    //
    // If this is a TableWrites table:
    //   1. We remove the table from the persist table write worker.
    //   2. The table removal is awaited in an async task.
    //   3. A message is sent to the storage controller that the table has been removed from the
    //      table write worker.
    //   4. The controller drains all table drop messages during `process`.
    //   5. `process` calls `drop_sources` with the dropped tables.
    //
    // If this is an IngestionExport table:
    //   1. We validate the ids and then call drop_sources_unvalidated to proceed dropping.
    fn drop_tables(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
        ts: Self::Timestamp,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        // Collect tables by their data_source
        let (table_write_ids, data_source_ids): (Vec<_>, Vec<_>) = identifiers
            .into_iter()
            .partition(|id| match self.collections[id].data_source {
                DataSource::Other(DataSourceOther::TableWrites) => true,
                DataSource::IngestionExport { .. } => false,
                _ => panic!("identifier is not a table: {}", id),
            });

        // Drop table write tables
        if table_write_ids.len() > 0 {
            let drop_notif = self
                .persist_table_worker
                .drop_handles(table_write_ids.clone(), ts);
            let tx = self.pending_table_handle_drops_tx.clone();
            mz_ore::task::spawn(|| "table-cleanup".to_string(), async move {
                drop_notif.await;
                for identifier in table_write_ids {
                    let _ = tx.send(identifier);
                }
            });
        }

        // Drop source-fed tables
        if data_source_ids.len() > 0 {
            self.validate_collection_ids(data_source_ids.iter().cloned())?;
            self.drop_sources_unvalidated(storage_metadata, data_source_ids)?;
        }

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
        let mut ingestions_to_drop = BTreeSet::new();
        for id in ids.iter() {
            let metadata = storage_metadata.get_collection_shard::<T>(*id);
            mz_ore::soft_assert_or_log!(
                matches!(metadata, Err(StorageError::IdentifierMissing(_))),
                "dropping {id}, but drop was not synchronized with storage \
                controller via `synchronize_collections`"
            );

            let collection_state = self.collections.get(id);

            if let Some(collection_state) = collection_state {
                match collection_state.data_source {
                    // Webhooks and tables are dropped differently from
                    // ingestions and other collections.
                    //
                    // We can immediately compact them, because they don't
                    // interact with clusterd.
                    DataSource::Webhook | DataSource::Other(DataSourceOther::TableWrites) => {
                        let pending_compaction_command = PendingCompactionCommand {
                            id: *id,
                            read_frontier: Antichain::new(),
                            cluster_id: None,
                        };

                        tracing::debug!(?pending_compaction_command, "pushing pending compaction");

                        self.pending_compaction_commands
                            .push(pending_compaction_command);
                    }
                    DataSource::Ingestion(_) => {
                        ingestions_to_drop.insert(id);
                    }
                    DataSource::IngestionExport { ingestion_id, .. } => {
                        // If we are dropping source exports, we need to modify the
                        // ingestion that it runs on.
                        //
                        // If we remove this export, we need to stop producing data to
                        // it, so plan to re-execute the ingestion with the amended
                        // description.
                        ingestions_to_execute.insert(ingestion_id);

                        // Adjust the source to remove this export.
                        let ingestion_state = match self.collections.get_mut(&ingestion_id) {
                            Some(ingestion_collection) => ingestion_collection,
                            // Primary ingestion already dropped.
                            None => {
                                tracing::error!("primary source {ingestion_id} seemingly dropped before subsource {id}");
                                continue;
                            }
                        };

                        match &mut ingestion_state.data_source {
                            DataSource::Ingestion(ingestion_desc) => {
                                let removed = ingestion_desc.source_exports.remove(id);
                                mz_ore::soft_assert_or_log!(
                                    removed.is_some(),
                                    "dropped subsource {id} already removed from source exports"
                                );
                            }
                            _ => unreachable!("SourceExport must only refer to primary sources that already exist"),
                        };

                        // Ingestion exports also have ReadHolds that we need to
                        // downgrade, and much of their drop machinery is the
                        // same as for the "main" ingestion.
                        ingestions_to_drop.insert(id);
                    }
                    DataSource::Other(_) | DataSource::Introspection(_) | DataSource::Progress => {
                        ()
                    }
                }
            }
        }

        // Do not bother re-executing ingestions we know we plan to drop.
        ingestions_to_execute.retain(|id| !ingestions_to_drop.contains(id));
        for ingestion_id in ingestions_to_execute {
            self.run_ingestion(ingestion_id)?;
        }

        // For ingestions, we fabricate a new hold that will propagate through
        // the cluster and then back to us.

        // We don't explicitly remove read capabilities! Downgrading the
        // frontier of the source to `[]` (the empty Antichain), will propagate
        // to the storage dependencies.
        let ingestion_policies = ingestions_to_drop
            .iter()
            .map(|id| (**id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();

        tracing::debug!(
            ?ingestion_policies,
            "dropping sources by setting read hold policies"
        );
        self.set_hold_policies(ingestion_policies);

        // Also let StorageCollections know!
        self.storage_collections
            .drop_collections_unvalidated(storage_metadata, ids);

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
        if self.read_only {
            // While in read only mode, ONLY collections that have been migrated
            // and need to be re-hydrated in read only mode can be written to.
            if !commands
                .iter()
                .all(|(id, _)| id.is_system() && self.migrated_storage_collections.contains(id))
            {
                return Err(StorageError::ReadOnly);
            }
        }

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
    fn snapshot(
        &self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> BoxFuture<Result<Vec<(Row, Diff)>, StorageError<Self::Timestamp>>> {
        snapshot(
            id,
            as_of,
            &self.storage_collections,
            &self.txns_read,
            &self.persist,
        )
    }

    async fn snapshot_latest(
        &self,
        id: GlobalId,
    ) -> Result<Vec<Row>, StorageError<Self::Timestamp>> {
        let upper = self.recent_upper(id).await?;
        let res = match upper.as_option() {
            Some(f) if f > &T::minimum() => {
                let as_of = f.step_back().unwrap();

                let snapshot = self.snapshot(id, as_of).await.unwrap();
                snapshot
                    .into_iter()
                    .map(|(row, diff)| {
                        assert!(diff == 1, "snapshot doesn't accumulate to set");
                        row
                    })
                    .collect()
            }
            Some(_min) => {
                // The collection must be empty!
                Vec::new()
            }
            // The collection is closed, we cannot determine a latest read
            // timestamp based on the upper.
            _ => {
                return Err(StorageError::InvalidUsage(
                    "collection closed, cannot determine a read timestamp based on the upper"
                        .to_string(),
                ));
            }
        };

        Ok(res)
    }

    async fn snapshot_cursor(
        &mut self,
        id: GlobalId,
        as_of: Self::Timestamp,
    ) -> Result<SnapshotCursor<Self::Timestamp>, StorageError<Self::Timestamp>>
    where
        Self::Timestamp: Timestamp + Lattice + Codec64,
    {
        let metadata = &self.storage_collections.collection_metadata(id)?;

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
                assert_eq!(txns_id, self.txns_read.txns_id());
                self.txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = self
                    .txns_read
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
        self.storage_collections.snapshot_stats(id, as_of).await
    }

    async fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        as_of: Antichain<Self::Timestamp>,
    ) -> BoxFuture<Result<SnapshotPartsStats, StorageError<Self::Timestamp>>> {
        self.storage_collections
            .snapshot_parts_stats(id, as_of)
            .await
    }

    #[instrument(level = "debug")]
    fn set_read_policy(&mut self, policies: Vec<(GlobalId, ReadPolicy<Self::Timestamp>)>) {
        self.storage_collections.set_read_policies(policies);
    }

    fn acquire_read_holds(
        &mut self,
        desired_holds: Vec<GlobalId>,
    ) -> Result<Vec<ReadHold<Self::Timestamp>>, ReadHoldError> {
        self.storage_collections.acquire_read_holds(desired_holds)
    }

    async fn ready(&mut self) {
        if self.pending_compaction_commands.len() > 0 {
            return;
        }
        if self.maintenance_scheduled {
            return;
        }

        if let Ok(dropped_id) = self.pending_table_handle_drops_rx.try_recv() {
            // HACKY: We cannot check if the channel has data on the version of
            // tokio that we're using, so we do a try_recv and put it back.
            self.pending_table_handle_drops_tx
                .send(dropped_id)
                .expect("ourselves are not dropped");
            return;
        }

        self.stashed_response = tokio::select! {
            // Order matters here. We want to process internal commands
            // before processing external commands.
            biased;

            Some(m) = self.internal_response_queue.recv() => Some(m),
            Some(m) = self.instance_response_rx.recv() => Some(m),
            _ = self.maintenance_ticker.tick() => {
                self.maintenance_scheduled = true;
                None
            },
        };
    }

    #[instrument(level = "debug")]
    fn process(
        &mut self,
        storage_metadata: &StorageMetadata,
    ) -> Result<Option<Response<T>>, anyhow::Error> {
        // Perform periodic maintenance work.
        if self.maintenance_scheduled {
            self.maintain();
            self.maintenance_ticker.reset();
            self.maintenance_scheduled = false;
        }

        for instance in self.instances.values_mut() {
            instance.rehydrate_failed_replicas();
        }

        let mut updated_frontiers = None;
        match self.stashed_response.take() {
            None => (),
            Some(StorageResponse::FrontierUppers(updates)) => {
                self.update_write_frontiers(&updates);
                updated_frontiers = Some(Response::FrontierUpdates(updates));
            }
            Some(StorageResponse::DroppedIds(ids)) => {
                for id in ids.iter() {
                    tracing::debug!("DroppedIds for collections {id}");

                    if let Some(_collection) = self.collections.remove(id) {
                        // Nothing to do, we already dropped read holds in
                        // `drop_sources_unvalidated`.
                    } else if let Some(export) = self.exports.get_mut(id) {
                        // TODO: Current main never drops export state, so we
                        // also don't do that, because it would be yet more
                        // refactoring. Instead, we downgrade to the empty
                        // frontier, which satisfies StorageCollections just as
                        // much.
                        tracing::info!("downgrading read hold of export {id} to empty frontier!");
                        export
                            .read_hold
                            .try_downgrade(Antichain::new())
                            .expect("must be possible");
                    } else {
                        soft_panic_or_log!(
                            "DroppedIds for ID {id} but we have neither ingestion nor export \
                             under that ID"
                        );
                    }
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
                            .and_modify(|current| current.stat().incorporate(stat));
                    }
                }

                {
                    let mut shared_stats = self.sink_statistics.lock().expect("poisoned");
                    for stat in sink_stats {
                        // Don't override it if its been removed.
                        shared_stats
                            .entry(stat.id)
                            .and_modify(|current| current.stat().incorporate(stat));
                    }
                }
            }
            Some(StorageResponse::StatusUpdates(updates)) => {
                self.record_status_updates(updates);
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
            } = compaction_command;

            // TODO(petrosagg): make this a strict check
            // TODO(aljoscha): What's up with this TODO?
            // Note that while collections are dropped, the `client` may already
            // be cleared out, before we do this post-processing!
            let instance = cluster_id.and_then(|cluster_id| self.instances.get_mut(&cluster_id));

            let internal_response_sender = self.internal_response_sender.clone();
            let spawn_cleanup_task = |drop_fut| {
                mz_ore::task::spawn(|| format!("storage-table-cleanup-{id}"), async move {
                    // Wait for the relevant component to drop its resources and handles, this
                    // guarantees we won't see any more writes.
                    drop_fut.await;

                    // Notify that this ID has been dropped, which will start finalization of
                    // the shard.
                    let _ = internal_response_sender.send(StorageResponse::DroppedIds([id].into()));
                });
            };

            if read_frontier.is_empty() {
                if instance.is_some() && self.collections.contains_key(&id) {
                    let collection = self.collections.get(&id).expect("known to exist");
                    match collection.extra_state {
                        CollectionStateExtra::Ingestion(_) => {
                            pending_source_drops.push(id);
                        }
                        CollectionStateExtra::None => {
                            // Nothing to do
                        }
                    }
                } else if let Some(collection) = self.collections.get(&id) {
                    match collection.data_source {
                        DataSource::Other(DataSourceOther::TableWrites) => {
                            pending_collection_drops.push(id);

                            // Hacky, return an empty future so the IDs are finalized below.
                            let drop_fut = async move {}.boxed();
                            spawn_cleanup_task(drop_fut);
                        }
                        DataSource::Webhook => {
                            pending_collection_drops.push(id);

                            // TODO(parkmycar): The Collection Manager and PersistMonotonicWriter
                            // could probably use some love and maybe get merged together?
                            let unregister_notif =
                                self.collection_manager.unregister_collection(id);
                            let drop_fut = async move {
                                // Wait for the collection manager to stop writing.
                                unregister_notif.await;
                            };
                            let drop_fut = drop_fut.boxed();

                            spawn_cleanup_task(drop_fut);
                        }
                        DataSource::Ingestion(_) => (),
                        DataSource::IngestionExport { .. } => (),
                        DataSource::Introspection(_) => (),
                        DataSource::Progress => (),
                        DataSource::Other(_) => (),
                    }
                } else if instance.is_some() && self.exports.contains_key(&id) {
                    pending_sink_drops.push(id);
                } else if instance.is_none() {
                    tracing::info!("Compaction command for id {id}, but we don't have a client.");
                } else {
                    soft_panic_or_log!("Reference to absent collection {id}");
                };
            }

            // Sources can have subsources, which don't have associated clusters, which
            // is why this operates differently than sinks.
            if read_frontier.is_empty() {
                source_statistics_to_drop.push(id);
            }

            // Note that while collections are dropped, the `client` may already
            // be cleared out, before we do this post-processing!
            if let Some(client) = instance {
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
        self.append_shard_mappings(shards_to_update.into_iter(), -1);

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

        if !self.read_only {
            self.collection_status_manager
                .append_updates(dropped_sources, IntrospectionType::SourceStatusHistory);
        }

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

        if !self.read_only {
            self.collection_status_manager
                .append_updates(dropped_sinks, IntrospectionType::SinkStatusHistory);
        }

        Ok(updated_frontiers)
    }

    async fn inspect_persist_state(
        &self,
        id: GlobalId,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let collection = &self.storage_collections.collection_metadata(id)?;
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

    fn append_introspection_updates(
        &mut self,
        type_: IntrospectionType,
        updates: Vec<(Row, Diff)>,
    ) {
        let id = self.introspection_ids.lock().expect("poisoned")[&type_];
        self.collection_manager.blind_write(id, updates);
    }

    fn update_introspection_collection(&mut self, type_: IntrospectionType, op: StorageWriteOp) {
        let id = self.introspection_ids.lock().expect("poisoned")[&type_];
        self.collection_manager.differential_write(id, op);
    }

    async fn initialize_state(
        &mut self,
        txn: &mut (dyn StorageTxn<T> + Send),
        init_ids: BTreeSet<GlobalId>,
        drop_ids: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<T>> {
        self.storage_collections
            .initialize_state(txn, init_ids, drop_ids)
            .await
    }

    async fn prepare_state(
        &self,
        txn: &mut (dyn StorageTxn<T> + Send),
        ids_to_add: BTreeSet<GlobalId>,
        ids_to_drop: BTreeSet<GlobalId>,
    ) -> Result<(), StorageError<T>> {
        self.storage_collections
            .prepare_state(txn, ids_to_add, ids_to_drop)
            .await
    }

    async fn real_time_recent_timestamp(
        &mut self,
        timestamp_objects: BTreeSet<GlobalId>,
        timeout: Duration,
    ) -> Result<
        BoxFuture<Result<Self::Timestamp, StorageError<Self::Timestamp>>>,
        StorageError<Self::Timestamp>,
    > {
        use mz_storage_types::sources::GenericSourceConnection;

        let mut rtr_futures = BTreeMap::new();

        // Only user sources can be read from w/ RTR.
        for id in timestamp_objects.into_iter().filter(GlobalId::is_user) {
            let collection = match self.collection(id) {
                Ok(c) => c,
                // Not a storage item, which we accept.
                Err(_) => continue,
            };

            let (source_conn, remap_id) = match &collection.data_source {
                DataSource::Ingestion(IngestionDescription {
                    desc: SourceDesc { connection, .. },
                    remap_collection_id,
                    ..
                }) => match connection {
                    GenericSourceConnection::Kafka(_)
                    | GenericSourceConnection::Postgres(_)
                    | GenericSourceConnection::MySql(_) => {
                        (connection.clone(), *remap_collection_id)
                    }

                    // These internal sources do not yet (and might never)
                    // support RTR. However, erroring if they're selected from
                    // poses an annoying user experience, so instead just skip
                    // over them.
                    GenericSourceConnection::LoadGenerator(_) => continue,
                },
                // Skip over all other objects
                _ => {
                    continue;
                }
            };

            // Prepare for getting the external system's frontier.
            let config = self.config().clone();

            // Determine the remap collection we plan to read from.
            //
            // Note that the process of reading from the remap shard is the same
            // as other areas in this code that do the same thing, but we inline
            // it here because we must prove that we have not taken ownership of
            // `self` to move the stream of data from the remap shard into a
            // future.
            let read_handle = self.read_handle_for_snapshot(remap_id).await?;

            // Have to acquire a read hold to prevent the since from advancing
            // while we read.
            let remap_read_hold = self
                .storage_collections
                .acquire_read_holds(vec![remap_id])
                .map_err(|_e| StorageError::ReadBeforeSince(remap_id))?
                .expect_element(|| "known to be exactly one");

            let remap_as_of = remap_read_hold
                .since()
                .to_owned()
                .into_option()
                .ok_or(StorageError::ReadBeforeSince(remap_id))?;

            rtr_futures.insert(
                id,
                tokio::time::timeout(timeout, async move {
                    use mz_storage_types::sources::SourceConnection as _;

                    // Fetch the remap shard's contents; we must do this first so
                    // that the `as_of` doesn't change.
                    let as_of = Antichain::from_elem(remap_as_of);
                    let remap_subscribe = read_handle
                        .subscribe(as_of.clone())
                        .await
                        .map_err(|_| StorageError::ReadBeforeSince(remap_id))?;

                    tracing::debug!(?id, type_ = source_conn.name(), upstream = ?source_conn.external_reference(), "fetching real time recency");

                    let result = rtr::real_time_recency_ts(source_conn, id, config, as_of, remap_subscribe)
                        .await.map_err(|e| {
                            tracing::debug!(?id, "real time recency error: {:?}", e);
                            e
                        });

                    // Drop once we have read succesfully.
                    drop(remap_read_hold);

                    result
                }),
            );
        }

        Ok(Box::pin(async move {
            let (ids, futs): (Vec<_>, Vec<_>) = rtr_futures.into_iter().unzip();
            ids.into_iter()
                .zip_eq(futures::future::join_all(futs).await)
                .try_fold(T::minimum(), |curr, (id, per_source_res)| {
                    let new =
                        per_source_res.map_err(|_e: Elapsed| StorageError::RtrTimeout(id))??;
                    Ok::<_, StorageError<Self::Timestamp>>(std::cmp::max(curr, new))
                })
        }))
    }
}

/// Seed [`StorageTxn`] with any state required to instantiate a
/// [`StorageController`].
///
/// This cannot be a member of [`StorageController`] because it cannot take a
/// `self` parameter.
///
pub fn prepare_initialization<T>(txn: &mut dyn StorageTxn<T>) -> Result<(), StorageError<T>> {
    if txn.get_txn_wal_shard().is_none() {
        let txns_id = ShardId::new();
        txn.write_txn_wal_shard(txns_id)?;
    }

    Ok(())
}

impl<T> Controller<T>
where
    T: Timestamp
        + Lattice
        + TotalOrder
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Into<Datum<'static>>,
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
        wallclock_lag: WallclockLagFn<T>,
        txns_metrics: Arc<TxnMetrics>,
        envd_epoch: NonZeroI64,
        read_only: bool,
        metrics_registry: MetricsRegistry,
        connection_context: ConnectionContext,
        txn: &dyn StorageTxn<T>,
        storage_collections: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // This value must be already installed because we must ensure it's
        // durably recorded before it is used, otherwise we risk leaking persist
        // state.
        let txns_id = txn
            .get_txn_wal_shard()
            .expect("must call prepare initialization before creating storage controller");

        let txns_client = persist_clients
            .open(persist_location.clone())
            .await
            .expect("location should be valid");

        let persist_table_worker = if read_only {
            let txns_write = txns_client
                .open_writer(
                    txns_id,
                    Arc::new(TxnsCodecRow::desc()),
                    Arc::new(UnitSchema),
                    Diagnostics {
                        shard_name: "txns".to_owned(),
                        handle_purpose: "follow txns upper".to_owned(),
                    },
                )
                .await
                .expect("txns schema shouldn't change");
            persist_handles::PersistTableWriteWorker::new_read_only_mode(txns_write)
        } else {
            let txns = TxnsHandle::open(
                T::minimum(),
                txns_client.clone(),
                txns_client.dyncfgs().clone(),
                Arc::clone(&txns_metrics),
                txns_id,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
            )
            .await;
            persist_handles::PersistTableWriteWorker::new_txns(txns)
        };
        let txns_read = TxnsRead::start::<TxnsCodecRow>(txns_client.clone(), txns_id).await;

        let collection_manager = collection_mgmt::CollectionManager::new(read_only, now.clone());

        let introspection_ids = Arc::new(Mutex::new(BTreeMap::new()));
        let introspection_tokens = Arc::new(Mutex::new(BTreeMap::new()));

        let collection_status_manager = crate::collection_status::CollectionStatusManager::new(
            collection_manager.clone(),
            Arc::clone(&introspection_ids),
        );

        let (statistics_interval_sender, _) =
            channel(mz_storage_types::parameters::STATISTICS_INTERVAL_DEFAULT);

        let (pending_table_handle_drops_tx, pending_table_handle_drops_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let mut maintenance_ticker = tokio::time::interval(Duration::from_secs(1));
        maintenance_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let (instance_response_tx, instance_response_rx) = mpsc::unbounded_channel();

        Self {
            build_info,
            collections: BTreeMap::default(),
            exports: BTreeMap::default(),
            persist_table_worker,
            txns_read,
            txns_metrics,
            stashed_response: None,
            pending_compaction_commands: vec![],
            pending_table_handle_drops_tx,
            pending_table_handle_drops_rx,
            collection_manager,
            collection_status_manager,
            introspection_ids,
            introspection_tokens,
            now,
            envd_epoch,
            read_only,
            source_statistics: Arc::new(Mutex::new(statistics::SourceStatistics {
                source_statistics: BTreeMap::new(),
                webhook_statistics: BTreeMap::new(),
            })),
            sink_statistics: Arc::new(Mutex::new(BTreeMap::new())),
            statistics_interval_sender,
            instances: BTreeMap::new(),
            initialized: false,
            config: StorageConfiguration::new(connection_context, mz_dyncfgs::all_dyncfgs()),
            internal_response_sender: tx,
            internal_response_queue: rx,
            persist_location,
            persist: persist_clients,
            metrics: StorageControllerMetrics::new(metrics_registry),
            recorded_frontiers: BTreeMap::new(),
            recorded_replica_frontiers: BTreeMap::new(),
            wallclock_lag,
            wallclock_lag_last_refresh: Instant::now(),
            storage_collections,
            migrated_storage_collections: BTreeSet::new(),
            maintenance_ticker,
            maintenance_scheduled: false,
            instance_response_rx,
            instance_response_tx,
        }
    }

    // This is different from `set_read_policies`, which is for external users.
    // This method is for setting the policy that the controller uses when
    // maintaining the read holds that it has for collections/exports at the
    // StorageCollections.
    //
    // This is really only used when dropping things, where we set the
    // ReadPolicy to the empty Antichain.
    #[instrument(level = "debug")]
    fn set_hold_policies(&mut self, policies: Vec<(GlobalId, ReadPolicy<T>)>) {
        let mut read_capability_changes = BTreeMap::default();

        for (id, policy) in policies.into_iter() {
            if let Some(collection) = self.collections.get_mut(&id) {
                let ingestion = match &mut collection.extra_state {
                    CollectionStateExtra::Ingestion(ingestion) => ingestion,
                    CollectionStateExtra::None => {
                        unreachable!("set_hold_policies is only called for ingestions");
                    }
                };
                let mut new_derived_since = policy.frontier(ingestion.write_frontier.borrow());

                if PartialOrder::less_equal(&ingestion.derived_since, &new_derived_since) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_derived_since.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut ingestion.derived_since, &mut new_derived_since);
                    update.extend(new_derived_since.iter().map(|time| (time.clone(), -1)));

                    if !update.is_empty() {
                        read_capability_changes.insert(id, update);
                    }
                }

                ingestion.hold_policy = policy;
            } else if let Some(_export) = self.exports.get_mut(&id) {
                unreachable!("set_hold_policies is only called for ingestions");
            }
        }

        if !read_capability_changes.is_empty() {
            self.update_hold_capabilities(&mut read_capability_changes);
        }
    }

    #[instrument(level = "debug", fields(updates))]
    fn update_write_frontiers(&mut self, updates: &[(GlobalId, Antichain<T>)]) {
        let mut read_capability_changes = BTreeMap::default();

        for (id, new_upper) in updates.iter() {
            if let Some(collection) = self.collections.get_mut(id) {
                let ingestion = match &mut collection.extra_state {
                    CollectionStateExtra::Ingestion(ingestion) => ingestion,
                    CollectionStateExtra::None => {
                        if matches!(collection.data_source, DataSource::Progress) {
                            // We do get these, but can't do anything with it!
                        } else {
                            tracing::error!(
                                ?collection,
                                ?new_upper,
                                "updated write frontier for collection which is not an ingestion"
                            );
                        }
                        continue;
                    }
                };

                if PartialOrder::less_than(&ingestion.write_frontier, new_upper) {
                    ingestion.write_frontier.clone_from(new_upper);
                }

                debug!(%id, ?ingestion, ?new_upper, "upper update for ingestion!");

                let mut new_derived_since = ingestion
                    .hold_policy
                    .frontier(ingestion.write_frontier.borrow());

                if PartialOrder::less_equal(&ingestion.derived_since, &new_derived_since) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_derived_since.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut ingestion.derived_since, &mut new_derived_since);
                    update.extend(new_derived_since.iter().map(|time| (time.clone(), -1)));

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
                let new_read_capability = if export.write_frontier.is_empty() {
                    export.write_frontier.clone()
                } else {
                    export.read_policy.frontier(export.write_frontier.borrow())
                };

                if PartialOrder::less_equal(export.read_hold.since(), &new_read_capability) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                    update.extend(
                        export
                            .read_hold
                            .since()
                            .iter()
                            .map(|time| (time.clone(), -1)),
                    );

                    if !update.is_empty() {
                        read_capability_changes.insert(*id, update);
                    }
                }
            } else if self.storage_collections.check_exists(*id).is_ok() {
                // StorageCollections is handling it!
            } else {
                // TODO: This can happen because subsources report back an upper
                // but we don't store them in our `ingestions` field, _nor_ do
                // we acquire read holds for them. Also because we don't get
                // `DroppedIds` messages for them, so we wouldn't know when to
                // clean up read holds.
                info!(
                    "Reference to absent collection {id}, new_upper={:?}",
                    new_upper
                );
            }
        }

        if !read_capability_changes.is_empty() {
            self.update_hold_capabilities(&mut read_capability_changes);
        }
    }

    // This is different from `update_read_capabilities`, which is for external users.
    // This method is for maintaining the read holds that the controller has at
    // the StorageCollections, for storage dependencies.
    #[instrument(level = "debug", fields(updates))]
    fn update_hold_capabilities(&mut self, updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>) {
        // Location to record consequences that we need to act on.
        let mut collections_net = BTreeMap::new();
        let mut exports_net = BTreeMap::new();

        // We must not rely on any specific relative ordering of `GlobalId`s.
        // That said, it is reasonable to assume that collections generally have
        // greater IDs than their dependencies, so starting with the largest is
        // a useful optimization.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();

            if key.is_user() {
                debug!(id = %key, ?update, "update_hold_capability");
            }

            if let Some(collection) = self.collections.get_mut(&key) {
                let ingestion = match &mut collection.extra_state {
                    CollectionStateExtra::Ingestion(ingestion) => ingestion,
                    CollectionStateExtra::None => {
                        // WIP: See if this ever panics in ci.
                        soft_panic_or_log!(
                            "trying to update holds for collection {collection:?} which is not \
                             an ingestion: {update:?}"
                        );
                        continue;
                    }
                };

                let changes = ingestion.read_capabilities.update_iter(update.drain());
                update.extend(changes);

                let (changes, frontier, _cluster_id) =
                    collections_net.entry(key).or_insert_with(|| {
                        (
                            <ChangeBatch<_>>::new(),
                            Antichain::new(),
                            ingestion.instance_id,
                        )
                    });

                changes.extend(update.drain());
                *frontier = ingestion.read_capabilities.frontier().to_owned();
            } else if let Ok(export) = self.export_mut(key) {
                // Seed with our current read hold, then apply changes, to
                // derive how we need to change our read hold.
                let mut staged_read_hold = MutableAntichain::new();
                staged_read_hold
                    .update_iter(export.read_hold.since().iter().map(|t| (t.clone(), 1)));
                let changes = staged_read_hold.update_iter(update.drain());
                update.extend(changes);

                // Make sure we also send `AllowCompaction` commands for sinks,
                // which drives updating the sink's `as_of`, among other things.
                let (changes, frontier, _cluster_id) =
                    exports_net.entry(key).or_insert_with(|| {
                        (
                            <ChangeBatch<_>>::new(),
                            Antichain::new(),
                            export.cluster_id(),
                        )
                    });

                changes.extend(update.drain());
                *frontier = staged_read_hold.frontier().to_owned();
            } else {
                // This is confusing and we should probably error.
                tracing::warn!(id = ?key, ?update, "update_hold_capabilities for unknown object");
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

        for (key, (mut changes, frontier, cluster_id)) in collections_net {
            if !changes.is_empty() {
                if key.is_user() {
                    debug!(id = %key, ?frontier, "downgrading ingestion read holds!");
                }

                let collection = self
                    .collections
                    .get_mut(&key)
                    .expect("missing collection state");

                let ingestion = match &mut collection.extra_state {
                    CollectionStateExtra::Ingestion(ingestion) => ingestion,
                    CollectionStateExtra::None => {
                        soft_panic_or_log!(
                            "trying to downgrade read holds for collection which is not an \
                             ingestion: {collection:?}"
                        );
                        continue;
                    }
                };

                for read_hold in ingestion.dependency_read_holds.iter_mut() {
                    read_hold
                        .try_downgrade(frontier.clone())
                        .expect("we only advance the frontier");
                }

                worker_compaction_commands.insert(key, (frontier.clone(), cluster_id));
            }
        }
        for (key, (mut changes, frontier, cluster_id)) in exports_net {
            if !changes.is_empty() {
                let export_state = self.exports.get_mut(&key).expect("missing export state");

                export_state
                    .read_hold
                    .try_downgrade(frontier.clone())
                    .expect("we only advance the frontier");

                worker_compaction_commands.insert(key, (frontier, cluster_id));
            }
        }

        for (id, (read_frontier, cluster_id)) in worker_compaction_commands {
            // Acquiring a client for a storage instance requires await, so we
            // instead stash these for later and process when we can.
            self.pending_compaction_commands
                .push(PendingCompactionCommand {
                    id,
                    read_frontier,
                    cluster_id: Some(cluster_id),
                });
        }
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_collection_ids(
        &self,
        ids: impl Iterator<Item = GlobalId>,
    ) -> Result<(), StorageError<T>> {
        for id in ids {
            self.storage_collections.check_exists(id)?;
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

    async fn recent_upper(&self, id: GlobalId) -> Result<Antichain<T>, StorageError<T>> {
        let metadata = &self.storage_collections.collection_metadata(id)?;
        let persist_client = self
            .persist
            .open(metadata.persist_location.clone())
            .await
            .unwrap();
        // Duplicate part of open_data_handles here because we don't need the
        // fetch_recent_upper call. The pubsub-updated shared_upper is enough.
        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
        };
        // NB: Opening a WriteHandle is cheap if it's never used in a
        // compare_and_append operation.
        let write = persist_client
            .open_writer::<SourceData, (), T, Diff>(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                diagnostics.clone(),
            )
            .await
            .expect("invalid persist usage");
        Ok(write.shared_upper())
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
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> WriteHandle<SourceData, (), T, Diff> {
        let diagnostics = Diagnostics {
            shard_name: id.to_string(),
            handle_purpose: format!("controller data for {}", id),
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

        write
    }

    /// Registers the given introspection collection and does any preparatory
    /// work that we have to do before we start writing to it. This
    /// preparatory work will include partial truncation or other cleanup
    /// schemes, depending on introspection type.
    async fn register_introspection_collection(
        &mut self,
        id: GlobalId,
        introspection_type: IntrospectionType,
        mut write_handle: WriteHandle<SourceData, (), T, Diff>,
    ) -> Result<(), StorageError<T>> {
        tracing::info!(%id, ?introspection_type, "registering introspection collection");

        // In read-only mode we create a new shard for all migrated storage collections. So we
        // "trick" the write task into thinking that it's not in read-only mode so something is
        // advancing this new shard.
        let force_writable = self.read_only && self.migrated_storage_collections.contains(&id);
        if force_writable {
            assert!(id.is_system(), "unexpected non-system global id: {id:?}");
            info!("writing to migrated storage collection {id} in read-only mode");
        }

        let prev = self
            .introspection_ids
            .lock()
            .expect("poisoned lock")
            .insert(introspection_type, id);
        assert!(
            prev.is_none(),
            "cannot have multiple IDs for introspection type"
        );

        let metadata = self.storage_collections.collection_metadata(id)?.clone();
        let persist_client = self
            .persist
            .open(metadata.persist_location.clone())
            .await
            .unwrap();

        let read_handle_fn = move || {
            let persist_client = persist_client.clone();
            let metadata = metadata.clone();

            let fut = async move {
                let read_handle = persist_client
                    .open_leased_reader::<SourceData, (), T, Diff>(
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
                read_handle
            };

            fut.boxed()
        };

        let recent_upper = write_handle.shared_upper();

        match CollectionManagerKind::from(&introspection_type) {
            // For these, we first register the collection and then prepare it,
            // because the code that prepares differential collection expects to
            // be able to update desired state via the collection manager
            // already.
            CollectionManagerKind::Differential => {
                // These do a shallow copy.
                let introspection_config = DifferentialIntrospectionConfig {
                    recent_upper,
                    introspection_type,
                    storage_collections: Arc::clone(&self.storage_collections),
                    txns_read: self.txns_read.clone(),
                    persist: Arc::clone(&self.persist),
                    collection_manager: self.collection_manager.clone(),
                    source_statistics: Arc::clone(&self.source_statistics),
                    sink_statistics: Arc::clone(&self.sink_statistics),
                    statistics_interval: self.config.parameters.statistics_interval.clone(),
                    statistics_interval_receiver: self.statistics_interval_sender.subscribe(),
                    metrics: self.metrics.clone(),
                    introspection_tokens: Arc::clone(&self.introspection_tokens),
                };
                self.collection_manager.register_differential_collection(
                    id,
                    write_handle,
                    read_handle_fn,
                    force_writable,
                    introspection_config,
                );
            }
            // For these, we first have to prepare and then register with
            // collection manager, because the preparation logic wants to read
            // the shard's contents and then do uncontested writes.
            //
            // TODO(aljoscha): We should make the truncation/cleanup work that
            // happens when we take over instead be a periodic thing, and make
            // it resilient to the upper moving concurrently.
            CollectionManagerKind::AppendOnly => {
                // TODO(jkosh44) Handle this inside of the append only task.
                if !self.read_only {
                    self.prepare_introspection_collection(
                        id,
                        introspection_type,
                        Some(&mut write_handle),
                    )
                    .await?;
                }

                let introspection_config = AppendOnlyIntrospectionConfig {
                    introspection_type,
                    config_set: Arc::clone(self.config.config_set()),
                    parameters: self.config.parameters.clone(),
                    storage_collections: Arc::clone(&self.storage_collections),
                    txns_read: self.txns_read.clone(),
                    persist: Arc::clone(&self.persist),
                };
                self.collection_manager.register_append_only_collection(
                    id,
                    write_handle,
                    force_writable,
                    Some(introspection_config),
                );
            }
        }

        Ok(())
    }

    /// Does any work that is required before this controller instance starts
    /// writing to the given introspection collection.
    ///
    /// This might include consolidation, deleting older entries or seeding
    /// in-memory state of, say, scrapers, with current collection contents.
    async fn prepare_introspection_collection(
        &mut self,
        id: GlobalId,
        introspection_type: IntrospectionType,
        write_handle: Option<&mut WriteHandle<SourceData, (), T, Diff>>,
    ) -> Result<(), StorageError<T>> {
        tracing::info!(%id, ?introspection_type, "preparing introspection collection for writes");

        match introspection_type {
            IntrospectionType::SourceStatusHistory => {
                let write_handle = write_handle.expect("filled in by caller");
                let last_status_per_id = partially_truncate_status_history(
                    id,
                    IntrospectionType::SourceStatusHistory,
                    write_handle,
                    source_status_history_desc(&self.config.parameters),
                    &self.storage_collections,
                    &self.txns_read,
                    &self.persist,
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
                let write_handle = write_handle.expect("filled in by caller");
                let last_status_per_id = partially_truncate_status_history(
                    id,
                    IntrospectionType::SinkStatusHistory,
                    write_handle,
                    sink_status_history_desc(&self.config.parameters),
                    &self.storage_collections,
                    &self.txns_read,
                    &self.persist,
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

            IntrospectionType::ShardMapping
            | IntrospectionType::Frontiers
            | IntrospectionType::ReplicaFrontiers
            | IntrospectionType::StorageSourceStatistics
            | IntrospectionType::StorageSinkStatistics
            | IntrospectionType::ComputeDependencies
            | IntrospectionType::ComputeOperatorHydrationStatus
            | IntrospectionType::ComputeMaterializedViewRefreshes
            | IntrospectionType::ComputeErrorCounts
            | IntrospectionType::ComputeHydrationTimes => {
                // Handled by differential task.
            }

            IntrospectionType::ReplicaMetricsHistory
            | IntrospectionType::WallclockLagHistory
            | IntrospectionType::PrivatelinkConnectionStatusHistory
            | IntrospectionType::ReplicaStatusHistory
            | IntrospectionType::PreparedStatementHistory
            | IntrospectionType::StatementExecutionHistory
            | IntrospectionType::SessionHistory
            | IntrospectionType::StatementLifecycleHistory
            | IntrospectionType::SqlText => {
                // Handled by append only task.
            }
        }

        Ok(())
    }

    /// Remove statistics for sources/sinks that were dropped but still have statistics rows
    /// hanging around.
    fn reconcile_dangling_statistics(&mut self) {
        self.source_statistics
            .lock()
            .expect("poisoned")
            .source_statistics
            // collections should also contain subsources.
            .retain(|k, _| self.storage_collections.check_exists(*k).is_ok());
        self.sink_statistics
            .lock()
            .expect("poisoned")
            .retain(|k, _| self.exports.contains_key(k));
    }

    /// Appends a new global ID, shard ID pair to the appropriate collection.
    /// Use a `diff` of 1 to append a new entry; -1 to retract an existing
    /// entry.
    ///
    /// # Panics
    /// - If `self.collections` does not have an entry for `global_id`.
    /// - If `IntrospectionType::ShardMapping`'s `GlobalId` is not registered as
    ///   a managed collection.
    /// - If diff is any value other than `1` or `-1`.
    #[instrument(level = "debug")]
    fn append_shard_mappings<I>(&self, global_ids: I, diff: i64)
    where
        I: Iterator<Item = GlobalId>,
    {
        mz_ore::soft_assert_or_log!(diff == -1 || diff == 1, "use 1 for insert or -1 for delete");

        let id = *self
            .introspection_ids
            .lock()
            .expect("poisoned")
            .get(&IntrospectionType::ShardMapping)
            .expect("should be registered before this call");

        let mut updates = vec![];
        // Pack updates into rows
        let mut row_buf = Row::default();

        for global_id in global_ids {
            let shard_id = if let Some(collection) = self.collections.get(&global_id) {
                collection.collection_metadata.data_shard.clone()
            } else {
                panic!("unknown global id: {}", global_id);
            };

            let mut packer = row_buf.packer();
            packer.push(Datum::from(global_id.to_string().as_str()));
            packer.push(Datum::from(shard_id.to_string().as_str()));
            updates.push((row_buf.clone(), diff));
        }

        self.collection_manager.differential_append(id, updates);
    }

    /// Determines and returns this collection's dependencies, if any.
    fn determine_collection_dependencies(
        &self,
        self_id: GlobalId,
        data_source: &DataSource,
    ) -> Result<Vec<GlobalId>, StorageError<T>> {
        let dependency = match &data_source {
            DataSource::Introspection(_)
            | DataSource::Webhook
            | DataSource::Other(DataSourceOther::TableWrites)
            | DataSource::Progress
            | DataSource::Other(DataSourceOther::Compute) => vec![],
            DataSource::IngestionExport { ingestion_id, .. } => {
                // Ingestion exports depend on their primary source's remap
                // collection.
                let source_collection = self.collection(*ingestion_id)?;
                let ingestion_remap_collection_id = match &source_collection.data_source {
                    DataSource::Ingestion(ingestion) => ingestion.remap_collection_id,
                    _ => unreachable!(
                        "SourceExport must only refer to primary sources that already exist"
                    ),
                };

                // Ingestion exports (aka. subsources) must make sure that 1)
                // their own collection's since stays one step behind the upper,
                // and, 2) that the remap shard's since stays one step behind
                // their upper. Hence they track themselves and the remap shard
                // as dependencies.
                vec![self_id, ingestion_remap_collection_id]
            }
            // Ingestions depend on their remap collection.
            DataSource::Ingestion(ingestion) => {
                // Ingestions must make sure that 1) their own collection's
                // since stays one step behind the upper, and, 2) that the remap
                // shard's since stays one step behind their upper. Hence they
                // track themselves and the remap shard as dependencies.
                vec![self_id, ingestion.remap_collection_id]
            }
        };

        Ok(dependency)
    }

    async fn read_handle_for_snapshot(
        &self,
        id: GlobalId,
    ) -> Result<ReadHandle<SourceData, (), T, Diff>, StorageError<T>> {
        let metadata = self.storage_collections.collection_metadata(id)?;
        read_handle_for_snapshot(&self.persist, id, &metadata).await
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

        let metadata = &self.storage_collections.collection_metadata(id)?;

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
                assert_eq!(txns_id, self.txns_read.txns_id());
                self.txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = self
                    .txns_read
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
    fn record_status_updates(&mut self, updates: Vec<StatusUpdate>) {
        if self.read_only {
            return;
        }

        let mut sink_status_updates = vec![];
        let mut source_status_updates = vec![];

        for update in updates {
            let id = update.id;
            if self.exports.contains_key(&id) {
                sink_status_updates.push(update);
            } else if self.storage_collections.check_exists(id).is_ok() {
                source_status_updates.push(update);
            }
        }

        self.collection_status_manager.append_updates(
            source_status_updates,
            IntrospectionType::SourceStatusHistory,
        );
        self.collection_status_manager
            .append_updates(sink_status_updates, IntrospectionType::SinkStatusHistory);
    }

    fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, StorageError<T>> {
        self.collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
    }

    /// Runs the identified ingestion using the current definition of the
    /// ingestion in-memory.
    fn run_ingestion(&mut self, id: GlobalId) -> Result<(), StorageError<T>> {
        tracing::info!(%id, "starting ingestion");

        let collection = self.collection(id)?;
        let ingestion_description = match &collection.data_source {
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
                storage_metadata: (),
                details,
                data_config,
            },
        ) in ingestion_description.source_exports
        {
            let export_storage_metadata = self.collection(export_id)?.collection_metadata.clone();
            source_exports.insert(
                export_id,
                SourceExport {
                    storage_metadata: export_storage_metadata,
                    details,
                    data_config,
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
        let instance = self
            .instances
            .get_mut(&storage_instance_id)
            .ok_or_else(|| StorageError::IngestionInstanceMissing {
                storage_instance_id,
                ingestion_id: id,
            })?;

        let augmented_ingestion = RunIngestionCommand { id, description };
        instance.send(StorageCommand::RunIngestions(vec![augmented_ingestion]));

        Ok(())
    }

    /// Runs the identified export using the current definition of the export
    /// that we have in memory.
    fn run_export(&mut self, id: GlobalId) -> Result<(), StorageError<T>> {
        let export = self.export(id)?;
        let description = &export.description;

        info!(
            sink_id = %id,
            from_id = %description.sink.from,
            as_of = ?description.sink.as_of,
            "run_export"
        );

        let from_storage_metadata = self
            .storage_collections
            .collection_metadata(description.sink.from)?;

        let status_id = if let Some(status_collection_id) = description.sink.status_id {
            Some(
                self.storage_collections
                    .collection_metadata(status_collection_id)?
                    .data_shard,
            )
        } else {
            None
        };

        let cmd = RunSinkCommand {
            id,
            description: StorageSinkDesc {
                from: description.sink.from,
                from_desc: description.sink.from_desc.clone(),
                connection: description.sink.connection.clone(),
                envelope: description.sink.envelope,
                as_of: description.sink.as_of.clone(),
                version: description.sink.version,
                partition_strategy: description.sink.partition_strategy.clone(),
                status_id,
                from_storage_metadata,
                with_snapshot: description.sink.with_snapshot,
            },
        };

        let storage_instance_id = description.instance_id.clone();

        let instance = self
            .instances
            .get_mut(&storage_instance_id)
            .ok_or_else(|| StorageError::ExportInstanceMissing {
                storage_instance_id,
                export_id: id,
            })?;

        instance.send(StorageCommand::RunSinks(vec![cmd]));

        Ok(())
    }

    /// Update introspection with the current frontiers of storage objects.
    ///
    /// This method is invoked by `Controller::maintain`, which we expect to be called once per
    /// second during normal operation.
    fn update_frontier_introspection(&mut self) {
        let mut global_frontiers = BTreeMap::new();
        let mut replica_frontiers = BTreeMap::new();

        for collection_frontiers in self.storage_collections.active_collection_frontiers() {
            let id = collection_frontiers.id;
            let since = collection_frontiers.read_capabilities;
            let upper = collection_frontiers.write_frontier;

            let instance = self
                .collections
                .get(&id)
                .and_then(|c| match &c.extra_state {
                    CollectionStateExtra::Ingestion(ingestion) => Some(ingestion),
                    CollectionStateExtra::None => None,
                })
                .and_then(|i| self.instances.get(&i.instance_id));
            if let Some(instance) = instance {
                for replica_id in instance.replica_ids() {
                    replica_frontiers.insert((id, replica_id), upper.clone());
                }
            }

            global_frontiers.insert(id, (since, upper));
        }

        for (id, export) in self.active_exports() {
            // Exports cannot be read from, so their `since` is always the empty frontier.
            let since = Antichain::new();
            let upper = export.write_frontier.clone();

            let instance = self.instances.get(&export.cluster_id());
            if let Some(instance) = instance {
                for replica_id in instance.replica_ids() {
                    replica_frontiers.insert((id, replica_id), upper.clone());
                }
            }

            global_frontiers.insert(id, (since, upper));
        }

        let mut global_updates = Vec::new();
        let mut replica_updates = Vec::new();

        let mut push_global_update =
            |id: GlobalId, (since, upper): (Antichain<T>, Antichain<T>), diff: Diff| {
                let read_frontier = since.into_option().map_or(Datum::Null, |t| t.into());
                let write_frontier = upper.into_option().map_or(Datum::Null, |t| t.into());
                let row = Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    read_frontier,
                    write_frontier,
                ]);
                global_updates.push((row, diff));
            };

        let mut push_replica_update =
            |(id, replica_id): (GlobalId, ReplicaId), upper: Antichain<T>, diff: Diff| {
                let write_frontier = upper.into_option().map_or(Datum::Null, |t| t.into());
                let row = Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::String(&replica_id.to_string()),
                    write_frontier,
                ]);
                replica_updates.push((row, diff));
            };

        let mut old_global_frontiers =
            std::mem::replace(&mut self.recorded_frontiers, global_frontiers);
        for (&id, new) in &self.recorded_frontiers {
            match old_global_frontiers.remove(&id) {
                Some(old) if &old != new => {
                    push_global_update(id, new.clone(), 1);
                    push_global_update(id, old, -1);
                }
                Some(_) => (),
                None => push_global_update(id, new.clone(), 1),
            }
        }
        for (id, old) in old_global_frontiers {
            push_global_update(id, old, -1);
        }

        let mut old_replica_frontiers =
            std::mem::replace(&mut self.recorded_replica_frontiers, replica_frontiers);
        for (&key, new) in &self.recorded_replica_frontiers {
            match old_replica_frontiers.remove(&key) {
                Some(old) if &old != new => {
                    push_replica_update(key, new.clone(), 1);
                    push_replica_update(key, old, -1);
                }
                Some(_) => (),
                None => push_replica_update(key, new.clone(), 1),
            }
        }
        for (key, old) in old_replica_frontiers {
            push_replica_update(key, old, -1);
        }

        let id = self.introspection_ids.lock().expect("poisoned")[&IntrospectionType::Frontiers];
        self.collection_manager
            .differential_append(id, global_updates);

        let id =
            self.introspection_ids.lock().expect("poisoned")[&IntrospectionType::ReplicaFrontiers];
        self.collection_manager
            .differential_append(id, replica_updates);
    }

    /// Update introspection with the current wallclock lag values.
    ///
    /// We measure the lag of write frontiers behind the wallclock time every second and track the
    /// maximum over 60 measurements (i.e., one minute). Every minute, we emit a new lag event to
    /// the `WallclockLagHistory` introspection with the current maximum.
    ///
    /// This method is invoked by `ComputeController::maintain`, which we expect to be called once
    /// per second during normal operation.
    fn update_wallclock_lag_introspection(&mut self) {
        let refresh_introspection = !self.read_only
            && self.wallclock_lag_last_refresh.elapsed()
                >= WALLCLOCK_LAG_REFRESH_INTERVAL.get(self.config.config_set());
        let mut introspection_updates = refresh_introspection.then(Vec::new);

        let now = mz_ore::now::to_datetime((self.now)());
        let now_tz = now.try_into().expect("must fit");

        let frontier_lag = |frontier: &Antichain<_>| match frontier.as_option() {
            Some(ts) => (self.wallclock_lag)(ts),
            None => Duration::ZERO,
        };
        let pack_row = |id: GlobalId, lag: Duration| {
            let lag_us = i64::try_from(lag.as_micros()).expect("must fit");
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::Null,
                Datum::Interval(Interval::new(0, 0, lag_us)),
                Datum::TimestampTz(now_tz),
            ])
        };

        for frontiers in self.storage_collections.active_collection_frontiers() {
            let id = frontiers.id;
            let Some(collection) = self.collections.get_mut(&id) else {
                continue;
            };
            let lag = frontier_lag(&frontiers.write_frontier);
            collection.wallclock_lag_max = std::cmp::max(collection.wallclock_lag_max, lag);

            if let Some(updates) = &mut introspection_updates {
                let lag = std::mem::take(&mut collection.wallclock_lag_max);
                let row = pack_row(id, lag);
                updates.push((row, 1));
            }
        }

        let active_exports = self.exports.iter_mut().filter(|(_id, e)| !e.is_dropped());
        for (id, export) in active_exports {
            let lag = frontier_lag(&export.write_frontier);
            export.wallclock_lag_max = std::cmp::max(export.wallclock_lag_max, lag);

            if let Some(updates) = &mut introspection_updates {
                let lag = std::mem::take(&mut export.wallclock_lag_max);
                let row = pack_row(*id, lag);
                updates.push((row, 1));
            }
        }

        if let Some(updates) = introspection_updates {
            self.append_introspection_updates(IntrospectionType::WallclockLagHistory, updates);
            self.wallclock_lag_last_refresh = Instant::now();
        }
    }

    /// Run periodic tasks.
    ///
    /// This method is invoked roughly once per second during normal operation. It is a good place
    /// for tasks that need to run periodically, such as state cleanup or updating of metrics.
    fn maintain(&mut self) {
        self.update_frontier_introspection();
        self.update_wallclock_lag_introspection();
    }
}

impl From<&IntrospectionType> for CollectionManagerKind {
    fn from(value: &IntrospectionType) -> Self {
        match value {
            IntrospectionType::ShardMapping
            | IntrospectionType::Frontiers
            | IntrospectionType::ReplicaFrontiers
            | IntrospectionType::StorageSourceStatistics
            | IntrospectionType::StorageSinkStatistics
            | IntrospectionType::ComputeDependencies
            | IntrospectionType::ComputeOperatorHydrationStatus
            | IntrospectionType::ComputeMaterializedViewRefreshes
            | IntrospectionType::ComputeErrorCounts
            | IntrospectionType::ComputeHydrationTimes => CollectionManagerKind::Differential,

            IntrospectionType::SourceStatusHistory
            | IntrospectionType::SinkStatusHistory
            | IntrospectionType::PrivatelinkConnectionStatusHistory
            | IntrospectionType::ReplicaStatusHistory
            | IntrospectionType::ReplicaMetricsHistory
            | IntrospectionType::WallclockLagHistory
            | IntrospectionType::PreparedStatementHistory
            | IntrospectionType::StatementExecutionHistory
            | IntrospectionType::SessionHistory
            | IntrospectionType::StatementLifecycleHistory
            | IntrospectionType::SqlText => CollectionManagerKind::AppendOnly,
        }
    }
}

/// Get the current rows in the given statistics table. This is used to bootstrap
/// the statistics tasks.
///
// TODO(guswynn): we need to be more careful about the update time we get here:
// <https://github.com/MaterializeInc/database-issues/issues/7564>
async fn snapshot_statistics<T>(
    id: GlobalId,
    upper: Antichain<T>,
    storage_collections: &Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
    txns_read: &TxnsRead<T>,
    persist: &Arc<PersistClientCache>,
) -> Vec<Row>
where
    T: Codec64 + From<EpochMillis> + TimestampManipulation,
{
    match upper.as_option() {
        Some(f) if f > &T::minimum() => {
            let as_of = f.step_back().unwrap();

            let snapshot = snapshot(id, as_of, storage_collections, txns_read, persist)
                .await
                .unwrap();
            snapshot
                .into_iter()
                .map(|(row, diff)| {
                    assert_eq!(diff, 1);
                    row
                })
                .collect()
        }
        // If collection is closed or the frontier is the minimum, we cannot
        // or don't need to truncate (respectively).
        _ => Vec::new(),
    }
}

// TODO(petrosagg): This signature is not very useful in the context of partially ordered times
// where the as_of frontier might have multiple elements. In the current form the mutually
// incomparable updates will be accumulated together to a state of the collection that never
// actually existed. We should include the original time in the updates advanced by the as_of
// frontier in the result and let the caller decide what to do with the information.
pub(crate) fn snapshot<T>(
    id: GlobalId,
    as_of: T,
    storage_collections: &Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
    txns_read: &TxnsRead<T>,
    persist: &Arc<PersistClientCache>,
) -> BoxFuture<Result<Vec<(Row, Diff)>, StorageError<T>>>
where
    T: Codec64 + From<EpochMillis> + TimestampManipulation,
{
    let metadata = match storage_collections.collection_metadata(id) {
        Ok(metadata) => metadata,
        Err(e) => return async { Err(e) }.boxed(),
    };
    let txns_read = metadata.txns_shard.as_ref().map(|txns_id| {
        assert_eq!(txns_id, txns_read.txns_id());
        txns_read.clone()
    });
    let persist = Arc::clone(persist);
    async move {
        let mut read_handle = read_handle_for_snapshot(&persist, id, &metadata).await?;
        let contents = match txns_read {
            None => {
                // We're not using txn-wal for tables, so we can take a snapshot directly.
                read_handle
                    .snapshot_and_fetch(Antichain::from_elem(as_of))
                    .await
            }
            Some(txns_read) => {
                // We _are_ using txn-wal for tables. It advances the physical upper of the
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
                txns_read.update_gt(as_of.clone()).await;
                let data_snapshot = txns_read
                    .data_snapshot(metadata.data_shard, as_of.clone())
                    .await;
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
    .boxed()
}

async fn read_handle_for_snapshot<T>(
    persist: &PersistClientCache,
    id: GlobalId,
    metadata: &CollectionMetadata,
) -> Result<ReadHandle<SourceData, (), T, Diff>, StorageError<T>>
where
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    let persist_client = persist
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

/// State maintained about individual collections.
#[derive(Debug)]
struct CollectionState<T: TimelyTimestamp> {
    /// The source of this collection's data.
    pub data_source: DataSource,

    pub collection_metadata: CollectionMetadata,

    pub extra_state: CollectionStateExtra<T>,

    /// Maximum frontier wallclock lag since the last introspection update.
    wallclock_lag_max: Duration,
}

/// Additional state that the controller maintains for select collection types.
#[derive(Debug)]
enum CollectionStateExtra<T: TimelyTimestamp> {
    Ingestion(IngestionState<T>),
    None,
}

/// State maintained about ingestions and ingestion exports
#[derive(Debug)]
struct IngestionState<T: TimelyTimestamp> {
    /// Really only for keeping track of changes to the `derived_since`.
    pub read_capabilities: MutableAntichain<T>,

    /// The current since frontier, derived from `write_frontier` using
    /// `hold_policy`.
    pub derived_since: Antichain<T>,

    /// Holds that this ingestion (or ingestion export) has on its dependencies.
    pub dependency_read_holds: Vec<ReadHold<T>>,

    /// Reported write frontier.
    pub write_frontier: Antichain<T>,

    /// The policy that drives how we downgrade our read hold. That is how we
    /// derive our since from our upper.
    ///
    /// This is a _storage-controller-internal_ policy used to derive its
    /// personal read hold on the collection. It should not be confused with any
    /// read policies that the adapter might install at [StorageCollections].
    pub hold_policy: ReadPolicy<T>,

    /// The ID of the instance in which the ingestion is running.
    pub instance_id: StorageInstanceId,
}

/// A description of a status history collection.
///
/// Used to inform partial truncation, see [`partially_truncate_status_history`].
struct StatusHistoryDesc<K> {
    keep_n: usize,
    extract_key: Box<dyn Fn(&[Datum]) -> K + Send>,
    extract_time: Box<dyn Fn(&[Datum]) -> CheckedTimestamp<DateTime<Utc>> + Send>,
}

fn source_status_history_desc(params: &StorageParameters) -> StatusHistoryDesc<GlobalId> {
    let desc = &collection_status::MZ_SOURCE_STATUS_HISTORY_DESC;
    let (key_idx, _) = desc.get_by_name(&"source_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    StatusHistoryDesc {
        keep_n: params.keep_n_source_status_history_entries,
        extract_key: Box::new(move |datums| {
            GlobalId::from_str(datums[key_idx].unwrap_str()).expect("GlobalId column")
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

fn sink_status_history_desc(params: &StorageParameters) -> StatusHistoryDesc<GlobalId> {
    let desc = &collection_status::MZ_SINK_STATUS_HISTORY_DESC;
    let (key_idx, _) = desc.get_by_name(&"sink_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    StatusHistoryDesc {
        keep_n: params.keep_n_sink_status_history_entries,
        extract_key: Box::new(move |datums| {
            GlobalId::from_str(datums[key_idx].unwrap_str()).expect("GlobalId column")
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

fn privatelink_status_history_desc(params: &StorageParameters) -> StatusHistoryDesc<GlobalId> {
    let desc = &collection_status::MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC;
    let (key_idx, _) = desc.get_by_name(&"connection_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    StatusHistoryDesc {
        keep_n: params.keep_n_privatelink_status_history_entries,
        extract_key: Box::new(move |datums| {
            GlobalId::from_str(datums[key_idx].unwrap_str()).expect("GlobalId column")
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

fn replica_status_history_desc(params: &StorageParameters) -> StatusHistoryDesc<(GlobalId, u64)> {
    let desc = &collection_status::REPLICA_STATUS_HISTORY_DESC;
    let (replica_idx, _) = desc.get_by_name(&"replica_id".into()).expect("exists");
    let (process_idx, _) = desc.get_by_name(&"process_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    StatusHistoryDesc {
        keep_n: params.keep_n_privatelink_status_history_entries,
        extract_key: Box::new(move |datums| {
            (
                GlobalId::from_str(datums[replica_idx].unwrap_str()).expect("GlobalId column"),
                datums[process_idx].unwrap_uint64(),
            )
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}
