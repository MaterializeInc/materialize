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
use std::collections::btree_map;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::FutureExt;
use futures::StreamExt;
use itertools::Itertools;
use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_client::metrics::{ControllerMetrics, WallclockLagMetrics};
use mz_cluster_client::{ReplicaId, WallclockLagFn};
use mz_controller_types::dyncfgs::{
    ENABLE_0DT_DEPLOYMENT_SOURCES, WALLCLOCK_LAG_RECORDING_INTERVAL,
};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::AbortOnDropHandle;
use mz_ore::{assert_none, halt, instrument, soft_panic_or_log};
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::schema::CaESchema;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::Codec64;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, RelationVersion, Row, TimestampManipulation};
use mz_storage_client::client::{
    AppendOnlyUpdate, RunIngestionCommand, RunOneshotIngestion, RunSinkCommand, Status,
    StatusUpdate, StorageCommand, StorageResponse, TableData,
};
use mz_storage_client::controller::{
    BoxFuture, CollectionDescription, DataSource, ExportDescription, ExportState,
    IntrospectionType, MonotonicAppender, PersistEpoch, Response, StorageController,
    StorageMetadata, StorageTxn, StorageWriteOp, WallclockLag, WallclockLagHistogramPeriod,
};
use mz_storage_client::healthcheck::{
    MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC, MZ_SINK_STATUS_HISTORY_DESC,
    MZ_SOURCE_STATUS_HISTORY_DESC, REPLICA_STATUS_HISTORY_DESC,
};
use mz_storage_client::metrics::StorageControllerMetrics;
use mz_storage_client::statistics::{
    ControllerSinkStatistics, ControllerSourceStatistics, WebhookStatistics,
};
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::controller::{AlterError, CollectionMetadata, StorageError, TxnsCodecRow};
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::oneshot_sources::{OneshotIngestionRequest, OneshotResultCallback};
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sinks::{StorageSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, SourceConnection, SourceData, SourceDesc,
    SourceExport, SourceExportDataConfig,
};
use mz_storage_types::{AlterCompatible, StorageDiff, dyncfgs};
use mz_txn_wal::metrics::Metrics as TxnMetrics;
use mz_txn_wal::txn_read::TxnsRead;
use mz_txn_wal::txns::TxnsHandle;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::Timestamp as TimelyTimestamp;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::watch::{Sender, channel};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tokio::time::error::Elapsed;
use tracing::{debug, info, warn};

use crate::collection_mgmt::{
    AppendOnlyIntrospectionConfig, CollectionManagerKind, DifferentialIntrospectionConfig,
};
use crate::instance::{Instance, ReplicaConfig};

mod collection_mgmt;
mod history;
mod instance;
mod persist_handles;
mod rtr;
mod statistics;

#[derive(Derivative)]
#[derivative(Debug)]
struct PendingOneshotIngestion {
    /// Callback used to provide results of the ingestion.
    #[derivative(Debug = "ignore")]
    result_tx: OneshotResultCallback<ProtoBatch>,
    /// Cluster currently running this ingestion
    cluster_id: StorageInstanceId,
}

impl PendingOneshotIngestion {
    /// Consume the pending ingestion, responding with a cancelation message.
    ///
    /// TODO(cf2): Refine these error messages so they're not stringly typed.
    pub(crate) fn cancel(self) {
        (self.result_tx)(vec![Err("canceled".to_string())])
    }
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

    /// Map from IDs of objects that have been dropped to replicas we are still
    /// expecting DroppedId messages from. This is cleared out once all replicas
    /// have responded.
    ///
    /// We use this only to catch problems in the protocol between controller
    /// and replicas, for example we can differentiate between late messages for
    /// objects that have already been dropped and unexpected (read erroneous)
    /// messages from the replica.
    dropped_objects: BTreeMap<GlobalId, BTreeSet<ReplicaId>>,

    /// Write handle for table shards.
    pub(crate) persist_table_worker: persist_handles::PersistTableWriteWorker<T>,
    /// A shared TxnsCache running in a task and communicated with over a channel.
    txns_read: TxnsRead<T>,
    txns_metrics: Arc<TxnMetrics>,
    stashed_responses: Vec<(Option<ReplicaId>, StorageResponse<T>)>,
    /// Channel for sending table handle drops.
    #[derivative(Debug = "ignore")]
    pending_table_handle_drops_tx: mpsc::UnboundedSender<GlobalId>,
    /// Channel for receiving table handle drops.
    #[derivative(Debug = "ignore")]
    pending_table_handle_drops_rx: mpsc::UnboundedReceiver<GlobalId>,
    /// Closures that can be used to send responses from oneshot ingestions.
    #[derivative(Debug = "ignore")]
    pending_oneshot_ingestions: BTreeMap<uuid::Uuid, PendingOneshotIngestion>,

    /// Interface for managed collections
    pub(crate) collection_manager: collection_mgmt::CollectionManager<T>,

    /// Tracks which collection is responsible for which [`IntrospectionType`].
    pub(crate) introspection_ids: BTreeMap<IntrospectionType, GlobalId>,
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
    sink_statistics: Arc<Mutex<BTreeMap<(GlobalId, Option<ReplicaId>), ControllerSinkStatistics>>>,
    /// A way to update the statistics interval in the statistics tasks.
    statistics_interval_sender: Sender<Duration>,

    /// Clients for all known storage instances.
    instances: BTreeMap<StorageInstanceId, Instance<T>>,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// Storage configuration to apply to newly provisioned instances, and use during purification.
    config: StorageConfiguration,
    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,
    /// A persist client used to write to storage collections
    persist: Arc<PersistClientCache>,
    /// Metrics of the Storage controller
    metrics: StorageControllerMetrics,
    /// `(read, write)` frontiers that have been recorded in the `Frontiers` collection, kept to be
    /// able to retract old rows.
    recorded_frontiers: BTreeMap<GlobalId, (Antichain<T>, Antichain<T>)>,
    /// Write frontiers that have been recorded in the `ReplicaFrontiers` collection, kept to be
    /// able to retract old rows.
    recorded_replica_frontiers: BTreeMap<(GlobalId, ReplicaId), Antichain<T>>,

    /// A function that computes the lag between the given time and wallclock time.
    #[derivative(Debug = "ignore")]
    wallclock_lag: WallclockLagFn<T>,
    /// The last time wallclock lag introspection was recorded.
    wallclock_lag_last_recorded: DateTime<Utc>,

    /// Handle to a [StorageCollections].
    storage_collections: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
    /// Migrated storage collections that can be written even in read only mode.
    migrated_storage_collections: BTreeSet<GlobalId>,

    /// Ticker for scheduling periodic maintenance work.
    maintenance_ticker: tokio::time::Interval,
    /// Whether maintenance work was scheduled.
    maintenance_scheduled: bool,

    /// Shared transmit channel for replicas to send responses.
    instance_response_tx: mpsc::UnboundedSender<(Option<ReplicaId>, StorageResponse<T>)>,
    /// Receive end for replica responses.
    instance_response_rx: mpsc::UnboundedReceiver<(Option<ReplicaId>, StorageResponse<T>)>,

    /// Background task run at startup to warm persist state.
    persist_warm_task: Option<AbortOnDropHandle<Box<dyn Debug + Send>>>,
}

/// Warm up persist state for `shard_ids` in a background task.
///
/// With better parallelism during startup this would likely be unnecessary, but empirically we see
/// some nice speedups with this relatively simple function.
fn warm_persist_state_in_background(
    client: PersistClient,
    shard_ids: impl Iterator<Item = ShardId> + Send + 'static,
) -> mz_ore::task::JoinHandle<Box<dyn Debug + Send>> {
    /// Bound the number of shards that we warm at a single time, to limit our overall resource use.
    const MAX_CONCURRENT_WARMS: usize = 16;
    let logic = async move {
        let fetchers: Vec<_> = tokio_stream::iter(shard_ids)
            .map(|shard_id| {
                let client = client.clone();
                async move {
                    client
                        .create_batch_fetcher::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                            shard_id,
                            Arc::new(RelationDesc::empty()),
                            Arc::new(UnitSchema),
                            true,
                            Diagnostics::from_purpose("warm persist load state"),
                        )
                        .await
                }
            })
            .buffer_unordered(MAX_CONCURRENT_WARMS)
            .collect()
            .await;
        let fetchers: Box<dyn Debug + Send> = Box::new(fetchers);
        fetchers
    };
    mz_ore::task::spawn(|| "warm_persist_load_state", logic)
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
            let params = Box::new(config_params.clone());
            instance.send(StorageCommand::UpdateConfiguration(params));
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

    fn collection_metadata(&self, id: GlobalId) -> Result<CollectionMetadata, CollectionMissing> {
        self.storage_collections.collection_metadata(id)
    }

    fn collection_hydrated(
        &self,
        collection_id: GlobalId,
    ) -> Result<bool, StorageError<Self::Timestamp>> {
        let collection = self.collection(collection_id)?;

        let instance_id = match &collection.data_source {
            DataSource::Ingestion(ingestion_description) => ingestion_description.instance_id,
            DataSource::IngestionExport { ingestion_id, .. } => {
                let ingestion_state = self.collections.get(ingestion_id).expect("known to exist");

                let instance_id = match &ingestion_state.data_source {
                    DataSource::Ingestion(ingestion_desc) => ingestion_desc.instance_id,
                    _ => unreachable!("SourceExport must only refer to primary source"),
                };

                instance_id
            }
            _ => return Ok(true),
        };

        let instance = self.instances.get(&instance_id).ok_or_else(|| {
            StorageError::IngestionInstanceMissing {
                storage_instance_id: instance_id,
                ingestion_id: collection_id,
            }
        })?;

        if instance.replica_ids().next().is_none() {
            // Ingestions on zero-replica clusters are always considered
            // hydrated.
            return Ok(true);
        }

        match &collection.extra_state {
            CollectionStateExtra::Ingestion(ingestion_state) => {
                // An ingestion is hydrated if it is hydrated on at least one replica.
                Ok(ingestion_state.hydrated_on.len() >= 1)
            }
            CollectionStateExtra::Export(_) => {
                // For now, sinks are always considered hydrated. We rely on
                // them starting up "instantly" and don't wait for them when
                // checking hydration status of a replica.  TODO(sinks): base
                // this off of the sink shard's frontier?
                Ok(true)
            }
            CollectionStateExtra::None => {
                // For now, objects that are not ingestions are always
                // considered hydrated. This is tables and webhooks, as of
                // today.
                Ok(true)
            }
        }
    }

    #[mz_ore::instrument(level = "debug")]
    fn collections_hydrated_on_replicas(
        &self,
        target_replica_ids: Option<Vec<ReplicaId>>,
        target_cluster_id: &StorageInstanceId,
        exclude_collections: &BTreeSet<GlobalId>,
    ) -> Result<bool, StorageError<Self::Timestamp>> {
        // If an empty set of replicas is provided there can be
        // no collections on them, we'll count this as hydrated.
        if target_replica_ids.as_ref().is_some_and(|v| v.is_empty()) {
            return Ok(true);
        }

        // If target_replica_ids is provided, use it as the set of target
        // replicas. Otherwise check for hydration on any replica.
        let target_replicas: Option<BTreeSet<ReplicaId>> =
            target_replica_ids.map(|ids| ids.into_iter().collect());

        let mut all_hydrated = true;
        for (collection_id, collection_state) in self.collections.iter() {
            if collection_id.is_transient() || exclude_collections.contains(collection_id) {
                continue;
            }
            let hydrated = match &collection_state.extra_state {
                CollectionStateExtra::Ingestion(state) => {
                    if &state.instance_id != target_cluster_id {
                        continue;
                    }
                    match &target_replicas {
                        Some(target_replicas) => !state.hydrated_on.is_disjoint(target_replicas),
                        None => {
                            // Not target replicas, so check that it's hydrated
                            // on at least one replica.
                            state.hydrated_on.len() >= 1
                        }
                    }
                }
                CollectionStateExtra::Export(_) => {
                    // For now, sinks are always considered hydrated. We rely on
                    // them starting up "instantly" and don't wait for them when
                    // checking hydration status of a replica.  TODO(sinks):
                    // base this off of the sink shard's frontier?
                    true
                }
                CollectionStateExtra::None => {
                    // For now, objects that are not ingestions are always
                    // considered hydrated. This is tables and webhooks, as of
                    // today.
                    true
                }
            };
            if !hydrated {
                tracing::info!(%collection_id, "collection is not hydrated on any replica");
                all_hydrated = false;
                // We continue with our loop instead of breaking out early, so
                // that we log all non-hydrated replicas.
            }
        }
        Ok(all_hydrated)
    }

    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<(Antichain<Self::Timestamp>, Antichain<Self::Timestamp>), CollectionMissing> {
        let frontiers = self.storage_collections.collection_frontiers(id)?;
        Ok((frontiers.implied_capability, frontiers.write_frontier))
    }

    fn collections_frontiers(
        &self,
        mut ids: Vec<GlobalId>,
    ) -> Result<Vec<(GlobalId, Antichain<T>, Antichain<T>)>, CollectionMissing> {
        let mut result = vec![];
        // In theory, we could pull all our frontiers from storage collections...
        // but in practice those frontiers may not be identical. For historical reasons, we use the
        // locally-tracked frontier for sinks but the storage-collections-maintained frontier for
        // sources.
        ids.retain(|&id| match self.export(id) {
            Ok(export) => {
                result.push((
                    id,
                    export.input_hold().since().clone(),
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

    fn active_ingestion_exports(
        &self,
        instance_id: StorageInstanceId,
    ) -> Box<dyn Iterator<Item = &GlobalId> + '_> {
        let active_storage_collections: BTreeMap<_, _> = self
            .storage_collections
            .active_collection_frontiers()
            .into_iter()
            .map(|c| (c.id, c))
            .collect();

        let active_exports = self.instances[&instance_id]
            .active_ingestion_exports()
            .filter(move |id| {
                let frontiers = active_storage_collections.get(id);
                match frontiers {
                    Some(frontiers) => !frontiers.write_frontier.is_empty(),
                    None => {
                        // Not "active", so we don't care here.
                        false
                    }
                }
            });

        Box::new(active_exports)
    }

    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError<Self::Timestamp>> {
        self.storage_collections.check_exists(id)
    }

    fn create_instance(&mut self, id: StorageInstanceId, workload_class: Option<String>) {
        let metrics = self.metrics.for_instance(id);
        let mut instance = Instance::new(
            workload_class,
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

        let params = Box::new(self.config.parameters.clone());
        instance.send(StorageCommand::UpdateConfiguration(params));

        let old_instance = self.instances.insert(id, instance);
        assert_none!(old_instance, "storage instance {id} already exists");
    }

    fn drop_instance(&mut self, id: StorageInstanceId) {
        let instance = self.instances.remove(&id);
        assert!(instance.is_some(), "storage instance {id} does not exist");
    }

    fn update_instance_workload_class(
        &mut self,
        id: StorageInstanceId,
        workload_class: Option<String>,
    ) {
        let instance = self
            .instances
            .get_mut(&id)
            .unwrap_or_else(|| panic!("instance {id} does not exist"));

        instance.workload_class = workload_class;
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
        let instance = self
            .instances
            .get_mut(&instance_id)
            .unwrap_or_else(|| panic!("instance {instance_id} does not exist"));

        let status_now = mz_ore::now::to_datetime((self.now)());
        let mut source_status_updates = vec![];
        let mut sink_status_updates = vec![];

        let make_update = |id, object_type| StatusUpdate {
            id,
            status: Status::Paused,
            timestamp: status_now,
            error: None,
            hints: BTreeSet::from([format!(
                "The replica running this {object_type} has been dropped"
            )]),
            namespaced_errors: Default::default(),
            replica_id: Some(replica_id),
        };

        for ingestion_id in instance.active_ingestions() {
            if let Some(active_replicas) = self.dropped_objects.get_mut(ingestion_id) {
                active_replicas.remove(&replica_id);
                if active_replicas.is_empty() {
                    self.dropped_objects.remove(ingestion_id);
                }
            }

            let ingestion = self
                .collections
                .get_mut(ingestion_id)
                .expect("instance contains unknown ingestion");

            let ingestion_description = match &ingestion.data_source {
                DataSource::Ingestion(ingestion_description) => ingestion_description.clone(),
                _ => panic!(
                    "unexpected data source for ingestion: {:?}",
                    ingestion.data_source
                ),
            };

            let old_style_ingestion = *ingestion_id != ingestion_description.remap_collection_id;
            let subsource_ids = ingestion_description.collection_ids().filter(|id| {
                // NOTE(aljoscha): We filter out the remap collection for old style
                // ingestions because it doesn't get any status updates about it from the
                // replica side. So we don't want to synthesize a 'paused' status here.
                // New style ingestion do, since the source itself contains the remap data.
                let should_discard =
                    old_style_ingestion && id == &ingestion_description.remap_collection_id;
                !should_discard
            });
            for id in subsource_ids {
                source_status_updates.push(make_update(id, "source"));
            }
        }

        for id in instance.active_exports() {
            if let Some(active_replicas) = self.dropped_objects.get_mut(id) {
                active_replicas.remove(&replica_id);
                if active_replicas.is_empty() {
                    self.dropped_objects.remove(id);
                }
            }

            sink_status_updates.push(make_update(*id, "sink"));
        }

        instance.drop_replica(replica_id);

        if !self.read_only {
            if !source_status_updates.is_empty() {
                self.append_status_introspection_updates(
                    IntrospectionType::SourceStatusHistory,
                    source_status_updates,
                );
            }
            if !sink_status_updates.is_empty() {
                self.append_status_introspection_updates(
                    IntrospectionType::SinkStatusHistory,
                    sink_status_updates,
                );
            }
        }
    }

    async fn evolve_nullability_for_bootstrap(
        &mut self,
        storage_metadata: &StorageMetadata,
        collections: Vec<(GlobalId, RelationDesc)>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .unwrap();

        for (global_id, relation_desc) in collections {
            let shard_id = storage_metadata.get_collection_shard(global_id)?;
            let diagnostics = Diagnostics {
                shard_name: global_id.to_string(),
                handle_purpose: "evolve nullability for bootstrap".to_string(),
            };
            let latest_schema = persist_client
                .latest_schema::<SourceData, (), T, StorageDiff>(shard_id, diagnostics)
                .await
                .expect("invalid persist usage");
            let Some((schema_id, current_schema, _)) = latest_schema else {
                tracing::debug!(?global_id, "no schema registered");
                continue;
            };
            tracing::debug!(?global_id, ?current_schema, new_schema = ?relation_desc, "migrating schema");

            let diagnostics = Diagnostics {
                shard_name: global_id.to_string(),
                handle_purpose: "evolve nullability for bootstrap".to_string(),
            };
            let evolve_result = persist_client
                .compare_and_evolve_schema::<SourceData, (), T, StorageDiff>(
                    shard_id,
                    schema_id,
                    &relation_desc,
                    &UnitSchema,
                    diagnostics,
                )
                .await
                .expect("invalid persist usage");
            match evolve_result {
                CaESchema::Ok(_) => (),
                CaESchema::ExpectedMismatch {
                    schema_id,
                    key,
                    val: _,
                } => {
                    return Err(StorageError::PersistSchemaEvolveRace {
                        global_id,
                        shard_id,
                        schema_id,
                        relation_desc: key,
                    });
                }
                CaESchema::Incompatible => {
                    return Err(StorageError::PersistInvalidSchemaEvolve {
                        global_id,
                        shard_id,
                    });
                }
            };
        }

        Ok(())
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
            .extend(migrated_storage_collections.iter().cloned());

        self.storage_collections
            .create_collections_for_bootstrap(
                storage_metadata,
                register_ts.clone(),
                collections.clone(),
                migrated_storage_collections,
            )
            .await?;

        // At this point we're connected to all the collection shards in persist. Our warming task
        // is no longer useful, so abort it if it's still running.
        drop(self.persist_warm_task.take());

        // Validate first, to avoid corrupting state.
        // 1. create a dropped identifier, or
        // 2. create an existing identifier with a new description.
        // Make sure to check for errors within `ingestions` as well.
        collections.sort_by_key(|(id, _)| *id);
        collections.dedup();
        for pos in 1..collections.len() {
            if collections[pos - 1].0 == collections[pos].0 {
                return Err(StorageError::CollectionIdReused(collections[pos].0));
            }
        }

        // We first enrich each collection description with some additional metadata...
        let enriched_with_metadata = collections
            .into_iter()
            .map(|(id, description)| {
                let data_shard = storage_metadata.get_collection_shard::<T>(id)?;

                // If the shard is being managed by txn-wal (initially, tables), then we need to
                // pass along the shard id for the txns shard to dataflow rendering.
                let txns_shard = description
                    .data_source
                    .in_txns()
                    .then(|| *self.txns_read.txns_id());

                let metadata = CollectionMetadata {
                    persist_location: self.persist_location.clone(),
                    data_shard,
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
                    debug!("mapping GlobalId={} to shard ({})", id, metadata.data_shard);

                    let write = this
                        .open_data_handles(
                            &id,
                            metadata.data_shard,
                            metadata.relation_desc.clone(),
                            persist_client,
                        )
                        .await;

                    Ok::<_, StorageError<T>>((id, description, write, metadata))
                }
            })
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

        // The set of collections that we should render at the end of this
        // function.
        let mut to_execute = BTreeSet::new();
        // New collections that are being created; this is distinct from the set
        // of collections we plan to execute because
        // `DataSource::IngestionExport` is added as a new collection, but is
        // not executed directly.
        let mut new_collections = BTreeSet::new();
        let mut table_registers = Vec::with_capacity(to_register.len());

        // Reorder in dependency order.
        to_register.sort_by_key(|(id, ..)| *id);

        // Register tables first, but register them in reverse order since earlier tables
        // can depend on later tables.
        //
        // Note: We could do more complex sorting to avoid the allocations, but IMO it's
        // easier to reason about it this way.
        let (tables_to_register, collections_to_register): (Vec<_>, Vec<_>) = to_register
            .into_iter()
            .partition(|(_id, desc, ..)| desc.data_source == DataSource::Table);
        let to_register = tables_to_register
            .into_iter()
            .rev()
            .chain(collections_to_register.into_iter());

        // Statistics need a level of indirection so we can mutably borrow
        // `self` when registering collections and when we are inserting
        // statistics.
        let mut new_source_statistic_entries = BTreeSet::new();
        let mut new_webhook_statistic_entries = BTreeSet::new();
        let mut new_sink_statistic_entries = BTreeSet::new();

        for (id, description, write, metadata) in to_register {
            let is_in_txns = |id, metadata: &CollectionMetadata| {
                metadata.txns_shard.is_some()
                    && !(self.read_only && migrated_storage_collections.contains(&id))
            };

            to_execute.insert(id);
            new_collections.insert(id);

            let write_frontier = write.upper();

            // Determine if this collection has another dependency.
            let storage_dependencies = self.determine_collection_dependencies(id, &description)?;

            let dependency_read_holds = self
                .storage_collections
                .acquire_read_holds(storage_dependencies)
                .expect("can acquire read holds");

            let mut dependency_since = Antichain::from_elem(T::minimum());
            for read_hold in dependency_read_holds.iter() {
                dependency_since.join_assign(read_hold.since());
            }

            let data_source = description.data_source;

            // Assert some invariants.
            //
            // TODO(alter_table): Include Tables (is_in_txns) in this check. After
            // supporting ALTER TABLE, it's now possible for a table to have a dependency
            // and thus run this check. But tables are managed by txn-wal and thus the
            // upper of the shard's `write_handle` generally isn't the logical upper of
            // the shard. Instead we need to thread through the upper of the `txn` shard
            // here so we can check this invariant.
            if !dependency_read_holds.is_empty()
                && !is_in_txns(id, &metadata)
                && !matches!(&data_source, DataSource::Sink { .. })
            {
                // As the halt message says, this can happen when trying to come
                // up in read-only mode and the current read-write
                // environmentd/controller deletes a collection. We exit
                // gracefully, which means we'll get restarted and get to try
                // again.
                if dependency_since.is_empty() {
                    halt!(
                        "dependency since frontier is empty while dependent upper \
                        is not empty (dependent id={id}, write_frontier={:?}, dependency_read_holds={:?}), \
                        this indicates concurrent deletion of a collection",
                        write_frontier,
                        dependency_read_holds,
                    );
                }

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

            // Perform data source-specific setup.
            let mut extra_state = CollectionStateExtra::None;
            let mut maybe_instance_id = None;
            match &data_source {
                DataSource::Introspection(typ) => {
                    debug!(
                        ?data_source, meta = ?metadata,
                        "registering {id} with persist monotonic worker",
                    );
                    // We always register the collection with the collection manager,
                    // regardless of read-only mode. The CollectionManager itself is
                    // aware of read-only mode and will not attempt to write before told
                    // to do so.
                    //
                    self.register_introspection_collection(
                        id,
                        *typ,
                        write,
                        persist_client.clone(),
                    )?;
                }
                DataSource::Webhook => {
                    debug!(
                        ?data_source, meta = ?metadata,
                        "registering {id} with persist monotonic worker",
                    );
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
                    debug!(
                        ?data_source, meta = ?metadata,
                        "not registering {id} with a controller persist worker",
                    );
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
                        hydrated_on: BTreeSet::new(),
                    };

                    extra_state = CollectionStateExtra::Ingestion(ingestion_state);
                    maybe_instance_id = Some(instance_id);

                    new_source_statistic_entries.insert(id);
                }
                DataSource::Table => {
                    debug!(
                        ?data_source, meta = ?metadata,
                        "registering {id} with persist table worker",
                    );
                    table_registers.push((id, write));
                }
                DataSource::Progress | DataSource::Other => {
                    debug!(
                        ?data_source, meta = ?metadata,
                        "not registering {id} with a controller persist worker",
                    );
                }
                DataSource::Ingestion(ingestion_desc) => {
                    debug!(
                        ?data_source, meta = ?metadata,
                        "not registering {id} with a controller persist worker",
                    );

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
                        hydrated_on: BTreeSet::new(),
                    };

                    extra_state = CollectionStateExtra::Ingestion(ingestion_state);
                    maybe_instance_id = Some(ingestion_desc.instance_id);

                    new_source_statistic_entries.insert(id);
                }
                DataSource::Sink { desc } => {
                    let mut dependency_since = Antichain::from_elem(T::minimum());
                    for read_hold in dependency_read_holds.iter() {
                        dependency_since.join_assign(read_hold.since());
                    }

                    let [self_hold, read_hold] =
                        dependency_read_holds.try_into().expect("two holds");

                    let state = ExportState::new(
                        desc.instance_id,
                        read_hold,
                        self_hold,
                        write_frontier.clone(),
                        ReadPolicy::step_back(),
                    );
                    maybe_instance_id = Some(state.cluster_id);
                    extra_state = CollectionStateExtra::Export(state);

                    new_sink_statistic_entries.insert(id);
                }
            }

            let wallclock_lag_metrics = self.metrics.wallclock_lag_metrics(id, maybe_instance_id);
            let collection_state =
                CollectionState::new(data_source, metadata, extra_state, wallclock_lag_metrics);

            self.collections.insert(id, collection_state);
        }

        {
            let mut source_statistics = self.source_statistics.lock().expect("poisoned");

            // Webhooks don't run on clusters/replicas, so we initialize their
            // statistics collection here.
            for id in new_webhook_statistic_entries {
                source_statistics.webhook_statistics.entry(id).or_default();
            }

            // Sources and sinks only have statistics in the collection when
            // there is a replica that is reporting them. No need to initialize
            // here.
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

        self.append_shard_mappings(new_collections.into_iter(), Diff::ONE);

        // TODO(guswynn): perform the io in this final section concurrently.
        for id in to_execute {
            match &self.collection(id)?.data_source {
                DataSource::Ingestion(ingestion) => {
                    if !self.read_only
                        || (ENABLE_0DT_DEPLOYMENT_SOURCES.get(self.config.config_set())
                            && ingestion.desc.connection.supports_read_only())
                    {
                        self.run_ingestion(id)?;
                    }
                }
                DataSource::IngestionExport { .. } => unreachable!(
                    "ingestion exports do not execute directly, but instead schedule their source to be re-executed"
                ),
                DataSource::Introspection(_)
                | DataSource::Webhook
                | DataSource::Table
                | DataSource::Progress
                | DataSource::Other => {}
                DataSource::Sink { .. } => {
                    if !self.read_only {
                        self.run_export(id)?;
                    }
                }
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

    async fn alter_ingestion_export_data_configs(
        &mut self,
        source_exports: BTreeMap<GlobalId, SourceExportDataConfig>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        // Also have to let StorageCollections know!
        self.storage_collections
            .alter_ingestion_export_data_configs(source_exports.clone())
            .await?;

        let mut ingestions_to_run = BTreeSet::new();

        for (source_export_id, new_data_config) in source_exports {
            // We need to adjust the data config on the CollectionState for
            // the source export collection directly
            let source_export_collection = self
                .collections
                .get_mut(&source_export_id)
                .ok_or_else(|| StorageError::IdentifierMissing(source_export_id))?;
            let ingestion_id = match &mut source_export_collection.data_source {
                DataSource::IngestionExport {
                    ingestion_id,
                    details: _,
                    data_config,
                } => {
                    *data_config = new_data_config.clone();
                    *ingestion_id
                }
                o => {
                    tracing::warn!("alter_ingestion_export_data_configs called on {:?}", o);
                    Err(StorageError::IdentifierInvalid(source_export_id))?
                }
            };
            // We also need to adjust the data config on the CollectionState of the
            // Ingestion that the export is associated with.
            let ingestion_collection = self
                .collections
                .get_mut(&ingestion_id)
                .ok_or_else(|| StorageError::IdentifierMissing(ingestion_id))?;

            match &mut ingestion_collection.data_source {
                DataSource::Ingestion(ingestion_desc) => {
                    let source_export = ingestion_desc
                        .source_exports
                        .get_mut(&source_export_id)
                        .ok_or_else(|| StorageError::IdentifierMissing(source_export_id))?;

                    // If the data config hasn't changed, there's no sense in
                    // re-rendering the dataflow.
                    if source_export.data_config != new_data_config {
                        tracing::info!(?source_export_id, from = ?source_export.data_config, to = ?new_data_config, "alter_ingestion_export_data_configs, updating");
                        source_export.data_config = new_data_config;

                        ingestions_to_run.insert(ingestion_id);
                    } else {
                        tracing::warn!(
                            "alter_ingestion_export_data_configs called on \
                                    export {source_export_id} of {ingestion_id} but \
                                    the data config was the same"
                        );
                    }
                }
                o => {
                    tracing::warn!("alter_ingestion_export_data_configs called on {:?}", o);
                    Err(StorageError::IdentifierInvalid(ingestion_id))?
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
        existing_collection: GlobalId,
        new_collection: GlobalId,
        new_desc: RelationDesc,
        expected_version: RelationVersion,
        register_ts: Self::Timestamp,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let data_shard = {
            let Controller {
                collections,
                storage_collections,
                ..
            } = self;

            let existing = collections
                .get(&existing_collection)
                .ok_or(StorageError::IdentifierMissing(existing_collection))?;
            if existing.data_source != DataSource::Table {
                return Err(StorageError::IdentifierInvalid(existing_collection));
            }

            // Let StorageCollections know!
            storage_collections
                .alter_table_desc(
                    existing_collection,
                    new_collection,
                    new_desc.clone(),
                    expected_version,
                )
                .await?;

            existing.collection_metadata.data_shard.clone()
        };

        let persist_client = self
            .persist
            .open(self.persist_location.clone())
            .await
            .expect("invalid persist location");
        let write_handle = self
            .open_data_handles(
                &existing_collection,
                data_shard,
                new_desc.clone(),
                &persist_client,
            )
            .await;

        let collection_meta = CollectionMetadata {
            persist_location: self.persist_location.clone(),
            data_shard,
            relation_desc: new_desc.clone(),
            // TODO(alter_table): Support schema evolution on sources.
            txns_shard: Some(self.txns_read.txns_id().clone()),
        };
        // TODO(alter_table): Support schema evolution on sources.
        let wallclock_lag_metrics = self.metrics.wallclock_lag_metrics(new_collection, None);
        let collection_state = CollectionState::new(
            DataSource::Table,
            collection_meta,
            CollectionStateExtra::None,
            wallclock_lag_metrics,
        );

        // Great! We have successfully evolved the schema of our Table, now we need to update our
        // in-memory data structures.
        self.collections.insert(new_collection, collection_state);

        self.persist_table_worker
            .register(register_ts, vec![(new_collection, write_handle)])
            .await
            .expect("table worker unexpectedly shut down");

        self.append_shard_mappings([new_collection].into_iter(), Diff::ONE);

        Ok(())
    }

    fn export(
        &self,
        id: GlobalId,
    ) -> Result<&ExportState<Self::Timestamp>, StorageError<Self::Timestamp>> {
        self.collections
            .get(&id)
            .and_then(|c| match &c.extra_state {
                CollectionStateExtra::Export(state) => Some(state),
                _ => None,
            })
            .ok_or(StorageError::IdentifierMissing(id))
    }

    fn export_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut ExportState<Self::Timestamp>, StorageError<Self::Timestamp>> {
        self.collections
            .get_mut(&id)
            .and_then(|c| match &mut c.extra_state {
                CollectionStateExtra::Export(state) => Some(state),
                _ => None,
            })
            .ok_or(StorageError::IdentifierMissing(id))
    }

    /// Create a oneshot ingestion.
    async fn create_oneshot_ingestion(
        &mut self,
        ingestion_id: uuid::Uuid,
        collection_id: GlobalId,
        instance_id: StorageInstanceId,
        request: OneshotIngestionRequest,
        result_tx: OneshotResultCallback<ProtoBatch>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        let collection_meta = self
            .collections
            .get(&collection_id)
            .ok_or_else(|| StorageError::IdentifierMissing(collection_id))?
            .collection_metadata
            .clone();
        let instance = self.instances.get_mut(&instance_id).ok_or_else(|| {
            // TODO(cf2): Refine this error.
            StorageError::Generic(anyhow::anyhow!("missing cluster {instance_id}"))
        })?;
        let oneshot_cmd = RunOneshotIngestion {
            ingestion_id,
            collection_id,
            collection_meta,
            request,
        };

        if !self.read_only {
            instance.send(StorageCommand::RunOneshotIngestion(Box::new(oneshot_cmd)));
            let pending = PendingOneshotIngestion {
                result_tx,
                cluster_id: instance_id,
            };
            let novel = self
                .pending_oneshot_ingestions
                .insert(ingestion_id, pending);
            assert_none!(novel);
            Ok(())
        } else {
            Err(StorageError::ReadOnly)
        }
    }

    fn cancel_oneshot_ingestion(
        &mut self,
        ingestion_id: uuid::Uuid,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        if self.read_only {
            return Err(StorageError::ReadOnly);
        }

        let pending = self
            .pending_oneshot_ingestions
            .remove(&ingestion_id)
            .ok_or_else(|| {
                // TODO(cf2): Refine this error.
                StorageError::Generic(anyhow::anyhow!("missing oneshot ingestion {ingestion_id}"))
            })?;

        match self.instances.get_mut(&pending.cluster_id) {
            Some(instance) => {
                instance.send(StorageCommand::CancelOneshotIngestion(ingestion_id));
            }
            None => {
                mz_ore::soft_panic_or_log!(
                    "canceling oneshot ingestion on non-existent cluster, ingestion {:?}, instance {}",
                    ingestion_id,
                    pending.cluster_id,
                );
            }
        }
        // Respond to the user that the request has been canceled.
        pending.cancel();

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
        let desired_read_holds = vec![from_id.clone(), id.clone()];
        let [input_hold, self_hold] = self
            .storage_collections
            .acquire_read_holds(desired_read_holds)
            .expect("missing dependency")
            .try_into()
            .expect("expected number of holds");
        let from_storage_metadata = self.storage_collections.collection_metadata(from_id)?;
        let to_storage_metadata = self.storage_collections.collection_metadata(id)?;

        // Check whether the sink's write frontier is beyond the read hold we got
        let cur_export = self.export_mut(id)?;
        let input_readable = cur_export
            .write_frontier
            .iter()
            .all(|t| input_hold.since().less_than(t));
        if !input_readable {
            return Err(StorageError::ReadBeforeSince(from_id));
        }

        let new_export = ExportState {
            read_capabilities: cur_export.read_capabilities.clone(),
            cluster_id: new_description.instance_id,
            derived_since: cur_export.derived_since.clone(),
            read_holds: [input_hold, self_hold],
            read_policy: cur_export.read_policy.clone(),
            write_frontier: cur_export.write_frontier.clone(),
        };
        *cur_export = new_export;

        let cmd = RunSinkCommand {
            id,
            description: StorageSinkDesc {
                from: from_id,
                from_desc: new_description.sink.from_desc,
                connection: new_description.sink.connection,
                envelope: new_description.sink.envelope,
                as_of: new_description.sink.as_of,
                version: new_description.sink.version,
                from_storage_metadata,
                with_snapshot: new_description.sink.with_snapshot,
                to_storage_metadata,
                commit_interval: new_description.sink.commit_interval,
            },
        };

        // Fetch the client for this export's cluster.
        let instance = self
            .instances
            .get_mut(&new_description.instance_id)
            .ok_or_else(|| StorageError::ExportInstanceMissing {
                storage_instance_id: new_description.instance_id,
                export_id: id,
            })?;

        instance.send(StorageCommand::RunSink(Box::new(cmd)));
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
            let (mut new_export_description, as_of): (ExportDescription<Self::Timestamp>, _) = {
                let export = &self.collections[&id];
                let DataSource::Sink { desc } = &export.data_source else {
                    panic!("export exists")
                };
                let CollectionStateExtra::Export(state) = &export.extra_state else {
                    panic!("export exists")
                };
                let export_description = desc.clone();
                let as_of = state.input_hold().since().clone();

                (export_description, as_of)
            };
            let current_sink = new_export_description.sink.clone();

            new_export_description.sink.connection = connection;

            // Ensure compatibility
            current_sink.alter_compatible(id, &new_export_description.sink)?;

            let from_storage_metadata = self
                .storage_collections
                .collection_metadata(new_export_description.sink.from)?;
            let to_storage_metadata = self.storage_collections.collection_metadata(id)?;

            let cmd = RunSinkCommand {
                id,
                description: StorageSinkDesc {
                    from: new_export_description.sink.from,
                    from_desc: new_export_description.sink.from_desc.clone(),
                    connection: new_export_description.sink.connection.clone(),
                    envelope: new_export_description.sink.envelope,
                    with_snapshot: new_export_description.sink.with_snapshot,
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
                    from_storage_metadata,
                    to_storage_metadata,
                    commit_interval: new_export_description.sink.commit_interval,
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

            for cmd in cmds {
                instance.send(StorageCommand::RunSink(Box::new(cmd)));
            }

            // Update state only after all possible errors have occurred.
            for (id, new_export_description) in export_updates {
                let Some(state) = self.collections.get_mut(&id) else {
                    panic!("export known to exist")
                };
                let DataSource::Sink { desc } = &mut state.data_source else {
                    panic!("export known to exist")
                };
                *desc = new_export_description;
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
                DataSource::Table => true,
                DataSource::IngestionExport { .. } | DataSource::Webhook => false,
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
        // Keep track of which ingestions we have to execute still, and which we
        // have to change because of dropped subsources.
        let mut ingestions_to_execute = BTreeSet::new();
        let mut ingestions_to_drop = BTreeSet::new();
        let mut source_statistics_to_drop = Vec::new();

        // Ingestions (and their exports) are also collections, but we keep
        // track of non-ingestion collections separately, because they have
        // slightly different cleanup logic below.
        let mut collections_to_drop = Vec::new();

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
                    DataSource::Webhook => {
                        // TODO(parkmycar): The Collection Manager and PersistMonotonicWriter
                        // could probably use some love and maybe get merged together?
                        let fut = self.collection_manager.unregister_collection(*id);
                        mz_ore::task::spawn(|| format!("storage-webhook-cleanup-{id}"), fut);

                        collections_to_drop.push(*id);
                        source_statistics_to_drop.push(*id);
                    }
                    DataSource::Ingestion(_) => {
                        ingestions_to_drop.insert(*id);
                        source_statistics_to_drop.push(*id);
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
                                tracing::error!(
                                    "primary source {ingestion_id} seemingly dropped before subsource {id}"
                                );
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
                            _ => unreachable!(
                                "SourceExport must only refer to primary sources that already exist"
                            ),
                        };

                        // Ingestion exports also have ReadHolds that we need to
                        // downgrade, and much of their drop machinery is the
                        // same as for the "main" ingestion.
                        ingestions_to_drop.insert(*id);
                        source_statistics_to_drop.push(*id);
                    }
                    DataSource::Progress | DataSource::Table | DataSource::Other => {
                        collections_to_drop.push(*id);
                    }
                    DataSource::Introspection(_) | DataSource::Sink { .. } => {
                        // Collections of these types are either not sources and should be dropped
                        // through other means, or are sources but should never be dropped.
                        soft_panic_or_log!(
                            "drop_sources called on a {:?} (id={id}))",
                            collection_state.data_source,
                        );
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
            .map(|id| (*id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();

        tracing::debug!(
            ?ingestion_policies,
            "dropping sources by setting read hold policies"
        );
        self.set_hold_policies(ingestion_policies);

        // Delete all collection->shard mappings
        let shards_to_update: BTreeSet<_> = ingestions_to_drop
            .iter()
            .chain(collections_to_drop.iter())
            .cloned()
            .collect();
        self.append_shard_mappings(shards_to_update.into_iter(), Diff::MINUS_ONE);

        let status_now = mz_ore::now::to_datetime((self.now)());
        let mut status_updates = vec![];
        for id in ingestions_to_drop.iter() {
            status_updates.push(StatusUpdate::new(*id, status_now, Status::Dropped));
        }

        if !self.read_only {
            self.append_status_introspection_updates(
                IntrospectionType::SourceStatusHistory,
                status_updates,
            );
        }

        {
            let mut source_statistics = self.source_statistics.lock().expect("poisoned");
            for id in source_statistics_to_drop {
                source_statistics
                    .source_statistics
                    .retain(|(stats_id, _), _| stats_id != &id);
                source_statistics
                    .webhook_statistics
                    .retain(|stats_id, _| stats_id != &id);
            }
        }

        // Remove collection state
        for id in ingestions_to_drop.iter().chain(collections_to_drop.iter()) {
            tracing::info!(%id, "dropping collection state");
            let collection = self
                .collections
                .remove(id)
                .expect("list populated after checking that self.collections contains it");

            let instance = match &collection.extra_state {
                CollectionStateExtra::Ingestion(ingestion) => Some(ingestion.instance_id),
                CollectionStateExtra::Export(export) => Some(export.cluster_id()),
                CollectionStateExtra::None => None,
            }
            .and_then(|i| self.instances.get(&i));

            // Record which replicas were running a collection, so that we can
            // match DroppedId messages against them and eventually remove state
            // from self.dropped_objects
            if let Some(instance) = instance {
                let active_replicas = instance.get_active_replicas_for_object(id);
                if !active_replicas.is_empty() {
                    // The remap collection of an ingestion doesn't have extra
                    // state and doesn't have an instance_id, but we still get
                    // upper updates for them, so want to make sure to populate
                    // dropped_ids.
                    // TODO(aljoscha): All this is a bit icky. But here we are
                    // for now...
                    match &collection.data_source {
                        DataSource::Ingestion(ingestion_desc) => {
                            if *id != ingestion_desc.remap_collection_id {
                                self.dropped_objects.insert(
                                    ingestion_desc.remap_collection_id,
                                    active_replicas.clone(),
                                );
                            }
                        }
                        _ => {}
                    }

                    self.dropped_objects.insert(*id, active_replicas);
                }
            }
        }

        // Also let StorageCollections know!
        self.storage_collections
            .drop_collections_unvalidated(storage_metadata, ids);

        Ok(())
    }

    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    fn drop_sinks(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), StorageError<Self::Timestamp>> {
        self.validate_export_ids(identifiers.iter().cloned())?;
        self.drop_sinks_unvalidated(storage_metadata, identifiers);
        Ok(())
    }

    fn drop_sinks_unvalidated(
        &mut self,
        storage_metadata: &StorageMetadata,
        mut sinks_to_drop: Vec<GlobalId>,
    ) {
        // Ignore exports that have already been removed.
        sinks_to_drop.retain(|id| self.export(*id).is_ok());

        // TODO: ideally we'd advance the write frontier ourselves here, but this function's
        // not yet marked async.

        // We don't explicitly remove read capabilities! Downgrading the
        // frontier of the source to `[]` (the empty Antichain), will propagate
        // to the storage dependencies.
        let drop_policy = sinks_to_drop
            .iter()
            .map(|id| (*id, ReadPolicy::ValidFrom(Antichain::new())))
            .collect();

        tracing::debug!(
            ?drop_policy,
            "dropping sources by setting read hold policies"
        );
        self.set_hold_policies(drop_policy);

        // Record the drop status for all sink drops.
        //
        // We also delete the items' statistics objects.
        //
        // The locks are held for a short time, only while we do some removals from a map.

        let status_now = mz_ore::now::to_datetime((self.now)());

        // Record the drop status for all pending sink drops.
        let mut status_updates = vec![];
        {
            let mut sink_statistics = self.sink_statistics.lock().expect("poisoned");
            for id in sinks_to_drop.iter() {
                status_updates.push(StatusUpdate::new(*id, status_now, Status::Dropped));
                sink_statistics.retain(|(stats_id, _), _| stats_id != id);
            }
        }

        if !self.read_only {
            self.append_status_introspection_updates(
                IntrospectionType::SinkStatusHistory,
                status_updates,
            );
        }

        // Remove collection/export state
        for id in sinks_to_drop.iter() {
            tracing::info!(%id, "dropping export state");
            let collection = self
                .collections
                .remove(id)
                .expect("list populated after checking that self.collections contains it");

            let instance = match &collection.extra_state {
                CollectionStateExtra::Ingestion(ingestion) => Some(ingestion.instance_id),
                CollectionStateExtra::Export(export) => Some(export.cluster_id()),
                CollectionStateExtra::None => None,
            }
            .and_then(|i| self.instances.get(&i));

            // Record how many replicas were running an export, so that we can
            // match `DroppedId` messages against it and eventually remove state
            // from `self.dropped_objects`.
            if let Some(instance) = instance {
                let active_replicas = instance.get_active_replicas_for_object(id);
                if !active_replicas.is_empty() {
                    self.dropped_objects.insert(*id, active_replicas);
                }
            }
        }

        // Also let StorageCollections know!
        self.storage_collections
            .drop_collections_unvalidated(storage_metadata, sinks_to_drop);
    }

    #[instrument(level = "debug")]
    fn append_table(
        &mut self,
        write_ts: Self::Timestamp,
        advance_to: Self::Timestamp,
        commands: Vec<(GlobalId, Vec<TableData>)>,
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

    async fn ready(&mut self) {
        if self.maintenance_scheduled {
            return;
        }

        if !self.pending_table_handle_drops_rx.is_empty() {
            return;
        }

        tokio::select! {
            Some(m) = self.instance_response_rx.recv() => {
                self.stashed_responses.push(m);
                while let Ok(m) = self.instance_response_rx.try_recv() {
                    self.stashed_responses.push(m);
                }
            }
            _ = self.maintenance_ticker.tick() => {
                self.maintenance_scheduled = true;
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
            self.maintenance_scheduled = false;
        }

        for instance in self.instances.values_mut() {
            instance.rehydrate_failed_replicas();
        }

        let mut status_updates = vec![];
        let mut updated_frontiers = BTreeMap::new();

        // Take the currently stashed responses so that we can call mut receiver functions in the loop.
        let stashed_responses = std::mem::take(&mut self.stashed_responses);
        for resp in stashed_responses {
            match resp {
                (_replica_id, StorageResponse::FrontierUpper(id, upper)) => {
                    self.update_write_frontier(id, &upper);
                    updated_frontiers.insert(id, upper);
                }
                (replica_id, StorageResponse::DroppedId(id)) => {
                    let replica_id = replica_id.expect("DroppedId from unknown replica");
                    if let Some(remaining_replicas) = self.dropped_objects.get_mut(&id) {
                        remaining_replicas.remove(&replica_id);
                        if remaining_replicas.is_empty() {
                            self.dropped_objects.remove(&id);
                        }
                    } else {
                        soft_panic_or_log!("unexpected DroppedId for {id}");
                    }
                }
                (replica_id, StorageResponse::StatisticsUpdates(source_stats, sink_stats)) => {
                    // Note we only hold the locks while moving some plain-old-data around here.
                    {
                        // NOTE(aljoscha): We explicitly unwrap the `Option`,
                        // because we expect that stats coming from replicas
                        // have a replica id.
                        //
                        // If this is `None` and we would use that to access
                        // state below we might clobber something unexpectedly.
                        let replica_id = if let Some(replica_id) = replica_id {
                            replica_id
                        } else {
                            tracing::error!(
                                ?source_stats,
                                "missing replica_id for source statistics update"
                            );
                            continue;
                        };

                        let mut shared_stats = self.source_statistics.lock().expect("poisoned");

                        for stat in source_stats {
                            let collection_id = stat.id.clone();

                            if self.collection(collection_id).is_err() {
                                // We can get updates for collections that have
                                // already been deleted, ignore those.
                                continue;
                            }

                            let entry = shared_stats
                                .source_statistics
                                .entry((stat.id, Some(replica_id)));

                            match entry {
                                btree_map::Entry::Vacant(vacant_entry) => {
                                    let mut stats = ControllerSourceStatistics::new(
                                        collection_id,
                                        Some(replica_id),
                                    );
                                    stats.incorporate(stat);
                                    vacant_entry.insert(stats);
                                }
                                btree_map::Entry::Occupied(mut occupied_entry) => {
                                    occupied_entry.get_mut().incorporate(stat);
                                }
                            }
                        }
                    }

                    {
                        // NOTE(aljoscha); Same as above. We want to be
                        // explicit.
                        //
                        // Also, technically for sinks there is no webhook
                        // "sources" that would force us to use an `Option`. But
                        // we still have to use an option for other reasons: the
                        // scraper expects a trait for working with stats, and
                        // that in the end forces the sink stats map to also
                        // have `Option` in the key. Do I like that? No, but
                        // here we are.
                        let replica_id = if let Some(replica_id) = replica_id {
                            replica_id
                        } else {
                            tracing::error!(
                                ?sink_stats,
                                "missing replica_id for sink statistics update"
                            );
                            continue;
                        };

                        let mut shared_stats = self.sink_statistics.lock().expect("poisoned");

                        for stat in sink_stats {
                            let collection_id = stat.id.clone();

                            if self.collection(collection_id).is_err() {
                                // We can get updates for collections that have
                                // already been deleted, ignore those.
                                continue;
                            }

                            let entry = shared_stats.entry((stat.id, Some(replica_id)));

                            match entry {
                                btree_map::Entry::Vacant(vacant_entry) => {
                                    let mut stats =
                                        ControllerSinkStatistics::new(collection_id, replica_id);
                                    stats.incorporate(stat);
                                    vacant_entry.insert(stats);
                                }
                                btree_map::Entry::Occupied(mut occupied_entry) => {
                                    occupied_entry.get_mut().incorporate(stat);
                                }
                            }
                        }
                    }
                }
                (replica_id, StorageResponse::StatusUpdate(mut status_update)) => {
                    // NOTE(aljoscha): We sniff out the hydration status for
                    // ingestions from status updates. This is the easiest we
                    // can do right now, without going deeper into changing the
                    // comms protocol between controller and cluster. We cannot,
                    // for example use `StorageResponse::FrontierUpper`,
                    // because those will already get sent when the ingestion is
                    // just being created.
                    //
                    // Sources differ in when they will report as Running. Kafka
                    // UPSERT sources will only switch to `Running` once their
                    // state has been initialized from persist, which is the
                    // first case that we care about right now.
                    //
                    // I wouldn't say it's ideal, but it's workable until we
                    // find something better.
                    match status_update.status {
                        Status::Running => {
                            let collection = self.collections.get_mut(&status_update.id);
                            match collection {
                                Some(collection) => {
                                    match collection.extra_state {
                                        CollectionStateExtra::Ingestion(
                                            ref mut ingestion_state,
                                        ) => {
                                            if ingestion_state.hydrated_on.is_empty() {
                                                tracing::debug!(ingestion_id = %status_update.id, "ingestion is hydrated");
                                            }
                                            ingestion_state.hydrated_on.insert(replica_id.expect(
                                                "replica id should be present for status running",
                                            ));
                                        }
                                        CollectionStateExtra::Export(_) => {
                                            // TODO(sinks): track sink hydration?
                                        }
                                        CollectionStateExtra::None => {
                                            // Nothing to do
                                        }
                                    }
                                }
                                None => (), // no collection, let's say that's fine
                                            // here
                            }
                        }
                        Status::Paused => {
                            let collection = self.collections.get_mut(&status_update.id);
                            match collection {
                                Some(collection) => {
                                    match collection.extra_state {
                                        CollectionStateExtra::Ingestion(
                                            ref mut ingestion_state,
                                        ) => {
                                            // TODO: Paused gets send when there
                                            // are no active replicas. We should
                                            // change this to send a targeted
                                            // Pause for each replica, and do
                                            // more fine-grained hydration
                                            // tracking here.
                                            tracing::debug!(ingestion_id = %status_update.id, "ingestion is now paused");
                                            ingestion_state.hydrated_on.clear();
                                        }
                                        CollectionStateExtra::Export(_) => {
                                            // TODO(sinks): track sink hydration?
                                        }
                                        CollectionStateExtra::None => {
                                            // Nothing to do
                                        }
                                    }
                                }
                                None => (), // no collection, let's say that's fine
                                            // here
                            }
                        }
                        _ => (),
                    }

                    // Set replica_id in the status update if available
                    if let Some(id) = replica_id {
                        status_update.replica_id = Some(id);
                    }
                    status_updates.push(status_update);
                }
                (_replica_id, StorageResponse::StagedBatches(batches)) => {
                    for (ingestion_id, batches) in batches {
                        match self.pending_oneshot_ingestions.remove(&ingestion_id) {
                            Some(pending) => {
                                // Send a cancel command so our command history is correct. And to
                                // avoid duplicate work once we have active replication.
                                if let Some(instance) = self.instances.get_mut(&pending.cluster_id)
                                {
                                    instance
                                        .send(StorageCommand::CancelOneshotIngestion(ingestion_id));
                                }
                                // Send the results down our channel.
                                (pending.result_tx)(batches)
                            }
                            None => {
                                // We might not be tracking this oneshot ingestion anymore because
                                // it was canceled.
                            }
                        }
                    }
                }
            }
        }

        self.record_status_updates(status_updates);

        // Process dropped tables in a single batch.
        let mut dropped_table_ids = Vec::new();
        while let Ok(dropped_id) = self.pending_table_handle_drops_rx.try_recv() {
            dropped_table_ids.push(dropped_id);
        }
        if !dropped_table_ids.is_empty() {
            self.drop_sources(storage_metadata, dropped_table_ids)?;
        }

        if updated_frontiers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Response::FrontierUpdates(
                updated_frontiers.into_iter().collect(),
            )))
        }
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
        let id = self.introspection_ids[&type_];
        let updates = updates.into_iter().map(|update| update.into()).collect();
        self.collection_manager.blind_write(id, updates);
    }

    fn append_status_introspection_updates(
        &mut self,
        type_: IntrospectionType,
        updates: Vec<StatusUpdate>,
    ) {
        let id = self.introspection_ids[&type_];
        let updates: Vec<_> = updates.into_iter().map(|update| update.into()).collect();
        if !updates.is_empty() {
            self.collection_manager.blind_write(id, updates);
        }
    }

    fn update_introspection_collection(&mut self, type_: IntrospectionType, op: StorageWriteOp) {
        let id = self.introspection_ids[&type_];
        self.collection_manager.differential_write(id, op);
    }

    fn append_only_introspection_tx(
        &self,
        type_: IntrospectionType,
    ) -> mpsc::UnboundedSender<(
        Vec<AppendOnlyUpdate>,
        oneshot::Sender<Result<(), StorageError<Self::Timestamp>>>,
    )> {
        let id = self.introspection_ids[&type_];
        self.collection_manager.append_only_write_sender(id)
    }

    fn differential_introspection_tx(
        &self,
        type_: IntrospectionType,
    ) -> mpsc::UnboundedSender<(
        StorageWriteOp,
        oneshot::Sender<Result<(), StorageError<Self::Timestamp>>>,
    )> {
        let id = self.introspection_ids[&type_];
        self.collection_manager.differential_write_sender(id)
    }

    async fn real_time_recent_timestamp(
        &self,
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
                    | GenericSourceConnection::MySql(_)
                    | GenericSourceConnection::SqlServer(_) => {
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

    fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            build_info: _,
            now: _,
            read_only,
            collections,
            dropped_objects,
            persist_table_worker: _,
            txns_read: _,
            txns_metrics: _,
            stashed_responses,
            pending_table_handle_drops_tx: _,
            pending_table_handle_drops_rx: _,
            pending_oneshot_ingestions,
            collection_manager: _,
            introspection_ids,
            introspection_tokens: _,
            source_statistics: _,
            sink_statistics: _,
            statistics_interval_sender: _,
            instances,
            initialized,
            config,
            persist_location,
            persist: _,
            metrics: _,
            recorded_frontiers,
            recorded_replica_frontiers,
            wallclock_lag: _,
            wallclock_lag_last_recorded,
            storage_collections: _,
            migrated_storage_collections,
            maintenance_ticker: _,
            maintenance_scheduled,
            instance_response_tx: _,
            instance_response_rx: _,
            persist_warm_task: _,
        } = self;

        let collections: BTreeMap<_, _> = collections
            .iter()
            .map(|(id, c)| (id.to_string(), format!("{c:?}")))
            .collect();
        let dropped_objects: BTreeMap<_, _> = dropped_objects
            .iter()
            .map(|(id, rs)| (id.to_string(), format!("{rs:?}")))
            .collect();
        let stashed_responses: Vec<_> =
            stashed_responses.iter().map(|r| format!("{r:?}")).collect();
        let pending_oneshot_ingestions: BTreeMap<_, _> = pending_oneshot_ingestions
            .iter()
            .map(|(uuid, i)| (uuid.to_string(), format!("{i:?}")))
            .collect();
        let introspection_ids: BTreeMap<_, _> = introspection_ids
            .iter()
            .map(|(typ, id)| (format!("{typ:?}"), id.to_string()))
            .collect();
        let instances: BTreeMap<_, _> = instances
            .iter()
            .map(|(id, i)| (id.to_string(), format!("{i:?}")))
            .collect();
        let recorded_frontiers: BTreeMap<_, _> = recorded_frontiers
            .iter()
            .map(|(id, fs)| (id.to_string(), format!("{fs:?}")))
            .collect();
        let recorded_replica_frontiers: Vec<_> = recorded_replica_frontiers
            .iter()
            .map(|((gid, rid), f)| (gid.to_string(), rid.to_string(), format!("{f:?}")))
            .collect();
        let migrated_storage_collections: Vec<_> = migrated_storage_collections
            .iter()
            .map(|id| id.to_string())
            .collect();

        Ok(serde_json::json!({
            "read_only": read_only,
            "collections": collections,
            "dropped_objects": dropped_objects,
            "stashed_responses": stashed_responses,
            "pending_oneshot_ingestions": pending_oneshot_ingestions,
            "introspection_ids": introspection_ids,
            "instances": instances,
            "initialized": initialized,
            "config": format!("{config:?}"),
            "persist_location": format!("{persist_location:?}"),
            "recorded_frontiers": recorded_frontiers,
            "recorded_replica_frontiers": recorded_replica_frontiers,
            "wallclock_lag_last_recorded": format!("{wallclock_lag_last_recorded:?}"),
            "migrated_storage_collections": migrated_storage_collections,
            "maintenance_scheduled": maintenance_scheduled,
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
        read_only: bool,
        metrics_registry: &MetricsRegistry,
        controller_metrics: ControllerMetrics,
        connection_context: ConnectionContext,
        txn: &dyn StorageTxn<T>,
        storage_collections: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
    ) -> Self {
        let txns_client = persist_clients
            .open(persist_location.clone())
            .await
            .expect("location should be valid");

        let persist_warm_task = warm_persist_state_in_background(
            txns_client.clone(),
            txn.get_collection_metadata().into_values(),
        );
        let persist_warm_task = Some(persist_warm_task.abort_on_drop());

        // This value must be already installed because we must ensure it's
        // durably recorded before it is used, otherwise we risk leaking persist
        // state.
        let txns_id = txn
            .get_txn_wal_shard()
            .expect("must call prepare initialization before creating storage controller");

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
            let mut txns = TxnsHandle::open(
                T::minimum(),
                txns_client.clone(),
                txns_client.dyncfgs().clone(),
                Arc::clone(&txns_metrics),
                txns_id,
            )
            .await;
            txns.upgrade_version().await;
            persist_handles::PersistTableWriteWorker::new_txns(txns)
        };
        let txns_read = TxnsRead::start::<TxnsCodecRow>(txns_client.clone(), txns_id).await;

        let collection_manager = collection_mgmt::CollectionManager::new(read_only, now.clone());

        let introspection_ids = BTreeMap::new();
        let introspection_tokens = Arc::new(Mutex::new(BTreeMap::new()));

        let (statistics_interval_sender, _) =
            channel(mz_storage_types::parameters::STATISTICS_INTERVAL_DEFAULT);

        let (pending_table_handle_drops_tx, pending_table_handle_drops_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let mut maintenance_ticker = tokio::time::interval(Duration::from_secs(1));
        maintenance_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let (instance_response_tx, instance_response_rx) = mpsc::unbounded_channel();

        let metrics = StorageControllerMetrics::new(metrics_registry, controller_metrics);

        let now_dt = mz_ore::now::to_datetime(now());

        Self {
            build_info,
            collections: BTreeMap::default(),
            dropped_objects: Default::default(),
            persist_table_worker,
            txns_read,
            txns_metrics,
            stashed_responses: vec![],
            pending_table_handle_drops_tx,
            pending_table_handle_drops_rx,
            pending_oneshot_ingestions: BTreeMap::default(),
            collection_manager,
            introspection_ids,
            introspection_tokens,
            now,
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
            persist_location,
            persist: persist_clients,
            metrics,
            recorded_frontiers: BTreeMap::new(),
            recorded_replica_frontiers: BTreeMap::new(),
            wallclock_lag,
            wallclock_lag_last_recorded: now_dt,
            storage_collections,
            migrated_storage_collections: BTreeSet::new(),
            maintenance_ticker,
            maintenance_scheduled: false,
            instance_response_rx,
            instance_response_tx,
            persist_warm_task,
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
                let (write_frontier, derived_since, hold_policy) = match &mut collection.extra_state
                {
                    CollectionStateExtra::Ingestion(ingestion) => (
                        ingestion.write_frontier.borrow(),
                        &mut ingestion.derived_since,
                        &mut ingestion.hold_policy,
                    ),
                    CollectionStateExtra::None => {
                        unreachable!("set_hold_policies is only called for ingestions");
                    }
                    CollectionStateExtra::Export(export) => (
                        export.write_frontier.borrow(),
                        &mut export.derived_since,
                        &mut export.read_policy,
                    ),
                };

                let new_derived_since = policy.frontier(write_frontier);
                let mut update = swap_updates(derived_since, new_derived_since);
                if !update.is_empty() {
                    read_capability_changes.insert(id, update);
                }

                *hold_policy = policy;
            }
        }

        if !read_capability_changes.is_empty() {
            self.update_hold_capabilities(&mut read_capability_changes);
        }
    }

    #[instrument(level = "debug", fields(updates))]
    fn update_write_frontier(&mut self, id: GlobalId, new_upper: &Antichain<T>) {
        let mut read_capability_changes = BTreeMap::default();

        if let Some(collection) = self.collections.get_mut(&id) {
            let (write_frontier, derived_since, hold_policy) = match &mut collection.extra_state {
                CollectionStateExtra::Ingestion(ingestion) => (
                    &mut ingestion.write_frontier,
                    &mut ingestion.derived_since,
                    &ingestion.hold_policy,
                ),
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
                    return;
                }
                CollectionStateExtra::Export(export) => (
                    &mut export.write_frontier,
                    &mut export.derived_since,
                    &export.read_policy,
                ),
            };

            if PartialOrder::less_than(write_frontier, new_upper) {
                write_frontier.clone_from(new_upper);
            }

            let new_derived_since = hold_policy.frontier(write_frontier.borrow());
            let mut update = swap_updates(derived_since, new_derived_since);
            if !update.is_empty() {
                read_capability_changes.insert(id, update);
            }
        } else if self.dropped_objects.contains_key(&id) {
            // We dropped an object but might still get updates from cluster
            // side, before it notices the drop. This is expected and fine.
        } else {
            soft_panic_or_log!("spurious upper update for {id}: {new_upper:?}");
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
                match &mut collection.extra_state {
                    CollectionStateExtra::Ingestion(ingestion) => {
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
                    }
                    CollectionStateExtra::None => {
                        // WIP: See if this ever panics in ci.
                        soft_panic_or_log!(
                            "trying to update holds for collection {collection:?} which is not \
                             an ingestion: {update:?}"
                        );
                        continue;
                    }
                    CollectionStateExtra::Export(export) => {
                        let changes = export.read_capabilities.update_iter(update.drain());
                        update.extend(changes);

                        let (changes, frontier, _cluster_id) =
                            collections_net.entry(key).or_insert_with(|| {
                                (<ChangeBatch<_>>::new(), Antichain::new(), export.cluster_id)
                            });

                        changes.extend(update.drain());
                        *frontier = export.read_capabilities.frontier().to_owned();
                    }
                }
            } else {
                // This is confusing and we should probably error.
                tracing::warn!(id = ?key, ?update, "update_hold_capabilities for unknown object");
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands and
        // downgrade persist sinces.
        for (key, (mut changes, frontier, cluster_id)) in collections_net {
            if !changes.is_empty() {
                if key.is_user() {
                    debug!(id = %key, ?frontier, "downgrading ingestion read holds!");
                }

                let collection = self
                    .collections
                    .get_mut(&key)
                    .expect("missing collection state");

                let read_holds = match &mut collection.extra_state {
                    CollectionStateExtra::Ingestion(ingestion) => {
                        ingestion.dependency_read_holds.as_mut_slice()
                    }
                    CollectionStateExtra::Export(export) => export.read_holds.as_mut_slice(),
                    CollectionStateExtra::None => {
                        soft_panic_or_log!(
                            "trying to downgrade read holds for collection which is not an \
                             ingestion: {collection:?}"
                        );
                        continue;
                    }
                };

                for read_hold in read_holds.iter_mut() {
                    read_hold
                        .try_downgrade(frontier.clone())
                        .expect("we only advance the frontier");
                }

                // Send AllowCompaction command directly to the instance
                if let Some(instance) = self.instances.get_mut(&cluster_id) {
                    instance.send(StorageCommand::AllowCompaction(key, frontier.clone()));
                } else {
                    soft_panic_or_log!(
                        "missing instance client for cluster {cluster_id} while we still have outstanding AllowCompaction command {frontier:?} for {key}"
                    );
                }
            }
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
    ) -> WriteHandle<SourceData, (), T, StorageDiff> {
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
    fn register_introspection_collection(
        &mut self,
        id: GlobalId,
        introspection_type: IntrospectionType,
        write_handle: WriteHandle<SourceData, (), T, StorageDiff>,
        persist_client: PersistClient,
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

        let prev = self.introspection_ids.insert(introspection_type, id);
        assert!(
            prev.is_none(),
            "cannot have multiple IDs for introspection type"
        );

        let metadata = self.storage_collections.collection_metadata(id)?.clone();

        let read_handle_fn = move || {
            let persist_client = persist_client.clone();
            let metadata = metadata.clone();

            let fut = async move {
                let read_handle = persist_client
                    .open_leased_reader::<SourceData, (), T, StorageDiff>(
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
                let statistics_retention_duration =
                    dyncfgs::STATISTICS_RETENTION_DURATION.get(self.config().config_set());

                // These do a shallow copy.
                let introspection_config = DifferentialIntrospectionConfig {
                    recent_upper,
                    introspection_type,
                    storage_collections: Arc::clone(&self.storage_collections),
                    collection_manager: self.collection_manager.clone(),
                    source_statistics: Arc::clone(&self.source_statistics),
                    sink_statistics: Arc::clone(&self.sink_statistics),
                    statistics_interval: self.config.parameters.statistics_interval.clone(),
                    statistics_interval_receiver: self.statistics_interval_sender.subscribe(),
                    statistics_retention_duration,
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
                let introspection_config = AppendOnlyIntrospectionConfig {
                    introspection_type,
                    config_set: Arc::clone(self.config.config_set()),
                    parameters: self.config.parameters.clone(),
                    storage_collections: Arc::clone(&self.storage_collections),
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

    /// Remove statistics for sources/sinks that were dropped but still have statistics rows
    /// hanging around.
    fn reconcile_dangling_statistics(&self) {
        self.source_statistics
            .lock()
            .expect("poisoned")
            .source_statistics
            // collections should also contain subsources.
            .retain(|(k, _replica_id), _| self.storage_collections.check_exists(*k).is_ok());
        self.sink_statistics
            .lock()
            .expect("poisoned")
            .retain(|(k, _replica_id), _| self.export(*k).is_ok());
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
    fn append_shard_mappings<I>(&self, global_ids: I, diff: Diff)
    where
        I: Iterator<Item = GlobalId>,
    {
        mz_ore::soft_assert_or_log!(
            diff == Diff::MINUS_ONE || diff == Diff::ONE,
            "use 1 for insert or -1 for delete"
        );

        let id = *self
            .introspection_ids
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
        collection_desc: &CollectionDescription<T>,
    ) -> Result<Vec<GlobalId>, StorageError<T>> {
        let mut dependencies = Vec::new();

        if let Some(id) = collection_desc.primary {
            dependencies.push(id);
        }

        match &collection_desc.data_source {
            DataSource::Introspection(_)
            | DataSource::Webhook
            | DataSource::Table
            | DataSource::Progress
            | DataSource::Other => (),
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
                dependencies.extend([self_id, ingestion_remap_collection_id]);
            }
            // Ingestions depend on their remap collection.
            DataSource::Ingestion(ingestion) => {
                // Ingestions must make sure that 1) their own collection's
                // since stays one step behind the upper, and, 2) that the remap
                // shard's since stays one step behind their upper. Hence they
                // track themselves and the remap shard as dependencies.
                dependencies.push(self_id);
                if self_id != ingestion.remap_collection_id {
                    dependencies.push(ingestion.remap_collection_id);
                }
            }
            DataSource::Sink { desc } => {
                // Sinks hold back their own frontier and the frontier of their input.
                dependencies.extend([self_id, desc.sink.from]);
            }
        };

        Ok(dependencies)
    }

    async fn read_handle_for_snapshot(
        &self,
        id: GlobalId,
    ) -> Result<ReadHandle<SourceData, (), T, StorageDiff>, StorageError<T>> {
        let metadata = self.storage_collections.collection_metadata(id)?;
        read_handle_for_snapshot(&self.persist, id, &metadata).await
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
            if self.export(id).is_ok() {
                sink_status_updates.push(update);
            } else if self.storage_collections.check_exists(id).is_ok() {
                source_status_updates.push(update);
            }
        }

        self.append_status_introspection_updates(
            IntrospectionType::SourceStatusHistory,
            source_status_updates,
        );
        self.append_status_introspection_updates(
            IntrospectionType::SinkStatusHistory,
            sink_status_updates,
        );
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
        for (export_id, export) in ingestion_description.source_exports.clone() {
            let export_storage_metadata = self.collection(export_id)?.collection_metadata.clone();
            source_exports.insert(
                export_id,
                SourceExport {
                    storage_metadata: export_storage_metadata,
                    details: export.details,
                    data_config: export.data_config,
                },
            );
        }

        let remap_collection = self.collection(ingestion_description.remap_collection_id)?;

        let description = IngestionDescription::<CollectionMetadata> {
            source_exports,
            remap_metadata: remap_collection.collection_metadata.clone(),
            // The rest of the fields are identical
            desc: ingestion_description.desc.clone(),
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

        let augmented_ingestion = Box::new(RunIngestionCommand { id, description });
        instance.send(StorageCommand::RunIngestion(augmented_ingestion));

        Ok(())
    }

    /// Runs the identified export using the current definition of the export
    /// that we have in memory.
    fn run_export(&mut self, id: GlobalId) -> Result<(), StorageError<T>> {
        let DataSource::Sink { desc: description } = &self.collections[&id].data_source else {
            return Err(StorageError::IdentifierMissing(id));
        };

        let from_storage_metadata = self
            .storage_collections
            .collection_metadata(description.sink.from)?;
        let to_storage_metadata = self.storage_collections.collection_metadata(id)?;

        // Choose an as-of frontier for this execution of the sink. If the write frontier of the sink
        // is strictly larger than its read hold, it must have at least written out its snapshot, and we can skip
        // reading it; otherwise assume we may have to replay from the beginning.
        let export_state = self.storage_collections.collection_frontiers(id)?;
        let mut as_of = description.sink.as_of.clone();
        as_of.join_assign(&export_state.implied_capability);
        let with_snapshot = description.sink.with_snapshot
            && !PartialOrder::less_than(&as_of, &export_state.write_frontier);

        info!(
            sink_id = %id,
            from_id = %description.sink.from,
            write_frontier = ?export_state.write_frontier,
            ?as_of,
            ?with_snapshot,
            "run_export"
        );

        let cmd = RunSinkCommand {
            id,
            description: StorageSinkDesc {
                from: description.sink.from,
                from_desc: description.sink.from_desc.clone(),
                connection: description.sink.connection.clone(),
                envelope: description.sink.envelope,
                as_of,
                version: description.sink.version,
                from_storage_metadata,
                with_snapshot,
                to_storage_metadata,
                commit_interval: description.sink.commit_interval,
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

        instance.send(StorageCommand::RunSink(Box::new(cmd)));

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
                .and_then(|collection_state| match &collection_state.extra_state {
                    CollectionStateExtra::Ingestion(ingestion) => Some(ingestion.instance_id),
                    CollectionStateExtra::Export(export) => Some(export.cluster_id()),
                    CollectionStateExtra::None => None,
                })
                .and_then(|i| self.instances.get(&i));

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
                    push_global_update(id, new.clone(), Diff::ONE);
                    push_global_update(id, old, Diff::MINUS_ONE);
                }
                Some(_) => (),
                None => push_global_update(id, new.clone(), Diff::ONE),
            }
        }
        for (id, old) in old_global_frontiers {
            push_global_update(id, old, Diff::MINUS_ONE);
        }

        let mut old_replica_frontiers =
            std::mem::replace(&mut self.recorded_replica_frontiers, replica_frontiers);
        for (&key, new) in &self.recorded_replica_frontiers {
            match old_replica_frontiers.remove(&key) {
                Some(old) if &old != new => {
                    push_replica_update(key, new.clone(), Diff::ONE);
                    push_replica_update(key, old, Diff::MINUS_ONE);
                }
                Some(_) => (),
                None => push_replica_update(key, new.clone(), Diff::ONE),
            }
        }
        for (key, old) in old_replica_frontiers {
            push_replica_update(key, old, Diff::MINUS_ONE);
        }

        let id = self.introspection_ids[&IntrospectionType::Frontiers];
        self.collection_manager
            .differential_append(id, global_updates);

        let id = self.introspection_ids[&IntrospectionType::ReplicaFrontiers];
        self.collection_manager
            .differential_append(id, replica_updates);
    }

    /// Refresh the wallclock lag introspection and metrics with the current lag values.
    ///
    /// This method produces wallclock lag metrics of two different shapes:
    ///
    /// * Histories: For each replica and each collection, we measure the lag of the write frontier
    ///   behind the wallclock time every second. Every minute we emit the maximum lag observed
    ///   over the last minute, together with the current time.
    /// * Histograms: For each collection, we measure the lag of the write frontier behind
    ///   wallclock time every second. Every minute we emit all lags observed over the last minute,
    ///   together with the current histogram period.
    ///
    /// Histories are emitted to both Mz introspection and Prometheus, histograms only to
    /// introspection. We treat lags of unreadable collections (i.e. collections that contain no
    /// readable times) as undefined and set them to NULL in introspection and `u64::MAX` in
    /// Prometheus.
    ///
    /// This method is invoked by `Controller::maintain`, which we expect to be called once per
    /// second during normal operation.
    fn refresh_wallclock_lag(&mut self) {
        let now_ms = (self.now)();
        let histogram_period =
            WallclockLagHistogramPeriod::from_epoch_millis(now_ms, self.config.config_set());

        let frontier_lag = |frontier: &Antichain<T>| match frontier.as_option() {
            Some(ts) => (self.wallclock_lag)(ts.clone()),
            None => Duration::ZERO,
        };

        for frontiers in self.storage_collections.active_collection_frontiers() {
            let id = frontiers.id;
            let Some(collection) = self.collections.get_mut(&id) else {
                continue;
            };

            let collection_unreadable =
                PartialOrder::less_equal(&frontiers.write_frontier, &frontiers.read_capabilities);
            let lag = if collection_unreadable {
                WallclockLag::Undefined
            } else {
                let lag = frontier_lag(&frontiers.write_frontier);
                WallclockLag::Seconds(lag.as_secs())
            };

            collection.wallclock_lag_max = collection.wallclock_lag_max.max(lag);

            // No way to specify values as undefined in Prometheus metrics, so we use the
            // maximum value instead.
            let secs = lag.unwrap_seconds_or(u64::MAX);
            collection.wallclock_lag_metrics.observe(secs);

            if let Some(stash) = &mut collection.wallclock_lag_histogram_stash {
                let bucket = lag.map_seconds(|secs| secs.next_power_of_two());

                let instance_id = match &collection.extra_state {
                    CollectionStateExtra::Ingestion(i) => Some(i.instance_id),
                    CollectionStateExtra::Export(e) => Some(e.cluster_id()),
                    CollectionStateExtra::None => None,
                };
                let workload_class = instance_id
                    .and_then(|id| self.instances.get(&id))
                    .and_then(|i| i.workload_class.clone());
                let labels = match workload_class {
                    Some(wc) => [("workload_class", wc.clone())].into(),
                    None => BTreeMap::new(),
                };

                let key = (histogram_period, bucket, labels);
                *stash.entry(key).or_default() += Diff::ONE;
            }
        }

        // Record lags to persist, if it's time.
        self.maybe_record_wallclock_lag();
    }

    /// Produce new wallclock lag introspection updates, provided enough time has passed since the
    /// last recording.
    ///
    /// We emit new introspection updates if the system time has passed into a new multiple of the
    /// recording interval (typically 1 minute) since the last refresh. The compute controller uses
    /// the same approach, ensuring that both controllers commit their lags at roughly the same
    /// time, avoiding confusion caused by inconsistencies.
    fn maybe_record_wallclock_lag(&mut self) {
        if self.read_only {
            return;
        }

        let duration_trunc = |datetime: DateTime<_>, interval| {
            let td = TimeDelta::from_std(interval).ok()?;
            datetime.duration_trunc(td).ok()
        };

        let interval = WALLCLOCK_LAG_RECORDING_INTERVAL.get(self.config.config_set());
        let now_dt = mz_ore::now::to_datetime((self.now)());
        let now_trunc = duration_trunc(now_dt, interval).unwrap_or_else(|| {
            soft_panic_or_log!("excessive wallclock lag recording interval: {interval:?}");
            let default = WALLCLOCK_LAG_RECORDING_INTERVAL.default();
            duration_trunc(now_dt, *default).unwrap()
        });
        if now_trunc <= self.wallclock_lag_last_recorded {
            return;
        }

        let now_ts: CheckedTimestamp<_> = now_trunc.try_into().expect("must fit");

        let mut history_updates = Vec::new();
        let mut histogram_updates = Vec::new();
        let mut row_buf = Row::default();
        for frontiers in self.storage_collections.active_collection_frontiers() {
            let id = frontiers.id;
            let Some(collection) = self.collections.get_mut(&id) else {
                continue;
            };

            let max_lag = std::mem::replace(&mut collection.wallclock_lag_max, WallclockLag::MIN);
            let row = Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::Null,
                max_lag.into_interval_datum(),
                Datum::TimestampTz(now_ts),
            ]);
            history_updates.push((row, Diff::ONE));

            let Some(stash) = &mut collection.wallclock_lag_histogram_stash else {
                continue;
            };

            for ((period, lag, labels), count) in std::mem::take(stash) {
                let mut packer = row_buf.packer();
                packer.extend([
                    Datum::TimestampTz(period.start),
                    Datum::TimestampTz(period.end),
                    Datum::String(&id.to_string()),
                    lag.into_uint64_datum(),
                ]);
                let labels = labels.iter().map(|(k, v)| (*k, Datum::String(v)));
                packer.push_dict(labels);

                histogram_updates.push((row_buf.clone(), count));
            }
        }

        if !history_updates.is_empty() {
            self.append_introspection_updates(
                IntrospectionType::WallclockLagHistory,
                history_updates,
            );
        }
        if !histogram_updates.is_empty() {
            self.append_introspection_updates(
                IntrospectionType::WallclockLagHistogram,
                histogram_updates,
            );
        }

        self.wallclock_lag_last_recorded = now_trunc;
    }

    /// Run periodic tasks.
    ///
    /// This method is invoked roughly once per second during normal operation. It is a good place
    /// for tasks that need to run periodically, such as state cleanup or updating of metrics.
    fn maintain(&mut self) {
        self.update_frontier_introspection();
        self.refresh_wallclock_lag();

        // Perform instance maintenance work.
        for instance in self.instances.values_mut() {
            instance.refresh_state_metrics();
        }
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
            | IntrospectionType::WallclockLagHistogram
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
) -> Vec<Row>
where
    T: Codec64 + From<EpochMillis> + TimestampManipulation,
{
    match upper.as_option() {
        Some(f) if f > &T::minimum() => {
            let as_of = f.step_back().unwrap();

            let snapshot = storage_collections.snapshot(id, as_of).await.unwrap();
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

async fn read_handle_for_snapshot<T>(
    persist: &PersistClientCache,
    id: GlobalId,
    metadata: &CollectionMetadata,
) -> Result<ReadHandle<SourceData, (), T, StorageDiff>, StorageError<T>>
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
    pub data_source: DataSource<T>,

    pub collection_metadata: CollectionMetadata,

    pub extra_state: CollectionStateExtra<T>,

    /// Maximum frontier wallclock lag since the last `WallclockLagHistory` introspection update.
    wallclock_lag_max: WallclockLag,
    /// Frontier wallclock lag measurements stashed until the next `WallclockLagHistogram`
    /// introspection update.
    ///
    /// Keys are `(period, lag, labels)` triples, values are counts.
    ///
    /// If this is `None`, wallclock lag is not tracked for this collection.
    wallclock_lag_histogram_stash: Option<
        BTreeMap<
            (
                WallclockLagHistogramPeriod,
                WallclockLag,
                BTreeMap<&'static str, String>,
            ),
            Diff,
        >,
    >,
    /// Frontier wallclock lag metrics tracked for this collection.
    wallclock_lag_metrics: WallclockLagMetrics,
}

impl<T: TimelyTimestamp> CollectionState<T> {
    fn new(
        data_source: DataSource<T>,
        collection_metadata: CollectionMetadata,
        extra_state: CollectionStateExtra<T>,
        wallclock_lag_metrics: WallclockLagMetrics,
    ) -> Self {
        // Only collect wallclock lag histogram data for collections written by storage, to avoid
        // duplicate measurements. Collections written by other components (e.g. compute) have
        // their wallclock lags recorded by these components.
        let wallclock_lag_histogram_stash = match &data_source {
            DataSource::Other => None,
            _ => Some(Default::default()),
        };

        Self {
            data_source,
            collection_metadata,
            extra_state,
            wallclock_lag_max: WallclockLag::MIN,
            wallclock_lag_histogram_stash,
            wallclock_lag_metrics,
        }
    }
}

/// Additional state that the controller maintains for select collection types.
#[derive(Debug)]
enum CollectionStateExtra<T: TimelyTimestamp> {
    Ingestion(IngestionState<T>),
    Export(ExportState<T>),
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

    /// Set of replica IDs on which this ingestion is hydrated.
    pub hydrated_on: BTreeSet<ReplicaId>,
}

/// A description of a status history collection.
///
/// Used to inform partial truncation, see
/// [`collection_mgmt::partially_truncate_status_history`].
struct StatusHistoryDesc<K> {
    retention_policy: StatusHistoryRetentionPolicy,
    extract_key: Box<dyn Fn(&[Datum]) -> K + Send>,
    extract_time: Box<dyn Fn(&[Datum]) -> CheckedTimestamp<DateTime<Utc>> + Send>,
}
enum StatusHistoryRetentionPolicy {
    // Truncates everything but the last N updates for each key.
    LastN(usize),
    // Truncates everything past the time window for each key.
    TimeWindow(Duration),
}

fn source_status_history_desc(
    params: &StorageParameters,
) -> StatusHistoryDesc<(GlobalId, Option<ReplicaId>)> {
    let desc = &MZ_SOURCE_STATUS_HISTORY_DESC;
    let (source_id_idx, _) = desc.get_by_name(&"source_id".into()).expect("exists");
    let (replica_id_idx, _) = desc.get_by_name(&"replica_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    StatusHistoryDesc {
        retention_policy: StatusHistoryRetentionPolicy::LastN(
            params.keep_n_source_status_history_entries,
        ),
        extract_key: Box::new(move |datums| {
            (
                GlobalId::from_str(datums[source_id_idx].unwrap_str()).expect("GlobalId column"),
                if datums[replica_id_idx].is_null() {
                    None
                } else {
                    Some(
                        ReplicaId::from_str(datums[replica_id_idx].unwrap_str())
                            .expect("ReplicaId column"),
                    )
                },
            )
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

fn sink_status_history_desc(
    params: &StorageParameters,
) -> StatusHistoryDesc<(GlobalId, Option<ReplicaId>)> {
    let desc = &MZ_SINK_STATUS_HISTORY_DESC;
    let (sink_id_idx, _) = desc.get_by_name(&"sink_id".into()).expect("exists");
    let (replica_id_idx, _) = desc.get_by_name(&"replica_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    StatusHistoryDesc {
        retention_policy: StatusHistoryRetentionPolicy::LastN(
            params.keep_n_sink_status_history_entries,
        ),
        extract_key: Box::new(move |datums| {
            (
                GlobalId::from_str(datums[sink_id_idx].unwrap_str()).expect("GlobalId column"),
                if datums[replica_id_idx].is_null() {
                    None
                } else {
                    Some(
                        ReplicaId::from_str(datums[replica_id_idx].unwrap_str())
                            .expect("ReplicaId column"),
                    )
                },
            )
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

fn privatelink_status_history_desc(params: &StorageParameters) -> StatusHistoryDesc<GlobalId> {
    let desc = &MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC;
    let (key_idx, _) = desc.get_by_name(&"connection_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    StatusHistoryDesc {
        retention_policy: StatusHistoryRetentionPolicy::LastN(
            params.keep_n_privatelink_status_history_entries,
        ),
        extract_key: Box::new(move |datums| {
            GlobalId::from_str(datums[key_idx].unwrap_str()).expect("GlobalId column")
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

fn replica_status_history_desc(params: &StorageParameters) -> StatusHistoryDesc<(GlobalId, u64)> {
    let desc = &REPLICA_STATUS_HISTORY_DESC;
    let (replica_idx, _) = desc.get_by_name(&"replica_id".into()).expect("exists");
    let (process_idx, _) = desc.get_by_name(&"process_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    StatusHistoryDesc {
        retention_policy: StatusHistoryRetentionPolicy::TimeWindow(
            params.replica_status_history_retention_window,
        ),
        extract_key: Box::new(move |datums| {
            (
                GlobalId::from_str(datums[replica_idx].unwrap_str()).expect("GlobalId column"),
                datums[process_idx].unwrap_uint64(),
            )
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

/// Replace one antichain with another, tracking the overall changes in the returned `ChangeBatch`.
fn swap_updates<T: Timestamp>(
    from: &mut Antichain<T>,
    mut replace_with: Antichain<T>,
) -> ChangeBatch<T> {
    let mut update = ChangeBatch::new();
    if PartialOrder::less_equal(from, &replace_with) {
        update.extend(replace_with.iter().map(|time| (time.clone(), 1)));
        std::mem::swap(from, &mut replace_with);
        update.extend(replace_with.iter().map(|time| (time.clone(), -1)));
    }
    update
}
