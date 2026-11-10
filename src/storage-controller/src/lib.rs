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
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::collection_mgmt::{
    AppendOnlyIntrospectionConfig, CollectionManagerKind, DifferentialIntrospectionConfig,
};
use crate::instance::Instance;
use async_trait::async_trait;
use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use derivative::Derivative;
use futures::FutureExt;
use futures::StreamExt;
use itertools::Itertools;
use mz_cluster_client::metrics::{ControllerMetrics, WallclockLagMetrics};
use mz_cluster_client::{ReplicaId, WallclockLagFn};
use mz_controller_types::dyncfgs::WALLCLOCK_LAG_RECORDING_INTERVAL;
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::task::AbortOnDropHandle;
use mz_ore::{assert_none, instrument, soft_panic_or_log};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::schema::CaESchema;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, RelationVersion, Row, Timestamp};
use mz_storage_client::client::{AppendOnlyUpdate, Status, StatusUpdate, StorageResponse};
use mz_storage_client::controller::{
    BoxFuture, CollectionDescription, DataSource, ExportDescription, IntrospectionType,
    StorageController, StorageMetadata, StorageTxn, StorageWriteOp, WallclockLag,
    WallclockLagHistogramPeriod,
};
use mz_storage_client::healthcheck::{
    MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC, MZ_SINK_STATUS_HISTORY_DESC,
    MZ_SOURCE_STATUS_HISTORY_DESC, REPLICA_STATUS_HISTORY_DESC,
};
use mz_storage_client::metrics::StorageControllerMetrics;
use mz_storage_client::statistics::{ControllerSinkStatistics, ControllerSourceStatistics};
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::connections::inline::InlinedConnection;
use mz_storage_types::controller::{CollectionMetadata, StorageError, TxnsCodecRow};
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::sinks::StorageSinkConnection;
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, SourceConnection, SourceData, SourceDesc,
    SourceExport, SourceExportDataConfig,
};
use mz_storage_types::{AlterCompatible, StorageDiff, dyncfgs};
use mz_txn_wal::metrics::Metrics as TxnMetrics;
use mz_txn_wal::txn_read::TxnsRead;
use timely::order::PartialOrder;
use timely::progress::Antichain;
use tokio::sync::watch::{Sender, channel};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tokio::time::error::Elapsed;
use tracing::{debug, info, warn};

pub mod adapter_storage;
mod collection_mgmt;
mod frontier_observations;
mod instance;
pub mod rtr;
mod statistics;

/// A storage controller for a storage instance.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Controller {
    /// A function that returns the current time.
    now: NowFn,

    /// Whether or not this controller is in read-only mode.
    ///
    /// Restricts adapter-owned writes without changing replica execution authority.
    read_only: bool,

    /// Committed collection inventory and passive observations.
    pub(crate) collections: BTreeMap<GlobalId, CollectionState>,

    /// A shared TxnsCache running in a task and communicated with over a channel.
    txns_read: TxnsRead<Timestamp>,
    txns_metrics: Arc<TxnMetrics>,
    stashed_responses: Vec<(Option<ReplicaId>, StorageResponse)>,

    /// Interface for managed collections
    pub(crate) collection_manager: collection_mgmt::CollectionManager,

    /// Tracks which collection is responsible for which [`IntrospectionType`].
    pub(crate) introspection_ids: BTreeMap<IntrospectionType, GlobalId>,
    storage_frontier_observations: Option<frontier_observations::StorageFrontierObservations>,
    collection_metadata_certifier:
        Option<Arc<dyn mz_storage_client::controller::CollectionMetadataCertifier>>,
    /// Tokens for tasks that drive updating introspection collections. Dropping
    /// this will make sure that any tasks (or other resources) will stop when
    /// needed.
    // TODO(aljoscha): Should these live somewhere else?
    introspection_tokens: Arc<Mutex<BTreeMap<GlobalId, Box<dyn Any + Send + Sync>>>>,

    // The following two fields must always be locked in order.
    /// Consolidated metrics updates to periodically write. We do not eagerly initialize this,
    /// and its contents are entirely driven by `StorageResponse::StatisticsUpdates`.
    source_statistics:
        Arc<Mutex<BTreeMap<(GlobalId, Option<ReplicaId>), ControllerSourceStatistics>>>,
    /// Consolidated metrics updates to periodically write. We do not eagerly initialize this,
    /// and its contents are entirely driven by `StorageResponse::StatisticsUpdates`'s.
    sink_statistics: Arc<Mutex<BTreeMap<(GlobalId, Option<ReplicaId>), ControllerSinkStatistics>>>,
    /// A way to update the statistics interval in the statistics tasks.
    statistics_interval_sender: Sender<Duration>,

    /// Membership and workload class for known storage instances.
    instances: BTreeMap<StorageInstanceId, Instance>,
    /// Configuration for local collection management and purification.
    config: StorageConfiguration,
    /// The persist location where all storage collections are being written to
    persist_location: PersistLocation,
    /// A persist client used to write to storage collections
    persist: Arc<PersistClientCache>,
    /// Metrics of the Storage controller
    metrics: StorageControllerMetrics,

    /// A function that computes the lag between the given time and wallclock time.
    #[derivative(Debug = "ignore")]
    wallclock_lag: WallclockLagFn<Timestamp>,
    /// The last time wallclock lag introspection was recorded.
    wallclock_lag_last_recorded: DateTime<Utc>,

    /// Handle to a [StorageCollections].
    storage_collections: Arc<dyn StorageCollections + Send + Sync>,
    /// Migrated storage collections that can be written even in read only mode.
    migrated_storage_collections: BTreeSet<GlobalId>,

    /// Ticker for scheduling periodic maintenance work.
    maintenance_ticker: tokio::time::Interval,
    /// Whether maintenance work was scheduled.
    maintenance_scheduled: bool,

    /// Shared transmit channel for replicas to send responses.
    instance_response_tx: mpsc::UnboundedSender<(Option<ReplicaId>, StorageResponse)>,
    /// Receive end for replica responses.
    instance_response_rx: mpsc::UnboundedReceiver<(Option<ReplicaId>, StorageResponse)>,

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
impl StorageController for Controller {
    fn set_collection_metadata_certifier(
        &mut self,
        certifier: Arc<dyn mz_storage_client::controller::CollectionMetadataCertifier>,
    ) {
        assert!(self.storage_frontier_observations.is_none());
        self.collection_metadata_certifier = Some(certifier);
    }

    fn initialization_complete(&mut self) {
        self.reconcile_dangling_statistics();
    }

    fn update_parameters(&mut self, config_params: StorageParameters) {
        self.storage_collections
            .update_parameters(config_params.clone());

        // We serialize the dyncfg updates in StorageParameters, but configure
        // persist separately.
        self.persist.cfg().apply_from(&config_params.dyncfg_updates);

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

    fn collection_hydrated(&self, collection_id: GlobalId) -> Result<bool, StorageError> {
        let collection = self.collection(collection_id)?;

        match &collection.data_source {
            DataSource::Ingestion(_) | DataSource::IngestionExport { .. } => (),
            _ => return Ok(true),
        };
        let instance_id = collection
            .extra_state
            .instance_id()
            .expect("source cluster");

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

        let scheduled = self.source_replicas(collection_id);
        Ok(collection
            .extra_state
            .hydrated_on()
            .is_some_and(|hydrated| !hydrated.is_disjoint(&scheduled)))
    }

    #[mz_ore::instrument(level = "debug")]
    fn collections_hydrated_on_replicas(
        &self,
        target_replica_ids: Option<Vec<ReplicaId>>,
        target_cluster_id: &StorageInstanceId,
        exclude_collections: &BTreeSet<GlobalId>,
    ) -> Result<bool, StorageError> {
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
        for (collection_id, collection_state) in &self.collections {
            if collection_id.is_transient()
                || exclude_collections.contains(collection_id)
                || collection_state.extra_state.instance_id().as_ref() != Some(target_cluster_id)
                || !matches!(
                    collection_state.data_source,
                    DataSource::Ingestion(_) | DataSource::IngestionExport { .. }
                )
            {
                continue;
            }
            let scheduled = self.source_replicas(*collection_id);
            let hydrated_on = collection_state
                .extra_state
                .hydrated_on()
                .expect("source hydration state");
            let hydrated = {
                let targets: BTreeSet<_> = scheduled
                    .into_iter()
                    .filter(|id| {
                        target_replicas
                            .as_ref()
                            .is_none_or(|targets| targets.contains(id))
                    })
                    .collect();
                targets.is_empty() || !hydrated_on.is_disjoint(&targets)
            };
            if !hydrated {
                tracing::info!(%collection_id, "collection is not hydrated on any replica");
                all_hydrated = false;
            }
        }
        Ok(all_hydrated)
    }

    fn collection_frontiers(
        &self,
        id: GlobalId,
    ) -> Result<(Antichain<Timestamp>, Antichain<Timestamp>), CollectionMissing> {
        let frontiers = self.storage_collections.collection_frontiers(id)?;
        Ok((frontiers.implied_capability, frontiers.write_frontier))
    }

    fn collections_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Result<Vec<(GlobalId, Antichain<Timestamp>, Antichain<Timestamp>)>, CollectionMissing>
    {
        Ok(self
            .storage_collections
            .collections_frontiers(ids)?
            .into_iter()
            .map(|frontiers| {
                (
                    frontiers.id,
                    frontiers.implied_capability,
                    frontiers.write_frontier,
                )
            })
            .collect())
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

        let exports: Box<dyn Iterator<Item = &GlobalId> + '_> = {
            Box::new(self.collections.iter().filter_map(move |(id, collection)| {
                (collection.extra_state.instance_id() == Some(instance_id)
                    && matches!(
                        collection.data_source,
                        DataSource::Ingestion(_) | DataSource::IngestionExport { .. }
                    )
                    && !self.source_replicas(*id).is_empty())
                .then_some(id)
            }))
        };
        Box::new(exports.filter(move |id| {
            active_storage_collections
                .get(id)
                .is_some_and(|frontiers| !frontiers.write_frontier.is_empty())
        }))
    }

    fn check_exists(&self, id: GlobalId) -> Result<(), StorageError> {
        self.storage_collections.check_exists(id)
    }

    fn create_instance(&mut self, id: StorageInstanceId, workload_class: Option<String>) {
        let instance = Instance::new(workload_class);

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

    fn register_replica(&mut self, instance_id: StorageInstanceId, replica_id: ReplicaId) {
        self.instances
            .get_mut(&instance_id)
            .expect("instance exists")
            .register_replica(replica_id);
    }

    fn replica_observations(&self) -> mz_storage_client::controller::ReplicaObservationSender {
        mz_storage_client::controller::ReplicaObservationSender::new(
            self.instance_response_tx.clone(),
        )
    }

    fn drop_replica(
        &mut self,
        instance_id: StorageInstanceId,
        replica_id: ReplicaId,
        publish_status_history: bool,
    ) {
        let (ingestions, exports) = {
            let mut ingestions = Vec::new();
            let mut exports = Vec::new();
            for (id, collection) in &self.collections {
                if collection.extra_state.instance_id() != Some(instance_id) {
                    continue;
                }
                match collection.data_source {
                    DataSource::Ingestion(_) => ingestions.push(*id),
                    DataSource::Sink { .. } => exports.push(*id),
                    _ => (),
                }
            }
            (ingestions, exports)
        };
        for collection in self.collections.values_mut() {
            if collection.extra_state.instance_id() == Some(instance_id) {
                if let Some(hydrated) = collection.extra_state.hydrated_on_mut() {
                    hydrated.remove(&replica_id);
                }
            }
        }
        let instance = self
            .instances
            .get_mut(&instance_id)
            .unwrap_or_else(|| panic!("instance {instance_id} does not exist"));

        let status_now = mz_ore::now::to_datetime((self.now)());
        let mut source_status_updates = vec![];
        let mut sink_status_updates = vec![];

        // NOTE: mz_source_statuses and mz_sink_statuses rely on per-replica `paused` meaning
        // "replica dropped"
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

        for ingestion_id in &ingestions {
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

        for id in &exports {
            sink_status_updates.push(make_update(*id, "sink"));
        }

        instance.drop_replica(replica_id);
        if publish_status_history {
            let ids = self
                .collections
                .iter()
                .filter_map(|(id, c)| {
                    (c.extra_state.instance_id() == Some(instance_id)).then_some(*id)
                })
                .collect::<Vec<_>>();
            self.queue_native_paused_statuses(ids);
        }

        if publish_status_history && !self.read_only {
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
    ) -> Result<(), StorageError> {
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
                .latest_schema::<SourceData, (), Timestamp, StorageDiff>(shard_id, diagnostics)
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
                .compare_and_evolve_schema::<SourceData, (), Timestamp, StorageDiff>(
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
    /// Registers shared collection metadata, local observation state and writers
    /// for adapter-owned introspection. Replicas own maintained execution.
    #[instrument(name = "storage::create_collections")]
    async fn create_collections_for_bootstrap(
        &mut self,
        storage_metadata: &StorageMetadata,
        register_ts: Option<Timestamp>,
        mut collections: Vec<(GlobalId, CollectionDescription)>,
        migrated_storage_collections: &BTreeSet<GlobalId>,
        publish_status_history: bool,
    ) -> Result<(), StorageError> {
        self.migrated_storage_collections
            .extend(migrated_storage_collections.iter().cloned());

        self.storage_collections
            .create_collections_for_bootstrap(
                storage_metadata,
                register_ts,
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
                let data_shard = storage_metadata.get_collection_shard(id)?;

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
            .map(|data: Result<_, StorageError>| {
                async move {
                    let (id, description, metadata) = data?;

                    // should be replaced with real introspection (https://github.com/MaterializeInc/database-issues/issues/4078)
                    // but for now, it's helpful to have this mapping written down somewhere
                    debug!("mapping GlobalId={} to shard ({})", id, metadata.data_shard);

                    // Execution writers belong to replicas and the table worker.
                    // StorageCollections already established the shared shard metadata.
                    let write = if matches!(description.data_source,
                        DataSource::Introspection(typ) if !typ.is_statement_history())
                    {
                        Some(
                            this.open_data_handles(
                                &id,
                                metadata.data_shard,
                                metadata.relation_desc.clone(),
                                persist_client,
                            )
                            .await,
                        )
                    } else {
                        None
                    };

                    Ok::<_, StorageError>((id, description, write, metadata))
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

        // New inventory entries whose mappings and synthetic history are published below.
        let mut new_collections = BTreeSet::new();

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

        for (id, description, write, metadata) in to_register {
            new_collections.insert(id);
            let data_source = description.data_source;

            // Perform data source-specific setup.
            let mut extra_state = CollectionStateExtra::None;
            let mut maybe_instance_id = None;
            match &data_source {
                DataSource::Introspection(typ) if !typ.is_statement_history() => {
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
                        write.expect("introspection writer"),
                        persist_client.clone(),
                    )?;
                }
                DataSource::Webhook | DataSource::Introspection(_) => {}
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

                    extra_state = CollectionStateExtra::Native {
                        instance_id,
                        hydrated_on: BTreeSet::new(),
                    };
                    maybe_instance_id = Some(instance_id);
                }
                DataSource::Table => {
                    debug!(
                        ?data_source, meta = ?metadata,
                        "not registering {id} with the txns shard here; the caller does that \
                         through the group committer",
                    );
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

                    extra_state = CollectionStateExtra::Native {
                        instance_id: ingestion_desc.instance_id,
                        hydrated_on: BTreeSet::new(),
                    };
                    maybe_instance_id = Some(ingestion_desc.instance_id);
                }
                DataSource::Sink { desc } => {
                    maybe_instance_id = Some(desc.instance_id);
                    extra_state = CollectionStateExtra::Native {
                        instance_id: desc.instance_id,
                        hydrated_on: BTreeSet::new(),
                    };
                }
            }

            let wallclock_lag_metrics = self.metrics.wallclock_lag_metrics(id, maybe_instance_id);
            let collection_state =
                CollectionState::new(data_source, metadata, extra_state, wallclock_lag_metrics);

            if let Some(sampler) = &self.storage_frontier_observations {
                sampler.upsert(id, collection_state.collection_metadata.data_shard);
            }
            self.collections.insert(id, collection_state);
        }

        if publish_status_history {
            self.queue_native_paused_statuses(new_collections.iter().copied());
        }
        self.append_shard_mappings(new_collections.into_iter(), Diff::ONE);

        Ok(())
    }

    async fn alter_ingestion_source_desc(
        &mut self,
        ingestion_ids: BTreeMap<GlobalId, SourceDesc>,
    ) -> Result<(), StorageError> {
        for (id, new_desc) in ingestion_ids {
            let collection = self
                .collections
                .get_mut(&id)
                .ok_or_else(|| StorageError::IdentifierMissing(id))?;

            match &mut collection.data_source {
                DataSource::Ingestion(ingestion) => {
                    if ingestion.desc != new_desc {
                        tracing::info!(
                            from = ?ingestion.desc,
                            to = ?new_desc,
                            "alter_ingestion_source_desc, updating"
                        );
                        ingestion.desc = new_desc;
                    }
                }
                o => {
                    tracing::warn!("alter_ingestion_source_desc called on {:?}", o);
                    Err(StorageError::IdentifierInvalid(id))?;
                }
            }
        }

        Ok(())
    }

    async fn alter_ingestion_connections(
        &mut self,
        source_connections: BTreeMap<GlobalId, GenericSourceConnection<InlinedConnection>>,
    ) -> Result<(), StorageError> {
        for (id, conn) in source_connections {
            let collection = self
                .collections
                .get_mut(&id)
                .ok_or_else(|| StorageError::IdentifierMissing(id))?;

            match &mut collection.data_source {
                DataSource::Ingestion(ingestion) => {
                    // Keep the local committed description current.
                    if ingestion.desc.connection != conn {
                        tracing::info!(from = ?ingestion.desc.connection, to = ?conn, "alter_ingestion_connections, updating");
                        ingestion.desc.connection = conn;
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

        Ok(())
    }

    async fn alter_ingestion_export_data_configs(
        &mut self,
        source_exports: BTreeMap<GlobalId, SourceExportDataConfig>,
    ) -> Result<(), StorageError> {
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

                    // Keep the ingestion's export metadata current too.
                    if source_export.data_config != new_data_config {
                        tracing::info!(?source_export_id, from = ?source_export.data_config, to = ?new_data_config, "alter_ingestion_export_data_configs, updating");
                        source_export.data_config = new_data_config;
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

        Ok(())
    }

    async fn alter_table_desc(
        &mut self,
        storage_metadata: &StorageMetadata,
        existing_collection: GlobalId,
        new_collection: GlobalId,
        new_desc: RelationDesc,
        expected_version: RelationVersion,
    ) -> Result<(), StorageError> {
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
                    storage_metadata,
                    existing_collection,
                    new_collection,
                    new_desc.clone(),
                    expected_version,
                )
                .await?;

            existing.collection_metadata.data_shard.clone()
        };

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
        if let Some(sampler) = &self.storage_frontier_observations {
            sampler.upsert(
                new_collection,
                collection_state.collection_metadata.data_shard,
            );
        }
        self.collections.insert(new_collection, collection_state);

        self.append_shard_mappings([new_collection].into_iter(), Diff::ONE);

        Ok(())
    }

    async fn alter_export(
        &mut self,
        id: GlobalId,
        new_description: ExportDescription,
        publish_status_history: bool,
    ) -> Result<(), StorageError> {
        let collection = self
            .collections
            .get_mut(&id)
            .ok_or(StorageError::IdentifierMissing(id))?;
        let DataSource::Sink { desc } = &mut collection.data_source else {
            return Err(StorageError::IdentifierInvalid(id));
        };
        let CollectionStateExtra::Native { instance_id, .. } = &mut collection.extra_state else {
            unreachable!("native sink has observation state");
        };
        *instance_id = new_description.instance_id;
        *desc = new_description;
        if publish_status_history {
            self.queue_native_paused_statuses([id]);
        }
        Ok(())
    }

    /// Create the sinks described by the `ExportDescription`.
    async fn alter_export_connections(
        &mut self,
        exports: BTreeMap<GlobalId, StorageSinkConnection>,
    ) -> Result<(), StorageError> {
        let mut descriptions = Vec::new();
        for (id, connection) in exports {
            let DataSource::Sink { desc } = &self.collection(id)?.data_source else {
                return Err(StorageError::IdentifierInvalid(id));
            };
            let mut updated = desc.clone();
            updated.sink.connection = connection;
            desc.sink.alter_compatible(id, &updated.sink)?;
            descriptions.push((id, updated));
        }
        for (id, desc) in descriptions {
            self.collections
                .get_mut(&id)
                .expect("validated")
                .data_source = DataSource::Sink { desc };
        }
        Ok(())
    }

    fn drop_tables(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
        publish_status_history: bool,
    ) -> Result<(), StorageError> {
        for id in &identifiers {
            match self.collection(*id)?.data_source {
                DataSource::Table | DataSource::IngestionExport { .. } | DataSource::Webhook => {}
                _ => panic!("identifier is not a table: {}", id),
            }
        }

        // Physical finalization separately requires shared txn-WAL retirement. Read
        // accounting may already be retired, but execution cleanup is ours.
        if !identifiers.is_empty() {
            self.drop_sources_unvalidated(storage_metadata, identifiers, publish_status_history)?;
        }

        Ok(())
    }

    fn drop_sources(
        &mut self,
        storage_metadata: &StorageMetadata,
        identifiers: Vec<GlobalId>,
        publish_status_history: bool,
    ) -> Result<(), StorageError> {
        self.validate_collection_ids(identifiers.iter().cloned())?;
        self.drop_sources_unvalidated(storage_metadata, identifiers, publish_status_history)
    }

    fn drop_sources_unvalidated(
        &mut self,
        storage_metadata: &StorageMetadata,
        ids: Vec<GlobalId>,
        publish_status_history: bool,
    ) -> Result<(), StorageError> {
        // Keep track of which ingestions we have to execute still, and which we
        // have to change because of dropped subsources.
        let mut ingestions_to_drop = BTreeSet::new();
        let mut source_statistics_to_drop = Vec::new();

        // Ingestions (and their exports) are also collections, but we keep
        // track of non-ingestion collections separately, because they have
        // slightly different cleanup logic below.
        let mut collections_to_drop = Vec::new();

        for id in ids.iter() {
            let collection_state = self.collections.get(id);

            if let Some(collection_state) = collection_state {
                match collection_state.data_source {
                    DataSource::Webhook => {
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

        if publish_status_history && !self.read_only {
            self.append_status_introspection_updates(
                IntrospectionType::SourceStatusHistory,
                status_updates,
            );
        }

        {
            let mut source_statistics = self.source_statistics.lock().expect("poisoned");
            for id in source_statistics_to_drop {
                source_statistics.retain(|(stats_id, _), _| stats_id != &id);
            }
        }

        // Remove collection state
        for id in ingestions_to_drop.iter().chain(collections_to_drop.iter()) {
            tracing::info!(%id, "dropping collection state");
            if let Some(sampler) = &self.storage_frontier_observations {
                sampler.remove(*id);
            }
            self.collections
                .remove(id)
                .expect("list populated after checking that self.collections contains it");
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
        publish_status_history: bool,
    ) -> Result<(), StorageError> {
        self.validate_export_ids(identifiers.iter().cloned())?;
        self.drop_sinks_unvalidated(storage_metadata, identifiers, publish_status_history);
        Ok(())
    }

    fn drop_sinks_unvalidated(
        &mut self,
        storage_metadata: &StorageMetadata,
        mut sinks_to_drop: Vec<GlobalId>,
        publish_status_history: bool,
    ) {
        // Ignore exports that have already been removed.
        sinks_to_drop.retain(|id| self.is_sink(*id));
        self.append_shard_mappings(sinks_to_drop.iter().copied(), Diff::MINUS_ONE);

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

        if publish_status_history && !self.read_only {
            self.append_status_introspection_updates(
                IntrospectionType::SinkStatusHistory,
                status_updates,
            );
        }

        // Remove collection/export state
        for id in sinks_to_drop.iter() {
            tracing::info!(%id, "dropping export state");
            if let Some(sampler) = &self.storage_frontier_observations {
                sampler.remove(*id);
            }
            self.collections
                .remove(id)
                .expect("list populated after checking that self.collections contains it");
        }

        // Also let StorageCollections know!
        self.storage_collections
            .drop_collections_unvalidated(storage_metadata, sinks_to_drop);
    }

    async fn ready(&mut self) {
        if self.maintenance_scheduled || !self.stashed_responses.is_empty() {
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
    fn process(&mut self) -> Result<(), anyhow::Error> {
        // Perform periodic maintenance work.
        if self.maintenance_scheduled {
            self.maintain();
            self.maintenance_scheduled = false;
        }

        let mut status_updates = vec![];

        // Take the currently stashed responses so that we can call mut receiver functions in the loop.
        let stashed_responses = std::mem::take(&mut self.stashed_responses);
        for resp in stashed_responses {
            match resp {
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

                            if self.collection(collection_id).is_err()
                                || !self.accepts_observation(collection_id, Some(replica_id))
                            {
                                // We can get updates for collections that have
                                // already been deleted, ignore those.
                                continue;
                            }

                            let entry = shared_stats.entry((stat.id, Some(replica_id)));

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

                            if self.collection(collection_id).is_err()
                                || !self.accepts_observation(collection_id, Some(replica_id))
                            {
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
                    let observation_replica = replica_id.or(status_update.replica_id);
                    // Native observations can race a committed drop or replica removal.
                    // Ignore those rather than resurrecting status or hydration metadata.
                    if !self.accepts_observation(status_update.id, observation_replica) {
                        continue;
                    }
                    if let Some(collection) = self.collections.get_mut(&status_update.id) {
                        if let Some(hydrated) = collection.extra_state.hydrated_on_mut() {
                            // A health stall does not undo hydration. A new
                            // attempt's Starting status must establish it anew.
                            if status_update.status == Status::Running {
                                hydrated.insert(
                                    observation_replica
                                        .expect("replica id should be present for status running"),
                                );
                            } else if status_update.status != Status::Stalled {
                                if let Some(replica) = observation_replica {
                                    hydrated.remove(&replica);
                                } else {
                                    hydrated.clear();
                                }
                            }
                        }
                    }

                    // Set replica_id in the status update if available
                    if let Some(id) = replica_id {
                        status_update.replica_id = Some(id);
                    }
                    // Native replicas publish original transitions themselves.
                    // Observation snapshots establish hydration, not history.
                    // Shared zero-replica events have no producer and remain here.
                    if observation_replica.is_none() {
                        status_updates.push(status_update);
                    }
                }
                (Some(replica), StorageResponse::QueryReady) => {
                    // A connection's fresh snapshot, not a cached status from
                    // its predecessor, must establish hydration after recovery.
                    for collection in self.collections.values_mut() {
                        if let Some(hydrated) = collection.extra_state.hydrated_on_mut() {
                            hydrated.remove(&replica);
                        }
                    }
                }
                (_, StorageResponse::QueryReady) => {
                    panic!("query response on lifecycle connection")
                }
                (_, StorageResponse::CatalogCatchup(_)) => {
                    panic!("replica catalog catchup on controller connection")
                }
                // Execution progress belongs to the replica and COPY results
                // to request-owned query connections, not observation streams.
                (
                    _,
                    StorageResponse::StagedBatches(_)
                    | StorageResponse::FrontierUpper(..)
                    | StorageResponse::DroppedId(_),
                ) => {}
            }
        }

        self.record_status_updates(status_updates);

        Ok(())
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
        oneshot::Sender<Result<(), StorageError>>,
    )> {
        let id = self.introspection_ids[&type_];
        self.collection_manager.append_only_write_sender(id)
    }

    fn differential_introspection_tx(
        &self,
        type_: IntrospectionType,
    ) -> mpsc::UnboundedSender<(StorageWriteOp, oneshot::Sender<Result<(), StorageError>>)> {
        let id = self.introspection_ids[&type_];
        self.collection_manager
            .differential_write_sender(id)
            .expect("introspection collection is registered")
    }

    async fn real_time_recent_timestamp(
        &self,
        timestamp_objects: BTreeSet<GlobalId>,
        timeout: Duration,
    ) -> Result<BoxFuture<Result<Timestamp, StorageError>>, StorageError> {
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

                    let result = rtr::real_time_recency_ts(
                        source_conn,
                        id,
                        config,
                        as_of,
                        remap_subscribe,
                    )
                    .await
                    .map_err(|e| {
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
                .try_fold(Timestamp::MIN, |curr, (id, per_source_res)| {
                    let new =
                        per_source_res.map_err(|_e: Elapsed| StorageError::RtrTimeout(id))??;
                    Ok::<_, StorageError>(std::cmp::max(curr, new))
                })
        }))
    }

    fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            now: _,
            storage_frontier_observations: _,
            collection_metadata_certifier: _,
            read_only,
            collections,
            txns_read: _,
            txns_metrics: _,
            stashed_responses,
            collection_manager: _,
            introspection_ids,
            introspection_tokens: _,
            source_statistics: _,
            sink_statistics: _,
            statistics_interval_sender: _,
            instances,
            config,
            persist_location,
            persist: _,
            metrics: _,
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
        let stashed_responses: Vec<_> =
            stashed_responses.iter().map(|r| format!("{r:?}")).collect();
        let introspection_ids: BTreeMap<_, _> = introspection_ids
            .iter()
            .map(|(typ, id)| (format!("{typ:?}"), id.to_string()))
            .collect();
        let instances: BTreeMap<_, _> = instances
            .iter()
            .map(|(id, i)| (id.to_string(), format!("{i:?}")))
            .collect();
        let migrated_storage_collections: Vec<_> = migrated_storage_collections
            .iter()
            .map(|id| id.to_string())
            .collect();

        Ok(serde_json::json!({
            "read_only": read_only,
            "collections": collections,
            "stashed_responses": stashed_responses,
            "introspection_ids": introspection_ids,
            "instances": instances,
            "config": format!("{config:?}"),
            "persist_location": format!("{persist_location:?}"),
            "wallclock_lag_last_recorded": format!("{wallclock_lag_last_recorded:?}"),
            "migrated_storage_collections": migrated_storage_collections,
            "maintenance_scheduled": maintenance_scheduled,
        }))
    }
}

impl Controller
where
    Self: StorageController,
{
    /// Creates passive storage inventory and adapter-owned collection writers.
    ///
    /// The caller must supply a protected native deployment. `deployment` qualifies
    /// persisted observations. Read-only mode restricts adapter-owned writes, not
    /// replica execution, which follows durable catalog authority.
    ///
    /// # Panics
    /// If `txn` is missing the durably initialized transaction WAL identity.
    pub async fn new(
        persist_location: PersistLocation,
        persist_clients: Arc<PersistClientCache>,
        now: NowFn,
        wallclock_lag: WallclockLagFn<Timestamp>,
        txns_metrics: Arc<TxnMetrics>,
        read_only: bool,
        deployment: u64,
        metrics_registry: &MetricsRegistry,
        controller_metrics: ControllerMetrics,
        connection_context: ConnectionContext,
        txn: &dyn StorageTxn,
        storage_collections: Arc<dyn StorageCollections + Send + Sync>,
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
            .expect("catalog bootstrap must initialize the transaction WAL identity");

        let txns_read = TxnsRead::start::<TxnsCodecRow>(txns_client.clone(), txns_id).await;

        let collection_manager =
            collection_mgmt::CollectionManager::new(read_only, now.clone(), Some(deployment));

        let introspection_ids = BTreeMap::new();
        let introspection_tokens = Arc::new(Mutex::new(BTreeMap::new()));

        let (statistics_interval_sender, _) =
            channel(mz_storage_types::parameters::STATISTICS_INTERVAL_DEFAULT);

        let mut maintenance_ticker = tokio::time::interval(Duration::from_secs(1));
        maintenance_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let (instance_response_tx, instance_response_rx) = mpsc::unbounded_channel();

        let metrics = StorageControllerMetrics::new(metrics_registry, controller_metrics);

        let now_dt = mz_ore::now::to_datetime(now());

        Self {
            collections: BTreeMap::default(),
            txns_read,
            txns_metrics,
            stashed_responses: vec![],
            collection_manager,
            introspection_ids,
            introspection_tokens,
            now,
            read_only,
            source_statistics: Arc::new(Mutex::new(BTreeMap::new())),
            sink_statistics: Arc::new(Mutex::new(BTreeMap::new())),
            statistics_interval_sender,
            instances: BTreeMap::new(),
            config: StorageConfiguration::new(connection_context, mz_dyncfgs::all_dyncfgs()),
            persist_location,
            persist: persist_clients,
            metrics,
            storage_frontier_observations: None,
            collection_metadata_certifier: None,
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

    /// Validates inventory membership even if StorageCollections read accounting
    /// has already been retired.
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
            if !self.is_sink(id) {
                return Err(StorageError::IdentifierMissing(id));
            }
        }
        Ok(())
    }

    /// Opens a write handle and synchronizes its cached upper.
    async fn open_data_handles(
        &self,
        id: &GlobalId,
        shard: ShardId,
        relation_desc: RelationDesc,
        persist_client: &PersistClient,
    ) -> WriteHandle<SourceData, (), Timestamp, StorageDiff> {
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

        // Synchronize the cached upper before it initializes collection state.
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
        write_handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        persist_client: PersistClient,
    ) -> Result<(), StorageError> {
        tracing::info!(%id, ?introspection_type, "registering introspection collection");

        // In read-only mode we create a new shard for all migrated storage collections. So we
        // "trick" the write task into thinking that it's not in read-only mode so something is
        // advancing this new shard.
        //
        // This is the storage-side of forcing writes to a self-owned replacement shard while
        // read-only. Migrated builtin MVs need the same treatment on their own write path; see
        // `ComputeController::allow_writes_in_read_only`.
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
        if introspection_type == IntrospectionType::StorageFrontierObservations {
            let sampler = frontier_observations::StorageFrontierObservations::new(
                persist_client.clone(),
                (id, metadata.data_shard, metadata.relation_desc.clone()),
                self.now.clone(),
                Arc::clone(
                    self.collection_metadata_certifier
                        .as_ref()
                        .expect("native sampler requires catalog certification"),
                ),
                Arc::clone(&self.storage_collections),
            );
            // Initial inventory only. Subsequent catalog changes update the
            // sampler at collection creation, schema evolution and removal.
            for (id, collection) in &self.collections {
                sampler.upsert(*id, collection.collection_metadata.data_shard);
            }
            self.storage_frontier_observations = Some(sampler);
        }

        let read_handle_fn = move || {
            let persist_client = persist_client.clone();
            let metadata = metadata.clone();

            let fut = async move {
                let read_handle = persist_client
                    .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
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
            // collections should also contain subsources.
            .retain(|(k, _replica_id), _| self.storage_collections.check_exists(*k).is_ok());
        self.sink_statistics
            .lock()
            .expect("poisoned")
            .retain(|(k, _replica_id), _| self.is_sink(*k));
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

    async fn read_handle_for_snapshot(
        &self,
        id: GlobalId,
    ) -> Result<ReadHandle<SourceData, (), Timestamp, StorageDiff>, StorageError> {
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
            if self.is_sink(id) {
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

    fn is_sink(&self, id: GlobalId) -> bool {
        self.collections
            .get(&id)
            .is_some_and(|c| matches!(c.data_source, DataSource::Sink { .. }))
    }

    /// Source placement follows the committed definition and current membership.
    /// Sink execution and output authority are checked by the replica.
    fn source_replicas(&self, id: GlobalId) -> BTreeSet<ReplicaId> {
        let Some(collection) = self.collections.get(&id) else {
            return BTreeSet::new();
        };
        let Some(instance) = collection
            .extra_state
            .instance_id()
            .and_then(|id| self.instances.get(&id))
        else {
            return BTreeSet::new();
        };
        let ingestion = match &collection.data_source {
            DataSource::Ingestion(desc) => Some(desc),
            DataSource::IngestionExport { ingestion_id, .. } => self
                .collections
                .get(ingestion_id)
                .and_then(|c| match &c.data_source {
                    DataSource::Ingestion(desc) => Some(desc),
                    _ => None,
                }),
            _ => None,
        };
        let Some(ingestion) = ingestion else {
            return BTreeSet::new();
        };
        let replicas = instance.replica_ids();
        if ingestion.desc.connection.prefers_single_replica() {
            replicas.take(1).collect()
        } else {
            replicas.collect()
        }
    }

    fn accepts_observation(&self, id: GlobalId, replica: Option<ReplicaId>) -> bool {
        let Some(collection) = self.collections.get(&id) else {
            return false;
        };
        let Some(instance) = collection
            .extra_state
            .instance_id()
            .and_then(|id| self.instances.get(&id))
        else {
            return false;
        };
        match replica {
            Some(replica) => instance.replica_ids().any(|id| id == replica),
            None => instance.replica_ids().next().is_none(),
        }
    }

    /// Queues zero-replica history for an originating committed effect.
    /// Callers must check publication authority before enqueueing because response
    /// processing cannot recover which catalog effect produced the event.
    fn queue_native_paused_statuses(&mut self, ids: impl IntoIterator<Item = GlobalId>) {
        let now = mz_ore::now::to_datetime((self.now)());
        for id in ids {
            if !self.accepts_observation(id, None) {
                continue;
            }
            let collection = &self.collections[&id];
            let object_type = match &collection.data_source {
                DataSource::Ingestion(_) | DataSource::IngestionExport { .. } => "source",
                DataSource::Sink { .. } => "sink",
                _ => continue,
            };
            let mut update = StatusUpdate::new(id, now, Status::Paused);
            update.hints.insert(format!(
                "There is currently no replica running this {object_type}"
            ));
            self.stashed_responses
                .push((None, StorageResponse::StatusUpdate(update)));
        }
    }

    fn collection(&self, id: GlobalId) -> Result<&CollectionState, StorageError> {
        self.collections
            .get(&id)
            .ok_or(StorageError::IdentifierMissing(id))
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

        let frontier_lag = |frontier: &Antichain<Timestamp>| match frontier.as_option() {
            Some(ts) => (self.wallclock_lag)(*ts),
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

                let instance_id = collection.extra_state.instance_id();
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
        if let Some(sampler) = &self.storage_frontier_observations {
            sampler.sample();
        }
        self.refresh_wallclock_lag();
    }
}

impl From<&IntrospectionType> for CollectionManagerKind {
    fn from(value: &IntrospectionType) -> Self {
        match value {
            IntrospectionType::ShardMapping
            | IntrospectionType::StorageSourceStatistics
            | IntrospectionType::StorageSinkStatistics
            | IntrospectionType::ComputeDependencies
            | IntrospectionType::ComputeOperatorHydrationStatus
            | IntrospectionType::ComputeMaterializedViewRefreshes
            | IntrospectionType::ComputeErrorCounts
            | IntrospectionType::ComputeHydrationTimes
            | IntrospectionType::ComputeObjectArrangementSizes => {
                CollectionManagerKind::Differential
            }

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
            | IntrospectionType::SqlText
            | IntrospectionType::StorageStatuses
            | IntrospectionType::ReplicaFrontierObservations
            | IntrospectionType::StorageFrontierObservations => CollectionManagerKind::AppendOnly,
        }
    }
}

/// Get the current rows in the given statistics table. This is used to bootstrap
/// the statistics tasks.
///
// TODO(guswynn): we need to be more careful about the update time we get here:
// <https://github.com/MaterializeInc/database-issues/issues/7564>
async fn snapshot_statistics(
    id: GlobalId,
    upper: Antichain<Timestamp>,
    storage_collections: &Arc<dyn StorageCollections + Send + Sync>,
) -> Vec<Row> {
    match upper.as_option() {
        Some(f) if f > &Timestamp::MIN => {
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

async fn read_handle_for_snapshot(
    persist: &PersistClientCache,
    id: GlobalId,
    metadata: &CollectionMetadata,
) -> Result<ReadHandle<SourceData, (), Timestamp, StorageDiff>, StorageError> {
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
struct CollectionState {
    /// The source of this collection's data.
    pub data_source: DataSource,

    pub collection_metadata: CollectionMetadata,

    pub extra_state: CollectionStateExtra,

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

impl CollectionState {
    fn new(
        data_source: DataSource,
        collection_metadata: CollectionMetadata,
        extra_state: CollectionStateExtra,
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
enum CollectionStateExtra {
    /// Observation state only, without installation holds or capability policy.
    Native {
        instance_id: StorageInstanceId,
        hydrated_on: BTreeSet<ReplicaId>,
    },
    None,
}

impl CollectionStateExtra {
    fn instance_id(&self) -> Option<StorageInstanceId> {
        match self {
            Self::Native { instance_id, .. } => Some(*instance_id),
            Self::None => None,
        }
    }

    fn hydrated_on(&self) -> Option<&BTreeSet<ReplicaId>> {
        match self {
            Self::Native { hydrated_on, .. } => Some(hydrated_on),
            _ => None,
        }
    }

    fn hydrated_on_mut(&mut self) -> Option<&mut BTreeSet<ReplicaId>> {
        match self {
            Self::Native { hydrated_on, .. } => Some(hydrated_on),
            _ => None,
        }
    }
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
) -> StatusHistoryDesc<(GlobalId, Option<ReplicaId>, Option<u64>)> {
    let desc = &MZ_SOURCE_STATUS_HISTORY_DESC;
    let (source_id_idx, _) = desc.get_by_name(&"source_id".into()).expect("exists");
    let (replica_id_idx, _) = desc.get_by_name(&"replica_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    let (generation_idx, _) = desc
        .get_by_name(&"deployment_generation".into())
        .expect("exists");

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
                (!datums[generation_idx].is_null()).then(|| datums[generation_idx].unwrap_uint64()),
            )
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

fn sink_status_history_desc(
    params: &StorageParameters,
) -> StatusHistoryDesc<(GlobalId, Option<ReplicaId>, Option<u64>)> {
    let desc = &MZ_SINK_STATUS_HISTORY_DESC;
    let (sink_id_idx, _) = desc.get_by_name(&"sink_id".into()).expect("exists");
    let (replica_id_idx, _) = desc.get_by_name(&"replica_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    let (generation_idx, _) = desc
        .get_by_name(&"deployment_generation".into())
        .expect("exists");

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
                (!datums[generation_idx].is_null()).then(|| datums[generation_idx].unwrap_uint64()),
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

fn replica_status_history_desc(
    params: &StorageParameters,
) -> StatusHistoryDesc<(GlobalId, u64, Option<u64>)> {
    let desc = &REPLICA_STATUS_HISTORY_DESC;
    let (replica_idx, _) = desc.get_by_name(&"replica_id".into()).expect("exists");
    let (process_idx, _) = desc.get_by_name(&"process_id".into()).expect("exists");
    let (time_idx, _) = desc.get_by_name(&"occurred_at".into()).expect("exists");

    let (generation_idx, _) = desc
        .get_by_name(&"deployment_generation".into())
        .expect("exists");

    StatusHistoryDesc {
        retention_policy: StatusHistoryRetentionPolicy::TimeWindow(
            params.replica_status_history_retention_window,
        ),
        extract_key: Box::new(move |datums| {
            (
                GlobalId::from_str(datums[replica_idx].unwrap_str()).expect("GlobalId column"),
                datums[process_idx].unwrap_uint64(),
                (!datums[generation_idx].is_null()).then(|| datums[generation_idx].unwrap_uint64()),
            )
        }),
        extract_time: Box::new(move |datums| datums[time_idx].unwrap_timestamptz()),
    }
}

#[cfg(test)]
mod tests {
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_secrets::InMemorySecretsController;
    use mz_storage_client::storage_collections::StorageCollectionsImpl;
    use mz_storage_types::connections::{KafkaConnection, Tunnel};
    use mz_storage_types::sinks::{
        KafkaIdStyle, KafkaSinkCompressionType, KafkaSinkConnection, KafkaSinkFormat,
        KafkaSinkFormatType, SinkEnvelope, StorageSinkDesc,
    };

    use super::*;

    struct TestTxn(ShardId);

    #[async_trait]
    impl StorageTxn for TestTxn {
        fn get_collection_metadata(&self) -> BTreeMap<GlobalId, ShardId> {
            BTreeMap::new()
        }
        fn insert_collection_metadata(
            &mut self,
            _: BTreeMap<GlobalId, ShardId>,
        ) -> Result<(), StorageError> {
            unimplemented!()
        }
        fn delete_collection_metadata(
            &mut self,
            _: BTreeSet<GlobalId>,
        ) -> Vec<(GlobalId, ShardId)> {
            unimplemented!()
        }
        fn get_unfinalized_shards(&self) -> BTreeSet<ShardId> {
            BTreeSet::new()
        }
        fn insert_unfinalized_shards(&mut self, _: BTreeSet<ShardId>) -> Result<(), StorageError> {
            unimplemented!()
        }
        fn remove_unfinalized_shards(&mut self, _: BTreeSet<ShardId>) {
            unimplemented!()
        }
        fn get_txn_wal_shard(&self) -> Option<ShardId> {
            Some(self.0)
        }
        fn write_txn_wal_shard(&mut self, _: ShardId) -> Result<(), StorageError> {
            unimplemented!()
        }
    }

    fn frontier(ts: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(ts.into())
    }

    mod native;

    pub(super) async fn test_controller(
        read_only: bool,
    ) -> (Controller, Arc<StorageCollectionsImpl>, PersistClient) {
        let registry = MetricsRegistry::new();
        let (controller, collections, persist, _peer) =
            test_controller_with_peer(read_only, registry).await;
        (controller, collections, persist)
    }

    pub(super) async fn test_controller_with_peer(
        read_only: bool,
        registry: MetricsRegistry,
    ) -> (
        Controller,
        Arc<StorageCollectionsImpl>,
        PersistClient,
        PersistClient,
    ) {
        let location = PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        };
        let mut config = PersistConfig::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
        config.critical_downgrade_interval = Duration::ZERO;
        let mut cache =
            PersistClientCache::new(config, &registry, |_, _| PubSubClientConnection::noop());
        let peer = cache.open(location.clone()).await.unwrap();
        cache.clear_state_cache();
        let cache = Arc::new(cache);
        let persist = cache.open(location.clone()).await.unwrap();
        let secrets_reader = Arc::new(InMemorySecretsController::new());
        let context = ConnectionContext::for_tests(secrets_reader);
        let txns_metrics = Arc::new(TxnMetrics::new(&registry));
        let txn = TestTxn(ShardId::new());
        let collections = Arc::new(
            StorageCollectionsImpl::new(
                location.clone(),
                Arc::clone(&cache),
                &registry,
                SYSTEM_TIME.clone(),
                Arc::clone(&txns_metrics),
                std::num::NonZeroI64::new(1).unwrap(),
                read_only,
                true,
                context.clone(),
                &txn,
            )
            .await,
        );
        let controller = Controller::new(
            location,
            cache,
            SYSTEM_TIME.clone(),
            WallclockLagFn::new(SYSTEM_TIME.clone()),
            txns_metrics,
            read_only,
            0,
            &registry,
            ControllerMetrics::new(&registry),
            context,
            &txn,
            Arc::<StorageCollectionsImpl>::clone(&collections),
        )
        .await;
        (controller, collections, persist, peer)
    }

    #[mz_ore::test(tokio::test)]
    async fn history_retention_preserves_replica_owned_current_statuses() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (controller, collections, persist) = test_controller(false).await;
            let status_row = |seconds, incarnation, status| {
                Row::pack_slice(&[
                    Datum::TimestampTz(
                        DateTime::from_timestamp(seconds, 0)
                            .unwrap()
                            .try_into()
                            .unwrap(),
                    ),
                    Datum::String("u1"),
                    Datum::String(status),
                    Datum::Null,
                    Datum::Null,
                    Datum::String("u1"),
                    Datum::UInt64(0),
                    Datum::UInt64(incarnation),
                ])
            };
            // Wall clocks can move backwards between client incarnations.
            // History retention orders by time, not by execution provenance.
            let predecessor = status_row(20, 1, "stalled");
            let successor = status_row(10, 2, "running");
            for (id, introspection_type, desc, expected) in [
                (
                    GlobalId::System(1),
                    IntrospectionType::SourceStatusHistory,
                    MZ_SOURCE_STATUS_HISTORY_DESC.clone(),
                    vec![Row::pack(predecessor.iter().take(7))],
                ),
                (
                    GlobalId::System(2),
                    IntrospectionType::StorageStatuses,
                    mz_storage_client::healthcheck::MZ_STORAGE_STATUSES_RAW_DESC.clone(),
                    vec![predecessor.clone(), successor.clone()],
                ),
            ] {
                let shard = ShardId::new();
                let metadata = StorageMetadata {
                    collection_metadata: [(id, shard)].into_iter().collect(),
                    compaction_bounds: [(id, frontier(0))].into_iter().collect(),
                    ..Default::default()
                };
                let mut description = CollectionDescription::for_other(desc.clone(), None);
                description.data_source = DataSource::Introspection(introspection_type);
                collections
                    .create_collections_for_bootstrap(
                        &metadata,
                        None,
                        vec![(id, description)],
                        &BTreeSet::new(),
                    )
                    .await
                    .unwrap();
                let mut writer = persist
                    .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                        shard,
                        Arc::new(desc.clone()),
                        Arc::new(UnitSchema),
                        Diagnostics::for_tests(),
                    )
                    .await
                    .unwrap();
                writer
                    .compare_and_append(
                        [predecessor.clone(), successor.clone()].map(|row| {
                            let row =
                                if introspection_type == IntrospectionType::SourceStatusHistory {
                                    Row::pack(row.iter().take(7))
                                } else {
                                    row
                                };
                            ((SourceData(Ok(row)), ()), Timestamp::MIN, 1)
                        }),
                        frontier(0),
                        frontier(1),
                    )
                    .await
                    .unwrap()
                    .unwrap();

                // Exercise both startup and joining a shard already prepared by
                // another collection-manager task.
                for _ in 0..2 {
                    let mut parameters = controller.config.parameters.clone();
                    parameters.keep_n_source_status_history_entries = 1;
                    controller
                        .collection_manager
                        .register_append_only_collection(
                            id,
                            writer,
                            false,
                            Some(AppendOnlyIntrospectionConfig {
                                introspection_type,
                                config_set: Arc::clone(controller.config.config_set()),
                                parameters,
                                storage_collections: Arc::clone(&controller.storage_collections),
                            }),
                        );
                    // The task prepares before servicing append requests. An
                    // empty acknowledged append is a barrier without adding rows.
                    controller
                        .collection_manager
                        .monotonic_appender(id)
                        .unwrap()
                        .append(Vec::new())
                        .await
                        .unwrap();
                    let mut actual = collections.snapshot_latest(id).await.unwrap();
                    let mut expected = expected.clone();
                    actual.sort();
                    expected.sort();
                    assert_eq!(actual, expected, "{introspection_type:?}");
                    controller
                        .collection_manager
                        .unregister_collection(id)
                        .await;
                    writer = persist
                        .open_writer(
                            shard,
                            Arc::new(desc.clone()),
                            Arc::new(UnitSchema),
                            Diagnostics::for_tests(),
                        )
                        .await
                        .unwrap();
                }
                writer.expire().await;
            }
        })
        .await
        .expect("status preparation must complete");
    }

    #[mz_ore::test(tokio::test)]
    async fn drop_tables_after_read_state_retired() {
        let (mut controller, collections, _) = test_controller(false).await;
        let table = GlobalId::User(1);
        let source = GlobalId::User(2);
        let unknown = GlobalId::User(99);
        let mut descriptions = vec![
            (
                table,
                CollectionDescription::for_table(RelationDesc::empty()),
            ),
            (
                source,
                CollectionDescription::for_other(RelationDesc::empty(), None),
            ),
        ];
        for (id, typ, desc) in [
            (
                GlobalId::System(1),
                IntrospectionType::ShardMapping,
                RelationDesc::builder()
                    .with_column("object_id", mz_repr::SqlScalarType::String.nullable(false))
                    .with_column("shard_id", mz_repr::SqlScalarType::String.nullable(false))
                    .finish(),
            ),
            (
                GlobalId::System(2),
                IntrospectionType::SourceStatusHistory,
                MZ_SOURCE_STATUS_HISTORY_DESC.clone(),
            ),
            (
                GlobalId::System(3),
                IntrospectionType::SinkStatusHistory,
                MZ_SINK_STATUS_HISTORY_DESC.clone(),
            ),
        ] {
            let mut description = CollectionDescription::for_other(desc, None);
            description.data_source = DataSource::Introspection(typ);
            descriptions.push((id, description));
        }
        let mut metadata = StorageMetadata {
            collection_metadata: descriptions
                .iter()
                .map(|(id, _)| (*id, ShardId::new()))
                .collect(),
            compaction_bounds: descriptions
                .iter()
                .map(|(id, _)| (*id, frontier(0)))
                .collect(),
            ..Default::default()
        };
        controller
            .create_collections_for_bootstrap(
                &metadata,
                None,
                descriptions,
                &BTreeSet::new(),
                false,
            )
            .await
            .unwrap();

        // Committed catalog removal and table forgetting can retire read accounting
        // before the controller is asked to clean up execution state.
        for id in [table, source] {
            metadata.collection_metadata.remove(&id);
            metadata.compaction_bounds.remove(&id);
        }
        collections.drop_collections_unvalidated(&metadata, vec![table, source]);
        for retired in [table, source] {
            assert!(matches!(
                collections.check_exists(retired),
                Err(StorageError::IdentifierMissing(id)) if id == retired
            ));
            assert!(controller.collection(retired).is_ok());
        }
        assert!(matches!(
            controller.collection(table).unwrap().data_source,
            DataSource::Table
        ));

        // Validate the whole batch before performing any execution cleanup.
        assert!(matches!(
            controller.drop_tables(&metadata, vec![table, unknown], true),
            Err(StorageError::IdentifierMissing(id)) if id == unknown
        ));
        assert!(matches!(
            controller.drop_sources(&metadata, vec![source, unknown], true),
            Err(StorageError::IdentifierMissing(id)) if id == unknown
        ));
        assert!(controller.collection(table).is_ok());
        assert!(controller.collection(source).is_ok());

        controller
            .drop_tables(&metadata, vec![table], true)
            .unwrap();
        assert!(matches!(
            controller.collection(table),
            Err(StorageError::IdentifierMissing(id)) if id == table
        ));
        controller
            .drop_sources(&metadata, vec![source], true)
            .unwrap();
        assert!(matches!(
            controller.collection(source),
            Err(StorageError::IdentifierMissing(id)) if id == source
        ));
        controller.process().unwrap();
    }

    fn export_description(from: GlobalId) -> ExportDescription {
        ExportDescription {
            sink: StorageSinkDesc {
                from,
                from_desc: RelationDesc::empty(),
                connection: StorageSinkConnection::Kafka(KafkaSinkConnection {
                    connection_id: mz_repr::CatalogItemId::System(1),
                    connection: KafkaConnection {
                        brokers: Default::default(),
                        default_tunnel: Tunnel::Direct,
                        progress_topic: Default::default(),
                        progress_topic_options: Default::default(),
                        options: Default::default(),
                        tls: Default::default(),
                        sasl: Default::default(),
                    },
                    format: KafkaSinkFormat {
                        key_format: None,
                        value_format: KafkaSinkFormatType::Text,
                    },
                    relation_key_indices: None,
                    key_desc_and_indices: None,
                    headers_index: None,
                    value_desc: RelationDesc::empty(),
                    partition_by: Default::default(),
                    topic: Default::default(),
                    topic_options: Default::default(),
                    compression_type: KafkaSinkCompressionType::None,
                    progress_group_id: KafkaIdStyle::Legacy,
                    transactional_id: KafkaIdStyle::Legacy,
                    topic_metadata_refresh_interval: Default::default(),
                }),
                with_snapshot: true,
                version: 0,
                envelope: SinkEnvelope::Upsert,
                as_of: frontier(5),
                from_storage_metadata: (),
                to_storage_metadata: (),
                commit_interval: Default::default(),
            },
            instance_id: StorageInstanceId::system(1).unwrap(),
        }
    }
}
