// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A representative of STORAGE and COMPUTE that maintains summaries of the involved objects.
//!
//! The `Controller` provides the ability to create and manipulate storage and compute instances.
//! Each of Storage and Compute provide their own controllers, accessed through the `storage()`
//! and `compute(instance_id)` methods. It is an error to access a compute instance before it has
//! been created.
//!
//! The controller also provides a `recv()` method that returns responses from the storage and
//! compute layers, which may remain of value to the interested user. With time, these responses
//! may be thinned down in an effort to make the controller more self contained.
//!
//! Consult the `StorageController` and `ComputeController` documentation for more information
//! about each of these interfaces.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use mz_build_info::BuildInfo;
use mz_cluster_client::metrics::ControllerMetrics;
use mz_cluster_client::{ReplicaId, WallclockLagFn};
use mz_compute_client::controller::{
    ComputeController, ComputeControllerResponse, ComputeControllerTimestamp, PeekNotification,
};
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::WatchSetId;
use mz_dyncfg::{ConfigSet, ConfigUpdates};
use mz_orchestrator::{NamespacedOrchestrator, Orchestrator, ServiceProcessMetrics};
use mz_ore::cast::CastFrom;
use mz_ore::id_gen::Gen;
use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::AbortOnDropHandle;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::Codec64;
use mz_repr::{Datum, GlobalId, Row, TimestampManipulation};
use mz_service::secrets::SecretsReaderCliArgs;
use mz_storage_client::controller::{
    IntrospectionType, StorageController, StorageMetadata, StorageTxn,
};
use mz_storage_client::storage_collections::{self, StorageCollections};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::StorageError;
use mz_txn_wal::metrics::Metrics as TxnMetrics;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc;
use uuid::Uuid;

pub mod clusters;

// Export this on behalf of the storage controller to provide a unified
// interface, allowing other crates to depend on this crate alone.
pub use mz_storage_controller::prepare_initialization;

/// Configures a controller.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// The orchestrator implementation to use.
    pub orchestrator: Arc<dyn Orchestrator>,
    /// The persist location where all storage collections will be written to.
    pub persist_location: PersistLocation,
    /// A process-global cache of (blob_uri, consensus_uri) ->
    /// PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<PersistClientCache>,
    /// The clusterd image to use when starting new cluster processes.
    pub clusterd_image: String,
    /// The init container image to use for clusterd.
    pub init_container_image: Option<String>,
    /// A number representing the environment's generation.
    ///
    /// This is incremented to request that the new process perform a graceful
    /// transition of power from the prior generation.
    pub deploy_generation: u64,
    /// The now function to advance the controller's introspection collections.
    pub now: NowFn,
    /// The metrics registry.
    pub metrics_registry: MetricsRegistry,
    /// The URL for Persist PubSub.
    pub persist_pubsub_url: String,
    /// Arguments for secrets readers.
    pub secrets_args: SecretsReaderCliArgs,
    /// The connection context, to thread through to clusterd, with cli flags.
    pub connection_context: ConnectionContext,
}

/// Responses that [`Controller`] can produce.
#[derive(Debug)]
pub enum ControllerResponse<T = mz_repr::Timestamp> {
    /// Notification of a worker's response to a specified (by connection id) peek.
    ///
    /// Additionally, an `OpenTelemetryContext` to forward trace information
    /// back into coord. This allows coord traces to be children of work
    /// done in compute!
    PeekNotification(Uuid, PeekNotification, OpenTelemetryContext),
    /// The worker's next response to a specified subscribe.
    SubscribeResponse(GlobalId, SubscribeBatch<T>),
    /// The worker's next response to a specified copy to.
    CopyToResponse(GlobalId, Result<u64, anyhow::Error>),
    /// Notification that a watch set has finished. See
    /// [`Controller::install_compute_watch_set`] and
    /// [`Controller::install_storage_watch_set`] for details.
    WatchSetFinished(Vec<WatchSetId>),
}

/// Whether one of the underlying controllers is ready for their `process`
/// method to be called.
#[derive(Debug, Default)]
pub enum Readiness<T> {
    /// No underlying controllers are ready.
    #[default]
    NotReady,
    /// The storage controller is ready.
    Storage,
    /// The compute controller is ready.
    Compute,
    /// A batch of metric data is ready.
    Metrics((ReplicaId, Vec<ServiceProcessMetrics>)),
    /// An internally-generated message is ready to be returned.
    Internal(ControllerResponse<T>),
}

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<T: ComputeControllerTimestamp = mz_repr::Timestamp> {
    pub storage: Box<dyn StorageController<Timestamp = T>>,
    pub storage_collections: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
    pub compute: ComputeController<T>,
    /// The clusterd image to use when starting new cluster processes.
    clusterd_image: String,
    /// The init container image to use for clusterd.
    init_container_image: Option<String>,
    /// A number representing the environment's generation.
    deploy_generation: u64,
    /// Whether or not this controller is in read-only mode.
    ///
    /// When in read-only mode, neither this controller nor the instances
    /// controlled by it are allowed to affect changes to external systems
    /// (largely persist).
    read_only: bool,
    /// The cluster orchestrator.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// Tracks the readiness of the underlying controllers.
    readiness: Readiness<T>,
    /// Tasks for collecting replica metrics.
    metrics_tasks: BTreeMap<ReplicaId, AbortOnDropHandle<()>>,
    /// Sender for the channel over which replica metrics are sent.
    metrics_tx: mpsc::UnboundedSender<(ReplicaId, Vec<ServiceProcessMetrics>)>,
    /// Receiver for the channel over which replica metrics are sent.
    metrics_rx: mpsc::UnboundedReceiver<(ReplicaId, Vec<ServiceProcessMetrics>)>,
    /// A function providing the current wallclock time.
    now: NowFn,

    /// The URL for Persist PubSub.
    persist_pubsub_url: String,

    /// Arguments for secrets readers.
    secrets_args: SecretsReaderCliArgs,

    /// A map associating a global ID to the set of all the unfulfilled watch
    /// set ids that include it.
    ///
    /// See [`Controller::install_compute_watch_set`]/[`Controller::install_storage_watch_set`] for a description of watch sets.
    // When a watch set is fulfilled for a given object (that is, when
    // the object's frontier advances to at least the watch set's
    // timestamp), the corresponding entry will be removed from the set.
    unfulfilled_watch_sets_by_object: BTreeMap<GlobalId, BTreeSet<WatchSetId>>,
    /// A map of installed watch sets indexed by id.
    unfulfilled_watch_sets: BTreeMap<WatchSetId, (BTreeSet<GlobalId>, T)>,
    /// A sequence of numbers used to mint unique WatchSetIds.
    watch_set_id_gen: Gen<WatchSetId>,

    /// A list of watch sets that were already fulfilled as soon as
    /// they were installed, and thus that must be returned to the
    /// client on the next call to [`Controller::process_compute_response`]/[`Controller::process_storage_response`].
    ///
    /// See [`Controller::install_compute_watch_set`]/[`Controller::install_storage_watch_set`] for a description of watch sets.
    immediate_watch_sets: Vec<WatchSetId>,

    /// Dynamic system configuration.
    dyncfg: ConfigSet,
}

impl<T: ComputeControllerTimestamp> Controller<T> {
    /// Update the controller configuration.
    pub fn update_configuration(&mut self, updates: ConfigUpdates) {
        updates.apply(&self.dyncfg);
    }

    /// Start sinking the compute controller's introspection data into storage.
    ///
    /// This method should be called once the introspection collections have been registered with
    /// the storage controller. It will panic if invoked earlier than that.
    pub fn start_compute_introspection_sink(&mut self) {
        self.compute.start_introspection_sink(&*self.storage);
    }

    /// Returns the connection context installed in the controller.
    ///
    /// This is purely a helper, and can be obtained from `self.storage`.
    pub fn connection_context(&self) -> &ConnectionContext {
        &self.storage.config().connection_context
    }

    /// Returns the storage configuration installed in the storage controller.
    ///
    /// This is purely a helper, and can be obtained from `self.storage`.
    pub fn storage_configuration(&self) -> &StorageConfiguration {
        self.storage.config()
    }

    /// Returns the state of the [`Controller`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    pub async fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Note: We purposefully use the `Debug` formatting for the value of all fields in the
        // returned object as a tradeoff between usability and stability. `serde_json` will fail
        // to serialize an object if the keys aren't strings, so `Debug` formatting the values
        // prevents a future unrelated change from silently breaking this method.

        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            storage_collections,
            storage,
            compute,
            clusterd_image: _,
            init_container_image: _,
            deploy_generation,
            read_only,
            orchestrator: _,
            readiness,
            metrics_tasks: _,
            metrics_tx: _,
            metrics_rx: _,
            now: _,
            persist_pubsub_url: _,
            secrets_args: _,
            unfulfilled_watch_sets_by_object: _,
            unfulfilled_watch_sets,
            watch_set_id_gen: _,
            immediate_watch_sets,
            dyncfg: _,
        } = self;

        let storage_collections = storage_collections.dump()?;
        let storage = storage.dump()?;
        let compute = compute.dump().await?;

        let unfulfilled_watch_sets: BTreeMap<_, _> = unfulfilled_watch_sets
            .iter()
            .map(|(ws_id, watches)| (format!("{ws_id:?}"), format!("{watches:?}")))
            .collect();
        let immediate_watch_sets: Vec<_> = immediate_watch_sets
            .iter()
            .map(|watch| format!("{watch:?}"))
            .collect();

        Ok(serde_json::json!({
            "storage_collections": storage_collections,
            "storage": storage,
            "compute": compute,
            "deploy_generation": deploy_generation,
            "read_only": read_only,
            "readiness": format!("{readiness:?}"),
            "unfulfilled_watch_sets": unfulfilled_watch_sets,
            "immediate_watch_sets": immediate_watch_sets,
        }))
    }
}

impl<T> Controller<T>
where
    T: ComputeControllerTimestamp,
{
    pub fn update_orchestrator_scheduling_config(
        &self,
        config: mz_orchestrator::scheduling_config::ServiceSchedulingConfig,
    ) {
        self.orchestrator.update_scheduling_config(config);
    }
    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.storage.initialization_complete();
        self.compute.initialization_complete();
    }

    /// Reports whether the controller is in read only mode.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Returns `Some` if there is an immediately available
    /// internally-generated response that we need to return to the
    /// client (as opposed to waiting for a response from compute or storage).
    fn take_internal_response(&mut self) -> Option<ControllerResponse<T>> {
        let ws = std::mem::take(&mut self.immediate_watch_sets);
        (!ws.is_empty()).then_some(ControllerResponse::WatchSetFinished(ws))
    }

    /// Waits until the controller is ready to process a response.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the owner should call [`Controller::ready`] to
    /// process the ready message.
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if let Readiness::NotReady = self.readiness {
            // the coordinator wants to be able to make a simple
            // sequence of ready, process, ready, process, .... calls,
            // but the controller sometimes has responses immediately
            // ready to be processed and should do so before calling
            // into either of the lower-level controllers. This `if`
            // statement handles that case.
            if let Some(response) = self.take_internal_response() {
                self.readiness = Readiness::Internal(response);
            } else {
                // The underlying `ready` methods are cancellation safe, so it is
                // safe to construct this `select!`.
                tokio::select! {
                    () = self.storage.ready() => {
                        self.readiness = Readiness::Storage;
                    }
                    () = self.compute.ready() => {
                        self.readiness = Readiness::Compute;
                    }
                    Some(metrics) = self.metrics_rx.recv() => {
                        self.readiness = Readiness::Metrics(metrics);
                    }
                }
            }
        }
    }

    /// Returns the [Readiness] status of this controller.
    pub fn get_readiness(&self) -> &Readiness<T> {
        &self.readiness
    }

    /// Install a _watch set_ in the controller.
    ///
    /// A _watch set_ is a request to be informed by the controller when
    /// all of the frontiers of a particular set of objects have advanced at
    /// least to a particular timestamp.
    ///
    /// When all the objects in `objects` have advanced to `t`, the watchset id
    /// is returned to the client on the next call to [`Self::process`].
    pub fn install_compute_watch_set(
        &mut self,
        mut objects: BTreeSet<GlobalId>,
        t: T,
    ) -> WatchSetId {
        let ws_id = self.watch_set_id_gen.allocate_id();

        objects.retain(|id| {
            let frontier = self
                .compute
                .collection_frontiers(*id, None)
                .map(|f| f.write_frontier)
                .expect("missing compute dependency");
            frontier.less_equal(&t)
        });
        if objects.is_empty() {
            self.immediate_watch_sets.push(ws_id);
        } else {
            for id in objects.iter() {
                self.unfulfilled_watch_sets_by_object
                    .entry(*id)
                    .or_default()
                    .insert(ws_id);
            }
            self.unfulfilled_watch_sets.insert(ws_id, (objects, t));
        }

        ws_id
    }

    /// Install a _watch set_ in the controller.
    ///
    /// A _watch set_ is a request to be informed by the controller when
    /// all of the frontiers of a particular set of objects have advanced at
    /// least to a particular timestamp.
    ///
    /// When all the objects in `objects` have advanced to `t`, the watchset id
    /// is returned to the client on the next call to [`Self::process`].
    pub fn install_storage_watch_set(
        &mut self,
        mut objects: BTreeSet<GlobalId>,
        t: T,
    ) -> WatchSetId {
        let ws_id = self.watch_set_id_gen.allocate_id();

        let uppers = self
            .storage
            .collections_frontiers(objects.iter().cloned().collect())
            .expect("missing storage dependencies")
            .into_iter()
            .map(|(id, _since, upper)| (id, upper))
            .collect::<BTreeMap<_, _>>();

        objects.retain(|id| {
            let upper = uppers.get(id).expect("missing collection");
            upper.less_equal(&t)
        });
        if objects.is_empty() {
            self.immediate_watch_sets.push(ws_id);
        } else {
            for id in objects.iter() {
                self.unfulfilled_watch_sets_by_object
                    .entry(*id)
                    .or_default()
                    .insert(ws_id);
            }
            self.unfulfilled_watch_sets.insert(ws_id, (objects, t));
        }
        ws_id
    }

    /// Uninstalls a previously installed WatchSetId. The method is a no-op if the watch set has
    /// already finished and therefore it's safe to call this function unconditionally.
    ///
    /// # Panics
    /// This method panics if called with a WatchSetId that was never returned by the function.
    pub fn uninstall_watch_set(&mut self, ws_id: &WatchSetId) {
        if let Some((obj_ids, _)) = self.unfulfilled_watch_sets.remove(ws_id) {
            for obj_id in obj_ids {
                let mut entry = match self.unfulfilled_watch_sets_by_object.entry(obj_id) {
                    Entry::Occupied(entry) => entry,
                    Entry::Vacant(_) => panic!("corrupted watchset state"),
                };
                entry.get_mut().remove(ws_id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
    }

    /// Process a pending response from the storage controller. If necessary,
    /// return a higher-level response to our client.
    fn process_storage_response(
        &mut self,
        storage_metadata: &StorageMetadata,
    ) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        let maybe_response = self.storage.process(storage_metadata)?;
        Ok(maybe_response.and_then(
            |mz_storage_client::controller::Response::FrontierUpdates(r)| {
                self.handle_frontier_updates(&r)
            },
        ))
    }

    /// Process a pending response from the compute controller. If necessary,
    /// return a higher-level response to our client.
    fn process_compute_response(&mut self) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        let response = self.compute.process();

        let response = response.and_then(|r| match r {
            ComputeControllerResponse::PeekNotification(uuid, peek, otel_ctx) => {
                Some(ControllerResponse::PeekNotification(uuid, peek, otel_ctx))
            }
            ComputeControllerResponse::SubscribeResponse(id, tail) => {
                Some(ControllerResponse::SubscribeResponse(id, tail))
            }
            ComputeControllerResponse::CopyToResponse(id, tail) => {
                Some(ControllerResponse::CopyToResponse(id, tail))
            }
            ComputeControllerResponse::FrontierUpper { id, upper } => {
                self.handle_frontier_updates(&[(id, upper)])
            }
        });
        Ok(response)
    }

    /// Processes the work queued by [`Controller::ready`].
    ///
    /// This method is guaranteed to return "quickly" unless doing so would
    /// compromise the correctness of the system.
    ///
    /// This method is **not** guaranteed to be cancellation safe. It **must**
    /// be awaited to completion.
    #[mz_ore::instrument(level = "debug")]
    pub fn process(
        &mut self,
        storage_metadata: &StorageMetadata,
    ) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        match mem::take(&mut self.readiness) {
            Readiness::NotReady => Ok(None),
            Readiness::Storage => self.process_storage_response(storage_metadata),
            Readiness::Compute => self.process_compute_response(),
            Readiness::Metrics((id, metrics)) => self.process_replica_metrics(id, metrics),
            Readiness::Internal(message) => Ok(Some(message)),
        }
    }

    /// Record updates to frontiers, and propagate any necessary responses.
    /// As of this writing (2/29/2024), the only response that can be generated
    /// from a frontier update is `WatchSetCompleted`.
    fn handle_frontier_updates(
        &mut self,
        updates: &[(GlobalId, Antichain<T>)],
    ) -> Option<ControllerResponse<T>> {
        let mut finished = vec![];
        for (obj_id, antichain) in updates {
            let ws_ids = self.unfulfilled_watch_sets_by_object.entry(*obj_id);
            if let Entry::Occupied(mut ws_ids) = ws_ids {
                ws_ids.get_mut().retain(|ws_id| {
                    let mut entry = match self.unfulfilled_watch_sets.entry(*ws_id) {
                        Entry::Occupied(entry) => entry,
                        Entry::Vacant(_) => panic!("corrupted watchset state"),
                    };
                    // If this object has made more progress than required by this watchset we:
                    if !antichain.less_equal(&entry.get().1) {
                        // 1. Remove the object from the set of pending objects for the watchset
                        entry.get_mut().0.remove(obj_id);
                        // 2. Mark the watchset as finished if this was the last watched object
                        if entry.get().0.is_empty() {
                            entry.remove();
                            finished.push(*ws_id);
                        }
                        // 3. Remove the watchset from the set of pending watchsets for the object
                        false
                    } else {
                        // Otherwise we keep the watchset around to re-check in the future
                        true
                    }
                });
                // Clear the entry if this was the last watchset that was interested in obj_id
                if ws_ids.get().is_empty() {
                    ws_ids.remove();
                }
            }
        }
        (!(finished.is_empty())).then(|| ControllerResponse::WatchSetFinished(finished))
    }

    fn process_replica_metrics(
        &mut self,
        id: ReplicaId,
        metrics: Vec<ServiceProcessMetrics>,
    ) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        self.record_replica_metrics(id, &metrics);
        Ok(None)
    }

    fn record_replica_metrics(&mut self, replica_id: ReplicaId, metrics: &[ServiceProcessMetrics]) {
        if self.read_only() {
            return;
        }

        let now = mz_ore::now::to_datetime((self.now)());
        let now_tz = now.try_into().expect("must fit");

        let replica_id = replica_id.to_string();
        let mut row = Row::default();
        let updates = metrics
            .iter()
            .enumerate()
            .map(|(process_id, m)| {
                row.packer().extend(&[
                    Datum::String(&replica_id),
                    Datum::UInt64(u64::cast_from(process_id)),
                    m.cpu_nano_cores.into(),
                    m.memory_bytes.into(),
                    m.disk_bytes.into(),
                    Datum::TimestampTz(now_tz),
                    m.heap_bytes.into(),
                    m.heap_limit.into(),
                ]);
                (row.clone(), mz_repr::Diff::ONE)
            })
            .collect();

        self.storage
            .append_introspection_updates(IntrospectionType::ReplicaMetricsHistory, updates);
    }

    /// Determine the "real-time recency" timestamp for all `ids`.
    ///
    /// Real-time recency is defined as the minimum value of `T` that all
    /// objects can be queried at to return all data visible in the upstream
    /// system the query was issued. In this case, "the upstream systems" are
    /// any user sources that connect to objects outside of Materialize, such as
    /// Kafka sources.
    ///
    /// If no items in `ids` connect to external systems, this function will
    /// return `Ok(T::minimum)`.
    pub async fn determine_real_time_recent_timestamp(
        &self,
        ids: BTreeSet<GlobalId>,
        timeout: Duration,
    ) -> Result<BoxFuture<'static, Result<T, StorageError<T>>>, StorageError<T>> {
        self.storage.real_time_recent_timestamp(ids, timeout).await
    }
}

impl<T> Controller<T>
where
    // Bounds needed by `StorageController` and/or `Controller`:
    T: Timestamp
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + std::fmt::Display
        + Into<mz_repr::Timestamp>,
    // Bounds needed by `ComputeController`:
    T: ComputeControllerTimestamp,
{
    /// Creates a new controller.
    ///
    /// For correctness, this function expects to have access to the mutations
    /// to the `storage_txn` that occurred in [`prepare_initialization`].
    ///
    /// # Panics
    /// If this function is called before [`prepare_initialization`].
    #[instrument(name = "controller::new")]
    pub async fn new(
        config: ControllerConfig,
        envd_epoch: NonZeroI64,
        read_only: bool,
        storage_txn: &dyn StorageTxn<T>,
    ) -> Self {
        if read_only {
            tracing::info!("starting controllers in read-only mode!");
        }

        let now_fn = config.now.clone();
        let wallclock_lag_fn = WallclockLagFn::new(now_fn);

        let controller_metrics = ControllerMetrics::new(&config.metrics_registry);

        let txns_metrics = Arc::new(TxnMetrics::new(&config.metrics_registry));
        let collections_ctl = storage_collections::StorageCollectionsImpl::new(
            config.persist_location.clone(),
            Arc::clone(&config.persist_clients),
            &config.metrics_registry,
            config.now.clone(),
            Arc::clone(&txns_metrics),
            envd_epoch,
            read_only,
            config.connection_context.clone(),
            storage_txn,
        )
        .await;

        let collections_ctl: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync> =
            Arc::new(collections_ctl);

        let storage_controller = mz_storage_controller::Controller::new(
            config.build_info,
            config.persist_location.clone(),
            config.persist_clients,
            config.now.clone(),
            wallclock_lag_fn.clone(),
            Arc::clone(&txns_metrics),
            read_only,
            &config.metrics_registry,
            controller_metrics.clone(),
            config.connection_context,
            storage_txn,
            Arc::clone(&collections_ctl),
        )
        .await;

        let storage_collections = Arc::clone(&collections_ctl);
        let compute_controller = ComputeController::new(
            config.build_info,
            storage_collections,
            read_only,
            &config.metrics_registry,
            config.persist_location,
            controller_metrics,
            config.now.clone(),
            wallclock_lag_fn,
        );
        let (metrics_tx, metrics_rx) = mpsc::unbounded_channel();

        let this = Self {
            storage: Box::new(storage_controller),
            storage_collections: collections_ctl,
            compute: compute_controller,
            clusterd_image: config.clusterd_image,
            init_container_image: config.init_container_image,
            deploy_generation: config.deploy_generation,
            read_only,
            orchestrator: config.orchestrator.namespace("cluster"),
            readiness: Readiness::NotReady,
            metrics_tasks: BTreeMap::new(),
            metrics_tx,
            metrics_rx,
            now: config.now,
            persist_pubsub_url: config.persist_pubsub_url,
            secrets_args: config.secrets_args,
            unfulfilled_watch_sets_by_object: BTreeMap::new(),
            unfulfilled_watch_sets: BTreeMap::new(),
            watch_set_id_gen: Gen::default(),
            immediate_watch_sets: Vec::new(),
            dyncfg: mz_dyncfgs::all_dyncfgs(),
        };

        if !this.read_only {
            this.remove_past_generation_replicas_in_background();
        }

        this
    }
}
