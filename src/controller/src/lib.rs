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

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::num::NonZeroI64;
use std::rc::Rc;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{Peekable, StreamExt};
use mz_build_info::BuildInfo;
use mz_cluster_client::ReplicaId;
use mz_compute_client::controller::{
    ActiveComputeController, ComputeController, ComputeControllerResponse,
    ComputeControllerTimestamp,
};
use mz_compute_client::protocol::response::{PeekResponse, SubscribeBatch};
use mz_compute_client::service::{ComputeClient, ComputeGrpcClient};
use mz_orchestrator::{NamespacedOrchestrator, Orchestrator, ServiceProcessMetrics};
use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::AbortOnDropHandle;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::PersistLocation;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::{Datum, GlobalId, TimestampManipulation};
use mz_service::secrets::SecretsReaderCliArgs;
use mz_storage_client::client::{
    ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageResponse,
};
use mz_storage_client::controller::{StorageController, StorageMetadata, StorageTxn};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::PersistTxnTablesImpl;
use serde::Serialize;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::{self, Duration, Interval, MissedTickBehavior};
use tokio_stream::wrappers::UnboundedReceiverStream;
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
    /// The stash URL for the storage controller.
    pub storage_stash_url: String,
    /// The clusterd image to use when starting new cluster processes.
    pub clusterd_image: String,
    /// The init container image to use for clusterd.
    pub init_container_image: Option<String>,
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
    /// The worker's response to a specified (by connection id) peek.
    ///
    /// Additionally, an `OpenTelemetryContext` to forward trace information
    /// back into coord. This allows coord traces to be children of work
    /// done in compute!
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// The worker's next response to a specified subscribe.
    SubscribeResponse(GlobalId, SubscribeBatch<T>),
    /// The worker's next response to a specified copy to.
    CopyToResponse(GlobalId, Result<u64, anyhow::Error>),
    /// Notification that new resource usage metrics are available for a given replica.
    ComputeReplicaMetrics(ReplicaId, Vec<ServiceProcessMetrics>),
    /// Notification that a watch set has finished. See
    /// [`Controller::install_compute_watch_set`] and
    /// [`Controller::install_storage_watch_set`] for details.
    WatchSetFinished(Vec<Box<dyn Any>>),
}

/// Whether one of the underlying controllers is ready for their `process`
/// method to be called.
#[derive(Debug, Default)]
enum Readiness<T> {
    /// No underlying controllers are ready.
    #[default]
    NotReady,
    /// The storage controller is ready.
    Storage,
    /// The compute controller is ready.
    Compute,
    /// The metrics channel is ready.
    Metrics,
    /// Frontiers are ready for recording.
    Frontiers,
    /// An internally-generated message is ready to be returned.
    Internal(ControllerResponse<T>),
}

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<T = mz_repr::Timestamp> {
    pub storage: Box<dyn StorageController<Timestamp = T>>,
    pub compute: ComputeController<T>,
    /// The clusterd image to use when starting new cluster processes.
    clusterd_image: String,
    /// The init container image to use for clusterd.
    init_container_image: Option<String>,
    /// The cluster orchestrator.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// Tracks the readiness of the underlying controllers.
    readiness: Readiness<T>,
    /// Tasks for collecting replica metrics.
    metrics_tasks: BTreeMap<ReplicaId, AbortOnDropHandle<()>>,
    /// Sender for the channel over which replica metrics are sent.
    metrics_tx: UnboundedSender<(ReplicaId, Vec<ServiceProcessMetrics>)>,
    /// Receiver for the channel over which replica metrics are sent.
    metrics_rx: Peekable<UnboundedReceiverStream<(ReplicaId, Vec<ServiceProcessMetrics>)>>,
    /// Periodic notification to record frontiers.
    frontiers_ticker: Interval,

    /// The URL for Persist PubSub.
    persist_pubsub_url: String,
    /// Whether to use the new persist-txn tables implementation or the legacy
    /// one.
    persist_txn_tables: PersistTxnTablesImpl,

    /// Arguments for secrets readers.
    secrets_args: SecretsReaderCliArgs,

    /// A map associating a global ID to a vector of all the watch sets
    /// that that ID is part of which have not yet been fulfilled for that object.
    ///
    /// See [`self.install_watch_set`] for a description of watch sets.
    // When a watch set is fulfilled for a given object (that is, when
    // the object's frontier advances to at least the watch set's
    // timestamp), the corresponding entry will be removed from the
    // vector here. That way, when the entire watch set is fulfilled,
    // the corresponding `Rc` will be the last reference to it, and
    // the call to `Rc::into_inner` will succeed.
    objects_to_unfulfilled_watch_sets: BTreeMap<GlobalId, Vec<Rc<(T, Box<dyn Any>)>>>,

    /// A list of watch sets that were already fulfilled as soon as
    /// they were installed, and thus that must be returned to the
    /// client on the next call to [`self.process`].
    ///
    /// See [`self.install_watch_set`] for a description of watch sets.
    immediate_watch_sets: Vec<Box<dyn Any>>,
}

impl<T: ComputeControllerTimestamp> Controller<T> {
    pub fn active_compute(&mut self) -> ActiveComputeController<T> {
        self.compute.activate(&mut *self.storage)
    }

    pub fn set_arrangement_exert_proportionality(&mut self, value: u32) {
        self.compute.set_arrangement_exert_proportionality(value);
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
    pub fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Note: We purposefully use the `Debug` formatting for the value of all fields in the
        // returned object as a tradeoff between usability and stability. `serde_json` will fail
        // to serialize an object if the keys aren't strings, so `Debug` formatting the values
        // prevents a future unrelated change from silently breaking this method.

        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            storage: _,
            compute,
            clusterd_image: _,
            init_container_image: _,
            orchestrator: _,
            readiness,
            metrics_tasks: _,
            metrics_tx: _,
            metrics_rx: _,
            frontiers_ticker: _,
            persist_pubsub_url: _,
            persist_txn_tables: _,
            secrets_args: _,
            objects_to_unfulfilled_watch_sets,
            immediate_watch_sets,
        } = self;

        let objects_to_unfulfilled_watch_sets: BTreeMap<_, _> = objects_to_unfulfilled_watch_sets
            .iter()
            .map(|(id, watches)| (id.to_string(), format!("{watches:?}")))
            .collect();
        let immediate_watch_sets: Vec<_> = immediate_watch_sets
            .iter()
            .map(|watch| format!("{watch:?}"))
            .collect();

        fn field(
            key: &str,
            value: impl Serialize,
        ) -> Result<(String, serde_json::Value), anyhow::Error> {
            let value = serde_json::to_value(value)?;
            Ok((key.to_string(), value))
        }

        let map = serde_json::Map::from_iter([
            field("compute", compute.dump()?)?,
            field("readiness", format!("{readiness:?}"))?,
            field(
                "objects_to_unfulfilled_watch_sets",
                objects_to_unfulfilled_watch_sets,
            )?,
            field("immediate_watch_sets", immediate_watch_sets)?,
        ]);
        Ok(serde_json::Value::Object(map))
    }
}

impl<T> Controller<T>
where
    T: ComputeControllerTimestamp,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub fn update_orchestrator_scheduling_config(
        &mut self,
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
                    _ = Pin::new(&mut self.metrics_rx).peek() => {
                        self.readiness = Readiness::Metrics;
                    }
                    _ = self.frontiers_ticker.tick() => {
                        self.readiness = Readiness::Frontiers;
                    }
                }
            }
        }
    }

    /// Install a _watch set_ in the controller.
    ///
    /// A _watch set_ is a request to be informed by the controller when
    /// all of the frontiers of a particular set of objects have advanced at
    /// least to a particular timestamp.
    ///
    /// When all the objects in `objects` have advanced to `t`, the object
    /// `token` is returned to the client on the next call to [`Self::process`].
    pub fn install_compute_watch_set(
        &mut self,
        mut objects: BTreeSet<GlobalId>,
        t: T,
        token: Box<dyn Any>,
    ) {
        objects.retain(|id| {
            let frontier = self
                .compute
                .find_collection(*id)
                .map(|s| s.write_frontier())
                .expect("missing compute dependency");
            frontier.less_equal(&t)
        });
        if objects.is_empty() {
            self.immediate_watch_sets.push(token);
        } else {
            let state = Rc::new((t, token));
            for id in objects {
                self.objects_to_unfulfilled_watch_sets
                    .entry(id)
                    .or_default()
                    .push(Rc::clone(&state));
            }
        }
    }

    /// Install a _watch set_ in the controller.
    ///
    /// A _watch set_ is a request to be informed by the controller when
    /// all of the frontiers of a particular set of objects have advanced at
    /// least to a particular timestamp.
    ///
    /// When all the objects in `objects` have advanced to `t`, the object
    /// `token` is returned to the client on the next call to [`Self::process`].
    pub fn install_storage_watch_set(
        &mut self,
        mut objects: BTreeSet<GlobalId>,
        t: T,
        token: Box<dyn Any>,
    ) {
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
            self.immediate_watch_sets.push(token);
        } else {
            let state = Rc::new((t, token));
            for id in objects {
                self.objects_to_unfulfilled_watch_sets
                    .entry(id)
                    .or_default()
                    .push(Rc::clone(&state));
            }
        }
    }

    /// Process a pending response from the storage controller. If necessary,
    /// return a higher-level response to our client.
    async fn process_storage_response(
        &mut self,
        storage_metadata: &StorageMetadata,
    ) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        let maybe_response = self.storage.process(storage_metadata).await?;
        Ok(maybe_response.and_then(
            |mz_storage_client::controller::Response::FrontierUpdates(r)| {
                self.handle_frontier_updates(&r)
            },
        ))
    }

    /// Process a pending response from the compute controller. If necessary,
    /// return a higher-level response to our client.
    async fn process_compute_response(
        &mut self,
    ) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        let response = self.active_compute().process().await;

        let response = response.and_then(|r| match r {
            ComputeControllerResponse::PeekResponse(uuid, peek, otel_ctx) => {
                Some(ControllerResponse::PeekResponse(uuid, peek, otel_ctx))
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
    pub async fn process(
        &mut self,
        storage_metadata: &StorageMetadata,
    ) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        match mem::take(&mut self.readiness) {
            Readiness::NotReady => Ok(None),
            Readiness::Storage => self.process_storage_response(storage_metadata).await,
            Readiness::Compute => self.process_compute_response().await,
            Readiness::Metrics => Ok(self
                .metrics_rx
                .next()
                .await
                .map(|(id, metrics)| ControllerResponse::ComputeReplicaMetrics(id, metrics))),
            Readiness::Frontiers => {
                self.record_frontiers().await;
                Ok(None)
            }
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
        for (id, antichain) in updates {
            if let Some(x) = self.objects_to_unfulfilled_watch_sets.get_mut(id) {
                let mut i = 0;
                while i < x.len() {
                    if !antichain.less_equal(&x[i].0) {
                        if let Some((_, token)) = Rc::into_inner(x.swap_remove(i)) {
                            finished.push(token)
                        }
                    } else {
                        i += 1;
                    }
                }
                if x.is_empty() {
                    self.objects_to_unfulfilled_watch_sets.remove(id);
                }
            }
        }
        (!(finished.is_empty())).then(|| ControllerResponse::WatchSetFinished(finished))
    }

    async fn record_frontiers(&mut self) {
        let compute_frontiers = self.compute.collection_frontiers();
        self.storage.record_frontiers(compute_frontiers).await;

        let compute_replica_frontiers = self.compute.replica_write_frontiers();
        self.storage
            .record_replica_frontiers(compute_replica_frontiers)
            .await;
    }

    /// Produces a timestamp that reflects all data available in
    /// `source_ids` at the time of the function call.
    #[allow(unused)]
    #[allow(clippy::unused_async)]
    pub fn recent_timestamp(
        &self,
        source_ids: impl Iterator<Item = GlobalId>,
    ) -> BoxFuture<'static, T> {
        // Dummy implementation
        Box::pin(async { T::minimum() })
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
        + Into<Datum<'static>>,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
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
        // Whether to use the new persist-txn tables implementation or the
        // legacy one.
        persist_txn_tables: PersistTxnTablesImpl,
        storage_txn: &dyn StorageTxn<T>,
    ) -> Self {
        let storage_controller = mz_storage_controller::Controller::new(
            config.build_info,
            config.persist_location,
            config.persist_clients,
            config.now,
            envd_epoch,
            config.metrics_registry.clone(),
            persist_txn_tables,
            config.connection_context,
            storage_txn,
        )
        .await;

        let compute_controller = ComputeController::new(
            config.build_info,
            envd_epoch,
            config.metrics_registry.clone(),
        );
        let (metrics_tx, metrics_rx) = mpsc::unbounded_channel();

        let mut frontiers_ticker = time::interval(Duration::from_secs(1));
        frontiers_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self {
            storage: Box::new(storage_controller),
            compute: compute_controller,
            clusterd_image: config.clusterd_image,
            init_container_image: config.init_container_image,
            orchestrator: config.orchestrator.namespace("cluster"),
            readiness: Readiness::NotReady,
            metrics_tasks: BTreeMap::new(),
            metrics_tx,
            metrics_rx: UnboundedReceiverStream::new(metrics_rx).peekable(),
            frontiers_ticker,
            persist_pubsub_url: config.persist_pubsub_url,
            persist_txn_tables,
            secrets_args: config.secrets_args,
            objects_to_unfulfilled_watch_sets: BTreeMap::new(),
            immediate_watch_sets: Vec::new(),
        }
    }
}
