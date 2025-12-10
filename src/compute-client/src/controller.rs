// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller that provides an interface to the compute layer, and the storage layer below it.
//!
//! The compute controller manages the creation, maintenance, and removal of compute instances.
//! This involves ensuring the intended service state with the orchestrator, as well as maintaining
//! a dedicated compute instance controller for each active compute instance.
//!
//! For each compute instance, the compute controller curates the creation of indexes and sinks
//! installed on the instance, the progress of readers through these collections, and their
//! eventual dropping and resource reclamation.
//!
//! The state maintained for a compute instance can be viewed as a partial map from `GlobalId` to
//! collection. It is an error to use an identifier before it has been "created" with
//! `create_dataflow()`. Once created, the controller holds a read capability for each output
//! collection of a dataflow, which is manipulated with `set_read_policy()`. Eventually, a
//! collection is dropped with `drop_collections()`.
//!
//! A dataflow can be in read-only or read-write mode. In read-only mode, the dataflow does not
//! modify any persistent state. Sending a `allow_write` message to the compute instance will
//! transition the dataflow to read-write mode, allowing it to write to persistent sinks.
//!
//! Created dataflows will prevent the compaction of their inputs, including other compute
//! collections but also collections managed by the storage layer. Each dataflow input is prevented
//! from compacting beyond the allowed compaction of each of its outputs, ensuring that we can
//! recover each dataflow to its current state in case of failure or other reconfiguration.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_client::metrics::ControllerMetrics;
use mz_cluster_client::{ReplicaId, WallclockLagFn};
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::config::ComputeReplicaConfig;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::dyncfgs::COMPUTE_REPLICA_EXPIRATION_OFFSET;
use mz_dyncfg::ConfigSet;
use mz_expr::RowSetFinishing;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_types::PersistLocation;
use mz_repr::{Datum, GlobalId, RelationDesc, Row, TimestampManipulation};
use mz_storage_client::controller::StorageController;
use mz_storage_types::dyncfgs::ORE_OVERFLOWING_BEHAVIOR;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};
use prometheus::proto::LabelPair;
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, MissedTickBehavior};
use uuid::Uuid;

use crate::controller::error::{
    CollectionLookupError, CollectionMissing, CollectionUpdateError, DataflowCreationError,
    HydrationCheckBadTarget, InstanceExists, InstanceMissing, PeekError, ReadPolicyError,
    ReplicaCreationError, ReplicaDropError,
};
use crate::controller::instance::{Instance, SharedCollectionState};
use crate::controller::introspection::{IntrospectionUpdates, spawn_introspection_sink};
use crate::controller::replica::ReplicaConfig;
use crate::logging::{LogVariant, LoggingConfig};
use crate::metrics::ComputeControllerMetrics;
use crate::protocol::command::{ComputeParameters, PeekTarget};
use crate::protocol::response::{PeekResponse, SubscribeBatch};

mod introspection;
mod replica;
mod sequential_hydration;

pub mod error;
pub mod instance;

pub(crate) type StorageCollections<T> = Arc<
    dyn mz_storage_client::storage_collections::StorageCollections<Timestamp = T> + Send + Sync,
>;

/// A composite trait for types that serve as timestamps in the Compute Controller.
/// `Into<Datum<'a>>` is needed for writing timestamps to introspection collections.
pub trait ComputeControllerTimestamp: TimestampManipulation + Into<Datum<'static>> + Sync {}

impl ComputeControllerTimestamp for mz_repr::Timestamp {}

/// Responses from the compute controller.
#[derive(Debug)]
pub enum ComputeControllerResponse<T> {
    /// See [`PeekNotification`].
    PeekNotification(Uuid, PeekNotification, OpenTelemetryContext),
    /// See [`crate::protocol::response::ComputeResponse::SubscribeResponse`].
    SubscribeResponse(GlobalId, SubscribeBatch<T>),
    /// The response from a dataflow containing an `CopyToS3Oneshot` sink.
    ///
    /// The `GlobalId` identifies the sink. The `Result` is the response from
    /// the sink, where an `Ok(n)` indicates that `n` rows were successfully
    /// copied to S3 and an `Err` indicates that an error was encountered
    /// during the copy operation.
    ///
    /// For a given `CopyToS3Oneshot` sink, there will be at most one `CopyToResponse`
    /// produced. (The sink may produce no responses if its dataflow is dropped
    /// before completion.)
    CopyToResponse(GlobalId, Result<u64, anyhow::Error>),
    /// A response reporting advancement of a collection's upper frontier.
    ///
    /// Once a collection's upper (aka "write frontier") has advanced to beyond a given time, the
    /// contents of the collection as of that time have been sealed and cannot change anymore.
    FrontierUpper {
        /// The ID of a compute collection.
        id: GlobalId,
        /// The new upper frontier of the identified compute collection.
        upper: Antichain<T>,
    },
}

/// Notification and summary of a received and forwarded [`crate::protocol::response::ComputeResponse::PeekResponse`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PeekNotification {
    /// Returned rows of a successful peek.
    Success {
        /// Number of rows in the returned peek result.
        rows: u64,
        /// Size of the returned peek result in bytes.
        result_size: u64,
    },
    /// Error of an unsuccessful peek, including the reason for the error.
    Error(String),
    /// The peek was canceled.
    Canceled,
}

impl PeekNotification {
    /// Construct a new [`PeekNotification`] from a [`PeekResponse`]. The `offset` and `limit`
    /// parameters are used to calculate the number of rows in the peek result.
    fn new(peek_response: &PeekResponse, offset: usize, limit: Option<usize>) -> Self {
        match peek_response {
            PeekResponse::Rows(rows) => {
                let num_rows = u64::cast_from(rows.count(offset, limit));
                let result_size = u64::cast_from(rows.byte_len());

                tracing::trace!(?num_rows, ?result_size, "inline peek result");

                Self::Success {
                    rows: num_rows,
                    result_size,
                }
            }
            PeekResponse::Stashed(stashed_response) => {
                let rows = stashed_response.num_rows(offset, limit);
                let result_size = stashed_response.size_bytes();

                tracing::trace!(?rows, ?result_size, "stashed peek result");

                Self::Success {
                    rows: u64::cast_from(rows),
                    result_size: u64::cast_from(result_size),
                }
            }
            PeekResponse::Error(err) => Self::Error(err.clone()),
            PeekResponse::Canceled => Self::Canceled,
        }
    }
}

/// A controller for the compute layer.
pub struct ComputeController<T: ComputeControllerTimestamp> {
    instances: BTreeMap<ComputeInstanceId, InstanceState<T>>,
    /// A map from an instance ID to an arbitrary string that describes the
    /// class of the workload that compute instance is running (e.g.,
    /// `production` or `staging`).
    instance_workload_classes: Arc<Mutex<BTreeMap<ComputeInstanceId, Option<String>>>>,
    build_info: &'static BuildInfo,
    /// A handle providing access to storage collections.
    storage_collections: StorageCollections<T>,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// Whether or not this controller is in read-only mode.
    ///
    /// When in read-only mode, neither this controller nor the instances
    /// controlled by it are allowed to affect changes to external systems
    /// (largely persist).
    read_only: bool,
    /// Compute configuration to apply to new instances.
    config: ComputeParameters,
    /// The persist location where we can stash large peek results.
    peek_stash_persist_location: PersistLocation,
    /// A controller response to be returned on the next call to [`ComputeController::process`].
    stashed_response: Option<ComputeControllerResponse<T>>,
    /// The compute controller metrics.
    metrics: ComputeControllerMetrics,
    /// A function that produces the current wallclock time.
    now: NowFn,
    /// A function that computes the lag between the given time and wallclock time.
    wallclock_lag: WallclockLagFn<T>,
    /// Dynamic system configuration.
    ///
    /// Updated through `ComputeController::update_configuration` calls and shared with all
    /// subcomponents of the compute controller.
    dyncfg: Arc<ConfigSet>,

    /// Receiver for responses produced by `Instance`s.
    response_rx: mpsc::UnboundedReceiver<ComputeControllerResponse<T>>,
    /// Response sender that's passed to new `Instance`s.
    response_tx: mpsc::UnboundedSender<ComputeControllerResponse<T>>,
    /// Receiver for introspection updates produced by `Instance`s.
    ///
    /// When [`ComputeController::start_introspection_sink`] is first called, this receiver is
    /// passed to the introspection sink task.
    introspection_rx: Option<mpsc::UnboundedReceiver<IntrospectionUpdates>>,
    /// Introspection updates sender that's passed to new `Instance`s.
    introspection_tx: mpsc::UnboundedSender<IntrospectionUpdates>,

    /// Ticker for scheduling periodic maintenance work.
    maintenance_ticker: tokio::time::Interval,
    /// Whether maintenance work was scheduled.
    maintenance_scheduled: bool,
}

impl<T: ComputeControllerTimestamp> ComputeController<T> {
    /// Construct a new [`ComputeController`].
    pub fn new(
        build_info: &'static BuildInfo,
        storage_collections: StorageCollections<T>,
        read_only: bool,
        metrics_registry: &MetricsRegistry,
        peek_stash_persist_location: PersistLocation,
        controller_metrics: ControllerMetrics,
        now: NowFn,
        wallclock_lag: WallclockLagFn<T>,
    ) -> Self {
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        let (introspection_tx, introspection_rx) = mpsc::unbounded_channel();

        let mut maintenance_ticker = time::interval(Duration::from_secs(1));
        maintenance_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let instance_workload_classes = Arc::new(Mutex::new(BTreeMap::<
            ComputeInstanceId,
            Option<String>,
        >::new()));

        // Apply a `workload_class` label to all metrics in the registry that
        // have an `instance_id` label for an instance whose workload class is
        // known.
        metrics_registry.register_postprocessor({
            let instance_workload_classes = Arc::clone(&instance_workload_classes);
            move |metrics| {
                let instance_workload_classes = instance_workload_classes
                    .lock()
                    .expect("lock poisoned")
                    .iter()
                    .map(|(id, workload_class)| (id.to_string(), workload_class.clone()))
                    .collect::<BTreeMap<String, Option<String>>>();
                for metric in metrics {
                    'metric: for metric in metric.mut_metric() {
                        for label in metric.get_label() {
                            if label.name() == "instance_id" {
                                if let Some(workload_class) = instance_workload_classes
                                    .get(label.value())
                                    .cloned()
                                    .flatten()
                                {
                                    let mut label = LabelPair::default();
                                    label.set_name("workload_class".into());
                                    label.set_value(workload_class.clone());

                                    let mut labels = metric.take_label();
                                    labels.push(label);
                                    metric.set_label(labels);
                                }
                                continue 'metric;
                            }
                        }
                    }
                }
            }
        });

        let metrics = ComputeControllerMetrics::new(metrics_registry, controller_metrics);

        Self {
            instances: BTreeMap::new(),
            instance_workload_classes,
            build_info,
            storage_collections,
            initialized: false,
            read_only,
            config: Default::default(),
            peek_stash_persist_location,
            stashed_response: None,
            metrics,
            now,
            wallclock_lag,
            dyncfg: Arc::new(mz_dyncfgs::all_dyncfgs()),
            response_rx,
            response_tx,
            introspection_rx: Some(introspection_rx),
            introspection_tx,
            maintenance_ticker,
            maintenance_scheduled: false,
        }
    }

    /// Start sinking the compute controller's introspection data into storage.
    ///
    /// This method should be called once the introspection collections have been registered with
    /// the storage controller. It will panic if invoked earlier than that.
    pub fn start_introspection_sink(
        &mut self,
        storage_controller: &dyn StorageController<Timestamp = T>,
    ) {
        if let Some(rx) = self.introspection_rx.take() {
            spawn_introspection_sink(rx, storage_controller);
        }
    }

    /// TODO(database-issues#7533): Add documentation.
    pub fn instance_exists(&self, id: ComputeInstanceId) -> bool {
        self.instances.contains_key(&id)
    }

    /// Return a reference to the indicated compute instance.
    fn instance(&self, id: ComputeInstanceId) -> Result<&InstanceState<T>, InstanceMissing> {
        self.instances.get(&id).ok_or(InstanceMissing(id))
    }

    /// Return an `instance::Client` for the indicated compute instance.
    pub fn instance_client(
        &self,
        id: ComputeInstanceId,
    ) -> Result<instance::Client<T>, InstanceMissing> {
        self.instance(id).map(|instance| instance.client.clone())
    }

    /// Return a mutable reference to the indicated compute instance.
    fn instance_mut(
        &mut self,
        id: ComputeInstanceId,
    ) -> Result<&mut InstanceState<T>, InstanceMissing> {
        self.instances.get_mut(&id).ok_or(InstanceMissing(id))
    }

    /// List the IDs of all collections in the identified compute instance.
    pub fn collection_ids(
        &self,
        instance_id: ComputeInstanceId,
    ) -> Result<impl Iterator<Item = GlobalId> + '_, InstanceMissing> {
        let instance = self.instance(instance_id)?;
        let ids = instance.collections.keys().copied();
        Ok(ids)
    }

    /// Return the frontiers of the indicated collection.
    ///
    /// If an `instance_id` is provided, the collection is assumed to be installed on that
    /// instance. Otherwise all available instances are searched.
    pub fn collection_frontiers(
        &self,
        collection_id: GlobalId,
        instance_id: Option<ComputeInstanceId>,
    ) -> Result<CollectionFrontiers<T>, CollectionLookupError> {
        let collection = match instance_id {
            Some(id) => self.instance(id)?.collection(collection_id)?,
            None => self
                .instances
                .values()
                .find_map(|i| i.collections.get(&collection_id))
                .ok_or(CollectionMissing(collection_id))?,
        };

        Ok(collection.frontiers())
    }

    /// List compute collections that depend on the given collection.
    pub fn collection_reverse_dependencies(
        &self,
        instance_id: ComputeInstanceId,
        id: GlobalId,
    ) -> Result<impl Iterator<Item = GlobalId> + '_, InstanceMissing> {
        let instance = self.instance(instance_id)?;
        let collections = instance.collections.iter();
        let ids = collections
            .filter_map(move |(cid, c)| c.compute_dependencies.contains(&id).then_some(*cid));
        Ok(ids)
    }

    /// Returns `true` iff the given collection has been hydrated.
    ///
    /// For this check, zero-replica clusters are always considered hydrated.
    /// Their collections would never normally be considered hydrated but it's
    /// clearly intentional that they have no replicas.
    pub async fn collection_hydrated(
        &self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
    ) -> Result<bool, anyhow::Error> {
        let instance = self.instance(instance_id)?;

        let res = instance
            .call_sync(move |i| i.collection_hydrated(collection_id))
            .await?;

        Ok(res)
    }

    /// Returns `true` if all non-transient, non-excluded collections are hydrated on any of the
    /// provided replicas.
    ///
    /// For this check, zero-replica clusters are always considered hydrated.
    /// Their collections would never normally be considered hydrated but it's
    /// clearly intentional that they have no replicas.
    pub fn collections_hydrated_for_replicas(
        &self,
        instance_id: ComputeInstanceId,
        replicas: Vec<ReplicaId>,
        exclude_collections: BTreeSet<GlobalId>,
    ) -> Result<oneshot::Receiver<bool>, anyhow::Error> {
        let instance = self.instance(instance_id)?;

        // Validation
        if instance.replicas.is_empty() && !replicas.iter().any(|id| instance.replicas.contains(id))
        {
            return Err(HydrationCheckBadTarget(replicas).into());
        }

        let (tx, rx) = oneshot::channel();
        instance.call(move |i| {
            let result = i
                .collections_hydrated_on_replicas(Some(replicas), &exclude_collections)
                .expect("validated");
            let _ = tx.send(result);
        });

        Ok(rx)
    }

    /// Returns the state of the [`ComputeController`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    pub async fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Note: We purposefully use the `Debug` formatting for the value of all fields in the
        // returned object as a tradeoff between usability and stability. `serde_json` will fail
        // to serialize an object if the keys aren't strings, so `Debug` formatting the values
        // prevents a future unrelated change from silently breaking this method.

        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            instances,
            instance_workload_classes,
            build_info: _,
            storage_collections: _,
            initialized,
            read_only,
            config: _,
            peek_stash_persist_location: _,
            stashed_response,
            metrics: _,
            now: _,
            wallclock_lag: _,
            dyncfg: _,
            response_rx: _,
            response_tx: _,
            introspection_rx: _,
            introspection_tx: _,
            maintenance_ticker: _,
            maintenance_scheduled,
        } = self;

        let mut instances_dump = BTreeMap::new();
        for (id, instance) in instances {
            let dump = instance.dump().await?;
            instances_dump.insert(id.to_string(), dump);
        }

        let instance_workload_classes: BTreeMap<_, _> = instance_workload_classes
            .lock()
            .expect("lock poisoned")
            .iter()
            .map(|(id, wc)| (id.to_string(), format!("{wc:?}")))
            .collect();

        Ok(serde_json::json!({
            "instances": instances_dump,
            "instance_workload_classes": instance_workload_classes,
            "initialized": initialized,
            "read_only": read_only,
            "stashed_response": format!("{stashed_response:?}"),
            "maintenance_scheduled": maintenance_scheduled,
        }))
    }
}

impl<T> ComputeController<T>
where
    T: ComputeControllerTimestamp,
{
    /// Create a compute instance.
    pub fn create_instance(
        &mut self,
        id: ComputeInstanceId,
        arranged_logs: BTreeMap<LogVariant, GlobalId>,
        workload_class: Option<String>,
    ) -> Result<(), InstanceExists> {
        if self.instances.contains_key(&id) {
            return Err(InstanceExists(id));
        }

        let mut collections = BTreeMap::new();
        let mut logs = Vec::with_capacity(arranged_logs.len());
        for (&log, &id) in &arranged_logs {
            let collection = Collection::new_log();
            let shared = collection.shared.clone();
            collections.insert(id, collection);
            logs.push((log, id, shared));
        }

        let client = instance::Client::spawn(
            id,
            self.build_info,
            Arc::clone(&self.storage_collections),
            self.peek_stash_persist_location.clone(),
            logs,
            self.metrics.for_instance(id),
            self.now.clone(),
            self.wallclock_lag.clone(),
            Arc::clone(&self.dyncfg),
            self.response_tx.clone(),
            self.introspection_tx.clone(),
            self.read_only,
        );

        let instance = InstanceState::new(client, collections);
        self.instances.insert(id, instance);

        self.instance_workload_classes
            .lock()
            .expect("lock poisoned")
            .insert(id, workload_class.clone());

        let instance = self.instances.get_mut(&id).expect("instance just added");
        if self.initialized {
            instance.call(Instance::initialization_complete);
        }

        let mut config_params = self.config.clone();
        config_params.workload_class = Some(workload_class);
        instance.call(|i| i.update_configuration(config_params));

        Ok(())
    }

    /// Updates a compute instance's workload class.
    pub fn update_instance_workload_class(
        &mut self,
        id: ComputeInstanceId,
        workload_class: Option<String>,
    ) -> Result<(), InstanceMissing> {
        // Ensure that the instance exists first.
        let _ = self.instance(id)?;

        self.instance_workload_classes
            .lock()
            .expect("lock poisoned")
            .insert(id, workload_class);

        // Cause a config update to notify the instance about its new workload class.
        self.update_configuration(Default::default());

        Ok(())
    }

    /// Remove a compute instance.
    ///
    /// # Panics
    ///
    /// Panics if the identified `instance` still has active replicas.
    pub fn drop_instance(&mut self, id: ComputeInstanceId) {
        if let Some(instance) = self.instances.remove(&id) {
            instance.call(|i| i.shutdown());
        }

        self.instance_workload_classes
            .lock()
            .expect("lock poisoned")
            .remove(&id);
    }

    /// Returns the compute controller's config set.
    pub fn dyncfg(&self) -> &Arc<ConfigSet> {
        &self.dyncfg
    }

    /// Update compute configuration.
    pub fn update_configuration(&mut self, config_params: ComputeParameters) {
        // Apply dyncfg updates.
        config_params.dyncfg_updates.apply(&self.dyncfg);

        let instance_workload_classes = self
            .instance_workload_classes
            .lock()
            .expect("lock poisoned");

        // Forward updates to existing clusters.
        // Workload classes are cluster-specific, so we need to overwrite them here.
        for (id, instance) in self.instances.iter_mut() {
            let mut params = config_params.clone();
            params.workload_class = Some(instance_workload_classes[id].clone());
            instance.call(|i| i.update_configuration(params));
        }

        let overflowing_behavior = ORE_OVERFLOWING_BEHAVIOR.get(&self.dyncfg);
        match overflowing_behavior.parse() {
            Ok(behavior) => mz_ore::overflowing::set_behavior(behavior),
            Err(err) => {
                tracing::error!(
                    err,
                    overflowing_behavior,
                    "Invalid value for ore_overflowing_behavior"
                );
            }
        }

        // Remember updates for future clusters.
        self.config.update(config_params);
    }

    /// Mark the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.initialized = true;
        for instance in self.instances.values_mut() {
            instance.call(Instance::initialization_complete);
        }
    }

    /// Wait until the controller is ready to do some processing.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the caller should call [`ComputeController::process`].
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if self.stashed_response.is_some() {
            // We still have a response stashed, which we are immediately ready to process.
            return;
        }
        if self.maintenance_scheduled {
            // Maintenance work has been scheduled.
            return;
        }

        tokio::select! {
            resp = self.response_rx.recv() => {
                let resp = resp.expect("`self.response_tx` not dropped");
                self.stashed_response = Some(resp);
            }
            _ = self.maintenance_ticker.tick() => {
                self.maintenance_scheduled = true;
            },
        }
    }

    /// Adds replicas of an instance.
    pub fn add_replica_to_instance(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        location: ClusterReplicaLocation,
        config: ComputeReplicaConfig,
    ) -> Result<(), ReplicaCreationError> {
        use ReplicaCreationError::*;

        let instance = self.instance(instance_id)?;

        // Validation
        if instance.replicas.contains(&replica_id) {
            return Err(ReplicaExists(replica_id));
        }

        let (enable_logging, interval) = match config.logging.interval {
            Some(interval) => (true, interval),
            None => (false, Duration::from_secs(1)),
        };

        let expiration_offset = COMPUTE_REPLICA_EXPIRATION_OFFSET.get(&self.dyncfg);

        let replica_config = ReplicaConfig {
            location,
            logging: LoggingConfig {
                interval,
                enable_logging,
                log_logging: config.logging.log_logging,
                index_logs: Default::default(),
            },
            grpc_client: self.config.grpc_client.clone(),
            expiration_offset: (!expiration_offset.is_zero()).then_some(expiration_offset),
        };

        let instance = self.instance_mut(instance_id).expect("validated");
        instance.replicas.insert(replica_id);

        instance.call(move |i| {
            i.add_replica(replica_id, replica_config, None)
                .expect("validated")
        });

        Ok(())
    }

    /// Removes a replica from an instance, including its service in the orchestrator.
    pub fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
    ) -> Result<(), ReplicaDropError> {
        use ReplicaDropError::*;

        let instance = self.instance_mut(instance_id)?;

        // Validation
        if !instance.replicas.contains(&replica_id) {
            return Err(ReplicaMissing(replica_id));
        }

        instance.replicas.remove(&replica_id);

        instance.call(move |i| i.remove_replica(replica_id).expect("validated"));

        Ok(())
    }

    /// Creates the described dataflow and initializes state for its output.
    ///
    /// If a `subscribe_target_replica` is given, any subscribes exported by the dataflow are
    /// configured to target that replica, i.e., only subscribe responses sent by that replica are
    /// considered.
    pub fn create_dataflow(
        &mut self,
        instance_id: ComputeInstanceId,
        mut dataflow: DataflowDescription<mz_compute_types::plan::Plan<T>, (), T>,
        subscribe_target_replica: Option<ReplicaId>,
    ) -> Result<(), DataflowCreationError> {
        use DataflowCreationError::*;

        let instance = self.instance(instance_id)?;

        // Validation: target replica
        if let Some(replica_id) = subscribe_target_replica {
            if !instance.replicas.contains(&replica_id) {
                return Err(ReplicaMissing(replica_id));
            }
        }

        // Validation: as_of
        let as_of = dataflow.as_of.as_ref().ok_or(MissingAsOf)?;
        if as_of.is_empty() && dataflow.subscribe_ids().next().is_some() {
            return Err(EmptyAsOfForSubscribe);
        }
        if as_of.is_empty() && dataflow.copy_to_ids().next().is_some() {
            return Err(EmptyAsOfForCopyTo);
        }

        // Validation: input collections
        let storage_ids = dataflow.imported_source_ids().collect();
        let mut import_read_holds = self.storage_collections.acquire_read_holds(storage_ids)?;
        for id in dataflow.imported_index_ids() {
            let read_hold = instance.acquire_read_hold(id)?;
            import_read_holds.push(read_hold);
        }
        for hold in &import_read_holds {
            if PartialOrder::less_than(as_of, hold.since()) {
                return Err(SinceViolation(hold.id()));
            }
        }

        // Validation: storage sink collections
        for id in dataflow.persist_sink_ids() {
            if self.storage_collections.check_exists(id).is_err() {
                return Err(CollectionMissing(id));
            }
        }
        let time_dependence = self
            .determine_time_dependence(instance_id, &dataflow)
            .expect("must exist");

        let instance = self.instance_mut(instance_id).expect("validated");

        let mut shared_collection_state = BTreeMap::new();
        for id in dataflow.export_ids() {
            let shared = SharedCollectionState::new(as_of.clone());
            let collection = Collection {
                write_only: dataflow.sink_exports.contains_key(&id),
                compute_dependencies: dataflow.imported_index_ids().collect(),
                shared: shared.clone(),
                time_dependence: time_dependence.clone(),
            };
            instance.collections.insert(id, collection);
            shared_collection_state.insert(id, shared);
        }

        dataflow.time_dependence = time_dependence;

        instance.call(move |i| {
            i.create_dataflow(
                dataflow,
                import_read_holds,
                subscribe_target_replica,
                shared_collection_state,
            )
            .expect("validated")
        });

        Ok(())
    }

    /// Drop the read capability for the given collections and allow their resources to be
    /// reclaimed.
    pub fn drop_collections(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_ids: Vec<GlobalId>,
    ) -> Result<(), CollectionUpdateError> {
        let instance = self.instance_mut(instance_id)?;

        // Validation
        for id in &collection_ids {
            instance.collection(*id)?;
        }

        for id in &collection_ids {
            instance.collections.remove(id);
        }

        instance.call(|i| i.drop_collections(collection_ids).expect("validated"));

        Ok(())
    }

    /// Initiate a peek request for the contents of the given collection at `timestamp`.
    pub fn peek(
        &self,
        instance_id: ComputeInstanceId,
        peek_target: PeekTarget,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        result_desc: RelationDesc,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
        peek_response_tx: oneshot::Sender<PeekResponse>,
    ) -> Result<(), PeekError> {
        use PeekError::*;

        let instance = self.instance(instance_id)?;

        // Validation: target replica
        if let Some(replica_id) = target_replica {
            if !instance.replicas.contains(&replica_id) {
                return Err(ReplicaMissing(replica_id));
            }
        }

        // Validation: peek target
        let read_hold = match &peek_target {
            PeekTarget::Index { id } => instance.acquire_read_hold(*id)?,
            PeekTarget::Persist { id, .. } => self
                .storage_collections
                .acquire_read_holds(vec![*id])?
                .into_element(),
        };
        if !read_hold.since().less_equal(&timestamp) {
            return Err(SinceViolation(peek_target.id()));
        }

        instance.call(move |i| {
            i.peek(
                peek_target,
                literal_constraints,
                uuid,
                timestamp,
                result_desc,
                finishing,
                map_filter_project,
                read_hold,
                target_replica,
                peek_response_tx,
            )
            .expect("validated")
        });

        Ok(())
    }

    /// Cancel an existing peek request.
    ///
    /// Canceling a peek is best effort. The caller may see any of the following
    /// after canceling a peek request:
    ///
    ///   * A `PeekResponse::Rows` indicating that the cancellation request did
    ///     not take effect in time and the query succeeded.
    ///   * A `PeekResponse::Canceled` affirming that the peek was canceled.
    ///   * No `PeekResponse` at all.
    pub fn cancel_peek(
        &self,
        instance_id: ComputeInstanceId,
        uuid: Uuid,
        reason: PeekResponse,
    ) -> Result<(), InstanceMissing> {
        self.instance(instance_id)?
            .call(move |i| i.cancel_peek(uuid, reason));
        Ok(())
    }

    /// Assign a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    ///
    /// It is an error to attempt to set a read policy for a collection that is not readable in the
    /// context of compute. At this time, only indexes are readable compute collections.
    pub fn set_read_policy(
        &self,
        instance_id: ComputeInstanceId,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), ReadPolicyError> {
        use ReadPolicyError::*;

        let instance = self.instance(instance_id)?;

        // Validation
        for (id, _) in &policies {
            let collection = instance.collection(*id)?;
            if collection.write_only {
                return Err(WriteOnlyCollection(*id));
            }
        }

        self.instance(instance_id)?
            .call(|i| i.set_read_policy(policies).expect("validated"));

        Ok(())
    }

    /// Acquires a [`ReadHold`] for the identified compute collection.
    pub fn acquire_read_hold(
        &self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
    ) -> Result<ReadHold<T>, CollectionUpdateError> {
        let read_hold = self
            .instance(instance_id)?
            .acquire_read_hold(collection_id)?;
        Ok(read_hold)
    }

    /// Determine the time dependence for a dataflow.
    fn determine_time_dependence(
        &self,
        instance_id: ComputeInstanceId,
        dataflow: &DataflowDescription<mz_compute_types::plan::Plan<T>, (), T>,
    ) -> Result<Option<TimeDependence>, TimeDependenceError> {
        // TODO(ct3): Continual tasks don't support replica expiration
        let is_continual_task = dataflow.continual_task_ids().next().is_some();
        if is_continual_task {
            return Ok(None);
        }

        let instance = self
            .instance(instance_id)
            .map_err(|err| TimeDependenceError::InstanceMissing(err.0))?;
        let mut time_dependencies = Vec::new();

        for id in dataflow.imported_index_ids() {
            let dependence = instance
                .get_time_dependence(id)
                .map_err(|err| TimeDependenceError::CollectionMissing(err.0))?;
            time_dependencies.push(dependence);
        }

        'source: for id in dataflow.imported_source_ids() {
            // We first check whether the id is backed by a compute object, in which case we use
            // the time dependence we know. This is true for materialized views, continual tasks,
            // etc.
            for instance in self.instances.values() {
                if let Ok(dependence) = instance.get_time_dependence(id) {
                    time_dependencies.push(dependence);
                    continue 'source;
                }
            }

            // Not a compute object: Consult the storage collections controller.
            time_dependencies.push(self.storage_collections.determine_time_dependence(id)?);
        }

        Ok(TimeDependence::merge(
            time_dependencies,
            dataflow.refresh_schedule.as_ref(),
        ))
    }

    /// Processes the work queued by [`ComputeController::ready`].
    #[mz_ore::instrument(level = "debug")]
    pub fn process(&mut self) -> Option<ComputeControllerResponse<T>> {
        // Perform periodic maintenance work.
        if self.maintenance_scheduled {
            self.maintain();
            self.maintenance_scheduled = false;
        }

        // Return a ready response, if any.
        self.stashed_response.take()
    }

    #[mz_ore::instrument(level = "debug")]
    fn maintain(&mut self) {
        // Perform instance maintenance work.
        for instance in self.instances.values_mut() {
            instance.call(Instance::maintain);
        }
    }

    /// Allow writes for the specified collections on `instance_id`.
    ///
    /// If this controller is in read-only mode, this is a no-op.
    pub fn allow_writes(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
    ) -> Result<(), CollectionUpdateError> {
        if self.read_only {
            tracing::debug!("Skipping allow_writes in read-only mode");
            return Ok(());
        }

        let instance = self.instance_mut(instance_id)?;

        // Validation
        instance.collection(collection_id)?;

        instance.call(move |i| i.allow_writes(collection_id).expect("validated"));

        Ok(())
    }
}

#[derive(Debug)]
struct InstanceState<T: ComputeControllerTimestamp> {
    client: instance::Client<T>,
    replicas: BTreeSet<ReplicaId>,
    collections: BTreeMap<GlobalId, Collection<T>>,
}

impl<T: ComputeControllerTimestamp> InstanceState<T> {
    fn new(client: instance::Client<T>, collections: BTreeMap<GlobalId, Collection<T>>) -> Self {
        Self {
            client,
            replicas: Default::default(),
            collections,
        }
    }

    fn collection(&self, id: GlobalId) -> Result<&Collection<T>, CollectionMissing> {
        self.collections.get(&id).ok_or(CollectionMissing(id))
    }

    /// Calls the given function on the instance task. Does not await the result.
    ///
    /// # Panics
    ///
    /// Panics if the instance corresponding to `self` does not exist.
    fn call<F>(&self, f: F)
    where
        F: FnOnce(&mut Instance<T>) + Send + 'static,
    {
        self.client.call(f).expect("instance not dropped")
    }

    /// Calls the given function on the instance task, and awaits the result.
    ///
    /// # Panics
    ///
    /// Panics if the instance corresponding to `self` does not exist.
    async fn call_sync<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Instance<T>) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.client
            .call_sync(f)
            .await
            .expect("instance not dropped")
    }

    /// Acquires a [`ReadHold`] for the identified compute collection.
    pub fn acquire_read_hold(&self, id: GlobalId) -> Result<ReadHold<T>, CollectionMissing> {
        // We acquire read holds at the earliest possible time rather than returning a copy
        // of the implied read hold. This is so that in `create_dataflow` we can acquire read holds
        // on compute dependencies at frontiers that are held back by other read holds the caller
        // has previously taken.
        //
        // If/when we change the compute API to expect callers to pass in the `ReadHold`s rather
        // than acquiring them ourselves, we might tighten this up and instead acquire read holds
        // at the implied capability.

        let collection = self.collection(id)?;
        let since = collection.shared.lock_read_capabilities(|caps| {
            let since = caps.frontier().to_owned();
            caps.update_iter(since.iter().map(|t| (t.clone(), 1)));
            since
        });

        let hold = ReadHold::new(id, since, self.client.read_hold_tx());
        Ok(hold)
    }

    /// Return the stored time dependence for a collection.
    fn get_time_dependence(
        &self,
        id: GlobalId,
    ) -> Result<Option<TimeDependence>, CollectionMissing> {
        Ok(self.collection(id)?.time_dependence.clone())
    }

    /// Returns the [`InstanceState`] formatted as JSON.
    pub async fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            client: _,
            replicas,
            collections,
        } = self;

        let instance = self.call_sync(|i| i.dump()).await?;
        let replicas: Vec<_> = replicas.iter().map(|id| id.to_string()).collect();
        let collections: BTreeMap<_, _> = collections
            .iter()
            .map(|(id, c)| (id.to_string(), format!("{c:?}")))
            .collect();

        Ok(serde_json::json!({
            "instance": instance,
            "replicas": replicas,
            "collections": collections,
        }))
    }
}

#[derive(Debug)]
struct Collection<T> {
    write_only: bool,
    compute_dependencies: BTreeSet<GlobalId>,
    shared: SharedCollectionState<T>,
    /// The computed time dependence for this collection. None indicates no specific information,
    /// a value describes how the collection relates to wall-clock time.
    time_dependence: Option<TimeDependence>,
}

impl<T: Timestamp> Collection<T> {
    fn new_log() -> Self {
        let as_of = Antichain::from_elem(T::minimum());
        Self {
            write_only: false,
            compute_dependencies: Default::default(),
            shared: SharedCollectionState::new(as_of),
            time_dependence: Some(TimeDependence::default()),
        }
    }

    fn frontiers(&self) -> CollectionFrontiers<T> {
        let read_frontier = self
            .shared
            .lock_read_capabilities(|c| c.frontier().to_owned());
        let write_frontier = self.shared.lock_write_frontier(|f| f.clone());
        CollectionFrontiers {
            read_frontier,
            write_frontier,
        }
    }
}

/// The frontiers of a compute collection.
#[derive(Clone, Debug)]
pub struct CollectionFrontiers<T> {
    /// The read frontier.
    pub read_frontier: Antichain<T>,
    /// The write frontier.
    pub write_frontier: Antichain<T>,
}

impl<T: Timestamp> Default for CollectionFrontiers<T> {
    fn default() -> Self {
        Self {
            read_frontier: Antichain::from_elem(T::minimum()),
            write_frontier: Antichain::from_elem(T::minimum()),
        }
    }
}
