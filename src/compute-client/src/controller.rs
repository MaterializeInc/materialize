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
//! Created dataflows will prevent the compaction of their inputs, including other compute
//! collections but also collections managed by the storage layer. Each dataflow input is prevented
//! from compacting beyond the allowed compaction of each of its outputs, ensuring that we can
//! recover each dataflow to its current state in case of failure or other reconfiguration.

use std::collections::BTreeMap;
use std::num::NonZeroI64;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use differential_dataflow::consolidation::consolidate;
use futures::{future, Future, FutureExt};
use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_cluster_client::ReplicaId;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::ComputeInstanceId;
use mz_dyncfg::ConfigSet;
use mz_expr::RowSetFinishing;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{soft_assert_or_log, soft_panic_or_log};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{Datum, Diff, GlobalId, Row, TimestampManipulation};
use mz_storage_client::controller::{IntrospectionType, StorageController, StorageWriteOp};
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::read_policy::ReadPolicy;
use prometheus::proto::LabelPair;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use tokio::sync::mpsc;
use tokio::time::{self, MissedTickBehavior};
use tracing::warn;
use uuid::Uuid;

use crate::controller::error::{
    CollectionLookupError, CollectionMissing, CollectionUpdateError, DataflowCreationError,
    InstanceExists, InstanceMissing, PeekError, ReadPolicyError, ReplicaCreationError,
    ReplicaDropError, SubscribeTargetError,
};
use crate::controller::instance::Instance;
use crate::controller::replica::ReplicaConfig;
use crate::logging::{LogVariant, LoggingConfig};
use crate::metrics::ComputeControllerMetrics;
use crate::protocol::command::{ComputeParameters, PeekTarget};
use crate::protocol::response::{ComputeResponse, PeekResponse, SubscribeBatch};
use crate::service::{ComputeClient, ComputeGrpcClient};

mod instance;
mod replica;
mod sequential_hydration;

pub mod error;

type IntrospectionUpdates = (IntrospectionType, Vec<(Row, Diff)>);
type WallclockLagFn<T> = Arc<dyn Fn(&T) -> Duration>;

/// A composite trait for types that serve as timestamps in the Compute Controller.
/// `Into<Datum<'a>>` is needed for writing timestamps to introspection collections.
pub trait ComputeControllerTimestamp: TimestampManipulation + Into<Datum<'static>> {}

impl ComputeControllerTimestamp for mz_repr::Timestamp {}

/// Responses from the compute controller.
#[derive(Debug)]
pub enum ComputeControllerResponse<T> {
    /// See [`ComputeResponse::PeekResponse`].
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// See [`ComputeResponse::SubscribeResponse`].
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

/// Replica configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComputeReplicaConfig {
    /// TODO(#25239): Add documentation.
    pub logging: ComputeReplicaLogging,
}

/// Logging configuration of a replica.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ComputeReplicaLogging {
    /// Whether to enable logging for the logging dataflows.
    pub log_logging: bool,
    /// The interval at which to log.
    ///
    /// A `None` value indicates that logging is disabled.
    pub interval: Option<Duration>,
}

impl ComputeReplicaLogging {
    /// Return whether logging is enabled.
    pub fn enabled(&self) -> bool {
        self.interval.is_some()
    }
}

/// A controller for the compute layer.
pub struct ComputeController<T: Timestamp> {
    instances: BTreeMap<ComputeInstanceId, Instance<T>>,
    /// A map from an instance ID to an arbitrary string that describes the
    /// class of the workload that compute instance is running (e.g.,
    /// `production` or `staging`).
    instance_workload_classes: Arc<Mutex<BTreeMap<ComputeInstanceId, Option<String>>>>,
    build_info: &'static BuildInfo,
    /// A handle providing access to storage collections.
    storage_collections: Arc<dyn StorageCollections<Timestamp = T>>,
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
    /// `arrangement_exert_proportionality` value passed to new replicas.
    arrangement_exert_proportionality: u32,
    /// A replica response to be handled by the corresponding `Instance` on a subsequent call to
    /// [`ComputeController::process`].
    stashed_replica_response: Option<(ComputeInstanceId, ReplicaId, ComputeResponse<T>)>,
    /// A number that increases on every `environmentd` restart.
    envd_epoch: NonZeroI64,
    /// The compute controller metrics.
    metrics: ComputeControllerMetrics,
    /// A function that compute the lag between the given time and wallclock time.
    wallclock_lag: WallclockLagFn<T>,
    /// Dynamic system configuration.
    ///
    /// Updated through `ComputeController::update_configuration` calls and shared with all
    /// subcomponents of the compute controller.
    dyncfg: Arc<ConfigSet>,

    /// Receiver for responses produced by `Instance`s, to be delivered on subsequent calls to
    /// [`ComputeController::process`].
    response_rx: crossbeam_channel::Receiver<ComputeControllerResponse<T>>,
    /// Response sender that's passed to new `Instance`s.
    response_tx: crossbeam_channel::Sender<ComputeControllerResponse<T>>,
    /// Receiver for introspection updates produced by `Instance`s.
    introspection_rx: crossbeam_channel::Receiver<IntrospectionUpdates>,
    /// Introspection updates sender that's passed to new `Instance`s.
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,

    /// Ticker for scheduling periodic maintenance work.
    maintenance_ticker: tokio::time::Interval,
    /// Whether maintenance work was scheduled.
    maintenance_scheduled: bool,
}

impl<T: ComputeControllerTimestamp> ComputeController<T> {
    /// Construct a new [`ComputeController`].
    pub fn new(
        build_info: &'static BuildInfo,
        storage_collections: Arc<dyn StorageCollections<Timestamp = T>>,
        envd_epoch: NonZeroI64,
        read_only: bool,
        metrics_registry: MetricsRegistry,
        wallclock_lag: WallclockLagFn<T>,
    ) -> Self {
        let (response_tx, response_rx) = crossbeam_channel::unbounded();
        let (introspection_tx, introspection_rx) = crossbeam_channel::unbounded();

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
                            if label.get_name() == "instance_id" {
                                if let Some(workload_class) = instance_workload_classes
                                    .get(label.get_value())
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

        Self {
            instances: BTreeMap::new(),
            instance_workload_classes,
            build_info,
            storage_collections,
            initialized: false,
            read_only,
            config: Default::default(),
            arrangement_exert_proportionality: 16,
            stashed_replica_response: None,
            envd_epoch,
            metrics: ComputeControllerMetrics::new(metrics_registry),
            wallclock_lag,
            dyncfg: Arc::new(mz_dyncfgs::all_dyncfgs()),
            response_rx,
            response_tx,
            introspection_rx,
            introspection_tx,
            maintenance_ticker,
            maintenance_scheduled: false,
        }
    }

    /// TODO(#25239): Add documentation.
    pub fn instance_exists(&self, id: ComputeInstanceId) -> bool {
        self.instances.contains_key(&id)
    }

    /// Return a reference to the indicated compute instance.
    fn instance(&self, id: ComputeInstanceId) -> Result<&Instance<T>, InstanceMissing> {
        self.instances.get(&id).ok_or(InstanceMissing(id))
    }

    /// Return a mutable reference to the indicated compute instance.
    fn instance_mut(&mut self, id: ComputeInstanceId) -> Result<&mut Instance<T>, InstanceMissing> {
        self.instances.get_mut(&id).ok_or(InstanceMissing(id))
    }

    /// Return a read-only handle to the indicated compute instance.
    pub fn instance_ref(
        &self,
        id: ComputeInstanceId,
    ) -> Result<ComputeInstanceRef<T>, InstanceMissing> {
        self.instance(id).map(|instance| ComputeInstanceRef {
            instance_id: id,
            instance,
        })
    }

    /// Return a read-only handle to the indicated collection.
    pub fn collection(
        &self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
    ) -> Result<&CollectionState<T>, CollectionLookupError> {
        let collection = self.instance(instance_id)?.collection(collection_id)?;
        Ok(collection)
    }

    /// Return a read-only handle to the indicated collection.
    pub fn find_collection(
        &self,
        collection_id: GlobalId,
    ) -> Result<&CollectionState<T>, CollectionLookupError> {
        self.instances
            .values()
            .flat_map(|i| i.collection(collection_id).ok())
            .next()
            .ok_or(CollectionLookupError::CollectionMissing(collection_id))
    }

    /// List compute collections that depend on the given collection.
    pub fn collection_reverse_dependencies(
        &self,
        instance_id: ComputeInstanceId,
        id: GlobalId,
    ) -> Result<impl Iterator<Item = &GlobalId>, InstanceMissing> {
        Ok(self
            .instance(instance_id)?
            .collection_reverse_dependencies(id))
    }

    /// Set the `arrangement_exert_proportionality` value to be passed to new replicas.
    pub fn set_arrangement_exert_proportionality(&mut self, value: u32) {
        self.arrangement_exert_proportionality = value;
    }

    /// Returns `true` iff all collections on all clusters have been hydrated.
    ///
    /// For this check, zero-replica clusters are always considered hydrated.
    /// Their collections would never normally be considered hydrated but it's
    /// clearly intentional that they have no replicas.
    pub fn clusters_hydrated(&self) -> bool {
        let mut result = true;
        for (instance_id, i) in &self.instances {
            let instance_hydrated = i.all_collections_hydrated();

            if !instance_hydrated {
                result = false;

                // We continue with our loop instead of breaking out early, so
                // that we log all non-hydrated clusters.
                tracing::info!("cluster {instance_id} is not hydrated");
            }
        }

        result
    }

    /// Returns `true` iff all (non-transient) collections have their write
    /// frontier (aka. upper) within `allowed_lag` of the "live" frontier
    /// reported in `live_frontiers`. The "live" frontiers are frontiers as
    /// reported by a currently running `environmentd` deployment, during a 0dt
    /// upgrade.
    ///
    /// For this check, zero-replica clusters are always considered caught up.
    /// Their collections would never normally be considered caught up but it's
    /// clearly intentional that they have no replicas.
    pub fn clusters_caught_up(
        &self,
        allowed_lag: T,
        live_frontiers: &BTreeMap<GlobalId, Antichain<T>>,
    ) -> bool {
        let mut result = true;
        for (instance_id, i) in &self.instances {
            let instance_hydrated =
                i.all_collections_caught_up(allowed_lag.clone(), live_frontiers);

            if !instance_hydrated {
                result = false;

                // We continue with our loop instead of breaking out early, so
                // that we log all non-hydrated clusters.
                tracing::info!("cluster {instance_id} is not hydrated");
            }
        }

        result
    }

    /// Returns the read and write frontiers for each collection.
    pub fn collection_frontiers(&self) -> BTreeMap<GlobalId, (Antichain<T>, Antichain<T>)> {
        let collections = self.instances.values().flat_map(|i| i.collections_iter());
        collections
            .map(|(id, collection)| {
                let since = collection.read_frontier().to_owned();
                let upper = collection.write_frontier().to_owned();
                (*id, (since, upper))
            })
            .collect()
    }

    /// Returns the write frontier for each collection installed on each replica.
    pub fn replica_write_frontiers(&self) -> BTreeMap<(GlobalId, ReplicaId), Antichain<T>> {
        let mut result = BTreeMap::new();
        let collections = self.instances.values().flat_map(|i| i.collections_iter());
        for (&collection_id, collection) in collections {
            for (&replica_id, frontier) in &collection.replica_write_frontiers {
                result.insert((collection_id, replica_id), frontier.clone());
            }
        }
        result
    }

    /// Returns the state of the [`ComputeController`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    pub fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
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
            arrangement_exert_proportionality,
            stashed_replica_response,
            envd_epoch,
            metrics: _,
            wallclock_lag: _,
            dyncfg: _,
            response_rx: _,
            response_tx: _,
            introspection_rx: _,
            introspection_tx: _,
            maintenance_ticker: _,
            maintenance_scheduled,
        } = self;

        let instances: BTreeMap<_, _> = instances
            .iter()
            .map(|(id, instance)| Ok((id.to_string(), instance.dump()?)))
            .collect::<Result<_, anyhow::Error>>()?;

        let instance_workload_classes: BTreeMap<_, _> = instance_workload_classes
            .lock()
            .expect("lock poisoned")
            .iter()
            .map(|(id, wc)| (id.to_string(), format!("{wc:?}")))
            .collect();

        fn field(
            key: &str,
            value: impl Serialize,
        ) -> Result<(String, serde_json::Value), anyhow::Error> {
            let value = serde_json::to_value(value)?;
            Ok((key.to_string(), value))
        }

        let map = serde_json::Map::from_iter([
            field("instances", instances)?,
            field("instance_workload_classes", instance_workload_classes)?,
            field("initialized", initialized)?,
            field("read_only", read_only)?,
            field(
                "arrangement_exert_proportionality",
                arrangement_exert_proportionality,
            )?,
            field(
                "stashed_replica_response",
                format!("{stashed_replica_response:?}"),
            )?,
            field("envd_epoch", envd_epoch)?,
            field("maintenance_scheduled", maintenance_scheduled)?,
        ]);
        Ok(serde_json::Value::Object(map))
    }
}

impl<T> ComputeController<T>
where
    T: ComputeControllerTimestamp,
    ComputeGrpcClient: ComputeClient<T>,
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

        self.instances.insert(
            id,
            Instance::new(
                self.build_info,
                Arc::clone(&self.storage_collections),
                arranged_logs,
                self.envd_epoch,
                self.metrics.for_instance(id),
                Arc::clone(&self.wallclock_lag),
                Arc::clone(&self.dyncfg),
                self.response_tx.clone(),
                self.introspection_tx.clone(),
            ),
        );

        self.instance_workload_classes
            .lock()
            .expect("lock poisoned")
            .insert(id, workload_class.clone());

        let instance = self.instances.get_mut(&id).expect("instance just added");
        if self.initialized {
            instance.initialization_complete();
        }

        if !self.read_only {
            instance.allow_writes();
        }

        let mut config_params = self.config.clone();
        config_params.workload_class = Some(workload_class);
        instance.update_configuration(config_params);

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
        if let Some(compute_state) = self.instances.remove(&id) {
            compute_state.drop();
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
            instance.update_configuration(params);
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
            instance.initialization_complete();
        }
    }

    /// Allow this controller and instances controller by it to write to
    /// external systems.
    pub fn allow_writes(&mut self) {
        self.read_only = false;
        for instance in self.instances.values_mut() {
            instance.allow_writes();
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
        if self.stashed_replica_response.is_some() {
            // We still have a response stashed, which we are immediately ready to process.
            return;
        }
        if !self.response_rx.is_empty() {
            // We have responses waiting to be processed.
            return;
        }
        if self.maintenance_scheduled {
            // Maintenance work has been scheduled.
            return;
        }

        let receives: Pin<Box<dyn Future<Output = _>>> = if self.instances.is_empty() {
            // Calling `select_all` with an empty list of futures will panic.
            Box::pin(future::pending())
        } else {
            // `Instance::recv` is cancellation safe, so it is safe to construct this `select_all`.
            let iter = self
                .instances
                .iter_mut()
                .map(|(id, instance)| Box::pin(instance.recv().map(|result| (*id, result))));
            Box::pin(future::select_all(iter))
        };

        tokio::select! {
            (response, _index, _remaining) = receives => {
                let (instance_id, (replica_id, resp)) = response;
                self.stashed_replica_response = Some((instance_id, replica_id, resp));
            },
            _ = self.maintenance_ticker.tick() => {
                self.maintenance_scheduled = true;
            },
        }
    }

    /// Assign a target replica to the identified subscribe.
    ///
    /// If a subscribe has a target replica assigned, only subscribe responses
    /// sent by that replica are considered.
    pub fn set_subscribe_target_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        subscribe_id: GlobalId,
        target_replica: ReplicaId,
    ) -> Result<(), SubscribeTargetError> {
        self.instance_mut(instance_id)?
            .set_subscribe_target_replica(subscribe_id, target_replica)?;
        Ok(())
    }

    /// Adds replicas of an instance.
    pub fn add_replica_to_instance(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        location: ClusterReplicaLocation,
        config: ComputeReplicaConfig,
    ) -> Result<(), ReplicaCreationError> {
        let (enable_logging, interval) = match config.logging.interval {
            Some(interval) => (true, interval),
            None => (false, Duration::from_secs(1)),
        };

        let replica_config = ReplicaConfig {
            location,
            logging: LoggingConfig {
                interval,
                enable_logging,
                log_logging: config.logging.log_logging,
                index_logs: Default::default(),
            },
            arrangement_exert_proportionality: self.arrangement_exert_proportionality,
            grpc_client: self.config.grpc_client.clone(),
        };

        self.instance_mut(instance_id)?
            .add_replica(replica_id, replica_config)?;
        Ok(())
    }

    /// Removes a replica from an instance, including its service in the orchestrator.
    pub fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
    ) -> Result<(), ReplicaDropError> {
        self.instance_mut(instance_id)?.remove_replica(replica_id)?;
        Ok(())
    }

    /// Create and maintain the described dataflows, and initialize state for their output.
    ///
    /// This method creates dataflows whose inputs are still readable at the dataflow `as_of`
    /// frontier, and initializes the outputs as readable from that frontier onward.
    /// It installs read dependencies from the outputs to the inputs, so that the input read
    /// capabilities will be held back to the output read capabilities, ensuring that we are
    /// always able to return to a state that can serve the output read capabilities.
    pub fn create_dataflow(
        &mut self,
        instance_id: ComputeInstanceId,
        dataflow: DataflowDescription<mz_compute_types::plan::Plan<T>, (), T>,
    ) -> Result<(), DataflowCreationError> {
        self.instance_mut(instance_id)?.create_dataflow(dataflow)?;
        Ok(())
    }

    /// Drop the read capability for the given collections and allow their resources to be
    /// reclaimed.
    pub fn drop_collections(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_ids: Vec<GlobalId>,
    ) -> Result<(), CollectionUpdateError> {
        self.instance_mut(instance_id)?
            .drop_collections(collection_ids)?;
        Ok(())
    }

    /// Initiate a peek request for the contents of the given collection at `timestamp`.
    pub fn peek(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
        peek_target: PeekTarget,
    ) -> Result<(), PeekError> {
        self.instance_mut(instance_id)?.peek(
            collection_id,
            literal_constraints,
            uuid,
            timestamp,
            finishing,
            map_filter_project,
            target_replica,
            peek_target,
        )?;
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
        &mut self,
        instance_id: ComputeInstanceId,
        uuid: Uuid,
    ) -> Result<(), InstanceMissing> {
        self.instance_mut(instance_id)?.cancel_peek(uuid);
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
        &mut self,
        instance_id: ComputeInstanceId,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), ReadPolicyError> {
        self.instance_mut(instance_id)?.set_read_policy(policies)?;
        Ok(())
    }

    /// Acquires a [`ReadHold`] for the identified compute collection.
    pub fn acquire_read_hold(
        &self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
    ) -> Result<ReadHold<T>, CollectionUpdateError> {
        let hold = self
            .instance(instance_id)?
            .acquire_read_hold(collection_id)?;
        Ok(hold)
    }

    #[mz_ore::instrument(level = "debug")]
    async fn record_introspection_updates(
        &mut self,
        storage: &mut dyn StorageController<Timestamp = T>,
    ) {
        // We could record the contents of `introspection_rx` directly here, but to reduce the
        // pressure on persist we spend some effort consolidating first.
        let mut updates_by_type = BTreeMap::new();

        for (type_, updates) in self.introspection_rx.try_iter() {
            updates_by_type
                .entry(type_)
                .or_insert_with(Vec::new)
                .extend(updates);
        }
        for updates in updates_by_type.values_mut() {
            consolidate(updates);
        }

        for (type_, updates) in updates_by_type {
            if !updates.is_empty() {
                let op = StorageWriteOp::Append { updates };
                storage.update_introspection_collection(type_, op).await;
            }
        }
    }

    /// Processes the work queued by [`ComputeController::ready`].
    #[mz_ore::instrument(level = "debug")]
    pub async fn process(
        &mut self,
        storage: &mut dyn StorageController<Timestamp = T>,
    ) -> Option<ComputeControllerResponse<T>> {
        // Perform periodic maintenance work.
        if self.maintenance_scheduled {
            self.maintain(storage).await;
            self.maintenance_ticker.reset();
            self.maintenance_scheduled = false;
        }

        // Process pending responses from replicas.
        if let Some((instance_id, replica_id, response)) = self.stashed_replica_response.take() {
            if let Some(instance) = self.instances.get_mut(&instance_id) {
                instance.handle_response(response, replica_id);
            } else {
                warn!(
                    ?instance_id,
                    ?response,
                    "processed response from unknown instance"
                );
            };
        }

        // Return a ready response, if any.
        match self.response_rx.try_recv() {
            Ok(response) => Some(response),
            Err(crossbeam_channel::TryRecvError::Empty) => None,
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                // This should never happen, since the `ComputeController` is always holding on to
                // a copy of the `response_tx`.
                panic!("response_tx has disconnected");
            }
        }
    }

    #[mz_ore::instrument(level = "debug")]
    async fn maintain(&mut self, storage: &mut dyn StorageController<Timestamp = T>) {
        // Perform instance maintenance work.
        for instance in self.instances.values_mut() {
            instance.maintain();
        }

        // Record pending introspection updates.
        //
        // It's beneficial to do this as the last maintenance step because previous steps can cause
        // dropping of state, which can can cause introspection retractions, which lower the volume
        // of data we have to record.
        self.record_introspection_updates(storage).await;
    }
}

/// A read-only handle to a compute instance.
#[derive(Clone, Copy)]
pub struct ComputeInstanceRef<'a, T: Timestamp> {
    instance_id: ComputeInstanceId,
    instance: &'a Instance<T>,
}

impl<T: ComputeControllerTimestamp> ComputeInstanceRef<'_, T> {
    /// Return the ID of this compute instance.
    pub fn instance_id(&self) -> ComputeInstanceId {
        self.instance_id
    }

    /// Return a read-only handle to the indicated collection.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, CollectionMissing> {
        self.instance.collection(id)
    }

    /// Return an iterator over the installed collections.
    pub fn collections(&self) -> impl Iterator<Item = (&GlobalId, &CollectionState<T>)> {
        self.instance.collections_iter()
    }
}

/// State maintained about individual compute collections.
///
/// A compute collection is either an index, or a storage sink, or a subscribe, exported by a
/// compute dataflow.
#[derive(Debug)]
pub struct CollectionState<T: Timestamp> {
    /// Whether this collection is a log collection.
    ///
    /// Log collections are special in that they are only maintained by a subset of all replicas.
    log_collection: bool,
    /// Whether this collection has been dropped by a controller client.
    ///
    /// The controller is allowed to remove the `CollectionState` for a collection only when
    /// `dropped == true`. Otherwise, clients might still expect to be able to query information
    /// about this collection.
    dropped: bool,
    /// Whether this collection has been scheduled, i.e., the controller has sent a `Schedule`
    /// command for it.
    scheduled: bool,

    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation contains the capabilities held by all [`ReadHold`]s given out for the
    /// collection, including `implied_read_hold` and `warmup_read_hold`.
    read_capabilities: MutableAntichain<T>,
    /// A read hold maintaining the implicit capability of the collection.
    ///
    /// This capability is kept to ensure that the collection remains readable according to its
    /// `read_policy`. It also ensures that read holds on the collection's dependencies are kept at
    /// some time not greater than the collection's `write_frontier`, guaranteeing that the
    /// collection's next outputs can always be computed without skipping times.
    implied_read_hold: ReadHold<T>,
    /// A read hold held to enable dataflow warmup.
    ///
    /// Dataflow warmup is an optimization that allows dataflows to immediately start hydrating
    /// even when their next output time (as implied by the `write_frontier`) is in the future.
    /// By installing a read capability derived from the write frontiers of the collection's
    /// inputs, we ensure that the as-of of new dataflows installed for the collection is at a time
    /// that is immediately available, so hydration can begin immediately too.
    warmup_read_hold: ReadHold<T>,
    /// The policy to use to downgrade `self.implied_read_hold`.
    ///
    /// If `None`, the collection is a write-only collection (i.e. a sink). For write-only
    /// collections, the `implied_read_hold` is only required for maintaining read holds on the
    /// inputs, so we can immediately downgrade it to the `write_frontier`.
    read_policy: Option<ReadPolicy<T>>,

    /// Storage identifiers on which this collection depends, and read holds this collection
    /// requires on them.
    storage_dependencies: BTreeMap<GlobalId, ReadHold<T>>,
    /// Compute identifiers on which this collection depends, and read holds this collection
    /// requires on them.
    compute_dependencies: BTreeMap<GlobalId, ReadHold<T>>,

    /// The write frontier of this collection.
    write_frontier: Antichain<T>,
    /// The write frontiers reported by individual replicas.
    replica_write_frontiers: BTreeMap<ReplicaId, Antichain<T>>,
    /// The input frontiers reported by individual replicas.
    replica_input_frontiers: BTreeMap<ReplicaId, Antichain<T>>,

    /// Introspection state associated with this collection.
    collection_introspection: CollectionIntrospection<T>,
}

impl<T: Timestamp> CollectionState<T> {
    /// Reports the current read capability.
    pub fn read_capability(&self) -> &Antichain<T> {
        self.implied_read_hold.since()
    }

    /// Reports the current read frontier.
    pub fn read_frontier(&self) -> AntichainRef<T> {
        self.read_capabilities.frontier()
    }

    /// Reports the current write frontier.
    pub fn write_frontier(&self) -> AntichainRef<T> {
        self.write_frontier.borrow()
    }

    fn storage_dependency_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.storage_dependencies.keys().copied()
    }

    fn compute_dependency_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.compute_dependencies.keys().copied()
    }

    /// Reports the IDs of the dependencies of this collection.
    fn dependency_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.compute_dependency_ids()
            .chain(self.storage_dependency_ids())
    }
}

impl<T: ComputeControllerTimestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
    pub(crate) fn new(
        collection_id: GlobalId,
        as_of: Antichain<T>,
        storage_dependencies: BTreeMap<GlobalId, ReadHold<T>>,
        compute_dependencies: BTreeMap<GlobalId, ReadHold<T>>,
        read_holds_tx: mpsc::UnboundedSender<(GlobalId, ChangeBatch<T>)>,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
        initial_as_of: Option<Antichain<T>>,
        refresh_schedule: Option<RefreshSchedule>,
    ) -> Self {
        // A collection is not readable before the `as_of`.
        let since = as_of.clone();
        // A collection won't produce updates for times before the `as_of`.
        let upper = as_of;

        // Initialize all read capabilities to the `since`.
        let implied_read_hold = ReadHold::new(collection_id, since.clone(), read_holds_tx.clone());
        let warmup_read_hold = ReadHold::new(collection_id, since.clone(), read_holds_tx);

        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(
            implied_read_hold
                .since()
                .iter()
                .map(|time| (time.clone(), 1)),
        );
        read_capabilities.update_iter(
            warmup_read_hold
                .since()
                .iter()
                .map(|time| (time.clone(), 1)),
        );

        Self {
            log_collection: false,
            dropped: false,
            scheduled: false,
            read_capabilities,
            implied_read_hold,
            warmup_read_hold,
            read_policy: Some(ReadPolicy::ValidFrom(since)),
            storage_dependencies,
            compute_dependencies,
            write_frontier: upper.clone(),
            replica_write_frontiers: BTreeMap::new(),
            replica_input_frontiers: BTreeMap::new(),
            collection_introspection: CollectionIntrospection::new(
                collection_id,
                introspection_tx,
                initial_as_of,
                refresh_schedule,
                upper,
            ),
        }
    }

    /// Creates a new collection state for a log collection.
    pub(crate) fn new_log_collection(
        id: GlobalId,
        read_holds_tx: mpsc::UnboundedSender<(GlobalId, ChangeBatch<T>)>,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    ) -> Self {
        let since = Antichain::from_elem(timely::progress::Timestamp::minimum());
        let mut state = Self::new(
            id,
            since,
            Default::default(),
            Default::default(),
            read_holds_tx,
            introspection_tx,
            None,
            None,
        );
        state.log_collection = true;
        // Log collections are created and scheduled implicitly as part of replica initialization.
        state.scheduled = true;
        state
    }
}

/// Manages certain introspection relations associated with a collection. Upon creation, it adds
/// rows to introspection relations. When dropped, it retracts its managed rows.
///
/// TODO: `ComputeDependencies` could be moved under this.
#[derive(Debug)]
struct CollectionIntrospection<T> {
    /// The ID of the compute collection.
    collection_id: GlobalId,
    /// A channel through which introspection updates are delivered.
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    /// Introspection state for `mz_materialized_view_refreshes`.
    /// `Some` if it is a REFRESH MV.
    refresh_introspection_state: Option<RefreshIntrospectionState<T>>,
}

impl<T> CollectionIntrospection<T> {
    fn send(&self, introspection_type: IntrospectionType, updates: Vec<(Row, Diff)>) {
        let result = self.introspection_tx.send((introspection_type, updates));

        if result.is_err() {
            // The global controller holds on to the `introspection_rx`. So when we get here that
            // probably means that the controller was dropped and the process is shutting down, in
            // which case we don't care about introspection updates anymore.
            tracing::info!(
                ?introspection_type,
                "discarding introspection update because the receiver disconnected"
            );
        }
    }
}

impl<T: ComputeControllerTimestamp> CollectionIntrospection<T> {
    pub fn new(
        collection_id: GlobalId,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
        initial_as_of: Option<Antichain<T>>,
        refresh_schedule: Option<RefreshSchedule>,
        upper: Antichain<T>,
    ) -> CollectionIntrospection<T> {
        let refresh_introspection_state = match (refresh_schedule, initial_as_of) {
            (Some(refresh_schedule), Some(initial_as_of)) => Some(RefreshIntrospectionState::new(
                refresh_schedule,
                initial_as_of,
                upper,
            )),
            (Some(_refresh_schedule), None) => {
                soft_panic_or_log!("if we have a refresh_schedule, then it's an MV, so we should also have an initial_as_of");
                None
            }
            _ => None,
        };
        let self_ = CollectionIntrospection {
            collection_id,
            introspection_tx,
            refresh_introspection_state,
        };

        if let Some(refresh_introspection_state) = self_.refresh_introspection_state.as_ref() {
            let insertion = refresh_introspection_state.row_for_collection(collection_id);
            self_.send(
                IntrospectionType::ComputeMaterializedViewRefreshes,
                vec![(insertion, 1)],
            );
        }

        self_
    }

    /// Should be called whenever the write frontier of the collection advances. It updates the
    /// state that should be recorded in introspection relations, and sends it through the
    /// `introspection_tx` channel.
    pub fn frontier_update(&mut self, write_frontier: &Antichain<T>) {
        if let Some(refresh_introspection_state) = &mut self.refresh_introspection_state {
            // Old row to retract based on old state.
            let retraction = refresh_introspection_state.row_for_collection(self.collection_id);
            // Update state.
            refresh_introspection_state.frontier_update(write_frontier);
            // New row to insert based on new state.
            let insertion = refresh_introspection_state.row_for_collection(self.collection_id);
            // Send changes.
            self.send(
                IntrospectionType::ComputeMaterializedViewRefreshes,
                vec![(retraction, -1), (insertion, 1)],
            );
        }
    }
}

impl<T> Drop for CollectionIntrospection<T> {
    fn drop(&mut self) {
        self.refresh_introspection_state
            .as_ref()
            .map(|refresh_introspection_state| {
                let retraction = refresh_introspection_state.row_for_collection(self.collection_id);
                self.send(
                    IntrospectionType::ComputeMaterializedViewRefreshes,
                    vec![(retraction, -1)],
                );
            });
    }
}

/// Information needed to compute introspection updates for a REFRESH materialized view when the
/// write frontier advances.
#[derive(Debug)]
struct RefreshIntrospectionState<T> {
    // Immutable properties of the MV
    refresh_schedule: RefreshSchedule,
    initial_as_of: Antichain<T>,
    // Refresh state
    next_refresh: Datum<'static>,           // Null or an MzTimestamp
    last_completed_refresh: Datum<'static>, // Null or an MzTimestamp
}

impl<T> RefreshIntrospectionState<T> {
    /// Return a `Row` reflecting the current refresh introspection state.
    pub fn row_for_collection(&self, collection_id: GlobalId) -> Row {
        Row::pack_slice(&[
            Datum::String(&collection_id.to_string()),
            self.last_completed_refresh,
            self.next_refresh,
        ])
    }
}

impl<T: ComputeControllerTimestamp> RefreshIntrospectionState<T> {
    /// Construct a new [`RefreshIntrospectionState`], and apply an initial `frontier_update()` at
    /// the `upper`.
    pub fn new(
        refresh_schedule: RefreshSchedule,
        initial_as_of: Antichain<T>,
        upper: Antichain<T>,
    ) -> RefreshIntrospectionState<T> {
        let mut self_ = RefreshIntrospectionState {
            refresh_schedule: refresh_schedule.clone(),
            initial_as_of: initial_as_of.clone(),
            next_refresh: Datum::Null,
            last_completed_refresh: Datum::Null,
        };
        self_.frontier_update(&upper);
        self_
    }

    /// Should be called whenever the write frontier of the collection advances. It updates the
    /// state that should be recorded in introspection relations, but doesn't send the updates yet.
    pub fn frontier_update(&mut self, write_frontier: &Antichain<T>) {
        if write_frontier.is_empty() {
            self.last_completed_refresh =
                if let Some(last_refresh) = self.refresh_schedule.last_refresh() {
                    last_refresh.into()
                } else {
                    // If there is no last refresh, then we have a `REFRESH EVERY`, in which case
                    // the saturating roundup puts a refresh at the maximum possible timestamp.
                    T::maximum().into()
                };
            self.next_refresh = Datum::Null;
        } else {
            if timely::PartialOrder::less_equal(write_frontier, &self.initial_as_of) {
                // We are before the first refresh.
                self.last_completed_refresh = Datum::Null;
                let initial_as_of = self.initial_as_of.as_option().expect(
                    "initial_as_of can't be [], because then there would be no refreshes at all",
                );
                let first_refresh = initial_as_of
                    .round_up(&self.refresh_schedule)
                    .expect("sequencing makes sure that REFRESH MVs always have a first refresh");
                soft_assert_or_log!(
                    first_refresh == *initial_as_of,
                    "initial_as_of should be set to the first refresh"
                );
                self.next_refresh = first_refresh.into();
            } else {
                // The first refresh has already happened.
                let write_frontier = write_frontier.as_option().expect("checked above");
                self.last_completed_refresh = write_frontier
                    .round_down_minus_1(&self.refresh_schedule)
                    .map_or_else(
                        || {
                            soft_panic_or_log!(
                                "rounding down should have returned the first refresh or later"
                            );
                            Datum::Null
                        },
                        |last_completed_refresh| last_completed_refresh.into(),
                    );
                self.next_refresh = write_frontier.clone().into();
            }
        }
    }
}
