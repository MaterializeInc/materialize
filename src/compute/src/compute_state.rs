// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for compute timely instances.

use std::any::Any;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::Hashable;
use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::protocol::command::{
    ComputeCommand, ComputeParameters, InstanceConfig, Peek, PeekTarget,
};
use mz_compute_client::protocol::history::ComputeCommandHistory;
use mz_compute_client::protocol::response::{
    ComputeResponse, CopyToResponse, FrontiersResponse, OperatorHydrationStatus, PeekResponse,
    StatusResponse, SubscribeResponse,
};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_compute_types::plan::LirId;
use mz_dyncfg::ConfigSet;
use mz_expr::row::RowCollection;
use mz_expr::SafeMfpPlan;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::UIntGauge;
use mz_ore::now::EpochMillis;
use mz_ore::task::AbortOnDropHandle;
use mz_ore::tracing::{OpenTelemetryContext, TracingHandle};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena, Timestamp};
use mz_storage_operators::stats::StatsCursor;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use mz_storage_types::time_dependence::TimeDependence;
use mz_txn_wal::operator::TxnsContext;
use mz_txn_wal::txn_cache::TxnsCache;
use timely::communication::Allocate;
use timely::dataflow::operators::probe;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::scheduling::Scheduler;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{oneshot, watch};
use tracing::{debug, error, info, span, warn, Level};
use uuid::Uuid;

use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::logging;
use crate::logging::compute::{CollectionLogging, ComputeEvent, PeekEvent};
use crate::metrics::ComputeMetrics;
use crate::metrics::WorkerMetrics;
use crate::render::{LinearJoinSpec, StartSignal};
use crate::server::{ComputeInstanceContext, ResponseSender};

/// Worker-local state that is maintained across dataflows.
///
/// This state is restricted to the COMPUTE state, the deterministic, idempotent work
/// done between data ingress and egress.
pub struct ComputeState {
    /// State kept for each installed compute collection.
    ///
    /// Each collection has exactly one frontier.
    /// How the frontier is communicated depends on the collection type:
    ///  * Frontiers of indexes are equal to the frontier of their corresponding traces in the
    ///    `TraceManager`.
    ///  * Persist sinks store their current frontier in `CollectionState::sink_write_frontier`.
    ///  * Subscribes report their frontiers through the `subscribe_response_buffer`.
    pub collections: BTreeMap<GlobalId, CollectionState>,
    /// Collections that were recently dropped and whose removal needs to be reported.
    pub dropped_collections: Vec<(GlobalId, DroppedCollection)>,
    /// The traces available for sharing across dataflows.
    pub traces: TraceManager,
    /// Shared buffer with SUBSCRIBE operator instances by which they can respond.
    ///
    /// The entries are pairs of sink identifier (to identify the subscribe instance)
    /// and the response itself.
    pub subscribe_response_buffer: Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>,
    /// Shared buffer with S3 oneshot operator instances by which they can respond.
    ///
    /// The entries are pairs of sink identifier (to identify the s3 oneshot instance)
    /// and the response itself.
    pub copy_to_response_buffer: Rc<RefCell<Vec<(GlobalId, CopyToResponse)>>>,
    /// Peek commands that are awaiting fulfillment.
    pub pending_peeks: BTreeMap<Uuid, PendingPeek>,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub compute_logger: Option<logging::compute::Logger>,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// History of commands received by this workers and all its peers.
    pub command_history: ComputeCommandHistory<UIntGauge>,
    /// Max size in bytes of any result.
    max_result_size: u64,
    /// Specification for rendering linear joins.
    pub linear_join_spec: LinearJoinSpec,
    /// Metrics for this replica.
    pub metrics: ComputeMetrics,
    /// Metrics for this replica, specific to a worker.
    pub worker_metrics: WorkerMetrics,
    /// A process-global handle to tracing configuration.
    tracing_handle: Arc<TracingHandle>,
    /// Other configuration for compute
    pub context: ComputeInstanceContext,
    /// Per-worker dynamic configuration.
    ///
    /// This is separate from the process-global `ConfigSet` and contains config options that need
    /// to be applied consistently with compute command order.
    ///
    /// For example, for options that influence dataflow rendering it is important that all workers
    /// render the same dataflow with the same options. If these options were stored in a global
    /// `ConfigSet`, we couldn't guarantee that all workers observe changes to them at the same
    /// point in the stream of compute commands. Storing per-worker configuration ensures that
    /// because each worker's configuration is only updated once that worker observes the
    /// respective `UpdateConfiguration` command.
    pub worker_config: ConfigSet,

    /// Receiver of operator hydration events.
    pub hydration_rx: mpsc::Receiver<HydrationEvent>,
    /// Transmitter of operator hydration events.
    ///
    /// Copies of this sender are passed to the hydration logging operators.
    pub hydration_tx: mpsc::Sender<HydrationEvent>,

    /// Collections awaiting schedule instruction by the controller.
    ///
    /// Each entry stores a reference to a token that can be dropped to unsuspend the collection's
    /// dataflow. Multiple collections can reference the same token if they are exported by the
    /// same dataflow.
    suspended_collections: BTreeMap<GlobalId, Rc<dyn Any>>,

    /// When this replica/cluster is in read-only mode it must not affect any
    /// changes to external state. This flag can only be changed by a
    /// [ComputeCommand::AllowWrites].
    ///
    /// Everything running on this replica/cluster must obey this flag. At the
    /// time of writing the only part that is doing this is `persist_sink`.
    ///
    /// NOTE: In the future, we might want a more complicated flag, for example
    /// something that tells us after which timestamp we are allowed to write.
    /// In this first version we are keeping things as simple as possible!
    pub read_only_rx: watch::Receiver<bool>,

    /// Send-side for read-only state.
    pub read_only_tx: watch::Sender<bool>,

    /// Interval at which to perform server maintenance tasks. Set to a zero interval to
    /// perform maintenance with every `step_or_park` invocation.
    pub server_maintenance_interval: Duration,

    /// The [`mz_ore::now::SYSTEM_TIME`] at which the replica was started.
    ///
    /// Used to compute `replica_expiration`.
    pub init_system_time: EpochMillis,

    /// The maximum time for which the replica is expected to live. If not empty, dataflows in the
    /// replica can drop diffs associated with timestamps beyond the replica expiration.
    /// The replica will panic if such dataflows are not dropped before the replica has expired.
    pub replica_expiration: Antichain<Timestamp>,
}

impl ComputeState {
    /// Construct a new `ComputeState`.
    pub fn new(
        worker_id: usize,
        persist_clients: Arc<PersistClientCache>,
        txns_ctx: TxnsContext,
        metrics: ComputeMetrics,
        tracing_handle: Arc<TracingHandle>,
        context: ComputeInstanceContext,
    ) -> Self {
        let traces = TraceManager::new(metrics.for_traces(worker_id));
        let command_history = ComputeCommandHistory::new(metrics.for_history(worker_id));
        let (hydration_tx, hydration_rx) = mpsc::channel();

        // We always initialize as read_only=true. Only when we're explicitly
        // allowed do we switch to doing writes.
        let (read_only_tx, read_only_rx) = watch::channel(true);

        let worker_metrics = WorkerMetrics::from(&metrics, worker_id);

        Self {
            collections: Default::default(),
            dropped_collections: Default::default(),
            traces,
            subscribe_response_buffer: Default::default(),
            copy_to_response_buffer: Default::default(),
            pending_peeks: Default::default(),
            compute_logger: None,
            persist_clients,
            txns_ctx,
            command_history,
            max_result_size: u64::MAX,
            linear_join_spec: Default::default(),
            metrics,
            worker_metrics,
            tracing_handle,
            context,
            worker_config: mz_dyncfgs::all_dyncfgs(),
            hydration_rx,
            hydration_tx,
            suspended_collections: Default::default(),
            read_only_tx,
            read_only_rx,
            server_maintenance_interval: Duration::ZERO,
            init_system_time: mz_ore::now::SYSTEM_TIME(),
            replica_expiration: Antichain::default(),
        }
    }

    /// Return whether a collection with the given ID exists.
    pub fn collection_exists(&self, id: GlobalId) -> bool {
        self.collections.contains_key(&id)
    }

    /// Return a reference to the identified collection.
    ///
    /// Panics if the collection doesn't exist.
    pub fn expect_collection(&self, id: GlobalId) -> &CollectionState {
        self.collections.get(&id).expect("collection must exist")
    }

    /// Return a mutable reference to the identified collection.
    ///
    /// Panics if the collection doesn't exist.
    pub fn expect_collection_mut(&mut self, id: GlobalId) -> &mut CollectionState {
        self.collections
            .get_mut(&id)
            .expect("collection must exist")
    }

    /// Construct a new frontier probe for the given input and add it to the state of the given
    /// collections.
    ///
    /// The caller is responsible for attaching the returned probe handle to the respective
    /// dataflow input stream.
    pub fn input_probe_for(
        &mut self,
        input_id: GlobalId,
        collection_ids: impl Iterator<Item = GlobalId>,
    ) -> probe::Handle<Timestamp> {
        let probe = probe::Handle::default();
        for id in collection_ids {
            if let Some(collection) = self.collections.get_mut(&id) {
                collection.input_probes.insert(input_id, probe.clone());
            }
        }
        probe
    }

    /// Apply the current `worker_config` to the compute state.
    fn apply_worker_config(&mut self) {
        use mz_compute_types::dyncfgs::*;

        let config = &self.worker_config;

        self.linear_join_spec = LinearJoinSpec::from_config(config);

        if ENABLE_COLUMNATION_LGALLOC.get(config) {
            if let Some(path) = &self.context.scratch_directory {
                let eager_return = ENABLE_LGALLOC_EAGER_RECLAMATION.get(config);
                let interval = LGALLOC_BACKGROUND_INTERVAL.get(config);
                let clear_bytes = LGALLOC_SLOW_CLEAR_BYTES.get(config);
                info!(
                    ?path,
                    eager_return, backgrund_interval=?interval, clear_bytes, "enabling lgalloc"
                );
                let background_worker_config = lgalloc::BackgroundWorkerConfig {
                    interval,
                    clear_bytes,
                };
                lgalloc::lgalloc_set_config(
                    lgalloc::LgAlloc::new()
                        .enable()
                        .with_path(path.clone())
                        .with_background_config(background_worker_config)
                        .eager_return(eager_return),
                );
            } else {
                debug!("not enabling lgalloc, scratch directory not specified");
            }
        } else {
            info!("disabling lgalloc");
            lgalloc::lgalloc_set_config(lgalloc::LgAlloc::new().disable());
        }

        let chunked_stack = ENABLE_CHUNKED_STACK.get(config);
        info!("using chunked stack: {chunked_stack}");
        mz_timely_util::containers::stack::use_chunked_stack(chunked_stack);

        // Remember the maintenance interval locally to avoid reading it from the config set on
        // every server iteration.
        self.server_maintenance_interval = COMPUTE_SERVER_MAINTENANCE_INTERVAL.get(config);
    }

    /// Apply the provided replica expiration `offset` by converting it to a frontier relative to
    /// the replica's initialization system time.
    ///
    /// Only expected to be called once when creating the instance. Guards against calling it
    /// multiple times by checking if the local expiration time is set.
    pub fn apply_expiration_offset(&mut self, offset: Duration) {
        if self.replica_expiration.is_empty() {
            let offset: EpochMillis = offset
                .as_millis()
                .try_into()
                .expect("duration must fit within u64");
            let replica_expiration_millis = self.init_system_time + offset;
            let replica_expiration = Timestamp::from(replica_expiration_millis);

            info!(
                offset = %offset,
                replica_expiration_millis = %replica_expiration_millis,
                replica_expiration_utc = %mz_ore::now::to_datetime(replica_expiration_millis),
                "setting replica expiration",
            );
            self.replica_expiration = Antichain::from_elem(replica_expiration);

            // Record the replica expiration in the metrics.
            self.worker_metrics
                .replica_expiration_timestamp_seconds
                .set(replica_expiration.into());
        }
    }

    /// Returns the cc or non-cc version of "dataflow_max_inflight_bytes", as
    /// appropriate to this replica.
    pub fn dataflow_max_inflight_bytes(&self) -> Option<usize> {
        use mz_compute_types::dyncfgs::{
            DATAFLOW_MAX_INFLIGHT_BYTES, DATAFLOW_MAX_INFLIGHT_BYTES_CC,
        };

        if self.persist_clients.cfg.is_cc_active {
            DATAFLOW_MAX_INFLIGHT_BYTES_CC.get(&self.worker_config)
        } else {
            DATAFLOW_MAX_INFLIGHT_BYTES.get(&self.worker_config)
        }
    }
}

/// A wrapper around [ComputeState] with a live timely worker and response channel.
pub(crate) struct ActiveComputeState<'a, A: Allocate> {
    /// The underlying Timely worker.
    pub timely_worker: &'a mut TimelyWorker<A>,
    /// The compute state itself.
    pub compute_state: &'a mut ComputeState,
    /// The channel over which frontier information is reported.
    pub response_tx: &'a mut ResponseSender,
}

/// A token that keeps a sink alive.
pub struct SinkToken(#[allow(dead_code)] Box<dyn Any>);

impl SinkToken {
    /// Create a new `SinkToken`.
    pub fn new(t: Box<dyn Any>) -> Self {
        Self(t)
    }
}

impl<'a, A: Allocate + 'static> ActiveComputeState<'a, A> {
    /// Entrypoint for applying a compute command.
    #[mz_ore::instrument(level = "debug")]
    pub fn handle_compute_command(&mut self, cmd: ComputeCommand) {
        use ComputeCommand::*;

        self.compute_state.command_history.push(cmd.clone());

        // Record the command duration, per worker and command kind.
        let timer = self
            .compute_state
            .worker_metrics
            .handle_command_duration_seconds
            .for_command(&cmd)
            .start_timer();

        match cmd {
            CreateTimely { .. } => panic!("CreateTimely must be captured before"),
            CreateInstance(instance_config) => self.handle_create_instance(instance_config),
            InitializationComplete => (),
            UpdateConfiguration(params) => self.handle_update_configuration(params),
            CreateDataflow(dataflow) => self.handle_create_dataflow(dataflow),
            Schedule(id) => self.handle_schedule(id),
            AllowCompaction { id, frontier } => self.handle_allow_compaction(id, frontier),
            Peek(peek) => {
                peek.otel_ctx.attach_as_parent();
                self.handle_peek(peek)
            }
            CancelPeek { uuid } => self.handle_cancel_peek(uuid),
            AllowWrites => {
                self.compute_state
                    .read_only_tx
                    .send(false)
                    .expect("we're holding one other end");
                self.compute_state.persist_clients.cfg().enable_compaction();
            }
        }

        timer.observe_duration();
    }

    fn handle_create_instance(&mut self, config: InstanceConfig) {
        // Ensure the state is consistent with the config before we initialize anything.
        self.compute_state.apply_worker_config();
        if let Some(offset) = config.expiration_offset {
            self.compute_state.apply_expiration_offset(offset);
        }

        self.initialize_logging(config.logging);
    }

    fn handle_update_configuration(&mut self, params: ComputeParameters) {
        info!("Applying configuration update: {params:?}");

        let ComputeParameters {
            workload_class,
            max_result_size,
            tracing,
            grpc_client: _grpc_client,
            dyncfg_updates,
        } = params;

        if let Some(v) = workload_class {
            self.compute_state.metrics.set_workload_class(v);
        }
        if let Some(v) = max_result_size {
            self.compute_state.max_result_size = v;
        }

        tracing.apply(self.compute_state.tracing_handle.as_ref());

        dyncfg_updates.apply(&self.compute_state.worker_config);
        self.compute_state
            .persist_clients
            .cfg()
            .apply_from(&dyncfg_updates);

        self.compute_state.apply_worker_config();
    }

    fn handle_create_dataflow(
        &mut self,
        dataflow: DataflowDescription<RenderPlan, CollectionMetadata>,
    ) {
        // Collect the exported object identifiers, paired with their associated "collection" identifier.
        // The latter is used to extract dependency information, which is in terms of collections ids.
        let dataflow_index = self.timely_worker.next_dataflow_index();
        let as_of = dataflow.as_of.clone().unwrap();

        let dataflow_expiration = dataflow
            .time_dependence
            .as_ref()
            .map(|time_dependence| {
                self.determine_dataflow_expiration(time_dependence, &dataflow.until)
            })
            .unwrap_or_default();

        // Add the dataflow expiration to `until`.
        let until = dataflow.until.meet(&dataflow_expiration);

        if dataflow.is_transient() {
            debug!(
                name = %dataflow.debug_name,
                import_ids = %dataflow.display_import_ids(),
                export_ids = %dataflow.display_export_ids(),
                as_of = ?as_of.elements(),
                time_dependence = ?dataflow.time_dependence,
                expiration = ?dataflow_expiration.elements(),
                expiration_datetime = ?dataflow_expiration.as_option().map(|t| mz_ore::now::to_datetime(t.into())),
                plan_until = ?dataflow.until.elements(),
                until = ?until.elements(),
                "creating dataflow",
            );
        } else {
            info!(
                name = %dataflow.debug_name,
                import_ids = %dataflow.display_import_ids(),
                export_ids = %dataflow.display_export_ids(),
                as_of = ?as_of.elements(),
                time_dependence = ?dataflow.time_dependence,
                expiration = ?dataflow_expiration.elements(),
                expiration_datetime = ?dataflow_expiration.as_option().map(|t| mz_ore::now::to_datetime(t.into())),
                plan_until = ?dataflow.until.elements(),
                until = ?until.elements(),
                "creating dataflow",
            );
        };

        let subscribe_copy_ids: BTreeSet<_> = dataflow
            .subscribe_ids()
            .chain(dataflow.copy_to_ids())
            .collect();

        // Initialize compute and logging state for each object.
        for object_id in dataflow.export_ids() {
            let is_subscribe_or_copy = subscribe_copy_ids.contains(&object_id);
            let mut collection = CollectionState::new(is_subscribe_or_copy, as_of.clone());

            if let Some(logger) = self.compute_state.compute_logger.clone() {
                let logging = CollectionLogging::new(
                    object_id,
                    logger,
                    dataflow_index,
                    dataflow.import_ids(),
                );
                collection.logging = Some(logging);
            }

            collection.reset_reported_frontiers(ReportedFrontier::NotReported {
                lower: as_of.clone(),
            });

            let existing = self.compute_state.collections.insert(object_id, collection);
            if existing.is_some() {
                error!(
                    id = ?object_id,
                    "existing collection for newly created dataflow",
                );
            }
        }

        let (start_signal, suspension_token) = StartSignal::new();
        for id in dataflow.export_ids() {
            self.compute_state
                .suspended_collections
                .insert(id, Rc::clone(&suspension_token));
        }

        crate::render::build_compute_dataflow(
            self.timely_worker,
            self.compute_state,
            dataflow,
            start_signal,
            until,
            dataflow_expiration,
        );
    }

    fn handle_schedule(&mut self, id: GlobalId) {
        // A `Schedule` command instructs us to begin dataflow computation for a collection, so
        // we should unsuspend it by dropping the corresponding suspension token. Note that a
        // dataflow can export multiple collections and they all share one suspension token, so the
        // computation of a dataflow will only start once all its exported collections have been
        // scheduled.
        let suspension_token = self.compute_state.suspended_collections.remove(&id);
        drop(suspension_token);
    }

    fn handle_allow_compaction(&mut self, id: GlobalId, frontier: Antichain<Timestamp>) {
        if frontier.is_empty() {
            // Indicates that we may drop `id`, as there are no more valid times to read.
            self.drop_collection(id);
        } else {
            self.compute_state
                .traces
                .allow_compaction(id, frontier.borrow());
        }
    }

    #[mz_ore::instrument(level = "debug")]
    fn handle_peek(&mut self, peek: Peek) {
        let pending = match &peek.target {
            PeekTarget::Index { id } => {
                // Acquire a copy of the trace suitable for fulfilling the peek.
                let trace_bundle = self.compute_state.traces.get(id).unwrap().clone();
                PendingPeek::index(peek, trace_bundle)
            }
            PeekTarget::Persist { metadata, .. } => {
                let metadata = metadata.clone();
                PendingPeek::persist(
                    peek,
                    Arc::clone(&self.compute_state.persist_clients),
                    metadata,
                    usize::cast_from(self.compute_state.max_result_size),
                    self.timely_worker,
                )
            }
        };

        // Log the receipt of the peek.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(&pending.as_log_event(true));
        }

        self.process_peek(&mut Antichain::new(), pending);
    }

    fn handle_cancel_peek(&mut self, uuid: Uuid) {
        if let Some(peek) = self.compute_state.pending_peeks.remove(&uuid) {
            self.send_peek_response(peek, PeekResponse::Canceled);
        }
    }

    /// Arrange for the given collection to be dropped.
    ///
    /// Collection dropping occurs in three phases:
    ///
    ///  1. This method removes the collection from the [`ComputeState`] and drops its
    ///     [`CollectionState`], including its held dataflow tokens. It then adds the dropped
    ///     collection to `dropped_collections`.
    ///  2. The next step of the Timely worker lets the source operators observe the token drops
    ///     and shut themselves down.
    ///  3. `report_dropped_collections` removes the entry from `dropped_collections` and emits any
    ///     outstanding final responses required by the compute protocol.
    ///
    /// These steps ensure that we don't report a collection as dropped to the controller before it
    /// has stopped reading from its inputs. Doing so would allow the controller to release its
    /// read holds on the inputs, which could lead to panics from the replica trying to read
    /// already compacted times.
    fn drop_collection(&mut self, id: GlobalId) {
        let collection = self
            .compute_state
            .collections
            .remove(&id)
            .expect("dropped untracked collection");

        // If this collection is an index, remove its trace.
        self.compute_state.traces.remove(&id);
        // If the collection is unscheduled, remove it from the list of waiting collections.
        self.compute_state.suspended_collections.remove(&id);

        // Remember the collection as dropped, for emission of outstanding final compute responses.
        let dropped = DroppedCollection {
            reported_frontiers: collection.reported_frontiers,
            is_subscribe_or_copy: collection.is_subscribe_or_copy,
        };
        self.compute_state.dropped_collections.push((id, dropped));
    }

    /// Initializes timely dataflow logging and publishes as a view.
    pub fn initialize_logging(&mut self, config: LoggingConfig) {
        if self.compute_state.compute_logger.is_some() {
            panic!("dataflow server has already initialized logging");
        }

        let (logger, traces) = logging::initialize(self.timely_worker, &config);

        let mut log_index_ids = config.index_logs;
        for (log, (trace, dataflow_index)) in traces {
            // Install trace as maintained index.
            let id = log_index_ids
                .remove(&log)
                .expect("`logging::initialize` does not invent logs");
            self.compute_state.traces.set(id, trace);

            // Initialize compute and logging state for the logging index.
            let is_subscribe_or_copy = false;
            let as_of = Antichain::from_elem(Timestamp::MIN);
            let mut collection = CollectionState::new(is_subscribe_or_copy, as_of);

            let logging =
                CollectionLogging::new(id, logger.clone(), dataflow_index, std::iter::empty());
            collection.logging = Some(logging);

            let existing = self.compute_state.collections.insert(id, collection);
            if existing.is_some() {
                error!(
                    id = ?id,
                    "existing collection for newly initialized logging export",
                );
            }
        }

        // Sanity check.
        assert!(
            log_index_ids.is_empty(),
            "failed to create requested logging indexes: {log_index_ids:?}",
        );

        self.compute_state.compute_logger = Some(logger);
    }

    /// Send progress information to the controller.
    pub fn report_frontiers(&mut self) {
        let mut responses = Vec::new();

        // Maintain a single allocation for `new_frontier` to avoid allocating on every iteration.
        let mut new_frontier = Antichain::new();

        for (&id, collection) in self.compute_state.collections.iter_mut() {
            // The compute protocol does not allow `Frontiers` responses for subscribe and copy-to
            // collections (database-issues#4701).
            if collection.is_subscribe_or_copy {
                continue;
            }

            let reported = collection.reported_frontiers();

            // Collect the write frontier and check for progress.
            new_frontier.clear();
            if let Some(traces) = self.compute_state.traces.get_mut(&id) {
                assert!(
                    collection.sink_write_frontier.is_none(),
                    "collection {id} has multiple frontiers"
                );
                traces.oks_mut().read_upper(&mut new_frontier);
            } else if let Some(frontier) = &collection.sink_write_frontier {
                new_frontier.clone_from(&frontier.borrow());
            } else {
                error!(id = ?id, "collection without write frontier");
                continue;
            }
            let new_write_frontier = reported
                .write_frontier
                .allows_reporting(&new_frontier)
                .then(|| new_frontier.clone());

            // Collect the output frontier and check for progress.
            //
            // By default, the output frontier equals the write frontier (which is still stored in
            // `new_frontier`). If the collection provides a compute frontier, we construct the
            // output frontier by taking the meet of write and compute frontier, to avoid:
            //  * reporting progress through times we have not yet written
            //  * reporting progress through times we have not yet fully processed, for
            //    collections that jump their write frontiers into the future
            if let Some(probe) = &collection.compute_probe {
                probe.with_frontier(|frontier| new_frontier.extend(frontier.iter().copied()));
            }
            let new_output_frontier = reported
                .output_frontier
                .allows_reporting(&new_frontier)
                .then(|| new_frontier.clone());

            // Collect the input frontier and check for progress.
            new_frontier.clear();
            for probe in collection.input_probes.values() {
                probe.with_frontier(|frontier| new_frontier.extend(frontier.iter().copied()));
            }
            let new_input_frontier = reported
                .input_frontier
                .allows_reporting(&new_frontier)
                .then(|| new_frontier.clone());

            if let Some(frontier) = &new_write_frontier {
                collection
                    .set_reported_write_frontier(ReportedFrontier::Reported(frontier.clone()));
            }
            if let Some(frontier) = &new_input_frontier {
                collection
                    .set_reported_input_frontier(ReportedFrontier::Reported(frontier.clone()));
            }
            if let Some(frontier) = &new_output_frontier {
                collection
                    .set_reported_output_frontier(ReportedFrontier::Reported(frontier.clone()));
            }

            let response = FrontiersResponse {
                write_frontier: new_write_frontier,
                input_frontier: new_input_frontier,
                output_frontier: new_output_frontier,
            };
            if response.has_updates() {
                responses.push((id, response));
            }
        }

        for (id, frontiers) in responses {
            self.send_compute_response(ComputeResponse::Frontiers(id, frontiers));
        }
    }

    /// Report dropped collections to the controller.
    pub fn report_dropped_collections(&mut self) {
        let dropped_collections = std::mem::take(&mut self.compute_state.dropped_collections);

        for (id, collection) in dropped_collections {
            // The compute protocol requires us to send a `Frontiers` response with empty frontiers
            // when a collection was dropped, unless:
            //  * The frontier was already reported as empty previously, or
            //  * The collection is a subscribe or copy-to.

            if collection.is_subscribe_or_copy {
                continue;
            }

            let reported = collection.reported_frontiers;
            let write_frontier = (!reported.write_frontier.is_empty()).then(Antichain::new);
            let input_frontier = (!reported.input_frontier.is_empty()).then(Antichain::new);
            let output_frontier = (!reported.output_frontier.is_empty()).then(Antichain::new);

            let frontiers = FrontiersResponse {
                write_frontier,
                input_frontier,
                output_frontier,
            };
            if frontiers.has_updates() {
                self.send_compute_response(ComputeResponse::Frontiers(id, frontiers));
            }
        }
    }

    /// Report operator hydration events.
    pub fn report_operator_hydration(&self) {
        let worker_id = self.timely_worker.index();
        for event in self.compute_state.hydration_rx.try_iter() {
            // The compute protocol forbids reporting `Status` about collections that have advanced
            // to the empty frontier, so we ignore updates for those.
            let collection = self.compute_state.collections.get(&event.export_id);
            if collection.map_or(true, |c| c.reported_frontiers().all_empty()) {
                continue;
            }

            let status = OperatorHydrationStatus {
                collection_id: event.export_id,
                lir_id: event.lir_id,
                worker_id,
                hydrated: event.hydrated,
            };
            let response = ComputeResponse::Status(StatusResponse::OperatorHydration(status));
            self.send_compute_response(response);
        }
    }

    /// Report per-worker metrics.
    pub(crate) fn report_metrics(&self) {
        if let Some(expiration) = self.compute_state.replica_expiration.as_option() {
            let now = Duration::from_millis(mz_ore::now::SYSTEM_TIME()).as_secs_f64();
            let expiration = Duration::from_millis(<u64>::from(expiration)).as_secs_f64();
            let remaining = expiration - now;
            self.compute_state
                .worker_metrics
                .replica_expiration_remaining_seconds
                .set(remaining)
        }
    }

    /// Either complete the peek (and send the response) or put it in the pending set.
    fn process_peek(&mut self, upper: &mut Antichain<Timestamp>, mut peek: PendingPeek) {
        let response = match &mut peek {
            PendingPeek::Index(peek) => {
                peek.seek_fulfillment(upper, self.compute_state.max_result_size)
            }
            PendingPeek::Persist(peek) => peek.result.try_recv().ok().map(|(result, duration)| {
                self.compute_state
                    .metrics
                    .persist_peek_seconds
                    .observe(duration.as_secs_f64());
                result
            }),
        };

        if let Some(response) = response {
            let _span = span!(parent: peek.span(), Level::DEBUG, "process_peek").entered();
            self.send_peek_response(peek, response)
        } else {
            let uuid = peek.peek().uuid;
            self.compute_state.pending_peeks.insert(uuid, peek);
        }
    }

    /// Scan pending peeks and attempt to retire each.
    pub fn process_peeks(&mut self) {
        let mut upper = Antichain::new();
        let pending_peeks = std::mem::take(&mut self.compute_state.pending_peeks);
        for (_uuid, peek) in pending_peeks {
            self.process_peek(&mut upper, peek);
        }
    }

    /// Sends a response for this peek's resolution to the coordinator.
    ///
    /// Note that this function takes ownership of the `PendingPeek`, which is
    /// meant to prevent multiple responses to the same peek.
    #[mz_ore::instrument(level = "debug")]
    fn send_peek_response(&mut self, peek: PendingPeek, response: PeekResponse) {
        let log_event = peek.as_log_event(false);
        // Respond with the response.
        self.send_compute_response(ComputeResponse::PeekResponse(
            peek.peek().uuid,
            response,
            OpenTelemetryContext::obtain(),
        ));

        // Log responding to the peek request.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(&log_event);
        }
    }

    /// Scan the shared subscribe response buffer, and forward results along.
    pub fn process_subscribes(&mut self) {
        let mut subscribe_responses = self.compute_state.subscribe_response_buffer.borrow_mut();
        for (sink_id, mut response) in subscribe_responses.drain(..) {
            // Update frontier logging for this subscribe.
            if let Some(collection) = self.compute_state.collections.get_mut(&sink_id) {
                let new_frontier = match &response {
                    SubscribeResponse::Batch(b) => b.upper.clone(),
                    SubscribeResponse::DroppedAt(_) => Antichain::new(),
                };

                let reported = collection.reported_frontiers();
                assert!(
                    reported.write_frontier.allows_reporting(&new_frontier),
                    "subscribe write frontier regression: {:?} -> {:?}",
                    reported.write_frontier,
                    new_frontier,
                );
                assert!(
                    reported.input_frontier.allows_reporting(&new_frontier),
                    "subscribe input frontier regression: {:?} -> {:?}",
                    reported.input_frontier,
                    new_frontier,
                );

                collection
                    .set_reported_write_frontier(ReportedFrontier::Reported(new_frontier.clone()));
                collection
                    .set_reported_input_frontier(ReportedFrontier::Reported(new_frontier.clone()));
                collection.set_reported_output_frontier(ReportedFrontier::Reported(new_frontier));
            } else {
                // Presumably tracking state for this subscribe was already dropped by
                // `drop_collection`. There is nothing left to do for logging.
            }

            response
                .to_error_if_exceeds(usize::try_from(self.compute_state.max_result_size).unwrap());
            self.send_compute_response(ComputeResponse::SubscribeResponse(sink_id, response));
        }
    }

    /// Scan the shared copy to response buffer, and forward results along.
    pub fn process_copy_tos(&self) {
        let mut responses = self.compute_state.copy_to_response_buffer.borrow_mut();
        for (sink_id, response) in responses.drain(..) {
            self.send_compute_response(ComputeResponse::CopyToResponse(sink_id, response));
        }
    }

    /// Send a response to the coordinator.
    fn send_compute_response(&self, response: ComputeResponse) {
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = self.response_tx.send(response);
    }

    /// Checks for dataflow expiration. Panics if we're past the replica expiration time.
    pub(crate) fn check_expiration(&self) {
        let now = mz_ore::now::SYSTEM_TIME();
        if self.compute_state.replica_expiration.less_than(&now.into()) {
            let now_datetime = mz_ore::now::to_datetime(now);
            let expiration_datetime = self
                .compute_state
                .replica_expiration
                .as_option()
                .map(Into::into)
                .map(mz_ore::now::to_datetime);

            // We error and assert separately to produce structured logs in anything that depends
            // on tracing.
            error!(
                now,
                now_datetime = ?now_datetime,
                expiration = ?self.compute_state.replica_expiration.elements(),
                expiration_datetime = ?expiration_datetime,
                "replica expired"
            );

            // Repeat condition for better error message.
            assert!(
                !self.compute_state.replica_expiration.less_than(&now.into()),
                "replica expired. now: {now} ({now_datetime:?}), expiration: {:?} ({expiration_datetime:?})",
                self.compute_state.replica_expiration.elements(),
            );
        }
    }

    /// Returns the dataflow expiration, i.e, the timestamp beyond which diffs can be
    /// dropped.
    ///
    /// Returns an empty timestamp if `replica_expiration` is unset or matches conditions under
    /// which dataflow expiration should be disabled.
    pub fn determine_dataflow_expiration(
        &self,
        time_dependence: &TimeDependence,
        until: &Antichain<mz_repr::Timestamp>,
    ) -> Antichain<mz_repr::Timestamp> {
        // Evaluate time dependence with respect to the expiration time.
        // * Step time forward to ensure the expiration time is different to the moment a dataflow
        //   can legitimately jump to.
        // * We cannot expire dataflow with an until that is less or equal to the expiration time.
        let iter = self
            .compute_state
            .replica_expiration
            .iter()
            .filter_map(|t| time_dependence.apply(*t))
            .filter_map(|t| mz_repr::Timestamp::try_step_forward(&t))
            .filter(|expiration| !until.less_equal(expiration));
        Antichain::from_iter(iter)
    }
}

/// A peek against either an index or a Persist collection.
///
/// Note that `PendingPeek` intentionally does not implement or derive `Clone`,
/// as each `PendingPeek` is meant to be dropped after it's responded to.
pub enum PendingPeek {
    /// A peek against an index. (Possibly a temporary index created for the purpose.)
    Index(IndexPeek),
    /// A peek against a Persist-backed collection.
    Persist(PersistPeek),
}

impl PendingPeek {
    /// Produces a corresponding log event.
    pub fn as_log_event(&self, installed: bool) -> ComputeEvent {
        let peek = self.peek();
        let (id, peek_type) = match &peek.target {
            PeekTarget::Index { id } => (id, logging::compute::PeekType::Index),
            PeekTarget::Persist { id, .. } => (id, logging::compute::PeekType::Persist),
        };
        ComputeEvent::Peek(PeekEvent {
            peek: logging::compute::Peek::new(*id, peek.timestamp, peek.uuid),
            peek_type,
            installed,
        })
    }

    fn index(peek: Peek, mut trace_bundle: TraceBundle) -> Self {
        let empty_frontier = Antichain::new();
        let timestamp_frontier = Antichain::from_elem(peek.timestamp);
        trace_bundle
            .oks_mut()
            .set_logical_compaction(timestamp_frontier.borrow());
        trace_bundle
            .errs_mut()
            .set_logical_compaction(timestamp_frontier.borrow());
        trace_bundle
            .oks_mut()
            .set_physical_compaction(empty_frontier.borrow());
        trace_bundle
            .errs_mut()
            .set_physical_compaction(empty_frontier.borrow());

        PendingPeek::Index(IndexPeek {
            peek,
            trace_bundle,
            span: tracing::Span::current(),
        })
    }

    fn persist<A: Allocate>(
        peek: Peek,
        persist_clients: Arc<PersistClientCache>,
        metadata: CollectionMetadata,
        max_result_size: usize,
        timely_worker: &TimelyWorker<A>,
    ) -> Self {
        let active_worker = {
            // Choose the worker that does the actual peek arbitrarily but consistently.
            let chosen_index = usize::cast_from(peek.uuid.hashed()) % timely_worker.peers();
            chosen_index == timely_worker.index()
        };
        let activator = timely_worker.sync_activator_for([].into());
        let peek_uuid = peek.uuid;

        let (result_tx, result_rx) = oneshot::channel();
        let timestamp = peek.timestamp;
        let mfp_plan = peek.map_filter_project.clone();
        let max_results_needed = peek
            .finishing
            .limit
            .map(|l| usize::cast_from(u64::from(l)))
            .unwrap_or(usize::MAX)
            + peek.finishing.offset;
        let order_by = peek.finishing.order_by.clone();

        let task_handle = mz_ore::task::spawn(|| "persist::peek", async move {
            let start = Instant::now();
            let result = if active_worker {
                PersistPeek::do_peek(
                    &persist_clients,
                    metadata,
                    timestamp,
                    mfp_plan,
                    max_result_size,
                    max_results_needed,
                )
                .await
            } else {
                Ok(vec![])
            };
            let result = match result {
                Ok(rows) => PeekResponse::Rows(RowCollection::new(rows, &order_by)),
                Err(e) => PeekResponse::Error(e.to_string()),
            };
            match result_tx.send((result, start.elapsed())) {
                Ok(()) => {}
                Err((_result, elapsed)) => {
                    debug!(duration =? elapsed, "dropping result for cancelled peek {peek_uuid}")
                }
            }
            match activator.activate() {
                Ok(()) => {}
                Err(_) => {
                    debug!("unable to wake timely after completed peek {peek_uuid}");
                }
            }
        });
        PendingPeek::Persist(PersistPeek {
            peek,
            _abort_handle: task_handle.abort_on_drop(),
            result: result_rx,
            span: tracing::Span::current(),
        })
    }

    fn span(&self) -> &tracing::Span {
        match self {
            PendingPeek::Index(p) => &p.span,
            PendingPeek::Persist(p) => &p.span,
        }
    }

    pub(crate) fn peek(&self) -> &Peek {
        match self {
            PendingPeek::Index(p) => &p.peek,
            PendingPeek::Persist(p) => &p.peek,
        }
    }
}

/// An in-progress Persist peek.
///
/// Note that `PendingPeek` intentionally does not implement or derive `Clone`,
/// as each `PendingPeek` is meant to be dropped after it's responded to.
pub struct PersistPeek {
    pub(crate) peek: Peek,
    /// A background task that's responsible for producing the peek results.
    /// If we're no longer interested in the results, we abort the task.
    _abort_handle: AbortOnDropHandle<()>,
    /// The result of the background task, eventually.
    result: oneshot::Receiver<(PeekResponse, Duration)>,
    /// The `tracing::Span` tracking this peek's operation
    span: tracing::Span,
}

impl PersistPeek {
    async fn do_peek(
        persist_clients: &PersistClientCache,
        metadata: CollectionMetadata,
        as_of: Timestamp,
        mfp_plan: SafeMfpPlan,
        max_result_size: usize,
        mut limit_remaining: usize,
    ) -> Result<Vec<(Row, NonZeroUsize)>, String> {
        let client = persist_clients
            .open(metadata.persist_location)
            .await
            .map_err(|e| e.to_string())?;

        let mut reader: ReadHandle<SourceData, (), Timestamp, Diff> = client
            .open_leased_reader(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("persist::peek"),
                USE_CRITICAL_SINCE_SNAPSHOT.get(client.dyncfgs()),
            )
            .await
            .map_err(|e| e.to_string())?;

        // If we are using txn-wal for this collection, then the upper might
        // be advanced lazily and we have to go through txn-wal for reads.
        //
        // TODO: If/when we have a process-wide TxnsRead worker for clusterd,
        // use in here (instead of opening a new TxnsCache) to save a persist
        // reader registration and some txns shard read traffic.
        let mut txns_read = if let Some(txns_id) = metadata.txns_shard {
            Some(TxnsCache::open(&client, txns_id, Some(metadata.data_shard)).await)
        } else {
            None
        };

        let metrics = client.metrics();

        let mut cursor = StatsCursor::new(
            &mut reader,
            txns_read.as_mut(),
            metrics,
            &metadata.relation_desc,
            Antichain::from_elem(as_of),
        )
        .await
        .map_err(|since| {
            format!("attempted to peek at {as_of}, but the since has advanced to {since:?}")
        })?;

        // Re-used state for processing and building rows.
        let mut result = vec![];
        let mut datum_vec = DatumVec::new();
        let mut row_builder = Row::default();
        let arena = RowArena::new();
        let mut total_size = 0usize;

        while limit_remaining > 0 {
            let Some(batch) = cursor.next().await else {
                break;
            };
            for (data, _, d) in batch {
                let row = data.map_err(|e| e.to_string())?;
                let count: usize = d.try_into().map_err(|_| {
                    format!(
                        "Invalid data in source, saw retractions ({}) for row that does not exist: {:?}",
                        d * -1,
                        row,
                    )
                })?;
                let Some(count) = NonZeroUsize::new(count) else {
                    continue;
                };
                let mut datum_local = datum_vec.borrow_with(&row);
                let eval_result = mfp_plan
                    .evaluate_into(&mut datum_local, &arena, &mut row_builder)
                    .map_err(|e| e.to_string())?;
                if let Some(row) = eval_result {
                    total_size = total_size
                        .saturating_add(row.byte_len())
                        .saturating_add(std::mem::size_of::<NonZeroUsize>());
                    if total_size > max_result_size {
                        return Err(format!(
                            "result exceeds max size of {}",
                            ByteSize::b(u64::cast_from(max_result_size))
                        ));
                    }
                    result.push((row, count));
                    limit_remaining = limit_remaining.saturating_sub(count.get());
                    if limit_remaining == 0 {
                        break;
                    }
                }
            }
        }

        Ok(result)
    }
}

/// An in-progress index-backed peek, and data to eventually fulfill it.
pub struct IndexPeek {
    peek: Peek,
    /// The data from which the trace derives.
    trace_bundle: TraceBundle,
    /// The `tracing::Span` tracking this peek's operation
    span: tracing::Span,
}

impl IndexPeek {
    /// Attempts to fulfill the peek and reports success.
    ///
    /// To produce output at `peek.timestamp`, we must be certain that
    /// it is no longer changing. A trace guarantees that all future
    /// changes will be greater than or equal to an element of `upper`.
    ///
    /// If an element of `upper` is less or equal to `peek.timestamp`,
    /// then there can be further updates that would change the output.
    /// If no element of `upper` is less or equal to `peek.timestamp`,
    /// then for any time `t` less or equal to `peek.timestamp` it is
    /// not the case that `upper` is less or equal to that timestamp,
    /// and so the result cannot further evolve.
    fn seek_fulfillment(
        &mut self,
        upper: &mut Antichain<Timestamp>,
        max_result_size: u64,
    ) -> Option<PeekResponse> {
        self.trace_bundle.oks_mut().read_upper(upper);
        if upper.less_equal(&self.peek.timestamp) {
            return None;
        }
        self.trace_bundle.errs_mut().read_upper(upper);
        if upper.less_equal(&self.peek.timestamp) {
            return None;
        }

        let read_frontier = self.trace_bundle.compaction_frontier();
        if !read_frontier.less_equal(&self.peek.timestamp) {
            let error = format!(
                "Arrangement compaction frontier ({:?}) is beyond the time of the attempted read ({})",
                read_frontier.elements(),
                self.peek.timestamp,
            );
            return Some(PeekResponse::Error(error));
        }

        let response = match self.collect_finished_data(max_result_size) {
            Ok(rows) => PeekResponse::Rows(RowCollection::new(rows, &self.peek.finishing.order_by)),
            Err(text) => PeekResponse::Error(text),
        };
        Some(response)
    }

    /// Collects data for a known-complete peek from the ok stream.
    fn collect_finished_data(
        &mut self,
        max_result_size: u64,
    ) -> Result<Vec<(Row, NonZeroUsize)>, String> {
        // Check if there exist any errors and, if so, return whatever one we
        // find first.
        let (mut cursor, storage) = self.trace_bundle.errs_mut().cursor();
        while cursor.key_valid(&storage) {
            let mut copies = 0;
            cursor.map_times(&storage, |time, diff| {
                if time.less_equal(&self.peek.timestamp) {
                    copies += diff;
                }
            });
            if copies < 0 {
                return Err(format!(
                    "Invalid data in source errors, saw retractions ({}) for row that does not exist: {}",
                    copies * -1,
                    cursor.key(&storage),
                ));
            }
            if copies > 0 {
                return Err(cursor.key(&storage).to_string());
            }
            cursor.step_key(&storage);
        }

        Self::collect_ok_finished_data(&mut self.peek, self.trace_bundle.oks_mut(), max_result_size)
    }

    /// Collects data for a known-complete peek from the ok stream.
    fn collect_ok_finished_data<Tr>(
        peek: &mut Peek<Timestamp>,
        oks_handle: &mut Tr,
        max_result_size: u64,
    ) -> Result<Vec<(Row, NonZeroUsize)>, String>
    where
        for<'a> Tr: TraceReader<DiffGat<'a> = &'a Diff>,
        for<'a> Tr::Key<'a>: ToDatumIter + IntoOwned<'a, Owned = Row> + Eq,
        for<'a> Tr::Val<'a>: ToDatumIter,
        for<'a> Tr::TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
    {
        let max_result_size = usize::cast_from(max_result_size);
        let count_byte_size = std::mem::size_of::<NonZeroUsize>();

        // Cursor and bound lifetime for `Row` data in the backing trace.
        let (mut cursor, storage) = oks_handle.cursor();
        // Accumulated `Vec<(row, count)>` results that we are likely to return.
        let mut results = Vec::new();
        let mut total_size: usize = 0;

        // When set, a bound on the number of records we need to return.
        // The requirements on the records are driven by the finishing's
        // `order_by` field. Further limiting will happen when the results
        // are collected, so we don't need to have exactly this many results,
        // just at least those results that would have been returned.
        let max_results = peek
            .finishing
            .limit
            .map(|l| usize::cast_from(u64::from(l)) + peek.finishing.offset);

        use mz_ore::result::ResultExt;

        let mut row_builder = Row::default();
        let mut datum_vec = DatumVec::new();
        let mut l_datum_vec = DatumVec::new();
        let mut r_datum_vec = DatumVec::new();

        // We have to sort the literal constraints because cursor.seek_key can seek only forward.
        peek.literal_constraints
            .iter_mut()
            .for_each(|vec| vec.sort());
        let has_literal_constraints = peek.literal_constraints.is_some();
        let mut literals = peek.literal_constraints.iter().flatten();
        let mut current_literal = None;

        while cursor.key_valid(&storage) {
            if has_literal_constraints {
                loop {
                    // Go to the next literal constraint.
                    // (i.e., to the next OR argument in something like `c=3 OR c=7 OR c=9`)
                    current_literal = literals.next();
                    match current_literal {
                        None => return Ok(results),
                        Some(current_literal) => {
                            // NOTE(vmarcos): We expect the extra allocations below to be manageable
                            // since we only perform as many of them as there are literals.
                            cursor.seek_key(&storage, IntoOwned::borrow_as(current_literal));
                            if !cursor.key_valid(&storage) {
                                return Ok(results);
                            }
                            if cursor.get_key(&storage).unwrap()
                                == IntoOwned::borrow_as(current_literal)
                            {
                                // The cursor found a record whose key matches the current literal.
                                // We break from the inner loop, and process this key.
                                break;
                            }
                            // The cursor landed on a record that has a different key, meaning that there is
                            // no record whose key would match the current literal.
                        }
                    }
                }
            }

            while cursor.val_valid(&storage) {
                // TODO: This arena could be maintained and reused for longer,
                // but it wasn't clear at what interval we should flush
                // it to ensure we don't accidentally spike our memory use.
                // This choice is conservative, and not the end of the world
                // from a performance perspective.
                let arena = RowArena::new();

                let key_item = cursor.key(&storage);
                let key = key_item.to_datum_iter();
                let row_item = cursor.val(&storage);
                let row = row_item.to_datum_iter();

                let mut borrow = datum_vec.borrow();
                borrow.extend(key);
                borrow.extend(row);

                if has_literal_constraints {
                    // The peek was created from an IndexedFilter join. We have to add those columns
                    // here that the join would add in a dataflow.
                    let datum_vec = borrow.deref_mut();
                    // unwrap is ok, because it could be None only if !has_literal_constraints or if
                    // the iteration is finished. In the latter case we already exited the while
                    // loop.
                    datum_vec.extend(current_literal.unwrap().iter());
                }
                if let Some(result) = peek
                    .map_filter_project
                    .evaluate_into(&mut borrow, &arena, &mut row_builder)
                    .map_err_to_string_with_causes()?
                {
                    let mut copies = 0;
                    cursor.map_times(&storage, |time, diff| {
                        if time.less_equal(&peek.timestamp) {
                            copies += diff;
                        }
                    });
                    let copies: usize = if copies < 0 {
                        return Err(format!(
                            "Invalid data in source, saw retractions ({}) for row that does not exist: {:?}",
                            copies * -1,
                            &*borrow,
                        ));
                    } else {
                        copies.try_into().unwrap()
                    };
                    // if copies > 0 ... otherwise skip
                    if let Some(copies) = NonZeroUsize::new(copies) {
                        total_size = total_size
                            .saturating_add(result.byte_len())
                            .saturating_add(count_byte_size);
                        if total_size > max_result_size {
                            return Err(format!(
                                "result exceeds max size of {}",
                                ByteSize::b(u64::cast_from(max_result_size))
                            ));
                        }
                        results.push((result, copies));
                    }

                    // If we hold many more than `max_results` records, we can thin down
                    // `results` using `self.finishing.ordering`.
                    if let Some(max_results) = max_results {
                        // We use a threshold twice what we intend, to amortize the work
                        // across all of the insertions. We could tighten this, but it
                        // works for the moment.
                        if results.len() >= 2 * max_results {
                            if peek.finishing.order_by.is_empty() {
                                results.truncate(max_results);
                                return Ok(results);
                            } else {
                                // We can sort `results` and then truncate to `max_results`.
                                // This has an effect similar to a priority queue, without
                                // its interactive dequeueing properties.
                                // TODO: Had we left these as `Vec<Datum>` we would avoid
                                // the unpacking; we should consider doing that, although
                                // it will require a re-pivot of the code to branch on this
                                // inner test (as we prefer not to maintain `Vec<Datum>`
                                // in the other case).
                                results.sort_by(|left, right| {
                                    let left_datums = l_datum_vec.borrow_with(&left.0);
                                    let right_datums = r_datum_vec.borrow_with(&right.0);
                                    mz_expr::compare_columns(
                                        &peek.finishing.order_by,
                                        &left_datums,
                                        &right_datums,
                                        || left.0.cmp(&right.0),
                                    )
                                });
                                let dropped = results.drain(max_results..);
                                let dropped_size =
                                    dropped.into_iter().fold(0, |acc: usize, (row, _count)| {
                                        acc.saturating_add(
                                            row.byte_len().saturating_add(count_byte_size),
                                        )
                                    });
                                total_size = total_size.saturating_sub(dropped_size);
                            }
                        }
                    }
                }
                cursor.step_val(&storage);
            }
            // The cursor doesn't have anything more to say for the current key.

            if !has_literal_constraints {
                // We are simply stepping through all the keys that the index has.
                cursor.step_key(&storage);
            }
        }

        Ok(results)
    }
}

/// The frontiers we have reported to the controller for a collection.
#[derive(Debug)]
struct ReportedFrontiers {
    /// The reported write frontier.
    write_frontier: ReportedFrontier,
    /// The reported input frontier.
    input_frontier: ReportedFrontier,
    /// The reported output frontier.
    output_frontier: ReportedFrontier,
}

impl ReportedFrontiers {
    /// Creates a new `ReportedFrontiers` instance.
    fn new() -> Self {
        Self {
            write_frontier: ReportedFrontier::new(),
            input_frontier: ReportedFrontier::new(),
            output_frontier: ReportedFrontier::new(),
        }
    }

    /// Returns whether all reported frontiers are empty.
    fn all_empty(&self) -> bool {
        self.write_frontier.is_empty()
            && self.input_frontier.is_empty()
            && self.output_frontier.is_empty()
    }
}

/// A frontier we have reported to the controller, or the least frontier we are allowed to report.
#[derive(Clone, Debug)]
pub enum ReportedFrontier {
    /// A frontier has been previously reported.
    Reported(Antichain<Timestamp>),
    /// No frontier has been reported yet.
    NotReported {
        /// A lower bound for frontiers that may be reported in the future.
        lower: Antichain<Timestamp>,
    },
}

impl ReportedFrontier {
    /// Create a new `ReportedFrontier` enforcing the minimum lower bound.
    pub fn new() -> Self {
        let lower = Antichain::from_elem(timely::progress::Timestamp::minimum());
        Self::NotReported { lower }
    }

    /// Whether the reported frontier is the empty frontier.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Reported(frontier) => frontier.is_empty(),
            Self::NotReported { .. } => false,
        }
    }

    /// Whether this `ReportedFrontier` allows reporting the given frontier.
    ///
    /// A `ReportedFrontier` allows reporting of another frontier if:
    ///  * The other frontier is greater than the reported frontier.
    ///  * The other frontier is greater than or equal to the lower bound.
    fn allows_reporting(&self, other: &Antichain<Timestamp>) -> bool {
        match self {
            Self::Reported(frontier) => PartialOrder::less_than(frontier, other),
            Self::NotReported { lower } => PartialOrder::less_equal(lower, other),
        }
    }
}

/// State maintained for a compute collection.
pub struct CollectionState {
    /// Tracks the frontiers that have been reported to the controller.
    reported_frontiers: ReportedFrontiers,
    /// Whether this collection is a subscribe or copy-to.
    ///
    /// The compute protocol does not allow `Frontiers` responses for subscribe and copy-to
    /// collections, so we need to be able to recognize them. This is something we would like to
    /// change in the future (database-issues#4701).
    pub is_subscribe_or_copy: bool,
    /// The collection's initial as-of frontier.
    ///
    /// Used to determine hydration status.
    as_of: Antichain<Timestamp>,

    /// A token that should be dropped when this collection is dropped to clean up associated
    /// sink state.
    ///
    /// Only `Some` if the collection is a sink.
    pub sink_token: Option<SinkToken>,
    /// Frontier of sink writes.
    ///
    /// Only `Some` if the collection is a sink and *not* a subscribe.
    pub sink_write_frontier: Option<Rc<RefCell<Antichain<Timestamp>>>>,
    /// Frontier probes for every input to the collection.
    pub input_probes: BTreeMap<GlobalId, probe::Handle<Timestamp>>,
    /// A probe reporting the frontier of times through which all collection outputs have been
    /// computed (but not necessarily written).
    ///
    /// `None` for collections with compute frontiers equal to their write frontiers.
    pub compute_probe: Option<probe::Handle<Timestamp>>,
    /// Logging state maintained for this collection.
    logging: Option<CollectionLogging>,
}

impl CollectionState {
    fn new(is_subscribe_or_copy: bool, as_of: Antichain<Timestamp>) -> Self {
        Self {
            reported_frontiers: ReportedFrontiers::new(),
            is_subscribe_or_copy,
            as_of,
            sink_token: None,
            sink_write_frontier: None,
            input_probes: Default::default(),
            compute_probe: None,
            logging: None,
        }
    }

    /// Return the frontiers that have been reported to the controller.
    fn reported_frontiers(&self) -> &ReportedFrontiers {
        &self.reported_frontiers
    }

    /// Reset all reported frontiers to the given value.
    pub fn reset_reported_frontiers(&mut self, frontier: ReportedFrontier) {
        self.reported_frontiers.write_frontier = frontier.clone();
        self.reported_frontiers.input_frontier = frontier.clone();
        self.reported_frontiers.output_frontier = frontier;
    }

    /// Set the write frontier that has been reported to the controller.
    fn set_reported_write_frontier(&mut self, frontier: ReportedFrontier) {
        if let Some(logging) = &mut self.logging {
            let time = match &frontier {
                ReportedFrontier::Reported(frontier) => frontier.get(0).copied(),
                ReportedFrontier::NotReported { .. } => Some(Timestamp::MIN),
            };
            logging.set_frontier(time);
        }

        self.reported_frontiers.write_frontier = frontier;
    }

    /// Set the input frontier that has been reported to the controller.
    fn set_reported_input_frontier(&mut self, frontier: ReportedFrontier) {
        // Use this opportunity to update our input frontier logging.
        if let Some(logging) = &mut self.logging {
            for (id, probe) in &self.input_probes {
                let new_time = probe.with_frontier(|frontier| frontier.as_option().copied());
                logging.set_import_frontier(*id, new_time);
            }
        }

        self.reported_frontiers.input_frontier = frontier;
    }

    /// Set the output frontier that has been reported to the controller.
    fn set_reported_output_frontier(&mut self, frontier: ReportedFrontier) {
        let already_hydrated = self.hydrated();

        self.reported_frontiers.output_frontier = frontier;

        if !already_hydrated && self.hydrated() {
            if let Some(logging) = &mut self.logging {
                logging.set_hydrated();
            }
        }
    }

    /// Return whether this collection is hydrated.
    fn hydrated(&self) -> bool {
        match &self.reported_frontiers.output_frontier {
            ReportedFrontier::Reported(frontier) => PartialOrder::less_than(&self.as_of, frontier),
            ReportedFrontier::NotReported { .. } => false,
        }
    }
}

/// State remembered about a dropped compute collection.
///
/// This is the subset of the full [`CollectionState`] that survives the invocation of
/// `drop_collection`, until it is finally dropped in `report_dropped_collections`. It includes any
/// information required to report the dropping of a collection to the controller.
///
/// Note that this state must _not_ store any state (such as tokens) whose dropping releases
/// resources elsewhere in the system. A `DroppedCollection` for a collection dropped during
/// reconciliation might be alive at the same time as the [`CollectionState`] for the re-created
/// collection, and if the dropped collection hasn't released all its held resources by the time
/// the new one is created, conflicts can ensue.
pub struct DroppedCollection {
    reported_frontiers: ReportedFrontiers,
    is_subscribe_or_copy: bool,
}

/// An event reporting the hydration status of an LIR node in a dataflow.
pub struct HydrationEvent {
    /// The ID of the export this dataflow maintains.
    pub export_id: GlobalId,
    /// The ID of the LIR node.
    pub lir_id: LirId,
    /// Whether the node is hydrated.
    pub hydrated: bool,
}
