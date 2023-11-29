// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for compute timely instances.

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::{Data, Hashable};
use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::protocol::command::{
    ComputeCommand, ComputeParameters, InstanceConfig, Peek, PeekTarget,
};
use mz_compute_client::protocol::history::ComputeCommandHistory;
use mz_compute_client::protocol::response::{ComputeResponse, PeekResponse, SubscribeResponse};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::SafeMfpPlan;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::UIntGauge;
use mz_ore::task::{AbortHandleExt, AbortOnDropAbortHandle};
use mz_ore::tracing::{OpenTelemetryContext, TracingHandle};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::fixed_length::{FromRowByTypes, IntoRowByTypes};
use mz_repr::{ColumnType, DatumVec, Diff, GlobalId, Row, RowArena, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use timely::communication::Allocate;
use timely::container::columnation::Columnation;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::oneshot;
use tracing::{error, info, span, warn, Level};
use uuid::Uuid;

use crate::arrangement::manager::{SpecializedTraceHandle, TraceBundle, TraceManager};
use crate::logging;
use crate::logging::compute::ComputeEvent;
use crate::metrics::ComputeMetrics;
use crate::render::{LinearJoinImpl, LinearJoinSpec};
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
    pub dropped_collections: Vec<GlobalId>,
    /// The traces available for sharing across dataflows.
    pub traces: TraceManager,
    /// Shared buffer with SUBSCRIBE operator instances by which they can respond.
    ///
    /// The entries are pairs of sink identifier (to identify the subscribe instance)
    /// and the response itself.
    pub subscribe_response_buffer: Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>,
    /// Peek commands that are awaiting fulfillment.
    pub pending_peeks: BTreeMap<Uuid, PendingPeek>,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub compute_logger: Option<logging::compute::Logger>,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<PersistClientCache>,
    /// History of commands received by this workers and all its peers.
    pub command_history: ComputeCommandHistory<UIntGauge>,
    /// Max size in bytes of any result.
    max_result_size: u32,
    /// Maximum number of in-flight bytes emitted by persist_sources feeding dataflows.
    pub dataflow_max_inflight_bytes: Option<usize>,
    /// Specification for rendering linear joins.
    pub linear_join_spec: LinearJoinSpec,
    /// Metrics for this replica.
    pub metrics: ComputeMetrics,
    /// A process-global handle to tracing configuration.
    tracing_handle: Arc<TracingHandle>,
    /// Enable arrangement type specialization.
    pub enable_specialized_arrangements: bool,
    /// Other configuration for compute
    pub context: ComputeInstanceContext,
}

impl ComputeState {
    /// Construct a new `ComputeState`.
    pub fn new(
        worker_id: usize,
        persist_clients: Arc<PersistClientCache>,
        metrics: ComputeMetrics,
        tracing_handle: Arc<TracingHandle>,
        context: ComputeInstanceContext,
    ) -> Self {
        let traces = TraceManager::new(metrics.for_traces(worker_id));
        let command_history = ComputeCommandHistory::new(metrics.for_history(worker_id));

        Self {
            collections: Default::default(),
            dropped_collections: Default::default(),
            traces,
            subscribe_response_buffer: Default::default(),
            pending_peeks: Default::default(),
            compute_logger: None,
            persist_clients,
            command_history,
            max_result_size: u32::MAX,
            dataflow_max_inflight_bytes: None,
            linear_join_spec: Default::default(),
            metrics,
            tracing_handle,
            enable_specialized_arrangements: Default::default(),
            context,
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
pub struct SinkToken(Box<dyn Any>);

impl SinkToken {
    /// Create a new `SinkToken`.
    pub fn new(t: Box<dyn Any>) -> Self {
        Self(t)
    }
}

impl<'a, A: Allocate + 'static> ActiveComputeState<'a, A> {
    /// Entrypoint for applying a compute command.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn handle_compute_command(&mut self, cmd: ComputeCommand) {
        use ComputeCommand::*;

        self.compute_state.command_history.push(cmd.clone());

        match cmd {
            CreateTimely { .. } => panic!("CreateTimely must be captured before"),
            CreateInstance(instance_config) => self.handle_create_instance(instance_config),
            InitializationComplete => (),
            UpdateConfiguration(params) => self.handle_update_configuration(params),
            CreateDataflow(dataflow) => self.handle_create_dataflow(dataflow),
            AllowCompaction { id, frontier } => self.handle_allow_compaction(id, frontier),
            Peek(peek) => {
                peek.otel_ctx.attach_as_parent();
                self.handle_peek(peek)
            }
            CancelPeek { uuid } => self.handle_cancel_peek(uuid),
        }
    }

    fn handle_create_instance(&mut self, config: InstanceConfig) {
        self.initialize_logging(&config.logging);
    }

    fn handle_update_configuration(&mut self, params: ComputeParameters) {
        info!("Applying configuration update: {params:?}");

        let ComputeParameters {
            max_result_size,
            dataflow_max_inflight_bytes,
            linear_join_yielding,
            enable_mz_join_core,
            enable_jemalloc_profiling,
            enable_specialized_arrangements,
            enable_columnation_lgalloc,
            persist,
            tracing,
            grpc_client: _grpc_client,
        } = params;

        if let Some(v) = max_result_size {
            self.compute_state.max_result_size = v;
        }
        if let Some(v) = dataflow_max_inflight_bytes {
            self.compute_state.dataflow_max_inflight_bytes = v;
        }
        if let Some(v) = linear_join_yielding {
            self.compute_state.linear_join_spec.yielding = v;
        }
        if let Some(v) = enable_specialized_arrangements {
            self.compute_state.enable_specialized_arrangements = v;
        }
        if let Some(v) = enable_mz_join_core {
            self.compute_state.linear_join_spec.implementation = match v {
                false => LinearJoinImpl::DifferentialDataflow,
                true => LinearJoinImpl::Materialize,
            };
        }
        match enable_jemalloc_profiling {
            Some(true) => {
                mz_ore::task::spawn(
                    || "activate-jemalloc-profiling",
                    mz_prof::activate_jemalloc_profiling(),
                );
            }
            Some(false) => {
                mz_ore::task::spawn(
                    || "deactivate-jemalloc-profiling",
                    mz_prof::deactivate_jemalloc_profiling(),
                );
            }
            None => (),
        }
        match enable_columnation_lgalloc {
            Some(true) => {
                if let Some(path) = self.compute_state.context.scratch_directory.as_ref() {
                    info!(path = ?path, "Enabling lgalloc");
                    lgalloc::lgalloc_set_config(
                        lgalloc::LgAlloc::new()
                            .enable()
                            .with_path(path.clone())
                            .with_background_config(lgalloc::BackgroundWorkerConfig {
                                interval: Duration::from_secs(1),
                                batch: 32,
                            }),
                    );
                } else {
                    warn!("Not enabling lgalloc, scratch directory not specified");
                }
            }
            Some(false) => {
                info!("Disabling lgalloc");
                lgalloc::lgalloc_set_config(&lgalloc::LgAlloc::new())
            }
            None => {}
        }

        persist.apply(self.compute_state.persist_clients.cfg());
        tracing.apply(self.compute_state.tracing_handle.as_ref());
    }

    fn handle_create_dataflow(&mut self, dataflow: DataflowDescription<Plan, CollectionMetadata>) {
        // Collect the exported object identifiers, paired with their associated "collection" identifier.
        // The latter is used to extract dependency information, which is in terms of collections ids.
        let dataflow_index = self.timely_worker.next_dataflow_index();
        let as_of = dataflow.as_of.clone().unwrap();

        if dataflow.is_transient() {
            tracing::debug!(
                name = %dataflow.debug_name,
                import_ids = %dataflow.display_import_ids(),
                export_ids = %dataflow.display_export_ids(),
                as_of = ?as_of.elements(),
                until = ?dataflow.until.elements(),
                "creating dataflow",
            );
        } else {
            tracing::info!(
                name = %dataflow.debug_name,
                import_ids = %dataflow.display_import_ids(),
                export_ids = %dataflow.display_export_ids(),
                as_of = ?as_of.elements(),
                until = ?dataflow.until.elements(),
                "creating dataflow",
            );
        };

        // Initialize compute and logging state for each object.
        for object_id in dataflow.export_ids() {
            let mut collection = CollectionState::new();

            collection.reported_frontier = ReportedFrontier::NotReported {
                lower: as_of.clone(),
            };

            let existing = self.compute_state.collections.insert(object_id, collection);
            if existing.is_some() {
                error!(
                    id = ?object_id,
                    "existing collection for newly created dataflow",
                );
            }

            // Log dataflow construction and frontier construction.
            if let Some(logger) = self.compute_state.compute_logger.as_mut() {
                logger.log(ComputeEvent::Export {
                    id: object_id,
                    dataflow_index,
                });
                logger.log(ComputeEvent::Frontier {
                    id: object_id,
                    time: timely::progress::Timestamp::minimum(),
                    diff: 1,
                });
            }
        }

        crate::render::build_compute_dataflow(self.timely_worker, self.compute_state, dataflow);
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

    #[tracing::instrument(level = "debug", skip(self))]
    fn handle_peek(&mut self, peek: Peek) {
        let pending = match &peek.target {
            PeekTarget::Index { id } => {
                // Acquire a copy of the trace suitable for fulfilling the peek.
                let trace_bundle = self.compute_state.traces.get(id).unwrap().clone();
                PendingPeek::index(peek, trace_bundle)
            }
            PeekTarget::Persist { metadata, .. } => {
                let active_worker = {
                    // Choose the worker that does the actual peek arbitrarily but consistently.
                    let chosen_index =
                        usize::cast_from(peek.uuid.hashed()) % self.timely_worker.peers();
                    chosen_index == self.timely_worker.index()
                };
                let metadata = metadata.clone();
                PendingPeek::persist(
                    peek,
                    Arc::clone(&self.compute_state.persist_clients),
                    metadata,
                    active_worker,
                )
            }
        };

        // Log the receipt of the peek.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(ComputeEvent::Peek(pending.as_log_event(), true));
        }

        self.process_peek(&mut Antichain::new(), pending);
    }

    fn handle_cancel_peek(&mut self, uuid: Uuid) {
        if let Some(peek) = self.compute_state.pending_peeks.remove(&uuid) {
            self.send_peek_response(peek, PeekResponse::Canceled);
        }
    }

    fn drop_collection(&mut self, id: GlobalId) {
        let collection = self
            .compute_state
            .collections
            .remove(&id)
            .expect("dropped untracked collection");

        // If this collection is an index, remove its trace.
        self.compute_state.traces.del_trace(&id);

        // Remove frontier logging.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(ComputeEvent::ExportDropped { id });
            if let Some(time) = collection.reported_frontier.logging_time() {
                logger.log(ComputeEvent::Frontier { id, time, diff: -1 });
            }
        }

        // We need to emit a final response reporting the dropping of this collection,
        // unless:
        //  * The collection is a subscribe, in which case we will emit a
        //    `SubscribeResponse::Dropped` independently.
        //  * The collection has already advanced to the empty frontier, in which case
        //    the final `FrontierUpper` response already serves the purpose of reporting
        //    the end of the dataflow.
        if !collection.is_subscribe() && !collection.reported_frontier.is_empty() {
            self.compute_state.dropped_collections.push(id);
        }
    }

    /// Initializes timely dataflow logging and publishes as a view.
    pub fn initialize_logging(&mut self, config: &LoggingConfig) {
        if self.compute_state.compute_logger.is_some() {
            panic!("dataflow server has already initialized logging");
        }

        let worker_id = self.timely_worker.index();
        let metrics = self.compute_state.metrics.for_logging(worker_id);

        let (logger, traces) = logging::initialize(self.timely_worker, config, metrics);

        // Install traces as maintained indexes
        for (log, trace) in traces {
            let id = config.index_logs[&log];
            self.compute_state.traces.set(id, trace);
        }

        // Initialize compute and logging state for each logging index.
        let index_ids = config.index_logs.values().copied();
        for id in index_ids {
            let collection = CollectionState::new();
            let existing = self.compute_state.collections.insert(id, collection);
            if existing.is_some() {
                error!(
                    id = ?id,
                    "existing collection for newly initialized logging export",
                );
            }

            logger.log(ComputeEvent::Frontier {
                id,
                time: timely::progress::Timestamp::minimum(),
                diff: 1,
            });
        }

        self.compute_state.compute_logger = Some(logger);
    }

    /// Send progress information to the coordinator.
    pub fn report_compute_frontiers(&mut self) {
        let mut new_uppers = Vec::new();

        // Maintain a single allocation for `new_frontier` to avoid allocating on every iteration.
        let mut new_frontier = Antichain::new();

        for (&id, collection) in self.compute_state.collections.iter_mut() {
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
                // Subscribe frontiers are reported in `process_subscribes` instead.
                if !collection.is_subscribe() {
                    error!(id = ?id, "collection without frontier");
                }
                continue;
            }

            match &collection.reported_frontier {
                ReportedFrontier::Reported(old_frontier) => {
                    // In steady state it is not expected for `old_frontier` to be beyond
                    // `new_frontier`. However, during reconcilation this can happen as we
                    // artificially advance the frontiers of to-be-dropped collections to disable
                    // frontier reporting for them.
                    if !PartialOrder::less_than(old_frontier, &new_frontier) {
                        continue; // nothing new to report
                    }
                }
                ReportedFrontier::NotReported { lower } => {
                    if !PartialOrder::less_equal(lower, &new_frontier) {
                        continue; // lower bound for reporting not yet reached
                    }
                }
            }

            let new_reported_frontier = ReportedFrontier::Reported(new_frontier.clone());

            if let Some(logger) = self.compute_state.compute_logger.as_mut() {
                if let Some(time) = collection.reported_frontier.logging_time() {
                    logger.log(ComputeEvent::Frontier { id, time, diff: -1 });
                }
                if let Some(time) = new_reported_frontier.logging_time() {
                    logger.log(ComputeEvent::Frontier { id, time, diff: 1 });
                }
            }

            new_uppers.push((id, new_frontier.clone()));
            collection.reported_frontier = new_reported_frontier;
        }

        for (id, upper) in new_uppers {
            self.send_compute_response(ComputeResponse::FrontierUpper { id, upper });
        }
    }

    /// Report dropped collections to the controller.
    pub fn report_dropped_collections(&mut self) {
        let dropped_collections = std::mem::take(&mut self.compute_state.dropped_collections);

        // TODO(#16275): It is, in fact, wrong to report the dropping of a collection before it has
        // advanced to the empty frontier by announcing that it has advanced to the empty
        // frontier. We should introduce a new compute response variant that has the right
        // semantics.
        for id in dropped_collections {
            // Sanity check: A collection that was dropped should not exist.
            assert!(
                !self.compute_state.collection_exists(id),
                "tried to report a dropped collection that still exists: {id}"
            );
            self.send_compute_response(ComputeResponse::FrontierUpper {
                id,
                upper: Antichain::new(),
            });
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
    #[tracing::instrument(level = "debug", skip(self, peek))]
    fn send_peek_response(&mut self, peek: PendingPeek, response: PeekResponse) {
        let log_event = peek.as_log_event();
        // Respond with the response.
        self.send_compute_response(ComputeResponse::PeekResponse(
            peek.peek().uuid,
            response,
            OpenTelemetryContext::obtain(),
        ));

        // Log responding to the peek request.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(ComputeEvent::Peek(log_event, false));
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

                match &collection.reported_frontier {
                    ReportedFrontier::Reported(old_frontier) => {
                        assert!(
                            PartialOrder::less_than(old_frontier, &new_frontier),
                            "new frontier {new_frontier:?} is not beyond \
                             old frontier {old_frontier:?}"
                        );
                    }
                    ReportedFrontier::NotReported { lower } => {
                        assert!(
                            PartialOrder::less_equal(lower, &new_frontier),
                            "new frontier {new_frontier:?} is before lower bound {lower:?}"
                        );
                    }
                }

                let new_reported_frontier = ReportedFrontier::Reported(new_frontier);

                if let Some(logger) = self.compute_state.compute_logger.as_mut() {
                    if let Some(time) = collection.reported_frontier.logging_time() {
                        logger.log(ComputeEvent::Frontier {
                            id: sink_id,
                            time,
                            diff: -1,
                        });
                    }
                    if let Some(time) = new_reported_frontier.logging_time() {
                        logger.log(ComputeEvent::Frontier {
                            id: sink_id,
                            time,
                            diff: 1,
                        });
                    }
                }

                collection.reported_frontier = new_reported_frontier;
            } else {
                // Presumably tracking state for this subscribe was already dropped by
                // `drop_collection`. There is nothing left to do for logging.
            }

            response
                .to_error_if_exceeds(usize::try_from(self.compute_state.max_result_size).unwrap());
            self.send_compute_response(ComputeResponse::SubscribeResponse(sink_id, response));
        }
    }

    /// Send a response to the coordinator.
    fn send_compute_response(&self, response: ComputeResponse) {
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = self.response_tx.send(response);
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
    pub fn as_log_event(&self) -> logging::compute::Peek {
        let peek = self.peek();
        logging::compute::Peek::new(peek.target.id(), peek.timestamp, peek.uuid)
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

    fn persist(
        peek: Peek,
        persist_clients: Arc<PersistClientCache>,
        metadata: CollectionMetadata,
        active_worker: bool,
    ) -> Self {
        let (result_tx, result_rx) = oneshot::channel();
        let timestamp = peek.timestamp;
        let mfp_plan = peek.map_filter_project.clone();
        let max_results_needed = peek.finishing.limit.unwrap_or(usize::MAX) + peek.finishing.offset;

        let task_handle = mz_ore::task::spawn(|| "persist::peek", async move {
            let start = Instant::now();
            let result = if active_worker {
                PersistPeek::do_peek(
                    &persist_clients,
                    metadata,
                    timestamp,
                    mfp_plan,
                    max_results_needed,
                )
                .await
            } else {
                Ok(vec![])
            };
            let result = match result {
                Ok(rows) => PeekResponse::Rows(rows),
                Err(e) => PeekResponse::Error(e.to_string()),
            };
            let _ = result_tx.send((result, start.elapsed()));
        });
        PendingPeek::Persist(PersistPeek {
            peek,
            _abort_handle: task_handle.abort_handle().abort_on_drop(),
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
    _abort_handle: AbortOnDropAbortHandle,
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
        mut limit_remaining: usize,
    ) -> Result<Vec<(Row, NonZeroUsize)>, String> {
        let client = persist_clients
            .open(metadata.persist_location)
            .await
            .map_err(|e| e.to_string())?;

        let mut reader: ReadHandle<SourceData, (), Timestamp, Diff> = client
            .open_leased_reader(
                metadata.data_shard,
                Arc::new(metadata.relation_desc),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("persist::peek"),
            )
            .await
            .map_err(|e| e.to_string())?;

        let mut cursor = reader
            .snapshot_cursor(Antichain::from_elem(as_of), |_| true)
            .await
            .map_err(|since| {
                format!("attempted to peek at {as_of}, but the since has advanced to {since:?}")
            })?;

        // Re-used state for processing and building rows.
        let mut result = vec![];
        let mut datum_vec = DatumVec::new();
        let mut row_builder = Row::default();
        let arena = RowArena::new();

        while limit_remaining > 0 {
            let Some(batch) = cursor.next().await else {
                break;
            };
            for ((k, v), _, d) in batch {
                let row = k?.0.map_err(|e| e.to_string())?;
                let () = v?;
                let count: usize = d.try_into().map_err(|_| {
                    format!(
                        "Invalid data in source, saw retractions ({}) for row that does not exist",
                        d * -1,
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
        max_result_size: u32,
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
            Ok(rows) => PeekResponse::Rows(rows),
            Err(text) => PeekResponse::Error(text),
        };
        Some(response)
    }

    /// Collects data for a known-complete peek from the ok stream.
    fn collect_finished_data(
        &mut self,
        max_result_size: u32,
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

        self.dispatch_collect_ok_finished_data(max_result_size)
    }

    /// Dispatches peek finishing of data in the ok stream according to
    /// arrangement key-value types.
    fn dispatch_collect_ok_finished_data(
        &mut self,
        max_result_size: u32,
    ) -> Result<Vec<(Row, NonZeroUsize)>, String> {
        let peek = &mut self.peek;
        let oks = self.trace_bundle.oks_mut();
        match oks {
            SpecializedTraceHandle::RowUnit(oks_handle) => {
                Self::collect_ok_finished_data(peek, oks_handle, None, Some(&[]), max_result_size)
            }
            SpecializedTraceHandle::RowRow(oks_handle) => {
                Self::collect_ok_finished_data(peek, oks_handle, None, None, max_result_size)
            }
        }
    }

    /// Collects data for a known-complete peek from the ok stream.
    fn collect_ok_finished_data<Tr, K, V>(
        peek: &mut Peek<Timestamp>,
        oks_handle: &mut TraceAgent<Tr>,
        key_types: Option<&[ColumnType]>,
        val_types: Option<&[ColumnType]>,
        max_result_size: u32,
    ) -> Result<Vec<(Row, NonZeroUsize)>, String>
    where
        Tr: TraceReader<Key = K, Val = V, Time = Timestamp, Diff = Diff>,
        Tr::Key: Columnation + Data + FromRowByTypes + IntoRowByTypes,
        Tr::Val: Columnation + Data + IntoRowByTypes,
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
        let max_results = peek.finishing.limit.map(|l| l + peek.finishing.offset);

        use mz_ore::result::ResultExt;

        let mut row_builder = Row::default();
        let mut datum_vec = DatumVec::new();
        let mut l_datum_vec = DatumVec::new();
        let mut r_datum_vec = DatumVec::new();
        let mut key_buf = K::default();

        // We have to sort the literal constraints because cursor.seek_key can seek only forward.
        peek.literal_constraints
            .iter_mut()
            .for_each(|vec| vec.sort());
        let has_literal_constraints = peek.literal_constraints.is_some();
        let mut literals = peek.literal_constraints.iter().flat_map(|l| l);
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
                            let current_literal =
                                key_buf.from_row(current_literal.clone(), key_types);
                            cursor.seek_key(&storage, &current_literal);
                            if !cursor.key_valid(&storage) {
                                return Ok(results);
                            }
                            if *cursor.get_key(&storage).unwrap() == current_literal {
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

                let key = cursor.key(&storage).into_datum_iter(key_types);
                let row = cursor.val(&storage).into_datum_iter(val_types);

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

/// A frontier we have reported to the controller, or the least frontier we are allowed to report.
#[derive(Debug)]
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

    /// Return a timestamp suitable for logging the reported frontier.
    pub fn logging_time(&self) -> Option<Timestamp> {
        match self {
            Self::Reported(frontier) => frontier.get(0).copied(),
            Self::NotReported { .. } => Some(timely::progress::Timestamp::minimum()),
        }
    }
}

/// State maintained for a compute collection.
pub struct CollectionState {
    /// Tracks the frontier that has been reported to the controller.
    pub reported_frontier: ReportedFrontier,
    /// A token that should be dropped when this collection is dropped to clean up associated
    /// sink state.
    ///
    /// Only `Some` if the collection is a sink.
    pub sink_token: Option<SinkToken>,
    /// Frontier of sink writes.
    ///
    /// Only `Some` if the collection is a sink and *not* a subscribe.
    pub sink_write_frontier: Option<Rc<RefCell<Antichain<Timestamp>>>>,
}

impl CollectionState {
    fn new() -> Self {
        Self {
            reported_frontier: ReportedFrontier::new(),
            sink_token: None,
            sink_write_frontier: None,
        }
    }

    fn is_subscribe(&self) -> bool {
        self.sink_token.is_some() && self.sink_write_frontier.is_none()
    }
}
