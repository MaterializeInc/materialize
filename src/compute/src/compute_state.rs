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
use std::sync::Arc;

use bytesize::ByteSize;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;
use tracing::{error, info, span, Level};
use uuid::Uuid;

use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::plan::Plan;
use mz_compute_client::protocol::command::{ComputeCommand, ComputeParameters, Peek};
use mz_compute_client::protocol::history::ComputeCommandHistory;
use mz_compute_client::protocol::response::{ComputeResponse, PeekResponse, SubscribeResponse};
use mz_compute_client::types::dataflows::DataflowDescription;
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;
use mz_timely_util::probe;

use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::logging;
use crate::logging::compute::ComputeEvent;
use crate::metrics::ComputeMetrics;

/// Worker-local state that is maintained across dataflows.
///
/// This state is restricted to the COMPUTE state, the deterministic, idempotent work
/// done between data ingress and egress.
pub struct ComputeState {
    /// The traces available for sharing across dataflows.
    pub traces: TraceManager,
    /// Tokens that should be dropped when a dataflow is dropped to clean up
    /// associated state.
    pub sink_tokens: BTreeMap<GlobalId, SinkToken>,
    /// Shared buffer with SUBSCRIBE operator instances by which they can respond.
    ///
    /// The entries are pairs of sink identifier (to identify the subscribe instance)
    /// and the response itself.
    pub subscribe_response_buffer: Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>,
    /// Frontier of sink writes (all subsequent writes will be at times at or
    /// equal to this frontier)
    pub sink_write_frontiers: BTreeMap<GlobalId, Rc<RefCell<Antichain<Timestamp>>>>,
    /// Probe handles for regulating the output of dataflow sources.
    ///
    /// Keys are IDs of indexes that are (transitively) fed by a flow-controlled source. New
    /// dataflows that depend on these indexes are expected to report their output frontiers
    /// through the corresponding probe handles.
    pub flow_control_probes: BTreeMap<GlobalId, Vec<probe::Handle<Timestamp>>>,
    /// Peek commands that are awaiting fulfillment.
    pub pending_peeks: BTreeMap<Uuid, PendingPeek>,
    /// Tracks the frontier information that has been sent over `response_tx`.
    pub reported_frontiers: BTreeMap<GlobalId, ReportedFrontier>,
    /// Collections that were recently dropped and whose removal needs to be reported.
    pub dropped_collections: Vec<GlobalId>,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub compute_logger: Option<logging::compute::Logger>,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<PersistClientCache>,
    /// History of commands received by this workers and all its peers.
    pub command_history: ComputeCommandHistory,
    /// Max size in bytes of any result.
    pub max_result_size: u32,
    /// Maximum number of in-flight bytes emitted by persist_sources feeding dataflows.
    pub dataflow_max_inflight_bytes: usize,
    /// Metrics for this replica.
    pub metrics: ComputeMetrics,
}

impl ComputeState {
    /// Return whether a collection with the given ID exists.
    pub fn collection_exists(&self, id: GlobalId) -> bool {
        self.traces.get(&id).is_some() || self.sink_tokens.contains_key(&id)
    }
}

/// A wrapper around [ComputeState] with a live timely worker and response channel.
pub struct ActiveComputeState<'a, A: Allocate> {
    /// The underlying Timely worker.
    pub timely_worker: &'a mut TimelyWorker<A>,
    /// The compute state itself.
    pub compute_state: &'a mut ComputeState,
    /// The channel over which frontier information is reported.
    pub response_tx: &'a mut mpsc::UnboundedSender<ComputeResponse>,
}

/// A token that keeps a sink alive.
pub struct SinkToken(Box<dyn Any>);

impl SinkToken {
    /// Create a new `SinkToken`.
    pub fn new(t: Box<dyn Any>) -> Self {
        Self(t)
    }
}

impl<'a, A: Allocate> ActiveComputeState<'a, A> {
    /// Entrypoint for applying a compute command.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn handle_compute_command(&mut self, cmd: ComputeCommand) {
        use ComputeCommand::*;
        self.compute_state
            .command_history
            .push(cmd.clone(), &self.compute_state.pending_peeks);
        self.compute_state.metrics.command_history_size.set(
            u64::try_from(self.compute_state.command_history.len()).expect(
                "The compute command history size must be non-negative and fit a 64-bit number",
            ),
        );
        self.compute_state.metrics.dataflow_count_in_history.set(
            u64::try_from(self.compute_state.command_history.dataflow_count()).expect(
                "The number of dataflows in the compute history must be non-negative and fit a 64-bit number",
            ),
        );
        match cmd {
            CreateTimely { .. } => panic!("CreateTimely must be captured before"),
            CreateInstance(logging) => self.handle_create_instance(logging),
            InitializationComplete => (),
            UpdateConfiguration(params) => self.handle_update_configuration(params),
            CreateDataflows(dataflows) => self.handle_create_dataflows(dataflows),
            AllowCompaction(list) => self.handle_allow_compaction(list),
            Peek(peek) => {
                peek.otel_ctx.attach_as_parent();
                self.handle_peek(peek)
            }
            CancelPeeks { uuids } => self.handle_cancel_peeks(uuids),
        }
    }

    fn handle_create_instance(&mut self, logging: LoggingConfig) {
        self.initialize_logging(&logging);
    }

    fn handle_update_configuration(&mut self, params: ComputeParameters) {
        info!("Applying configuration update: {params:?}");

        let ComputeParameters {
            max_result_size,
            dataflow_max_inflight_bytes,
            persist,
        } = params;

        if let Some(v) = max_result_size {
            self.compute_state.max_result_size = v;
        }
        if let Some(v) = dataflow_max_inflight_bytes {
            self.compute_state.dataflow_max_inflight_bytes = v;
        }

        persist.apply(self.compute_state.persist_clients.cfg())
    }

    fn handle_create_dataflows(
        &mut self,
        dataflows: Vec<DataflowDescription<Plan, CollectionMetadata>>,
    ) {
        for dataflow in dataflows.into_iter() {
            // Collect the exported object identifiers, paired with their associated "collection" identifier.
            // The latter is used to extract dependency information, which is in terms of collections ids.
            let sink_ids = dataflow
                .sink_exports
                .iter()
                .map(|(sink_id, sink)| (*sink_id, sink.from));
            let index_ids = dataflow
                .index_exports
                .iter()
                .map(|(idx_id, (idx, _))| (*idx_id, idx.on_id));
            let exported_ids = index_ids.chain(sink_ids);

            let dataflow_index = self.timely_worker.next_dataflow_index();

            // Initialize frontiers for each object, and optionally log their construction.
            for (object_id, collection_id) in exported_ids {
                let reported_frontier = ReportedFrontier::NotReported {
                    lower: dataflow.as_of.clone().unwrap(),
                };
                if let Some(frontier) = self
                    .compute_state
                    .reported_frontiers
                    .insert(object_id, reported_frontier)
                {
                    error!(
                        "existing frontier {frontier:?} for newly created dataflow id {object_id}"
                    );
                }

                // Log dataflow construction, frontier construction, and any dependencies.
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
                    for import_id in dataflow.depends_on_imports(collection_id) {
                        logger.log(ComputeEvent::ExportDependency {
                            export_id: object_id,
                            import_id,
                        })
                    }
                }
            }

            crate::render::build_compute_dataflow(self.timely_worker, self.compute_state, dataflow);
        }
    }

    fn handle_allow_compaction(&mut self, list: Vec<(GlobalId, Antichain<Timestamp>)>) {
        for (id, frontier) in list {
            if frontier.is_empty() {
                // Indicates that we may drop `id`, as there are no more valid times to read.

                let is_subscribe = self.compute_state.sink_tokens.contains_key(&id)
                    && !self.compute_state.sink_write_frontiers.contains_key(&id);

                // Sink-specific work:
                self.compute_state.sink_write_frontiers.remove(&id);
                self.compute_state.sink_tokens.remove(&id);
                // Index-specific work:
                self.compute_state.traces.del_trace(&id);
                self.compute_state.flow_control_probes.remove(&id);

                // Work common to sinks and indexes (removing frontier tracking and cleaning up logging).
                let prev_frontier = self
                    .compute_state
                    .reported_frontiers
                    .remove(&id)
                    .expect("Dropped compute collection with no frontier");
                if let Some(logger) = self.compute_state.compute_logger.as_mut() {
                    logger.log(ComputeEvent::ExportDropped { id });
                    if let Some(time) = prev_frontier.logging_time() {
                        logger.log(ComputeEvent::Frontier { id, time, diff: -1 });
                    }
                }

                // We need to emit a final response reporting the dropping of this collection,
                // unless:
                //  * The collection is a subscribe, in which case we will emit a
                //    `SubscribeResponse::Dropped` independently.
                //  * The collection has already advanced to the empty frontier, in which case
                //    the final `FrontierUppers` response already serves the purpose of reporting
                //    the end of the dataflow.
                if !is_subscribe && !prev_frontier.is_empty() {
                    self.compute_state.dropped_collections.push(id);
                }
            } else {
                self.compute_state
                    .traces
                    .allow_compaction(id, frontier.borrow());
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn handle_peek(&mut self, peek: Peek) {
        // Acquire a copy of the trace suitable for fulfilling the peek.
        let mut trace_bundle = self.compute_state.traces.get(&peek.id).unwrap().clone();
        let timestamp_frontier = Antichain::from_elem(peek.timestamp);
        let empty_frontier = Antichain::new();
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
        // Prepare a description of the peek work to do.
        let mut peek = PendingPeek {
            peek,
            trace_bundle,
            span: tracing::Span::current(),
        };
        // Log the receipt of the peek.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(ComputeEvent::Peek(peek.as_log_event(), true));
        }
        // Attempt to fulfill the peek.
        if let Some(response) =
            peek.seek_fulfillment(&mut Antichain::new(), self.compute_state.max_result_size)
        {
            self.send_peek_response(peek, response);
        } else {
            peek.span = span!(parent: &peek.span, Level::DEBUG, "pending peek");
            self.compute_state
                .pending_peeks
                .insert(peek.peek.uuid, peek);
        }
    }

    fn handle_cancel_peeks(&mut self, uuids: BTreeSet<uuid::Uuid>) {
        let pending_peeks = std::mem::take(&mut self.compute_state.pending_peeks);
        for (uuid, peek) in pending_peeks {
            if uuids.contains(&uuid) {
                self.send_peek_response(peek, PeekResponse::Canceled);
            } else {
                self.compute_state.pending_peeks.insert(uuid, peek);
            }
        }
    }

    /// Initializes timely dataflow logging and publishes as a view.
    pub fn initialize_logging(&mut self, config: &LoggingConfig) {
        if self.compute_state.compute_logger.is_some() {
            panic!("dataflow server has already initialized logging");
        }

        let (logger, traces) = logging::initialize(self.timely_worker, config);

        // TODO: Construct error collection and trace bundles inside `logging::initialize`.
        let errs = self
            .timely_worker
            .dataflow_named("Dataflow: logging", |scope| {
                Collection::<_, DataflowError, Diff>::empty(scope)
                    .arrange_named("Arrange logging err")
                    .trace
            });

        // Install traces as maintained indexes
        for (log, (trace, token)) in traces {
            let id = config.index_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()).with_drop(token));
        }

        // Initialize frontier reporting for all logging indexes.
        let index_ids = config.index_logs.values().copied();
        for id in index_ids {
            if let Some(frontier) = self
                .compute_state
                .reported_frontiers
                .insert(id, ReportedFrontier::new())
            {
                error!(
                    "existing frontier {frontier:?} for newly initialized logging export id {id}"
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

    /// Disables timely dataflow logging.
    ///
    /// This does not unpublish views and is only useful to terminate logging streams to ensure that
    /// clusterd can terminate cleanly.
    pub fn shutdown_logging(&mut self) {
        self.timely_worker.log_register().remove("timely");
        self.timely_worker
            .log_register()
            .remove("timely/reachability");
        self.timely_worker
            .log_register()
            .remove("differential/arrange");
        self.timely_worker
            .log_register()
            .remove("materialize/compute");
    }

    /// Send progress information to the coordinator.
    pub fn report_compute_frontiers(&mut self) {
        let mut new_uppers = Vec::new();

        let mut update_frontier = |id, new_frontier: &Antichain<Timestamp>| {
            let reported_frontier = self
                .compute_state
                .reported_frontiers
                .get_mut(&id)
                .unwrap_or_else(|| panic!("Frontier update for untracked identifier: {id}"));

            match reported_frontier {
                ReportedFrontier::Reported(old_frontier) => {
                    assert!(PartialOrder::less_equal(old_frontier, new_frontier));
                    if old_frontier == new_frontier {
                        return; // nothing new to report
                    }
                }
                ReportedFrontier::NotReported { lower } => {
                    if !PartialOrder::less_equal(lower, new_frontier) {
                        return; // lower bound for reporting not yet reached
                    }
                }
            }

            let new_reported_frontier = ReportedFrontier::Reported(new_frontier.clone());

            if let Some(logger) = self.compute_state.compute_logger.as_mut() {
                if let Some(time) = reported_frontier.logging_time() {
                    logger.log(ComputeEvent::Frontier { id, time, diff: -1 });
                }
                if let Some(time) = new_reported_frontier.logging_time() {
                    logger.log(ComputeEvent::Frontier { id, time, diff: 1 });
                }
            }

            new_uppers.push((id, new_frontier.clone()));
            *reported_frontier = new_reported_frontier;
        };

        let mut new_frontier = Antichain::new();
        for (id, traces) in self.compute_state.traces.traces.iter_mut() {
            traces.oks_mut().read_upper(&mut new_frontier);
            update_frontier(*id, &new_frontier);
        }
        for (id, frontier) in self.compute_state.sink_write_frontiers.iter() {
            new_frontier.clone_from(&frontier.borrow());
            update_frontier(*id, &new_frontier);
        }

        if !new_uppers.is_empty() {
            self.send_compute_response(ComputeResponse::FrontierUppers(new_uppers));
        }
    }

    /// Report dropped collections to the controller.
    pub fn report_dropped_collections(&mut self) {
        let dropped_collections = std::mem::take(&mut self.compute_state.dropped_collections);

        // TODO(teske): It is, in fact, wrong to report the dropping of a collection before it has
        // advanced to the empty frontier by announcing that it has advanced to the empty
        // frontier. We should introduce a new compute response variant that has the right
        // semantics.
        let new_uppers: Vec<_> = dropped_collections
            .into_iter()
            .filter(|id| {
                // The collection might have been temporarily dropped and re-created during
                // reconciliation. In this case, announcing its removal would confuse the controller.
                !self.compute_state.collection_exists(*id)
            })
            .map(|id| (id, Antichain::new()))
            .collect();

        if !new_uppers.is_empty() {
            self.send_compute_response(ComputeResponse::FrontierUppers(new_uppers));
        }
    }

    /// Scan pending peeks and attempt to retire each.
    pub fn process_peeks(&mut self) {
        let mut upper = Antichain::new();
        let pending_peeks = std::mem::take(&mut self.compute_state.pending_peeks);
        for (uuid, mut peek) in pending_peeks {
            if let Some(response) =
                peek.seek_fulfillment(&mut upper, self.compute_state.max_result_size)
            {
                let _span = tracing::info_span!(parent: &peek.span,  "process_peek").entered();
                self.send_peek_response(peek, response);
            } else {
                self.compute_state.pending_peeks.insert(uuid, peek);
            }
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
            peek.peek.uuid,
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
            if let Some(reported_frontier) = self.compute_state.reported_frontiers.get_mut(&sink_id)
            {
                let new_frontier = match &response {
                    SubscribeResponse::Batch(b) => b.upper.clone(),
                    SubscribeResponse::DroppedAt(_) => Antichain::new(),
                };

                match reported_frontier {
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
                    if let Some(time) = reported_frontier.logging_time() {
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

                *reported_frontier = new_reported_frontier;
            } else {
                // Presumably tracking state for this frontier was already dropped by
                // `handle_allow_compaction`. There is nothing left to do for logging.
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

/// An in-progress peek, and data to eventually fulfill it.
///
/// Note that `PendingPeek` intentionally does not implement or derive `Clone`,
/// as each `PendingPeek` is meant to be dropped after it's responded to.
pub struct PendingPeek {
    peek: Peek,
    /// The data from which the trace derives.
    trace_bundle: TraceBundle,
    /// The `tracing::Span` tracking this peek's operation
    span: tracing::Span,
}

impl PendingPeek {
    /// Produces a corresponding log event.
    pub fn as_log_event(&self) -> crate::logging::compute::Peek {
        crate::logging::compute::Peek::new(self.peek.id, self.peek.timestamp, self.peek.uuid)
    }

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

    /// Collects data for a known-complete peek.
    fn collect_finished_data(
        &mut self,
        max_result_size: u32,
    ) -> Result<Vec<(Row, NonZeroUsize)>, String> {
        let max_result_size = usize::cast_from(max_result_size);
        let count_byte_size = std::mem::size_of::<NonZeroUsize>();
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

        // Cursor and bound lifetime for `Row` data in the backing trace.
        let (mut cursor, storage) = self.trace_bundle.oks_mut().cursor();
        // Accumulated `Vec<(row, count)>` results that we are likely to return.
        let mut results = Vec::new();
        let mut total_size: usize = 0;

        // When set, a bound on the number of records we need to return.
        // The requirements on the records are driven by the finishing's
        // `order_by` field. Further limiting will happen when the results
        // are collected, so we don't need to have exactly this many results,
        // just at least those results that would have been returned.
        let max_results = self
            .peek
            .finishing
            .limit
            .map(|l| l + self.peek.finishing.offset);

        use differential_dataflow::trace::Cursor;
        use mz_ore::result::ResultExt;
        use mz_repr::{DatumVec, RowArena};

        let mut row_builder = Row::default();
        let mut datum_vec = DatumVec::new();
        let mut l_datum_vec = DatumVec::new();
        let mut r_datum_vec = DatumVec::new();

        // We have to sort the literal constraints because cursor.seek_key can seek only forward.
        self.peek
            .literal_constraints
            .iter_mut()
            .for_each(|vec| vec.sort());
        let has_literal_constraints = self.peek.literal_constraints.is_some();
        let mut literals = self.peek.literal_constraints.iter().flat_map(|l| l);
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
                            cursor.seek_key(&storage, current_literal);
                            if !cursor.key_valid(&storage) {
                                return Ok(results);
                            }
                            if *cursor.get_key(&storage).unwrap() == *current_literal {
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
                let key = cursor.key(&storage);
                let row = cursor.val(&storage);
                // TODO: We could unpack into a re-used allocation, except
                // for the arena above (the allocation would not be allowed
                // to outlive the arena above, from which it might borrow).
                let mut borrow = datum_vec.borrow_with_many(&[key, row]);

                if has_literal_constraints {
                    // The peek was created from an IndexedFilter join. We have to add those columns
                    // here that the join would add in a dataflow.
                    let datum_vec = borrow.deref_mut();
                    // unwrap is ok, because it could be None only if !has_literal_constraints or if
                    // the iteration is finished. In the latter case we already exited the while
                    // loop.
                    datum_vec.extend(current_literal.unwrap().iter());
                }
                if let Some(result) = self
                    .peek
                    .map_filter_project
                    .evaluate_into(&mut borrow, &arena, &mut row_builder)
                    .map_err_to_string()?
                {
                    let mut copies = 0;
                    cursor.map_times(&storage, |time, diff| {
                        if time.less_equal(&self.peek.timestamp) {
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
                            if self.peek.finishing.order_by.is_empty() {
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
                                        &self.peek.finishing.order_by,
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
