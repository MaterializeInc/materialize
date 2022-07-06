// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for compute timely instances.

use std::any::Any;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use mz_persist_client::cache::PersistClientCache;
use timely::communication::Allocate;
use timely::logging::Logger;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::reachability::logging::TrackerEvent;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, Mutex};

use mz_compute_client::command::{
    ComputeCommand, DataflowDescription, InstanceConfig, Peek, ReplicaId,
};
use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::plan::Plan;
use mz_compute_client::response::{ComputeResponse, PeekResponse, TailResponse};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage::client::connections::ConnectionContext;
use mz_storage::client::controller::CollectionMetadata;
use mz_storage::client::errors::DataflowError;
use mz_timely_util::activator::RcActivator;
use mz_timely_util::operator::CollectionExt;

use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::logging;
use crate::logging::compute::ComputeEvent;
use crate::sink::SinkBaseMetrics;

/// Worker-local state that is maintained across dataflows.
///
/// This state is restricted to the COMPUTE state, the deterministic, idempotent work
/// done between data ingress and egress.
pub struct ComputeState {
    /// The ID of the replica this worker belongs to.
    pub replica_id: ReplicaId,
    /// The traces available for sharing across dataflows.
    pub traces: TraceManager,
    /// Tokens that should be dropped when a dataflow is dropped to clean up
    /// associated state.
    pub dataflow_tokens: HashMap<GlobalId, Box<dyn Any>>,
    /// Shared buffer with TAIL operator instances by which they can respond.
    ///
    /// The entries are pairs of sink identifier (to identify the tail instance)
    /// and the response itself.
    pub tail_response_buffer: Rc<RefCell<Vec<(GlobalId, TailResponse)>>>,
    /// Frontier of sink writes (all subsequent writes will be at times at or
    /// equal to this frontier)
    pub sink_write_frontiers: HashMap<GlobalId, Rc<RefCell<Antichain<Timestamp>>>>,
    /// Peek commands that are awaiting fulfillment.
    pub pending_peeks: Vec<PendingPeek>,
    /// Tracks the frontier information that has been sent over `response_tx`.
    pub reported_frontiers: HashMap<GlobalId, Antichain<Timestamp>>,
    /// Undocumented
    pub sink_metrics: SinkBaseMetrics,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub compute_logger: Option<logging::compute::Logger>,
    /// Configuration for sink connections.
    // TODO: remove when sinks move to storage.
    pub connection_context: ConnectionContext,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<Mutex<PersistClientCache>>,
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

impl<'a, A: Allocate> ActiveComputeState<'a, A> {
    /// Entrypoint for applying a compute command.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn handle_compute_command(&mut self, cmd: ComputeCommand) {
        use ComputeCommand::*;

        match cmd {
            CreateInstance(config) => self.handle_create_instance(config),
            DropInstance => (),
            CreateDataflows(dataflows) => self.handle_create_dataflows(dataflows),
            AllowCompaction(list) => self.handle_allow_compaction(list),
            Peek(peek) => {
                peek.otel_ctx.attach_as_parent();
                self.handle_peek(peek)
            }
            CancelPeeks { uuids } => self.handle_cancel_peeks(uuids),
        }
    }

    fn handle_create_instance(&mut self, config: InstanceConfig) {
        if let Some(logging) = config.logging {
            self.initialize_logging(&logging);
        }
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

            // Initialize frontiers for each object, and optionally log their construction.
            for (object_id, collection_id) in exported_ids {
                self.compute_state
                    .reported_frontiers
                    .insert(object_id, Antichain::from_elem(0));

                // Log dataflow construction, frontier construction, and any dependencies.
                if let Some(logger) = self.compute_state.compute_logger.as_mut() {
                    logger.log(ComputeEvent::Dataflow(object_id, true));
                    logger.log(ComputeEvent::Frontier(object_id, 0, 1));
                    for import_id in dataflow.depends_on(collection_id) {
                        logger.log(ComputeEvent::DataflowDependency {
                            dataflow: object_id,
                            source: import_id,
                        })
                    }
                }
            }

            crate::render::build_compute_dataflow(
                self.timely_worker,
                &mut self.compute_state,
                dataflow,
            );
        }
    }

    fn handle_allow_compaction(&mut self, list: Vec<(GlobalId, Antichain<Timestamp>)>) {
        for (id, frontier) in list {
            if frontier.is_empty() {
                // Indicates that we may drop `id`, as there are no more valid times to read.

                // Sink-specific work:
                self.compute_state.sink_write_frontiers.remove(&id);
                self.compute_state.dataflow_tokens.remove(&id);
                // Index-specific work:
                self.compute_state.traces.del_trace(&id);

                // Work common to sinks and indexes (removing frontier tracking and cleaning up logging).
                let frontier = self
                    .compute_state
                    .reported_frontiers
                    .remove(&id)
                    .expect("Dropped compute collection with no frontier");
                if let Some(logger) = self.compute_state.compute_logger.as_mut() {
                    logger.log(ComputeEvent::Dataflow(id, false));
                    for time in frontier.elements().iter() {
                        logger.log(ComputeEvent::Frontier(id, *time, -1));
                    }
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
        // Only handle peeks that are not targeted at a different replica.
        if let Some(target) = peek.target_replica {
            if target != self.compute_state.replica_id {
                return;
            }
        }

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
        if let Some(response) = peek.seek_fulfillment(&mut Antichain::new()) {
            self.send_peek_response(peek, response);
        } else {
            self.compute_state.pending_peeks.push(peek);
        }
    }

    fn handle_cancel_peeks(&mut self, uuids: BTreeSet<uuid::Uuid>) {
        let pending_peeks_len = self.compute_state.pending_peeks.len();
        let mut pending_peeks = std::mem::replace(
            &mut self.compute_state.pending_peeks,
            Vec::with_capacity(pending_peeks_len),
        );
        for peek in pending_peeks.drain(..) {
            if uuids.contains(&peek.peek.uuid) {
                self.send_peek_response(peek, PeekResponse::Canceled);
            } else {
                self.compute_state.pending_peeks.push(peek);
            }
        }
    }

    /// Initializes timely dataflow logging and publishes as a view.
    pub fn initialize_logging(&mut self, logging: &LoggingConfig) {
        if self.compute_state.compute_logger.is_some() {
            panic!("dataflow server has already initialized logging");
        }

        use crate::logging::BatchLogger;
        use timely::dataflow::operators::capture::event::link::EventLink;

        let granularity_ms = std::cmp::max(1, logging.granularity_ns / 1_000_000) as Timestamp;

        // Track time relative to the Unix epoch, rather than when the server
        // started, so that the logging sources can be joined with tables and
        // other real time sources for semi-sensible results.
        let now = Instant::now();
        let start_offset = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("Failed to get duration since Unix epoch");

        // Establish loggers first, so we can either log the logging or not, as we like.
        let t_linked = std::rc::Rc::new(EventLink::new());
        let mut t_logger = BatchLogger::new(Rc::clone(&t_linked), granularity_ms);
        let r_linked = std::rc::Rc::new(EventLink::new());
        let mut r_logger = BatchLogger::new(Rc::clone(&r_linked), granularity_ms);
        let d_linked = std::rc::Rc::new(EventLink::new());
        let mut d_logger = BatchLogger::new(Rc::clone(&d_linked), granularity_ms);
        let c_linked = std::rc::Rc::new(EventLink::new());
        let mut c_logger = BatchLogger::new(Rc::clone(&c_linked), granularity_ms);

        let mut t_traces = HashMap::new();
        let mut r_traces = HashMap::new();
        let mut d_traces = HashMap::new();
        let mut c_traces = HashMap::new();

        let activate_after = 128;

        let t_activator = RcActivator::new("t_activator".into(), activate_after);
        let r_activator = RcActivator::new("r_activator".into(), activate_after);
        let d_activator = RcActivator::new("d_activator".into(), activate_after);
        let c_activator = RcActivator::new("c_activator".into(), activate_after);

        if !logging.log_logging {
            // Construct logging dataflows and endpoints before registering any.
            t_traces.extend(logging::timely::construct(
                &mut self.timely_worker,
                logging,
                Rc::clone(&t_linked),
                t_activator.clone(),
            ));
            r_traces.extend(logging::reachability::construct(
                &mut self.timely_worker,
                logging,
                Rc::clone(&r_linked),
                r_activator.clone(),
            ));
            d_traces.extend(logging::differential::construct(
                &mut self.timely_worker,
                logging,
                Rc::clone(&d_linked),
                d_activator.clone(),
            ));
            c_traces.extend(logging::compute::construct(
                &mut self.timely_worker,
                logging,
                Rc::clone(&c_linked),
                c_activator.clone(),
            ));
        }

        // Register each logger endpoint.
        let activator = t_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "timely",
            Logger::new(
                now,
                start_offset,
                self.timely_worker.index(),
                move |time, data| {
                    t_logger.publish_batch(time, data);
                    activator.activate();
                },
            ),
        );

        let activator = r_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "timely/reachability",
            Logger::new(
                now,
                start_offset,
                self.timely_worker.index(),
                move |time, data: &mut Vec<(Duration, usize, TrackerEvent)>| {
                    let mut converted_updates = Vec::new();
                    for event in data.drain(..) {
                        match event.2 {
                            TrackerEvent::SourceUpdate(update) => {
                                let massaged: Vec<_> = update
                                    .updates
                                    .iter()
                                    .map(|u| {
                                        let ts = u.2.as_any().downcast_ref::<Timestamp>().copied();
                                        (*u.0, *u.1, true, ts, *u.3)
                                    })
                                    .collect();

                                converted_updates.push((
                                    event.0,
                                    event.1,
                                    (update.tracker_id, massaged),
                                ));
                            }
                            TrackerEvent::TargetUpdate(update) => {
                                let massaged: Vec<_> = update
                                    .updates
                                    .iter()
                                    .map(|u| {
                                        let ts = u.2.as_any().downcast_ref::<Timestamp>().copied();
                                        (*u.0, *u.1, true, ts, *u.3)
                                    })
                                    .collect();

                                converted_updates.push((
                                    event.0,
                                    event.1,
                                    (update.tracker_id, massaged),
                                ));
                            }
                        }
                    }
                    r_logger.publish_batch(time, &mut converted_updates);
                    activator.activate();
                },
            ),
        );

        let activator = d_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "differential/arrange",
            Logger::new(
                now,
                start_offset,
                self.timely_worker.index(),
                move |time, data| {
                    d_logger.publish_batch(time, data);
                    activator.activate();
                },
            ),
        );

        let activator = c_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "materialize/compute",
            Logger::new(
                now,
                start_offset,
                self.timely_worker.index(),
                move |time, data| {
                    c_logger.publish_batch(time, data);
                    activator.activate();
                },
            ),
        );

        let errs = self
            .timely_worker
            .dataflow_named("Dataflow: logging", |scope| {
                Collection::<_, DataflowError, Diff>::empty(scope)
                    .arrange()
                    .trace
            });

        let logger = self
            .timely_worker
            .log_register()
            .get("materialize/compute")
            .unwrap();

        if logging.log_logging {
            // Create log processing dataflows after registering logging so we can log the
            // logging.
            t_traces.extend(logging::timely::construct(
                &mut self.timely_worker,
                logging,
                t_linked,
                t_activator,
            ));
            r_traces.extend(logging::reachability::construct(
                &mut self.timely_worker,
                logging,
                r_linked,
                r_activator,
            ));
            d_traces.extend(logging::differential::construct(
                &mut self.timely_worker,
                logging,
                d_linked,
                d_activator,
            ));
            c_traces.extend(logging::compute::construct(
                &mut self.timely_worker,
                logging,
                c_linked,
                c_activator,
            ));
        }

        // Install traces as maintained indexes
        for (log, (trace, token)) in t_traces {
            let id = logging.active_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()).with_drop(token));
            self.compute_state
                .reported_frontiers
                .insert(id, Antichain::from_elem(0));
            logger.log(ComputeEvent::Frontier(id, 0, 1));
        }
        for (log, (trace, token)) in r_traces {
            let id = logging.active_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()).with_drop(token));
            self.compute_state
                .reported_frontiers
                .insert(id, Antichain::from_elem(0));
            logger.log(ComputeEvent::Frontier(id, 0, 1));
        }
        for (log, (trace, token)) in d_traces {
            let id = logging.active_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()).with_drop(token));
            self.compute_state
                .reported_frontiers
                .insert(id, Antichain::from_elem(0));
            logger.log(ComputeEvent::Frontier(id, 0, 1));
        }
        for (log, (trace, token)) in c_traces {
            let id = logging.active_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()).with_drop(token));
            self.compute_state
                .reported_frontiers
                .insert(id, Antichain::from_elem(0));
            logger.log(ComputeEvent::Frontier(id, 0, 1));
        }

        self.compute_state.compute_logger = Some(logger);
    }

    /// Disables timely dataflow logging.
    ///
    /// This does not unpublish views and is only useful to terminate logging streams to ensure that
    /// computed can terminate cleanly.
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
        fn add_progress(
            id: GlobalId,
            new_frontier: &Antichain<Timestamp>,
            prev_frontier: &Antichain<Timestamp>,
            progress: &mut Vec<(GlobalId, ChangeBatch<Timestamp>)>,
        ) {
            let mut changes = ChangeBatch::new();
            for time in prev_frontier.elements().iter() {
                changes.update(time.clone(), -1);
            }
            for time in new_frontier.elements().iter() {
                changes.update(time.clone(), 1);
            }
            changes.compact();
            if !changes.is_empty() {
                progress.push((id, changes));
            }
        }

        let mut new_frontier = Antichain::new();
        let mut progress = Vec::new();
        for (id, traces) in self.compute_state.traces.traces.iter_mut() {
            // Read the upper frontier and compare to what we've reported.
            traces.oks_mut().read_upper(&mut new_frontier);
            let prev_frontier = self
                .compute_state
                .reported_frontiers
                .get_mut(&id)
                .expect("Index frontier missing!");
            if prev_frontier != &new_frontier {
                add_progress(*id, &new_frontier, &prev_frontier, &mut progress);
                prev_frontier.clone_from(&new_frontier);
            }
        }

        // Log index frontier changes
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            for (id, changes) in &mut progress {
                for (time, diff) in changes.iter() {
                    logger.log(ComputeEvent::Frontier(*id, *time, *diff));
                }
            }
        }

        for (id, frontier) in self.compute_state.sink_write_frontiers.iter() {
            new_frontier.clone_from(&frontier.borrow());
            let prev_frontier = self
                .compute_state
                .reported_frontiers
                .get_mut(&id)
                .expect("Sink frontier missing!");
            assert!(<_ as PartialOrder>::less_equal(
                prev_frontier,
                &new_frontier
            ));
            if prev_frontier != &new_frontier {
                add_progress(*id, &new_frontier, &prev_frontier, &mut progress);
                prev_frontier.clone_from(&new_frontier);
            }
        }

        if !progress.is_empty() {
            self.send_compute_response(ComputeResponse::FrontierUppers(progress));
        }
    }

    /// Scan pending peeks and attempt to retire each.
    pub fn process_peeks(&mut self) {
        let mut upper = Antichain::new();
        let pending_peeks_len = self.compute_state.pending_peeks.len();
        let mut pending_peeks = std::mem::replace(
            &mut self.compute_state.pending_peeks,
            Vec::with_capacity(pending_peeks_len),
        );
        for mut peek in pending_peeks.drain(..) {
            if let Some(response) = peek.seek_fulfillment(&mut upper) {
                let _span = tracing::info_span!(parent: &peek.span,  "process_peek").entered();
                self.send_peek_response(peek, response);
            } else {
                self.compute_state.pending_peeks.push(peek);
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

    /// Scan the shared tail response buffer, and forward results along.
    pub fn process_tails(&mut self) {
        let mut tail_responses = self.compute_state.tail_response_buffer.borrow_mut();
        for (sink_id, response) in tail_responses.drain(..) {
            self.send_compute_response(ComputeResponse::TailResponse(sink_id, response));
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
    fn seek_fulfillment(&mut self, upper: &mut Antichain<Timestamp>) -> Option<PeekResponse> {
        self.trace_bundle.oks_mut().read_upper(upper);
        if upper.less_equal(&self.peek.timestamp) {
            return None;
        }
        self.trace_bundle.errs_mut().read_upper(upper);
        if upper.less_equal(&self.peek.timestamp) {
            return None;
        }
        let response = match self.collect_finished_data() {
            Ok(rows) => PeekResponse::Rows(rows),
            Err(text) => PeekResponse::Error(text),
        };
        Some(response)
    }

    /// Collects data for a known-complete peek.
    fn collect_finished_data(&mut self) -> Result<Vec<(Row, NonZeroUsize)>, String> {
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

        if let Some(literal) = &self.peek.key {
            cursor.seek_key(&storage, literal);
        }

        use differential_dataflow::trace::Cursor;
        use mz_ore::result::ResultExt;
        use mz_repr::{DatumVec, RowArena};

        let mut row_builder = Row::default();
        let mut datum_vec = DatumVec::new();
        let mut l_datum_vec = DatumVec::new();
        let mut r_datum_vec = DatumVec::new();

        while cursor.key_valid(&storage) {
            while cursor.val_valid(&storage) {
                // TODO: This arena could be maintained and reuse for longer
                // but it wasn't clear at what granularity we should flush
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
                                results.truncate(max_results);
                            }
                        }
                    }
                }
                cursor.step_val(&storage);
            }
            // If we had a key, we are now done and can return.
            if self.peek.key.is_some() {
                return Ok(results);
            } else {
                cursor.step_key(&storage);
            }
        }

        Ok(results)
    }
}
