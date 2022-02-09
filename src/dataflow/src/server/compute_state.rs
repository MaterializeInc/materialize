use std::any::Any;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::{Duration, Instant};

use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{AsCollection, Collection};
use timely::communication::Allocate;
use timely::dataflow::Scope;
use timely::logging::Logger;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::reachability::logging::TrackerEvent;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

use mz_dataflow_types::client::{ComputeCommand, ComputeResponse, Response};
use mz_dataflow_types::logging::LoggingConfig;
use mz_dataflow_types::{DataflowDescription, DataflowError, PeekResponse, TailResponse};
use mz_expr::GlobalId;
use mz_ore::collections::CollectionExt as IteratorExt;
use mz_repr::Timestamp;

use crate::activator::RcActivator;
use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::logging;
use crate::logging::materialized::MaterializedEvent;
use crate::operator::CollectionExt;
use crate::replay::MzReplay;
use crate::server::{PendingPeek, SourceBoundary};
use crate::sink::SinkBaseMetrics;

/// Worker-local state that is maintained across dataflows.
///
/// This state is restricted to the COMPUTE state, the deterministic, idempotent work
/// done between data ingress and egress.
pub struct ComputeState {
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
    pub(crate) pending_peeks: Vec<PendingPeek>,
    /// Tracks the frontier information that has been sent over `response_tx`.
    pub reported_frontiers: HashMap<GlobalId, Antichain<Timestamp>>,
    /// Undocumented
    pub sink_metrics: SinkBaseMetrics,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub materialized_logger: Option<logging::materialized::Logger>,
}

/// Assemble the "compute"  side of a dataflow, i.e. all but the sources.
///
/// This method imports sources from provided assets, and then builds the remaining
/// dataflow using "compute-local" assets like shared arrangements, and producing
/// both arrangements and sinks.
pub fn build_compute_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    compute_state: &mut ComputeState,
    sources: BTreeMap<GlobalId, SourceBoundary>,
    dataflow: DataflowDescription<mz_dataflow_types::plan::Plan>,
) {
    let worker_logging = timely_worker.log_register().get("timely");
    let name = format!("Dataflow: {}", &dataflow.debug_name);

    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        scope.clone().region_named(&name, |region| {
            let mut context = crate::render::context::Context::for_dataflow(
                &dataflow,
                scope.addr().into_element(),
            );
            let mut tokens = BTreeMap::new();

            // Import declared sources into the rendering context.
            for (source_id, source) in sources.into_iter() {
                // Associate collection bundle with the source identifier.
                let ok = Some(source.ok.inner)
                    .mz_replay(
                        region,
                        &format!("{name}-{source_id}"),
                        Duration::MAX,
                        source.ok.activator,
                    )
                    .as_collection();
                let err = Some(source.err.inner)
                    .mz_replay(
                        region,
                        &format!("{name}-{source_id}-err"),
                        Duration::MAX,
                        source.err.activator,
                    )
                    .as_collection();
                context.insert_id(
                    mz_expr::Id::Global(source_id),
                    crate::render::CollectionBundle::from_collections(ok, err),
                );

                // Associate returned tokens with the source identifier.
                let prior = tokens.insert(source_id, source.token);
                assert!(prior.is_none());
            }

            // Import declared indexes into the rendering context.
            for (idx_id, idx) in &dataflow.index_imports {
                context.import_index(compute_state, &mut tokens, scope, region, *idx_id, &idx.0);
            }

            // We first determine indexes and sinks to export, then build the declared object, and
            // finally export indexes and sinks. The reason for this is that we want to avoid
            // cloning the dataflow plan for `build_object`, which can be expensive.

            // Determine indexes to export
            let indexes = dataflow
                .index_exports
                .iter()
                .cloned()
                .map(|(idx_id, idx, _typ)| (idx_id, dataflow.get_imports(&idx.on_id), idx))
                .collect::<Vec<_>>();

            // Determine sinks to export
            let sinks = dataflow
                .sink_exports
                .iter()
                .cloned()
                .map(|(sink_id, sink)| (sink_id, dataflow.get_imports(&sink.from), sink))
                .collect::<Vec<_>>();

            // Build declared objects.
            for object in dataflow.objects_to_build {
                context.build_object(region, object);
            }

            // Export declared indexes.
            for (idx_id, imports, idx) in indexes {
                context.export_index(compute_state, &mut tokens, imports, idx_id, &idx);
            }

            // Export declared sinks.
            for (sink_id, imports, sink) in sinks {
                context.export_sink(compute_state, &mut tokens, imports, sink_id, &sink);
            }
        });
    })
}

pub struct ActiveComputeState<'a, A: Allocate> {
    /// The underlying Timely worker.
    pub timely_worker: &'a mut TimelyWorker<A>,
    /// The compute state itself.
    pub compute_state: &'a mut ComputeState,
    /// The channel over which frontier information is reported.
    pub response_tx: &'a mut mpsc::UnboundedSender<Response>,
}

impl<'a, A: Allocate> ActiveComputeState<'a, A> {
    pub(crate) fn handle_compute_command(&mut self, cmd: ComputeCommand) {
        match cmd {
            ComputeCommand::CreateInstance | ComputeCommand::DropInstance => {
                // Can be ignored for the moment.
                // Should eventually be filtered outside of this method,
                // as we are already deep in a specific instance.
            }
            ComputeCommand::CreateDataflows(_dataflows) => {
                unreachable!("CreateDataflows should not be issued directly to compute state")
            }

            ComputeCommand::DropSinks(ids) => {
                for id in ids {
                    self.compute_state.reported_frontiers.remove(&id);
                    self.compute_state.sink_write_frontiers.remove(&id);
                    self.compute_state.dataflow_tokens.remove(&id);
                }
            }
            ComputeCommand::DropIndexes(ids) => {
                for id in ids {
                    self.compute_state.traces.del_trace(&id);
                    let frontier = self
                        .compute_state
                        .reported_frontiers
                        .remove(&id)
                        .expect("Dropped index with no frontier");
                    if let Some(logger) = self.compute_state.materialized_logger.as_mut() {
                        logger.log(MaterializedEvent::Dataflow(id, false));
                        for time in frontier.elements().iter() {
                            logger.log(MaterializedEvent::Frontier(id, *time, -1));
                        }
                    }
                }
            }

            ComputeCommand::Peek {
                id,
                key,
                timestamp,
                conn_id,
                finishing,
                map_filter_project,
            } => {
                // Acquire a copy of the trace suitable for fulfilling the peek.
                let mut trace_bundle = self.compute_state.traces.get(&id).unwrap().clone();
                let timestamp_frontier = Antichain::from_elem(timestamp);
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
                    id,
                    key,
                    conn_id,
                    timestamp,
                    finishing,
                    trace_bundle,
                    map_filter_project,
                };
                // Log the receipt of the peek.
                if let Some(logger) = self.compute_state.materialized_logger.as_mut() {
                    logger.log(MaterializedEvent::Peek(peek.as_log_event(), true));
                }
                // Attempt to fulfill the peek.
                if let Some(response) = peek.seek_fulfillment(&mut Antichain::new()) {
                    self.send_peek_response(peek, response);
                } else {
                    self.compute_state.pending_peeks.push(peek);
                }
            }

            ComputeCommand::CancelPeek { conn_id } => {
                let pending_peeks_len = self.compute_state.pending_peeks.len();
                let mut pending_peeks = std::mem::replace(
                    &mut self.compute_state.pending_peeks,
                    Vec::with_capacity(pending_peeks_len),
                );
                for peek in pending_peeks.drain(..) {
                    if peek.conn_id == conn_id {
                        self.send_peek_response(peek, PeekResponse::Canceled);
                    } else {
                        self.compute_state.pending_peeks.push(peek);
                    }
                }
            }
            ComputeCommand::AllowIndexCompaction(list) => {
                for (id, frontier) in list {
                    self.compute_state
                        .traces
                        .allow_compaction(id, frontier.borrow());
                }
            }
            ComputeCommand::EnableLogging(config) => {
                self.initialize_logging(&config);
            }
        }
    }

    /// Initializes timely dataflow logging and publishes as a view.
    pub fn initialize_logging(&mut self, logging: &LoggingConfig) {
        if self.compute_state.materialized_logger.is_some() {
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
        let m_linked = std::rc::Rc::new(EventLink::new());
        let mut m_logger = BatchLogger::new(Rc::clone(&m_linked), granularity_ms);

        let mut t_traces = HashMap::new();
        let mut r_traces = HashMap::new();
        let mut d_traces = HashMap::new();
        let mut m_traces = HashMap::new();

        let activate_after = 128;

        let t_activator = RcActivator::new("t_activator".into(), activate_after);
        let r_activator = RcActivator::new("r_activator".into(), activate_after);
        let d_activator = RcActivator::new("d_activator".into(), activate_after);
        let m_activator = RcActivator::new("m_activator".into(), activate_after);

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
            m_traces.extend(logging::materialized::construct(
                &mut self.timely_worker,
                logging,
                Rc::clone(&m_linked),
                m_activator.clone(),
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
                                        (*u.0, *u.1, true, ts, *u.3 as isize)
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
                                        (*u.0, *u.1, true, ts, *u.3 as isize)
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

        let activator = m_activator.clone();
        self.timely_worker.log_register().insert_logger(
            "materialized",
            Logger::new(
                now,
                start_offset,
                self.timely_worker.index(),
                move |time, data| {
                    m_logger.publish_batch(time, data);
                    activator.activate();
                },
            ),
        );

        let errs = self
            .timely_worker
            .dataflow_named("Dataflow: logging", |scope| {
                Collection::<_, DataflowError, isize>::empty(scope)
                    .arrange()
                    .trace
            });

        let logger = self
            .timely_worker
            .log_register()
            .get("materialized")
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
            m_traces.extend(logging::materialized::construct(
                &mut self.timely_worker,
                logging,
                m_linked,
                m_activator,
            ));
        }

        // Install traces as maintained indexes
        for (log, trace) in t_traces {
            let id = logging.active_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()));
            self.compute_state
                .reported_frontiers
                .insert(id, Antichain::from_elem(0));
            logger.log(MaterializedEvent::Frontier(id, 0, 1));
        }
        for (log, trace) in r_traces {
            let id = logging.active_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()));
            self.compute_state
                .reported_frontiers
                .insert(id, Antichain::from_elem(0));
            logger.log(MaterializedEvent::Frontier(id, 0, 1));
        }
        for (log, trace) in d_traces {
            let id = logging.active_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()));
            self.compute_state
                .reported_frontiers
                .insert(id, Antichain::from_elem(0));
            logger.log(MaterializedEvent::Frontier(id, 0, 1));
        }
        for (log, trace) in m_traces {
            let id = logging.active_logs[&log];
            self.compute_state
                .traces
                .set(id, TraceBundle::new(trace, errs.clone()));
            self.compute_state
                .reported_frontiers
                .insert(id, Antichain::from_elem(0));
            logger.log(MaterializedEvent::Frontier(id, 0, 1));
        }

        self.compute_state.materialized_logger = Some(logger);
    }

    /// Disables timely dataflow logging.
    ///
    /// This does not unpublish views and is only useful to terminate logging streams to ensure that
    /// materialized can terminate cleanly.
    pub fn shutdown_logging(&mut self) {
        self.timely_worker.log_register().remove("timely");
        self.timely_worker
            .log_register()
            .remove("timely/reachability");
        self.timely_worker
            .log_register()
            .remove("differential/arrange");
        self.timely_worker.log_register().remove("materialized");
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
        if let Some(logger) = self.compute_state.materialized_logger.as_mut() {
            for (id, changes) in &mut progress {
                for (time, diff) in changes.iter() {
                    logger.log(MaterializedEvent::Frontier(*id, *time, *diff));
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
    fn send_peek_response(&mut self, peek: PendingPeek, response: PeekResponse) {
        // Respond with the response.
        self.send_compute_response(ComputeResponse::PeekResponse(peek.conn_id, response));

        // Log responding to the peek request.
        if let Some(logger) = self.compute_state.materialized_logger.as_mut() {
            logger.log(MaterializedEvent::Peek(peek.as_log_event(), false));
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
        let _ = self.response_tx.send(Response::Compute(response));
    }
}
