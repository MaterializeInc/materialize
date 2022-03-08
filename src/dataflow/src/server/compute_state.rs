// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use timely::communication::Allocate;
use timely::logging::Logger;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::reachability::logging::TrackerEvent;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

use mz_dataflow_types::client::{ComputeCommand, ComputeInstanceId, ComputeResponse, Response};
use mz_dataflow_types::logging::LoggingConfig;
use mz_dataflow_types::{DataflowError, PeekResponse, TailResponse};
use mz_expr::GlobalId;
use mz_repr::{Diff, Timestamp};

use crate::activator::RcActivator;
use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::logging;
use crate::logging::materialized::MaterializedEvent;
use crate::operator::CollectionExt;
use crate::server::boundary::ComputeReplay;
use crate::server::PendingPeek;
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

/// A wrapper around [ComputeState] with a live timely worker and response channel.
pub struct ActiveComputeState<'a, A: Allocate, B: ComputeReplay> {
    /// The underlying Timely worker.
    pub timely_worker: &'a mut TimelyWorker<A>,
    /// The compute state itself.
    pub compute_state: &'a mut ComputeState,
    /// The identifier of the compute instance, for forming responses.
    pub instance_id: ComputeInstanceId,
    /// The channel over which frontier information is reported.
    pub response_tx: &'a mut mpsc::UnboundedSender<Response>,
    /// The boundary with the Storage layer
    pub boundary: &'a mut B,
}

impl<'a, A: Allocate, B: ComputeReplay> ActiveComputeState<'a, A, B> {
    /// Entrypoint for applying a compute command.
    pub(crate) fn handle_compute_command(&mut self, cmd: ComputeCommand) {
        match cmd {
            ComputeCommand::CreateInstance(logging) => {
                if let Some(logging) = logging {
                    self.initialize_logging(&logging);
                }
            }
            ComputeCommand::DropInstance => {}

            ComputeCommand::CreateDataflows(dataflows) => {
                for dataflow in dataflows.into_iter() {
                    for (sink_id, _) in dataflow.sink_exports.iter() {
                        self.compute_state
                            .reported_frontiers
                            .insert(*sink_id, Antichain::from_elem(0));
                    }
                    for (idx_id, idx, _) in dataflow.index_exports.iter() {
                        self.compute_state
                            .reported_frontiers
                            .insert(*idx_id, Antichain::from_elem(0));
                        if let Some(logger) = self.compute_state.materialized_logger.as_mut() {
                            logger.log(MaterializedEvent::Dataflow(*idx_id, true));
                            logger.log(MaterializedEvent::Frontier(*idx_id, 0, 1));
                            for import_id in dataflow.depends_on(idx.on_id) {
                                logger.log(MaterializedEvent::DataflowDependency {
                                    dataflow: *idx_id,
                                    source: import_id,
                                })
                            }
                        }
                    }

                    crate::render::build_compute_dataflow(
                        self.timely_worker,
                        &mut self.compute_state,
                        dataflow,
                        self.boundary,
                    );
                }
            }
            ComputeCommand::AllowCompaction(list) => {
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
                        if let Some(logger) = self.compute_state.materialized_logger.as_mut() {
                            logger.log(MaterializedEvent::Dataflow(id, false));
                            for time in frontier.elements().iter() {
                                logger.log(MaterializedEvent::Frontier(id, *time, -1));
                            }
                        }
                    } else {
                        self.compute_state
                            .traces
                            .allow_compaction(id, frontier.borrow());
                    }
                }
            }

            ComputeCommand::Peek {
                id,
                key,
                timestamp,
                uuid,
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
                    uuid,
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
            ComputeCommand::CancelPeeks { uuids } => {
                let pending_peeks_len = self.compute_state.pending_peeks.len();
                let mut pending_peeks = std::mem::replace(
                    &mut self.compute_state.pending_peeks,
                    Vec::with_capacity(pending_peeks_len),
                );
                for peek in pending_peeks.drain(..) {
                    if uuids.contains(&peek.uuid) {
                        self.send_peek_response(peek, PeekResponse::Canceled);
                    } else {
                        self.compute_state.pending_peeks.push(peek);
                    }
                }
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
                Collection::<_, DataflowError, Diff>::empty(scope)
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
        self.send_compute_response(ComputeResponse::PeekResponse(peek.uuid, response));

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
        let _ = self
            .response_tx
            .send(Response::Compute(response, self.instance_id));
    }
}
