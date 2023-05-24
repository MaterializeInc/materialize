// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Initialization of logging dataflows.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use differential_dataflow::logging::DifferentialEvent;
use differential_dataflow::Collection;
use mz_compute_client::logging::{LogVariant, LoggingConfig};
use mz_repr::{Diff, Timestamp};
use mz_storage_client::types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;
use timely::communication::Allocate;
use timely::logging::{Logger, TimelyEvent};
use timely::progress::reachability::logging::TrackerEvent;

use crate::arrangement::manager::TraceBundle;
use crate::extensions::operator::MzArrange;
use crate::logging::compute::ComputeEvent;
use crate::logging::reachability::ReachabilityEvent;
use crate::logging::{BatchLogger, EventQueue, SharedLoggingState};

/// Initialize logging dataflows.
///
/// Returns a logger for compute events, and for each `LogVariant` a trace bundle usable for
/// retrieving logged records.
pub fn initialize<A: Allocate + 'static>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
) -> (super::compute::Logger, BTreeMap<LogVariant, TraceBundle>) {
    let interval_ms = std::cmp::max(1, config.interval.as_millis())
        .try_into()
        .expect("must fit");

    // Track time relative to the Unix epoch, rather than when the server
    // started, so that the logging sources can be joined with tables and
    // other real time sources for semi-sensible results.
    let now = Instant::now();
    let start_offset = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("Failed to get duration since Unix epoch");

    let mut context = LoggingContext {
        worker,
        config,
        interval_ms,
        now,
        start_offset,
        t_event_queue: EventQueue::new("t"),
        r_event_queue: EventQueue::new("r"),
        d_event_queue: EventQueue::new("d"),
        c_event_queue: EventQueue::new("c"),
        shared_state: Default::default(),
    };

    // Depending on whether we should log the creation of the logging dataflows, we register the
    // loggers with timely either before or after creating them.
    let traces = if config.log_logging {
        context.register_loggers();
        context.construct_dataflows()
    } else {
        let traces = context.construct_dataflows();
        context.register_loggers();
        traces
    };

    let logger = worker.log_register().get("materialize/compute").unwrap();
    (logger, traces)
}

struct LoggingContext<'a, A: Allocate> {
    worker: &'a mut timely::worker::Worker<A>,
    config: &'a LoggingConfig,
    interval_ms: u64,
    now: Instant,
    start_offset: Duration,
    t_event_queue: EventQueue<TimelyEvent>,
    r_event_queue: EventQueue<ReachabilityEvent>,
    d_event_queue: EventQueue<DifferentialEvent>,
    c_event_queue: EventQueue<ComputeEvent>,
    shared_state: Rc<RefCell<SharedLoggingState>>,
}

impl<A: Allocate + 'static> LoggingContext<'_, A> {
    fn construct_dataflows(&mut self) -> BTreeMap<LogVariant, TraceBundle> {
        let mut traces = BTreeMap::new();
        traces.extend(super::timely::construct(
            self.worker,
            self.config,
            self.t_event_queue.clone(),
        ));
        traces.extend(super::reachability::construct(
            self.worker,
            self.config,
            self.r_event_queue.clone(),
        ));
        traces.extend(super::differential::construct(
            self.worker,
            self.config,
            self.d_event_queue.clone(),
            Rc::clone(&self.shared_state),
        ));
        traces.extend(super::compute::construct(
            self.worker,
            self.config,
            self.c_event_queue.clone(),
            Rc::clone(&self.shared_state),
        ));

        let errs = self
            .worker
            .dataflow_named("Dataflow: logging errors", |scope| {
                Collection::<_, DataflowError, Diff>::empty(scope)
                    .mz_arrange("Arrange logging err")
                    .trace
            });

        traces
            .into_iter()
            .map(|(log, (trace, token))| {
                let bundle = TraceBundle::new(trace, errs.clone()).with_drop(token);
                (log, bundle)
            })
            .collect()
    }

    fn register_loggers(&self) {
        self.worker
            .log_register()
            .insert_logger("timely", self.simple_logger(self.t_event_queue.clone()));
        self.worker
            .log_register()
            .insert_logger("timely/reachability", self.reachability_logger());
        self.worker.log_register().insert_logger(
            "differential/arrange",
            self.simple_logger(self.d_event_queue.clone()),
        );
        self.worker.log_register().insert_logger(
            "materialize/compute",
            self.simple_logger(self.c_event_queue.clone()),
        );
    }

    fn simple_logger<E: 'static>(&self, event_queue: EventQueue<E>) -> Logger<E> {
        let mut logger = BatchLogger::new(event_queue.link, self.interval_ms);
        Logger::new(
            self.now,
            self.start_offset,
            self.worker.index(),
            move |time, data| {
                logger.publish_batch(time, data);
                event_queue.activator.activate();
            },
        )
    }

    fn reachability_logger(&self) -> Logger<TrackerEvent> {
        let event_queue = self.r_event_queue.clone();
        let mut logger = BatchLogger::new(event_queue.link, self.interval_ms);
        Logger::new(
            self.now,
            self.start_offset,
            self.worker.index(),
            move |time, data| {
                let mut converted_updates = Vec::new();
                for event in data.drain(..) {
                    match event.2 {
                        TrackerEvent::SourceUpdate(update) => {
                            let massaged: Vec<_> = update
                                .updates
                                .iter()
                                .map(|(node, port, time, diff)| {
                                    let ts = time.as_any().downcast_ref::<Timestamp>().copied();
                                    let is_source = true;
                                    (*node, *port, is_source, ts, *diff)
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
                                .map(|(node, port, time, diff)| {
                                    let ts = time.as_any().downcast_ref::<Timestamp>().copied();
                                    let is_source = false;
                                    (*node, *port, is_source, ts, *diff)
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
                logger.publish_batch(time, &mut converted_updates);
                event_queue.activator.activate();
            },
        )
    }
}
