// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Initialization of logging dataflows.

use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use differential_dataflow::logging::DifferentialEvent;
use timely::communication::Allocate;
use timely::logging::{Logger, TimelyEvent};
use timely::progress::reachability::logging::TrackerEvent;

use mz_compute_client::logging::{LogVariant, LoggingConfig};
use mz_repr::Timestamp;

use crate::typedefs::KeysValsHandle;

use super::compute::ComputeEvent;
use super::reachability::ReachabilityEvent;
use super::{BatchLogger, Plumbing};

/// Initialize logging dataflows.
///
/// Returns a logger for compute events, and for each `LogVariant` a trace handle and a dataflow
/// drop token.
pub fn initialize<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
) -> (
    super::compute::Logger,
    BTreeMap<LogVariant, (KeysValsHandle, Rc<dyn Any>)>,
) {
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
        t_plumbing: Plumbing::new("t"),
        r_plumbing: Plumbing::new("r"),
        d_plumbing: Plumbing::new("d"),
        c_plumbing: Plumbing::new("c"),
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
    t_plumbing: Plumbing<TimelyEvent>,
    r_plumbing: Plumbing<ReachabilityEvent>,
    d_plumbing: Plumbing<DifferentialEvent>,
    c_plumbing: Plumbing<ComputeEvent>,
}

impl<A: Allocate> LoggingContext<'_, A> {
    fn construct_dataflows(&mut self) -> BTreeMap<LogVariant, (KeysValsHandle, Rc<dyn Any>)> {
        let mut traces = BTreeMap::new();
        traces.extend(super::timely::construct(
            self.worker,
            self.config,
            self.t_plumbing.clone(),
        ));
        traces.extend(super::reachability::construct(
            self.worker,
            self.config,
            self.r_plumbing.clone(),
        ));
        traces.extend(super::differential::construct(
            self.worker,
            self.config,
            self.d_plumbing.clone(),
        ));
        traces.extend(super::compute::construct(
            self.worker,
            self.config,
            self.c_plumbing.clone(),
        ));
        traces
    }

    fn register_loggers(&self) {
        self.worker
            .log_register()
            .insert_logger("timely", self.simple_logger(self.t_plumbing.clone()));
        self.worker
            .log_register()
            .insert_logger("timely/reachability", self.reachability_logger());
        self.worker.log_register().insert_logger(
            "differential/arrange",
            self.simple_logger(self.d_plumbing.clone()),
        );
        self.worker.log_register().insert_logger(
            "materialize/compute",
            self.simple_logger(self.c_plumbing.clone()),
        );
    }

    fn simple_logger<E: 'static>(&self, plumbing: Plumbing<E>) -> Logger<E> {
        let mut logger = BatchLogger::new(plumbing.link, self.interval_ms);
        Logger::new(
            self.now,
            self.start_offset,
            self.worker.index(),
            move |time, data| {
                logger.publish_batch(time, data);
                plumbing.activator.activate();
            },
        )
    }

    fn reachability_logger(&self) -> Logger<TrackerEvent> {
        let plumbing = self.r_plumbing.clone();
        let mut logger = BatchLogger::new(plumbing.link, self.interval_ms);
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
                plumbing.activator.activate();
            },
        )
    }
}
