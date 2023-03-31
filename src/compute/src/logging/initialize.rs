// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Initialization of logging dataflows.

use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use timely::communication::Allocate;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::logging::Logger;
use timely::progress::reachability::logging::TrackerEvent;

use mz_compute_client::logging::{LogVariant, LoggingConfig};
use mz_repr::Timestamp;
use mz_timely_util::activator::RcActivator;

use crate::typedefs::KeysValsHandle;

use super::BatchLogger;

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
    let interval = std::cmp::max(1, config.interval.as_millis())
        .try_into()
        .expect("must fit");

    // Track time relative to the Unix epoch, rather than when the server
    // started, so that the logging sources can be joined with tables and
    // other real time sources for semi-sensible results.
    let now = Instant::now();
    let start_offset = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("Failed to get duration since Unix epoch");

    // Establish loggers first, so we can either log the logging or not, as we like.
    let t_linked = Rc::new(EventLink::new());
    let mut t_logger = BatchLogger::new(Rc::clone(&t_linked), interval);
    let r_linked = Rc::new(EventLink::new());
    let mut r_logger = BatchLogger::new(Rc::clone(&r_linked), interval);
    let d_linked = Rc::new(EventLink::new());
    let mut d_logger = BatchLogger::new(Rc::clone(&d_linked), interval);
    let c_linked = Rc::new(EventLink::new());
    let mut c_logger = BatchLogger::new(Rc::clone(&c_linked), interval);

    let activate_after = 128;
    let t_activator = RcActivator::new("t_activator".into(), activate_after);
    let r_activator = RcActivator::new("r_activator".into(), activate_after);
    let d_activator = RcActivator::new("d_activator".into(), activate_after);
    let c_activator = RcActivator::new("c_activator".into(), activate_after);

    let mut traces = BTreeMap::new();

    if !config.log_logging {
        // Construct logging dataflows and endpoints before registering any.
        traces.extend(super::timely::construct(
            worker,
            config,
            Rc::clone(&t_linked),
            t_activator.clone(),
        ));
        traces.extend(super::reachability::construct(
            worker,
            config,
            Rc::clone(&r_linked),
            r_activator.clone(),
        ));
        traces.extend(super::differential::construct(
            worker,
            config,
            Rc::clone(&d_linked),
            d_activator.clone(),
        ));
        traces.extend(super::compute::construct(
            worker,
            config,
            Rc::clone(&c_linked),
            c_activator.clone(),
        ));
    }

    // Register each logger endpoint.
    let activator = t_activator.clone();
    worker.log_register().insert_logger(
        "timely",
        Logger::new(now, start_offset, worker.index(), move |time, data| {
            t_logger.publish_batch(time, data);
            activator.activate();
        }),
    );

    let activator = r_activator.clone();
    worker.log_register().insert_logger(
        "timely/reachability",
        Logger::new(
            now,
            start_offset,
            worker.index(),
            move |time, data: &mut Vec<(Duration, usize, TrackerEvent)>| {
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
                r_logger.publish_batch(time, &mut converted_updates);
                activator.activate();
            },
        ),
    );

    let activator = d_activator.clone();
    worker.log_register().insert_logger(
        "differential/arrange",
        Logger::new(now, start_offset, worker.index(), move |time, data| {
            d_logger.publish_batch(time, data);
            activator.activate();
        }),
    );

    let activator = c_activator.clone();
    worker.log_register().insert_logger(
        "materialize/compute",
        Logger::new(now, start_offset, worker.index(), move |time, data| {
            c_logger.publish_batch(time, data);
            activator.activate();
        }),
    );

    let logger = worker.log_register().get("materialize/compute").unwrap();

    if config.log_logging {
        // Create log processing dataflows after registering logging so we can log the
        // logging.
        traces.extend(super::timely::construct(
            worker,
            config,
            t_linked,
            t_activator,
        ));
        traces.extend(super::reachability::construct(
            worker,
            config,
            r_linked,
            r_activator,
        ));
        traces.extend(super::differential::construct(
            worker,
            config,
            d_linked,
            d_activator,
        ));
        traces.extend(super::compute::construct(
            worker,
            config,
            c_linked,
            c_activator,
        ));
    }

    (logger, traces)
}
