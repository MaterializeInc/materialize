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
use timely::logging::Logger;
use timely::progress::reachability::logging::TrackerEvent;

use mz_compute_client::logging::{LogVariant, LoggingConfig};
use mz_repr::Timestamp;

use crate::typedefs::KeysValsHandle;

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

    let t_plumbing = Plumbing::new("t");
    let r_plumbing = Plumbing::new("r");
    let d_plumbing = Plumbing::new("d");
    let c_plumbing = Plumbing::new("c");

    let mut traces = BTreeMap::new();

    if !config.log_logging {
        // Construct logging dataflows and endpoints before registering any.
        traces.extend(super::timely::construct(worker, config, t_plumbing.clone()));
        traces.extend(super::reachability::construct(
            worker,
            config,
            r_plumbing.clone(),
        ));
        traces.extend(super::differential::construct(
            worker,
            config,
            d_plumbing.clone(),
        ));
        traces.extend(super::compute::construct(
            worker,
            config,
            c_plumbing.clone(),
        ));
    }

    // Register each logger endpoint.
    let plumbing = t_plumbing.clone();
    worker.log_register().insert_logger("timely", {
        let mut logger = BatchLogger::new(plumbing.link, interval_ms);
        Logger::new(now, start_offset, worker.index(), move |time, data| {
            logger.publish_batch(time, data);
            plumbing.activator.activate();
        })
    });

    let plumbing = r_plumbing.clone();
    worker.log_register().insert_logger("timely/reachability", {
        let mut logger = BatchLogger::new(plumbing.link, interval_ms);
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
                logger.publish_batch(time, &mut converted_updates);
                plumbing.activator.activate();
            },
        )
    });

    let plumbing = d_plumbing.clone();
    worker
        .log_register()
        .insert_logger("differential/arrange", {
            let mut logger = BatchLogger::new(plumbing.link, interval_ms);
            Logger::new(now, start_offset, worker.index(), move |time, data| {
                logger.publish_batch(time, data);
                plumbing.activator.activate();
            })
        });

    let plumbing = c_plumbing.clone();
    worker.log_register().insert_logger("materialize/compute", {
        let mut logger = BatchLogger::new(plumbing.link, interval_ms);
        Logger::new(now, start_offset, worker.index(), move |time, data| {
            logger.publish_batch(time, data);
            plumbing.activator.activate();
        })
    });

    let logger = worker.log_register().get("materialize/compute").unwrap();

    if config.log_logging {
        // Create log processing dataflows after registering logging so we can log the
        // logging.
        traces.extend(super::timely::construct(worker, config, t_plumbing));
        traces.extend(super::reachability::construct(worker, config, r_plumbing));
        traces.extend(super::differential::construct(worker, config, d_plumbing));
        traces.extend(super::compute::construct(worker, config, c_plumbing));
    }

    (logger, traces)
}
