// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Routing of events logged on any worker to the worker that aggregates them.
//!
//! Storage produces reports, such as health updates and statistics, on every worker, but
//! aggregates them per object on a single worker. Producers log such events to a timely logger,
//! which needs nothing but a handle to the worker, so reports need no dataflow edges. One
//! long-lived dataflow per worker and event type drains the logger and exchanges the events to
//! the aggregating worker.
//!
//! Each worker's events of one type arrive at the aggregating worker in the order they were
//! logged, because the logger preserves order and timely channels are FIFO per pair of workers.
//! Events of different workers interleave arbitrarily.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_repr::GlobalId;
use mz_timely_util::scope_label::ScopeExt;
use timely::ExchangeData;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::source;
use timely::logging_core::Logger;
use timely::scheduling::Activator;
use timely::worker::Worker as TimelyWorker;

/// A logger for events of type `E`.
pub(crate) type EventLogger<E> = Logger<CapacityContainerBuilder<Vec<(Duration, E)>>>;

/// The worker among `worker_count` workers that aggregates the events about `id`.
pub(crate) fn aggregating_worker(id: GlobalId, worker_count: usize) -> usize {
    usize::cast_from(id.hashed()) % worker_count
}

/// Returns the logger registered under `name` on `worker`, if [`render_event_log`] registered
/// it.
pub(crate) fn event_logger<E: 'static>(
    worker: &TimelyWorker,
    name: &str,
) -> Option<EventLogger<E>> {
    worker.logger_for(name)
}

/// Renders a dataflow that exchanges events logged under `logger_name` on any worker to the
/// worker that `route` selects, and registers the logger with `worker`.
///
/// `consumer` runs on every worker whenever its operator is scheduled, with the events routed to
/// that worker since its last run, possibly none. It can request a later run through the
/// activator it is passed. Must be called once per worker and logger name, at the same point in
/// the dataflow rendering order on every worker. The dataflow shuts down once the returned token
/// is dropped on every worker. Events logged before the drop are delivered, later ones are
/// discarded.
pub(crate) fn render_event_log<E, R, C>(
    worker: &mut TimelyWorker,
    dataflow_name: &str,
    logger_name: &str,
    route: R,
    mut consumer: C,
) -> EventLogDataflow
where
    E: ExchangeData,
    R: Fn(&E) -> u64 + 'static,
    C: FnMut(Vec<E>, &Activator) + 'static,
{
    let queue: Rc<RefCell<Vec<E>>> = Default::default();
    let activator_slot: Rc<RefCell<Option<Activator>>> = Default::default();
    let alive = Rc::new(());

    worker.dataflow_named::<(), _, _>(dataflow_name, |scope| {
        let scope = scope.with_label();

        let source_queue = Rc::clone(&queue);
        let source_activator = Rc::clone(&activator_slot);
        let weak_alive = Rc::downgrade(&alive);
        let events = source::<_, CapacityContainerBuilder<Vec<E>>, _, _>(
            scope,
            &format!("{dataflow_name}: events"),
            move |cap, info| {
                *source_activator.borrow_mut() = Some(scope.activator_for(info.address));
                let mut capability = Some(cap);
                move |output| {
                    let Some(cap) = capability.as_ref() else {
                        return;
                    };
                    let mut events = std::mem::take(&mut *source_queue.borrow_mut());
                    if !events.is_empty() {
                        output.session(cap).give_container(&mut events);
                    }
                    if weak_alive.upgrade().is_none() {
                        capability = None;
                    }
                }
            },
        );

        let mut builder = OperatorBuilder::new(format!("{dataflow_name}: aggregate"), scope);
        let activator = scope.activator_for(builder.operator_info().address);
        let mut input = builder.new_input(events, Exchange::new(route));
        builder.build(move |_capabilities| {
            move |_frontiers| {
                let mut events = Vec::new();
                input.for_each(|_time, data| events.append(data));
                consumer(events, &activator);
            }
        });
    });

    let logger_queue = Rc::clone(&queue);
    let logger_activator = Rc::clone(&activator_slot);
    let logger_alive = Rc::downgrade(&alive);
    let logger = EventLogger::<E>::new(
        Instant::now(),
        Duration::ZERO,
        move |_time, data: &mut Option<Vec<(Duration, E)>>| {
            let Some(data) = data else {
                return;
            };
            if logger_alive.upgrade().is_none() {
                // The dataflow is gone, and nothing drains the queue anymore.
                data.clear();
                return;
            }
            logger_queue
                .borrow_mut()
                .extend(data.drain(..).map(|(_time, event)| event));
            if let Some(activator) = logger_activator.borrow().as_ref() {
                activator.activate();
            }
        },
    );
    let previous = worker
        .log_register()
        .expect("logging must be enabled")
        .insert_logger(logger_name, logger.clone());
    assert!(previous.is_none(), "logger {logger_name} registered twice");

    EventLogDataflow {
        _alive: alive,
        activator: activator_slot,
        flush: Box::new(move || logger.flush()),
    }
}

/// Keeps an event log dataflow running. Dropping it shuts the dataflow down.
pub struct EventLogDataflow {
    _alive: Rc<()>,
    activator: Rc<RefCell<Option<Activator>>>,
    /// Flushes the logger, which buffers events until the worker's next step ends.
    flush: Box<dyn Fn()>,
}

impl Drop for EventLogDataflow {
    fn drop(&mut self) {
        // Hand buffered events to the source while the dataflow is still alive, then wake the
        // source, so it drains them, observes the shutdown, and drops its capability.
        (self.flush)();
        if let Some(activator) = self.activator.borrow().as_ref() {
            activator.activate();
        }
    }
}

#[cfg(test)]
mod tests;
