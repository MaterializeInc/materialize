// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Initialization of logging dataflows.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
use differential_dataflow::Collection;
use mz_compute_client::logging::{LogVariant, LoggingConfig};
use mz_repr::{Diff, Timestamp};
use mz_storage_operators::persist_source::Subtime;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::containers::{Column, ColumnBuilder};
use mz_timely_util::operator::CollectionExt;
use timely::communication::Allocate;
use timely::container::{ContainerBuilder, PushInto};
use timely::logging::{ProgressEventTimestamp, TimelyEvent, TimelyEventBuilder};
use timely::logging_core::Logger;
use timely::order::Product;
use timely::progress::reachability::logging::{TrackerEvent, TrackerEventBuilder};

use crate::arrangement::manager::TraceBundle;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::logging::compute::{ComputeEvent, ComputeEventBuilder};
use crate::logging::{BatchLogger, EventQueue, SharedLoggingState};
use crate::typedefs::{ErrBatcher, ErrBuilder};

/// Initialize logging dataflows.
///
/// Returns a logger for compute events, and for each `LogVariant` a trace bundle usable for
/// retrieving logged records as well as the index of the exporting dataflow.
pub fn initialize<A: Allocate + 'static>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
) -> (
    super::compute::Logger,
    BTreeMap<LogVariant, (TraceBundle, usize)>,
) {
    let interval_ms = std::cmp::max(1, config.interval.as_millis());

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

pub(super) type ReachabilityEvent = (
    Vec<usize>,
    Vec<(usize, usize, bool, Option<Timestamp>, Diff)>,
);

struct LoggingContext<'a, A: Allocate> {
    worker: &'a mut timely::worker::Worker<A>,
    config: &'a LoggingConfig,
    interval_ms: u128,
    now: Instant,
    start_offset: Duration,
    t_event_queue: EventQueue<Vec<(Duration, TimelyEvent)>>,
    r_event_queue: EventQueue<Column<(Duration, ReachabilityEvent)>>,
    d_event_queue: EventQueue<Vec<(Duration, DifferentialEvent)>>,
    c_event_queue: EventQueue<Column<(Duration, ComputeEvent)>>,
    shared_state: Rc<RefCell<SharedLoggingState>>,
}

impl<A: Allocate + 'static> LoggingContext<'_, A> {
    fn construct_dataflows(&mut self) -> BTreeMap<LogVariant, (TraceBundle, usize)> {
        let mut collections = BTreeMap::new();
        collections.extend(super::timely::construct(
            self.worker,
            self.config,
            self.t_event_queue.clone(),
            Rc::clone(&self.shared_state),
        ));
        collections.extend(super::reachability::construct(
            self.worker,
            self.config,
            self.r_event_queue.clone(),
        ));
        collections.extend(super::differential::construct(
            self.worker,
            self.config,
            self.d_event_queue.clone(),
            Rc::clone(&self.shared_state),
        ));
        collections.extend(super::compute::construct(
            self.worker,
            self.config,
            self.c_event_queue.clone(),
            Rc::clone(&self.shared_state),
        ));

        let errs = self
            .worker
            .dataflow_named("Dataflow: logging errors", |scope| {
                let collection: KeyCollection<_, DataflowError, Diff> =
                    Collection::empty(scope).into();
                collection
                    .mz_arrange::<ErrBatcher<_, _>, ErrBuilder<_, _>, _>("Arrange logging err")
                    .trace
            });

        // TODO(vmarcos): If we introduce introspection sources that would match
        // type specialization for keys, we'd need to ensure that type specialized
        // variants reach the map below (issue database-issues#6763).
        collections
            .into_iter()
            .map(|(log, collection)| {
                let bundle =
                    TraceBundle::new(collection.trace, errs.clone()).with_drop(collection.token);
                (log, (bundle, collection.dataflow_index))
            })
            .collect()
    }

    fn register_loggers(&self) {
        let t_logger = self.simple_logger::<TimelyEventBuilder>(self.t_event_queue.clone());
        let r_logger = self.reachability_logger();
        let d_logger = self.simple_logger::<DifferentialEventBuilder>(self.d_event_queue.clone());
        let c_logger = self.simple_logger::<ComputeEventBuilder>(self.c_event_queue.clone());

        let mut register = self.worker.log_register();
        register.insert_logger("timely", t_logger);
        register.insert_logger("timely/reachability", r_logger);
        register.insert_logger("differential/arrange", d_logger);
        register.insert_logger("materialize/compute", c_logger.clone());

        self.shared_state.borrow_mut().compute_logger = Some(c_logger);
    }

    fn simple_logger<CB: ContainerBuilder>(
        &self,
        event_queue: EventQueue<CB::Container>,
    ) -> Logger<CB> {
        let mut logger = BatchLogger::<_, _>::new(event_queue.link, self.interval_ms);
        Logger::new(
            self.now,
            self.start_offset,
            move |time, data: &mut Option<CB::Container>| {
                if let Some(data) = data.take() {
                    logger.publish_batch(data);
                } else if logger.report_progress(*time) {
                    event_queue.activator.activate();
                }
            },
        )
    }

    fn reachability_logger(&self) -> Logger<TrackerEventBuilder> {
        let event_queue = self.r_event_queue.clone();

        let mut logger = BatchLogger::<_, _>::new(event_queue.link, self.interval_ms);
        let mut massaged = Vec::new();
        let mut builder = ColumnBuilder::default();

        let action = move |batch_time: &Duration, data: &mut Option<Vec<_>>| {
            if let Some(data) = data {
                // Handle data
                for (time, event) in data.drain(..) {
                    match event {
                        TrackerEvent::SourceUpdate(update) => {
                            massaged.extend(update.updates.iter().map(
                                |(node, port, time, diff)| {
                                    let ts = try_downcast_timestamp(time);
                                    let is_source = true;
                                    (*node, *port, is_source, ts, *diff)
                                },
                            ));

                            builder.push_into((time, (&update.tracker_id, &massaged)));
                            massaged.clear();
                        }
                        TrackerEvent::TargetUpdate(update) => {
                            massaged.extend(update.updates.iter().map(
                                |(node, port, time, diff)| {
                                    let ts = try_downcast_timestamp(time);
                                    let is_source = false;
                                    (*node, *port, is_source, ts, *diff)
                                },
                            ));

                            builder.push_into((time, (&update.tracker_id, &massaged)));
                            massaged.clear();
                        }
                    }
                    while let Some(container) = builder.extract() {
                        logger.publish_batch(std::mem::take(container));
                    }
                }
            } else {
                // Handle a flush
                while let Some(container) = builder.finish() {
                    logger.publish_batch(std::mem::take(container));
                }

                if logger.report_progress(*batch_time) {
                    event_queue.activator.activate();
                }
            }
        };

        Logger::new(self.now, self.start_offset, action)
    }
}

/// Extracts a `Timestamp` from a `dyn ProgressEventTimestamp`.
///
/// For nested timestamps, only extracts the outermost one. The rest of the timestamps are
/// ignored for now.
#[inline]
fn try_downcast_timestamp(time: &dyn ProgressEventTimestamp) -> Option<Timestamp> {
    let time_any = time.as_any();
    time_any
        .downcast_ref::<Timestamp>()
        .copied()
        .or_else(|| {
            time_any
                .downcast_ref::<Product<Timestamp, PointStamp<u64>>>()
                .map(|t| t.outer)
        })
        .or_else(|| time_any.downcast_ref::<(Timestamp, Subtime)>().map(|t| t.0))
}
