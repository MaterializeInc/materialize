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
use timely::logging::{TimelyEvent, TimelyEventBuilder};
use timely::logging_core::{Logger, Registry};
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

pub(super) type ReachabilityEvent = (usize, Vec<(usize, usize, bool, Timestamp, Diff)>);

struct LoggingContext<'a, A: Allocate> {
    worker: &'a mut timely::worker::Worker<A>,
    config: &'a LoggingConfig,
    interval_ms: u128,
    now: Instant,
    start_offset: Duration,
    t_event_queue: EventQueue<Vec<(Duration, TimelyEvent)>>,
    r_event_queue: EventQueue<Column<(Duration, ReachabilityEvent)>, 3>,
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

    /// Construct a new reachability logger for timestamp type `T`.
    ///
    /// Inserts a logger with the name `timely/reachability/{type_name::<T>()}`, following
    /// Timely naming convention.
    fn register_reachability_logger<T: ExtractTimestamp>(
        &self,
        registry: &mut Registry,
        index: usize,
    ) {
        let logger = self.reachability_logger::<T>(index);
        let type_name = std::any::type_name::<T>();
        registry.insert_logger(&format!("timely/reachability/{type_name}"), logger);
    }

    /// Register all loggers with the timely worker.
    ///
    /// Registers the timely, differential, compute, and reachability loggers.
    fn register_loggers(&self) {
        let t_logger = self.simple_logger::<TimelyEventBuilder>(self.t_event_queue.clone());
        let d_logger = self.simple_logger::<DifferentialEventBuilder>(self.d_event_queue.clone());
        let c_logger = self.simple_logger::<ComputeEventBuilder>(self.c_event_queue.clone());

        let mut register = self.worker.log_register();
        register.insert_logger("timely", t_logger);
        // Note that each reachability logger has a unique index, this is crucial to avoid dropping
        // data because the event link structure is not multi-producer safe.
        self.register_reachability_logger::<Timestamp>(&mut register, 0);
        self.register_reachability_logger::<Product<Timestamp, PointStamp<u64>>>(&mut register, 1);
        self.register_reachability_logger::<(Timestamp, Subtime)>(&mut register, 2);
        register.insert_logger("differential/arrange", d_logger);
        register.insert_logger("materialize/compute", c_logger.clone());

        self.shared_state.borrow_mut().compute_logger = Some(c_logger);
    }

    fn simple_logger<CB: ContainerBuilder>(
        &self,
        event_queue: EventQueue<CB::Container>,
    ) -> Logger<CB> {
        let [link] = event_queue.links;
        let mut logger = BatchLogger::new(link, self.interval_ms);
        let activator = event_queue.activator.clone();
        Logger::new(
            self.now,
            self.start_offset,
            move |time, data: &mut Option<CB::Container>| {
                if let Some(data) = data.take() {
                    logger.publish_batch(data);
                } else if logger.report_progress(*time) {
                    activator.activate();
                }
            },
        )
    }

    /// Construct a reachability logger for timestamp type `T`. The index must
    /// refer to a unique link in the reachability event queue.
    fn reachability_logger<T>(&self, index: usize) -> Logger<TrackerEventBuilder<T>>
    where
        T: ExtractTimestamp,
    {
        let link = Rc::clone(&self.r_event_queue.links[index]);
        let mut logger = BatchLogger::new(link, self.interval_ms);
        let mut massaged = Vec::new();
        let mut builder = ColumnBuilder::default();
        let activator = self.r_event_queue.activator.clone();

        let action = move |batch_time: &Duration, data: &mut Option<Vec<_>>| {
            if let Some(data) = data {
                // Handle data
                for (time, event) in data.drain(..) {
                    match event {
                        TrackerEvent::SourceUpdate(update) => {
                            massaged.extend(update.updates.iter().map(
                                |(node, port, time, diff)| {
                                    let is_source = true;
                                    (*node, *port, is_source, T::extract(time), *diff)
                                },
                            ));

                            builder.push_into((time, (update.tracker_id, &massaged)));
                            massaged.clear();
                        }
                        TrackerEvent::TargetUpdate(update) => {
                            massaged.extend(update.updates.iter().map(
                                |(node, port, time, diff)| {
                                    let is_source = false;
                                    (*node, *port, is_source, time.extract(), *diff)
                                },
                            ));

                            builder.push_into((time, (update.tracker_id, &massaged)));
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
                    activator.activate();
                }
            }
        };

        Logger::new(self.now, self.start_offset, action)
    }
}

/// Helper trait to extract a timestamp from various types of timestamp used in rendering.
trait ExtractTimestamp: Clone + 'static {
    /// Extracts the timestamp from the type.
    fn extract(&self) -> Timestamp;
}

impl ExtractTimestamp for Timestamp {
    fn extract(&self) -> Timestamp {
        *self
    }
}

impl ExtractTimestamp for Product<Timestamp, PointStamp<u64>> {
    fn extract(&self) -> Timestamp {
        self.outer
    }
}

impl ExtractTimestamp for (Timestamp, Subtime) {
    fn extract(&self) -> Timestamp {
        self.0
    }
}
