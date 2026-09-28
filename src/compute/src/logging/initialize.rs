// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Initialization of logging dataflows.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use differential_dataflow::VecCollection;
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
use mz_compute_client::logging::{LogVariant, LoggingConfig};
use mz_dyncfg::ConfigSet;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::{Diff, Timestamp};
use mz_storage_operators::persist_source::Subtime;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnation::ColumnationChunker;
use mz_timely_util::operator::CollectionExt;
use mz_timely_util::scope_label::ScopeExt;
use timely::ContainerBuilder;
use timely::container::{ContainerBuilder as _, PushInto};
use timely::logging::{
    OperatesSummaryEvent, TimelyEvent, TimelyEventBuilder, TimelySummaryEventBuilder,
};
use timely::logging_core::{Logger, Registry};
use timely::order::Product;
use timely::progress::reachability::logging::{TrackerEvent, TrackerEventBuilder};
use timely::progress::timestamp::Refines;

use crate::arrangement::manager::TraceBundle;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::logging::compute::{ComputeEvent, ComputeEventBuilder};
use crate::logging::{BatchLogger, EventQueue, SharedLoggingState};
use crate::render::errors::DataflowErrorSer;
use crate::typedefs::{ErrBatcher, ErrBuilder};

/// Initialize logging dataflows.
///
/// Returns a logger for compute events, and for each `LogVariant` a trace bundle usable for
/// retrieving logged records as well as the index of the exporting dataflow.
pub fn initialize(
    worker: &mut timely::worker::Worker,
    config: &LoggingConfig,
    metrics_registry: MetricsRegistry,
    worker_config: Rc<ConfigSet>,
    workers_per_process: usize,
) -> LoggingTraces {
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
        metrics_registry,
        worker_config,
        workers_per_process,
    };

    // Depending on whether we should log the creation of the logging dataflows, we register the
    // loggers with timely either before or after creating them.
    let dataflow_index = context.worker.next_dataflow_index();
    let traces = if config.log_logging {
        context.register_loggers();
        context.construct_dataflow()
    } else {
        let traces = context.construct_dataflow();
        context.register_loggers();
        traces
    };

    let compute_logger = worker.logger_for("materialize/compute").unwrap();
    LoggingTraces {
        traces,
        dataflow_index,
        compute_logger,
    }
}

pub(super) type ReachabilityEvent = (usize, Vec<(usize, usize, bool, Timestamp, Diff)>);

struct LoggingContext<'a> {
    worker: &'a mut timely::worker::Worker,
    config: &'a LoggingConfig,
    interval_ms: u128,
    now: Instant,
    start_offset: Duration,
    t_event_queue: EventQueue<Vec<(Duration, TimelyEvent)>>,
    r_event_queue: EventQueue<Column<(Duration, ReachabilityEvent)>, 3>,
    d_event_queue: EventQueue<Vec<(Duration, DifferentialEvent)>>,
    c_event_queue: EventQueue<Column<(Duration, ComputeEvent)>>,
    shared_state: Rc<RefCell<SharedLoggingState>>,
    metrics_registry: MetricsRegistry,
    worker_config: Rc<ConfigSet>,
    workers_per_process: usize,
}

pub(crate) struct LoggingTraces {
    /// Exported traces, by log variant.
    pub traces: BTreeMap<LogVariant, TraceBundle>,
    /// The index of the dataflow that exports the traces.
    pub dataflow_index: usize,
    /// The compute logger.
    pub compute_logger: super::compute::Logger,
}

impl LoggingContext<'_> {
    fn construct_dataflow(&mut self) -> BTreeMap<LogVariant, TraceBundle> {
        self.worker.dataflow_named("Dataflow: logging", |scope| {
            let scope = scope.with_label();

            let mut collections = BTreeMap::new();

            let super::timely::Return {
                collections: timely_collections,
            } = super::timely::construct(
                scope,
                self.config,
                self.t_event_queue.clone(),
                Rc::clone(&self.shared_state),
            );
            collections.extend(timely_collections);

            let super::reachability::Return {
                collections: reachability_collections,
            } = super::reachability::construct(scope, self.config, self.r_event_queue.clone());
            collections.extend(reachability_collections);

            let super::differential::Return {
                collections: differential_collections,
            } = super::differential::construct(
                scope,
                self.config,
                self.d_event_queue.clone(),
                Rc::clone(&self.shared_state),
            );
            collections.extend(differential_collections);

            let super::compute::Return {
                collections: compute_collections,
            } = super::compute::construct(
                scope.clone(),
                scope.activations(),
                self.config,
                self.c_event_queue.clone(),
                Rc::clone(&self.shared_state),
            );
            collections.extend(compute_collections);

            let super::prometheus::Return {
                collections: prometheus_collections,
            } = super::prometheus::construct(
                scope,
                self.config,
                self.metrics_registry.clone(),
                self.now,
                self.start_offset,
                Rc::clone(&self.worker_config),
                self.workers_per_process,
            );
            collections.extend(prometheus_collections);

            let super::resource_usage::Return {
                collections: resource_usage_collections,
            } = super::resource_usage::construct(
                scope,
                self.config,
                self.now,
                self.start_offset,
                self.workers_per_process,
            );
            collections.extend(resource_usage_collections);

            let errs = scope.scoped("logging errors", |scope| {
                let collection: KeyCollection<_, DataflowErrorSer, Diff> =
                    VecCollection::empty(scope).into();
                collection
                    .mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, _>(
                        "Arrange logging err",
                    )
                    .trace
            });

            let traces = collections
                .into_iter()
                .map(|(log, collection)| {
                    let bundle = TraceBundle::new(collection.trace, errs.clone())
                        .with_drop(collection.token);
                    (log, bundle)
                })
                .collect();
            traces
        })
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

        let mut register = self.worker.log_register().expect("Logging must be enabled");
        register.insert_logger("timely", t_logger);
        // Note that each reachability logger has a unique index, this is crucial to avoid dropping
        // data because the event link structure is not multi-producer safe.
        self.register_reachability_logger::<Timestamp>(&mut register, 0);
        self.register_reachability_logger::<Product<Timestamp, PointStamp<u64>>>(&mut register, 1);
        self.register_reachability_logger::<(Timestamp, Subtime)>(&mut register, 2);
        // Nothing claims pending summaries without the logging dataflow's demux, so they would
        // accumulate forever.
        if self.config.enable_logging {
            self.register_summary_logger::<Timestamp>(&mut register);
            self.register_summary_logger::<Product<Timestamp, PointStamp<u64>>>(&mut register);
            self.register_summary_logger::<(Timestamp, Subtime)>(&mut register);
        }
        register.insert_logger("differential/arrange", d_logger);
        register.insert_logger("materialize/compute", c_logger.clone());

        self.shared_state.borrow_mut().compute_logger = Some(c_logger);
    }

    /// Register an operator summary logger for scopes with timestamp type `T`.
    ///
    /// Timely looks up the logger by the name `timely/summary/{type_name::<T>()}` when it builds a
    /// scope's children. The logger reduces each summary to its outer timestamp and hands it to the
    /// timely demux through [`SharedLoggingState::pending_summaries`].
    fn register_summary_logger<T>(&self, registry: &mut Registry)
    where
        T: timely::progress::Timestamp + Refines<Timestamp>,
    {
        let shared_state = Rc::clone(&self.shared_state);
        let logger = Logger::<TimelySummaryEventBuilder<T::Summary>>::new(
            self.now,
            self.start_offset,
            move |_time, data: &mut Option<Vec<(Duration, OperatesSummaryEvent<T::Summary>)>>| {
                let Some(data) = data else { return };
                // NOTE: This borrow cannot conflict with the timely demux's, because timely only
                // flushes this logger while building operators or at the end of a worker step,
                // never while the logging dataflow runs.
                let mut shared_state = shared_state.borrow_mut();
                for (_time, event) in data.drain(..) {
                    let mut rows = Vec::new();
                    for (input, connectivity) in event.summary.iter().enumerate() {
                        let before = rows.len();
                        for (output, summaries) in connectivity.iter_ports() {
                            // Summaries in an antichain are incomparable, so the smallest outer
                            // delay is the delay the outer timestamp is guaranteed to incur.
                            let delay = summaries
                                .elements()
                                .iter()
                                .map(|s| u64::from(<T as Refines<Timestamp>>::summarize(s.clone())))
                                .min();
                            rows.push((input, Some(output), delay));
                        }
                        if rows.len() == before {
                            rows.push((input, None, None));
                        }
                    }
                    shared_state.pending_summaries.insert(event.id, rows);
                }
            },
        );
        let type_name = std::any::type_name::<T>();
        registry.insert_logger(&format!("timely/summary/{type_name}"), logger);
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
                                    (*node, *port, is_source, T::extract(time), Diff::from(*diff))
                                },
                            ));

                            builder.push_into((time, (update.tracker_id, &massaged)));
                            massaged.clear();
                        }
                        TrackerEvent::TargetUpdate(update) => {
                            massaged.extend(update.updates.iter().map(
                                |(node, port, time, diff)| {
                                    let is_source = false;
                                    (*node, *port, is_source, time.extract(), Diff::from(*diff))
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
