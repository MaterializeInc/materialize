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
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_storage_operators::persist_source::Subtime;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnation::ColumnationChunker;
use mz_timely_util::operator::CollectionExt;
use mz_timely_util::scope_label::ScopeExt;
use prometheus::IntCounter;
use timely::ContainerBuilder;
use timely::container::{ContainerBuilder as _, PushInto};
use timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder, TimelyLogger};
use timely::logging_core::{Logger, Registry};
use timely::order::Product;
use timely::progress::reachability::logging::{TrackerEvent, TrackerEventBuilder};

use crate::arrangement::manager::TraceBundle;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::logging::compute::{ComputeEvent, ComputeEventBuilder};
use crate::logging::{BatchLogger, EventQueue, SharedLoggingState};
use crate::metrics::LoggingMetrics;
use crate::render::errors::DataflowErrorSer;
use crate::sharing::Publisher;
use crate::typedefs::{ErrAgent, ErrBatcher, ErrBuilder, RowRowAgent};

/// Initialize logging dataflows.
///
/// Returns a logger for compute events, and for each `LogVariant` a trace bundle usable for
/// retrieving logged records as well as the index of the exporting dataflow.
pub fn initialize(
    worker: &mut timely::worker::Worker,
    config: &LoggingConfig,
    metrics_registry: MetricsRegistry,
    metrics: LoggingMetrics,
    worker_config: Rc<ConfigSet>,
    workers_per_process: usize,
    publisher: Publisher,
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
        metrics,
        worker_config,
        workers_per_process,
        publisher,
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
    metrics: LoggingMetrics,
    worker_config: Rc<ConfigSet>,
    workers_per_process: usize,
    /// Publishes the logging indexes for the peer runtime.
    publisher: Publisher,
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
        let step_logger = self.step_logger();
        self.worker.dataflow_core(
            "Dataflow: logging",
            step_logger,
            Box::new(()),
            |_, scope| {
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
                        let publication = self.config.index_logs.get(&log).and_then(|&id| {
                            self.publisher
                                .publish(id, scope.worker(), &collection.trace, &errs)
                        });
                        let bundle = TraceBundle::new(collection.trace, errs.clone())
                            .with_drop((collection.token, publication));
                        (log, bundle)
                    })
                    .collect();
                traces
            },
        )
    }

    /// Construct the timely logger that the worker hands to the logging dataflow itself.
    ///
    /// The logging dataflow's operators log nothing unless `log_logging` is set, but the worker
    /// logs every scheduling of the dataflow as a whole to this logger, which observes the
    /// duration of each scheduling. With `log_logging` set, returns the worker's timely logger
    /// instead and observes nothing.
    fn step_logger(&self) -> Option<TimelyLogger> {
        if let Some(logger) = self.worker.logging() {
            // Forwarding events from a second logger would re-timestamp them at flush time, and
            // `mz_scheduling_elapsed` already covers the logging dataflow in this mode.
            return Some(logger);
        }

        // `dataflow_core` allocates the dataflow's identifier first.
        let dataflow_id = self.worker.peek_identifier();
        let step_duration_seconds = self.metrics.step_duration_seconds.clone();
        let mut started = None;
        let logger = Logger::<TimelyEventBuilder>::new(
            self.now,
            self.start_offset,
            move |_time, data: &mut Option<Vec<(Duration, TimelyEvent)>>| {
                let Some(data) = data else { return };
                for (time, event) in data.drain(..) {
                    if let TimelyEvent::Schedule(schedule) = event
                        && schedule.id == dataflow_id
                    {
                        match schedule.start_stop {
                            StartStop::Start => started = Some(time),
                            StartStop::Stop => {
                                if let Some(start) = started.take() {
                                    let elapsed = time.saturating_sub(start);
                                    step_duration_seconds.observe(elapsed.as_secs_f64());
                                }
                            }
                        }
                    }
                }
            },
        );
        // The worker flushes only registered loggers at the end of each step. Unregistered,
        // observations would wait for the logger's buffer to fill.
        let mut register = self.worker.log_register().expect("Logging must be enabled");
        register.insert_logger("materialize/logging-step", logger.clone());
        Some(logger.into())
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
        let t_logger = self.simple_logger::<TimelyEventBuilder>(
            self.t_event_queue.clone(),
            self.metrics.timely_records_total.clone(),
        );
        let d_logger = self.simple_logger::<DifferentialEventBuilder>(
            self.d_event_queue.clone(),
            self.metrics.differential_records_total.clone(),
        );
        let c_logger = self.simple_logger::<ComputeEventBuilder>(
            self.c_event_queue.clone(),
            self.metrics.compute_records_total.clone(),
        );

        let mut register = self.worker.log_register().expect("Logging must be enabled");
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
        records_total: IntCounter,
    ) -> Logger<CB> {
        let [link] = event_queue.links;
        let mut logger = BatchLogger::new(link, self.interval_ms, records_total);
        let activator = event_queue.activator.clone();
        Logger::new(
            self.now,
            self.start_offset,
            move |time, data: &mut Option<CB::Container>| {
                if let Some(data) = data.take() {
                    logger.publish_batch(data);
                    // Count every batch towards the replay's activation threshold, so the logging
                    // dataflow drains events in bounded chunks. Without this, the replay only
                    // wakes once per logging interval and processes the whole interval's events
                    // in one uninterruptible call, stalling every other dataflow on the worker.
                    // The activator is worker-local and never unparks the thread: a threshold
                    // crossed while flushing before a park takes effect on the next wakeup.
                    activator.activate();
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
        let mut logger = BatchLogger::new(
            link,
            self.interval_ms,
            self.metrics.reachability_records_total.clone(),
        );
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
                        // See `simple_logger`.
                        activator.activate();
                    }
                }
            } else {
                // Handle a flush
                while let Some(container) = builder.finish() {
                    logger.publish_batch(std::mem::take(container));
                    // See `simple_logger`.
                    activator.activate();
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

