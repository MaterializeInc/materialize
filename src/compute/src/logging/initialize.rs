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
use timely::ContainerBuilder;
use timely::container::{ContainerBuilder as _, PushInto};
use timely::dataflow::operators::InspectCore;
use timely::logging::{TimelyEvent, TimelyEventBuilder};
use timely::logging_core::{Logger, Registry};
use timely::order::Product;
use timely::progress::reachability::logging::{TrackerEvent, TrackerEventBuilder};

use crate::arrangement::manager::TraceBundle;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::logging::compute::{ComputeEvent, ComputeEventBuilder};
use crate::logging::{BatchLogger, EventQueue, SharedLoggingState};
use crate::render::errors::DataflowErrorSer;
use crate::server::ComputeRuntimeRole;
use crate::shared_trace::PublishArrangement;
use crate::sharing::ArrangementSharingRegistry;
use crate::typedefs::{ErrAgent, ErrBatcher, ErrBuilder, RowRowAgent};

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
    storage_log_reader: Option<crate::server::StorageTimelyLogReader>,
    role: ComputeRuntimeRole,
    sharing_registry: ArrangementSharingRegistry,
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
        storage_log_reader,
        role,
        sharing_registry,
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
    /// Optional reader for storage timely logging events.
    storage_log_reader: Option<crate::server::StorageTimelyLogReader>,
    /// This runtime's role. Only `Maintenance` publishes its logging indexes into the sharing
    /// registry.
    role: ComputeRuntimeRole,
    /// The per-process registry maintenance publishes its logging indexes into.
    sharing_registry: ArrangementSharingRegistry,
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
                self.storage_log_reader.take(),
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
                    // Publish maintenance's logging index into the sharing registry so the
                    // interactive runtime serves introspection peeks from it. Gated on the
                    // Maintenance role inside the helper, so this is a no-op (adds no operators) on
                    // Interactive and Solo.
                    if let Some(&id) = self.config.index_logs.get(&log) {
                        publish_logging_index(
                            self.role,
                            &self.sharing_registry,
                            &scope,
                            id,
                            &collection.trace,
                            &errs,
                        );
                    }
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

/// Publishes a maintenance logging index's `oks`/`errs` arrangements into the sharing registry so
/// the interactive runtime serves introspection peeks from them.
///
/// Gated strictly on the `Maintenance` role. Interactive must not publish: it reads maintenance's
/// slot, and its own (empty) copy would clobber it. Solo has no registry peer. The gate is
/// deliberately NOT `should_publish_index`, whose dyncfg/`publishes_unconditionally` path also
/// admits Interactive.
///
/// The arrangements are re-imported from their trace handles into `scope`. The original arrange
/// streams are consumed inside the per-log construction regions, so only the trace handles survive
/// here, and `Arranged::publish` needs a live arrangement stream on this scope to attach its
/// publisher operator.
fn publish_logging_index(
    role: ComputeRuntimeRole,
    registry: &ArrangementSharingRegistry,
    scope: &timely::dataflow::Scope<'_, Timestamp>,
    id: GlobalId,
    oks_trace: &RowRowAgent<Timestamp, Diff>,
    errs_trace: &ErrAgent<Timestamp, Diff>,
) {
    if role != ComputeRuntimeRole::Maintenance {
        return;
    }

    // Re-import the trace handles to obtain live arrangement streams `publish` can attach a
    // publisher operator to. The publisher refreshes its published chain from the trace, the
    // authoritative source, so the re-import replay only drives the publisher's wakeups.
    let mut oks = oks_trace
        .clone()
        .import_named(scope.clone(), &format!("PublishLog({id})"));
    let mut errs = errs_trace
        .clone()
        .import_named(scope.clone(), &format!("PublishLogErr({id})"));

    // Seal signal for interactive peeks, mirroring `render::export_index`. On each frontier advance
    // of either stream, wake the interactive worker of the same ordinal via `note_frontier` so an
    // import waiting on the seal at that as_of is re-examined. Both streams must signal: an
    // introspection read whose result is an error (for example a division-by-zero surfacing in
    // `mz_compute_error_counts_raw_unified`) carries its data on the errs stream, and without an
    // errs tap the interactive import is never re-woken once the errs frontier advances.
    let worker_index = scope.index();
    let oks_registry = registry.clone();
    oks.stream = oks.stream.inspect_container(move |event| {
        if event.is_err() {
            oks_registry.note_frontier(id, worker_index);
        }
    });
    let errs_registry = registry.clone();
    errs.stream = errs.stream.inspect_container(move |event| {
        if event.is_err() {
            errs_registry.note_frontier(id, worker_index);
        }
    });

    // Adopt the registry's placeholder slot for `id` rather than publishing fresh and inserting:
    // whichever side (this maintenance publish, or an interactive import ahead of it) touches `id`
    // first creates the slot, so this fills it in place instead of overwriting a placeholder a reader
    // has already imported. `get_or_create_placeholder` does not notify on create, so notify
    // explicitly once both halves are adopted, mirroring the notification `insert` used to fire.
    let slot = registry.get_or_create_placeholder(id, worker_index, scope.peers());
    PublishArrangement::adopt(&oks, &slot.oks);
    PublishArrangement::adopt(&errs, &slot.errs);
    registry.notify(id, worker_index);
}

#[cfg(test)]
mod tests {
    use differential_dataflow::input::Input;
    use mz_repr::{Diff, GlobalId, Row, Timestamp};
    use mz_row_spine::{RowRowBatcher, RowRowBuilder};
    use mz_timely_util::columnation::ColumnationChunker;

    use crate::extensions::arrange::{KeyCollection, MzArrange};
    use crate::render::errors::DataflowErrorSer;
    use crate::server::ComputeRuntimeRole;
    use crate::sharing::ArrangementSharingRegistry;
    use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine, RowRowSpine};

    use super::publish_logging_index;

    /// A logging/introspection index is a `RowRow` `oks` arrangement plus an (empty) `errs`
    /// arrangement, published into the sharing registry only by the maintenance runtime. Interactive
    /// and Solo must not publish: interactive reads maintenance's slot rather than clobbering it with
    /// its own empty copy, and Solo has no registry peer.
    ///
    /// Builds real `RowRow`/`Err` arrangements (the exact types the logging path produces) and drives
    /// [`publish_logging_index`] for each role, asserting only maintenance ends up published.
    #[mz_ore::test]
    fn maintenance_publishes_logging_index_others_do_not() {
        for (role, expect_published) in [
            (ComputeRuntimeRole::Maintenance, true),
            (ComputeRuntimeRole::Interactive, false),
            (ComputeRuntimeRole::Solo, false),
        ] {
            let id = GlobalId::System(1);
            let registry = ArrangementSharingRegistry::new();
            let registry_in = registry.clone();

            timely::execute_directly(move |worker| {
                worker.dataflow::<Timestamp, _, _>(|scope| {
                    let (mut oks_input, oks_collection) =
                        scope.new_collection::<(Row, Row), Diff>();
                    let oks = oks_collection.mz_arrange::<
                        ColumnationChunker<_>,
                        RowRowBatcher<_, _>,
                        RowRowBuilder<_, _>,
                        RowRowSpine<_, _>,
                    >("test log oks");

                    let (mut errs_input, errs_collection) =
                        scope.new_collection::<DataflowErrorSer, Diff>();
                    let errs = KeyCollection::from(errs_collection).mz_arrange::<
                        ColumnationChunker<_>,
                        ErrBatcher<_, _>,
                        ErrBuilder<_, _>,
                        ErrSpine<_, _>,
                    >("test log errs");

                    publish_logging_index(
                        role,
                        &registry_in,
                        &scope.clone(),
                        id,
                        &oks.trace,
                        &errs.trace,
                    );

                    oks_input.advance_to(Timestamp::from(1_u64));
                    oks_input.flush();
                    errs_input.advance_to(Timestamp::from(1_u64));
                    errs_input.flush();
                });
            });

            assert_eq!(
                registry.handles(&id, 0).is_some(),
                expect_published,
                "role {role:?} publication mismatch"
            );
        }
    }
}
