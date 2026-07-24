// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders a plan into a timely/differential dataflow computation.
//!
//! ## Error handling
//!
//! Timely and differential have no idioms for computations that can error. The
//! philosophy is, reasonably, to define the semantics of the computation such
//! that errors are unnecessary: e.g., by using wrap-around semantics for
//! integer overflow.
//!
//! Unfortunately, SQL semantics are not nearly so elegant, and require errors
//! in myriad cases. The classic example is a division by zero, but invalid
//! input for casts, overflowing integer operations, and dozens of other
//! functions need the ability to produce errors ar runtime.
//!
//! At the moment, only *scalar* expression evaluation can fail, so only
//! operators that evaluate scalar expressions can fail. At the time of writing,
//! that includes map, filter, reduce, and join operators. Constants are a bit
//! of a special case: they can be either a constant vector of rows *or* a
//! constant, singular error.
//!
//! The approach taken is to build two parallel trees of computation: one for
//! the rows that have been successfully evaluated (the "oks tree"), and one for
//! the errors that have been generated (the "errs tree"). For example:
//!
//! ```text
//!    oks1  errs1       oks2  errs2
//!      |     |           |     |
//!      |     |           |     |
//!   project  |           |     |
//!      |     |           |     |
//!      |     |           |     |
//!     map    |           |     |
//!      |\    |           |     |
//!      | \   |           |     |
//!      |  \  |           |     |
//!      |   \ |           |     |
//!      |    \|           |     |
//!   project  +           +     +
//!      |     |          /     /
//!      |     |         /     /
//!    join ------------+     /
//!      |     |             /
//!      |     | +----------+
//!      |     |/
//!     oks   errs
//! ```
//!
//! The project operation cannot fail, so errors from errs1 are propagated
//! directly. Map operators are fallible and so can inject additional errors
//! into the stream. Join operators combine the errors from each of their
//! inputs.
//!
//! The semantics of the error stream are minimal. From the perspective of SQL,
//! a dataflow is considered to be in an error state if there is at least one
//! element in the final errs collection. The error value returned to the user
//! is selected arbitrarily; SQL only makes provisions to return one error to
//! the user at a time. There are plans to make the err collection accessible to
//! end users, so they can see all errors at once.
//!
//! To make errors transient, simply ensure that the operator can retract any
//! produced errors when corrected data arrives. To make errors permanent, write
//! the operator such that it never retracts the errors it produced. Future work
//! will likely want to introduce some sort of sort order for errors, so that
//! permanent errors are returned to the user ahead of transient errors—probably
//! by introducing a new error type a la:
//!
//! ```no_run
//! # struct EvalError;
//! # struct SourceError;
//! enum DataflowError {
//!     Transient(EvalError),
//!     Permanent(SourceError),
//! }
//! ```
//!
//! If the error stream is empty, the oks stream must be correct. If the error
//! stream is non-empty, then there are no semantics for the oks stream. This is
//! sufficient to support SQL in its current form, but is likely to be
//! unsatisfactory long term. We suspect that we can continue to imbue the oks
//! stream with semantics if we are very careful in describing what data should
//! and should not be produced upon encountering an error. Roughly speaking, the
//! oks stream could represent the correct result of the computation where all
//! rows that caused an error have been pruned from the stream. There are
//! strange and confusing questions here around foreign keys, though: what if
//! the optimizer proves that a particular key must exist in a collection, but
//! the key gets pruned away because its row participated in a scalar expression
//! evaluation that errored?
//!
//! In the meantime, it is probably wise for operators to keep the oks stream
//! roughly "as correct as possible" even when errors are present in the errs
//! stream. This reduces the amount of recomputation that must be performed
//! if/when the errors are retracted.

use std::any::Any;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::task::Poll;

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::lattice::{Lattice, antichain_join};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::ShutdownButton;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::trace::cursor::{BatchCursor, BatchDiff, BatchKey, BatchVal};
use differential_dataflow::trace::{BatchReader, Cursor, Navigable, TraceReader};
use differential_dataflow::{AsCollection, Data, VecCollection};
use futures::FutureExt;
use futures::channel::oneshot;
use itertools::Itertools;
use mz_compute_types::dataflows::{DataflowDescription, IndexDesc};
use mz_compute_types::dyncfgs::{
    COMPUTE_APPLY_COLUMN_DEMANDS, COMPUTE_LOGICAL_BACKPRESSURE_INFLIGHT_SLACK,
    COMPUTE_LOGICAL_BACKPRESSURE_MAX_RETAINED_CAPABILITIES, ENABLE_COMPUTE_LOGICAL_BACKPRESSURE,
    ENABLE_COMPUTE_TEMPORAL_BUCKETING, ENABLE_INDEX_ARRANGEMENT_SHARING,
    SUBSCRIBE_SNAPSHOT_OPTIMIZATION, TEMPORAL_BUCKETING_SUMMARY,
};
use mz_compute_types::plan::render_plan::{
    self, BindStage, LetBind, LetFreePlan, RecBind, RenderPlan,
};
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_compute_types::plan::{ArrangementStrategy, LirId};
use mz_dyncfg::ConfigSet;
use mz_expr::{EvalError, Id, LocalId, permutation_for_arrangement};
use mz_persist_client::operators::shard_source::{ErrorHandler, SnapshotMode};
use mz_repr::explain::DummyHumanizer;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{Datum, DatumVec, Diff, GlobalId, ReprRelationType, Row, RowArena, SharedRow};
use mz_storage_operators::persist_source;
use mz_storage_types::controller::CollectionMetadata;
use mz_timely_util::columnation::ColumnationChunker;
use mz_timely_util::operator::{CollectionExt, StreamExt};
use mz_timely_util::probe::{Handle as MzProbeHandle, ProbeNotify};
use mz_timely_util::scope_label::ScopeExt;
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::vec::ToStream;
use timely::dataflow::operators::vec::{BranchWhen, Filter};
use timely::dataflow::operators::{Capability, InspectCore, Operator, Probe, probe};
use timely::dataflow::{Scope, Stream, StreamVec};
use timely::order::{Product, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::ActivateOnDrop;
use timely::worker::Worker as TimelyWorker;

use crate::arrangement::manager::TraceBundle;
use crate::compute_state::ComputeState;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::extensions::reduce::MzReduce;
use crate::extensions::temporal_bucket::TemporalBucketing;
use crate::logging::compute::{
    ComputeEvent, DataflowGlobal, LirMapping, LirMetadata, LogDataflowErrors, OperatorHydration,
};
use crate::render::columnar::CollectionEdge;
use crate::render::context::{ArrangementFlavor, Context};
use crate::render::errors::DataflowErrorSer;
use crate::server::ComputeRuntimeRole;
use crate::shared_trace::PublishArrangement;
use crate::sharing::{
    ArrangementSharingRegistry, SharedErrsFrontier, SharedErrsHandle, SharedIndexArrangement,
    SharedOksFrontier, SharedOksHandle,
};
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine, KeyBatcher, MzTimestamp};
use mz_row_spine::{DatumSeq, RowRowBatcher, RowRowBuilder};

pub(crate) mod columnar;
pub mod context;
pub(crate) mod errors;
mod flat_map;
mod join;
mod reduce;
pub mod sinks;
mod threshold;
mod top_k;

pub use context::CollectionBundle;
pub use join::LinearJoinSpec;

/// Whether a freshly rendered index should be published into the per-process arrangement-sharing
/// registry.
///
/// Publication happens when the `enable_index_arrangement_sharing` dyncfg is on. It is additionally
/// forced on for a two-runtime process's maintenance and interactive runtimes (see
/// [`ComputeRuntimeRole::publishes_unconditionally`]): maintenance publishes its maintained indexes
/// and interactive publishes its transient query outputs, both read from the registry. Coupling
/// publication to the runtime rather than the dyncfg alone keeps a disabled dyncfg from leaving an
/// interactive read blocking until it times out.
fn should_publish_index(worker_config: &ConfigSet, role: ComputeRuntimeRole) -> bool {
    ENABLE_INDEX_ARRANGEMENT_SHARING.get(worker_config) || role.publishes_unconditionally()
}

/// Guard that presses a differential [`ShutdownButton`] when dropped.
///
/// Dropping this guard releases the imported trace's capabilities.
struct PressOnDrop<T>(ShutdownButton<T>);

impl<T> Drop for PressOnDrop<T> {
    fn drop(&mut self) {
        self.0.press();
    }
}

/// Assemble the "compute"  side of a dataflow, i.e. all but the sources.
///
/// This method imports sources from provided assets, and then builds the remaining
/// dataflow using "compute-local" assets like shared arrangements, and producing
/// both arrangements and sinks.
pub fn build_compute_dataflow(
    timely_worker: &mut TimelyWorker,
    compute_state: &mut ComputeState,
    dataflow: DataflowDescription<RenderPlan, CollectionMetadata>,
    start_signal: StartSignal,
    until: Antichain<mz_repr::Timestamp>,
    dataflow_expiration: Antichain<mz_repr::Timestamp>,
) {
    // Mutually recursive view definitions require special handling.
    let recursive = dataflow
        .objects_to_build
        .iter()
        .any(|object| object.plan.is_recursive());

    // Determine indexes to export, and their dependencies.
    let indexes = dataflow
        .index_exports
        .iter()
        .map(|(idx_id, (idx, _typ))| (*idx_id, dataflow.depends_on(idx.on_id), idx.as_lir()))
        .collect::<Vec<_>>();

    // Determine sinks to export, and their dependencies.
    let sinks = dataflow
        .sink_exports
        .iter()
        .map(|(sink_id, sink)| (*sink_id, dataflow.depends_on(sink.from), sink.clone()))
        .collect::<Vec<_>>();

    let worker_logging = timely_worker.logger_for("timely").map(Into::into);
    let apply_demands = COMPUTE_APPLY_COLUMN_DEMANDS.get(&compute_state.worker_config);
    let subscribe_snapshot_optimization =
        SUBSCRIBE_SNAPSHOT_OPTIMIZATION.get(&compute_state.worker_config);

    let name = format!("Dataflow: {}", &dataflow.debug_name);
    let input_name = format!("InputRegion: {}", &dataflow.debug_name);
    let build_name = format!("BuildRegion: {}", &dataflow.debug_name);

    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        let scope = scope.with_label();

        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        let mut imported_sources = Vec::new();
        let mut tokens: BTreeMap<_, Rc<dyn Any>> = BTreeMap::new();
        let output_probe = MzProbeHandle::default();

        scope.clone().region_named(&input_name, |region| {
            // Import declared sources into the rendering context.
            for (source_id, import) in dataflow.source_imports.iter() {
                region.region_named(&format!("Source({:?})", source_id), |inner| {
                    let mut read_schema = None;
                    let mut mfp = import.desc.arguments.operators.clone().map(|mut ops| {
                        // If enabled, we read from Persist with a `RelationDesc` that
                        // omits uneeded columns.
                        if apply_demands {
                            let demands = ops.demand();
                            let new_desc = import
                                .desc
                                .storage_metadata
                                .relation_desc
                                .apply_demand(&demands);
                            let new_arity = demands.len();
                            let remap: BTreeMap<_, _> = demands
                                .into_iter()
                                .enumerate()
                                .map(|(new, old)| (old, new))
                                .collect();
                            ops.permute_fn(|old_idx| remap[&old_idx], new_arity);
                            read_schema = Some(new_desc);
                        }

                        mz_expr::MfpPlan::create_from(ops)
                            .expect("Linear operators should always be valid")
                    });

                    let snapshot_mode = if import.with_snapshot || !subscribe_snapshot_optimization
                    {
                        SnapshotMode::Include
                    } else {
                        compute_state.metrics.inc_subscribe_snapshot_optimization();
                        SnapshotMode::Exclude
                    };
                    let suppress_early_progress_as_of = dataflow.as_of.clone();

                    // Note: For correctness, we require that sources only emit times advanced by
                    // `dataflow.as_of`. `persist_source` is documented to provide this guarantee.
                    let (mut ok_stream, err_stream, token) =
                        persist_source::persist_source::<DataflowErrorSer>(
                            inner,
                            *source_id,
                            Arc::clone(&compute_state.persist_clients),
                            &compute_state.txns_ctx,
                            import.desc.storage_metadata.clone(),
                            read_schema,
                            dataflow.as_of.clone(),
                            snapshot_mode,
                            until.clone(),
                            mfp.as_mut(),
                            compute_state.dataflow_max_inflight_bytes(),
                            start_signal.clone().into_send_future(),
                            ErrorHandler::Halt("compute_import"),
                        );

                    // If `mfp` is non-identity, we need to apply what remains.
                    // For the moment, assert that it is either trivial or `None`.
                    assert!(mfp.map(|x| x.is_identity()).unwrap_or(true));

                    // To avoid a memory spike during arrangement hydration (database-issues#6368), need to
                    // ensure that the first frontier we report into the dataflow is beyond the
                    // `as_of`.
                    if let Some(as_of) = suppress_early_progress_as_of {
                        ok_stream = suppress_early_progress(ok_stream, as_of);
                    }

                    if ENABLE_COMPUTE_LOGICAL_BACKPRESSURE.get(&compute_state.worker_config) {
                        // Apply logical backpressure to the source.
                        let limit = COMPUTE_LOGICAL_BACKPRESSURE_MAX_RETAINED_CAPABILITIES
                            .get(&compute_state.worker_config);
                        let slack = COMPUTE_LOGICAL_BACKPRESSURE_INFLIGHT_SLACK
                            .get(&compute_state.worker_config)
                            .as_millis()
                            .try_into()
                            .expect("must fit");

                        let stream = ok_stream.limit_progress(
                            output_probe.clone(),
                            slack,
                            limit,
                            import.upper.clone(),
                            name.clone(),
                        );
                        ok_stream = stream;
                    }

                    // Attach a probe reporting the input frontier.
                    let input_probe =
                        compute_state.input_probe_for(*source_id, dataflow.export_ids());
                    ok_stream = ok_stream.probe_with(&input_probe);

                    let (oks, errs) = (
                        ok_stream
                            .as_collection()
                            .leave_region(region)
                            .leave_region(scope),
                        err_stream
                            .as_collection()
                            .leave_region(region)
                            .leave_region(scope),
                    );

                    imported_sources.push((mz_expr::Id::Global(*source_id), (oks, errs)));

                    // Associate returned tokens with the source identifier.
                    tokens.insert(*source_id, Rc::new(token));
                });
            }
        });

        // If there exists a recursive expression, we'll need to use a non-region scope,
        // in order to support additional timestamp coordinates for iteration.
        if recursive {
            scope.clone().iterative::<PointStamp<u64>, _, _>(|region| {
                let mut context = Context::for_dataflow_in(
                    &dataflow,
                    region.clone(),
                    compute_state,
                    until,
                    dataflow_expiration,
                );

                for (id, (oks, errs)) in imported_sources.into_iter() {
                    let bundle = crate::render::CollectionBundle::from_collections(
                        oks.enter(region),
                        errs.enter(region),
                    );
                    // Associate collection bundle with the source identifier.
                    context.insert_id(id, bundle);
                }

                // Import declared indexes into the rendering context.
                for (idx_id, idx) in &dataflow.index_imports {
                    let input_probe = compute_state.input_probe_for(*idx_id, dataflow.export_ids());
                    let snapshot_mode = if idx.with_snapshot || !subscribe_snapshot_optimization {
                        SnapshotMode::Include
                    } else {
                        compute_state.metrics.inc_subscribe_snapshot_optimization();
                        SnapshotMode::Exclude
                    };
                    context.import_index(
                        scope,
                        compute_state,
                        &mut tokens,
                        input_probe,
                        *idx_id,
                        &idx.desc.as_lir(),
                        &idx.typ,
                        snapshot_mode,
                        start_signal.clone(),
                    );
                }

                // Build declared objects.
                for object in dataflow.objects_to_build {
                    let bundle = context.scope.clone().region_named(
                        &format!("BuildingObject({:?})", object.id),
                        |region| {
                            let depends = object.plan.depends();
                            let in_let = object.plan.is_recursive();
                            context
                                .enter_region(region, Some(&depends))
                                .render_recursive_plan(
                                    object.id,
                                    0,
                                    object.plan,
                                    // recursive plans _must_ have bodies in a let
                                    BindingInfo::Body { in_let },
                                )
                                .leave_region(context.scope)
                        },
                    );
                    let global_id = object.id;

                    context.log_dataflow_global_id(
                        *bundle
                            .scope()
                            .addr()
                            .first()
                            .expect("Dataflow root id must exist"),
                        global_id,
                    );
                    context.insert_id(Id::Global(object.id), bundle);
                }

                // Export declared indexes.
                for (idx_id, dependencies, idx) in indexes {
                    context.export_index_iterative(
                        scope,
                        compute_state,
                        &tokens,
                        dependencies,
                        idx_id,
                        &idx,
                        &output_probe,
                    );
                }

                // Export declared sinks.
                for (sink_id, dependencies, sink) in sinks {
                    context.export_sink(
                        compute_state,
                        &tokens,
                        dependencies,
                        sink_id,
                        &sink,
                        start_signal.clone(),
                        &output_probe,
                        scope,
                    );
                }
            });
        } else {
            scope.clone().region_named(&build_name, |region| {
                let mut context = Context::for_dataflow_in(
                    &dataflow,
                    region.clone(),
                    compute_state,
                    until,
                    dataflow_expiration,
                );

                for (id, (oks, errs)) in imported_sources.into_iter() {
                    let bundle = crate::render::CollectionBundle::from_collections(
                        oks.enter_region(region),
                        errs.enter_region(region),
                    );
                    // Associate collection bundle with the source identifier.
                    context.insert_id(id, bundle);
                }

                // Import declared indexes into the rendering context.
                for (idx_id, idx) in &dataflow.index_imports {
                    let input_probe = compute_state.input_probe_for(*idx_id, dataflow.export_ids());
                    let snapshot_mode = if idx.with_snapshot || !subscribe_snapshot_optimization {
                        SnapshotMode::Include
                    } else {
                        compute_state.metrics.inc_subscribe_snapshot_optimization();
                        SnapshotMode::Exclude
                    };
                    context.import_index(
                        scope,
                        compute_state,
                        &mut tokens,
                        input_probe,
                        *idx_id,
                        &idx.desc.as_lir(),
                        &idx.typ,
                        snapshot_mode,
                        start_signal.clone(),
                    );
                }

                // Build declared objects.
                for object in dataflow.objects_to_build {
                    let bundle = context.scope.clone().region_named(
                        &format!("BuildingObject({:?})", object.id),
                        |region| {
                            let depends = object.plan.depends();
                            context
                                .enter_region(region, Some(&depends))
                                .render_plan(object.id, object.plan)
                                .leave_region(context.scope)
                        },
                    );
                    let global_id = object.id;
                    context.log_dataflow_global_id(
                        *bundle
                            .scope()
                            .addr()
                            .first()
                            .expect("Dataflow root id must exist"),
                        global_id,
                    );
                    context.insert_id(Id::Global(object.id), bundle);
                }

                // Export declared indexes.
                for (idx_id, dependencies, idx) in indexes {
                    context.export_index(
                        compute_state,
                        &tokens,
                        dependencies,
                        idx_id,
                        &idx,
                        &output_probe,
                    );
                }

                // Export declared sinks.
                for (sink_id, dependencies, sink) in sinks {
                    context.export_sink(
                        compute_state,
                        &tokens,
                        dependencies,
                        sink_id,
                        &sink,
                        start_signal.clone(),
                        &output_probe,
                        scope,
                    );
                }
            });
        }
    });
}

/// Imports the maintenance-published `oks`/`errs` arrangements for `idx_id` from `registry` into
/// `outer` as a static snapshot at `as_of`, bounded by `until`, via
/// `SharedTraceHandle::import_snapshot_at`.
///
/// Binds through [`ArrangementSharingRegistry::get_or_create_placeholder`], which always returns a
/// slot: an existing published arrangement, or a freshly created placeholder if the dependency is not
/// yet published on `worker_index`. An import over an unadopted placeholder produces no data and holds
/// its output frontier at the minimum until a maintenance publisher adopts the same slot. This is what
/// lets `handle_create_dataflow` build every interactive dataflow immediately in command arrival order
/// rather than deferring a build whose dependency is not yet published.
///
/// Returns the slot `Arc<SharedIndexArrangement>` alongside the arrangements and read holds. The
/// caller MUST retain it for as long as the import is alive:
/// [`ArrangementSharingRegistry::evict_unadopted`] detects the last reader of an unadopted placeholder
/// by the slot Arc's strong count, and a `SharedTraceHandle` holds only the inner `Arc<SharedTrace>`
/// one level down, which does not count. Dropping the slot Arc while the import is still live would let
/// eviction remove a slot this reader depends on.
///
/// The returned handle clones are the importing dataflow's read hold on the shared trace, advanced
/// to `as_of`. Keeping them alive (for example in the dataflow's token set) pins the shared trace at
/// `as_of`; dropping them releases the hold, after which maintenance may compact past `as_of`.
///
/// Cleanup of the imported maintenance index is not this runtime's responsibility, and is not
/// coordinated across runtimes. It rests on the controller's read-hold discipline: the controller
/// holds a read hold on the imported id for every reader (a peek, or a dataflow importing it) and
/// releases that hold only once the reader retires, at which point it may issue the index's
/// `AllowCompaction` (see `mz_compute_client::controller::instance::Instance::finish_peek`). The
/// interactive runtime releases these handle holds and reports its empty frontier when its own
/// dataflow is dropped, which reaches the controller before it frees the index. So the imported
/// index is never compacted or dropped while this import still reads it, without any cross-runtime
/// signal. A controller that violated this discipline would be an incorrect protocol instantiation,
/// which the runtime panics on rather than defends against.
///
/// Panics if the slot's `since` is already beyond `as_of` at import time: the controller offered
/// an `as_of` the shared trace can no longer read accurately, a protocol error.
///
/// The import replays the published chain and tracks the shared trace's frontier, presenting the
/// accumulation at `as_of` bounded by `until`. Its output capability drops once the trace seals past
/// `until`, so a single-time interactive read completes. The returned `stream` and `trace` stay
/// consistent (the trace never runs ahead of the stream), which a differential join over the import
/// requires. See [`crate::shared_trace::SharedTraceHandle::import_snapshot_at`].
fn import_shared_index<'outer>(
    outer: Scope<'outer, mz_repr::Timestamp>,
    registry: &ArrangementSharingRegistry,
    idx_id: GlobalId,
    name: &str,
    as_of: &Antichain<mz_repr::Timestamp>,
    until: &Antichain<mz_repr::Timestamp>,
) -> (
    Arranged<'outer, SharedOksFrontier>,
    Arranged<'outer, SharedErrsFrontier>,
    SharedOksHandle,
    SharedErrsHandle,
    Arc<SharedIndexArrangement>,
) {
    // Pairwise import reads publisher worker `i` from importer worker `i`, so worker indices must
    // line up. The primitive's import additionally asserts equal total peer counts.
    let worker_index = outer.index();
    let peers = outer.peers();
    // Bind through get-or-create so a not-yet-published dependency yields a placeholder rather than
    // failing. The returned slot Arc is handed back to the caller, which retains it (see the doc
    // comment): eviction counts live readers by this Arc's strong count, and the handles minted below
    // wrap only the inner `Arc<SharedTrace>`, which does not contribute to that count.
    let slot = registry.get_or_create_placeholder(idx_id, worker_index, peers);
    let oks_handle = slot.oks.handle();
    let errs_handle = slot.errs.handle();

    // Cloning a `SharedTraceHandle` registers an independent hold. Advance these holds to `as_of` so
    // the shared trace does not compact past `as_of` before the snapshot is read, then keep them as
    // the caller's read-hold tokens. The hold releases only when the caller drops them.
    let mut oks_hold = oks_handle.clone();
    let mut errs_hold = errs_handle.clone();

    // A published slot's `since` may already sit above `as_of` if the controller offered an
    // unreadable `as_of`, a protocol error. Check before advancing the holds below: doing so
    // first would pull `since` up to `as_of` and make this assert trivially true. A fresh
    // placeholder's `since` is `minimum`, so this holds vacuously for an unadopted slot. Mirrors
    // the maintenance path's `TraceBundle::compaction_frontier` assert in `import_index`, joining
    // the two handles' compaction frontiers the same way `shared_index_peek_response` does.
    let since = antichain_join(
        &oks_hold.get_logical_compaction(),
        &errs_hold.get_logical_compaction(),
    );
    assert!(
        PartialOrder::less_equal(&since, as_of),
        "Index {idx_id} has been allowed to compact beyond the dataflow as_of: \
         since {:?}, as_of {:?}",
        since.elements(),
        as_of.elements(),
    );

    oks_hold.set_logical_compaction(as_of.borrow());
    oks_hold.set_physical_compaction(as_of.borrow());
    errs_hold.set_logical_compaction(as_of.borrow());
    errs_hold.set_physical_compaction(as_of.borrow());

    // Import a static snapshot at `as_of`, bounded by `until`. Interactive work is single-time, so a
    // snapshot is exactly what is needed, and it avoids the live import's forward-batch replay
    // (whose capability handling and lack of `as_of` coalescing are unsound for a read at `as_of`).
    let oks_arranged = oks_handle.import_snapshot_at(
        outer.clone(),
        &format!("Shared{name}"),
        as_of.clone(),
        until.clone(),
    );
    let errs_arranged = errs_handle.import_snapshot_at(
        outer,
        &format!("SharedErr{name}"),
        as_of.clone(),
        until.clone(),
    );

    (oks_arranged, errs_arranged, oks_hold, errs_hold, slot)
}

// This implementation block allows child timestamps to vary from parent timestamps,
// but requires the parent timestamp to be `repr::Timestamp`.
impl<'g, T> Context<'g, T>
where
    T: Refines<mz_repr::Timestamp> + RenderTimestamp,
{
    /// Import the collection from the arrangement, discarding batches from the snapshot.
    /// (This does not guarantee that no records from the snapshot are included; the assumption is
    /// that we'll filter those out later if necessary.)
    fn import_filtered_index_collection<
        'outer,
        Tr: TraceReader<Time = mz_repr::Timestamp, Batch: Navigable> + Clone,
        V: Data,
    >(
        &self,
        arranged: Arranged<'outer, Tr>,
        start_signal: StartSignal,
        mut logic: impl FnMut(BatchKey<'_, Tr>, BatchVal<'_, Tr>) -> V + 'static,
    ) -> VecCollection<'g, T, V, BatchDiff<Tr>>
    where
        // This is implied by the fact that the outer timestamp = mz_repr::Timestamp, but it's essential
        // for our batch-level filtering to be safe, so we document it here regardless.
        mz_repr::Timestamp: TotalOrder,
        BatchCursor<Tr>: Cursor<Time = mz_repr::Timestamp>,
    {
        let oks = arranged.stream.with_start_signal(start_signal).filter({
            let as_of = self.as_of_frontier.clone();
            move |b| !<Antichain<mz_repr::Timestamp> as PartialOrder>::less_equal(b.upper(), &as_of)
        });
        Arranged::<'outer, Tr>::flat_map_batches(oks, move |a, b| [logic(a, b)]).enter(self.scope)
    }

    pub(crate) fn import_index<'outer>(
        &mut self,
        outer: Scope<'outer, mz_repr::Timestamp>,
        compute_state: &mut ComputeState,
        tokens: &mut BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        input_probe: probe::Handle<mz_repr::Timestamp>,
        idx_id: GlobalId,
        idx: &IndexDesc<LirScalarExpr>,
        typ: &ReprRelationType,
        snapshot_mode: SnapshotMode,
        start_signal: StartSignal,
    ) {
        // The interactive runtime maintains no traces of its own: the maintenance runtime publishes
        // them into the per-process sharing registry. Source the import from there via change-stream
        // replay. The maintenance runtime keeps using the local `TraceManager` below, unchanged.
        if compute_state.role() == ComputeRuntimeRole::Interactive {
            self.import_index_shared(
                outer,
                compute_state,
                tokens,
                input_probe,
                idx_id,
                idx,
                typ,
                start_signal,
            );
            return;
        }

        if let Some(traces) = compute_state.traces.get_mut(&idx_id) {
            assert!(
                PartialOrder::less_equal(&traces.compaction_frontier(), &self.as_of_frontier),
                "Index {idx_id} has been allowed to compact beyond the dataflow as_of"
            );

            let token = traces.to_drop().clone();

            let (mut oks, ok_button) = traces.oks_mut().import_frontier_core(
                outer,
                &format!("Index({}, {:?})", idx.on_id, idx.key),
                self.as_of_frontier.clone(),
                self.until.clone(),
            );

            oks.stream = oks.stream.probe_with(&input_probe);

            let (err_arranged, err_button) = traces.errs_mut().import_frontier_core(
                outer,
                &format!("ErrIndex({}, {:?})", idx.on_id, idx.key),
                self.as_of_frontier.clone(),
                self.until.clone(),
            );

            let bundle = match snapshot_mode {
                SnapshotMode::Include => {
                    let ok_arranged = oks
                        .enter(self.scope)
                        .with_start_signal(start_signal.clone());
                    let err_arranged = err_arranged
                        .enter(self.scope)
                        .with_start_signal(start_signal);
                    CollectionBundle::from_expressions(
                        idx.key.clone(),
                        ArrangementFlavor::Trace(idx_id, ok_arranged, err_arranged),
                    )
                }
                SnapshotMode::Exclude => {
                    // When we import an index without a snapshot, we have two balancing considerations:
                    // - It's easy to filter out irrelevant batches from the stream, but hard to filter them out from an arrangement.
                    //   (The `TraceFrontier` wrapper allows us to set an "until" frontier, but not a lower.)
                    // - We do not actually need to reference the arrangement in this dataflow, since all operators that use the arrangement
                    //   (joins, reduces, etc.) also require the snapshot data.
                    // So: when the snapshot is excluded, we import only the (filtered) collection itself and ignore the arrangement.
                    let oks = {
                        let mut datums = DatumVec::new();
                        let (permutation, _thinning) =
                            permutation_for_arrangement(&idx.key, typ.arity());
                        self.import_filtered_index_collection(
                            oks,
                            start_signal.clone(),
                            move |k: DatumSeq, v: DatumSeq| {
                                let temp_storage = RowArena::new();
                                let mut datums_borrow = datums.borrow();
                                k.extend_datums(&temp_storage, &mut datums_borrow, None);
                                v.extend_datums(&temp_storage, &mut datums_borrow, None);
                                SharedRow::pack(permutation.iter().map(|i| datums_borrow[*i]))
                            },
                        )
                    };
                    let errs = self.import_filtered_index_collection(
                        err_arranged,
                        start_signal,
                        |e, _| e.clone(),
                    );
                    CollectionBundle::from_collections(oks, errs)
                }
            };
            self.update_id(Id::Global(idx.on_id), bundle);
            tokens.insert(
                idx_id,
                Rc::new((PressOnDrop(ok_button), PressOnDrop(err_button), token)),
            );
        } else {
            panic!(
                "import of index {} failed while building dataflow {}",
                idx_id, self.dataflow_id
            );
        }
    }

    /// The interactive-runtime counterpart to the maintenance [`Self::import_index`] path.
    ///
    /// Imports the maintenance-published index *as an arrangement* via
    /// [`ArrangementFlavor::SharedTrace`], so a downstream `Get` of `idx.on_id` receives the shared
    /// arrangement the LIR plan expects, with its key and permutation intact, and joins/reduces
    /// consume it as an arrangement rather than re-deriving it from a collection.
    ///
    /// Sources the imported `oks`/`errs` from the per-process sharing registry via
    /// `SharedTraceHandle::import_snapshot_at` instead of the local `TraceManager`, which holds
    /// nothing on the interactive runtime. See [`import_shared_index`] for the read-hold semantics.
    ///
    /// The imported `Arranged`s are backed by `TraceFrontier<SharedTraceHandle>`. They share the
    /// `RowRow`/`Err` batch and cursor types of the maintenance `TraceAgent`, so the shared
    /// arrangement drives the same generic consumer bodies the [`ArrangementFlavor::Trace`] variant
    /// does.
    ///
    /// The import is a static snapshot at `as_of`, coalesced to `as_of` and bounded by `until`, the
    /// same `TraceFrontier` semantics as the maintenance `import_frontier_core(as_of, until)` path.
    /// Interactive work is single-time, so a snapshot is exactly what a one-shot peek or its query
    /// dataflow needs. A future migration of multi-time SUBSCRIBE onto the interactive runtime would
    /// need the live change stream instead, and must not reuse this snapshot path.
    fn import_index_shared<'outer>(
        &mut self,
        outer: Scope<'outer, mz_repr::Timestamp>,
        compute_state: &ComputeState,
        tokens: &mut BTreeMap<GlobalId, Rc<dyn Any>>,
        input_probe: probe::Handle<mz_repr::Timestamp>,
        idx_id: GlobalId,
        idx: &IndexDesc<LirScalarExpr>,
        _typ: &ReprRelationType,
        start_signal: StartSignal,
    ) {
        let name = format!("Index({}, {:?})", idx.on_id, idx.key);
        // Bound the snapshot to the single read time `as_of`. Interactive work is single-time, so the
        // import's capability must drop once the shared trace seals past `as_of`, letting the one-shot
        // result complete. `self.until` may be empty (unbounded) for a long-lived dependency, which a
        // live `upper` never reaches, so it cannot serve as the snapshot bound.
        //
        // `try_step_forward` yields the frontier strictly greater than `as_of`. For an `as_of` at
        // `Timestamp::MAX` there is no such finite time, so the element drops out and the bound is the
        // empty (end-of-time) frontier, matching the semantics of "read the final state".
        let snapshot_until = Antichain::from_iter(
            self.as_of_frontier
                .iter()
                .filter_map(|t| t.try_step_forward()),
        );
        let (mut oks_arranged, errs_arranged, oks_hold, errs_hold, slot) = import_shared_index(
            outer,
            &compute_state.sharing_registry,
            idx_id,
            &name,
            &self.as_of_frontier,
            &snapshot_until,
        );

        // Attach the input probe to the replayed batch stream so hydration tracking observes it,
        // mirroring the maintenance import.
        oks_arranged.stream = oks_arranged.stream.probe_with(&input_probe);

        // Enter the dataflow scope and gate on the start signal, mirroring the maintenance Trace
        // import's `.enter(self.scope).with_start_signal(..)`. The shared handle shares the
        // maintenance arrangement's batch/cursor types, so the entered `Arranged` is a real
        // arrangement `ArrangementFlavor::SharedTrace` can carry and downstream operators consume.
        let ok_arranged = oks_arranged
            .enter(self.scope)
            .with_start_signal(start_signal.clone());
        let err_arranged = errs_arranged
            .enter(self.scope)
            .with_start_signal(start_signal);

        let bundle = CollectionBundle::from_expressions(
            idx.key.clone(),
            ArrangementFlavor::SharedTrace(idx_id, ok_arranged, err_arranged),
        );
        self.update_id(Id::Global(idx.on_id), bundle);

        // The read hold releases when this token drops with the dataflow. The slot Arc rides along so
        // the import counts as a live reader for `evict_unadopted` for the dataflow's whole lifetime.
        tokens.insert(idx_id, Rc::new((oks_hold, errs_hold, slot)));
    }
}

// This implementation block requires the scopes have the same timestamp as the trace manager.
// That makes some sense, because we are hoping to deposit an arrangement in the trace manager.
impl<'g> Context<'g, mz_repr::Timestamp> {
    pub(crate) fn export_index(
        &self,
        compute_state: &mut ComputeState,
        tokens: &BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        dependency_ids: BTreeSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc<LirScalarExpr>,
        output_probe: &MzProbeHandle<mz_repr::Timestamp>,
    ) {
        // put together tokens that belong to the export
        let mut needed_tokens = Vec::new();
        for dep_id in dependency_ids {
            if let Some(token) = tokens.get(&dep_id) {
                needed_tokens.push(Rc::clone(token));
            }
        }
        let bundle = self.lookup_id(Id::Global(idx_id)).unwrap_or_else(|| {
            panic!(
                "Arrangement alarmingly absent! id: {:?}",
                Id::Global(idx_id)
            )
        });

        let key = &idx.key;
        match bundle.arrangement(key) {
            Some(ArrangementFlavor::Local(mut oks, mut errs)) => {
                // Ensure that the frontier does not advance past the expiration time, if set.
                // Otherwise, we might write down incorrect data.
                if let Some(&expiration) = self.dataflow_expiration.as_option() {
                    oks.stream = oks.stream.expire_stream_at(
                        &format!("{}_export_index_oks", self.debug_name),
                        expiration,
                    );
                    errs.stream = errs.stream.expire_stream_at(
                        &format!("{}_export_index_errs", self.debug_name),
                        expiration,
                    );
                }

                oks.stream = oks.stream.probe_notify_with(vec![output_probe.clone()]);

                // Attach logging of dataflow errors.
                if let Some(logger) = compute_state.compute_logger.clone() {
                    errs.stream = errs.stream.log_dataflow_errors(logger, idx_id);
                }

                // Publish the arrangement into the per-process sharing registry when the gate opens
                // (see `should_publish_index`). `publish` borrows the arrangements, so it must
                // happen before their traces are moved into the `TraceBundle` below. Publishing pins
                // no compaction floor of its own, so the local trace behavior is unchanged.
                if should_publish_index(&compute_state.worker_config, compute_state.role()) {
                    // Seal signal for interactive fast-path peeks. `insert` fires the interactive
                    // worker's waker on publication, but a later frontier advance with no new
                    // publication must also re-examine a peek waiting only on the seal. Tap the
                    // output progress streams: on each advance, mark the id dirty and wake the
                    // interactive worker of the same ordinal via `note_frontier`.
                    //
                    // Both the oks and errs streams must signal. A peek whose result is an error
                    // (for example a runtime division-by-zero or a `WITH MUTUALLY RECURSIVE ...
                    // ERROR AT RECURSION LIMIT` that trips its limit) carries its data on the errs
                    // stream, whose frontier is held back until the error is emitted. Without an
                    // errs tap the interactive import at that as_of is never re-woken once the errs
                    // frontier finally advances, and the peek hangs. The oks stream is trivially
                    // sealed for such a peek, and vice versa for a normal result, so tapping only
                    // one stream leaves the other class of peek stuck.
                    let worker_index = self.scope.index();
                    let oks_registry = compute_state.sharing_registry.clone();
                    oks.stream = oks.stream.inspect_container(move |event| {
                        if event.is_err() {
                            oks_registry.note_frontier(idx_id, worker_index);
                        }
                    });
                    let errs_registry = compute_state.sharing_registry.clone();
                    errs.stream = errs.stream.inspect_container(move |event| {
                        if event.is_err() {
                            errs_registry.note_frontier(idx_id, worker_index);
                        }
                    });

                    // Adopt the registry's placeholder slot for `idx_id` rather than publishing
                    // fresh and inserting: whichever side (this maintenance render, or an
                    // interactive import ahead of it) touches `idx_id` first creates the slot, so
                    // this fills it in place instead of risking an overwrite of a placeholder a
                    // reader has already imported. `get_or_create_placeholder` does not notify on
                    // create, so notify explicitly once both halves are adopted, mirroring the
                    // notification `insert` used to fire for a freshly published slot.
                    let slot = compute_state.sharing_registry.get_or_create_placeholder(
                        idx_id,
                        worker_index,
                        self.scope.peers(),
                    );
                    PublishArrangement::adopt(&oks, &slot.oks);
                    PublishArrangement::adopt(&errs, &slot.errs);
                    compute_state.sharing_registry.notify(idx_id, worker_index);
                }

                compute_state.traces.set(
                    idx_id,
                    TraceBundle::new(oks.trace, errs.trace).with_drop(needed_tokens),
                );
            }
            Some(ArrangementFlavor::Trace(gid, _, _)) => {
                // Duplicate of existing arrangement with id `gid`, so
                // just create another handle to that arrangement.
                let trace = compute_state.traces.get(&gid).unwrap().clone();
                compute_state.traces.set(idx_id, trace);

                // Mirror the trace aliasing in the sharing registry: re-register the arrangement
                // already published under `gid` on this worker under `idx_id` as well. This arm
                // builds no streams of its own, so it installs no seal-signal tap like the `Local`
                // arm above. `reexport` records `idx_id` as an alias of `gid` so `gid`'s tap wakes
                // interactive peeks waiting on `idx_id`'s seal. Without that, such a peek hangs.
                if should_publish_index(&compute_state.worker_config, compute_state.role()) {
                    compute_state.sharing_registry.reexport(
                        &gid,
                        idx_id,
                        self.scope.index(),
                        self.scope.peers(),
                    );
                }
            }
            Some(ArrangementFlavor::SharedTrace(..)) => {
                // Only the interactive runtime produces `SharedTrace`, and only for imports it reads
                // from the sharing registry. Its exports are transient query outputs, which are
                // freshly rendered `Local` arrangements (a join/reduce output), never a direct
                // re-export of an imported shared arrangement. The maintenance runtime's imports are
                // `Local`/`Trace`. So an export can never observe a `SharedTrace` input.
                unreachable!(
                    "interactive runtime does not re-export an imported shared arrangement"
                );
            }
            None => {
                println!("collection available: {:?}", bundle.collection.is_none());
                println!(
                    "keys available: {:?}",
                    bundle.arranged.keys().collect::<Vec<_>>()
                );
                panic!(
                    "Arrangement alarmingly absent! id: {:?}, keys: {:?}",
                    Id::Global(idx_id),
                    &key
                );
            }
        };
    }
}

// This implementation block requires the scopes have the same timestamp as the trace manager.
// That makes some sense, because we are hoping to deposit an arrangement in the trace manager.
impl<'g, T> Context<'g, T>
where
    T: RenderTimestamp,
{
    pub(crate) fn export_index_iterative<'outer>(
        &self,
        outer: Scope<'outer, mz_repr::Timestamp>,
        compute_state: &mut ComputeState,
        tokens: &BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        dependency_ids: BTreeSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc<LirScalarExpr>,
        output_probe: &MzProbeHandle<mz_repr::Timestamp>,
    ) {
        // put together tokens that belong to the export
        let mut needed_tokens = Vec::new();
        for dep_id in dependency_ids {
            if let Some(token) = tokens.get(&dep_id) {
                needed_tokens.push(Rc::clone(token));
            }
        }
        let bundle = self.lookup_id(Id::Global(idx_id)).unwrap_or_else(|| {
            panic!(
                "Arrangement alarmingly absent! id: {:?}",
                Id::Global(idx_id)
            )
        });

        let key = &idx.key;
        match bundle.arrangement(key) {
            Some(ArrangementFlavor::Local(oks, errs)) => {
                // TODO: The following as_collection/leave/arrange sequence could be optimized.
                //   * Combine as_collection and leave into a single function.
                //   * Use columnar to extract columns from the batches to implement leave.
                let mut oks = oks
                    .as_collection(|k, v| (k.to_row(), v.to_row()))
                    .leave(outer)
                    .mz_arrange::<
                        ColumnationChunker<_>,
                        RowRowBatcher<_, _>,
                        RowRowBuilder<_, _>,
                        _,
                    >(
                        "Arrange export iterative",
                    );

                let mut errs = errs
                    .as_collection(|k, v| (k.clone(), v.clone()))
                    .leave(outer)
                    .mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, _>(
                        "Arrange export iterative err",
                    );

                // Ensure that the frontier does not advance past the expiration time, if set.
                // Otherwise, we might write down incorrect data.
                if let Some(&expiration) = self.dataflow_expiration.as_option() {
                    oks.stream = oks.stream.expire_stream_at(
                        &format!("{}_export_index_iterative_oks", self.debug_name),
                        expiration,
                    );
                    errs.stream = errs.stream.expire_stream_at(
                        &format!("{}_export_index_iterative_err", self.debug_name),
                        expiration,
                    );
                }

                oks.stream = oks.stream.probe_notify_with(vec![output_probe.clone()]);

                // Attach logging of dataflow errors.
                if let Some(logger) = compute_state.compute_logger.clone() {
                    errs.stream = errs.stream.log_dataflow_errors(logger, idx_id);
                }

                // Publish the arrangement into the per-process sharing registry when the gate opens
                // (see `should_publish_index`). The
                // arrangements were re-arranged onto `outer` above (the worker scope carrying
                // `mz_repr::Timestamp`), so publish and record the worker ordinal from `outer`. As
                // above, `publish` borrows the arrangements, so it happens before their traces move.
                if should_publish_index(&compute_state.worker_config, compute_state.role()) {
                    // Seal signal for interactive fast-path peeks. See `export_index`: forward each
                    // advance of the re-arranged `outer`-scope output frontier to `note_frontier`,
                    // so a peek waiting only on the seal is re-examined even without a new
                    // publication.
                    //
                    // Both the oks and errs streams must signal. A peek whose result is an error
                    // carries its data on the errs stream, whose frontier is held back until the
                    // error is emitted. Tapping only oks leaves such a peek stuck once the errs
                    // frontier finally advances. See the matching tap in `export_index`.
                    let worker_index = outer.index();
                    let oks_registry = compute_state.sharing_registry.clone();
                    oks.stream = oks.stream.inspect_container(move |event| {
                        if event.is_err() {
                            oks_registry.note_frontier(idx_id, worker_index);
                        }
                    });
                    let errs_registry = compute_state.sharing_registry.clone();
                    errs.stream = errs.stream.inspect_container(move |event| {
                        if event.is_err() {
                            errs_registry.note_frontier(idx_id, worker_index);
                        }
                    });

                    // Adopt the registry's placeholder slot for `idx_id` rather than publishing
                    // fresh and inserting. See the matching adopt in `export_index` for why: it
                    // fills a slot in place instead of risking an overwrite of a placeholder a
                    // reader has already imported, and it must notify explicitly once both halves
                    // are adopted since `get_or_create_placeholder` does not notify on create.
                    let slot = compute_state.sharing_registry.get_or_create_placeholder(
                        idx_id,
                        worker_index,
                        outer.peers(),
                    );
                    PublishArrangement::adopt(&oks, &slot.oks);
                    PublishArrangement::adopt(&errs, &slot.errs);
                    compute_state.sharing_registry.notify(idx_id, worker_index);
                }

                compute_state.traces.set(
                    idx_id,
                    TraceBundle::new(oks.trace, errs.trace).with_drop(needed_tokens),
                );
            }
            Some(ArrangementFlavor::Trace(gid, _, _)) => {
                // Duplicate of existing arrangement with id `gid`, so
                // just create another handle to that arrangement.
                let trace = compute_state.traces.get(&gid).unwrap().clone();
                compute_state.traces.set(idx_id, trace);

                // Mirror the trace aliasing in the sharing registry: re-register the arrangement
                // already published under `gid` on this worker under `idx_id` as well.
                if should_publish_index(&compute_state.worker_config, compute_state.role()) {
                    compute_state.sharing_registry.reexport(
                        &gid,
                        idx_id,
                        outer.index(),
                        outer.peers(),
                    );
                }
            }
            Some(ArrangementFlavor::SharedTrace(..)) => {
                // See `export_index`: only the interactive runtime produces `SharedTrace`, and its
                // exports are freshly rendered `Local` query outputs, never a re-export of an
                // imported shared arrangement, so an export can never observe this variant.
                unreachable!(
                    "interactive runtime does not re-export an imported shared arrangement"
                );
            }
            None => {
                println!("collection available: {:?}", bundle.collection.is_none());
                println!(
                    "keys available: {:?}",
                    bundle.arranged.keys().collect::<Vec<_>>()
                );
                panic!(
                    "Arrangement alarmingly absent! id: {:?}, keys: {:?}",
                    Id::Global(idx_id),
                    &key,
                );
            }
        };
    }
}

/// Information about bindings, tracked in `render_recursive_plan` and
/// `render_plan`, to be passed to `render_letfree_plan`.
///
/// `render_letfree_plan` uses these to produce nice output (e.g., `With ...
/// Returning ...`) for local bindings in the `mz_lir_mapping` output.
enum BindingInfo {
    Body { in_let: bool },
    Let { id: LocalId, last: bool },
    LetRec { id: LocalId, last: bool },
}

impl<'scope> Context<'scope, Product<mz_repr::Timestamp, PointStamp<u64>>> {
    /// Renders a plan to a differential dataflow, producing the collection of results.
    ///
    /// This method allows for `plan` to contain [`RecBind`]s, and is planned
    /// in the context of `level` pre-existing iteration coordinates.
    ///
    /// This method recursively descends [`RecBind`] values, establishing nested scopes for each
    /// and establishing the appropriate recursive dependencies among the bound variables.
    /// Once all [`RecBind`]s have been rendered it calls in to `render_plan` which will error if
    /// further [`RecBind`]s are found.
    ///
    /// The method requires that all variables conclude with a physical representation that
    /// contains a collection (i.e. a non-arrangement), and it will panic otherwise.
    fn render_recursive_plan(
        &mut self,
        object_id: GlobalId,
        level: usize,
        plan: RenderPlan,
        binding: BindingInfo,
    ) -> CollectionBundle<'scope, Product<mz_repr::Timestamp, PointStamp<u64>>> {
        for BindStage { lets, recs } in plan.binds {
            // Render the let bindings in order.
            let mut let_iter = lets.into_iter().peekable();
            while let Some(LetBind { id, value }) = let_iter.next() {
                let bundle =
                    self.scope
                        .clone()
                        .region_named(&format!("Binding({:?})", id), |region| {
                            let depends = value.depends();
                            let last = let_iter.peek().is_none();
                            let binding = BindingInfo::Let { id, last };
                            self.enter_region(region, Some(&depends))
                                .render_letfree_plan(object_id, value, binding)
                                .leave_region(self.scope)
                        });
                self.insert_id(Id::Local(id), bundle);
            }

            let rec_ids: Vec<_> = recs.iter().map(|r| r.id).collect();

            // Define variables for rec bindings.
            // It is important that we only use the `Variable` until the object is bound.
            // At that point, all subsequent uses should have access to the object itself.
            let mut variables = BTreeMap::new();
            for id in rec_ids.iter() {
                use differential_dataflow::dynamic::feedback_summary;
                let inner = feedback_summary::<u64>(level + 1, 1);
                let (oks_v, oks_collection) =
                    Variable::new(self.scope, Product::new(Default::default(), inner.clone()));
                let (err_v, err_collection) =
                    Variable::new(self.scope, Product::new(Default::default(), inner));

                self.insert_id(
                    Id::Local(*id),
                    CollectionBundle::from_collections(oks_collection, err_collection),
                );
                variables.insert(Id::Local(*id), (oks_v, err_v));
            }
            // Now render each of the rec bindings.
            let mut rec_iter = recs.into_iter().peekable();
            while let Some(RecBind { id, value, limit }) = rec_iter.next() {
                let last = rec_iter.peek().is_none();
                let binding = BindingInfo::LetRec { id, last };
                let bundle = self.render_recursive_plan(object_id, level + 1, value, binding);
                // We need to ensure that the raw collection exists, but do not have enough information
                // here to cause that to happen.
                let (oks, mut err) = bundle.collection.clone().unwrap();
                let oks = oks.into_vec();
                self.insert_id(Id::Local(id), bundle);
                let (oks_v, err_v) = variables.remove(&Id::Local(id)).unwrap();

                // Set oks variable to `oks` but consolidated to ensure iteration ceases at fixed point.
                let mut oks = CollectionExt::consolidate_named::<KeyBatcher<_, _, _>>(
                    oks,
                    "LetRecConsolidation",
                );

                if let Some(limit) = limit {
                    // We swallow the results of the `max_iter`th iteration, because
                    // these results would go into the `max_iter + 1`th iteration.
                    let (in_limit, over_limit) =
                        oks.inner.branch_when(move |Product { inner: ps, .. }| {
                            // The iteration number, or if missing a zero (as trailing zeros are truncated).
                            let iteration_index = *ps.get(level).unwrap_or(&0);
                            // The pointstamp starts counting from 0, so we need to add 1.
                            iteration_index + 1 >= limit.max_iters.into()
                        });
                    oks = VecCollection::new(in_limit);
                    if !limit.return_at_limit {
                        err = err.concat(VecCollection::new(over_limit).map(move |_data| {
                            DataflowErrorSer::from(EvalError::LetRecLimitExceeded(
                                format!("{}", limit.max_iters.get()).into(),
                            ))
                        }));
                    }
                }

                // Set err variable to the distinct elements of `err`.
                // Distinctness is important, as we otherwise might add the same error each iteration,
                // say if the limit of `oks` has an error. This would result in non-terminating rather
                // than a clean report of the error. The trade-off is that we lose information about
                // multiplicities of errors, but .. this seems to be the better call.
                let err: KeyCollection<_, _, _> = err.into();
                let errs = err
                    .mz_arrange::<
                        ColumnationChunker<_>,
                        ErrBatcher<_, _>,
                        ErrBuilder<_, _>,
                        ErrSpine<_, _>,
                    >("Arrange recursive err")
                    .mz_reduce_abelian::<_, ErrBuilder<_, _>, ErrSpine<_, _>, _>(
                        "Distinct recursive err",
                        move |_k, _s, t| t.push(((), Diff::ONE)),
                    )
                    .as_collection(|k, _| k.clone());

                oks_v.set(oks);
                err_v.set(errs);
            }
            // Now extract each of the rec bindings into the outer scope.
            for id in rec_ids.into_iter() {
                let bundle = self.remove_id(Id::Local(id)).unwrap();
                let (oks, err) = bundle.collection.unwrap();
                let oks = oks.into_vec();
                self.insert_id(
                    Id::Local(id),
                    CollectionBundle::from_collections(
                        oks.leave_dynamic(level + 1),
                        err.leave_dynamic(level + 1),
                    ),
                );
            }
        }

        self.render_letfree_plan(object_id, plan.body, binding)
    }
}

impl<'scope, T: RenderTimestamp + MaybeBucketByTime> Context<'scope, T> {
    /// Renders a non-recursive plan to a differential dataflow, producing the collection of
    /// results.
    ///
    /// The return type reflects the uncertainty about the data representation, perhaps
    /// as a stream of data, perhaps as an arrangement, perhaps as a stream of batches.
    ///
    /// # Panics
    ///
    /// Panics if the given plan contains any [`RecBind`]s. Recursive plans must be rendered using
    /// `render_recursive_plan` instead.
    fn render_plan(
        &mut self,
        object_id: GlobalId,
        plan: RenderPlan,
    ) -> CollectionBundle<'scope, T> {
        let mut in_let = false;
        for BindStage { lets, recs } in plan.binds {
            assert!(recs.is_empty());

            let mut let_iter = lets.into_iter().peekable();
            while let Some(LetBind { id, value }) = let_iter.next() {
                // if we encounter a single let, the body is in a let
                in_let = true;
                let bundle =
                    self.scope
                        .clone()
                        .region_named(&format!("Binding({:?})", id), |region| {
                            let depends = value.depends();
                            let last = let_iter.peek().is_none();
                            let binding = BindingInfo::Let { id, last };
                            self.enter_region(region, Some(&depends))
                                .render_letfree_plan(object_id, value, binding)
                                .leave_region(self.scope)
                        });
                self.insert_id(Id::Local(id), bundle);
            }
        }

        self.scope.clone().region_named("Main Body", |region| {
            let depends = plan.body.depends();
            self.enter_region(region, Some(&depends))
                .render_letfree_plan(object_id, plan.body, BindingInfo::Body { in_let })
                .leave_region(self.scope)
        })
    }

    /// Renders a let-free plan to a differential dataflow, producing the collection of results.
    fn render_letfree_plan(
        &self,
        object_id: GlobalId,
        plan: LetFreePlan,
        binding: BindingInfo,
    ) -> CollectionBundle<'scope, T> {
        let (mut nodes, root_id, topological_order) = plan.destruct();

        // Rendered collections by their `LirId`.
        let mut collections = BTreeMap::new();

        // Mappings to send along.
        // To save overhead, we'll only compute mappings when we need to,
        // which means things get gated behind options. Unfortunately, that means we
        // have several `Option<...>` types that are _all_ `Some` or `None` together,
        // but there's no convenient way to express the invariant.
        let should_compute_lir_metadata = self.compute_logger.is_some();
        let mut lir_mapping_metadata = if should_compute_lir_metadata {
            Some(Vec::with_capacity(nodes.len()))
        } else {
            None
        };

        let mut topo_iter = topological_order.into_iter().peekable();
        while let Some(lir_id) = topo_iter.next() {
            let node = nodes.remove(&lir_id).unwrap();

            // TODO(mgree) need ExprHumanizer in DataflowDescription to get nice column names
            // ActiveComputeState can't have a catalog reference, so we'll need to capture the names
            // in some other structure and have that structure impl ExprHumanizer
            let metadata = if should_compute_lir_metadata {
                let operator = node.expr.humanize(&DummyHumanizer);

                // mark the last operator in topo order with any binding decoration
                let operator = if topo_iter.peek().is_none() {
                    match &binding {
                        BindingInfo::Body { in_let: true } => format!("Returning {operator}"),
                        BindingInfo::Body { in_let: false } => operator,
                        BindingInfo::Let { id, last: true } => {
                            format!("With {id} = {operator}")
                        }
                        BindingInfo::Let { id, last: false } => {
                            format!("{id} = {operator}")
                        }
                        BindingInfo::LetRec { id, last: true } => {
                            format!("With Recursive {id} = {operator}")
                        }
                        BindingInfo::LetRec { id, last: false } => {
                            format!("{id} = {operator}")
                        }
                    }
                } else {
                    operator
                };

                let operator_id_start = self.scope.worker().peek_identifier();
                Some((operator, operator_id_start))
            } else {
                None
            };

            let mut bundle = self.render_plan_expr(node.expr, &collections);

            if let Some((operator, operator_id_start)) = metadata {
                let operator_id_end = self.scope.worker().peek_identifier();
                let operator_span = (operator_id_start, operator_id_end);

                if let Some(lir_mapping_metadata) = &mut lir_mapping_metadata {
                    lir_mapping_metadata.push((
                        lir_id,
                        LirMetadata::new(operator, node.parent, node.nesting, operator_span),
                    ))
                }
            }

            self.log_operator_hydration(&mut bundle, lir_id);

            collections.insert(lir_id, bundle);
        }

        if let Some(lir_mapping_metadata) = lir_mapping_metadata {
            self.log_lir_mapping(object_id, lir_mapping_metadata);
        }

        collections
            .remove(&root_id)
            .expect("LetFreePlan invariant (1)")
    }

    /// Renders a [`render_plan::Expr`], producing the collection of results.
    ///
    /// # Panics
    ///
    /// Panics if any of the expr's inputs is not found in `collections`.
    /// Callers must ensure that input nodes have been rendered previously.
    fn render_plan_expr(
        &self,
        expr: render_plan::Expr,
        collections: &BTreeMap<LirId, CollectionBundle<'scope, T>>,
    ) -> CollectionBundle<'scope, T> {
        use render_plan::Expr::*;

        let expect_input = |id| {
            collections
                .get(&id)
                .cloned()
                .unwrap_or_else(|| panic!("missing input collection: {id}"))
        };

        match expr {
            Constant { rows } => {
                // Produce both rows and errs to avoid conditional dataflow construction.
                let (rows, errs) = match rows {
                    Ok(rows) => (rows, Vec::new()),
                    Err(e) => (Vec::new(), vec![e]),
                };

                // We should advance times in constant collections to start from `as_of`.
                let as_of_frontier = self.as_of_frontier.clone();
                let until = self.until.clone();
                let ok_collection = rows
                    .into_iter()
                    .filter_map(move |(row, mut time, diff)| {
                        time.advance_by(as_of_frontier.borrow());
                        if !until.less_equal(&time) {
                            Some((
                                row,
                                <T as Refines<mz_repr::Timestamp>>::to_inner(time),
                                diff,
                            ))
                        } else {
                            None
                        }
                    })
                    .to_stream(self.scope)
                    .as_collection();

                let mut error_time: mz_repr::Timestamp = Timestamp::minimum();
                error_time.advance_by(self.as_of_frontier.borrow());
                let err_collection = errs
                    .into_iter()
                    .map(move |e| {
                        (
                            DataflowErrorSer::from(e),
                            <T as Refines<mz_repr::Timestamp>>::to_inner(error_time),
                            Diff::ONE,
                        )
                    })
                    .to_stream(self.scope)
                    .as_collection();

                CollectionBundle::from_collections(ok_collection, err_collection)
            }
            Get { id, keys, plan } => {
                // Recover the collection from `self` and then apply `mfp` to it.
                // If `mfp` happens to be trivial, we can just return the collection.
                let mut collection = self
                    .lookup_id(id)
                    .unwrap_or_else(|| panic!("Get({:?}) not found at render time", id));
                match plan {
                    mz_compute_types::plan::GetPlan::PassArrangements => {
                        // Assert that each of `keys` are present in `collection`.
                        assert!(
                            keys.arranged
                                .iter()
                                .all(|(key, _, _)| collection.arranged.contains_key(key))
                        );
                        assert!(keys.raw <= collection.collection.is_some());
                        // Retain only those keys we want to import.
                        collection.arranged.retain(|key, _value| {
                            keys.arranged.iter().any(|(key2, _, _)| key2 == key)
                        });
                        collection
                    }
                    mz_compute_types::plan::GetPlan::Arrangement(key, row, mfp) => {
                        let (oks, errs) = collection.as_collection_core(
                            mfp,
                            Some((key, row)),
                            self.until.clone(),
                            &self.config_set,
                        );
                        CollectionBundle::from_collections(oks, errs)
                    }
                    mz_compute_types::plan::GetPlan::Collection(mfp) => {
                        let (oks, errs) = collection.as_collection_core(
                            mfp,
                            None,
                            self.until.clone(),
                            &self.config_set,
                        );
                        CollectionBundle::from_collections(oks, errs)
                    }
                }
            }
            Mfp {
                input,
                mfp,
                input_key_val,
            } => {
                let input = expect_input(input);
                // If `mfp` is non-trivial, we should apply it and produce a collection.
                if mfp.is_identity() {
                    input
                } else {
                    let (oks, errs) = input.as_collection_core(
                        mfp,
                        input_key_val,
                        self.until.clone(),
                        &self.config_set,
                    );
                    CollectionBundle::from_collections(oks, errs)
                }
            }
            FlatMap {
                input_key,
                input,
                exprs,
                func,
                mfp_after: mfp,
            } => {
                let input = expect_input(input);
                self.render_flat_map(input_key, input, exprs, func, mfp)
            }
            Join { inputs, plan } => {
                let inputs = inputs.into_iter().map(expect_input).collect();
                match plan {
                    mz_compute_types::plan::join::JoinPlan::Linear(linear_plan) => {
                        self.render_join(inputs, linear_plan)
                    }
                    mz_compute_types::plan::join::JoinPlan::Delta(delta_plan) => {
                        self.render_delta_join(inputs, delta_plan)
                    }
                }
            }
            Reduce {
                input_key,
                input,
                key_val_plan,
                plan,
                mfp_after,
                temporal_bucketing_strategy,
            } => {
                let input = expect_input(input);
                let mfp_option = (!mfp_after.is_identity()).then_some(mfp_after);
                self.render_reduce(
                    input_key,
                    input,
                    key_val_plan,
                    plan,
                    mfp_option,
                    temporal_bucketing_strategy,
                )
            }
            TopK {
                input,
                top_k_plan,
                temporal_bucketing_strategy,
            } => {
                let input = expect_input(input);
                self.render_topk(input, top_k_plan, temporal_bucketing_strategy)
            }
            Negate { input } => {
                let input = expect_input(input);
                let (oks, errs) = input
                    .collection
                    .clone()
                    .expect("Negate input must be an unarranged collection");
                CollectionBundle::from_edge(oks.negate(), errs)
            }
            Threshold {
                input,
                threshold_plan,
            } => {
                let input = expect_input(input);
                self.render_threshold(input, threshold_plan)
            }
            Union {
                inputs,
                consolidate_output,
                temporal_bucketing_strategies,
            } => {
                let mut oks = Vec::new();
                let mut errs = Vec::new();
                for (input, strategy) in inputs.into_iter().zip_eq(temporal_bucketing_strategies) {
                    let (os, es) = expect_input(input)
                        .collection
                        .clone()
                        .expect("Union input must be an unarranged collection");
                    // Apply per-input temporal bucketing. No-op for `Direct`.
                    // Only consolidating Unions carry non-`Direct` strategies;
                    // see the `Union` arm of `lower_mir_expr_stack_safe`.
                    let os = if matches!(strategy, ArrangementStrategy::TemporalBucketing)
                        && ENABLE_COMPUTE_TEMPORAL_BUCKETING.get(&self.config_set)
                    {
                        let summary: mz_repr::Timestamp = TEMPORAL_BUCKETING_SUMMARY
                            .get(&self.config_set)
                            .try_into()
                            .expect("must fit");
                        let os = os.into_vec();
                        CollectionEdge::Vec(T::maybe_apply_temporal_bucketing(
                            os.inner,
                            self.as_of_frontier.clone(),
                            summary,
                        ))
                    } else {
                        os
                    };
                    oks.push(os);
                    errs.push(es);
                }
                let oks = CollectionEdge::concat_many(self.scope, oks);
                let oks = if consolidate_output {
                    oks.consolidate_named("UnionConsolidation")
                } else {
                    oks
                };
                let errs = differential_dataflow::collection::concatenate(self.scope, errs);
                CollectionBundle::from_edge(oks, errs)
            }
            ArrangeBy {
                input_key,
                input,
                input_mfp,
                forms: keys,
                strategy,
            } => {
                let input = expect_input(input);
                input.ensure_collections(
                    keys,
                    input_key,
                    input_mfp,
                    self.as_of_frontier.clone(),
                    self.until.clone(),
                    &self.config_set,
                    strategy,
                )
            }
        }
    }

    fn log_dataflow_global_id(&self, dataflow_index: usize, global_id: GlobalId) {
        if let Some(logger) = &self.compute_logger {
            logger.log(&ComputeEvent::DataflowGlobal(DataflowGlobal {
                dataflow_index,
                global_id,
            }));
        }
    }

    fn log_lir_mapping(&self, global_id: GlobalId, mapping: Vec<(LirId, LirMetadata)>) {
        if let Some(logger) = &self.compute_logger {
            logger.log(&ComputeEvent::LirMapping(LirMapping { global_id, mapping }));
        }
    }

    fn log_operator_hydration(&self, bundle: &mut CollectionBundle<'scope, T>, lir_id: LirId) {
        // A `CollectionBundle` can contain more than one collection, which makes it not obvious to
        // which we should attach the logging operator.
        //
        // We could attach to each collection and track the lower bound of output frontiers.
        // However, that would be of limited use because we expect all collections to hydrate at
        // roughly the same time: The `ArrangeBy` operator is not fueled, so as soon as it sees the
        // frontier of the unarranged collection advance, it will perform all work necessary to
        // also advance its own frontier. We don't expect significant delays between frontier
        // advancements of the unarranged and arranged collections, so attaching the logging
        // operator to any one of them should produce accurate results.
        //
        // If the `CollectionBundle` contains both unarranged and arranged representations it is
        // beneficial to attach the logging operator to one of the arranged representation to avoid
        // unnecessary cloning of data. The unarranged collection feeds into the arrangements, so
        // if we attached the logging operator to it, we would introduce a fork in its output
        // stream, which would necessitate that all output data is cloned. In contrast, we can hope
        // that the output streams of the arrangements don't yet feed into anything else, so
        // attaching a (pass-through) logging operator does not introduce a fork.

        match bundle.arranged.values_mut().next() {
            Some(arrangement) => {
                use ArrangementFlavor::*;

                match arrangement {
                    Local(a, _) => {
                        a.stream = self.log_operator_hydration_inner(a.stream.clone(), lir_id);
                    }
                    Trace(_, a, _) => {
                        a.stream = self.log_operator_hydration_inner(a.stream.clone(), lir_id);
                    }
                    SharedTrace(_, a, _) => {
                        a.stream = self.log_operator_hydration_inner(a.stream.clone(), lir_id);
                    }
                }
            }
            None => {
                let (oks, _) = bundle
                    .collection
                    .as_mut()
                    .expect("CollectionBundle invariant");
                match oks {
                    CollectionEdge::Vec(c) => {
                        let stream = self.log_operator_hydration_inner(c.inner.clone(), lir_id);
                        *c = stream.as_collection();
                    }
                    CollectionEdge::Columnar(c) => {
                        let stream = self.log_operator_hydration_inner(c.inner.clone(), lir_id);
                        *c = stream.as_collection();
                    }
                }
            }
        }
    }

    fn log_operator_hydration_inner<D>(
        &self,
        stream: Stream<'scope, T, D>,
        lir_id: LirId,
    ) -> Stream<'scope, T, D>
    where
        D: timely::Container + Clone + 'static,
    {
        let Some(logger) = self.compute_logger.clone() else {
            return stream.clone(); // hydration logging disabled
        };

        let export_ids = self.export_ids.clone();

        // Convert the dataflow as-of into a frontier we can compare with input frontiers.
        //
        // We (somewhat arbitrarily) define operators in iterative scopes to be hydrated when their
        // frontier advances to an outer time that's greater than the `as_of`. Comparing
        // `refine(as_of) < input_frontier` would find the moment when the first iteration was
        // complete, which is not what we want. We want `refine(as_of + 1) <= input_frontier`
        // instead.
        let mut hydration_frontier = Antichain::new();
        for time in self.as_of_frontier.iter() {
            if let Some(time) = time.try_step_forward() {
                hydration_frontier.insert(Refines::to_inner(time));
            }
        }

        let name = format!("LogOperatorHydration ({lir_id})");
        stream.unary_frontier(Pipeline, &name, |_cap, _info| {
            let mut hydrated = false;

            for &export_id in &export_ids {
                logger.log(&ComputeEvent::OperatorHydration(OperatorHydration {
                    export_id,
                    lir_id,
                    hydrated,
                }));
            }

            move |(input, frontier), output| {
                // Pass through inputs.
                input.for_each(|cap, data| {
                    output.session(&cap).give_container(data);
                });

                if hydrated {
                    return;
                }

                if PartialOrder::less_equal(&hydration_frontier.borrow(), &frontier.frontier()) {
                    hydrated = true;

                    for &export_id in &export_ids {
                        logger.log(&ComputeEvent::OperatorHydration(OperatorHydration {
                            export_id,
                            lir_id,
                            hydrated,
                        }));
                    }
                }
            }
        })
    }
}

#[allow(dead_code)] // Some of the methods on this trait are unused, but useful to have.
/// A timestamp type that can be used for operations within MZ's dataflow layer.
pub trait RenderTimestamp: MzTimestamp + Default + Refines<mz_repr::Timestamp> {
    /// The system timestamp component of the timestamp.
    ///
    /// This is useful for manipulating the system time, as when delaying
    /// updates for subsequent cancellation, as with monotonic reduction.
    fn system_time(&mut self) -> &mut mz_repr::Timestamp;
    /// Effects a system delay in terms of the timestamp summary.
    fn system_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary;
    /// The event timestamp component of the timestamp.
    fn event_time(&self) -> mz_repr::Timestamp;
    /// The event timestamp component of the timestamp, as a mutable reference.
    fn event_time_mut(&mut self) -> &mut mz_repr::Timestamp;
    /// Effects an event delay in terms of the timestamp summary.
    fn event_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary;
    /// Steps the timestamp back so that logical compaction to the output will
    /// not conflate `self` with any historical times.
    fn step_back(&self) -> Self;
}

/// Apply temporal bucketing to a stream when the timestamp type supports it.
///
/// Sibling to [`RenderTimestamp`]: bucketing is an arrangement-time concern, not a
/// general property of a render timestamp, so the dispatch lives in its own trait.
/// Total-ordered timestamps perform real bucketing; partially-ordered timestamps
/// (e.g. `Product<…>` in iterative scopes) implement this as a no-op.
pub trait MaybeBucketByTime: Timestamp {
    fn maybe_apply_temporal_bucketing<'scope, D>(
        stream: StreamVec<'scope, Self, (D, Self, Diff)>,
        as_of: Antichain<mz_repr::Timestamp>,
        summary: mz_repr::Timestamp,
    ) -> VecCollection<'scope, Self, D, Diff>
    where
        D: differential_dataflow::ExchangeData
            + crate::typedefs::MzData
            + differential_dataflow::Hashable;
}

impl RenderTimestamp for mz_repr::Timestamp {
    fn system_time(&mut self) -> &mut mz_repr::Timestamp {
        self
    }
    fn system_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary {
        delay
    }
    fn event_time(&self) -> mz_repr::Timestamp {
        *self
    }
    fn event_time_mut(&mut self) -> &mut mz_repr::Timestamp {
        self
    }
    fn event_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary {
        delay
    }
    fn step_back(&self) -> Self {
        self.saturating_sub(1)
    }
}

impl MaybeBucketByTime for mz_repr::Timestamp {
    fn maybe_apply_temporal_bucketing<'scope, D>(
        stream: StreamVec<'scope, Self, (D, Self, Diff)>,
        as_of: Antichain<mz_repr::Timestamp>,
        summary: mz_repr::Timestamp,
    ) -> VecCollection<'scope, Self, D, Diff>
    where
        D: differential_dataflow::ExchangeData
            + crate::typedefs::MzData
            + differential_dataflow::Hashable,
    {
        stream
            .bucket::<CapacityContainerBuilder<_>>(as_of, summary)
            .as_collection()
    }
}

impl RenderTimestamp for Product<mz_repr::Timestamp, PointStamp<u64>> {
    fn system_time(&mut self) -> &mut mz_repr::Timestamp {
        &mut self.outer
    }
    fn system_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary {
        Product::new(delay, Default::default())
    }
    fn event_time(&self) -> mz_repr::Timestamp {
        self.outer
    }
    fn event_time_mut(&mut self) -> &mut mz_repr::Timestamp {
        &mut self.outer
    }
    fn event_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary {
        Product::new(delay, Default::default())
    }
    fn step_back(&self) -> Self {
        // It is necessary to step back both coordinates of a product,
        // and when one is a `PointStamp` that also means all coordinates
        // of the pointstamp.
        let inner = self.inner.clone();
        let mut vec = inner.into_inner();
        for item in vec.iter_mut() {
            *item = item.saturating_sub(1);
        }
        Product::new(self.outer.saturating_sub(1), PointStamp::new(vec))
    }
}

impl MaybeBucketByTime for Product<mz_repr::Timestamp, PointStamp<u64>> {
    fn maybe_apply_temporal_bucketing<'scope, D>(
        stream: StreamVec<'scope, Self, (D, Self, Diff)>,
        _as_of: Antichain<mz_repr::Timestamp>,
        _summary: mz_repr::Timestamp,
    ) -> VecCollection<'scope, Self, D, Diff>
    where
        D: differential_dataflow::ExchangeData
            + crate::typedefs::MzData
            + differential_dataflow::Hashable,
    {
        // TODO: Implement bucketing on outer timestamp for iterative scopes.
        stream.as_collection()
    }
}

/// A signal that can be awaited by operators to suspend them prior to startup.
///
/// Creating a signal also yields a token, dropping of which causes the signal to fire.
///
/// `StartSignal` is designed to be usable by both async and sync Timely operators.
///
///  * Async operators can simply `await` it.
///  * Sync operators should register an [`ActivateOnDrop`] value via [`StartSignal::drop_on_fire`]
///    and then check `StartSignal::has_fired()` on each activation.
#[derive(Clone)]
pub(crate) struct StartSignal {
    /// A future that completes when the signal fires.
    ///
    /// The inner type is `Infallible` because no data is ever expected on this channel. Instead the
    /// signal is activated by dropping the corresponding `Sender`.
    fut: futures::future::Shared<oneshot::Receiver<Infallible>>,
    /// A weak reference to the token, to register drop-on-fire values.
    token_ref: Weak<RefCell<Box<dyn Any>>>,
}

impl StartSignal {
    /// Create a new `StartSignal` and a corresponding token that activates the signal when
    /// dropped.
    pub fn new() -> (Self, Rc<dyn Any>) {
        let (tx, rx) = oneshot::channel::<Infallible>();
        let token: Rc<RefCell<Box<dyn Any>>> = Rc::new(RefCell::new(Box::new(tx)));
        let signal = Self {
            fut: rx.shared(),
            token_ref: Rc::downgrade(&token),
        };
        (signal, token)
    }

    pub fn has_fired(&self) -> bool {
        self.token_ref.strong_count() == 0
    }

    /// Returns a Send-safe future that completes when the signal fires.
    ///
    /// Unlike `StartSignal` itself, the returned future does not retain a reference to the token,
    /// so it cannot be used for `drop_on_fire` or `has_fired` checks.
    pub fn into_send_future(self) -> impl Future<Output = ()> + Send {
        use futures::FutureExt;
        self.fut.map(|_| ())
    }

    pub fn drop_on_fire(&self, to_drop: Box<dyn Any>) {
        if let Some(token) = self.token_ref.upgrade() {
            let mut token = token.borrow_mut();
            let inner = std::mem::replace(&mut *token, Box::new(()));
            *token = Box::new((inner, to_drop));
        }
    }
}

impl Future for StartSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx).map(|_| ())
    }
}

/// Extension trait to attach a `StartSignal` to operator outputs.
pub(crate) trait WithStartSignal {
    /// Delays data and progress updates until the start signal has fired.
    ///
    /// Note that this operator needs to buffer all incoming data, so it has some memory footprint,
    /// depending on the amount and shape of its inputs.
    fn with_start_signal(self, signal: StartSignal) -> Self;
}

impl<'scope, Tr> WithStartSignal for Arranged<'scope, Tr>
where
    Tr: TraceReader<Time: RenderTimestamp> + Clone,
{
    fn with_start_signal(self, signal: StartSignal) -> Self {
        Arranged {
            stream: self.stream.with_start_signal(signal),
            trace: self.trace,
        }
    }
}

impl<'scope, T: Timestamp, D> WithStartSignal for Stream<'scope, T, D>
where
    D: timely::Container + Clone + 'static,
{
    fn with_start_signal(self, signal: StartSignal) -> Self {
        let activations = self.scope().activations();
        self.unary(Pipeline, "StartSignal", |_cap, info| {
            let token = Box::new(ActivateOnDrop::new((), info.address, activations));
            signal.drop_on_fire(token);

            let mut stash = Vec::new();

            move |input, output| {
                // Stash incoming updates as long as the start signal has not fired.
                if !signal.has_fired() {
                    input.for_each(|cap, data| stash.push((cap, std::mem::take(data))));
                    return;
                }

                // Release any data we might still have stashed.
                for (cap, mut data) in std::mem::take(&mut stash) {
                    output.session(&cap).give_container(&mut data);
                }

                // Pass through all remaining input data.
                input.for_each(|cap, data| {
                    output.session(&cap).give_container(data);
                });
            }
        })
    }
}

/// Suppress progress messages for times before the given `as_of`.
///
/// This operator exists specifically to work around a memory spike we'd otherwise see when
/// hydrating arrangements (database-issues#6368). The memory spike happens because when the `arrange_core`
/// operator observes a frontier advancement without data it inserts an empty batch into the spine.
/// When it later inserts the snapshot batch into the spine, an empty batch is already there and
/// the spine initiates a merge of these batches, which requires allocating a new batch the size of
/// the snapshot batch.
///
/// The strategy to avoid the spike is to prevent the insertion of that initial empty batch by
/// ensuring that the first frontier advancement downstream `arrange_core` operators observe is
/// beyond the `as_of`, so the snapshot data has already been collected.
///
/// To ensure this, this operator needs to take two measures:
///  * Keep around a minimum capability until the input announces progress beyond the `as_of`.
///  * Reclock all updates emitted at times not beyond the `as_of` to the minimum time.
///
/// The second measure requires elaboration: If we wouldn't reclock snapshot updates, they might
/// still be upstream of `arrange_core` operators when those get to know about us dropping the
/// minimum capability. The in-flight snapshot updates would hold back the input frontiers of
/// `arrange_core` operators to the `as_of`, which would cause them to insert empty batches.
fn suppress_early_progress<'scope, T: Timestamp, D>(
    stream: Stream<'scope, T, D>,
    as_of: Antichain<T>,
) -> Stream<'scope, T, D>
where
    D: Data + timely::Container,
{
    stream.unary_frontier(Pipeline, "SuppressEarlyProgress", |default_cap, _info| {
        let mut early_cap = Some(default_cap);

        move |(input, frontier), output| {
            input.for_each_time(|data_cap, data| {
                if as_of.less_than(data_cap.time()) {
                    let mut session = output.session(&data_cap);
                    for data in data {
                        session.give_container(data);
                    }
                } else {
                    let cap = early_cap.as_ref().expect("early_cap can't be dropped yet");
                    let mut session = output.session(&cap);
                    for data in data {
                        session.give_container(data);
                    }
                }
            });

            if !PartialOrder::less_equal(&frontier.frontier(), &as_of.borrow()) {
                early_cap.take();
            }
        }
    })
}

/// Extension trait for [`Stream`] to selectively limit progress.
trait LimitProgress<T: Timestamp> {
    /// Limit the progress of the stream until its frontier reaches the given `upper` bound. Expects
    /// the implementation to observe times in data, and release capabilities based on the probe's
    /// frontier, after applying `slack` to round up timestamps.
    ///
    /// The implementation of this operator is subtle to avoid regressions in the rest of the
    /// system. Specifically joins hold back compaction on the other side of the join, so we need to
    /// make sure we release capabilities as soon as possible. This is why we only limit progress
    /// for times before the `upper`, which is the time until which the source can distinguish
    /// updates at the time of rendering. Once we make progress to the `upper`, we need to release
    /// our capability.
    ///
    /// This isn't perfect, and can result in regressions if on of the inputs lags behind. We could
    /// consider using the join of the uppers, i.e, use lower bound upper of all available inputs.
    ///
    /// Once the input frontier reaches `[]`, the implementation must release any capability to
    /// allow downstream operators to release resources.
    ///
    /// The implementation should limit the number of pending times to `limit` if it is `Some` to
    /// avoid unbounded memory usage.
    ///
    /// * `handle` is a probe installed on the dataflow's outputs as late as possible, but before
    ///   any timestamp rounding happens (c.f., `REFRESH EVERY` materialized views).
    /// * `slack_ms` is the number of milliseconds to round up timestamps to.
    /// * `name` is a human-readable name for the operator.
    /// * `limit` is the maximum number of pending times to keep around.
    /// * `upper` is the upper bound of the stream's frontier until which the implementation can
    ///   retain a capability.
    fn limit_progress(
        self,
        handle: MzProbeHandle<T>,
        slack_ms: u64,
        limit: Option<usize>,
        upper: Antichain<T>,
        name: String,
    ) -> Self;
}

// TODO: We could make this generic over a `T` that can be converted to and from a u64 millisecond
// number.
impl<'scope, D, R> LimitProgress<mz_repr::Timestamp>
    for StreamVec<'scope, mz_repr::Timestamp, (D, mz_repr::Timestamp, R)>
where
    D: Clone + 'static,
    R: Clone + 'static,
{
    fn limit_progress(
        self,
        handle: MzProbeHandle<mz_repr::Timestamp>,
        slack_ms: u64,
        limit: Option<usize>,
        upper: Antichain<mz_repr::Timestamp>,
        name: String,
    ) -> Self {
        let scope = self.scope();
        let stream =
            self.unary_frontier(Pipeline, &format!("LimitProgress({name})"), |_cap, info| {
                // Times that we've observed on our input.
                let mut pending_times: BTreeSet<mz_repr::Timestamp> = BTreeSet::new();
                // Capability for the lower bound of `pending_times`, if any.
                let mut retained_cap: Option<Capability<mz_repr::Timestamp>> = None;

                let activator = scope.activator_for(info.address);
                handle.activate(activator.clone());

                move |(input, frontier), output| {
                    input.for_each(|cap, data| {
                        for time in data
                            .iter()
                            .flat_map(|(_, time, _)| u64::from(time).checked_add(slack_ms))
                        {
                            // `slack_ms == 0` means no rounding; otherwise round up to the next
                            // multiple of `slack_ms`. Avoids a divide-by-zero panic when the
                            // operator is configured without slack.
                            let rounded_time = if slack_ms == 0 {
                                time
                            } else {
                                (time / slack_ms).saturating_add(1).saturating_mul(slack_ms)
                            };
                            if !upper.less_than(&rounded_time.into()) {
                                pending_times.insert(rounded_time.into());
                            }
                        }
                        output.session(&cap).give_container(data);
                        if retained_cap.as_ref().is_none_or(|c| {
                            !c.time().less_than(cap.time()) && !upper.less_than(cap.time())
                        }) {
                            retained_cap = Some(cap.retain(0));
                        }
                    });

                    handle.with_frontier(|f| {
                        while pending_times
                            .first()
                            .map_or(false, |retained_time| !f.less_than(&retained_time))
                        {
                            let _ = pending_times.pop_first();
                        }
                    });

                    while limit.map_or(false, |limit| pending_times.len() > limit) {
                        let _ = pending_times.pop_first();
                    }

                    match (retained_cap.as_mut(), pending_times.first()) {
                        (Some(cap), Some(first)) => cap.downgrade(first),
                        (_, None) => retained_cap = None,
                        _ => {}
                    }

                    if frontier.is_empty() {
                        retained_cap = None;
                        pending_times.clear();
                    }

                    if !pending_times.is_empty() {
                        tracing::debug!(
                            name,
                            info.global_id,
                            pending_times = %PendingTimesDisplay(pending_times.iter().cloned()),
                            frontier = ?frontier.frontier().get(0),
                            probe = ?handle.with_frontier(|f| f.get(0).cloned()),
                            ?upper,
                            "pending times",
                        );
                    }
                }
            });
        stream
    }
}

/// A formatter for an iterator of timestamps that displays the first element, and subsequently
/// the difference between timestamps.
struct PendingTimesDisplay<T>(T);

impl<T> std::fmt::Display for PendingTimesDisplay<T>
where
    T: IntoIterator<Item = mz_repr::Timestamp> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.0.clone().into_iter();
        write!(f, "[")?;
        if let Some(first) = iter.next() {
            write!(f, "{}", first)?;
            let mut last = u64::from(first);
            for time in iter {
                write!(f, ", +{}", u64::from(time) - last)?;
                last = u64::from(time);
            }
        }
        write!(f, "]")?;
        Ok(())
    }
}

/// Helper to merge pairs of datum iterators into a row or split a datum iterator
/// into two rows, given the arity of the first component.
#[derive(Clone, Copy, Debug)]
struct Pairer {
    split_arity: usize,
}

impl Pairer {
    /// Creates a pairer with knowledge of the arity of first component in the pair.
    fn new(split_arity: usize) -> Self {
        Self { split_arity }
    }

    /// Merges a pair of datum iterators creating a `Row` instance.
    fn merge<'a, I1, I2>(&self, first: I1, second: I2) -> Row
    where
        I1: IntoIterator<Item = Datum<'a>>,
        I2: IntoIterator<Item = Datum<'a>>,
    {
        SharedRow::pack(first.into_iter().chain(second))
    }

    /// Splits a datum iterator into a pair of `Row` instances.
    fn split<'a>(&self, datum_iter: impl IntoIterator<Item = Datum<'a>>) -> (Row, Row) {
        let mut datum_iter = datum_iter.into_iter();
        let mut row_builder = SharedRow::get();
        let first = row_builder.pack_using(datum_iter.by_ref().take(self.split_arity));
        let second = row_builder.pack_using(datum_iter);
        (first, second)
    }
}

#[cfg(test)]
mod interactive_import_tests {
    use std::sync::mpsc;

    use differential_dataflow::input::{Input, InputSession};
    use differential_dataflow::operators::arrange::Arranged;
    use differential_dataflow::trace::TraceReader;
    use mz_compute_types::dyncfgs::ENABLE_INDEX_ARRANGEMENT_SHARING;
    use mz_dyncfg::{ConfigSet, ConfigUpdates};
    use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
    use mz_row_spine::{DatumSeq, RowRowBatcher, RowRowBuilder};
    use mz_timely_util::columnation::ColumnationChunker;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Probe};
    use timely::dataflow::{ProbeHandle, Scope};
    use timely::progress::Antichain;

    use crate::extensions::arrange::{KeyCollection, MzArrange};
    use crate::shared_trace::PublishArrangement;
    use crate::sharing::{ArrangementSharingRegistry, SharedOksFrontier};
    use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine, RowRowAgent, RowRowSpine};

    use super::{import_shared_index, should_publish_index};
    use crate::server::ComputeRuntimeRole;

    fn test_rows() -> Vec<(Row, Row)> {
        vec![
            (
                Row::pack_slice(&[Datum::Int32(1)]),
                Row::pack_slice(&[Datum::String("a")]),
            ),
            (
                Row::pack_slice(&[Datum::Int32(2)]),
                Row::pack_slice(&[Datum::String("b")]),
            ),
        ]
    }

    /// Publishes `rows` as a `(RowRow oks, Err errs)` index into `registry` under `id` on worker 0
    /// of `scope`. The updates are written at time 0 and sealed by advancing the inputs to 1.
    ///
    /// The `InputSession` handles drop at the end of this call, buffering the sealed updates for the
    /// worker to process on later steps, mirroring `sharing.rs`'s `publish_index_into`.
    fn publish_index(
        scope: Scope<'_, Timestamp>,
        registry: &ArrangementSharingRegistry,
        id: GlobalId,
        rows: Vec<(Row, Row)>,
    ) {
        let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
        let oks = oks_collection.mz_arrange::<
            ColumnationChunker<_>,
            RowRowBatcher<_, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >("test oks");

        let (mut errs_input, errs_collection) =
            scope.new_collection::<crate::render::errors::DataflowErrorSer, Diff>();
        let errs = KeyCollection::from(errs_collection).mz_arrange::<
            ColumnationChunker<_>,
            ErrBatcher<_, _>,
            ErrBuilder<_, _>,
            ErrSpine<_, _>,
        >("test errs");

        let slot = registry.get_or_create_placeholder(id, 0, 1);
        PublishArrangement::adopt(&oks, &slot.oks);
        PublishArrangement::adopt(&errs, &slot.errs);
        registry.notify(id, 0);

        for (k, v) in rows {
            oks_input.update((k, v), Diff::ONE);
        }
        oks_input.advance_to(Timestamp::from(1_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(1_u64));
        errs_input.flush();
    }

    /// The interactive import path imports a maintenance-published arrangement into a second
    /// dataflow as a static `as_of` snapshot via `SharedTraceHandle::import_snapshot_at`,
    /// reconstructing the same rows, and registers a read hold at the importing dataflow's `as_of`.
    #[mz_ore::test]
    fn interactive_import_replays_rows_and_holds_at_as_of() {
        let id = GlobalId::User(1);
        let rows = test_rows();
        let mut expected: Vec<(Row, Row)> = rows.clone();
        expected.sort();

        // `as_of` beyond the publish-time `since` (0), so a correct hold advance is observable: the
        // freshly minted handle's hold starts at `since` (0) and must be advanced to `as_of` (1).
        let as_of = Antichain::from_elem(Timestamp::from(1_u64));
        let registry = ArrangementSharingRegistry::new();

        let (capture_tx, capture_rx) = mpsc::channel();
        let registry_in = registry.clone();
        let as_of_in = as_of.clone();

        timely::execute_directly(move |worker| {
            // Maintenance runtime: publish the index into the shared registry.
            worker.dataflow::<Timestamp, _, _>(|scope| {
                publish_index(scope, &registry_in, id, rows.clone());
            });

            // Interactive runtime: a temporary dataflow imports the published arrangement via the
            // new path and captures the reconstructed rows.
            let probe = ProbeHandle::new();
            let (mut oks_hold, mut errs_hold) = worker.dataflow::<Timestamp, _, _>(|scope| {
                // `until` empty: no upper suppression, so the whole snapshot at `as_of` flows.
                let (oks_arranged, _errs_arranged, oks_hold, errs_hold, _slot) =
                    import_shared_index(
                        scope.clone(),
                        &registry_in,
                        id,
                        "Index",
                        &as_of_in,
                        &Antichain::new(),
                    );

                let collected = Arranged::<SharedOksFrontier>::flat_map_batches(
                    oks_arranged.stream,
                    |k: DatumSeq, v: DatumSeq| {
                        let key = Row::pack_slice(&k.into_iter().collect::<Vec<_>>());
                        let val = Row::pack_slice(&v.into_iter().collect::<Vec<_>>());
                        [(key, val)]
                    },
                );
                collected.inner.probe_with(&probe).capture_into(capture_tx);
                (oks_hold, errs_hold)
            });

            // The read hold sits at the dataflow's `as_of`, not the publish-time `since`.
            assert_eq!(oks_hold.get_logical_compaction(), as_of_in.borrow());
            assert_eq!(errs_hold.get_logical_compaction(), as_of_in.borrow());

            // Drive both dataflows until the imported-and-reconstructed output has sealed time 0.
            while probe.less_than(&Timestamp::from(1_u64)) {
                worker.step();
            }
        });

        let mut found: Vec<(Row, Row)> = capture_rx
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .filter(|(_, _, diff)| diff.is_positive())
            .map(|((k, v), _, _)| (k, v))
            .collect();
        found.sort();
        assert_eq!(found, expected);
    }

    /// Like [`publish_index`], but also returns the writer-side `oks` `InputSession` and a plain
    /// `TraceAgent` clone of the `oks` trace (not a `SharedTraceHandle`), so a test can keep
    /// publishing after the initial seal and force compaction directly on the writer. Mirrors the
    /// `writer` handle in the differential-dataflow primitive's own `import_hold_pins_then_releases`
    /// (`differential-dataflow/tests/sharing.rs`), which drives the writer side of the identical
    /// pin-then-release scenario one layer down.
    fn publish_index_with_writer(
        scope: Scope<'_, Timestamp>,
        registry: &ArrangementSharingRegistry,
        id: GlobalId,
        rows: Vec<(Row, Row)>,
    ) -> (
        InputSession<Timestamp, (Row, Row), Diff>,
        InputSession<Timestamp, crate::render::errors::DataflowErrorSer, Diff>,
        RowRowAgent<Timestamp, Diff>,
    ) {
        let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
        let oks = oks_collection.mz_arrange::<
            ColumnationChunker<_>,
            RowRowBatcher<_, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >("test oks");
        let oks_writer = oks.trace.clone();

        let (mut errs_input, errs_collection) =
            scope.new_collection::<crate::render::errors::DataflowErrorSer, Diff>();
        let errs = KeyCollection::from(errs_collection).mz_arrange::<
            ColumnationChunker<_>,
            ErrBatcher<_, _>,
            ErrBuilder<_, _>,
            ErrSpine<_, _>,
        >("test errs");

        let slot = registry.get_or_create_placeholder(id, 0, 1);
        PublishArrangement::adopt(&oks, &slot.oks);
        PublishArrangement::adopt(&errs, &slot.errs);
        registry.notify(id, 0);

        for (k, v) in rows {
            oks_input.update((k, v), Diff::ONE);
        }
        oks_input.advance_to(Timestamp::from(1_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(1_u64));
        errs_input.flush();

        (oks_input, errs_input, oks_writer)
    }

    /// Feeds `oks_input` a filler update at `at`, advances it to `next`, and steps `worker` a few
    /// times, mirroring the `tick` helper in `differential-dataflow`'s own `sharing.rs` test suite.
    /// The publisher operator only recomputes its forwarded compaction when a batch runs through
    /// it, so a bare `set_logical_compaction`/`set_physical_compaction` call on a writer handle is
    /// invisible to the published `since` until the next such tick.
    fn tick(
        worker: &mut timely::worker::Worker,
        oks_input: &mut InputSession<Timestamp, (Row, Row), Diff>,
        at: Timestamp,
        next: Timestamp,
    ) {
        oks_input.advance_to(at);
        oks_input.update(
            (
                Row::pack_slice(&[Datum::Int32(-1)]),
                Row::pack_slice(&[Datum::String("tick")]),
            ),
            Diff::ONE,
        );
        oks_input.advance_to(next);
        oks_input.flush();
        for _ in 0..20 {
            worker.step();
        }
    }

    /// The interactive import's read hold pins the maintenance trace at `as_of` only while it is
    /// alive: once the importing dataflow drops (and with it the `tokens` entry holding
    /// `oks_hold`/`errs_hold`), the trace is free to compact past `as_of`, which it could not do
    /// before the drop.
    ///
    /// Mirrors the differential-dataflow primitive's own `import_hold_pins_then_releases`
    /// (`differential-dataflow/tests/sharing.rs`), which demonstrates the identical pin-then-release
    /// contract one layer down, directly on a bare `SharedTraceHandle` with no compute-level
    /// wrapping. This test drives the same `import_shared_index` primitive that
    /// `import_index_shared` calls in production, rather than re-deriving the contract from
    /// scratch.
    ///
    /// Staging this end-to-end through the real `ComputeState`/`TraceManager`, as the maintenance
    /// `import_index` path would, is not practical in this harness: there is no controller driving
    /// frontier advancement, so nothing would ever request compaction past `as_of` for real (the
    /// same limitation that keeps the since-gate tests elsewhere in this crate on
    /// `execute_directly` plus a directly-driven writer, rather than a full coordinator). The
    /// closest observable proxy is used instead: a writer-side compaction request advanced directly
    /// on the published trace, exactly as `import_hold_pins_then_releases` does, with the assertion
    /// made through `SharedTraceHandle::snapshot_at` (a real read against the shared trace's actual
    /// `since`, not a count or a flag).
    #[mz_ore::test]
    fn interactive_import_hold_releases_on_drop() {
        let id = GlobalId::User(1);
        let rows = test_rows();
        let as_of_time = Timestamp::from(1_u64);
        let as_of = Antichain::from_elem(as_of_time);
        let registry = ArrangementSharingRegistry::new();

        timely::execute_directly(move |worker| {
            // Maintenance runtime: publish the index, keeping the `oks` `InputSession` (so we can
            // tick the dataflow afterward) and a plain writer trace handle (so we can request
            // compaction on it directly, as a controller would) alive across the whole closure.
            let (mut oks_input, _errs_input, mut oks_writer) =
                worker.dataflow::<Timestamp, _, _>(|scope| {
                    publish_index_with_writer(scope, &registry, id, rows.clone())
                });

            // Interactive runtime: import at `as_of`, exactly as `import_index_shared` does. Only
            // `oks_hold`/`errs_hold` are kept: the `Arranged`s themselves each carry their own
            // independent hold (`SharedTraceHandle::clone` mints a fresh registration), and
            // production code drops them the same way once it has read out their `stream`s, keeping
            // only the hold pair alive in `tokens`.
            let (oks_hold, errs_hold) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let (_oks_arranged, _errs_arranged, oks_hold, errs_hold, _slot) =
                    import_shared_index(
                        scope.clone(),
                        &registry,
                        id,
                        "Index",
                        &as_of,
                        &Antichain::new(),
                    );
                (oks_hold, errs_hold)
            });

            // The writer requests compaction well past `as_of`, then a filler tick reactivates the
            // publisher so it recomputes its forwarded `since` from the current reader holds.
            let target = Antichain::from_elem(Timestamp::from(10_u64));
            oks_writer.set_logical_compaction(target.borrow());
            oks_writer.set_physical_compaction(target.borrow());
            tick(
                worker,
                &mut oks_input,
                Timestamp::from(5_u64),
                Timestamp::from(6_u64),
            );

            // The live interactive-import hold still pins the trace at `as_of`: a read there still
            // succeeds despite the writer's request. `snapshot_at` only inspects the shared trace's
            // actual state, so reading via `oks_hold` itself introduces no additional hold.
            assert!(
                oks_hold.snapshot_at(&as_of_time).is_some(),
                "the live interactive-import hold must keep `as_of` readable"
            );

            // Drop the hold, as happens when the interactive dataflow (and its `tokens` entry)
            // drops. With no reader hold left, the next tick lets the publisher's forwarded `since`
            // follow the writer's request.
            drop(oks_hold);
            drop(errs_hold);
            tick(
                worker,
                &mut oks_input,
                Timestamp::from(11_u64),
                Timestamp::from(12_u64),
            );

            // The trace compacted past `as_of`: a fresh handle (minted only now, so it introduces no
            // new hold at `as_of`) can no longer read there.
            let (released_oks, _released_errs) = registry.handles(&id, 0).expect("still published");
            assert!(
                released_oks.snapshot_at(&as_of_time).is_none(),
                "after the hold drops, the trace must be free to compact past `as_of`"
            );
        });
    }

    /// A published slot's `since` may already sit above the dataflow's requested `as_of` if the
    /// controller offered an unreadable `as_of`, a protocol error: `import_shared_index` must
    /// panic rather than let the read silently see coalesced data, mirroring the maintenance
    /// path's `compaction_frontier` assert in `import_index`.
    ///
    /// Advances the writer's compaction well past `as_of` with no reader hold registered yet, the
    /// same `publish_without_readers_does_not_pin_compaction` scenario `shared_trace.rs` covers,
    /// so a freshly minted handle's hold starts at the already-advanced `since`. Importing at
    /// `as_of` afterward must panic.
    #[mz_ore::test]
    #[should_panic(expected = "since")]
    fn import_asserts_since_at_most_as_of() {
        let id = GlobalId::User(1);
        let rows = test_rows();
        let as_of = Antichain::from_elem(Timestamp::from(1_u64));
        let registry = ArrangementSharingRegistry::new();

        timely::execute_directly(move |worker| {
            let (mut oks_input, _errs_input, mut oks_writer) =
                worker.dataflow::<Timestamp, _, _>(|scope| {
                    publish_index_with_writer(scope, &registry, id, rows.clone())
                });

            // Advance the writer's compaction well past `as_of`, with no reader hold registered
            // yet, then tick so the publisher forwards it into the published `since`.
            let target = Antichain::from_elem(Timestamp::from(10_u64));
            oks_writer.set_logical_compaction(target.borrow());
            oks_writer.set_physical_compaction(target.borrow());
            tick(
                worker,
                &mut oks_input,
                Timestamp::from(5_u64),
                Timestamp::from(6_u64),
            );

            // Importing at `as_of` now finds a `since` already beyond it: the assert must panic.
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let _ = import_shared_index(
                    scope.clone(),
                    &registry,
                    id,
                    "Index",
                    &as_of,
                    &Antichain::new(),
                );
            });
        });
    }

    /// A two-runtime process's maintenance runtime publishes into the sharing registry even with
    /// the `enable_index_arrangement_sharing` dyncfg off. Its interactive peer reads only from the
    /// registry, so unconditional publication is what keeps interactive peeks from blocking until
    /// they time out.
    #[mz_ore::test]
    fn maintenance_role_publishes_without_dyncfg() {
        let config = ConfigSet::default().add(&ENABLE_INDEX_ARRANGEMENT_SHARING);
        // Dyncfg left at its default (off).
        assert!(should_publish_index(
            &config,
            ComputeRuntimeRole::Maintenance
        ));
    }

    /// A two-runtime process's interactive runtime publishes its transient query outputs into the
    /// sharing registry even with the dyncfg off, so a result peek served from the registry can read
    /// the output and receive its seal notifications.
    #[mz_ore::test]
    fn interactive_role_publishes_without_dyncfg() {
        let config = ConfigSet::default().add(&ENABLE_INDEX_ARRANGEMENT_SHARING);
        // Dyncfg left at its default (off).
        assert!(should_publish_index(
            &config,
            ComputeRuntimeRole::Interactive
        ));
    }

    /// The `Solo` (single-runtime) role publishes only when the dyncfg opts in, preserving the
    /// original single-runtime behavior.
    #[mz_ore::test]
    fn solo_role_requires_dyncfg_to_publish() {
        let off = ConfigSet::default().add(&ENABLE_INDEX_ARRANGEMENT_SHARING);
        assert!(!should_publish_index(&off, ComputeRuntimeRole::Solo));

        let on = ConfigSet::default().add(&ENABLE_INDEX_ARRANGEMENT_SHARING);
        let mut updates = ConfigUpdates::default();
        updates.add(&ENABLE_INDEX_ARRANGEMENT_SHARING, true);
        updates.apply(&on);
        assert!(should_publish_index(&on, ComputeRuntimeRole::Solo));
    }
}
