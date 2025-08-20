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
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, ShutdownButton};
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{AsCollection, Collection, Data};
use futures::FutureExt;
use futures::channel::oneshot;
use mz_compute_types::dataflows::{DataflowDescription, IndexDesc};
use mz_compute_types::dyncfgs::{
    COMPUTE_APPLY_COLUMN_DEMANDS, COMPUTE_LOGICAL_BACKPRESSURE_INFLIGHT_SLACK,
    COMPUTE_LOGICAL_BACKPRESSURE_MAX_RETAINED_CAPABILITIES, ENABLE_COMPUTE_LOGICAL_BACKPRESSURE,
    ENABLE_TEMPORAL_BUCKETING, TEMPORAL_BUCKETING_SUMMARY,
};
use mz_compute_types::plan::LirId;
use mz_compute_types::plan::render_plan::{
    self, BindStage, LetBind, LetFreePlan, RecBind, RenderPlan,
};
use mz_expr::{EvalError, Id, LocalId};
use mz_ore::str::separated;
use mz_persist_client::operators::shard_source::{ErrorHandler, SnapshotMode};
use mz_repr::explain::DummyHumanizer;
use mz_repr::{Datum, Diff, GlobalId, Row, SharedRow};
use mz_storage_operators::persist_source;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::{CollectionExt, StreamExt};
use mz_timely_util::probe::{Handle as MzProbeHandle, ProbeNotify};
use timely::PartialOrder;
use timely::communication::Allocate;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::{BranchWhen, Capability, Operator, Probe, probe};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream, StreamCore};
use timely::order::Product;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::ActivateOnDrop;
use timely::worker::{AsWorker, Worker as TimelyWorker};

use crate::arrangement::manager::TraceBundle;
use crate::compute_state::ComputeState;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::extensions::reduce::MzReduce;
use crate::extensions::temporal_bucket::TemporalBucketing;
use crate::logging::compute::{
    ComputeEvent, DataflowGlobal, LirMapping, LirMetadata, LogDataflowErrors,
};
use crate::render::context::{ArrangementFlavor, Context, ShutdownProbe, shutdown_token};
use crate::render::continual_task::ContinualTaskCtx;
use crate::row_spine::{RowRowBatcher, RowRowBuilder};
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine, KeyBatcher, MzTimestamp};

pub mod context;
pub(crate) mod continual_task;
mod errors;
mod flat_map;
mod join;
mod reduce;
pub mod sinks;
mod threshold;
mod top_k;

pub use context::CollectionBundle;
pub use join::LinearJoinSpec;

/// Assemble the "compute"  side of a dataflow, i.e. all but the sources.
///
/// This method imports sources from provided assets, and then builds the remaining
/// dataflow using "compute-local" assets like shared arrangements, and producing
/// both arrangements and sinks.
pub fn build_compute_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
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
        .map(|(idx_id, (idx, _typ))| (*idx_id, dataflow.depends_on(idx.on_id), idx.clone()))
        .collect::<Vec<_>>();

    // Determine sinks to export, and their dependencies.
    let sinks = dataflow
        .sink_exports
        .iter()
        .map(|(sink_id, sink)| (*sink_id, dataflow.depends_on(sink.from), sink.clone()))
        .collect::<Vec<_>>();

    let worker_logging = timely_worker.logger_for("timely").map(Into::into);
    let apply_demands = COMPUTE_APPLY_COLUMN_DEMANDS.get(&compute_state.worker_config);

    // If you change the format here to something other than "Dataflow: {name}",
    // you should also update MZ_MAPPABLE_OBJECTS in `src/catalog/src/builtin.rs`
    let name = format!(
        "Dataflow: {}",
        separated(
            ", ",
            &dataflow
                .objects_to_build
                .iter()
                .map(|o| o.id.to_string())
                .collect::<Vec<_>>()
        )
    );
    let input_name = format!("InputRegion: {}", &dataflow.debug_name);
    let build_name = format!("BuildRegion: {}", &dataflow.debug_name);

    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        // TODO(ct3): This should be a config of the source instead, but at least try
        // to contain the hacks.
        let mut ct_ctx = ContinualTaskCtx::new(&dataflow);

        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        let mut imported_sources = Vec::new();
        let mut tokens: BTreeMap<_, Rc<dyn Any>> = BTreeMap::new();
        let output_probe = MzProbeHandle::default();

        scope.clone().region_named(&input_name, |region| {
            // Import declared sources into the rendering context.
            for (source_id, (source, _monotonic, upper)) in dataflow.source_imports.iter() {
                region.region_named(&format!("Source({:?})", source_id), |inner| {
                    let mut read_schema = None;
                    let mut mfp = source.arguments.operators.clone().map(|mut ops| {
                        // If enabled, we read from Persist with a `RelationDesc` that
                        // omits uneeded columns.
                        if apply_demands {
                            let demands = ops.demand();
                            let new_desc =
                                source.storage_metadata.relation_desc.apply_demand(&demands);
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

                    let mut snapshot_mode = SnapshotMode::Include;
                    let mut suppress_early_progress_as_of = dataflow.as_of.clone();
                    let ct_source_transformer = ct_ctx.get_ct_source_transformer(*source_id);
                    if let Some(x) = ct_source_transformer.as_ref() {
                        snapshot_mode = x.snapshot_mode();
                        suppress_early_progress_as_of = suppress_early_progress_as_of
                            .map(|as_of| x.suppress_early_progress_as_of(as_of));
                    }

                    // Note: For correctness, we require that sources only emit times advanced by
                    // `dataflow.as_of`. `persist_source` is documented to provide this guarantee.
                    let (mut ok_stream, err_stream, token) = persist_source::persist_source(
                        inner,
                        *source_id,
                        Arc::clone(&compute_state.persist_clients),
                        &compute_state.txns_ctx,
                        &compute_state.worker_config,
                        source.storage_metadata.clone(),
                        read_schema,
                        dataflow.as_of.clone(),
                        snapshot_mode,
                        until.clone(),
                        mfp.as_mut(),
                        compute_state.dataflow_max_inflight_bytes(),
                        start_signal.clone(),
                        ErrorHandler::Halt("compute_import"),
                    );

                    let mut source_tokens: Vec<Rc<dyn Any>> = vec![Rc::new(token)];

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

                        let (token, stream) = ok_stream.limit_progress(
                            output_probe.clone(),
                            slack,
                            limit,
                            upper.clone(),
                            name.clone(),
                        );
                        ok_stream = stream;
                        source_tokens.push(token);
                    }

                    // Attach a probe reporting the input frontier.
                    let input_probe =
                        compute_state.input_probe_for(*source_id, dataflow.export_ids());
                    ok_stream = ok_stream.probe_with(&input_probe);

                    // The `suppress_early_progress` operator and the input
                    // probe both want to work on the untransformed ct input,
                    // make sure this stays after them.
                    let (ok_stream, err_stream) = match ct_source_transformer {
                        None => (ok_stream, err_stream),
                        Some(ct_source_transformer) => {
                            let (oks, errs, ct_times) = ct_source_transformer
                                .transform(ok_stream.as_collection(), err_stream.as_collection());
                            // TODO(ct3): Ideally this would be encapsulated by
                            // ContinualTaskCtx, but the types are tricky.
                            ct_ctx.ct_times.push(ct_times.leave_region().leave_region());
                            (oks.inner, errs.inner)
                        }
                    };

                    let (oks, errs) = (
                        ok_stream.as_collection().leave_region().leave_region(),
                        err_stream.as_collection().leave_region().leave_region(),
                    );

                    imported_sources.push((mz_expr::Id::Global(*source_id), (oks, errs)));

                    // Associate returned tokens with the source identifier.
                    tokens.insert(*source_id, Rc::new(source_tokens));
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
                    context.import_index(
                        compute_state,
                        &mut tokens,
                        input_probe,
                        *idx_id,
                        &idx.desc,
                        start_signal.clone(),
                    );
                }

                // Build declared objects.
                for object in dataflow.objects_to_build {
                    let (probe, token) = shutdown_token(region);
                    context.shutdown_probe = probe;
                    tokens.insert(object.id, Rc::new(token));

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
                                .leave_region()
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
                        ct_ctx.input_times(&context.scope.parent),
                        &output_probe,
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
                    let oks = if ENABLE_TEMPORAL_BUCKETING.get(&compute_state.worker_config) {
                        let as_of = context.as_of_frontier.clone();
                        let summary = TEMPORAL_BUCKETING_SUMMARY
                            .get(&compute_state.worker_config)
                            .try_into()
                            .expect("must fit");
                        oks.inner
                            .bucket::<CapacityContainerBuilder<_>>(as_of, summary)
                            .as_collection()
                    } else {
                        oks
                    };
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
                    context.import_index(
                        compute_state,
                        &mut tokens,
                        input_probe,
                        *idx_id,
                        &idx.desc,
                        start_signal.clone(),
                    );
                }

                // Build declared objects.
                for object in dataflow.objects_to_build {
                    let (probe, token) = shutdown_token(region);
                    context.shutdown_probe = probe;
                    tokens.insert(object.id, Rc::new(token));

                    let bundle = context.scope.clone().region_named(
                        &format!("BuildingObject({:?})", object.id),
                        |region| {
                            let depends = object.plan.depends();
                            context
                                .enter_region(region, Some(&depends))
                                .render_plan(object.id, object.plan)
                                .leave_region()
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
                        ct_ctx.input_times(&context.scope.parent),
                        &output_probe,
                    );
                }
            });
        }
    })
}

// This implementation block allows child timestamps to vary from parent timestamps,
// but requires the parent timestamp to be `repr::Timestamp`.
impl<'g, G, T> Context<Child<'g, G, T>>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    T: Refines<G::Timestamp> + RenderTimestamp,
{
    pub(crate) fn import_index(
        &mut self,
        compute_state: &mut ComputeState,
        tokens: &mut BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        input_probe: probe::Handle<mz_repr::Timestamp>,
        idx_id: GlobalId,
        idx: &IndexDesc,
        start_signal: StartSignal,
    ) {
        if let Some(traces) = compute_state.traces.get_mut(&idx_id) {
            assert!(
                PartialOrder::less_equal(&traces.compaction_frontier(), &self.as_of_frontier),
                "Index {idx_id} has been allowed to compact beyond the dataflow as_of"
            );

            let token = traces.to_drop().clone();

            let (mut oks, ok_button) = traces.oks_mut().import_frontier_core(
                &self.scope.parent,
                &format!("Index({}, {:?})", idx.on_id, idx.key),
                self.as_of_frontier.clone(),
                self.until.clone(),
            );

            oks.stream = oks.stream.probe_with(&input_probe);

            let (err_arranged, err_button) = traces.errs_mut().import_frontier_core(
                &self.scope.parent,
                &format!("ErrIndex({}, {:?})", idx.on_id, idx.key),
                self.as_of_frontier.clone(),
                self.until.clone(),
            );

            let ok_arranged = oks
                .enter(&self.scope)
                .with_start_signal(start_signal.clone());
            let err_arranged = err_arranged
                .enter(&self.scope)
                .with_start_signal(start_signal);

            self.update_id(
                Id::Global(idx.on_id),
                CollectionBundle::from_expressions(
                    idx.key.clone(),
                    ArrangementFlavor::Trace(idx_id, ok_arranged, err_arranged),
                ),
            );
            tokens.insert(
                idx_id,
                Rc::new((ok_button.press_on_drop(), err_button.press_on_drop(), token)),
            );
        } else {
            panic!(
                "import of index {} failed while building dataflow {}",
                idx_id, self.dataflow_id
            );
        }
    }
}

// This implementation block requires the scopes have the same timestamp as the trace manager.
// That makes some sense, because we are hoping to deposit an arrangement in the trace manager.
impl<'g, G> Context<Child<'g, G, G::Timestamp>, G::Timestamp>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    pub(crate) fn export_index(
        &self,
        compute_state: &mut ComputeState,
        tokens: &BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        dependency_ids: BTreeSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
        output_probe: &MzProbeHandle<G::Timestamp>,
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

        match bundle.arrangement(&idx.key) {
            Some(ArrangementFlavor::Local(mut oks, mut errs)) => {
                // Ensure that the frontier does not advance past the expiration time, if set.
                // Otherwise, we might write down incorrect data.
                if let Some(&expiration) = self.dataflow_expiration.as_option() {
                    let token = Rc::new(());
                    let shutdown_token = Rc::downgrade(&token);
                    oks.stream = oks.stream.expire_stream_at(
                        &format!("{}_export_index_oks", self.debug_name),
                        expiration,
                        Weak::clone(&shutdown_token),
                    );
                    errs.stream = errs.stream.expire_stream_at(
                        &format!("{}_export_index_errs", self.debug_name),
                        expiration,
                        shutdown_token,
                    );
                    needed_tokens.push(token);
                }

                oks.stream = oks.stream.probe_notify_with(vec![output_probe.clone()]);

                // Attach logging of dataflow errors.
                if let Some(logger) = compute_state.compute_logger.clone() {
                    errs.stream.log_dataflow_errors(logger, idx_id);
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
                    &idx.key
                );
            }
        };
    }
}

// This implementation block requires the scopes have the same timestamp as the trace manager.
// That makes some sense, because we are hoping to deposit an arrangement in the trace manager.
impl<'g, G, T> Context<Child<'g, G, T>>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    T: RenderTimestamp,
{
    pub(crate) fn export_index_iterative(
        &self,
        compute_state: &mut ComputeState,
        tokens: &BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        dependency_ids: BTreeSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
        output_probe: &MzProbeHandle<G::Timestamp>,
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

        match bundle.arrangement(&idx.key) {
            Some(ArrangementFlavor::Local(oks, errs)) => {
                // TODO: The following as_collection/leave/arrange sequence could be optimized.
                //   * Combine as_collection and leave into a single function.
                //   * Use columnar to extract columns from the batches to implement leave.
                let mut oks = oks
                    .as_collection(|k, v| (k.to_row(), v.to_row()))
                    .leave()
                    .mz_arrange::<RowRowBatcher<_, _>, RowRowBuilder<_, _>, _>(
                    "Arrange export iterative",
                );

                let mut errs = errs
                    .as_collection(|k, v| (k.clone(), v.clone()))
                    .leave()
                    .mz_arrange::<ErrBatcher<_, _>, ErrBuilder<_, _>, _>(
                        "Arrange export iterative err",
                    );

                // Ensure that the frontier does not advance past the expiration time, if set.
                // Otherwise, we might write down incorrect data.
                if let Some(&expiration) = self.dataflow_expiration.as_option() {
                    let token = Rc::new(());
                    let shutdown_token = Rc::downgrade(&token);
                    oks.stream = oks.stream.expire_stream_at(
                        &format!("{}_export_index_iterative_oks", self.debug_name),
                        expiration,
                        Weak::clone(&shutdown_token),
                    );
                    errs.stream = errs.stream.expire_stream_at(
                        &format!("{}_export_index_iterative_err", self.debug_name),
                        expiration,
                        shutdown_token,
                    );
                    needed_tokens.push(token);
                }

                oks.stream = oks.stream.probe_notify_with(vec![output_probe.clone()]);

                // Attach logging of dataflow errors.
                if let Some(logger) = compute_state.compute_logger.clone() {
                    errs.stream.log_dataflow_errors(logger, idx_id);
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
                    &idx.key
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

impl<G> Context<G>
where
    G: Scope<Timestamp = Product<mz_repr::Timestamp, PointStamp<u64>>>,
{
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
    ) -> CollectionBundle<G> {
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
                                .leave_region()
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
                use differential_dataflow::operators::iterate::Variable;
                let inner = feedback_summary::<u64>(level + 1, 1);
                let oks_v = Variable::new(
                    &mut self.scope,
                    Product::new(Default::default(), inner.clone()),
                );
                let err_v = Variable::new(&mut self.scope, Product::new(Default::default(), inner));

                self.insert_id(
                    Id::Local(*id),
                    CollectionBundle::from_collections(oks_v.clone(), err_v.clone()),
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
                self.insert_id(Id::Local(id), bundle);
                let (oks_v, err_v) = variables.remove(&Id::Local(id)).unwrap();

                // Set oks variable to `oks` but consolidated to ensure iteration ceases at fixed point.
                let mut oks = oks.consolidate_named::<KeyBatcher<_, _, _>>("LetRecConsolidation");

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
                    oks = Collection::new(in_limit);
                    if !limit.return_at_limit {
                        err = err.concat(&Collection::new(over_limit).map(move |_data| {
                            DataflowError::EvalError(Box::new(EvalError::LetRecLimitExceeded(
                                format!("{}", limit.max_iters.get()).into(),
                            )))
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
                    .mz_arrange::<ErrBatcher<_, _>, ErrBuilder<_, _>, ErrSpine<_, _>>(
                        "Arrange recursive err",
                    )
                    .mz_reduce_abelian::<_, ErrBuilder<_, _>, ErrSpine<_, _>>(
                        "Distinct recursive err",
                        move |_k, _s, t| t.push(((), Diff::ONE)),
                    )
                    .as_collection(|k, _| k.clone());

                // Actively interrupt the data flow during shutdown, to ensure we won't keep
                // iterating for long (or even forever).
                let oks = render_shutdown_fuse(oks, self.shutdown_probe.clone());
                let errs = render_shutdown_fuse(errs, self.shutdown_probe.clone());

                oks_v.set(&oks);
                err_v.set(&errs);
            }
            // Now extract each of the rec bindings into the outer scope.
            for id in rec_ids.into_iter() {
                let bundle = self.remove_id(Id::Local(id)).unwrap();
                let (oks, err) = bundle.collection.unwrap();
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

impl<G> Context<G>
where
    G: Scope,
    G::Timestamp: RenderTimestamp,
{
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
    fn render_plan(&mut self, object_id: GlobalId, plan: RenderPlan) -> CollectionBundle<G> {
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
                                .leave_region()
                        });
                self.insert_id(Id::Local(id), bundle);
            }
        }

        self.scope.clone().region_named("Main Body", |region| {
            let depends = plan.body.depends();
            self.enter_region(region, Some(&depends))
                .render_letfree_plan(object_id, plan.body, BindingInfo::Body { in_let })
                .leave_region()
        })
    }

    /// Renders a let-free plan to a differential dataflow, producing the collection of results.
    fn render_letfree_plan(
        &mut self,
        object_id: GlobalId,
        plan: LetFreePlan,
        binding: BindingInfo,
    ) -> CollectionBundle<G> {
        let (mut nodes, root_id, topological_order) = plan.destruct();

        // Rendered collections by their `LirId`.
        let mut collections = BTreeMap::new();

        // Mappings to send along.
        // To save overhead, we'll only compute mappings when we need to,
        // which means things get gated behind options. Unfortuantely, that means we
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

                let operator_id_start = self.scope.peek_identifier();
                Some((operator, operator_id_start))
            } else {
                None
            };

            let mut bundle = self.render_plan_expr(node.expr, &collections);

            if let Some((operator, operator_id_start)) = metadata {
                let operator_id_end = self.scope.peek_identifier();
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
        &mut self,
        expr: render_plan::Expr,
        collections: &BTreeMap<LirId, CollectionBundle<G>>,
    ) -> CollectionBundle<G> {
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
                                <G::Timestamp as Refines<mz_repr::Timestamp>>::to_inner(time),
                                diff,
                            ))
                        } else {
                            None
                        }
                    })
                    .to_stream(&mut self.scope)
                    .as_collection();

                let mut error_time: mz_repr::Timestamp = Timestamp::minimum();
                error_time.advance_by(self.as_of_frontier.borrow());
                let err_collection = errs
                    .into_iter()
                    .map(move |e| {
                        (
                            DataflowError::from(e),
                            <G::Timestamp as Refines<mz_repr::Timestamp>>::to_inner(error_time),
                            Diff::ONE,
                        )
                    })
                    .to_stream(&mut self.scope)
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
            } => {
                let input = expect_input(input);
                let mfp_option = (!mfp_after.is_identity()).then_some(mfp_after);
                self.render_reduce(input_key, input, key_val_plan, plan, mfp_option)
            }
            TopK { input, top_k_plan } => {
                let input = expect_input(input);
                self.render_topk(input, top_k_plan)
            }
            Negate { input } => {
                let input = expect_input(input);
                let (oks, errs) = input.as_specific_collection(None, &self.config_set);
                CollectionBundle::from_collections(oks.negate(), errs)
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
            } => {
                let mut oks = Vec::new();
                let mut errs = Vec::new();
                for input in inputs.into_iter() {
                    let (os, es) =
                        expect_input(input).as_specific_collection(None, &self.config_set);
                    oks.push(os);
                    errs.push(es);
                }
                let mut oks = differential_dataflow::collection::concatenate(&mut self.scope, oks);
                if consolidate_output {
                    oks = oks.consolidate_named::<KeyBatcher<_, _, _>>("UnionConsolidation")
                }
                let errs = differential_dataflow::collection::concatenate(&mut self.scope, errs);
                CollectionBundle::from_collections(oks, errs)
            }
            ArrangeBy {
                input_key,
                input,
                input_mfp,
                forms: keys,
            } => {
                let input = expect_input(input);
                input.ensure_collections(
                    keys,
                    input_key,
                    input_mfp,
                    self.until.clone(),
                    &self.config_set,
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

    fn log_operator_hydration(&self, bundle: &mut CollectionBundle<G>, lir_id: LirId) {
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
                        a.stream = self.log_operator_hydration_inner(&a.stream, lir_id);
                    }
                    Trace(_, a, _) => {
                        a.stream = self.log_operator_hydration_inner(&a.stream, lir_id);
                    }
                }
            }
            None => {
                let (oks, _) = bundle
                    .collection
                    .as_mut()
                    .expect("CollectionBundle invariant");
                let stream = self.log_operator_hydration_inner(&oks.inner, lir_id);
                *oks = stream.as_collection();
            }
        }
    }

    fn log_operator_hydration_inner<D>(&self, stream: &Stream<G, D>, lir_id: LirId) -> Stream<G, D>
    where
        D: Clone + 'static,
    {
        let Some(logger) = self.hydration_logger.clone() else {
            return stream.clone(); // hydration logging disabled
        };

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
            logger.log(lir_id, hydrated);

            move |input, output| {
                // Pass through inputs.
                input.for_each(|cap, data| {
                    output.session(&cap).give_container(data);
                });

                if hydrated {
                    return;
                }

                let frontier = input.frontier().frontier();
                if PartialOrder::less_equal(&hydration_frontier.borrow(), &frontier) {
                    hydrated = true;
                    logger.log(lir_id, hydrated);
                }
            }
        })
    }
}

#[allow(dead_code)] // Some of the methods on this trait are unused, but useful to have.
/// A timestamp type that can be used for operations within MZ's dataflow layer.
pub trait RenderTimestamp: MzTimestamp + Refines<mz_repr::Timestamp> {
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
        let mut vec = inner.into_vec();
        for item in vec.iter_mut() {
            *item = item.saturating_sub(1);
        }
        Product::new(self.outer.saturating_sub(1), PointStamp::new(vec))
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

impl<S, Tr> WithStartSignal for Arranged<S, Tr>
where
    S: Scope,
    S::Timestamp: RenderTimestamp,
    Tr: TraceReader + Clone,
{
    fn with_start_signal(self, signal: StartSignal) -> Self {
        Arranged {
            stream: self.stream.with_start_signal(signal),
            trace: self.trace,
        }
    }
}

impl<S, D> WithStartSignal for Stream<S, D>
where
    S: Scope,
    D: timely::Data,
{
    fn with_start_signal(self, signal: StartSignal) -> Self {
        self.unary(Pipeline, "StartSignal", |_cap, info| {
            let token = Box::new(ActivateOnDrop::new(
                (),
                info.address,
                self.scope().activations(),
            ));
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

/// Wraps the provided `Collection` with an operator that passes through all received inputs as
/// long as the provided `ShutdownProbe` does not announce dataflow shutdown. Once the token
/// dataflow is shutting down, all data flowing into the operator is dropped.
fn render_shutdown_fuse<G, D>(
    collection: Collection<G, D, Diff>,
    probe: ShutdownProbe,
) -> Collection<G, D, Diff>
where
    G: Scope,
    D: Data,
{
    let stream = collection.inner;
    let stream = stream.unary(Pipeline, "ShutdownFuse", move |_cap, _info| {
        move |input, output| {
            input.for_each(|cap, data| {
                if !probe.in_shutdown() {
                    output.session(&cap).give_container(data);
                }
            });
        }
    });
    stream.as_collection()
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
fn suppress_early_progress<G, D>(
    stream: Stream<G, D>,
    as_of: Antichain<G::Timestamp>,
) -> Stream<G, D>
where
    G: Scope,
    D: Data,
{
    stream.unary_frontier(Pipeline, "SuppressEarlyProgress", |default_cap, _info| {
        let mut early_cap = Some(default_cap);

        move |input, output| {
            input.for_each(|data_cap, data| {
                let mut session = if as_of.less_than(data_cap.time()) {
                    output.session(&data_cap)
                } else {
                    let cap = early_cap.as_ref().expect("early_cap can't be dropped yet");
                    output.session(cap)
                };
                session.give_container(data);
            });

            let frontier = input.frontier().frontier();
            if !PartialOrder::less_equal(&frontier, &as_of.borrow()) {
                early_cap.take();
            }
        }
    })
}

/// Extension trait for [`StreamCore`] to selectively limit progress.
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
    ///
    /// Returns a shutdown button and the stream with the progress limiting applied.
    fn limit_progress(
        &self,
        handle: MzProbeHandle<T>,
        slack_ms: u64,
        limit: Option<usize>,
        upper: Antichain<T>,
        name: String,
    ) -> (Rc<dyn Any>, Self);
}

// TODO: We could make this generic over a `T` that can be converted to and from a u64 millisecond
// number.
impl<G, D, R> LimitProgress<mz_repr::Timestamp> for StreamCore<G, Vec<(D, mz_repr::Timestamp, R)>>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    D: timely::Data,
    R: timely::Data,
{
    fn limit_progress(
        &self,
        handle: MzProbeHandle<mz_repr::Timestamp>,
        slack_ms: u64,
        limit: Option<usize>,
        upper: Antichain<mz_repr::Timestamp>,
        name: String,
    ) -> (Rc<dyn Any>, Self) {
        let mut button = None;

        let stream =
            self.unary_frontier(Pipeline, &format!("LimitProgress({name})"), |_cap, info| {
                // Times that we've observed on our input.
                let mut pending_times: BTreeSet<G::Timestamp> = BTreeSet::new();
                // Capability for the lower bound of `pending_times`, if any.
                let mut retained_cap: Option<Capability<G::Timestamp>> = None;

                let activator = self.scope().activator_for(info.address);
                handle.activate(activator.clone());

                let shutdown = Rc::new(());
                button = Some(ShutdownButton::new(
                    Rc::new(RefCell::new(Some(Rc::clone(&shutdown)))),
                    activator,
                ));
                let shutdown = Rc::downgrade(&shutdown);

                move |input, output| {
                    // We've been shut down, release all resources and consume inputs.
                    if shutdown.strong_count() == 0 {
                        retained_cap = None;
                        pending_times.clear();
                        while let Some(_) = input.next() {}
                        return;
                    }

                    while let Some((cap, data)) = input.next() {
                        for time in data
                            .iter()
                            .flat_map(|(_, time, _)| u64::from(time).checked_add(slack_ms))
                        {
                            let rounded_time =
                                (time / slack_ms).saturating_add(1).saturating_mul(slack_ms);
                            if !upper.less_than(&rounded_time.into()) {
                                pending_times.insert(rounded_time.into());
                            }
                        }
                        output.session(&cap).give_container(data);
                        if retained_cap.as_ref().is_none_or(|c| {
                            !c.time().less_than(cap.time()) && !upper.less_than(cap.time())
                        }) {
                            retained_cap = Some(cap.retain());
                        }
                    }

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

                    if input.frontier.is_empty() {
                        retained_cap = None;
                        pending_times.clear();
                    }

                    if !pending_times.is_empty() {
                        tracing::debug!(
                            name,
                            info.global_id,
                            pending_times = %PendingTimesDisplay(pending_times.iter().cloned()),
                            frontier = ?input.frontier.frontier().get(0),
                            probe = ?handle.with_frontier(|f| f.get(0).cloned()),
                            ?upper,
                            "pending times",
                        );
                    }
                }
            });
        (Rc::new(button.unwrap().press_on_drop()), stream)
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
