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

use std::collections::{BTreeMap, BTreeSet};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};
use itertools::izip;
use mz_compute_client::plan::Plan;
use mz_compute_client::types::dataflows::{BuildDesc, DataflowDescription, IndexDesc};
use mz_expr::Id;
use mz_repr::{GlobalId, Row};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::source::persist_source;
use mz_storage_client::source::persist_source::FlowControl;
use mz_storage_client::types::errors::DataflowError;
use mz_timely_util::arrange::{IntoKeyCollection, MzArrange};
use mz_timely_util::operator::{CollectionExt, ConsolidateExt};
use mz_timely_util::probe::{self, ProbeNotify};
use mz_timely_util::reduce::MzReduce;
use timely::communication::Allocate;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::BranchWhen;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::order::Product;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::worker::Worker as TimelyWorker;
use timely::PartialOrder;

use crate::arrangement::manager::TraceBundle;
use crate::compute_state::ComputeState;
use crate::logging::compute::LogImportFrontiers;
use crate::render::context::{ArrangementFlavor, Context, ShutdownToken};
use crate::typedefs::{ErrSpine, RowKeySpine};

pub mod context;
mod errors;
mod flat_map;
mod join;
mod reduce;
pub mod sinks;
mod threshold;
mod top_k;

pub use context::CollectionBundle;
pub use join::LinearJoinImpl;

/// Assemble the "compute"  side of a dataflow, i.e. all but the sources.
///
/// This method imports sources from provided assets, and then builds the remaining
/// dataflow using "compute-local" assets like shared arrangements, and producing
/// both arrangements and sinks.
pub fn build_compute_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    compute_state: &mut ComputeState,
    dataflow: DataflowDescription<Plan, CollectionMetadata>,
) {
    // Mutually recursive view definitions require special handling.
    let recursive = dataflow.objects_to_build.iter().any(|object| {
        if let Plan::LetRec { .. } = object.plan {
            true
        } else {
            false
        }
    });

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

    let worker_logging = timely_worker.log_register().get("timely");

    // Probe providing feedback to `persist_source` flow control.
    // Only set if the dataflow instantiates any `persist_source`s.
    let mut flow_control_probe: Option<probe::Handle<_>> = None;

    let name = format!("Dataflow: {}", &dataflow.debug_name);
    let input_name = format!("InputRegion: {}", &dataflow.debug_name);
    let build_name = format!("BuildRegion: {}", &dataflow.debug_name);

    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        let mut imported_sources = Vec::new();
        let mut tokens = BTreeMap::new();
        scope.clone().region_named(&input_name, |region| {
            // Import declared sources into the rendering context.
            for (source_id, (source, _monotonic)) in dataflow.source_imports.iter() {
                region.region_named(&format!("Source({:?})", source_id), |inner| {
                    let mut mfp = source.arguments.operators.clone().map(|ops| {
                        mz_expr::MfpPlan::create_from(ops)
                            .expect("Linear operators should always be valid")
                    });

                    let probe = flow_control_probe.get_or_insert_with(Default::default);
                    let flow_control_input = probe::source(
                        inner.clone(),
                        format!("flow_control_input({source_id})"),
                        probe.clone(),
                    );
                    let flow_control = FlowControl {
                        progress_stream: flow_control_input,
                        max_inflight_bytes: compute_state.dataflow_max_inflight_bytes,
                    };

                    // Note: For correctness, we require that sources only emit times advanced by
                    // `dataflow.as_of`. `persist_source` is documented to provide this guarantee.
                    let (mut ok_stream, err_stream, token) = persist_source::persist_source(
                        inner,
                        *source_id,
                        Arc::clone(&compute_state.persist_clients),
                        source.storage_metadata.clone(),
                        dataflow.as_of.clone(),
                        dataflow.until.clone(),
                        mfp.as_mut(),
                        Some(flow_control),
                        // Copy the logic in DeltaJoin/Get/Join to start.
                        |_timer, count| count > 1_000_000,
                    );

                    // If `mfp` is non-identity, we need to apply what remains.
                    // For the moment, assert that it is either trivial or `None`.
                    assert!(mfp.map(|x| x.is_identity()).unwrap_or(true));

                    // If logging is enabled, log source frontier advancements. Note that we do
                    // this here instead of in the server.rs worker loop since we want to catch the
                    // wall-clock time of the frontier advancement for each dataflow as early as
                    // possible.
                    if let Some(logger) = compute_state.compute_logger.clone() {
                        let export_ids = dataflow.export_ids().collect();
                        ok_stream = ok_stream.log_import_frontiers(logger, *source_id, export_ids);
                    }

                    let (oks, errs) = (
                        ok_stream.as_collection().leave_region().leave_region(),
                        err_stream.as_collection().leave_region().leave_region(),
                    );

                    imported_sources.push((mz_expr::Id::Global(*source_id), (oks, errs)));

                    // Associate returned tokens with the source identifier.
                    tokens.insert(*source_id, token);
                });
            }
        });

        // Collect flow control probes for this dataflow.
        let index_ids = dataflow.index_imports.keys();
        let output_probes: Vec<_> = index_ids
            .flat_map(|id| compute_state.flow_control_probes.get(id))
            .flatten()
            .cloned()
            .chain(flow_control_probe)
            .collect();

        // If there exists a recursive expression, we'll need to use a non-region scope,
        // in order to support additional timestamp coordinates for iteration.
        if recursive {
            scope.clone().iterative::<PointStamp<u64>, _, _>(|region| {
                let mut context = Context::for_dataflow_in(&dataflow, region.clone());
                context.linear_join_impl = compute_state.linear_join_impl;

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
                    let export_ids = dataflow.export_ids().collect();
                    context.import_index(compute_state, &mut tokens, export_ids, *idx_id, &idx.0);
                }

                // Build declared objects.
                for object in dataflow.objects_to_build {
                    let object_token = Rc::new(());
                    context.shutdown_token = ShutdownToken::new(Rc::downgrade(&object_token));
                    tokens.insert(object.id, object_token);

                    let bundle = context.render_recursive_plan(0, object.plan);
                    context.insert_id(Id::Global(object.id), bundle);
                }

                // Export declared indexes.
                for (idx_id, dependencies, idx) in indexes {
                    context.export_index_iterative(
                        compute_state,
                        &mut tokens,
                        dependencies,
                        idx_id,
                        &idx,
                        output_probes.clone(),
                    );
                }

                // Export declared sinks.
                for (sink_id, dependencies, sink) in sinks {
                    context.export_sink(
                        compute_state,
                        &mut tokens,
                        dependencies,
                        sink_id,
                        &sink,
                        output_probes.clone(),
                    );
                }
            });
        } else {
            scope.clone().region_named(&build_name, |region| {
                let mut context = Context::for_dataflow_in(&dataflow, region.clone());
                context.linear_join_impl = compute_state.linear_join_impl;

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
                    let export_ids = dataflow.export_ids().collect();
                    context.import_index(compute_state, &mut tokens, export_ids, *idx_id, &idx.0);
                }

                // Build declared objects.
                for object in dataflow.objects_to_build {
                    let object_token = Rc::new(());
                    context.shutdown_token = ShutdownToken::new(Rc::downgrade(&object_token));
                    tokens.insert(object.id, object_token);

                    context.build_object(object);
                }

                // Export declared indexes.
                for (idx_id, dependencies, idx) in indexes {
                    context.export_index(
                        compute_state,
                        &mut tokens,
                        dependencies,
                        idx_id,
                        &idx,
                        output_probes.clone(),
                    );
                }

                // Export declared sinks.
                for (sink_id, dependencies, sink) in sinks {
                    context.export_sink(
                        compute_state,
                        &mut tokens,
                        dependencies,
                        sink_id,
                        &sink,
                        output_probes.clone(),
                    );
                }
            });
        }
    })
}

// This implementation block allows child timestamps to vary from parent timestamps,
// but requires the parent timestamp to be `repr::Timestamp`.
impl<'g, G, T> Context<Child<'g, G, T>, Row>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    T: Refines<G::Timestamp> + RenderTimestamp,
{
    pub(crate) fn import_index(
        &mut self,
        compute_state: &mut ComputeState,
        tokens: &mut BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        export_ids: Vec<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
    ) {
        if let Some(traces) = compute_state.traces.get_mut(&idx_id) {
            assert!(
                PartialOrder::less_equal(&traces.compaction_frontier(), &self.as_of_frontier),
                "Index {idx_id} has been allowed to compact beyond the dataflow as_of"
            );

            let token = traces.to_drop().clone();
            let (mut ok_arranged, ok_button) = traces.oks_mut().import_frontier_core(
                &self.scope.parent,
                &format!("Index({}, {:?})", idx.on_id, idx.key),
                self.as_of_frontier.clone(),
                self.until.clone(),
            );
            let (err_arranged, err_button) = traces.errs_mut().import_frontier_core(
                &self.scope.parent,
                &format!("ErrIndex({}, {:?})", idx.on_id, idx.key),
                self.as_of_frontier.clone(),
                self.until.clone(),
            );

            // If logging is enabled, log import frontier advancements. Note that we do
            // this here instead of in the server.rs worker loop since we want to catch the
            // wall-clock time of the frontier advancement for each dataflow as early as
            // possible.
            if let Some(logger) = compute_state.compute_logger.clone() {
                ok_arranged = ok_arranged.log_import_frontiers(logger, idx_id, export_ids);
            }

            let ok_arranged = ok_arranged.enter(&self.scope);
            let err_arranged = err_arranged.enter(&self.scope);

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

// This implementation block allows child timestamps to vary from parent timestamps.
impl<G> Context<G, Row>
where
    G: Scope,
    G::Timestamp: RenderTimestamp,
{
    pub(crate) fn build_object(&mut self, object: BuildDesc<Plan>) {
        // First, transform the relation expression into a render plan.
        let bundle = self.render_plan(object.plan);
        self.insert_id(Id::Global(object.id), bundle);
    }
}

// This implementation block requires the scopes have the same timestamp as the trace manager.
// That makes some sense, because we are hoping to deposit an arrangement in the trace manager.
impl<'g, G> Context<Child<'g, G, G::Timestamp>, Row, G::Timestamp>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    pub(crate) fn export_index(
        &mut self,
        compute_state: &mut ComputeState,
        tokens: &mut BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        dependency_ids: BTreeSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
        probes: Vec<probe::Handle<mz_repr::Timestamp>>,
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

        compute_state
            .flow_control_probes
            .insert(idx_id, probes.clone());

        match bundle.arrangement(&idx.key) {
            Some(ArrangementFlavor::Local(oks, errs)) => {
                // Set up probes to notify on index frontier advancement.
                oks.stream.probe_notify_with(probes);
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
impl<'g, G, T> Context<Child<'g, G, T>, Row>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    T: RenderTimestamp,
{
    pub(crate) fn export_index_iterative(
        &mut self,
        compute_state: &mut ComputeState,
        tokens: &mut BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        dependency_ids: BTreeSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
        probes: Vec<probe::Handle<mz_repr::Timestamp>>,
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

        compute_state
            .flow_control_probes
            .insert(idx_id, probes.clone());

        match bundle.arrangement(&idx.key) {
            Some(ArrangementFlavor::Local(oks, errs)) => {
                let oks = oks
                    .as_collection(|k, v| (k.clone(), v.clone()))
                    .leave()
                    .mz_arrange("Arrange export iterative");
                oks.stream.probe_notify_with(probes);
                let errs = errs
                    .as_collection(|k, v| (k.clone(), v.clone()))
                    .leave()
                    .mz_arrange("Arrange export iterative err");
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

impl<G> Context<G, Row>
where
    G: Scope<Timestamp = Product<mz_repr::Timestamp, PointStamp<u64>>>,
{
    /// Renders a plan to a differential dataflow, producing the collection of results.
    ///
    /// This method allows for `plan` to contain a `LetRec` variant at its root, and is planned
    /// in the context of `level` pre-existing iteration coordinates.
    ///
    /// This method recursively descends `LetRec` nodes, establishing nested scopes for each
    /// and establishing the appropriate recursive dependencies among the bound variables.
    /// Once non-`LetRec` nodes are reached it calls in to `render_plan` which will error if
    /// further `LetRec` variants are found.
    ///
    /// The method requires that all variables conclude with a physical representation that
    /// contains a collection (i.e. a non-arrangement), and it will panic otherwise.
    pub fn render_recursive_plan(&mut self, level: usize, plan: Plan) -> CollectionBundle<G, Row> {
        if let Plan::LetRec {
            ids,
            values,
            max_iters,
            body,
        } = plan
        {
            assert_eq!(ids.len(), values.len());
            assert_eq!(ids.len(), max_iters.len());
            // It is important that we only use the `Variable` until the object is bound.
            // At that point, all subsequent uses should have access to the object itself.
            let mut variables = BTreeMap::new();
            for id in ids.iter() {
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
            // Now render each of the bindings.
            for (id, value, max_iter) in
                izip!(ids.iter(), values.into_iter(), max_iters.into_iter())
            {
                let bundle = self.render_recursive_plan(level + 1, value);
                // We need to ensure that the raw collection exists, but do not have enough information
                // here to cause that to happen.
                let (oks, err) = bundle.collection.clone().unwrap();
                self.insert_id(Id::Local(*id), bundle);
                let (oks_v, err_v) = variables.remove(&Id::Local(*id)).unwrap();

                // Set oks variable to `oks` but consolidated to ensure iteration ceases at fixed point.
                let mut oks = oks.mz_consolidate::<RowKeySpine<_, _, _>>("LetRecConsolidation");
                if let Some(token) = &self.shutdown_token.get_inner() {
                    oks = oks.with_token(Weak::clone(token));
                }

                if let Some(max_iter) = max_iter {
                    // We swallow the results of the `max_iter`th iteration, because
                    // these results would go into the `max_iter + 1`th iteration.
                    let (in_limit, _over_limit) =
                        oks.inner.branch_when(move |Product { inner: ps, .. }| {
                            // We get None in the first iteration, because the `PointStamp` doesn't yet have
                            // the `level`th element. It will get created when applying the summary for the
                            // first time.
                            let iteration_index = *ps.vector.get(level).unwrap_or(&0);
                            // The pointstamp starts counting from 0, so we need to add 1.
                            iteration_index + 1 >= max_iter.into()
                        });
                    oks = Collection::new(in_limit);
                }

                oks_v.set(&oks);

                // Set err variable to the distinct elements of `err`.
                // Distinctness is important, as we otherwise might add the same error each iteration,
                // say if the limit of `oks` has an error. This would result in non-terminating rather
                // than a clean report of the error. The trade-off is that we lose information about
                // multiplicities of errors, but .. this seems to be the better call.
                let mut errs = err
                    .into_key_collection()
                    .mz_arrange::<ErrSpine<DataflowError, _, _>>("Arrange recursive err")
                    .mz_reduce_abelian::<_, ErrSpine<_, _, _>>(
                        "Distinct recursive err",
                        move |_k, _s, t| t.push(((), 1)),
                    )
                    .as_collection(|k, _| k.clone());
                if let Some(token) = &self.shutdown_token.get_inner() {
                    errs = errs.with_token(Weak::clone(token));
                }
                err_v.set(&errs);
            }
            // Now extract each of the bindings into the outer scope.
            for id in ids.into_iter() {
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

            self.render_recursive_plan(level, *body)
        } else {
            self.render_plan(plan)
        }
    }
}

impl<G> Context<G, Row>
where
    G: Scope,
    G::Timestamp: RenderTimestamp,
{
    /// Renders a plan to a differential dataflow, producing the collection of results.
    ///
    /// The return type reflects the uncertainty about the data representation, perhaps
    /// as a stream of data, perhaps as an arrangement, perhaps as a stream of batches.
    pub fn render_plan(&mut self, plan: Plan) -> CollectionBundle<G, Row> {
        match plan {
            Plan::Constant { rows } => {
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
                            1,
                        )
                    })
                    .to_stream(&mut self.scope)
                    .as_collection();

                CollectionBundle::from_collections(ok_collection, err_collection)
            }
            Plan::Get { id, keys, plan } => {
                // Recover the collection from `self` and then apply `mfp` to it.
                // If `mfp` happens to be trivial, we can just return the collection.
                let mut collection = self
                    .lookup_id(id)
                    .unwrap_or_else(|| panic!("Get({:?}) not found at render time", id));
                match plan {
                    mz_compute_client::plan::GetPlan::PassArrangements => {
                        // Assert that each of `keys` are present in `collection`.
                        assert!(keys
                            .arranged
                            .iter()
                            .all(|(key, _, _)| collection.arranged.contains_key(key)));
                        assert!(keys.raw <= collection.collection.is_some());
                        // Retain only those keys we want to import.
                        collection.arranged.retain(|key, _value| {
                            keys.arranged.iter().any(|(key2, _, _)| key2 == key)
                        });
                        collection
                    }
                    mz_compute_client::plan::GetPlan::Arrangement(key, row, mfp) => {
                        let (oks, errs) = collection.as_collection_core(
                            mfp,
                            Some((key, row)),
                            self.until.clone(),
                        );
                        CollectionBundle::from_collections(oks, errs)
                    }
                    mz_compute_client::plan::GetPlan::Collection(mfp) => {
                        let (oks, errs) =
                            collection.as_collection_core(mfp, None, self.until.clone());
                        CollectionBundle::from_collections(oks, errs)
                    }
                }
            }
            Plan::Let { id, value, body } => {
                // Render `value` and bind it to `id`. Complain if this shadows an id.
                let value = self.render_plan(*value);
                let prebound = self.insert_id(Id::Local(id), value);
                assert!(prebound.is_none());

                let body = self.render_plan(*body);
                self.remove_id(Id::Local(id));
                body
            }
            Plan::LetRec { .. } => {
                unreachable!("LetRec should have been extracted and rendered");
            }
            Plan::Mfp {
                input,
                mfp,
                input_key_val,
            } => {
                let input = self.render_plan(*input);
                // If `mfp` is non-trivial, we should apply it and produce a collection.
                if mfp.is_identity() {
                    input
                } else {
                    let (oks, errs) =
                        input.as_collection_core(mfp, input_key_val, self.until.clone());
                    CollectionBundle::from_collections(oks, errs)
                }
            }
            Plan::FlatMap {
                input,
                func,
                exprs,
                mfp,
                input_key,
            } => {
                let input = self.render_plan(*input);
                self.render_flat_map(input, func, exprs, mfp, input_key)
            }
            Plan::Join { inputs, plan } => {
                let inputs = inputs
                    .into_iter()
                    .map(|input| self.render_plan(input))
                    .collect();
                match plan {
                    mz_compute_client::plan::join::JoinPlan::Linear(linear_plan) => {
                        self.render_join(inputs, linear_plan)
                    }
                    mz_compute_client::plan::join::JoinPlan::Delta(delta_plan) => {
                        self.render_delta_join(inputs, delta_plan)
                    }
                }
            }
            Plan::Reduce {
                input,
                key_val_plan,
                plan,
                input_key,
            } => {
                let input = self.render_plan(*input);
                self.render_reduce(input, key_val_plan, plan, input_key)
            }
            Plan::TopK { input, top_k_plan } => {
                let input = self.render_plan(*input);
                self.render_topk(input, top_k_plan)
            }
            Plan::Negate { input } => {
                let input = self.render_plan(*input);
                let (oks, errs) = input.as_specific_collection(None);
                CollectionBundle::from_collections(oks.negate(), errs)
            }
            Plan::Threshold {
                input,
                threshold_plan,
            } => {
                let input = self.render_plan(*input);
                self.render_threshold(input, threshold_plan)
            }
            Plan::Union { inputs } => {
                let mut oks = Vec::new();
                let mut errs = Vec::new();
                for input in inputs.into_iter() {
                    let (os, es) = self.render_plan(input).as_specific_collection(None);
                    oks.push(os);
                    errs.push(es);
                }
                let oks = differential_dataflow::collection::concatenate(&mut self.scope, oks);
                let errs = differential_dataflow::collection::concatenate(&mut self.scope, errs);
                CollectionBundle::from_collections(oks, errs)
            }
            Plan::ArrangeBy {
                input,
                forms: keys,
                input_key,
                input_mfp,
            } => {
                let input = self.render_plan(*input);
                input.ensure_collections(keys, input_key, input_mfp, self.until.clone())
            }
        }
    }
}

/// A timestamp type that can be used for operations within MZ's dataflow layer.
pub trait RenderTimestamp: Timestamp + Lattice + Refines<mz_repr::Timestamp> {
    /// The system timestamp component of the timestamp.
    ///
    /// This is useful for manipulating the system time, as when delaying
    /// updates for subsequent cancellation, as with monotonic reduction.
    fn system_time(&mut self) -> &mut mz_repr::Timestamp;
    /// Effects a system delay in terms of the timestamp summary.
    fn system_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary;
    /// The event timestamp component of the timestamp.
    fn event_time(&mut self) -> &mut mz_repr::Timestamp;
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
    fn event_time(&mut self) -> &mut mz_repr::Timestamp {
        self
    }
    fn event_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary {
        delay
    }
    fn step_back(&self) -> Self {
        self.saturating_sub(1)
    }
}

impl<T: Timestamp + Lattice> RenderTimestamp for Product<mz_repr::Timestamp, T> {
    fn system_time(&mut self) -> &mut mz_repr::Timestamp {
        &mut self.outer
    }
    fn system_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary {
        Product::new(delay, Default::default())
    }
    fn event_time(&mut self) -> &mut mz_repr::Timestamp {
        &mut self.outer
    }
    fn event_delay(delay: mz_repr::Timestamp) -> <Self as Timestamp>::Summary {
        Product::new(delay, Default::default())
    }
    fn step_back(&self) -> Self {
        Product::new(self.outer.saturating_sub(1), self.inner.clone())
    }
}
