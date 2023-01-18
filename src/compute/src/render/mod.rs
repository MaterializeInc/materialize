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
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::AsCollection;
use timely::communication::Allocate;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::InspectCore;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::order::Product;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::worker::Worker as TimelyWorker;
use timely::PartialOrder;

use mz_compute_client::plan::Plan;
use mz_compute_client::types::dataflows::{BuildDesc, DataflowDescription, IndexDesc};
use mz_expr::Id;
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::source::persist_source;
use mz_storage_client::source::persist_source::FlowControl;
use mz_storage_client::types::errors::DataflowError;
use mz_timely_util::probe::{self, ProbeNotify};

use crate::arrangement::manager::TraceBundle;
use crate::compute_state::ComputeState;
use crate::logging::compute::ComputeEvent;
use crate::logging::compute::Logger;
pub use context::CollectionBundle;
use context::{ArrangementFlavor, Context};

pub mod context;
mod flat_map;
mod join;
mod reduce;
pub mod sinks;
mod threshold;
mod top_k;

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

    // Determine indexes to export
    let indexes = dataflow
        .index_exports
        .iter()
        .map(|(idx_id, (idx, _typ))| (*idx_id, dataflow.depends_on(idx.on_id), idx.clone()))
        .collect::<Vec<_>>();

    // Determine sinks to export
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
                        // TODO: Enable flow control with a sensible limit value. This is currently
                        // blocked by a bug in `persist_source` (#16995).
                        max_inflight_bytes: usize::MAX,
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

                    // If logging is enabled, intercept frontier advancements coming from persist to track materialization lags.
                    // Note that we do this here instead of in the server.rs worker loop since we want to catch the wall-clock
                    // time of the frontier advancement for each dataflow as early as possible.
                    if let Some(logger) = compute_state.compute_logger.clone() {
                        let export_ids = dataflow.export_ids().collect();
                        ok_stream = intercept_source_instantiation_frontiers(
                            &ok_stream, logger, *source_id, export_ids,
                        );
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

        if recursive {
            scope.clone().iterative::<usize, _, _>(|region| {
                let mut context =
                    crate::render::context::Context::for_dataflow_in(&dataflow, region.clone());

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
                    context.import_index(compute_state, &mut tokens, *idx_id, &idx.0);
                }

                // Build declared objects.
                let mut any_letrec = false;
                for object in dataflow.objects_to_build {
                    if let Plan::LetRec { ids, values, body } = object.plan {
                        assert!(!any_letrec, "Cannot render multiple instances of LetRec");
                        any_letrec = true;
                        // Build declared objects.
                        // It is important that we only use the `Variable` until the object is bound.
                        // At that point, all subsequent uses should have access to the object itself.
                        let mut variables = BTreeMap::new();
                        for id in ids.iter() {
                            use differential_dataflow::operators::iterate::Variable;

                            let oks_v = Variable::new(region, Product::new(Default::default(), 1));
                            let err_v = Variable::new(region, Product::new(Default::default(), 1));

                            use differential_dataflow::operators::Consolidate;
                            context.insert_id(
                                Id::Local(*id),
                                CollectionBundle::from_collections(
                                    oks_v.consolidate(),
                                    err_v.consolidate(),
                                ),
                            );
                            variables.insert(Id::Local(*id), (oks_v, err_v));
                        }
                        for (id, value) in ids.into_iter().zip(values.into_iter()) {
                            let bundle = context.render_plan(value);
                            // We need to ensure that the raw collection exists, but do not have enough information
                            // here to cause that to happen.
                            let (oks, err) = bundle.collection.clone().unwrap();
                            context.insert_id(Id::Local(id), bundle);
                            let (oks_v, err_v) = variables.remove(&Id::Local(id)).unwrap();
                            oks_v.set(&oks);
                            err_v.set(&err);
                        }

                        let bundle = context.render_plan(*body);
                        context.insert_id(Id::Global(object.id), bundle);
                    } else {
                        context.build_object(object);
                    }
                }

                // Export declared indexes.
                for (idx_id, imports, idx) in indexes {
                    context.export_index_iterative(
                        compute_state,
                        &mut tokens,
                        imports,
                        idx_id,
                        &idx,
                        output_probes.clone(),
                    );
                }

                // Export declared sinks.
                for (sink_id, imports, sink) in sinks {
                    context.export_sink(
                        compute_state,
                        &mut tokens,
                        imports,
                        sink_id,
                        &sink,
                        output_probes.clone(),
                    );
                }
            });
        } else {
            scope.clone().region_named(&build_name, |region| {
                let mut context =
                    crate::render::context::Context::for_dataflow_in(&dataflow, region.clone());

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
                    context.import_index(compute_state, &mut tokens, *idx_id, &idx.0);
                }

                // Build declared objects.
                for object in dataflow.objects_to_build {
                    context.build_object(object);
                }

                // Export declared indexes.
                for (idx_id, imports, idx) in indexes {
                    context.export_index(
                        compute_state,
                        &mut tokens,
                        imports,
                        idx_id,
                        &idx,
                        output_probes.clone(),
                    );
                }

                // Export declared sinks.
                for (sink_id, imports, sink) in sinks {
                    context.export_sink(
                        compute_state,
                        &mut tokens,
                        imports,
                        sink_id,
                        &sink,
                        output_probes.clone(),
                    );
                }
            });
        }
    })
}

// This helper function adds an operator to track source instantiation frontier advancements
// in a dataflow. The tracking supports instrospection sources populated by compute logging.
fn intercept_source_instantiation_frontiers<G>(
    source_instantiation: &Stream<G, (Row, mz_repr::Timestamp, Diff)>,
    logger: Logger,
    source_id: GlobalId,
    dataflow_ids: Vec<GlobalId>,
) -> Stream<G, (Row, mz_repr::Timestamp, Diff)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let mut previous_time = None;
    source_instantiation.inspect_container(move |event| {
        if let Err(frontier) = event {
            if let Some(previous) = previous_time {
                for dataflow_id in dataflow_ids.iter() {
                    logger.log(ComputeEvent::SourceFrontier(
                        *dataflow_id,
                        source_id,
                        previous,
                        -1,
                    ));
                }
            }
            if let Some(time) = frontier.get(0) {
                for dataflow_id in dataflow_ids.iter() {
                    logger.log(ComputeEvent::SourceFrontier(
                        *dataflow_id,
                        source_id,
                        *time,
                        1,
                    ));
                }
                previous_time = Some(*time);
            } else {
                previous_time = None;
            }
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
        idx_id: GlobalId,
        idx: &IndexDesc,
    ) {
        if let Some(traces) = compute_state.traces.get_mut(&idx_id) {
            assert!(
                PartialOrder::less_equal(&traces.compaction_frontier(), &self.as_of_frontier),
                "Index {idx_id} has been allowed to compact beyond the dataflow as_of"
            );

            let token = traces.to_drop().clone();
            let (ok_arranged, ok_button) = traces.oks_mut().import_frontier_core(
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
        import_ids: BTreeSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
        mut probes: Vec<probe::Handle<mz_repr::Timestamp>>,
    ) {
        // put together tokens that belong to the export
        let mut needed_tokens = Vec::new();
        for import_id in import_ids {
            if let Some(token) = tokens.get(&import_id) {
                needed_tokens.push(Rc::clone(token));
            }
        }
        let bundle = self.lookup_id(Id::Global(idx_id)).unwrap_or_else(|| {
            panic!(
                "Arrangement alarmingly absent! id: {:?}",
                Id::Global(idx_id)
            )
        });
        let arrangement = bundle.arrangement(&idx.key);

        // Set up probes to notify on index frontier advancement.
        if let Some(arr) = &arrangement {
            let (collection, _) = arr.as_collection();
            let stream = collection.inner;
            for handle in probes.iter_mut() {
                stream.probe_notify_with(handle);
            }
        }

        compute_state.flow_control_probes.insert(idx_id, probes);

        match arrangement {
            Some(ArrangementFlavor::Local(oks, errs)) => {
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
        import_ids: BTreeSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
        mut probes: Vec<probe::Handle<mz_repr::Timestamp>>,
    ) {
        // put together tokens that belong to the export
        let mut needed_tokens = Vec::new();
        for import_id in import_ids {
            if let Some(token) = tokens.get(&import_id) {
                needed_tokens.push(Rc::clone(token));
            }
        }
        let bundle = self.lookup_id(Id::Global(idx_id)).unwrap_or_else(|| {
            panic!(
                "Arrangement alarmingly absent! id: {:?}",
                Id::Global(idx_id)
            )
        });
        let arrangement = bundle.arrangement(&idx.key);

        // Set up probes to notify on index frontier advancement.
        if let Some(arr) = &arrangement {
            let (collection, _) = arr.as_collection();
            let stream = collection.leave().inner;
            for handle in probes.iter_mut() {
                stream.probe_notify_with(handle);
            }
        }

        compute_state.flow_control_probes.insert(idx_id, probes);

        match arrangement {
            Some(ArrangementFlavor::Local(oks, errs)) => {
                use differential_dataflow::operators::arrange::Arrange;
                let oks = oks
                    .as_collection(|k, v| (k.clone(), v.clone()))
                    .leave()
                    .arrange();
                let errs = errs
                    .as_collection(|k, v| (k.clone(), v.clone()))
                    .leave()
                    .arrange();
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
                unimplemented!("Not yet implemented; sorry!");
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
