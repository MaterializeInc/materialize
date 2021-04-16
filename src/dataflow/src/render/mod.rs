// Copyright Materialize, Inc. All rights reserved.
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
use std::collections::{HashMap, HashSet};
use std::iter;
use std::rc::Rc;
use std::rc::Weak;

use differential_dataflow::AsCollection;
use timely::communication::Allocate;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

use dataflow_types::*;
use expr::{GlobalId, Id, MapFilterProject, MirRelationExpr};
use ore::collections::CollectionExt as _;
use ore::iter::IteratorExt;
use repr::{RelationType, Row, RowArena, Timestamp};

use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::operator::CollectionExt;
use crate::render::context::{ArrangementFlavor, Context};
use crate::server::{CacheMessage, LocalInput};
use crate::source::timestamp::TimestampDataUpdates;
use crate::source::SourceToken;

mod arrange_by;
mod context;
pub(crate) mod filter;
mod flat_map;
mod join;
mod reduce;
mod sinks;
mod sources;
mod threshold;
mod top_k;
mod upsert;

/// Worker-local state used during rendering.
pub struct RenderState {
    /// The traces available for sharing across dataflows.
    pub traces: TraceManager,
    /// Handles to local inputs, keyed by ID.
    pub local_inputs: HashMap<GlobalId, LocalInput>,
    /// Handles to external sources, keyed by ID.
    pub ts_source_mapping: HashMap<GlobalId, Vec<Weak<Option<SourceToken>>>>,
    /// Timestamp data updates for each source.
    pub ts_histories: TimestampDataUpdates,
    /// Tokens that should be dropped when a dataflow is dropped to clean up
    /// associated state.
    pub dataflow_tokens: HashMap<GlobalId, Box<dyn Any>>,
    /// Sender to give data to be cached.
    pub caching_tx: Option<mpsc::UnboundedSender<CacheMessage>>,
}

/// Build a dataflow from a description.
pub fn build_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    render_state: &mut RenderState,
    dataflow: DataflowDesc,
) {
    let worker_logging = timely_worker.log_register().get("timely");
    let name = format!("Dataflow: {}", &dataflow.debug_name);
    let materialized_logging = timely_worker.log_register().get("materialized");

    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        scope.clone().region(|region| {
            let mut context = Context::for_dataflow(&dataflow, scope.addr().into_element());

            assert!(
                !dataflow
                    .source_imports
                    .iter()
                    .map(|(id, _src)| id)
                    .has_duplicates(),
                "computation of unique IDs assumes a source appears no more than once per dataflow"
            );

            // Import declared sources into the rendering context.
            for (src_id, (src, orig_id)) in dataflow.source_imports.clone() {
                context.import_source(
                    render_state,
                    region,
                    materialized_logging.clone(),
                    src_id,
                    src,
                    orig_id,
                );
            }

            // Import declared indexes into the rendering context.
            for (idx_id, idx) in &dataflow.index_imports {
                context.import_index(render_state, scope, region, *idx_id, idx);
            }

            // Build declared objects.
            for object in &dataflow.objects_to_build {
                context.build_object(region, object);
            }

            // Export declared indexes.
            for (idx_id, idx, typ) in &dataflow.index_exports {
                let imports = dataflow.get_imports(&idx.on_id);
                context.export_index(render_state, imports, *idx_id, idx, typ);
            }

            // Export declared sinks.
            for (sink_id, sink) in &dataflow.sink_exports {
                let imports = dataflow.get_imports(&sink.from);
                context.export_sink(render_state, imports, *sink_id, sink);
            }
        });
    })
}

impl<'g, G> Context<Child<'g, G, G::Timestamp>, MirRelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn import_index(
        &mut self,
        render_state: &mut RenderState,
        scope: &mut G,
        region: &mut Child<'g, G, G::Timestamp>,
        idx_id: GlobalId,
        (idx, typ): &(IndexDesc, RelationType),
    ) {
        if let Some(traces) = render_state.traces.get_mut(&idx_id) {
            let token = traces.to_drop().clone();
            let (ok_arranged, ok_button) = traces.oks_mut().import_frontier_core(
                scope,
                &format!("Index({}, {:?})", idx.on_id, idx.keys),
                self.as_of_frontier.clone(),
            );
            let (err_arranged, err_button) = traces.errs_mut().import_frontier_core(
                scope,
                &format!("ErrIndex({}, {:?})", idx.on_id, idx.keys),
                self.as_of_frontier.clone(),
            );
            let ok_arranged = ok_arranged.enter(region);
            let err_arranged = err_arranged.enter(region);
            let get_expr = MirRelationExpr::global_get(idx.on_id, typ.clone());
            self.set_trace(idx_id, &get_expr, &idx.keys, (ok_arranged, err_arranged));
            self.additional_tokens
                .entry(idx_id)
                .or_insert_with(Vec::new)
                .push(Rc::new((
                    ok_button.press_on_drop(),
                    err_button.press_on_drop(),
                    token,
                )));
        } else {
            panic!(
                "import of index {} failed while building dataflow {}",
                idx_id, self.dataflow_id
            );
        }
    }

    fn build_object(&mut self, scope: &mut Child<'g, G, G::Timestamp>, object: &BuildDesc) {
        self.ensure_rendered(object.relation_expr.as_ref(), scope, scope.index());
        if let Some(typ) = &object.typ {
            self.clone_from_to(
                &object.relation_expr.as_ref(),
                &MirRelationExpr::global_get(object.id, typ.clone()),
            );
        } else {
            self.render_arrangeby(&object.relation_expr.as_ref(), Some(&object.id.to_string()));
            // Under the premise that this is always an arrange_by aroung a global get,
            // this will leave behind the arrangements bound to the global get, so that
            // we will not tidy them up in the next pass.
        }

        // After building each object, we want to tear down all other cached collections
        // and arrangements to avoid accidentally providing hits on local identifiers.
        // We could relax this if we better understood which expressions are dangerous
        // (e.g. expressions containing gets of local identifiers not covered by a let).
        //
        // TODO: Improve collection and arrangement re-use.
        self.collections.retain(|e, _| {
            matches!(
                e,
                MirRelationExpr::Get {
                    id: Id::Global(_),
                    typ: _,
                }
            )
        });
        self.local.retain(|e, _| {
            matches!(
                e,
                MirRelationExpr::Get {
                    id: Id::Global(_),
                    typ: _,
                }
            )
        });
        // We do not install in `context.trace`, and can skip deleting things from it.
    }

    fn export_index(
        &mut self,
        render_state: &mut RenderState,
        import_ids: HashSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
        typ: &RelationType,
    ) {
        // put together tokens that belong to the export
        let mut needed_source_tokens = Vec::new();
        let mut needed_additional_tokens = Vec::new();
        for import_id in import_ids {
            if let Some(addls) = self.additional_tokens.get(&import_id) {
                needed_additional_tokens.extend_from_slice(addls);
            }
            if let Some(source_token) = self.source_tokens.get(&import_id) {
                needed_source_tokens.push(source_token.clone());
            }
        }
        let tokens = Rc::new((needed_source_tokens, needed_additional_tokens));
        let get_expr = MirRelationExpr::global_get(idx.on_id, typ.clone());
        match self.arrangement(&get_expr, &idx.keys) {
            Some(ArrangementFlavor::Local(oks, errs)) => {
                render_state.traces.set(
                    idx_id,
                    TraceBundle::new(oks.trace, errs.trace).with_drop(tokens),
                );
            }
            Some(ArrangementFlavor::Trace(gid, _, _)) => {
                // Duplicate of existing arrangement with id `gid`, so
                // just create another handle to that arrangement.
                let trace = render_state.traces.get(&gid).unwrap().clone();
                render_state.traces.set(idx_id, trace);
            }
            None => {
                panic!("Arrangement alarmingly absent!");
            }
        };
    }
}

impl<G> Context<G, MirRelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Attempt to render a chain of map/filter/project operators on top of another operator.
    ///
    /// Returns true if it was successful, and false otherwise. If this method returns false,
    /// we should continue with the traditional individual render implementations of each
    /// operator.
    fn try_render_map_filter_project(
        &mut self,
        relation_expr: &MirRelationExpr,
        scope: &mut G,
        worker_index: usize,
    ) -> bool {
        // Extract a MapFilterProject and residual from `relation_expr`.
        let (mut mfp, input) = MapFilterProject::extract_from_expression(relation_expr);
        mfp.optimize();
        match input {
            MirRelationExpr::Get { .. } => {
                // TODO: determine if `mfp` is no-op to simplify implementation.
                let mfp2 = mfp.clone();
                self.ensure_rendered(&input, scope, worker_index);
                let (ok_collection, mut err_collection) = self
                    .flat_map_ref(&input, move |exprs| mfp2.literal_constraints(exprs), {
                        let mut row_packer = repr::Row::default();
                        let mut datums = vec![];
                        move |row| {
                            let pack_result = {
                                let temp_storage = RowArena::new();
                                let mut datums_local = std::mem::take(&mut datums);
                                datums_local.extend(row.iter());
                                // Temporary assignment looks weird, but seems needed to convince
                                // Rust that the lifetime of `evaluate_iter` does not escape.
                                let result =
                                    match mfp.evaluate_iter(&mut datums_local, &temp_storage) {
                                        Ok(Some(iter)) => {
                                            row_packer.clear();
                                            row_packer.extend(iter);
                                            Some(Ok(()))
                                        }
                                        Ok(None) => None,
                                        Err(e) => Some(Err(e.into())),
                                    };
                                datums = ore::vec::repurpose_allocation(datums_local);
                                result
                            };

                            // Re-use the input `row` if at all possible.
                            pack_result.map(|res| {
                                res.map(|()| match row {
                                    timely::communication::message::RefOrMut::Ref(_) => {
                                        row_packer.finish_and_reuse()
                                    }
                                    timely::communication::message::RefOrMut::Mut(r) => {
                                        // Copy the contents into `r` and clear `row_packer`.
                                        r.clone_from(&row_packer);
                                        row_packer.clear();
                                        std::mem::take(r)
                                    }
                                })
                            })
                        }
                    })
                    .unwrap();

                use timely::dataflow::operators::ok_err::OkErr;
                let (oks, errors) = ok_collection.inner.ok_err(|(x, t, d)| match x {
                    Ok(x) => Ok((x, t, d)),
                    Err(x) => Err((x, t, d)),
                });
                err_collection = err_collection.concat(&errors.as_collection());

                self.collections
                    .insert(relation_expr.clone(), (oks.as_collection(), err_collection));
                true
            }
            MirRelationExpr::FlatMap { input: input2, .. } => {
                self.ensure_rendered(&input2, scope, worker_index);
                let (oks, err) = self.render_flat_map(input, Some(mfp));
                self.collections.insert(relation_expr.clone(), (oks, err));
                true
            }

            MirRelationExpr::Join {
                inputs,
                implementation,
                ..
            } => {
                for input in inputs {
                    self.ensure_rendered(input, scope, worker_index);
                }
                match implementation {
                    expr::JoinImplementation::Differential(_start, _order) => {
                        let collection = self.render_join(input, mfp, scope);
                        self.collections.insert(relation_expr.clone(), collection);
                    }
                    expr::JoinImplementation::DeltaQuery(_orders) => {
                        let collection =
                            self.render_delta_join(input, mfp, scope, worker_index, |t| {
                                t.saturating_sub(1)
                            });
                        self.collections.insert(relation_expr.clone(), collection);
                    }
                    expr::JoinImplementation::Unimplemented => {
                        panic!("Attempt to render unimplemented join");
                    }
                }
                true
            }
            _ => false,
        }
    }

    /// Ensures the context contains an entry for `relation_expr`.
    ///
    /// This method may construct new dataflow elements and register then in the context,
    /// and is only obliged to ensure that a call to `self.collection(relation_expr)` will
    /// result in a non-`None` result. This may be a raw collection or an arrangement by
    /// any set of keys.
    ///
    /// The rough structure of the logic for each expression is to ensure that any input
    /// collections are rendered,
    pub fn ensure_rendered(
        &mut self,
        relation_expr: &MirRelationExpr,
        scope: &mut G,
        worker_index: usize,
    ) {
        if !self.has_collection(relation_expr) {
            // Each of the `MirRelationExpr` variants have logic to render themselves to either
            // a collection or an arrangement. In either case, we associate the result with
            // the `relation_expr` argument in the context.
            match relation_expr {
                // The constant collection is instantiated only on worker zero.
                MirRelationExpr::Constant { rows, .. } => {
                    // Determine what this worker will contribute.
                    let locally = if worker_index == 0 {
                        rows.clone()
                    } else {
                        Ok(vec![])
                    };
                    // Produce both rows and errs to avoid conditional dataflow construction.
                    let (rows, errs) = match locally {
                        Ok(rows) => (rows, Vec::new()),
                        Err(e) => (Vec::new(), vec![e]),
                    };

                    let ok_collection = rows
                        .into_iter()
                        .map(|(x, diff)| (x, timely::progress::Timestamp::minimum(), diff))
                        .to_stream(scope)
                        .as_collection();

                    let err_collection = errs
                        .into_iter()
                        .map(|e| {
                            (
                                DataflowError::from(e),
                                timely::progress::Timestamp::minimum(),
                                1,
                            )
                        })
                        .to_stream(scope)
                        .as_collection();

                    self.collections
                        .insert(relation_expr.clone(), (ok_collection, err_collection));
                }

                // A get should have been loaded into the context, and it is surprising to
                // reach this point given the `has_collection()` guard at the top of the method.
                MirRelationExpr::Get { id, typ } => {
                    // TODO: something more tasteful.
                    // perhaps load an empty collection, warn?
                    panic!("Collection {} (typ: {:?}) not pre-loaded", id, typ);
                }

                MirRelationExpr::Let { id, value, body } => {
                    let typ = value.typ();
                    let bind = MirRelationExpr::Get {
                        id: Id::Local(*id),
                        typ,
                    };
                    if self.has_collection(&bind) {
                        panic!("Inappropriate to re-bind name: {:?}", bind);
                    } else {
                        self.ensure_rendered(value, scope, worker_index);
                        self.clone_from_to(value, &bind);
                        self.ensure_rendered(body, scope, worker_index);
                        self.clone_from_to(body, relation_expr);
                    }
                }

                MirRelationExpr::Project { input, outputs } => {
                    if !self.try_render_map_filter_project(relation_expr, scope, worker_index) {
                        self.ensure_rendered(input, scope, worker_index);
                        let outputs = outputs.clone();
                        let (ok_collection, err_collection) = self.collection(input).unwrap();
                        let ok_collection = ok_collection.map({
                            move |row| {
                                let datums = row.unpack();
                                let iterator = outputs.iter().map(|i| datums[*i]);
                                let total_size = repr::datums_size(iterator.clone());
                                let mut row = Row::with_capacity(total_size);
                                row.extend(iterator);
                                row
                            }
                        });

                        self.collections
                            .insert(relation_expr.clone(), (ok_collection, err_collection));
                    }
                }

                MirRelationExpr::Map { input, scalars } => {
                    if !self.try_render_map_filter_project(relation_expr, scope, worker_index) {
                        self.ensure_rendered(input, scope, worker_index);
                        let scalars = scalars.clone();
                        let (ok_collection, err_collection) = self.collection(input).unwrap();
                        let (ok_collection, new_err_collection) = ok_collection.map_fallible({
                            move |input_row| {
                                let mut datums = input_row.unpack();
                                let temp_storage = RowArena::new();
                                for scalar in &scalars {
                                    let datum = scalar.eval(&datums, &temp_storage)?;
                                    // Scalar is allowed to see the outputs of previous scalars.
                                    // To avoid repeatedly unpacking input_row, we just push the outputs into datums so later scalars can see them.
                                    // Note that this doesn't mutate input_row.
                                    datums.push(datum);
                                }
                                Ok::<_, DataflowError>(Row::pack_slice(&datums))
                            }
                        });
                        let err_collection = err_collection.concat(&new_err_collection);
                        self.collections
                            .insert(relation_expr.clone(), (ok_collection, err_collection));
                    }
                }

                MirRelationExpr::FlatMap { input, .. } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let (oks, err) = self.render_flat_map(relation_expr, None);
                    self.collections.insert(relation_expr.clone(), (oks, err));
                }

                MirRelationExpr::Filter { input, .. } => {
                    if !self.try_render_map_filter_project(relation_expr, scope, worker_index) {
                        self.ensure_rendered(input, scope, worker_index);
                        let collections = self.render_filter(relation_expr);
                        self.collections.insert(relation_expr.clone(), collections);
                    }
                }

                MirRelationExpr::Join {
                    inputs,
                    implementation,
                    ..
                } => {
                    for input in inputs {
                        self.ensure_rendered(input, scope, worker_index);
                    }
                    let input_mapper = expr::JoinInputMapper::new(inputs);
                    let mfp = MapFilterProject::new(input_mapper.total_columns());
                    match implementation {
                        expr::JoinImplementation::Differential(_start, _order) => {
                            let collection = self.render_join(relation_expr, mfp, scope);
                            self.collections.insert(relation_expr.clone(), collection);
                        }
                        expr::JoinImplementation::DeltaQuery(_orders) => {
                            let collection = self.render_delta_join(
                                relation_expr,
                                mfp,
                                scope,
                                worker_index,
                                |t| t.saturating_sub(1),
                            );
                            self.collections.insert(relation_expr.clone(), collection);
                        }
                        expr::JoinImplementation::Unimplemented => {
                            panic!("Attempt to render unimplemented join");
                        }
                    }
                }

                MirRelationExpr::Reduce { input, .. } => {
                    self.ensure_rendered(input, scope, worker_index);
                    self.render_reduce(relation_expr);
                }

                MirRelationExpr::TopK { input, .. } => {
                    self.ensure_rendered(input, scope, worker_index);
                    self.render_topk(relation_expr);
                }

                MirRelationExpr::Negate { input } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let (ok_collection, err_collection) = self.collection(input).unwrap();
                    let ok_collection = ok_collection.negate();
                    self.collections
                        .insert(relation_expr.clone(), (ok_collection, err_collection));
                }

                MirRelationExpr::Threshold { input } => {
                    self.ensure_rendered(input, scope, worker_index);
                    self.render_threshold(relation_expr);
                }

                MirRelationExpr::Union { base, inputs } => {
                    let (oks, errs): (Vec<_>, Vec<_>) = iter::once(&**base)
                        .chain(inputs)
                        .map(|input| {
                            self.ensure_rendered(input, scope, worker_index);
                            self.collection(input).unwrap()
                        })
                        .unzip();

                    let ok = differential_dataflow::collection::concatenate(scope, oks);
                    let err = differential_dataflow::collection::concatenate(scope, errs);

                    self.collections.insert(relation_expr.clone(), (ok, err));
                }

                MirRelationExpr::ArrangeBy { input, keys } => {
                    // We can avoid rendering if we have all arrangements present,
                    // and there is at least one of them (to ensure the collection
                    // is available independent of arrangements).
                    if keys.is_empty()
                        || keys
                            .iter()
                            .any(|key| self.arrangement(&input, key).is_none())
                    {
                        self.ensure_rendered(input, scope, worker_index);
                    }
                    self.render_arrangeby(relation_expr, None);
                }

                MirRelationExpr::DeclareKeys { input, keys: _ } => {
                    // TODO - some kind of debug mode where we assert that the keys are truly keys?
                    self.ensure_rendered(input, scope, worker_index);
                    self.clone_from_to(input, relation_expr);
                }
            };
        }
    }
}

/// A re-useable vector of `Datum` with varying lifetimes.
///
/// This type is meant to allow us to recycle an underlying allocation with
/// a specific lifetime, under the condition that the vector is emptied before
/// this happens (to prevent leaking of invalid references).
///
/// It uses `ore::vec::repurpose_allocation` to accomplish this, which contains
/// unsafe code.
pub mod datum_vec {

    use repr::{Datum, Row};

    /// A re-useable vector of `Datum` with no particular lifetime.
    pub struct DatumVec {
        outer: Vec<Datum<'static>>,
    }

    impl DatumVec {
        /// Allocate a new instance.
        pub fn new() -> Self {
            Self { outer: Vec::new() }
        }
        /// Borrow an instance with a specific lifetime.
        ///
        /// When the result is dropped, its allocation will be returned to `self`.
        pub fn borrow<'a>(&'a mut self) -> DatumVecBorrow<'a> {
            let inner = std::mem::take(&mut self.outer);
            DatumVecBorrow {
                outer: &mut self.outer,
                inner,
            }
        }
        /// Borrow an instance with a specific lifetime, and pre-populate with a `Row`.
        pub fn borrow_with<'a>(&'a mut self, row: &'a Row) -> DatumVecBorrow<'a> {
            let mut borrow = self.borrow();
            borrow.extend(row.iter());
            borrow
        }
    }

    /// A borrowed allocation of `Datum` with a specific lifetime.
    ///
    /// When an instance is dropped, its allocation is returned to the vector from
    /// which it was extracted.
    pub struct DatumVecBorrow<'outer> {
        outer: &'outer mut Vec<Datum<'static>>,
        inner: Vec<Datum<'outer>>,
    }

    impl<'outer> Drop for DatumVecBorrow<'outer> {
        fn drop(&mut self) {
            *self.outer = ore::vec::repurpose_allocation(std::mem::take(&mut self.inner));
        }
    }

    impl<'outer> std::ops::Deref for DatumVecBorrow<'outer> {
        type Target = Vec<Datum<'outer>>;
        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl<'outer> std::ops::DerefMut for DatumVecBorrow<'outer> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.inner
        }
    }
}
