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
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::rc::Weak;

use differential_dataflow::AsCollection;
use timely::communication::Allocate;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;

use dataflow_types::*;
use expr::{GlobalId, Id};
use itertools::Itertools;
use ore::collections::CollectionExt as _;
use ore::now::NowFn;
use repr::{Row, Timestamp};

use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::render::context::CollectionBundle;
use crate::render::context::{ArrangementFlavor, Context};
use crate::server::LocalInput;
use crate::source::timestamp::TimestampBindingRc;
use crate::source::SourceToken;

mod context;
mod flat_map;
mod join;
mod reduce;
mod sinks;
mod sources;
mod threshold;
mod top_k;
mod upsert;

/// Worker-local state that is maintained across dataflows.
pub struct RenderState {
    /// The traces available for sharing across dataflows.
    pub traces: TraceManager,
    /// Handles to local inputs, keyed by ID.
    pub local_inputs: HashMap<GlobalId, LocalInput>,
    /// Handles to external sources, keyed by ID.
    pub ts_source_mapping: HashMap<GlobalId, Vec<Weak<Option<SourceToken>>>>,
    /// Timestamp data updates for each source.
    pub ts_histories: HashMap<GlobalId, TimestampBindingRc>,
    /// Tokens that should be dropped when a dataflow is dropped to clean up
    /// associated state.
    pub dataflow_tokens: HashMap<GlobalId, Box<dyn Any>>,
    /// Frontier of sink writes (all subsequent writes will be at times at or
    /// equal to this frontier)
    pub sink_write_frontiers: HashMap<GlobalId, Rc<RefCell<Antichain<Timestamp>>>>,
}

/// A container for "tokens" that are relevant to an in-construction dataflow.
///
/// Tokens are used by consumers of data to keep their sources of data running.
/// Once all tokens referencing a source are dropped, the source can shut down,
/// which will wind down (eventually) the dataflow containing it.
#[derive(Default)]
pub struct RelevantTokens {
    /// The source tokens for all sources that have been built in this context.
    pub source_tokens: HashMap<GlobalId, Rc<Option<SourceToken>>>,
    /// Any other tokens that need to be dropped when an object is dropped.
    pub additional_tokens: HashMap<GlobalId, Vec<Rc<dyn Any>>>,
    /// Tokens for CDCv2 capture sources that have been built in this context.
    pub cdc_tokens: HashMap<GlobalId, Rc<dyn Any>>,
}

/// Build a dataflow from a description.
pub fn build_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    render_state: &mut RenderState,
    dataflow: DataflowDescription<plan::Plan>,
    now: NowFn,
) {
    let worker_logging = timely_worker.log_register().get("timely");
    let name = format!("Dataflow: {}", &dataflow.debug_name);
    let materialized_logging = timely_worker.log_register().get("materialized");

    timely_worker.dataflow_core(&name, worker_logging, Box::new(()), |_, scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        scope.clone().region_named(&name, |region| {
            let mut context = Context::for_dataflow(&dataflow, scope.addr().into_element());
            let mut tokens = RelevantTokens::default();

            assert!(
                !dataflow
                    .source_imports
                    .iter()
                    .map(|(id, _src)| id)
                    .duplicates()
                    .next()
                    .is_some(),
                "computation of unique IDs assumes a source appears no more than once per dataflow"
            );

            // Import declared sources into the rendering context.
            for (src_id, (src, orig_id)) in dataflow.source_imports.clone() {
                context.import_source(
                    render_state,
                    &mut tokens,
                    region,
                    materialized_logging.clone(),
                    src_id,
                    src,
                    orig_id,
                    now,
                );
            }

            // Import declared indexes into the rendering context.
            for (idx_id, idx) in &dataflow.index_imports {
                context.import_index(render_state, &mut tokens, scope, region, *idx_id, &idx.0);
            }

            // Build declared objects.
            for object in &dataflow.objects_to_build {
                // We clone because we cannot deconstruct `object` for its members due
                // to subsequent use of `dataflow.get_imports`.
                // TODO: fix that and avoid the clones.
                context.build_object(region, object.clone());
            }

            // Export declared indexes.
            for (idx_id, idx, _typ) in &dataflow.index_exports {
                let imports = dataflow.get_imports(&idx.on_id);
                context.export_index(render_state, &mut tokens, imports, *idx_id, idx);
            }

            // Export declared sinks.
            for (sink_id, sink) in &dataflow.sink_exports {
                let imports = dataflow.get_imports(&sink.from);
                context.export_sink(
                    render_state,
                    &mut tokens,
                    imports,
                    *sink_id,
                    sink,
                    region.index(),
                    region.peers(),
                );
            }
        });
    })
}

impl<'g, G> Context<Child<'g, G, G::Timestamp>, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn import_index(
        &mut self,
        render_state: &mut RenderState,
        tokens: &mut RelevantTokens,
        scope: &mut G,
        region: &mut Child<'g, G, G::Timestamp>,
        idx_id: GlobalId,
        idx: &IndexDesc,
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
            self.update_id(
                Id::Global(idx.on_id),
                CollectionBundle::from_expressions(
                    idx.keys.clone(),
                    ArrangementFlavor::Trace(idx_id, ok_arranged, err_arranged),
                ),
            );
            tokens
                .additional_tokens
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

    fn build_object(
        &mut self,
        scope: &mut Child<'g, G, G::Timestamp>,
        object: BuildDesc<plan::Plan>,
    ) {
        // First, transform the relation expression into a render plan.
        let bundle = self.render_plan(object.view, scope, scope.index());
        self.insert_id(Id::Global(object.id), bundle);
    }

    fn export_index(
        &mut self,
        render_state: &mut RenderState,
        tokens: &mut RelevantTokens,
        import_ids: HashSet<GlobalId>,
        idx_id: GlobalId,
        idx: &IndexDesc,
    ) {
        // put together tokens that belong to the export
        let mut needed_source_tokens = Vec::new();
        let mut needed_additional_tokens = Vec::new();
        for import_id in import_ids {
            if let Some(addls) = tokens.additional_tokens.get(&import_id) {
                needed_additional_tokens.extend_from_slice(addls);
            }
            if let Some(source_token) = tokens.source_tokens.get(&import_id) {
                needed_source_tokens.push(source_token.clone());
            }
        }
        let tokens = Rc::new((needed_source_tokens, needed_additional_tokens));
        let bundle = self.lookup_id(Id::Global(idx_id)).unwrap_or_else(|| {
            panic!(
                "Arrangement alarmingly absent! id: {:?}",
                Id::Global(idx_id)
            )
        });
        match bundle.arrangement(&idx.keys) {
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
                println!("collection available: {:?}", bundle.collection.is_none());
                println!(
                    "keys available: {:?}",
                    bundle.arranged.keys().collect::<Vec<_>>()
                );
                panic!(
                    "Arrangement alarmingly absent! id: {:?}, keys: {:?}",
                    Id::Global(idx_id),
                    &idx.keys
                );
            }
        };
    }
}

impl<G> Context<G, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Renders a plan to a differential dataflow, producing the collection of results.
    ///
    /// The return type reflects the uncertainty about the data representation, perhaps
    /// as a stream of data, perhaps as an arrangement, perhaps as a stream of batches.
    pub fn render_plan(
        &mut self,
        plan: plan::Plan,
        scope: &mut G,
        worker_index: usize,
    ) -> CollectionBundle<G, Row, G::Timestamp> {
        use plan::Plan;
        match plan {
            Plan::Constant { rows } => {
                // Determine what this worker will contribute.
                let locally = if worker_index == 0 { rows } else { Ok(vec![]) };
                // Produce both rows and errs to avoid conditional dataflow construction.
                let (rows, errs) = match locally {
                    Ok(rows) => (rows, Vec::new()),
                    Err(e) => (Vec::new(), vec![e]),
                };

                let ok_collection = rows.into_iter().to_stream(scope).as_collection();

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

                CollectionBundle::from_collections(ok_collection, err_collection)
            }
            Plan::Get { id, mfp } => {
                // Recover the collection from `self` and then apply `mfp` to it.
                // If `mfp` happens to be trivial, we can just return the collection.
                let collection = self
                    .lookup_id(id)
                    .unwrap_or_else(|| panic!("Get({:?}) not found at render time", id));
                if mfp.is_identity() {
                    collection
                } else {
                    let (oks, errs) = collection.as_collection_core(mfp);
                    CollectionBundle::from_collections(oks, errs)
                }
            }
            Plan::Let { id, value, body } => {
                // Render `value` and bind it to `id`. Complain if this shadows an id.
                let value = self.render_plan(*value, scope, worker_index);
                let prebound = self.insert_id(Id::Local(id), value);
                assert!(prebound.is_none());

                let body = self.render_plan(*body, scope, worker_index);
                self.remove_id(Id::Local(id));
                body
            }
            Plan::Mfp { input, mfp } => {
                // If `mfp` is non-trivial, we should apply it and produce a collection.
                let input = self.render_plan(*input, scope, worker_index);
                if mfp.is_identity() {
                    input
                } else {
                    let (oks, errs) = input.as_collection_core(mfp);
                    CollectionBundle::from_collections(oks, errs)
                }
            }
            Plan::FlatMap {
                input,
                func,
                exprs,
                mfp,
            } => {
                let input = self.render_plan(*input, scope, worker_index);
                self.render_flat_map(input, func, exprs, mfp)
            }
            Plan::Join { inputs, plan } => {
                let inputs = inputs
                    .into_iter()
                    .map(|input| self.render_plan(input, scope, worker_index))
                    .collect();
                match plan {
                    crate::render::join::JoinPlan::Linear(linear_plan) => {
                        self.render_join(inputs, linear_plan, scope)
                    }
                    crate::render::join::JoinPlan::Delta(delta_plan) => {
                        self.render_delta_join(inputs, delta_plan, scope)
                    }
                }
            }
            Plan::Reduce {
                input,
                key_val_plan,
                plan,
            } => {
                let input = self.render_plan(*input, scope, worker_index);
                self.render_reduce(input, key_val_plan, plan)
            }
            Plan::TopK { input, top_k_plan } => {
                let input = self.render_plan(*input, scope, worker_index);
                self.render_topk(input, top_k_plan)
            }
            Plan::Negate { input } => {
                let input = self.render_plan(*input, scope, worker_index);
                let (oks, errs) = input.as_collection();
                CollectionBundle::from_collections(oks.negate(), errs)
            }
            Plan::Threshold {
                input,
                threshold_plan,
            } => {
                let input = self.render_plan(*input, scope, worker_index);
                self.render_threshold(input, threshold_plan)
            }
            Plan::Union { inputs } => {
                let mut oks = Vec::new();
                let mut errs = Vec::new();
                for input in inputs.into_iter() {
                    let (os, es) = self.render_plan(input, scope, worker_index).as_collection();
                    oks.push(os);
                    errs.push(es);
                }
                let oks = differential_dataflow::collection::concatenate(scope, oks);
                let errs = differential_dataflow::collection::concatenate(scope, errs);
                CollectionBundle::from_collections(oks, errs)
            }
            Plan::ArrangeBy { input, keys } => {
                let input = self.render_plan(*input, scope, worker_index);
                input.ensure_arrangements(keys)
            }
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

/// An explicit represenation of a rendering plan for provided dataflows.
pub mod plan {

    use crate::render::join::{DeltaJoinPlan, JoinPlan, LinearJoinPlan};
    use crate::render::reduce::{KeyValPlan, ReducePlan};
    use crate::render::threshold::ThresholdPlan;
    use crate::render::top_k::TopKPlan;
    use dataflow_types::DataflowDescription;
    use expr::{
        EvalError, Id, JoinInputMapper, LocalId, MapFilterProject, MirRelationExpr, MirScalarExpr,
        OptimizedMirRelationExpr, TableFunc,
    };
    use repr::{Datum, Row};

    /// A rendering plan with all conditional logic removed.
    ///
    /// This type is exposed publicly but the intent is that its details are under
    /// the control of this crate, and they are subject to change as we find more
    /// compelling ways to represent renderable plans. Several stages have already
    /// encapsulated much of their logic in their own stage-specific plans, and we
    /// expect more of the plans to do the same in the future, without consultation.
    #[derive(Clone, Debug)]
    pub enum Plan {
        /// A collection containing a pre-determined collection.
        Constant {
            /// Explicit update triples for the collection.
            rows: Result<Vec<(Row, repr::Timestamp, isize)>, EvalError>,
        },
        /// A reference to a bound collection.
        ///
        /// This is commonly either an external reference to an existing source or
        /// maintained arrangement, or an internal reference to a `Let` identifier.
        Get {
            /// A global or local identifier naming the collection.
            id: Id,
            /// Any linear operator work to apply as part of producing the data.
            ///
            /// This logic allows us to efficiently extract collections from data
            /// that have been pre-arranged, avoiding copying rows that are not
            /// used and columns that are projected away.
            mfp: MapFilterProject,
        },
        /// Binds `value` to `id`, and then results in `body` with that binding.
        ///
        /// This stage has the effect of sharing `value` across multiple possible
        /// uses in `body`, and is the only mechanism we have for sharing collection
        /// information across parts of a dataflow.
        ///
        /// The binding is not available outside of `body`.
        Let {
            /// The local identifier to be used, available to `body` as `Id::Local(id)`.
            id: LocalId,
            /// The collection that should be bound to `id`.
            value: Box<Plan>,
            /// The collection that results, which is allowed to contain `Get` stages
            /// that reference `Id::Local(id)`.
            body: Box<Plan>,
        },
        /// Map, Filter, and Project operators.
        ///
        /// This stage contains work that we would ideally like to fuse to other plan
        /// stages, but for practical reasons cannot. For example: reduce, threshold,
        /// and topk stages are not able to absorb this operator.
        Mfp {
            /// The input collection.
            input: Box<Plan>,
            /// Linear operator to apply to each record.
            mfp: MapFilterProject,
        },
        /// A variable number of output records for each input record.
        ///
        /// This stage is a bit of a catch-all for logic that does not easily fit in
        /// map stages. This includes table valued functions, but also functions of
        /// multiple arguments, and functions that modify the sign of updates.
        ///
        /// This stage allows a `MapFilterProject` operator to be fused to its output,
        /// and this can be very important as otherwise the output of `func` is just
        /// appended to the input record, for as many outputs as it has. This has the
        /// unpleasant default behavior of repeating potentially large records that
        /// are being unpacked, producing quadratic output in those cases. Instead,
        /// in these cases use a `mfp` member that projects away these large fields.
        FlatMap {
            /// The input collection.
            input: Box<Plan>,
            /// The variable-record emitting function.
            func: TableFunc,
            /// Expressions that for each row prepare the arguments to `func`.
            exprs: Vec<MirScalarExpr>,
            /// Linear operator to apply to each record produced by `func`.
            mfp: MapFilterProject,
        },
        /// A multiway relational equijoin, with fused map, filter, and projection.
        ///
        /// This stage performs a multiway join among `inputs`, using the equality
        /// constraints expressed in `plan`. The plan also describes the implementataion
        /// strategy we will use, and any pushed down per-record work.
        Join {
            /// An ordered list of inputs that will be joined.
            inputs: Vec<Plan>,
            /// Detailed information about the implementation of the join.
            ///
            /// This includes information about the implementation strategy, but also
            /// any map, filter, project work that we might follow the join with, but
            /// potentially pushed down into the implementation of the join.
            plan: JoinPlan,
        },
        /// Aggregation by key.
        Reduce {
            /// The input collection.
            input: Box<Plan>,
            /// A plan for changing input records into key, value pairs.
            key_val_plan: KeyValPlan,
            /// A plan for performing the reduce.
            ///
            /// The implementation of reduction has several different strategies based
            /// on the properties of the reduction, and the input itself. Please check
            /// out the documentation for this type for more detail.
            plan: ReducePlan,
        },
        /// Key-based "Top K" operator, retaining the first K records in each group.
        TopK {
            /// The input collection.
            input: Box<Plan>,
            /// A plan for performing the Top-K.
            ///
            /// The implementation of reduction has several different strategies based
            /// on the properties of the reduction, and the input itself. Please check
            /// out the documentation for this type for more detail.
            top_k_plan: TopKPlan,
        },
        /// Inverts the sign of each update.
        Negate {
            /// The input collection.
            input: Box<Plan>,
        },
        /// Filters records that accumulate negatively.
        ///
        /// Although the operator suppresses updates, it is a stateful operator taking
        /// resources proportional to the number of records with non-zero accumulation.
        Threshold {
            /// The input collection.
            input: Box<Plan>,
            /// A plan for performing the threshold.
            ///
            /// The implementation of reduction has several different strategies based
            /// on the properties of the reduction, and the input itself. Please check
            /// out the documentation for this type for more detail.
            threshold_plan: ThresholdPlan,
        },
        /// Adds the contents of the input collections.
        ///
        /// Importantly, this is *multiset* union, so the multiplicities of records will
        /// add. This is in contrast to *set* union, where the multiplicities would be
        /// capped at one. A set union can be formed with `Union` followed by `Reduce`
        /// implementing the "distinct" operator.
        Union {
            /// The input collections.
            inputs: Vec<Plan>,
        },
        /// The `input` plan, but with additional arrangements.
        ///
        /// This operator does not change the logical contents of `input`, but ensures
        /// that certain arrangements are available in the results. This operator can
        /// be important for e.g. the `Join` stage which benefits from multiple arrangements
        /// or to cap a `Plan` so that indexes can be exported.
        ArrangeBy {
            /// The input collection.
            input: Box<Plan>,
            /// A list of arrangement keys that will be added to those of the input.
            ///
            /// If any of these keys are already present in the input, they have no effect.
            keys: Vec<Vec<MirScalarExpr>>,
        },
    }

    impl Plan {
        /// This method converts a MirRelationExpr into a plan that can be directly rendered.
        ///
        /// The rough structure is that we repeatedly extract map/filter/project operators
        /// from each expression we see, bundle them up as a `MapFilterProject` object, and
        /// then produce a plan for the combination of that with the next operator.
        pub fn from_mir(expr: &MirRelationExpr) -> Result<Self, ()> {
            // Extract a maximally large MapFilterProject from `expr`.
            // We will then try and push this in to the resulting expression.
            //
            // Importantly, `mfp` may contain temporal operators and not be a "safe" MFP.
            // While we would eventually like all plan stages to be able to absorb such
            // general operators, not all of them can.
            let (mut mfp, expr) = MapFilterProject::extract_from_expression(expr);
            // We attempt to plan what we have remaining, in the context of `mfp`.
            // We may not be able to do this, and must wrap some operators with a `Mfp` stage.
            let plan = match expr {
                // These operators should have been extracted from the expression.
                MirRelationExpr::Map { .. } => {
                    panic!("This operator should have been extracted");
                }
                MirRelationExpr::Filter { .. } => {
                    panic!("This operator should have been extracted");
                }
                MirRelationExpr::Project { .. } => {
                    panic!("This operator should have been extracted");
                }
                // These operators may not have been extracted, and need to result in a `Plan`.
                MirRelationExpr::Constant { rows, typ: _ } => {
                    use timely::progress::Timestamp;
                    let plan = Plan::Constant {
                        rows: rows.clone().map(|rows| {
                            rows.into_iter()
                                .map(|(row, diff)| (row, repr::Timestamp::minimum(), diff))
                                .collect()
                        }),
                    };
                    plan
                }
                MirRelationExpr::Get { id, typ: _ } => {
                    // This stage can absorb arbitrary MFP operators.
                    let mfp = mfp.take();
                    Plan::Get {
                        id: id.clone(),
                        mfp,
                    }
                }
                MirRelationExpr::Let { id, value, body } => {
                    // It would be unfortunate to have a non-trivial `mfp` here, as we hope
                    // that they would be pushed down. I am not sure if we should take the
                    // initiative to push down the `mfp` ourselves.
                    let value = Box::new(Plan::from_mir(value)?);
                    let body = Box::new(Plan::from_mir(body)?);
                    Plan::Let {
                        id: id.clone(),
                        value,
                        body,
                    }
                }
                MirRelationExpr::FlatMap {
                    input,
                    func,
                    exprs,
                    demand,
                } => {
                    // Map the demand into the MapFilterProject.
                    // TODO: Remove this once demand is removed.
                    if let Some(demand) = demand {
                        prepend_mfp_demand(&mut mfp, expr, demand);
                    }
                    let input = Box::new(Plan::from_mir(input)?);
                    // This stage can absorb arbitrary MFP instances.
                    let mfp = mfp.take();
                    Plan::FlatMap {
                        input,
                        func: func.clone(),
                        exprs: exprs.clone(),
                        mfp,
                    }
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    demand,
                    implementation,
                } => {
                    // Map the demand into the MapFilterProject.
                    // TODO: Remove this once demand is removed.
                    if let Some(demand) = demand {
                        prepend_mfp_demand(&mut mfp, expr, demand);
                    }

                    let input_mapper = JoinInputMapper::new(inputs);

                    // Plan each of the join inputs independently.
                    let mut plans = Vec::new();
                    for input in inputs.iter() {
                        plans.push(Plan::from_mir(input)?);
                    }
                    // Extract temporal predicates as joins cannot currently absorb them.
                    let plan = match implementation {
                        expr::JoinImplementation::Differential((start, _start_arr), order) => {
                            JoinPlan::Linear(LinearJoinPlan::create_from(
                                *start,
                                equivalences,
                                order,
                                input_mapper,
                                &mut mfp,
                            ))
                        }
                        expr::JoinImplementation::DeltaQuery(orders) => {
                            JoinPlan::Delta(DeltaJoinPlan::create_from(
                                equivalences,
                                &orders[..],
                                input_mapper,
                                &mut mfp,
                            ))
                        }
                        // Other plans are errors, and should be reported as such.
                        _ => return Err(()),
                    };
                    Plan::Join {
                        inputs: plans,
                        plan,
                    }
                }
                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    monotonic,
                    expected_group_size,
                } => {
                    let input_arity = input.arity();
                    let input = Box::new(Self::from_mir(input)?);
                    let key_val_plan = KeyValPlan::new(input_arity, group_key, aggregates);
                    let reduce_plan = ReducePlan::create_from(
                        aggregates.clone(),
                        *monotonic,
                        *expected_group_size,
                    );
                    Plan::Reduce {
                        input,
                        key_val_plan,
                        plan: reduce_plan,
                    }
                }
                MirRelationExpr::TopK {
                    input,
                    group_key,
                    order_key,
                    limit,
                    offset,
                    monotonic,
                } => {
                    let arity = input.arity();
                    let input = Box::new(Self::from_mir(input)?);
                    let top_k_plan = TopKPlan::create_from(
                        group_key.clone(),
                        order_key.clone(),
                        *offset,
                        *limit,
                        arity,
                        *monotonic,
                    );
                    Plan::TopK { input, top_k_plan }
                }
                MirRelationExpr::Negate { input } => {
                    let input = Box::new(Self::from_mir(input)?);
                    Plan::Negate { input }
                }
                MirRelationExpr::Threshold { input } => {
                    let arity = input.arity();
                    let input = Box::new(Self::from_mir(input)?);
                    let threshold_plan = ThresholdPlan::create_from(arity, false);
                    Plan::Threshold {
                        input,
                        threshold_plan,
                    }
                }
                MirRelationExpr::Union { base, inputs } => {
                    let mut plans = Vec::with_capacity(1 + inputs.len());
                    plans.push(Self::from_mir(base)?);
                    for input in inputs.iter() {
                        plans.push(Self::from_mir(input)?)
                    }
                    Plan::Union { inputs: plans }
                }
                MirRelationExpr::ArrangeBy { input, keys } => {
                    let input = Box::new(Self::from_mir(input)?);
                    Plan::ArrangeBy {
                        input,
                        keys: keys.clone(),
                    }
                }
                MirRelationExpr::DeclareKeys { input, keys: _ } => Self::from_mir(input)?,
            };

            // If the plan stage did not absorb all linear operators, introduce a new stage to implement them.
            if !mfp.is_identity() {
                Ok(Plan::Mfp {
                    input: Box::new(plan),
                    mfp,
                })
            } else {
                Ok(plan)
            }
        }

        /// Convert the dataflow description into one that uses render plans.
        pub fn finalize_dataflow(
            desc: DataflowDescription<OptimizedMirRelationExpr>,
        ) -> DataflowDescription<Self> {
            let objects_to_build = desc
                .objects_to_build
                .into_iter()
                .map(|build| dataflow_types::BuildDesc {
                    id: build.id,
                    view: Self::from_mir(&build.view)
                        .expect("Dataflow finalization failed to produce plan"),
                })
                .collect::<Vec<_>>();
            DataflowDescription {
                source_imports: desc.source_imports,
                index_imports: desc.index_imports,
                objects_to_build,
                index_exports: desc.index_exports,
                sink_exports: desc.sink_exports,
                dependent_objects: desc.dependent_objects,
                as_of: desc.as_of,
                debug_name: desc.debug_name,
            }
        }
    }

    /// Pre-prends a MapFilterProject instance with a transform that blanks out all but the columns in `demand`.
    fn prepend_mfp_demand(
        mfp: &mut MapFilterProject,
        relation_expr: &MirRelationExpr,
        demand: &[usize],
    ) {
        let output_arity = relation_expr.arity();
        // Determine dummy columns for un-demanded outputs, and a projection.
        let mut dummies = Vec::new();
        let mut demand_projection = Vec::new();
        for (column, typ) in relation_expr.typ().column_types.into_iter().enumerate() {
            if demand.contains(&column) {
                demand_projection.push(column);
            } else {
                demand_projection.push(output_arity + dummies.len());
                dummies.push(MirScalarExpr::literal_ok(Datum::Dummy, typ.scalar_type));
            }
        }

        let (map, filter, project) = mfp.as_map_filter_project();

        let map_filter_project = MapFilterProject::new(output_arity)
            .map(dummies)
            .project(demand_projection)
            .map(map)
            .filter(filter)
            .project(project);

        *mfp = map_filter_project;
    }
}
