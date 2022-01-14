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
use persist::client::RuntimeClient;
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
use crate::metrics::Metrics;
use crate::render::context::CollectionBundle;
use crate::render::context::{ArrangementFlavor, Context};
use crate::server::LocalInput;
use crate::sink::SinkBaseMetrics;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::timestamp::TimestampBindingRc;
use crate::source::SourceToken;

mod context;
mod flat_map;
mod join;
mod reduce;
pub mod sinks;
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
    /// Metrics reported by all dataflows.
    pub metrics: Metrics,
    /// Handle to the persistence runtime. None if disabled.
    pub persist: Option<RuntimeClient>,
    /// Shared buffer with TAIL operator instances by which they can respond.
    ///
    /// The entries are pairs of sink identifier (to identify the tail instance)
    /// and the response itself.
    pub tail_response_buffer: Rc<RefCell<Vec<(GlobalId, TailResponse)>>>,
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
    source_metrics: &SourceBaseMetrics,
    sink_metrics: &SinkBaseMetrics,
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
            for (src_id, (src, orig_id)) in &dataflow.source_imports {
                context.import_source(
                    render_state,
                    &mut tokens,
                    region,
                    materialized_logging.clone(),
                    src_id.clone(),
                    src.clone(),
                    orig_id.clone(),
                    now.clone(),
                    source_metrics,
                );
            }

            // Import declared indexes into the rendering context.
            for (idx_id, idx) in &dataflow.index_imports {
                context.import_index(render_state, &mut tokens, scope, region, *idx_id, &idx.0);
            }

            // We first determine indexes and sinks to export, then build the declared object, and
            // finally export indexes and sinks. The reason for this is that we want to avoid
            // cloning the dataflow plan for `build_object`, which can be expensive.

            // Determine indexes to export
            let indexes = dataflow
                .index_exports
                .iter()
                .cloned()
                .map(|(idx_id, idx, _typ)| (idx_id, dataflow.get_imports(&idx.on_id), idx))
                .collect::<Vec<_>>();

            // Determine sinks to export
            let sinks = dataflow
                .sink_exports
                .iter()
                .cloned()
                .map(|(sink_id, sink)| (sink_id, dataflow.get_imports(&sink.from), sink))
                .collect::<Vec<_>>();

            // Build declared objects.
            for object in dataflow.objects_to_build {
                context.build_object(region, object);
            }

            // Export declared indexes.
            for (idx_id, imports, idx) in indexes {
                context.export_index(render_state, &mut tokens, imports, idx_id, &idx);
            }

            // Export declared sinks.
            for (sink_id, imports, sink) in sinks {
                context.export_sink(
                    render_state,
                    &mut tokens,
                    imports,
                    sink_id,
                    &sink,
                    sink_metrics,
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
            let permutation = traces.permutation().clone();
            self.update_id(
                Id::Global(idx.on_id),
                CollectionBundle::from_expressions(
                    idx.keys.clone(),
                    ArrangementFlavor::Trace(idx_id, ok_arranged, err_arranged, permutation),
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
            Some(ArrangementFlavor::Local(oks, errs, permutation)) => {
                render_state.traces.set(
                    idx_id,
                    TraceBundle::new(oks.trace, errs.trace, permutation).with_drop(tokens),
                );
            }
            Some(ArrangementFlavor::Trace(gid, _, _, _)) => {
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
        match plan {
            Plan::Constant { rows } => {
                // Produce both rows and errs to avoid conditional dataflow construction.
                let (mut rows, errs) = match rows {
                    Ok(rows) => (rows, Vec::new()),
                    Err(e) => (Vec::new(), vec![e]),
                };

                // We should advance times in constant collections to start from `as_of`.
                use differential_dataflow::lattice::Lattice;
                for (_, time, _) in rows.iter_mut() {
                    time.advance_by(self.as_of_frontier.borrow());
                }
                let mut error_time: G::Timestamp = timely::progress::Timestamp::minimum();
                error_time.advance_by(self.as_of_frontier.borrow());

                let ok_collection = rows.into_iter().to_stream(scope).as_collection();

                let err_collection = errs
                    .into_iter()
                    .map(move |e| (DataflowError::from(e), error_time, 1))
                    .to_stream(scope)
                    .as_collection();

                CollectionBundle::from_collections(ok_collection, err_collection)
            }
            Plan::Get {
                id,
                keys,
                mfp,
                key_val,
            } => {
                // Recover the collection from `self` and then apply `mfp` to it.
                // If `mfp` happens to be trivial, we can just return the collection.
                let mut collection = self
                    .lookup_id(id)
                    .unwrap_or_else(|| panic!("Get({:?}) not found at render time", id));
                if mfp.is_identity() {
                    // Assert that each of `keys` are present in `collection`.
                    assert!(keys.iter().all(|key| collection.arranged.contains_key(key)));
                    // Retain only those keys we want to import.
                    collection.arranged.retain(|key, _value| keys.contains(key));
                    collection
                } else {
                    let (oks, errs) = collection.as_collection_core(mfp, key_val);
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
            Plan::Mfp {
                input,
                mfp,
                key_val,
            } => {
                // If `mfp` is non-trivial, we should apply it and produce a collection.
                let input = self.render_plan(*input, scope, worker_index);
                if mfp.is_identity() {
                    input
                } else {
                    let (oks, errs) = input.as_collection_core(mfp, key_val);
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
                    dataflow_types::plan::join::JoinPlan::Linear(linear_plan) => {
                        self.render_join(inputs, linear_plan, scope)
                    }
                    dataflow_types::plan::join::JoinPlan::Delta(delta_plan) => {
                        self.render_delta_join(inputs, delta_plan, scope)
                    }
                }
            }
            Plan::Reduce {
                input,
                key_val_plan,
                plan,
                permutation,
            } => {
                let input = self.render_plan(*input, scope, worker_index);
                self.render_reduce(input, key_val_plan, plan, permutation)
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
            Plan::ArrangeBy {
                input,
                ensure_arrangements,
            } => {
                let input = self.render_plan(*input, scope, worker_index);
                input.ensure_arrangements(ensure_arrangements)
            }
        }
    }
}
