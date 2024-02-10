// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::AsCollection;
use mz_compute_types::plan::join::JoinPlan;
use mz_compute_types::plan::reduce::{KeyValPlan, ReducePlan};
use mz_compute_types::plan::threshold::ThresholdPlan;
use mz_compute_types::plan::top_k::TopKPlan;
use mz_compute_types::plan::{AvailableCollections, GetPlan, Plan};
use mz_expr::{EvalError, Id, LocalId, MapFilterProject, MirScalarExpr, TableFunc};
use mz_repr::{Diff, Row};
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;
use timely::dataflow::operators::ToStream;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;

use crate::render::context::Context;
use crate::render::{CollectionBundle, RenderTimestamp};
use crate::typedefs::KeyBatcher;

/// A (mostly) non-recursive representation of [`Plan`].
///
/// This representation enables an iterative, rather than recursive, rendering implementation. Such
/// an implementations is advantageous because it avoids the risk of stack overflows.
///
/// A `FlatPlan` is obtained by removing fields that hold `Plan`s. There are two exceptions:
///
///  * `Let` retains its `body` plan. `Let` is the only variant that cannot be rendered in a
///    postorder fashion, so we keep rendering it recursively for simplicity.
///  * `LetRec` is not supported. Rendering code should handle `LetRec`s separately.
#[derive(Debug)]
pub(super) enum FlatPlan {
    Constant {
        rows: Result<Vec<(Row, mz_repr::Timestamp, Diff)>, EvalError>,
    },
    Get {
        id: Id,
        keys: AvailableCollections,
        plan: GetPlan,
    },
    Let {
        id: LocalId,
        body: Plan,
    },
    Mfp {
        mfp: MapFilterProject,
        input_key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
    },
    FlatMap {
        func: TableFunc,
        exprs: Vec<MirScalarExpr>,
        mfp_after: MapFilterProject,
        input_key: Option<Vec<MirScalarExpr>>,
    },
    Join {
        arity: usize,
        plan: JoinPlan,
    },
    Reduce {
        key_val_plan: KeyValPlan,
        plan: ReducePlan,
        input_key: Option<Vec<MirScalarExpr>>,
        mfp_after: MapFilterProject,
    },
    TopK(TopKPlan),
    Negate,
    Threshold(ThresholdPlan),
    Union {
        arity: usize,
        consolidate_output: bool,
    },
    ArrangeBy {
        forms: AvailableCollections,
        input_key: Option<Vec<MirScalarExpr>>,
        input_mfp: MapFilterProject,
    },
}

pub(super) fn flatten_plan(
    plan: Plan,
) -> (FlatPlan, impl Iterator<Item = Plan> + DoubleEndedIterator) {
    use Plan::*;

    let mut single_child = None;
    let mut many_children = Vec::new();

    let flat_plan = match plan {
        Constant { rows, node_id: _ } => FlatPlan::Constant { rows },
        Get {
            id,
            keys,
            plan,
            node_id: _,
        } => FlatPlan::Get { id, keys, plan },
        Let {
            id,
            value,
            body,
            node_id: _,
        } => {
            single_child = Some(*value);
            FlatPlan::Let { id, body: *body }
        }
        LetRec { .. } => panic!("`LetRec` cannot be flattened"),
        Mfp {
            input,
            mfp,
            input_key_val,
            node_id: _,
        } => {
            single_child = Some(*input);
            FlatPlan::Mfp { mfp, input_key_val }
        }
        FlatMap {
            input,
            func,
            exprs,
            mfp_after,
            input_key,
            node_id: _,
        } => {
            single_child = Some(*input);
            FlatPlan::FlatMap {
                func,
                exprs,
                mfp_after,
                input_key,
            }
        }
        Join {
            inputs,
            plan,
            node_id: _,
        } => {
            let arity = inputs.len();
            many_children = inputs;
            FlatPlan::Join { arity, plan }
        }
        Reduce {
            input,
            key_val_plan,
            plan,
            input_key,
            mfp_after,
            node_id: _,
        } => {
            single_child = Some(*input);
            FlatPlan::Reduce {
                key_val_plan,
                plan,
                input_key,
                mfp_after,
            }
        }
        TopK {
            input,
            top_k_plan,
            node_id: _,
        } => {
            single_child = Some(*input);
            FlatPlan::TopK(top_k_plan)
        }
        Negate { input, node_id: _ } => {
            single_child = Some(*input);
            FlatPlan::Negate
        }
        Threshold {
            input,
            threshold_plan,
            node_id: _,
        } => {
            single_child = Some(*input);
            FlatPlan::Threshold(threshold_plan)
        }
        Union {
            inputs,
            consolidate_output,
            node_id: _,
        } => {
            let arity = inputs.len();
            many_children = inputs;
            FlatPlan::Union {
                arity,
                consolidate_output,
            }
        }
        ArrangeBy {
            input,
            forms,
            input_key,
            input_mfp,
            node_id: _,
        } => {
            single_child = Some(*input);
            FlatPlan::ArrangeBy {
                forms,
                input_key,
                input_mfp,
            }
        }
    };

    let children = single_child.into_iter().chain(many_children.into_iter());

    (flat_plan, children)
}

impl<G> Context<G>
where
    G: Scope,
    G::Timestamp: RenderTimestamp,
{
    pub(super) fn render_flat_plan(
        &mut self,
        plan: FlatPlan,
        inputs: &mut VecDeque<CollectionBundle<G>>,
    ) -> CollectionBundle<G>
    where
        G: Scope,
        G::Timestamp: RenderTimestamp,
    {
        use FlatPlan::*;

        match plan {
            Constant { rows } => self.render_constant(rows),
            Get { id, keys, plan } => self.render_get(id, keys, plan),
            Let { id, body } => {
                let value = inputs.pop_front().unwrap();
                self.render_let(id, value, body)
            }
            Mfp { mfp, input_key_val } => {
                let input = inputs.pop_front().unwrap();
                self.render_mfp(input, mfp, input_key_val)
            }
            FlatMap {
                func,
                exprs,
                mfp_after,
                input_key,
            } => {
                let input = inputs.pop_front().unwrap();
                self.render_flat_map(input, func, exprs, mfp_after, input_key)
            }
            Join { arity, plan } => {
                let inputs = inputs.drain(..arity).collect();
                self.render_join(inputs, plan)
            }
            Reduce {
                key_val_plan,
                plan,
                input_key,
                mfp_after,
            } => {
                let input = inputs.pop_front().unwrap();
                let mfp_after = (!mfp_after.is_identity()).then_some(mfp_after);
                self.render_reduce(input, key_val_plan, plan, input_key, mfp_after)
            }
            TopK(plan) => {
                let input = inputs.pop_front().unwrap();
                self.render_topk(input, plan)
            }
            Negate => {
                let input = inputs.pop_front().unwrap();
                self.render_negate(input)
            }
            Threshold(plan) => {
                let input = inputs.pop_front().unwrap();
                self.render_threshold(input, plan)
            }
            Union {
                arity,
                consolidate_output,
            } => {
                let inputs = inputs.drain(..arity).collect();
                self.render_union(inputs, consolidate_output)
            }
            ArrangeBy {
                forms,
                input_key,
                input_mfp,
            } => {
                let input = inputs.pop_front().unwrap();
                self.render_arrange_by(input, forms, input_key, input_mfp)
            }
        }
    }

    fn render_constant(
        &mut self,
        rows: Result<Vec<(Row, mz_repr::Timestamp, i64)>, EvalError>,
    ) -> CollectionBundle<G> {
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
                    Some((row, G::Timestamp::to_inner(time), diff))
                } else {
                    None
                }
            })
            .to_stream(&mut self.scope)
            .as_collection();

        let mut error_time = mz_repr::Timestamp::MIN;
        error_time.advance_by(self.as_of_frontier.borrow());
        let err_collection = errs
            .into_iter()
            .map(move |e| {
                (
                    DataflowError::from(e),
                    G::Timestamp::to_inner(error_time),
                    1,
                )
            })
            .to_stream(&mut self.scope)
            .as_collection();

        CollectionBundle::from_collections(ok_collection, err_collection)
    }

    fn render_get(&self, id: Id, keys: AvailableCollections, plan: GetPlan) -> CollectionBundle<G> {
        // Recover the collection from `self` and then apply `mfp` to it.
        // If `mfp` happens to be trivial, we can just return the collection.
        let mut collection = self
            .lookup_id(id)
            .unwrap_or_else(|| panic!("Get({:?}) not found at render time", id));
        match plan {
            GetPlan::PassArrangements => {
                // Assert that each of `keys` are present in `collection`.
                assert!(keys
                    .arranged
                    .iter()
                    .all(|(key, _, _)| collection.arranged.contains_key(key)));
                assert!(keys.raw <= collection.collection.is_some());
                // Retain only those keys we want to import.
                collection
                    .arranged
                    .retain(|key, _value| keys.arranged.iter().any(|(key2, _, _)| key2 == key));
                collection
            }
            GetPlan::Arrangement(key, row, mfp) => {
                let (oks, errs) =
                    collection.as_collection_core(mfp, Some((key, row)), self.until.clone());
                CollectionBundle::from_collections(oks, errs)
            }
            GetPlan::Collection(mfp) => {
                let (oks, errs) = collection.as_collection_core(mfp, None, self.until.clone());
                CollectionBundle::from_collections(oks, errs)
            }
        }
    }

    fn render_let(
        &mut self,
        id: LocalId,
        value: CollectionBundle<G>,
        body: Plan,
    ) -> CollectionBundle<G> {
        // Bind `value` to `id`. Complain if this shadows an id.
        let prebound = self.insert_id(Id::Local(id), value);
        assert!(prebound.is_none());

        let body = self.render_plan(body);
        self.remove_id(Id::Local(id));
        body
    }

    fn render_mfp(
        &self,
        input: CollectionBundle<G>,
        mfp: MapFilterProject,
        input_key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
    ) -> CollectionBundle<G> {
        // If `mfp` is non-trivial, we should apply it and produce a collection.
        if mfp.is_identity() {
            input
        } else {
            let (oks, errs) = input.as_collection_core(mfp, input_key_val, self.until.clone());
            CollectionBundle::from_collections(oks, errs)
        }
    }

    fn render_join(
        &mut self,
        inputs: Vec<CollectionBundle<G>>,
        plan: JoinPlan,
    ) -> CollectionBundle<G> {
        match plan {
            mz_compute_types::plan::join::JoinPlan::Linear(linear_plan) => {
                self.render_linear_join(inputs, linear_plan)
            }
            mz_compute_types::plan::join::JoinPlan::Delta(delta_plan) => {
                self.render_delta_join(inputs, delta_plan)
            }
        }
    }

    fn render_negate(&self, input: CollectionBundle<G>) -> CollectionBundle<G> {
        let (oks, errs) = input.as_specific_collection(None);
        CollectionBundle::from_collections(oks.negate(), errs)
    }

    fn render_union(
        &mut self,
        inputs: Vec<CollectionBundle<G>>,
        consolidate_output: bool,
    ) -> CollectionBundle<G> {
        let mut oks = Vec::new();
        let mut errs = Vec::new();
        for input in inputs {
            let (os, es) = input.as_specific_collection(None);
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

    fn render_arrange_by(
        &self,
        input: CollectionBundle<G>,
        forms: AvailableCollections,
        input_key: Option<Vec<MirScalarExpr>>,
        input_mfp: MapFilterProject,
    ) -> CollectionBundle<G> {
        input.ensure_collections(
            forms,
            input_key,
            input_mfp,
            self.until.clone(),
            self.enable_specialized_arrangements,
        )
    }
}
