// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An explicit representation of a rendering plan for provided dataflows.

#![warn(missing_debug_implementations, missing_docs)]

pub mod join;
pub mod reduce;
pub mod threshold;
pub mod top_k;

use join::{DeltaJoinPlan, JoinPlan, LinearJoinPlan};
use reduce::{KeyValPlan, ReducePlan};
use threshold::ThresholdPlan;
use top_k::TopKPlan;

use serde::{Deserialize, Serialize};

use crate::DataflowDescription;
use expr::{
    EvalError, Id, JoinInputMapper, LocalId, MapFilterProject, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, TableFunc,
};

use repr::{Datum, Diff, Row};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use self::join::delta_join::DeltaPathPlan;
use self::join::delta_join::DeltaStagePlan;

/// A rendering plan with as much conditional logic as possible removed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Plan {
    /// A collection containing a pre-determined collection.
    Constant {
        /// Explicit update triples for the collection.
        rows: Result<Vec<(Row, repr::Timestamp, Diff)>, EvalError>,
    },
    /// A reference to a bound collection.
    ///
    /// This is commonly either an external reference to an existing source or
    /// maintained arrangement, or an internal reference to a `Let` identifier.
    Get {
        /// A global or local identifier naming the collection.
        id: Id,
        /// Arrangements that will be available.
        ///
        /// The collection will also be loaded if available, which it will
        /// not be for imported data, but which it may be for locally defined
        /// data.
        // TODO: Be more explicit about whether a collection is available,
        // although one can always produce it from an arrangement, and it
        // seems generally advantageous to do that instead (to avoid cloning
        // rows, by using `mfp` first on borrowed data).
        keys: Vec<Vec<MirScalarExpr>>,
        /// Any linear operator work to apply as part of producing the data.
        ///
        /// This logic allows us to efficiently extract collections from data
        /// that have been pre-arranged, avoiding copying rows that are not
        /// used and columns that are projected away.
        mfp: MapFilterProject,
        /// Whether the input is from an arrangement, and if so,
        /// whether we can seek to a specific value therein
        key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
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
        /// Whether the input is from an arrangement, and if so,
        /// whether we can seek to a specific value therein
        input_key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
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
        /// The particular arrangement of the input we expect to use,
        /// if any
        input_key: Option<Vec<MirScalarExpr>>,
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
        /// The particular arrangement of the input we expect to use,
        /// if any
        input_key: Option<Vec<MirScalarExpr>>,
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
        /// The input collections
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
        /// The arity of the collection
        arity: usize,
    },
}

impl Plan {
    /// This method converts a MirRelationExpr into a plan that can be directly rendered.
    ///
    /// The rough structure is that we repeatedly extract map/filter/project operators
    /// from each expression we see, bundle them up as a `MapFilterProject` object, and
    /// then produce a plan for the combination of that with the next operator.
    ///
    /// The method takes as an argument the existing arrangements for each bound identifier,
    /// which it will locally add to and remove from for `Let` bindings (by the end of the
    /// call it should contain the same bindings as when it started).
    ///
    /// The result of the method is both a `Plan`, but also a list of arrangements that
    /// are certain to be produced, which can be relied on by the next steps in the plan.
    /// Each of the arrangement keys is associated with an MFP that must be applied if that arrangement is used,
    /// to back out the permutation associated with that arrangement.

    /// An empty list of arrangement keys indicates that only a `Collection` stream can
    /// be assumed to exist.
    pub fn from_mir(
        expr: &MirRelationExpr,
        arrangements: &mut BTreeMap<Id, Vec<Vec<MirScalarExpr>>>,
    ) -> Result<(Self, Vec<Vec<MirScalarExpr>>), ()> {
        // This function is recursive and can overflow its stack, so grow it if
        // needed. The growth here is unbounded. Our general solution for this problem
        // is to use [`ore::stack::RecursionGuard`] to additionally limit the stack
        // depth. That however requires upstream error handling. This function is
        // currently called by the Coordinator after calls to `catalog_transact`,
        // and thus are not allowed to fail. Until that allows errors, we choose
        // to allow the unbounded growth here. We are though somewhat protected by
        // higher levels enforcing their own limits on stack depth (in the parser,
        // transformer/desugarer, and planner).
        ore::stack::maybe_grow(|| Plan::from_mir_inner(expr, arrangements))
    }

    fn from_mir_inner(
        expr: &MirRelationExpr,
        arrangements: &mut BTreeMap<Id, Vec<Vec<MirScalarExpr>>>,
    ) -> Result<(Self, Vec<Vec<MirScalarExpr>>), ()> {
        // let output_arity = expr.arity();
        // Extract a maximally large MapFilterProject from `expr`.
        // We will then try and push this in to the resulting expression.
        //
        // Importantly, `mfp` may contain temporal operators and not be a "safe" MFP.
        // While we would eventually like all plan stages to be able to absorb such
        // general operators, not all of them can.
        let (mut mfp, expr) = MapFilterProject::extract_from_expression(expr);
        // We attempt to plan what we have remaining, in the context of `mfp`.
        // We may not be able to do this, and must wrap some operators with a `Mfp` stage.
        let (mut plan, mut keys) = match expr {
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
                // The plan, not arranged in any way.
                (plan, Vec::new())
            }
            MirRelationExpr::Get { id, typ: _ } => {
                // This stage can absorb arbitrary MFP operators.
                let mut mfp = mfp.take();
                // If `mfp` is the identity, we can surface all imported arrangements.
                // Otherwise, we apply `mfp` and promise no arrangements.
                let mut in_keys = arrangements.get(id).cloned().unwrap_or_else(Vec::new);

                // Seek out an arrangement key that might be constrained to a literal.
                // TODO: Improve key selection heuristic.
                let key_val = in_keys
                    .iter()
                    .filter_map(|key| {
                        mfp.literal_constraints(key)
                            .map(|val| (key.clone(), Some(val)))
                    })
                    .max_by_key(|(key, _val)| key.len())
                    .or_else(|| in_keys.iter().next().map(|key| (key.clone(), None)));

                if let Some((key, _)) = &key_val {
                    mfp = mfp.permute_for_arrangement(&*key);
                }

                let out_keys = if mfp.is_identity() {
                    in_keys.clone()
                } else {
                    Vec::new()
                };

                // If we discover a literal constraint, we can discard other arrangements.
                if let Some((key, Some(_))) = &key_val {
                    in_keys = vec![key.clone()];
                }
                // Return the plan, and any keys if an identity `mfp`.
                (
                    Plan::Get {
                        id: id.clone(),
                        keys: in_keys,
                        mfp,
                        key_val,
                    },
                    out_keys,
                )
            }
            MirRelationExpr::Let { id, value, body } => {
                // It would be unfortunate to have a non-trivial `mfp` here, as we hope
                // that they would be pushed down. I am not sure if we should take the
                // initiative to push down the `mfp` ourselves.

                // Plan the value using only the initial arrangements, but
                // introduce any resulting arrangements bound to `id`.
                let (value, v_keys) = Plan::from_mir(value, arrangements)?;
                let pre_existing = arrangements.insert(Id::Local(*id), v_keys);
                assert!(pre_existing.is_none());
                // Plan the body using initial and `value` arrangements,
                // and then remove reference to the value arrangements.
                let (body, b_keys) = Plan::from_mir(body, arrangements)?;
                arrangements.remove(&Id::Local(*id));
                // Return the plan, and any `body` arrangements.
                (
                    Plan::Let {
                        id: id.clone(),
                        value: Box::new(value),
                        body: Box::new(body),
                    },
                    b_keys,
                )
            }
            MirRelationExpr::FlatMap { input, func, exprs } => {
                let (input, keys) = Plan::from_mir(input, arrangements)?;
                // This stage can absorb arbitrary MFP instances.
                let mut mfp = mfp.take();
                let input_key = if let Some(k) = keys.get(0) {
                    mfp = mfp.permute_for_arrangement(&*k);
                    Some(k.clone())
                } else {
                    None
                };
                // Return the plan, and no arrangements.
                (
                    Plan::FlatMap {
                        input: Box::new(input),
                        func: func.clone(),
                        exprs: exprs.clone(),
                        mfp,
                        input_key,
                    },
                    Vec::new(),
                )
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                implementation,
            } => {
                let input_mapper = JoinInputMapper::new(inputs);

                // Plan each of the join inputs independently.
                // The `plans` get surfaced upwards, and the `input_keys` should
                // be used as part of join planning / to validate the existing
                // plans / to aid in indexed seeding of update streams.
                let mut plans = Vec::new();
                let mut input_keys = Vec::<HashSet<_>>::new();
                let mut input_arities = Vec::new();
                for input in inputs.iter() {
                    let arity = input.arity();
                    let (plan, keys) = Plan::from_mir(input, arrangements)?;
                    plans.push(plan);
                    input_keys.push(keys.into_iter().collect());
                    input_arities.push(arity);
                }

                // Extract temporal predicates as joins cannot currently absorb them.
                let plan = match implementation {
                    expr::JoinImplementation::Differential((start, _start_arr), order) => {
                        let source_key = input_keys[*start].get(0).cloned();
                        JoinPlan::Linear(LinearJoinPlan::create_from(
                            *start,
                            source_key,
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
                let mut required_arrangements = vec![HashSet::new(); inputs.len()];
                // Delta joins should only be planned if a particular set of arrangements exists.
                // Empirically, we've found that there are sometimes bugs causing them to be planned
                // anyway. If this is the case, we need to create the arrangements, so we do so here,
                // and complain with an error message.
                if let JoinPlan::Delta(DeltaJoinPlan { path_plans }) = &plan {
                    for DeltaPathPlan { stage_plans, .. } in path_plans {
                        for DeltaStagePlan {
                            lookup_relation,
                            lookup_key,
                            ..
                        } in stage_plans
                        {
                            required_arrangements[*lookup_relation].insert(lookup_key.clone());
                        }
                    }
                } else {
                    // Linear joins handle rendering all the arrangements they need.
                }
                for (((arrangements, plan), required_arrangements), &arity) in input_keys
                    .iter()
                    .zip(plans.iter_mut())
                    .zip(required_arrangements.iter())
                    .zip(input_arities.iter())
                {
                    let missing: Vec<_> = required_arrangements
                        .difference(arrangements)
                        .cloned()
                        .collect();
                    if !missing.is_empty() {
                        tracing::error!("Arrangements depended on by delta join alarmingly absent: {:?}
This is not expected to cause incorrect results, but could indicate a performance issue in Materialize.", missing);
                        let new_ensure_arrangements = missing.into_iter().map(|key| {
                            let (permutation, thinning_expression) =
                                Permutation::construct_from_expr(&key, arity);
                            (key, permutation, thinning_expression)
                        });
                        if let Plan::ArrangeBy {
                            ensure_arrangements,
                            ..
                        } = plan
                        {
                            ensure_arrangements.extend(new_ensure_arrangements);
                        } else {
                            let base_plan =
                                std::mem::replace(plan, Plan::Constant { rows: Ok(vec![]) });
                            *plan = Plan::ArrangeBy {
                                input: Box::new(base_plan),
                                ensure_arrangements: new_ensure_arrangements.collect(),
                            }
                        }
                    }
                }
                // Return the plan, and no arrangements.
                (
                    Plan::Join {
                        inputs: plans,
                        plan,
                    },
                    Vec::new(),
                )
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => {
                let input_arity = input.arity();
                let (input, keys) = Self::from_mir(input, arrangements)?;
                let input_key = keys.get(0).map(|k| k.clone());
                let key_val_plan =
                    KeyValPlan::new(input_arity, group_key, aggregates, input_key.as_deref());
                let reduce_plan =
                    ReducePlan::create_from(aggregates.clone(), *monotonic, *expected_group_size);
                let output_keys = reduce_plan.keys(group_key.len());
                // Return the plan, and the keys it produces.
                (
                    Plan::Reduce {
                        input: Box::new(input),
                        key_val_plan,
                        plan: reduce_plan,
                        input_key,
                    },
                    output_keys,
                )
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
                let (input, keys) = Self::from_mir(input, arrangements)?;

                let top_k_plan = TopKPlan::create_from(
                    group_key.clone(),
                    order_key.clone(),
                    *offset,
                    *limit,
                    arity,
                    *monotonic,
                );

                // We don't have an MFP here -- install one to permute the
                // input, if necessary.
                let input_key = keys.get(0).map(|k| k.clone());
                let input = if let Some(input_key) = input_key {
                    let mfp = MapFilterProject::new(arity).permute_for_arrangement(&*input_key);
                    Plan::Mfp {
                        input: Box::new(input),
                        mfp,
                        input_key_val: Some((input_key, None)),
                    }
                } else {
                    input
                };
                // Return the plan, and no arrangements.
                (
                    Plan::TopK {
                        input: Box::new(input),
                        top_k_plan,
                    },
                    Vec::new(),
                )
            }
            MirRelationExpr::Negate { input } => {
                let arity = input.arity();
                let (input, keys) = Self::from_mir(input, arrangements)?;

                // We don't have an MFP here -- install one to permute the
                // input, if necessary.
                let input_key = keys.get(0).map(|k| k.clone());
                let input = if let Some(input_key) = input_key {
                    let mfp = MapFilterProject::new(arity).permute_for_arrangement(&*input_key);
                    Plan::Mfp {
                        input: Box::new(input),
                        mfp,
                        input_key_val: Some((input_key, None)),
                    }
                } else {
                    input
                };
                // Return the plan, and no arrangements.
                (
                    Plan::Negate {
                        input: Box::new(input),
                    },
                    Vec::new(),
                )
            }
            MirRelationExpr::Threshold { input } => {
                let arity = input.arity();
                let (input, keys) = Self::from_mir(input, arrangements)?;
                // We don't have an MFP here -- install one to permute the
                // input, if necessary.
                let input_key = keys.get(0).map(|k| k.clone());
                let input = if let Some(input_key) = input_key {
                    let mfp = MapFilterProject::new(arity).permute_for_arrangement(&*input_key);
                    Plan::Mfp {
                        input: Box::new(input),
                        mfp,
                        input_key_val: Some((input_key, None)),
                    }
                } else {
                    input
                };
                let threshold_plan = ThresholdPlan::create_from(arity, false);
                let output_keys = threshold_plan.keys();
                // Return the plan, and any produced keys.
                (
                    Plan::Threshold {
                        input: Box::new(input),
                        threshold_plan,
                    },
                    output_keys,
                )
            }
            MirRelationExpr::Union { base, inputs } => {
                let input_arity = base.arity();
                let mut plans_keys = Vec::with_capacity(1 + inputs.len());
                let (plan, keys) = Self::from_mir(base, arrangements)?;
                plans_keys.push((plan, keys));
                for input in inputs.iter() {
                    let (plan, keys) = Self::from_mir(input, arrangements)?;
                    plans_keys.push((plan, keys));
                }
                let plans = plans_keys
                    .into_iter()
                    .map(|(plan, keys)| {
                        // We don't have an MFP here -- install one to permute the
                        // input, if necessary.
                        let input_key = keys.get(0).map(|k| k.clone());
                        if let Some(input_key) = input_key {
                            let mfp = MapFilterProject::new(input_arity)
                                .permute_for_arrangement(&*input_key);
                            Plan::Mfp {
                                input: Box::new(plan),
                                mfp,
                                input_key_val: Some((input_key, None)),
                            }
                        } else {
                            plan
                        }
                    })
                    .collect();
                // Return the plan and no arrangements.
                let plan = Plan::Union { inputs: plans };
                (plan, Vec::new())
            }
            MirRelationExpr::ArrangeBy { input, keys } => {
                let arity = input.arity();
                let (input, mut input_keys) = Self::from_mir(input, arrangements)?;
                input_keys.extend(keys.iter().cloned());
                input_keys.sort();
                input_keys.dedup();

                // Return the plan and extended keys.
                (
                    Plan::ArrangeBy {
                        input: Box::new(input),
                        keys: keys.clone(),
                        arity,
                    },
                    input_keys,
                )
            }
            MirRelationExpr::DeclareKeys { input, keys: _ } => Self::from_mir(input, arrangements)?,
        };

        // If the plan stage did not absorb all linear operators, introduce a new stage to implement them.
        if !mfp.is_identity() {
            // Normally the next node up would
            // permute its input, but in this case the arrangement will be
            // hidden by this MFP node, so we need to apply that permutation here.
            //
            // In order to do so, we need to decide which of the possibly multiple
            // input arrangements we will be using, which can be influenced by the
            // existence of a valid `key_val`.
            let permuted_mfps: Vec<_> = keys
                .iter()
                .map(|k| {
                    let mfp = mfp.clone();
                    mfp.permute_for_arrangement(&*k)
                })
                .collect();

            // Seek out an arrangement key that might be constrained to a literal.
            // TODO: Improve key selection heuristic.
            let key_val_index = keys
                .iter()
                .enumerate()
                .filter_map(|(i, key)| {
                    permuted_mfps[i]
                        .literal_constraints(key)
                        .map(|val| (i, key.clone(), val))
                })
                .max_by_key(|(_i, key, _val)| key.len());

            let (input_key_val, mfp) = if let Some((i, k, v)) = key_val_index {
                (Some((k, Some(v))), permuted_mfps[i].clone())
            } else if let Some(k) = keys.get(0) {
                (Some((k.clone(), None)), permuted_mfps[0].clone())
            } else {
                (None, mfp)
            };

            plan = Plan::Mfp {
                input: Box::new(plan),
                mfp,
                input_key_val,
            };
            keys = Vec::new();
        }

        Ok((plan, keys))
    }

    /// Convert the dataflow description into one that uses render plans.
    pub fn finalize_dataflow(
        desc: DataflowDescription<OptimizedMirRelationExpr>,
    ) -> Result<DataflowDescription<Self>, ()> {
        // Collect available arrangements by identifier.
        let mut arrangements = BTreeMap::new();
        // Sources might provide arranged forms of their data, in the future.
        // Indexes provide arranged forms of their data.
        for (index_desc, _type) in desc.index_imports.values() {
            arrangements
                .entry(Id::Global(index_desc.on_id))
                .or_insert_with(Vec::new)
                .push(index_desc.keys.clone());
        }
        // Build each object in order, registering the arrangements it forms.
        let mut objects_to_build = Vec::with_capacity(desc.objects_to_build.len());
        for build in desc.objects_to_build.into_iter() {
            let (plan, keys) = Self::from_mir(&build.view, &mut arrangements)?;
            arrangements.insert(Id::Global(build.id), keys);
            objects_to_build.push(crate::BuildDesc {
                id: build.id,
                view: plan,
            });
        }

        Ok(DataflowDescription {
            source_imports: desc.source_imports,
            index_imports: desc.index_imports,
            objects_to_build,
            index_exports: desc.index_exports,
            sink_exports: desc.sink_exports,
            dependent_objects: desc.dependent_objects,
            as_of: desc.as_of,
            debug_name: desc.debug_name,
        })
    }

    /// Partitions the plan into `parts` many disjoint pieces.
    ///
    /// This is used to partition `Plan::Constant` stages so that the work
    /// can be distributed across many workers.
    pub fn partition_among(self, parts: usize) -> Vec<Self> {
        if parts == 0 {
            Vec::new()
        } else if parts == 1 {
            vec![self]
        } else {
            match self {
                // For constants, balance the rows across the workers.
                Plan::Constant { rows } => match rows {
                    Ok(rows) => {
                        let mut rows_parts = vec![Vec::new(); parts];
                        for (index, row) in rows.into_iter().enumerate() {
                            rows_parts[index % parts].push(row);
                        }
                        rows_parts
                            .into_iter()
                            .map(|rows| Plan::Constant { rows: Ok(rows) })
                            .collect()
                    }
                    Err(err) => {
                        let mut result = vec![
                            Plan::Constant {
                                rows: Ok(Vec::new())
                            };
                            parts
                        ];
                        result[0] = Plan::Constant { rows: Err(err) };
                        result
                    }
                },

                // For all other variants, just replace inputs with appropriately sharded versions.
                // This is surprisingly verbose, but that is all it is doing.
                Plan::Get {
                    id,
                    keys,
                    mfp,
                    key_val,
                } => vec![
                    Plan::Get {
                        id,
                        keys,
                        mfp,
                        key_val,
                    };
                    parts
                ],
                Plan::Let { value, body, id } => {
                    let value_parts = value.partition_among(parts);
                    let body_parts = body.partition_among(parts);
                    value_parts
                        .into_iter()
                        .zip(body_parts)
                        .map(|(value, body)| Plan::Let {
                            value: Box::new(value),
                            body: Box::new(body),
                            id,
                        })
                        .collect()
                }
                Plan::Mfp {
                    input,
                    input_key_val,
                    mfp,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Mfp {
                        input: Box::new(input),
                        mfp: mfp.clone(),
                        input_key_val: input_key_val.clone(),
                    })
                    .collect(),
                Plan::FlatMap {
                    input,
                    input_key,
                    func,
                    exprs,
                    mfp,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::FlatMap {
                        input: Box::new(input),
                        input_key: input_key.clone(),
                        func: func.clone(),
                        exprs: exprs.clone(),
                        mfp: mfp.clone(),
                    })
                    .collect(),
                Plan::Join { inputs, plan } => {
                    let mut inputs_parts = vec![Vec::new(); parts];
                    for input in inputs.into_iter() {
                        for (index, input_part) in
                            input.partition_among(parts).into_iter().enumerate()
                        {
                            inputs_parts[index].push(input_part);
                        }
                    }
                    inputs_parts
                        .into_iter()
                        .map(|inputs| Plan::Join {
                            inputs,
                            plan: plan.clone(),
                        })
                        .collect()
                }
                Plan::Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Reduce {
                        input: Box::new(input),
                        input_key: input_key.clone(),
                        key_val_plan: key_val_plan.clone(),
                        plan: plan.clone(),
                    })
                    .collect(),
                Plan::TopK { input, top_k_plan } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::TopK {
                        input: Box::new(input),
                        top_k_plan: top_k_plan.clone(),
                    })
                    .collect(),
                Plan::Negate { input } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Negate {
                        input: Box::new(input),
                    })
                    .collect(),
                Plan::Threshold {
                    input,
                    threshold_plan,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Threshold {
                        input: Box::new(input),
                        threshold_plan: threshold_plan.clone(),
                    })
                    .collect(),
                Plan::Union { inputs } => {
                    let mut inputs_parts = vec![Vec::new(); parts];
                    for input in inputs.into_iter() {
                        for (index, input_part) in
                            input.partition_among(parts).into_iter().enumerate()
                        {
                            inputs_parts[index].push(input_part);
                        }
                    }
                    inputs_parts
                        .into_iter()
                        .map(|inputs| Plan::Union { inputs })
                        .collect()
                }
                Plan::ArrangeBy { input, keys, arity } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::ArrangeBy {
                        input: Box::new(input),
                        keys: keys.clone(),
                        arity,
                    })
                    .collect(),
            }
        }
    }
}

/// Helper method to convert linear operators to MapFilterProject instances.
///
/// This method produces a `MapFilterProject` instance that first applies any predicates,
/// and then introduces `Datum::Dummy` literals in columns that are not demanded.
/// The `RelationType` is required so that we can fill in the correct type of `Datum::Dummy`.
pub fn linear_to_mfp(linear: crate::LinearOperator, typ: &repr::RelationType) -> MapFilterProject {
    let crate::types::LinearOperator {
        predicates,
        projection,
    } = linear;

    let arity = typ.arity();
    let mut dummies = Vec::new();
    let mut demand_projection = Vec::new();
    for (column, typ) in typ.column_types.iter().enumerate() {
        if projection.contains(&column) {
            demand_projection.push(column);
        } else {
            demand_projection.push(arity + dummies.len());
            dummies.push(MirScalarExpr::literal_ok(
                Datum::Dummy,
                typ.scalar_type.clone(),
            ));
        }
    }

    // First filter, then introduce and reposition `Datum::Dummy` values.
    MapFilterProject::new(arity)
        .filter(predicates)
        .map(dummies)
        .project(demand_projection)
}

/// Return the set of columns in a relation of a given arity that are not redundant with a given key.
///
/// Example:
/// Given a four-column relation that is to be arranged by the key [Column(0), Column(2)],
/// this function will return [1, 3], as we may store only those columns in the value of the arrangement
/// with no loss of information.
pub fn make_thinning_expression(key: &[MirScalarExpr], arity: usize) -> Vec<usize> {
    let columns_in_key: HashSet<_> = key.iter().filter_map(|expr| expr.as_column()).collect();
    (0..arity)
        .into_iter()
        .filter(|c| !columns_in_key.contains(&c))
        .collect()
}
