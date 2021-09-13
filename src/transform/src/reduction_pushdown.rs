// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tries to convert a reduce around a join to a join of reduces.
//! Also absorbs Map operators into Reduce operators.
//!
//! In a traditional DB, this transformation has a potential benefit of reducing
//! the size of the join. In our streaming system built on top of Timely
//! Dataflow and Differential Dataflow, there are two other potential benefits:
//! 1) Reducing data skew in the arrangements constructed for a join.
//! 2) The join can potentially reuse the final arrangement constructed for the
//!    reduce and not have to construct its own arrangement.
//! 3) Reducing the frequency with which we have to recalculate the result of a join.
//!
//! Suppose there are two inputs R and T being joined. According to
//! [Galindo-Legaria and Joshi (2001)](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf),
//! a full reduction pushdown to R can be done if and only if:
//! 1) Columns from R involved in join constraints are a subset of the group by keys.
//! 2) The key of T is a subset of the group by keys.
//! 3) The columns involved in the aggregation all belong to R.
//!
//! In our current implementation:
//! * We abide by condition 1 to the letter.
//! * We work around condition 2 by pushing the reduction down to both inputs.
//! * TODO: We work around condition 3 in some cases by noting that `sum(foo.a * bar.a)`
//!   is equivalent to `sum(foo.a) * sum(bar.a)`.
//!
//! Full documentation with examples can be found
//! [here](https://docs.google.com/document/d/1xrBJGGDkkiGBKRSNYR2W-nKba96ZOdC2mVbLqMLjJY0/edit)
//!
//! The current implementation is chosen so that reduction pushdown kicks in
//! only in the subset of cases mostly likely to help users. In the future, we
//! may allow the user to toggle the aggressiveness of reduction pushdown. A
//! more aggressive reduction pushdown implementation may, for example, try to
//! work around condition 1 by pushing down an inner reduce through the join
//! while retaining the original outer reduce.

use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

use crate::TransformArgs;
use expr::{AggregateExpr, JoinInputMapper, MirRelationExpr, MirScalarExpr};

/// Pushes Reduce operators toward sources.
#[derive(Debug)]
pub struct ReductionPushdown;

impl crate::Transform for ReductionPushdown {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        // `try_visit_mut_pre` is used here because after pushing down a reduction,
        // we want to see if we can push the same reduction further down.
        relation.try_visit_mut_pre(&mut |e| self.action(e))
    }
}

impl ReductionPushdown {
    /// Pushes Reduce operators toward sources.
    ///
    /// A join can be thought of as a multigraph where vertices are inputs and
    /// edges are join constraints. After removing constraints containing a
    /// GroupBy, the reduce will be pushed down to all connected components. If
    /// there is only one connected component, this method is a no-op.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } = relation
        {
            // Map expressions can be absorbed into the Reduce at no cost.
            if let MirRelationExpr::Map {
                input: inner,
                scalars,
            } = &mut **input
            {
                let arity = inner.arity();

                // Normalize the scalars to not be self-referential.
                let mut scalars = scalars.clone();
                for index in 0..scalars.len() {
                    let (lower, upper) = scalars.split_at_mut(index);
                    upper[0].visit_mut(&mut |e| {
                        if let expr::MirScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = lower[*c - arity].clone();
                            }
                        }
                    });
                }
                for key in group_key.iter_mut() {
                    key.visit_mut(&mut |e| {
                        if let expr::MirScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }
                for agg in aggregates.iter_mut() {
                    agg.expr.visit_mut(&mut |e| {
                        if let expr::MirScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }

                **input = inner.take_dangerous()
            }
            if let MirRelationExpr::Join {
                inputs,
                equivalences,
                implementation: _,
            } = &mut **input
            {
                if let Some(new_relation_expr) = try_push_reduction(
                    inputs,
                    equivalences,
                    group_key,
                    aggregates,
                    *monotonic,
                    *expected_group_size,
                ) {
                    *relation = new_relation_expr;
                }
            }
        }
        Ok(())
    }
}

fn try_push_reduction(
    inputs: &Vec<MirRelationExpr>,
    equivalences: &Vec<Vec<MirScalarExpr>>,
    group_key: &Vec<MirScalarExpr>,
    aggregates: &Vec<AggregateExpr>,
    monotonic: bool,
    expected_group_size: Option<usize>,
) -> Option<MirRelationExpr> {
    // Variable name details:
    // The goal is to turn `old` (`Reduce { Join { <inputs> }}`) into
    // `new`, which looks like:
    // ```
    // Project {
    //    Join {
    //      Reduce { <component> }, ... , Reduce { <component> }
    //    }
    // }
    // ```
    //
    // `<component>` is either `Join`

    let old_join_mapper =
        JoinInputMapper::new_from_input_types(&inputs.iter().map(|i| i.typ()).collect::<Vec<_>>());
    // 1) Partition the join constraints into constraints containing a group
    //    key and onstraints that don't.
    let (new_join_equivalences, component_equivalences): (Vec<_>, Vec<_>) = equivalences
        .to_vec()
        .into_iter()
        .partition(|cls| cls.iter().any(|expr| group_key.contains(expr)));

    // 2) Find the connected components that remain after removing constraints
    //    containing the group key. Also, track the set of constraints that
    //    connect the inputs in each component.
    let mut components = (0..inputs.len())
        .map(|i| (vec![i], Vec::new()))
        .collect::<Vec<_>>();
    for equivalence in component_equivalences {
        // a) Find the inputs referenced by the constraint.
        let inputs_to_connect = HashSet::<usize>::from_iter(
            equivalence
                .iter()
                .flat_map(|expr| old_join_mapper.lookup_inputs(expr)),
        );
        // b) Extract the set of components that covers the inputs.
        let (components_to_connect, other): (Vec<_>, Vec<_>) = components
            .into_iter()
            .partition(|(inputs, _)| inputs.iter().any(|i| inputs_to_connect.contains(i)));
        // c) Union the components. Add the current constraint to
        let mut connected_component = Vec::new();
        let mut all_constraints = vec![equivalence];
        for (mut componentn, mut constraintsn) in components_to_connect {
            connected_component.append(&mut componentn);
            all_constraints.append(&mut constraintsn);
        }
        connected_component.sort();
        connected_component.dedup();
        components = other;
        // d) Push the unioned component back into the list of components.
        components.push((connected_component, all_constraints));
        // e) Abort reduction pushdown if there are less than two connected components.
        if components.len() < 2 {
            return None;
        }
    }
    components.sort();

    // Maps (input idxs from old join) -> (idx of component it belongs to)
    let input_component_map = HashMap::from_iter(
        components
            .iter()
            .enumerate()
            .flat_map(|(new, (input_idxs, _))| input_idxs.iter().map(move |old| (*old, new))),
    );

    // 3) Construct a reduce to push to each input
    let mut new_reduces = components
        .into_iter()
        .map(|(input_idxs, constraints)| {
            ReduceBuilder::new(
                input_idxs.iter().map(|i| inputs[*i].clone()).collect(),
                constraints,
                input_idxs
                    .iter()
                    .flat_map(|i| old_join_mapper.global_columns(*i))
                    .enumerate()
                    .map(|(local, global)| (global, local))
                    .collect::<HashMap<_, _>>(),
            )
        })
        .collect::<Vec<_>>();

    // The new projection and new join equivalences will reference columns
    // produced by the new reduces, but we don't know the arities of the new
    // reduces yet. Thus, they are temporarily stored as
    // `(component_idx, column_idx_relative_to_new_reduce)`.
    let mut new_projection = Vec::with_capacity(group_key.len());
    let mut new_join_equivalences_by_component = Vec::new();

    // 3a) Calculate the group key for each reduction
    for key in group_key {
        // i) Find the equivalence class that the key is in.
        if let Some(cls) = new_join_equivalences
            .iter()
            .find(|cls| cls.iter().any(|expr| expr == key))
        {
            // ii) Rewrite the join equivalence in terms of columns produced by
            // the pushed down reduction.
            let mut new_join_cls = Vec::new();
            for expr in cls {
                if let Some(component) =
                    lookup_corresponding_component(expr, &old_join_mapper, &input_component_map)
                {
                    if key == expr {
                        new_projection.push((component, new_reduces[component].arity()));
                    }
                    new_join_cls.push((component, new_reduces[component].arity()));
                    new_reduces[component].add_group_key(expr.clone());
                } else {
                    // TODO: support reduction pushdown where a constraint
                    // contains an multi-component expression.
                    return None;
                }
            }
            new_join_equivalences_by_component.push(new_join_cls);
        } else if let Some(component) =
            lookup_corresponding_component(key, &old_join_mapper, &input_component_map)
        {
            // If GroupBy key does not belong in an equivalence class,
            // add the key to new projection + add it as a GroupBy key to
            // the new input
            new_projection.push((component, new_reduces[component].arity()));
            new_reduces[component].add_group_key(key.clone())
        } else {
            return None;
        }
    }

    // 3b) Deduce the aggregates that each reduce needs calculate in order to
    // reconstruct each aggregate in the old reduce.
    for agg in aggregates {
        if let Some(component) =
            lookup_corresponding_component(&agg.expr, &old_join_mapper, &input_component_map)
        {
            if agg.distinct == false {
                // TODO: support non-distinct aggs
                return None;
            }
            new_projection.push((component, new_reduces[component].arity()));
            new_reduces[component].add_aggregate(agg.clone());
        } else {
            // TODO: support multi- and zero- component aggs
            return None;
        }
    }

    // 4) Construct the new `MirRelationExpr`.
    let new_join_mapper = JoinInputMapper::new_from_input_arities(
        new_reduces.iter().map(|builder| builder.arity()).collect(),
    );

    let new_inputs = new_reduces
        .into_iter()
        .map(|builder| builder.construct_reduce(monotonic, expected_group_size))
        .collect::<Vec<_>>();

    let new_equivalences = new_join_equivalences_by_component
        .into_iter()
        .map(|cls| {
            cls.into_iter()
                .map(|(idx, col)| {
                    MirScalarExpr::Column(new_join_mapper.map_column_to_global(col, idx))
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let new_projection = new_projection
        .into_iter()
        .map(|(idx, col)| new_join_mapper.map_column_to_global(col, idx))
        .collect::<Vec<_>>();

    return Some(
        MirRelationExpr::join_scalars(new_inputs, new_equivalences).project(new_projection),
    );
}

fn lookup_corresponding_component(
    expr: &MirScalarExpr,
    old_join_mapper: &JoinInputMapper,
    input_component_map: &HashMap<usize, usize>,
) -> Option<usize> {
    let mut dedupped = old_join_mapper
        .lookup_inputs(expr)
        .map(|i| input_component_map[&i])
        .collect::<HashSet<_>>();
    if dedupped.len() == 1 {
        dedupped.drain().next()
    } else {
        None
    }
}

/// Constructs a Reduce around a component, localizing column references.
struct ReduceBuilder {
    input: MirRelationExpr,
    group_key: Vec<MirScalarExpr>,
    aggregates: Vec<AggregateExpr>,
    /// Maps (global column relative to old join) -> (local column relative to `input`)
    localize_map: HashMap<usize, usize>,
}

impl ReduceBuilder {
    fn new(
        mut inputs: Vec<MirRelationExpr>,
        mut equivalences: Vec<Vec<MirScalarExpr>>,
        localize_map: HashMap<usize, usize>,
    ) -> Self {
        for equivalence in equivalences.iter_mut() {
            for expr in equivalence.iter_mut() {
                expr.permute_map(&localize_map)
            }
        }
        let input = if inputs.len() == 1 {
            inputs.pop().unwrap()
        } else {
            MirRelationExpr::join_scalars(inputs, equivalences)
        };
        Self {
            input,
            group_key: Vec::new(),
            aggregates: Vec::new(),
            localize_map,
        }
    }

    fn add_group_key(&mut self, mut key: MirScalarExpr) {
        key.permute_map(&self.localize_map);
        self.group_key.push(key);
    }

    fn add_aggregate(&mut self, mut agg: AggregateExpr) {
        agg.expr.permute_map(&self.localize_map);
        self.aggregates.push(agg);
    }

    fn arity(&self) -> usize {
        self.group_key.len() + self.aggregates.len()
    }

    fn construct_reduce(
        self,
        monotonic: bool,
        expected_group_size: Option<usize>,
    ) -> MirRelationExpr {
        MirRelationExpr::Reduce {
            input: Box::new(self.input),
            group_key: self.group_key,
            aggregates: self.aggregates,
            monotonic,
            expected_group_size,
        }
    }
}
