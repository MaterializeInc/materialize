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
//! Suppose there are two inputs R and S being joined. According to
//! [Galindo-Legaria and Joshi (2001)](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf),
//! a full reduction pushdown to R can be done if and only if:
//! 1) Columns from R involved in join constraints are a subset of the group by keys.
//! 2) The key of S is a subset of the group by keys.
//! 3) The columns involved in the aggregation all belong to R.
//!
//! In our current implementation:
//! * We abide by condition 1 to the letter.
//! * We work around condition 2 by rewriting the reduce around a join of R to
//!   S with an equivalent relational expression involving a join of R to
//!   ```ignore
//!   select <columns involved in join constraints>, count(true)
//!   from S
//!   group by <columns involved in join constraints>
//!   ```
//! * TODO: We work around condition 3 in some cases by noting that `sum(R.a * S.a)`
//!   is equivalent to `sum(R.a) * sum(S.a)`.
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
use mz_expr::{AggregateExpr, JoinInputMapper, MirRelationExpr, MirScalarExpr};

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
                    upper[0].visit_mut_post(&mut |e| {
                        if let mz_expr::MirScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = lower[*c - arity].clone();
                            }
                        }
                    });
                }
                for key in group_key.iter_mut() {
                    key.visit_mut_post(&mut |e| {
                        if let mz_expr::MirScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }
                for agg in aggregates.iter_mut() {
                    agg.expr.visit_mut_post(&mut |e| {
                        if let mz_expr::MirScalarExpr::Column(c) = e {
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
                if let Some(new_relation_expr) = try_push_reduce_through_join(
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

fn try_push_reduce_through_join(
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
    // ... or, for a partial pushdown:
    // ```
    // Reduce {
    //     Join {
    //       <join input>, ..., <join input>
    //     }
    // }
    // ```
    //
    // ... where:
    //   - `<component>` is either `Join {<subset of inputs>}` or
    //      `<element of inputs>`.
    //   - `<join input>` is either `Reduce { <component> }` or
    //     `<component>`.

    let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
    let old_join_mapper = JoinInputMapper::new_from_input_types(&input_types);

    // Partition the join constraints into constraints containing a group
    // key and constraints that don't.
    let (new_join_equivalences, component_equivalences): (Vec<_>, Vec<_>) = equivalences
        .iter()
        .cloned()
        .partition(|cls| cls.iter().any(|expr| group_key.contains(expr)));

    // Find the connected components that remain after removing constraints
    // containing the group_key. Also, track the set of constraints that
    // connect the inputs in each component.
    let mut components = (0..inputs.len()).map(Component::new).collect::<Vec<_>>();
    for equivalence in component_equivalences {
        // Find the inputs referenced by the constraint.
        let inputs_to_connect = HashSet::<usize>::from_iter(
            equivalence
                .iter()
                .flat_map(|expr| old_join_mapper.lookup_inputs(expr)),
        );
        // Extract the set of components that covers the inputs.
        let (mut components_to_connect, other): (Vec<_>, Vec<_>) = components
            .into_iter()
            .partition(|c| c.inputs.iter().any(|i| inputs_to_connect.contains(i)));
        components = other;
        // Connect the components and push the result back into the list of
        // components.
        if let Some(mut connected_component) = components_to_connect.pop() {
            connected_component.connect(components_to_connect, equivalence);
            components.push(connected_component);
        }
        // Abort reduction pushdown if there are less than two connected components.
        if components.len() < 2 {
            return None;
        }
    }
    components.sort();
    // TODO: Connect components referenced by the same multi-input expression
    // contained in a constraint containing a GroupBy key.
    // For the example query below, there should be two components `{foo, bar}`
    // and `baz`.
    // ```
    // select sum(foo.b) from foo, bar, baz
    // where foo.a * bar.a = 24 group by foo.a * bar.a
    // ```

    // Perform a partial pushdown if some (but not all) of the components are
    // constant.
    let constants = components
        .iter()
        .filter(|c| is_constant_component(c, inputs))
        .count();

    if constants > 0 && constants < components.len() {
        try_partial_pushdown_to_constants(
            inputs,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
            &old_join_mapper,
            components,
            new_join_equivalences,
        )
    } else {
        try_full_pushdown(
            inputs,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
            &old_join_mapper,
            components,
            new_join_equivalences,
        )
    }
}

fn try_full_pushdown(
    inputs: &Vec<MirRelationExpr>,
    group_key: &Vec<MirScalarExpr>,
    aggregates: &Vec<AggregateExpr>,
    monotonic: bool,
    expected_group_size: Option<usize>,
    old_join_mapper: &JoinInputMapper,
    components: Vec<Component>,
    new_join_equivalences: Vec<Vec<MirScalarExpr>>,
) -> Option<MirRelationExpr> {
    // Maps (input idxs from old join) -> (idx of component it belongs to)
    let input_component_map = HashMap::from_iter(
        components
            .iter()
            .enumerate()
            .flat_map(|(c_idx, c)| c.inputs.iter().map(move |i| (*i, c_idx))),
    );

    // Construct a reduce to push to each input.
    let mut new_reduces = components
        .into_iter()
        .map(|component| JoinInputBuilder::new_reduce(component, inputs, &old_join_mapper))
        .collect::<Vec<_>>();

    // The new projection and new join equivalences will reference columns
    // produced by the new reduces, but we don't know the arities of the new
    // reduces yet. Thus, they are temporarily stored as
    // `(component_idx, column_idx_relative_to_new_reduce)`.
    let mut new_projection = Vec::with_capacity(group_key.len());
    let mut new_join_equivalences_by_component = Vec::new();

    // Calculate the group key for each new reduce. We must make sure that
    // the union of group keys across the new reduces can produce:
    // (1) the group keys of the old reduce.
    // (2) every expression in the equivalences of the new join.
    for key in group_key {
        // Find the equivalence class that the key is in.
        if let Some(cls) = new_join_equivalences
            .iter()
            .find(|cls| cls.iter().any(|expr| expr == key))
        {
            // Rewrite the join equivalence in terms of columns produced by
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
                    // Abort reduction pushdown if the expression does not
                    // refer to exactly one component.
                    return None;
                }
            }
            new_join_equivalences_by_component.push(new_join_cls);
        } else {
            // If GroupBy key does not belong in an equivalence class,
            // add the key to new projection + add it as a GroupBy key to
            // the new component
            if let Some(component) =
                lookup_corresponding_component(key, &old_join_mapper, &input_component_map)
            {
                new_projection.push((component, new_reduces[component].arity()));
                new_reduces[component].add_group_key(key.clone())
            } else {
                // Abort reduction pushdown if the expression does not
                // refer to exactly one component.
                return None;
            }
        }
    }

    // Deduce the aggregates that each reduce needs to calculate in order to
    // reconstruct each aggregate in the old reduce.
    for agg in aggregates {
        if let Some(component) =
            lookup_corresponding_component(&agg.expr, &old_join_mapper, &input_component_map)
        {
            if !agg.distinct {
                // TODO: support non-distinct aggs.
                // For more details, see https://github.com/MaterializeInc/materialize/issues/9604
                return None;
            }
            new_projection.push((component, new_reduces[component].arity()));
            new_reduces[component].add_aggregate(agg.clone());
        } else {
            // TODO: support multi- and zero- component aggs
            // For more details, see https://github.com/MaterializeInc/materialize/issues/9604
            return None;
        }
    }

    // Construct the new `MirRelationExpr`.
    let new_join_mapper =
        JoinInputMapper::new_from_input_arities(new_reduces.iter().map(|builder| builder.arity()));

    let new_inputs = new_reduces
        .into_iter()
        .map(|builder| builder.construct(monotonic, expected_group_size))
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

fn try_partial_pushdown_to_constants(
    inputs: &Vec<MirRelationExpr>,
    group_key: &Vec<MirScalarExpr>,
    aggregates: &Vec<AggregateExpr>,
    monotonic: bool,
    expected_group_size: Option<usize>,
    old_join_mapper: &JoinInputMapper,
    components: Vec<Component>,
    new_join_equivalences: Vec<Vec<MirScalarExpr>>,
) -> Option<MirRelationExpr> {
    // Maps (input idxs from old join) -> (idx of component it belongs to)
    let input_component_map = HashMap::from_iter(
        components
            .iter()
            .enumerate()
            .flat_map(|(c_idx, c)| c.inputs.iter().map(move |i| (*i, c_idx))),
    );

    // Construct inputs for the new join. Push reduces to constant inputs.
    let mut new_inputs = components
        .into_iter()
        .map(|component| {
            if is_constant_component(&component, inputs) {
                JoinInputBuilder::new_reduce(component, inputs, &old_join_mapper)
            } else {
                JoinInputBuilder::new(component, inputs, &old_join_mapper)
            }
        })
        .collect::<Vec<_>>();

    // The new group key and new join equivalences will reference columns
    // produced by the new inputs, but we don't know the arities of the new
    // reduces yet. Thus, they are temporarily stored as
    // `(component_idx, column_idx_relative_to_new_input)`.
    let mut new_group_key = Vec::with_capacity(group_key.len());
    let mut new_join_equivalences_by_component = Vec::new();

    // Calculate the group key for each new reduce.
    for key in group_key {
        // Find the equivalence class that the key is in.
        if let Some(cls) = new_join_equivalences
            .iter()
            .find(|cls| cls.iter().any(|expr| expr == key))
        {
            // Rewrite the join equivalence in terms of columns produced by
            // the new join input.
            let mut new_join_cls = Vec::new();
            for expr in cls {
                if let Some(component) =
                    lookup_corresponding_component(expr, &old_join_mapper, &input_component_map)
                {
                    let input = &mut new_inputs[component];
                    if input.is_reduce() {
                        let column = MirScalarExpr::Column(input.arity());
                        if key == expr {
                            new_group_key.push((component, column.clone()));
                        }
                        new_join_cls.push((component, column));
                        input.add_group_key(expr.clone());
                    } else {
                        let mut local_expr = expr.clone();
                        local_expr.permute_map(&input.localize_map);
                        if key == expr {
                            new_group_key.push((component, local_expr.clone()));
                        }
                        new_join_cls.push((component, local_expr));
                    }
                } else {
                    // Abort reduction pushdown if the expression does not
                    // refer to exactly one component.
                    return None;
                }
            }
            new_join_equivalences_by_component.push(new_join_cls);
        } else {
            // If GroupBy key does not belong in an equivalence class,
            // add the key to new group key + add it as a GroupBy key to
            // the new reduce.
            if let Some(component) =
                lookup_corresponding_component(key, &old_join_mapper, &input_component_map)
            {
                let input = &mut new_inputs[component];
                if input.is_reduce() {
                    new_group_key.push((component, MirScalarExpr::Column(input.arity())));
                    input.add_group_key(key.clone())
                } else {
                    let mut local_key = key.clone();
                    local_key.permute_map(&input.localize_map);
                    new_group_key.push((component, local_key));
                }
            } else {
                // Abort reduction pushdown if the expression does not
                // refer to exactly one component.
                return None;
            }
        }
    }

    // Deduce the aggregates that each reduce needs to calculate in order to
    // reconstruct each aggregate in the old reduce.
    let mut new_aggregates = Vec::new();
    for agg in aggregates {
        if let Some(component) =
            lookup_corresponding_component(&agg.expr, &old_join_mapper, &input_component_map)
        {
            if !agg.distinct {
                // TODO: support non-distinct aggs
                return None;
            }
            let input = &mut new_inputs[component];
            if input.is_reduce() {
                new_group_key.push((component, MirScalarExpr::Column(input.arity())));
                input.add_aggregate(agg.clone());
            } else {
                new_aggregates.push(agg.clone());
            }
        } else {
            // TODO: support multi- and zero- component aggs
            return None;
        }
    }

    // Construct the new `MirRelationExpr`.
    let new_join_mapper =
        JoinInputMapper::new_from_input_arities(new_inputs.iter().map(|input| input.arity()));

    let new_inputs = new_inputs
        .into_iter()
        .map(|builder| builder.construct(monotonic, expected_group_size))
        .collect::<Vec<_>>();

    let new_equivalences = new_join_equivalences_by_component
        .into_iter()
        .map(|cls| {
            cls.into_iter()
                .map(|(idx, expr)| new_join_mapper.map_expr_to_global(expr, idx))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let new_join = MirRelationExpr::join_scalars(new_inputs, new_equivalences);

    let new_group_key = new_group_key
        .into_iter()
        .map(|(idx, col)| new_join_mapper.map_expr_to_global(col, idx))
        .collect::<Vec<_>>();

    Some(MirRelationExpr::Reduce {
        input: Box::new(new_join),
        group_key: new_group_key,
        aggregates: new_aggregates,
        monotonic,
        expected_group_size,
    })
}

/// Returns `true` iff all of the component's inputs are constants.
fn is_constant_component(component: &Component, inputs: &[MirRelationExpr]) -> bool {
    component.inputs.iter().all(|i| inputs[*i].is_constant())
}

/// Returns None if `expr` does not belong to exactly one component.
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

/// A subjoin represented as a multigraph.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct Component {
    /// Index numbers of the inputs in the subjoin.
    /// Are the vertices in the multigraph.
    inputs: Vec<usize>,
    /// The edges in the multigraph.
    constraints: Vec<Vec<MirScalarExpr>>,
}

impl Component {
    /// Create a new component that contains only one input.
    fn new(i: usize) -> Self {
        Component {
            inputs: vec![i],
            constraints: Vec::new(),
        }
    }

    /// Connect `self` with `others` using the edge `connecting_constraint`.
    fn connect(&mut self, others: Vec<Component>, connecting_constraint: Vec<MirScalarExpr>) {
        self.constraints.push(connecting_constraint);
        for mut other in others {
            self.inputs.append(&mut other.inputs);
            self.constraints.append(&mut other.constraints);
        }
        self.inputs.sort();
        self.inputs.dedup();
    }
}

struct JoinInputBuilder {
    input: MirRelationExpr,
    /// Maps (global column relative to old join) -> (local column relative to `input`)
    localize_map: HashMap<usize, usize>,
    reduce: Option<ReduceArgs>,
}

#[derive(Default)]
struct ReduceArgs {
    group_key: Vec<MirScalarExpr>,
    aggregates: Vec<AggregateExpr>,
}

impl JoinInputBuilder {
    fn new(
        mut component: Component,
        inputs: &Vec<MirRelationExpr>,
        old_join_mapper: &JoinInputMapper,
    ) -> Self {
        let localize_map = component
            .inputs
            .iter()
            .flat_map(|i| old_join_mapper.global_columns(*i))
            .enumerate()
            .map(|(local, global)| (global, local))
            .collect::<HashMap<_, _>>();
        // Convert the subjoin from the `Component` representation to a
        // `MirRelationExpr` representation.
        let mut inputs = component
            .inputs
            .iter()
            .map(|i| inputs[*i].clone())
            .collect::<Vec<_>>();
        // Constraints need to be localized to the subjoin.
        for constraint in component.constraints.iter_mut() {
            for expr in constraint.iter_mut() {
                expr.permute_map(&localize_map)
            }
        }
        let input = if inputs.len() == 1 {
            inputs.pop().unwrap()
        } else {
            MirRelationExpr::join_scalars(inputs, component.constraints)
        };
        Self {
            input,
            localize_map,
            reduce: None,
        }
    }

    fn new_reduce(
        component: Component,
        inputs: &Vec<MirRelationExpr>,
        old_join_mapper: &JoinInputMapper,
    ) -> Self {
        let mut builder = Self::new(component, inputs, old_join_mapper);
        builder.reduce = Some(Default::default());
        builder
    }

    fn is_reduce(&self) -> bool {
        self.reduce.is_some()
    }

    fn add_group_key(&mut self, mut key: MirScalarExpr) {
        let reduce = self.reduce.as_mut().expect("not a reduce");
        key.permute_map(&self.localize_map);
        reduce.group_key.push(key);
    }

    fn add_aggregate(&mut self, mut agg: AggregateExpr) {
        let reduce = self.reduce.as_mut().expect("not a reduce");
        agg.expr.permute_map(&self.localize_map);
        reduce.aggregates.push(agg);
    }

    fn arity(&self) -> usize {
        if let Some(reduce) = &self.reduce {
            reduce.group_key.len() + reduce.aggregates.len()
        } else {
            self.input.arity()
        }
    }

    fn construct(self, monotonic: bool, expected_group_size: Option<usize>) -> MirRelationExpr {
        if let Some(reduce) = self.reduce {
            MirRelationExpr::Reduce {
                input: Box::new(self.input),
                group_key: reduce.group_key,
                aggregates: reduce.aggregates,
                monotonic,
                expected_group_size,
            }
        } else {
            self.input
        }
    }
}
