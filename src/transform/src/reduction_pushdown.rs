// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Preliminary work on reduction push-down.
//!
//! At the moment, this only
//! * absorbs Map operators into Reduce operators.
//! * partially pushes Reduce operators into joins.

use std::collections::{HashMap, HashSet};

use crate::TransformArgs;
use expr::{AggregateExpr, AggregateFunc, JoinInputMapper, RelationExpr, ScalarExpr};

/// Pushes Reduce operators toward sources.
#[derive(Debug)]
pub struct ReductionPushdown;

impl crate::Transform for ReductionPushdown {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl ReductionPushdown {
    /// Pushes Reduce operators toward sources.
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } = relation
        {
            // Map expressions can be absorbed into the Reduce at no cost.
            if let RelationExpr::Map {
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
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = lower[*c - arity].clone();
                            }
                        }
                    });
                }
                for key in group_key.iter_mut() {
                    key.visit_mut(&mut |e| {
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }
                for agg in aggregates.iter_mut() {
                    agg.expr.visit_mut(&mut |e| {
                        if let crate::ScalarExpr::Column(c) = e {
                            if *c >= arity {
                                *e = scalars[*c - arity].clone();
                            }
                        }
                    });
                }

                **input = inner.take_dangerous()
            } else if let RelationExpr::Join {
                inputs,
                equivalences,
                demand: _,
                implementation,
            } = &mut **input
            {
                if let Some(red_pusher) = ReductionPusher::try_new(
                    group_key,
                    aggregates,
                    inputs,
                    equivalences,
                    implementation,
                ) {
                    if let Some(new_relation_expr) = red_pusher
                        .get_relation_with_reduction_pushed(monotonic, expected_group_size)
                    {
                        *relation = new_relation_expr;
                    }
                }
            }
        }
    }
}

/// Convenience struct to track the many variables involved in the reduction pushdown.
struct ReductionPusher<'a> {
    // Components of the new relation. Once the reduction pushdown is complete,
    // the relation would assemble roughly like this:
    // RelationExpr::Project {
    //   input: RelationExpr::Map {
    //     input: RelationExpr::Reduce {
    //       input: RelationExpr::Join {
    //         inputs: new_inputs,
    //         ..
    //         },
    //       },
    //       group_key: outer_group_key,
    //       aggregates: outer_aggs,
    //     }
    //     scalars: downcast_partial_reduce,
    //   }
    //   outputs: <derived from the map inside>
    // }
    // except that column references in `outer_group_key` and `outer_aggs` are
    // always in terms of the join of the old relation, and would need to be
    // updated when assembling the new relation. In the case where
    // an outer aggregation refers to a partial aggregate that only exist in the join of
    // the new relation, the outer aggregation will instead refer to
    // `ScalarExpr::Column(old_join_arity + agg_num)`
    /// The inputs in the join in the new relation
    new_inputs: Vec<RelationExpr>,
    /// The group key for the outer reduce. Identical to the group key of the
    /// original reduce.
    outer_group_key: &'a [ScalarExpr],
    /// The aggregations for the outer reduce. Contains (position of
    /// aggregation in the `aggregates` vector of the old reduce,
    /// the outer aggregation).
    outer_aggs: Vec<(usize, AggregateExpr)>,
    /// Reductions currently implicitly upcast the type of the aggregated
    /// column. A partial reduction would cause the type to be upcast twice.
    /// To maintain correctness, it is necessary to downcast after the outer
    /// reduce.
    downcast_partial_reduce: Vec<ScalarExpr>,

    // Fields used to track the mapping of columns and expressions in the old
    // relation to their location in the new relation.
    /// A running column count of the new join. Assumes that the new join is
    /// created by trying to do a pushdown on input 0, followed by input 1,
    /// followed by input 2, and so on
    next_new_join_column: usize,
    /// An array to look up the column in the new join corresponding to
    /// * a column from the old join. The lookup key is the column number
    ///   in the old join
    /// * a partial aggregation or a pushed down aggregation. The lookup
    ///   key is the `old_total_columns` + `agg_num`
    ///   The array gives correct answers as long as it is never used to
    ///   query for a column that is not present in the new join
    permutation: HashMap<usize, usize>,
    /// Reduction pushdown may cause expression join keys or expression group
    /// keys to become evaluated already in an input of the new join.
    /// Maps expressions from the old join to where they are evaluated in the
    /// new join.
    evaluated_expressions: HashMap<ScalarExpr, ScalarExpr>,

    // Aggregations to try to push down
    /// For each input, the aggregations that involve just that input.
    /// Contains (position of aggregation, the aggregation)
    single_input_aggs_to_push: Vec<Vec<(usize, AggregateExpr)>>,
    /// Aggregations involving no inputs. They can be partially pushed down to
    /// any input as long as the join key is not a primary key
    /// Contains (position of aggregation, the aggregation)
    no_input_aggs_to_push: Vec<(usize, AggregateExpr)>,

    // Fields used to determine whether aggregations should not be pushed down
    // into a particular input
    /// Whether the join implementation is DeltaQuery. Avoid pushdowns that
    /// would invalidate the DeltaQuery plan.
    is_delta: bool,
    /// The unique keys of each of the old inputs. Avoid pushdowns where there
    /// exists a unique key that is a subset of the group keys
    old_input_keys: Vec<Vec<Vec<usize>>>,

    // Other helper fields
    /// A reference to the equivalences of the old join
    equivalences: &'a [Vec<ScalarExpr>],
    /// The support of aggregations that we do not try to push down, sorted by
    /// input. Inner reduces must not cause these columns to be removed from
    /// the join.
    aggs_not_pushed_support: Vec<HashSet<usize>>,
    /// The mapping of global to local columns in the old join
    old_join_mapper: JoinInputMapper,
    /// For each input, stores the inputs it is bound to and the (global) join
    /// keys that bind it to the particular input
    join_graph: Vec<HashMap<usize, Vec<ScalarExpr>>>,
    /// whether aggregations were pushed down to any input
    pushdown_succeeded: bool,
}

impl<'a> ReductionPusher<'a> {
    /// Creates a struct for pushing down reductions. Returns `None` if the
    /// relation is ineligible for reduction pushdown.
    fn try_new(
        group_key: &'a [ScalarExpr],
        aggregates: &[AggregateExpr],
        inputs: &[RelationExpr],
        equivalences: &'a [Vec<ScalarExpr>],
        implementation: &expr::JoinImplementation,
    ) -> Option<Self> {
        let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
        let old_join_mapper = JoinInputMapper::new_from_input_types(&input_types);

        // sort aggregations by how and whether they can be pushed down
        // TODO: attempt to rewrite multi-input aggregations in terms of a
        // single input
        // TODO: attempt to reduce the number of inputs that aggregations come
        // from by rewriting aggregations on join keys in terms of the
        // equivalent expression from the other input
        let mut outer_aggs = Vec::new();
        let mut single_input_aggs_to_push = vec![Vec::new(); inputs.len()];
        let mut no_input_aggs_to_push = Vec::new();

        for (agg_num, agg) in aggregates.iter().enumerate() {
            let mut expr_inputs = old_join_mapper.lookup_inputs(&agg.expr);
            if let AggregateFunc::JsonbAgg = agg.func {
                // JsonbAgg cannot be partially pushed down, so it goes in the
                // outer reduce
                outer_aggs.push((agg_num, agg.clone()));
            } else if let AggregateFunc::Dummy = agg.func {
                // do not bother pushing dummy functions down
                outer_aggs.push((agg_num, agg.clone()));
            } else if agg.distinct && !agg.func.hierarchical_when_distinct() {
                outer_aggs.push((agg_num, agg.clone()));
            } else if let Some(expr_input) = expr_inputs.next() {
                if expr_inputs.next().is_some() {
                    // an aggregation involving more than one input
                    outer_aggs.push((agg_num, agg.clone()));
                } else {
                    // an aggregation involving one input.
                    let mut localized_agg = agg.clone();
                    localized_agg.expr = old_join_mapper.map_expr_to_local(localized_agg.expr);
                    if let RelationExpr::Reduce { group_key, .. } = &inputs[expr_input] {
                        if localized_agg
                            .expr
                            .support()
                            .iter()
                            .any(|i| i >= &group_key.len())
                        {
                            // skip pushing down the aggregation if it aggregates a
                            // partial aggregate to prevent infinite layers of
                            // partial reduction pushdown
                            outer_aggs.push((agg_num, agg.clone()));
                            continue;
                        }
                    }
                    single_input_aggs_to_push[expr_input].push((agg_num, localized_agg));
                }
            } else {
                // an aggregation involving no inputs
                no_input_aggs_to_push.push((agg_num, agg.clone()));
            }
        }

        // skip reduction pushdown if there are no eligible aggregations to push
        // down
        if single_input_aggs_to_push.iter().all(|aggs| aggs.is_empty())
            && no_input_aggs_to_push.is_empty()
        {
            return None;
        }

        let join_graph = create_join_graph(&old_join_mapper, equivalences)?;

        // Union together the supports of all outer aggs, which will become
        // `aggs_not_pushed_support`.
        let mut outer_agg_support_iter = outer_aggs.iter().map(|(_, agg)| agg.expr.support());
        let outer_agg_support = outer_agg_support_iter
            .next()
            .map(|set| outer_agg_support_iter.fold(set, |set1, set2| &set1 | &set2))
            .unwrap_or_default();

        Some(Self {
            new_inputs: inputs.to_vec(),
            outer_group_key: group_key,
            outer_aggs,
            downcast_partial_reduce: Vec::new(),
            next_new_join_column: 0,
            permutation: HashMap::new(),
            evaluated_expressions: HashMap::new(),
            single_input_aggs_to_push,
            no_input_aggs_to_push,
            is_delta: matches!(implementation, expr::JoinImplementation::DeltaQuery(_)),
            old_input_keys: input_types
                .iter()
                .map(|typ| typ.keys.clone())
                .collect::<Vec<_>>(),
            equivalences,
            aggs_not_pushed_support: old_join_mapper.split_column_set_by_input(&outer_agg_support),
            old_join_mapper,
            join_graph,
            pushdown_succeeded: false,
        })
    }

    /// Construct the reduction pushdown, if able
    fn get_relation_with_reduction_pushed(
        mut self,
        monotonic: &bool,
        expected_group_size: &Option<usize>,
    ) -> Option<RelationExpr> {
        // Attempt reduction pushdown.
        // Try two ways to construct a new join where at least one of the
        // inputs gains a new inner reduce:
        // 1) if there are aggregations on a single input, try to push them
        // down. No-input aggregations will be pushed down to
        // the first input where we can successfully push down single-input
        // aggregations.
        if self
            .single_input_aggs_to_push
            .iter()
            .any(|aggs| !aggs.is_empty())
        {
            for idx in 0..self.new_inputs.len() {
                self.try_pushdown(idx, true);
            }
        }
        // 2) If no single-input aggregations get pushed down, then try pushing
        // down no-input aggregations to any input
        if !self.pushdown_succeeded && !self.no_input_aggs_to_push.is_empty() {
            // reset the mapping of the old relation to the new relation.
            // this is ok because none of the join inputs should have changed,
            // so the mapping should be the identity mapping.
            self.permutation = HashMap::new();
            self.next_new_join_column = 0;
            self.evaluated_expressions = HashMap::new();

            for idx in 0..self.new_inputs.len() {
                self.try_pushdown(idx, false);
            }
        }

        if self.pushdown_succeeded {
            // construct the new join
            let mut new_inputs = Vec::new();
            std::mem::swap(&mut new_inputs, &mut self.new_inputs);
            let new_equivalences = self
                .equivalences
                .iter()
                .map(|equivalence| {
                    equivalence
                        .iter()
                        .map(|expr| {
                            if let Some(new_expr) = self.evaluated_expressions.get(expr) {
                                new_expr.clone()
                            } else {
                                let mut new_expr = expr.clone();
                                new_expr.permute_map(&self.permutation);
                                new_expr
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            // construct the new outer reduce
            let mut outer_group_key = self.outer_group_key.to_vec();
            for gk in outer_group_key.iter_mut() {
                if let Some(new_expr) = self.evaluated_expressions.get(&gk) {
                    *gk = new_expr.clone()
                } else {
                    gk.permute_map(&self.permutation);
                }
            }

            self.outer_aggs.sort_by(|a, b| a.0.cmp(&b.0));
            let outer_aggs = self
                .outer_aggs
                .iter()
                .map(|(_, agg)| {
                    let mut agg = agg.clone();
                    agg.expr.permute_map(&self.permutation);
                    agg
                })
                .collect::<Vec<_>>();

            let total_column_after_outer_reduce = outer_group_key.len() + outer_aggs.len();

            let mut new_relation_expr = RelationExpr::Reduce {
                input: Box::new(RelationExpr::join_scalars(new_inputs, new_equivalences)),
                group_key: outer_group_key,
                aggregates: outer_aggs,
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            };

            // If any column's type needs to be downcast,
            // Add a map + project around the outer reduce
            if !self.downcast_partial_reduce.is_empty() {
                let mut project_downcast =
                    (0..(total_column_after_outer_reduce)).collect::<Vec<_>>();
                for (map_num, downcast) in self.downcast_partial_reduce.iter().enumerate() {
                    let column = downcast.support();
                    assert_eq!(column.len(), 1);
                    if let Some(column) = column.iter().next() {
                        project_downcast[*column] = total_column_after_outer_reduce + map_num;
                    }
                }
                new_relation_expr = RelationExpr::Project {
                    input: Box::new(RelationExpr::Map {
                        input: Box::new(new_relation_expr),
                        scalars: self.downcast_partial_reduce,
                    }),
                    outputs: project_downcast,
                }
            }
            return Some(new_relation_expr);
        }
        None
    }

    /// Add input `idx` to the new join, with an inner reduce around it if
    /// possible. Assumes that inputs `0..idx` have all been added to the
    /// new join already.
    fn try_pushdown(&mut self, idx: usize, single_input_aggs_exist: bool) {
        if (self.is_delta && self.join_graph[idx].len() > 1)
            || (single_input_aggs_exist && self.single_input_aggs_to_push[idx].is_empty())
        {
            self.skip_pushdown_on_input(idx);
            return;
        }

        // extract the columns from the group key of the outer reduce that will
        // go inside the group key for the inner reduce
        // TODO: try to rewrite expressions that span both input `idx` and
        // another input in terms of input `idx`?
        let mut inner_group_keys = self
            .outer_group_key
            .iter()
            .flat_map(|gk| {
                let support = gk
                    .support()
                    .iter()
                    .map(|c| self.old_join_mapper.map_column_to_local(*c))
                    .collect::<Vec<_>>();
                if !support.is_empty() && support.iter().all(|(_, src)| *src == idx) {
                    vec![gk.clone()]
                } else {
                    support
                        .iter()
                        .filter_map(|(local_i, source_idx)| {
                            if *source_idx == idx {
                                Some(ScalarExpr::Column(*local_i))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                }
            })
            .collect::<Vec<_>>();
        // add the join keys to input group keys if not done so already
        for (_, join_keys) in self.join_graph[idx].iter() {
            for join_key in join_keys {
                let localized_join_key = self.old_join_mapper.map_expr_to_local(join_key.clone());
                if inner_group_keys
                    .iter()
                    .find(|k| k == &&localized_join_key)
                    .is_none()
                {
                    inner_group_keys.push(localized_join_key);
                }
            }
        }
        // add the support of aggregations we are not trying to push down to the
        // group keys of the inner reduce
        for col in self.aggs_not_pushed_support[idx].iter() {
            if inner_group_keys
                .iter()
                .find(|k| k == &&ScalarExpr::Column(*col))
                .is_none()
            {
                inner_group_keys.push(ScalarExpr::Column(*col));
            }
        }
        // check if a key of the input is a subset of the group keys for the
        // inner reduce. If yes, abort pushdown
        for key_set in self.old_input_keys[idx].iter() {
            if key_set.iter().all(|k| {
                inner_group_keys
                    .iter()
                    .any(|igk| igk == &ScalarExpr::Column(*k))
            }) {
                self.skip_pushdown_on_input(idx);
                return;
            }
        }
        // do partial pushdown
        self.partial_pushdown(idx, inner_group_keys);
    }

    /// Do a partial reduction pushdown on input `idx` with the inner reduce
    /// having group key `inner_group_keys` and aggs as whatever is left in
    /// `self.no_input_aggs_to_push` and `self.single_input_aggs_to_push[idx]`
    /// Assumes that inputs `0..idx` have all been added to the new join
    /// already.
    fn skip_pushdown_on_input(&mut self, idx: usize) {
        // add single input aggs to outer reduce
        for (agg_num, mut agg) in self.single_input_aggs_to_push[idx].drain(0..) {
            agg.expr = self.old_join_mapper.map_expr_to_global(agg.expr, idx);
            self.outer_aggs.push((agg_num, agg));
        }
        // adjust the permutation
        for column in self.old_join_mapper.global_columns(idx) {
            self.permutation.insert(column, self.next_new_join_column);
            self.next_new_join_column += 1;
        }
    }

    /// Do a partial reduction pushdown on input `idx` with the inner reduce
    /// having group key `inner_group_keys` and aggs as whatever is left in
    /// `self.no_input_aggs_to_push` and `self.single_input_aggs_to_push[idx]`
    /// Assumes that inputs `0..idx` have all been added to the new join
    /// already.
    fn partial_pushdown(&mut self, idx: usize, inner_group_keys: Vec<ScalarExpr>) {
        // calculate inner_aggs
        let mut inner_aggs = Vec::new();
        inner_aggs.extend(self.single_input_aggs_to_push[idx].drain(0..));
        inner_aggs.extend(self.no_input_aggs_to_push.drain(0..));
        while let RelationExpr::ArrangeBy { input: inner, .. } = &mut self.new_inputs[idx] {
            self.new_inputs[idx] = inner.take_dangerous();
        }
        // Open question: is it useful to propagate the expected group size from
        // the outer reduce?
        // Is it worthwhile to propagate monotonicity from the outer reduce?
        // If an outer reduce is not monotonic, an inner reduce can be monotonic
        // or not monotonic, so there should be a separate pass to check
        // monotonicity or inner reduces anyway.
        self.new_inputs[idx] = RelationExpr::Reduce {
            input: Box::new(self.new_inputs[idx].clone()),
            group_key: inner_group_keys.clone(),
            aggregates: inner_aggs.iter().map(|(_, agg)| agg.clone()).collect(),
            monotonic: false,
            expected_group_size: None,
        };
        // calculate new permutation. Register expressions evaluated in the
        // inner reduce
        for group_key in inner_group_keys {
            if let ScalarExpr::Column(c1) = group_key {
                self.permutation.insert(
                    self.old_join_mapper.map_column_to_global(c1, idx),
                    self.next_new_join_column,
                );
            } else {
                self.evaluated_expressions.insert(
                    self.old_join_mapper.map_expr_to_global(group_key, idx),
                    ScalarExpr::Column(self.next_new_join_column),
                );
            }
            self.next_new_join_column += 1;
        }
        for (agg_num, agg) in inner_aggs {
            let agg_lookup_key = self.old_join_mapper.total_columns() + agg_num;
            let (outer_agg_func, downcast) = agg.func.outer_agg();
            // calculate aggs to add to outer reduce
            let outer_agg = AggregateExpr {
                func: outer_agg_func,
                expr: ScalarExpr::Column(agg_lookup_key),
                distinct: agg.distinct,
            };
            if let Some(downcast) = downcast {
                self.downcast_partial_reduce.push(ScalarExpr::CallUnary {
                    func: downcast,
                    expr: Box::new(ScalarExpr::Column(self.outer_group_key.len() + agg_num)),
                });
            }
            self.outer_aggs.push((agg_num, outer_agg));
            self.permutation
                .insert(agg_lookup_key, self.next_new_join_column);
            self.next_new_join_column += 1;
        }
        self.pushdown_succeeded = true;
    }
}

/// Construct a graph showing, for each input, which other inputs it is joined
/// to and on which join keys.
/// Example:
/// For the query
/// ```
/// select *
/// from foo, bar, baz
/// where foo.a = bar.b
/// and foo.c = bar.d
/// and bar.e = baz.f
/// ```
/// The corresponding graph will be
/// "foo" -> ["bar" -> ["a", "c"]]
/// "bar" -> ["foo" -> ["b", "d"], "baz" -> ["e"]]
/// "baz" -> ["baz" -> ["f"]]
fn create_join_graph(
    old_join_mapper: &JoinInputMapper,
    equivalences: &[Vec<ScalarExpr>],
) -> Option<Vec<HashMap<usize, Vec<ScalarExpr>>>> {
    let mut join_keys_by_input = vec![HashMap::new(); old_join_mapper.total_inputs()];
    for equivalence in equivalences.iter() {
        // track all inputs referenced in the equivalence class
        let mut inputs_in_equivalence = Vec::new();
        // localize each expression in equivalence to its respective input
        let mut expr_with_inputs = Vec::new();
        for expr in equivalence.iter() {
            let mut expr_inputs = old_join_mapper.lookup_inputs(expr);
            if let Some(expr_input) = expr_inputs.next() {
                if expr_inputs.next().is_none() {
                    expr_with_inputs.push((expr_input, expr.clone()));
                    inputs_in_equivalence.push(expr_input);
                } else {
                    // there is an expression in a join equivalence class that
                    // spans multiple inputs. For now, skip reduction pushdown
                    // in this case.
                    return None;
                }
            }
        }
        if inputs_in_equivalence.len() > 1 {
            // for each input referenced in an equivalence class,
            // jot down the other inputs in the same equivalence class.
            for (expr_input, expr) in expr_with_inputs {
                for other_input in inputs_in_equivalence.iter() {
                    if *other_input != expr_input {
                        join_keys_by_input[expr_input]
                            .entry(*other_input)
                            .or_insert(Vec::new())
                            .push(expr.clone());
                    }
                }
            }
        }
    }
    Some(join_keys_by_input)
}
