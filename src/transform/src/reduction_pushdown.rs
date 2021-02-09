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

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::TransformArgs;
use expr::{AggregateExpr, AggregateFunc, JoinInputMapper, MirRelationExpr, MirScalarExpr};

/// Pushes Reduce operators toward sources.
#[derive(Debug)]
pub struct ReductionPushdown;

impl crate::Transform for ReductionPushdown {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
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
    pub fn action(&self, relation: &mut MirRelationExpr) {
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

                **input = inner.take_dangerous();

                // visit relation again to see if the reduction can be pushed
                // down into whatever the map was around
                self.action(relation)
            } else if let MirRelationExpr::Join {
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
                    &[],
                ) {
                    if let Some(new_relation_expr) = red_pusher
                        .get_relation_with_reduction_pushed(monotonic, expected_group_size)
                    {
                        *relation = new_relation_expr;
                    }
                }
            } else if let MirRelationExpr::Filter { input, predicates } = &mut **input {
                if let MirRelationExpr::Join {
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
                        predicates,
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
}

/// Convenience struct to track the many variables involved in the reduction pushdown.
struct ReductionPusher<'a> {
    // Components of the new relation. Once the reduction pushdown is complete,
    // the relation would assemble roughly like this:
    // MirRelationExpr::Project {
    //   input: MirRelationExpr::Map {
    //     input: MirRelationExpr::Reduce {
    //       input: MirRelationExpr::Join {
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
    // `MirScalarExpr::Column(old_join_arity + agg_num)`
    /// The inputs in the join in the new relation
    new_inputs: Vec<MirRelationExpr>,
    /// The group key for the outer reduce. Identical to the group key of the
    /// original reduce.
    outer_group_key: &'a [MirScalarExpr],
    /// The aggregations for the outer reduce. Contains (position of
    /// aggregation in the `aggregates` vector of the old reduce,
    /// the outer aggregation).
    outer_aggs: Vec<(usize, AggregateExpr)>,
    /// Reductions currently implicitly upcast the type of the aggregated
    /// column. A partial reduction would cause the type to be upcast twice.
    /// To maintain correctness, it is necessary to downcast after the outer
    /// reduce.
    downcast_partial_reduce: Vec<MirScalarExpr>,

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
    evaluated_expressions: HashMap<MirScalarExpr, MirScalarExpr>,

    // Aggregations to try to push down
    /// For each input, the aggregations and filters that involve just that input.
    /// Contains ((position of aggregation, the aggregation), filters)
    single_input_aggs_to_push: Vec<(Vec<(usize, AggregateExpr)>, Vec<MirScalarExpr>)>,
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
    /// The equivalences of the old join, with all single-input filters removed
    equivalences: Vec<Vec<MirScalarExpr>>,
    /// The support of aggregations that we do not try to push down, sorted by
    /// input. Inner reduces must not cause these columns to be removed from
    /// the join.
    aggs_not_pushed_support: Vec<HashSet<usize>>,
    /// The mapping of global to local columns in the old join
    old_join_mapper: JoinInputMapper,
    /// For each input, stores the inputs it is bound to and the (global) join
    /// keys that bind it to the particular input. This is a `BTreeMap` so that
    /// the resulting plan is deterministic.
    join_graph: Vec<BTreeMap<usize, Vec<MirScalarExpr>>>,
    /// whether aggregations were pushed down to any input
    pushdown_succeeded: bool,
}

impl<'a> ReductionPusher<'a> {
    /// Creates a struct for pushing down reductions. Returns `None` if the
    /// relation is ineligible for reduction pushdown.
    fn try_new(
        group_key: &'a [MirScalarExpr],
        aggregates: &[AggregateExpr],
        inputs: &[MirRelationExpr],
        equivalences: &'a [Vec<MirScalarExpr>],
        implementation: &expr::JoinImplementation,
        predicates: &[MirScalarExpr],
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
        let mut single_input_aggs_to_push = vec![(Vec::new(), Vec::new()); inputs.len()];
        let mut no_input_aggs_to_push = Vec::new();

        // classify the aggregations by how many inputs each aggregation references
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
                    if let MirRelationExpr::Reduce { group_key, .. } = &inputs[expr_input] {
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
                    single_input_aggs_to_push[expr_input]
                        .0
                        .push((agg_num, localized_agg));
                }
            } else {
                // an aggregation involving no inputs
                if agg.func.row_count_dependent() {
                    no_input_aggs_to_push.push((agg_num, agg.clone()));
                } else {
                    // it is not worthwhile to push down a reduce on a constant
                    // whose result does not change based on the input to the
                    // reduce.
                    // TODO: an aggregation on a literal whose result does not
                    // change based on the input of the reduce should be
                    // converted into a map
                    outer_aggs.push((agg_num, agg.clone()));
                }
            }
        }

        // Classify predicates by how many inputs the predicate references
        for predicate in predicates {
            let mut expr_inputs = old_join_mapper.lookup_inputs(predicate);
            if let Some(expr_input) = expr_inputs.next() {
                if expr_inputs.next().is_some() {
                    // TODO: support multi-input predicates
                    // TODO: attempt to rewrite multi-input predicates in terms of a
                    // single input
                    return None;
                } else {
                    single_input_aggs_to_push[expr_input]
                        .1
                        .push(old_join_mapper.map_expr_to_local(predicate.clone()))
                }
            }
        }

        // Union together the supports of all outer aggs, which will become
        // `aggs_not_pushed_support`.
        let mut outer_agg_support_iter = outer_aggs.iter().map(|(_, agg)| agg.expr.support());
        let outer_agg_support = outer_agg_support_iter
            .next()
            .map(|set| outer_agg_support_iter.fold(set, |set1, set2| &set1 | &set2))
            .unwrap_or_default();
        let aggs_not_pushed_support = old_join_mapper.split_column_set_by_input(&outer_agg_support);

        // skip reduction pushdown if there are no eligible aggregations to push
        // down
        if single_input_aggs_to_push
            .iter()
            .all(|(aggs, _)| aggs.is_empty())
            && no_input_aggs_to_push.is_empty()
        {
            return None;
        }

        // Create join graph. While creating the join graph, extract
        // single-input filters from equivalences and move them to
        // `single_input_aggs_to_push`.
        let mut equivalences = equivalences.to_vec();

        let (join_graph, single_input_filters) =
            create_join_graph(&old_join_mapper, &mut equivalences);
        let join_graph = join_graph?;

        for (filter, idx) in single_input_filters {
            single_input_aggs_to_push[idx]
                .1
                .push(old_join_mapper.map_expr_to_local(filter));
        }

        // If the reduction pushdown is a success, we will want to remove all
        // `ArrangeBys` from the original inputs so that join can plan anew.
        let mut new_inputs = inputs.to_vec();
        for new_input in new_inputs.iter_mut() {
            if let MirRelationExpr::ArrangeBy { input: inner, .. } = new_input {
                *new_input = inner.take_dangerous();
            }
        }

        Some(Self {
            new_inputs,
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
            aggs_not_pushed_support,
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
    ) -> Option<MirRelationExpr> {
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
            .any(|(aggs, _)| !aggs.is_empty())
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
            let mut new_equivalences = Vec::new();
            std::mem::swap(&mut new_equivalences, &mut self.equivalences);
            for equivalence in new_equivalences.iter_mut() {
                for expr in equivalence.iter_mut() {
                    if let Some(new_expr) = self.evaluated_expressions.get(expr) {
                        *expr = new_expr.clone();
                    } else {
                        expr.permute_map(&self.permutation);
                    }
                }
            }

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

            let mut new_relation_expr = MirRelationExpr::join_scalars(new_inputs, new_equivalences);

            let mut outer_predicates = Vec::new();
            for (idx, (_, predicates)) in self.single_input_aggs_to_push.iter_mut().enumerate() {
                for predicate in predicates.drain(0..) {
                    let mut global_predicate =
                        self.old_join_mapper.map_expr_to_global(predicate, idx);
                    global_predicate.permute_map(&self.permutation);
                    outer_predicates.push(global_predicate);
                }
            }
            if !outer_predicates.is_empty() {
                new_relation_expr = MirRelationExpr::Filter {
                    input: Box::new(new_relation_expr),
                    predicates: outer_predicates,
                }
            }

            new_relation_expr = MirRelationExpr::Reduce {
                input: Box::new(new_relation_expr),
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
                new_relation_expr = MirRelationExpr::Project {
                    input: Box::new(MirRelationExpr::Map {
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
            || (single_input_aggs_exist && self.single_input_aggs_to_push[idx].0.is_empty())
            || (!single_input_aggs_exist && self.no_input_aggs_to_push.is_empty())
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
                    vec![self.old_join_mapper.map_expr_to_local(gk.clone())]
                } else {
                    support
                        .iter()
                        .filter_map(|(local_i, source_idx)| {
                            if *source_idx == idx {
                                Some(MirScalarExpr::Column(*local_i))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                }
            })
            .collect::<Vec<_>>();

        // add the support of aggregations we are not trying to push down to the
        // group keys of the inner reduce
        for col in self.aggs_not_pushed_support[idx].iter() {
            if inner_group_keys
                .iter()
                .find(|k| k == &&MirScalarExpr::Column(*col))
                .is_none()
            {
                inner_group_keys.push(MirScalarExpr::Column(*col));
            }
        }

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
            if self.is_delta && inner_group_keys.len() > join_keys.len() {
                // If we are checking if we can do a reduction pushdown on an
                // input when the join is a delta query, the outer loop only
                // runs once. Also, if we reduce on anything on other then the
                // join keys, then the delta query will be invalidated. Abort
                // pushdown to avoid invalidating the delta query.
                self.skip_pushdown_on_input(idx);
                return;
            }
        }

        // check if a key of the input is a subset of the group keys for the
        // inner reduce. If yes, abort pushdown
        for key_set in self.old_input_keys[idx].iter() {
            if key_set.iter().all(|k| {
                inner_group_keys
                    .iter()
                    .any(|igk| igk == &MirScalarExpr::Column(*k))
            }) {
                self.skip_pushdown_on_input(idx);
                return;
            }
        }
        // do partial pushdown
        self.partial_pushdown(idx, inner_group_keys);
    }

    /// Add input `idx` to the new join without pushing any aggregations down.
    /// Assumes that inputs `0..idx` have all been added to the new join
    /// already.
    fn skip_pushdown_on_input(&mut self, idx: usize) {
        // add single input aggs to outer reduce
        for (agg_num, mut agg) in self.single_input_aggs_to_push[idx].0.drain(0..) {
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
    /// `self.no_input_aggs_to_push` and `self.single_input_aggs_to_push[idx].0`
    /// Assumes that inputs `0..idx` have all been added to the new join
    /// already.
    fn partial_pushdown(&mut self, idx: usize, inner_group_keys: Vec<MirScalarExpr>) {
        // calculate inner_aggs
        let mut inner_aggs = Vec::new();
        inner_aggs.extend(self.single_input_aggs_to_push[idx].0.drain(0..));
        inner_aggs.extend(self.no_input_aggs_to_push.drain(0..));
        assert_eq!(inner_aggs.is_empty(), false);
        while let MirRelationExpr::ArrangeBy { input: inner, .. } = &mut self.new_inputs[idx] {
            self.new_inputs[idx] = inner.take_dangerous();
        }

        let mut inner_predicates = Vec::new();
        std::mem::swap(
            &mut inner_predicates,
            &mut self.single_input_aggs_to_push[idx].1,
        );

        let new_input = if inner_predicates.is_empty() {
            self.new_inputs[idx].clone()
        } else {
            self.new_inputs[idx].clone().filter(inner_predicates)
        };
        // Open question: is it useful to propagate the expected group size from
        // the outer reduce?
        // Is it worthwhile to propagate monotonicity from the outer reduce?
        // If an outer reduce is not monotonic, an inner reduce can be monotonic
        // or not monotonic, so there should be a separate pass to check
        // monotonicity or inner reduces anyway.
        self.new_inputs[idx] = MirRelationExpr::Reduce {
            input: Box::new(new_input),
            group_key: inner_group_keys.clone(),
            aggregates: inner_aggs.iter().map(|(_, agg)| agg.clone()).collect(),
            monotonic: false,
            expected_group_size: None,
        };
        // calculate new permutation. Register expressions evaluated in the
        // inner reduce
        for group_key in inner_group_keys {
            if let MirScalarExpr::Column(c1) = group_key {
                self.permutation.insert(
                    self.old_join_mapper.map_column_to_global(c1, idx),
                    self.next_new_join_column,
                );
            } else {
                self.evaluated_expressions.insert(
                    self.old_join_mapper.map_expr_to_global(group_key, idx),
                    MirScalarExpr::Column(self.next_new_join_column),
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
                expr: MirScalarExpr::Column(agg_lookup_key),
                distinct: agg.distinct,
            };
            if let Some(downcast) = downcast {
                self.downcast_partial_reduce.push(MirScalarExpr::CallUnary {
                    func: downcast,
                    expr: Box::new(MirScalarExpr::Column(self.outer_group_key.len() + agg_num)),
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

/// 1) Construct a graph showing, for each input, which other inputs it is joined
/// to and on which join keys.
/// Example:
/// For the query
/// """
/// select *
/// from foo, bar, baz
/// where foo.a = bar.b
/// and foo.c = bar.d
/// and bar.e = baz.f
/// """
/// The corresponding graph will be
/// "foo" -> ["bar" -> ["a", "c"]]
/// "bar" -> ["foo" -> ["b", "d"], "baz" -> ["e"]]
/// "baz" -> ["bar" -> ["f"]]
/// 2) extracts single-input equivalence classes from `equivalences` and returns
///    them as equality filter predicates
fn create_join_graph(
    old_join_mapper: &JoinInputMapper,
    equivalences: &mut Vec<Vec<MirScalarExpr>>,
) -> (
    Option<Vec<BTreeMap<usize, Vec<MirScalarExpr>>>>,
    Vec<(MirScalarExpr, usize)>,
) {
    let mut join_keys_by_input = vec![BTreeMap::new(); old_join_mapper.total_inputs()];
    let mut single_input_filters = Vec::new();
    let mut equivalences_to_remove = Vec::new();
    for (idx, equivalence) in equivalences.iter().enumerate() {
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
                    return (None, single_input_filters);
                }
            }
        }
        match inputs_in_equivalence.len() {
            0 => {}
            1 => {
                //
                equivalences_to_remove.push(idx);
                let (_, first_expr) = expr_with_inputs.pop().unwrap();
                for (expr_input, expr) in expr_with_inputs {
                    single_input_filters.push((
                        MirScalarExpr::CallBinary {
                            func: expr::BinaryFunc::Eq,
                            expr1: Box::new(first_expr.clone()),
                            expr2: Box::new(old_join_mapper.map_expr_to_local(expr)),
                        },
                        expr_input,
                    ))
                }
            }
            _ => {
                // for each input referenced in an equivalence class,
                // jot down the other inputs in the same equivalence class.
                for (expr_input, expr) in expr_with_inputs {
                    for other_input in inputs_in_equivalence.iter() {
                        if *other_input != expr_input {
                            join_keys_by_input[expr_input]
                                .entry(*other_input)
                                .or_insert_with(Vec::new)
                                .push(expr.clone());
                        }
                    }
                }
            }
        }
    }
    for idx in equivalences_to_remove.into_iter().rev() {
        equivalences.remove(idx);
    }
    (Some(join_keys_by_input), single_input_filters)
}
