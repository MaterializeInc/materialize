// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Delta join execution planning.
//!
//! Delta joins are a join over multiple input relations, implemented by an
//! independent dataflow path for each input. Each path is joined against the
//! other inputs using a "lookup" operator, and the path results are collected
//! and return as the output for the entire dataflow.
//!
//! This implementation strategy allows us to re-use existing arrangements, and
//! not create any new stateful operators.

use crate::plan::join::JoinBuildState;
use crate::plan::join::JoinClosure;
use crate::plan::make_thinning_expression;
use expr::JoinInputMapper;
use expr::MapFilterProject;
use expr::MirScalarExpr;
use serde::{Deserialize, Serialize};

/// A delta query is implemented by a set of paths, one for each input.
///
/// Each delta query path responds to its input changes by repeated lookups
/// in arrangements for other join inputs. These lookups require specific
/// instructions about which expressions to use as keys. Along the way,
/// various closures are applied to filter and project as early as possible.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaJoinPlan {
    /// The set of path plans.
    ///
    /// Each path identifies its source relation, so the order is only
    /// important for determinism of dataflow construction.
    pub path_plans: Vec<DeltaPathPlan>,
}

/// A delta query path is implemented by a sequences of stages,
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaPathPlan {
    /// The relation whose updates seed the dataflow path.
    pub source_relation: usize,
    /// The key we expect the source relation to be arranged by.
    pub source_key: Vec<MirScalarExpr>,
    /// An initial closure to apply before any stages.
    pub initial_closure: JoinClosure,
    /// A *sequence* of stages to apply one after the other.
    pub stage_plans: Vec<DeltaStagePlan>,
    /// A concluding closure to apply after the last stage.
    ///
    /// Values of `None` indicate the identity closure.
    pub final_closure: Option<JoinClosure>,
}

/// A delta query stage performs a stream lookup into an arrangement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaStagePlan {
    /// The relation index into which we will look up.
    pub lookup_relation: usize,
    /// The key expressions to use for the streamed relation.
    ///
    /// While this starts as a stream of the source relation,
    /// it evolves through multiple lookups and ceases to be
    /// the same thing, hence the different name.
    pub stream_key: Vec<MirScalarExpr>,
    /// The columns in the stream that are not redundant with the key
    pub stream_thinning: Vec<usize>,
    /// The key expressions to use for the lookup relation.
    pub lookup_key: Vec<MirScalarExpr>,
    /// The closure to apply to the concatenation of columns
    /// of the stream and lookup relations.
    pub closure: JoinClosure,
}

impl DeltaJoinPlan {
    /// Create a new join plan from the required arguments.
    pub fn create_from(
        equivalences: &[Vec<MirScalarExpr>],
        join_orders: &[Vec<(usize, Vec<MirScalarExpr>)>],
        input_mapper: JoinInputMapper,
        map_filter_project: &mut MapFilterProject,
    ) -> Self {
        let number_of_inputs = join_orders.len();

        // Pick the "first" (by `Ord`) key for the source relation of each path.
        // (This matches the probably arbitrary historical practice from `mod render`.)
        let mut source_keys = vec![None; number_of_inputs];
        for source_relation in 0..number_of_inputs {
            for (lookup_relation, lookup_key) in &join_orders[source_relation] {
                let key = &mut source_keys[*lookup_relation];
                if key.is_none() || key.as_ref().unwrap() > lookup_key {
                    *key = Some(lookup_key.clone())
                }
            }
        }
        let source_keys: Vec<_> = source_keys
            .into_iter()
            .map(|k| k.expect("There should be at least one arrangement for each relation!"))
            .collect();

        // Create an empty plan, with capacity for the intended number of path plans.
        let mut join_plan = DeltaJoinPlan {
            path_plans: Vec::with_capacity(number_of_inputs),
        };

        let temporal_mfp = map_filter_project.extract_temporal();

        // Each source relation will contribute a path to the join plan.
        for source_relation in 0..number_of_inputs {
            // Construct initial join build state.
            // This state will evolves as we build the join dataflow.
            let mut join_build_state = JoinBuildState::new(
                input_mapper.global_columns(source_relation),
                &equivalences,
                &map_filter_project,
            );

            let source_key = &source_keys[source_relation];
            // Initial action we can take on the source relation before joining.
            let initial_closure = join_build_state.extract_closure(None, source_key);

            // Sequence of steps to apply.
            let mut stage_plans = Vec::with_capacity(number_of_inputs - 1);

            // We track the input relations as they are added to the join so we can figure out
            // which expressions have been bound.
            let mut bound_inputs = vec![source_relation];
            // We use the order specified by the implementation.
            let order = &join_orders[source_relation];

            let mut unthinned_stream_arity = initial_closure.before.projection.len();

            for (lookup_relation, lookup_key) in order.iter() {
                // rebase the intended key to use global column identifiers.
                let lookup_key_rebased = lookup_key
                    .iter()
                    .map(|k| input_mapper.map_expr_to_global(k.clone(), *lookup_relation))
                    .collect::<Vec<_>>();

                // Expressions to use as a key for the stream of incoming updates
                // are determined by locating the elements of `lookup_key` among
                // the existing bound `columns`. If that cannot be done, the plan
                // is irrecoverably defective and we panic.
                // TODO: explicitly validate this before rendering.
                let stream_key = lookup_key_rebased
                    .iter()
                    .map(|expr| {
                        let mut bound_expr = input_mapper
                            .find_bound_expr(expr, &bound_inputs, &join_build_state.equivalences)
                            .expect("Expression in join plan is not bound at time of use");
                        // Rewrite column references to physical locations.
                        bound_expr.permute_map(&join_build_state.column_map);
                        bound_expr
                    })
                    .collect::<Vec<_>>();

                let stream_thinning = make_thinning_expression(&stream_key, unthinned_stream_arity);

                // Introduce new columns and expressions they enable. Form a new closure.
                let closure = join_build_state.add_columns(
                    input_mapper.global_columns(*lookup_relation),
                    &lookup_key_rebased,
                    Some((&stream_key, stream_thinning.len())),
                    &lookup_key,
                );
                unthinned_stream_arity = closure.before.projection.len();

                bound_inputs.push(*lookup_relation);
                // record the stage plan as next in the path.
                stage_plans.push(DeltaStagePlan {
                    lookup_relation: *lookup_relation,
                    stream_key,
                    lookup_key: lookup_key.clone(),
                    stream_thinning,
                    closure,
                });
            }
            // determine a final closure, and complete the path plan.
            let final_closure = join_build_state.complete();
            let final_closure = if final_closure.is_identity() {
                None
            } else {
                Some(final_closure)
            };

            // Insert the path plan.
            join_plan.path_plans.push(DeltaPathPlan {
                source_relation,
                initial_closure,
                stage_plans,
                final_closure,
                source_key: source_key.to_vec(),
            });
        }

        // Now that `map_filter_project` has been captured in the state builder,
        // assign the remaining temporal predicates to it, for the caller's use.
        *map_filter_project = temporal_mfp;

        join_plan
    }
}
