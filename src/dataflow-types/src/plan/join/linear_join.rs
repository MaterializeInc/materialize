// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning of linear joins.

use crate::plan::join::JoinBuildState;
use crate::plan::join::JoinClosure;
use crate::plan::Permutation;
use expr::MapFilterProject;
use expr::MirScalarExpr;
use serde::{Deserialize, Serialize};

/// A plan for the execution of a linear join.
///
/// A linear join is a sequence of stages, each of which introduces
/// a new collecion. Each stage is represented by a [LinearStagePlan].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinearJoinPlan {
    /// The source relation from which we start the join.
    pub source_relation: usize,
    /// An initial closure to apply before any stages.
    ///
    /// Values of `None` indicate the identity closure.
    pub initial_closure: Option<JoinClosure>,
    /// A *sequence* of stages to apply one after the other.
    pub stage_plans: Vec<LinearStagePlan>,
    /// A concluding closure to apply after the last stage.
    ///
    /// Values of `None` indicate the identity closure.
    pub final_closure: Option<JoinClosure>,
}

/// A plan for the execution of one stage of a linear join.
///
/// Each stage is a binary join between the current accumulated
/// join results, and a new collection. The former is referred to
/// as the "stream" and the latter the "lookup".
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinearStagePlan {
    /// The relation index into which we will look up.
    pub lookup_relation: usize,
    /// The key expressions to use for the streamed relation.
    ///
    /// While this starts as a stream of the source relation,
    /// it evolves through multiple lookups and ceases to be
    /// the same thing, hence the different name.
    pub stream_key: Vec<MirScalarExpr>,
    /// The permutation of the stream
    pub stream_permutation: Permutation,
    /// The thinning expression to apply on the value part of the stream
    pub stream_thinning: Vec<usize>,
    /// The key expressions to use for the lookup relation.
    pub lookup_key: Vec<MirScalarExpr>,
    /// The lookup key permutation
    pub lookup_permutation: Permutation,
    /// The thinning expression to apply on the lookup relation
    pub lookup_thinning: Vec<usize>,
    /// The closure to apply to the concatenation of columns
    /// of the stream and lookup relations.
    pub closure: JoinClosure,
}

impl LinearJoinPlan {
    /// Create a new join plan from the required arguments.
    pub fn create_from(
        source_relation: usize,
        equivalences: &[Vec<MirScalarExpr>],
        join_order: &[(usize, Vec<MirScalarExpr>)],
        input_mapper: expr::JoinInputMapper,
        map_filter_project: &mut MapFilterProject,
    ) -> Self {
        let temporal_mfp = map_filter_project.extract_temporal();

        // Construct initial join build state.
        // This state will evolves as we build the join dataflow.
        let mut join_build_state = JoinBuildState::new(
            input_mapper.global_columns(source_relation),
            &equivalences,
            &map_filter_project,
        );

        // We would prefer to extract a closure here, but we do not know if
        // the input will be arranged or not.
        let initial_closure = None;

        // Sequence of steps to apply.
        let mut stage_plans = Vec::with_capacity(join_order.len());

        // Track the set of bound input relations, for equivalence resolution.
        let mut bound_inputs = vec![source_relation];

        // The arity of the stream of updates, to be modified for each lookup relation
        let mut stream_arity = input_mapper.input_arity(source_relation);
        // Iterate through the join order instructions, assembling keys and
        // closures to use.
        for (lookup_relation, lookup_key) in join_order.iter() {
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

            // Introduce new columns and expressions they enable. Form a new closure.
            let closure = join_build_state.add_columns(
                input_mapper.global_columns(*lookup_relation),
                &lookup_key_rebased,
            );
            let (stream_permutation, stream_thinning) =
                Permutation::construct_from_expr(&stream_key, stream_arity);
            let (lookup_permutation, lookup_thinning) = Permutation::construct_from_expr(
                &lookup_key,
                input_mapper.input_arity(*lookup_relation),
            );
            stream_arity = closure.before.projection.len();

            bound_inputs.push(*lookup_relation);

            // record the stage plan as next in the path.
            stage_plans.push(LinearStagePlan {
                lookup_relation: *lookup_relation,
                stream_key,
                stream_permutation,
                stream_thinning,
                lookup_key: lookup_key.clone(),
                lookup_permutation,
                lookup_thinning,
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

        // Now that `map_filter_project` has been captured in the state builder,
        // assign the remaining temporal predicates to it, for the caller's use.
        *map_filter_project = temporal_mfp;

        // Form and return the complete join plan.
        LinearJoinPlan {
            source_relation,
            initial_closure,
            stage_plans,
            final_closure,
        }
    }
}
