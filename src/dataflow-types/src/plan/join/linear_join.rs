// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning of linear joins.

use std::collections::HashMap;

use crate::plan::join::JoinBuildState;
use crate::plan::join::JoinClosure;
use crate::plan::AvailableCollections;
use mz_expr::MapFilterProject;

use mz_expr::join_permutations;
use mz_expr::permutation_for_arrangement;
use mz_expr::MirScalarExpr;
use mz_repr::proto::newapi::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use proptest::prelude::*;
use proptest::result::Probability;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use super::ProtoLinearJoinPlan;
use super::ProtoLinearStagePlan;
use super::ProtoMirScalarVec;

/// A plan for the execution of a linear join.
///
/// A linear join is a sequence of stages, each of which introduces
/// a new collection. Each stage is represented by a [LinearStagePlan].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LinearJoinPlan {
    /// The source relation from which we start the join.
    pub source_relation: usize,
    /// The arrangement to use for the source relation, if any
    pub source_key: Option<Vec<MirScalarExpr>>,
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

impl Arbitrary for LinearJoinPlan {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<usize>(),
            any_with::<Option<Vec<MirScalarExpr>>>((Probability::default(), ((0..3).into(), ()))),
            any::<Option<JoinClosure>>(),
            prop::collection::vec(any::<LinearStagePlan>(), 0..3),
            any::<Option<JoinClosure>>(),
        )
            .prop_map(
                |(source_relation, source_key, initial_closure, stage_plans, final_closure)| {
                    LinearJoinPlan {
                        source_relation,
                        source_key,
                        initial_closure,
                        stage_plans,
                        final_closure,
                    }
                },
            )
            .boxed()
    }
}

impl RustType<ProtoLinearJoinPlan> for LinearJoinPlan {
    fn into_proto(&self) -> ProtoLinearJoinPlan {
        ProtoLinearJoinPlan {
            source_relation: self.source_relation.into_proto(),
            source_key: self.source_key.into_proto(),
            initial_closure: self.initial_closure.into_proto(),
            stage_plans: self.stage_plans.into_proto(),
            final_closure: self.final_closure.into_proto(),
        }
    }

    fn from_proto(proto: ProtoLinearJoinPlan) -> Result<Self, TryFromProtoError> {
        Ok(LinearJoinPlan {
            source_relation: proto.source_relation.into_rust()?,
            source_key: proto.source_key.into_rust()?,
            initial_closure: proto.initial_closure.into_rust()?,
            stage_plans: proto.stage_plans.into_rust()?,
            final_closure: proto.final_closure.into_rust()?,
        })
    }
}

impl RustType<ProtoMirScalarVec> for Vec<MirScalarExpr> {
    fn into_proto(&self) -> ProtoMirScalarVec {
        ProtoMirScalarVec {
            values: self.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMirScalarVec) -> Result<Self, TryFromProtoError> {
        proto.values.into_rust()
    }
}

/// A plan for the execution of one stage of a linear join.
///
/// Each stage is a binary join between the current accumulated
/// join results, and a new collection. The former is referred to
/// as the "stream" and the latter the "lookup".
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LinearStagePlan {
    /// The relation index into which we will look up.
    pub lookup_relation: usize,
    /// The key expressions to use for the streamed relation.
    ///
    /// While this starts as a stream of the source relation,
    /// it evolves through multiple lookups and ceases to be
    /// the same thing, hence the different name.
    pub stream_key: Vec<MirScalarExpr>,
    /// The thinning expression to
    /// use to remove redundant value columns when
    /// arranging the stream.
    pub stream_thinning: Vec<usize>,
    /// The key expressions to use for the lookup relation.
    pub lookup_key: Vec<MirScalarExpr>,
    /// The closure to apply to the concatenation of columns
    /// of the stream and lookup relations.
    pub closure: JoinClosure,
}

impl RustType<ProtoLinearStagePlan> for LinearStagePlan {
    fn into_proto(&self) -> ProtoLinearStagePlan {
        ProtoLinearStagePlan {
            lookup_relation: self.lookup_relation.into_proto(),
            stream_key: self.stream_key.into_proto(),
            stream_thinning: self.stream_thinning.into_proto(),
            lookup_key: self.lookup_key.into_proto(),
            closure: Some(self.closure.into_proto()),
        }
    }

    fn from_proto(proto: ProtoLinearStagePlan) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            lookup_relation: proto.lookup_relation.into_rust()?,
            stream_key: proto.stream_key.into_rust()?,
            stream_thinning: proto.stream_thinning.into_rust()?,
            lookup_key: proto.lookup_key.into_rust()?,
            closure: proto
                .closure
                .into_rust_if_some("ProtoLinearStagePlan::closure")?,
        })
    }
}

impl LinearJoinPlan {
    /// Create a new join plan from the required arguments.
    pub fn create_from(
        source_relation: usize,
        source_arrangement: Option<&(Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>)>,
        equivalences: &[Vec<MirScalarExpr>],
        join_order: &[(usize, Vec<MirScalarExpr>)],
        input_mapper: mz_expr::JoinInputMapper,
        mfp_above: &mut MapFilterProject,
        available: &[AvailableCollections],
    ) -> (Self, Vec<AvailableCollections>) {
        let mut requested: Vec<AvailableCollections> =
            vec![Default::default(); input_mapper.total_inputs()];
        let temporal_mfp = mfp_above.extract_temporal();
        // Construct initial join build state.
        // This state will evolves as we build the join dataflow.
        let mut join_build_state = JoinBuildState::new(
            input_mapper.global_columns(source_relation),
            &equivalences,
            &mfp_above,
        );

        let unthinned_source_arity = input_mapper.input_arity(source_relation);
        let (initial_closure, source_key) =
            if let Some((key, permutation, thinning)) = source_arrangement {
                let mut mfp = MapFilterProject::new(unthinned_source_arity);
                mfp.permute(permutation.clone(), key.len() + thinning.len());
                let mfp = mfp.into_plan().unwrap().into_nontemporal().unwrap();
                (
                    Some(JoinClosure {
                        ready_equivalences: vec![],
                        before: mfp,
                    }),
                    Some(key.clone()),
                )
            } else {
                (None, None)
            };
        let mut unthinned_stream_arity = initial_closure
            .as_ref()
            .map(|closure| closure.before.projection.len())
            .unwrap_or(unthinned_source_arity);
        // Sequence of steps to apply.
        let mut stage_plans = Vec::with_capacity(join_order.len());

        // Track the set of bound input relations, for equivalence resolution.
        let mut bound_inputs = vec![source_relation];

        // Iterate through the join order instructions, assembling keys and
        // closures to use.
        for (lookup_relation, lookup_key) in join_order.iter() {
            let available = &available[*lookup_relation];

            let (lookup_permutation, lookup_thinning) = available
                .arranged
                .iter()
                .find_map(|(key, permutation, thinning)| {
                    if key == lookup_key {
                        Some((permutation.clone(), thinning.clone()))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| {
                    let (permutation, thinning) = permutation_for_arrangement::<HashMap<_, _>>(
                        lookup_key,
                        input_mapper.input_arity(*lookup_relation),
                    );
                    requested[*lookup_relation].arranged.push((
                        lookup_key.clone(),
                        permutation.clone(),
                        thinning.clone(),
                    ));
                    (permutation, thinning)
                });
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
            let (stream_permutation, stream_thinning) =
                permutation_for_arrangement::<HashMap<_, _>>(&stream_key, unthinned_stream_arity);
            let key_arity = stream_key.len();
            let permutation = join_permutations(
                key_arity,
                stream_permutation.clone(),
                stream_thinning.len(),
                lookup_permutation.clone(),
            );
            // Introduce new columns and expressions they enable. Form a new closure.
            let closure = join_build_state.add_columns(
                input_mapper.global_columns(*lookup_relation),
                &lookup_key_rebased,
                key_arity + stream_thinning.len() + lookup_thinning.len(),
                permutation,
            );
            let new_unthinned_stream_arity = closure.before.projection.len();

            bound_inputs.push(*lookup_relation);

            // record the stage plan as next in the path.
            stage_plans.push(LinearStagePlan {
                lookup_relation: *lookup_relation,
                stream_key,
                stream_thinning,
                lookup_key: lookup_key.clone(),
                closure,
            });
            unthinned_stream_arity = new_unthinned_stream_arity;
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
        *mfp_above = temporal_mfp;

        // Form and return the complete join plan.
        let plan = LinearJoinPlan {
            source_relation,
            source_key,
            initial_closure,
            stage_plans,
            final_closure,
        };
        (plan, requested)
    }
}
