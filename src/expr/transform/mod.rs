// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(missing_debug_implementations)]
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr};

pub mod binding;
pub mod column_knowledge;
pub mod constant_join;
pub mod demand;
pub mod empty_map;
pub mod filter_lets;
pub mod fusion;
pub mod inline_let;
pub mod join_elision;
pub mod join_implementation;
// pub mod join_order;
pub mod nonnull_requirements;
pub mod nonnullable;
pub mod predicate_pushdown;
pub mod projection_extraction;
pub mod projection_lifting;
pub mod reduce_elision;
pub mod reduction;
pub mod reduction_pushdown;
pub mod redundant_join;
pub mod simplify;
pub mod split_predicates;
pub mod update_let;
// pub mod use_indexes;

/// Types capable of transforming relation expressions.
pub trait Transform: std::fmt::Debug {
    /// Transform a relation into a functionally equivalent relation.
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        env: &EvalEnv,
    );
}

#[derive(Debug)]
pub struct Fixpoint {
    transforms: Vec<Box<dyn crate::transform::Transform + Send>>,
}

impl Transform for Fixpoint {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        env: &EvalEnv,
    ) {
        for _ in 0..100 {
            let original = relation.clone();
            for transform in self.transforms.iter() {
                transform.transform(relation, indexes, env);
            }
            if *relation == original {
                return;
            }
        }
        let original = relation.clone();
        for transform in self.transforms.iter() {
            transform.transform(relation, indexes, env);
        }
        panic!(
            "Fixpoint looped 100 times! {:#?} {}\n{}",
            self,
            original.pretty(),
            relation.pretty()
        );
    }
}

/// A naive optimizer for relation expressions.
///
/// The optimizer currently applies only peep-hole optimizations, from a limited
/// set that were sufficient to get some of TPC-H up and working. It is worth a
/// review at some point to improve the quality, coverage, and architecture of
/// the optimizations.
#[derive(Debug)]
pub struct Optimizer {
    transforms: Vec<Box<dyn crate::transform::Transform + Send>>,
}

impl Transform for Optimizer {
    /// Optimizes the supplied relation expression.
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        env: &EvalEnv,
    ) {
        for transform in self.transforms.iter() {
            transform.transform(relation, indexes, env);
        }
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        let transforms: Vec<Box<dyn crate::transform::Transform + Send>> = vec![
            // The first block are peep-hole optimizations that simplify
            // the representation of the query and are largely uncontentious.
            Box::new(crate::transform::join_elision::JoinElision),
            Box::new(crate::transform::inline_let::InlineLet),
            Box::new(crate::transform::reduction::FoldConstants),
            Box::new(crate::transform::split_predicates::SplitPredicates),
            Box::new(crate::transform::fusion::filter::Filter),
            Box::new(crate::transform::fusion::map::Map),
            Box::new(crate::transform::projection_extraction::ProjectionExtraction),
            Box::new(crate::transform::fusion::project::Project),
            Box::new(crate::transform::fusion::join::Join),
            Box::new(crate::transform::join_elision::JoinElision),
            Box::new(crate::transform::empty_map::EmptyMap),
            // Unbinding risks exponential blow-up on queries with deep let bindings.
            // Box::new(crate::transform::binding::Unbind),
            Box::new(crate::transform::binding::Deduplicate),
            // Early actions include "no-brainer" transformations that reduce complexity in linear passes.
            Box::new(crate::transform::join_elision::JoinElision),
            Box::new(crate::transform::reduction::FoldConstants),
            Box::new(crate::transform::fusion::filter::Filter),
            Box::new(crate::transform::fusion::map::Map),
            Box::new(crate::transform::reduction::FoldConstants),
            Box::new(crate::transform::reduction::DeMorgans),
            Box::new(crate::transform::reduction::UndistributeAnd),
            Box::new(crate::transform::split_predicates::SplitPredicates),
            Box::new(crate::transform::Fixpoint {
                transforms: vec![
                    Box::new(crate::transform::nonnullable::NonNullable),
                    Box::new(crate::transform::reduction::FoldConstants),
                    Box::new(crate::transform::simplify::SimplifyFilterPredicates),
                    Box::new(crate::transform::predicate_pushdown::PredicatePushdown),
                    Box::new(crate::transform::fusion::join::Join),
                    Box::new(crate::transform::fusion::filter::Filter),
                    Box::new(crate::transform::fusion::project::Project),
                    Box::new(crate::transform::fusion::map::Map),
                    Box::new(crate::transform::empty_map::EmptyMap),
                    Box::new(crate::transform::join_elision::JoinElision),
                    Box::new(crate::transform::reduce_elision::ReduceElision),
                    Box::new(crate::transform::inline_let::InlineLet),
                    Box::new(crate::transform::update_let::UpdateLet),
                    Box::new(crate::transform::projection_extraction::ProjectionExtraction),
                    Box::new(crate::transform::projection_lifting::ProjectionLifting),
                    Box::new(crate::transform::filter_lets::FilterLets),
                    Box::new(crate::transform::nonnull_requirements::NonNullRequirements),
                    Box::new(crate::transform::column_knowledge::ColumnKnowledge),
                    Box::new(crate::transform::constant_join::InsertConstantJoin),
                    Box::new(crate::transform::reduction_pushdown::ReductionPushdown),
                    Box::new(crate::transform::redundant_join::RedundantJoin),
                ],
            }),
            // TODO (wangandi): materialize#616 the FilterEqualLiteral transform
            // exists but is currently unevaluated with the new join implementations.

            /*Box::new(crate::transform::use_indexes::FilterEqualLiteral),
            Box::new(crate::transform::projection_lifting::ProjectionLifting),
            Box::new(crate::transform::column_knowledge::ColumnKnowledge),
            Box::new(crate::transform::reduction::FoldConstants),
            Box::new(crate::transform::predicate_pushdown::PredicatePushdown),
            Box::new(crate::transform::fusion::join::Join),
            Box::new(crate::transform::redundant_join::RedundantJoin),*/
            // JoinOrder adds Projects, hence need project fusion again.

            // Implementation transformations
            Box::new(crate::transform::constant_join::RemoveConstantJoin),
            Box::new(crate::transform::Fixpoint {
                transforms: vec![
                    Box::new(crate::transform::projection_lifting::ProjectionLifting),
                    Box::new(crate::transform::join_implementation::JoinImplementation),
                    Box::new(crate::transform::fusion::filter::Filter),
                    Box::new(crate::transform::demand::Demand),
                ],
            }),
            Box::new(crate::transform::fusion::project::Project),
            Box::new(crate::transform::reduction_pushdown::ReductionPushdown),
        ];
        Self { transforms }
    }
}

impl Optimizer {
    /// Optimizes the supplied relation expression.
    pub fn optimize(
        &mut self,
        mut relation: RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        env: &EvalEnv,
    ) -> OptimizedRelationExpr {
        self.transform(&mut relation, indexes, env);
        OptimizedRelationExpr(relation)
    }

    /// Simple fusion and elision transformations to render the query readable.
    pub fn pre_optimization() -> Self {
        let transforms: Vec<Box<dyn crate::transform::Transform + Send>> = vec![
            Box::new(crate::transform::join_elision::JoinElision),
            Box::new(crate::transform::inline_let::InlineLet),
            Box::new(crate::transform::reduction::FoldConstants),
            Box::new(crate::transform::split_predicates::SplitPredicates),
            Box::new(crate::transform::fusion::filter::Filter),
            Box::new(crate::transform::fusion::map::Map),
            Box::new(crate::transform::projection_extraction::ProjectionExtraction),
            Box::new(crate::transform::fusion::project::Project),
            Box::new(crate::transform::fusion::join::Join),
            Box::new(crate::transform::join_elision::JoinElision),
            Box::new(crate::transform::empty_map::EmptyMap),
        ];
        Self { transforms }
    }
}

/// A [`RelationExpr`] that claims to have been optimized, e.g., by an
/// [`Optimizer`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct OptimizedRelationExpr(RelationExpr);

impl OptimizedRelationExpr {
    /// Declare that the input `expr` is optimized, without actually running it
    /// through an optimizer. This can be useful to mark as optimized literal
    /// `RelationExpr`s that are obviously optimal, without invoking the whole
    /// machinery of the optimizer.
    pub fn declare_optimized(expr: RelationExpr) -> OptimizedRelationExpr {
        OptimizedRelationExpr(expr)
    }
}

impl AsRef<RelationExpr> for OptimizedRelationExpr {
    fn as_ref(&self) -> &RelationExpr {
        &self.0
    }
}

impl AsMut<RelationExpr> for OptimizedRelationExpr {
    fn as_mut(&mut self) -> &mut RelationExpr {
        &mut self.0
    }
}
