// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![deny(missing_debug_implementations)]

use crate::RelationExpr;

pub mod binding;
pub mod constant_join;
pub mod demand;
pub mod empty_map;
pub mod filter_lets;
pub mod fusion;
pub mod inline_let;
pub mod join_elision;
pub mod join_order;
pub mod nonnull_requirements;
pub mod nonnullable;
pub mod predicate_pushdown;
pub mod projection_extraction;
pub mod projection_lifting;
pub mod reduce_elision;
pub mod reduction;
pub mod simplify;
pub mod split_predicates;
pub mod column_knowledge;

/// Types capable of transforming relation expressions.
pub trait Transform: std::fmt::Debug {
    /// Transform a relation into a functionally equivalent relation.
    ///
    /// Arguably the metadata *shouldn't* change, but we're new here.
    fn transform(&self, relation: &mut RelationExpr);
}

#[derive(Debug)]
pub struct Fixpoint {
    transforms: Vec<Box<dyn crate::transform::Transform + Send>>,
}

impl Transform for Fixpoint {
    fn transform(&self, relation: &mut RelationExpr) {
        for _ in 0..100 {
            let original = relation.clone();
            for transform in self.transforms.iter() {
                transform.transform(relation);
            }
            if *relation == original {
                return;
            }
        }
        panic!("Fixpoint looped 100 times! {:#?} {:#?}", self, relation);
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
    fn transform(&self, relation: &mut RelationExpr) {
        for transform in self.transforms.iter() {
            transform.transform(relation);
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
            // Unbinding increases the complexity, but exposes more optimization opportunities.
            Box::new(crate::transform::binding::Unbind),
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
                    Box::new(crate::transform::projection_extraction::ProjectionExtraction),
                    Box::new(crate::transform::projection_lifting::ProjectionLifting),
                    Box::new(crate::transform::filter_lets::FilterLets),
                    Box::new(crate::transform::nonnull_requirements::NonNullRequirements),
                    Box::new(crate::transform::column_knowledge::ColumnKnowledge),
                ],
            }),
            // JoinOrder adds Projects, hence need project fusion again.
            Box::new(crate::transform::demand::Demand),
            Box::new(crate::transform::join_order::JoinOrder),
            Box::new(crate::transform::predicate_pushdown::PredicatePushdown),
            Box::new(crate::transform::projection_lifting::ProjectionLifting),
            Box::new(crate::transform::fusion::project::Project),
            Box::new(crate::transform::constant_join::ConstantJoin),
        ];
        Self { transforms }
    }
}

impl Optimizer {
    /// Optimizes the supplied relation expression.
    pub fn optimize(&mut self, relation: &mut RelationExpr) {
        self.transform(relation);
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
