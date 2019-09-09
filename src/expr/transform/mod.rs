// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![deny(missing_debug_implementations)]

use std::fmt;

use crate::RelationExpr;
use repr::RelationType;

pub mod aggregation;
pub mod empty_map;
pub mod fusion;
pub mod join_order;
pub mod predicate_pushdown;
pub mod reduction;
pub mod split_predicates;

pub trait Transform {
    /// Transform a relation into a functionally equivalent relation.
    ///
    /// Arguably the metadata *shouldn't* change, but we're new here.
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType);
}

/// A naive optimizer for relation expressions.
///
/// The optimizer currently applies only peep-hole optimizations, from a limited
/// set that were sufficient to get some of TPC-H up and working. It is worth a
/// review at some point to improve the quality, coverage, and architecture of
/// the optimizations.
pub struct Optimizer {
    transforms: Vec<Box<dyn crate::transform::Transform + Send>>,
}

impl fmt::Debug for Optimizer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Optimizer({} transforms)", self.transforms.len())
    }
}

impl Optimizer {
    /// Optimizes the supplied relation expression.
    pub fn optimize(&mut self, relation: &mut RelationExpr, metadata: &RelationType) {
        for transform in self.transforms.iter() {
            transform.transform(relation, metadata);
        }
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        let transforms: Vec<Box<dyn crate::transform::Transform + Send>> = vec![
            Box::new(crate::transform::reduction::FoldConstants),
            Box::new(crate::transform::reduction::DeMorgans),
            Box::new(crate::transform::reduction::UndistributeAnd),
            Box::new(crate::transform::split_predicates::SplitPredicates),
            Box::new(crate::transform::fusion::join::Join),
            Box::new(crate::transform::predicate_pushdown::PredicatePushdown),
            Box::new(crate::transform::fusion::filter::Filter),
            Box::new(crate::transform::join_order::JoinOrder),
            Box::new(crate::transform::empty_map::EmptyMap),
        ];
        Self { transforms }
    }
}
