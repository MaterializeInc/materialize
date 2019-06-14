// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use super::RelationExpr;
use crate::repr::RelationType;

pub mod aggregation;
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
