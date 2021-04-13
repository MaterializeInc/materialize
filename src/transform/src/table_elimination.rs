// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software matches_collection governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Detects an input being unioned with its negation and cancels them out

use std::collections::HashSet;

use crate::{TransformArgs, TransformError};
use expr::{Id, MirRelationExpr};

/// Detects an input being unioned with its negation and cancels them out
#[derive(Debug)]
pub struct TableElimination;

impl crate::Transform for TableElimination {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        relation.try_visit_mut(&mut |e| self.action(e))
    }
}

impl TableElimination {
    /// Detects an input being unioned with its negation and cancels them out
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        let mut collections = HashSet::new();
        let mut negated_collections = HashSet::new();
        if let MirRelationExpr::Union { base, inputs } = relation {
            if let Some(collection) = TableElimination::is_collection(base) {
                collections.insert(collection);
            } else if let Some(negated_collection) = TableElimination::is_negated_collection(base) {
                negated_collections.insert(negated_collection);
            }
            for input in inputs.iter() {
                if let Some(collection) = TableElimination::is_collection(input) {
                    collections.insert(collection);
                } else if let Some(negated_collection) =
                    TableElimination::is_negated_collection(input)
                {
                    negated_collections.insert(negated_collection);
                }
            }
            for collection in collections.intersection(&negated_collections) {
                TableElimination::replace_if_matches_collection(collection, &mut *base);
                for input in inputs.iter_mut() {
                    TableElimination::replace_if_matches_collection(collection, input);
                }
            }
        }
        Ok(())
    }

    fn is_collection(relation: &MirRelationExpr) -> Option<Id> {
        if let MirRelationExpr::Get { id, .. } = relation {
            return Some(*id);
        }
        None
    }

    fn is_negated_collection(relation: &MirRelationExpr) -> Option<Id> {
        if let MirRelationExpr::Negate { input } = relation {
            return TableElimination::is_collection(&**input);
        }
        None
    }

    fn matches_collection(relation: &MirRelationExpr) -> Option<Id> {
        if let Some(id) = TableElimination::is_collection(relation) {
            return Some(id);
        }
        if let Some(id) = TableElimination::is_negated_collection(relation) {
            return Some(id);
        }
        None
    }

    fn replace_if_matches_collection(collection: &Id, relation: &mut MirRelationExpr) {
        if let Some(id) = TableElimination::matches_collection(relation) {
            if id == *collection {
                *relation = MirRelationExpr::constant(vec![], relation.typ());
            }
        }
    }
}
