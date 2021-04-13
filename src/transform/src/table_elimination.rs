// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software matches_collection governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Detects an input being unioned with its negation and cancels them out

use crate::{TransformArgs, TransformError};
use expr::MirRelationExpr;

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
        if let MirRelationExpr::Union { base, inputs } = relation {
            if let MirRelationExpr::Negate { input } = &**base {
                if TableElimination::find_and_replace_relation(&input, inputs.iter_mut()) {
                    TableElimination::cancel_relation(&mut *base);
                }
            } else {
                if TableElimination::find_and_replace_negated_relation(&*base, inputs.iter_mut()) {
                    TableElimination::cancel_relation(&mut *base);
                }
            }
        }
        Ok(())
    }

    fn find_and_replace_relation<'a, I>(relation: &MirRelationExpr, mut it: I) -> bool
    where
        I: Iterator<Item = &'a mut MirRelationExpr>,
    {
        while let Some(mut other) = it.next() {
            if *relation == *other {
                TableElimination::cancel_relation(&mut other);
                return true;
            }
        }
        false
    }

    fn find_and_replace_negated_relation<'a, I>(relation: &MirRelationExpr, mut it: I) -> bool
    where
        I: Iterator<Item = &'a mut MirRelationExpr>,
    {
        while let Some(mut other) = it.next() {
            if let MirRelationExpr::Negate { input } = &*other {
                if *relation == **input {
                    TableElimination::cancel_relation(&mut other);
                    return true;
                }
            }
        }
        false
    }

    fn cancel_relation(relation: &mut MirRelationExpr) {
        *relation = MirRelationExpr::constant(vec![], relation.typ());
    }
}
