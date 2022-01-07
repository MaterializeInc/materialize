// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Canonicalizes MFPs and performs common sub-expression elimination.
//!
//! Also reduces map and filter expressions.
//!
//! This transform takes a sequence of Maps, Filters, and Projects and
//! canonicalizes it to a sequence like this:
//! | Filter
//! | Map
//! | Filter
//! | Project
//! The filters before the map are those that can be
//! pushed down through a map according the rules of
//! [crate::projection_pushdown::ProjectionPushdown.push_filters_through_map()].
//! TODO: It would be nice to canonicalize it to just an MFP, but currently
//! putting a filter after instead of before a Map can result in the loss of
//! nullability information.
//!
//! After canonicalizing, this transform looks at the Map-Filter-Project
//! subsequence and identifies common `ScalarExpr` expressions across and within
//! expressions that are arguments to the Map-Filter-Project. It reforms the
//! `Map-Filter-Project` subsequence to build each distinct expression at most
//! once and to re-use expressions instead of re-evaluating them.
//!
//! The re-use policy at the moment is severe and re-uses everything.
//! It may be worth considering relations of this if it results in more
//! busywork and less efficiency, but the wins can be substantial when
//! expressions re-use complex subexpressions.

use crate::TransformArgs;
use expr::canonicalize::canonicalize_predicates;
use expr::MirRelationExpr;

/// Canonicalizes MFPs and performs common sub-expression elimination.
#[derive(Debug)]
pub struct CanonicalizeMfp;

impl crate::Transform for CanonicalizeMfp {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(relation)
    }
}

impl CanonicalizeMfp {
    fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        let mut mfp = expr::MapFilterProject::extract_non_errors_from_expr_mut(relation);
        relation.try_visit_mut_children(|e| self.action(e))?;
        mfp.optimize();

        // Loop expression reduction and MFP canonicalization until a steady
        // point is reached.
        loop {
            if mfp.is_identity() {
                break;
            }

            let (mut map, mut filter, project) = mfp.as_map_filter_project();
            let mut relation_type = relation.typ();
            for expr in map.iter_mut() {
                expr.reduce(&relation_type);
                relation_type.column_types.push(expr.typ(&relation_type));
            }
            if !filter.is_empty() {
                // Push down the predicates that can be pushed down, removing
                // them from the mfp object to be optimized.
                canonicalize_predicates(&mut filter, &relation_type);
                let all_errors = filter.iter().all(|p| p.is_literal_err());
                let (retained, mut pushdown) =
                    crate::predicate_pushdown::PredicatePushdown::default()
                        .push_filters_through_map(&map, &mut filter, mfp.input_arity, all_errors);
                if !pushdown.is_empty() {
                    for predicate in pushdown.iter_mut() {
                        predicate.reduce(&relation_type);
                    }
                    *relation = relation.take_dangerous().filter(pushdown);
                }
                filter = retained;
            }

            let mut new_mfp = expr::MapFilterProject::new(mfp.input_arity)
                .map(map)
                .filter(filter)
                .project(project);
            new_mfp.optimize();

            if mfp == new_mfp {
                break;
            }
            mfp = new_mfp;
        }

        if !mfp.is_identity() {
            let (map, filter, project) = mfp.as_map_filter_project();
            let total_arity = mfp.input_arity + map.len();
            if !map.is_empty() {
                *relation = relation.take_dangerous().map(map);
            }
            if !filter.is_empty() {
                *relation = relation.take_dangerous().filter(filter);
            }
            if project.len() != total_arity || !project.iter().enumerate().all(|(i, o)| i == *o) {
                *relation = relation.take_dangerous().project(project);
            }
        }
        Ok(())
    }
}
