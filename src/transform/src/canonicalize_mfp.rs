// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Canonicalizes MFPs, e.g., performs CSE on the scalar expressions, eliminates identity MFPs.
//!
//! Also calls `canonicalize_predicates` through `fusion::filter`.
//!
//! This transform takes a sequence of Maps, Filters, and Projects and
//! canonicalizes it to a sequence like this:
//! | Map
//! | Filter
//! | Project
//!
//! As part of canonicalizing, this transform looks at the Map-Filter-Project
//! subsequence and identifies common `ScalarExpr` expressions across and within
//! expressions that are arguments to the Map-Filter-Project. It reforms the
//! `Map-Filter-Project` subsequence to build each distinct expression at most
//! once and to re-use expressions instead of re-evaluating them.
//!
//! The re-use policy at the moment is severe and re-uses everything.
//! It may be worth considering relations of this if it results in more
//! busywork and less efficiency, but the wins can be substantial when
//! expressions re-use complex subexpressions.

use mz_expr::visit::VisitChildren;
use mz_expr::{MapFilterProject, MirRelationExpr};

use crate::TransformCtx;

/// Canonicalizes MFPs and performs common sub-expression elimination.
#[derive(Debug)]
pub struct CanonicalizeMfp;

impl crate::Transform for CanonicalizeMfp {
    fn name(&self) -> &'static str {
        "CanonicalizeMfp"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "canonicalize_mfp")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let result = self.action(relation);
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

impl CanonicalizeMfp {
    /// Extract and optimize MFPs.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        let mut mfp = MapFilterProject::extract_non_errors_from_expr_mut(relation);
        // Optimize MFP, e.g., perform CSE Push MFPs through `Negate` operators,
        // if encountered. This is a first steps toward a `CanonicalizeLinear`,
        // which puts linear operators in a canonical representation.
        mfp.optimize();
        if let MirRelationExpr::Negate { input } = relation {
            Self::rebuild_mfp(mfp, &mut **input);
            relation.try_visit_mut_children(|e| self.action(e))?;
        } else {
            relation.try_visit_mut_children(|e| self.action(e))?;
            Self::rebuild_mfp(mfp, relation);
        }
        Ok(())
    }

    /// Canonicalize the MapFilterProject to Map-Filter-Project, in that order.
    pub fn rebuild_mfp(mfp: MapFilterProject, relation: &mut MirRelationExpr) {
        if !mfp.is_identity() {
            let (map, filter, project) = mfp.as_map_filter_project();
            let total_arity = mfp.input_arity + map.len();
            if !map.is_empty() {
                *relation = relation.take_dangerous().map(map);
            }
            if !filter.is_empty() {
                *relation = relation.take_dangerous().filter(filter);
                crate::fusion::filter::Filter::action(relation);
            }
            if project.len() != total_arity || !project.iter().enumerate().all(|(i, o)| i == *o) {
                *relation = relation.take_dangerous().project(project);
            }
        }
    }
}
