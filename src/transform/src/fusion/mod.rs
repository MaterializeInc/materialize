// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations that fuse together others of their kind.

pub mod filter;
pub mod join;
pub mod map;
pub mod negate;
pub mod project;
pub mod reduce;
pub mod top_k;
pub mod union;

use mz_expr::MirRelationExpr;

use crate::{all, TransformArgs};

/// Fuses multiple like operators together when possible.
#[derive(Debug)]
pub struct Fusion;

impl crate::Transform for Fusion {
    fn recursion_safe(&self) -> bool {
        // Keep this in sync with the actions called in `Fusion::action`!
        all![
            filter::Filter.recursion_safe(),
            map::Map.recursion_safe(),
            project::Project.recursion_safe(),
            negate::Negate.recursion_safe(),
            top_k::TopK.recursion_safe(),
            union::Union.recursion_safe(),
        ]
    }

    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "fusion")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        use mz_expr::visit::Visit;
        relation.visit_mut_post(&mut Self::action)?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl Fusion {
    /// Apply fusion action for variants we know how to fuse.
    pub(crate) fn action(expr: &mut MirRelationExpr) {
        match expr {
            MirRelationExpr::Filter { .. } => filter::Filter::action(expr),
            MirRelationExpr::Map { .. } => map::Map::action(expr),
            MirRelationExpr::Project { .. } => project::Project::action(expr),
            MirRelationExpr::Negate { .. } => negate::Negate::action(expr),
            MirRelationExpr::TopK { .. } => top_k::TopK::action(expr),
            MirRelationExpr::Union { .. } => union::Union::action(expr),
            _ => {}
        }
    }
}
