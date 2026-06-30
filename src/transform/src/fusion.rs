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

use crate::TransformCtx;

/// Fuses multiple like operators together when possible.
#[derive(Debug)]
pub struct Fusion;

impl crate::Transform for Fusion {
    fn name(&self) -> &'static str {
        "Fusion"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "fusion")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        use mz_expr::visit::Visit;
        let enable_eqsat_scalar = ctx.features.enable_eqsat_scalar_canonicalize;
        relation.visit_mut_post(&mut |e| Self::action(e, enable_eqsat_scalar));
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl Fusion {
    /// Apply fusion action for variants we know how to fuse.
    ///
    /// `enable_eqsat_scalar` is forwarded to the `Filter` fusion's predicate
    /// canonicalization.
    pub(crate) fn action(expr: &mut MirRelationExpr, enable_eqsat_scalar: bool) {
        match expr {
            MirRelationExpr::Filter { .. } => filter::Filter::action(expr, enable_eqsat_scalar),
            MirRelationExpr::Map { .. } => map::Map::action(expr),
            MirRelationExpr::Project { .. } => project::Project::action(expr),
            MirRelationExpr::Negate { .. } => negate::Negate::action(expr),
            MirRelationExpr::TopK { .. } => top_k::TopK::action(expr),
            MirRelationExpr::Union { .. } => union::Union::action(expr),
            _ => {}
        }
    }
}
