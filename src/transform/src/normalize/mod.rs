// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Normalize the structure of `Let` and `LetRec` operators in expressions.
//!
//! Normalization happens in the context of "scopes", corresponding to
//! 1. the expression's root and 2. each instance of a `LetRec` AST node.
//!
//! Within each scope,
//! 1. Each expression is normalized to have all `Let` nodes at the root
//! of the expression, in order of identifier.
//! 2. Each expression assigns a contiguous block of identifiers.
//!
//! The transform may remove some `Let` and `Get` operators, and does not
//! introduce any new operators.
//!
//! The module also publishes the function `renumber_bindings` which can
//! be used to renumber bindings in an expression starting from a provided
//! `IdGen`, which is used to prepare distinct expressions for inlining.

use mz_expr::{visit::Visit, MirRelationExpr};

use crate::TransformArgs;

/// Install replace certain `Get` operators with their `Let` value.
#[derive(Debug)]
pub struct Normalize {
    normalize_lets: Box<crate::normalize_lets::NormalizeLets>,
}

impl Normalize {
    /// Construct a new [`Normalize`] instance.
    pub fn new() -> Normalize {
        Normalize {
            normalize_lets: Box::new(crate::normalize_lets::NormalizeLets::new(false)),
        }
    }
}

impl crate::Transform for Normalize {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "normalize")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        // (1) Normalize lets first. This might enable transforms in (2).
        self.normalize_lets.transform_without_trace(relation)?;
        // (2) Canonicalize and fuse various operators as a bottom-up transform.
        relation.try_visit_mut_post::<_, crate::TransformError>(
            &mut |expr: &mut MirRelationExpr| {
                // (a) Might enable Map fusion in the next step.
                crate::canonicalization::FlatMapToMap::action(expr);
                crate::canonicalization::TopKElision::action(expr);
                // (b) Fuse various like-kinded operators. Might enable furhter canonicalization.
                crate::fusion::Fusion::action(expr);
                // (c) Fuse join trees (might lift in-between Filters).
                crate::fusion::join::Join::action(expr)?;
                // (d) Extract column references in Map as Project.
                crate::projection_extraction::ProjectionExtraction::action(expr)?;
                // Done!
                Ok(())
            },
        )?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}
