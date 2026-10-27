// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A transform that collects optimizer notices that don't naturally fit into other transforms.

use mz_expr::{BinaryFunc, MirRelationExpr, MirScalarExpr};

use crate::notice::EqualsNull;
use crate::{TransformCtx, TransformError};

/// A transform that collects optimizer notices that don't naturally fit into other transforms.
///
/// This transform scans the plan for patterns that should generate notices,
/// such as `= NULL` comparisons, which are almost always mistakes.
#[derive(Debug)]
pub struct CollectNotices;

impl crate::Transform for CollectNotices {
    fn name(&self) -> &'static str {
        "CollectNotices"
    }

    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        let mut found_equals_null = false;

        relation.visit_scalars(&mut |scalar: &MirScalarExpr| {
            if !found_equals_null {
                found_equals_null = Self::contains_equals_null(scalar);
            }
        });

        if found_equals_null {
            ctx.df_meta.push_optimizer_notice_dedup(EqualsNull);
        }

        Ok(())
    }
}

impl CollectNotices {
    /// Checks if the given scalar expression contains an `= NULL` or `<> NULL` comparison.
    fn contains_equals_null(expr: &MirScalarExpr) -> bool {
        let mut found = false;
        expr.visit_pre(|e| {
            if let MirScalarExpr::CallBinary { func, expr1, expr2 } = e {
                if matches!(func, BinaryFunc::Eq(_) | BinaryFunc::NotEq(_)) {
                    if expr1.is_literal_null() || expr2.is_literal_null() {
                        found = true;
                    }
                }
            }
        });
        found
    }
}
