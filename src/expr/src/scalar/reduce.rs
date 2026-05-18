// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rewrite-rule-driven simplification of `MirScalarExpr`.
//!
//! `reduce` repeatedly applies a fixed set of local rewrite rules to an
//! expression until reaching a fixed point. The per-variant rules live in
//! sibling modules (`unary`, `binary`, `variadic`, `if_then`); this file
//! owns the fixed-point loop and the pre/post-pass dispatch.
//!
//! Behavioral parity with the prior imperative implementation is a goal:
//! rule order and the pre-/post-pass split are preserved.

use mz_repr::{ReprColumnType, RowArena};

use crate::MirScalarExpr;
use crate::scalar::func::UnaryFunc;
use crate::visit::Visit;

mod binary;
mod if_then;
mod unary;
mod variadic;

/// Reduce `expr` to a simpler equivalent form by repeatedly applying local
/// rewrite rules until reaching a fixed point.
pub fn reduce(expr: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    let temp_storage = &RowArena::new();

    // Simplifications run in a loop until `expr` no longer changes.
    let mut old = MirScalarExpr::column(0);
    while old != *expr {
        old = expr.clone();
        #[allow(deprecated)]
        expr.visit_mut_pre_post_nolimit(
            &mut |e| {
                reduce_pre(e, column_types);
                None
            },
            &mut |e| reduce_post(e, column_types, temp_storage),
        );
    }
}

/// Pre-order rewrites, applied before children are visited.
///
/// `IsNull` and `Not` need to fire pre-order: if they push themselves inward
/// (e.g. `Not(Not(x)) → x`), the result is the new node at this position,
/// which the visitor will then descend into for normal post-order handling.
fn reduce_pre(e: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    match e {
        MirScalarExpr::CallUnary { func, expr } => match func {
            UnaryFunc::IsNull(_) => {
                if !expr.typ(column_types).nullable {
                    *e = MirScalarExpr::literal_false();
                } else if let Some(rewritten) = expr.decompose_is_null() {
                    // Try to at least decompose IsNull into a disjunction of
                    // simpler IsNull subexpressions.
                    *e = rewritten;
                }
            }
            UnaryFunc::Not(_) => match &mut **expr {
                // Push down not expressions
                // Two negates cancel each other out.
                MirScalarExpr::CallUnary {
                    expr: inner_expr,
                    func: UnaryFunc::Not(_),
                } => {
                    *e = inner_expr.take();
                }
                // Transforms `NOT(a <op> b)` to `a negate(<op>) b` if a
                // negation exists.
                MirScalarExpr::CallBinary {
                    expr1,
                    expr2,
                    func: bf,
                } => {
                    if let Some(negated) = bf.negate() {
                        *e = MirScalarExpr::CallBinary {
                            expr1: Box::new(expr1.take()),
                            expr2: Box::new(expr2.take()),
                            func: negated,
                        };
                    }
                }
                MirScalarExpr::CallVariadic { .. } => e.demorgans(),
                _ => {}
            },
            _ => {}
        },
        _ => {}
    }
}

/// Post-order rewrites, applied after children have been reduced.
fn reduce_post(e: &mut MirScalarExpr, column_types: &[ReprColumnType], temp_storage: &RowArena) {
    match e {
        // Evaluate and pull up constants
        MirScalarExpr::Column(_, _)
        | MirScalarExpr::Literal(_, _)
        | MirScalarExpr::CallUnmaterializable(_) => {}
        MirScalarExpr::CallUnary { .. } => unary::reduce_call_unary(e, column_types, temp_storage),
        MirScalarExpr::CallBinary { .. } => {
            binary::reduce_call_binary(e, column_types, temp_storage)
        }
        MirScalarExpr::CallVariadic { .. } => {
            variadic::reduce_call_variadic(e, column_types, temp_storage)
        }
        MirScalarExpr::If { .. } => if_then::reduce_if(e, column_types),
    }
}
