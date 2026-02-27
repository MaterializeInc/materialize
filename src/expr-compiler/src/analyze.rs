// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Expression analysis: determines whether a [`MirScalarExpr`] can be compiled to WASM.
//!
//! For milestone 1, only a small subset of expressions is compilable:
//! * `Column(idx)` — reads from an input column
//! * `Literal(Ok(row), _)` where the datum is `Int64`
//! * `CallBinary { func: BinaryFunc::AddInt64(..), .. }` where both children are compilable

use mz_expr::MirScalarExpr;
use mz_repr::Datum;

/// Returns `true` if the given expression tree can be compiled to WASM.
///
/// Falls back to the interpreter for anything this returns `false` for.
pub fn is_compilable(expr: &MirScalarExpr) -> bool {
    match expr {
        MirScalarExpr::Column(_, _) => true,
        MirScalarExpr::Literal(Ok(row), _col_type) => {
            let datum = row.unpack_first();
            matches!(datum, Datum::Int64(_) | Datum::Null)
        }
        MirScalarExpr::Literal(Err(_), _) => false,
        MirScalarExpr::CallBinary { func, expr1, expr2 } => {
            is_compilable_binary_func(func) && is_compilable(expr1) && is_compilable(expr2)
        }
        _ => false,
    }
}

/// Returns `true` if the binary function is supported for WASM compilation.
fn is_compilable_binary_func(func: &mz_expr::BinaryFunc) -> bool {
    use mz_expr::BinaryFunc;
    matches!(func, BinaryFunc::AddInt64(_))
}

/// Collects the set of input column indices referenced by a compilable expression.
pub fn referenced_columns(expr: &MirScalarExpr) -> Vec<usize> {
    let mut cols = Vec::new();
    collect_columns(expr, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn collect_columns(expr: &MirScalarExpr, out: &mut Vec<usize>) {
    match expr {
        MirScalarExpr::Column(idx, _) => out.push(*idx),
        MirScalarExpr::Literal(_, _) => {}
        MirScalarExpr::CallBinary { expr1, expr2, .. } => {
            collect_columns(expr1, out);
            collect_columns(expr2, out);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{BinaryFunc, MirScalarExpr};
    use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row};

    fn col(idx: usize) -> MirScalarExpr {
        MirScalarExpr::Column(idx, Default::default())
    }

    fn lit_i64(v: i64) -> MirScalarExpr {
        MirScalarExpr::Literal(
            Ok(Row::pack_slice(&[Datum::Int64(v)])),
            ReprColumnType {
                scalar_type: ReprScalarType::Int64,
                nullable: false,
            },
        )
    }

    fn add_i64(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallBinary {
            func: BinaryFunc::AddInt64(mz_expr::func::AddInt64),
            expr1: Box::new(a),
            expr2: Box::new(b),
        }
    }

    #[mz_ore::test]
    fn test_column_is_compilable() {
        assert!(is_compilable(&col(0)));
        assert!(is_compilable(&col(42)));
    }

    #[mz_ore::test]
    fn test_literal_int64_is_compilable() {
        assert!(is_compilable(&lit_i64(42)));
    }

    #[mz_ore::test]
    fn test_add_int64_is_compilable() {
        assert!(is_compilable(&add_i64(col(0), col(1))));
        assert!(is_compilable(&add_i64(col(0), lit_i64(1))));
        assert!(is_compilable(&add_i64(lit_i64(1), lit_i64(2))));
    }

    #[mz_ore::test]
    fn test_nested_add_is_compilable() {
        let expr = add_i64(add_i64(col(0), col(1)), lit_i64(10));
        assert!(is_compilable(&expr));
    }

    #[mz_ore::test]
    fn test_unsupported_not_compilable() {
        // CallUnary is not supported
        let expr = MirScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce),
            exprs: vec![col(0)],
        };
        assert!(!is_compilable(&expr));
    }

    #[mz_ore::test]
    fn test_referenced_columns() {
        let expr = add_i64(col(0), add_i64(col(2), col(0)));
        assert_eq!(referenced_columns(&expr), vec![0, 2]);
    }
}
