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
//! Compilable expressions:
//! * `Column(idx)` — reads from an input column
//! * `Literal(Ok(row), _)` where the datum is `Int64`, `Null`, `True`, or `False`
//! * `CallBinary` with a supported Int64 binary function (add, sub, mul, div, mod,
//!   bitand, bitor, bitxor) where both children are compilable
//! * `CallBinary` with a comparison function (Eq, NotEq, Lt, Lte, Gt, Gte) where both
//!   children infer to Int64 and are recursively compilable
//! * `CallUnary` with a supported Int64 unary function (neg, bitnot, abs)
//!   where the child is compilable
//! * `CallUnary` with `Not` where the child infers to Bool and is compilable
//! * `CallUnary` with `IsNull` where the child is compilable (any type)
//! * `CallUnary` with `IsTrue`/`IsFalse` where the child infers to Bool and is compilable
//! * `CallVariadic` with `And`/`Or` where all children infer to Bool and are compilable

use mz_expr::{MapFilterProject, MirScalarExpr};
use mz_repr::{Datum, SqlScalarType};

/// Infers the output scalar type of an expression, given column types.
///
/// Returns `None` if the type cannot be determined (unsupported expression).
pub fn infer_type(expr: &MirScalarExpr, input_types: &[SqlScalarType]) -> Option<SqlScalarType> {
    match expr {
        MirScalarExpr::Column(idx, _) => input_types.get(*idx).cloned(),
        MirScalarExpr::Literal(Ok(_row), col_type) => {
            Some(SqlScalarType::from_repr(&col_type.scalar_type))
        }
        MirScalarExpr::Literal(Err(_), _) => None,
        MirScalarExpr::CallBinary { func, .. } => {
            use mz_expr::BinaryFunc;
            match func {
                // Int64 arithmetic and bitwise ops produce Int64.
                BinaryFunc::AddInt64(_)
                | BinaryFunc::SubInt64(_)
                | BinaryFunc::MulInt64(_)
                | BinaryFunc::DivInt64(_)
                | BinaryFunc::ModInt64(_)
                | BinaryFunc::BitAndInt64(_)
                | BinaryFunc::BitOrInt64(_)
                | BinaryFunc::BitXorInt64(_) => Some(SqlScalarType::Int64),
                // Comparison ops produce Bool.
                BinaryFunc::Eq(_)
                | BinaryFunc::NotEq(_)
                | BinaryFunc::Lt(_)
                | BinaryFunc::Lte(_)
                | BinaryFunc::Gt(_)
                | BinaryFunc::Gte(_) => Some(SqlScalarType::Bool),
                _ => None,
            }
        }
        MirScalarExpr::CallUnary { func, .. } => {
            use mz_expr::UnaryFunc;
            match func {
                // Int64 unary ops produce Int64.
                UnaryFunc::NegInt64(_) | UnaryFunc::BitNotInt64(_) | UnaryFunc::AbsInt64(_) => {
                    Some(SqlScalarType::Int64)
                }
                // Bool-producing unary ops.
                UnaryFunc::Not(_)
                | UnaryFunc::IsNull(_)
                | UnaryFunc::IsTrue(_)
                | UnaryFunc::IsFalse(_) => Some(SqlScalarType::Bool),
                _ => None,
            }
        }
        MirScalarExpr::CallVariadic { func, .. } => {
            use mz_expr::VariadicFunc;
            match func {
                VariadicFunc::And(_) | VariadicFunc::Or(_) => Some(SqlScalarType::Bool),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Returns `true` if the given expression tree can be compiled to WASM.
///
/// Falls back to the interpreter for anything this returns `false` for.
/// `input_types` provides the scalar type of each input column, used
/// for type-checking generic comparison operators.
pub fn is_compilable(expr: &MirScalarExpr, input_types: &[SqlScalarType]) -> bool {
    match expr {
        MirScalarExpr::Column(_, _) => true,
        MirScalarExpr::Literal(Ok(row), _col_type) => {
            let datum = row.unpack_first();
            matches!(
                datum,
                Datum::Int64(_) | Datum::Null | Datum::True | Datum::False
            )
        }
        MirScalarExpr::Literal(Err(_), _) => false,
        MirScalarExpr::CallBinary { func, expr1, expr2 } => {
            if is_int64_binary_func(func) {
                is_compilable(expr1, input_types) && is_compilable(expr2, input_types)
            } else if is_comparison_func(func) {
                // Generic comparison ops: only compilable when both operands are Int64.
                infer_type(expr1, input_types) == Some(SqlScalarType::Int64)
                    && infer_type(expr2, input_types) == Some(SqlScalarType::Int64)
                    && is_compilable(expr1, input_types)
                    && is_compilable(expr2, input_types)
            } else {
                false
            }
        }
        MirScalarExpr::CallUnary { func, expr } => {
            if is_int64_unary_func(func) {
                is_compilable(expr, input_types)
            } else if is_bool_unary_func(func) {
                // Not/IsTrue/IsFalse: compilable iff child infers to Bool and is compilable.
                infer_type(expr, input_types) == Some(SqlScalarType::Bool)
                    && is_compilable(expr, input_types)
            } else if is_null_consuming_unary_func(func) {
                // IsNull: compilable for any child type.
                is_compilable(expr, input_types)
            } else {
                false
            }
        }
        MirScalarExpr::CallVariadic { func, exprs } => {
            if is_bool_variadic_func(func) {
                // And/Or: compilable when all children infer to Bool and are compilable.
                exprs.iter().all(|e| {
                    infer_type(e, input_types) == Some(SqlScalarType::Bool)
                        && is_compilable(e, input_types)
                })
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Returns `true` if the binary function is a typed Int64 operation.
fn is_int64_binary_func(func: &mz_expr::BinaryFunc) -> bool {
    use mz_expr::BinaryFunc;
    matches!(
        func,
        BinaryFunc::AddInt64(_)
            | BinaryFunc::SubInt64(_)
            | BinaryFunc::MulInt64(_)
            | BinaryFunc::DivInt64(_)
            | BinaryFunc::ModInt64(_)
            | BinaryFunc::BitAndInt64(_)
            | BinaryFunc::BitOrInt64(_)
            | BinaryFunc::BitXorInt64(_)
    )
}

/// Returns `true` if the binary function is a generic comparison operator.
fn is_comparison_func(func: &mz_expr::BinaryFunc) -> bool {
    use mz_expr::BinaryFunc;
    matches!(
        func,
        BinaryFunc::Eq(_)
            | BinaryFunc::NotEq(_)
            | BinaryFunc::Lt(_)
            | BinaryFunc::Lte(_)
            | BinaryFunc::Gt(_)
            | BinaryFunc::Gte(_)
    )
}

/// Returns `true` if the unary function is a typed Int64 operation.
fn is_int64_unary_func(func: &mz_expr::UnaryFunc) -> bool {
    use mz_expr::UnaryFunc;
    matches!(
        func,
        UnaryFunc::NegInt64(_) | UnaryFunc::BitNotInt64(_) | UnaryFunc::AbsInt64(_)
    )
}

/// Returns `true` if the unary function is a Bool-to-Bool operation.
fn is_bool_unary_func(func: &mz_expr::UnaryFunc) -> bool {
    use mz_expr::UnaryFunc;
    matches!(
        func,
        UnaryFunc::Not(_) | UnaryFunc::IsTrue(_) | UnaryFunc::IsFalse(_)
    )
}

/// Returns `true` if the unary function is a null-consuming operation (any input type).
fn is_null_consuming_unary_func(func: &mz_expr::UnaryFunc) -> bool {
    use mz_expr::UnaryFunc;
    matches!(func, UnaryFunc::IsNull(_))
}

/// Returns `true` if the variadic function is a Bool operation (And/Or).
fn is_bool_variadic_func(func: &mz_expr::VariadicFunc) -> bool {
    use mz_expr::VariadicFunc;
    matches!(func, VariadicFunc::And(_) | VariadicFunc::Or(_))
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
        MirScalarExpr::CallUnary { expr, .. } => {
            collect_columns(expr, out);
        }
        MirScalarExpr::CallVariadic { exprs, .. } => {
            for e in exprs {
                collect_columns(e, out);
            }
        }
        _ => {}
    }
}

/// Infers input column types from the operations used in an MFP.
///
/// Scans all expressions and predicates to find typed operations that constrain
/// column types. For example, if a column appears as an operand of `AddInt64`,
/// it must be `Int64`. If it appears as an operand of `Not`, it must be `Bool`.
///
/// Returns a `Vec<SqlScalarType>` with `input_arity` entries. Columns that
/// cannot be inferred default to `Int64` for backwards compatibility.
pub fn infer_input_types_from_mfp(mfp: &MapFilterProject) -> Vec<SqlScalarType> {
    let mut types: Vec<Option<SqlScalarType>> = vec![None; mfp.input_arity];

    // Walk all expressions and predicates.
    for expr in &mfp.expressions {
        infer_column_types_from_expr(expr, &mut types);
    }
    for (_support, predicate) in &mfp.predicates {
        infer_column_types_from_expr(predicate, &mut types);
    }

    // Default unknown columns to Int64.
    types
        .into_iter()
        .map(|t| t.unwrap_or(SqlScalarType::Int64))
        .collect()
}

/// Recursively walks an expression, setting column types based on typed operations.
fn infer_column_types_from_expr(expr: &MirScalarExpr, types: &mut Vec<Option<SqlScalarType>>) {
    match expr {
        MirScalarExpr::CallBinary { func, expr1, expr2 } => {
            if is_int64_binary_func(func) {
                // Both operands must be Int64.
                set_column_type(expr1, SqlScalarType::Int64, types);
                set_column_type(expr2, SqlScalarType::Int64, types);
            } else if is_comparison_func(func) {
                // Propagate: if one side has a known type and the other is a column,
                // propagate the type. Both sides are typically Int64 for compilable comparisons.
                propagate_comparison_types(expr1, expr2, types);
            }
            infer_column_types_from_expr(expr1, types);
            infer_column_types_from_expr(expr2, types);
        }
        MirScalarExpr::CallUnary { func, expr } => {
            use mz_expr::UnaryFunc;
            match func {
                UnaryFunc::NegInt64(_) | UnaryFunc::BitNotInt64(_) | UnaryFunc::AbsInt64(_) => {
                    set_column_type(expr, SqlScalarType::Int64, types);
                }
                UnaryFunc::Not(_) | UnaryFunc::IsTrue(_) | UnaryFunc::IsFalse(_) => {
                    set_column_type(expr, SqlScalarType::Bool, types);
                }
                _ => {}
            }
            infer_column_types_from_expr(expr, types);
        }
        MirScalarExpr::CallVariadic { func, exprs } => {
            use mz_expr::VariadicFunc;
            if matches!(func, VariadicFunc::And(_) | VariadicFunc::Or(_)) {
                for e in exprs {
                    set_column_type(e, SqlScalarType::Bool, types);
                }
            }
            for e in exprs {
                infer_column_types_from_expr(e, types);
            }
        }
        _ => {}
    }
}

/// Sets the type for a column reference if the expression is a direct column ref.
fn set_column_type(expr: &MirScalarExpr, ty: SqlScalarType, types: &mut [Option<SqlScalarType>]) {
    if let MirScalarExpr::Column(idx, _) = expr {
        if *idx < types.len() {
            types[*idx] = Some(ty);
        }
    }
}

/// For a comparison, propagate types between the two operands.
/// If one side is a typed operation (e.g., AddInt64) and the other is a column,
/// propagate the inferred type.
fn propagate_comparison_types(
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    types: &mut [Option<SqlScalarType>],
) {
    // Use a simple Vec<SqlScalarType> snapshot for infer_type calls.
    let snapshot: Vec<SqlScalarType> = types
        .iter()
        .map(|t| t.clone().unwrap_or(SqlScalarType::Int64))
        .collect();

    let ty1 = infer_type(expr1, &snapshot);
    let ty2 = infer_type(expr2, &snapshot);

    if let Some(ty) = ty1 {
        set_column_type(expr2, ty, types);
    }
    if let Some(ty) = ty2 {
        set_column_type(expr1, ty, types);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{BinaryFunc, MirScalarExpr, UnaryFunc};
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

    fn lit_bool(v: bool) -> MirScalarExpr {
        MirScalarExpr::Literal(
            Ok(Row::pack_slice(&[if v {
                Datum::True
            } else {
                Datum::False
            }])),
            ReprColumnType {
                scalar_type: ReprScalarType::Bool,
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

    fn eq(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallBinary {
            func: BinaryFunc::Eq(mz_expr::func::Eq),
            expr1: Box::new(a),
            expr2: Box::new(b),
        }
    }

    fn lt(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallBinary {
            func: BinaryFunc::Lt(mz_expr::func::Lt),
            expr1: Box::new(a),
            expr2: Box::new(b),
        }
    }

    fn not(a: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallUnary {
            func: UnaryFunc::Not(mz_expr::func::Not),
            expr: Box::new(a),
        }
    }

    #[mz_ore::test]
    fn test_column_is_compilable() {
        let types = vec![SqlScalarType::Int64];
        assert!(is_compilable(&col(0), &types));
        assert!(is_compilable(&col(42), &[]));
    }

    #[mz_ore::test]
    fn test_literal_int64_is_compilable() {
        assert!(is_compilable(&lit_i64(42), &[]));
    }

    #[mz_ore::test]
    fn test_literal_bool_is_compilable() {
        assert!(is_compilable(&lit_bool(true), &[]));
        assert!(is_compilable(&lit_bool(false), &[]));
    }

    #[mz_ore::test]
    fn test_add_int64_is_compilable() {
        let types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        assert!(is_compilable(&add_i64(col(0), col(1)), &types));
        assert!(is_compilable(&add_i64(col(0), lit_i64(1)), &types));
        assert!(is_compilable(&add_i64(lit_i64(1), lit_i64(2)), &types));
    }

    #[mz_ore::test]
    fn test_nested_add_is_compilable() {
        let types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        let expr = add_i64(add_i64(col(0), col(1)), lit_i64(10));
        assert!(is_compilable(&expr, &types));
    }

    #[mz_ore::test]
    fn test_comparison_int64_is_compilable() {
        let types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        assert!(is_compilable(&eq(col(0), col(1)), &types));
        assert!(is_compilable(&lt(col(0), lit_i64(42)), &types));
    }

    #[mz_ore::test]
    fn test_comparison_non_int64_not_compilable() {
        // Without input types, columns can't be inferred.
        assert!(!is_compilable(&eq(col(0), col(1)), &[]));
        // Bool columns aren't supported for comparison.
        let types = vec![SqlScalarType::Bool, SqlScalarType::Bool];
        assert!(!is_compilable(&eq(col(0), col(1)), &types));
    }

    #[mz_ore::test]
    fn test_not_bool_is_compilable() {
        let types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        // not(col0 < col1) — child is comparison (Bool), so Not is compilable.
        assert!(is_compilable(&not(lt(col(0), col(1))), &types));
        // not(col0) where col0 is Bool.
        let bool_types = vec![SqlScalarType::Bool];
        assert!(is_compilable(&not(col(0)), &bool_types));
    }

    #[mz_ore::test]
    fn test_not_non_bool_not_compilable() {
        let types = vec![SqlScalarType::Int64];
        // not(col0) where col0 is Int64 — shouldn't compile.
        assert!(!is_compilable(&not(col(0)), &types));
    }

    #[mz_ore::test]
    fn test_unsupported_not_compilable() {
        // Coalesce is not supported.
        let expr = MirScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce),
            exprs: vec![col(0)],
        };
        assert!(!is_compilable(&expr, &[]));
    }

    // --- And/Or compilability ---

    fn and_expr(children: Vec<MirScalarExpr>) -> MirScalarExpr {
        MirScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: children,
        }
    }

    fn or_expr(children: Vec<MirScalarExpr>) -> MirScalarExpr {
        MirScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: children,
        }
    }

    fn is_null(a: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallUnary {
            func: UnaryFunc::IsNull(mz_expr::func::IsNull),
            expr: Box::new(a),
        }
    }

    fn is_true(a: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallUnary {
            func: UnaryFunc::IsTrue(mz_expr::func::IsTrue),
            expr: Box::new(a),
        }
    }

    fn is_false(a: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallUnary {
            func: UnaryFunc::IsFalse(mz_expr::func::IsFalse),
            expr: Box::new(a),
        }
    }

    #[mz_ore::test]
    fn test_and_is_compilable() {
        let types = vec![SqlScalarType::Bool, SqlScalarType::Bool];
        assert!(is_compilable(&and_expr(vec![col(0), col(1)]), &types));
        // And with comparison children.
        let int_types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        assert!(is_compilable(
            &and_expr(vec![lt(col(0), col(1)), eq(col(0), lit_i64(0))]),
            &int_types
        ));
    }

    #[mz_ore::test]
    fn test_or_is_compilable() {
        let types = vec![SqlScalarType::Bool, SqlScalarType::Bool];
        assert!(is_compilable(&or_expr(vec![col(0), col(1)]), &types));
    }

    #[mz_ore::test]
    fn test_and_non_bool_not_compilable() {
        // And with Int64 children — not compilable.
        let types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        assert!(!is_compilable(&and_expr(vec![col(0), col(1)]), &types));
    }

    #[mz_ore::test]
    fn test_is_null_is_compilable() {
        let int_types = vec![SqlScalarType::Int64];
        assert!(is_compilable(&is_null(col(0)), &int_types));
        let bool_types = vec![SqlScalarType::Bool];
        assert!(is_compilable(&is_null(col(0)), &bool_types));
        // IsNull of a nested expression.
        assert!(is_compilable(
            &is_null(add_i64(col(0), lit_i64(1))),
            &int_types
        ));
    }

    #[mz_ore::test]
    fn test_is_true_is_compilable() {
        let bool_types = vec![SqlScalarType::Bool];
        assert!(is_compilable(&is_true(col(0)), &bool_types));
        // IsTrue of comparison.
        let int_types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        assert!(is_compilable(&is_true(lt(col(0), col(1))), &int_types));
    }

    #[mz_ore::test]
    fn test_is_false_is_compilable() {
        let bool_types = vec![SqlScalarType::Bool];
        assert!(is_compilable(&is_false(col(0)), &bool_types));
    }

    #[mz_ore::test]
    fn test_is_true_non_bool_not_compilable() {
        // IsTrue on Int64 column — not compilable (child must be Bool).
        let types = vec![SqlScalarType::Int64];
        assert!(!is_compilable(&is_true(col(0)), &types));
    }

    #[mz_ore::test]
    fn test_infer_type_and_or() {
        let types = vec![SqlScalarType::Bool, SqlScalarType::Bool];
        assert_eq!(
            infer_type(&and_expr(vec![col(0), col(1)]), &types),
            Some(SqlScalarType::Bool)
        );
        assert_eq!(
            infer_type(&or_expr(vec![col(0), col(1)]), &types),
            Some(SqlScalarType::Bool)
        );
    }

    #[mz_ore::test]
    fn test_infer_type_is_null_true_false() {
        let types = vec![SqlScalarType::Int64];
        assert_eq!(
            infer_type(&is_null(col(0)), &types),
            Some(SqlScalarType::Bool)
        );
        let bool_types = vec![SqlScalarType::Bool];
        assert_eq!(
            infer_type(&is_true(col(0)), &bool_types),
            Some(SqlScalarType::Bool)
        );
        assert_eq!(
            infer_type(&is_false(col(0)), &bool_types),
            Some(SqlScalarType::Bool)
        );
    }

    // --- Input type inference from MFP ---

    #[mz_ore::test]
    fn test_infer_input_types_from_mfp_simple_add() {
        // MFP: col(0) + col(1), input_arity=2
        let mfp = MapFilterProject {
            expressions: vec![add_i64(col(0), col(1))],
            predicates: vec![],
            projection: vec![0, 1, 2],
            input_arity: 2,
        };
        let types = infer_input_types_from_mfp(&mfp);
        assert_eq!(types, vec![SqlScalarType::Int64, SqlScalarType::Int64]);
    }

    #[mz_ore::test]
    fn test_infer_input_types_from_mfp_comparison_predicate() {
        // MFP: predicate col(0) < 42, input_arity=2
        let mfp = MapFilterProject {
            expressions: vec![],
            predicates: vec![(2, lt(col(0), lit_i64(42)))],
            projection: vec![0, 1],
            input_arity: 2,
        };
        let types = infer_input_types_from_mfp(&mfp);
        // col(0) is compared against a literal Int64, so should be inferred as Int64.
        assert_eq!(types[0], SqlScalarType::Int64);
    }

    #[mz_ore::test]
    fn test_infer_input_types_from_mfp_bool_not() {
        // MFP: predicate NOT(col(0)), input_arity=1
        let mfp = MapFilterProject {
            expressions: vec![],
            predicates: vec![(1, not(col(0)))],
            projection: vec![0],
            input_arity: 1,
        };
        let types = infer_input_types_from_mfp(&mfp);
        assert_eq!(types[0], SqlScalarType::Bool);
    }

    #[mz_ore::test]
    fn test_referenced_columns() {
        let expr = add_i64(col(0), add_i64(col(2), col(0)));
        assert_eq!(referenced_columns(&expr), vec![0, 2]);
    }

    #[mz_ore::test]
    fn test_referenced_columns_variadic() {
        let expr = and_expr(vec![lt(col(0), col(1)), eq(col(2), col(0))]);
        assert_eq!(referenced_columns(&expr), vec![0, 1, 2]);
    }

    #[mz_ore::test]
    fn test_infer_type_column() {
        let types = vec![SqlScalarType::Int64, SqlScalarType::Bool];
        assert_eq!(infer_type(&col(0), &types), Some(SqlScalarType::Int64));
        assert_eq!(infer_type(&col(1), &types), Some(SqlScalarType::Bool));
        assert_eq!(infer_type(&col(5), &types), None);
    }

    #[mz_ore::test]
    fn test_infer_type_literal() {
        assert_eq!(infer_type(&lit_i64(42), &[]), Some(SqlScalarType::Int64));
        assert_eq!(infer_type(&lit_bool(true), &[]), Some(SqlScalarType::Bool));
    }

    #[mz_ore::test]
    fn test_infer_type_binary() {
        let types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        assert_eq!(
            infer_type(&add_i64(col(0), col(1)), &types),
            Some(SqlScalarType::Int64)
        );
        assert_eq!(
            infer_type(&eq(col(0), col(1)), &types),
            Some(SqlScalarType::Bool)
        );
    }

    #[mz_ore::test]
    fn test_infer_type_not() {
        let types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        assert_eq!(
            infer_type(&not(lt(col(0), col(1))), &types),
            Some(SqlScalarType::Bool)
        );
    }
}
