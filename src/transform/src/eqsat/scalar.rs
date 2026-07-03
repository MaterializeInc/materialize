// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A contained equality-saturation canonicalizer for scalar expressions.
//!
//! Phase 0 is the structural skeleton: a scalar e-graph ([`egraph`]) with a
//! faithful lower ([`lower`]) and raise ([`raise`]) bridge, and no rewrite
//! rules. With zero rules the canonicalizer is the identity, which is what the
//! round-trip test asserts. Later phases add analyses, rules, a form-targeted
//! cost, and the `CanonicalizeMfp` boundary.
//!
//! See `doc/developer/design/20260624_eqsat/20260625_eqsat_scalar_expressions.md`.

pub mod analysis;
pub mod egraph;
pub mod lang;
pub mod lower;
pub mod node;
pub mod raise;
pub mod rules;

use mz_expr::MirScalarExpr;
use mz_repr::ReprColumnType;

use crate::eqsat::scalar::egraph::ScalarEGraph;
use crate::eqsat::scalar_saturate::canonicalize_combined;

/// Canonicalize a scalar expression by equality saturation.
///
/// `col_types` are the column types of the relation `expr` is evaluated
/// against, indexed by column position. Rules that build a typed literal (a
/// null or an error) read them through the e-graph to compute a call's result
/// type. The type-agnostic rules (const-fold, boolean, NOT, If) never consult
/// them, so passing `&[]` is sound whenever only those rules can fire.
pub fn canonicalize(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> MirScalarExpr {
    let mut eg = ScalarEGraph::new();
    eg.data_mut().col_types = col_types.to_vec();
    let root = lower::lower(&mut eg, expr);
    egraph::saturate(&mut eg);
    raise::raise(&eg, root)
}

/// Canonicalize a filter's `predicates`, selecting the per-predicate scalar
/// canonicalizer with `enable_eqsat_scalar`.
///
/// When set, step 1 of [`mz_expr::canonicalize::canonicalize_predicates`] is
/// performed by the equality-saturation scalar canonicalizer
/// ([`canonicalize_combined`]) instead of `MirScalarExpr::reduce`. When clear,
/// this delegates to the unmodified
/// [`mz_expr::canonicalize::canonicalize_predicates`], so the flag-off path is
/// byte-identical to calling that function directly.
///
/// This is the single injection point through which the `mz-transform` callers
/// reach the `mz-expr` predicate canonicalizer, since `mz-expr` cannot depend on
/// the eqsat engine.
pub(crate) fn canonicalize_predicates(
    predicates: &mut Vec<MirScalarExpr>,
    col_types: &[ReprColumnType],
    enable_eqsat_scalar: bool,
) {
    if enable_eqsat_scalar {
        mz_expr::canonicalize::canonicalize_predicates_with(
            predicates,
            col_types,
            Some(&|e: &mut MirScalarExpr, ct: &[ReprColumnType]| {
                *e = canonicalize_combined(e, ct);
            }),
        );
    } else {
        mz_expr::canonicalize::canonicalize_predicates(predicates, col_types);
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::{
        BinaryFunc, EvalError, MirScalarExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc,
    };
    use mz_repr::{Datum, ReprScalarType};

    use super::*;

    fn col(i: usize) -> MirScalarExpr {
        MirScalarExpr::column(i)
    }

    fn lit_int(v: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64)
    }

    fn lit_bool(b: bool) -> MirScalarExpr {
        MirScalarExpr::literal_ok(
            if b { Datum::True } else { Datum::False },
            ReprScalarType::Bool,
        )
    }

    fn add64() -> BinaryFunc {
        BinaryFunc::AddInt64(mz_expr::func::AddInt64)
    }

    fn not() -> UnaryFunc {
        UnaryFunc::Not(mz_expr::func::Not)
    }

    fn and() -> VariadicFunc {
        VariadicFunc::And(mz_expr::func::variadic::And)
    }

    fn or() -> VariadicFunc {
        VariadicFunc::Or(mz_expr::func::variadic::Or)
    }

    /// Round-trip `expr` through lower then raise (no rules) and assert it is
    /// returned unchanged.
    ///
    /// With zero rules the faithful invariant is identity, `raise(lower(e)) == e`,
    /// not equality against `reduce`. The plan wrote the gate against `reduce`,
    /// but that differential belongs to Phase 1: `reduce` rewrites the term,
    /// while a no-rule bridge must reproduce it exactly. We assert the stronger
    /// identity.
    fn assert_round_trip(expr: MirScalarExpr) {
        let mut eg = ScalarEGraph::new();
        let root = lower::lower(&mut eg, &expr);
        egraph::saturate(&mut eg);
        let raised = raise::raise(&eg, root);
        assert_eq!(raised, expr, "round-trip changed the expression");
    }

    #[mz_ore::test]
    fn round_trip_column() {
        assert_round_trip(col(0));
        assert_round_trip(col(7));
    }

    #[mz_ore::test]
    fn round_trip_literal() {
        assert_round_trip(lit_int(42));
        assert_round_trip(lit_bool(true));
        assert_round_trip(MirScalarExpr::literal_null(ReprScalarType::Int64));
    }

    #[mz_ore::test]
    fn round_trip_error_literal() {
        // An error literal is the case the `Literal(Result<Row, EvalError>, _)`
        // deviation exists for: the `Err` arm must survive lower then raise.
        assert_round_trip(MirScalarExpr::literal(
            Err(EvalError::DivisionByZero),
            ReprScalarType::Int64,
        ));
    }

    #[mz_ore::test]
    fn round_trip_unmaterializable() {
        assert_round_trip(MirScalarExpr::CallUnmaterializable(
            UnmaterializableFunc::MzNow,
        ));
    }

    #[mz_ore::test]
    fn round_trip_unary() {
        // NOT(column 0)
        assert_round_trip(col(0).call_unary(not()));
    }

    #[mz_ore::test]
    fn round_trip_binary() {
        // column 0 + column 1
        assert_round_trip(col(0).call_binary(col(1), add64()));
    }

    #[mz_ore::test]
    fn round_trip_variadic() {
        // AND(column 0, column 1, column 2)
        assert_round_trip(MirScalarExpr::CallVariadic {
            func: and(),
            exprs: vec![col(0), col(1), col(2)],
        });
        // OR(column 0, column 1, column 2)
        assert_round_trip(MirScalarExpr::CallVariadic {
            func: or(),
            exprs: vec![col(0), col(1), col(2)],
        });
    }

    #[mz_ore::test]
    fn round_trip_if() {
        // if (column 0) then 1 else 2
        assert_round_trip(MirScalarExpr::If {
            cond: Box::new(col(0)),
            then: Box::new(lit_int(1)),
            els: Box::new(lit_int(2)),
        });
    }

    #[mz_ore::test]
    fn round_trip_nested_mix() {
        // if AND(c1, NOT(c0)) then (c2 + 5) else if c3 then 0 else (c2 + 5).
        //
        // Operands of And are given in sorted order (Column(1) < CallUnary(Not,
        // Column(0)) by MirScalarExpr::Ord) so the round-trip remains the
        // identity: extraction sorts And/Or operands, so the input must already
        // be sorted for raise(lower(e)) == e to hold.
        let inner_then = col(2).call_binary(lit_int(5), add64());
        let inner_if = MirScalarExpr::If {
            cond: Box::new(col(3)),
            then: Box::new(lit_int(0)),
            // Reuse the same subterm to exercise hash-cons sharing on raise.
            els: Box::new(col(2).call_binary(lit_int(5), add64())),
        };
        let cond = MirScalarExpr::CallVariadic {
            func: and(),
            exprs: vec![col(1), col(0).call_unary(not())],
        };
        assert_round_trip(MirScalarExpr::If {
            cond: Box::new(cond),
            then: Box::new(inner_then),
            els: Box::new(inner_if),
        });
    }

    #[mz_ore::test]
    fn canonicalize_is_identity() {
        // The public entry is identity in Phase 0 (no rules).
        // `&[]` is sound: a non-foldable `col + lit` triggers no typed-literal
        // rule, so no column type is ever consulted.
        let expr = col(0).call_binary(lit_int(3), add64());
        assert_eq!(canonicalize(&expr, &[]), expr);
    }
}
