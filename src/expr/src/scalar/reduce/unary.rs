// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Post-order rewrites for `CallUnary` nodes.

use mz_repr::{ReprColumnType, RowArena};

use crate::scalar::func::{self, UnaryFunc, VariadicFunc};
use crate::{Eval, MirScalarExpr};

pub(super) fn reduce_call_unary(
    e: &mut MirScalarExpr,
    column_types: &[ReprColumnType],
    temp_storage: &RowArena,
) {
    let MirScalarExpr::CallUnary { func, expr } = e else {
        unreachable!()
    };

    if expr.is_literal() && *func != UnaryFunc::Panic(func::Panic) {
        *e = MirScalarExpr::literal(e.eval(&[], temp_storage), e.typ(column_types).scalar_type);
        return;
    }

    // `f(g(x))` → `x` when `f` is a left inverse of `g`. Mind the contract:
    // `g.inverse()` does not always return a mathematical inverse. Per its
    // documentation it returns a left inverse — what this elimination needs —
    // exactly when *`g` itself* preserves uniqueness; when only the *returned
    // function* preserves uniqueness it is merely a right inverse (`g(f(y)) =
    // y`, useful for moving casts across equalities, useless here). Hence the
    // `preserves_uniqueness` check below is on `g`, and is load-bearing.
    //
    // Eliding the calls must also not change error or null behavior, so both
    // functions must be infallible and propagate nulls. (This is what
    // excludes e.g. `-(-x)` on integers, whose inner negation errors on the
    // minimum value, and `int8(int4(x))`-style widenings, whose outer
    // narrowing can error — while admitting `NOT(NOT(x))`, `~(~x)`,
    // `reverse(reverse(x))`, numeric `-(-x)`, and infallible bijective cast
    // roundtrips such as `bool::int4::bool`.)
    if let MirScalarExpr::CallUnary {
        func: inner_func,
        expr: inner_expr,
    } = &mut **expr
    {
        if inner_func.inverse().as_ref() == Some(func)
            && inner_func.preserves_uniqueness()
            && !inner_func.could_error()
            && !func.could_error()
            && inner_func.propagates_nulls()
            && func.propagates_nulls()
        {
            let inner = inner_expr.take();
            *e = inner;
            return;
        }
    }

    // `RecordGet(i)(RecordCreate(args))` → `args[i]`.
    if let UnaryFunc::RecordGet(func::RecordGet(i)) = *func {
        if let MirScalarExpr::CallVariadic {
            func: VariadicFunc::RecordCreate(..),
            exprs,
        } = &mut **expr
        {
            *e = exprs.swap_remove(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::ReprScalarType;

    use crate::MirScalarExpr;
    use crate::scalar::func;

    #[mz_ore::test]
    fn involution_folds() {
        // `f(g(x))` folds to `x` when `g` names `f` as its inverse and both
        // calls are infallible.
        let col = || MirScalarExpr::column(0);

        let bool_col = [ReprScalarType::Bool.nullable(true)];
        let mut e = col().call_unary(func::Not).call_unary(func::Not);
        e.reduce(&bool_col);
        assert_eq!(e, col());

        let int32 = [ReprScalarType::Int.nullable(true)];
        let mut e = col()
            .call_unary(func::BitNotInt32)
            .call_unary(func::BitNotInt32);
        e.reduce(&int32);
        assert_eq!(e, col());

        let string = [ReprScalarType::String.nullable(true)];
        let mut e = col().call_unary(func::Reverse).call_unary(func::Reverse);
        e.reduce(&string);
        assert_eq!(e, col());

        // Numeric negation is infallible, so its double application folds.
        let numeric = [ReprScalarType::Numeric {}.nullable(true)];
        let mut e = col()
            .call_unary(func::NegNumeric)
            .call_unary(func::NegNumeric);
        e.reduce(&numeric);
        assert_eq!(e, col());

        // An infallible bijective cast roundtrip folds: `int4(bool)` declares
        // `bool(int4)` as its (left) inverse, and both directions are total.
        let bool_col = [ReprScalarType::Bool.nullable(true)];
        let mut e = col()
            .call_unary(func::CastBoolToInt32)
            .call_unary(func::CastInt32ToBool);
        e.reduce(&bool_col);
        assert_eq!(e, col());

        // Integer negation errors on the minimum value, so eliding the double
        // negation would suppress that error; it must not fold.
        let int64 = [ReprScalarType::Int.nullable(true)];
        let mut e = col().call_unary(func::NegInt64).call_unary(func::NegInt64);
        e.reduce(&int64);
        assert_ne!(e, col());

        // A widening cast roundtrip stays: the outer narrowing can error in
        // general, and the gate does not reason about images.
        let int32 = [ReprScalarType::Int.nullable(true)];
        let mut e = col()
            .call_unary(func::CastInt32ToInt64)
            .call_unary(func::CastInt64ToInt32);
        e.reduce(&int32);
        assert_ne!(e, col());
    }
}
