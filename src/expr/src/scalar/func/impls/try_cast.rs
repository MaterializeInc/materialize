// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_repr::{Datum, RowArena, SqlColumnType};
use serde::{Deserialize, Serialize};

use crate::scalar::func::{LazyUnaryFunc, UnaryFunc};
use crate::{Eval, EvalError, MirScalarExpr};

/// What a cast does when the value cannot be converted to the target type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CastFailureMode {
    /// The cast raises an error.
    Error,
    /// The cast evaluates to `NULL`. Each cast function is wrapped in
    /// [`TryCast`], so errors raised while evaluating the cast's argument
    /// still propagate.
    NullFallback,
}

/// Wraps a cast function so that its errors become `NULL`.
///
/// The argument is evaluated first, and its errors propagate. Only errors
/// raised by `inner` itself are swallowed. Because the wrapper is strict in its
/// argument and never errors, it is safe for the optimizer to treat it like
/// any other infallible unary function.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
pub struct TryCast<E = MirScalarExpr> {
    pub inner: Box<UnaryFunc<E>>,
}

/// Evaluates to the first column of the row it is given.
///
/// `TryCast::eval` needs to hand an already evaluated datum to `inner`, but
/// `LazyUnaryFunc::eval` takes the argument as an `&'a impl Eval` and returns
/// a `Datum<'a>`, so a value constructed inside `eval` cannot serve as the
/// argument. Instead the datum becomes a one-column row and this `'static`
/// reader stands in for the argument, mirroring how the element casts of
/// `CastList1ToList2` evaluate against a one-column row.
struct FirstColumn;

static FIRST_COLUMN: FirstColumn = FirstColumn;

impl Eval for FirstColumn {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        _temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        Ok(datums[0])
    }

    fn could_error(&self) -> bool {
        false
    }
}

impl<E: Eval> LazyUnaryFunc for TryCast<E> {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a impl Eval,
    ) -> Result<Datum<'a>, EvalError> {
        let a = a.eval(datums, temp_storage)?;
        match self.inner.eval(&[a], temp_storage, &FIRST_COLUMN) {
            Ok(datum) => Ok(datum),
            Err(_) => Ok(Datum::Null),
        }
    }

    fn output_sql_type(&self, input_type: SqlColumnType) -> SqlColumnType {
        self.inner.output_sql_type(input_type).nullable(true)
    }

    fn propagates_nulls(&self) -> bool {
        self.inner.propagates_nulls()
    }

    fn introduces_nulls(&self) -> bool {
        self.inner.could_error() || self.inner.introduces_nulls()
    }

    fn could_error(&self) -> bool {
        false
    }

    fn preserves_uniqueness(&self) -> bool {
        // Every failing input maps to the same NULL.
        //
        // If there was somehow a _unique_ failing input
        // and no other value produce NULL, this could be true.
        // It's not clear that situation ever arises.
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        // The failing inputs, which may sit anywhere in the domain, map to
        // NULL, so the wrapper is not monotone even when `inner` is.
        //
        // If the domain of failures was somehow monotonic with respect to NULL,
        // this could be true.
        false
    }

    fn is_eliminable_cast(&self) -> bool {
        false
    }
}

impl<E: Eval> UnaryFunc<E> {
    /// Wraps `inner` in [`TryCast`] if it could error, so that its errors
    /// become NULL. A function that cannot error has nothing to fall back
    /// from and is returned as is, keeping its nullability.
    pub fn try_cast(inner: UnaryFunc<E>) -> UnaryFunc<E> {
        if inner.could_error() {
            UnaryFunc::TryCast(TryCast {
                inner: Box::new(inner),
            })
        } else {
            inner
        }
    }
}

impl<E> TryCast<E> {
    /// Rebuilds this function with any expressions stored in `inner` converted
    /// to `E2`.
    pub fn try_map_expr<'a, E2: TryFrom<&'a E>>(&'a self) -> Result<TryCast<E2>, E2::Error> {
        Ok(TryCast {
            inner: Box::new(self.inner.try_map_expr()?),
        })
    }

    /// Rebuilds this function with any expressions stored in `inner` converted
    /// to `E2`.
    pub fn map_expr<'a, E2: From<&'a E>>(&'a self) -> TryCast<E2> {
        TryCast {
            inner: Box::new(self.inner.map_expr()),
        }
    }
}

impl<E> fmt::Display for TryCast<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // The brackets attach the guard to the function rather than to its
        // argument: `try_cast[f](x)` catches errors from `f`, not from `x`.
        write!(f, "try_cast[{}]", self.inner)
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, ReprScalarType, RowArena};

    use crate::scalar::func::{CastInt64ToInt32, CastStringToInt32, CastStringToInt64, TryCast};
    use crate::{Eval, EvalError, MirScalarExpr, UnaryFunc};

    fn try_cast(inner: impl Into<UnaryFunc>) -> UnaryFunc {
        TryCast {
            inner: Box::new(inner.into()),
        }
        .into()
    }

    fn eval(expr: &MirScalarExpr) -> Result<Datum<'_>, EvalError> {
        // The expressions under test are closed, so an empty row suffices.
        let arena = Box::leak(Box::new(RowArena::new()));
        expr.eval(&[], arena)
    }

    fn text(s: &str) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::String(s), ReprScalarType::String)
    }

    #[mz_ore::test]
    fn cast_error_becomes_null() {
        let expr = text("abc").call_unary(try_cast(CastStringToInt32));
        assert_eq!(eval(&expr), Ok(Datum::Null));
    }

    #[mz_ore::test]
    fn success_agrees_with_strict_cast() {
        let lenient = text("42").call_unary(try_cast(CastStringToInt32));
        let strict = text("42").call_unary(CastStringToInt32);
        assert_eq!(eval(&lenient), Ok(Datum::Int32(42)));
        assert_eq!(eval(&lenient), eval(&strict));
    }

    #[mz_ore::test]
    fn null_argument_stays_null() {
        let expr = MirScalarExpr::literal_null(ReprScalarType::String)
            .call_unary(try_cast(CastStringToInt32));
        assert_eq!(eval(&expr), Ok(Datum::Null));
    }

    #[mz_ore::test]
    fn argument_error_propagates() {
        let expr = MirScalarExpr::literal(Err(EvalError::DivisionByZero), ReprScalarType::String)
            .call_unary(try_cast(CastStringToInt32));
        assert_eq!(eval(&expr), Err(EvalError::DivisionByZero));
    }

    /// A two-stage chain, text to int8 to int4, wrapped per stage as the
    /// planner does. A failure at either stage yields NULL because the later
    /// stage propagates the NULL the earlier one produced.
    #[mz_ore::test]
    fn chain_nulls_at_either_stage() {
        let chain = |s: &str| {
            text(s)
                .call_unary(try_cast(CastStringToInt64))
                .call_unary(try_cast(CastInt64ToInt32))
        };
        assert_eq!(eval(&chain("abc")), Ok(Datum::Null), "fails at stage one");
        assert_eq!(
            eval(&chain("3000000000")),
            Ok(Datum::Null),
            "fails at stage two"
        );
        assert_eq!(eval(&chain("5")), Ok(Datum::Int32(5)));
    }

    #[mz_ore::test]
    fn metadata() {
        let func = try_cast(CastStringToInt32);
        assert!(!func.could_error());
        assert!(func.introduces_nulls());
        assert!(func.propagates_nulls());
        assert!(!func.is_monotone());
        assert!(!func.preserves_uniqueness());
        assert_eq!(func.inverse(), None);
        let input = mz_repr::SqlScalarType::String.nullable(false);
        assert!(func.output_sql_type(input).nullable);
        assert_eq!(func.to_string(), "try_cast[text_to_integer]");
    }
}
