// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for binary functions.

use mz_repr::{ColumnType, Datum, DatumType, RowArena};

use crate::{EvalError, MirScalarExpr};

/// A description of an SQL binary function that has the ability to lazy evaluate its arguments
// This trait will eventually be annotated with #[enum_dispatch] to autogenerate the UnaryFunc enum
#[allow(unused)]
pub(crate) trait LazyBinaryFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        b: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError>;

    /// The output ColumnType of this function.
    fn output_type(&self, input_type_a: ColumnType, input_type_b: ColumnType) -> ColumnType;

    /// Whether this function will produce NULL on NULL input.
    fn propagates_nulls(&self) -> bool;

    /// Whether this function will produce NULL on non-NULL input.
    fn introduces_nulls(&self) -> bool;

    /// Whether this function might error on non-error input.
    fn could_error(&self) -> bool {
        // NB: override this for functions that never error.
        true
    }

    /// Returns the negation of the function, if one exists.
    fn negate(&self) -> Option<crate::BinaryFunc>;

    /// Returns true if the function is monotone. (Non-strict; either increasing or decreasing.)
    /// Monotone functions map ranges to ranges: ie. given a range of possible inputs, we can
    /// determine the range of possible outputs just by mapping the endpoints.
    ///
    /// This describes the *pointwise* behaviour of the function:
    /// ie. the behaviour of any specific argument as the others are held constant. (For example, `a - b` is
    /// monotone in the first argument because for any particular value of `b`, increasing `a` will
    /// always cause the result to increase... and in the second argument because for any specific `a`,
    /// increasing `b` will always cause the result to _decrease_.)
    ///
    /// This property describes the behaviour of the function over ranges where the function is defined:
    /// ie. the arguments and the result are non-error datums.
    fn is_monotone(&self) -> (bool, bool);

    /// Yep, I guess this returns true for infix operators.
    fn is_infix_op(&self) -> bool;
}

#[allow(unused)]
pub(crate) trait EagerBinaryFunc<'a> {
    type Input1: DatumType<'a, EvalError>;
    type Input2: DatumType<'a, EvalError>;
    type Output: DatumType<'a, EvalError>;

    fn call(&self, a: Self::Input1, b: Self::Input2, temp_storage: &'a RowArena) -> Self::Output;

    /// The output ColumnType of this function
    fn output_type(&self, input_type_a: ColumnType, input_type_b: ColumnType) -> ColumnType;

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        // If the inputs are not nullable then nulls are propagated
        !Self::Input1::nullable() && !Self::Input2::nullable()
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        // If the output is nullable then nulls can be introduced
        Self::Output::nullable()
    }

    /// Whether this function could produce an error
    fn could_error(&self) -> bool {
        Self::Output::fallible()
    }

    /// Returns the negation of the given binary function, if it exists.
    fn negate(&self) -> Option<crate::BinaryFunc> {
        None
    }

    fn is_monotone(&self) -> (bool, bool) {
        (false, false)
    }

    fn is_infix_op(&self) -> bool {
        false
    }
}

impl<T: for<'a> EagerBinaryFunc<'a>> LazyBinaryFunc for T {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        b: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = match T::Input1::try_from_result(a.eval(datums, temp_storage)) {
            // If we can convert to the input type then we call the function
            Ok(input) => input,
            // If we can't and we got a non-null datum something went wrong in the planner
            Err(Ok(datum)) if !datum.is_null() => {
                return Err(EvalError::Internal("invalid input type".into()))
            }
            // Otherwise we just propagate NULLs and errors
            Err(res) => return res,
        };
        let b = match T::Input2::try_from_result(b.eval(datums, temp_storage)) {
            // If we can convert to the input type then we call the function
            Ok(input) => input,
            // If we can't and we got a non-null datum something went wrong in the planner
            Err(Ok(datum)) if !datum.is_null() => {
                return Err(EvalError::Internal("invalid input type".into()))
            }
            // Otherwise we just propagate NULLs and errors
            Err(res) => return res,
        };
        self.call(a, b, temp_storage).into_result(temp_storage)
    }

    fn output_type(&self, input_type_a: ColumnType, input_type_b: ColumnType) -> ColumnType {
        self.output_type(input_type_a, input_type_b)
    }

    fn propagates_nulls(&self) -> bool {
        self.propagates_nulls()
    }

    fn introduces_nulls(&self) -> bool {
        self.introduces_nulls()
    }

    fn could_error(&self) -> bool {
        self.could_error()
    }

    fn negate(&self) -> Option<crate::BinaryFunc> {
        self.negate()
    }

    fn is_monotone(&self) -> (bool, bool) {
        self.is_monotone()
    }

    fn is_infix_op(&self) -> bool {
        self.is_infix_op()
    }
}

#[cfg(test)]
mod test {
    use mz_expr_derive::sqlfunc;
    use mz_repr::ColumnType;
    use mz_repr::ScalarType;

    use crate::scalar::func::binary::LazyBinaryFunc;
    use crate::{func, BinaryFunc, EvalError};

    #[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
    #[sqlfunc(sqlname = "INFALLIBLE", is_infix_op = true)]
    fn infallible1(a: f32, b: f32) -> f32 {
        a + b
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
    #[sqlfunc]
    fn infallible2(a: Option<f32>, b: Option<f32>) -> f32 {
        a.unwrap_or_default() + b.unwrap_or_default()
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
    #[sqlfunc]
    fn infallible3(a: f32, b: f32) -> Option<f32> {
        Some(a + b)
    }

    #[mz_ore::test]
    fn elision_rules_infallible() {
        assert_eq!(format!("{}", Infallible1), "INFALLIBLE");
        assert!(Infallible1.propagates_nulls());
        assert!(!Infallible1.introduces_nulls());

        assert!(!Infallible2.propagates_nulls());
        assert!(!Infallible2.introduces_nulls());

        assert!(Infallible3.propagates_nulls());
        assert!(Infallible3.introduces_nulls());
    }

    #[mz_ore::test]
    fn output_types_infallible() {
        assert_eq!(
            Infallible1.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible1.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible1.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible1.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Infallible2.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Infallible2.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Infallible2.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Infallible2.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Infallible3.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible3.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible3.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible3.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(true)
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
    #[sqlfunc]
    fn fallible1(a: f32, b: f32) -> Result<f32, EvalError> {
        Ok(a + b)
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
    #[sqlfunc]
    fn fallible2(a: Option<f32>, b: Option<f32>) -> Result<f32, EvalError> {
        Ok(a.unwrap_or_default() + b.unwrap_or_default())
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
    #[sqlfunc]
    fn fallible3(a: f32, b: f32) -> Result<Option<f32>, EvalError> {
        Ok(Some(a + b))
    }

    #[mz_ore::test]
    fn elision_rules_fallible() {
        assert!(Fallible1.propagates_nulls());
        assert!(!Fallible1.introduces_nulls());

        assert!(!Fallible2.propagates_nulls());
        assert!(!Fallible2.introduces_nulls());

        assert!(Fallible3.propagates_nulls());
        assert!(Fallible3.introduces_nulls());
    }

    #[mz_ore::test]
    fn output_types_fallible() {
        assert_eq!(
            Fallible1.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible1.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible1.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible1.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Fallible2.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Fallible2.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Fallible2.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Fallible2.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Fallible3.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible3.output_type(
                ScalarType::Float32.nullable(true),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible3.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(true)
            ),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible3.output_type(
                ScalarType::Float32.nullable(false),
                ScalarType::Float32.nullable(false)
            ),
            ScalarType::Float32.nullable(true)
        );
    }

    #[mz_ore::test]
    fn test_equivalence() {
        #[track_caller]
        fn check<T: LazyBinaryFunc + std::fmt::Display>(
            new: T,
            old: BinaryFunc,
            column_ty: ColumnType,
        ) {
            assert_eq!(
                new.propagates_nulls(),
                old.propagates_nulls(),
                "propagates_nulls mismatch"
            );
            assert_eq!(
                new.introduces_nulls(),
                old.introduces_nulls(),
                "introduces_nulls mismatch"
            );
            assert_eq!(new.could_error(), old.could_error(), "could_error mismatch");
            assert_eq!(new.is_monotone(), old.is_monotone(), "is_monotone mismatch");
            assert_eq!(new.is_infix_op(), old.is_infix_op(), "is_infix_op mismatch");
            assert_eq!(
                new.output_type(column_ty.clone(), column_ty.clone()),
                old.output_type(column_ty.clone(), column_ty.clone()),
                "output_type mismatch"
            );
            assert_eq!(format!("{}", new), format!("{}", old), "format mismatch");
        }
        let i32_ty = ColumnType {
            nullable: true,
            scalar_type: ScalarType::Int32,
        };
        let ts_tz_ty = ColumnType {
            nullable: true,
            scalar_type: ScalarType::TimestampTz { precision: None },
        };

        use BinaryFunc as BF;

        check(func::AddInt16, BF::AddInt16, i32_ty.clone());
        check(func::AddInt32, BF::AddInt32, i32_ty.clone());
        check(func::AddInt64, BF::AddInt64, i32_ty.clone());
        check(func::AddUint16, BF::AddUInt16, i32_ty.clone());
        check(func::AddUint32, BF::AddUInt32, i32_ty.clone());
        check(func::AddUint64, BF::AddUInt64, i32_ty.clone());
        check(func::AddFloat32, BF::AddFloat32, i32_ty.clone());
        check(func::AddFloat64, BF::AddFloat64, i32_ty.clone());
        check(func::AddDateTime, BF::AddDateTime, i32_ty.clone());
        check(func::AddDateInterval, BF::AddDateInterval, i32_ty.clone());
        check(func::AddTimeInterval, BF::AddTimeInterval, ts_tz_ty.clone());
        check(func::RoundNumericBinary, BF::RoundNumeric, i32_ty.clone());
    }
}
