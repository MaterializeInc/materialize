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
                return Err(EvalError::Internal("invalid input type".into()));
            }
            // Otherwise we just propagate NULLs and errors
            Err(res) => return res,
        };
        let b = match T::Input2::try_from_result(b.eval(datums, temp_storage)) {
            // If we can convert to the input type then we call the function
            Ok(input) => input,
            // If we can't and we got a non-null datum something went wrong in the planner
            Err(Ok(datum)) if !datum.is_null() => {
                return Err(EvalError::Internal("invalid input type".into()));
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
    use crate::{BinaryFunc, EvalError, func};

    #[sqlfunc(sqlname = "INFALLIBLE", is_infix_op = true)]
    fn infallible1(a: f32, b: f32) -> f32 {
        a + b
    }

    #[sqlfunc]
    fn infallible2(a: Option<f32>, b: Option<f32>) -> f32 {
        a.unwrap_or_default() + b.unwrap_or_default()
    }

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

    #[sqlfunc]
    fn fallible1(a: f32, b: f32) -> Result<f32, EvalError> {
        Ok(a + b)
    }

    #[sqlfunc]
    fn fallible2(a: Option<f32>, b: Option<f32>) -> Result<f32, EvalError> {
        Ok(a.unwrap_or_default() + b.unwrap_or_default())
    }

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
    fn test_equivalence_nullable() {
        test_equivalence_inner(true);
    }

    #[mz_ore::test]
    fn test_equivalence_non_nullable() {
        test_equivalence_inner(false);
    }

    /// Test the equivalence of the binary functions in the `func` module with their
    /// derived sqlfunc implementation. The `input_nullable` parameter determines
    /// whether the input colum is marked nullable or not.
    fn test_equivalence_inner(input_nullable: bool) {
        #[track_caller]
        fn check<T: LazyBinaryFunc + std::fmt::Display + std::fmt::Debug>(
            new: T,
            old: BinaryFunc,
            column_a_ty: &ColumnType,
            column_b_ty: &ColumnType,
        ) {
            assert_eq!(
                new.propagates_nulls(),
                old.propagates_nulls(),
                "{new:?} propagates_nulls mismatch"
            );
            assert_eq!(
                new.introduces_nulls(),
                old.introduces_nulls(),
                "{new:?} introduces_nulls mismatch"
            );
            assert_eq!(
                new.could_error(),
                old.could_error(),
                "{new:?} could_error mismatch"
            );
            assert_eq!(
                new.is_monotone(),
                old.is_monotone(),
                "{new:?} is_monotone mismatch"
            );
            assert_eq!(
                new.is_infix_op(),
                old.is_infix_op(),
                "{new:?} is_infix_op mismatch"
            );
            assert_eq!(
                new.output_type(column_a_ty.clone(), column_b_ty.clone()),
                old.output_type(column_a_ty.clone(), column_b_ty.clone()),
                "{new:?} output_type mismatch"
            );
            assert_eq!(
                format!("{}", new),
                format!("{}", old),
                "{new:?} format mismatch"
            );
        }

        let i32_ty = ColumnType {
            nullable: input_nullable,
            scalar_type: ScalarType::Int32,
        };
        let ts_tz_ty = ColumnType {
            nullable: input_nullable,
            scalar_type: ScalarType::TimestampTz { precision: None },
        };
        let time_ty = ColumnType {
            nullable: input_nullable,
            scalar_type: ScalarType::Time,
        };
        let interval_ty = ColumnType {
            nullable: input_nullable,
            scalar_type: ScalarType::Interval,
        };

        use BinaryFunc as BF;

        // TODO: We're passing unexpected column types to the functions here,
        //   which works because most don't look at the type. We should fix this
        //   and pass expected column types.

        check(func::AddInt16, BF::AddInt16, &i32_ty, &i32_ty);
        check(func::AddInt32, BF::AddInt32, &i32_ty, &i32_ty);
        check(func::AddInt64, BF::AddInt64, &i32_ty, &i32_ty);
        check(func::AddUint16, BF::AddUInt16, &i32_ty, &i32_ty);
        check(func::AddUint32, BF::AddUInt32, &i32_ty, &i32_ty);
        check(func::AddUint64, BF::AddUInt64, &i32_ty, &i32_ty);
        check(func::AddFloat32, BF::AddFloat32, &i32_ty, &i32_ty);
        check(func::AddFloat64, BF::AddFloat64, &i32_ty, &i32_ty);
        check(func::AddDateTime, BF::AddDateTime, &i32_ty, &i32_ty);
        check(func::AddDateInterval, BF::AddDateInterval, &i32_ty, &i32_ty);
        check(
            func::AddTimeInterval,
            BF::AddTimeInterval,
            &ts_tz_ty,
            &i32_ty,
        );
        check(func::RoundNumericBinary, BF::RoundNumeric, &i32_ty, &i32_ty);
        check(func::ConvertFrom, BF::ConvertFrom, &i32_ty, &i32_ty);
        check(func::Encode, BF::Encode, &i32_ty, &i32_ty);
        check(
            func::EncodedBytesCharLength,
            BF::EncodedBytesCharLength,
            &i32_ty,
            &i32_ty,
        );
        check(func::AddNumeric, BF::AddNumeric, &i32_ty, &i32_ty);
        check(func::AddInterval, BF::AddInterval, &i32_ty, &i32_ty);
        check(func::BitAndInt16, BF::BitAndInt16, &i32_ty, &i32_ty);
        check(func::BitAndInt32, BF::BitAndInt32, &i32_ty, &i32_ty);
        check(func::BitAndInt64, BF::BitAndInt64, &i32_ty, &i32_ty);
        check(func::BitAndUint16, BF::BitAndUInt16, &i32_ty, &i32_ty);
        check(func::BitAndUint32, BF::BitAndUInt32, &i32_ty, &i32_ty);
        check(func::BitAndUint64, BF::BitAndUInt64, &i32_ty, &i32_ty);
        check(func::BitOrInt16, BF::BitOrInt16, &i32_ty, &i32_ty);
        check(func::BitOrInt32, BF::BitOrInt32, &i32_ty, &i32_ty);
        check(func::BitOrInt64, BF::BitOrInt64, &i32_ty, &i32_ty);
        check(func::BitOrUint16, BF::BitOrUInt16, &i32_ty, &i32_ty);
        check(func::BitOrUint32, BF::BitOrUInt32, &i32_ty, &i32_ty);
        check(func::BitOrUint64, BF::BitOrUInt64, &i32_ty, &i32_ty);
        check(func::BitXorInt16, BF::BitXorInt16, &i32_ty, &i32_ty);
        check(func::BitXorInt32, BF::BitXorInt32, &i32_ty, &i32_ty);
        check(func::BitXorInt64, BF::BitXorInt64, &i32_ty, &i32_ty);
        check(func::BitXorUint16, BF::BitXorUInt16, &i32_ty, &i32_ty);
        check(func::BitXorUint32, BF::BitXorUInt32, &i32_ty, &i32_ty);
        check(func::BitXorUint64, BF::BitXorUInt64, &i32_ty, &i32_ty);

        check(
            func::BitShiftLeftInt16,
            BF::BitShiftLeftInt16,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftLeftInt32,
            BF::BitShiftLeftInt32,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftLeftInt64,
            BF::BitShiftLeftInt64,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftLeftUint16,
            BF::BitShiftLeftUInt16,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftLeftUint32,
            BF::BitShiftLeftUInt32,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftLeftUint64,
            BF::BitShiftLeftUInt64,
            &i32_ty,
            &i32_ty,
        );

        check(
            func::BitShiftRightInt16,
            BF::BitShiftRightInt16,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftRightInt32,
            BF::BitShiftRightInt32,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftRightInt64,
            BF::BitShiftRightInt64,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftRightUint16,
            BF::BitShiftRightUInt16,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftRightUint32,
            BF::BitShiftRightUInt32,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftRightUint64,
            BF::BitShiftRightUInt64,
            &i32_ty,
            &i32_ty,
        );

        check(func::SubInt16, BF::SubInt16, &i32_ty, &i32_ty);
        check(func::SubInt32, BF::SubInt32, &i32_ty, &i32_ty);
        check(func::SubInt64, BF::SubInt64, &i32_ty, &i32_ty);
        check(func::SubUint16, BF::SubUInt16, &i32_ty, &i32_ty);
        check(func::SubUint32, BF::SubUInt32, &i32_ty, &i32_ty);
        check(func::SubUint64, BF::SubUInt64, &i32_ty, &i32_ty);
        check(func::SubFloat32, BF::SubFloat32, &i32_ty, &i32_ty);
        check(func::SubFloat64, BF::SubFloat64, &i32_ty, &i32_ty);
        check(func::SubNumeric, BF::SubNumeric, &i32_ty, &i32_ty);

        check(func::AgeTimestamp, BF::AgeTimestamp, &i32_ty, &i32_ty);
        check(func::AgeTimestamptz, BF::AgeTimestampTz, &i32_ty, &i32_ty);

        check(func::SubTimestamp, BF::SubTimestamp, &ts_tz_ty, &i32_ty);
        check(func::SubTimestamptz, BF::SubTimestampTz, &ts_tz_ty, &i32_ty);
        check(func::SubDate, BF::SubDate, &i32_ty, &i32_ty);
        check(func::SubTime, BF::SubTime, &i32_ty, &i32_ty);
        check(func::SubInterval, BF::SubInterval, &i32_ty, &i32_ty);
        check(func::SubDateInterval, BF::SubDateInterval, &i32_ty, &i32_ty);
        check(
            func::SubTimeInterval,
            BF::SubTimeInterval,
            &time_ty,
            &interval_ty,
        );

        check(func::MulInt16, BF::MulInt16, &i32_ty, &i32_ty);
        check(func::MulInt32, BF::MulInt32, &i32_ty, &i32_ty);
        check(func::MulInt64, BF::MulInt64, &i32_ty, &i32_ty);
        check(func::MulUint16, BF::MulUInt16, &i32_ty, &i32_ty);
        check(func::MulUint32, BF::MulUInt32, &i32_ty, &i32_ty);
        check(func::MulUint64, BF::MulUInt64, &i32_ty, &i32_ty);
        check(func::MulFloat32, BF::MulFloat32, &i32_ty, &i32_ty);
        check(func::MulFloat64, BF::MulFloat64, &i32_ty, &i32_ty);
        check(func::MulNumeric, BF::MulNumeric, &i32_ty, &i32_ty);
        check(func::MulInterval, BF::MulInterval, &i32_ty, &i32_ty);

        check(func::DivInt16, BF::DivInt16, &i32_ty, &i32_ty);
        check(func::DivInt32, BF::DivInt32, &i32_ty, &i32_ty);
        check(func::DivInt64, BF::DivInt64, &i32_ty, &i32_ty);
        check(func::DivUint16, BF::DivUInt16, &i32_ty, &i32_ty);
        check(func::DivUint32, BF::DivUInt32, &i32_ty, &i32_ty);
        check(func::DivUint64, BF::DivUInt64, &i32_ty, &i32_ty);
        check(func::DivFloat32, BF::DivFloat32, &i32_ty, &i32_ty);
        check(func::DivFloat64, BF::DivFloat64, &i32_ty, &i32_ty);
        check(func::DivNumeric, BF::DivNumeric, &i32_ty, &i32_ty);
        check(func::DivInterval, BF::DivInterval, &i32_ty, &i32_ty);

        check(func::ModInt16, BF::ModInt16, &i32_ty, &i32_ty);
        check(func::ModInt32, BF::ModInt32, &i32_ty, &i32_ty);
        check(func::ModInt64, BF::ModInt64, &i32_ty, &i32_ty);
        check(func::ModUint16, BF::ModUInt16, &i32_ty, &i32_ty);
        check(func::ModUint32, BF::ModUInt32, &i32_ty, &i32_ty);
        check(func::ModUint64, BF::ModUInt64, &i32_ty, &i32_ty);
        check(func::ModFloat32, BF::ModFloat32, &i32_ty, &i32_ty);
        check(func::ModFloat64, BF::ModFloat64, &i32_ty, &i32_ty);
        check(func::ModNumeric, BF::ModNumeric, &i32_ty, &i32_ty);

        check(func::LogBaseNumeric, BF::LogNumeric, &i32_ty, &i32_ty);
        check(func::Power, BF::Power, &i32_ty, &i32_ty);
        check(func::PowerNumeric, BF::PowerNumeric, &i32_ty, &i32_ty);

        check(func::UuidGenerateV5, BF::UuidGenerateV5, &i32_ty, &i32_ty);

        check(func::GetBit, BF::GetBit, &i32_ty, &i32_ty);
        check(func::GetByte, BF::GetByte, &i32_ty, &i32_ty);

        check(
            func::ConstantTimeEqBytes,
            BF::ConstantTimeEqBytes,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::ConstantTimeEqString,
            BF::ConstantTimeEqString,
            &i32_ty,
            &i32_ty,
        );

        check(
            func::RangeContainsRange,
            BF::RangeContainsRange { rev: false },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsRangeRev,
            BF::RangeContainsRange { rev: true },
            &i32_ty,
            &i32_ty,
        );
        check(func::RangeOverlaps, BF::RangeOverlaps, &i32_ty, &i32_ty);
        check(func::RangeAfter, BF::RangeAfter, &i32_ty, &i32_ty);
        check(func::RangeBefore, BF::RangeBefore, &i32_ty, &i32_ty);
        check(func::RangeOverleft, BF::RangeOverleft, &i32_ty, &i32_ty);
        check(func::RangeOverright, BF::RangeOverright, &i32_ty, &i32_ty);
        check(func::RangeAdjacent, BF::RangeAdjacent, &i32_ty, &i32_ty);

        check(func::Eq, BF::Eq, &i32_ty, &i32_ty);
        check(func::NotEq, BF::NotEq, &i32_ty, &i32_ty);
        check(func::Lt, BF::Lt, &i32_ty, &i32_ty);
        check(func::Lte, BF::Lte, &i32_ty, &i32_ty);
        check(func::Gt, BF::Gt, &i32_ty, &i32_ty);
        check(func::Gte, BF::Gte, &i32_ty, &i32_ty);

        check(
            func::JsonbContainsString,
            BF::JsonbContainsString,
            &i32_ty,
            &i32_ty,
        );
        check(func::MapContainsKey, BF::MapContainsKey, &i32_ty, &i32_ty);
        check(
            func::MapContainsAllKeys,
            BF::MapContainsAllKeys,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::MapContainsAnyKeys,
            BF::MapContainsAnyKeys,
            &i32_ty,
            &i32_ty,
        );
        check(func::MapContainsMap, BF::MapContainsMap, &i32_ty, &i32_ty);

        check(
            func::JsonbContainsJsonb,
            BF::JsonbContainsJsonb,
            &i32_ty,
            &i32_ty,
        );

        check(func::ExtractDateUnits, BF::ExtractDate, &i32_ty, &i32_ty);
        check(
            func::DateTruncInterval,
            BF::DateTruncInterval,
            &i32_ty,
            &i32_ty,
        );

        check(func::ArrayLength, BF::ArrayLength, &i32_ty, &i32_ty);
    }
}
