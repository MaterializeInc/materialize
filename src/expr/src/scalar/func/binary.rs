// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for binary functions.

use mz_repr::{Datum, DatumType, RowArena, SqlColumnType};

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

    /// The output SqlColumnType of this function.
    fn output_type(
        &self,
        input_type_a: SqlColumnType,
        input_type_b: SqlColumnType,
    ) -> SqlColumnType;

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

    /// The output SqlColumnType of this function
    fn output_type(
        &self,
        input_type_a: SqlColumnType,
        input_type_b: SqlColumnType,
    ) -> SqlColumnType;

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

    fn output_type(
        &self,
        input_type_a: SqlColumnType,
        input_type_b: SqlColumnType,
    ) -> SqlColumnType {
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

mod derive {
    use crate::scalar::func::*;

    derive_binary! {
        AddInt16,
        AddInt32,
        AddInt64,
        AddUint16,
        AddUint32,
        AddUint64,
        AddFloat32,
        AddFloat64,
        AddInterval,
    }
}

#[cfg(test)]
mod test {
    use mz_expr_derive::sqlfunc;
    use mz_repr::SqlColumnType;
    use mz_repr::SqlScalarType;

    use crate::scalar::func::binary::LazyBinaryFunc;
    use crate::{BinaryFunc, EvalError, func};

    #[sqlfunc(sqlname = "INFALLIBLE", is_infix_op = true)]
    #[allow(dead_code)]
    fn infallible1(a: f32, b: f32) -> f32 {
        a + b
    }

    #[sqlfunc]
    #[allow(dead_code)]
    fn infallible2(a: Option<f32>, b: Option<f32>) -> f32 {
        a.unwrap_or_default() + b.unwrap_or_default()
    }

    #[sqlfunc]
    #[allow(dead_code)]
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
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible1.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible1.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible1.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Infallible2.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Infallible2.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Infallible2.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Infallible2.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Infallible3.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible3.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible3.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible3.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(true)
        );
    }

    #[sqlfunc]
    #[allow(dead_code)]
    fn fallible1(a: f32, b: f32) -> Result<f32, EvalError> {
        Ok(a + b)
    }

    #[sqlfunc]
    #[allow(dead_code)]
    fn fallible2(a: Option<f32>, b: Option<f32>) -> Result<f32, EvalError> {
        Ok(a.unwrap_or_default() + b.unwrap_or_default())
    }

    #[sqlfunc]
    #[allow(dead_code)]
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
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible1.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible1.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible1.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Fallible2.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Fallible2.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Fallible2.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Fallible2.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Fallible3.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible3.output_type(
                SqlScalarType::Float32.nullable(true),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible3.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(true)
            ),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible3.output_type(
                SqlScalarType::Float32.nullable(false),
                SqlScalarType::Float32.nullable(false)
            ),
            SqlScalarType::Float32.nullable(true)
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
            column_a_ty: &SqlColumnType,
            column_b_ty: &SqlColumnType,
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
            assert_eq!(new.negate(), old.negate(), "{new:?} negate mismatch");
            assert_eq!(
                format!("{}", new),
                format!("{}", old),
                "{new:?} format mismatch"
            );
        }

        let i32_ty = SqlColumnType {
            nullable: input_nullable,
            scalar_type: SqlScalarType::Int32,
        };
        let ts_tz_ty = SqlColumnType {
            nullable: input_nullable,
            scalar_type: SqlScalarType::TimestampTz { precision: None },
        };
        let time_ty = SqlColumnType {
            nullable: input_nullable,
            scalar_type: SqlScalarType::Time,
        };
        let interval_ty = SqlColumnType {
            nullable: input_nullable,
            scalar_type: SqlScalarType::Interval,
        };
        let i32_map_ty = SqlColumnType {
            nullable: input_nullable,
            scalar_type: SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::Int32),
                custom_id: None,
            },
        };

        use BinaryFunc as BF;

        // TODO: We're passing unexpected column types to the functions here,
        //   which works because most don't look at the type. We should fix this
        //   and pass expected column types.

        check(func::AddDateInterval, BF::AddDateInterval, &i32_ty, &i32_ty);
        check(
            func::AddTimeInterval,
            BF::AddTimeInterval,
            &ts_tz_ty,
            &i32_ty,
        );
        check(func::RoundNumericBinary, BF::RoundNumeric, &i32_ty, &i32_ty);
        check(func::ConvertFrom, BF::ConvertFrom, &i32_ty, &i32_ty);
        check(func::Left, BF::Left, &i32_ty, &i32_ty);
        check(func::Right, BF::Right, &i32_ty, &i32_ty);
        check(func::Trim, BF::Trim, &i32_ty, &i32_ty);
        check(func::TrimLeading, BF::TrimLeading, &i32_ty, &i32_ty);
        check(func::TrimTrailing, BF::TrimTrailing, &i32_ty, &i32_ty);
        check(func::Encode, BF::Encode, &i32_ty, &i32_ty);
        check(func::Decode, BF::Decode, &i32_ty, &i32_ty);
        check(
            func::EncodedBytesCharLength,
            BF::EncodedBytesCharLength,
            &i32_ty,
            &i32_ty,
        );
        check(func::AddNumeric, BF::AddNumeric, &i32_ty, &i32_ty);
        check(func::BitAndInt16, BF::BitAndInt16, &i32_ty, &i32_ty);
        check(func::BitAndInt32, BF::BitAndInt32, &i32_ty, &i32_ty);
        check(func::BitAndInt64, BF::BitAndInt64, &i32_ty, &i32_ty);
        check(func::BitAndUint16, BF::BitAndUint16, &i32_ty, &i32_ty);
        check(func::BitAndUint32, BF::BitAndUint32, &i32_ty, &i32_ty);
        check(func::BitAndUint64, BF::BitAndUint64, &i32_ty, &i32_ty);
        check(func::BitOrInt16, BF::BitOrInt16, &i32_ty, &i32_ty);
        check(func::BitOrInt32, BF::BitOrInt32, &i32_ty, &i32_ty);
        check(func::BitOrInt64, BF::BitOrInt64, &i32_ty, &i32_ty);
        check(func::BitOrUint16, BF::BitOrUint16, &i32_ty, &i32_ty);
        check(func::BitOrUint32, BF::BitOrUint32, &i32_ty, &i32_ty);
        check(func::BitOrUint64, BF::BitOrUint64, &i32_ty, &i32_ty);
        check(func::BitXorInt16, BF::BitXorInt16, &i32_ty, &i32_ty);
        check(func::BitXorInt32, BF::BitXorInt32, &i32_ty, &i32_ty);
        check(func::BitXorInt64, BF::BitXorInt64, &i32_ty, &i32_ty);
        check(func::BitXorUint16, BF::BitXorUint16, &i32_ty, &i32_ty);
        check(func::BitXorUint32, BF::BitXorUint32, &i32_ty, &i32_ty);
        check(func::BitXorUint64, BF::BitXorUint64, &i32_ty, &i32_ty);

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
            BF::BitShiftLeftUint16,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftLeftUint32,
            BF::BitShiftLeftUint32,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftLeftUint64,
            BF::BitShiftLeftUint64,
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
            BF::BitShiftRightUint16,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftRightUint32,
            BF::BitShiftRightUint32,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::BitShiftRightUint64,
            BF::BitShiftRightUint64,
            &i32_ty,
            &i32_ty,
        );

        check(func::SubInt16, BF::SubInt16, &i32_ty, &i32_ty);
        check(func::SubInt32, BF::SubInt32, &i32_ty, &i32_ty);
        check(func::SubInt64, BF::SubInt64, &i32_ty, &i32_ty);
        check(func::SubUint16, BF::SubUint16, &i32_ty, &i32_ty);
        check(func::SubUint32, BF::SubUint32, &i32_ty, &i32_ty);
        check(func::SubUint64, BF::SubUint64, &i32_ty, &i32_ty);
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
        check(func::MulUint16, BF::MulUint16, &i32_ty, &i32_ty);
        check(func::MulUint32, BF::MulUint32, &i32_ty, &i32_ty);
        check(func::MulUint64, BF::MulUint64, &i32_ty, &i32_ty);
        check(func::MulFloat32, BF::MulFloat32, &i32_ty, &i32_ty);
        check(func::MulFloat64, BF::MulFloat64, &i32_ty, &i32_ty);
        check(func::MulNumeric, BF::MulNumeric, &i32_ty, &i32_ty);
        check(func::MulInterval, BF::MulInterval, &i32_ty, &i32_ty);

        check(func::DivInt16, BF::DivInt16, &i32_ty, &i32_ty);
        check(func::DivInt32, BF::DivInt32, &i32_ty, &i32_ty);
        check(func::DivInt64, BF::DivInt64, &i32_ty, &i32_ty);
        check(func::DivUint16, BF::DivUint16, &i32_ty, &i32_ty);
        check(func::DivUint32, BF::DivUint32, &i32_ty, &i32_ty);
        check(func::DivUint64, BF::DivUint64, &i32_ty, &i32_ty);
        check(func::DivFloat32, BF::DivFloat32, &i32_ty, &i32_ty);
        check(func::DivFloat64, BF::DivFloat64, &i32_ty, &i32_ty);
        check(func::DivNumeric, BF::DivNumeric, &i32_ty, &i32_ty);
        check(func::DivInterval, BF::DivInterval, &i32_ty, &i32_ty);

        check(func::ModInt16, BF::ModInt16, &i32_ty, &i32_ty);
        check(func::ModInt32, BF::ModInt32, &i32_ty, &i32_ty);
        check(func::ModInt64, BF::ModInt64, &i32_ty, &i32_ty);
        check(func::ModUint16, BF::ModUint16, &i32_ty, &i32_ty);
        check(func::ModUint32, BF::ModUint32, &i32_ty, &i32_ty);
        check(func::ModUint64, BF::ModUint64, &i32_ty, &i32_ty);
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
            func::RangeContainsI32,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Int32,
                rev: false,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsI64,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Int64,
                rev: false,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsDate,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Date,
                rev: false,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsNumeric,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Numeric { max_scale: None },
                rev: false,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsTimestamp,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Timestamp { precision: None },
                rev: false,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsTimestampTz,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::TimestampTz { precision: None },
                rev: false,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsI32Rev,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Int32,
                rev: true,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsI64Rev,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Int64,
                rev: true,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsDateRev,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Date,
                rev: true,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsNumericRev,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Numeric { max_scale: None },
                rev: true,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsTimestampRev,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::Timestamp { precision: None },
                rev: true,
            },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::RangeContainsTimestampTzRev,
            BF::RangeContainsElem {
                elem_type: SqlScalarType::TimestampTz { precision: None },
                rev: true,
            },
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

        check(func::RangeUnion, BF::RangeUnion, &i32_ty, &i32_ty);
        check(
            func::RangeIntersection,
            BF::RangeIntersection,
            &i32_ty,
            &i32_ty,
        );
        check(func::RangeDifference, BF::RangeDifference, &i32_ty, &i32_ty);

        check(func::Eq, BF::Eq, &i32_ty, &i32_ty);
        check(func::NotEq, BF::NotEq, &i32_ty, &i32_ty);
        check(func::Lt, BF::Lt, &i32_ty, &i32_ty);
        check(func::Lte, BF::Lte, &i32_ty, &i32_ty);
        check(func::Gt, BF::Gt, &i32_ty, &i32_ty);
        check(func::Gte, BF::Gte, &i32_ty, &i32_ty);

        check(func::LikeEscape, BF::LikeEscape, &i32_ty, &i32_ty);
        check(func::TimezoneOffset, BF::TimezoneOffset, &i32_ty, &i32_ty);
        check(func::TextConcatBinary, BF::TextConcat, &i32_ty, &i32_ty);

        check(
            func::ToCharTimestampFormat,
            BF::ToCharTimestamp,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::ToCharTimestampTzFormat,
            BF::ToCharTimestampTz,
            &i32_ty,
            &i32_ty,
        );

        // JsonbGet* have a `stringify` parameter that doesn't work with the sqlfunc macro.
        // check(func::JsonbGetInt64, BF::JsonbGetInt64, &i32_ty, &i32_ty);
        // check(func::JsonbGetString, BF::JsonbGetString, &i32_ty, &i32_ty);
        // check(func::JsonbGetPath, BF::JsonbGetPath, &i32_ty, &i32_ty);
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
        check(func::MapGetValue, BF::MapGetValue, &i32_map_ty, &i32_ty);
        check(
            func::ListContainsList,
            BF::ListContainsList { rev: false },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::ListContainsListRev,
            BF::ListContainsList { rev: true },
            &i32_ty,
            &i32_ty,
        );

        check(
            func::JsonbContainsJsonb,
            BF::JsonbContainsJsonb,
            &i32_ty,
            &i32_ty,
        );
        check(func::JsonbConcat, BF::JsonbConcat, &i32_ty, &i32_ty);
        check(
            func::JsonbDeleteInt64,
            BF::JsonbDeleteInt64,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::JsonbDeleteString,
            BF::JsonbDeleteString,
            &i32_ty,
            &i32_ty,
        );

        check(
            func::DateBinTimestamp,
            BF::DateBinTimestamp,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::DateBinTimestampTz,
            BF::DateBinTimestampTz,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::DatePartIntervalNumeric,
            BF::ExtractInterval,
            &i32_ty,
            &i32_ty,
        );
        check(func::DatePartTimeNumeric, BF::ExtractTime, &i32_ty, &i32_ty);
        check(
            func::DatePartTimestampTimestampNumeric,
            BF::ExtractTimestamp,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::DatePartTimestampTimestampTzNumeric,
            BF::ExtractTimestampTz,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::DatePartIntervalF64,
            BF::DatePartInterval,
            &i32_ty,
            &i32_ty,
        );
        check(func::DatePartTimeF64, BF::DatePartTime, &i32_ty, &i32_ty);
        check(
            func::DatePartTimestampTimestampF64,
            BF::DatePartTimestamp,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::DatePartTimestampTimestampTzF64,
            BF::DatePartTimestampTz,
            &i32_ty,
            &i32_ty,
        );

        check(func::ExtractDateUnits, BF::ExtractDate, &i32_ty, &i32_ty);
        check(
            func::DateTruncUnitsTimestamp,
            BF::DateTruncTimestamp,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::DateTruncUnitsTimestampTz,
            BF::DateTruncTimestampTz,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::DateTruncInterval,
            BF::DateTruncInterval,
            &i32_ty,
            &i32_ty,
        );

        check(func::ArrayLength, BF::ArrayLength, &i32_ty, &i32_ty);
        check(func::ArrayLower, BF::ArrayLower, &i32_ty, &i32_ty);
        check(func::ArrayRemove, BF::ArrayRemove, &i32_ty, &i32_ty);
        check(func::ArrayUpper, BF::ArrayUpper, &i32_ty, &i32_ty);
        // check(func::ListLength, BF::ListLength, &i32_ty, &i32_ty);
        check(func::ArrayContains, BF::ArrayContains, &i32_ty, &i32_ty);
        check(
            func::ArrayContainsArray,
            BF::ArrayContainsArray { rev: false },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::ArrayContainsArrayRev,
            BF::ArrayContainsArray { rev: true },
            &i32_ty,
            &i32_ty,
        );
        check(
            func::ArrayArrayConcat,
            BF::ArrayArrayConcat,
            &i32_ty,
            &i32_ty,
        );
        check(func::ListListConcat, BF::ListListConcat, &i32_ty, &i32_ty);
        check(
            func::ListElementConcat,
            BF::ListElementConcat,
            &i32_ty,
            &i32_ty,
        );
        check(
            func::ElementListConcat,
            BF::ElementListConcat,
            &i32_ty,
            &i32_ty,
        );
        check(func::ListRemove, BF::ListRemove, &i32_ty, &i32_ty);
        check(func::DigestString, BF::DigestString, &i32_ty, &i32_ty);
        check(func::DigestBytes, BF::DigestBytes, &i32_ty, &i32_ty);
        check(func::MzRenderTypmod, BF::MzRenderTypmod, &i32_ty, &i32_ty);
        check(
            func::MzAclItemContainsPrivilege,
            BF::MzAclItemContainsPrivilege,
            &i32_ty,
            &i32_ty,
        );
        check(func::ParseIdent, BF::ParseIdent, &i32_ty, &i32_ty);
        check(func::StartsWith, BF::StartsWith, &i32_ty, &i32_ty);
        check(func::PrettySql, BF::PrettySql, &i32_ty, &i32_ty);
    }
}
