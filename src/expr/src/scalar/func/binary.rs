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
        let a = a.eval(datums, temp_storage)?;
        let b = b.eval(datums, temp_storage)?;
        let a = match T::Input1::try_from_result(Ok(a)) {
            // If we can convert to the input type then we call the function
            Ok(input) => input,
            // If we can't and we got a non-null datum something went wrong in the planner
            Err(Ok(datum)) if !datum.is_null() => {
                return Err(EvalError::Internal("invalid input type".into()));
            }
            // Otherwise we just propagate NULLs and errors
            Err(res) => return res,
        };
        let b = match T::Input2::try_from_result(Ok(b)) {
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

    derive_binary_from! {
        AddDateInterval,
        AddDateTime,
        AddFloat32,
        AddFloat64,
        AddInt16,
        AddInt32,
        AddInt64,
        AddInterval,
        AddNumeric,
        AddTimeInterval,
        AddTimestampInterval,
        AddTimestampTzInterval,
        AddUint16,
        AddUint32,
        AddUint64,
        AgeTimestamp,
        AgeTimestampTz,
        ArrayArrayConcat,
        ArrayContains,
        ArrayContainsArray,
        ArrayContainsArrayRev,
        ArrayLength,
        ArrayLower,
        ArrayRemove,
        ArrayUpper,
        BitAndInt16,
        BitAndInt32,
        BitAndInt64,
        BitAndUint16,
        BitAndUint32,
        BitAndUint64,
        BitOrInt16,
        BitOrInt32,
        BitOrInt64,
        BitOrUint16,
        BitOrUint32,
        BitOrUint64,
        BitShiftLeftInt16,
        BitShiftLeftInt32,
        BitShiftLeftInt64,
        BitShiftLeftUint16,
        BitShiftLeftUint32,
        BitShiftLeftUint64,
        BitShiftRightInt16,
        BitShiftRightInt32,
        BitShiftRightInt64,
        BitShiftRightUint16,
        BitShiftRightUint32,
        BitShiftRightUint64,
        BitXorInt16,
        BitXorInt32,
        BitXorInt64,
        BitXorUint16,
        BitXorUint32,
        BitXorUint64,
        ConstantTimeEqBytes,
        ConstantTimeEqString,
        ConvertFrom,
        DateBinTimestamp,
        DateBinTimestampTz,
        DatePartInterval(DatePartIntervalF64),
        DatePartTime(DatePartTimeF64),
        DatePartTimestamp(DatePartTimestampTimestampF64),
        DatePartTimestampTz(DatePartTimestampTimestampTzF64),
        DateTruncInterval,
        DateTruncTimestamp(DateTruncUnitsTimestamp),
        DateTruncTimestampTz(DateTruncUnitsTimestampTz),
        Decode,
        DigestBytes,
        DigestString,
        DivFloat32,
        DivFloat64,
        DivInt16,
        DivInt32,
        DivInt64,
        DivInterval,
        DivNumeric,
        DivUint16,
        DivUint32,
        DivUint64,
        ElementListConcat,
        Encode,
        EncodedBytesCharLength,
        Eq,
        ExtractDate(ExtractDateUnits),
        ExtractInterval(DatePartIntervalNumeric),
        ExtractTime(DatePartTimeNumeric),
        ExtractTimestamp(DatePartTimestampTimestampNumeric),
        ExtractTimestampTz(DatePartTimestampTimestampTzNumeric),
        GetBit,
        GetByte,
        Gt,
        Gte,
        IsLikeMatchCaseInsensitive,
        IsLikeMatchCaseSensitive,
        // IsRegexpMatch
        JsonbConcat,
        JsonbContainsJsonb,
        JsonbContainsString,
        JsonbDeleteInt64,
        JsonbDeleteString,
        JsonbGetInt64,
        JsonbGetInt64Stringify,
        JsonbGetPath,
        JsonbGetPathStringify,
        JsonbGetString,
        JsonbGetStringStringify,
        Left,
        LikeEscape,
        ListContainsList,
        ListContainsListRev,
        ListElementConcat,
        ListLengthMax,
        ListListConcat,
        ListRemove,
        LogNumeric(LogBaseNumeric),
        Lt,
        Lte,
        MapContainsAllKeys,
        MapContainsAnyKeys,
        MapContainsKey,
        MapContainsMap,
        MapGetValue,
        ModFloat32,
        ModFloat64,
        ModInt16,
        ModInt32,
        ModInt64,
        ModNumeric,
        ModUint16,
        ModUint32,
        ModUint64,
        MulFloat32,
        MulFloat64,
        MulInt16,
        MulInt32,
        MulInt64,
        MulInterval,
        MulNumeric,
        MulUint16,
        MulUint32,
        MulUint64,
        MzAclItemContainsPrivilege,
        MzRenderTypmod,
        // Normalize,
        NotEq,
        ParseIdent,
        Position,
        Power,
        PowerNumeric,
        PrettySql,
        RangeAdjacent,
        RangeAfter,
        RangeBefore,
        // RangeContainsElem
        RangeContainsRange,
        RangeContainsRangeRev,
        RangeDifference,
        RangeIntersection,
        RangeOverlaps,
        RangeOverleft,
        RangeOverright,
        RangeUnion,
        // RegexpReplace
        // RepeatString,
        Right,
        RoundNumeric(RoundNumericBinary),
        StartsWith,
        SubDate,
        SubDateInterval,
        SubFloat32,
        SubFloat64,
        SubInt16,
        SubInt32,
        SubInt64,
        SubInterval,
        SubNumeric,
        SubTime,
        SubTimeInterval,
        SubTimestamp,
        SubTimestampInterval,
        SubTimestampTz,
        SubTimestampTzInterval,
        SubUint16,
        SubUint32,
        SubUint64,
        TextConcat(TextConcatBinary),
        // TimezoneIntervalTime,
        // TimezoneIntervalTimestamp,
        // TimezoneIntervalTimestampTz,
        TimezoneOffset,
        // TimezoneTimestamp,
        // TimezoneTimestampTz,
        ToCharTimestamp(ToCharTimestampFormat),
        ToCharTimestampTz(ToCharTimestampTzFormat),
        Trim,
        TrimLeading,
        TrimTrailing,
        UuidGenerateV5,
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

        use BinaryFunc as BF;

        // TODO: We're passing unexpected column types to the functions here,
        //   which works because most don't look at the type. We should fix this
        //   and pass expected column types.
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

        // check(func::ListLength, BF::ListLength, &i32_ty, &i32_ty);
    }
}
