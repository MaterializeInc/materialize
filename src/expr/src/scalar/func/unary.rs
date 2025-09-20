// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the PostgreSQL project. The original
// source code is subject to the terms of the PostgreSQL license, a copy of
// which can be found in the LICENSE file at the root of this repository.

//! Definition of the [`UnaryFunc`] enum and related machinery.

use std::{fmt, str};

use mz_repr::{Datum, DatumType, RowArena, SqlColumnType};

use crate::scalar::func::impls::*;
use crate::{EvalError, MirScalarExpr};

/// A description of an SQL unary function that has the ability to lazy evaluate its arguments
// This trait will eventually be annotated with #[enum_dispatch] to autogenerate the UnaryFunc enum
pub trait LazyUnaryFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError>;

    /// The output SqlColumnType of this function.
    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType;

    /// Whether this function will produce NULL on NULL input.
    fn propagates_nulls(&self) -> bool;

    /// Whether this function will produce NULL on non-NULL input.
    fn introduces_nulls(&self) -> bool;

    /// Whether this function might error on non-error input.
    fn could_error(&self) -> bool {
        // NB: override this for functions that never error.
        true
    }

    /// Whether this function preserves uniqueness.
    ///
    /// Uniqueness is preserved when `if f(x) = f(y) then x = y` is true. This
    /// is used by the optimizer when a guarantee can be made that a collection
    /// with unique items will stay unique when mapped by this function.
    ///
    /// Note that error results are not covered: Even with `preserves_uniqueness = true`, it can
    /// happen that two different inputs produce the same error result. (e.g., in case of a
    /// narrowing cast)
    ///
    /// Functions should conservatively return `false` unless they are certain
    /// the above property is true.
    fn preserves_uniqueness(&self) -> bool;

    /// The [inverse] of this function, if it has one and we have determined it.
    ///
    /// The optimizer _can_ use this information when selecting indexes, e.g. an
    /// indexed column has a cast applied to it, by moving the right inverse of
    /// the cast to another value, we can select the indexed column.
    ///
    /// Note that a value of `None` does not imply that the inverse does not
    /// exist; it could also mean we have not yet invested the energy in
    /// representing it. For example, in the case of complex casts, such as
    /// between two list types, we could determine the right inverse, but doing
    /// so is not immediately necessary as this information is only used by the
    /// optimizer.
    ///
    /// ## Right vs. left vs. inverses
    /// - Right inverses are when the inverse function preserves uniqueness.
    ///   These are the functions that the optimizer uses to move casts between
    ///   expressions.
    /// - Left inverses are when the function itself preserves uniqueness.
    /// - Inverses are when a function is both a right and a left inverse (e.g.,
    ///   bit_not_int64 is both a right and left inverse of itself).
    ///
    /// We call this function `inverse` for simplicity's sake; it doesn't always
    /// correspond to the mathematical notion of "inverse." However, in
    /// conjunction with checks to `preserves_uniqueness` you can determine
    /// which type of inverse we return.
    ///
    /// [inverse]: https://en.wikipedia.org/wiki/Inverse_function
    fn inverse(&self) -> Option<crate::UnaryFunc>;

    /// Returns true if the function is monotone. (Non-strict; either increasing or decreasing.)
    /// Monotone functions map ranges to ranges: ie. given a range of possible inputs, we can
    /// determine the range of possible outputs just by mapping the endpoints.
    ///
    /// This property describes the behaviour of the function over ranges where the function is defined:
    /// ie. the argument and the result are non-error datums.
    fn is_monotone(&self) -> bool;
}

/// A description of an SQL unary function that operates on eagerly evaluated expressions
pub trait EagerUnaryFunc<'a> {
    type Input: DatumType<'a, EvalError>;
    type Output: DatumType<'a, EvalError>;

    fn call(&self, input: Self::Input) -> Self::Output;

    /// The output SqlColumnType of this function
    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType;

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        // If the input is not nullable then nulls are propagated
        !Self::Input::nullable()
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

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl<T: for<'a> EagerUnaryFunc<'a>> LazyUnaryFunc for T {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        match T::Input::try_from_result(a.eval(datums, temp_storage)) {
            // If we can convert to the input type then we call the function
            Ok(input) => self.call(input).into_result(temp_storage),
            // If we can't and we got a non-null datum something went wrong in the planner
            Err(Ok(datum)) if !datum.is_null() => {
                Err(EvalError::Internal("invalid input type".into()))
            }
            // Otherwise we just propagate NULLs and errors
            Err(res) => res,
        }
    }

    fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType {
        self.output_type(input_type)
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

    fn preserves_uniqueness(&self) -> bool {
        self.preserves_uniqueness()
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        self.inverse()
    }

    fn is_monotone(&self) -> bool {
        self.is_monotone()
    }
}

derive_unary!(
    Not,
    IsNull,
    IsTrue,
    IsFalse,
    BitNotInt16,
    BitNotInt32,
    BitNotInt64,
    BitNotUint16,
    BitNotUint32,
    BitNotUint64,
    NegInt16,
    NegInt32,
    NegInt64,
    NegFloat32,
    NegFloat64,
    NegNumeric,
    NegInterval,
    SqrtFloat64,
    SqrtNumeric,
    CbrtFloat64,
    AbsInt16,
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    AbsNumeric,
    CastBoolToString,
    CastBoolToStringNonstandard,
    CastBoolToInt32,
    CastBoolToInt64,
    CastInt16ToFloat32,
    CastInt16ToFloat64,
    CastInt16ToInt32,
    CastInt16ToInt64,
    CastInt16ToUint16,
    CastInt16ToUint32,
    CastInt16ToUint64,
    CastInt16ToString,
    CastInt2VectorToArray,
    CastInt32ToBool,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt32ToOid,
    CastInt32ToPgLegacyChar,
    CastInt32ToInt16,
    CastInt32ToInt64,
    CastInt32ToUint16,
    CastInt32ToUint32,
    CastInt32ToUint64,
    CastInt32ToString,
    CastOidToInt32,
    CastOidToInt64,
    CastOidToString,
    CastOidToRegClass,
    CastRegClassToOid,
    CastOidToRegProc,
    CastRegProcToOid,
    CastOidToRegType,
    CastRegTypeToOid,
    CastInt64ToInt16,
    CastInt64ToInt32,
    CastInt64ToUint16,
    CastInt64ToUint32,
    CastInt64ToUint64,
    CastInt16ToNumeric,
    CastInt32ToNumeric,
    CastInt64ToBool,
    CastInt64ToNumeric,
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastInt64ToOid,
    CastInt64ToString,
    CastUint16ToUint32,
    CastUint16ToUint64,
    CastUint16ToInt16,
    CastUint16ToInt32,
    CastUint16ToInt64,
    CastUint16ToNumeric,
    CastUint16ToFloat32,
    CastUint16ToFloat64,
    CastUint16ToString,
    CastUint32ToUint16,
    CastUint32ToUint64,
    CastUint32ToInt16,
    CastUint32ToInt32,
    CastUint32ToInt64,
    CastUint32ToNumeric,
    CastUint32ToFloat32,
    CastUint32ToFloat64,
    CastUint32ToString,
    CastUint64ToUint16,
    CastUint64ToUint32,
    CastUint64ToInt16,
    CastUint64ToInt32,
    CastUint64ToInt64,
    CastUint64ToNumeric,
    CastUint64ToFloat32,
    CastUint64ToFloat64,
    CastUint64ToString,
    CastFloat32ToInt16,
    CastFloat32ToInt32,
    CastFloat32ToInt64,
    CastFloat32ToUint16,
    CastFloat32ToUint32,
    CastFloat32ToUint64,
    CastFloat32ToFloat64,
    CastFloat32ToString,
    CastFloat32ToNumeric,
    CastFloat64ToNumeric,
    CastFloat64ToInt16,
    CastFloat64ToInt32,
    CastFloat64ToInt64,
    CastFloat64ToUint16,
    CastFloat64ToUint32,
    CastFloat64ToUint64,
    CastFloat64ToFloat32,
    CastFloat64ToString,
    CastNumericToFloat32,
    CastNumericToFloat64,
    CastNumericToInt16,
    CastNumericToInt32,
    CastNumericToInt64,
    CastNumericToUint16,
    CastNumericToUint32,
    CastNumericToUint64,
    CastNumericToString,
    CastMzTimestampToString,
    CastMzTimestampToTimestamp,
    CastMzTimestampToTimestampTz,
    CastStringToMzTimestamp,
    CastUint64ToMzTimestamp,
    CastUint32ToMzTimestamp,
    CastInt64ToMzTimestamp,
    CastInt32ToMzTimestamp,
    CastNumericToMzTimestamp,
    CastTimestampToMzTimestamp,
    CastTimestampTzToMzTimestamp,
    CastDateToMzTimestamp,
    CastStringToBool,
    CastStringToPgLegacyChar,
    CastStringToPgLegacyName,
    CastStringToBytes,
    CastStringToInt16,
    CastStringToInt32,
    CastStringToInt64,
    CastStringToUint16,
    CastStringToUint32,
    CastStringToUint64,
    CastStringToInt2Vector,
    CastStringToOid,
    CastStringToFloat32,
    CastStringToFloat64,
    CastStringToDate,
    CastStringToArray,
    CastStringToList,
    CastStringToMap,
    CastStringToRange,
    CastStringToTime,
    CastStringToTimestamp,
    CastStringToTimestampTz,
    CastStringToInterval,
    CastStringToNumeric,
    CastStringToUuid,
    CastStringToChar,
    PadChar,
    CastStringToVarChar,
    CastCharToString,
    CastVarCharToString,
    CastDateToTimestamp,
    CastDateToTimestampTz,
    CastDateToString,
    CastTimeToInterval,
    CastTimeToString,
    CastIntervalToString,
    CastIntervalToTime,
    CastTimestampToDate,
    AdjustTimestampPrecision,
    CastTimestampToTimestampTz,
    CastTimestampToString,
    CastTimestampToTime,
    CastTimestampTzToDate,
    CastTimestampTzToTimestamp,
    AdjustTimestampTzPrecision,
    CastTimestampTzToString,
    CastTimestampTzToTime,
    CastPgLegacyCharToString,
    CastPgLegacyCharToChar,
    CastPgLegacyCharToVarChar,
    CastPgLegacyCharToInt32,
    CastBytesToString,
    CastStringToJsonb,
    CastJsonbToString,
    CastJsonbableToJsonb,
    CastJsonbToInt16,
    CastJsonbToInt32,
    CastJsonbToInt64,
    CastJsonbToFloat32,
    CastJsonbToFloat64,
    CastJsonbToNumeric,
    CastJsonbToBool,
    CastUuidToString,
    CastRecordToString,
    CastRecord1ToRecord2,
    CastArrayToArray,
    CastArrayToJsonb,
    CastArrayToString,
    CastListToString,
    CastListToJsonb,
    CastList1ToList2,
    CastArrayToListOneDim,
    CastMapToString,
    CastInt2VectorToString,
    CastRangeToString,
    CeilFloat32,
    CeilFloat64,
    CeilNumeric,
    FloorFloat32,
    FloorFloat64,
    FloorNumeric,
    Ascii,
    BitCountBytes,
    BitLengthBytes,
    BitLengthString,
    ByteLengthBytes,
    ByteLengthString,
    CharLength,
    Chr,
    IsLikeMatch,
    IsRegexpMatch,
    RegexpMatch,
    ExtractInterval,
    ExtractTime,
    ExtractTimestamp,
    ExtractTimestampTz,
    ExtractDate,
    DatePartInterval,
    DatePartTime,
    DatePartTimestamp,
    DatePartTimestampTz,
    DateTruncTimestamp,
    DateTruncTimestampTz,
    TimezoneTimestamp,
    TimezoneTimestampTz,
    TimezoneTime,
    ToTimestamp,
    ToCharTimestamp,
    ToCharTimestampTz,
    JustifyDays,
    JustifyHours,
    JustifyInterval,
    JsonbArrayLength,
    JsonbTypeof,
    JsonbStripNulls,
    JsonbPretty,
    RoundFloat32,
    RoundFloat64,
    RoundNumeric,
    TruncFloat32,
    TruncFloat64,
    TruncNumeric,
    TrimWhitespace,
    TrimLeadingWhitespace,
    TrimTrailingWhitespace,
    Initcap,
    RecordGet,
    ListLength,
    MapLength,
    MapBuildFromRecordList,
    Upper,
    Lower,
    Cos,
    Acos,
    Cosh,
    Acosh,
    Sin,
    Asin,
    Sinh,
    Asinh,
    Tan,
    Atan,
    Tanh,
    Atanh,
    Cot,
    Degrees,
    Radians,
    Log10,
    Log10Numeric,
    Ln,
    LnNumeric,
    Exp,
    ExpNumeric,
    Sleep,
    Panic,
    AdjustNumericScale,
    PgColumnSize,
    MzRowSize,
    MzTypeName,
    StepMzTimestamp,
    RangeLower,
    RangeUpper,
    RangeEmpty,
    RangeLowerInc,
    RangeUpperInc,
    RangeLowerInf,
    RangeUpperInf,
    MzAclItemGrantor,
    MzAclItemGrantee,
    MzAclItemPrivileges,
    MzFormatPrivileges,
    MzValidatePrivileges,
    MzValidateRolePrivilege,
    AclItemGrantor,
    AclItemGrantee,
    AclItemPrivileges,
    QuoteIdent,
    TryParseMonotonicIso8601Timestamp,
    RegexpSplitToArray,
    PgSizePretty,
    Crc32Bytes,
    Crc32String,
    KafkaMurmur2Bytes,
    KafkaMurmur2String,
    SeahashBytes,
    SeahashString,
    Reverse
);

impl UnaryFunc {
    /// If the unary_func represents "IS X", return X.
    ///
    /// A helper method for being able to print Not(IsX) as IS NOT X.
    pub fn is(&self) -> Option<&'static str> {
        match self {
            UnaryFunc::IsNull(_) => Some("NULL"),
            UnaryFunc::IsTrue(_) => Some("TRUE"),
            UnaryFunc::IsFalse(_) => Some("FALSE"),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use mz_repr::{PropDatum, SqlScalarType};

    use crate::like_pattern;

    use super::*;

    #[mz_ore::test]
    fn test_could_error() {
        for func in [
            UnaryFunc::IsNull(IsNull),
            UnaryFunc::CastVarCharToString(CastVarCharToString),
            UnaryFunc::Not(Not),
            UnaryFunc::IsLikeMatch(IsLikeMatch(like_pattern::compile("%hi%", false).unwrap())),
        ] {
            assert!(!func.could_error())
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_is_monotone() {
        use proptest::prelude::*;

        /// Asserts that the function is either monotonically increasing or decreasing over
        /// the given sets of arguments.
        fn assert_monotone<'a, const N: usize>(
            expr: &MirScalarExpr,
            arena: &'a RowArena,
            datums: &[[Datum<'a>; N]],
        ) {
            // TODO: assertions for nulls, errors
            let Ok(results) = datums
                .iter()
                .map(|args| expr.eval(args.as_slice(), arena))
                .collect::<Result<Vec<_>, _>>()
            else {
                return;
            };

            let forward = results.iter().tuple_windows().all(|(a, b)| a <= b);
            let reverse = results.iter().tuple_windows().all(|(a, b)| a >= b);
            assert!(
                forward || reverse,
                "expected {expr} to be monotone, but passing {datums:?} returned {results:?}"
            );
        }

        fn proptest_unary<'a>(
            func: UnaryFunc,
            arena: &'a RowArena,
            arg: impl Strategy<Value = PropDatum>,
        ) {
            let is_monotone = func.is_monotone();
            let expr = MirScalarExpr::CallUnary {
                func,
                expr: Box::new(MirScalarExpr::column(0)),
            };
            if is_monotone {
                proptest!(|(
                    mut arg in proptest::array::uniform3(arg),
                )| {
                    arg.sort();
                    let args: Vec<_> = arg.iter().map(|a| [Datum::from(a)]).collect();
                    assert_monotone(&expr, arena, &args);
                });
            }
        }

        let interesting_i32s: Vec<Datum<'static>> =
            SqlScalarType::Int32.interesting_datums().collect();
        let i32_datums = proptest::strategy::Union::new([
            any::<i32>().prop_map(PropDatum::Int32).boxed(),
            (0..interesting_i32s.len())
                .prop_map(move |i| {
                    let Datum::Int32(val) = interesting_i32s[i] else {
                        unreachable!("interesting int32 has non-i32s")
                    };
                    PropDatum::Int32(val)
                })
                .boxed(),
            (-10i32..10).prop_map(PropDatum::Int32).boxed(),
        ]);

        let arena = RowArena::new();

        // It would be interesting to test all funcs here, but we currently need to hardcode
        // the generators for the argument types, which makes this tedious. Choose an interesting
        // subset for now.
        proptest_unary(
            UnaryFunc::CastInt32ToNumeric(CastInt32ToNumeric(None)),
            &arena,
            &i32_datums,
        );
        proptest_unary(
            UnaryFunc::CastInt32ToUint16(CastInt32ToUint16),
            &arena,
            &i32_datums,
        );
        proptest_unary(
            UnaryFunc::CastInt32ToString(CastInt32ToString),
            &arena,
            &i32_datums,
        );
    }
}
