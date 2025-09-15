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

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::adt::regex::any_regex;
use mz_repr::{ColumnType, Datum, DatumType, RowArena, ScalarType};
use proptest::prelude::*;
use proptest::strategy::*;

use crate::scalar::ProtoUnaryFunc;
use crate::scalar::func::impls::{self, *};
use crate::{EvalError, MirScalarExpr, like_pattern};

/// A description of an SQL unary function that has the ability to lazy evaluate its arguments
// This trait will eventually be annotated with #[enum_dispatch] to autogenerate the UnaryFunc enum
pub trait LazyUnaryFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError>;

    /// The output ColumnType of this function.
    fn output_type(&self, input_type: ColumnType) -> ColumnType;

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

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType;

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

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
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

/// An explicit [`Arbitrary`] implementation needed here because of a known
/// `proptest` issue.
///
/// Revert to the derive-macro implementation once the issue[^1] is fixed.
///
/// [^1]: <https://github.com/AltSysrq/proptest/issues/152>
impl Arbitrary for UnaryFunc {
    type Parameters = ();

    type Strategy = Union<BoxedStrategy<Self>>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            Not::arbitrary().prop_map_into().boxed(),
            IsNull::arbitrary().prop_map_into().boxed(),
            IsTrue::arbitrary().prop_map_into().boxed(),
            IsFalse::arbitrary().prop_map_into().boxed(),
            BitNotInt16::arbitrary().prop_map_into().boxed(),
            BitNotInt32::arbitrary().prop_map_into().boxed(),
            BitNotInt64::arbitrary().prop_map_into().boxed(),
            BitNotUint16::arbitrary().prop_map_into().boxed(),
            BitNotUint32::arbitrary().prop_map_into().boxed(),
            BitNotUint64::arbitrary().prop_map_into().boxed(),
            NegInt16::arbitrary().prop_map_into().boxed(),
            NegInt32::arbitrary().prop_map_into().boxed(),
            NegInt64::arbitrary().prop_map_into().boxed(),
            NegFloat32::arbitrary().prop_map_into().boxed(),
            NegFloat64::arbitrary().prop_map_into().boxed(),
            NegNumeric::arbitrary().prop_map_into().boxed(),
            NegInterval::arbitrary().prop_map_into().boxed(),
            SqrtFloat64::arbitrary().prop_map_into().boxed(),
            SqrtNumeric::arbitrary().prop_map_into().boxed(),
            CbrtFloat64::arbitrary().prop_map_into().boxed(),
            AbsInt16::arbitrary().prop_map_into().boxed(),
            AbsInt32::arbitrary().prop_map_into().boxed(),
            AbsInt64::arbitrary().prop_map_into().boxed(),
            AbsFloat32::arbitrary().prop_map_into().boxed(),
            AbsFloat64::arbitrary().prop_map_into().boxed(),
            AbsNumeric::arbitrary().prop_map_into().boxed(),
            CastBoolToString::arbitrary().prop_map_into().boxed(),
            CastBoolToStringNonstandard::arbitrary()
                .prop_map_into()
                .boxed(),
            CastBoolToInt32::arbitrary().prop_map_into().boxed(),
            CastBoolToInt64::arbitrary().prop_map_into().boxed(),
            CastInt16ToFloat32::arbitrary().prop_map_into().boxed(),
            CastInt16ToFloat64::arbitrary().prop_map_into().boxed(),
            CastInt16ToInt32::arbitrary().prop_map_into().boxed(),
            CastInt16ToInt64::arbitrary().prop_map_into().boxed(),
            CastInt16ToUint16::arbitrary().prop_map_into().boxed(),
            CastInt16ToUint32::arbitrary().prop_map_into().boxed(),
            CastInt16ToUint64::arbitrary().prop_map_into().boxed(),
            CastInt16ToString::arbitrary().prop_map_into().boxed(),
            CastInt2VectorToArray::arbitrary().prop_map_into().boxed(),
            CastInt32ToBool::arbitrary().prop_map_into().boxed(),
            CastInt32ToFloat32::arbitrary().prop_map_into().boxed(),
            CastInt32ToFloat64::arbitrary().prop_map_into().boxed(),
            CastInt32ToOid::arbitrary().prop_map_into().boxed(),
            CastInt32ToPgLegacyChar::arbitrary().prop_map_into().boxed(),
            CastInt32ToInt16::arbitrary().prop_map_into().boxed(),
            CastInt32ToInt64::arbitrary().prop_map_into().boxed(),
            CastInt32ToUint16::arbitrary().prop_map_into().boxed(),
            CastInt32ToUint32::arbitrary().prop_map_into().boxed(),
            CastInt32ToUint64::arbitrary().prop_map_into().boxed(),
            CastInt32ToString::arbitrary().prop_map_into().boxed(),
            CastOidToInt32::arbitrary().prop_map_into().boxed(),
            CastOidToInt64::arbitrary().prop_map_into().boxed(),
            CastOidToString::arbitrary().prop_map_into().boxed(),
            CastOidToRegClass::arbitrary().prop_map_into().boxed(),
            CastRegClassToOid::arbitrary().prop_map_into().boxed(),
            CastOidToRegProc::arbitrary().prop_map_into().boxed(),
            CastRegProcToOid::arbitrary().prop_map_into().boxed(),
            CastOidToRegType::arbitrary().prop_map_into().boxed(),
            CastRegTypeToOid::arbitrary().prop_map_into().boxed(),
            CastInt64ToInt16::arbitrary().prop_map_into().boxed(),
            CastInt64ToInt32::arbitrary().prop_map_into().boxed(),
            CastInt64ToUint16::arbitrary().prop_map_into().boxed(),
            CastInt64ToUint32::arbitrary().prop_map_into().boxed(),
            CastInt64ToUint64::arbitrary().prop_map_into().boxed(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastInt16ToNumeric(CastInt16ToNumeric(i)))
                .boxed(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastInt32ToNumeric(CastInt32ToNumeric(i)))
                .boxed(),
            CastInt64ToBool::arbitrary().prop_map_into().boxed(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastInt64ToNumeric(CastInt64ToNumeric(i)))
                .boxed(),
            CastInt64ToFloat32::arbitrary().prop_map_into().boxed(),
            CastInt64ToFloat64::arbitrary().prop_map_into().boxed(),
            CastInt64ToOid::arbitrary().prop_map_into().boxed(),
            CastInt64ToString::arbitrary().prop_map_into().boxed(),
            CastUint16ToUint32::arbitrary().prop_map_into().boxed(),
            CastUint16ToUint64::arbitrary().prop_map_into().boxed(),
            CastUint16ToInt16::arbitrary().prop_map_into().boxed(),
            CastUint16ToInt32::arbitrary().prop_map_into().boxed(),
            CastUint16ToInt64::arbitrary().prop_map_into().boxed(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastUint16ToNumeric(CastUint16ToNumeric(i)))
                .boxed(),
            CastUint16ToFloat32::arbitrary().prop_map_into().boxed(),
            CastUint16ToFloat64::arbitrary().prop_map_into().boxed(),
            CastUint16ToString::arbitrary().prop_map_into().boxed(),
            CastUint32ToUint16::arbitrary().prop_map_into().boxed(),
            CastUint32ToUint64::arbitrary().prop_map_into().boxed(),
            CastUint32ToInt32::arbitrary().prop_map_into().boxed(),
            CastUint32ToInt64::arbitrary().prop_map_into().boxed(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastUint32ToNumeric(CastUint32ToNumeric(i)))
                .boxed(),
            CastUint32ToFloat32::arbitrary().prop_map_into().boxed(),
            CastUint32ToFloat64::arbitrary().prop_map_into().boxed(),
            CastUint32ToString::arbitrary().prop_map_into().boxed(),
            CastUint64ToUint16::arbitrary().prop_map_into().boxed(),
            CastUint64ToUint32::arbitrary().prop_map_into().boxed(),
            CastUint64ToInt32::arbitrary().prop_map_into().boxed(),
            CastUint64ToInt64::arbitrary().prop_map_into().boxed(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastUint64ToNumeric(CastUint64ToNumeric(i)))
                .boxed(),
            CastUint64ToFloat32::arbitrary().prop_map_into().boxed(),
            CastUint64ToFloat64::arbitrary().prop_map_into().boxed(),
            CastUint64ToString::arbitrary().prop_map_into().boxed(),
            CastFloat32ToInt16::arbitrary().prop_map_into().boxed(),
            CastFloat32ToInt32::arbitrary().prop_map_into().boxed(),
            CastFloat32ToInt64::arbitrary().prop_map_into().boxed(),
            CastFloat32ToUint16::arbitrary().prop_map_into().boxed(),
            CastFloat32ToUint32::arbitrary().prop_map_into().boxed(),
            CastFloat32ToUint64::arbitrary().prop_map_into().boxed(),
            CastFloat32ToFloat64::arbitrary().prop_map_into().boxed(),
            CastFloat32ToString::arbitrary().prop_map_into().boxed(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastFloat32ToNumeric(CastFloat32ToNumeric(i)))
                .boxed(),
            any::<Option<NumericMaxScale>>()
                .prop_map(|i| UnaryFunc::CastFloat64ToNumeric(CastFloat64ToNumeric(i)))
                .boxed(),
            CastFloat64ToInt16::arbitrary().prop_map_into().boxed(),
            CastFloat64ToInt32::arbitrary().prop_map_into().boxed(),
            CastFloat64ToInt64::arbitrary().prop_map_into().boxed(),
            CastFloat64ToUint16::arbitrary().prop_map_into().boxed(),
            CastFloat64ToUint32::arbitrary().prop_map_into().boxed(),
            CastFloat64ToUint64::arbitrary().prop_map_into().boxed(),
            CastFloat64ToFloat32::arbitrary().prop_map_into().boxed(),
            CastFloat64ToString::arbitrary().prop_map_into().boxed(),
            CastNumericToFloat32::arbitrary().prop_map_into().boxed(),
            CastNumericToFloat64::arbitrary().prop_map_into().boxed(),
            CastNumericToInt16::arbitrary().prop_map_into().boxed(),
            CastNumericToInt32::arbitrary().prop_map_into().boxed(),
            CastNumericToInt64::arbitrary().prop_map_into().boxed(),
            CastNumericToUint16::arbitrary().prop_map_into().boxed(),
            CastNumericToUint32::arbitrary().prop_map_into().boxed(),
            CastNumericToUint64::arbitrary().prop_map_into().boxed(),
            CastNumericToString::arbitrary().prop_map_into().boxed(),
            CastStringToBool::arbitrary().prop_map_into().boxed(),
            CastStringToPgLegacyChar::arbitrary()
                .prop_map_into()
                .boxed(),
            CastStringToPgLegacyName::arbitrary()
                .prop_map_into()
                .boxed(),
            CastStringToBytes::arbitrary().prop_map_into().boxed(),
            CastStringToInt16::arbitrary().prop_map_into().boxed(),
            CastStringToInt32::arbitrary().prop_map_into().boxed(),
            CastStringToInt64::arbitrary().prop_map_into().boxed(),
            CastStringToUint16::arbitrary().prop_map_into().boxed(),
            CastStringToUint32::arbitrary().prop_map_into().boxed(),
            CastStringToUint64::arbitrary().prop_map_into().boxed(),
            CastStringToInt2Vector::arbitrary().prop_map_into().boxed(),
            CastStringToOid::arbitrary().prop_map_into().boxed(),
            CastStringToFloat32::arbitrary().prop_map_into().boxed(),
            CastStringToFloat64::arbitrary().prop_map_into().boxed(),
            CastStringToDate::arbitrary().prop_map_into().boxed(),
            (any::<ScalarType>(), any::<MirScalarExpr>())
                .prop_map(|(return_ty, expr)| {
                    UnaryFunc::CastStringToArray(CastStringToArray {
                        return_ty,
                        cast_expr: Box::new(expr),
                    })
                })
                .boxed(),
            (any::<ScalarType>(), any::<MirScalarExpr>())
                .prop_map(|(return_ty, expr)| {
                    UnaryFunc::CastStringToList(CastStringToList {
                        return_ty,
                        cast_expr: Box::new(expr),
                    })
                })
                .boxed(),
            (any::<ScalarType>(), any::<MirScalarExpr>())
                .prop_map(|(return_ty, expr)| {
                    UnaryFunc::CastStringToMap(CastStringToMap {
                        return_ty,
                        cast_expr: Box::new(expr),
                    })
                })
                .boxed(),
            (any::<ScalarType>(), any::<MirScalarExpr>())
                .prop_map(|(return_ty, expr)| {
                    UnaryFunc::CastStringToRange(CastStringToRange {
                        return_ty,
                        cast_expr: Box::new(expr),
                    })
                })
                .boxed(),
            CastStringToTime::arbitrary().prop_map_into().boxed(),
            CastStringToTimestamp::arbitrary().prop_map_into().boxed(),
            CastStringToTimestampTz::arbitrary().prop_map_into().boxed(),
            CastStringToInterval::arbitrary().prop_map_into().boxed(),
            CastStringToNumeric::arbitrary().prop_map_into().boxed(),
            CastStringToUuid::arbitrary().prop_map_into().boxed(),
            CastStringToChar::arbitrary().prop_map_into().boxed(),
            PadChar::arbitrary().prop_map_into().boxed(),
            CastStringToVarChar::arbitrary().prop_map_into().boxed(),
            CastCharToString::arbitrary().prop_map_into().boxed(),
            CastVarCharToString::arbitrary().prop_map_into().boxed(),
            CastDateToTimestamp::arbitrary().prop_map_into().boxed(),
            CastDateToTimestampTz::arbitrary().prop_map_into().boxed(),
            CastDateToString::arbitrary().prop_map_into().boxed(),
            CastTimeToInterval::arbitrary().prop_map_into().boxed(),
            CastTimeToString::arbitrary().prop_map_into().boxed(),
            CastIntervalToString::arbitrary().prop_map_into().boxed(),
            CastIntervalToTime::arbitrary().prop_map_into().boxed(),
            CastTimestampToDate::arbitrary().prop_map_into().boxed(),
            CastTimestampToTimestampTz::arbitrary()
                .prop_map_into()
                .boxed(),
            CastTimestampToString::arbitrary().prop_map_into().boxed(),
            CastTimestampToTime::arbitrary().prop_map_into().boxed(),
            CastTimestampTzToDate::arbitrary().prop_map_into().boxed(),
            CastTimestampTzToTimestamp::arbitrary()
                .prop_map_into()
                .boxed(),
            CastTimestampTzToString::arbitrary().prop_map_into().boxed(),
            CastTimestampTzToTime::arbitrary().prop_map_into().boxed(),
            CastPgLegacyCharToString::arbitrary()
                .prop_map_into()
                .boxed(),
            CastPgLegacyCharToChar::arbitrary().prop_map_into().boxed(),
            CastPgLegacyCharToVarChar::arbitrary()
                .prop_map_into()
                .boxed(),
            CastPgLegacyCharToInt32::arbitrary().prop_map_into().boxed(),
            CastBytesToString::arbitrary().prop_map_into().boxed(),
            CastStringToJsonb::arbitrary().prop_map_into().boxed(),
            CastJsonbToString::arbitrary().prop_map_into().boxed(),
            CastJsonbableToJsonb::arbitrary().prop_map_into().boxed(),
            CastJsonbToInt16::arbitrary().prop_map_into().boxed(),
            CastJsonbToInt32::arbitrary().prop_map_into().boxed(),
            CastJsonbToInt64::arbitrary().prop_map_into().boxed(),
            CastJsonbToFloat32::arbitrary().prop_map_into().boxed(),
            CastJsonbToFloat64::arbitrary().prop_map_into().boxed(),
            CastJsonbToNumeric::arbitrary().prop_map_into().boxed(),
            CastJsonbToBool::arbitrary().prop_map_into().boxed(),
            CastUuidToString::arbitrary().prop_map_into().boxed(),
            CastRecordToString::arbitrary().prop_map_into().boxed(),
            (
                any::<ScalarType>(),
                proptest::collection::vec(any::<MirScalarExpr>(), 1..5),
            )
                .prop_map(|(return_ty, cast_exprs)| {
                    UnaryFunc::CastRecord1ToRecord2(CastRecord1ToRecord2 {
                        return_ty,
                        cast_exprs: cast_exprs.into(),
                    })
                })
                .boxed(),
            CastArrayToJsonb::arbitrary().prop_map_into().boxed(),
            CastArrayToString::arbitrary().prop_map_into().boxed(),
            CastListToString::arbitrary().prop_map_into().boxed(),
            CastListToJsonb::arbitrary().prop_map_into().boxed(),
            (any::<ScalarType>(), any::<MirScalarExpr>())
                .prop_map(|(return_ty, expr)| {
                    UnaryFunc::CastList1ToList2(CastList1ToList2 {
                        return_ty,
                        cast_expr: Box::new(expr),
                    })
                })
                .boxed(),
            CastArrayToListOneDim::arbitrary().prop_map_into().boxed(),
            CastMapToString::arbitrary().prop_map_into().boxed(),
            CastInt2VectorToString::arbitrary().prop_map_into().boxed(),
            CastRangeToString::arbitrary().prop_map_into().boxed(),
            CeilFloat32::arbitrary().prop_map_into().boxed(),
            CeilFloat64::arbitrary().prop_map_into().boxed(),
            CeilNumeric::arbitrary().prop_map_into().boxed(),
            FloorFloat32::arbitrary().prop_map_into().boxed(),
            FloorFloat64::arbitrary().prop_map_into().boxed(),
            FloorNumeric::arbitrary().prop_map_into().boxed(),
            Ascii::arbitrary().prop_map_into().boxed(),
            BitCountBytes::arbitrary().prop_map_into().boxed(),
            BitLengthBytes::arbitrary().prop_map_into().boxed(),
            BitLengthString::arbitrary().prop_map_into().boxed(),
            ByteLengthBytes::arbitrary().prop_map_into().boxed(),
            ByteLengthString::arbitrary().prop_map_into().boxed(),
            CharLength::arbitrary().prop_map_into().boxed(),
            Chr::arbitrary().prop_map_into().boxed(),
            like_pattern::any_matcher()
                .prop_map(|matcher| UnaryFunc::IsLikeMatch(IsLikeMatch(matcher)))
                .boxed(),
            any_regex()
                .prop_map(|regex| UnaryFunc::IsRegexpMatch(IsRegexpMatch(regex)))
                .boxed(),
            any_regex()
                .prop_map(|regex| UnaryFunc::RegexpMatch(RegexpMatch(regex)))
                .boxed(),
            any_regex()
                .prop_map(|regex| UnaryFunc::RegexpSplitToArray(RegexpSplitToArray(regex)))
                .boxed(),
            ExtractInterval::arbitrary().prop_map_into().boxed(),
            ExtractTime::arbitrary().prop_map_into().boxed(),
            ExtractTimestamp::arbitrary().prop_map_into().boxed(),
            ExtractTimestampTz::arbitrary().prop_map_into().boxed(),
            ExtractDate::arbitrary().prop_map_into().boxed(),
            DatePartInterval::arbitrary().prop_map_into().boxed(),
            DatePartTime::arbitrary().prop_map_into().boxed(),
            DatePartTimestamp::arbitrary().prop_map_into().boxed(),
            DatePartTimestampTz::arbitrary().prop_map_into().boxed(),
            DateTruncTimestamp::arbitrary().prop_map_into().boxed(),
            DateTruncTimestampTz::arbitrary().prop_map_into().boxed(),
            TimezoneTimestamp::arbitrary().prop_map_into().boxed(),
            TimezoneTimestampTz::arbitrary().prop_map_into().boxed(),
            TimezoneTime::arbitrary().prop_map_into().boxed(),
            ToTimestamp::arbitrary().prop_map_into().boxed(),
            JustifyDays::arbitrary().prop_map_into().boxed(),
            JustifyHours::arbitrary().prop_map_into().boxed(),
            JustifyInterval::arbitrary().prop_map_into().boxed(),
            JsonbArrayLength::arbitrary().prop_map_into().boxed(),
            JsonbTypeof::arbitrary().prop_map_into().boxed(),
            JsonbStripNulls::arbitrary().prop_map_into().boxed(),
            JsonbPretty::arbitrary().prop_map_into().boxed(),
            RoundFloat32::arbitrary().prop_map_into().boxed(),
            RoundFloat64::arbitrary().prop_map_into().boxed(),
            RoundNumeric::arbitrary().prop_map_into().boxed(),
            TruncFloat32::arbitrary().prop_map_into().boxed(),
            TruncFloat64::arbitrary().prop_map_into().boxed(),
            TruncNumeric::arbitrary().prop_map_into().boxed(),
            TrimWhitespace::arbitrary().prop_map_into().boxed(),
            TrimLeadingWhitespace::arbitrary().prop_map_into().boxed(),
            TrimTrailingWhitespace::arbitrary().prop_map_into().boxed(),
            RecordGet::arbitrary().prop_map_into().boxed(),
            ListLength::arbitrary().prop_map_into().boxed(),
            (any::<ScalarType>())
                .prop_map(|value_type| {
                    UnaryFunc::MapBuildFromRecordList(MapBuildFromRecordList { value_type })
                })
                .boxed(),
            MapLength::arbitrary().prop_map_into().boxed(),
            Upper::arbitrary().prop_map_into().boxed(),
            Lower::arbitrary().prop_map_into().boxed(),
            Cos::arbitrary().prop_map_into().boxed(),
            Acos::arbitrary().prop_map_into().boxed(),
            Cosh::arbitrary().prop_map_into().boxed(),
            Acosh::arbitrary().prop_map_into().boxed(),
            Sin::arbitrary().prop_map_into().boxed(),
            Asin::arbitrary().prop_map_into().boxed(),
            Sinh::arbitrary().prop_map_into().boxed(),
            Asinh::arbitrary().prop_map_into().boxed(),
            Tan::arbitrary().prop_map_into().boxed(),
            Atan::arbitrary().prop_map_into().boxed(),
            Tanh::arbitrary().prop_map_into().boxed(),
            Atanh::arbitrary().prop_map_into().boxed(),
            Cot::arbitrary().prop_map_into().boxed(),
            Degrees::arbitrary().prop_map_into().boxed(),
            Radians::arbitrary().prop_map_into().boxed(),
            Log10::arbitrary().prop_map_into().boxed(),
            Log10Numeric::arbitrary().prop_map_into().boxed(),
            Ln::arbitrary().prop_map_into().boxed(),
            LnNumeric::arbitrary().prop_map_into().boxed(),
            Exp::arbitrary().prop_map_into().boxed(),
            ExpNumeric::arbitrary().prop_map_into().boxed(),
            Sleep::arbitrary().prop_map_into().boxed(),
            Panic::arbitrary().prop_map_into().boxed(),
            AdjustNumericScale::arbitrary().prop_map_into().boxed(),
            PgColumnSize::arbitrary().prop_map_into().boxed(),
            PgSizePretty::arbitrary().prop_map_into().boxed(),
            MzRowSize::arbitrary().prop_map_into().boxed(),
            MzTypeName::arbitrary().prop_map_into().boxed(),
            RangeLower::arbitrary().prop_map_into().boxed(),
            RangeUpper::arbitrary().prop_map_into().boxed(),
            RangeEmpty::arbitrary().prop_map_into().boxed(),
            RangeLowerInc::arbitrary().prop_map_into().boxed(),
            RangeUpperInc::arbitrary().prop_map_into().boxed(),
            RangeLowerInf::arbitrary().prop_map_into().boxed(),
            RangeUpperInf::arbitrary().prop_map_into().boxed(),
            MzAclItemGrantor::arbitrary().prop_map_into().boxed(),
            MzAclItemGrantee::arbitrary().prop_map_into().boxed(),
            MzAclItemPrivileges::arbitrary().prop_map_into().boxed(),
            MzFormatPrivileges::arbitrary().prop_map_into().boxed(),
            MzValidatePrivileges::arbitrary().prop_map_into().boxed(),
            MzValidateRolePrivilege::arbitrary().prop_map_into().boxed(),
            AclItemGrantor::arbitrary().prop_map_into().boxed(),
            AclItemGrantee::arbitrary().prop_map_into().boxed(),
            AclItemPrivileges::arbitrary().prop_map_into().boxed(),
            QuoteIdent::arbitrary().prop_map_into().boxed(),
        ])
    }
}

impl RustType<ProtoUnaryFunc> for UnaryFunc {
    fn into_proto(&self) -> ProtoUnaryFunc {
        use crate::scalar::proto_unary_func::Kind::*;
        use crate::scalar::proto_unary_func::*;
        let kind = match self {
            UnaryFunc::Not(_) => Not(()),
            UnaryFunc::IsNull(_) => IsNull(()),
            UnaryFunc::IsTrue(_) => IsTrue(()),
            UnaryFunc::IsFalse(_) => IsFalse(()),
            UnaryFunc::BitNotInt16(_) => BitNotInt16(()),
            UnaryFunc::BitNotInt32(_) => BitNotInt32(()),
            UnaryFunc::BitNotInt64(_) => BitNotInt64(()),
            UnaryFunc::BitNotUint16(_) => BitNotUint16(()),
            UnaryFunc::BitNotUint32(_) => BitNotUint32(()),
            UnaryFunc::BitNotUint64(_) => BitNotUint64(()),
            UnaryFunc::NegInt16(_) => NegInt16(()),
            UnaryFunc::NegInt32(_) => NegInt32(()),
            UnaryFunc::NegInt64(_) => NegInt64(()),
            UnaryFunc::NegFloat32(_) => NegFloat32(()),
            UnaryFunc::NegFloat64(_) => NegFloat64(()),
            UnaryFunc::NegNumeric(_) => NegNumeric(()),
            UnaryFunc::NegInterval(_) => NegInterval(()),
            UnaryFunc::SqrtFloat64(_) => SqrtFloat64(()),
            UnaryFunc::SqrtNumeric(_) => SqrtNumeric(()),
            UnaryFunc::CbrtFloat64(_) => CbrtFloat64(()),
            UnaryFunc::AbsInt16(_) => AbsInt16(()),
            UnaryFunc::AbsInt32(_) => AbsInt32(()),
            UnaryFunc::AbsInt64(_) => AbsInt64(()),
            UnaryFunc::AbsFloat32(_) => AbsFloat32(()),
            UnaryFunc::AbsFloat64(_) => AbsFloat64(()),
            UnaryFunc::AbsNumeric(_) => AbsNumeric(()),
            UnaryFunc::CastBoolToString(_) => CastBoolToString(()),
            UnaryFunc::CastBoolToStringNonstandard(_) => CastBoolToStringNonstandard(()),
            UnaryFunc::CastBoolToInt32(_) => CastBoolToInt32(()),
            UnaryFunc::CastBoolToInt64(_) => CastBoolToInt64(()),
            UnaryFunc::CastInt16ToFloat32(_) => CastInt16ToFloat32(()),
            UnaryFunc::CastInt16ToFloat64(_) => CastInt16ToFloat64(()),
            UnaryFunc::CastInt16ToInt32(_) => CastInt16ToInt32(()),
            UnaryFunc::CastInt16ToInt64(_) => CastInt16ToInt64(()),
            UnaryFunc::CastInt16ToUint16(_) => CastInt16ToUint16(()),
            UnaryFunc::CastInt16ToUint32(_) => CastInt16ToUint32(()),
            UnaryFunc::CastInt16ToUint64(_) => CastInt16ToUint64(()),
            UnaryFunc::CastInt16ToString(_) => CastInt16ToString(()),
            UnaryFunc::CastInt2VectorToArray(_) => CastInt2VectorToArray(()),
            UnaryFunc::CastInt32ToBool(_) => CastInt32ToBool(()),
            UnaryFunc::CastInt32ToFloat32(_) => CastInt32ToFloat32(()),
            UnaryFunc::CastInt32ToFloat64(_) => CastInt32ToFloat64(()),
            UnaryFunc::CastInt32ToOid(_) => CastInt32ToOid(()),
            UnaryFunc::CastInt32ToPgLegacyChar(_) => CastInt32ToPgLegacyChar(()),
            UnaryFunc::CastInt32ToInt16(_) => CastInt32ToInt16(()),
            UnaryFunc::CastInt32ToInt64(_) => CastInt32ToInt64(()),
            UnaryFunc::CastInt32ToUint16(_) => CastInt32ToUint16(()),
            UnaryFunc::CastInt32ToUint32(_) => CastInt32ToUint32(()),
            UnaryFunc::CastInt32ToUint64(_) => CastInt32ToUint64(()),
            UnaryFunc::CastInt32ToString(_) => CastInt32ToString(()),
            UnaryFunc::CastOidToInt32(_) => CastOidToInt32(()),
            UnaryFunc::CastOidToInt64(_) => CastOidToInt64(()),
            UnaryFunc::CastOidToString(_) => CastOidToString(()),
            UnaryFunc::CastOidToRegClass(_) => CastOidToRegClass(()),
            UnaryFunc::CastRegClassToOid(_) => CastRegClassToOid(()),
            UnaryFunc::CastOidToRegProc(_) => CastOidToRegProc(()),
            UnaryFunc::CastRegProcToOid(_) => CastRegProcToOid(()),
            UnaryFunc::CastOidToRegType(_) => CastOidToRegType(()),
            UnaryFunc::CastRegTypeToOid(_) => CastRegTypeToOid(()),
            UnaryFunc::CastInt64ToInt16(_) => CastInt64ToInt16(()),
            UnaryFunc::CastInt64ToInt32(_) => CastInt64ToInt32(()),
            UnaryFunc::CastInt64ToUint16(_) => CastInt64ToUint16(()),
            UnaryFunc::CastInt64ToUint32(_) => CastInt64ToUint32(()),
            UnaryFunc::CastInt64ToUint64(_) => CastInt64ToUint64(()),
            UnaryFunc::CastInt16ToNumeric(func) => CastInt16ToNumeric(func.0.into_proto()),
            UnaryFunc::CastInt32ToNumeric(func) => CastInt32ToNumeric(func.0.into_proto()),
            UnaryFunc::CastInt64ToBool(_) => CastInt64ToBool(()),
            UnaryFunc::CastInt64ToNumeric(func) => CastInt64ToNumeric(func.0.into_proto()),
            UnaryFunc::CastInt64ToFloat32(_) => CastInt64ToFloat32(()),
            UnaryFunc::CastInt64ToFloat64(_) => CastInt64ToFloat64(()),
            UnaryFunc::CastInt64ToOid(_) => CastInt64ToOid(()),
            UnaryFunc::CastInt64ToString(_) => CastInt64ToString(()),
            UnaryFunc::CastUint16ToUint32(_) => CastUint16ToUint32(()),
            UnaryFunc::CastUint16ToUint64(_) => CastUint16ToUint64(()),
            UnaryFunc::CastUint16ToInt16(_) => CastUint16ToInt16(()),
            UnaryFunc::CastUint16ToInt32(_) => CastUint16ToInt32(()),
            UnaryFunc::CastUint16ToInt64(_) => CastUint16ToInt64(()),
            UnaryFunc::CastUint16ToNumeric(func) => CastUint16ToNumeric(func.0.into_proto()),
            UnaryFunc::CastUint16ToFloat32(_) => CastUint16ToFloat32(()),
            UnaryFunc::CastUint16ToFloat64(_) => CastUint16ToFloat64(()),
            UnaryFunc::CastUint16ToString(_) => CastUint16ToString(()),
            UnaryFunc::CastUint32ToUint16(_) => CastUint32ToUint16(()),
            UnaryFunc::CastUint32ToUint64(_) => CastUint32ToUint64(()),
            UnaryFunc::CastUint32ToInt16(_) => CastUint32ToInt16(()),
            UnaryFunc::CastUint32ToInt32(_) => CastUint32ToInt32(()),
            UnaryFunc::CastUint32ToInt64(_) => CastUint32ToInt64(()),
            UnaryFunc::CastUint32ToNumeric(func) => CastUint32ToNumeric(func.0.into_proto()),
            UnaryFunc::CastUint32ToFloat32(_) => CastUint32ToFloat32(()),
            UnaryFunc::CastUint32ToFloat64(_) => CastUint32ToFloat64(()),
            UnaryFunc::CastUint32ToString(_) => CastUint32ToString(()),
            UnaryFunc::CastUint64ToUint16(_) => CastUint64ToUint16(()),
            UnaryFunc::CastUint64ToUint32(_) => CastUint64ToUint32(()),
            UnaryFunc::CastUint64ToInt16(_) => CastUint64ToInt16(()),
            UnaryFunc::CastUint64ToInt32(_) => CastUint64ToInt32(()),
            UnaryFunc::CastUint64ToInt64(_) => CastUint64ToInt64(()),
            UnaryFunc::CastUint64ToNumeric(func) => CastUint64ToNumeric(func.0.into_proto()),
            UnaryFunc::CastUint64ToFloat32(_) => CastUint64ToFloat32(()),
            UnaryFunc::CastUint64ToFloat64(_) => CastUint64ToFloat64(()),
            UnaryFunc::CastUint64ToString(_) => CastUint64ToString(()),
            UnaryFunc::CastFloat32ToInt16(_) => CastFloat32ToInt16(()),
            UnaryFunc::CastFloat32ToInt32(_) => CastFloat32ToInt32(()),
            UnaryFunc::CastFloat32ToInt64(_) => CastFloat32ToInt64(()),
            UnaryFunc::CastFloat32ToUint16(_) => CastFloat32ToUint16(()),
            UnaryFunc::CastFloat32ToUint32(_) => CastFloat32ToUint32(()),
            UnaryFunc::CastFloat32ToUint64(_) => CastFloat32ToUint64(()),
            UnaryFunc::CastFloat32ToFloat64(_) => CastFloat32ToFloat64(()),
            UnaryFunc::CastFloat32ToString(_) => CastFloat32ToString(()),
            UnaryFunc::CastFloat32ToNumeric(func) => CastFloat32ToNumeric(func.0.into_proto()),
            UnaryFunc::CastFloat64ToNumeric(func) => CastFloat64ToNumeric(func.0.into_proto()),
            UnaryFunc::CastFloat64ToInt16(_) => CastFloat64ToInt16(()),
            UnaryFunc::CastFloat64ToInt32(_) => CastFloat64ToInt32(()),
            UnaryFunc::CastFloat64ToInt64(_) => CastFloat64ToInt64(()),
            UnaryFunc::CastFloat64ToUint16(_) => CastFloat64ToUint16(()),
            UnaryFunc::CastFloat64ToUint32(_) => CastFloat64ToUint32(()),
            UnaryFunc::CastFloat64ToUint64(_) => CastFloat64ToUint64(()),
            UnaryFunc::CastFloat64ToFloat32(_) => CastFloat64ToFloat32(()),
            UnaryFunc::CastFloat64ToString(_) => CastFloat64ToString(()),
            UnaryFunc::CastNumericToFloat32(_) => CastNumericToFloat32(()),
            UnaryFunc::CastNumericToFloat64(_) => CastNumericToFloat64(()),
            UnaryFunc::CastNumericToInt16(_) => CastNumericToInt16(()),
            UnaryFunc::CastNumericToInt32(_) => CastNumericToInt32(()),
            UnaryFunc::CastNumericToInt64(_) => CastNumericToInt64(()),
            UnaryFunc::CastNumericToUint16(_) => CastNumericToUint16(()),
            UnaryFunc::CastNumericToUint32(_) => CastNumericToUint32(()),
            UnaryFunc::CastNumericToUint64(_) => CastNumericToUint64(()),
            UnaryFunc::CastNumericToString(_) => CastNumericToString(()),
            UnaryFunc::CastStringToBool(_) => CastStringToBool(()),
            UnaryFunc::CastStringToPgLegacyChar(_) => CastStringToPgLegacyChar(()),
            UnaryFunc::CastStringToPgLegacyName(_) => CastStringToPgLegacyName(()),
            UnaryFunc::CastStringToBytes(_) => CastStringToBytes(()),
            UnaryFunc::CastStringToInt16(_) => CastStringToInt16(()),
            UnaryFunc::CastStringToInt32(_) => CastStringToInt32(()),
            UnaryFunc::CastStringToInt64(_) => CastStringToInt64(()),
            UnaryFunc::CastStringToUint16(_) => CastStringToUint16(()),
            UnaryFunc::CastStringToUint32(_) => CastStringToUint32(()),
            UnaryFunc::CastStringToUint64(_) => CastStringToUint64(()),
            UnaryFunc::CastStringToInt2Vector(_) => CastStringToInt2Vector(()),
            UnaryFunc::CastStringToOid(_) => CastStringToOid(()),
            UnaryFunc::CastStringToFloat32(_) => CastStringToFloat32(()),
            UnaryFunc::CastStringToFloat64(_) => CastStringToFloat64(()),
            UnaryFunc::CastStringToDate(_) => CastStringToDate(()),
            UnaryFunc::CastStringToArray(inner) => {
                CastStringToArray(Box::new(ProtoCastToVariableType {
                    return_ty: Some(inner.return_ty.into_proto()),
                    cast_expr: Some(inner.cast_expr.into_proto()),
                }))
            }
            UnaryFunc::CastStringToList(inner) => {
                CastStringToList(Box::new(ProtoCastToVariableType {
                    return_ty: Some(inner.return_ty.into_proto()),
                    cast_expr: Some(inner.cast_expr.into_proto()),
                }))
            }
            UnaryFunc::CastStringToMap(inner) => {
                CastStringToMap(Box::new(ProtoCastToVariableType {
                    return_ty: Some(inner.return_ty.into_proto()),
                    cast_expr: Some(inner.cast_expr.into_proto()),
                }))
            }
            UnaryFunc::CastStringToRange(inner) => {
                CastStringToRange(Box::new(ProtoCastToVariableType {
                    return_ty: Some(inner.return_ty.into_proto()),
                    cast_expr: Some(inner.cast_expr.into_proto()),
                }))
            }
            UnaryFunc::CastStringToTime(_) => CastStringToTime(()),
            UnaryFunc::CastStringToTimestamp(precision) => {
                CastStringToTimestamp(precision.0.into_proto())
            }
            UnaryFunc::CastStringToTimestampTz(precision) => {
                CastStringToTimestampTz(precision.0.into_proto())
            }
            UnaryFunc::CastStringToInterval(_) => CastStringToInterval(()),
            UnaryFunc::CastStringToNumeric(func) => CastStringToNumeric(func.0.into_proto()),
            UnaryFunc::CastStringToUuid(_) => CastStringToUuid(()),
            UnaryFunc::CastStringToChar(func) => CastStringToChar(ProtoCastStringToChar {
                length: func.length.into_proto(),
                fail_on_len: func.fail_on_len,
            }),
            UnaryFunc::PadChar(func) => PadChar(ProtoPadChar {
                length: func.length.into_proto(),
            }),
            UnaryFunc::CastStringToVarChar(func) => CastStringToVarChar(ProtoCastStringToVarChar {
                length: func.length.into_proto(),
                fail_on_len: func.fail_on_len,
            }),
            UnaryFunc::CastCharToString(_) => CastCharToString(()),
            UnaryFunc::CastVarCharToString(_) => CastVarCharToString(()),
            UnaryFunc::CastDateToTimestamp(func) => CastDateToTimestamp(func.0.into_proto()),
            UnaryFunc::CastDateToTimestampTz(func) => CastDateToTimestampTz(func.0.into_proto()),
            UnaryFunc::CastDateToString(_) => CastDateToString(()),
            UnaryFunc::CastTimeToInterval(_) => CastTimeToInterval(()),
            UnaryFunc::CastTimeToString(_) => CastTimeToString(()),
            UnaryFunc::CastIntervalToString(_) => CastIntervalToString(()),
            UnaryFunc::CastIntervalToTime(_) => CastIntervalToTime(()),
            UnaryFunc::CastTimestampToDate(_) => CastTimestampToDate(()),
            UnaryFunc::AdjustTimestampPrecision(func) => Kind::AdjustTimestampPrecision(
                mz_repr::adt::timestamp::ProtoFromToTimestampPrecisions {
                    from: func.from.map(|p| p.into_proto()),
                    to: func.to.map(|p| p.into_proto()),
                },
            ),
            UnaryFunc::CastTimestampToTimestampTz(func) => CastTimestampToTimestampTz(
                mz_repr::adt::timestamp::ProtoFromToTimestampPrecisions {
                    from: func.from.map(|p| p.into_proto()),
                    to: func.to.map(|p| p.into_proto()),
                },
            ),
            UnaryFunc::CastTimestampToString(_) => CastTimestampToString(()),
            UnaryFunc::CastTimestampToTime(_) => CastTimestampToTime(()),
            UnaryFunc::CastTimestampTzToDate(_) => CastTimestampTzToDate(()),
            UnaryFunc::AdjustTimestampTzPrecision(func) => Kind::AdjustTimestampTzPrecision(
                mz_repr::adt::timestamp::ProtoFromToTimestampPrecisions {
                    from: func.from.map(|p| p.into_proto()),
                    to: func.to.map(|p| p.into_proto()),
                },
            ),
            UnaryFunc::CastTimestampTzToTimestamp(func) => CastTimestampTzToTimestamp(
                mz_repr::adt::timestamp::ProtoFromToTimestampPrecisions {
                    from: func.from.map(|p| p.into_proto()),
                    to: func.to.map(|p| p.into_proto()),
                },
            ),
            UnaryFunc::CastTimestampTzToString(_) => CastTimestampTzToString(()),
            UnaryFunc::CastTimestampTzToTime(_) => CastTimestampTzToTime(()),
            UnaryFunc::CastPgLegacyCharToString(_) => CastPgLegacyCharToString(()),
            UnaryFunc::CastPgLegacyCharToChar(_) => CastPgLegacyCharToChar(()),
            UnaryFunc::CastPgLegacyCharToVarChar(_) => CastPgLegacyCharToVarChar(()),
            UnaryFunc::CastPgLegacyCharToInt32(_) => CastPgLegacyCharToInt32(()),
            UnaryFunc::CastBytesToString(_) => CastBytesToString(()),
            UnaryFunc::CastStringToJsonb(_) => CastStringToJsonb(()),
            UnaryFunc::CastJsonbToString(_) => CastJsonbToString(()),
            UnaryFunc::CastJsonbableToJsonb(_) => CastJsonbableToJsonb(()),
            UnaryFunc::CastJsonbToInt16(_) => CastJsonbToInt16(()),
            UnaryFunc::CastJsonbToInt32(_) => CastJsonbToInt32(()),
            UnaryFunc::CastJsonbToInt64(_) => CastJsonbToInt64(()),
            UnaryFunc::CastJsonbToFloat32(_) => CastJsonbToFloat32(()),
            UnaryFunc::CastJsonbToFloat64(_) => CastJsonbToFloat64(()),
            UnaryFunc::CastJsonbToNumeric(func) => CastJsonbToNumeric(func.0.into_proto()),
            UnaryFunc::CastJsonbToBool(_) => CastJsonbToBool(()),
            UnaryFunc::CastUuidToString(_) => CastUuidToString(()),
            UnaryFunc::CastRecordToString(func) => CastRecordToString(func.ty.into_proto()),
            UnaryFunc::CastRecord1ToRecord2(inner) => {
                CastRecord1ToRecord2(ProtoCastRecord1ToRecord2 {
                    return_ty: Some(inner.return_ty.into_proto()),
                    cast_exprs: inner.cast_exprs.into_proto(),
                })
            }
            UnaryFunc::CastArrayToArray(inner) => {
                CastArrayToArray(Box::new(ProtoCastToVariableType {
                    return_ty: Some(inner.return_ty.into_proto()),
                    cast_expr: Some(inner.cast_expr.into_proto()),
                }))
            }
            UnaryFunc::CastArrayToJsonb(inner) => CastArrayToJsonb(inner.cast_element.into_proto()),
            UnaryFunc::CastArrayToString(func) => CastArrayToString(func.ty.into_proto()),
            UnaryFunc::CastListToJsonb(inner) => CastListToJsonb(inner.cast_element.into_proto()),
            UnaryFunc::CastListToString(func) => CastListToString(func.ty.into_proto()),
            UnaryFunc::CastList1ToList2(inner) => {
                CastList1ToList2(Box::new(ProtoCastToVariableType {
                    return_ty: Some(inner.return_ty.into_proto()),
                    cast_expr: Some(inner.cast_expr.into_proto()),
                }))
            }
            UnaryFunc::CastArrayToListOneDim(_) => CastArrayToListOneDim(()),
            UnaryFunc::CastMapToString(func) => CastMapToString(func.ty.into_proto()),
            UnaryFunc::CastInt2VectorToString(_) => CastInt2VectorToString(()),
            UnaryFunc::CastRangeToString(func) => CastRangeToString(func.ty.into_proto()),
            UnaryFunc::CeilFloat32(_) => CeilFloat32(()),
            UnaryFunc::CeilFloat64(_) => CeilFloat64(()),
            UnaryFunc::CeilNumeric(_) => CeilNumeric(()),
            UnaryFunc::FloorFloat32(_) => FloorFloat32(()),
            UnaryFunc::FloorFloat64(_) => FloorFloat64(()),
            UnaryFunc::FloorNumeric(_) => FloorNumeric(()),
            UnaryFunc::Ascii(_) => Ascii(()),
            UnaryFunc::BitCountBytes(_) => BitCountBytes(()),
            UnaryFunc::BitLengthBytes(_) => BitLengthBytes(()),
            UnaryFunc::BitLengthString(_) => BitLengthString(()),
            UnaryFunc::ByteLengthBytes(_) => ByteLengthBytes(()),
            UnaryFunc::ByteLengthString(_) => ByteLengthString(()),
            UnaryFunc::CharLength(_) => CharLength(()),
            UnaryFunc::Chr(_) => Chr(()),
            UnaryFunc::IsLikeMatch(pattern) => IsLikeMatch(pattern.0.into_proto()),
            UnaryFunc::IsRegexpMatch(regex) => IsRegexpMatch(regex.0.into_proto()),
            UnaryFunc::RegexpMatch(regex) => RegexpMatch(regex.0.into_proto()),
            UnaryFunc::RegexpSplitToArray(regex) => RegexpSplitToArray(regex.0.into_proto()),
            UnaryFunc::ExtractInterval(func) => ExtractInterval(func.0.into_proto()),
            UnaryFunc::ExtractTime(func) => ExtractTime(func.0.into_proto()),
            UnaryFunc::ExtractTimestamp(func) => ExtractTimestamp(func.0.into_proto()),
            UnaryFunc::ExtractTimestampTz(func) => ExtractTimestampTz(func.0.into_proto()),
            UnaryFunc::ExtractDate(func) => ExtractDate(func.0.into_proto()),
            UnaryFunc::DatePartInterval(func) => DatePartInterval(func.0.into_proto()),
            UnaryFunc::DatePartTime(func) => DatePartTime(func.0.into_proto()),
            UnaryFunc::DatePartTimestamp(func) => DatePartTimestamp(func.0.into_proto()),
            UnaryFunc::DatePartTimestampTz(func) => DatePartTimestampTz(func.0.into_proto()),
            UnaryFunc::DateTruncTimestamp(func) => DateTruncTimestamp(func.0.into_proto()),
            UnaryFunc::DateTruncTimestampTz(func) => DateTruncTimestampTz(func.0.into_proto()),
            UnaryFunc::TimezoneTimestamp(func) => TimezoneTimestamp(func.0.into_proto()),
            UnaryFunc::TimezoneTimestampTz(func) => TimezoneTimestampTz(func.0.into_proto()),
            UnaryFunc::TimezoneTime(func) => TimezoneTime(ProtoTimezoneTime {
                tz: Some(func.tz.into_proto()),
                wall_time: Some(func.wall_time.into_proto()),
            }),
            UnaryFunc::ToTimestamp(_) => ToTimestamp(()),
            UnaryFunc::ToCharTimestamp(func) => ToCharTimestamp(ProtoToCharTimestamp {
                format_string: func.format_string.into_proto(),
                format: Some(func.format.into_proto()),
            }),
            UnaryFunc::ToCharTimestampTz(func) => ToCharTimestampTz(ProtoToCharTimestamp {
                format_string: func.format_string.into_proto(),
                format: Some(func.format.into_proto()),
            }),
            UnaryFunc::JustifyDays(_) => JustifyDays(()),
            UnaryFunc::JustifyHours(_) => JustifyHours(()),
            UnaryFunc::JustifyInterval(_) => JustifyInterval(()),
            UnaryFunc::JsonbArrayLength(_) => JsonbArrayLength(()),
            UnaryFunc::JsonbTypeof(_) => JsonbTypeof(()),
            UnaryFunc::JsonbStripNulls(_) => JsonbStripNulls(()),
            UnaryFunc::JsonbPretty(_) => JsonbPretty(()),
            UnaryFunc::RoundFloat32(_) => RoundFloat32(()),
            UnaryFunc::RoundFloat64(_) => RoundFloat64(()),
            UnaryFunc::RoundNumeric(_) => RoundNumeric(()),
            UnaryFunc::TruncFloat32(_) => TruncFloat32(()),
            UnaryFunc::TruncFloat64(_) => TruncFloat64(()),
            UnaryFunc::TruncNumeric(_) => TruncNumeric(()),
            UnaryFunc::TrimWhitespace(_) => TrimWhitespace(()),
            UnaryFunc::TrimLeadingWhitespace(_) => TrimLeadingWhitespace(()),
            UnaryFunc::TrimTrailingWhitespace(_) => TrimTrailingWhitespace(()),
            UnaryFunc::Initcap(_) => Initcap(()),
            UnaryFunc::RecordGet(func) => RecordGet(func.0.into_proto()),
            UnaryFunc::ListLength(_) => ListLength(()),
            UnaryFunc::MapBuildFromRecordList(inner) => {
                MapBuildFromRecordList(inner.value_type.into_proto())
            }
            UnaryFunc::MapLength(_) => MapLength(()),
            UnaryFunc::Upper(_) => Upper(()),
            UnaryFunc::Lower(_) => Lower(()),
            UnaryFunc::Cos(_) => Cos(()),
            UnaryFunc::Acos(_) => Acos(()),
            UnaryFunc::Cosh(_) => Cosh(()),
            UnaryFunc::Acosh(_) => Acosh(()),
            UnaryFunc::Sin(_) => Sin(()),
            UnaryFunc::Asin(_) => Asin(()),
            UnaryFunc::Sinh(_) => Sinh(()),
            UnaryFunc::Asinh(_) => Asinh(()),
            UnaryFunc::Tan(_) => Tan(()),
            UnaryFunc::Atan(_) => Atan(()),
            UnaryFunc::Tanh(_) => Tanh(()),
            UnaryFunc::Atanh(_) => Atanh(()),
            UnaryFunc::Cot(_) => Cot(()),
            UnaryFunc::Degrees(_) => Degrees(()),
            UnaryFunc::Radians(_) => Radians(()),
            UnaryFunc::Log10(_) => Log10(()),
            UnaryFunc::Log10Numeric(_) => Log10Numeric(()),
            UnaryFunc::Ln(_) => Ln(()),
            UnaryFunc::LnNumeric(_) => LnNumeric(()),
            UnaryFunc::Exp(_) => Exp(()),
            UnaryFunc::ExpNumeric(_) => ExpNumeric(()),
            UnaryFunc::Sleep(_) => Sleep(()),
            UnaryFunc::Panic(_) => Panic(()),
            UnaryFunc::AdjustNumericScale(func) => AdjustNumericScale(func.0.into_proto()),
            UnaryFunc::PgColumnSize(_) => PgColumnSize(()),
            UnaryFunc::PgSizePretty(_) => PgSizePretty(()),
            UnaryFunc::MzRowSize(_) => MzRowSize(()),
            UnaryFunc::MzTypeName(_) => MzTypeName(()),
            UnaryFunc::CastMzTimestampToString(_) => CastMzTimestampToString(()),
            UnaryFunc::CastMzTimestampToTimestamp(_) => CastMzTimestampToTimestamp(()),
            UnaryFunc::CastMzTimestampToTimestampTz(_) => CastMzTimestampToTimestampTz(()),
            UnaryFunc::CastStringToMzTimestamp(_) => CastStringToMzTimestamp(()),
            UnaryFunc::CastUint64ToMzTimestamp(_) => CastUint64ToMzTimestamp(()),
            UnaryFunc::CastUint32ToMzTimestamp(_) => CastUint32ToMzTimestamp(()),
            UnaryFunc::CastInt64ToMzTimestamp(_) => CastInt64ToMzTimestamp(()),
            UnaryFunc::CastInt32ToMzTimestamp(_) => CastInt32ToMzTimestamp(()),
            UnaryFunc::CastNumericToMzTimestamp(_) => CastNumericToMzTimestamp(()),
            UnaryFunc::CastTimestampToMzTimestamp(_) => CastTimestampToMzTimestamp(()),
            UnaryFunc::CastTimestampTzToMzTimestamp(_) => CastTimestampTzToMzTimestamp(()),
            UnaryFunc::CastDateToMzTimestamp(_) => CastDateToMzTimestamp(()),
            UnaryFunc::StepMzTimestamp(_) => StepMzTimestamp(()),
            UnaryFunc::RangeLower(_) => RangeLower(()),
            UnaryFunc::RangeUpper(_) => RangeUpper(()),
            UnaryFunc::RangeEmpty(_) => RangeEmpty(()),
            UnaryFunc::RangeLowerInc(_) => RangeLowerInc(()),
            UnaryFunc::RangeUpperInc(_) => RangeUpperInc(()),
            UnaryFunc::RangeLowerInf(_) => RangeLowerInf(()),
            UnaryFunc::RangeUpperInf(_) => RangeUpperInf(()),
            UnaryFunc::MzAclItemGrantor(_) => MzAclItemGrantor(()),
            UnaryFunc::MzAclItemGrantee(_) => MzAclItemGrantee(()),
            UnaryFunc::MzAclItemPrivileges(_) => MzAclItemPrivileges(()),
            UnaryFunc::MzFormatPrivileges(_) => MzFormatPrivileges(()),
            UnaryFunc::MzValidatePrivileges(_) => MzValidatePrivileges(()),
            UnaryFunc::MzValidateRolePrivilege(_) => MzValidateRolePrivilege(()),
            UnaryFunc::AclItemGrantor(_) => AclItemGrantor(()),
            UnaryFunc::AclItemGrantee(_) => AclItemGrantee(()),
            UnaryFunc::AclItemPrivileges(_) => AclItemPrivileges(()),
            UnaryFunc::QuoteIdent(_) => QuoteIdent(()),
            UnaryFunc::TryParseMonotonicIso8601Timestamp(_) => {
                TryParseMonotonicIso8601Timestamp(())
            }
            UnaryFunc::Crc32Bytes(_) => Crc32Bytes(()),
            UnaryFunc::Crc32String(_) => Crc32String(()),
            UnaryFunc::KafkaMurmur2Bytes(_) => KafkaMurmur2Bytes(()),
            UnaryFunc::KafkaMurmur2String(_) => KafkaMurmur2String(()),
            UnaryFunc::SeahashBytes(_) => SeahashBytes(()),
            UnaryFunc::SeahashString(_) => SeahashString(()),
            UnaryFunc::Reverse(_) => Reverse(()),
        };
        ProtoUnaryFunc { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoUnaryFunc) -> Result<Self, TryFromProtoError> {
        use crate::scalar::proto_unary_func::Kind::*;
        if let Some(kind) = proto.kind {
            match kind {
                Not(()) => Ok(impls::Not.into()),
                IsNull(()) => Ok(impls::IsNull.into()),
                IsTrue(()) => Ok(impls::IsTrue.into()),
                IsFalse(()) => Ok(impls::IsFalse.into()),
                BitNotInt16(()) => Ok(impls::BitNotInt16.into()),
                BitNotInt32(()) => Ok(impls::BitNotInt32.into()),
                BitNotInt64(()) => Ok(impls::BitNotInt64.into()),
                BitNotUint16(()) => Ok(impls::BitNotUint16.into()),
                BitNotUint32(()) => Ok(impls::BitNotUint32.into()),
                BitNotUint64(()) => Ok(impls::BitNotUint64.into()),
                NegInt16(()) => Ok(impls::NegInt16.into()),
                NegInt32(()) => Ok(impls::NegInt32.into()),
                NegInt64(()) => Ok(impls::NegInt64.into()),
                NegFloat32(()) => Ok(impls::NegFloat32.into()),
                NegFloat64(()) => Ok(impls::NegFloat64.into()),
                NegNumeric(()) => Ok(impls::NegNumeric.into()),
                NegInterval(()) => Ok(impls::NegInterval.into()),
                SqrtFloat64(()) => Ok(impls::SqrtFloat64.into()),
                SqrtNumeric(()) => Ok(impls::SqrtNumeric.into()),
                CbrtFloat64(()) => Ok(impls::CbrtFloat64.into()),
                AbsInt16(()) => Ok(impls::AbsInt16.into()),
                AbsInt32(()) => Ok(impls::AbsInt32.into()),
                AbsInt64(()) => Ok(impls::AbsInt64.into()),
                AbsFloat32(()) => Ok(impls::AbsFloat32.into()),
                AbsFloat64(()) => Ok(impls::AbsFloat64.into()),
                AbsNumeric(()) => Ok(impls::AbsNumeric.into()),
                CastBoolToString(()) => Ok(impls::CastBoolToString.into()),
                CastBoolToStringNonstandard(()) => Ok(impls::CastBoolToStringNonstandard.into()),
                CastBoolToInt32(()) => Ok(impls::CastBoolToInt32.into()),
                CastBoolToInt64(()) => Ok(impls::CastBoolToInt64.into()),
                CastInt16ToFloat32(()) => Ok(impls::CastInt16ToFloat32.into()),
                CastInt16ToFloat64(()) => Ok(impls::CastInt16ToFloat64.into()),
                CastInt16ToInt32(()) => Ok(impls::CastInt16ToInt32.into()),
                CastInt16ToInt64(()) => Ok(impls::CastInt16ToInt64.into()),
                CastInt16ToUint16(()) => Ok(impls::CastInt16ToUint16.into()),
                CastInt16ToUint32(()) => Ok(impls::CastInt16ToUint32.into()),
                CastInt16ToUint64(()) => Ok(impls::CastInt16ToUint64.into()),
                CastInt16ToString(()) => Ok(impls::CastInt16ToString.into()),
                CastInt2VectorToArray(()) => Ok(impls::CastInt2VectorToArray.into()),
                CastInt32ToBool(()) => Ok(impls::CastInt32ToBool.into()),
                CastInt32ToFloat32(()) => Ok(impls::CastInt32ToFloat32.into()),
                CastInt32ToFloat64(()) => Ok(impls::CastInt32ToFloat64.into()),
                CastInt32ToOid(()) => Ok(impls::CastInt32ToOid.into()),
                CastInt32ToPgLegacyChar(()) => Ok(impls::CastInt32ToPgLegacyChar.into()),
                CastInt32ToInt16(()) => Ok(impls::CastInt32ToInt16.into()),
                CastInt32ToInt64(()) => Ok(impls::CastInt32ToInt64.into()),
                CastInt32ToUint16(()) => Ok(impls::CastInt32ToUint16.into()),
                CastInt32ToUint32(()) => Ok(impls::CastInt32ToUint32.into()),
                CastInt32ToUint64(()) => Ok(impls::CastInt32ToUint64.into()),
                CastInt32ToString(()) => Ok(impls::CastInt32ToString.into()),
                CastOidToInt32(()) => Ok(impls::CastOidToInt32.into()),
                CastOidToInt64(()) => Ok(impls::CastOidToInt64.into()),
                CastOidToString(()) => Ok(impls::CastOidToString.into()),
                CastOidToRegClass(()) => Ok(impls::CastOidToRegClass.into()),
                CastRegClassToOid(()) => Ok(impls::CastRegClassToOid.into()),
                CastOidToRegProc(()) => Ok(impls::CastOidToRegProc.into()),
                CastRegProcToOid(()) => Ok(impls::CastRegProcToOid.into()),
                CastOidToRegType(()) => Ok(impls::CastOidToRegType.into()),
                CastRegTypeToOid(()) => Ok(impls::CastRegTypeToOid.into()),
                CastInt64ToInt16(()) => Ok(impls::CastInt64ToInt16.into()),
                CastInt64ToInt32(()) => Ok(impls::CastInt64ToInt32.into()),
                CastInt64ToUint16(()) => Ok(impls::CastInt64ToUint16.into()),
                CastInt64ToUint32(()) => Ok(impls::CastInt64ToUint32.into()),
                CastInt64ToUint64(()) => Ok(impls::CastInt64ToUint64.into()),
                CastInt16ToNumeric(max_scale) => {
                    Ok(impls::CastInt16ToNumeric(max_scale.into_rust()?).into())
                }
                CastInt32ToNumeric(max_scale) => {
                    Ok(impls::CastInt32ToNumeric(max_scale.into_rust()?).into())
                }
                CastInt64ToBool(()) => Ok(impls::CastInt64ToBool.into()),
                CastInt64ToNumeric(max_scale) => {
                    Ok(impls::CastInt64ToNumeric(max_scale.into_rust()?).into())
                }
                CastInt64ToFloat32(()) => Ok(impls::CastInt64ToFloat32.into()),
                CastInt64ToFloat64(()) => Ok(impls::CastInt64ToFloat64.into()),
                CastInt64ToOid(()) => Ok(impls::CastInt64ToOid.into()),
                CastInt64ToString(()) => Ok(impls::CastInt64ToString.into()),
                CastUint16ToUint32(()) => Ok(impls::CastUint16ToUint32.into()),
                CastUint16ToUint64(()) => Ok(impls::CastUint16ToUint64.into()),
                CastUint16ToInt16(()) => Ok(impls::CastUint16ToInt16.into()),
                CastUint16ToInt32(()) => Ok(impls::CastUint16ToInt32.into()),
                CastUint16ToInt64(()) => Ok(impls::CastUint16ToInt64.into()),
                CastUint16ToNumeric(max_scale) => {
                    Ok(impls::CastUint16ToNumeric(max_scale.into_rust()?).into())
                }
                CastUint16ToFloat32(()) => Ok(impls::CastUint16ToFloat32.into()),
                CastUint16ToFloat64(()) => Ok(impls::CastUint16ToFloat64.into()),
                CastUint16ToString(()) => Ok(impls::CastUint16ToString.into()),
                CastUint32ToUint16(()) => Ok(impls::CastUint32ToUint16.into()),
                CastUint32ToUint64(()) => Ok(impls::CastUint32ToUint64.into()),
                CastUint32ToInt16(()) => Ok(impls::CastUint32ToInt16.into()),
                CastUint32ToInt32(()) => Ok(impls::CastUint32ToInt32.into()),
                CastUint32ToInt64(()) => Ok(impls::CastUint32ToInt64.into()),
                CastUint32ToNumeric(max_scale) => {
                    Ok(impls::CastUint32ToNumeric(max_scale.into_rust()?).into())
                }
                CastUint32ToFloat32(()) => Ok(impls::CastUint32ToFloat32.into()),
                CastUint32ToFloat64(()) => Ok(impls::CastUint32ToFloat64.into()),
                CastUint32ToString(()) => Ok(impls::CastUint32ToString.into()),
                CastUint64ToUint16(()) => Ok(impls::CastUint64ToUint16.into()),
                CastUint64ToUint32(()) => Ok(impls::CastUint64ToUint32.into()),
                CastUint64ToInt16(()) => Ok(impls::CastUint64ToInt16.into()),
                CastUint64ToInt32(()) => Ok(impls::CastUint64ToInt32.into()),
                CastUint64ToInt64(()) => Ok(impls::CastUint64ToInt64.into()),
                CastUint64ToNumeric(max_scale) => {
                    Ok(impls::CastUint64ToNumeric(max_scale.into_rust()?).into())
                }
                CastUint64ToFloat32(()) => Ok(impls::CastUint64ToFloat32.into()),
                CastUint64ToFloat64(()) => Ok(impls::CastUint64ToFloat64.into()),
                CastUint64ToString(()) => Ok(impls::CastUint64ToString.into()),
                CastFloat32ToInt16(()) => Ok(impls::CastFloat32ToInt16.into()),
                CastFloat32ToInt32(()) => Ok(impls::CastFloat32ToInt32.into()),
                CastFloat32ToInt64(()) => Ok(impls::CastFloat32ToInt64.into()),
                CastFloat32ToUint16(()) => Ok(impls::CastFloat32ToUint16.into()),
                CastFloat32ToUint32(()) => Ok(impls::CastFloat32ToUint32.into()),
                CastFloat32ToUint64(()) => Ok(impls::CastFloat32ToUint64.into()),
                CastFloat32ToFloat64(()) => Ok(impls::CastFloat32ToFloat64.into()),
                CastFloat32ToString(()) => Ok(impls::CastFloat32ToString.into()),
                CastFloat32ToNumeric(max_scale) => {
                    Ok(impls::CastFloat32ToNumeric(max_scale.into_rust()?).into())
                }
                CastFloat64ToNumeric(max_scale) => {
                    Ok(impls::CastFloat64ToNumeric(max_scale.into_rust()?).into())
                }
                CastFloat64ToInt16(()) => Ok(impls::CastFloat64ToInt16.into()),
                CastFloat64ToInt32(()) => Ok(impls::CastFloat64ToInt32.into()),
                CastFloat64ToInt64(()) => Ok(impls::CastFloat64ToInt64.into()),
                CastFloat64ToUint16(()) => Ok(impls::CastFloat64ToUint16.into()),
                CastFloat64ToUint32(()) => Ok(impls::CastFloat64ToUint32.into()),
                CastFloat64ToUint64(()) => Ok(impls::CastFloat64ToUint64.into()),
                CastFloat64ToFloat32(()) => Ok(impls::CastFloat64ToFloat32.into()),
                CastFloat64ToString(()) => Ok(impls::CastFloat64ToString.into()),
                CastNumericToFloat32(()) => Ok(impls::CastNumericToFloat32.into()),
                CastNumericToFloat64(()) => Ok(impls::CastNumericToFloat64.into()),
                CastNumericToInt16(()) => Ok(impls::CastNumericToInt16.into()),
                CastNumericToInt32(()) => Ok(impls::CastNumericToInt32.into()),
                CastNumericToInt64(()) => Ok(impls::CastNumericToInt64.into()),
                CastNumericToUint16(()) => Ok(impls::CastNumericToUint16.into()),
                CastNumericToUint32(()) => Ok(impls::CastNumericToUint32.into()),
                CastNumericToUint64(()) => Ok(impls::CastNumericToUint64.into()),
                CastNumericToString(()) => Ok(impls::CastNumericToString.into()),
                CastStringToBool(()) => Ok(impls::CastStringToBool.into()),
                CastStringToPgLegacyChar(()) => Ok(impls::CastStringToPgLegacyChar.into()),
                CastStringToPgLegacyName(()) => Ok(impls::CastStringToPgLegacyName.into()),
                CastStringToBytes(()) => Ok(impls::CastStringToBytes.into()),
                CastStringToInt16(()) => Ok(impls::CastStringToInt16.into()),
                CastStringToInt32(()) => Ok(impls::CastStringToInt32.into()),
                CastStringToInt64(()) => Ok(impls::CastStringToInt64.into()),
                CastStringToUint16(()) => Ok(impls::CastStringToUint16.into()),
                CastStringToUint32(()) => Ok(impls::CastStringToUint32.into()),
                CastStringToUint64(()) => Ok(impls::CastStringToUint64.into()),
                CastStringToInt2Vector(()) => Ok(impls::CastStringToInt2Vector.into()),
                CastStringToOid(()) => Ok(impls::CastStringToOid.into()),
                CastStringToFloat32(()) => Ok(impls::CastStringToFloat32.into()),
                CastStringToFloat64(()) => Ok(impls::CastStringToFloat64.into()),
                CastStringToDate(()) => Ok(impls::CastStringToDate.into()),
                CastStringToArray(inner) => Ok(impls::CastStringToArray {
                    return_ty: inner
                        .return_ty
                        .into_rust_if_some("ProtoCastStringToArray::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .into_rust_if_some("ProtoCastStringToArray::cast_expr")?,
                }
                .into()),
                CastStringToList(inner) => Ok(impls::CastStringToList {
                    return_ty: inner
                        .return_ty
                        .into_rust_if_some("ProtoCastStringToList::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .into_rust_if_some("ProtoCastStringToList::cast_expr")?,
                }
                .into()),
                CastStringToRange(inner) => Ok(impls::CastStringToRange {
                    return_ty: inner
                        .return_ty
                        .into_rust_if_some("ProtoCastStringToRange::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .into_rust_if_some("ProtoCastStringToRange::cast_expr")?,
                }
                .into()),
                CastStringToMap(inner) => Ok(impls::CastStringToMap {
                    return_ty: inner
                        .return_ty
                        .into_rust_if_some("ProtoCastStringToMap::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .into_rust_if_some("ProtoCastStringToMap::cast_expr")?,
                }
                .into()),
                CastStringToTime(()) => Ok(impls::CastStringToTime.into()),
                CastStringToTimestamp(precision) => {
                    Ok(impls::CastStringToTimestamp(precision.into_rust()?).into())
                }
                CastStringToTimestampTz(precision) => {
                    Ok(impls::CastStringToTimestampTz(precision.into_rust()?).into())
                }
                CastStringToInterval(()) => Ok(impls::CastStringToInterval.into()),
                CastStringToNumeric(max_scale) => {
                    Ok(impls::CastStringToNumeric(max_scale.into_rust()?).into())
                }
                CastStringToUuid(()) => Ok(impls::CastStringToUuid.into()),
                CastStringToChar(func) => Ok(impls::CastStringToChar {
                    length: func.length.into_rust()?,
                    fail_on_len: func.fail_on_len,
                }
                .into()),
                PadChar(func) => Ok(impls::PadChar {
                    length: func.length.into_rust()?,
                }
                .into()),
                CastStringToVarChar(func) => Ok(impls::CastStringToVarChar {
                    length: func.length.into_rust()?,
                    fail_on_len: func.fail_on_len,
                }
                .into()),
                CastCharToString(()) => Ok(impls::CastCharToString.into()),
                CastVarCharToString(()) => Ok(impls::CastVarCharToString.into()),
                CastDateToTimestamp(precision) => {
                    Ok(impls::CastDateToTimestamp(precision.into_rust()?).into())
                }
                CastDateToTimestampTz(precision) => {
                    Ok(impls::CastDateToTimestampTz(precision.into_rust()?).into())
                }
                CastDateToString(()) => Ok(impls::CastDateToString.into()),
                CastTimeToInterval(()) => Ok(impls::CastTimeToInterval.into()),
                CastTimeToString(()) => Ok(impls::CastTimeToString.into()),
                CastIntervalToString(()) => Ok(impls::CastIntervalToString.into()),
                CastIntervalToTime(()) => Ok(impls::CastIntervalToTime.into()),
                CastTimestampToDate(()) => Ok(impls::CastTimestampToDate.into()),
                AdjustTimestampPrecision(precisions) => Ok(impls::AdjustTimestampPrecision {
                    from: precisions.from.into_rust()?,
                    to: precisions.to.into_rust()?,
                }
                .into()),
                CastTimestampToTimestampTz(precisions) => Ok(impls::CastTimestampToTimestampTz {
                    from: precisions.from.into_rust()?,
                    to: precisions.to.into_rust()?,
                }
                .into()),
                CastTimestampToString(()) => Ok(impls::CastTimestampToString.into()),
                CastTimestampToTime(()) => Ok(impls::CastTimestampToTime.into()),
                CastTimestampTzToDate(()) => Ok(impls::CastTimestampTzToDate.into()),
                CastTimestampTzToTimestamp(precisions) => Ok(impls::CastTimestampTzToTimestamp {
                    from: precisions.from.into_rust()?,
                    to: precisions.to.into_rust()?,
                }
                .into()),
                AdjustTimestampTzPrecision(precisions) => Ok(impls::AdjustTimestampTzPrecision {
                    from: precisions.from.into_rust()?,
                    to: precisions.to.into_rust()?,
                }
                .into()),
                CastTimestampTzToString(()) => Ok(impls::CastTimestampTzToString.into()),
                CastTimestampTzToTime(()) => Ok(impls::CastTimestampTzToTime.into()),
                CastPgLegacyCharToString(()) => Ok(impls::CastPgLegacyCharToString.into()),
                CastPgLegacyCharToChar(()) => Ok(impls::CastPgLegacyCharToChar.into()),
                CastPgLegacyCharToVarChar(()) => Ok(impls::CastPgLegacyCharToVarChar.into()),
                CastPgLegacyCharToInt32(()) => Ok(impls::CastPgLegacyCharToInt32.into()),
                CastBytesToString(()) => Ok(impls::CastBytesToString.into()),
                CastStringToJsonb(()) => Ok(impls::CastStringToJsonb.into()),
                CastJsonbToString(()) => Ok(impls::CastJsonbToString.into()),
                CastJsonbableToJsonb(()) => Ok(impls::CastJsonbableToJsonb.into()),
                CastJsonbToInt16(()) => Ok(impls::CastJsonbToInt16.into()),
                CastJsonbToInt32(()) => Ok(impls::CastJsonbToInt32.into()),
                CastJsonbToInt64(()) => Ok(impls::CastJsonbToInt64.into()),
                CastJsonbToFloat32(()) => Ok(impls::CastJsonbToFloat32.into()),
                CastJsonbToFloat64(()) => Ok(impls::CastJsonbToFloat64.into()),
                CastJsonbToNumeric(max_scale) => {
                    Ok(impls::CastJsonbToNumeric(max_scale.into_rust()?).into())
                }
                CastJsonbToBool(()) => Ok(impls::CastJsonbToBool.into()),
                CastUuidToString(()) => Ok(impls::CastUuidToString.into()),
                CastRecordToString(ty) => Ok(impls::CastRecordToString {
                    ty: ty.into_rust()?,
                }
                .into()),
                CastRecord1ToRecord2(inner) => Ok(impls::CastRecord1ToRecord2 {
                    return_ty: inner
                        .return_ty
                        .into_rust_if_some("ProtoCastRecord1ToRecord2::return_ty")?,
                    cast_exprs: inner.cast_exprs.into_rust()?,
                }
                .into()),
                CastArrayToArray(inner) => Ok(impls::CastArrayToArray {
                    return_ty: inner
                        .return_ty
                        .into_rust_if_some("ProtoCastArrayToArray::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .into_rust_if_some("ProtoCastArrayToArray::cast_expr")?,
                }
                .into()),
                CastArrayToJsonb(cast_element) => Ok(impls::CastArrayToJsonb {
                    cast_element: cast_element.into_rust()?,
                }
                .into()),
                CastArrayToString(ty) => Ok(impls::CastArrayToString {
                    ty: ty.into_rust()?,
                }
                .into()),
                CastListToJsonb(cast_element) => Ok(impls::CastListToJsonb {
                    cast_element: cast_element.into_rust()?,
                }
                .into()),
                CastListToString(ty) => Ok(impls::CastListToString {
                    ty: ty.into_rust()?,
                }
                .into()),
                CastList1ToList2(inner) => Ok(impls::CastList1ToList2 {
                    return_ty: inner
                        .return_ty
                        .into_rust_if_some("ProtoCastList1ToList2::return_ty")?,
                    cast_expr: inner
                        .cast_expr
                        .into_rust_if_some("ProtoCastList1ToList2::cast_expr")?,
                }
                .into()),
                CastArrayToListOneDim(()) => Ok(impls::CastArrayToListOneDim.into()),
                CastMapToString(ty) => Ok(impls::CastMapToString {
                    ty: ty.into_rust()?,
                }
                .into()),
                CastInt2VectorToString(_) => Ok(impls::CastInt2VectorToString.into()),
                CastRangeToString(ty) => Ok(impls::CastRangeToString {
                    ty: ty.into_rust()?,
                }
                .into()),
                CeilFloat32(_) => Ok(impls::CeilFloat32.into()),
                CeilFloat64(_) => Ok(impls::CeilFloat64.into()),
                CeilNumeric(_) => Ok(impls::CeilNumeric.into()),
                FloorFloat32(_) => Ok(impls::FloorFloat32.into()),
                FloorFloat64(_) => Ok(impls::FloorFloat64.into()),
                FloorNumeric(_) => Ok(impls::FloorNumeric.into()),
                Ascii(_) => Ok(impls::Ascii.into()),
                BitCountBytes(_) => Ok(impls::BitCountBytes.into()),
                BitLengthBytes(_) => Ok(impls::BitLengthBytes.into()),
                BitLengthString(_) => Ok(impls::BitLengthString.into()),
                ByteLengthBytes(_) => Ok(impls::ByteLengthBytes.into()),
                ByteLengthString(_) => Ok(impls::ByteLengthString.into()),
                CharLength(_) => Ok(impls::CharLength.into()),
                Chr(_) => Ok(impls::Chr.into()),
                IsLikeMatch(pattern) => Ok(impls::IsLikeMatch(pattern.into_rust()?).into()),
                IsRegexpMatch(regex) => Ok(impls::IsRegexpMatch(regex.into_rust()?).into()),
                RegexpMatch(regex) => Ok(impls::RegexpMatch(regex.into_rust()?).into()),
                RegexpSplitToArray(regex) => {
                    Ok(impls::RegexpSplitToArray(regex.into_rust()?).into())
                }
                ExtractInterval(units) => Ok(impls::ExtractInterval(units.into_rust()?).into()),
                ExtractTime(units) => Ok(impls::ExtractTime(units.into_rust()?).into()),
                ExtractTimestamp(units) => Ok(impls::ExtractTimestamp(units.into_rust()?).into()),
                ExtractTimestampTz(units) => {
                    Ok(impls::ExtractTimestampTz(units.into_rust()?).into())
                }
                ExtractDate(units) => Ok(impls::ExtractDate(units.into_rust()?).into()),
                DatePartInterval(units) => Ok(impls::DatePartInterval(units.into_rust()?).into()),
                DatePartTime(units) => Ok(impls::DatePartTime(units.into_rust()?).into()),
                DatePartTimestamp(units) => Ok(impls::DatePartTimestamp(units.into_rust()?).into()),
                DatePartTimestampTz(units) => {
                    Ok(impls::DatePartTimestampTz(units.into_rust()?).into())
                }
                DateTruncTimestamp(units) => {
                    Ok(impls::DateTruncTimestamp(units.into_rust()?).into())
                }
                DateTruncTimestampTz(units) => {
                    Ok(impls::DateTruncTimestampTz(units.into_rust()?).into())
                }
                TimezoneTimestamp(tz) => Ok(impls::TimezoneTimestamp(tz.into_rust()?).into()),
                TimezoneTimestampTz(tz) => Ok(impls::TimezoneTimestampTz(tz.into_rust()?).into()),
                TimezoneTime(func) => Ok(impls::TimezoneTime {
                    tz: func.tz.into_rust_if_some("ProtoTimezoneTime::tz")?,
                    wall_time: func
                        .wall_time
                        .into_rust_if_some("ProtoTimezoneTime::wall_time")?,
                }
                .into()),
                ToTimestamp(()) => Ok(impls::ToTimestamp.into()),
                ToCharTimestamp(func) => Ok(impls::ToCharTimestamp {
                    format_string: func.format_string,
                    format: func
                        .format
                        .into_rust_if_some("ProtoToCharTimestamp::format")?,
                }
                .into()),
                ToCharTimestampTz(func) => Ok(impls::ToCharTimestampTz {
                    format_string: func.format_string,
                    format: func
                        .format
                        .into_rust_if_some("ProtoToCharTimestamp::format")?,
                }
                .into()),
                JustifyDays(()) => Ok(impls::JustifyDays.into()),
                JustifyHours(()) => Ok(impls::JustifyHours.into()),
                JustifyInterval(()) => Ok(impls::JustifyInterval.into()),
                JsonbArrayLength(()) => Ok(impls::JsonbArrayLength.into()),
                JsonbTypeof(()) => Ok(impls::JsonbTypeof.into()),
                JsonbStripNulls(()) => Ok(impls::JsonbStripNulls.into()),
                JsonbPretty(()) => Ok(impls::JsonbPretty.into()),
                RoundFloat32(()) => Ok(impls::RoundFloat32.into()),
                RoundFloat64(()) => Ok(impls::RoundFloat64.into()),
                RoundNumeric(()) => Ok(impls::RoundNumeric.into()),
                TruncFloat32(()) => Ok(impls::TruncFloat32.into()),
                TruncFloat64(()) => Ok(impls::TruncFloat64.into()),
                TruncNumeric(()) => Ok(impls::TruncNumeric.into()),
                TrimWhitespace(()) => Ok(impls::TrimWhitespace.into()),
                TrimLeadingWhitespace(()) => Ok(impls::TrimLeadingWhitespace.into()),
                TrimTrailingWhitespace(()) => Ok(impls::TrimTrailingWhitespace.into()),
                Initcap(()) => Ok(impls::Initcap.into()),
                RecordGet(field) => Ok(impls::RecordGet(field.into_rust()?).into()),
                ListLength(()) => Ok(impls::ListLength.into()),
                MapBuildFromRecordList(value_type) => Ok(impls::MapBuildFromRecordList {
                    value_type: value_type.into_rust()?,
                }
                .into()),
                MapLength(()) => Ok(impls::MapLength.into()),
                Upper(()) => Ok(impls::Upper.into()),
                Lower(()) => Ok(impls::Lower.into()),
                Cos(()) => Ok(impls::Cos.into()),
                Acos(()) => Ok(impls::Acos.into()),
                Cosh(()) => Ok(impls::Cosh.into()),
                Acosh(()) => Ok(impls::Acosh.into()),
                Sin(()) => Ok(impls::Sin.into()),
                Asin(()) => Ok(impls::Asin.into()),
                Sinh(()) => Ok(impls::Sinh.into()),
                Asinh(()) => Ok(impls::Asinh.into()),
                Tan(()) => Ok(impls::Tan.into()),
                Atan(()) => Ok(impls::Atan.into()),
                Tanh(()) => Ok(impls::Tanh.into()),
                Atanh(()) => Ok(impls::Atanh.into()),
                Cot(()) => Ok(impls::Cot.into()),
                Degrees(()) => Ok(impls::Degrees.into()),
                Radians(()) => Ok(impls::Radians.into()),
                Log10(()) => Ok(impls::Log10.into()),
                Log10Numeric(()) => Ok(impls::Log10Numeric.into()),
                Ln(()) => Ok(impls::Ln.into()),
                LnNumeric(()) => Ok(impls::LnNumeric.into()),
                Exp(()) => Ok(impls::Exp.into()),
                ExpNumeric(()) => Ok(impls::ExpNumeric.into()),
                Sleep(()) => Ok(impls::Sleep.into()),
                Panic(()) => Ok(impls::Panic.into()),
                AdjustNumericScale(max_scale) => {
                    Ok(impls::AdjustNumericScale(max_scale.into_rust()?).into())
                }
                PgColumnSize(()) => Ok(impls::PgColumnSize.into()),
                PgSizePretty(()) => Ok(impls::PgSizePretty.into()),
                MzRowSize(()) => Ok(impls::MzRowSize.into()),
                MzTypeName(()) => Ok(impls::MzTypeName.into()),

                CastMzTimestampToString(()) => Ok(impls::CastMzTimestampToString.into()),
                CastMzTimestampToTimestamp(()) => Ok(impls::CastMzTimestampToTimestamp.into()),
                CastMzTimestampToTimestampTz(()) => Ok(impls::CastMzTimestampToTimestampTz.into()),
                CastStringToMzTimestamp(()) => Ok(impls::CastStringToMzTimestamp.into()),
                CastUint64ToMzTimestamp(()) => Ok(impls::CastUint64ToMzTimestamp.into()),
                CastUint32ToMzTimestamp(()) => Ok(impls::CastUint32ToMzTimestamp.into()),
                CastInt64ToMzTimestamp(()) => Ok(impls::CastInt64ToMzTimestamp.into()),
                CastInt32ToMzTimestamp(()) => Ok(impls::CastInt32ToMzTimestamp.into()),
                CastNumericToMzTimestamp(()) => Ok(impls::CastNumericToMzTimestamp.into()),
                CastTimestampToMzTimestamp(()) => Ok(impls::CastTimestampToMzTimestamp.into()),
                CastTimestampTzToMzTimestamp(()) => Ok(impls::CastTimestampTzToMzTimestamp.into()),
                CastDateToMzTimestamp(()) => Ok(impls::CastDateToMzTimestamp.into()),
                StepMzTimestamp(()) => Ok(impls::StepMzTimestamp.into()),
                RangeLower(()) => Ok(impls::RangeLower.into()),
                RangeUpper(()) => Ok(impls::RangeUpper.into()),
                RangeEmpty(()) => Ok(impls::RangeEmpty.into()),
                RangeLowerInc(_) => Ok(impls::RangeLowerInc.into()),
                RangeUpperInc(_) => Ok(impls::RangeUpperInc.into()),
                RangeLowerInf(_) => Ok(impls::RangeLowerInf.into()),
                RangeUpperInf(_) => Ok(impls::RangeUpperInf.into()),
                MzAclItemGrantor(_) => Ok(impls::MzAclItemGrantor.into()),
                MzAclItemGrantee(_) => Ok(impls::MzAclItemGrantee.into()),
                MzAclItemPrivileges(_) => Ok(impls::MzAclItemPrivileges.into()),
                MzFormatPrivileges(_) => Ok(impls::MzFormatPrivileges.into()),
                MzValidatePrivileges(_) => Ok(impls::MzValidatePrivileges.into()),
                MzValidateRolePrivilege(_) => Ok(impls::MzValidateRolePrivilege.into()),
                AclItemGrantor(_) => Ok(impls::AclItemGrantor.into()),
                AclItemGrantee(_) => Ok(impls::AclItemGrantee.into()),
                AclItemPrivileges(_) => Ok(impls::AclItemPrivileges.into()),
                QuoteIdent(_) => Ok(impls::QuoteIdent.into()),
                TryParseMonotonicIso8601Timestamp(_) => {
                    Ok(impls::TryParseMonotonicIso8601Timestamp.into())
                }
                Crc32Bytes(()) => Ok(impls::Crc32Bytes.into()),
                Crc32String(()) => Ok(impls::Crc32String.into()),
                KafkaMurmur2Bytes(()) => Ok(impls::KafkaMurmur2Bytes.into()),
                KafkaMurmur2String(()) => Ok(impls::KafkaMurmur2String.into()),
                SeahashBytes(()) => Ok(impls::SeahashBytes.into()),
                SeahashString(()) => Ok(impls::SeahashString.into()),
                Reverse(()) => Ok(impls::Reverse.into()),
            }
        } else {
            Err(TryFromProtoError::missing_field("ProtoUnaryFunc::kind"))
        }
    }
}

impl IntoRustIfSome<UnaryFunc> for Option<Box<ProtoUnaryFunc>> {
    fn into_rust_if_some<S: ToString>(self, field: S) -> Result<UnaryFunc, TryFromProtoError> {
        let value = self.ok_or_else(|| TryFromProtoError::missing_field(field))?;
        (*value).into_rust()
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use mz_repr::PropDatum;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn unary_func_protobuf_roundtrip(expect in any::<UnaryFunc>()) {
            let actual = protobuf_roundtrip::<_, ProtoUnaryFunc>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }

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
            ScalarType::Int32.interesting_datums().collect();
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
