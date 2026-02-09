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

pub use derive::BinaryFunc;

mod derive {
    use std::fmt;

    use mz_repr::{Datum, RowArena, SqlColumnType};

    use crate::scalar::func::binary::LazyBinaryFunc;
    use crate::scalar::func::*;
    use crate::{EvalError, MirScalarExpr};

    derive_binary! {
        AddInt16(AddInt16),
        AddInt32(AddInt32),
        AddInt64(AddInt64),
        AddUint16(AddUint16),
        AddUint32(AddUint32),
        AddUint64(AddUint64),
        AddFloat32(AddFloat32),
        AddFloat64(AddFloat64),
        AddInterval(AddInterval),
        AddTimestampInterval(AddTimestampInterval),
        AddTimestampTzInterval(AddTimestampTzInterval),
        AddDateInterval(AddDateInterval),
        AddDateTime(AddDateTime),
        AddTimeInterval(AddTimeInterval),
        AddNumeric(AddNumeric),
        AgeTimestamp(AgeTimestamp),
        AgeTimestampTz(AgeTimestampTz),
        BitAndInt16(BitAndInt16),
        BitAndInt32(BitAndInt32),
        BitAndInt64(BitAndInt64),
        BitAndUint16(BitAndUint16),
        BitAndUint32(BitAndUint32),
        BitAndUint64(BitAndUint64),
        BitOrInt16(BitOrInt16),
        BitOrInt32(BitOrInt32),
        BitOrInt64(BitOrInt64),
        BitOrUint16(BitOrUint16),
        BitOrUint32(BitOrUint32),
        BitOrUint64(BitOrUint64),
        BitXorInt16(BitXorInt16),
        BitXorInt32(BitXorInt32),
        BitXorInt64(BitXorInt64),
        BitXorUint16(BitXorUint16),
        BitXorUint32(BitXorUint32),
        BitXorUint64(BitXorUint64),
        BitShiftLeftInt16(BitShiftLeftInt16),
        BitShiftLeftInt32(BitShiftLeftInt32),
        BitShiftLeftInt64(BitShiftLeftInt64),
        BitShiftLeftUint16(BitShiftLeftUint16),
        BitShiftLeftUint32(BitShiftLeftUint32),
        BitShiftLeftUint64(BitShiftLeftUint64),
        BitShiftRightInt16(BitShiftRightInt16),
        BitShiftRightInt32(BitShiftRightInt32),
        BitShiftRightInt64(BitShiftRightInt64),
        BitShiftRightUint16(BitShiftRightUint16),
        BitShiftRightUint32(BitShiftRightUint32),
        BitShiftRightUint64(BitShiftRightUint64),
        SubInt16(SubInt16),
        SubInt32(SubInt32),
        SubInt64(SubInt64),
        SubUint16(SubUint16),
        SubUint32(SubUint32),
        SubUint64(SubUint64),
        SubFloat32(SubFloat32),
        SubFloat64(SubFloat64),
        SubInterval(SubInterval),
        SubTimestamp(SubTimestamp),
        SubTimestampTz(SubTimestampTz),
        SubTimestampInterval(SubTimestampInterval),
        SubTimestampTzInterval(SubTimestampTzInterval),
        SubDate(SubDate),
        SubDateInterval(SubDateInterval),
        SubTime(SubTime),
        SubTimeInterval(SubTimeInterval),
        SubNumeric(SubNumeric),
        MulInt16(MulInt16),
        MulInt32(MulInt32),
        MulInt64(MulInt64),
        MulUint16(MulUint16),
        MulUint32(MulUint32),
        MulUint64(MulUint64),
        MulFloat32(MulFloat32),
        MulFloat64(MulFloat64),
        MulNumeric(MulNumeric),
        MulInterval(MulInterval),
        DivInt16(DivInt16),
        DivInt32(DivInt32),
        DivInt64(DivInt64),
        DivUint16(DivUint16),
        DivUint32(DivUint32),
        DivUint64(DivUint64),
        DivFloat32(DivFloat32),
        DivFloat64(DivFloat64),
        DivNumeric(DivNumeric),
        DivInterval(DivInterval),
        ModInt16(ModInt16),
        ModInt32(ModInt32),
        ModInt64(ModInt64),
        ModUint16(ModUint16),
        ModUint32(ModUint32),
        ModUint64(ModUint64),
        ModFloat32(ModFloat32),
        ModFloat64(ModFloat64),
        ModNumeric(ModNumeric),
        RoundNumeric(RoundNumericBinary),
        Eq(Eq),
        NotEq(NotEq),
        Lt(Lt),
        Lte(Lte),
        Gt(Gt),
        Gte(Gte),
        LikeEscape(LikeEscape),
        IsLikeMatchCaseInsensitive(IsLikeMatchCaseInsensitive),
        IsLikeMatchCaseSensitive(IsLikeMatchCaseSensitive),
        IsRegexpMatchCaseSensitive(IsRegexpMatchCaseSensitive),
        IsRegexpMatchCaseInsensitive(IsRegexpMatchCaseInsensitive),
        ToCharTimestamp(ToCharTimestampFormat),
        ToCharTimestampTz(ToCharTimestampTzFormat),
        DateBinTimestamp(DateBinTimestamp),
        DateBinTimestampTz(DateBinTimestampTz),
        ExtractInterval(DatePartIntervalNumeric),
        ExtractTime(DatePartTimeNumeric),
        ExtractTimestamp(DatePartTimestampTimestampNumeric),
        ExtractTimestampTz(DatePartTimestampTimestampTzNumeric),
        ExtractDate(ExtractDateUnits),
        DatePartInterval(DatePartIntervalF64),
        DatePartTime(DatePartTimeF64),
        DatePartTimestamp(DatePartTimestampTimestampF64),
        DatePartTimestampTz(DatePartTimestampTimestampTzF64),
        DateTruncTimestamp(DateTruncUnitsTimestamp),
        DateTruncTimestampTz(DateTruncUnitsTimestampTz),
        DateTruncInterval(DateTruncInterval),
        TimezoneTimestampBinary(TimezoneTimestampBinary),
        TimezoneTimestampTzBinary(TimezoneTimestampTzBinary),
        TimezoneIntervalTimestampBinary(TimezoneIntervalTimestampBinary),
        TimezoneIntervalTimestampTzBinary(TimezoneIntervalTimestampTzBinary),
        TimezoneIntervalTimeBinary(TimezoneIntervalTimeBinary),
        TimezoneOffset(TimezoneOffset),
        TextConcat(TextConcatBinary),
        JsonbGetInt64(JsonbGetInt64),
        JsonbGetInt64Stringify(JsonbGetInt64Stringify),
        JsonbGetString(JsonbGetString),
        JsonbGetStringStringify(JsonbGetStringStringify),
        JsonbGetPath(JsonbGetPath),
        JsonbGetPathStringify(JsonbGetPathStringify),
        JsonbContainsString(JsonbContainsString),
        JsonbConcat(JsonbConcat),
        JsonbContainsJsonb(JsonbContainsJsonb),
        JsonbDeleteInt64(JsonbDeleteInt64),
        JsonbDeleteString(JsonbDeleteString),
        MapContainsKey(MapContainsKey),
        MapGetValue(MapGetValue),
        MapContainsAllKeys(MapContainsAllKeys),
        MapContainsAnyKeys(MapContainsAnyKeys),
        MapContainsMap(MapContainsMap),
        ConvertFrom(ConvertFrom),
        Left(Left),
        Position(Position),
        Right(Right),
        RepeatString(RepeatString),
        Normalize(Normalize),
        Trim(Trim),
        TrimLeading(TrimLeading),
        TrimTrailing(TrimTrailing),
        EncodedBytesCharLength(EncodedBytesCharLength),
        ListLengthMax(ListLengthMax),
        ArrayContains(ArrayContains),
        ArrayContainsArray(ArrayContainsArray),
        ArrayContainsArrayRev(ArrayContainsArrayRev),
        ArrayLength(ArrayLength),
        ArrayLower(ArrayLower),
        ArrayRemove(ArrayRemove),
        ArrayUpper(ArrayUpper),
        ArrayArrayConcat(ArrayArrayConcat),
        ListListConcat(ListListConcat),
        ListElementConcat(ListElementConcat),
        ElementListConcat(ElementListConcat),
        ListRemove(ListRemove),
        ListContainsList(ListContainsList),
        ListContainsListRev(ListContainsListRev),
        DigestString(DigestString),
        DigestBytes(DigestBytes),
        MzRenderTypmod(MzRenderTypmod),
        Encode(Encode),
        Decode(Decode),
        LogNumeric(LogBaseNumeric),
        Power(Power),
        PowerNumeric(PowerNumeric),
        GetBit(GetBit),
        GetByte(GetByte),
        ConstantTimeEqBytes(ConstantTimeEqBytes),
        ConstantTimeEqString(ConstantTimeEqString),
        RangeContainsDate(RangeContainsDate),
        RangeContainsDateRev(RangeContainsDateRev),
        RangeContainsI32(RangeContainsI32),
        RangeContainsI32Rev(RangeContainsI32Rev),
        RangeContainsI64(RangeContainsI64),
        RangeContainsI64Rev(RangeContainsI64Rev),
        RangeContainsNumeric(RangeContainsNumeric),
        RangeContainsNumericRev(RangeContainsNumericRev),
        RangeContainsRange(RangeContainsRange),
        RangeContainsRangeRev(RangeContainsRangeRev),
        RangeContainsTimestamp(RangeContainsTimestamp),
        RangeContainsTimestampRev(RangeContainsTimestampRev),
        RangeContainsTimestampTz(RangeContainsTimestampTz),
        RangeContainsTimestampTzRev(RangeContainsTimestampTzRev),
        RangeOverlaps(RangeOverlaps),
        RangeAfter(RangeAfter),
        RangeBefore(RangeBefore),
        RangeOverleft(RangeOverleft),
        RangeOverright(RangeOverright),
        RangeAdjacent(RangeAdjacent),
        RangeUnion(RangeUnion),
        RangeIntersection(RangeIntersection),
        RangeDifference(RangeDifference),
        UuidGenerateV5(UuidGenerateV5),
        MzAclItemContainsPrivilege(MzAclItemContainsPrivilege),
        ParseIdent(ParseIdent),
        PrettySql(PrettySql),
        RegexpReplace(RegexpReplace),
        StartsWith(StartsWith),
    }
}

#[cfg(test)]
mod test {
    use mz_expr_derive::sqlfunc;
    use mz_repr::SqlScalarType;

    use crate::EvalError;
    use crate::scalar::func::binary::LazyBinaryFunc;

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
    fn mz_reflect_binary_func() {
        use crate::BinaryFunc;
        use mz_lowertest::{MzReflect, ReflectedTypeInfo};

        let mut rti = ReflectedTypeInfo::default();
        BinaryFunc::add_to_reflected_type_info(&mut rti);

        // Check that the enum is registered
        let variants = rti
            .enum_dict
            .get("BinaryFunc")
            .expect("BinaryFunc should be in enum_dict");
        assert!(
            variants.contains_key("AddInt64"),
            "AddInt64 variant should exist"
        );
        assert!(variants.contains_key("Gte"), "Gte variant should exist");

        // Check that inner types are registered in struct_dict
        assert!(
            rti.struct_dict.contains_key("AddInt64"),
            "AddInt64 should be in struct_dict"
        );
        assert!(
            rti.struct_dict.contains_key("Gte"),
            "Gte should be in struct_dict"
        );

        // Verify zero-field unit structs
        let (names, types) = rti.struct_dict.get("AddInt64").unwrap();
        assert!(names.is_empty(), "AddInt64 should have no field names");
        assert!(types.is_empty(), "AddInt64 should have no field types");
    }
}
