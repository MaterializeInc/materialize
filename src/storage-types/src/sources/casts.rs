// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Storage-specific scalar expression and cast function types, decoupled from
//! `MirScalarExpr` to avoid a dependency on the compute layer.

use std::borrow::Cow;

use mz_expr::EvalError;
use mz_ore::cast::CastFrom;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::char::{CharLength, format_str_trim};
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::numeric::{self, NumericMaxScale};
use mz_repr::adt::system::{Oid, PgLegacyChar};
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{Datum, ReprColumnType, Row, RowArena, SqlScalarType, strconv};
use serde::{Deserialize, Serialize};

/// A scalar expression used in storage contexts, covering only the subset of
/// operations needed for string-to-column casts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageScalarExpr {
    /// A reference to a column by index.
    Column(usize),
    /// A literal value together with its column type.
    Literal(Row, ReprColumnType),
    /// A unary function application.
    CallUnary(CastFunc, Box<StorageScalarExpr>),
    /// Return an error if the inner expression evaluates to null.
    ErrorIfNull(Box<StorageScalarExpr>, String),
}

/// Cast functions from string to a typed value, mirroring the subset of
/// `mz_expr::UnaryFunc` variants used when casting source columns.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CastFunc {
    CastStringToBool,
    CastStringToPgLegacyChar,
    CastStringToPgLegacyName,
    CastStringToBytes,
    CastStringToInt16,
    CastStringToInt32,
    CastStringToInt64,
    CastStringToFloat32,
    CastStringToFloat64,
    CastStringToOid,
    CastStringToUint16,
    CastStringToUint32,
    CastStringToUint64,
    CastStringToDate,
    CastStringToTime,
    CastStringToInterval,
    CastStringToUuid,
    CastStringToJsonb,
    CastStringToMzTimestamp,
    CastStringToInt2Vector,
    CastStringToNumeric(Option<NumericMaxScale>),
    CastStringToTimestamp(Option<TimestampPrecision>),
    CastStringToTimestampTz(Option<TimestampPrecision>),
    CastStringToChar {
        length: Option<CharLength>,
        fail_on_len: bool,
    },
    CastStringToVarChar {
        length: Option<VarCharMaxLength>,
        fail_on_len: bool,
    },
    CastStringToArray {
        return_ty: SqlScalarType,
        cast_expr: Box<StorageScalarExpr>,
    },
    CastStringToList {
        return_ty: SqlScalarType,
        cast_expr: Box<StorageScalarExpr>,
    },
    CastStringToMap {
        return_ty: SqlScalarType,
        cast_expr: Box<StorageScalarExpr>,
    },
    CastStringToRange {
        return_ty: SqlScalarType,
        cast_expr: Box<StorageScalarExpr>,
    },
}

impl StorageScalarExpr {
    /// Evaluate this expression against the given datums, using `arena` for
    /// temporary allocations.
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        arena: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        match self {
            StorageScalarExpr::Column(i) => Ok(datums[*i]),
            StorageScalarExpr::Literal(row, _typ) => Ok(row.unpack_first()),
            StorageScalarExpr::CallUnary(func, expr) => {
                let datum = expr.eval(datums, arena)?;
                if datum.is_null() {
                    return Ok(Datum::Null);
                }
                func.eval(datum, arena)
            }
            StorageScalarExpr::ErrorIfNull(expr, message) => {
                let datum = expr.eval(datums, arena)?;
                if datum.is_null() {
                    Err(EvalError::IfNullError(message.clone().into()))
                } else {
                    Ok(datum)
                }
            }
        }
    }
}

/// Convert a [`strconv::ParseError`] to an [`EvalError`].
///
/// This helper exists to disambiguate `From` conversions: `EvalError` has
/// many `From<_>` impls, so `EvalError::from` as a function pointer is
/// ambiguous. Providing a concrete function removes the ambiguity.
fn parse_err(e: strconv::ParseError) -> EvalError {
    EvalError::from(e)
}

impl CastFunc {
    /// Evaluate this cast function on a non-null datum.
    ///
    /// The implementations mirror the `CastStringTo*` functions from
    /// `mz_expr::scalar::func::impls::string`, producing bit-for-bit
    /// identical results.
    pub fn eval<'a>(
        &'a self,
        datum: Datum<'a>,
        arena: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        let a = datum.unwrap_str();
        match self {
            // Simple eager casts that delegate to strconv::parse_*.
            CastFunc::CastStringToBool => {
                Ok(Datum::from(strconv::parse_bool(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToPgLegacyChar => Ok(Datum::UInt8(
                PgLegacyChar(a.as_bytes().first().copied().unwrap_or(0)).0,
            )),
            CastFunc::CastStringToPgLegacyName => Ok(Datum::String(
                arena.push_string(strconv::parse_pg_legacy_name(a)),
            )),
            CastFunc::CastStringToBytes => {
                let bytes: Vec<u8> = strconv::parse_bytes(a).map_err(parse_err)?;
                Ok(Datum::Bytes(arena.push_bytes(bytes)))
            }
            CastFunc::CastStringToInt16 => {
                Ok(Datum::Int16(strconv::parse_int16(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToInt32 => {
                Ok(Datum::Int32(strconv::parse_int32(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToInt64 => {
                Ok(Datum::Int64(strconv::parse_int64(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToFloat32 => {
                let f: f32 = strconv::parse_float32(a).map_err(parse_err)?;
                Ok(Datum::Float32(f.into()))
            }
            CastFunc::CastStringToFloat64 => {
                let f: f64 = strconv::parse_float64(a).map_err(parse_err)?;
                Ok(Datum::Float64(f.into()))
            }
            CastFunc::CastStringToOid => Ok(Datum::UInt32(Oid(strconv::parse_oid(a)?).0)),
            CastFunc::CastStringToUint16 => {
                Ok(Datum::UInt16(strconv::parse_uint16(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToUint32 => {
                Ok(Datum::UInt32(strconv::parse_uint32(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToUint64 => {
                Ok(Datum::UInt64(strconv::parse_uint64(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToDate => {
                Ok(Datum::Date(strconv::parse_date(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToTime => {
                Ok(Datum::Time(strconv::parse_time(a).map_err(parse_err)?))
            }
            CastFunc::CastStringToInterval => Ok(Datum::Interval(
                strconv::parse_interval(a).map_err(parse_err)?,
            )),
            CastFunc::CastStringToUuid => {
                Ok(Datum::Uuid(strconv::parse_uuid(a).map_err(parse_err)?))
            }
            // TODO(jamii): it would be much more efficient to skip the
            // intermediate repr::jsonb::Jsonb.
            CastFunc::CastStringToJsonb => {
                let jsonb: Jsonb = strconv::parse_jsonb(a)?;
                Ok(arena.push_unary_row(jsonb.into_row()))
            }
            CastFunc::CastStringToMzTimestamp => Ok(Datum::MzTimestamp(
                strconv::parse_mz_timestamp(a).map_err(parse_err)?,
            )),

            // Parameterized eager casts.
            CastFunc::CastStringToNumeric(scale) => {
                let mut d = strconv::parse_numeric(a)?;
                if let Some(scale) = scale {
                    if numeric::rescale(&mut d.0, scale.into_u8()).is_err() {
                        return Err(EvalError::NumericFieldOverflow);
                    }
                }
                Ok(Datum::from(d.into_inner()))
            }
            CastFunc::CastStringToTimestamp(precision) => {
                let out = strconv::parse_timestamp(a)?;
                let updated = out.round_to_precision(*precision)?;
                Ok(Datum::Timestamp(updated))
            }
            CastFunc::CastStringToTimestampTz(precision) => {
                let out = strconv::parse_timestamptz(a)?;
                let updated = out.round_to_precision(*precision)?;
                Ok(Datum::TimestampTz(updated))
            }
            CastFunc::CastStringToChar {
                length,
                fail_on_len,
            } => {
                let s = format_str_trim(a, *length, *fail_on_len).map_err(|_| {
                    assert!(*fail_on_len);
                    EvalError::StringValueTooLong {
                        target_type: "character".into(),
                        length: usize::cast_from(length.unwrap().into_u32()),
                    }
                })?;
                Ok(Datum::String(arena.push_string(s)))
            }
            CastFunc::CastStringToVarChar {
                length,
                fail_on_len,
            } => {
                let s =
                    mz_repr::adt::varchar::format_str(a, *length, *fail_on_len).map_err(|_| {
                        assert!(*fail_on_len);
                        EvalError::StringValueTooLong {
                            target_type: "character varying".into(),
                            length: usize::cast_from(length.unwrap().into_u32()),
                        }
                    })?;
                Ok(Datum::String(s))
            }

            // Recursive lazy casts for container types.
            CastFunc::CastStringToArray {
                return_ty: _,
                cast_expr,
            } => {
                let (datums, dims) = strconv::parse_array(
                    a,
                    || Datum::Null,
                    |elem_text| {
                        let elem_text = match elem_text {
                            Cow::Owned(s) => arena.push_string(s),
                            Cow::Borrowed(s) => s,
                        };
                        cast_expr.eval(&[Datum::String(elem_text)], arena)
                    },
                )?;
                Ok(arena.try_make_datum(|packer| packer.try_push_array(&dims, datums))?)
            }
            CastFunc::CastStringToList {
                return_ty,
                cast_expr,
            } => {
                let parsed_datums = strconv::parse_list(
                    a,
                    matches!(
                        return_ty.unwrap_list_element_type(),
                        SqlScalarType::List { .. }
                    ),
                    || Datum::Null,
                    |elem_text| {
                        let elem_text = match elem_text {
                            Cow::Owned(s) => arena.push_string(s),
                            Cow::Borrowed(s) => s,
                        };
                        cast_expr.eval(&[Datum::String(elem_text)], arena)
                    },
                )?;
                Ok(arena.make_datum(|packer| packer.push_list(parsed_datums)))
            }
            CastFunc::CastStringToMap {
                return_ty,
                cast_expr,
            } => {
                let parsed_map = strconv::parse_map(
                    a,
                    matches!(return_ty.unwrap_map_value_type(), SqlScalarType::Map { .. }),
                    |value_text| -> Result<Datum, EvalError> {
                        let value_text = match value_text {
                            Some(Cow::Owned(s)) => Datum::String(arena.push_string(s)),
                            Some(Cow::Borrowed(s)) => Datum::String(s),
                            None => Datum::Null,
                        };
                        cast_expr.eval(&[value_text], arena)
                    },
                )?;
                let mut pairs: Vec<(String, Datum)> =
                    parsed_map.into_iter().map(|(k, v)| (k, v)).collect();
                pairs.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));
                pairs.dedup_by(|(k1, _v1), (k2, _v2)| k1 == k2);
                Ok(arena.make_datum(|packer| {
                    packer.push_dict_with(|packer| {
                        for (k, v) in pairs {
                            packer.push(Datum::String(&k));
                            packer.push(v);
                        }
                    })
                }))
            }
            CastFunc::CastStringToRange {
                return_ty: _,
                cast_expr,
            } => {
                let mut range = strconv::parse_range(a, |elem_text| {
                    let elem_text = match elem_text {
                        Cow::Owned(s) => arena.push_string(s),
                        Cow::Borrowed(s) => s,
                    };
                    cast_expr.eval(&[Datum::String(elem_text)], arena)
                })?;
                range.canonicalize()?;
                Ok(arena.make_datum(|packer| {
                    packer
                        .push_range(range)
                        .expect("must have already handled errors")
                }))
            }
            CastFunc::CastStringToInt2Vector => {
                let datums =
                    strconv::parse_legacy_vector(a, |elem_text| -> Result<Datum, EvalError> {
                        let elem_text = match elem_text {
                            Cow::Owned(s) => arena.push_string(s),
                            Cow::Borrowed(s) => s,
                        };
                        // Int2Vector elements are always cast from string to int16.
                        let i: i16 = strconv::parse_int16(elem_text).map_err(parse_err)?;
                        Ok(Datum::Int16(i))
                    })?;
                // Construct a one-dimensional array from the parsed elements,
                // matching array_create_scalar from mz_expr.
                let mut dims = &[ArrayDimension {
                    lower_bound: 1,
                    length: datums.len(),
                }][..];
                if datums.is_empty() {
                    // Per PostgreSQL, empty arrays are represented with zero
                    // dimensions, not one dimension of zero length.
                    dims = &[];
                }
                Ok(arena.try_make_datum(|packer| packer.try_push_array(dims, &datums))?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::EvalError;
    use mz_repr::adt::char::CharLength;
    use mz_repr::adt::numeric::NumericMaxScale;
    use mz_repr::adt::timestamp::TimestampPrecision;
    use mz_repr::adt::varchar::VarCharMaxLength;
    use mz_repr::{Datum, RowArena};

    use super::*;

    // Helper: build a CallUnary expression that casts Column(0) with the given func.
    fn cast_col0(func: CastFunc) -> StorageScalarExpr {
        StorageScalarExpr::CallUnary(func, Box::new(StorageScalarExpr::Column(0)))
    }

    // --- Simple scalar casts ---

    #[mz_ore::test]
    fn test_cast_string_to_bool() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToBool);
        assert_eq!(
            expr.eval(&[Datum::String("true")], &arena).unwrap(),
            Datum::True
        );
        assert_eq!(
            expr.eval(&[Datum::String("false")], &arena).unwrap(),
            Datum::False
        );
    }

    #[mz_ore::test]
    fn test_cast_string_to_int16() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToInt16);
        assert_eq!(
            expr.eval(&[Datum::String("32767")], &arena).unwrap(),
            Datum::Int16(32767)
        );
    }

    #[mz_ore::test]
    fn test_cast_string_to_int32() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToInt32);
        assert_eq!(
            expr.eval(&[Datum::String("42")], &arena).unwrap(),
            Datum::Int32(42)
        );
    }

    #[mz_ore::test]
    fn test_cast_string_to_int64() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToInt64);
        assert_eq!(
            expr.eval(&[Datum::String("-9000000000")], &arena).unwrap(),
            Datum::Int64(-9_000_000_000)
        );
    }

    #[mz_ore::test]
    fn test_cast_string_to_float32() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToFloat32);
        let result = expr.eval(&[Datum::String("1.5")], &arena).unwrap();
        match result {
            Datum::Float32(f) => assert!((f.into_inner() - 1.5_f32).abs() < f32::EPSILON),
            other => panic!("expected Float32, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_cast_string_to_float64() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToFloat64);
        // Use 2.5, which is exactly representable in IEEE 754.
        let result = expr.eval(&[Datum::String("2.5")], &arena).unwrap();
        match result {
            Datum::Float64(f) => assert!((f.into_inner() - 2.5_f64).abs() < f64::EPSILON),
            other => panic!("expected Float64, got {:?}", other),
        }
    }

    #[mz_ore::test]
    fn test_cast_string_to_date() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToDate);
        // Parsing should succeed; just verify it's a Date datum.
        let result = expr.eval(&[Datum::String("2024-01-15")], &arena).unwrap();
        assert!(matches!(result, Datum::Date(_)));
    }

    #[mz_ore::test]
    fn test_cast_string_to_uuid() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToUuid);
        let result = expr
            .eval(
                &[Datum::String("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")],
                &arena,
            )
            .unwrap();
        assert!(matches!(result, Datum::Uuid(_)));
    }

    #[mz_ore::test]
    fn test_cast_string_to_bytes() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToBytes);
        // "\\x4142" is the hex escape for "AB"
        let result = expr.eval(&[Datum::String("\\x4142")], &arena).unwrap();
        assert_eq!(result, Datum::Bytes(&[0x41, 0x42]));
    }

    #[mz_ore::test]
    fn test_cast_string_to_jsonb() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToJsonb);
        let result = expr.eval(&[Datum::String("{\"key\": 1}")], &arena).unwrap();
        // JSONB is stored as a nested datum; just verify it's not null/error.
        assert!(!result.is_null());
    }

    // --- Parameterized casts ---

    #[mz_ore::test]
    fn test_cast_string_to_numeric_no_scale() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToNumeric(None));
        let result = expr.eval(&[Datum::String("123.456")], &arena).unwrap();
        assert!(matches!(result, Datum::Numeric(_)));
    }

    #[mz_ore::test]
    fn test_cast_string_to_numeric_with_scale() {
        let arena = RowArena::new();
        let scale = NumericMaxScale::try_from(2_i64).unwrap();
        let expr = cast_col0(CastFunc::CastStringToNumeric(Some(scale)));
        // "123.456" rounded to scale 2 => "123.46"
        let result = expr.eval(&[Datum::String("123.456")], &arena).unwrap();
        assert!(matches!(result, Datum::Numeric(_)));
    }

    #[mz_ore::test]
    fn test_cast_string_to_timestamp_no_precision() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToTimestamp(None));
        let result = expr
            .eval(&[Datum::String("2024-01-15 12:34:56")], &arena)
            .unwrap();
        assert!(matches!(result, Datum::Timestamp(_)));
    }

    #[mz_ore::test]
    fn test_cast_string_to_timestamp_with_precision() {
        let arena = RowArena::new();
        let precision = TimestampPrecision::try_from(0_i64).unwrap();
        let expr = cast_col0(CastFunc::CastStringToTimestamp(Some(precision)));
        // Precision 0 truncates sub-second part.
        let result = expr
            .eval(&[Datum::String("2024-01-15 12:34:56.789")], &arena)
            .unwrap();
        assert!(matches!(result, Datum::Timestamp(_)));
    }

    #[mz_ore::test]
    fn test_cast_string_to_char_with_length() {
        let arena = RowArena::new();
        let length = CharLength::try_from(5_i64).unwrap();
        let expr = cast_col0(CastFunc::CastStringToChar {
            length: Some(length),
            fail_on_len: false,
        });
        // format_str_trim stores the trimmed string (no trailing space padding)
        // in the Datum; padding is only added at display time via format_str_pad.
        let result = expr.eval(&[Datum::String("hi")], &arena).unwrap();
        assert_eq!(result, Datum::String("hi"));
    }

    #[mz_ore::test]
    fn test_cast_string_to_varchar_with_length() {
        let arena = RowArena::new();
        let length = VarCharMaxLength::try_from(3_i64).unwrap();
        let expr = cast_col0(CastFunc::CastStringToVarChar {
            length: Some(length),
            fail_on_len: false,
        });
        // "hello" truncated to 3.
        let result = expr.eval(&[Datum::String("hello")], &arena).unwrap();
        assert_eq!(result, Datum::String("hel"));
    }

    // --- Null propagation ---

    #[mz_ore::test]
    fn test_call_unary_null_propagation() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToInt32);
        // Null input must propagate to Null output without error.
        let result = expr.eval(&[Datum::Null], &arena).unwrap();
        assert_eq!(result, Datum::Null);
    }

    // --- ErrorIfNull ---

    #[mz_ore::test]
    fn test_error_if_null_fires_on_null() {
        let arena = RowArena::new();
        let expr = StorageScalarExpr::ErrorIfNull(
            Box::new(StorageScalarExpr::Column(0)),
            "column must not be null".to_string(),
        );
        let err = expr.eval(&[Datum::Null], &arena).unwrap_err();
        assert!(
            matches!(err, EvalError::IfNullError(_)),
            "expected IfNullError, got {:?}",
            err
        );
    }

    #[mz_ore::test]
    fn test_error_if_null_passes_through_non_null() {
        let arena = RowArena::new();
        let expr = StorageScalarExpr::ErrorIfNull(
            Box::new(StorageScalarExpr::Column(0)),
            "should not fire".to_string(),
        );
        let result = expr.eval(&[Datum::Int32(7)], &arena).unwrap();
        assert_eq!(result, Datum::Int32(7));
    }

    // --- Error cases ---

    #[mz_ore::test]
    fn test_cast_invalid_int32_returns_error() {
        let arena = RowArena::new();
        let expr = cast_col0(CastFunc::CastStringToInt32);
        let result = expr.eval(&[Datum::String("not_a_number")], &arena);
        assert!(result.is_err(), "expected error for invalid int32 input");
    }

    #[mz_ore::test]
    fn test_cast_string_to_char_fail_on_len() {
        let arena = RowArena::new();
        let length = CharLength::try_from(3_i64).unwrap();
        let expr = cast_col0(CastFunc::CastStringToChar {
            length: Some(length),
            fail_on_len: true,
        });
        // "toolong" exceeds length 3 and fail_on_len is true.
        let result = expr.eval(&[Datum::String("toolong")], &arena);
        assert!(
            matches!(result, Err(EvalError::StringValueTooLong { .. })),
            "expected StringValueTooLong, got {:?}",
            result
        );
    }

    // --- Literal ---

    #[mz_ore::test]
    fn test_literal_unpacks_correctly() {
        use mz_repr::{ReprColumnType, ReprScalarType, Row};
        let mut row = Row::default();
        row.packer().push(Datum::Int32(99));
        let expr = StorageScalarExpr::Literal(
            row,
            ReprColumnType {
                scalar_type: ReprScalarType::Int32,
                nullable: false,
            },
        );
        let arena = RowArena::new();
        let result = expr.eval(&[], &arena).unwrap();
        assert_eq!(result, Datum::Int32(99));
    }

    // --- Column ---

    #[mz_ore::test]
    fn test_column_extracts_correct_datum() {
        let arena = RowArena::new();
        let expr = StorageScalarExpr::Column(2);
        let datums = [Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)];
        let result = expr.eval(&datums, &arena).unwrap();
        assert_eq!(result, Datum::Int32(2));
    }

    // --- Error snapshot tests: hardcode expected EvalError variants.
    // These survive after parity tests are removed.

    mod error_snapshots {
        use mz_expr::EvalError;
        use mz_repr::strconv::{ParseError, ParseErrorKind};
        use mz_repr::{Datum, RowArena};

        use super::*;

        fn eval_cast_err(func: CastFunc, input: &str) -> EvalError {
            let arena = RowArena::new();
            let expr = StorageScalarExpr::CallUnary(func, Box::new(StorageScalarExpr::Column(0)));
            expr.eval(&[Datum::String(input)], &arena)
                .expect_err("expected error")
        }

        fn parse_err(type_name: &'static str, input: &str) -> EvalError {
            EvalError::Parse(ParseError {
                kind: ParseErrorKind::InvalidInputSyntax,
                type_name: type_name.into(),
                input: input.into(),
                details: None,
            })
        }

        fn parse_err_with_details(
            type_name: &'static str,
            input: &str,
            details: &str,
        ) -> EvalError {
            EvalError::Parse(ParseError {
                kind: ParseErrorKind::InvalidInputSyntax,
                type_name: type_name.into(),
                input: input.into(),
                details: Some(details.into()),
            })
        }

        #[mz_ore::test]
        fn error_bool() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToBool, "bad"),
                parse_err("boolean", "bad"),
            );
        }

        #[mz_ore::test]
        fn error_int16() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToInt16, "bad"),
                parse_err_with_details("smallint", "bad", "invalid digit found in string"),
            );
        }

        #[mz_ore::test]
        fn error_int32() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToInt32, "bad"),
                parse_err_with_details("integer", "bad", "invalid digit found in string"),
            );
        }

        #[mz_ore::test]
        fn error_int64() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToInt64, "bad"),
                parse_err_with_details("bigint", "bad", "invalid digit found in string"),
            );
        }

        #[mz_ore::test]
        fn error_float32() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToFloat32, "bad"),
                parse_err("real", "bad"),
            );
        }

        #[mz_ore::test]
        fn error_float64() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToFloat64, "bad"),
                parse_err("double precision", "bad"),
            );
        }

        #[mz_ore::test]
        fn error_date() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToDate, "bad"),
                parse_err_with_details("date", "bad", "YEAR, MONTH, DAY are all required"),
            );
        }

        #[mz_ore::test]
        fn error_time() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToTime, "bad"),
                parse_err_with_details("time", "bad", "unknown units bad"),
            );
        }

        #[mz_ore::test]
        fn error_timestamp() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToTimestamp(None), "bad"),
                parse_err_with_details("timestamp", "bad", "YEAR, MONTH, DAY are all required"),
            );
        }

        #[mz_ore::test]
        fn error_timestamptz() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToTimestampTz(None), "bad"),
                parse_err_with_details(
                    "timestamp with time zone",
                    "bad",
                    "YEAR, MONTH, DAY are all required"
                ),
            );
        }

        #[mz_ore::test]
        fn error_interval() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToInterval, "bad"),
                parse_err_with_details("interval", "bad", "unknown units bad"),
            );
        }

        #[mz_ore::test]
        fn error_uuid() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToUuid, "bad"),
                parse_err_with_details(
                    "uuid",
                    "bad",
                    "invalid length: expected length 32 for simple format, found 3"
                ),
            );
        }

        #[mz_ore::test]
        fn error_numeric() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToNumeric(None), "bad"),
                parse_err("numeric", "bad"),
            );
        }

        #[mz_ore::test]
        fn error_bytes() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToBytes, "\\xZZ"),
                parse_err_with_details("bytea", "\\xZZ", "invalid hexadecimal digit: \"Z\""),
            );
        }

        #[mz_ore::test]
        fn error_jsonb() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToJsonb, "{bad"),
                parse_err_with_details("jsonb", "{bad", "key must be a string at line 1 column 2"),
            );
        }

        #[mz_ore::test]
        fn error_uint16() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToUint16, "-1"),
                parse_err_with_details("uint2", "-1", "invalid digit found in string"),
            );
        }

        #[mz_ore::test]
        fn error_uint32() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToUint32, "-1"),
                parse_err_with_details("uint4", "-1", "invalid digit found in string"),
            );
        }

        #[mz_ore::test]
        fn error_uint64() {
            assert_eq!(
                eval_cast_err(CastFunc::CastStringToUint64, "-1"),
                parse_err_with_details("uint8", "-1", "invalid digit found in string"),
            );
        }

        #[mz_ore::test]
        fn error_char_too_long() {
            let length = mz_repr::adt::char::CharLength::try_from(3_i64).unwrap();
            assert_eq!(
                eval_cast_err(
                    CastFunc::CastStringToChar {
                        length: Some(length),
                        fail_on_len: true,
                    },
                    "toolong"
                ),
                EvalError::StringValueTooLong {
                    target_type: "character".into(),
                    length: 3,
                },
            );
        }

        #[mz_ore::test]
        fn error_varchar_too_long() {
            let length = mz_repr::adt::varchar::VarCharMaxLength::try_from(3_i64).unwrap();
            assert_eq!(
                eval_cast_err(
                    CastFunc::CastStringToVarChar {
                        length: Some(length),
                        fail_on_len: true,
                    },
                    "toolong"
                ),
                EvalError::StringValueTooLong {
                    target_type: "character varying".into(),
                    length: 3,
                },
            );
        }

        #[mz_ore::test]
        fn error_if_null() {
            let arena = RowArena::new();
            let expr = StorageScalarExpr::ErrorIfNull(
                Box::new(StorageScalarExpr::Column(0)),
                "col must not be null".to_string(),
            );
            assert_eq!(
                expr.eval(&[Datum::Null], &arena).unwrap_err(),
                EvalError::IfNullError("col must not be null".into()),
            );
        }
    }

    // --- Parity tests: StorageScalarExpr must produce identical results
    // (Ok values and Err variants) as MirScalarExpr for the same inputs.

    mod parity {
        use mz_expr::{MirScalarExpr, UnaryFunc};
        use mz_repr::{Datum, RowArena};

        use super::*;

        /// Assert both produce structurally equal results for all inputs.
        fn assert_parity(name: &str, storage_func: CastFunc, mir_func: UnaryFunc, inputs: &[&str]) {
            let arena = RowArena::new();

            let s_expr =
                StorageScalarExpr::CallUnary(storage_func, Box::new(StorageScalarExpr::Column(0)));
            let m_expr = MirScalarExpr::CallUnary {
                func: mir_func,
                expr: Box::new(MirScalarExpr::column(0)),
            };

            for &input in inputs {
                let s = s_expr.eval(&[Datum::String(input)], &arena);
                let m = m_expr.eval(&[Datum::String(input)], &arena);
                assert_eq!(s, m, "{name}: divergent for input {input:?}");
            }
            // Also check null propagation.
            let s = s_expr.eval(&[Datum::Null], &arena);
            let m = m_expr.eval(&[Datum::Null], &arena);
            assert_eq!(s, m, "{name}: divergent for Null");
        }

        #[mz_ore::test]
        fn parity_bool() {
            use mz_expr::func::CastStringToBool;
            assert_parity(
                "Bool",
                CastFunc::CastStringToBool,
                UnaryFunc::CastStringToBool(CastStringToBool),
                &["true", "false", "t", "f", "yes", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_int16() {
            use mz_expr::func::CastStringToInt16;
            assert_parity(
                "Int16",
                CastFunc::CastStringToInt16,
                UnaryFunc::CastStringToInt16(CastStringToInt16),
                &["42", "-1", "0", "99999", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_int32() {
            use mz_expr::func::CastStringToInt32;
            assert_parity(
                "Int32",
                CastFunc::CastStringToInt32,
                UnaryFunc::CastStringToInt32(CastStringToInt32),
                &["42", "-1", "0", "99999999999", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_int64() {
            use mz_expr::func::CastStringToInt64;
            assert_parity(
                "Int64",
                CastFunc::CastStringToInt64,
                UnaryFunc::CastStringToInt64(CastStringToInt64),
                &["42", "-1", "0", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_float32() {
            use mz_expr::func::CastStringToFloat32;
            assert_parity(
                "Float32",
                CastFunc::CastStringToFloat32,
                UnaryFunc::CastStringToFloat32(CastStringToFloat32),
                &["1.5", "-0.0", "NaN", "inf", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_float64() {
            use mz_expr::func::CastStringToFloat64;
            assert_parity(
                "Float64",
                CastFunc::CastStringToFloat64,
                UnaryFunc::CastStringToFloat64(CastStringToFloat64),
                &["1.5", "-0.0", "NaN", "inf", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_date() {
            use mz_expr::func::CastStringToDate;
            assert_parity(
                "Date",
                CastFunc::CastStringToDate,
                UnaryFunc::CastStringToDate(CastStringToDate),
                &["2023-01-15", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_time() {
            use mz_expr::func::CastStringToTime;
            assert_parity(
                "Time",
                CastFunc::CastStringToTime,
                UnaryFunc::CastStringToTime(CastStringToTime),
                &["12:34:56", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_timestamp() {
            use mz_expr::func::CastStringToTimestamp;
            assert_parity(
                "Timestamp",
                CastFunc::CastStringToTimestamp(None),
                UnaryFunc::CastStringToTimestamp(CastStringToTimestamp(None)),
                &["2023-01-15 12:34:56", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_timestamptz() {
            use mz_expr::func::CastStringToTimestampTz;
            assert_parity(
                "TimestampTz",
                CastFunc::CastStringToTimestampTz(None),
                UnaryFunc::CastStringToTimestampTz(CastStringToTimestampTz(None)),
                &["2023-01-15 12:34:56+00", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_interval() {
            use mz_expr::func::CastStringToInterval;
            assert_parity(
                "Interval",
                CastFunc::CastStringToInterval,
                UnaryFunc::CastStringToInterval(CastStringToInterval),
                &["1 day", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_uuid() {
            use mz_expr::func::CastStringToUuid;
            assert_parity(
                "Uuid",
                CastFunc::CastStringToUuid,
                UnaryFunc::CastStringToUuid(CastStringToUuid),
                &["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_jsonb() {
            use mz_expr::func::CastStringToJsonb;
            assert_parity(
                "Jsonb",
                CastFunc::CastStringToJsonb,
                UnaryFunc::CastStringToJsonb(CastStringToJsonb),
                &[r#"{"a":1}"#, "true", "42", "{bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_bytes() {
            use mz_expr::func::CastStringToBytes;
            assert_parity(
                "Bytes",
                CastFunc::CastStringToBytes,
                UnaryFunc::CastStringToBytes(CastStringToBytes),
                &["\\xDEADBEEF", "\\x00", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_numeric() {
            use mz_expr::func::CastStringToNumeric;
            assert_parity(
                "Numeric",
                CastFunc::CastStringToNumeric(None),
                UnaryFunc::CastStringToNumeric(CastStringToNumeric(None)),
                &["1.23", "-99.9", "0", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_oid() {
            use mz_expr::func::CastStringToOid;
            assert_parity(
                "Oid",
                CastFunc::CastStringToOid,
                UnaryFunc::CastStringToOid(CastStringToOid),
                &["42", "0", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_uint16() {
            use mz_expr::func::CastStringToUint16;
            assert_parity(
                "Uint16",
                CastFunc::CastStringToUint16,
                UnaryFunc::CastStringToUint16(CastStringToUint16),
                &["42", "0", "-1", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_uint32() {
            use mz_expr::func::CastStringToUint32;
            assert_parity(
                "Uint32",
                CastFunc::CastStringToUint32,
                UnaryFunc::CastStringToUint32(CastStringToUint32),
                &["42", "0", "-1", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_uint64() {
            use mz_expr::func::CastStringToUint64;
            assert_parity(
                "Uint64",
                CastFunc::CastStringToUint64,
                UnaryFunc::CastStringToUint64(CastStringToUint64),
                &["42", "0", "-1", "bad", ""],
            );
        }

        #[mz_ore::test]
        fn parity_pg_legacy_char() {
            use mz_expr::func::CastStringToPgLegacyChar;
            assert_parity(
                "PgLegacyChar",
                CastFunc::CastStringToPgLegacyChar,
                UnaryFunc::CastStringToPgLegacyChar(CastStringToPgLegacyChar),
                &["a", "", "abc"],
            );
        }

        #[mz_ore::test]
        fn parity_pg_legacy_name() {
            use mz_expr::func::CastStringToPgLegacyName;
            assert_parity(
                "PgLegacyName",
                CastFunc::CastStringToPgLegacyName,
                UnaryFunc::CastStringToPgLegacyName(CastStringToPgLegacyName),
                &[
                    "hello",
                    "",
                    "a_long_name_that_exceeds_sixty_four_chars_xxxxxxxxxxxxxxxxxxxxxxxx",
                ],
            );
        }

        #[mz_ore::test]
        fn parity_mz_timestamp() {
            use mz_expr::func::CastStringToMzTimestamp;
            assert_parity(
                "MzTimestamp",
                CastFunc::CastStringToMzTimestamp,
                UnaryFunc::CastStringToMzTimestamp(CastStringToMzTimestamp),
                &["42", "0", "bad", ""],
            );
        }
    }
}
