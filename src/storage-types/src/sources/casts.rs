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
