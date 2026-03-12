// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_expr_derive::sqlfunc;
use mz_lowertest::MzReflect;
use mz_repr::adt::jsonb::{Jsonb, JsonbRef};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::role_id::RoleId;
use mz_repr::{ArrayRustType, Datum, Row, RowPacker, SqlColumnType, SqlScalarType, strconv};
use serde::{Deserialize, Serialize};

use crate::EvalError;
use crate::scalar::func::EagerUnaryFunc;
use crate::scalar::func::impls::numeric::*;

#[sqlfunc(
    sqlname = "jsonb_to_text",
    preserves_uniqueness = false,
    inverse = to_unary!(super::CastStringToJsonb)
)]
pub fn cast_jsonb_to_string<'a>(a: JsonbRef<'a>) -> String {
    let mut buf = String::new();
    strconv::format_jsonb(&mut buf, a);
    buf
}

#[sqlfunc(sqlname = "jsonb_to_smallint", is_monotone = true)]
fn cast_jsonb_to_int16<'a>(a: JsonbRef<'a>) -> Result<i16, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_int16(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "smallint".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonb_to_integer", is_monotone = true)]
fn cast_jsonb_to_int32<'a>(a: JsonbRef<'a>) -> Result<i32, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_int32(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "integer".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonb_to_bigint", is_monotone = true)]
fn cast_jsonb_to_int64<'a>(a: JsonbRef<'a>) -> Result<i64, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_int64(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "bigint".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonb_to_real", is_monotone = true)]
fn cast_jsonb_to_float32<'a>(a: JsonbRef<'a>) -> Result<f32, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_float32(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "real".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonb_to_double", is_monotone = true)]
fn cast_jsonb_to_float64<'a>(a: JsonbRef<'a>) -> Result<f64, EvalError> {
    match a.into_datum() {
        Datum::Numeric(a) => cast_numeric_to_float64(a.into_inner()),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "double precision".into(),
        }),
    }
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct CastJsonbToNumeric(pub Option<NumericMaxScale>);

impl EagerUnaryFunc for CastJsonbToNumeric {
    type Input<'a> = JsonbRef<'a>;
    type Output<'a> = Result<Numeric, EvalError>;

    fn call<'a>(&self, a: Self::Input<'a>) -> Self::Output<'a> {
        match a.into_datum() {
            Datum::Numeric(mut num) => match self.0 {
                None => Ok(num.into_inner()),
                Some(scale) => {
                    if numeric::rescale(&mut num.0, scale.into_u8()).is_err() {
                        return Err(EvalError::NumericFieldOverflow);
                    };
                    Ok(num.into_inner())
                }
            },
            datum => Err(EvalError::InvalidJsonbCast {
                from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
                to: "numeric".into(),
            }),
        }
    }

    fn output_sql_type(&self, input: SqlColumnType) -> SqlColumnType {
        SqlScalarType::Numeric { max_scale: self.0 }.nullable(input.nullable)
    }

    fn is_monotone(&self) -> bool {
        true
    }
}

impl fmt::Display for CastJsonbToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("jsonb_to_numeric")
    }
}

#[sqlfunc(sqlname = "jsonb_to_boolean", is_monotone = true)]
fn cast_jsonb_to_bool<'a>(a: JsonbRef<'a>) -> Result<bool, EvalError> {
    match a.into_datum() {
        Datum::True => Ok(true),
        Datum::False => Ok(false),
        datum => Err(EvalError::InvalidJsonbCast {
            from: jsonb_typeof(JsonbRef::from_datum(datum)).into(),
            to: "boolean".into(),
        }),
    }
}

#[sqlfunc(sqlname = "jsonbable_to_jsonb")]
fn cast_jsonbable_to_jsonb<'a>(a: JsonbRef<'a>) -> JsonbRef<'a> {
    match a.into_datum() {
        Datum::Numeric(n) => {
            let n = n.into_inner();
            let datum = if n.is_finite() {
                Datum::from(n)
            } else if n.is_nan() {
                Datum::String("NaN")
            } else if n.is_negative() {
                Datum::String("-Infinity")
            } else {
                Datum::String("Infinity")
            };
            JsonbRef::from_datum(datum)
        }
        datum => JsonbRef::from_datum(datum),
    }
}

#[sqlfunc]
fn jsonb_array_length<'a>(a: JsonbRef<'a>) -> Result<Option<i32>, EvalError> {
    match a.into_datum() {
        Datum::List(list) => {
            let count = list.iter().count();
            match i32::try_from(count) {
                Ok(len) => Ok(Some(len)),
                Err(_) => Err(EvalError::Int32OutOfRange(count.to_string().into())),
            }
        }
        _ => Ok(None),
    }
}

#[sqlfunc]
fn jsonb_typeof<'a>(a: JsonbRef<'a>) -> &'a str {
    match a.into_datum() {
        Datum::Map(_) => "object",
        Datum::List(_) => "array",
        Datum::String(_) => "string",
        Datum::Numeric(_) => "number",
        Datum::True | Datum::False => "boolean",
        Datum::JsonNull => "null",
        d => panic!("Not jsonb: {:?}", d),
    }
}

#[sqlfunc]
fn jsonb_strip_nulls<'a>(a: JsonbRef<'a>) -> Jsonb {
    fn strip_nulls(a: Datum, row: &mut RowPacker) {
        match a {
            Datum::Map(dict) => row.push_dict_with(|row| {
                for (k, v) in dict.iter() {
                    match v {
                        Datum::JsonNull => (),
                        _ => {
                            row.push(Datum::String(k));
                            strip_nulls(v, row);
                        }
                    }
                }
            }),
            Datum::List(list) => row.push_list_with(|row| {
                for elem in list.iter() {
                    strip_nulls(elem, row);
                }
            }),
            _ => row.push(a),
        }
    }
    let mut row = Row::default();
    strip_nulls(a.into_datum(), &mut row.packer());
    Jsonb::from_row(row)
}

#[sqlfunc]
fn jsonb_pretty<'a>(a: JsonbRef<'a>) -> String {
    let mut buf = String::new();
    strconv::format_jsonb_pretty(&mut buf, a);
    buf
}

/// Converts a JSONB-serialized serde tagged enum ID to Materialize's string
/// ID format. Handles both newtype variants like `{"User": 1}` → `"u1"` and
/// unit variants like `"Public"` → `"p"`.
#[sqlfunc(sqlname = "parse_catalog_id")]
fn parse_catalog_id<'a>(a: JsonbRef<'a>) -> Result<String, EvalError> {
    match a.into_datum() {
        // Unit variant, e.g. "Public"
        Datum::String(variant) => match variant {
            "Public" => Ok("p".to_string()),
            other => Err(EvalError::Internal(
                format!("unexpected catalog ID variant: {other}").into(),
            )),
        },
        // Newtype variant, e.g. {"User": 1}
        Datum::Map(dict) => {
            let (key, val) = dict
                .iter()
                .next()
                .ok_or_else(|| EvalError::Internal("empty object for catalog ID".into()))?;
            let prefix = match key {
                "User" => "u",
                "System" => "s",
                "Predefined" => "g",
                "Transient" => "t",
                other => {
                    return Err(EvalError::Internal(
                        format!("unexpected catalog ID variant: {other}").into(),
                    ));
                }
            };
            match val {
                Datum::Numeric(n) => Ok(format!("{prefix}{}", n.0.to_standard_notation_string())),
                _ => Err(EvalError::Internal(
                    "expected numeric value in catalog ID".into(),
                )),
            }
        }
        _ => Err(EvalError::Internal(
            "expected string or object for catalog ID".into(),
        )),
    }
}

/// Extracts a u64 from a JSONB numeric datum.
fn datum_to_u64(d: Datum) -> Result<u64, EvalError> {
    match d {
        Datum::Numeric(n) => {
            let mut cx = numeric::cx_datum();
            cx.try_into_u64(n.0)
                .or_else(|_| Err(EvalError::Internal("catalog ID out of range".into())))
        }
        _ => Err(EvalError::Internal(
            "expected numeric value in catalog ID".into(),
        )),
    }
}

/// Parses a JSONB-serialized tagged enum into a RoleId.
fn datum_to_role_id(d: Datum) -> Result<RoleId, EvalError> {
    match d {
        Datum::String("Public") => Ok(RoleId::Public),
        Datum::String(other) => Err(EvalError::Internal(
            format!("unexpected role ID variant: {other}").into(),
        )),
        Datum::Map(dict) => {
            let (key, val) = dict
                .iter()
                .next()
                .ok_or_else(|| EvalError::Internal("empty object for role ID".into()))?;
            let id = datum_to_u64(val)?;
            match key {
                "User" => Ok(RoleId::User(id)),
                "System" => Ok(RoleId::System(id)),
                "Predefined" => Ok(RoleId::Predefined(id)),
                other => Err(EvalError::Internal(
                    format!("unexpected role ID variant: {other}").into(),
                )),
            }
        }
        _ => Err(EvalError::Internal(
            "expected string or object for role ID".into(),
        )),
    }
}

/// Converts a JSONB-serialized privilege array from the catalog shard into
/// an `mz_aclitem[]`. Each element has the shape:
/// `{"acl_mode": {"bitflags": N}, "grantee": <id>, "grantor": <id>}`
#[sqlfunc(sqlname = "parse_catalog_privileges")]
fn parse_catalog_privileges<'a>(a: JsonbRef<'a>) -> Result<ArrayRustType<MzAclItem>, EvalError> {
    let mut result = Vec::new();
    match a.into_datum() {
        Datum::List(list) => {
            for item in list.iter() {
                match item {
                    Datum::Map(dict) => {
                        let mut grantee = None;
                        let mut grantor = None;
                        let mut acl_mode = None;
                        for (key, val) in dict.iter() {
                            match key {
                                "grantee" => grantee = Some(datum_to_role_id(val)?),
                                "grantor" => grantor = Some(datum_to_role_id(val)?),
                                "acl_mode" => match val {
                                    Datum::Map(mode_dict) => {
                                        for (k, v) in mode_dict.iter() {
                                            if k == "bitflags" {
                                                let bits = datum_to_u64(v)?;
                                                acl_mode = AclMode::from_bits(bits);
                                            }
                                        }
                                    }
                                    _ => {
                                        return Err(EvalError::Internal(
                                            "expected object for acl_mode".into(),
                                        ));
                                    }
                                },
                                _ => {}
                            }
                        }
                        result.push(MzAclItem {
                            grantee: grantee.ok_or_else(|| {
                                EvalError::Internal("missing grantee in privilege".into())
                            })?,
                            grantor: grantor.ok_or_else(|| {
                                EvalError::Internal("missing grantor in privilege".into())
                            })?,
                            acl_mode: acl_mode.ok_or_else(|| {
                                EvalError::Internal("missing acl_mode in privilege".into())
                            })?,
                        });
                    }
                    _ => {
                        return Err(EvalError::Internal(
                            "expected object in privilege array".into(),
                        ));
                    }
                }
            }
        }
        _ => return Err(EvalError::Internal("expected array for privileges".into())),
    }
    Ok(ArrayRustType(result))
}
