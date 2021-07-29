// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities to build objects from the `repr` crate for unit testing.
//!
//! These test utilities are relied by crates other than `repr`.

use chrono::NaiveDateTime;
use lazy_static::lazy_static;
use proc_macro2::TokenTree;

use lowertest::{
    deserialize_optional, gen_reflect_info_func, GenericTestDeserializeContext, MzEnumReflect,
    MzStructReflect, ReflectedTypeInfo,
};
use ore::str::StrExt;
use repr::adt::numeric::Numeric;
use repr::{ColumnType, Datum, ScalarType};

/* #region Generate information required to construct arbitrary `ScalarType`*/
gen_reflect_info_func!(produce_rti, [ScalarType], [ColumnType]);

lazy_static! {
    static ref RTI: ReflectedTypeInfo = produce_rti();
}

/* #endregion */

fn parse_litval<'a, F>(litval: &'a str, littyp: &str) -> Result<F, String>
where
    F: std::str::FromStr,
    F::Err: ToString,
{
    litval.parse::<F>().map_err(|e| {
        format!(
            "error when parsing {} into {}: {}",
            litval,
            littyp,
            e.to_string()
        )
    })
}

/// Constructs a `Datum` from `litval` and `littyp`.
///
/// See [get_scalar_type_or_default] for creating a `ScalarType`.
///
/// Because Datums do not own their strings, if `littyp` is
/// `ScalarType::String`, make sure that `litval` has already
/// been unquoted by [unquote_string].
///
/// Generally, `litval` can be parsed into a Datum in the manner you would
/// imagine. Exceptions:
/// * A Timestamp should be in the format `"\"%Y-%m-%d %H:%M:%S%.f\""` or
///   `"\"%Y-%m-%d %H:%M:%S\""`
pub fn get_datum_from_str<'a>(litval: &'a str, littyp: &ScalarType) -> Result<Datum<'a>, String> {
    if litval == "null" {
        return Ok(Datum::Null);
    }
    match littyp {
        ScalarType::Bool => Ok(Datum::from(parse_litval::<bool>(litval, "bool")?)),
        ScalarType::Numeric { .. } => Ok(Datum::from(parse_litval::<Numeric>(litval, "Numeric")?)),
        ScalarType::Int16 => Ok(Datum::from(parse_litval::<i16>(litval, "i16")?)),
        ScalarType::Int32 => Ok(Datum::from(parse_litval::<i32>(litval, "i32")?)),
        ScalarType::Int64 => Ok(Datum::from(parse_litval::<i64>(litval, "i64")?)),
        ScalarType::Float32 => Ok(Datum::from(parse_litval::<f32>(litval, "f32")?)),
        ScalarType::Float64 => Ok(Datum::from(parse_litval::<f64>(litval, "f64")?)),
        ScalarType::String => Ok(Datum::from(litval)),
        ScalarType::Timestamp => {
            let datetime = if litval.contains('.') {
                NaiveDateTime::parse_from_str(litval, "\"%Y-%m-%d %H:%M:%S%.f\"")
            } else {
                NaiveDateTime::parse_from_str(litval, "\"%Y-%m-%d %H:%M:%S\"")
            };
            Ok(Datum::from(datetime.map_err(|e| {
                format!("Error while parsing NaiveDateTime: {}", e)
            })?))
        }
        _ => Err(format!("Unsupported literal type {:?}", littyp)),
    }
}

/// Changes `"\"foo\""` to `"foo"` if scalar type is String
pub fn unquote_string(litval: &str, littyp: &ScalarType) -> String {
    if littyp == &ScalarType::String {
        lowertest::unquote(litval)
    } else {
        litval.to_string()
    }
}

/// Convert a Datum to a String such that [get_datum_from_str] can convert the
/// String back into the same Datum.
///
/// Currently supports only Datums supported by [get_datum_from_str].
pub fn datum_to_test_spec(datum: Datum) -> String {
    let result = format!("{}", datum);
    match datum {
        Datum::Timestamp(_) => result.quoted().to_string(),
        _ => result,
    }
}

/// Parses `ScalarType` from `scalar_type_stream` or infers it from `litval`
///
/// See [lowertest::to_json] for the syntax for specifying a `ScalarType`.
/// If `scalar_type_stream` is empty, will attempt to guess a `ScalarType` for
/// the literal:
/// * If `litval` is "true", "false", or "null", will return `Bool`.
/// * Else if starts with `'"'`, will return String.
/// * Else if contains `'.'`, will return Float64.
/// * Otherwise, returns Int64.
pub fn get_scalar_type_or_default<I>(
    litval: &str,
    scalar_type_stream: &mut I,
) -> Result<ScalarType, String>
where
    I: Iterator<Item = TokenTree>,
{
    let typ: Option<ScalarType> = deserialize_optional(
        scalar_type_stream,
        "ScalarType",
        &RTI,
        &mut GenericTestDeserializeContext::default(),
    )?;
    match typ {
        Some(typ) => Ok(typ),
        None => {
            if ["true", "false", "null"].contains(&litval) {
                Ok(ScalarType::Bool)
            } else if litval.starts_with('\"') {
                Ok(ScalarType::String)
            } else if litval.contains('.') {
                Ok(ScalarType::Float64)
            } else {
                Ok(ScalarType::Int64)
            }
        }
    }
}
