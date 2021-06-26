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

use lazy_static::lazy_static;
use proc_macro2::TokenTree;

use lowertest::{
    deserialize_optional, gen_reflect_info_func, GenericTestDeserializeContext, MzEnumReflect,
    MzStructReflect, ReflectedTypeInfo,
};
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

/// Constructs a `Datum` from a `&str` and a `ScalarType`.
///
/// See [get_scalar_type_or_default] for creating a ScalarType from a string.
pub fn get_datum_from_str<'a>(litval: &'a str, littyp: &ScalarType) -> Result<Datum<'a>, String> {
    if litval == "null" {
        return Ok(Datum::Null);
    }
    match littyp {
        ScalarType::Bool => Ok(Datum::from(parse_litval::<bool>(litval, "bool")?)),
        ScalarType::APD { .. } | ScalarType::Decimal(_, _) => {
            Ok(Datum::from(parse_litval::<i128>(litval, "i128")?))
        }
        ScalarType::Int32 => Ok(Datum::from(parse_litval::<i32>(litval, "i32")?)),
        ScalarType::Int64 => Ok(Datum::from(parse_litval::<i64>(litval, "i64")?)),
        ScalarType::Float32 => Ok(Datum::from(parse_litval::<f32>(litval, "f32")?)),
        ScalarType::Float64 => Ok(Datum::from(parse_litval::<f64>(litval, "f64")?)),
        ScalarType::String => Ok(Datum::from(litval.trim_matches('"'))),
        _ => Err(format!("Unsupported literal type {:?}", littyp)),
    }
}

/// Parses `ScalarType` from `scalar_type_stream` or infers it from `litval`
///
/// See [lowertest::to_json] for the syntax for specifying a `ScalarType`.
/// If `scalar_type_stream` is empty, will attempt to guess a `ScalarType` for
/// the literal:
/// * If `litval` is "true", "false", or "null", will return `Bool`.
/// * Else if starts with `'"'`, will return String.
/// * Else if contains '.', will return Float64
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
