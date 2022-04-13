// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::{strconv, ColumnType, Datum, ScalarType};

use crate::scalar::func::impls::numeric::*;
use crate::scalar::func::EagerUnaryFunc;
use crate::EvalError;

fn jsonb_type(d: Datum<'_>) -> &'static str {
    match d {
        Datum::JsonNull => "null",
        Datum::False | Datum::True => "boolean",
        Datum::String(_) => "string",
        Datum::Numeric(_) => "numeric",
        Datum::List(_) => "array",
        Datum::Map(_) => "object",
        _ => unreachable!("jsonb_type called on invalid datum {:?}", d),
    }
}

sqlfunc!(
    #[sqlname = "jsonbtostr"]
    fn cast_jsonb_to_string<'a>(a: JsonbRef<'a>) -> String {
        let mut buf = String::new();
        strconv::format_jsonb(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "jsonbtoi16"]
    fn cast_jsonb_to_int16<'a>(a: JsonbRef<'a>) -> Result<i16, EvalError> {
        match a.into_datum() {
            Datum::Numeric(a) => cast_numeric_to_int16(a.into_inner()),
            datum => Err(EvalError::InvalidJsonbCast {
                from: jsonb_type(datum).into(),
                to: "smallint".into(),
            }),
        }
    }
);

sqlfunc!(
    #[sqlname = "jsonbtoi32"]
    fn cast_jsonb_to_int32<'a>(a: JsonbRef<'a>) -> Result<i32, EvalError> {
        match a.into_datum() {
            Datum::Numeric(a) => cast_numeric_to_int32(a.into_inner()),
            datum => Err(EvalError::InvalidJsonbCast {
                from: jsonb_type(datum).into(),
                to: "integer".into(),
            }),
        }
    }
);

sqlfunc!(
    #[sqlname = "jsonbtoi64"]
    fn cast_jsonb_to_int64<'a>(a: JsonbRef<'a>) -> Result<i64, EvalError> {
        match a.into_datum() {
            Datum::Numeric(a) => cast_numeric_to_int64(a.into_inner()),
            datum => Err(EvalError::InvalidJsonbCast {
                from: jsonb_type(datum).into(),
                to: "bigint".into(),
            }),
        }
    }
);

sqlfunc!(
    #[sqlname = "jsonbtof32"]
    fn cast_jsonb_to_float32<'a>(a: JsonbRef<'a>) -> Result<f32, EvalError> {
        match a.into_datum() {
            Datum::Numeric(a) => cast_numeric_to_float32(a.into_inner()),
            datum => Err(EvalError::InvalidJsonbCast {
                from: jsonb_type(datum).into(),
                to: "real".into(),
            }),
        }
    }
);

sqlfunc!(
    #[sqlname = "jsonbtof64"]
    fn cast_jsonb_to_float64<'a>(a: JsonbRef<'a>) -> Result<f64, EvalError> {
        match a.into_datum() {
            Datum::Numeric(a) => cast_numeric_to_float64(a.into_inner()),
            datum => Err(EvalError::InvalidJsonbCast {
                from: jsonb_type(datum).into(),
                to: "double precision".into(),
            }),
        }
    }
);

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CastJsonbToNumeric(pub Option<NumericMaxScale>);

impl<'a> EagerUnaryFunc<'a> for CastJsonbToNumeric {
    type Input = JsonbRef<'a>;
    type Output = Result<Numeric, EvalError>;

    fn call(&self, a: JsonbRef<'a>) -> Result<Numeric, EvalError> {
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
                from: jsonb_type(datum).into(),
                to: "numeric".into(),
            }),
        }
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Numeric { max_scale: self.0 }.nullable(input.nullable)
    }
}

impl fmt::Display for CastJsonbToNumeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("jsonbtonumeric")
    }
}

sqlfunc!(
    #[sqlname = "jsonbtobool"]
    fn cast_jsonb_to_bool<'a>(a: JsonbRef<'a>) -> Result<bool, EvalError> {
        match a.into_datum() {
            Datum::True => Ok(true),
            Datum::False => Ok(false),
            datum => Err(EvalError::InvalidJsonbCast {
                from: jsonb_type(datum).into(),
                to: "boolean".into(),
            }),
        }
    }
);

sqlfunc!(
    #[sqlname = "jsonb?tojsonb"]
    fn cast_jsonb_or_null_to_jsonb<'a>(a: Option<JsonbRef<'a>>) -> JsonbRef<'a> {
        match a.map(|v| v.into_datum()) {
            None => JsonbRef::from_datum(Datum::JsonNull),
            Some(Datum::Numeric(n)) => {
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
            Some(datum) => JsonbRef::from_datum(datum),
        }
    }
);
