// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Maintains a catalog of valid casts between [`repr::ScalarType`]s.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

use lazy_static::lazy_static;
use repr::{ColumnType, Datum, ScalarType};

use super::expr::{BinaryFunc, ScalarExpr, UnaryFunc};
use super::query::ExprContext;

/// Describes methods of planning a conversion between [`ScalarType`]s, which
/// can be invoked with [`CastOp::gen_expr`].
pub enum CastOp {
    U(UnaryFunc),
    F(fn(&ExprContext, ScalarExpr, CastTo) -> ScalarExpr),
}

impl fmt::Debug for CastOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CastOp::U(u) => write!(f, "CastOp::U({:?})", u),
            CastOp::F(_) => write!(f, "CastOp::F"),
        }
    }
}

// Provides a shorthand for writing `CastOp::U`.
impl From<UnaryFunc> for CastOp {
    fn from(u: UnaryFunc) -> CastOp {
        CastOp::U(u)
    }
}

impl CastOp {
    /// Generates the [`ScalarExpr`] to cast between [`ScalarType`]s.
    pub fn gen_expr(&self, ecx: &ExprContext, e: ScalarExpr, cast_to: CastTo) -> ScalarExpr {
        match self {
            CastOp::U(u) => e.call_unary(u.clone()),
            CastOp::F(f) => f(ecx, e, cast_to),
        }
    }
}

// Used when the [`ScalarExpr`] is already of the desired [`ScalarType`].
fn noop_cast(_: &ExprContext, e: ScalarExpr, _: CastTo) -> ScalarExpr {
    e
}

fn jsonb_any_string_cast(ecx: &ExprContext, e: ScalarExpr, _: CastTo) -> ScalarExpr {
    let s = ecx.scalar_type(&e);
    let to = CastTo::Explicit(ScalarType::String);

    let cast_op = get_cast(&s, &to).unwrap();

    cast_op
        .gen_expr(ecx, e, to)
        .call_unary(UnaryFunc::CastJsonbOrNullToJsonb)
}

fn jsonb_any_f64_cast(ecx: &ExprContext, e: ScalarExpr, _: CastTo) -> ScalarExpr {
    let s = ecx.scalar_type(&e);
    let to = CastTo::Explicit(ScalarType::Float64);

    let cast_op = get_cast(&s, &to).unwrap();

    cast_op
        .gen_expr(ecx, e, to)
        .call_unary(UnaryFunc::CastJsonbOrNullToJsonb)
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
/// Describes the context of the cast, the target type.
pub enum CastTo {
    /// Only allow implicit casts. Typically these are lossless casts, such as
    /// `ScalarType::Int32` to `ScalarType::Int64`.
    Implicit(ScalarType),
    /// Allow either explicit or implicit casts.
    Explicit(ScalarType),
    /// Cast the source to a JSONB element directly, or cast to a compatible
    /// intermediary type (`ScalarType::String`, `ScalarType::Float64`) and then
    /// to a JSONB element.
    JsonbAny,
}

impl fmt::Display for CastTo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CastTo::Implicit(t) | CastTo::Explicit(t) => write!(f, "{}", t),
            CastTo::JsonbAny => write!(f, "jsonbany"),
        }
    }
}

impl CastTo {
    pub fn scalar_type(&self) -> ScalarType {
        match self {
            CastTo::Implicit(t) | CastTo::Explicit(t) => t.clone(),
            CastTo::JsonbAny => ScalarType::Jsonb,
        }
    }
}

macro_rules! casts(
    {
        $(
            $from_castto:expr => $castop:expr
        ),+
    } => {{
        let mut m: HashMap<(ScalarType, CastTo), CastOp> = HashMap::new();
        $(
            m.insert($from_castto, $castop.into());
        )+
        m
    }};
);

lazy_static! {
    static ref VALID_CASTS: HashMap<(ScalarType, CastTo), CastOp> = {
        use CastTo::*;
        use ScalarType::*;
        use UnaryFunc::*;

        casts! {
            // BOOL
            (Bool, Implicit(String)) => CastBoolToStringImplicit,
            (Bool, Explicit(String)) => CastBoolToStringExplicit,
            (Bool, Explicit(Jsonb)) => CastJsonbOrNullToJsonb,
            (Bool, JsonbAny) => CastJsonbOrNullToJsonb,

            //INT32
            (Int32, Explicit(Bool)) => CastInt32ToBool,
            (Int32, Explicit(Jsonb)) => CastOp::F(|_ecx, e, _to_type| {
                e
                .call_unary(UnaryFunc::CastInt32ToFloat64)
                .call_unary(UnaryFunc::CastJsonbOrNullToJsonb)
            }),
            (Int32, Implicit(Int64)) => CastInt32ToInt64,
            (Int32, Implicit(Float32)) => CastInt32ToFloat32,
            (Int32, Implicit(Float64)) => CastInt32ToFloat64,
            (Int32, Implicit(Decimal(0, 0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                rescale_decimal(e.call_unary(CastInt32ToDecimal), 0, s)
            }),
            (Int32, Explicit(String)) => CastInt32ToString,
            (Int32, JsonbAny) => CastOp::F(jsonb_any_f64_cast),

            // INT64
            (Int64, Explicit(Bool)) => CastInt64ToBool,
            (Int64, Explicit(Int32)) => CastInt64ToInt32,
            (Int64, Implicit(Decimal(0, 0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                rescale_decimal(e.call_unary(CastInt64ToDecimal), 0, s)
            }),
            (Int64, Implicit(Float32)) => CastInt64ToFloat32,
            (Int64, Implicit(Float64)) => CastInt64ToFloat64,
            (Int64, Explicit(String)) => CastInt64ToString,
            (Int64, JsonbAny) => CastOp::F(jsonb_any_f64_cast),

            // FLOAT32
            (Float32, Explicit(Int64)) => CastFloat32ToInt64,
            (Float32, Implicit(Float64)) => CastFloat32ToFloat64,
            (Float32, Explicit(Decimal(0, 0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                let s = ScalarExpr::literal(
                    Datum::from(i32::from(s)), ColumnType::new(to_type.scalar_type())
                );
                e.call_binary(s, BinaryFunc::CastFloat32ToDecimal)
            }),
            (Float32, Explicit(String)) => CastFloat32ToString,
            (Float32, JsonbAny) => CastOp::F(jsonb_any_f64_cast),

            // FLOAT64
            (Float64, Explicit(Int32)) => CastFloat64ToInt32,
            (Float64, Explicit(Int64)) => CastFloat64ToInt64,
            (Float64, Explicit(Decimal(0, 0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                let s = ScalarExpr::literal(Datum::from(
                    i32::from(s)), ColumnType::new(to_type.scalar_type()));
                e.call_binary(s, BinaryFunc::CastFloat64ToDecimal)
            }),
            (Float64, Explicit(String)) => CastFloat64ToString,
            (Float64, Explicit(Jsonb)) => CastJsonbOrNullToJsonb,
            (Float64, JsonbAny) => CastJsonbOrNullToJsonb,

            // DECIMAL
            (Decimal(0, 0), Explicit(Int32)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                rescale_decimal(e, s, 0).call_unary(CastDecimalToInt32)
            }),
            (Decimal(0, 0), Explicit(Int64)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                rescale_decimal(e, s, 0).call_unary(CastDecimalToInt64)
            }),
            (Decimal(0, 0), Implicit(Float32)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let factor = 10_f32.powi(i32::from(s));
                let factor =
                    ScalarExpr::literal(Datum::from(factor), ColumnType::new(Float32));
                e.call_unary(CastSignificandToFloat32)
                    .call_binary(factor, BinaryFunc::DivFloat32)
            }),
            (Decimal(0, 0), Implicit(Float64)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let factor = 10_f64.powi(i32::from(s));
                let factor =
                    ScalarExpr::literal(Datum::from(factor), ColumnType::new(Float32));
                e.call_unary(CastSignificandToFloat64)
                    .call_binary(factor, BinaryFunc::DivFloat64)
            }),
            (Decimal(0, 0), Implicit(Decimal(0, 0))) => CastOp::F(|ecx, e, to_type| {
                let (_, f) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let (_, t) = to_type.scalar_type().unwrap_decimal_parts();
                rescale_decimal(e, f, t)
            }),
            (Decimal(0, 0), Explicit(String)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                e.call_unary(CastDecimalToString(s))
            }),
            (Decimal(0, 0), JsonbAny) => CastOp::F(jsonb_any_f64_cast),

            // DATE
            (Date, Implicit(Timestamp)) => CastDateToTimestamp,
            (Date, Implicit(TimestampTz)) => CastDateToTimestampTz,
            (Date, Explicit(String)) => CastDateToString,
            (Date, JsonbAny) => CastOp::F(jsonb_any_string_cast),

            // TIME
            (Time, Implicit(Interval)) => CastTimeToInterval,
            (Time, Explicit(String)) => CastTimeToString,
            (Time, JsonbAny) => CastOp::F(jsonb_any_string_cast),

            // TIMESTAMP
            (Timestamp, Explicit(Date)) => CastTimestampToDate,
            (Timestamp, Implicit(TimestampTz)) => CastTimestampToTimestampTz,
            (Timestamp, Explicit(String)) => CastTimestampToString,
            (Timestamp, JsonbAny) => CastOp::F(jsonb_any_string_cast),

            // TIMESTAMPTZ
            (TimestampTz, Explicit(Date)) => CastTimestampTzToDate,
            (TimestampTz, Explicit(Timestamp)) => CastTimestampTzToTimestamp,
            (TimestampTz, Explicit(String)) => CastTimestampTzToString,
            (TimestampTz, JsonbAny) => CastOp::F(jsonb_any_string_cast),

            // INTERVAL
            (Interval, Explicit(Time)) => CastIntervalToTime,
            (Interval, Explicit(String)) => CastIntervalToString,
            (Interval, JsonbAny) => CastOp::F(jsonb_any_string_cast),

            // BYTES
            (Bytes, Explicit(String)) => CastBytesToString,
            (Bytes, JsonbAny) => CastOp::F(jsonb_any_string_cast),

            // STRING
            (String, Explicit(Bool)) => CastStringToBool,
            (String, Explicit(Int32)) => CastStringToInt32,
            (String, Explicit(Int64)) => CastStringToInt64,
            (String, Explicit(Float32)) => CastStringToFloat32,
            (String, Explicit(Float64)) => CastStringToFloat64,
            (String, Explicit(Decimal(0,0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                e.call_unary(CastStringToDecimal(s))
            }),
            (String, Explicit(Date)) => CastStringToDate,
            (String, Explicit(Time)) => CastStringToTime,
            (String, Explicit(Timestamp)) => CastStringToTimestamp,
            (String, Explicit(TimestampTz)) => CastStringToTimestampTz,
            (String, Explicit(Interval)) => CastStringToInterval,
            (String, Explicit(Bytes)) => CastStringToBytes,
            (String, Explicit(Jsonb)) => CastStringToJsonb,
            (String, JsonbAny) => CastJsonbOrNullToJsonb,

            // JSONB
            (Jsonb, Explicit(String)) => JsonbStringify,
            (Jsonb, JsonbAny) => CastJsonbOrNullToJsonb
        }
    };
}

/// Get a cast, if one exists, from a [`ScalarType`] to another, with control
/// over allowing implicit or explicit casts using [`CastTo`].
///
/// Use the returned [`CastOp`] with [`CastOp::gen_expr`].
pub fn get_cast<'a>(from: &ScalarType, cast_to: &CastTo) -> Option<&'a CastOp> {
    use CastTo::*;

    if *from == cast_to.scalar_type() {
        return Some(&CastOp::F(noop_cast));
    }

    let cast_to = match cast_to {
        Implicit(t) => Implicit(t.desaturate()),
        Explicit(t) => Explicit(t.desaturate()),
        JsonbAny => JsonbAny,
    };

    let cast = VALID_CASTS.get(&(from.desaturate(), cast_to.clone()));

    match (cast, cast_to) {
        // If no explicit implementation, look for an implicit one.
        (None, CastTo::Explicit(t)) => VALID_CASTS.get(&(from.desaturate(), CastTo::Implicit(t))),
        (c, _) => c,
    }
}

pub fn rescale_decimal(expr: ScalarExpr, s1: u8, s2: u8) -> ScalarExpr {
    match s1.cmp(&s2) {
        Ordering::Less => {
            let typ = ColumnType::new(ScalarType::Decimal(38, s2 - s1));
            let factor = 10_i128.pow(u32::from(s2 - s1));
            let factor = ScalarExpr::literal(Datum::from(factor), typ);
            expr.call_binary(factor, BinaryFunc::MulDecimal)
        }
        Ordering::Equal => expr,
        Ordering::Greater => {
            let typ = ColumnType::new(ScalarType::Decimal(38, s1 - s2));
            let factor = 10_i128.pow(u32::from(s1 - s2));
            let factor = ScalarExpr::literal(Datum::from(factor), typ);
            expr.call_binary(factor, BinaryFunc::DivDecimal)
        }
    }
}
