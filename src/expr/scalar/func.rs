// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use crate::like::build_like_regex_from_string;
use repr::Datum;

pub fn and(a: Datum, b: Datum) -> Datum {
    match (&a, &b) {
        (Datum::False, _) => Datum::False,
        (_, Datum::False) => Datum::False,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::True, Datum::True) => Datum::True,
        _ => panic!("Cannot compute {:?} AND {:?}", a, b),
    }
}

pub fn or(a: Datum, b: Datum) -> Datum {
    match (&a, &b) {
        (Datum::True, _) => Datum::True,
        (_, Datum::True) => Datum::True,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::False, Datum::False) => Datum::False,
        _ => panic!("Cannot compute {:?} OR {:?}", a, b),
    }
}

pub fn not(a: Datum) -> Datum {
    match &a {
        Datum::False => Datum::True,
        Datum::True => Datum::False,
        Datum::Null => Datum::Null,
        _ => panic!("Cannot compute NOT {:?}", a),
    }
}

pub fn abs_int32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32().abs())
}

pub fn abs_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64().abs())
}

pub fn abs_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32().abs())
}

pub fn abs_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64().abs())
}

pub fn cast_int32_to_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int32() as f32)
}

pub fn cast_int32_to_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_int32()))
}

pub fn cast_int32_to_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(i64::from(a.unwrap_int32()))
}

pub fn cast_int64_to_int32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): we need to do something better than panicking if the
    // datum doesn't fit in an int32, but what? Poison the whole dataflow?
    // The SQL standard says this an error, but runtime errors are complicated
    // in a streaming setting.
    Datum::from(i32::try_from(a.unwrap_int64()).unwrap())
}

pub fn cast_int32_to_decimal(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(i128::from(a.unwrap_int32()))
}

pub fn cast_int64_to_decimal(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(i128::from(a.unwrap_int64()))
}

pub fn cast_int64_to_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f32)
}

pub fn cast_int64_to_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f64)
}

pub fn cast_float32_to_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float32() as i64)
}

pub fn cast_float32_to_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_float32()))
}

pub fn cast_float64_to_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float64() as i64)
}

pub fn cast_decimal_to_int32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal().into_i128() as i32)
}

pub fn cast_decimal_to_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal().into_i128() as i64)
}

pub fn cast_decimal_to_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal().into_i128() as f32)
}

pub fn cast_decimal_to_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal().into_i128() as f64)
}

pub fn add_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() + b.unwrap_int32())
}

pub fn add_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() + b.unwrap_int64())
}

pub fn add_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() + b.unwrap_float32())
}

pub fn add_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() + b.unwrap_float64())
}

pub fn add_decimal(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal() + b.unwrap_decimal())
}

pub fn sub_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() - b.unwrap_int32())
}

pub fn sub_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() - b.unwrap_int64())
}

pub fn sub_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() - b.unwrap_float32())
}

pub fn sub_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() - b.unwrap_float64())
}

pub fn sub_decimal(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal() - b.unwrap_decimal())
}

pub fn mul_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() * b.unwrap_int32())
}

pub fn mul_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() * b.unwrap_int64())
}

pub fn mul_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() * b.unwrap_float32())
}

pub fn mul_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() * b.unwrap_float64())
}

pub fn mul_decimal(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal() * b.unwrap_decimal())
}

// TODO(jamii) we don't currently have any way of reporting errors from functions, so for now we just adopt sqlite's approach 1/0 = null

pub fn div_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_int32();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int32() / b)
    }
}

pub fn div_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_int64();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int64() / b)
    }
}

pub fn div_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() / b.unwrap_float32())
}

pub fn div_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() / b.unwrap_float64())
}

pub fn div_decimal(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_decimal() / b.unwrap_decimal())
}

pub fn mod_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() % b.unwrap_int32())
}

pub fn mod_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() % b.unwrap_int64())
}

pub fn mod_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() % b.unwrap_float32())
}

pub fn mod_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() % b.unwrap_float64())
}

pub fn neg_int32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_int32())
}

pub fn neg_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_int64())
}

pub fn neg_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_float32())
}

pub fn neg_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_float64())
}

pub fn eq(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a == b)
}

pub fn not_eq(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a != b)
}

pub fn lt(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a < b)
}

pub fn lte(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a <= b)
}

pub fn gt(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a > b)
}

pub fn gte(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a >= b)
}

pub fn match_regex(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let haystack = a.unwrap_string();
    let needle = b.unwrap_regex();
    Datum::from(needle.is_match(&haystack))
}

pub fn build_like_regex(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }

    match build_like_regex_from_string(&a.unwrap_string()) {
        Ok(regex) => Datum::from(regex),
        Err(_) => {
            // TODO(benesch): this should cause a runtime error, but we don't
            // support those yet, so just return NULL for now.
            Datum::Null
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum BinaryFunc {
    And,
    Or,
    AddInt32,
    AddInt64,
    AddFloat32,
    AddFloat64,
    AddDecimal,
    SubInt32,
    SubInt64,
    SubFloat32,
    SubFloat64,
    SubDecimal,
    MulInt32,
    MulInt64,
    MulFloat32,
    MulFloat64,
    MulDecimal,
    DivInt32,
    DivInt64,
    DivFloat32,
    DivFloat64,
    DivDecimal,
    ModInt32,
    ModInt64,
    ModFloat32,
    ModFloat64,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    MatchRegex,
}

impl BinaryFunc {
    pub fn func(self) -> fn(Datum, Datum) -> Datum {
        match self {
            BinaryFunc::And => and,
            BinaryFunc::Or => or,
            BinaryFunc::AddInt32 => add_int32,
            BinaryFunc::AddInt64 => add_int64,
            BinaryFunc::AddFloat32 => add_float32,
            BinaryFunc::AddFloat64 => add_float64,
            BinaryFunc::AddDecimal => add_decimal,
            BinaryFunc::SubInt32 => sub_int32,
            BinaryFunc::SubInt64 => sub_int64,
            BinaryFunc::SubFloat32 => sub_float32,
            BinaryFunc::SubFloat64 => sub_float64,
            BinaryFunc::SubDecimal => sub_decimal,
            BinaryFunc::MulInt32 => mul_int32,
            BinaryFunc::MulInt64 => mul_int64,
            BinaryFunc::MulFloat32 => mul_float32,
            BinaryFunc::MulFloat64 => mul_float64,
            BinaryFunc::MulDecimal => mul_decimal,
            BinaryFunc::DivInt32 => div_int32,
            BinaryFunc::DivInt64 => div_int64,
            BinaryFunc::DivFloat32 => div_float32,
            BinaryFunc::DivFloat64 => div_float64,
            BinaryFunc::DivDecimal => div_decimal,
            BinaryFunc::ModInt32 => mod_int32,
            BinaryFunc::ModInt64 => mod_int64,
            BinaryFunc::ModFloat32 => mod_float32,
            BinaryFunc::ModFloat64 => mod_float64,
            BinaryFunc::Eq => eq,
            BinaryFunc::NotEq => not_eq,
            BinaryFunc::Lt => lt,
            BinaryFunc::Lte => lte,
            BinaryFunc::Gt => gt,
            BinaryFunc::Gte => gte,
            BinaryFunc::MatchRegex => match_regex,
        }
    }
}

pub fn is_null(a: Datum) -> Datum {
    Datum::from(a == Datum::Null)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum UnaryFunc {
    Not,
    IsNull,
    NegInt32,
    NegInt64,
    NegFloat32,
    NegFloat64,
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt32ToInt64,
    CastInt64ToInt32,
    CastInt32ToDecimal,
    CastInt64ToDecimal,
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastFloat32ToInt64,
    CastFloat32ToFloat64,
    CastFloat64ToInt64,
    CastDecimalToInt32,
    CastDecimalToInt64,
    CastDecimalToFloat32,
    CastDecimalToFloat64,
    BuildLikeRegex,
}

impl UnaryFunc {
    pub fn func(self) -> fn(Datum) -> Datum {
        match self {
            UnaryFunc::Not => not,
            UnaryFunc::IsNull => is_null,
            UnaryFunc::NegInt32 => neg_int32,
            UnaryFunc::NegInt64 => neg_int64,
            UnaryFunc::NegFloat32 => neg_float32,
            UnaryFunc::NegFloat64 => neg_float64,
            UnaryFunc::AbsInt32 => abs_int32,
            UnaryFunc::AbsInt64 => abs_int64,
            UnaryFunc::AbsFloat32 => abs_float32,
            UnaryFunc::AbsFloat64 => abs_float64,
            UnaryFunc::CastInt32ToFloat32 => cast_int32_to_float32,
            UnaryFunc::CastInt32ToFloat64 => cast_int32_to_float64,
            UnaryFunc::CastInt32ToInt64 => cast_int32_to_int64,
            UnaryFunc::CastInt64ToInt32 => cast_int64_to_int32,
            UnaryFunc::CastInt32ToDecimal => cast_int32_to_decimal,
            UnaryFunc::CastInt64ToDecimal => cast_int64_to_decimal,
            UnaryFunc::CastInt64ToFloat32 => cast_int64_to_float32,
            UnaryFunc::CastInt64ToFloat64 => cast_int64_to_float64,
            UnaryFunc::CastFloat32ToInt64 => cast_float32_to_int64,
            UnaryFunc::CastFloat32ToFloat64 => cast_float32_to_float64,
            UnaryFunc::CastFloat64ToInt64 => cast_float64_to_int64,
            UnaryFunc::CastDecimalToInt32 => cast_decimal_to_int32,
            UnaryFunc::CastDecimalToInt64 => cast_decimal_to_int64,
            UnaryFunc::CastDecimalToFloat32 => cast_decimal_to_float32,
            UnaryFunc::CastDecimalToFloat64 => cast_decimal_to_float64,
            UnaryFunc::BuildLikeRegex => build_like_regex,
        }
    }
}

pub fn coalesce(datums: Vec<Datum>) -> Datum {
    datums
        .into_iter()
        .find(|d| !d.is_null())
        .unwrap_or(Datum::Null)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum VariadicFunc {
    Coalesce,
}

impl VariadicFunc {
    pub fn func(self) -> fn(Vec<Datum>) -> Datum {
        match self {
            VariadicFunc::Coalesce => coalesce,
        }
    }
}
