// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Float functions.

use ore::result::ResultExt;
use repr::adt::decimal::MAX_DECIMAL_PRECISION;
use repr::{strconv, Datum, RowArena, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub const CAST_FLOAT_TO_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn cast_float32_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_float32(&mut buf, a.unwrap_float32());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub fn cast_float64_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_float64(&mut buf, a.unwrap_float64());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const CAST_STRING_TO_FLOAT32_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float32),
};

pub fn cast_string_to_float32(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_float32(a.unwrap_str())
        .map(|n| Datum::Float32(n.into()))
        .err_into()
}

pub const CAST_STRING_TO_FLOAT64_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float64),
};

pub fn cast_string_to_float64(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_float64(a.unwrap_str())
        .map(|n| Datum::Float64(n.into()))
        .err_into()
}

pub const CAST_FLOAT_TO_INT32_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true,
    },
    output_type: OutputType::Fixed(ScalarType::Int32),
};

pub fn cast_float64_to_int32(a: Datum) -> Result<Datum, EvalError> {
    let f = a.unwrap_float64();
    if f > (i32::max_value() as f64) || f < (i32::min_value() as f64) {
        Ok(Datum::Null)
    } else {
        Ok(Datum::from(f as i32))
    }
}

pub const CAST_FLOAT_TO_INT64_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int64),
};

pub fn cast_float32_to_int64(a: Datum) -> Result<Datum, EvalError> {
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Ok(Datum::from(a.unwrap_float32() as i64))
}

pub fn cast_float64_to_int64(a: Datum) -> Result<Datum, EvalError> {
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Ok(Datum::from(a.unwrap_float64() as i64))
}

pub const CAST_FLOAT32_TO_FLOAT64_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float64),
};

pub fn cast_float32_to_float64(a: Datum) -> Result<Datum, EvalError> {
    // TODO(benesch): is this cast valid?
    Ok(Datum::from(f64::from(a.unwrap_float32())))
}

pub fn cast_float_to_decimal_props(scale: u8) -> FuncProps {
    FuncProps {
        can_error: true,
        preserves_uniqueness: false,
        nulls: Nulls::Sometimes {
            propagates_nulls: true,
            introduces_nulls: false,
        },
        output_type: OutputType::Fixed(ScalarType::Decimal(MAX_DECIMAL_PRECISION, scale)),
    }
}

pub fn cast_float32_to_decimal(a: Datum, scale: u8) -> Result<Datum, EvalError> {
    let f = a.unwrap_float32();

    if f > 10_f32.powi((MAX_DECIMAL_PRECISION - scale) as i32) {
        // When we can return error detail:
        // format!("A field with precision {}, \
        //         scale {} must round to an absolute value less than 10^{}.",
        //         MAX_DECIMAL_PRECISION, scale, MAX_DECIMAL_PRECISION - scale)
        return Err(EvalError::NumericFieldOverflow);
    }

    Ok(Datum::from((f * 10_f32.powi(scale as i32)) as i128))
}

pub fn cast_float64_to_decimal(a: Datum, scale: u8) -> Result<Datum, EvalError> {
    let f = a.unwrap_float64();

    if f > 10_f64.powi((MAX_DECIMAL_PRECISION - scale) as i32) {
        // When we can return error detail:
        // format!("A field with precision {}, \
        //         scale {} must round to an absolute value less than 10^{}.",
        //         MAX_DECIMAL_PRECISION, scale, MAX_DECIMAL_PRECISION - scale)
        return Err(EvalError::NumericFieldOverflow);
    }

    Ok(Datum::from((f * 10_f64.powi(scale as i32)) as i128))
}

pub const FLOAT32_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float32),
};

pub const FLOAT64_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float64),
};

pub fn abs_float32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_float32().abs()))
}

pub fn abs_float64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_float64().abs()))
}

pub fn neg_float32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(-a.unwrap_float32()))
}

pub fn neg_float64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(-a.unwrap_float64()))
}

pub fn ceil_float32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_float32().ceil()))
}

pub fn ceil_float64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_float64().ceil()))
}

pub fn floor_float32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_float32().floor()))
}

pub fn floor_float64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_float64().floor()))
}

pub fn round_float32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_float32().round()))
}

pub fn round_float64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_float64().round()))
}

pub fn add_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_float32() + b.unwrap_float32()))
}

pub fn add_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_float64() + b.unwrap_float64()))
}

pub fn sub_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_float32() - b.unwrap_float32()))
}

pub fn sub_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_float64() - b.unwrap_float64()))
}

pub fn mul_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_float32() * b.unwrap_float32()))
}

pub fn mul_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_float64() * b.unwrap_float64()))
}

pub fn div_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float32();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float32() / b))
    }
}

pub fn div_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float64() / b))
    }
}

pub fn mod_float32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float32();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float32() % b))
    }
}

pub fn mod_float64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_float64();
    if b == 0.0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_float64() % b))
    }
}

pub fn sqrt_float32(a: Datum) -> Result<Datum, EvalError> {
    let x = a.unwrap_float32();
    if x < 0.0 {
        return Err(EvalError::NegSqrt);
    }
    Ok(Datum::from(x.sqrt()))
}

pub fn sqrt_float64(a: Datum) -> Result<Datum, EvalError> {
    let x = a.unwrap_float64();
    if x < 0.0 {
        return Err(EvalError::NegSqrt);
    }

    Ok(Datum::from(x.sqrt()))
}
