// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decimal functions.

use std::cmp::Ordering;

use ore::collections::CollectionExt;
use ore::result::ResultExt;
use repr::adt::decimal::MAX_DECIMAL_PRECISION;
use repr::{strconv, Datum, RowArena, ScalarType};

use crate::scalar::func::{float, FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub const CAST_DECIMAL_TO_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn cast_decimal_to_string<'a>(
    a: Datum<'a>,
    scale: u8,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_decimal(&mut buf, &a.unwrap_decimal().with_scale(scale));
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub fn cast_string_to_decimal_props(scale: u8) -> FuncProps {
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

pub fn cast_string_to_decimal(a: Datum, scale: u8) -> Result<Datum, EvalError> {
    strconv::parse_decimal(a.unwrap_str())
        .map(|d| {
            Datum::from(match d.scale().cmp(&scale) {
                Ordering::Less => d.significand() * 10_i128.pow(u32::from(scale - d.scale())),
                Ordering::Equal => d.significand(),
                Ordering::Greater => d.significand() / 10_i128.pow(u32::from(d.scale() - scale)),
            })
        })
        .err_into()
}

pub const CAST_DECIMAL_TO_INT32_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int32),
};

pub fn cast_decimal_to_int32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_decimal().as_i128() as i32))
}

pub const CAST_DECIMAL_TO_INT64_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int64),
};

pub fn cast_decimal_to_int64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_decimal().as_i128() as i64))
}

pub const CAST_DECIMAL_TO_FLOAT32_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float32),
};

pub fn cast_decimal_to_float32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_decimal().as_i128() as f32))
}

pub const CAST_DECIMAL_TO_FLOAT64_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float64),
};

pub fn cast_decimal_to_float64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_decimal().as_i128() as f64))
}

pub const DECIMAL_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::MatchesInput,
};

pub fn abs_decimal(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_decimal().abs()))
}

pub fn neg_decimal(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(-a.unwrap_decimal()))
}

pub fn add_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_decimal() + b.unwrap_decimal()))
}

pub fn sub_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_decimal() - b.unwrap_decimal()))
}

pub fn mod_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_decimal();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_decimal() % b))
    }
}

pub const MUL_DECIMAL_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Computed(|input_types| {
        assert_eq!(input_types.len(), 2);
        let (_, s1) = input_types[0].scalar_type.unwrap_decimal_parts();
        let (_, s2) = input_types[1].scalar_type.unwrap_decimal_parts();
        ScalarType::Decimal(MAX_DECIMAL_PRECISION, s1 + s2)
    }),
};

pub fn mul_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::from(a.unwrap_decimal() * b.unwrap_decimal()))
}

pub const DIV_DECIMAL_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Computed(|input_types| {
        assert_eq!(input_types.len(), 2);
        let (_, s1) = input_types[0].scalar_type.unwrap_decimal_parts();
        let (_, s2) = input_types[1].scalar_type.unwrap_decimal_parts();
        ScalarType::Decimal(MAX_DECIMAL_PRECISION, s1 - s2)
    }),
};

pub fn div_decimal<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_decimal();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_decimal() / b))
    }
}

pub fn ceil_decimal(a: Datum, scale: u8) -> Result<Datum, EvalError> {
    let decimal = a.unwrap_decimal();
    Ok(Datum::from(decimal.with_scale(scale).ceil().significand()))
}

pub fn floor_decimal(a: Datum, scale: u8) -> Result<Datum, EvalError> {
    let decimal = a.unwrap_decimal();
    Ok(Datum::from(decimal.with_scale(scale).floor().significand()))
}

pub const ROUND_DECIMAL_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Computed(|input_types| input_types.into_first().scalar_type),
};

pub fn round_decimal_unary(a: Datum, a_scale: u8) -> Result<Datum, EvalError> {
    round_decimal_binary(a, Datum::Int64(0), a_scale)
}

pub fn round_decimal_binary<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    a_scale: u8,
) -> Result<Datum<'a>, EvalError> {
    let round_to = b.unwrap_int64();
    let decimal = a.unwrap_decimal().with_scale(a_scale);
    Ok(Datum::from(decimal.round(round_to).significand()))
}

pub const SQRT_DECIMAL_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::MatchesInput,
};

pub fn sqrt_decimal(a: Datum, scale: u8) -> Result<Datum, EvalError> {
    let d = a.unwrap_decimal();
    if d.as_i128() < 0 {
        return Err(EvalError::NegSqrt);
    }
    let d_f64 = cast_decimal_to_float64(a)?;
    let d_scaled = d_f64.unwrap_float64() / 10_f64.powi(i32::from(scale));
    float::cast_float64_to_decimal(Datum::from(d_scaled.sqrt()), scale)
}
