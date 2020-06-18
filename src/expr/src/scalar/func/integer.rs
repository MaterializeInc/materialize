// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integer functions.

use std::convert::TryFrom;

use ore::result::ResultExt;
use repr::{strconv, Datum, RowArena, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub const CAST_INT_TO_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: true,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn cast_int32_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_int32(&mut buf, a.unwrap_int32());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub fn cast_int64_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_int64(&mut buf, a.unwrap_int64());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const CAST_STRING_TO_INT32_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int32),
};

pub fn cast_string_to_int32(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_int32(a.unwrap_str())
        .map(Datum::Int32)
        .err_into()
}

pub const CAST_STRING_TO_INT64_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int64),
};

pub fn cast_string_to_int64(a: Datum) -> Result<Datum, EvalError> {
    strconv::parse_int64(a.unwrap_str())
        .map(Datum::Int64)
        .err_into()
}

pub const CAST_INT_TO_BOOL_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn cast_int32_to_bool(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_int32() != 0))
}

pub fn cast_int64_to_bool(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_int64() != 0))
}

pub const CAST_INT_TO_FLOAT32_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float32),
};

pub fn cast_int32_to_float32(a: Datum) -> Result<Datum, EvalError> {
    // TODO(benesch): is this cast valid?
    Ok(Datum::from(a.unwrap_int32() as f32))
}

pub fn cast_int64_to_float32(a: Datum) -> Result<Datum, EvalError> {
    // TODO(benesch): is this cast valid?
    Ok(Datum::from(a.unwrap_int64() as f32))
}

pub const CAST_INT_TO_FLOAT64_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Float64),
};

pub fn cast_int32_to_float64(a: Datum) -> Result<Datum, EvalError> {
    // TODO(benesch): is this cast valid?
    Ok(Datum::from(f64::from(a.unwrap_int32())))
}

pub fn cast_int64_to_float64(a: Datum) -> Result<Datum, EvalError> {
    // TODO(benesch): is this cast valid?
    Ok(Datum::from(a.unwrap_int64() as f64))
}

pub const CAST_INT32_TO_DECIMAL_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Decimal(10, 0)),
};

pub fn cast_int32_to_decimal(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(i128::from(a.unwrap_int32())))
}

pub const CAST_INT64_TO_DECIMAL_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Decimal(20, 0)),
};

pub fn cast_int64_to_decimal(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(i128::from(a.unwrap_int64())))
}

pub const CAST_INT32_TO_INT64_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: true,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int64),
};

pub fn cast_int32_to_int64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(i64::from(a.unwrap_int32())))
}

pub const CAST_INT64_TO_INT32_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int32),
};

pub fn cast_int64_to_int32(a: Datum) -> Result<Datum, EvalError> {
    match i32::try_from(a.unwrap_int64()) {
        Ok(n) => Ok(Datum::from(n)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
    }
}

pub const INT32_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int32),
};

pub const INT64_MATH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int64),
};

pub fn abs_int32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_int32().abs()))
}

pub fn abs_int64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_int64().abs()))
}

pub fn neg_int32(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(-a.unwrap_int32()))
}

pub fn neg_int64(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(-a.unwrap_int64()))
}

pub fn add_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int32()
        .checked_add(b.unwrap_int32())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

pub fn add_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int64()
        .checked_add(b.unwrap_int64())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

pub fn sub_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int32()
        .checked_sub(b.unwrap_int32())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

pub fn sub_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int64()
        .checked_sub(b.unwrap_int64())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

pub fn mul_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int32()
        .checked_mul(b.unwrap_int32())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

pub fn mul_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int64()
        .checked_mul(b.unwrap_int64())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}

pub fn div_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int32();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int32() / b))
    }
}

pub fn div_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int64();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int64() / b))
    }
}

pub fn mod_int32<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int32();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int32() % b))
    }
}

pub fn mod_int64<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let b = b.unwrap_int64();
    if b == 0 {
        Err(EvalError::DivisionByZero)
    } else {
        Ok(Datum::from(a.unwrap_int64() % b))
    }
}
