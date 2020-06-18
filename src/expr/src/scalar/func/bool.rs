// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Boolean functions.

use repr::strconv;
use repr::{Datum, RowArena, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::{EvalError, ScalarExpr};

pub const CAST_BOOL_TO_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: true,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn cast_bool_to_string_explicit(a: Datum) -> Result<Datum, EvalError> {
    // N.B. this function differs from `cast_bool_to_string_implicit` because
    // the SQL specification requires `true` and `false` to be spelled out
    // in explicit casts, while PostgreSQL prefers its more concise `t` and `f`
    // representation in implicit casts.
    match a.unwrap_bool() {
        true => Ok(Datum::from("true")),
        false => Ok(Datum::from("false")),
    }
}

pub fn cast_bool_to_string_implicit(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::String(strconv::format_bool_static(a.unwrap_bool())))
}

pub const CAST_STRING_TO_BOOL_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn cast_string_to_bool(a: Datum) -> Result<Datum, EvalError> {
    match strconv::parse_bool(a.unwrap_str())? {
        true => Ok(Datum::True),
        false => Ok(Datum::False),
    }
}

pub const AND_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn and<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    a_expr: &'a ScalarExpr,
    b_expr: &'a ScalarExpr,
) -> Result<Datum<'a>, EvalError> {
    match a_expr.eval(datums, temp_storage)? {
        Datum::False => Ok(Datum::False),
        a => match (a, b_expr.eval(datums, temp_storage)?) {
            (_, Datum::False) => Ok(Datum::False),
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            (Datum::True, Datum::True) => Ok(Datum::True),
            _ => unreachable!(),
        },
    }
}

pub const OR_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn or<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    a_expr: &'a ScalarExpr,
    b_expr: &'a ScalarExpr,
) -> Result<Datum<'a>, EvalError> {
    match a_expr.eval(datums, temp_storage)? {
        Datum::True => Ok(Datum::True),
        a => match (a, b_expr.eval(datums, temp_storage)?) {
            (_, Datum::True) => Ok(Datum::True),
            (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
            (Datum::False, Datum::False) => Ok(Datum::False),
            _ => unreachable!(),
        },
    }
}

pub const NOT_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: true,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn not(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(!a.unwrap_bool()))
}
