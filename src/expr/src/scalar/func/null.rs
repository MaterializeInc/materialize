// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Null-handling functions.

use repr::{Datum, RowArena, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::{EvalError, ScalarExpr};

pub const IS_NULL_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Never,
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn is_null(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a == Datum::Null))
}

pub const COALESCE_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: false,
    },
    output_type: OutputType::MatchesInput,
};

pub fn coalesce<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
    exprs: &'a [ScalarExpr],
) -> Result<Datum<'a>, EvalError> {
    for e in exprs {
        let d = e.eval(datums, temp_storage)?;
        if !d.is_null() {
            return Ok(d);
        }
    }
    Ok(Datum::Null)
}
