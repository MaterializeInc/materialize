// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! List functions.

use repr::{Datum, RowArena, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub fn list_create_props(elem_type: ScalarType) -> FuncProps {
    FuncProps {
        can_error: false,
        preserves_uniqueness: false,
        nulls: Nulls::Never,
        output_type: OutputType::Fixed(ScalarType::List(Box::new(elem_type))),
    }
}

pub fn list_create<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(temp_storage.make_datum(|packer| packer.push_list(datums)))
}
