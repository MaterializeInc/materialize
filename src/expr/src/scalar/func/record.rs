// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Record functions.

use ore::collections::CollectionExt;
use repr::{ColumnName, Datum, RowArena, ScalarType};

use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub fn record_create_props(field_names: Vec<ColumnName>, oid: Option<u32>) -> FuncProps {
    FuncProps {
        can_error: false,
        preserves_uniqueness: false,
        nulls: Nulls::Never,
        output_type: OutputType::Computed2(Box::new(move |input_types| ScalarType::Record {
            fields: field_names
                .clone()
                .into_iter()
                .zip(input_types)
                .map(|(name, ty)| (name, ty.scalar_type))
                .collect(),
            oid,
        })),
    }
}

pub fn record_create<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(temp_storage.make_datum(|packer| packer.push_list(datums)))
}

pub fn record_get_props(i: usize) -> FuncProps {
    FuncProps {
        can_error: false,
        preserves_uniqueness: false,
        nulls: Nulls::Never,
        output_type: OutputType::Computed2(Box::new(move |input_types| {
            match input_types.into_first().scalar_type {
                ScalarType::Record { mut fields, .. } => fields.remove(i).1,
                _ => unreachable!(),
            }
        })),
    }
}

pub fn record_get(a: Datum, i: usize) -> Result<Datum, EvalError> {
    Ok(a.unwrap_list().iter().nth(i).unwrap())
}
