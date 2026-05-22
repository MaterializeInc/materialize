// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use mz_expr_derive::sqlfunc;
use mz_lowertest::MzReflect;
use mz_repr::{Datum, DatumList, DatumMap, RowArena, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::EvalError;
use crate::scalar::func::stringify_datum;

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct CastMapToString {
    pub ty: SqlScalarType,
}

#[sqlfunc(
    CastMapToString,
    sqlname = "maptostr",
    preserves_uniqueness = true,
    introduces_nulls = false
)]
fn cast_map_to_string<'a>(
    &self,
    a: DatumMap<'a>,
    _temp_storage: &'a RowArena,
) -> Result<String, EvalError> {
    let mut buf = String::new();
    stringify_datum(&mut buf, Datum::Map(a), &self.ty)?;
    Ok(buf)
}

#[sqlfunc(sqlname = "map_length")]
fn map_length<'a>(a: DatumMap<'a>) -> Result<i32, EvalError> {
    let count = a.iter().count();
    count
        .try_into()
        .map_err(|_| EvalError::Int32OutOfRange(count.to_string().into()))
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct MapBuildFromRecordList {
    pub value_type: SqlScalarType,
}

#[sqlfunc(
    MapBuildFromRecordList,
    sqlname = "map_build",
    output_type_expr = "SqlScalarType::Map { value_type: Box::new(self.value_type.clone()), custom_id: None }.nullable(true)",
    introduces_nulls = true
)]
fn map_build_from_record_list<'a>(
    &self,
    a: DatumList<'a>,
    temp_storage: &'a RowArena,
) -> Datum<'a> {
    let mut map = std::collections::BTreeMap::new();
    for i in a.iter() {
        if i.is_null() {
            continue;
        }
        for (k, v) in i.unwrap_list().iter().tuples() {
            if k.is_null() {
                continue;
            }
            map.insert(k.unwrap_str(), v);
        }
    }
    temp_storage.make_datum(|packer| packer.push_dict(map))
}
