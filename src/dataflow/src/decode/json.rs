// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use mz_dataflow_types::DecodeError;
use mz_repr::strconv::parse_uuid;
use mz_repr::{strconv, ColumnName, ColumnType, Datum, Row, RowArena, ScalarType};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug)]
pub struct JsonDecoderState {
    columns: Vec<(ColumnName, ColumnType)>,
    packer: Row,
}

impl JsonDecoderState {
    pub fn new(columns: Vec<(ColumnName, ColumnType)>) -> JsonDecoderState {
        JsonDecoderState {
            columns,
            packer: Row::default(),
        }
    }

    pub fn decode(&mut self, chunk: &mut &[u8]) -> Result<Option<Row>, DecodeError> {
        self.packer.clear();
        for (col_name, col_type) in &self.columns {
            let json: serde_json::Value = serde_json::from_slice(chunk).unwrap();
            if let Err(e) = decode_column(&mut self.packer, json, col_name, col_type) {
                return Err(DecodeError::Text(format!(
                    "error decoding column {}: {:#}",
                    col_name.as_str(),
                    e
                )));
            }
        }

        Ok(Some(self.packer.finish_and_reuse()))
    }
}

pub fn decode_column(
    row: &mut Row,
    json: serde_json::Value,
    col_name: &ColumnName,
    col_type: &ColumnType,
) -> Result<(), anyhow::Error> {
    let ColumnType {
        nullable,
        scalar_type,
    } = col_type;

    let json_field = json.get(col_name.as_str());

    if json_field.is_none() {
        if *nullable {
            row.push(Datum::Null);
        } else {
            bail!("column {} cannot be null", col_name.as_str())
        }
        return Ok(());
    }

    let json_field = json_field.unwrap();

    println!(
        "Working on {} for col_name {} col type {:?}",
        json_field,
        col_name.as_str(),
        col_type,
    );

    let arena = RowArena::default();
    decode_and_push_value(row, &arena, json_field, scalar_type)
}

pub fn decode_value<'a>(
    row_arena: &'a RowArena,
    json_field: &'a Value,
    scalar_type: &'a ScalarType,
) -> Result<Datum<'a>, anyhow::Error> {
    match scalar_type {
        ScalarType::Bool => {
            if let Some(v) = json_field.as_bool() {
                return Ok(Datum::from(v));
            }
        }
        ScalarType::Int16 => {
            if let Some(v) = json_field.as_i64() {
                let v = i16::try_from(v)?;
                return Ok(Datum::Int16(v));
            }
        }
        ScalarType::Int32
        | ScalarType::Oid
        | ScalarType::RegClass
        | ScalarType::RegProc
        | ScalarType::RegType => {
            if let Some(v) = json_field.as_i64() {
                let v = i32::try_from(v)?;
                return Ok(Datum::Int32(v));
            }
        }
        ScalarType::Int64 => {
            if let Some(v) = json_field.as_i64() {
                return Ok(Datum::Int64(v));
            }
        }
        ScalarType::Float32 => {
            if let Some(v) = json_field.as_f64() {
                return Ok(Datum::Float64(v.into()));
            }
        }
        ScalarType::Float64 => {
            if let Some(v) = json_field.as_f64() {
                return Ok(Datum::Float64(v.into()));
            }
        }
        ScalarType::Numeric { .. } => {
            if let Some(v) = json_field.as_str() {
                let v = strconv::parse_numeric(v)?;
                return Ok(Datum::Numeric(v));
            }
        }
        ScalarType::Date => {
            if let Some(v) = json_field.as_str() {
                let v = strconv::parse_date(v)?;
                return Ok(Datum::Date(v));
            }
        }
        ScalarType::Time => {
            if let Some(v) = json_field.as_str() {
                let v = strconv::parse_time(v)?;
                return Ok(Datum::Time(v));
            }
        }
        ScalarType::Timestamp => {
            if let Some(v) = json_field.as_str() {
                let v = strconv::parse_timestamp(v)?;
                return Ok(Datum::Timestamp(v));
            }
        }
        ScalarType::TimestampTz => {
            if let Some(v) = json_field.as_str() {
                let v = strconv::parse_timestamptz(v)?;
                return Ok(Datum::TimestampTz(v));
            }
        }
        ScalarType::Interval => {
            if let Some(v) = json_field.as_str() {
                let v = strconv::parse_interval(v)?;
                return Ok(Datum::Interval(v));
            }
        }
        ScalarType::Bytes => {
            if let Some(v) = json_field.as_str() {
                return Ok(Datum::Bytes(v.as_bytes()));
            }
        }
        ScalarType::String => {
            if let Some(v) = json_field.as_str() {
                return Ok(Datum::String(v));
            }
        }
        ScalarType::Char { length } => {
            if let Some(v) = json_field.as_str() {
                let v = mz_repr::adt::char::format_str_trim(v, *length, false)?;
                return Ok(Datum::String(row_arena.push_string(v)));
            }
        }
        ScalarType::VarChar { .. } => {
            if let Some(v) = json_field.as_str() {
                return Ok(Datum::String(v));
            }
        }
        ScalarType::Jsonb => {
            unimplemented!()
        }
        ScalarType::Uuid => {
            if let Some(v) = json_field.as_str() {
                let v = parse_uuid(v)?;
                return Ok(Datum::Uuid(v));
            }
        }
        ScalarType::Array(_) => unreachable!(),
        ScalarType::List { .. } => unreachable!(),
        ScalarType::Record { .. } => unreachable!(),
        ScalarType::Map { .. } => unreachable!(),
        ScalarType::Int2Vector => unreachable!(),
    }

    bail!(
        "unable to read value {} of type {:?}",
        json_field,
        scalar_type
    );
}

pub fn decode_and_push_value(
    row: &mut Row,
    row_arena: &RowArena,
    json_field: &Value,
    scalar_type: &ScalarType,
) -> Result<(), anyhow::Error> {
    match scalar_type {
        ScalarType::Bool
        | ScalarType::Int16
        | ScalarType::Int32
        | ScalarType::Oid
        | ScalarType::RegClass
        | ScalarType::RegProc
        | ScalarType::RegType
        | ScalarType::Int64
        | ScalarType::Float32
        | ScalarType::Float64
        | ScalarType::Numeric { .. }
        | ScalarType::Date
        | ScalarType::Time
        | ScalarType::Timestamp
        | ScalarType::TimestampTz
        | ScalarType::Interval
        | ScalarType::Bytes
        | ScalarType::String
        | ScalarType::Char { .. }
        | ScalarType::VarChar { .. }
        | ScalarType::Jsonb
        | ScalarType::Uuid => {
            row.push(decode_value(row_arena, json_field, scalar_type)?);
            return Ok(());
        }
        ScalarType::Array(inner) => {
            let mut values = vec![];
            if let Some(array) = json_field.as_array() {
                for x in array {
                    values.push(decode_value(row_arena, x, inner)?);
                }
            }
            let dims = mz_repr::adt::array::ArrayDimension {
                lower_bound: 1,
                length: values.len(),
            };
            row.push_array(&[dims], values);
            return Ok(());
        }
        ScalarType::List { element_type, .. } => {
            row.push_list_with(|row| {
                if let Some(elems) = json_field.as_array() {
                    for x in elems {
                        decode_and_push_value(row, row_arena, x, element_type);
                    }
                }
            });
            return Ok(());
        }
        ScalarType::Record { fields, .. } => {
            row.push_list_with(|row| {
                let columns: HashMap<&str, &ColumnType> =
                    fields.iter().map(|(n, t)| (n.as_str(), t)).collect();
                if let Some(elems) = json_field.as_object() {
                    for (name, value) in elems {
                        let col_type = columns.get(name.as_str());
                        decode_and_push_value(
                            row,
                            row_arena,
                            value,
                            &col_type.unwrap().scalar_type,
                        );
                    }
                }
            });
            return Ok(());
        }
        ScalarType::Map { .. } => {
            unimplemented!()
        }
        ScalarType::Int2Vector => {
            unimplemented!()
        }
    }
}
