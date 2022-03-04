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
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::strconv::parse_uuid;
use mz_repr::{strconv, ColumnName, ColumnType, Datum, Row, RowArena, RowPacker, ScalarType};
use serde_json::Value;

#[derive(Debug)]
pub struct JsonDecoderState {
    columns: Vec<(ColumnName, ColumnType)>,
    row: Row,
}

impl JsonDecoderState {
    pub fn new(columns: Vec<(ColumnName, ColumnType)>) -> JsonDecoderState {
        JsonDecoderState {
            columns,
            row: Row::default(),
        }
    }

    pub fn decode(&mut self, chunk: &mut &[u8]) -> Result<Option<Row>, DecodeError> {
        let mut packer = self.row.packer();
        let row_arena = RowArena::default();
        for (col_name, col_type) in &self.columns {
            match serde_json::from_slice(chunk) {
                Ok(deserialized) => {
                    let json: serde_json::Value = deserialized;
                    if let Err(e) = decode_column(&mut packer, &row_arena, json, col_name, col_type)
                    {
                        return Err(DecodeError::Text(format!(
                            "error decoding column {}: {:#}",
                            col_name.as_str(),
                            e
                        )));
                    }
                }
                Err(e) => {
                    return Err(DecodeError::Text(format!("invalid json: {:#}", e)));
                }
            }
        }

        let row = self.row.clone();
        println!("Completed Row: {:?}", row);
        Ok(Some(row))
    }
}

pub fn decode_column(
    packer: &mut RowPacker,
    row_arena: &RowArena,
    json: serde_json::Value,
    col_name: &ColumnName,
    col_type: &ColumnType,
) -> Result<(), anyhow::Error> {
    if let Some(json_field) = json.get(col_name.as_str()) {
        println!(
            "Working on {} for col_name {} col type {:?}",
            json_field,
            col_name.as_str(),
            col_type,
        );
        decode_and_pack_value(packer, row_arena, json_field, col_type)
    } else {
        bail!("no value for field {}", col_name.as_str())
    }
}

pub fn decode_and_pack_value(
    packer: &mut RowPacker,
    row_arena: &RowArena,
    json_field: &Value,
    col_type: &ColumnType,
) -> Result<(), anyhow::Error> {
    let ColumnType {
        scalar_type,
        nullable,
    } = col_type;

    if json_field.is_null() {
        if *nullable {
            packer.push(Datum::Null);
            return Ok(());
        } else {
            bail!(
                "invalid `null` value for field {} of type {:?}",
                json_field,
                col_type
            );
        }
    }

    match scalar_type {
        ScalarType::Bool
        | ScalarType::Int16
        | ScalarType::Int32
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
        | ScalarType::Uuid => {
            packer.push(decode_value(row_arena, json_field, scalar_type)?);
            Ok(())
        }
        ScalarType::Jsonb => {
            JsonbPacker::new(packer).pack_serde_json(json_field.clone())?;
            Ok(())
        }
        ScalarType::Array(inner) => {
            let mut values = vec![];
            if let Some(array) = json_field.as_array() {
                for v in array {
                    values.push(decode_value(row_arena, v, inner)?);
                }
            }
            let dims = mz_repr::adt::array::ArrayDimension {
                lower_bound: 1,
                length: values.len(),
            };
            packer.push_array(&[dims], values)?;
            Ok(())
        }
        ScalarType::List { element_type, .. } => packer.push_list_with(|row| {
            if let Some(elems) = json_field.as_array() {
                let col_type = ColumnType {
                    scalar_type: *element_type.clone(),
                    nullable: true,
                };
                for elem in elems {
                    decode_and_pack_value(row, row_arena, elem, &col_type)?;
                }
            }
            Ok::<_, anyhow::Error>(())
        }),
        ScalarType::Record { fields, .. } => packer.push_list_with(|row| {
            if let Some(json) = json_field.as_object() {
                for (field_name, field_type) in fields {
                    if let Some(v) = json.get(field_name.as_str()) {
                        decode_and_pack_value(row, row_arena, v, &field_type)?;
                    } else {
                        bail!("no value for record field {}", field_name.as_str());
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        }),
        ScalarType::Map { value_type, .. } => packer.push_dict_with(|packer| {
            if let Some(elems) = json_field.as_object() {
                // so that it does not go unstated: we must process map keys in ascending order
                // to satisfy the contract of `push_dict_with`. fortunately this is the default
                // behavior of serde_json (unless the `preserve_order` feature is used)
                let col_type = ColumnType {
                    scalar_type: *value_type.clone(),
                    nullable: true,
                };
                for (name, value) in elems {
                    packer.push(Datum::String(name));
                    decode_and_pack_value(packer, row_arena, value, &col_type)?;
                }
            }
            Ok::<_, anyhow::Error>(())
        }),
        ScalarType::Int2Vector => {
            let mut values = vec![];
            if let Some(array) = json_field.as_array() {
                for v in array {
                    values.push(decode_value(row_arena, v, &ScalarType::Int16)?);
                }
            }
            let dims = mz_repr::adt::array::ArrayDimension {
                lower_bound: 1,
                length: values.len(),
            };
            packer.push_array(&[dims], values)?;
            Ok(())
        }
        ScalarType::Oid | ScalarType::RegClass | ScalarType::RegProc | ScalarType::RegType => {
            bail!("unsupported type ")
        }
    }
}

pub fn decode_value<'a>(
    row_arena: &'a RowArena,
    json_field: &'a Value,
    scalar_type: &'a ScalarType,
) -> Result<Datum<'a>, anyhow::Error> {
    if json_field.is_null() {
        return Ok(Datum::Null);
    }

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
        ScalarType::Int32 => {
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
                return Ok(Datum::Float32((v as f32).into()));
            }
        }
        ScalarType::Float64 => {
            if let Some(v) = json_field.as_f64() {
                return Ok(Datum::Float64(v.into()));
            }
        }
        ScalarType::Numeric { .. } => {
            match json_field {
                Value::Number(n) => {
                    let v = strconv::parse_numeric(format!("{}", n).as_str())?;
                    return Ok(Datum::Numeric(v));
                }
                _ => {}
            }
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
        ScalarType::Uuid => {
            if let Some(v) = json_field.as_str() {
                let v = parse_uuid(v)?;
                return Ok(Datum::Uuid(v));
            }
        }
        ScalarType::Oid | ScalarType::RegClass | ScalarType::RegProc | ScalarType::RegType => {
            // should be disallowed by planning
            unreachable!(format!(
                "scalar type {:?} disallowed for JSON sources",
                scalar_type
            ));
        }
        ScalarType::Jsonb
        | ScalarType::Array(_)
        | ScalarType::List { .. }
        | ScalarType::Record { .. }
        | ScalarType::Map { .. }
        | ScalarType::Int2Vector => {
            // should be handled by `decode_and_pack_value`
            unreachable!()
        }
    }

    bail!(
        "unable to read value {} of type {:?}",
        json_field,
        scalar_type
    );
}
