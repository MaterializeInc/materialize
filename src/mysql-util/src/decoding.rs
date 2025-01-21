// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use itertools::{EitherOrBoth, Itertools};
use mysql_common::value::convert::from_value_opt;
use mysql_common::{Row as MySqlRow, Value};

use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::adt::date::Date;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::adt::numeric::{get_precision, get_scale, Numeric, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Row, RowPacker, ScalarType};

use crate::desc::MySqlColumnMeta;
use crate::{MySqlColumnDesc, MySqlError, MySqlTableDesc};

pub fn pack_mysql_row(
    row_container: &mut Row,
    row: MySqlRow,
    table_desc: &MySqlTableDesc,
) -> Result<Row, MySqlError> {
    let mut packer = row_container.packer();
    let row_values = row.unwrap();

    for values in table_desc.columns.iter().zip_longest(row_values) {
        let (col_desc, value) = match values {
            EitherOrBoth::Both(col_desc, value) => (col_desc, value),
            EitherOrBoth::Left(col_desc) => {
                tracing::error!(
                    "mysql: extra column description {col_desc:?} for table {}",
                    table_desc.name
                );
                Err(MySqlError::ValueDecodeError {
                    column_name: col_desc.name.clone(),
                    qualified_table_name: format!("{}.{}", table_desc.schema_name, table_desc.name),
                    error: "extra column description".to_string(),
                })?
            }
            EitherOrBoth::Right(_) => {
                // If there are extra columns on the upstream table we can safely ignore them
                break;
            }
        };
        if col_desc.column_type.is_none() {
            // This column is ignored, so don't decode it.
            continue;
        }
        match pack_val_as_datum(value, col_desc, &mut packer) {
            Err(err) => Err(MySqlError::ValueDecodeError {
                column_name: col_desc.name.clone(),
                qualified_table_name: format!("{}.{}", table_desc.schema_name, table_desc.name),
                error: err.to_string(),
            })?,
            Ok(()) => (),
        };
    }

    Ok(row_container.clone())
}

// TODO(guswynn|roshan): This function has various `.to_string()` and `format!` calls that should
// use a shared allocation if possible.
fn pack_val_as_datum(
    value: Value,
    col_desc: &MySqlColumnDesc,
    packer: &mut RowPacker,
) -> Result<(), anyhow::Error> {
    let column_type = match col_desc.column_type {
        Some(ref column_type) => column_type,
        None => anyhow::bail!("column type is not set for column: {}", col_desc.name),
    };
    match value {
        Value::NULL => {
            if column_type.nullable {
                packer.push(Datum::Null);
            } else {
                Err(anyhow::anyhow!(
                    "received a null value in a non-null column".to_string(),
                ))?
            }
        }
        value => match &column_type.scalar_type {
            ScalarType::Bool => packer.push(Datum::from(from_value_opt::<bool>(value)?)),
            ScalarType::UInt16 => packer.push(Datum::from(from_value_opt::<u16>(value)?)),
            ScalarType::Int16 => packer.push(Datum::from(from_value_opt::<i16>(value)?)),
            ScalarType::UInt32 => packer.push(Datum::from(from_value_opt::<u32>(value)?)),
            ScalarType::Int32 => packer.push(Datum::from(from_value_opt::<i32>(value)?)),
            ScalarType::UInt64 => {
                if let Some(MySqlColumnMeta::Bit(precision)) = &col_desc.meta {
                    let mut value = from_value_opt::<Vec<u8>>(value)?;

                    // Ensure we have the correct number of bytes.
                    let precision_bytes = (precision + 7) / 8;
                    if value.len() != usize::cast_from(precision_bytes) {
                        return Err(anyhow::anyhow!("'bit' column out of range!"));
                    }
                    // Be defensive and prune any bits that come over the wire and are
                    // greater than our precision.
                    let bit_index = precision % 8;
                    if bit_index != 0 {
                        let mask = !(u8::MAX << bit_index);
                        if value.len() > 0 {
                            value[0] &= mask;
                        }
                    }

                    // Based on experimentation the value coming across the wire is
                    // encoded in big-endian.
                    let mut buf = [0u8; 8];
                    buf[(8 - value.len())..].copy_from_slice(value.as_slice());
                    let value = u64::from_be_bytes(buf);
                    packer.push(Datum::from(value))
                } else {
                    packer.push(Datum::from(from_value_opt::<u64>(value)?))
                }
            }
            ScalarType::Int64 => packer.push(Datum::from(from_value_opt::<i64>(value)?)),
            ScalarType::Float32 => packer.push(Datum::from(from_value_opt::<f32>(value)?)),
            ScalarType::Float64 => packer.push(Datum::from(from_value_opt::<f64>(value)?)),
            ScalarType::Char { length } => {
                let val = from_value_opt::<String>(value)?;
                check_char_length(length.map(|l| l.into_u32()), &val, col_desc)?;
                packer.push(Datum::String(&val));
            }
            ScalarType::VarChar { max_length } => {
                let val = from_value_opt::<String>(value)?;
                check_char_length(max_length.map(|l| l.into_u32()), &val, col_desc)?;
                packer.push(Datum::String(&val));
            }
            ScalarType::String => {
                // Special case for string types, since this is the scalar type used for a column
                // specified as a 'TEXT COLUMN'. In some cases we need to check the column
                // metadata to know if the upstream value needs special handling
                match &col_desc.meta {
                    Some(MySqlColumnMeta::Enum(e)) => {
                        match value {
                            Value::Bytes(data) => {
                                let data = std::str::from_utf8(&data)?;
                                packer.push(Datum::String(data));
                            }
                            Value::Int(val) => {
                                // Enum types are provided as 1-indexed integers in the replication
                                // stream, so we need to find the string value from the enum meta
                                let enum_val = e.values.get(usize::try_from(val)? - 1).ok_or(
                                    anyhow::anyhow!(
                                        "received invalid enum value: {} for column {}",
                                        val,
                                        col_desc.name
                                    ),
                                )?;
                                packer.push(Datum::String(enum_val));
                            }
                            _ => Err(anyhow::anyhow!(
                                "received unexpected value for enum type: {:?}",
                                value
                            ))?,
                        }
                    }
                    Some(MySqlColumnMeta::Json) => {
                        // JSON types in a query response are encoded as a string with whitespace,
                        // but when parsed from the binlog event by mysql-common they are provided
                        // as an encoded string sans-whitespace.
                        if let Value::Bytes(data) = value {
                            let json = serde_json::from_slice::<serde_json::Value>(&data)?;
                            packer.push(Datum::String(&json.to_string()));
                        } else {
                            Err(anyhow::anyhow!(
                                "received unexpected value for json type: {:?}",
                                value
                            ))?;
                        }
                    }
                    Some(MySqlColumnMeta::Year) => {
                        let val = from_value_opt::<u16>(value)?;
                        packer.push(Datum::String(&val.to_string()));
                    }
                    Some(MySqlColumnMeta::Date) => {
                        // Some MySQL dates are invalid in chrono/NaiveDate (e.g. 0000-00-00), so
                        // we need to handle them directly as strings
                        if let Value::Date(y, m, d, 0, 0, 0, 0) = value {
                            packer.push(Datum::String(&format!("{:04}-{:02}-{:02}", y, m, d)));
                        } else {
                            Err(anyhow::anyhow!(
                                "received unexpected value for date type: {:?}",
                                value
                            ))?;
                        }
                    }
                    Some(MySqlColumnMeta::Timestamp(precision)) => {
                        // Some MySQL dates are invalid in chrono/NaiveDate (e.g. 0000-00-00), so
                        // we need to handle them directly as strings
                        if let Value::Date(y, m, d, h, mm, s, ms) = value {
                            if *precision > 0 {
                                let precision: usize = (*precision).try_into()?;
                                packer.push(Datum::String(&format!(
                                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:0precision$}",
                                    y,
                                    m,
                                    d,
                                    h,
                                    mm,
                                    s,
                                    ms,
                                    precision = precision
                                )));
                            } else {
                                packer.push(Datum::String(&format!(
                                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                                    y, m, d, h, mm, s
                                )));
                            }
                        } else {
                            Err(anyhow::anyhow!(
                                "received unexpected value for timestamp type: {:?}",
                                value
                            ))?;
                        }
                    }
                    Some(MySqlColumnMeta::Bit(_)) => unreachable!("parsed as a u64"),
                    None => {
                        packer.push(Datum::String(&from_value_opt::<String>(value)?));
                    }
                }
            }
            ScalarType::Jsonb => {
                if let Value::Bytes(data) = value {
                    let packer = JsonbPacker::new(packer);
                    // TODO(guswynn): This still produces and extract allocation (in the
                    // `DeserializeSeed` impl used internally), which should be improved,
                    // for all users of the APIs in that module.
                    packer.pack_slice(&data).map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to decode JSON: {}",
                            // See if we can output the string that failed to be converted to JSON.
                            match std::str::from_utf8(&data) {
                                Ok(str) => str.to_string(),
                                // Otherwise produce the nominally helpful error.
                                Err(_) => e.display_with_causes().to_string(),
                            }
                        )
                    })?;
                } else {
                    Err(anyhow::anyhow!(
                        "received unexpected value for json type: {:?}",
                        value
                    ))?
                }
            }
            ScalarType::Bytes => {
                let data = from_value_opt::<Vec<u8>>(value)?;
                packer.push(Datum::Bytes(&data));
            }
            ScalarType::Date => {
                let date = Date::try_from(from_value_opt::<chrono::NaiveDate>(value)?)?;
                packer.push(Datum::from(date));
            }
            ScalarType::Timestamp { precision: _ } => {
                // Timestamps are encoded as different mysql_common::Value types depending on
                // whether they are from a binlog event or a query, and depending on which
                // mysql timestamp version is used. We handle those cases here
                // https://github.com/blackbeam/rust_mysql_common/blob/v0.31.0/src/binlog/value.rs#L87-L155
                // https://github.com/blackbeam/rust_mysql_common/blob/v0.31.0/src/value/mod.rs#L332
                let chrono_timestamp = match value {
                    Value::Date(..) => from_value_opt::<chrono::NaiveDateTime>(value)?,
                    // old temporal format from before MySQL 5.6; didn't support fractional seconds
                    Value::Int(val) => chrono::DateTime::from_timestamp(val, 0)
                        .ok_or(anyhow::anyhow!("received invalid timestamp value: {}", val))?
                        .naive_utc(),
                    Value::Bytes(data) => {
                        let data = std::str::from_utf8(&data)?;
                        if data.contains('.') {
                            chrono::NaiveDateTime::parse_from_str(data, "%s%.6f")?
                        } else {
                            chrono::NaiveDateTime::parse_from_str(data, "%s")?
                        }
                    }
                    _ => Err(anyhow::anyhow!(
                        "received unexpected value for timestamp type: {:?}",
                        value
                    ))?,
                };
                packer.push(Datum::try_from(CheckedTimestamp::try_from(
                    chrono_timestamp,
                )?)?);
            }
            ScalarType::Time => {
                packer.push(Datum::from(from_value_opt::<chrono::NaiveTime>(value)?));
            }
            ScalarType::Numeric { max_scale } => {
                // The wire-format of numeric types is a string when sent in a binary query
                // response but is represented in a decimal binary format when sent in a binlog
                // event. However the mysql-common crate abstracts this away and always returns
                // a string. We parse the string into a numeric type here.
                let val = from_value_opt::<String>(value)?;
                let val = Numeric::from_str(&val)?;
                if get_precision(&val) > NUMERIC_DATUM_MAX_PRECISION.into() {
                    Err(anyhow::anyhow!(
                        "received numeric value with precision {} for column {} which has a max precision of {}",
                        get_precision(&val),
                        col_desc.name,
                        NUMERIC_DATUM_MAX_PRECISION
                    ))?
                }
                if let Some(max_scale) = max_scale {
                    if get_scale(&val) > max_scale.into_u8().into() {
                        Err(anyhow::anyhow!(
                            "received numeric value with scale {} for column {} which has a max scale of {}",
                            get_scale(&val),
                            col_desc.name,
                            max_scale.into_u8()
                        ))?
                    }
                }
                packer.push(Datum::from(val));
            }
            // TODO(roshan): IMPLEMENT OTHER TYPES
            data_type => Err(anyhow::anyhow!(
                "received unexpected value for type: {:?}: {:?}",
                data_type,
                value
            ))?,
        },
    }
    Ok(())
}

fn check_char_length(
    length: Option<u32>,
    val: &str,
    col_desc: &MySqlColumnDesc,
) -> Result<(), anyhow::Error> {
    if let Some(length) = length {
        if let Some(_) = val.char_indices().nth(usize::cast_from(length)) {
            Err(anyhow::anyhow!(
                "received string value of length {} for column {} which has a max length of {}",
                val.len(),
                col_desc.name,
                length
            ))?
        }
    }
    Ok(())
}
