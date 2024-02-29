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
use mz_repr::adt::date::Date;
use mz_repr::adt::numeric::{get_precision, get_scale, Numeric, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Row, ScalarType};

use crate::{MySqlColumnDesc, MySqlError, MySqlTableDesc};

pub fn pack_mysql_row(
    row_container: &mut Row,
    row: MySqlRow,
    table_desc: &MySqlTableDesc,
) -> Result<Row, MySqlError> {
    let mut packer = row_container.packer();
    let mut temp_bytes = vec![];
    let mut temp_strs = vec![];
    let row_values = row.unwrap();

    for values in table_desc.columns.iter().zip_longest(row_values) {
        let (col_desc, value) = match values {
            EitherOrBoth::Both(col_desc, value) => (col_desc, value),
            EitherOrBoth::Left(col_desc) => {
                tracing::error!(
                    "mysql: extra column description {col_desc:?} for table {}",
                    table_desc.name
                );
                Err(MySqlError::DecodeError(format!(
                    "extra column description {col_desc:?} for table {}",
                    table_desc.name
                )))?
            }
            EitherOrBoth::Right(_) => {
                // If there are extra columns on the upstream table we can safely ignore them
                break;
            }
        };
        let datum = match val_to_datum(value, col_desc, &mut temp_strs, &mut temp_bytes) {
            Err(err) => Err(MySqlError::ValueDecodeError {
                column_name: col_desc.name.clone(),
                qualified_table_name: format!("{}.{}", table_desc.schema_name, table_desc.name),
                error: err.to_string(),
            })?,
            Ok(datum) => datum,
        };
        packer.push(datum);
    }

    Ok(row_container.clone())
}

fn val_to_datum<'a>(
    value: Value,
    col_desc: &MySqlColumnDesc,
    temp_strs: &'a mut Vec<String>,
    temp_bytes: &'a mut Vec<Vec<u8>>,
) -> Result<Datum<'a>, anyhow::Error> {
    Ok(match value {
        Value::NULL => {
            if col_desc.column_type.nullable {
                Datum::Null
            } else {
                Err(anyhow::anyhow!(
                    "received a null value in a non-null column".to_string(),
                ))?
            }
        }
        value => match &col_desc.column_type.scalar_type {
            ScalarType::Bool => Datum::from(from_value_opt::<bool>(value)?),
            ScalarType::UInt16 => Datum::from(from_value_opt::<u16>(value)?),
            ScalarType::Int16 => Datum::from(from_value_opt::<i16>(value)?),
            ScalarType::UInt32 => Datum::from(from_value_opt::<u32>(value)?),
            ScalarType::Int32 => Datum::from(from_value_opt::<i32>(value)?),
            ScalarType::UInt64 => Datum::from(from_value_opt::<u64>(value)?),
            ScalarType::Int64 => Datum::from(from_value_opt::<i64>(value)?),
            ScalarType::Float32 => Datum::from(from_value_opt::<f32>(value)?),
            ScalarType::Float64 => Datum::from(from_value_opt::<f64>(value)?),
            ScalarType::Char { length } => {
                let val = from_value_opt::<String>(value)?;
                check_char_length(length.map(|l| l.into_u32()), &val, col_desc)?;
                temp_strs.push(val);
                Datum::from(temp_strs.last().unwrap().as_str())
            }
            ScalarType::VarChar { max_length } => {
                let val = from_value_opt::<String>(value)?;
                check_char_length(max_length.map(|l| l.into_u32()), &val, col_desc)?;
                temp_strs.push(val);
                Datum::from(temp_strs.last().unwrap().as_str())
            }
            ScalarType::String => {
                temp_strs.push(from_value_opt::<String>(value)?);
                Datum::from(temp_strs.last().unwrap().as_str())
            }
            ScalarType::Bytes => {
                let data = from_value_opt::<Vec<u8>>(value)?;
                temp_bytes.push(data);
                Datum::from(temp_bytes.last().unwrap().as_slice())
            }
            ScalarType::Date => {
                let date = Date::try_from(from_value_opt::<chrono::NaiveDate>(value)?)?;
                Datum::from(date)
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
                    Value::Int(val) => chrono::NaiveDateTime::from_timestamp_opt(val, 0)
                        .ok_or(anyhow::anyhow!("received invalid timestamp value: {}", val))?,
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
                Datum::try_from(CheckedTimestamp::try_from(chrono_timestamp)?)?
            }
            ScalarType::Time => Datum::from(from_value_opt::<chrono::NaiveTime>(value)?),
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
                Datum::from(val)
            }
            // TODO(roshan): IMPLEMENT OTHER TYPES
            data_type => Err(anyhow::anyhow!(
                "received unexpected value for type: {:?}: {:?}",
                data_type,
                value
            ))?,
        },
    })
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
