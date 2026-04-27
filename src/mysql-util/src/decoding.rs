// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::str::FromStr;

use mysql_async::binlog;
use mysql_common::value::convert::from_value_opt;
use mysql_common::{Row as MySqlRow, Value};

use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::adt::date::Date;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::adt::numeric::{NUMERIC_DATUM_MAX_PRECISION, Numeric, get_precision, get_scale};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Row, RowPacker, SqlScalarType};

use crate::desc::MySqlColumnMeta;
use crate::{MySqlColumnDesc, MySqlError, MySqlTableDesc};

pub fn pack_mysql_row(
    row_container: &mut Row,
    row: MySqlRow,
    table_desc: &MySqlTableDesc,
    gtid_set: Option<&str>,
    binlog_full_metadata: bool,
) -> Result<Row, MySqlError> {
    let mut packer = row_container.packer();

    // If a column name begins with '@', then the binlog does not have full row metadata,
    // meaning that full column names are not available and we need to rely on the order
    // of the columns in the upstream table matching the order of the columns in the row.
    // This is a fallback for MySQL servers that do not have `binlog_row_metadata` set to
    // `FULL`. If the first column name does not begin with '@', then we can assume that
    // full metadata is available and we can match columns by name.
    let fallback_names = row
        .columns_ref()
        .first()
        .is_some_and(|col| col.name_ref().starts_with(b"@"));

    if binlog_full_metadata && fallback_names {
        // This should never happen, but if it does, it's a sign that something is very wrong with the MySQL server's binlog configuration. We want to error rather than silently producing incorrect results.
        return Err(MySqlError::ValueDecodeError {
            column_name: "<unknown>".to_string(),
            qualified_table_name: format!("{}.{}", table_desc.schema_name, table_desc.name),
            error: "Table was created with binlog_row_metadata=FULL but binlog_row_metadata has since been set to a different value, meaning we cannot reliably decode the columns".to_string(),
        });
    }

    // For each column in `table_desc` (in descriptor order), resolve its wire
    // index. Non-fallback rows are matched by name so a reordered upstream
    // still decodes correctly; fallback rows have no names and are matched
    // positionally. A `None` here means the upstream row is missing this
    // column and is only tolerated for ignored columns.
    for (i, col_desc) in table_desc.columns.iter().enumerate() {
        let wire_idx = if !binlog_full_metadata {
            (i < row.len()).then_some(i)
        } else {
            row.columns_ref()
                .iter()
                .position(|wc| wc.name_str() == col_desc.name.as_str())
        };
        if col_desc.column_type.is_none() {
            // This column is ignored, so don't decode it.
            continue;
        }
        let wire_idx = match wire_idx {
            Some(idx) => idx,
            None => {
                return Err(decode_error(
                    "upstream row is missing column",
                    col_desc,
                    table_desc,
                    gtid_set,
                    &row,
                ));
            }
        };
        let value = row
            .as_ref(wire_idx)
            .expect("wire_idx resolved from row")
            .clone();
        if let Err(err) = pack_val_as_datum(value, col_desc, &mut packer) {
            return Err(decode_error(
                &err.to_string(),
                col_desc,
                table_desc,
                gtid_set,
                &row,
            ));
        }
    }

    Ok(row_container.clone())
}

/// Build a `ValueDecodeError`, logging the schema, table, column, source
/// gtid_set (if any), and a shape description of `row` at the same time.
/// The shape string is only built here — pack_mysql_row's happy path does no
/// per-row allocation beyond what decoding requires.
fn decode_error(
    err_msg: &str,
    col_desc: &MySqlColumnDesc,
    table_desc: &MySqlTableDesc,
    gtid_set: Option<&str>,
    row: &MySqlRow,
) -> MySqlError {
    let row_shape = describe_row_shape(row, table_desc);
    tracing::warn!(
        "mysql decode error for `{}`.`{}` column `{}`: {}; gtid_set={:?}; row_shape={}",
        table_desc.schema_name,
        table_desc.name,
        col_desc.name,
        err_msg,
        gtid_set,
        row_shape,
    );
    MySqlError::ValueDecodeError {
        column_name: col_desc.name.clone(),
        qualified_table_name: format!("{}.{}", table_desc.schema_name, table_desc.name),
        error: err_msg.to_string(),
    }
}

/// Describes the structural shape of a row without revealing any data values.
/// Iterates every wire column. For each, emits the wire name, the binlog
/// wire type, the character-set id (or `binary`), a classification relative
/// to `table_desc` (`expected=<scalar>` for active columns, `ignored` for
/// columns excluded from the source, `extra` for upstream columns with no
/// descriptor entry), and a value disposition (`null` or `bytes(len=N)` /
/// primitive kind). Intended for diagnostic logging on decode errors: MySQL
/// serializes CHAR, VARCHAR, TEXT, JSON, BLOB, etc. all as `Value::Bytes`,
/// so the wire type tag and the expected scalar type are what distinguish
/// them.
fn describe_row_shape(row: &MySqlRow, table_desc: &MySqlTableDesc) -> String {
    // Binlogs without full row metadata use positional "@N" names, so we
    // have to match by wire position rather than by name.
    let fallback_names = row
        .columns_ref()
        .first()
        .is_some_and(|col| col.name_ref().starts_with(b"@"));

    let mut out = String::new();
    out.push('[');
    for (i, wire_col) in row.columns_ref().iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let wire_name = wire_col.name_str();
        let cs = wire_col.character_set();
        // 63 = binary collation (binary/blob columns).
        let cs_str = if cs == 63 {
            "binary".to_string()
        } else {
            format!("charset={cs}")
        };
        let wire_type = format!("{:?}", wire_col.column_type());

        let matched_col = if fallback_names {
            table_desc.columns.get(i)
        } else {
            table_desc
                .columns
                .iter()
                .find(|c| c.name.as_str() == wire_name)
        };
        let match_info = match matched_col {
            Some(col) => match &col.column_type {
                Some(ct) => format!("expected={:?}", ct.scalar_type),
                None => "ignored".to_string(),
            },
            None => "extra".to_string(),
        };

        let val_desc = match row.as_ref(i) {
            None => "absent".to_string(),
            Some(Value::NULL) => "null".to_string(),
            Some(Value::Bytes(b)) => format!("bytes(len={})", b.len()),
            Some(Value::Int(_)) => "int".to_string(),
            Some(Value::UInt(_)) => "uint".to_string(),
            Some(Value::Float(_)) => "float".to_string(),
            Some(Value::Double(_)) => "double".to_string(),
            Some(Value::Date(..)) => "date".to_string(),
            Some(Value::Time(..)) => "time".to_string(),
        };

        let _ = write!(
            out,
            "{{name={wire_name}, wire={wire_type}, {cs_str}, {match_info}, val={val_desc}}}"
        );
    }
    out.push(']');
    out
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
            SqlScalarType::Bool => packer.push(Datum::from(from_value_opt::<bool>(value)?)),
            SqlScalarType::UInt16 => packer.push(Datum::from(from_value_opt::<u16>(value)?)),
            SqlScalarType::Int16 => packer.push(Datum::from(from_value_opt::<i16>(value)?)),
            SqlScalarType::UInt32 => packer.push(Datum::from(from_value_opt::<u32>(value)?)),
            SqlScalarType::Int32 => packer.push(Datum::from(from_value_opt::<i32>(value)?)),
            SqlScalarType::UInt64 => {
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
            SqlScalarType::Int64 => packer.push(Datum::from(from_value_opt::<i64>(value)?)),
            SqlScalarType::Float32 => packer.push(Datum::from(from_value_opt::<f32>(value)?)),
            SqlScalarType::Float64 => packer.push(Datum::from(from_value_opt::<f64>(value)?)),
            SqlScalarType::Char { length } => {
                let val = from_value_opt::<String>(value)?;
                check_char_length(length.map(|l| l.into_u32()), &val, col_desc)?;
                packer.push(Datum::String(&val));
            }
            SqlScalarType::VarChar { max_length } => {
                let val = from_value_opt::<String>(value)?;
                check_char_length(max_length.map(|l| l.into_u32()), &val, col_desc)?;
                packer.push(Datum::String(&val));
            }
            SqlScalarType::String => {
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
                                let enum_int = usize::try_from(val)?;

                                // If mysql strict mode is disabled when an invalid entry is inserted
                                // then the entry will be replicated as a 0. Outside the 1 indexed enum.
                                // https://dev.mysql.com/doc/refman/8.4/en/enum.html#enum-indexes
                                if enum_int == 0 {
                                    packer.push(Datum::String(""));
                                } else {
                                    // Enum types are provided as 1-indexed integers in the replication
                                    // stream, so we need to find the string value from the enum meta
                                    let enum_val = e.values.get(enum_int - 1).ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "received invalid enum value: {} for column {}",
                                            val,
                                            col_desc.name
                                        )
                                    })?;

                                    packer.push(Datum::String(enum_val));
                                }
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
            SqlScalarType::Jsonb => {
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
            SqlScalarType::Bytes => {
                let data = from_value_opt::<Vec<u8>>(value)?;
                packer.push(Datum::Bytes(&data));
            }
            SqlScalarType::Date => {
                let date = Date::try_from(from_value_opt::<chrono::NaiveDate>(value)?)?;
                packer.push(Datum::from(date));
            }
            SqlScalarType::Timestamp { precision: _ } => {
                // Timestamps are encoded as different mysql_common::Value types depending on
                // whether they are from a binlog event or a query, and depending on which
                // mysql timestamp version is used. We handle those cases here
                // https://github.com/blackbeam/rust_mysql_common/blob/v0.31.0/src/binlog/value.rs#L87-L155
                // https://github.com/blackbeam/rust_mysql_common/blob/v0.31.0/src/value/mod.rs#L332
                let chrono_timestamp = match value {
                    Value::Date(..) => from_value_opt::<chrono::NaiveDateTime>(value)?,
                    // old temporal format from before MySQL 5.6; didn't support fractional seconds
                    Value::Int(val) => chrono::DateTime::from_timestamp(val, 0)
                        .ok_or_else(|| {
                            anyhow::anyhow!("received invalid timestamp value: {}", val)
                        })?
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
            SqlScalarType::Time => {
                packer.push(Datum::from(from_value_opt::<chrono::NaiveTime>(value)?));
            }
            SqlScalarType::Numeric { max_scale } => {
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
