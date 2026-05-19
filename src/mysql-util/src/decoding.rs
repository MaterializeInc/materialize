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

/// Canonical text for the MySQL zero-date sentinel ('0000-00-00 00:00:00').
/// In binlog MYSQL_TYPE_DATETIME/MYSQL_TYPE_DATETIME2 encodings, sec=0 *cannot* represent unix
/// epoch 0.  The TIMESTAMP type's supported range starts at '1970-01-01 00:00:01' UTC
/// (<https://dev.mysql.com/doc/refman/8.0/en/datetime.html>), so any sec=0 is
/// unambiguously this sentinel.
const MYSQL_ZERO_TIMESTAMP: &str = "0000-00-00 00:00:00";

/// Format the zero-date sentinel for a column with the given fractional
/// precision (matches the Date arm's `{:0precision$}` behavior).
fn mysql_zero_timestamp(precision: u32) -> String {
    if precision > 0 {
        format!(
            "{}.{}",
            MYSQL_ZERO_TIMESTAMP,
            "0".repeat(usize::cast_from(precision))
        )
    } else {
        MYSQL_ZERO_TIMESTAMP.to_string()
    }
}

pub fn pack_mysql_row(
    row_container: &mut Row,
    row: MySqlRow,
    table_desc: &MySqlTableDesc,
    gtid_set: Option<&str>,
    binlog_full_metadata: bool,
) -> Result<Row, MySqlError> {
    let mut packer = row_container.packer();

    // For each column in `table_desc` (in descriptor order), resolve its wire
    // index. With binlog_full_metadata=true, columns are matched by name so a reordered upstream
    // still decodes correctly; without binlog_full_metadata, rows have no column names and must be
    // matched positionally. A `None` here means the upstream row is missing this column and is
    // only tolerated for ignored columns, and for binlog_full_metadata = false, is only tolerated
    // for ignored columns at the end of the table.
    for (i, col_desc) in table_desc.columns.iter().enumerate() {
        if col_desc.column_type.is_none() {
            // This column is ignored, so don't decode it.
            continue;
        }
        let wire_idx = if !binlog_full_metadata {
            // No column name metadata, so we match by index.
            (i < row.len()).then_some(i)
        } else {
            // This means the row from the binlog has column name included in the metadata,
            // so we can match on that instead of position.
            row.columns_ref()
                .iter()
                .position(|wc| wc.name_str() == col_desc.name.as_str())
        };

        let wire_idx = match wire_idx {
            Some(idx) => idx,
            None => {
                // We could not find a column in the incoming row that matches this descriptor column.
                // This is an error as the column is not ignored (ignored columns have already been skipped).
                return Err(decode_error(
                    "extra column description",
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
                        // Materialize treats DATETIME and TIMESTAMP as MySqlColumnMeta::Timestamp,
                        // but they have slightly different semantics as far as the range of dates
                        // they can represent.
                        // (see https://dev.mysql.com/doc/refman/8.0/en/date-and-time-types.html).
                        //
                        // Three mysql_common::Value variants exist, which are mapped to
                        // [`MySqlColumnMeta::Timestamp`]
                        // (see https://github.com/blackbeam/rust_mysql_common/blob/2e6f6696de03c91b9fd95a87356d081285290704/src/binlog/value.rs):
                        //   Value::Date  — MZ snapshot & binlog MYSQL_TYPE_DATETIME/MYSQL_TYPE_DATETIME2
                        //                  (value/mod.rs:443-445, binlog/value.rs:109-161)
                        //   Value::Int   — legacy binlog MYSQL_TYPE_TIMESTAMP, pre-5.6,
                        //                  4-byte unix epoch (binlog/value.rs:87-90)
                        //   Value::Bytes — binlog MYSQL_TYPE_TIMESTAMP2, 5.6+,
                        //                  "<sec>" or "<sec>.<usec>" (binlog/value.rs:145-154)
                        let str_timestamp = match value {
                            Value::Date(y, m, d, h, mm, s, ms) => {
                                if *precision > 0 {
                                    let precision: usize = (*precision).try_into()?;
                                    format!(
                                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:0precision$}",
                                        y,
                                        m,
                                        d,
                                        h,
                                        mm,
                                        s,
                                        ms,
                                        precision = precision
                                    )
                                } else {
                                    format!(
                                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                                        y, m, d, h, mm, s
                                    )
                                }
                            }
                            // Pre-5.6 unix epoch, no fractional seconds.
                            // val == 0 is the zero-date sentinel, not epoch 0.
                            Value::Int(0) => mysql_zero_timestamp(*precision),
                            Value::Int(val) => chrono::DateTime::from_timestamp(val, 0)
                                .ok_or_else(|| {
                                    anyhow::anyhow!("received invalid timestamp value: {}", val)
                                })?
                                .naive_utc()
                                .format("%Y-%m-%d %H:%M:%S")
                                .to_string(),
                            // 5.6+ epoch string; parse + reformat so all variants emit the
                            // same canonical YYYY-MM-DD HH:MM:SS[.ffff] text.
                            Value::Bytes(data) => {
                                let s = std::str::from_utf8(&data).map_err(|_| {
                                    anyhow::anyhow!("received invalid timestamp value: {:?}", data)
                                })?;
                                // sec=0 (with or without fractional component) is the
                                // zero-date sentinel.
                                if s.split('.').next() == Some("0") {
                                    mysql_zero_timestamp(*precision)
                                } else {
                                    let dt = if s.contains('.') {
                                        chrono::NaiveDateTime::parse_from_str(s, "%s%.6f")
                                    } else {
                                        chrono::NaiveDateTime::parse_from_str(s, "%s")
                                    }
                                    .map_err(|_| {
                                        anyhow::anyhow!("received invalid timestamp value: {:?}", s)
                                    })?;
                                    if *precision > 0 {
                                        let p: usize = (*precision).try_into()?;
                                        dt.format(&format!("%Y-%m-%d %H:%M:%S.%{p}f")).to_string()
                                    } else {
                                        dt.format("%Y-%m-%d %H:%M:%S").to_string()
                                    }
                                }
                            }
                            _ => Err(anyhow::anyhow!(
                                "received unexpected value for timestamp type: {:?}",
                                value
                            ))?,
                        };
                        packer.push(Datum::String(&str_timestamp));
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
                // https://github.com/blackbeam/rust_mysql_common/blob/v0.35.5/src/binlog/value.rs#L87-L155
                // https://github.com/blackbeam/rust_mysql_common/blob/v0.35.5/src/value/mod.rs#L332
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

#[cfg(test)]
mod tests {
    //! Unit tests for the TEXT-COLUMNS decoding of MySQL TIMESTAMP values.
    //!
    //! These cover the regression where a MySQL TIMESTAMP column declared as
    //! a TEXT COLUMN fails to decode when the wire value arrives as
    //! `Value::Bytes("<unix-epoch>")` or `Value::Int(<unix-epoch>)` instead
    //! of `Value::Date(..)`. The integration test in
    //! `test/mysql-cdc/text-columns-timestamp.td` exercises this through
    //! a real MySQL container but is non-deterministic: which `Value`
    //! variant `mysql-async` produces depends on connection-state timing.
    //! These unit tests pin each variant down directly.
    //!
    //! The wire-variant matrix exercised below is derived from mysql_common
    //! v0.35.5:
    //!
    //!   * Value::Int(epoch) — binlog MYSQL_TYPE_TIMESTAMP (pre-5.6):
    //!     https://github.com/blackbeam/rust_mysql_common/blob/v0.35.5/src/binlog/value.rs#L87-L90
    //!   * Value::Bytes("<sec>"/"<sec>.<usec>") — binlog MYSQL_TYPE_TIMESTAMP2 (5.6+):
    //!     https://github.com/blackbeam/rust_mysql_common/blob/v0.35.5/src/binlog/value.rs#L145-L154
    //!   * Value::Date(...) — binary query response + binlog DATETIME[2]:
    //!     https://github.com/blackbeam/rust_mysql_common/blob/v0.35.5/src/value/mod.rs#L443-L445
    //!     https://github.com/blackbeam/rust_mysql_common/blob/v0.35.5/src/binlog/value.rs#L109-L161
    //!
    //! MySQL semantics referenced by the zero-date and fractional-precision
    //! cases:
    //!
    //!   * Zero-date allowed when sql_mode disables NO_ZERO_DATE:
    //!     https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_date
    //!   * TIMESTAMP(p) / DATETIME(p) fractional seconds:
    //!     https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
    use super::*;
    use mz_repr::{SqlColumnType, SqlScalarType};

    fn timestamp_text_col(precision: u32) -> MySqlColumnDesc {
        MySqlColumnDesc {
            name: "created_at".to_string(),
            column_type: Some(SqlColumnType {
                scalar_type: SqlScalarType::String,
                nullable: true,
            }),
            meta: Some(MySqlColumnMeta::Timestamp(precision)),
        }
    }

    fn pack_one(value: Value, col: &MySqlColumnDesc) -> Result<String, anyhow::Error> {
        let mut row = Row::default();
        pack_val_as_datum(value, col, &mut row.packer())?;
        Ok(row.unpack_first().unwrap_str().to_string())
    }

    #[mz_ore::test]
    fn timestamp_value_date_no_precision() {
        let col = timestamp_text_col(0);
        let s = pack_one(Value::Date(2024, 4, 3, 10, 15, 13, 0), &col).unwrap();
        assert_eq!(s, "2024-04-03 10:15:13");
    }

    #[mz_ore::test]
    fn timestamp_value_date_with_precision() {
        let col = timestamp_text_col(6);
        let s = pack_one(Value::Date(2024, 4, 3, 10, 15, 13, 123456), &col).unwrap();
        assert_eq!(s, "2024-04-03 10:15:13.123456");
    }

    #[mz_ore::test]
    fn timestamp_value_date_zero_date() {
        // The whole reason TEXT COLUMNS exists for TIMESTAMP: a
        // zero-date arriving as Value::Date(0,..) should round-trip as
        // the literal MySQL zero-timestamp string.
        let col = timestamp_text_col(0);
        let s = pack_one(Value::Date(0, 0, 0, 0, 0, 0, 0), &col).unwrap();
        assert_eq!(s, "0000-00-00 00:00:00");
    }

    /// Regression: Value::Int (pre-5.6 legacy temporal format, unix
    /// epoch seconds) was previously rejected with
    /// `received unexpected value for timestamp type: Int(..)`.
    #[mz_ore::test]
    fn timestamp_value_int_epoch() {
        let col = timestamp_text_col(0);
        // 1743661234 == 2025-04-03 06:20:34 UTC
        let s = pack_one(Value::Int(1_743_661_234), &col).unwrap();
        assert_eq!(s, "2025-04-03 06:20:34");
    }

    /// sec=0 in the legacy TIMESTAMP encoding is the zero-date sentinel,
    /// not unix epoch 0 — TIMESTAMP's range starts at '1970-01-01 00:00:01'
    /// UTC so epoch 0 isn't a representable column value.
    #[mz_ore::test]
    fn timestamp_value_int_zero_is_sentinel() {
        let col = timestamp_text_col(0);
        let s = pack_one(Value::Int(0), &col).unwrap();
        assert_eq!(s, "0000-00-00 00:00:00");
    }

    /// Out-of-range epochs must error rather than silently producing
    /// a zero-timestamp — they aren't the MySQL zero-date marker, just
    /// garbage chrono can't represent.
    #[mz_ore::test]
    fn timestamp_value_int_out_of_range_errors() {
        let col = timestamp_text_col(0);
        let err = pack_one(Value::Int(i64::MAX), &col).unwrap_err();
        assert!(
            err.to_string().contains("invalid timestamp value"),
            "unexpected error message: {err}"
        );
    }

    /// Regression: Value::Bytes carrying a unix-epoch string is the
    /// wire variant that triggered the production failure
    ///   received unexpected value for timestamp type: Bytes("17436613..")
    #[mz_ore::test]
    fn timestamp_value_bytes_epoch() {
        let col = timestamp_text_col(0);
        let s = pack_one(Value::Bytes(b"1743661234".to_vec()), &col).unwrap();
        assert_eq!(s, "2025-04-03 06:20:34");
    }

    /// sec=0 in the TIMESTAMP2 encoding is the zero-date sentinel; same
    /// reasoning as `timestamp_value_int_zero_is_sentinel`.
    #[mz_ore::test]
    fn timestamp_value_bytes_zero_is_sentinel() {
        let col = timestamp_text_col(0);
        let s = pack_one(Value::Bytes(b"0".to_vec()), &col).unwrap();
        assert_eq!(s, "0000-00-00 00:00:00");
    }

    /// Sentinel detection survives a fractional component ("0.NNNNNN"),
    /// and the helper pads the output to the column's precision so that
    /// snapshot and binlog paths produce identical text for the same
    /// upstream row.
    #[mz_ore::test]
    fn timestamp_value_bytes_zero_with_fractional_is_sentinel() {
        let col = timestamp_text_col(6);
        let s = pack_one(Value::Bytes(b"0.000000".to_vec()), &col).unwrap();
        assert_eq!(s, "0000-00-00 00:00:00.000000");
    }

    /// Fractional form of the TIMESTAMP2 binlog encoding —
    /// "<sec>.<usec>" wrapped in Value::Bytes (binlog/value.rs:151-153).
    /// Hits the `s.contains('.')` branch and the precision-aware
    /// reformat.
    #[mz_ore::test]
    fn timestamp_value_bytes_epoch_fractional() {
        let col = timestamp_text_col(6);
        let s = pack_one(Value::Bytes(b"1743661234.123456".to_vec()), &col).unwrap();
        assert_eq!(s, "2025-04-03 06:20:34.123456");
    }

    /// Bytes that aren't valid UTF-8 should produce a meaningful error,
    /// not a panic.
    #[mz_ore::test]
    fn timestamp_value_bytes_invalid_utf8_errors() {
        let col = timestamp_text_col(0);
        // 0xC3 0x28 is an invalid 2-byte UTF-8 sequence.
        let err = pack_one(Value::Bytes(vec![0xC3, 0x28]), &col).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid timestamp value"),
            "unexpected error message: {msg}"
        );
    }

    /// Bytes that are valid UTF-8 but not parseable as a unix epoch
    /// should produce the same structured error as invalid UTF-8 —
    /// covers the chrono parse failure path that
    /// `timestamp_value_bytes_invalid_utf8_errors` doesn't reach.
    #[mz_ore::test]
    fn timestamp_value_bytes_unparseable_errors() {
        let col = timestamp_text_col(0);
        for payload in [&b""[..], &b"not-an-epoch"[..], &b"2024-04-03 10:15:13"[..]] {
            let err = pack_one(Value::Bytes(payload.to_vec()), &col).unwrap_err();
            assert!(
                err.to_string().contains("invalid timestamp value"),
                "payload {payload:?}: unexpected error message: {err}"
            );
        }
    }

    /// Variants that have no defined mapping for a TIMESTAMP column
    /// must still produce the existing structured decode error so the
    /// source health surface can flag them.
    #[mz_ore::test]
    fn timestamp_value_unsupported_variant_errors() {
        let col = timestamp_text_col(0);
        let err = pack_one(Value::Float(1.0), &col).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unexpected value for timestamp"),
            "unexpected error message: {msg}"
        );
    }
}
