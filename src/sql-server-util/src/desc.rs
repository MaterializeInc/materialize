// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metadata about tables, columns, and other objects from SQL Server.
//!
//! ### Tables
//!
//! When creating a SQL Server source we will query system tables from the
//! upstream instance to get a [`SqlServerTableRaw`]. From this raw information
//! we create a [`SqlServerTableDesc`] which describes how the upstream table
//! will get represented in Materialize.
//!
//! ### Rows
//!
//! With a [`SqlServerTableDesc`] and an [`mz_repr::RelationDesc`] we can
//! create a [`SqlServerRowDecoder`] which will be used when running a source
//! to efficiently decode [`tiberius::Row`]s into [`mz_repr::Row`]s.

use base64::Engine;
use chrono::{NaiveDateTime, SubsecRound};
use dec::OrderedDecimal;
use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType, RustType};
use mz_repr::adt::char::CharLength;
use mz_repr::adt::numeric::{Numeric, NumericMaxScale};
use mz_repr::adt::timestamp::{CheckedTimestamp, TimestampPrecision};
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{Datum, RelationDesc, Row, RowArena, SqlColumnType, SqlScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::{SqlServerDecodeError, SqlServerError};

include!(concat!(env!("OUT_DIR"), "/mz_sql_server_util.rs"));

/// Materialize compatible description of a table in Microsoft SQL Server.
///
/// See [`SqlServerTableRaw`] for the raw information we read from the upstream
/// system.
///
/// Note: We map a [`SqlServerTableDesc`] to a Materialize [`RelationDesc`] as
/// part of purification. Specifically we use this description to generate a
/// SQL statement for subsource and it's the _parsing of that statement_ which
/// actually generates a [`RelationDesc`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct SqlServerTableDesc {
    /// Name of the schema that the table belongs to.
    pub schema_name: Arc<str>,
    /// Name of the table.
    pub name: Arc<str>,
    /// Columns for the table.
    pub columns: Box<[SqlServerColumnDesc]>,
}

impl SqlServerTableDesc {
    /// Creating a [`SqlServerTableDesc`] from a [`SqlServerTableRaw`] description.
    ///
    /// Note: Not all columns from SQL Server can be ingested into Materialize. To determine if a
    /// column is supported see [`SqlServerColumnDesc::is_supported`].
    pub fn new(raw: SqlServerTableRaw) -> Self {
        let columns: Box<[_]> = raw
            .columns
            .into_iter()
            .map(SqlServerColumnDesc::new)
            .collect();
        SqlServerTableDesc {
            schema_name: raw.schema_name,
            name: raw.name,
            columns,
        }
    }

    /// Returns the [`SqlServerQualifiedTableName`] for this [`SqlServerTableDesc`].
    pub fn qualified_name(&self) -> SqlServerQualifiedTableName {
        SqlServerQualifiedTableName {
            schema_name: Arc::clone(&self.schema_name),
            table_name: Arc::clone(&self.name),
        }
    }

    /// Update this [`SqlServerTableDesc`] to represent the specified columns
    /// as text in Materialize.
    pub fn apply_text_columns(&mut self, text_columns: &BTreeSet<&str>) {
        for column in &mut self.columns {
            if text_columns.contains(column.name.as_ref()) {
                column.represent_as_text();
            }
        }
    }

    /// Update this [`SqlServerTableDesc`] to exclude the specified columns from being
    /// replicated into Materialize.
    pub fn apply_excl_columns(&mut self, excl_columns: &BTreeSet<&str>) {
        for column in &mut self.columns {
            if excl_columns.contains(column.name.as_ref()) {
                column.exclude();
            }
        }
    }

    /// Returns a [`SqlServerRowDecoder`] which can be used to decode [`tiberius::Row`]s into
    /// [`mz_repr::Row`]s that match the shape of the provided [`RelationDesc`].
    pub fn decoder(&self, desc: &RelationDesc) -> Result<SqlServerRowDecoder, SqlServerError> {
        let decoder = SqlServerRowDecoder::try_new(self, desc)?;
        Ok(decoder)
    }
}

impl RustType<ProtoSqlServerTableDesc> for SqlServerTableDesc {
    fn into_proto(&self) -> ProtoSqlServerTableDesc {
        ProtoSqlServerTableDesc {
            name: self.name.to_string(),
            schema_name: self.schema_name.to_string(),
            columns: self.columns.iter().map(|c| c.into_proto()).collect(),
        }
    }

    fn from_proto(proto: ProtoSqlServerTableDesc) -> Result<Self, mz_proto::TryFromProtoError> {
        let columns = proto
            .columns
            .into_iter()
            .map(|c| c.into_rust())
            .collect::<Result<_, _>>()?;
        Ok(SqlServerTableDesc {
            schema_name: proto.schema_name.into(),
            name: proto.name.into(),
            columns,
        })
    }
}

/// Partially qualified name of a table from Microsoft SQL Server.
///
/// TODO(sql_server3): Change this to use a &str.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlServerQualifiedTableName {
    pub schema_name: Arc<str>,
    pub table_name: Arc<str>,
}

impl ToString for SqlServerQualifiedTableName {
    fn to_string(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}

/// Raw metadata for a table from Microsoft SQL Server.
///
/// See [`SqlServerTableDesc`] for a refined description that is compatible
/// with Materialize.
#[derive(Debug, Clone)]
pub struct SqlServerTableRaw {
    /// Name of the schema the table belongs to.
    pub schema_name: Arc<str>,
    /// Name of the table.
    pub name: Arc<str>,
    /// The capture instance replicating changes.
    pub capture_instance: Arc<SqlServerCaptureInstanceRaw>,
    /// Columns for the table.
    pub columns: Arc<[SqlServerColumnRaw]>,
}

/// Raw capture instance metadata.
#[derive(Debug, Clone)]
pub struct SqlServerCaptureInstanceRaw {
    /// The capture instance replicating changes.
    pub name: Arc<str>,
    /// The creation date of the capture instance.
    pub create_date: Arc<NaiveDateTime>,
}

/// Description of a column from a table in Microsoft SQL Server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct SqlServerColumnDesc {
    /// Name of the column.
    pub name: Arc<str>,
    /// The intended data type of the this column in Materialize. `None` indicates this
    /// column should be excluded when replicating into Materialize.
    ///
    /// Note: This type might differ from the `decode_type`, e.g. a user can
    /// specify `TEXT COLUMNS` to decode columns as text.
    pub column_type: Option<SqlColumnType>,
    /// If this column is part of the primary key for the table, and the name of the constraint.
    pub primary_key_constraint: Option<Arc<str>>,
    /// Rust type we should parse the data from a [`tiberius::Row`] as.
    pub decode_type: SqlServerColumnDecodeType,
    /// Raw type of the column as we read it from upstream.
    ///
    /// This is useful to keep around for debugging purposes.
    pub raw_type: Arc<str>,
}

impl SqlServerColumnDesc {
    /// Create a [`SqlServerColumnDesc`] from a [`SqlServerColumnRaw`] description.
    pub fn new(raw: &SqlServerColumnRaw) -> Self {
        let (column_type, decode_type) = match parse_data_type(raw) {
            Ok((scalar_type, decode_type)) => {
                let column_type = scalar_type.nullable(raw.is_nullable);
                (Some(column_type), decode_type)
            }
            Err(err) => {
                tracing::warn!(
                    ?err,
                    ?raw,
                    "found an unsupported data type when parsing raw data"
                );
                (
                    None,
                    SqlServerColumnDecodeType::Unsupported {
                        context: err.reason,
                    },
                )
            }
        };
        SqlServerColumnDesc {
            name: Arc::clone(&raw.name),
            primary_key_constraint: raw.primary_key_constraint.clone(),
            column_type,
            decode_type,
            raw_type: Arc::clone(&raw.data_type),
        }
    }

    /// Returns true if this column can be replicated into Materialize.
    pub fn is_supported(&self) -> bool {
        !matches!(
            self.decode_type,
            SqlServerColumnDecodeType::Unsupported { .. }
        )
    }

    /// Change this [`SqlServerColumnDesc`] to be represented as text in Materialize.
    pub fn represent_as_text(&mut self) {
        self.column_type = self
            .column_type
            .as_ref()
            .map(|ct| SqlScalarType::String.nullable(ct.nullable));
    }

    /// Exclude this [`SqlServerColumnDesc`] from being replicated into Materialize.
    pub fn exclude(&mut self) {
        self.column_type = None;
    }

    /// Check if this [`SqlServerColumnDesc`] is excluded from being replicated into Materialize.
    pub fn is_excluded(&self) -> bool {
        self.column_type.is_none()
    }
}

impl RustType<ProtoSqlServerColumnDesc> for SqlServerColumnDesc {
    fn into_proto(&self) -> ProtoSqlServerColumnDesc {
        ProtoSqlServerColumnDesc {
            name: self.name.to_string(),
            column_type: self.column_type.into_proto(),
            primary_key_constraint: self.primary_key_constraint.as_ref().map(|v| v.to_string()),
            decode_type: Some(self.decode_type.into_proto()),
            raw_type: self.raw_type.to_string(),
        }
    }

    fn from_proto(proto: ProtoSqlServerColumnDesc) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(SqlServerColumnDesc {
            name: proto.name.into(),
            column_type: proto.column_type.into_rust()?,
            primary_key_constraint: proto.primary_key_constraint.map(|v| v.into()),
            decode_type: proto
                .decode_type
                .into_rust_if_some("ProtoSqlServerColumnDesc::decode_type")?,
            raw_type: proto.raw_type.into(),
        })
    }
}

/// The raw datatype from SQL Server is not supported in Materialize.
#[derive(Debug)]
#[allow(dead_code)]
pub struct UnsupportedDataType {
    column_name: String,
    column_type: String,
    reason: String,
}

/// Parse a raw data type from SQL Server into a Materialize [`SqlScalarType`].
///
/// Returns the [`SqlScalarType`] that we'll map this column to and the [`SqlServerColumnDecodeType`]
/// that we use to decode the raw value.
fn parse_data_type(
    raw: &SqlServerColumnRaw,
) -> Result<(SqlScalarType, SqlServerColumnDecodeType), UnsupportedDataType> {
    let scalar = match raw.data_type.to_lowercase().as_str() {
        "tinyint" => (SqlScalarType::Int16, SqlServerColumnDecodeType::U8),
        "smallint" => (SqlScalarType::Int16, SqlServerColumnDecodeType::I16),
        "int" => (SqlScalarType::Int32, SqlServerColumnDecodeType::I32),
        "bigint" => (SqlScalarType::Int64, SqlServerColumnDecodeType::I64),
        "bit" => (SqlScalarType::Bool, SqlServerColumnDecodeType::Bool),
        "decimal" | "numeric" | "money" | "smallmoney" => {
            // SQL Server supports a precision in the range of [1, 38] and then
            // the scale is 0 <= scale <= precision.
            //
            // Materialize numerics are floating point with a fixed precision of 39.
            //
            // See: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-ver16#arguments>
            if raw.precision > 38 || raw.scale > raw.precision {
                tracing::warn!(
                    "unexpected value from SQL Server, precision of {} and scale of {}",
                    raw.precision,
                    raw.scale,
                );
            }
            if raw.precision > 39 {
                let reason = format!(
                    "precision of {} is greater than our maximum of 39",
                    raw.precision
                );
                return Err(UnsupportedDataType {
                    column_name: raw.name.to_string(),
                    column_type: raw.data_type.to_string(),
                    reason,
                });
            }

            let raw_scale = usize::cast_from(raw.scale);
            let max_scale =
                NumericMaxScale::try_from(raw_scale).map_err(|_| UnsupportedDataType {
                    column_type: raw.data_type.to_string(),
                    column_name: raw.name.to_string(),
                    reason: format!("scale of {} is too large", raw.scale),
                })?;
            let column_type = SqlScalarType::Numeric {
                max_scale: Some(max_scale),
            };

            (column_type, SqlServerColumnDecodeType::Numeric)
        }
        "real" => (SqlScalarType::Float32, SqlServerColumnDecodeType::F32),
        "double" => (SqlScalarType::Float64, SqlServerColumnDecodeType::F64),
        dt @ ("char" | "nchar" | "varchar" | "nvarchar" | "sysname") => {
            // When the `max_length` is -1 SQL Server will not present us with the "before" value
            // for updated columns.
            //
            // TODO(sql_server3): Support UPSERT semantics for SQL Server.
            if raw.max_length == -1 {
                return Err(UnsupportedDataType {
                    column_name: raw.name.to_string(),
                    column_type: raw.data_type.to_string(),
                    reason: "columns with unlimited size do not support CDC".to_string(),
                });
            }

            let column_type = match dt {
                "char" => {
                    let length = CharLength::try_from(i64::from(raw.max_length)).map_err(|e| {
                        UnsupportedDataType {
                            column_name: raw.name.to_string(),
                            column_type: raw.data_type.to_string(),
                            reason: e.to_string(),
                        }
                    })?;
                    SqlScalarType::Char {
                        length: Some(length),
                    }
                }
                "varchar" => {
                    let length =
                        VarCharMaxLength::try_from(i64::from(raw.max_length)).map_err(|e| {
                            UnsupportedDataType {
                                column_name: raw.name.to_string(),
                                column_type: raw.data_type.to_string(),
                                reason: e.to_string(),
                            }
                        })?;
                    SqlScalarType::VarChar {
                        max_length: Some(length),
                    }
                }
                // Determining the max character count for these types is difficult
                // because of different character encodings, so we fallback to just
                // representing them as "text".
                "nchar" | "nvarchar" | "sysname" => SqlScalarType::String,
                other => unreachable!("'{other}' checked above"),
            };

            (column_type, SqlServerColumnDecodeType::String)
        }
        "text" | "ntext" | "image" => {
            // SQL Server docs indicate this should always be 16. There's no
            // issue if it's not, but it's good to track.
            mz_ore::soft_assert_eq_no_log!(raw.max_length, 16);

            // TODO(sql_server3): Support UPSERT semantics for SQL Server.
            return Err(UnsupportedDataType {
                column_name: raw.name.to_string(),
                column_type: raw.data_type.to_string(),
                reason: "columns with unlimited size do not support CDC".to_string(),
            });
        }
        "xml" => {
            // When the `max_length` is -1 SQL Server will not present us with the "before" value
            // for updated columns.
            //
            // TODO(sql_server3): Support UPSERT semantics for SQL Server.
            if raw.max_length == -1 {
                return Err(UnsupportedDataType {
                    column_name: raw.name.to_string(),
                    column_type: raw.data_type.to_string(),
                    reason: "columns with unlimited size do not support CDC".to_string(),
                });
            }
            (SqlScalarType::String, SqlServerColumnDecodeType::Xml)
        }
        "binary" | "varbinary" => {
            // When the `max_length` is -1 if this column changes as part of an `UPDATE`
            // or `DELETE` statement, SQL Server will not provide the "old" value for
            // this column, but we need this value so we can emit a retraction.
            //
            // TODO(sql_server3): Support UPSERT semantics for SQL Server.
            if raw.max_length == -1 {
                return Err(UnsupportedDataType {
                    column_name: raw.name.to_string(),
                    column_type: raw.data_type.to_string(),
                    reason: "columns with unlimited size do not support CDC".to_string(),
                });
            }

            (SqlScalarType::Bytes, SqlServerColumnDecodeType::Bytes)
        }
        "json" => (SqlScalarType::Jsonb, SqlServerColumnDecodeType::String),
        "date" => (SqlScalarType::Date, SqlServerColumnDecodeType::NaiveDate),
        // SQL Server supports a scale of (and defaults to) 7 digits (aka 100 nanoseconds)
        // for time related types.
        //
        // Internally Materialize supports a scale of 9 (aka nanoseconds), but for Postgres
        // compatibility we constraint ourselves to a scale of 6 (aka microseconds). By
        // default we will round values we get from  SQL Server to fit in Materialize.
        //
        // TODO(sql_server3): Support a "strict" mode where we're fail the creation of the
        // source if the scale is too large.
        // TODO(sql_server3): Support specifying a precision for SqlScalarType::Time.
        //
        // See: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql?view=sql-server-ver16>.
        "time" => (SqlScalarType::Time, SqlServerColumnDecodeType::NaiveTime),
        dt @ ("smalldatetime" | "datetime" | "datetime2" | "datetimeoffset") => {
            if raw.scale > 7 {
                tracing::warn!("unexpected scale '{}' from SQL Server", raw.scale);
            }
            if raw.scale > mz_repr::adt::timestamp::MAX_PRECISION {
                tracing::warn!("truncating scale of '{}' for '{}'", raw.scale, dt);
            }
            let precision = std::cmp::min(raw.scale, mz_repr::adt::timestamp::MAX_PRECISION);
            let precision =
                Some(TimestampPrecision::try_from(i64::from(precision)).expect("known to fit"));

            match dt {
                "smalldatetime" | "datetime" | "datetime2" => (
                    SqlScalarType::Timestamp { precision },
                    SqlServerColumnDecodeType::NaiveDateTime,
                ),
                "datetimeoffset" => (
                    SqlScalarType::TimestampTz { precision },
                    SqlServerColumnDecodeType::DateTime,
                ),
                other => unreachable!("'{other}' checked above"),
            }
        }
        "uniqueidentifier" => (SqlScalarType::Uuid, SqlServerColumnDecodeType::Uuid),
        // TODO(sql_server3): Support reading the following types, at least as text:
        //
        // * geography
        // * geometry
        // * json (preview)
        // * vector (preview)
        //
        // None of these types are implemented in `tiberius`, the crate that
        // provides our SQL Server client, so we'll need to implement support
        // for decoding them.
        //
        // See <https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/355f7890-6e91-4978-ab76-2ded17ee09bc>.
        other => {
            return Err(UnsupportedDataType {
                column_type: other.to_string(),
                column_name: raw.name.to_string(),
                reason: format!("'{other}' is unimplemented"),
            });
        }
    };
    Ok(scalar)
}

/// Raw metadata for a column from a table in Microsoft SQL Server.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-columns-transact-sql?view=sql-server-ver16>.
#[derive(Clone, Debug)]
pub struct SqlServerColumnRaw {
    /// Name of this column.
    pub name: Arc<str>,
    /// Name of the data type.
    pub data_type: Arc<str>,
    /// Whether or not the column is nullable.
    pub is_nullable: bool,
    /// If the column is part of the primary key for the table, and the name of the constraint.
    pub primary_key_constraint: Option<Arc<str>>,
    /// Maximum length (in bytes) of the column.
    ///
    /// For `varchar(max)`, `nvarchar(max)`, `varbinary(max)`, or `xml` this will be `-1`. For
    /// `text`, `ntext`, and `image` columns this will be 16.
    ///
    /// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-columns-transact-sql?view=sql-server-ver16>.
    ///
    /// TODO(sql_server2): Validate this value for `json` columns where were introduced
    /// Azure SQL 2024.
    pub max_length: i16,
    /// Precision of the column, if numeric-based; otherwise 0.
    pub precision: u8,
    /// Scale of the columns, if numeric-based; otherwise 0.
    pub scale: u8,
}

/// Rust type that we should use when reading a column from SQL Server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub enum SqlServerColumnDecodeType {
    Bool,
    U8,
    I16,
    I32,
    I64,
    F32,
    F64,
    String,
    Bytes,
    /// [`uuid::Uuid`].
    Uuid,
    /// [`tiberius::numeric::Numeric`].
    Numeric,
    /// [`tiberius::xml::XmlData`].
    Xml,
    /// [`chrono::NaiveDate`].
    NaiveDate,
    /// [`chrono::NaiveTime`].
    NaiveTime,
    /// [`chrono::DateTime`].
    DateTime,
    /// [`chrono::NaiveDateTime`].
    NaiveDateTime,
    /// Decoding this type isn't supported.
    Unsupported {
        /// Any additional context as to why this type isn't supported.
        context: String,
    },
}

impl SqlServerColumnDecodeType {
    /// Decode the column with `name` out of the provided `data`.
    pub fn decode<'a>(
        &self,
        data: &'a tiberius::Row,
        name: &'a str,
        column: &'a SqlColumnType,
        arena: &'a RowArena,
    ) -> Result<Datum<'a>, SqlServerDecodeError> {
        let maybe_datum = match (&column.scalar_type, self) {
            (SqlScalarType::Bool, SqlServerColumnDecodeType::Bool) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "bool"))?
                .map(|val: bool| if val { Datum::True } else { Datum::False }),
            (SqlScalarType::Int16, SqlServerColumnDecodeType::U8) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "u8"))?
                .map(|val: u8| Datum::Int16(i16::cast_from(val))),
            (SqlScalarType::Int16, SqlServerColumnDecodeType::I16) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "i16"))?
                .map(Datum::Int16),
            (SqlScalarType::Int32, SqlServerColumnDecodeType::I32) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "i32"))?
                .map(Datum::Int32),
            (SqlScalarType::Int64, SqlServerColumnDecodeType::I64) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "i64"))?
                .map(Datum::Int64),
            (SqlScalarType::Float32, SqlServerColumnDecodeType::F32) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "f32"))?
                .map(|val: f32| Datum::Float32(ordered_float::OrderedFloat(val))),
            (SqlScalarType::Float64, SqlServerColumnDecodeType::F64) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "f64"))?
                .map(|val: f64| Datum::Float64(ordered_float::OrderedFloat(val))),
            (SqlScalarType::String, SqlServerColumnDecodeType::String) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "string"))?
                .map(Datum::String),
            (SqlScalarType::Char { length }, SqlServerColumnDecodeType::String) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "char"))?
                .map(|val: &str| match length {
                    Some(expected) => {
                        let found_chars = val.chars().count();
                        let expct_chars = usize::cast_from(expected.into_u32());
                        if found_chars != expct_chars {
                            Err(SqlServerDecodeError::invalid_char(
                                name,
                                expct_chars,
                                found_chars,
                            ))
                        } else {
                            Ok(Datum::String(val))
                        }
                    }
                    None => Ok(Datum::String(val)),
                })
                .transpose()?,
            (SqlScalarType::VarChar { max_length }, SqlServerColumnDecodeType::String) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "varchar"))?
                .map(|val: &str| match max_length {
                    Some(max) => {
                        let found_chars = val.chars().count();
                        let max_chars = usize::cast_from(max.into_u32());
                        if found_chars > max_chars {
                            Err(SqlServerDecodeError::invalid_varchar(
                                name,
                                max_chars,
                                found_chars,
                            ))
                        } else {
                            Ok(Datum::String(val))
                        }
                    }
                    None => Ok(Datum::String(val)),
                })
                .transpose()?,
            (SqlScalarType::Bytes, SqlServerColumnDecodeType::Bytes) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "bytes"))?
                .map(Datum::Bytes),
            (SqlScalarType::Uuid, SqlServerColumnDecodeType::Uuid) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "uuid"))?
                .map(Datum::Uuid),
            (SqlScalarType::Numeric { .. }, SqlServerColumnDecodeType::Numeric) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "numeric"))?
                .map(|val: tiberius::numeric::Numeric| {
                    let numeric = tiberius_numeric_to_mz_numeric(val);
                    Datum::Numeric(OrderedDecimal(numeric))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::Xml) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "xml"))?
                .map(|val: &tiberius::xml::XmlData| Datum::String(val.as_ref())),
            (SqlScalarType::Date, SqlServerColumnDecodeType::NaiveDate) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "date"))?
                .map(|val: chrono::NaiveDate| {
                    let date = val
                        .try_into()
                        .map_err(|e| SqlServerDecodeError::invalid_date(name, e))?;
                    Ok::<_, SqlServerDecodeError>(Datum::Date(date))
                })
                .transpose()?,
            (SqlScalarType::Time, SqlServerColumnDecodeType::NaiveTime) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "time"))?
                .map(|val: chrono::NaiveTime| {
                    // Postgres' maximum precision is 6 (aka microseconds).
                    //
                    // While the Postgres spec supports specifying a precision
                    // Materialize does not.
                    let rounded = val.round_subsecs(6);
                    // Overflowed.
                    let val = if rounded < val {
                        val.trunc_subsecs(6)
                    } else {
                        val
                    };
                    Datum::Time(val)
                }),
            (SqlScalarType::Timestamp { precision }, SqlServerColumnDecodeType::NaiveDateTime) => {
                data.try_get(name)
                    .map_err(|_| SqlServerDecodeError::invalid_column(name, "timestamp"))?
                    .map(|val: chrono::NaiveDateTime| {
                        let ts: CheckedTimestamp<chrono::NaiveDateTime> = val
                            .try_into()
                            .map_err(|e| SqlServerDecodeError::invalid_timestamp(name, e))?;
                        let rounded = ts
                            .round_to_precision(*precision)
                            .map_err(|e| SqlServerDecodeError::invalid_timestamp(name, e))?;
                        Ok::<_, SqlServerDecodeError>(Datum::Timestamp(rounded))
                    })
                    .transpose()?
            }
            (SqlScalarType::TimestampTz { precision }, SqlServerColumnDecodeType::DateTime) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "timestamptz"))?
                .map(|val: chrono::DateTime<chrono::Utc>| {
                    let ts: CheckedTimestamp<chrono::DateTime<chrono::Utc>> = val
                        .try_into()
                        .map_err(|e| SqlServerDecodeError::invalid_timestamp(name, e))?;
                    let rounded = ts
                        .round_to_precision(*precision)
                        .map_err(|e| SqlServerDecodeError::invalid_timestamp(name, e))?;
                    Ok::<_, SqlServerDecodeError>(Datum::TimestampTz(rounded))
                })
                .transpose()?,
            // We support mapping any type to a string.
            (SqlScalarType::String, SqlServerColumnDecodeType::Bool) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "bool-text"))?
                .map(|val: bool| {
                    if val {
                        Datum::String("true")
                    } else {
                        Datum::String("false")
                    }
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::U8) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "u8-text"))?
                .map(|val: u8| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::I16) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "i16-text"))?
                .map(|val: i16| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::I32) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "i32-text"))?
                .map(|val: i32| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::I64) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "i64-text"))?
                .map(|val: i64| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::F32) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "f32-text"))?
                .map(|val: f32| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::F64) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "f64-text"))?
                .map(|val: f64| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::Uuid) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "uuid-text"))?
                .map(|val: uuid::Uuid| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::Bytes) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "bytes-text"))?
                .map(|val: &[u8]| {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(val);
                    arena.make_datum(|packer| packer.push(Datum::String(&encoded)))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::Numeric) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "numeric-text"))?
                .map(|val: tiberius::numeric::Numeric| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::NaiveDate) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "naivedate-text"))?
                .map(|val: chrono::NaiveDate| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::NaiveTime) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "naivetime-text"))?
                .map(|val: chrono::NaiveTime| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::DateTime) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "datetime-text"))?
                .map(|val: chrono::DateTime<chrono::Utc>| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (SqlScalarType::String, SqlServerColumnDecodeType::NaiveDateTime) => data
                .try_get(name)
                .map_err(|_| SqlServerDecodeError::invalid_column(name, "naivedatetime-text"))?
                .map(|val: chrono::NaiveDateTime| {
                    arena.make_datum(|packer| packer.push(Datum::String(&val.to_string())))
                }),
            (column_type, decode_type) => {
                return Err(SqlServerDecodeError::Unsupported {
                    sql_server_type: decode_type.clone(),
                    mz_type: column_type.clone(),
                });
            }
        };

        match (maybe_datum, column.nullable) {
            (Some(datum), _) => Ok(datum),
            (None, true) => Ok(Datum::Null),
            (None, false) => Err(SqlServerDecodeError::InvalidData {
                column_name: name.to_string(),
                // Note: This error string is durably recorded in Persist, do not change.
                error: "found Null in non-nullable column".to_string(),
            }),
        }
    }
}

impl RustType<proto_sql_server_column_desc::DecodeType> for SqlServerColumnDecodeType {
    fn into_proto(&self) -> proto_sql_server_column_desc::DecodeType {
        match self {
            SqlServerColumnDecodeType::Bool => proto_sql_server_column_desc::DecodeType::Bool(()),
            SqlServerColumnDecodeType::U8 => proto_sql_server_column_desc::DecodeType::U8(()),
            SqlServerColumnDecodeType::I16 => proto_sql_server_column_desc::DecodeType::I16(()),
            SqlServerColumnDecodeType::I32 => proto_sql_server_column_desc::DecodeType::I32(()),
            SqlServerColumnDecodeType::I64 => proto_sql_server_column_desc::DecodeType::I64(()),
            SqlServerColumnDecodeType::F32 => proto_sql_server_column_desc::DecodeType::F32(()),
            SqlServerColumnDecodeType::F64 => proto_sql_server_column_desc::DecodeType::F64(()),
            SqlServerColumnDecodeType::String => {
                proto_sql_server_column_desc::DecodeType::String(())
            }
            SqlServerColumnDecodeType::Bytes => proto_sql_server_column_desc::DecodeType::Bytes(()),
            SqlServerColumnDecodeType::Uuid => proto_sql_server_column_desc::DecodeType::Uuid(()),
            SqlServerColumnDecodeType::Numeric => {
                proto_sql_server_column_desc::DecodeType::Numeric(())
            }
            SqlServerColumnDecodeType::Xml => proto_sql_server_column_desc::DecodeType::Xml(()),
            SqlServerColumnDecodeType::NaiveDate => {
                proto_sql_server_column_desc::DecodeType::NaiveDate(())
            }
            SqlServerColumnDecodeType::NaiveTime => {
                proto_sql_server_column_desc::DecodeType::NaiveTime(())
            }
            SqlServerColumnDecodeType::DateTime => {
                proto_sql_server_column_desc::DecodeType::DateTime(())
            }
            SqlServerColumnDecodeType::NaiveDateTime => {
                proto_sql_server_column_desc::DecodeType::NaiveDateTime(())
            }
            SqlServerColumnDecodeType::Unsupported { context } => {
                proto_sql_server_column_desc::DecodeType::Unsupported(context.clone())
            }
        }
    }

    fn from_proto(
        proto: proto_sql_server_column_desc::DecodeType,
    ) -> Result<Self, mz_proto::TryFromProtoError> {
        let val = match proto {
            proto_sql_server_column_desc::DecodeType::Bool(()) => SqlServerColumnDecodeType::Bool,
            proto_sql_server_column_desc::DecodeType::U8(()) => SqlServerColumnDecodeType::U8,
            proto_sql_server_column_desc::DecodeType::I16(()) => SqlServerColumnDecodeType::I16,
            proto_sql_server_column_desc::DecodeType::I32(()) => SqlServerColumnDecodeType::I32,
            proto_sql_server_column_desc::DecodeType::I64(()) => SqlServerColumnDecodeType::I64,
            proto_sql_server_column_desc::DecodeType::F32(()) => SqlServerColumnDecodeType::F32,
            proto_sql_server_column_desc::DecodeType::F64(()) => SqlServerColumnDecodeType::F64,
            proto_sql_server_column_desc::DecodeType::String(()) => {
                SqlServerColumnDecodeType::String
            }
            proto_sql_server_column_desc::DecodeType::Bytes(()) => SqlServerColumnDecodeType::Bytes,
            proto_sql_server_column_desc::DecodeType::Uuid(()) => SqlServerColumnDecodeType::Uuid,
            proto_sql_server_column_desc::DecodeType::Numeric(()) => {
                SqlServerColumnDecodeType::Numeric
            }
            proto_sql_server_column_desc::DecodeType::Xml(()) => SqlServerColumnDecodeType::Xml,
            proto_sql_server_column_desc::DecodeType::NaiveDate(()) => {
                SqlServerColumnDecodeType::NaiveDate
            }
            proto_sql_server_column_desc::DecodeType::NaiveTime(()) => {
                SqlServerColumnDecodeType::NaiveTime
            }
            proto_sql_server_column_desc::DecodeType::DateTime(()) => {
                SqlServerColumnDecodeType::DateTime
            }
            proto_sql_server_column_desc::DecodeType::NaiveDateTime(()) => {
                SqlServerColumnDecodeType::NaiveDateTime
            }
            proto_sql_server_column_desc::DecodeType::Unsupported(context) => {
                SqlServerColumnDecodeType::Unsupported { context }
            }
        };
        Ok(val)
    }
}

/// Numerics in SQL Server have a maximum precision of 38 digits, where [`Numeric`]s in
/// Materialize have a maximum precision of 39 digits, so this conversion is infallible.
fn tiberius_numeric_to_mz_numeric(val: tiberius::numeric::Numeric) -> Numeric {
    let mut numeric = mz_repr::adt::numeric::cx_datum().from_i128(val.value());
    // Use scaleb to adjust the exponent directly, avoiding precision loss from division
    // scaleb(x, -n) computes x * 10^(-n)
    mz_repr::adt::numeric::cx_datum().scaleb(&mut numeric, &Numeric::from(-i32::from(val.scale())));
    numeric
}

/// A decoder from [`tiberius::Row`] to [`mz_repr::Row`].
///
/// The goal of this type is to perform any expensive "downcasts" so in the hot
/// path of decoding rows we do the minimal amount of work.
#[derive(Debug)]
pub struct SqlServerRowDecoder {
    decoders: Vec<(Arc<str>, SqlColumnType, SqlServerColumnDecodeType)>,
}

impl SqlServerRowDecoder {
    /// Try to create a [`SqlServerRowDecoder`] that will decode [`tiberius::Row`]s that match
    /// the shape of the provided [`SqlServerTableDesc`], to [`mz_repr::Row`]s that match the
    /// shape of the provided [`RelationDesc`].
    pub fn try_new(
        table: &SqlServerTableDesc,
        desc: &RelationDesc,
    ) -> Result<Self, SqlServerError> {
        let decoders = desc
            .iter()
            .map(|(col_name, col_type)| {
                let sql_server_col = table
                    .columns
                    .iter()
                    .find(|col| col.name.as_ref() == col_name.as_str())
                    .ok_or_else(|| {
                        // TODO(sql_server2): Structured Error.
                        anyhow::anyhow!("no SQL Server column with name {col_name} found")
                    })?;
                let Some(sql_server_col_typ) = sql_server_col.column_type.as_ref() else {
                    return Err(SqlServerError::ProgrammingError(format!(
                        "programming error, {col_name} should have been exluded",
                    )));
                };

                // This shouldn't be true, but be defensive.
                //
                // TODO(sql_server2): Maybe allow the Materialize column type to be
                // more nullable than our decoding type?
                //
                // Sad. Our timestamp types don't roundtrip their precision through
                // parsing so we ignore the mismatch here.
                let matches = match (&sql_server_col_typ.scalar_type, &col_type.scalar_type) {
                    (SqlScalarType::Timestamp { .. }, SqlScalarType::Timestamp { .. })
                    | (SqlScalarType::TimestampTz { .. }, SqlScalarType::TimestampTz { .. }) => {
                        // Types match so check nullability.
                        sql_server_col_typ.nullable == col_type.nullable
                    }
                    (_, _) => sql_server_col_typ == col_type,
                };
                if !matches {
                    return Err(SqlServerError::ProgrammingError(format!(
                        "programming error, {col_name} has mismatched type {:?} vs {:?}",
                        sql_server_col.column_type, col_type
                    )));
                }

                let name = Arc::clone(&sql_server_col.name);
                let decoder = sql_server_col.decode_type.clone();
                // Note: We specifically use the `SqlColumnType` from the SqlServerTableDesc
                // because it retains precision.
                //
                // See: <https://github.com/MaterializeInc/database-issues/issues/3179>.
                let col_typ = sql_server_col_typ.clone();

                Ok::<_, SqlServerError>((name, col_typ, decoder))
            })
            .collect::<Result<_, _>>()?;

        Ok(SqlServerRowDecoder { decoders })
    }

    /// Decode data from the provided [`tiberius::Row`] into the provided [`Row`].
    pub fn decode(
        &self,
        data: &tiberius::Row,
        row: &mut Row,
        arena: &RowArena,
    ) -> Result<(), SqlServerDecodeError> {
        let mut packer = row.packer();
        for (col_name, col_type, decoder) in &self.decoders {
            let datum = decoder.decode(data, col_name, col_type, arena)?;
            packer.push(datum);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use chrono::NaiveDateTime;
    use itertools::Itertools;
    use mz_ore::assert_contains;
    use mz_ore::collections::CollectionExt;
    use mz_repr::adt::numeric::NumericMaxScale;
    use mz_repr::adt::varchar::VarCharMaxLength;
    use mz_repr::{Datum, RelationDesc, Row, RowArena, SqlScalarType};
    use tiberius::RowTestExt;

    use crate::desc::{
        SqlServerCaptureInstanceRaw, SqlServerColumnDecodeType, SqlServerColumnDesc,
        SqlServerTableDesc, SqlServerTableRaw, tiberius_numeric_to_mz_numeric,
    };

    use super::SqlServerColumnRaw;

    impl SqlServerColumnRaw {
        /// Create a new [`SqlServerColumnRaw`]. The specified `data_type` is
        /// _not_ checked for validity.
        fn new(name: &str, data_type: &str) -> Self {
            SqlServerColumnRaw {
                name: name.into(),
                data_type: data_type.into(),
                is_nullable: false,
                primary_key_constraint: None,
                max_length: 0,
                precision: 0,
                scale: 0,
            }
        }

        fn nullable(mut self, nullable: bool) -> Self {
            self.is_nullable = nullable;
            self
        }

        fn max_length(mut self, max_length: i16) -> Self {
            self.max_length = max_length;
            self
        }

        fn precision(mut self, precision: u8) -> Self {
            self.precision = precision;
            self
        }

        fn scale(mut self, scale: u8) -> Self {
            self.scale = scale;
            self
        }
    }

    #[mz_ore::test]
    fn smoketest_column_raw() {
        let raw = SqlServerColumnRaw::new("foo", "bit");
        let col = SqlServerColumnDesc::new(&raw);

        assert_eq!(&*col.name, "foo");
        assert_eq!(col.column_type, Some(SqlScalarType::Bool.nullable(false)));
        assert_eq!(col.decode_type, SqlServerColumnDecodeType::Bool);

        let raw = SqlServerColumnRaw::new("foo", "decimal")
            .precision(20)
            .scale(10);
        let col = SqlServerColumnDesc::new(&raw);

        let col_type = SqlScalarType::Numeric {
            max_scale: Some(NumericMaxScale::try_from(10i64).expect("known valid")),
        }
        .nullable(false);
        assert_eq!(col.column_type, Some(col_type));
        assert_eq!(col.decode_type, SqlServerColumnDecodeType::Numeric);
    }

    #[mz_ore::test]
    fn smoketest_column_raw_invalid() {
        let raw = SqlServerColumnRaw::new("foo", "bad_data_type");
        let desc = SqlServerColumnDesc::new(&raw);
        let SqlServerColumnDecodeType::Unsupported { context } = desc.decode_type else {
            panic!("unexpected decode type {desc:?}");
        };
        assert_contains!(context, "'bad_data_type' is unimplemented");

        let raw = SqlServerColumnRaw::new("foo", "decimal")
            .precision(100)
            .scale(10);
        let desc = SqlServerColumnDesc::new(&raw);
        assert!(!desc.is_supported());

        let raw = SqlServerColumnRaw::new("foo", "varchar").max_length(-1);
        let desc = SqlServerColumnDesc::new(&raw);
        let SqlServerColumnDecodeType::Unsupported { context } = desc.decode_type else {
            panic!("unexpected decode type {desc:?}");
        };
        assert_contains!(context, "columns with unlimited size do not support CDC");
    }

    #[mz_ore::test]
    fn smoketest_decoder() {
        let sql_server_columns = [
            SqlServerColumnRaw::new("a", "varchar").max_length(16),
            SqlServerColumnRaw::new("b", "int").nullable(true),
            SqlServerColumnRaw::new("c", "bit"),
        ];
        let sql_server_desc = SqlServerTableRaw {
            schema_name: "my_schema".into(),
            name: "my_table".into(),
            capture_instance: Arc::new(SqlServerCaptureInstanceRaw {
                name: "my_table_CT".into(),
                create_date: NaiveDateTime::parse_from_str(
                    "2024-01-01 00:00:00",
                    "%Y-%m-%d %H:%M:%S",
                )
                .unwrap()
                .into(),
            }),
            columns: sql_server_columns.into(),
        };
        let sql_server_desc = SqlServerTableDesc::new(sql_server_desc);

        let max_length = Some(VarCharMaxLength::try_from(16).unwrap());
        let relation_desc = RelationDesc::builder()
            .with_column("a", SqlScalarType::VarChar { max_length }.nullable(false))
            // Note: In the upstream table 'c' is ordered after 'b'.
            .with_column("c", SqlScalarType::Bool.nullable(false))
            .with_column("b", SqlScalarType::Int32.nullable(true))
            .finish();

        // This decoder should shape the SQL Server Rows into Rows compatible with the RelationDesc.
        let decoder = sql_server_desc
            .decoder(&relation_desc)
            .expect("known valid");

        let sql_server_columns = [
            tiberius::Column::new("a".to_string(), tiberius::ColumnType::BigVarChar),
            tiberius::Column::new("b".to_string(), tiberius::ColumnType::Int4),
            tiberius::Column::new("c".to_string(), tiberius::ColumnType::Bit),
        ];

        let data_a = [
            tiberius::ColumnData::String(Some("hello world".into())),
            tiberius::ColumnData::I32(Some(42)),
            tiberius::ColumnData::Bit(Some(true)),
        ];
        let sql_server_row_a = tiberius::Row::build(
            sql_server_columns
                .iter()
                .cloned()
                .zip_eq(data_a.into_iter()),
        );

        let data_b = [
            tiberius::ColumnData::String(Some("foo bar".into())),
            tiberius::ColumnData::I32(None),
            tiberius::ColumnData::Bit(Some(false)),
        ];
        let sql_server_row_b =
            tiberius::Row::build(sql_server_columns.into_iter().zip_eq(data_b.into_iter()));

        let mut rnd_row = Row::default();
        let arena = RowArena::default();

        decoder
            .decode(&sql_server_row_a, &mut rnd_row, &arena)
            .unwrap();
        assert_eq!(
            &rnd_row,
            &Row::pack_slice(&[Datum::String("hello world"), Datum::True, Datum::Int32(42)])
        );

        decoder
            .decode(&sql_server_row_b, &mut rnd_row, &arena)
            .unwrap();
        assert_eq!(
            &rnd_row,
            &Row::pack_slice(&[Datum::String("foo bar"), Datum::False, Datum::Null])
        );
    }

    #[mz_ore::test]
    fn smoketest_decode_to_string() {
        #[track_caller]
        fn testcase(
            data_type: &'static str,
            col_type: tiberius::ColumnType,
            col_data: tiberius::ColumnData<'static>,
        ) {
            let columns = [SqlServerColumnRaw::new("a", data_type)];
            let sql_server_desc = SqlServerTableRaw {
                schema_name: "my_schema".into(),
                name: "my_table".into(),
                capture_instance: Arc::new(SqlServerCaptureInstanceRaw {
                    name: "my_table_CT".into(),
                    create_date: NaiveDateTime::parse_from_str(
                        "2024-01-01 00:00:00",
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap()
                    .into(),
                }),
                columns: columns.into(),
            };
            let mut sql_server_desc = SqlServerTableDesc::new(sql_server_desc);
            sql_server_desc.apply_text_columns(&BTreeSet::from(["a"]));

            // We should support decoding every datatype to a string.
            let relation_desc = RelationDesc::builder()
                .with_column("a", SqlScalarType::String.nullable(false))
                .finish();

            // This decoder should shape the SQL Server Rows into Rows compatible with the RelationDesc.
            let decoder = sql_server_desc
                .decoder(&relation_desc)
                .expect("known valid");

            let sql_server_row = tiberius::Row::build([(
                tiberius::Column::new("a".to_string(), col_type),
                col_data,
            )]);
            let mut mz_row = Row::default();
            let arena = RowArena::new();
            decoder
                .decode(&sql_server_row, &mut mz_row, &arena)
                .unwrap();

            let str_datum = mz_row.into_element();
            assert!(matches!(str_datum, Datum::String(_)));
        }

        use tiberius::ColumnData;

        testcase(
            "bit",
            tiberius::ColumnType::Bit,
            ColumnData::Bit(Some(true)),
        );
        testcase(
            "bit",
            tiberius::ColumnType::Bit,
            ColumnData::Bit(Some(false)),
        );
        testcase(
            "tinyint",
            tiberius::ColumnType::Int1,
            ColumnData::U8(Some(33)),
        );
        testcase(
            "smallint",
            tiberius::ColumnType::Int2,
            ColumnData::I16(Some(101)),
        );
        testcase(
            "int",
            tiberius::ColumnType::Int4,
            ColumnData::I32(Some(-42)),
        );
        {
            let datetime = tiberius::time::DateTime::new(10, 300);
            testcase(
                "datetime",
                tiberius::ColumnType::Datetime,
                ColumnData::DateTime(Some(datetime)),
            );
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn smoketest_numeric_conversion() {
        let a = tiberius::numeric::Numeric::new_with_scale(12345, 2);
        let rnd = tiberius_numeric_to_mz_numeric(a);
        let og = mz_repr::adt::numeric::cx_datum().parse("123.45").unwrap();
        assert_eq!(og, rnd);

        let a = tiberius::numeric::Numeric::new_with_scale(-99999, 5);
        let rnd = tiberius_numeric_to_mz_numeric(a);
        let og = mz_repr::adt::numeric::cx_datum().parse("-.99999").unwrap();
        assert_eq!(og, rnd);

        let a = tiberius::numeric::Numeric::new_with_scale(1, 29);
        let rnd = tiberius_numeric_to_mz_numeric(a);
        let og = mz_repr::adt::numeric::cx_datum()
            .parse("0.00000000000000000000000000001")
            .unwrap();
        assert_eq!(og, rnd);

        let a = tiberius::numeric::Numeric::new_with_scale(-111111111111111111, 0);
        let rnd = tiberius_numeric_to_mz_numeric(a);
        let og = mz_repr::adt::numeric::cx_datum()
            .parse("-111111111111111111")
            .unwrap();
        assert_eq!(og, rnd);
    }

    // TODO(sql_server2): Proptest the decoder.
}
