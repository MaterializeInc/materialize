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

use anyhow::Context;
use dec::OrderedDecimal;
use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType, RustType};
use mz_repr::adt::numeric::{Dec, Numeric, NumericMaxScale};
use mz_repr::{ColumnType, Datum, RelationDesc, Row, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use std::sync::Arc;

use crate::SqlServerError;

include!(concat!(env!("OUT_DIR"), "/mz_sql_server_util.rs"));

/// Materialize compatible description of a table in Microsoft SQL Server.
///
/// See [`SqlServerTableRaw`] for the raw information we read from the upstream
/// system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct SqlServerTableDesc {
    /// Name of the schema that the table belongs to.
    pub schema_name: Arc<str>,
    /// Name of the table.
    pub name: Arc<str>,
    /// Columns for the table.
    pub columns: Arc<[SqlServerColumnDesc]>,
    /// Is CDC enabled for this table, required to replicate into Materialize.
    pub is_cdc_enabled: bool,
}

impl SqlServerTableDesc {
    /// Try creating a [`SqlServerTableDesc`] from a [`SqlServerTableRaw`] description.
    ///
    /// Returns an error if the raw table description is not compatible with Materialize.
    pub fn try_new(raw: SqlServerTableRaw) -> Result<Self, SqlServerError> {
        let columns: Arc<[_]> = raw
            .columns
            .into_iter()
            .map(SqlServerColumnDesc::try_new)
            .collect::<Result<_, _>>()?;
        Ok(SqlServerTableDesc {
            schema_name: raw.schema_name,
            name: raw.name,
            columns,
            is_cdc_enabled: raw.is_cdc_enabled,
        })
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
            is_cdc_enabled: self.is_cdc_enabled,
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
            is_cdc_enabled: proto.is_cdc_enabled,
        })
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
    /// Columns for the table.
    pub columns: Arc<[SqlServerColumnRaw]>,
    /// Whether or not CDC is enabled for this table.
    pub is_cdc_enabled: bool,
}

/// Description of a column from a table in Microsoft SQL Server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
pub struct SqlServerColumnDesc {
    /// Name of the column.
    pub name: Arc<str>,
    /// The intended data type of the this column in Materialize.
    pub column_type: ColumnType,
    /// Rust type we should parse the data from a [`tiberius::Row`] as.
    pub decode_type: SqlServerColumnDecodeType,
}

impl SqlServerColumnDesc {
    /// Try creating a [`SqlServerColumnDesc`] from a [`SqlServerColumnRaw`] description.
    ///
    /// Returns an error if the upstream column is not compatible with Materialize, e.g. the
    /// data type doesn't support CDC.
    pub fn try_new(raw: &SqlServerColumnRaw) -> Result<Self, SqlServerError> {
        let (scalar_type, decode_type) = parse_data_type(raw)?;
        Ok(SqlServerColumnDesc {
            name: Arc::clone(&raw.name),
            column_type: scalar_type.nullable(raw.is_nullable),
            decode_type,
        })
    }
}

impl RustType<ProtoSqlServerColumnDesc> for SqlServerColumnDesc {
    fn into_proto(&self) -> ProtoSqlServerColumnDesc {
        ProtoSqlServerColumnDesc {
            name: self.name.to_string(),
            column_type: Some(self.column_type.into_proto()),
            decode_type: Some(self.decode_type.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSqlServerColumnDesc) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(SqlServerColumnDesc {
            name: proto.name.into(),
            column_type: proto
                .column_type
                .into_rust_if_some("ProtoSqlServerColumnDesc::column_type")?,
            decode_type: proto
                .decode_type
                .into_rust_if_some("ProtoSqlServerColumnDesc::decode_type")?,
        })
    }
}

/// Parse a raw data type from SQL Server into a Materialize [`ScalarType`].
///
/// Returns the [`ScalarType`] that we'll map this column to and the [`SqlServerColumnDecodeType`]
/// that we use to decode the raw value.
fn parse_data_type(
    raw: &SqlServerColumnRaw,
) -> Result<(ScalarType, SqlServerColumnDecodeType), SqlServerError> {
    let scalar = match raw.data_type.to_lowercase().as_str() {
        "tinyint" => (ScalarType::Int16, SqlServerColumnDecodeType::U8),
        "smallint" => (ScalarType::Int16, SqlServerColumnDecodeType::I16),
        "int" => (ScalarType::Int32, SqlServerColumnDecodeType::I32),
        "bigint" => (ScalarType::Int64, SqlServerColumnDecodeType::I64),
        "bit" => (ScalarType::Bool, SqlServerColumnDecodeType::Bool),
        "decimal" | "numeric" => {
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
                return Err(SqlServerError::UnsupportedDataType {
                    column_name: raw.name.to_string(),
                    column_type: raw.data_type.to_string(),
                    reason,
                });
            }

            let raw_scale = usize::cast_from(raw.scale);
            let max_scale = NumericMaxScale::try_from(raw_scale).map_err(|_| {
                SqlServerError::UnsupportedDataType {
                    column_type: raw.data_type.to_string(),
                    column_name: raw.name.to_string(),
                    reason: format!("scale of {} is too large", raw.scale),
                }
            })?;
            let column_type = ScalarType::Numeric {
                max_scale: Some(max_scale),
            };

            (column_type, SqlServerColumnDecodeType::Numeric)
        }
        "real" => (ScalarType::Float32, SqlServerColumnDecodeType::F32),
        "double" => (ScalarType::Float64, SqlServerColumnDecodeType::F64),
        "char" | "nchar" | "varchar" | "nvarchar" | "sysname" => {
            // When the `max_length` is -1 SQL Server will not present us with the "before" value
            // for updated columns.
            //
            // TODO(sql_server3): Support UPSERT semantics for SQL Server.
            if raw.max_length == -1 {
                return Err(SqlServerError::UnsupportedDataType {
                    column_name: raw.name.to_string(),
                    column_type: raw.data_type.to_string(),
                    reason: "columns with unlimited size do not support CDC".to_string(),
                });
            }

            (ScalarType::String, SqlServerColumnDecodeType::String)
        }
        "text" | "ntext" | "image" => {
            // SQL Server docs indicate this should always be 16. There's no
            // issue if it's not, but it's good to track.
            mz_ore::soft_assert_eq_no_log!(raw.max_length, 16);

            // TODO(sql_server3): Support UPSERT semantics for SQL Server.
            return Err(SqlServerError::UnsupportedDataType {
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
                return Err(SqlServerError::UnsupportedDataType {
                    column_name: raw.name.to_string(),
                    column_type: raw.data_type.to_string(),
                    reason: "columns with unlimited size do not support CDC".to_string(),
                });
            }
            (ScalarType::String, SqlServerColumnDecodeType::Xml)
        }
        "binary" | "varbinary" => {
            // When the `max_length` is -1 if this column changes as part of an `UPDATE`
            // or `DELETE` statement, SQL Server will not provide the "old" value for
            // this column, but we need this value so we can emit a retraction.
            //
            // TODO(sql_server3): Support UPSERT semantics for SQL Server.
            if raw.max_length == -1 {
                return Err(SqlServerError::UnsupportedDataType {
                    column_name: raw.name.to_string(),
                    column_type: raw.data_type.to_string(),
                    reason: "columns with unlimited size do not support CDC".to_string(),
                });
            }

            (ScalarType::Bytes, SqlServerColumnDecodeType::Bytes)
        }
        "json" => (ScalarType::Jsonb, SqlServerColumnDecodeType::String),
        "date" => (ScalarType::Date, SqlServerColumnDecodeType::NaiveDate),
        "time" => (ScalarType::Time, SqlServerColumnDecodeType::NaiveTime),
        // TODO(sql_server1): We should probably specify a precision here.
        "smalldatetime" | "datetime" | "datetime2" => (
            ScalarType::Timestamp { precision: None },
            SqlServerColumnDecodeType::NaiveDateTime,
        ),
        // TODO(sql_server1): We should probably specify a precision here.
        "datetimeoffset" => (
            ScalarType::TimestampTz { precision: None },
            SqlServerColumnDecodeType::DateTime,
        ),
        "uniqueidentifier" => (ScalarType::Uuid, SqlServerColumnDecodeType::Uuid),
        // TODO(sql_server1): Support more data types.
        other => {
            return Err(SqlServerError::UnsupportedDataType {
                column_type: other.to_string(),
                column_name: raw.name.to_string(),
                reason: "unimplemented".to_string(),
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Arbitrary)]
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
}

impl SqlServerColumnDecodeType {
    /// Decode the column with `name` out of the provided `data`.
    pub fn decode<'a>(
        self,
        data: &'a tiberius::Row,
        name: &'a str,
        column: &'a ColumnType,
    ) -> Result<Datum<'a>, SqlServerError> {
        let maybe_datum = match (&column.scalar_type, self) {
            (ScalarType::Bool, SqlServerColumnDecodeType::Bool) => data
                .try_get(name)
                .context("bool")?
                .map(|val: bool| if val { Datum::True } else { Datum::False }),
            (ScalarType::Int16, SqlServerColumnDecodeType::U8) => data
                .try_get(name)
                .context("u8")?
                .map(|val: u8| Datum::Int16(i16::cast_from(val))),
            (ScalarType::Int16, SqlServerColumnDecodeType::I16) => {
                data.try_get(name).context("i16")?.map(Datum::Int16)
            }
            (ScalarType::Int32, SqlServerColumnDecodeType::I32) => {
                data.try_get(name).context("i32")?.map(Datum::Int32)
            }
            (ScalarType::Int64, SqlServerColumnDecodeType::I64) => {
                data.try_get(name).context("i64")?.map(Datum::Int64)
            }
            (ScalarType::Float32, SqlServerColumnDecodeType::F32) => data
                .try_get(name)
                .context("f32")?
                .map(|val: f32| Datum::Float32(ordered_float::OrderedFloat(val))),
            (ScalarType::Float64, SqlServerColumnDecodeType::F64) => data
                .try_get(name)
                .context("f64")?
                .map(|val: f64| Datum::Float64(ordered_float::OrderedFloat(val))),
            (ScalarType::String, SqlServerColumnDecodeType::String) => {
                data.try_get(name).context("string")?.map(Datum::String)
            }
            (ScalarType::Bytes, SqlServerColumnDecodeType::Bytes) => {
                data.try_get(name).context("bytes")?.map(Datum::Bytes)
            }
            (ScalarType::Uuid, SqlServerColumnDecodeType::Uuid) => {
                data.try_get(name).context("uuid")?.map(Datum::Uuid)
            }
            (ScalarType::Numeric { .. }, SqlServerColumnDecodeType::Numeric) => data
                .try_get(name)
                .context("numeric")?
                .map(|val: tiberius::numeric::Numeric| {
                    // TODO(sql_server3): Make decimal parsing more performant.
                    let numeric = Numeric::context()
                        .parse(val.to_string())
                        .context("parsing")?;
                    Ok::<_, SqlServerError>(Datum::Numeric(OrderedDecimal(numeric)))
                })
                .transpose()?,
            (ScalarType::String, SqlServerColumnDecodeType::Xml) => data
                .try_get(name)
                .context("xml")?
                .map(|val: &tiberius::xml::XmlData| Datum::String(val.as_ref())),
            (ScalarType::Date, SqlServerColumnDecodeType::NaiveDate) => data
                .try_get(name)
                .context("date")?
                .map(|val: chrono::NaiveDate| {
                    let date = val.try_into().context("parse date")?;
                    Ok::<_, SqlServerError>(Datum::Date(date))
                })
                .transpose()?,
            // TODO(sql_server1): SQL Server's time related types support a resolution
            // of 100 nanoseconds, while Postgres supports 1,000 nanoseconds (aka 1 microsecond).
            //
            // Internally we can support 1 nanosecond precision, but we should exercise
            // this case and see what breaks.
            (ScalarType::Time, SqlServerColumnDecodeType::NaiveTime) => {
                data.try_get(name).context("time")?.map(Datum::Time)
            }
            (ScalarType::Timestamp { .. }, SqlServerColumnDecodeType::NaiveDateTime) => data
                .try_get(name)
                .context("timestamp")?
                .map(|val: chrono::NaiveDateTime| {
                    let ts = val.try_into().context("parse timestamp")?;
                    Ok::<_, SqlServerError>(Datum::Timestamp(ts))
                })
                .transpose()?,
            (ScalarType::TimestampTz { .. }, SqlServerColumnDecodeType::DateTime) => data
                .try_get(name)
                .context("timestamptz")?
                .map(|val: chrono::DateTime<chrono::Utc>| {
                    let ts = val.try_into().context("parse timestamptz")?;
                    Ok::<_, SqlServerError>(Datum::TimestampTz(ts))
                })
                .transpose()?,
            (column_type, decode_type) => {
                let msg = format!("don't know how to parse {decode_type:?} as {column_type:?}");
                return Err(SqlServerError::ProgrammingError(msg));
            }
        };

        match (maybe_datum, column.nullable) {
            (Some(datum), _) => Ok(datum),
            (None, true) => Ok(Datum::Null),
            (None, false) => Err(SqlServerError::InvalidData {
                column_name: name.to_string(),
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
        };
        Ok(val)
    }
}

/// A decoder from [`tiberius::Row`] to [`mz_repr::Row`].
///
/// The goal of this type is to perform any expensive "downcasts" so in the hot
/// path of decoding rows we do the minimal amount of work.
pub struct SqlServerRowDecoder {
    decoders: Vec<(Arc<str>, ColumnType, SqlServerColumnDecodeType)>,
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

                // This shouldn't be true, but be defensive.
                //
                // TODO(sql_server2): Maybe allow the Materialize column type to be more nullable
                // than our decoding type?
                if &sql_server_col.column_type != col_type {
                    return Err(SqlServerError::ProgrammingError(format!(
                        "programming error, {col_name} has mismatched type {:?} vs {:?}",
                        sql_server_col.column_type, col_type
                    )));
                }

                let name = Arc::clone(&sql_server_col.name);
                let decoder = sql_server_col.decode_type;

                Ok::<_, SqlServerError>((name, col_type.clone(), decoder))
            })
            .collect::<Result<_, _>>()?;

        Ok(SqlServerRowDecoder { decoders })
    }

    /// Decode data from the provided [`tiberius::Row`] into the provided [`Row`].
    pub fn decode(&self, data: &tiberius::Row, row: &mut Row) -> Result<(), SqlServerError> {
        let mut packer = row.packer();
        for (col_name, col_type, decoder) in &self.decoders {
            let datum = decoder.decode(data, col_name, col_type)?;
            packer.push(datum);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::desc::{
        SqlServerColumnDecodeType, SqlServerColumnDesc, SqlServerTableDesc, SqlServerTableRaw,
    };

    use super::SqlServerColumnRaw;
    use mz_ore::assert_contains;
    use mz_repr::adt::numeric::NumericMaxScale;
    use mz_repr::{Datum, RelationDesc, Row, ScalarType};
    use tiberius::RowTestExt;

    impl SqlServerColumnRaw {
        /// Create a new [`SqlServerColumnRaw`]. The specified `data_type` is
        /// _not_ checked for validity.
        fn new(name: &str, data_type: &str) -> Self {
            SqlServerColumnRaw {
                name: name.into(),
                data_type: data_type.into(),
                is_nullable: false,
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
        let col = SqlServerColumnDesc::try_new(&raw).unwrap();

        assert_eq!(&*col.name, "foo");
        assert_eq!(col.column_type, ScalarType::Bool.nullable(false));
        assert_eq!(col.decode_type, SqlServerColumnDecodeType::Bool);

        let raw = SqlServerColumnRaw::new("foo", "decimal")
            .precision(20)
            .scale(10);
        let col = SqlServerColumnDesc::try_new(&raw).unwrap();

        let col_type = ScalarType::Numeric {
            max_scale: Some(NumericMaxScale::try_from(10i64).expect("known valid")),
        }
        .nullable(false);
        assert_eq!(col.column_type, col_type);
        assert_eq!(col.decode_type, SqlServerColumnDecodeType::Numeric);
    }

    #[mz_ore::test]
    fn smoketest_column_raw_invalid() {
        let raw = SqlServerColumnRaw::new("foo", "bad_data_type");
        let err = SqlServerColumnDesc::try_new(&raw).unwrap_err();
        assert_contains!(err.to_string(), "'bad_data_type' from column 'foo'");

        let raw = SqlServerColumnRaw::new("foo", "decimal")
            .precision(100)
            .scale(10);
        let err = SqlServerColumnDesc::try_new(&raw).unwrap_err();
        assert_contains!(
            err.to_string(),
            "precision of 100 is greater than our maximum of 39"
        );

        let raw = SqlServerColumnRaw::new("foo", "varchar").max_length(-1);
        let err = SqlServerColumnDesc::try_new(&raw).unwrap_err();
        assert_contains!(
            err.to_string(),
            "columns with unlimited size do not support CDC"
        );
    }

    #[mz_ore::test]
    fn smoketest_decoder() {
        let sql_server_columns = [
            SqlServerColumnRaw::new("a", "varchar"),
            SqlServerColumnRaw::new("b", "int").nullable(true),
            SqlServerColumnRaw::new("c", "bit"),
        ];
        let sql_server_desc = SqlServerTableRaw {
            schema_name: "my_schema".into(),
            name: "my_table".into(),
            columns: sql_server_columns.into(),
            is_cdc_enabled: true,
        };
        let sql_server_desc = SqlServerTableDesc::try_new(sql_server_desc).expect("known valid");

        let relation_desc = RelationDesc::builder()
            .with_column("a", ScalarType::String.nullable(false))
            // Note: In the upstream table 'c' is ordered after 'b'.
            .with_column("c", ScalarType::Bool.nullable(false))
            .with_column("b", ScalarType::Int32.nullable(true))
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
        let sql_server_row_a =
            tiberius::Row::build(sql_server_columns.iter().cloned().zip(data_a.into_iter()));

        let data_b = [
            tiberius::ColumnData::String(Some("foo bar".into())),
            tiberius::ColumnData::I32(None),
            tiberius::ColumnData::Bit(Some(false)),
        ];
        let sql_server_row_b =
            tiberius::Row::build(sql_server_columns.into_iter().zip(data_b.into_iter()));

        let mut rnd_row = Row::default();
        decoder.decode(&sql_server_row_a, &mut rnd_row).unwrap();
        assert_eq!(
            &rnd_row,
            &Row::pack_slice(&[Datum::String("hello world"), Datum::True, Datum::Int32(42)])
        );

        decoder.decode(&sql_server_row_b, &mut rnd_row).unwrap();
        assert_eq!(
            &rnd_row,
            &Row::pack_slice(&[Datum::String("foo bar"), Datum::False, Datum::Null])
        );
    }

    // TODO(sql_server2): Proptest the decoder.
}
