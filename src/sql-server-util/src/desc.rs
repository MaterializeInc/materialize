// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metadata about tables, columns, and other objects from SQL Server.

use mz_ore::cast::CastFrom;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::{ColumnType, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use std::sync::Arc;

use crate::SqlServerError;

/// Materialize compatible description of a table in Microsoft SQL Server.
///
/// See [`SqlServerTableRaw`] for the raw information we read from the upstream
/// system.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct SqlServerColumnDesc {
    /// Name of the column.
    pub name: Arc<str>,
    /// The intended data type of the this column in Materialize.
    ///
    /// If this is `None` then then the column is intended to be skipped.
    pub column_type: Option<ColumnType>,
}

impl SqlServerColumnDesc {
    pub fn try_new(raw: &SqlServerColumnRaw) -> Result<Self, SqlServerError> {
        let column_type = parse_data_type(&raw)?.nullable(raw.is_nullable);
        Ok(SqlServerColumnDesc {
            name: Arc::clone(&raw.name),
            column_type: Some(column_type),
        })
    }
}

/// Parse a raw data type from SQL Server into a Materialize [`ScalarType`].
fn parse_data_type(raw: &SqlServerColumnRaw) -> Result<ScalarType, SqlServerError> {
    let scalar = match raw.data_type.to_lowercase().as_str() {
        "tinyint" | "smallint" => ScalarType::Int16,
        "int" => ScalarType::Int32,
        "bigint" => ScalarType::Int64,
        "bit" => ScalarType::Bool,
        "decimal" | "numeric" => {
            let raw_scale = usize::cast_from(raw.scale);
            let max_scale = NumericMaxScale::try_from(raw_scale).map_err(|_| {
                SqlServerError::UnsupportedDataType {
                    column_type: raw.data_type.to_string(),
                    column_name: raw.name.to_string(),
                }
            })?;
            ScalarType::Numeric {
                max_scale: Some(max_scale),
            }
        }
        "real" => ScalarType::Float32,
        "double" => ScalarType::Float64,
        "char" | "nchar" | "varchar" | "nvarchar" | "text" | "sysname" => ScalarType::String,
        "xml" => ScalarType::String,
        "binary" | "varbinary" => ScalarType::Bytes,
        "json" => ScalarType::Jsonb,
        "date" => ScalarType::Date,
        "time" => ScalarType::Time,
        // TODO(sql_server1): We should probably specify a precision here.
        "smalldatetime" | "datetime" | "datetime2" => ScalarType::Timestamp { precision: None },
        "datetimeoffset" => ScalarType::Interval,
        "uniqueidentifier" => ScalarType::Uuid,
        // TODO(sql_server1): Support more data types.
        other => {
            return Err(SqlServerError::UnsupportedDataType {
                column_type: other.to_string(),
                column_name: raw.name.to_string(),
            })
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
    pub max_length: i16,
    /// Precision of the column, if numeric-based; otherwise 0.
    pub precision: u8,
    /// Scale of the columns, if numeric-based; otherwise 0.
    pub scale: u8,
}
