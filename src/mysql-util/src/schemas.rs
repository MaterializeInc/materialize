// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;

use mysql_async::prelude::Queryable;
use mysql_async::Conn;

use mz_repr::adt::char::CharLength;
use mz_repr::adt::numeric::{NumericMaxScale, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{ColumnType, ScalarType};

use crate::desc::{MySqlColumnDesc, MySqlKeyDesc, MySqlTableDesc};
use crate::MySqlError;

/// Helper for querying information_schema.columns
const INFO_SCHEMA_COLS: &[&str] = &[
    "column_name",
    "data_type",
    "column_type",
    "is_nullable",
    "ordinal_position",
    "numeric_precision",
    "numeric_scale",
    "datetime_precision",
    "character_maximum_length",
];

type InfoSchemaColumnsRow = (
    String,      // column_name
    String,      // data_type
    String,      // column_type
    String,      // is_nullable
    u16,         // ordinal_position
    Option<i64>, // numeric_precision
    Option<i64>, // numeric_scale
    Option<i64>, // datetime_precision
    Option<i64>, // character_maximum_length
);

/// Retrieve the tables and column descriptions for tables in the given schemas.
pub async fn schema_info(
    conn: &mut Conn,
    schemas: &Option<Vec<String>>,
) -> Result<Vec<MySqlTableDesc>, MySqlError> {
    let table_rows: Vec<(String, String)> = if let Some(schemas) = schemas {
        // Get all tables of type 'Base Table' in specified schemas
        assert!(!schemas.is_empty());
        let table_q = format!(
            "SELECT table_name, table_schema
                       FROM information_schema.tables
                       WHERE table_type = 'BASE TABLE'
                       AND table_schema IN ({})",
            schemas.iter().map(|_| "?").join(", ")
        );
        conn.exec(table_q, schemas).await?
    } else {
        // Get all tables in non-system schemas.
        // TODO(roshan): Many users create user-defined tables in the `mysql` system schema, since mysql doesn't
        // prevent this. We may want to consider adding a warning for this in the docs, since that schema
        // contains dozens of built-in system tables that we need to filter out.
        let table_q = "SELECT table_name, table_schema
                       FROM information_schema.tables
                       WHERE table_type = 'BASE TABLE'
                       AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')";
        conn.exec(table_q, ()).await?
    };

    let mut tables = vec![];
    for (table_name, schema_name) in table_rows {
        // NOTE: It's important that we order by ordinal_position ASC since we rely on this as
        // the ordering in which columns are returned in a row.
        let column_q = format!(
            "SELECT {}
             FROM information_schema.columns
             WHERE table_name = ? AND table_schema = ?
             ORDER BY ordinal_position ASC",
            INFO_SCHEMA_COLS.join(", ")
        );
        let column_rows = conn
            .exec::<InfoSchemaColumnsRow, _, _>(column_q, (&table_name, &schema_name))
            .await?;

        let mut columns = Vec::with_capacity(column_rows.len());
        for (
            column_name,
            data_type,
            column_type,
            is_nullable,
            _,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            character_maximum_length,
        ) in column_rows
        {
            let unsigned = column_type.contains("unsigned");

            let scalar_type = match data_type.as_str() {
                "tinyint" | "smallint" => {
                    if unsigned {
                        ScalarType::UInt16
                    } else {
                        ScalarType::Int16
                    }
                }
                "mediumint" | "int" => {
                    if unsigned {
                        ScalarType::UInt32
                    } else {
                        ScalarType::Int32
                    }
                }
                "bigint" => {
                    if unsigned {
                        ScalarType::UInt64
                    } else {
                        ScalarType::Int64
                    }
                }
                "float" => ScalarType::Float32,
                "double" => ScalarType::Float64,
                "date" => ScalarType::Date,
                "datetime" | "timestamp" => ScalarType::Timestamp {
                    // both mysql and our scalar type use a max six-digit fractional-second precision
                    // this is bounds-checked in the TryFrom impl
                    precision: datetime_precision
                        .and_then(|precision| Some(TimestampPrecision::try_from(precision)))
                        .transpose()
                        .map_err(|_| MySqlError::UnsupportedDataType {
                            column_type,
                            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                            column_name: column_name.clone(),
                        })?,
                },
                "time" => ScalarType::Time,
                "decimal" | "numeric" => {
                    // validate the precision is within the bounds of our numeric type
                    // here since we don't use this precision on the ScalarType itself
                    // whereas the scale will be bounds-checked in the TryFrom impl
                    if numeric_precision.unwrap_or_default() > NUMERIC_DATUM_MAX_PRECISION.into() {
                        Err(MySqlError::UnsupportedDataType {
                            column_type: column_type.clone(),
                            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                            column_name: column_name.clone(),
                        })?
                    }
                    ScalarType::Numeric {
                        max_scale: numeric_scale
                            .and_then(|f| Some(NumericMaxScale::try_from(f)))
                            .transpose()
                            .map_err(|_| MySqlError::UnsupportedDataType {
                                column_type,
                                qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                                column_name: column_name.clone(),
                            })?,
                    }
                }
                "char" => ScalarType::Char {
                    length: character_maximum_length
                        .and_then(|f| Some(CharLength::try_from(f)))
                        .transpose()
                        .map_err(|_| MySqlError::UnsupportedDataType {
                            column_type,
                            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                            column_name: column_name.clone(),
                        })?,
                },
                "varchar" => ScalarType::VarChar {
                    max_length: character_maximum_length
                        .and_then(|f| Some(VarCharMaxLength::try_from(f)))
                        .transpose()
                        .map_err(|_| MySqlError::UnsupportedDataType {
                            column_type,
                            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                            column_name: column_name.clone(),
                        })?,
                },
                "text" | "mediumtext" | "longtext" => ScalarType::String,
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                    ScalarType::Bytes
                }
                // TODO: Implement other types
                _ => Err(MySqlError::UnsupportedDataType {
                    column_type,
                    qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                    column_name: column_name.clone(),
                })?,
            };
            columns.push(MySqlColumnDesc {
                name: column_name,
                column_type: ColumnType {
                    scalar_type,
                    nullable: &is_nullable == "YES",
                },
            })
        }

        // Query for primary key and unique constraints that do not contain expressions / functional key parts.
        let index_rows = conn.exec::<(String, String), _, _>("
            WITH indexes AS (
                SELECT
                    index_name,
                    group_concat(column_name ORDER BY seq_in_index separator ', ') AS columns,
                    group_concat(expression ORDER BY seq_in_index separator ', ') AS expressions
                FROM information_schema.statistics
                WHERE
                    table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    AND non_unique = 0
                    AND table_name = ?
                    AND table_schema = ?
                GROUP BY index_name
                ORDER BY index_name
            )
            SELECT index_name, columns FROM indexes WHERE expressions IS NULL AND columns IS NOT NULL;
        ", (&table_name, &schema_name))
        .await?;

        let mut keys = Vec::with_capacity(index_rows.len());
        for (index_name, columns) in index_rows {
            let columns = columns
                .split(", ")
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            keys.push(MySqlKeyDesc {
                is_primary: &index_name == "PRIMARY",
                name: index_name,
                columns,
            });
        }

        tables.push(MySqlTableDesc {
            schema_name,
            name: table_name,
            columns,
            keys: keys.into_iter().collect(),
        });
    }
    Ok(tables)
}
