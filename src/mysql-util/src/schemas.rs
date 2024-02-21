// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;

use mysql_async::prelude::{FromRow, Queryable};
use mysql_async::{FromRowError, Row};

use mz_repr::adt::char::CharLength;
use mz_repr::adt::numeric::{NumericMaxScale, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{ColumnType, ScalarType};

use crate::desc::{MySqlColumnDesc, MySqlKeyDesc, MySqlTableDesc};
use crate::MySqlError;

/// Helper for querying information_schema.columns
// NOTE: The order of these names *must* match the order of fields of the [`InfoSchema`] struct.
const INFO_SCHEMA_COLS: &[&str] = &[
    "column_name",
    "data_type",
    "column_type",
    "is_nullable",
    "numeric_precision",
    "numeric_scale",
    "datetime_precision",
    "character_maximum_length",
];

// NOTE: The order of these fields *must* match the order of names of the [`INFO_SCHEMA_COLS`] list.
struct InfoSchema {
    column_name: String,
    data_type: String,
    column_type: String,
    is_nullable: String,
    numeric_precision: Option<i64>,
    numeric_scale: Option<i64>,
    datetime_precision: Option<i64>,
    character_maximum_length: Option<i64>,
}

impl FromRow for InfoSchema {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError> {
        let actual = row.columns_ref().iter().map(|c| c.name_ref());
        let expected = INFO_SCHEMA_COLS.iter().map(|c| c.as_bytes());
        itertools::assert_equal(actual, expected);
        let (a, b, c, d, e, f, g, h) = FromRow::from_row_opt(row)?;
        Ok(Self {
            column_name: a,
            data_type: b,
            column_type: c,
            is_nullable: d,
            numeric_precision: e,
            numeric_scale: f,
            datetime_precision: g,
            character_maximum_length: h,
        })
    }
}

/// Request for table schemas from MySQL
pub enum SchemaRequest<'a> {
    /// Request schemas for all tables in the database
    All,
    /// Request schemas for all tables in the specified schemas/databases
    Schemas(Vec<&'a str>),
    /// Request schemas for all specified tables, specified as (schema_name, table_name)
    Tables(Vec<(&'a str, &'a str)>),
}

/// Retrieve the tables and column descriptions for tables in the given schemas.
pub async fn schema_info<'a, Q>(
    conn: &mut Q,
    schema_request: &SchemaRequest<'a>,
) -> Result<Vec<MySqlTableDesc>, MySqlError>
where
    Q: Queryable,
{
    let table_rows: Vec<(String, String)> = match schema_request {
        SchemaRequest::All => {
            // Get all tables in non-system schemas.
            // TODO(roshan): Many users create user-defined tables in the `mysql` system schema, since mysql doesn't
            // prevent this. We may want to consider adding a warning for this in the docs, since that schema
            // contains dozens of built-in system tables that we need to filter out.
            let table_q = "SELECT table_name, table_schema
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')";
            conn.exec(table_q, ()).await?
        }
        SchemaRequest::Schemas(schemas) => {
            // Get all tables of type 'Base Table' in specified schemas
            if schemas.is_empty() {
                return Ok(vec![]);
            }
            let table_q = format!(
                "SELECT table_name, table_schema
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema IN ({})",
                schemas.iter().map(|_| "?").join(", ")
            );
            conn.exec(table_q, schemas).await?
        }
        SchemaRequest::Tables(tables) => {
            // Get all specified tables
            if tables.is_empty() {
                return Ok(vec![]);
            }
            let table_q = format!(
                "SELECT table_name, table_schema
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND (table_schema, table_name) IN ({})",
                tables.iter().map(|_| "(?, ?)").join(", ")
            );
            conn.exec(
                table_q,
                tables
                    .iter()
                    .flat_map(|(s, t)| [*s, *t])
                    .collect::<Vec<_>>(),
            )
            .await?
        }
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
            INFO_SCHEMA_COLS
                .iter()
                .map(|c| format!("{c} AS {c}"))
                .join(", ")
        );
        let column_rows = conn
            .exec::<InfoSchema, _, _>(column_q, (&table_name, &schema_name))
            .await?;

        let mut columns = Vec::with_capacity(column_rows.len());
        for info in column_rows {
            let unsigned = info.column_type.contains("unsigned");

            let scalar_type = match info.data_type.as_str() {
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
                    precision: info
                        .datetime_precision
                        .map(TimestampPrecision::try_from)
                        .transpose()
                        .map_err(|_| MySqlError::UnsupportedDataType {
                            column_type: info.column_type,
                            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                            column_name: info.column_name.clone(),
                        })?,
                },
                "time" => ScalarType::Time,
                "decimal" | "numeric" => {
                    // validate the precision is within the bounds of our numeric type
                    // here since we don't use this precision on the ScalarType itself
                    // whereas the scale will be bounds-checked in the TryFrom impl
                    if info.numeric_precision.unwrap_or_default()
                        > NUMERIC_DATUM_MAX_PRECISION.into()
                    {
                        Err(MySqlError::UnsupportedDataType {
                            column_type: info.column_type.clone(),
                            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                            column_name: info.column_name.clone(),
                        })?
                    }
                    ScalarType::Numeric {
                        max_scale: info
                            .numeric_scale
                            .map(NumericMaxScale::try_from)
                            .transpose()
                            .map_err(|_| MySqlError::UnsupportedDataType {
                                column_type: info.column_type,
                                qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                                column_name: info.column_name.clone(),
                            })?,
                    }
                }
                "char" => ScalarType::Char {
                    length: info
                        .character_maximum_length
                        .and_then(|f| Some(CharLength::try_from(f)))
                        .transpose()
                        .map_err(|_| MySqlError::UnsupportedDataType {
                            column_type: info.column_type,
                            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                            column_name: info.column_name.clone(),
                        })?,
                },
                "varchar" => ScalarType::VarChar {
                    max_length: info
                        .character_maximum_length
                        .and_then(|f| Some(VarCharMaxLength::try_from(f)))
                        .transpose()
                        .map_err(|_| MySqlError::UnsupportedDataType {
                            column_type: info.column_type,
                            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                            column_name: info.column_name.clone(),
                        })?,
                },
                "text" | "mediumtext" | "longtext" => ScalarType::String,
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                    ScalarType::Bytes
                }
                // TODO: Implement other types
                _ => Err(MySqlError::UnsupportedDataType {
                    column_type: info.column_type,
                    qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                    column_name: info.column_name.clone(),
                })?,
            };
            columns.push(MySqlColumnDesc {
                name: info.column_name,
                column_type: ColumnType {
                    scalar_type,
                    nullable: &info.is_nullable == "YES",
                },
            })
        }

        // Query for primary key and unique constraints that do not contain expressions / functional key parts.
        // When a constraint contains expressions, the column_name field is NULL.
        let index_rows = conn
            .exec::<(String, String), _, _>(
                "SELECT
                    index_name,
                    column_name
                FROM information_schema.statistics AS outt
                WHERE
                    table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
                    AND NOT EXISTS (
                        SELECT 1
                        FROM information_schema.statistics AS inn
                        WHERE outt.index_name = inn.index_name AND inn.column_name IS NULL
                    )
                    AND non_unique = 0
                    AND table_name = ?
                    AND table_schema = ?
                ORDER BY index_name, seq_in_index
            ",
                (&table_name, &schema_name),
            )
            .await?;

        let mut indices = BTreeMap::new();
        for (index_name, column) in index_rows {
            indices
                .entry(index_name)
                .or_insert_with(Vec::new)
                .push(column);
        }
        let mut keys = BTreeSet::new();
        while let Some((index_name, columns)) = indices.pop_first() {
            keys.insert(MySqlKeyDesc {
                is_primary: &index_name == "PRIMARY",
                name: index_name,
                columns,
            });
        }

        tables.push(MySqlTableDesc {
            schema_name,
            name: table_name,
            columns,
            keys,
        });
    }
    Ok(tables)
}
