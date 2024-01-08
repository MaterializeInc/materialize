// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indexmap::IndexMap;
use itertools::Itertools;
use once_cell::sync::Lazy;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};

use mz_repr::{ColumnType, ScalarType};

use crate::desc::{MySqlColumnDesc, MySqlColumnKey, MySqlTableDesc};
use crate::MySqlError;

/// Helper for querying information_schema.columns by storing the column
/// index for easier access.
static INFO_SCHEMA_COLS: Lazy<IndexMap<String, usize>> = Lazy::new(|| {
    [
        "column_name",
        "data_type",
        "column_type",
        "column_key",
        "is_nullable",
        "ordinal_position",
        "numeric_scale",
        "datetime_precision",
        "character_maximum_length",
    ]
    .iter()
    .enumerate()
    .map(|(i, col)| (col.to_string(), i))
    .collect()
});

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
        let column_q = format!(
            "SELECT {}
             FROM information_schema.columns
             WHERE table_name = ? AND table_schema = ?
             ORDER BY ordinal_position ASC",
            INFO_SCHEMA_COLS.keys().join(", ")
        );
        let column_rows = conn
            .exec::<Row, _, _>(column_q, (&table_name, &schema_name))
            .await?;

        let mut columns = Vec::with_capacity(column_rows.len());
        for mut row in column_rows {
            let name = row.take(INFO_SCHEMA_COLS["column_name"]).unwrap();
            let unsigned = row
                .take::<String, _>(INFO_SCHEMA_COLS["column_type"])
                .unwrap()
                .contains("unsigned");
            let numeric_scale: Option<i64> = row
                .take::<Option<i64>, _>(INFO_SCHEMA_COLS["numeric_scale"])
                .unwrap();
            let datetime_precision = row
                .take::<Option<i64>, _>(INFO_SCHEMA_COLS["datetime_precision"])
                .unwrap();
            let character_maximum_length = row
                .take::<Option<i64>, _>(INFO_SCHEMA_COLS["character_maximum_length"])
                .unwrap();

            let scalar_type = match row
                .take::<String, _>(INFO_SCHEMA_COLS["data_type"])
                .expect("data_type should be not-null")
                .as_str()
            {
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
                "timestamp" => ScalarType::Timestamp {
                    precision: datetime_precision.and_then(|f| f.try_into().ok()),
                },
                "date" => ScalarType::Date,
                "datetime" => ScalarType::Timestamp {
                    precision: datetime_precision.and_then(|f| f.try_into().ok()),
                },
                "time" => ScalarType::Time,
                "year" => ScalarType::Int32,
                "decimal" => ScalarType::Numeric {
                    max_scale: numeric_scale.and_then(|f| f.try_into().ok()),
                },
                "numeric" => ScalarType::Numeric {
                    max_scale: numeric_scale.and_then(|f| f.try_into().ok()),
                },
                "char" => ScalarType::Char {
                    length: character_maximum_length.and_then(|f| f.try_into().ok()),
                },
                "varchar" => ScalarType::VarChar {
                    max_length: character_maximum_length.and_then(|f| f.try_into().ok()),
                },
                "text" | "mediumtext" | "longtext" => ScalarType::String,
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                    ScalarType::Bytes
                }
                // TODO: Implement other types
                ref data_type => {
                    return Err(MySqlError::UnsupportedDataType {
                        data_type: data_type.to_string(),
                        qualified_table_name: format!("{}.{}", schema_name, table_name),
                        column_name: name,
                    })
                }
            };
            columns.push((
                row.take::<u16, _>(INFO_SCHEMA_COLS["ordinal_position"])
                    .unwrap(),
                MySqlColumnDesc {
                    name,
                    column_type: ColumnType {
                        scalar_type,
                        nullable: &row
                            .take::<String, _>(INFO_SCHEMA_COLS["is_nullable"])
                            .unwrap()
                            == "YES",
                    },
                    column_key: row
                        .take::<String, _>(INFO_SCHEMA_COLS["column_key"])
                        .and_then(|s| match s.as_str() {
                            "PRI" => Some(MySqlColumnKey::PRI),
                            "UNI" => Some(MySqlColumnKey::UNI),
                            "MUL" => Some(MySqlColumnKey::MUL),
                            _ => None,
                        }),
                },
            ));
        }

        tables.push(MySqlTableDesc {
            schema_name,
            name: table_name,
            columns: columns.into_iter().map(|(_, c)| c).collect(),
        });
    }
    Ok(tables)
}
