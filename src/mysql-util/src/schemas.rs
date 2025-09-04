// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use itertools::Itertools;
use maplit::btreeset;
use regex::Regex;

use mysql_async::prelude::{FromRow, Queryable};
use mysql_async::{FromRowError, Row};

use mz_repr::adt::char::CharLength;
use mz_repr::adt::numeric::{NUMERIC_DATUM_MAX_PRECISION, NumericMaxScale};
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{ColumnType, ScalarType};

use crate::desc::{
    MySqlColumnDesc, MySqlColumnMeta, MySqlColumnMetaEnum, MySqlKeyDesc, MySqlTableDesc,
};
use crate::{MySqlError, UnsupportedDataType};

/// Built-in system schemas that should be ignored when querying for user-defined tables
/// since they contain dozens of built-in system tables that are likely not needed.
pub static SYSTEM_SCHEMAS: LazyLock<BTreeSet<&str>> = LazyLock::new(|| {
    btreeset! {
        "information_schema",
        "performance_schema",
        "mysql",
        "sys",
    }
});

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
#[derive(Debug, Clone)]
pub struct InfoSchema {
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

impl InfoSchema {
    pub fn name(self) -> String {
        self.column_name
    }
}

/// A representation of the raw schema info for a table from MySQL
#[derive(Debug, Clone)]
pub struct MySqlTableSchema {
    pub schema_name: String,
    pub name: String,
    pub columns: Vec<InfoSchema>,
    pub keys: BTreeSet<MySqlKeyDesc>,
}

impl MySqlTableSchema {
    pub fn table_ref<'a>(&'a self) -> QualifiedTableRef<'a> {
        QualifiedTableRef {
            schema_name: &self.schema_name,
            table_name: &self.name,
        }
    }

    /// Convert the raw table schema to our MySqlTableDesc representation
    /// using any provided text_columns and exclude_columns
    pub fn to_desc(
        self,
        text_columns: Option<&BTreeSet<&str>>,
        exclude_columns: Option<&BTreeSet<&str>>,
    ) -> Result<MySqlTableDesc, MySqlError> {
        // Verify there are no duplicates in text_columns and exclude_columns
        match (&text_columns, &exclude_columns) {
            (Some(text_cols), Some(ignore_cols)) => {
                let intersection: Vec<_> = text_cols.intersection(ignore_cols).collect();
                if !intersection.is_empty() {
                    Err(MySqlError::DuplicatedColumnNames {
                        qualified_table_name: format!("{:?}.{:?}", self.schema_name, self.name),
                        columns: intersection.iter().map(|s| (*s).to_string()).collect(),
                    })?;
                }
            }
            _ => (),
        };

        let mut columns = Vec::with_capacity(self.columns.len());
        let mut error_cols = vec![];
        for info in self.columns {
            // If this column is designated as a text column and of a supported text-column type
            // treat it as a string and skip type parsing.
            if let Some(text_columns) = &text_columns {
                if text_columns.contains(&info.column_name.as_str()) {
                    match parse_as_text_column(&info, &self.schema_name, &self.name) {
                        Err(err) => error_cols.push(err),
                        Ok((scalar_type, meta)) => columns.push(MySqlColumnDesc {
                            name: info.column_name,
                            column_type: Some(ColumnType {
                                scalar_type,
                                nullable: &info.is_nullable == "YES",
                            }),
                            meta,
                        }),
                    }
                    continue;
                }
            }

            // If this column is ignored, use None for the column type to signal that it should be.
            if let Some(ignore_cols) = &exclude_columns {
                if ignore_cols.contains(&info.column_name.as_str()) {
                    columns.push(MySqlColumnDesc {
                        name: info.column_name,
                        column_type: None,
                        meta: None,
                    });
                    continue;
                }
            }

            // Collect the parsed data types or errors for later reporting.
            match parse_data_type(&info, &self.schema_name, &self.name) {
                Err(err) => error_cols.push(err),
                Ok((scalar_type, meta)) => columns.push(MySqlColumnDesc {
                    name: info.column_name,
                    column_type: Some(ColumnType {
                        scalar_type,
                        nullable: &info.is_nullable == "YES",
                    }),
                    meta,
                }),
            }
        }
        if error_cols.len() > 0 {
            Err(MySqlError::UnsupportedDataTypes {
                columns: error_cols,
            })?;
        }

        Ok(MySqlTableDesc {
            schema_name: self.schema_name,
            name: self.name,
            columns,
            keys: self.keys,
        })
    }
}

/// Request for table schemas from MySQL
pub enum SchemaRequest<'a> {
    /// Request schemas for all tables in the database, excluding tables in
    /// the built-in system schemas.
    All,
    /// Request schemas for all tables in the database, including tables from
    /// the built-in system schemas.
    AllWithSystemSchemas,
    /// Request schemas for all tables in the specified schemas/databases
    Schemas(Vec<&'a str>),
    /// Request schemas for all specified tables, specified as (schema_name, table_name)
    Tables(Vec<(&'a str, &'a str)>),
}

/// A reference to a table in a schema/database
#[derive(Debug, Hash, PartialEq, Eq, Clone, Ord, PartialOrd)]
pub struct QualifiedTableRef<'a> {
    pub schema_name: &'a str,
    pub table_name: &'a str,
}

/// Retrieve the tables and column descriptions for tables in the given schemas.
pub async fn schema_info<'a, Q>(
    conn: &mut Q,
    schema_request: &SchemaRequest<'a>,
) -> Result<Vec<MySqlTableSchema>, MySqlError>
where
    Q: Queryable,
{
    let table_rows: Vec<(String, String)> = match schema_request {
        SchemaRequest::All => {
            let table_q = format!(
                "SELECT table_name, table_schema
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ({})",
                SYSTEM_SCHEMAS.iter().map(|s| format!("'{}'", s)).join(", ")
            );
            conn.exec(table_q, ()).await?
        }
        SchemaRequest::AllWithSystemSchemas => {
            let table_q = "SELECT table_name, table_schema
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'";
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

        tables.push(MySqlTableSchema {
            schema_name,
            name: table_name,
            columns: column_rows,
            keys,
        });
    }

    Ok(tables)
}

fn parse_data_type(
    info: &InfoSchema,
    schema_name: &str,
    table_name: &str,
) -> Result<(ScalarType, Option<MySqlColumnMeta>), UnsupportedDataType> {
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
                .map_err(|_| UnsupportedDataType {
                    column_type: info.column_type.clone(),
                    qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                    column_name: info.column_name.clone(),
                    intended_type: None,
                })?,
        },
        "time" => ScalarType::Time,
        "decimal" | "numeric" => {
            // validate the precision is within the bounds of our numeric type
            // here since we don't use this precision on the ScalarType itself
            // whereas the scale will be bounds-checked in the TryFrom impl
            if info.numeric_precision.unwrap_or_default() > NUMERIC_DATUM_MAX_PRECISION.into() {
                Err(UnsupportedDataType {
                    column_type: info.column_type.clone(),
                    qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                    column_name: info.column_name.clone(),
                    intended_type: None,
                })?
            }
            ScalarType::Numeric {
                max_scale: info
                    .numeric_scale
                    .map(NumericMaxScale::try_from)
                    .transpose()
                    .map_err(|_| UnsupportedDataType {
                        column_type: info.column_type.clone(),
                        qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                        column_name: info.column_name.clone(),
                        intended_type: None,
                    })?,
            }
        }
        "char" => ScalarType::Char {
            length: info
                .character_maximum_length
                .and_then(|f| Some(CharLength::try_from(f)))
                .transpose()
                .map_err(|_| UnsupportedDataType {
                    column_type: info.column_type.clone(),
                    qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                    column_name: info.column_name.clone(),
                    intended_type: None,
                })?,
        },
        "varchar" => ScalarType::VarChar {
            max_length: info
                .character_maximum_length
                .and_then(|f| Some(VarCharMaxLength::try_from(f)))
                .transpose()
                .map_err(|_| UnsupportedDataType {
                    column_type: info.column_type.clone(),
                    qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                    column_name: info.column_name.clone(),
                    intended_type: None,
                })?,
        },
        "text" | "tinytext" | "mediumtext" | "longtext" => ScalarType::String,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            ScalarType::Bytes
        }
        "json" => ScalarType::Jsonb,
        // TODO(mysql): Support the `bit` type natively in Materialize.
        "bit" => {
            let precision = match info.numeric_precision {
                Some(x @ 0..=64) => u32::try_from(x).expect("known good value"),
                prec => {
                    mz_ore::soft_panic_or_log!(
                        "found invalid bit precision, {prec:?}, falling back"
                    );
                    64u32
                }
            };
            return Ok((ScalarType::UInt64, Some(MySqlColumnMeta::Bit(precision))));
        }
        typ => {
            tracing::warn!(?typ, "found unsupported data type");
            return Err(UnsupportedDataType {
                column_type: info.column_type.clone(),
                qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                column_name: info.column_name.clone(),
                intended_type: None,
            });
        }
    };

    Ok((scalar_type, None))
}

/// Parse the specified column as a TEXT COLUMN. We only support the set of types that are
/// represented as an encoded-string in both the mysql-common binary query response and binlog
/// event representation, OR types that we've added explicit casting support for.
fn parse_as_text_column(
    info: &InfoSchema,
    schema_name: &str,
    table_name: &str,
) -> Result<(ScalarType, Option<MySqlColumnMeta>), UnsupportedDataType> {
    match info.data_type.as_str() {
        "year" => Ok((ScalarType::String, Some(MySqlColumnMeta::Year))),
        "json" => Ok((ScalarType::String, Some(MySqlColumnMeta::Json))),
        "enum" => Ok((
            ScalarType::String,
            Some(MySqlColumnMeta::Enum(MySqlColumnMetaEnum {
                values: enum_vals_from_column_type(info.column_type.as_str()).map_err(|_| {
                    UnsupportedDataType {
                        column_type: info.column_type.clone(),
                        qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                        column_name: info.column_name.clone(),
                        intended_type: Some("text".to_string()),
                    }
                })?,
            })),
        )),
        "date" => Ok((ScalarType::String, Some(MySqlColumnMeta::Date))),
        "datetime" | "timestamp" => Ok((
            ScalarType::String,
            Some(MySqlColumnMeta::Timestamp(
                info.datetime_precision
                    // Default precision is 0 in MySQL if not specified
                    .unwrap_or_default()
                    .try_into()
                    .map_err(|_| UnsupportedDataType {
                        column_type: info.column_type.clone(),
                        qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
                        column_name: info.column_name.clone(),
                        intended_type: Some("text".to_string()),
                    })?,
            )),
        )),
        _ => Err(UnsupportedDataType {
            column_type: info.column_type.clone(),
            qualified_table_name: format!("{:?}.{:?}", schema_name, table_name),
            column_name: info.column_name.clone(),
            intended_type: Some("text".to_string()),
        }),
    }
}

static ENUM_VAL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"'((?:[^']|'')*)'").expect("valid regex"));

/// Parse the enum values from a column_type value on an enum column, which is a string like
/// "enum('apple','banana','cher,ry','ora''nge')"
/// We need to handle the case where the enum value itself contains a comma or a
/// single quote (escaped with another quote), so we use a regex to do so
fn enum_vals_from_column_type(s: &str) -> Result<Vec<String>, anyhow::Error> {
    let vals_str = s
        .strip_prefix("enum(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| anyhow::format_err!("Unable to parse enum column type string"))?;

    Ok(ENUM_VAL_REGEX
        .captures_iter(vals_str)
        .map(|s| s[1].replace("''", "'"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_enum_value_parsing() {
        let vals =
            enum_vals_from_column_type("enum('apple','banana','cher,ry','ora''nge')").unwrap();
        assert_eq!(vals, vec!["apple", "banana", "cher,ry", "ora'nge"]);
    }
}
