// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Useful queries to inspect the state of a SQL Server instance.

use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::desc::{SqlServerColumnRaw, SqlServerTableRaw};
use crate::{Client, SqlServerError};

/// Ensure change data capture (CDC) is enabled for the database the provided
/// `client` is currently connected to.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver16>
pub async fn ensure_database_cdc_enabled(client: &mut Client) -> Result<(), SqlServerError> {
    static DATABASE_CDC_ENABLED_QUERY: &str =
        "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID();";
    let result = client.simple_query(DATABASE_CDC_ENABLED_QUERY).await?;

    check_system_result(&result, "database CDC".to_string(), true)?;
    Ok(())
}

/// Ensure change data capture (CDC) is enabled for the specified table.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver16#enable-for-a-table>
pub async fn ensure_table_cdc_enabled(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<(), SqlServerError> {
    static TABLE_CDC_ENABLED_QUERY: &str = "
SELECT is_tracked_by_cdc FROM sys.tables tables
JOIN sys.schemas schemas
ON tables.schema_id = schemas.schema_id
WHERE schemas.name = @P1 AND tables.name = @P2;
";
    let result = client
        .query(TABLE_CDC_ENABLED_QUERY, &[&schema, &table])
        .await?;

    check_system_result(&result, "table CDC".to_string(), true)?;
    Ok(())
}

/// Ensure the `SNAPSHOT` transaction isolation level is enabled for the
/// database the provided `client` is currently connected to.
///
/// See: <https://learn.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql?view=sql-server-ver16>
pub async fn ensure_snapshot_isolation_enabled(client: &mut Client) -> Result<(), SqlServerError> {
    static SNAPSHOT_ISOLATION_QUERY: &str =
        "SELECT snapshot_isolation_state FROM sys.databases WHERE database_id = DB_ID();";
    let result = client.simple_query(SNAPSHOT_ISOLATION_QUERY).await?;

    check_system_result(&result, "snapshot isolation".to_string(), 1u8)?;
    Ok(())
}

/// Returns the maximum log sequence number.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-max-lsn-transact-sql?view=sql-server-ver16>
pub async fn get_max_lsn(client: &mut Client) -> Result<Vec<u8>, SqlServerError> {
    static MAX_LSN_QUERY: &str = "SELECT sys.fn_cdc_get_max_lsn();";
    let result = client.simple_query(MAX_LSN_QUERY).await?;

    match &result[..] {
        [row] => row
            .try_get::<&[u8], _>(0)?
            .map(|lsn| lsn.to_vec())
            .ok_or_else(|| SqlServerError::InvalidSystemSetting {
                name: "max_lsn".to_string(),
                expected: "10 byte binary".to_string(),
                actual: format!("{result:?}"),
            }),
        other => Err(SqlServerError::InvariantViolated(format!(
            "expected 1 row, got {other:?}"
        ))),
    }
}

/// Returns metadata about the columns from the specified table.
///
/// Note: The implementation of TryFrom for [`SqlServerColumnRaw`] relies on
/// the naming of these columns.
pub async fn get_table_columns(
    client: &mut Client,
    schema: &str,
    name: &str,
) -> Result<Vec<SqlServerColumnRaw>, SqlServerError> {
    // TODO(sql_server3): Handle user defined types.
    static TABLE_COLUMNS_QUERY: &str = "
SELECT c.name as col_name, t.name as data_type, c.is_nullable, c.max_length, c.precision, c.scale
FROM sys.columns c
JOIN sys.types t
ON c.system_type_id = t.system_type_id
WHERE c.object_id = OBJECT_ID(@P1);
";
    let object = format!("{schema}.{name}");
    let result = client.query(TABLE_COLUMNS_QUERY, &[&object]).await?;

    fn get_value<'a, T: tiberius::FromSql<'a>>(
        row: &'a tiberius::Row,
        name: &'static str,
    ) -> Result<T, SqlServerError> {
        row.try_get(name)?
            .ok_or(SqlServerError::MissingColumn(name))
    }

    let columns: Vec<_> = result
        .iter()
        .map(|row| {
            let column = SqlServerColumnRaw {
                name: get_value::<'_, &str>(row, "name")?.into(),
                data_type: get_value::<'_, &str>(row, "data_type")?.into(),
                is_nullable: get_value(row, "is_nullable")?,
                max_length: get_value(row, "max_length")?,
                precision: get_value(row, "precision")?,
                scale: get_value(row, "scale")?,
            };
            Ok::<_, SqlServerError>(column)
        })
        .collect::<Result<_, _>>()?;
    Ok(columns)
}

pub async fn get_tables(client: &mut Client) -> Result<Vec<SqlServerTableRaw>, SqlServerError> {
    static GET_TABLES_QUERY: &str = "
SELECT
    s.name as schema_name,
    t.name as table_name,
    c.name as col_name,
    ty.name as col_type,
    c.is_nullable as col_nullable,
    c.max_length as col_max_length,
    c.precision as col_precision,
    c.scale as col_scale,
    t.is_tracked_by_cdc
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.columns c ON t.object_id = c.object_id
JOIN sys.types ty ON c.system_type_id = ty.system_type_id
";
    fn get_value<'a, T: tiberius::FromSql<'a>>(
        row: &'a tiberius::Row,
        name: &'static str,
    ) -> Result<T, SqlServerError> {
        row.try_get(name)?
            .ok_or(SqlServerError::MissingColumn(name))
    }

    let result = client.simple_query(GET_TABLES_QUERY).await?;

    // Group our columns by (schema, name).
    let mut tables = BTreeMap::default();
    for row in result {
        let schema_name: Arc<str> = get_value::<&str>(&row, "schema_name")?.into();
        let table_name: Arc<str> = get_value::<&str>(&row, "table_name")?.into();
        let is_tracked_by_cdc: bool = get_value(&row, "is_tracked_by_cdc")?;

        let column_name = get_value::<&str>(&row, "col_name")?.into();
        let column = SqlServerColumnRaw {
            name: Arc::clone(&column_name),
            data_type: get_value::<&str>(&row, "col_type")?.into(),
            is_nullable: get_value(&row, "col_nullable")?,
            max_length: get_value(&row, "col_max_length")?,
            precision: get_value(&row, "col_precision")?,
            scale: get_value(&row, "col_scale")?,
        };

        let (columns, is_cdc) = tables
            .entry((Arc::clone(&schema_name), Arc::clone(&table_name)))
            .or_insert_with(|| (Vec::default(), None));
        columns.push(column);

        // Fold the values of the 'is_tracked_by_cdc' column.
        match is_cdc {
            None => *is_cdc = Some(is_tracked_by_cdc),
            Some(prev_value) => {
                if *prev_value != is_tracked_by_cdc {
                    let msg = format!(
                        "is_tracked_by_cdc changed for {}.{} {}. Was {} and now {}",
                        schema_name, table_name, column_name, prev_value, is_tracked_by_cdc,
                    );
                    return Err(SqlServerError::ProgrammingError(msg));
                }
            }
        }
    }

    // Flatten into our raw Table description.
    let tables = tables
        .into_iter()
        .map(|((schema, name), (columns, is_cdc_tracked))| {
            let is_cdc_enabled = is_cdc_tracked.ok_or_else(|| {
                SqlServerError::ProgrammingError("'is_cdc_tracked' was never set!".to_string())
            })?;

            Ok::<_, SqlServerError>(SqlServerTableRaw {
                schema_name: schema,
                name,
                columns: columns.into(),
                is_cdc_enabled,
            })
        })
        .collect::<Result<_, _>>()?;

    Ok(tables)
}

/// Helper function to parse an expected result from a "system" query.
fn check_system_result<'a, T>(
    result: &'a SmallVec<[tiberius::Row; 1]>,
    name: String,
    expected: T,
) -> Result<(), SqlServerError>
where
    T: tiberius::FromSql<'a> + Copy + fmt::Debug + fmt::Display + PartialEq,
{
    match &result[..] {
        [row] => {
            let result: Option<T> = row.try_get(0)?;
            if result == Some(expected) {
                Ok(())
            } else {
                Err(SqlServerError::InvalidSystemSetting {
                    name,
                    expected: expected.to_string(),
                    actual: format!("{result:?}"),
                })
            }
        }
        other => Err(SqlServerError::InvariantViolated(format!(
            "expected 1 row, got {other:?}"
        ))),
    }
}
