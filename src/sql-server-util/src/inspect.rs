// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Useful queries to inspect the state of a SQL Server instance.

use itertools::Itertools;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::cdc::{Lsn, RowFilterOption};
use crate::desc::{SqlServerColumnRaw, SqlServerTableRaw};
use crate::{Client, SqlServerError};

/// Returns the minimum log sequence number for the specified `capture_instance`.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-min-lsn-transact-sql?view=sql-server-ver16>
pub async fn get_min_lsn(
    client: &mut Client,
    capture_instance: &str,
) -> Result<Lsn, SqlServerError> {
    static MIN_LSN_QUERY: &str = "SELECT sys.fn_cdc_get_min_lsn(@P1);";
    let result = client.query(MIN_LSN_QUERY, &[&capture_instance]).await?;

    mz_ore::soft_assert_eq_or_log!(result.len(), 1);
    parse_lsn(&result[..1])
}

/// Returns the maximum log sequence number for the entire database.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-max-lsn-transact-sql?view=sql-server-ver16>
pub async fn get_max_lsn(client: &mut Client) -> Result<Lsn, SqlServerError> {
    static MAX_LSN_QUERY: &str = "SELECT sys.fn_cdc_get_max_lsn();";
    let result = client.simple_query(MAX_LSN_QUERY).await?;

    mz_ore::soft_assert_eq_or_log!(result.len(), 1);
    parse_lsn(&result[..1])
}

/// Increments the log sequence number.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-increment-lsn-transact-sql?view=sql-server-ver16>
pub async fn increment_lsn(client: &mut Client, lsn: Lsn) -> Result<Lsn, SqlServerError> {
    static INCREMENT_LSN_QUERY: &str = "SELECT sys.fn_cdc_increment_lsn(@P1);";
    let result = client
        .query(INCREMENT_LSN_QUERY, &[&lsn.as_bytes()])
        .await?;

    mz_ore::soft_assert_eq_or_log!(result.len(), 1);
    parse_lsn(&result[..1])
}

/// Parse an [`Lsn`] from the first column of the provided [`tiberius::Row`].
///
/// Returns an error if the provided slice doesn't have exactly one row.
fn parse_lsn(result: &[tiberius::Row]) -> Result<Lsn, SqlServerError> {
    match result {
        [row] => {
            let val = row
                .try_get::<&[u8], _>(0)?
                .ok_or_else(|| SqlServerError::InvalidData {
                    column_name: "lsn".to_string(),
                    error: "expected LSN at column 0, but found Null".to_string(),
                })?;
            let lsn = Lsn::try_from(val).map_err(|msg| SqlServerError::InvalidData {
                column_name: "lsn".to_string(),
                error: msg,
            })?;

            Ok(lsn)
        }
        other => Err(SqlServerError::InvalidData {
            column_name: "lsn".to_string(),
            error: format!("expected 1 column, got {other:?}"),
        }),
    }
}

/// Queries the specified capture instance and returns all changes from `start_lsn` to `end_lsn`.
///
/// TODO(sql_server1): This presents an opportunity for SQL injection. We should create a stored
/// procedure using `QUOTENAME` to sanitize the input for the capture instance provided by the
/// user.
pub async fn get_changes(
    client: &mut Client,
    capture_instance: &str,
    start_lsn: Lsn,
    end_lsn: Lsn,
    filter: RowFilterOption,
) -> Result<SmallVec<[tiberius::Row; 1]>, SqlServerError> {
    let query = format!(
        "SELECT * FROM cdc.fn_cdc_get_all_changes_{capture_instance}(@P1, @P2, N'{filter}');"
    );
    let results = client
        .query(&query, &[&start_lsn.as_bytes(), &end_lsn.as_bytes()])
        .await?;

    Ok(results)
}

/// Returns the `(capture_instance, schema_name, table_name)` for the tables
/// that are tracked by the specified `capture_instance`s.
pub async fn get_tables_for_capture_instance<'a>(
    client: &mut Client,
    capture_instances: impl IntoIterator<Item = &str>,
) -> Result<Vec<(Arc<str>, Arc<str>, Arc<str>)>, SqlServerError> {
    // SQL Server does not have support for array types, so we need to manually construct
    // the parameterized query.
    let params: SmallVec<[_; 1]> = capture_instances.into_iter().collect();
    // TODO(sql_server3): Remove this redundant collection.
    #[allow(clippy::as_conversions)]
    let params_dyn: SmallVec<[_; 1]> = params
        .iter()
        .map(|instance| instance as &dyn tiberius::ToSql)
        .collect();
    let param_indexes = params
        .iter()
        .enumerate()
        // Params are 1-based indexed.
        .map(|(idx, _)| format!("@P{}", idx + 1))
        .join(", ");

    let table_for_capture_instance_query = format!(
        "
SELECT c.capture_instance, SCHEMA_NAME(o.schema_id) as schema_name, o.name as obj_name
FROM sys.objects o
JOIN cdc.change_tables c
ON o.object_id = c.source_object_id
WHERE c.capture_instance IN ({param_indexes});"
    );

    let result = client
        .query(&table_for_capture_instance_query, &params_dyn[..])
        .await?;
    let tables = result
        .into_iter()
        .map(|row| {
            let capture_instance: &str = row.try_get("capture_instance")?.ok_or_else(|| {
                SqlServerError::ProgrammingError("missing column 'capture_instance'".to_string())
            })?;
            let schema_name: &str = row.try_get("schema_name")?.ok_or_else(|| {
                SqlServerError::ProgrammingError("missing column 'schema_name'".to_string())
            })?;
            let table_name: &str = row.try_get("obj_name")?.ok_or_else(|| {
                SqlServerError::ProgrammingError("missing column 'schema_name'".to_string())
            })?;

            Ok::<_, SqlServerError>((
                capture_instance.into(),
                schema_name.into(),
                table_name.into(),
            ))
        })
        .collect::<Result<_, _>>()?;

    Ok(tables)
}

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

pub async fn get_tables(client: &mut Client) -> Result<Vec<SqlServerTableRaw>, SqlServerError> {
    static GET_TABLES_QUERY: &str = "
SELECT
    s.name as schema_name,
    t.name as table_name,
    ch.capture_instance as capture_instance,
    c.name as col_name,
    ty.name as col_type,
    c.is_nullable as col_nullable,
    c.max_length as col_max_length,
    c.precision as col_precision,
    c.scale as col_scale
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.columns c ON t.object_id = c.object_id
JOIN sys.types ty ON c.system_type_id = ty.system_type_id
JOIN cdc.change_tables ch ON t.object_id = ch.source_object_id
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
        let capture_instance: Arc<str> = get_value::<&str>(&row, "capture_instance")?.into();

        let column_name = get_value::<&str>(&row, "col_name")?.into();
        let column = SqlServerColumnRaw {
            name: Arc::clone(&column_name),
            data_type: get_value::<&str>(&row, "col_type")?.into(),
            is_nullable: get_value(&row, "col_nullable")?,
            max_length: get_value(&row, "col_max_length")?,
            precision: get_value(&row, "col_precision")?,
            scale: get_value(&row, "col_scale")?,
        };

        let columns: &mut Vec<_> = tables
            .entry((
                Arc::clone(&schema_name),
                Arc::clone(&table_name),
                Arc::clone(&capture_instance),
            ))
            .or_default();
        columns.push(column);
    }

    // Flatten into our raw Table description.
    let tables = tables
        .into_iter()
        .map(|((schema, name, capture_instance), columns)| {
            Ok::<_, SqlServerError>(SqlServerTableRaw {
                schema_name: schema,
                name,
                capture_instance,
                columns: columns.into(),
                is_cdc_enabled: true,
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
