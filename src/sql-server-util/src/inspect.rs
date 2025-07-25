// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Useful queries to inspect the state of a SQL Server instance.

use futures::Stream;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::retry::RetryResult;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use tiberius::numeric::Numeric;
use std::time::Duration;

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
/// Returns the minimum log sequence number for the specified `capture_instance`, retrying
/// if the log sequence number is not available.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-min-lsn-transact-sql?view=sql-server-ver16>
pub async fn get_min_lsn_retry(
    client: &mut Client,
    capture_instance: &str,
    max_retry_duration: Duration,
) -> Result<Lsn, SqlServerError> {
    let (_client, lsn_result) = mz_ore::retry::Retry::default()
        .max_duration(max_retry_duration)
        .retry_async_with_state(client, |_, client| async {
            let result = crate::inspect::get_min_lsn(client, capture_instance).await;
            (client, map_null_lsn_to_retry(result))
        })
        .await;
    let Ok(lsn) = lsn_result else {
        tracing::warn!("database did not report a minimum LSN in time");
        return lsn_result;
    };
    Ok(lsn)
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

/// Returns the maximum log sequence number for the entire database, retrying
/// if the log sequence number is not available.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-max-lsn-transact-sql?view=sql-server-ver16>
pub async fn get_max_lsn_retry(
    client: &mut Client,
    max_retry_duration: Duration,
) -> Result<Lsn, SqlServerError> {
    let (_client, lsn_result) = mz_ore::retry::Retry::default()
        .max_duration(max_retry_duration)
        .retry_async_with_state(client, |_, client| async {
            let result = crate::inspect::get_max_lsn(client).await;
            (client, map_null_lsn_to_retry(result))
        })
        .await;

    let Ok(lsn) = lsn_result else {
        tracing::warn!("database did not report a maximum LSN in time");
        return lsn_result;
    };
    Ok(lsn)
}

fn map_null_lsn_to_retry<T>(result: Result<T, SqlServerError>) -> RetryResult<T, SqlServerError> {
    match result {
        Ok(val) => RetryResult::Ok(val),
        Err(err @ SqlServerError::NullLsn) => RetryResult::RetryableErr(err),
        Err(other) => RetryResult::FatalErr(other),
    }
}

/// Increments the log sequence number.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-increment-lsn-transact-sql?view=sql-server-ver16>
pub async fn increment_lsn(client: &mut Client, lsn: Lsn) -> Result<Lsn, SqlServerError> {
    static INCREMENT_LSN_QUERY: &str = "SELECT sys.fn_cdc_increment_lsn(@P1);";
    let result = client
        .query(INCREMENT_LSN_QUERY, &[&lsn.as_bytes().as_slice()])
        .await?;

    mz_ore::soft_assert_eq_or_log!(result.len(), 1);
    parse_lsn(&result[..1])
}

/// Parse an [`Lsn`] in Decimal(25,0) format of the provided [`tiberius::Row`].
///
/// Returns an error if the provided slice doesn't have exactly one row.
pub(crate) fn parse_numeric_lsn(row: &[tiberius::Row]) -> Result<Lsn, SqlServerError> {
    match row {
        [r] => {
            let numeric_lsn = r
                .try_get::<Numeric, _>(0)?
                .ok_or_else(|| SqlServerError::NullLsn)?;
            let lsn = Lsn::try_from(numeric_lsn).map_err(|msg| SqlServerError::InvalidData {
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

/// Parse an [`Lsn`] from the first column of the provided [`tiberius::Row`].
///
/// Returns an error if the provided slice doesn't have exactly one row.
fn parse_lsn(result: &[tiberius::Row]) -> Result<Lsn, SqlServerError> {
    match result {
        [row] => {
            let val = row
                .try_get::<&[u8], _>(0)?
                .ok_or_else(|| SqlServerError::NullLsn)?;
            if val.is_empty() {
                Err(SqlServerError::NullLsn)
            } else {
                let lsn = Lsn::try_from(val).map_err(|msg| SqlServerError::InvalidData {
                    column_name: "lsn".to_string(),
                    error: msg,
                })?;
                Ok(lsn)
            }
        }
        other => Err(SqlServerError::InvalidData {
            column_name: "lsn".to_string(),
            error: format!("expected 1 column, got {other:?}"),
        }),
    }
}

/// Queries the specified capture instance and returns all changes from
/// `[start_lsn, end_lsn)`, ordered by `start_lsn` in an ascending fashion.
///
/// TODO(sql_server2): This presents an opportunity for SQL injection. We should create a stored
/// procedure using `QUOTENAME` to sanitize the input for the capture instance provided by the
/// user.
pub fn get_changes_asc(
    client: &mut Client,
    capture_instance: &str,
    start_lsn: Lsn,
    end_lsn: Lsn,
    filter: RowFilterOption,
) -> impl Stream<Item = Result<tiberius::Row, SqlServerError>> + Send {
    const START_LSN_COLUMN: &str = "__$start_lsn";
    let query = format!(
        "SELECT * FROM cdc.fn_cdc_get_all_changes_{capture_instance}(@P1, @P2, N'{filter}') ORDER BY {START_LSN_COLUMN} ASC;"
    );
    client.query_streaming(
        query,
        &[
            &start_lsn.as_bytes().as_slice(),
            &end_lsn.as_bytes().as_slice(),
        ],
    )
}

/// Cleans up the change table associated with the specified `capture_instance` by
/// deleting `max_deletes` entries with a `start_lsn` less than `low_water_mark`.
///
/// Note: At the moment cleanup is kind of "best effort".  If this query succeeds
/// then at most `max_delete` rows were deleted, but the number of actual rows
/// deleted is not returned as part of the query. The number of rows _should_ be
/// present in an informational message (i.e. a Notice) that is returned, but
/// [`tiberius`] doesn't expose these to us.
///
/// TODO(sql_server2): Update [`tiberius`] to return informational messages so we
/// can determine how many rows got deleted.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-cleanup-change-table-transact-sql?view=sql-server-ver16>.
pub async fn cleanup_change_table(
    client: &mut Client,
    capture_instance: &str,
    low_water_mark: &Lsn,
    max_deletes: u32,
) -> Result<(), SqlServerError> {
    static GET_LSN_QUERY: &str =
        "SELECT MAX(start_lsn) FROM cdc.lsn_time_mapping WHERE start_lsn <= @P1";
    static CLEANUP_QUERY: &str = "
DECLARE @mz_cleanup_status_bit BIT;
SET @mz_cleanup_status_bit = 0;
EXEC sys.sp_cdc_cleanup_change_table
    @capture_instance = @P1,
    @low_water_mark = @P2,
    @threshold = @P3,
    @fCleanupFailed = @mz_cleanup_status_bit OUTPUT;
SELECT @mz_cleanup_status_bit;
    ";

    let max_deletes = i64::cast_from(max_deletes);

    // First we need to get a valid LSN as our low watermark. If we try to cleanup
    // a change table with an LSN that doesn't exist in the `cdc.lsn_time_mapping`
    // table we'll get an error code `22964`.
    let result = client
        .query(GET_LSN_QUERY, &[&low_water_mark.as_bytes().as_slice()])
        .await?;
    let low_water_mark_to_use = match &result[..] {
        [row] => row
            .try_get::<&[u8], _>(0)?
            .ok_or_else(|| SqlServerError::InvalidData {
                column_name: "mz_cleanup_status_bit".to_string(),
                error: "expected a bool, found NULL".to_string(),
            })?,
        other => Err(SqlServerError::ProgrammingError(format!(
            "expected one row for low water mark, found {other:?}"
        )))?,
    };

    // Once we get a valid LSN that is less than or equal to the provided watermark
    // we can clean up the specified change table!
    let result = client
        .query(
            CLEANUP_QUERY,
            &[&capture_instance, &low_water_mark_to_use, &max_deletes],
        )
        .await;

    let rows = match result {
        Ok(rows) => rows,
        Err(SqlServerError::SqlServer(e)) => {
            // See these remarks from the SQL Server Documentation.
            //
            // <https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-cleanup-change-table-transact-sql?view=sql-server-ver16#remarks>.
            let already_cleaned_up = e.code().map(|code| code == 22957).unwrap_or(false);

            if already_cleaned_up {
                return Ok(());
            } else {
                return Err(SqlServerError::SqlServer(e));
            }
        }
        Err(other) => return Err(other),
    };

    match &rows[..] {
        [row] => {
            let failure =
                row.try_get::<bool, _>(0)?
                    .ok_or_else(|| SqlServerError::InvalidData {
                        column_name: "mz_cleanup_status_bit".to_string(),
                        error: "expected a bool, found NULL".to_string(),
                    })?;

            if failure {
                Err(super::cdc::CdcError::CleanupFailed {
                    capture_instance: capture_instance.to_string(),
                    low_water_mark: *low_water_mark,
                })?
            } else {
                Ok(())
            }
        }
        other => Err(SqlServerError::ProgrammingError(format!(
            "expected one status row, found {other:?}"
        ))),
    }
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
    // If there are no tables to check for just return an empty list.
    if params.is_empty() {
        return Ok(Vec::default());
    }

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
    c.scale as col_scale,
    tc.constraint_name AS col_primary_key_constraint
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.columns c ON t.object_id = c.object_id
JOIN sys.types ty ON c.user_type_id = ty.user_type_id
JOIN cdc.change_tables ch ON t.object_id = ch.source_object_id
LEFT JOIN information_schema.key_column_usage kc
    ON kc.table_schema = s.name
    AND kc.table_name = t.name
    AND kc.column_name = c.name
LEFT JOIN information_schema.table_constraints tc
    ON tc.constraint_catalog = kc.constraint_catalog
    AND tc.constraint_schema = kc.constraint_schema
    AND tc.constraint_name = kc.constraint_name
    AND tc.table_schema = kc.table_schema
    AND tc.table_name = kc.table_name
    AND tc.constraint_type = 'PRIMARY KEY';
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
        let primary_key_constraint: Option<Arc<str>> = row
            .try_get::<&str, _>("col_primary_key_constraint")?
            .map(|v| v.into());

        let column_name = get_value::<&str>(&row, "col_name")?.into();
        let column = SqlServerColumnRaw {
            name: Arc::clone(&column_name),
            data_type: get_value::<&str>(&row, "col_type")?.into(),
            is_nullable: get_value(&row, "col_nullable")?,
            primary_key_constraint,
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
            })
        })
        .collect::<Result<_, _>>()?;

    Ok(tables)
}

/// Return a [`Stream`] that is the entire snapshot of the specified table.
pub fn snapshot(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> impl Stream<Item = Result<tiberius::Row, SqlServerError>> {
    let query = format!("SELECT * FROM {schema}.{table};");
    client.query_streaming(query, &[])
}

/// Returns the total number of rows present in the specified table.
pub async fn snapshot_size(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<usize, SqlServerError> {
    let query = format!("SELECT COUNT(*) FROM {schema}.{table};");
    let result = client.query(query, &[]).await?;

    match &result[..] {
        [row] => match row.try_get::<i32, _>(0)? {
            Some(count @ 0..) => Ok(usize::try_from(count).expect("known to fit")),
            Some(negative) => Err(SqlServerError::InvalidData {
                column_name: "count".to_string(),
                error: format!("found negative count: {negative}"),
            }),
            None => Err(SqlServerError::InvalidData {
                column_name: "count".to_string(),
                error: "expected a value found NULL".to_string(),
            }),
        },
        other => Err(SqlServerError::InvariantViolated(format!(
            "expected one row, got {other:?}"
        ))),
    }
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
