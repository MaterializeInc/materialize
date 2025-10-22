// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Useful queries to inspect the state of a SQL Server instance.

use anyhow::Context;
use chrono::NaiveDateTime;
use futures::Stream;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::retry::RetryResult;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tiberius::numeric::Numeric;

use crate::cdc::{Lsn, RowFilterOption};
use crate::desc::{
    SqlServerCaptureInstanceRaw, SqlServerColumnRaw, SqlServerQualifiedTableName, SqlServerTableRaw,
};
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
/// This implementation relies on CDC, which is asynchronous, so may
/// return an LSN that is less than the maximum LSN of SQL server.
///
/// See:
/// - <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-max-lsn-transact-sql?view=sql-server-ver16>
/// - <https://groups.google.com/g/debezium/c/47Yg2r166KM/m/lHqtRF2xAQAJ?pli=1>
pub async fn get_max_lsn(client: &mut Client) -> Result<Lsn, SqlServerError> {
    static MAX_LSN_QUERY: &str = "SELECT sys.fn_cdc_get_max_lsn();";
    let result = client.simple_query(MAX_LSN_QUERY).await?;

    mz_ore::soft_assert_eq_or_log!(result.len(), 1);
    parse_lsn(&result[..1])
}

/// Retrieves the minumum [`Lsn`] (start_lsn field) from `cdc.change_tables`
/// for the specified capture instances.
///
/// This is based on the `sys.fn_cdc_get_min_lsn` implementation, which has logic
/// that we want to bypass. Specifically, `sys.fn_cdc_get_min_lsn` returns NULL
/// if the `start_lsn` in `cdc.change_tables` is less than or equal to the LSN
/// returned by `sys.fn_cdc_get_max_lsn`.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql?view=sql-server-ver16>
pub async fn get_min_lsns(
    client: &mut Client,
    capture_instances: impl IntoIterator<Item = &str>,
) -> Result<BTreeMap<Arc<str>, Lsn>, SqlServerError> {
    let capture_instances: SmallVec<[_; 1]> = capture_instances.into_iter().collect();
    let values: Vec<_> = capture_instances
        .iter()
        .map(|ci| {
            let ci: &dyn tiberius::ToSql = ci;
            ci
        })
        .collect();
    let args = (0..capture_instances.len())
        .map(|i| format!("@P{}", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    let stmt = format!(
        "SELECT capture_instance, start_lsn FROM cdc.change_tables WHERE capture_instance IN ({args});"
    );
    let result = client.query(stmt, &values).await?;
    let min_lsns = result
        .into_iter()
        .map(|row| {
            let capture_instance: Arc<str> = row
                .try_get::<&str, _>("capture_instance")?
                .ok_or_else(|| {
                    SqlServerError::ProgrammingError(
                        "missing column 'capture_instance'".to_string(),
                    )
                })?
                .into();
            let start_lsn: &[u8] = row.try_get("start_lsn")?.ok_or_else(|| {
                SqlServerError::ProgrammingError("missing column 'start_lsn'".to_string())
            })?;
            let min_lsn = Lsn::try_from(start_lsn).map_err(|msg| SqlServerError::InvalidData {
                column_name: "lsn".to_string(),
                error: format!("Error parsing LSN for {capture_instance}: {msg}"),
            })?;
            Ok::<_, SqlServerError>((capture_instance, min_lsn))
        })
        .collect::<Result<_, _>>()?;

    Ok(min_lsns)
}

/// Returns the maximum log sequence number for the entire database, retrying
/// if the log sequence number is not available. This implementation relies on
/// CDC, which is asynchronous, so may return an LSN that is less than the
/// maximum LSN of SQL server.
///
/// See:
/// - <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-max-lsn-transact-sql?view=sql-server-ver16>
/// - <https://groups.google.com/g/debezium/c/47Yg2r166KM/m/lHqtRF2xAQAJ?pli=1>
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

// Retrieves all columns in tables that have CDC (Change Data Capture) enabled.
//
// Returns metadata needed to create an instance of ['SqlServerTableRaw`].
//
// The query joins several system tables:
// - sys.tables: Source tables in the database
// - sys.schemas: Schema information for proper table identification
// - sys.columns: Column definitions including nullability
// - sys.types: Data type information for each column
// - cdc.change_tables: CDC configuration linking capture instances to source tables
// - information_schema views: To identify primary key constraints
//
// For each column, it returns:
// - Table identification (schema_name, table_name, capture_instance)
// - Column metadata (name, type, nullable, max_length, precision, scale)
// - Primary key information (constraint name if the column is part of a PK)
static GET_COLUMNS_FOR_TABLES_WITH_CDC_QUERY: &str = "
SELECT
    s.name as schema_name,
    t.name as table_name,
    ch.capture_instance as capture_instance,
    ch.create_date as capture_instance_create_date,
    c.name as col_name,
    ty.name as col_type,
    c.is_nullable as col_nullable,
    c.max_length as col_max_length,
    c.precision as col_precision,
    c.scale as col_scale,
    c.is_computed as col_is_computed,
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
    AND tc.constraint_type = 'PRIMARY KEY'
";

/// Returns the table metadata for the tables that are tracked by the specified `capture_instance`s.
pub async fn get_tables_for_capture_instance<'a>(
    client: &mut Client,
    capture_instances: impl IntoIterator<Item = &str>,
) -> Result<Vec<SqlServerTableRaw>, SqlServerError> {
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
        "{GET_COLUMNS_FOR_TABLES_WITH_CDC_QUERY} WHERE ch.capture_instance IN ({param_indexes});"
    );

    let result = client
        .query(&table_for_capture_instance_query, &params_dyn[..])
        .await?;

    let tables = deserialize_table_columns_to_raw_tables(&result)?;

    Ok(tables)
}

/// Retrieves column metdata from the CDC table maintained by the provided capture instance. The
/// resulting column information collection is similar to the information collected for the
/// upstream table, with the exclusion of nullability and primary key constraints, which contain
/// static values for CDC columns. CDC table schema is automatically generated and does not attempt
/// to enforce the same constraints on the data as the upstream table.
pub async fn get_cdc_table_columns(
    client: &mut Client,
    capture_instance: &str,
) -> Result<BTreeMap<Arc<str>, SqlServerColumnRaw>, SqlServerError> {
    static CDC_COLUMNS_QUERY: &str = "SELECT \
        c.name AS col_name, \
        t.name AS col_type, \
        c.max_length AS col_max_length, \
        c.precision AS col_precision, \
        c.scale AS col_scale, \
        c.is_computed as col_is_computed \
    FROM \
        sys.columns AS c \
    JOIN sys.types AS t ON c.system_type_id = t.system_type_id AND c.user_type_id = t.user_type_id \
    WHERE \
        c.object_id = OBJECT_ID(@P1) AND c.name NOT LIKE '__$%' \
    ORDER BY c.column_id;";
    let cdc_table_name = format!("cdc.{capture_instance}_CT");
    let result = client.query(CDC_COLUMNS_QUERY, &[&cdc_table_name]).await?;
    let mut columns = BTreeMap::new();
    for row in result.iter() {
        let column_name: Arc<str> = get_value::<&str>(row, "col_name")?.into();
        // Reusing this struct even though some of the fields aren't needed because it simplifies
        // comparison with the upstream table metadata
        let column = SqlServerColumnRaw {
            name: Arc::clone(&column_name),
            data_type: get_value::<&str>(row, "col_type")?.into(),
            is_nullable: true,
            primary_key_constraint: None,
            max_length: get_value(row, "col_max_length")?,
            precision: get_value(row, "col_precision")?,
            scale: get_value(row, "col_scale")?,
            is_computed: get_value(row, "col_is_computed")?,
        };
        columns.insert(column_name, column);
    }
    Ok(columns)
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

/// Retrieves the largest `restore_history_id` from SQL Server for the current database.  The
/// `restore_history_id` column is of type `IDENTITY(1,1)` based on `EXEC sp_help restorehistory`.
/// We expect it to start at 1 and be incremented by 1, with possible gaps in values.
/// See:
/// - <https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/restorehistory-transact-sql?view=sql-server-ver17>
/// - <https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property?view=sql-server-ver17>
pub async fn get_latest_restore_history_id(
    client: &mut Client,
) -> Result<Option<i32>, SqlServerError> {
    static LATEST_RESTORE_ID_QUERY: &str = "SELECT TOP 1 restore_history_id \
        FROM msdb.dbo.restorehistory \
        WHERE destination_database_name = DB_NAME() \
        ORDER BY restore_history_id DESC;";
    let result = client.simple_query(LATEST_RESTORE_ID_QUERY).await?;

    match &result[..] {
        [] => Ok(None),
        [row] => Ok(row.try_get::<i32, _>(0)?),
        other => Err(SqlServerError::InvariantViolated(format!(
            "expected one row, got {other:?}"
        ))),
    }
}

/// A DDL event collected from the `cdc.ddl_history` table.
#[derive(Debug)]
pub struct DDLEvent {
    pub lsn: Lsn,
    pub ddl_command: Arc<str>,
}

impl DDLEvent {
    /// Returns true if the DDL event is a compatible change, or false if it is not.
    /// This performs a naive parsing of the DDL command looking for modification of columns
    ///  1. ALTER TABLE .. ALTER COLUMN
    ///  2. ALTER TABLE .. DROP COLUMN
    ///
    /// See <https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver17>
    pub fn is_compatible(&self) -> bool {
        // TODO (maz): This is currently a basic check that doesn't take into account type changes.
        // At some point, we will need to move this to SqlServerTableDesc and expand it.
        let mut words = self.ddl_command.split_ascii_whitespace();
        match (
            words.next().map(str::to_ascii_lowercase).as_deref(),
            words.next().map(str::to_ascii_lowercase).as_deref(),
        ) {
            (Some("alter"), Some("table")) => {
                let mut peekable = words.peekable();
                let mut compatible = true;
                while compatible && let Some(token) = peekable.next() {
                    compatible = match token.to_ascii_lowercase().as_str() {
                        "alter" | "drop" => peekable
                            .peek()
                            .is_some_and(|next_tok| !next_tok.eq_ignore_ascii_case("column")),
                        _ => true,
                    }
                }
                compatible
            }
            _ => true,
        }
    }
}

/// Returns DDL changes made to the source table for the given capture instance.  This follows the
/// same convention as `cdc.fn_cdc_get_all_changes_<capture_instance>`, in that the range is
/// inclusive, i.e. `[from_lsn, to_lsn]`. The events are returned in ascending order of
/// LSN.
pub async fn get_ddl_history(
    client: &mut Client,
    capture_instance: &str,
    from_lsn: &Lsn,
    to_lsn: &Lsn,
) -> Result<BTreeMap<SqlServerQualifiedTableName, Vec<DDLEvent>>, SqlServerError> {
    // We query the ddl_history table instead of using the stored procedure as there doesn't
    // appear to be a way to apply filters or projections against output of the stored procedure
    // without an intermediate table.
    static DDL_HISTORY_QUERY: &str = "SELECT \
                s.name AS schema_name, \
                t.name AS table_name, \
                dh.ddl_lsn, \
                dh.ddl_command
            FROM \
                cdc.change_tables ct \
            JOIN cdc.ddl_history dh ON dh.object_id = ct.object_id \
            JOIN sys.tables t ON t.object_id = dh.source_object_id \
            JOIN sys.schemas s ON s.schema_id = t.schema_id \
            WHERE \
                ct.capture_instance = @P1 \
                AND dh.ddl_lsn >= @P2 \
                AND dh.ddl_lsn <= @P3 \
            ORDER BY ddl_lsn;";

    let result = client
        .query(
            DDL_HISTORY_QUERY,
            &[
                &capture_instance,
                &from_lsn.as_bytes().as_slice(),
                &to_lsn.as_bytes().as_slice(),
            ],
        )
        .await?;

    // SQL server doesn't support array types, and using string_agg to collect LSN
    // would require more parsing, so we opt for a BTreeMap to accumulate the results.
    let mut collector: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for row in result.iter() {
        let schema_name: Arc<str> = get_value::<&str>(row, "schema_name")?.into();
        let table_name: Arc<str> = get_value::<&str>(row, "table_name")?.into();
        let lsn: &[u8] = get_value::<&[u8]>(row, "ddl_lsn")?;
        let ddl_command: Arc<str> = get_value::<&str>(row, "ddl_command")?.into();

        let qualified_table_name = SqlServerQualifiedTableName {
            schema_name,
            table_name,
        };
        let lsn = Lsn::try_from(lsn).map_err(|lsn_err| SqlServerError::InvalidData {
            column_name: "ddl_lsn".to_string(),
            error: lsn_err,
        })?;

        collector
            .entry(qualified_table_name)
            .or_default()
            .push(DDLEvent { lsn, ddl_command });
    }

    Ok(collector)
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

/// Ensure the SQL Server Agent is running.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-server-services-transact-sql?view=azuresqldb-current&viewFallbackFrom=sql-server-ver17>
pub async fn ensure_sql_server_agent_running(client: &mut Client) -> Result<(), SqlServerError> {
    static AGENT_STATUS_QUERY: &str = "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%';";
    let result = client.simple_query(AGENT_STATUS_QUERY).await?;

    check_system_result(&result, "SQL Server Agent status".to_string(), "Running")?;
    Ok(())
}

pub async fn get_tables(client: &mut Client) -> Result<Vec<SqlServerTableRaw>, SqlServerError> {
    let result = client
        .simple_query(&format!("{GET_COLUMNS_FOR_TABLES_WITH_CDC_QUERY};"))
        .await?;

    let tables = deserialize_table_columns_to_raw_tables(&result)?;

    Ok(tables)
}

// Helper function to retrieve value from a row.
fn get_value<'a, T: tiberius::FromSql<'a>>(
    row: &'a tiberius::Row,
    name: &'static str,
) -> Result<T, SqlServerError> {
    row.try_get(name)?
        .ok_or(SqlServerError::MissingColumn(name))
}

fn deserialize_table_columns_to_raw_tables(
    rows: &[tiberius::Row],
) -> Result<Vec<SqlServerTableRaw>, SqlServerError> {
    // Group our columns by (schema, name).
    let mut tables = BTreeMap::default();
    for row in rows {
        let schema_name: Arc<str> = get_value::<&str>(row, "schema_name")?.into();
        let table_name: Arc<str> = get_value::<&str>(row, "table_name")?.into();
        let capture_instance: Arc<str> = get_value::<&str>(row, "capture_instance")?.into();
        let capture_instance_create_date: NaiveDateTime =
            get_value::<NaiveDateTime>(row, "capture_instance_create_date")?;
        let primary_key_constraint: Option<Arc<str>> = row
            .try_get::<&str, _>("col_primary_key_constraint")?
            .map(|v| v.into());

        let column_name = get_value::<&str>(row, "col_name")?.into();
        let column = SqlServerColumnRaw {
            name: Arc::clone(&column_name),
            data_type: get_value::<&str>(row, "col_type")?.into(),
            is_nullable: get_value(row, "col_nullable")?,
            primary_key_constraint,
            max_length: get_value(row, "col_max_length")?,
            precision: get_value(row, "col_precision")?,
            scale: get_value(row, "col_scale")?,
            is_computed: get_value(row, "col_is_computed")?,
        };

        let columns: &mut Vec<_> = tables
            .entry((
                Arc::clone(&schema_name),
                Arc::clone(&table_name),
                Arc::clone(&capture_instance),
                capture_instance_create_date,
            ))
            .or_default();
        columns.push(column);
    }

    // Flatten into our raw Table description.
    let raw_tables = tables
        .into_iter()
        .map(
            |((schema, name, capture_instance, capture_instance_create_date), columns)| {
                SqlServerTableRaw {
                    schema_name: schema,
                    name,
                    capture_instance: Arc::new(SqlServerCaptureInstanceRaw {
                        name: capture_instance,
                        create_date: capture_instance_create_date.into(),
                    }),
                    columns: columns.into(),
                }
            },
        )
        .collect::<Vec<SqlServerTableRaw>>();

    Ok(raw_tables)
}

/// Return a [`Stream`] that is the entire snapshot of the specified table.
pub fn snapshot(
    client: &mut Client,
    table: &SqlServerTableRaw,
) -> impl Stream<Item = Result<tiberius::Row, SqlServerError>> {
    let schema_name = &table.schema_name;
    let table_name = &table.name;
    let cols = table
        .columns
        .iter()
        .map(|SqlServerColumnRaw { name, .. }| format!("[{name}]"))
        .join(",");
    let query = format!("SELECT {cols} FROM [{schema_name}].[{table_name}];");
    client.query_streaming(query, &[])
}

/// Returns the total number of rows present in the specified table.
pub async fn snapshot_size(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<usize, SqlServerError> {
    let query = format!("SELECT COUNT(*) FROM [{schema}].[{table}];");
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

/// Return a Result that is empty if all tables, columns, and capture instances
/// have the necessary permissions to and an error if any table, column,
/// or capture instance does not have the necessary permissions
/// for tracking changes.
pub async fn validate_source_privileges<'a>(
    client: &mut Client,
    capture_instances: impl IntoIterator<Item = &str>,
) -> Result<(), SqlServerError> {
    let params: SmallVec<[_; 1]> = capture_instances.into_iter().collect();

    if params.is_empty() {
        return Ok(());
    }

    let params_dyn: SmallVec<[_; 1]> = params
        .iter()
        .map(|instance| {
            let instance: &dyn tiberius::ToSql = instance;
            instance
        })
        .collect();

    let param_indexes = (1..params.len() + 1)
        .map(|idx| format!("@P{}", idx))
        .join(", ");

    // NB(ptravers): we rely on HAS_PERMS_BY_NAME to check both table and column permissions.
    let capture_instance_query = format!(
            "
        SELECT
            SCHEMA_NAME(o.schema_id) + '.' + o.name AS qualified_table_name,
            ct.capture_instance AS capture_instance,
            COALESCE(HAS_PERMS_BY_NAME(SCHEMA_NAME(o.schema_id) + '.' + o.name, 'OBJECT', 'SELECT'), 0) AS table_select,
            COALESCE(HAS_PERMS_BY_NAME('cdc.' + ct.capture_instance + '_CT', 'OBJECT', 'SELECT'), 0) AS capture_table_select
        FROM cdc.change_tables ct
        JOIN sys.objects o ON o.object_id = ct.source_object_id
        WHERE ct.capture_instance IN ({param_indexes});
            "
        );

    let rows = client
        .query(capture_instance_query, &params_dyn[..])
        .await?;

    let mut capture_instances_without_perms = vec![];
    let mut tables_without_perms = vec![];

    for row in rows {
        let table: &str = row
            .try_get("qualified_table_name")
            .context("getting table column")?
            .ok_or_else(|| anyhow::anyhow!("no table column?"))?;

        let capture_instance: &str = row
            .try_get("capture_instance")
            .context("getting capture_instance column")?
            .ok_or_else(|| anyhow::anyhow!("no capture_instance column?"))?;

        let permitted_table: i32 = row
            .try_get("table_select")
            .context("getting table_select column")?
            .ok_or_else(|| anyhow::anyhow!("no table_select column?"))?;

        let permitted_capture_instance: i32 = row
            .try_get("capture_table_select")
            .context("getting capture_table_select column")?
            .ok_or_else(|| anyhow::anyhow!("no capture_table_select column?"))?;

        if permitted_table == 0 {
            tables_without_perms.push(table.to_string());
        }

        if permitted_capture_instance == 0 {
            capture_instances_without_perms.push(capture_instance.to_string());
        }
    }

    if !capture_instances_without_perms.is_empty() || !tables_without_perms.is_empty() {
        return Err(SqlServerError::AuthorizationError {
            tables: tables_without_perms.join(", "),
            capture_instances: capture_instances_without_perms.join(", "),
        });
    }

    Ok(())
}
