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
use smallvec::SmallVec;
use std::sync::Arc;

use crate::cdc::{Lsn, RowFilterOption};
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

/// Queries the specified capture instance and returns all changes from `start_lsn` to
/// `end_lsn`, ordered by `start_lsn` in an ascending fashion.
///
/// TODO(sql_server1): This presents an opportunity for SQL injection. We should create a stored
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
    client.query_streaming(query, &[&start_lsn.as_bytes(), &end_lsn.as_bytes()])
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
