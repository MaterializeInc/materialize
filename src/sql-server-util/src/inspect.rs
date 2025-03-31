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
