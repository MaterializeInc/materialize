// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::disallowed_methods)]

pub use mz_ore::sql::{Sql, SqlFormatError, SqlTemplateError, sql_template_placeholder_count};

use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, GenericClient, Row, SimpleQueryMessage, SimpleQueryRow, Statement};

use crate::PostgresError;

/// Runs the given query using the client and expects at most a single row to be returned.
pub async fn simple_query_opt(
    client: &Client,
    query: Sql,
) -> Result<Option<SimpleQueryRow>, PostgresError> {
    let result = simple_query(client, query).await?;
    let mut rows = result.into_iter().filter_map(|msg| match msg {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    });
    match (rows.next(), rows.next()) {
        (Some(row), None) => Ok(Some(row)),
        (None, None) => Ok(None),
        _ => Err(PostgresError::UnexpectedRow),
    }
}

/// Runs a simple query and returns all protocol messages.
pub async fn simple_query(
    client: &Client,
    query: Sql,
) -> Result<Vec<SimpleQueryMessage>, PostgresError> {
    Ok(client.simple_query(query.as_str()).await?)
}

/// Runs a query and returns all resulting rows.
pub async fn query<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, PostgresError> {
    Ok(client.query(query.as_str(), params).await?)
}

/// Runs a prepared query and returns all resulting rows.
pub async fn query_prepared<C: GenericClient + Sync>(
    client: &C,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, PostgresError> {
    Ok(client.query(statement, params).await?)
}

/// Runs a query and returns exactly one row.
pub async fn query_one<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Row, PostgresError> {
    Ok(client.query_one(query.as_str(), params).await?)
}

/// Runs a prepared query and returns exactly one row.
pub async fn query_one_prepared<C: GenericClient + Sync>(
    client: &C,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Row, PostgresError> {
    Ok(client.query_one(statement, params).await?)
}

/// Runs a query and returns at most one row.
pub async fn query_opt<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, PostgresError> {
    Ok(client.query_opt(query.as_str(), params).await?)
}

/// Runs a prepared query and returns at most one row.
pub async fn query_opt_prepared<C: GenericClient + Sync>(
    client: &C,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, PostgresError> {
    Ok(client.query_opt(statement, params).await?)
}

/// Runs a query and returns the number of affected rows.
pub async fn execute<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, PostgresError> {
    Ok(client.execute(query.as_str(), params).await?)
}

/// Runs a prepared query and returns the number of affected rows.
pub async fn execute_prepared<C: GenericClient + Sync>(
    client: &C,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, PostgresError> {
    Ok(client.execute(statement, params).await?)
}

/// Runs one or more SQL statements with no returned rows.
pub async fn batch_execute<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
) -> Result<(), PostgresError> {
    Ok(client.batch_execute(query.as_str()).await?)
}
