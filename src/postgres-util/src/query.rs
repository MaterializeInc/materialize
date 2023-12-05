// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow};

use crate::PostgresError;

/// Runs the given query using the client and expects at most a single row to be returned.
pub async fn simple_query_opt(
    client: &Client,
    query: &str,
) -> Result<Option<SimpleQueryRow>, PostgresError> {
    let result = client.simple_query(query).await?;
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
