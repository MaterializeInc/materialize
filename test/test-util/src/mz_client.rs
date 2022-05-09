// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use tokio::time::{self, Duration};
use tokio_postgres::{error::SqlState, Client, Error, NoTls, Row};
use tracing::{debug, info};

use mz_ore::task;

/// Create and return a new PostgreSQL client, spawning off the connection
/// object along the way.
pub async fn client(host: &str, port: u16) -> Result<Client> {
    let (mz_client, conn) = tokio_postgres::Config::new()
        .user("materialize")
        .host(host)
        .port(port)
        .connect(NoTls)
        .await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    task::spawn(|| "test_util_mz_client", async move {
        if let Err(e) = conn.await {
            panic!("connection error: {}", e);
        }
    });

    Ok(mz_client)
}

/// Try running PostgresSQL's `query` function, checking for a common
/// Materialize error in `check_error`.
pub async fn try_query(mz_client: &Client, query: &str, delay: Duration) -> Result<Vec<Row>> {
    loop {
        let timer = std::time::Instant::now();
        match mz_client.query(&*query, &[]).await {
            Ok(rows) => return Ok(rows),
            Err(e) => check_error(e)?,
        }
        delay_for(timer.elapsed(), delay).await;
    }
}

/// Try running PostgreSQL's `query_one` function, checking for a common
/// Materialize error in `check_error`.
pub async fn try_query_one(mz_client: &Client, query: &str, delay: Duration) -> Result<Row> {
    loop {
        let timer = std::time::Instant::now();
        match mz_client.query_one(&*query, &[]).await {
            Ok(rows) => return Ok(rows),
            Err(e) => check_error(e)?,
        }
        delay_for(timer.elapsed(), delay).await;
    }
}

/// The SQL_STATEMENT_NOT_YET_COMPLETE error will surface if we query a view in
/// Materialize before data exists for that view. It is common to hit this error
/// just after creating a view, particularly in testing or demo code.
///
/// Since this error is likely transient, we should retry reading from the view
/// instead of failing.
fn check_error(e: Error) -> Result<()> {
    if e.code() == Some(&SqlState::SQL_STATEMENT_NOT_YET_COMPLETE) {
        info!("Error querying, will try again... {}", e.to_string());
        Ok(())
    } else {
        Err(anyhow::Error::from(e))
    }
}

/// Limit the queries per second against a view in Materialize.
async fn delay_for(elapsed: Duration, delay: Duration) {
    if elapsed < delay {
        time::sleep(delay - elapsed).await;
    } else {
        info!(
            "Expected to query for records in {:#?}, took {:#?}",
            delay, elapsed
        );
    }
}

/// Run Materialize's `SHOW SOURCES` command
pub async fn show_sources(mz_client: &Client) -> Result<Vec<String>> {
    let mut res = Vec::new();
    for row in mz_client.query("SHOW SOURCES", &[]).await? {
        res.push(row.get(0))
    }

    Ok(res)
}

/// Delete a source and all dependent views, if the source exists
pub async fn drop_source(mz_client: &Client, name: &str) -> Result<()> {
    let q = format!("DROP SOURCE IF EXISTS {} CASCADE", name);
    debug!("deleting source=> {}", q);
    mz_client.execute(&*q, &[]).await?;
    Ok(())
}

/// Delete a table and all dependent views, if the table exists
pub async fn drop_table(mz_client: &Client, name: &str) -> Result<()> {
    let q = format!("DROP TABLE IF EXISTS {} CASCADE", name);
    debug!("deleting table=> {}", q);
    mz_client.execute(&*q, &[]).await?;
    Ok(())
}

/// Delete an index
pub async fn drop_index(mz_client: &Client, name: &str) -> Result<()> {
    let q = format!("DROP INDEX {}", name);
    debug!("deleting index=> {}", q);
    mz_client.execute(&*q, &[]).await?;
    Ok(())
}

/// Run PostgreSQL's `execute` function
pub async fn execute(mz_client: &Client, query: &str) -> Result<u64> {
    debug!("exec=> {}", query);
    Ok(mz_client.execute(query, &[]).await?)
}
