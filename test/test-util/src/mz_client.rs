// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tokio_postgres::{Client, Error, NoTls, Row};

/// Create and return a new PostgreSQL client, spawning off the connection
/// object along the way.
pub async fn client(host: &str, port: u16) -> Result<Client, anyhow::Error> {
    let (mz_client, conn) = tokio_postgres::Config::new()
        .user("mzd")
        .host(host)
        .port(port)
        .connect(NoTls)
        .await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            panic!("connection error: {}", e);
        }
    });

    Ok(mz_client)
}

/// Try running PostgresSQL's `query` function, checking for a common
/// Materialize error in `check_error`.
pub async fn try_query(
    mz_client: &Client,
    query: &str,
    delay: Duration,
) -> Result<Vec<Row>, anyhow::Error> {
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
pub async fn try_query_one(
    mz_client: &Client,
    query: &str,
    delay: Duration,
) -> Result<Row, anyhow::Error> {
    loop {
        let timer = std::time::Instant::now();
        match mz_client.query_one(&*query, &[]).await {
            Ok(rows) => return Ok(rows),
            Err(e) => check_error(e)?,
        }
        delay_for(timer.elapsed(), delay).await;
    }
}

/// This error ("At least one input has no complete timestamps yet") will surface if
/// we query a view in Materialize before data exists for that view. It is common to hit
/// this error just after creating a view, particularly in testing or demo code.
///
/// Since this error is likely transient, we should retry reading from the view
/// instead of failing.
fn check_error(e: Error) -> Result<(), anyhow::Error> {
    if e.to_string()
        .contains("At least one input has no complete timestamps yet")
    {
        log::info!("Error querying, will try again... {}", e.to_string());
        Ok(())
    } else {
        Err(anyhow::Error::from(e))
    }
}

/// Limit the queries per second against a view in Materialize.
async fn delay_for(elapsed: Duration, delay: Duration) {
    if elapsed < delay {
        tokio::time::delay_for(delay - elapsed).await;
    } else {
        log::info!(
            "Expected to query for records in {:#?}, took {:#?}",
            delay,
            elapsed
        );
    }
}
