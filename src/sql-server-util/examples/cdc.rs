// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Example of streaming CDC changes from an instance of SQL Server.
//!
//! To run this example follow these steps:
//!   1. Get the latest SQL Server Docker image: <https://hub.docker.com/r/microsoft/mssql-server>.
//!   2. Shell into the running container to enable the SQL Agent which is
//!      required for CDC.
//!     ```
//!     docker exec -t --user root <container_id> bash
//!     $ /opt/mssql/bin/mssql-conf set sqlagent.enabled true
//!     $ systemctl restart mssql-server.service
//!     ```
//!   3. Run the following SQL to setup the proper environment.
//!     ```
//!     -- Setup the database for CDC.
//!     > CREATE DATABASE materialize;
//!     > USE materialize;
//!     > EXEC sys.sp_cdc_enable_db;
//!
//!     -- Enable CDC for our specific table.
//!     > CREATE TABLE t1 (a int);
//!     > EXEC sys.sp_cdc_enable_table
//!       @source_schema = 'dbo',
//!       @source_name = 't1',
//!       @role_name = NULL,
//!       @capture_instance = 'materialize_t1';
//!     ```
//!   4. Watch CDC events come streaming in as you change the table!

use futures::StreamExt;
use mz_sql_server_util::Client;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut config = tiberius::Config::new();

    config.host("localhost");
    config.port(1433);
    config.database("materialize");
    config.authentication(tiberius::AuthMethod::sql_server("SA", "password123?"));
    config.trust_cert();

    let (mut client, connection) = Client::connect(config).await?;
    mz_ore::task::spawn(|| "sql-server connection", async move { connection.await });
    tracing::info!("connection successful!");

    let mut cdc_handle = client.cdc("materialize_t1");

    // Get an initial snapshot of the table.
    let (lsn, snapshot) = cdc_handle.snapshot().await?;
    {
        let mut snapshot = std::pin::pin!(snapshot);
        while let Some(result) = snapshot.next().await {
            let row = result?;
            tracing::info!("snapshot: {row:?}");
        }
    }

    // Get a stream of changes from the table with the provided LSN.
    let changes = cdc_handle.start_lsn(lsn).into_stream();
    let mut changes = std::pin::pin!(changes);
    while let Some(change) = changes.next().await {
        let change = change?;
        tracing::info!("event: {change:?}");
    }

    Ok(())
}
