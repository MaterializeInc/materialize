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

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use mz_ore::future::InTask;
use mz_sql_server_util::cdc::{CdcEvent, SqlServerSnapshotMetadata};
use mz_sql_server_util::config::TunnelConfig;
use mz_sql_server_util::{Client, Config};
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

    // Open one client to stream changes.
    let mz_config = Config::new(config.clone(), TunnelConfig::Direct, InTask::No);
    let mut client_1 = Client::connect(mz_config).await?;
    tracing::info!("connection 1 successful!");

    let capture_instances = ["materialize_t1", "materialize_t2"];
    let mut cdc_handle = client_1.cdc(capture_instances);

    cdc_handle.wait_for_ready().await?;

    // Open a second client that we can use to cleanup the underlying change tables.
    let mz_config = Config::new(config.clone(), TunnelConfig::Direct, InTask::No);
    let mut client_2 = Client::connect(mz_config).await?;
    tracing::info!("connection 2 successful!");

    let tables = mz_sql_server_util::inspect::get_tables_for_capture_instance(
        &mut client_2,
        capture_instances,
    )
    .await?;
    let mut instance_to_lsn = BTreeMap::new();

    for table in tables {
        // Get an initial snapshot of the table.
        let (snapshot_metadata, snapshot) = cdc_handle
            .snapshot(&table, 1, mz_repr::GlobalId::User(1))
            .await?;

        let SqlServerSnapshotMetadata {
            lsn, count: stats, ..
        } = snapshot_metadata;
        tracing::info!("snapshot stats: {stats:?}");

        instance_to_lsn.insert(Arc::clone(&table.capture_instance.name), lsn);

        let mut snapshot = std::pin::pin!(snapshot);
        while let Some(result) = snapshot.next().await {
            let row = result?;
            tracing::info!("snapshot: {} {row:?}", &table.capture_instance.name);
        }
    }

    // Initialize all capture instances at the LSN we just snapshotted.
    for instance in capture_instances {
        let lsn = instance_to_lsn
            .remove(instance)
            .expect("table must have instance");

        cdc_handle = cdc_handle.start_lsn(instance, lsn);
    }
    // Get a stream of changes from the table.
    let changes = cdc_handle.into_stream();
    let mut changes = std::pin::pin!(changes);
    while let Some(result) = changes.next().await {
        let event = result?;
        match event {
            CdcEvent::Data {
                capture_instance,
                lsn,
                changes,
            } => {
                tracing::info!("data: {capture_instance} @ {lsn} : {changes:?}");
            }
            // We've done all we need with this change, so cleanup the upstream tables.
            CdcEvent::Progress { next_lsn } => {
                tracing::info!("progress: received all data < {next_lsn}");
                for instance in capture_instances {
                    let result = mz_sql_server_util::inspect::cleanup_change_table(
                        &mut client_2,
                        instance,
                        &next_lsn,
                        1000,
                    )
                    .await?;
                    tracing::info!(?result, "cleanup: {instance} data < {next_lsn}");
                }
            }
        }
    }

    Ok(())
}
