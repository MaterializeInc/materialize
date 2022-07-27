// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::{bail, Context};
use itertools::Itertools;
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use tracing::debug;

use mz_metabase::{
    DatabaseMetadata, LoginRequest, SetupDatabase, SetupDatabaseDetails, SetupPrefs, SetupRequest,
    SetupUser, Table, TableField,
};
use mz_ore::retry::Retry;
use mz_ore::task;

const DUMMY_EMAIL: &str = "ci@materialize.io";
const DUMMY_PASSWORD: &str = "dummydummy1";

async fn connect_materialized() -> Result<tokio_postgres::Client, anyhow::Error> {
    Retry::default()
        .retry_async(|_| async {
            let res = TcpStream::connect("materialized:6875").await;
            if let Err(e) = &res {
                debug!("error connecting to materialized: {}", e);
            }
            res
        })
        .await?;
    let (client, conn) = tokio_postgres::connect(
        "postgres://materialize@materialized:6875/materialize",
        NoTls,
    )
    .await
    .context("failed connecting to materialized")?;
    task::spawn(|| "metabase_smoketest_mz", async {
        if let Err(e) = conn.await {
            panic!("postgres connection error: {}", e);
        }
    });
    Ok(client)
}

async fn connect_metabase() -> Result<mz_metabase::Client, anyhow::Error> {
    let mut client = mz_metabase::Client::new("http://metabase:3000")
        .context("failed creating metabase client")?;
    let setup_token = Retry::default()
        .max_duration(Duration::from_secs(30))
        .retry_async(|_| async {
            let res = client.session_properties().await;
            if let Err(e) = &res {
                debug!("error connecting to metabase: {}", e);
            }
            res.map(|res| res.setup_token)
        })
        .await?;
    let session_id = match setup_token {
        None => {
            let req = LoginRequest {
                username: DUMMY_EMAIL.into(),
                password: DUMMY_PASSWORD.into(),
            };
            client.login(&req).await?.id
        }
        Some(setup_token) => {
            let req = &SetupRequest {
                allow_tracking: false,
                database: SetupDatabase {
                    engine: "postgres".into(),
                    name: "Materialize".into(),
                    details: SetupDatabaseDetails {
                        host: "materialized".into(),
                        port: 6875,
                        dbname: "materialize".into(),
                        user: "materialize".into(),
                    },
                },
                token: setup_token,
                prefs: SetupPrefs {
                    site_name: "Materialize".into(),
                },
                user: SetupUser {
                    email: DUMMY_EMAIL.into(),
                    first_name: "Materialize".into(),
                    last_name: "CI".into(),
                    password: DUMMY_PASSWORD.into(),
                    site_name: "Materialize".into(),
                },
            };
            client.setup(req).await?.id
        }
    };
    client.set_session_id(session_id);
    Ok(client)
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    mz_ore::test::init_tracing().await;

    let pgclient = connect_materialized().await?;
    pgclient
        .batch_execute(
            "CREATE OR REPLACE MATERIALIZED VIEW orders (id, date, quantity, total) AS
             VALUES (1, '2020-01-03'::date, 6, 10.99), (2, '2020-01-04'::date, 4, 7.48)",
        )
        .await?;

    let metabase_client = connect_metabase().await?;

    let databases = metabase_client.databases().await?;
    debug!("Databases: {:#?}", databases);

    let database_names: Vec<_> = databases.iter().map(|d| &d.name).sorted().collect();
    assert_eq!(database_names, &["Materialize", "Sample Dataset"]);

    let mzdb = databases.iter().find(|d| d.name == "Materialize").unwrap();
    let expected_metadata = DatabaseMetadata {
        tables: vec![Table {
            name: "orders".into(),
            schema: "public".into(),
            fields: vec![
                TableField {
                    name: "date".into(),
                    database_type: "date".into(),
                    base_type: "type/Date".into(),
                    special_type: None,
                },
                TableField {
                    name: "id".into(),
                    database_type: "int4".into(),
                    base_type: "type/Integer".into(),
                    special_type: None,
                },
                TableField {
                    name: "quantity".into(),
                    database_type: "int4".into(),
                    base_type: "type/Integer".into(),
                    special_type: None,
                },
                TableField {
                    name: "total".into(),
                    database_type: "numeric".into(),
                    base_type: "type/Decimal".into(),
                    special_type: None,
                },
            ],
        }],
    };
    // The database sync happens asynchronously and the API doesn't appear to
    // expose when it is complete, so just retry a few times waiting for the
    // metadata we expect.
    Retry::default()
        .retry_async(|_| async {
            let mut metadata = metabase_client.database_metadata(mzdb.id).await?;
            metadata.tables.retain(|t| t.schema == "public");
            metadata.tables.sort_by(|a, b| a.name.cmp(&b.name));
            for t in &mut metadata.tables {
                t.fields.sort_by(|a, b| a.name.cmp(&b.name));
            }
            debug!("Materialize database metadata: {:#?}", metadata);
            if expected_metadata != metadata {
                bail!(
                    "metadata did not match\nexpected:\n{:#?}\nactual:\n{:#?}",
                    expected_metadata,
                    metadata,
                );
            }
            Ok(())
        })
        .await?;

    println!("OK");
    Ok(())
}
