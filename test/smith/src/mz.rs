// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

use test_util::mz_client;

#[derive(Clone, Debug, Serialize)]
pub struct Table {
    name: String,
    can_insert: bool,
    columns: Vec<Column>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Column {
    pub data_type: String,
    pub name: String,
}

pub async fn show_views(mz_client: &Client) -> Result<Vec<Table>> {
    let mut views = vec![];
    for row in mz_client.query("SHOW VIEWS", &[]).await? {
        let name = row.get(0);
        let columns = print_columns(&mz_client, row.get(0)).await?;
        views.push(Table {
            name,
            can_insert: false,
            columns,
        })
    }
    Ok(views)
}

async fn print_columns(mz_client: &Client, view: &str) -> Result<Vec<Column>> {
    let mut res = Vec::new();
    let q = format!("SHOW COLUMNS IN {}", view);
    for row in mz_client.query(&*q, &[]).await? {
        let name: String = row.get(0);

        // TODO cleaner way to map our types to types Smith understands
        res.push(Column {
            data_type: "int".to_string(),
            name,
        });
    }

    Ok(res)
}

pub async fn seed_views(mz_client: &Client) -> Result<()> {
    let views = vec![
        "CREATE MATERIALIZED VIEW view1 (a, b) AS VALUES (1, 2), (3, 4)",
        "CREATE MATERIALIZED VIEW view2 (a) AS VALUES ('foo'), ('bar')",
    ];

    for v in views.iter() {
        mz_client::execute(&mz_client, v).await?;
    }

    Ok(())
}
