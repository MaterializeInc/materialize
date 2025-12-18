// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod execute;
mod query;

pub use execute::run_execute;
pub use query::run_query;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use duckdb::Connection;

use crate::action::State;

/// Gets or creates a DuckDB connection with the given name.
pub(crate) async fn get_or_create_connection(
    state: &mut State,
    name: String,
) -> Result<Arc<Mutex<Connection>>, anyhow::Error> {
    if let Some(conn) = state.duckdb_clients.get(&name) {
        return Ok(Arc::clone(conn));
    }

    let temp_path = state.temp_path.clone();
    let conn = create_connection(temp_path).await?;
    let conn = Arc::new(Mutex::new(conn));
    state.duckdb_clients.insert(name, Arc::clone(&conn));
    Ok(conn)
}

async fn create_connection(temp_path: PathBuf) -> Result<Connection, anyhow::Error> {
    mz_ore::task::spawn_blocking(
        || "duckdb_connect".to_string(),
        move || {
            let conn =
                Connection::open_in_memory().context("opening in-memory DuckDB connection")?;

            let ext_dir = temp_path.join("duckdb_extensions");
            conn.execute(
                &format!("SET extension_directory = '{}';", ext_dir.display()),
                [],
            )
            .context("setting extension_directory")?;

            conn.execute_batch("INSTALL iceberg; LOAD iceberg;")
                .context("installing iceberg extension")?;
            conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
                .context("installing httpfs extension")?;

            Ok::<_, anyhow::Error>(conn)
        },
    )
    .await
}
