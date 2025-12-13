// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use anyhow::Context;
use duckdb::Connection;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_connect(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;

    cmd.args.done()?;

    // Get temp directory for DuckDB extensions
    let temp_path = state.temp_path.clone();

    let conn = mz_ore::task::spawn_blocking(
        || "duckdb_connect".to_string(),
        move || {
            // Create in-memory DuckDB connection
            let conn =
                Connection::open_in_memory().context("opening in-memory DuckDB connection")?;

            // Set extension directory to a writable location (needed in Docker containers)
            let ext_dir = temp_path.join("duckdb_extensions");
            conn.execute(
                &format!("SET extension_directory = '{}';", ext_dir.display()),
                [],
            )
            .context("setting extension_directory")?;

            // Install and load required extensions
            conn.execute_batch("INSTALL iceberg; LOAD iceberg;")
                .context("installing iceberg extension")?;
            conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
                .context("installing httpfs extension")?;

            Ok::<_, anyhow::Error>(conn)
        },
    )
    .await?;

    state
        .duckdb_clients
        .insert(name, Arc::new(Mutex::new(conn)));
    Ok(ControlFlow::Continue)
}
