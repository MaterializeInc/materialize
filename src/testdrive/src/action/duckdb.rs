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

            // If `MZ_DUCKDB_EXTENSION_DIR` names a directory containing
            // pre-staged `*.duckdb_extension` files, load them directly via
            // `LOAD '<path>'` rather than going through `INSTALL`.  `INSTALL`
            // would otherwise reach out to `extensions.duckdb.org` over HTTP,
            // which fails in network-sandboxed environments (the Antithesis
            // sandbox in particular).  Pre-staging happens at image-build time
            // — see `test/antithesis/workload/Dockerfile`.
            //
            // `LOAD '<path>'` per
            // https://duckdb.org/docs/current/extensions/advanced_installation_methods
            // skips the install step entirely.  Order matches the historical
            // `INSTALL; LOAD` pair so any test that previously assumed iceberg
            // loaded before httpfs still sees the same load order.
            if let Ok(ext_dir) = std::env::var("MZ_DUCKDB_EXTENSION_DIR") {
                // `avro` must precede `iceberg`: iceberg's init function
                // calls into avro and auto-INSTALLs it if not loaded.  In a
                // network-sandboxed environment that auto-INSTALL fails with
                // `Failed to download extension "avro"`, which surfaces as
                // an `iceberg_duckdb_cpp_init` exception when LOADing
                // iceberg.  Pre-load avro so iceberg's init finds it
                // already-resident in the duckdb process.
                conn.execute_batch(&format!(
                    "LOAD '{ext_dir}/avro.duckdb_extension';"
                ))
                .context("loading pre-staged avro extension")?;
                conn.execute_batch(&format!(
                    "LOAD '{ext_dir}/iceberg.duckdb_extension';"
                ))
                .context("loading pre-staged iceberg extension")?;
                conn.execute_batch(&format!(
                    "LOAD '{ext_dir}/httpfs.duckdb_extension';"
                ))
                .context("loading pre-staged httpfs extension")?;
                return Ok::<_, anyhow::Error>(conn);
            }

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
