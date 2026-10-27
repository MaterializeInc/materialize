// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generate data contracts command — resolves declared dependencies from
//! `project.toml` into a `types.lock` file.
//!
//! Reads the `dependencies` list from `project.toml` and queries the target
//! database for each declared object's column schema and kind. Also discovers
//! `CREATE TABLE FROM SOURCE` tables via full project compilation (a
//! lightweight syntax-only path is a planned follow-up). Hard-errors if any
//! declared dependency does not exist in the target database.

use std::time::Instant;

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::Client;
use crate::config::Settings;
use crate::project::ir::object_id::ObjectId;

/// Resolve declared dependencies into a types.lock file.
pub async fn run(settings: &Settings) -> Result<(), CliError> {
    let directory = &settings.directory;

    let start = Instant::now();
    let canonical = directory.canonicalize();
    let shown = canonical.as_deref().unwrap_or(directory);
    progress::action("Locking", &shown.display().to_string());

    // Discover source tables via compilation (pragmatic first step;
    // lightweight syntax-only extraction is a follow-up optimization)
    let source_tables = discover_source_tables(settings)?;

    if settings.dependencies.is_empty() && source_tables.is_empty() {
        progress::finished("lock", start.elapsed());
        return Ok(());
    }

    // Connect to the database
    let profile = settings.connection();
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    // Resolve types for declared dependencies and source tables in one
    // catalog query. Missing objects (those not in the target catalog) are
    // surfaced as DeclaredDependenciesMissing with a user-friendly hint.
    let declared: Vec<ObjectId> = settings.dependencies.iter().cloned().collect();
    let (types, missing) = client
        .types()
        .query_types_for_objects(&declared, &source_tables)
        .await
        .map_err(CliError::Connection)?;

    if !missing.is_empty() {
        return Err(CliError::DeclaredDependenciesMissing { missing });
    }

    types.write_types_lock(directory)?;

    progress::finished(
        &format!("locking {} objects", types.tables.len()),
        start.elapsed(),
    );

    Ok(())
}

/// Discover CREATE TABLE FROM SOURCE tables by compiling the project.
fn discover_source_tables(settings: &Settings) -> Result<Vec<ObjectId>, CliError> {
    let fs = crate::fs::FileSystem::new();
    let planned = crate::project::plan_sync(
        &fs,
        &settings.directory,
        settings.profile_name(),
        settings.profile_suffix(),
        settings.variables(),
    )?;
    Ok(planned.get_tables_from_source().collect())
}
