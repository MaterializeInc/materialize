// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

const STASH_DB_NAME: &str = "stash";
const STORAGE_DB_NAME: &str = "storage";

/// Creates a temporary copy of Materialize's mzdata databases
///
/// This is useful because running validations against the catalog can conflict with the running
/// Materialize instance. Therefore it's better to run validations on a copy of the catalog.
pub fn mzdata_copy(catalog_path: &PathBuf) -> Result<TempDir, anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    fs::copy(
        catalog_path.join(STORAGE_DB_NAME),
        temp_dir.path().join(STORAGE_DB_NAME),
    )?;
    Ok(temp_dir)
}

/// Creates a temporary copy of Materialize's mzdata's sqlite stash database
///
/// This is useful because running validations against the catalog can conflict with the running
/// Materialize instance. Therefore it's better to run validations on a copy of the catalog.
pub fn catalog_copy(catalog_path: &PathBuf) -> Result<TempDir, anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    fs::copy(
        catalog_path.join(STASH_DB_NAME),
        temp_dir.path().join(STASH_DB_NAME),
    )?;
    Ok(temp_dir)
}
