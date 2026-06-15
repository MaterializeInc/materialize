// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQLite-backed compiler cache shared by the writer and the reader.
//!
//! The cache is a single SQLite file per profile namespace. [`BuildArtifact`]
//! is the read/write handle used during compilation; [`ProjectCache`] is the
//! read-only handle used by downstream consumers (the LSP, in particular). Both
//! agree on the file location, schema version, and error vocabulary defined
//! here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub(crate) mod build_artifact;
pub(crate) mod project_cache;
pub(crate) mod schema;

pub(crate) use build_artifact::BuildArtifact;
pub(crate) use project_cache::ProjectCache;

pub(crate) const DB_FILE: &str = "build_artifact.db";

/// Compute the path to the compiler cache database for a given project and profile.
pub(crate) fn db_path(
    root: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> PathBuf {
    root.join(crate::types::BUILD_DIR)
        .join(super::COMPILER_DIR)
        .join(super::profile_namespace(profile, profile_suffix, variables))
        .join(DB_FILE)
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("failed to create compiler cache directory: {path}")]
    DirectoryCreationFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open build artifact database: {path}")]
    DatabaseOpenFailed {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to operate on build artifact database: {path}")]
    DatabaseOperationFailed {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to read cached source file: {path}")]
    FileReadFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
