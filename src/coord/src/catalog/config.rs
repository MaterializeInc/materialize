// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::time::Duration;

use build_info::BuildInfo;

/// Configures a catalog.
#[derive(Clone, Debug)]
pub struct Config<'a> {
    /// The path to the catalog on disk.
    pub path: &'a Path,
    /// Whether to enable experimental mode.
    pub experimental_mode: Option<bool>,
    /// Whether to enable safe mode.
    pub safe_mode: bool,
    /// Whether to enable logging sources and the views that depend upon them.
    pub enable_logging: bool,
    /// Path to cache source data to disk.
    ///
    /// If set to `None`, indicates that source caching is disabled.
    pub cache_directory: Option<PathBuf>,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// The number of workers in use by the server.
    pub num_workers: usize,
    /// Timestamp frequency to use for CREATE SOURCE
    pub timestamp_frequency: Duration,
}
