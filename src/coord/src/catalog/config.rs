// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

/// Configures a catalog.
#[derive(Debug)]
pub struct Config<'a> {
    /// The path to the catalog on disk.
    ///
    /// If set to `None`, indicates that an ephemeral in-memory catalog should
    /// be used.
    pub path: Option<&'a Path>,
    /// Whether to enable experimental mode.
    pub experimental_mode: Option<bool>,
    /// Whether to enable logging sources and the views that depend upon them.
    pub enable_logging: bool,
}
