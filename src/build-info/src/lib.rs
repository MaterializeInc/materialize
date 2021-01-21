// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metadata about a Materialize build.
//!
//! These types are located in a dependency-free crate so they can be used
//! from any layer of the stack.

/// Build information.
#[derive(Debug, Clone)]
pub struct BuildInfo {
    /// The version number of the build.
    pub version: &'static str,
    /// The 40-character SHA-1 hash identifying the Git commit of the build.
    pub sha: &'static str,
    /// The time of the build in UTC as an ISO 8601-compliant string.
    pub time: &'static str,
    /// The target triple of the platform.
    pub target_triple: &'static str,
}

/// Dummy build information.
///
/// Intended for use in contexts where getting the correct build information is
/// impossible or unnecessary, like in tests.
pub const DUMMY_BUILD_INFO: BuildInfo = BuildInfo {
    version: "",
    sha: "",
    time: "",
    target_triple: "",
};

impl BuildInfo {
    /// Constructs a human-readable version string.
    pub fn human_version(&self) -> String {
        format!("v{} ({})", self.version, &self.sha[..9])
    }
}
