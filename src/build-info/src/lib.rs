// Copyright Materialize, Inc. and contributors. All rights reserved.
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    version: "0.0.0+dummy",
    sha: "0000000000000000000000000000000000000000",
    time: "",
    target_triple: "",
};

impl BuildInfo {
    /// Constructs a human-readable version string.
    pub fn human_version(&self) -> String {
        format!("v{} ({})", self.version, &self.sha[..9])
    }

    /// Returns the version as a rich [semantic version][semver].
    ///
    /// This method is only available when the `semver` feature is active.
    ///
    /// # Panics
    ///
    /// Panics if the `version` field is not a valid semantic version.
    ///
    /// [semver]: https://semver.org
    #[cfg(feature = "semver")]
    pub fn semver_version(&self) -> semver::Version {
        self.version
            .parse()
            .expect("build version is not valid semver")
    }
}

pub const fn make_build_info(
    version: &'static str,
    sha: &'static str,
    time: &'static str,
    target_triple: &'static str,
) -> BuildInfo {
    BuildInfo {
        version,
        sha,
        time,
        target_triple,
    }
}
