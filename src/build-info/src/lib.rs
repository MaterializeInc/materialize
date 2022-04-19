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

#[macro_export]
/// Generate an appropriate `BuildInfo` instance.
/// Use of this macro requires that the user
/// write out a `TARGET_TRIPLE` environment variable in its build script;
/// see `src/materialized/build/main.rs` for an example.
macro_rules! build_info {
    () => {
        BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
            sha: run_command_str!(
                "sh",
                "-c",
                r#"if [ -n "$MZ_DEV_BUILD_SHA" ]; then
                       echo "$MZ_DEV_BUILD_SHA"
                   else
                       # Unfortunately we need to suppress error messages from `git`, as
                       # run_command_str will display no error message at all if we print
                       # more than one line of output to stderr.
                       git rev-parse --verify HEAD 2>/dev/null || {
                           printf "error: unable to determine Git SHA; " >&2
                           printf "either build from working Git clone " >&2
                           printf "(see https://materialize.com/docs/install/#build-from-source), " >&2
                           printf "or specify SHA manually by setting MZ_DEV_BUILD_SHA environment variable" >&2
                           exit 1
                       }
                   fi"#
            ),
            time: compile_time_run::run_command_str!("date", "-u", "+%Y-%m-%dT%H:%M:%SZ"),
            target_triple: env!("TARGET_TRIPLE"),
        }
    }
}
