// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use assert_cmd::Command;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("materialized").unwrap();
    cmd.env_clear().timeout(Duration::from_secs(10));
    cmd
}

/// This test seems a bit tautological, but it protects against Clap defaults
/// changing and overwriting our custom version output.
#[test]
fn test_version() {
    // We don't make assertions about the build SHA because caching in CI can
    // cause the test binary and `materialized` to have different embedded SHAs.
    let expected_version = materialized::BUILD_INFO.version;
    assert!(!expected_version.is_empty());
    cmd()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicates::str::starts_with(format!(
            "materialized v{}",
            expected_version
        )));
}
