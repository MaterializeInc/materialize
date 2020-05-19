// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    let mut cmd = Command::cargo_bin("materialized").unwrap();
    cmd.env_clear()
        .env("MZ_DEV", "1")
        .timeout(Duration::from_secs(10));
    cmd
}

#[test]
fn test_threads() {
    let assert_fail = |cmd: &mut Command| {
        cmd.assert().failure().stderr(predicate::str::starts_with(
            "materialized: '--threads' must be specified and greater than 0",
        ))
    };
    assert_fail(&mut cmd());
    assert_fail(cmd().arg("-w0"));
    assert_fail(cmd().env("MZ_THREADS", "0"));

    cmd()
        .arg("-w")
        .arg("-1")
        .assert()
        .failure()
        .stderr(predicate::str::starts_with(
            "materialized: invalid digit found in string",
        ));

    cmd()
        .env("MZ_THREADS", OsStr::from_bytes(&[0xc2, 0xc2]))
        .assert()
        .failure()
        .stderr(predicate::str::starts_with(
            "materialized: non-unicode character found in MZ_THREADS",
        ));

    // NOTE: we don't test the successful case, where `MZ_THREADS` or `-w` is
    // specified correctly, because it's presently hard to check if Materialized
    // has started correctly, since it runs forever. The success code path is
    // well exercised by integration tests, so it's not a big deal.
}
