// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::Command;

fn main() {
    let output = Command::new("sh").args(&[
        "-c",
        r#"[ -n "$MZ_DEV_BUILD_SHA" ] && echo "$MZ_DEV_BUILD_SHA" || git rev-parse --verify HEAD"#,
    ]).output().expect("Failed to run command to get build SHA.");
    if output.status.success() {
        let stdout = String::from_utf8(output.stdout)
            .expect("Command to get build SHA returned non-UTF-8 result.");
        println!("cargo:rerun-if-env-changed=MZ_DEV_BUILD_SHA");
        println!(r#"cargo:rustc-env=MZ_DEV_BUILD_SHA={}"#, stdout);
    } else {
        println!(
            r#"Failed to get build SHA! This could mean that you are building from a source tarball, which is not supported.
Please build from git, according to the instructions at https://materialize.io/docs/install/#build-from-source .

If you know what you're doing, you can attempt to build by passing a version string in the environment variable
MZ_DEV_BUILD_SHA .
"#
        );
        std::process::exit(1)
    }
}
