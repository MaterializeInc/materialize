// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::process;

use duct::cmd;

fn main() {
    let sha = cmd!("git", "rev-parse", "--verify", "HEAD")
        .read()
        .unwrap_or_else(|err| {
            eprintln!("unable to determine build SHA: {}", err);
            process::exit(1);
        });
    println!("cargo:rustc-env=MZ_GIT_SHA={}", sha);
}
