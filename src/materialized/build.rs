// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::process;

use chrono::Utc;
use duct::cmd;

fn main() {
    let sha = cmd!("git", "rev-parse", "--verify", "HEAD")
        .read()
        .unwrap_or_else(|err| {
            eprintln!("unable to determine build SHA: {}", err);
            process::exit(1);
        });
    let build_time = Utc::now().format("%Y-%m-%d %H:%M Z");
    println!("cargo:rustc-env=MZ_GIT_SHA={}", sha);
    println!("cargo:rustc-env=MZ_BUILD_TIME={}", build_time);
}
