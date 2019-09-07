// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::process::Command;

fn main() {
    let out = Command::new("git")
        .arg("rev-parse")
        .arg("--verify")
        .arg("HEAD")
        .output();
    let sha = match out {
        Ok(sha) => String::from_utf8(sha.stdout[..10].to_vec()).unwrap(),
        Err(_) => "".into(),
    };
    println!("cargo:rustc-env=MZ_GIT_SHA={}", sha);
}
