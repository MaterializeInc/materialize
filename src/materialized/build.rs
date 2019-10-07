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
    let sha = out
        .as_ref()
        .map(|sha| std::str::from_utf8(&sha.stdout[..10]).unwrap())
        .expect("able to load git sha");
    println!("cargo:rustc-env=MZ_GIT_SHA={}", sha);
}
