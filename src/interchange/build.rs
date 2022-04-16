// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::fs::File;
use std::path::PathBuf;
use std::process::Command;

const PROTOS: &[&str] = &["benchmark.proto"];
const INCLUDE_DIRS: &[&str] = &["testdata"];

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    // Compile protobufs to Rust.
    prost_build::Config::new()
        .include_file("mod.rs")
        .compile_protos(PROTOS, INCLUDE_DIRS)
        .unwrap();

    // Compile protobufs to a file descriptor set.
    let mut cmd = Command::new(prost_build::protoc());
    cmd.arg("--include_imports")
        .arg("-o")
        .arg(out_dir.join("file_descriptor_set.pb"));
    for dir in INCLUDE_DIRS {
        cmd.arg("-I");
        cmd.arg(dir);
    }
    for proto in PROTOS {
        cmd.arg(proto);
    }
    let status = cmd.status().expect("building file descriptor set failed");
    if !status.success() {
        panic!("building file descriptor set failed with status {}", status);
    }

    // Work around a prost bug in which the module index expects to include a
    // file for the well-known types.
    File::create(out_dir.join("google.protobuf.rs")).unwrap();
}
