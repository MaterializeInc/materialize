// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Build script to regenerate proto serializing code for tests
//! To run invoke `MZ_GENERATE_PROTO=1 cargo build`

use std::env;
use std::fs;

fn main() {
    let out_dir = "protobuf/gen";
    let input = &["resources/simple.proto", "resources/billing.proto"];

    for fname in input {
        println!("cargo:rerun-if-changed={}", fname);
    }
    let env_var = "MZ_GENERATE_PROTO";
    println!("cargo:rerun-if-env-changed={}", env_var);
    if env::var_os(env_var).is_none() {
        return;
    }

    if !fs::metadata(out_dir).map(|md| md.is_dir()).unwrap_or(false) {
        panic!(
            "out directory for proto generation does not exist: {}",
            out_dir
        );
    }
    for fname in input {
        if !fs::metadata(fname).map(|md| md.is_file()).unwrap_or(false) {
            panic!("proto schema file does not exist: {}", fname);
        }
    }

    protoc_rust::run(protoc_rust::Args {
        out_dir,
        input,
        includes: &[],
        customize: Default::default(),
    })
    .expect("protoc");
}
