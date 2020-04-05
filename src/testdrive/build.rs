// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Build script to regenerate proto serializing code for tests
//! To run invoke `MZ_GENERATE_PROTO=1 cargo build`

use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;

fn main() {
    println!("cargo:rustc-cfg=feature=\"with-serde\"");

    let out_dir = "src/format/protobuf/gen";
    let input = &[
        "src/format/protobuf/billing.proto",
        "src/format/protobuf/simple.proto",
    ];

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

    protoc_rust::Codegen::new()
        .out_dir(out_dir)
        .inputs(input)
        .customize(protoc_rust::Customize {
            serde_derive: Some(true),
            ..Default::default()
        })
        .run()
        .expect("protoc");

    // TODO(benesch): this missing import is fixed in the unreleased v3.0 of
    // rust-protobuf.
    for entry in fs::read_dir(out_dir).unwrap() {
        let entry = entry.unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(entry.path())
            .unwrap();
        writeln!(file, "{}", "use serde::{Deserialize, Serialize};").unwrap();
    }
}
