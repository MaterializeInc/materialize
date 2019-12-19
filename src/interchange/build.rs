// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//
// Build script to regenerate proto serializing code for tests
// To run invoke MATERIALIZE_GENERATE_PROTO=1 cargo build

extern crate protoc_rust;

use std::env;

use protoc_rust::Customize;

fn main() {
    let generate_protos = env::var("MATERIALIZE_GENERATE_PROTO")
        .map(|v| v == "1")
        .unwrap_or(false);

    if !generate_protos {
        return;
    }

    protoc_rust::run(protoc_rust::Args {
        out_dir: "protobuf/test/",
        input: &["testdata/test-proto-schemas.proto"],
        includes: &[],
        customize: Customize {
            ..Default::default()
        },
    })
    .expect("protoc");
}
