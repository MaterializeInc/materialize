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

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    prost_build::Config::new()
        .include_file("mod.rs")
        .file_descriptor_set_path(out_dir.join("file_descriptor_set.pb"))
        .compile_protos(&["billing.proto"], &["resources"])
        .unwrap();

    // Work around a prost bug in which the module index expects to include a
    // file for the well-known types.
    File::create(out_dir.join("google.protobuf.rs")).unwrap();
}
