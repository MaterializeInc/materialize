// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    prost_build::Config::new()
        .protoc_executable(mz_build_tools::protoc())
        .include_file("benchproto.rs")
        .file_descriptor_set_path(out_dir.join("file_descriptor_set.pb"))
        .btree_map(["."])
        .compile_protos(&["interchange/testdata/benchmark.proto"], &[".."])
        .unwrap_or_else(|e| panic!("{e}"))
}
