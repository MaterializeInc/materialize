// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

fn main() {
    prost_build::Config::new()
        .protoc_executable(mz_build_tools::protoc())
        .type_attribute(".", "#[derive(serde::Serialize)]")
        .type_attribute(".mz_persist_types.arrow", "#[derive(serde::Deserialize)]")
        .btree_map(["."])
        .bytes([".mz_persist_types.arrow.Buffer"])
        .compile_protos(
            &[
                "persist-types/src/arrow.proto",
                "persist-types/src/stats.proto",
            ],
            &[PathBuf::from(".."), mz_build_tools::protoc_include()],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
