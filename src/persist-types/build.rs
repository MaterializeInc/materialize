// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() {
    std::env::set_var("PROTOC", mz_build_tools::protoc());
    std::env::set_var("PROTOC_INCLUDE", mz_build_tools::protoc_include());

    prost_build::Config::new()
        .type_attribute(".", "#[derive(serde::Serialize)]")
        .btree_map(["."])
        .bytes([".mz_persist_types.arrow.Buffer"])
        .compile_protos(
            &[
                "persist-types/src/arrow.proto",
                "persist-types/src/stats.proto",
            ],
            &[".."],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
