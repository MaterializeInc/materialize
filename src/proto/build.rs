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
        .btree_map(["."])
        .type_attribute(
            ".mz_proto.ProtoDuration",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, proptest_derive::Arbitrary)]",
        )
        .compile_protos(
            &[
                "proto/src/chrono.proto",
                "proto/src/proto.proto",
                "proto/src/tokio_postgres.proto",
            ],
            &[PathBuf::from(".."), mz_build_tools::protoc_include()],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
