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
    // Existing prost-only compilation for persist.proto (no gRPC services).
    prost_build::Config::new()
        .protoc_executable(mz_build_tools::protoc())
        .btree_map(["."])
        .type_attribute(
            ".mz_persist.gen.persist.ProtoColumnarRecords",
            "#[derive(serde::Serialize)]",
        )
        .bytes([".mz_persist.gen.persist.ProtoColumnarRecords"])
        .extern_path(".mz_persist_types.arrow", "::mz_persist_types::arrow")
        .compile_protos(
            &["persist/src/persist.proto"],
            &[PathBuf::from(".."), mz_build_tools::protoc_include()],
        )
        .unwrap_or_else(|e| panic!("{e}"));

    // Tonic + prost compilation for consensus_service.proto (gRPC service).
    let mut config = prost_build::Config::new();
    config.protoc_executable(mz_build_tools::protoc());
    tonic_prost_build::configure()
        .emit_rerun_if_changed(false)
        .compile_with_config(
            config,
            &[PathBuf::from("persist/src/consensus_service.proto")],
            &[PathBuf::from(".."), mz_build_tools::protoc_include()],
        )
        .unwrap_or_else(|e| panic!("{e}"));
}
