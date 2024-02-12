// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;

fn main() {
    env::set_var("PROTOC", protobuf_src::protoc());

    let mut config = prost_build::Config::new();
    config
        .btree_map(["."])
        // Note(parkertimmerman): We purposefully omit `.mz_persist_client.internal.diff` from here
        // because we want to use `bytes::Bytes` for the types in that package, and `Bytes` doesn't
        // implement `serde::Serialize`
        .type_attribute(
            ".mz_persist_client.internal.state",
            "#[derive(serde::Serialize)]",
        )
        .type_attribute(
            ".mz_persist_client.internal.state.ProtoHollowBatch",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(
            ".mz_persist_client.internal.state.ProtoHollowBatchPart",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(
            ".mz_persist_client.internal.state.ProtoU64Description",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(
            ".mz_persist_client.internal.state.ProtoU64Antichain",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(
            ".mz_persist_client.batch",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(".", "#[allow(missing_docs)]")
        .btree_map(["."])
        .bytes([
            ".mz_persist_client.internal.diff.ProtoStateFieldDiffs",
            ".mz_persist_client.internal.state.ProtoHollowBatchPart",
            ".mz_persist_client.internal.state.ProtoVersionedData",
            ".mz_persist_client.internal.service.ProtoPushDiff",
        ]);

    tonic_build::configure()
        // Enabling `emit_rerun_if_changed` will rerun the build script when
        // anything in the include directory (..) changes. This causes quite a
        // bit of spurious recompilation, so we disable it. The default behavior
        // is to re-run if any file in the crate changes; that's still a bit too
        // broad, but it's better.
        .emit_rerun_if_changed(false)
        .extern_path(".mz_persist_types", "::mz_persist_types")
        .extern_path(".mz_proto", "::mz_proto")
        .compile_with_config(
            config,
            &[
                "persist-client/src/batch.proto",
                "persist-client/src/internal/service.proto",
                "persist-client/src/internal/state.proto",
                "persist-client/src/internal/diff.proto",
            ],
            &[".."],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
