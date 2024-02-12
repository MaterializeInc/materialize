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

    prost_build::Config::new()
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
            &[".."],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
