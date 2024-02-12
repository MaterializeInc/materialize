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
        .type_attribute(".", "#[derive(serde::Serialize)]")
        .btree_map(["."])
        .compile_protos(&["persist-types/src/stats.proto"], &[".."])
        .unwrap_or_else(|e| panic!("{e}"))
}
