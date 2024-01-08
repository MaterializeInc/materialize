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
        .bytes(["."])
        .extern_path(".mz_persist_client.batch", "::mz_persist_client::batch")
        .compile_protos(&["persist-txn/src/txns.proto"], &[".."])
        .unwrap_or_else(|e| panic!("{e}"))
}
