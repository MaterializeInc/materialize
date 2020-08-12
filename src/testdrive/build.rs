// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() {
    protoc::Protoc::new()
        .serde(true)
        .include("src/format/protobuf")
        .input("src/format/protobuf/billing.proto")
        .input("src/format/protobuf/simple.proto")
        .build_script_exec();
}
