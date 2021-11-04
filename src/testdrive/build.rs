// Copyright Materialize, Inc. and contributors. All rights reserved.
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
        .input("src/format/protobuf/no_messages.proto")
        .input("src/format/protobuf/imported.proto")
        .input("src/format/protobuf/billing.proto")
        .input("src/format/protobuf/recursive.proto")
        .input("src/format/protobuf/simple.proto")
        .input("src/format/protobuf/nested.proto")
        .input("src/format/protobuf/well_known_imports.proto")
        .build_script_exec();
}
