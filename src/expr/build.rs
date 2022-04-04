// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() {
    prost_build::Config::new()
        .extern_path(".adt.array", "::mz_repr::proto::adt::array")
        .compile_protos(&["id.proto"], &["src/proto", "../repr/src/proto"])
        .unwrap();
}
