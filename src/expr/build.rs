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
        .extern_path(".adt.numeric", "::mz_repr::proto::adt::numeric")
        .extern_path(".strconv", "::mz_repr::proto::strconv")
        .compile_protos(
            &["id.proto", "scalar.proto", "scalar/func.proto"],
            &["src/proto", "../repr/src/proto"],
        )
        .unwrap();
}
