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
        .extern_path(".mz_repr.adt.array", "::mz_repr::adt::array")
        .extern_path(".mz_repr.adt.numeric", "::mz_repr::adt::numeric")
        .extern_path(".mz_repr.chrono", "::mz_repr::chrono")
        .extern_path(".mz_repr.global_id", "::mz_repr::global_id")
        .extern_path(".mz_repr.relation_and_scalar", "::mz_repr")
        .extern_path(".mz_repr.strconv", "::mz_repr::strconv")
        .compile_protos(
            &[
                "expr/src/id.proto",
                "expr/src/scalar.proto",
                "expr/src/scalar/func.proto",
            ],
            &[".."],
        )
        .unwrap();
}
