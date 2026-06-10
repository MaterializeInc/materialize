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
    prost_build::Config::new()
        .protoc_executable(mz_build_tools::protoc())
        .extern_path(".mz_repr.adt.array", "::mz_repr::adt::array")
        .extern_path(".mz_repr.adt.range", "::mz_repr::adt::range")
        .extern_path(".mz_repr.strconv", "::mz_repr::strconv")
        .type_attribute(".", "#[allow(missing_docs)]")
        .btree_map(["."])
        .bytes([".mz_expr.row.collection"])
        .compile_protos(
            &["expr/src/scalar.proto"],
            &[PathBuf::from(".."), mz_build_tools::protoc_include()],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
