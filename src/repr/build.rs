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
        .btree_map(["."])
        .extern_path(".mz_proto", "::mz_proto")
        .bytes([".mz_repr.row.ProtoDatum.bytes"])
        .compile_protos(
            &[
                "repr/src/adt/array.proto",
                "repr/src/adt/char.proto",
                "repr/src/adt/date.proto",
                "repr/src/adt/interval.proto",
                "repr/src/adt/mz_acl_item.proto",
                "repr/src/adt/numeric.proto",
                "repr/src/adt/range.proto",
                "repr/src/adt/timestamp.proto",
                "repr/src/adt/varchar.proto",
                "repr/src/catalog_item_id.proto",
                "repr/src/relation_and_scalar.proto",
                "repr/src/role_id.proto",
                "repr/src/row.proto",
                "repr/src/strconv.proto",
                "repr/src/timestamp.proto",
            ],
            &[PathBuf::from(".."), mz_build_tools::protoc_include()],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
