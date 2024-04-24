// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() {
    std::env::set_var("PROTOC", mz_build_tools::protoc());

    prost_build::Config::new()
        .btree_map(["."])
        .extern_path(".mz_proto", "::mz_proto")
        .bytes([".mz_repr.row.ProtoDatum.bytes"])
        .compile_protos(
            &[
                "repr/src/antichain.proto",
                "repr/src/global_id.proto",
                "repr/src/row.proto",
                "repr/src/strconv.proto",
                "repr/src/relation_and_scalar.proto",
                "repr/src/role_id.proto",
                "repr/src/url.proto",
                "repr/src/timestamp.proto",
                "repr/src/adt/array.proto",
                "repr/src/adt/char.proto",
                "repr/src/adt/date.proto",
                "repr/src/adt/datetime.proto",
                "repr/src/adt/interval.proto",
                "repr/src/adt/mz_acl_item.proto",
                "repr/src/adt/numeric.proto",
                "repr/src/adt/range.proto",
                "repr/src/refresh_schedule.proto",
                "repr/src/adt/regex.proto",
                "repr/src/adt/timestamp.proto",
                "repr/src/adt/varchar.proto",
            ],
            &[".."],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
