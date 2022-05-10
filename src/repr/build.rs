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
        .compile_protos(
            &[
                "repr/src/chrono.proto",
                "repr/src/global_id.proto",
                "repr/src/proto.proto",
                "repr/src/row.proto",
                "repr/src/strconv.proto",
                "repr/src/relation_and_scalar.proto",
                "repr/src/adt/array.proto",
                "repr/src/adt/char.proto",
                "repr/src/adt/datetime.proto",
                "repr/src/adt/interval.proto",
                "repr/src/adt/numeric.proto",
                "repr/src/adt/regex.proto",
                "repr/src/adt/varchar.proto",
            ],
            &[".."],
        )
        .unwrap();
}
