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
        .extern_path(".mz_expr.scalar", "::mz_expr")
        .extern_path(".mz_postgres_util.desc", "::mz_postgres_util::desc")
        .extern_path(".mz_mysql_util", "::mz_mysql_util")
        .extern_path(".mz_sql_server_util", "::mz_sql_server_util")
        .extern_path(".mz_repr.row", "::mz_repr")
        .compile_protos(
            &[
                "storage-types/src/errors.proto",
                "storage-types/src/sources.proto",
                "storage-types/src/sources/kafka.proto",
                "storage-types/src/sources/mysql.proto",
                "storage-types/src/sources/postgres.proto",
                "storage-types/src/sources/sql_server.proto",
                "storage-types/src/sources/load_generator.proto",
            ],
            &[PathBuf::from(".."), mz_build_tools::protoc_include()],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
