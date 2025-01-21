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
    const ATTR: &str = "#[derive(Eq, PartialOrd, Ord, ::serde::Serialize, ::serde::Deserialize, ::proptest_derive::Arbitrary)]";

    let mut config = prost_build::Config::new();
    config
        .protoc_executable(mz_build_tools::protoc())
        .btree_map(["."]);

    tonic_build::configure()
        // Enabling `emit_rerun_if_changed` will rerun the build script when
        // anything in the include directory (..) changes. This causes quite a
        // bit of spurious recompilation, so we disable it. The default behavior
        // is to re-run if any file in the crate changes; that's still a bit too
        // broad, but it's better.
        .emit_rerun_if_changed(false)
        .message_attribute(".mz_storage_types.collections", ATTR)
        .enum_attribute("GlobalId.value", ATTR)
        .extern_path(".mz_ccsr.config", "::mz_ccsr")
        .extern_path(".mz_dyncfg", "::mz_dyncfg")
        .extern_path(".mz_expr.id", "::mz_expr")
        .extern_path(".mz_expr.linear", "::mz_expr")
        .extern_path(".mz_expr.relation", "::mz_expr")
        .extern_path(".mz_expr.scalar", "::mz_expr")
        .extern_path(".mz_repr.refresh_schedule", "::mz_repr::refresh_schedule")
        .extern_path(".mz_kafka_util.addr", "::mz_kafka_util")
        .extern_path(".mz_postgres_util.desc", "::mz_postgres_util::desc")
        .extern_path(".mz_mysql_util", "::mz_mysql_util")
        .extern_path(".mz_repr.adt.regex", "::mz_repr::adt::regex")
        .extern_path(".mz_repr.antichain", "::mz_repr::antichain")
        .extern_path(".mz_repr.global_id", "::mz_repr::global_id")
        .extern_path(".mz_repr.catalog_item_id", "::mz_repr::catalog_item_id")
        .extern_path(".mz_orchestrator", "::mz_orchestrator")
        .extern_path(".mz_pgcopy.copy", "::mz_pgcopy")
        .extern_path(".mz_postgres_util.tunnel", "::mz_postgres_util::tunnel")
        .extern_path(".mz_proto", "::mz_proto")
        .extern_path(".mz_repr.relation_and_scalar", "::mz_repr")
        .extern_path(".mz_repr.row", "::mz_repr")
        .extern_path(".mz_repr.url", "::mz_repr::url")
        .extern_path(".mz_rocksdb_types", "::mz_rocksdb_types")
        .extern_path(".mz_cluster_client", "::mz_cluster_client")
        .extern_path(".mz_tracing", "::mz_tracing")
        .extern_path(".mz_service", "::mz_service")
        .compile_with_config(
            config,
            &[
                "storage-types/src/controller.proto",
                "storage-types/src/collections.proto",
                "storage-types/src/connections/aws.proto",
                "storage-types/src/connections/string_or_secret.proto",
                "storage-types/src/connections.proto",
                "storage-types/src/errors.proto",
                "storage-types/src/instances.proto",
                "storage-types/src/oneshot_sources.proto",
                "storage-types/src/parameters.proto",
                "storage-types/src/sinks.proto",
                "storage-types/src/sources.proto",
                "storage-types/src/sources/encoding.proto",
                "storage-types/src/sources/envelope.proto",
                "storage-types/src/sources/kafka.proto",
                "storage-types/src/sources/mysql.proto",
                "storage-types/src/sources/postgres.proto",
                "storage-types/src/sources/load_generator.proto",
                "storage-types/src/time_dependence.proto",
            ],
            &[PathBuf::from(".."), mz_build_tools::protoc_include()],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
