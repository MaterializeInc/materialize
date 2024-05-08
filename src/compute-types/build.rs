// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;

fn main() {
    env::set_var("PROTOC", mz_build_tools::protoc());

    let mut config = prost_build::Config::new();
    config
        .btree_map(["."])
        .type_attribute(".", "#[allow(missing_docs)]");

    tonic_build::configure()
        // Enabling `emit_rerun_if_changed` will rerun the build script when
        // anything in the include directory (..) changes. This causes quite a
        // bit of spurious recompilation, so we disable it. The default behavior
        // is to re-run if any file in the crate changes; that's still a bit too
        // broad, but it's better.
        .emit_rerun_if_changed(false)
        .extern_path(".mz_ccsr.config", "::mz_ccsr")
        .extern_path(".mz_expr.id", "::mz_expr")
        .extern_path(".mz_expr.linear", "::mz_expr")
        .extern_path(".mz_repr.refresh_schedule", "::mz_repr::refresh_schedule")
        .extern_path(".mz_expr.relation", "::mz_expr")
        .extern_path(".mz_expr.scalar", "::mz_expr")
        .extern_path(".mz_kafka_util.addr", "::mz_kafka_util")
        .extern_path(".mz_persist_client", "::mz_persist_client")
        .extern_path(".mz_proto", "::mz_proto")
        .extern_path(".mz_postgres_util.desc", "::mz_postgres_util::desc")
        .extern_path(".mz_repr.adt.regex", "::mz_repr::adt::regex")
        .extern_path(".mz_repr.antichain", "::mz_repr::antichain")
        .extern_path(".mz_repr.global_id", "::mz_repr::global_id")
        .extern_path(".mz_repr.relation_and_scalar", "::mz_repr")
        .extern_path(".mz_repr.explain", "::mz_repr")
        .extern_path(".mz_repr.row", "::mz_repr")
        .extern_path(".mz_repr.url", "::mz_repr::url")
        .extern_path(".mz_cluster_client", "::mz_cluster_client")
        .extern_path(".mz_storage_types", "::mz_storage_types")
        .extern_path(".mz_tracing", "::mz_tracing")
        .extern_path(".mz_service", "::mz_service")
        .compile_with_config(
            config,
            &[
                "compute-types/src/dataflows.proto",
                "compute-types/src/sinks.proto",
                "compute-types/src/sources.proto",
                "compute-types/src/plan.proto",
                "compute-types/src/plan/flat_plan.proto",
                "compute-types/src/plan/join.proto",
                "compute-types/src/plan/reduce.proto",
                "compute-types/src/plan/threshold.proto",
                "compute-types/src/plan/top_k.proto",
            ],
            &[".."],
        )
        .unwrap_or_else(|e| panic!("{e}"));
}
