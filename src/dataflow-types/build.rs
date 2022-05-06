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
        .extern_path(".mz_expr.linear", "::mz_expr")
        .extern_path(".mz_expr.relation", "::mz_expr")
        .extern_path(".mz_expr.scalar", "::mz_expr")
        .extern_path(".mz_expr.id", "::mz_expr")
        .extern_path(".mz_repr.global_id", "::mz_repr::global_id")
        .extern_path(".mz_repr.proto", "::mz_repr::proto")
        .extern_path(".mz_repr.row", "::mz_repr")
        .type_attribute(
            ".mz_dataflow_types.postgres_source",
            "#[derive(Eq, serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(
            &[
                "dataflow-types/src/client.proto",
                "dataflow-types/src/errors.proto",
                "dataflow-types/src/logging.proto",
                "dataflow-types/src/postgres_source.proto",
                "dataflow-types/src/plan.proto",
                "dataflow-types/src/plan/join.proto",
                "dataflow-types/src/plan/reduce.proto",
                "dataflow-types/src/plan/threshold.proto",
                "dataflow-types/src/plan/top_k.proto",
            ],
            &[".."],
        )
        .unwrap();
}
