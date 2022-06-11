// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() {
    tonic_build::configure()
        .extern_path(".mz_ccsr.config", "::mz_ccsr")
        .extern_path(".mz_expr.id", "::mz_expr")
        .extern_path(".mz_expr.linear", "::mz_expr")
        .extern_path(".mz_expr.relation", "::mz_expr")
        .extern_path(".mz_expr.scalar", "::mz_expr")
        .extern_path(".mz_kafka_util.addr", "::mz_kafka_util")
        .extern_path(".mz_persist.gen.persist", "::mz_persist::gen::persist")
        .extern_path(".mz_postgres_util.desc", "::mz_postgres_util::desc")
        .extern_path(".mz_repr.adt.regex", "::mz_repr::adt::regex")
        .extern_path(".mz_repr.chrono", "::mz_repr::chrono")
        .extern_path(".mz_repr.global_id", "::mz_repr::global_id")
        .extern_path(".mz_repr.proto", "::mz_repr::proto")
        .extern_path(".mz_repr.relation_and_scalar", "::mz_repr")
        .extern_path(".mz_repr.row", "::mz_repr")
        .type_attribute(
            ".mz_dataflow_types.postgres_source",
            "#[derive(Eq, serde::Serialize, serde::Deserialize)]",
        )
        .compile(
            &[
                "dataflow-types/src/client.proto",
                "dataflow-types/src/client/controller/storage.proto",
                "dataflow-types/src/errors.proto",
                "dataflow-types/src/logging.proto",
                "dataflow-types/src/types/connections/aws.proto",
                "dataflow-types/src/types/sinks.proto",
                "dataflow-types/src/types/sources.proto",
                "dataflow-types/src/types/sources/encoding.proto",
                "dataflow-types/src/plan.proto",
                "dataflow-types/src/plan/join.proto",
                "dataflow-types/src/plan/reduce.proto",
                "dataflow-types/src/plan/threshold.proto",
                "dataflow-types/src/plan/top_k.proto",
                "dataflow-types/src/types.proto",
            ],
            &[".."],
        )
        .unwrap();
}
