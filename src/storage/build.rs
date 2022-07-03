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
        .extern_path(".mz_proto", "::mz_proto")
        .extern_path(".mz_repr.relation_and_scalar", "::mz_repr")
        .extern_path(".mz_repr.row", "::mz_repr")
        .extern_path(".mz_repr.url", "::mz_repr::url")
        .type_attribute(
            ".mz_dataflow_types.postgres_source",
            "#[derive(Eq, serde::Serialize, serde::Deserialize)]",
        )
        .compile(
            &[
                "storage/src/client.proto",
                "storage/src/client/controller.proto",
                "storage/src/client/errors.proto",
                "storage/src/client/connections/aws.proto",
                "storage/src/client/sinks.proto",
                "storage/src/client/sources.proto",
                "storage/src/client/sources/encoding.proto",
                "storage/src/client/transforms.proto",
            ],
            &[".."],
        )
        .unwrap();
}
