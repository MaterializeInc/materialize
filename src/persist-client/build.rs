// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

use std::env;

fn main() {
    env::set_var("PROTOC", protobuf_src::protoc());

    let mut config = prost_build::Config::new();
    config
        .btree_map(["."])
        // Note(parkertimmerman): We purposefully omit `.mz_persist_client.internal.diff` from here
        // because we want to use `bytes::Bytes` for the types in that package, and `Bytes` doesn't
        // implement `serde::Serialize`
        .type_attribute(
            ".mz_persist_client.internal.state",
            "#[derive(serde::Serialize)]",
        )
        .type_attribute(
            ".mz_persist_client.internal.state.ProtoHollowBatch",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(
            ".mz_persist_client.internal.state.ProtoHollowBatchPart",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(
            ".mz_persist_client.internal.state.ProtoU64Description",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(
            ".mz_persist_client.internal.state.ProtoU64Antichain",
            "#[derive(serde::Deserialize)]",
        )
        .type_attribute(
            ".mz_persist_client.batch",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(".", "#[allow(missing_docs)]")
        .btree_map(["."])
        .bytes([
            ".mz_persist_client.batch.ProtoBatch",
            ".mz_persist_client.internal.diff.ProtoStateFieldDiffs",
            ".mz_persist_client.internal.state.ProtoHollowBatchPart",
            ".mz_persist_client.internal.state.ProtoVersionedData",
            ".mz_persist_client.internal.service.ProtoPushDiff",
        ]);

    tonic_build::configure()
        // Enabling `emit_rerun_if_changed` will rerun the build script when
        // anything in the include directory (..) changes. This causes quite a
        // bit of spurious recompilation, so we disable it. The default behavior
        // is to re-run if any file in the crate changes; that's still a bit too
        // broad, but it's better.
        .emit_rerun_if_changed(false)
        .extern_path(".mz_persist_types", "::mz_persist_types")
        .extern_path(".mz_proto", "::mz_proto")
        .compile_with_config(
            config,
            &[
                "persist-client/src/batch.proto",
                "persist-client/src/cfg.proto",
                "persist-client/src/internal/service.proto",
                "persist-client/src/internal/state.proto",
                "persist-client/src/internal/diff.proto",
            ],
            &[".."],
        )
        .unwrap_or_else(|e| panic!("{e}"))
}
