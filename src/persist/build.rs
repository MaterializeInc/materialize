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
#![allow(
    clippy::style,
    clippy::complexity,
    clippy::large_enum_variant,
    clippy::mutable_key_type,
    clippy::stable_sort_primitive,
    clippy::map_entry,
    clippy::box_default
)]
#![warn(
    clippy::bool_comparison,
    clippy::clone_on_ref_ptr,
    clippy::no_effect,
    clippy::unnecessary_unwrap,
    clippy::dbg_macro,
    clippy::todo,
    clippy::wildcard_dependencies,
    clippy::zero_prefixed_literal,
    clippy::borrowed_box,
    clippy::deref_addrof,
    clippy::double_must_use,
    clippy::double_parens,
    clippy::extra_unused_lifetimes,
    clippy::needless_borrow,
    clippy::needless_question_mark,
    clippy::needless_return,
    clippy::redundant_pattern,
    clippy::redundant_slicing,
    clippy::redundant_static_lifetimes,
    clippy::single_component_path_imports,
    clippy::unnecessary_cast,
    clippy::useless_asref,
    clippy::useless_conversion,
    clippy::builtin_type_shadow,
    clippy::duplicate_underscore_argument,
    clippy::double_neg,
    clippy::unnecessary_mut_passed,
    clippy::wildcard_in_or_patterns,
    clippy::collapsible_if,
    clippy::collapsible_else_if,
    clippy::crosspointer_transmute,
    clippy::excessive_precision,
    clippy::overflow_check_conditional,
    clippy::as_conversions,
    clippy::match_overlapping_arm,
    clippy::zero_divided_by_zero,
    clippy::must_use_unit,
    clippy::suspicious_assignment_formatting,
    clippy::suspicious_else_formatting,
    clippy::suspicious_unary_op_formatting,
    clippy::mut_mutex_lock,
    clippy::print_literal,
    clippy::same_item_push,
    clippy::useless_format,
    clippy::write_literal,
    clippy::redundant_closure,
    clippy::redundant_closure_call,
    clippy::unnecessary_lazy_evaluations,
    clippy::partialeq_ne_impl,
    clippy::redundant_field_names,
    clippy::transmutes_expressible_as_ptr_casts,
    clippy::unused_async,
    clippy::disallowed_methods,
    clippy::disallowed_macros,
    clippy::from_over_into
)]
// END LINT CONFIG

use std::env;

fn main() {
    env::set_var("PROTOC", protobuf_src::protoc());

    prost_build::Config::new()
        .compile_protos(&["persist/src/persist.proto"], &[".."])
        .unwrap();
}
