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
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
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

mod test {
    use mz_expr_test_util::*;
    use mz_lowertest::{serialize, MzReflect, TestDeserializeContext};
    use mz_ore::result::ResultExt;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    /// Build a object of type `T` from the test spec, turn it back into a
    /// test spec, construct an object from the new test spec, and see if the
    /// two objects are the same.
    fn roundtrip<F, T, G, C>(
        s: &str,
        type_name: &str,
        build_obj: F,
        ctx_gen: G,
    ) -> Result<T, String>
    where
        T: DeserializeOwned + MzReflect + Serialize + Eq + Clone,
        F: Fn(&str) -> Result<T, String>,
        C: TestDeserializeContext,
        G: Fn() -> C,
    {
        let result: T = build_obj(s)?;
        let json = serde_json::to_value(result.clone()).map_err_to_string_with_causes()?;
        let new_s = serialize::<T, _>(&json, type_name, &mut ctx_gen());
        let new_result = build_obj(&new_s)?;
        if new_result.eq(&result) {
            Ok(result)
        } else {
            Err(format!(
                "Round trip failed. New spec:\n{}
            Original {type_name}:\n{}
            New {type_name}:\n{}
            JSON for original {type_name}:\n{}",
                new_s,
                serde_json::to_string_pretty(&result).unwrap(),
                serde_json::to_string_pretty(&new_result).unwrap(),
                json,
                type_name = type_name
            ))
        }
    }

    /// An extended version of `roundtrip`.
    ///
    /// Only works for `MirRelationExpr`.
    ///
    /// When converting the relation back to the test spec, this method also
    /// figures out the catalog commands required to create the relation from a
    /// blank catalog.
    fn roundtrip_with_catalog(orig_spec: &str, orig_catalog: &TestCatalog) -> Result<(), String> {
        let orig_rel = build_rel(orig_spec, orig_catalog)?;
        let json = serde_json::to_value(orig_rel.clone()).map_err_to_string_with_causes()?;
        let mut new_catalog = TestCatalog::default();
        let (new_spec, source_defs) = json_to_spec(&json.to_string(), &new_catalog);
        for source_def in source_defs {
            new_catalog.handle_test_command(&source_def)?;
        }
        let new_rel = build_rel(&new_spec, &new_catalog)?;
        if new_rel.eq(&orig_rel) {
            Ok(())
        } else {
            Err(format!(
                "Round trip failed. New spec:\n{}
            Original relation:\n{}
            New relation:\n{}
            JSON for original relation:\n{}",
                new_spec,
                serde_json::to_string_pretty(&orig_rel).unwrap(),
                serde_json::to_string_pretty(&new_rel).unwrap(),
                json,
            ))
        }
    }

    #[mz_ore::test]
    fn run_roundtrip_tests() {
        datadriven::walk("tests/testdata", |f| {
            let mut catalog = TestCatalog::default();
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    "cat" => match catalog.handle_test_command(&s.input) {
                        Ok(()) => String::from("ok\n"),
                        Err(err) => format!("error: {}\n", err),
                    },
                    // tests that we can build `MirScalarExpr`s
                    "build-scalar" => {
                        match roundtrip(
                            &s.input,
                            "MirScalarExpr",
                            build_scalar,
                            MirScalarExprDeserializeContext::default,
                        ) {
                            Ok(scalar) => format!("{}\n", scalar),
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "build" => {
                        match roundtrip(
                            &s.input,
                            "MirRelationExpr",
                            |s| build_rel(s, &catalog),
                            || MirRelationExprDeserializeContext::new(&catalog),
                        ) {
                            // Generally, explanations for fully optimized queries
                            // are not allowed to have whitespace at the end;
                            // however, a partially optimized query can.
                            // Since clippy rejects test results with trailing
                            // whitespace, remove whitespace before comparing results.
                            Ok(rel) => format!("{}\n", rel.pretty()),
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "rel-to-test" => match roundtrip_with_catalog(&s.input, &catalog) {
                        Ok(()) => "ok\n".to_string(),
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
