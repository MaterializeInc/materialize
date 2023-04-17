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
    use mz_expr::canonicalize::{canonicalize_equivalences, canonicalize_predicates};
    use mz_expr::{MapFilterProject, MirScalarExpr};
    use mz_expr_test_util::*;
    use mz_lowertest::{deserialize, deserialize_optional, tokenize, MzReflect};
    use mz_ore::result::ResultExt;
    use mz_ore::str::separated;
    use mz_repr::ColumnType;

    use serde::{Deserialize, Serialize};

    fn reduce(s: &str) -> Result<MirScalarExpr, String> {
        let mut input_stream = tokenize(s)?.into_iter();
        let mut ctx = MirScalarExprDeserializeContext::default();
        let mut scalar: MirScalarExpr = deserialize(&mut input_stream, "MirScalarExpr", &mut ctx)?;
        let typ: Vec<ColumnType> = deserialize(&mut input_stream, "Vec<ColumnType> ", &mut ctx)?;
        let before = scalar.typ(&typ);
        scalar.reduce(&typ);
        let after = scalar.typ(&typ);
        // Verify that `reduce` did not change the type of the scalar.
        if before.scalar_type != after.scalar_type {
            return Err(format!(
                "FAIL: Type of scalar has changed:\nbefore: {:?}\nafter: {:?}\n",
                before, after
            ));
        }
        Ok(scalar)
    }

    fn test_canonicalize_pred(s: &str) -> Result<Vec<MirScalarExpr>, String> {
        let mut input_stream = tokenize(s)?.into_iter();
        let mut ctx = MirScalarExprDeserializeContext::default();
        let input_predicates: Vec<MirScalarExpr> =
            deserialize(&mut input_stream, "Vec<MirScalarExpr>", &mut ctx)?;
        let typ: Vec<ColumnType> = deserialize(&mut input_stream, "Vec<ColumnType>", &mut ctx)?;
        // predicate canonicalization is meant to produce the same output regardless of the
        // order of the input predicates.
        let mut predicates1 = input_predicates.clone();
        canonicalize_predicates(&mut predicates1, &typ);
        let mut predicates2 = input_predicates.clone();
        predicates2.sort();
        canonicalize_predicates(&mut predicates2, &typ);
        let mut predicates3 = input_predicates;
        predicates3.sort();
        predicates3.reverse();
        canonicalize_predicates(&mut predicates3, &typ);
        if predicates1 != predicates2 || predicates1 != predicates3 {
            Err(format!(
                "predicate canonicalization resulted in unrealiable output: [{}] vs [{}] vs [{}]",
                separated(", ", predicates1.iter().map(|p| p.to_string())),
                separated(", ", predicates2.iter().map(|p| p.to_string())),
                separated(", ", predicates3.iter().map(|p| p.to_string())),
            ))
        } else {
            Ok(predicates1)
        }
    }

    #[derive(Deserialize, Serialize, MzReflect)]
    enum MFPTestCommand {
        Map(Vec<MirScalarExpr>),
        Filter(Vec<MirScalarExpr>),
        Project(Vec<usize>),
        Optimize,
    }

    /// Builds a [MapFilterProject] of a certain arity, then modifies it.
    /// The test syntax is `<input_arity> [<commands>]`
    /// The syntax for a command is `<name_of_command> [<args>]`
    fn test_mfp(s: &str) -> Result<MapFilterProject, String> {
        let mut input_stream = tokenize(s)?.into_iter();
        let mut ctx = MirScalarExprDeserializeContext::default();
        let input_arity = input_stream
            .next()
            .unwrap()
            .to_string()
            .parse::<usize>()
            .map_err_to_string_with_causes()?;
        let mut mfp = MapFilterProject::new(input_arity);
        while let Some(command) = deserialize_optional::<MFPTestCommand, _, _>(
            &mut input_stream,
            "MFPTestCommand",
            &mut ctx,
        )? {
            match command {
                MFPTestCommand::Map(map) => mfp = mfp.map(map),
                MFPTestCommand::Filter(filter) => mfp = mfp.filter(filter),
                MFPTestCommand::Project(project) => mfp = mfp.project(project),
                MFPTestCommand::Optimize => mfp.optimize(),
            }
        }
        Ok(mfp)
    }

    fn test_canonicalize_equiv(s: &str) -> Result<Vec<Vec<MirScalarExpr>>, String> {
        let mut input_stream = tokenize(s)?.into_iter();
        let mut ctx = MirScalarExprDeserializeContext::default();
        let mut equivalences: Vec<Vec<MirScalarExpr>> =
            deserialize(&mut input_stream, "Vec<Vec<MirScalarExpr>>", &mut ctx)?;
        let input_type: Vec<ColumnType> =
            deserialize(&mut input_stream, "Vec<ColumnType>", &mut ctx)?;
        canonicalize_equivalences(&mut equivalences, std::iter::once(&input_type));
        Ok(equivalences)
    }

    #[test]
    fn run() {
        datadriven::walk("tests/testdata", |f| {
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    // tests simplification of scalars
                    "reduce" => match reduce(&s.input) {
                        Ok(scalar) => {
                            format!("{}\n", scalar)
                        }
                        Err(err) => format!("error: {}\n", err),
                    },
                    "canonicalize" => match test_canonicalize_pred(&s.input) {
                        Ok(preds) => {
                            format!("{}\n", separated("\n", preds.iter().map(|p| p.to_string())))
                        }
                        Err(err) => format!("error: {}\n", err),
                    },
                    "mfp" => match test_mfp(&s.input) {
                        Ok(mfp) => {
                            let (map, filter, project) = mfp.as_map_filter_project();
                            format!(
                                "[{}]\n[{}]\n[{}]\n",
                                separated(" ", map.iter()),
                                separated(" ", filter.iter()),
                                separated(" ", project.iter())
                            )
                        }
                        Err(err) => format!("error: {}\n", err),
                    },
                    "canonicalize-join" => match test_canonicalize_equiv(&s.input) {
                        Ok(equivalences) => {
                            format!(
                                "{}\n",
                                separated(
                                    "\n",
                                    equivalences.iter().map(|e| format!(
                                        "[{}]",
                                        separated(" ", e.iter().map(|expr| format!("{}", expr)))
                                    ))
                                )
                            )
                        }
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
