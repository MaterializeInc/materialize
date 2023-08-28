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

use mz_expr_parser::{handle_define, try_parse_mir, TestCatalog};
use mz_repr::explain::ExplainConfig;
use mz_transform::typecheck::TypeErrorHumanizer;

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn run_tests() {
    // Interpret datadriven tests.
    datadriven::walk("tests/test_transforms", |f| {
        let mut catalog = TestCatalog::default();
        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "define" => handle_define(&mut catalog, &test_case.input),
                "apply" => handle_apply(&catalog, &test_case.input, &test_case.args),
                "typecheck" => handle_typecheck(&catalog, &test_case.input, &test_case.args),
                _ => format!("unknown directive: {}", test_case.directive),
            }
        })
    });
}

#[allow(clippy::disallowed_types)]
fn handle_typecheck(
    catalog: &TestCatalog,
    input: &str,
    _args: &std::collections::HashMap<String, Vec<String>>,
) -> String {
    // Parse the relation, returning early on parse error.
    let relation = match try_parse_mir(catalog, input) {
        Ok(relation) => relation,
        Err(err) => return err,
    };

    // Apply the transformation, returning early on TransformError.
    use mz_transform::typecheck::{columns_pretty, Typecheck};
    let ctx = mz_transform::typecheck::empty_context();

    let tc = Typecheck::new(std::rc::Rc::clone(&ctx));

    let res = tc.typecheck(&relation, &ctx.borrow_mut());

    match res {
        Ok(typ) => format!("{}\n", columns_pretty(&typ, catalog).trim()),
        Err(err) => format!(
            "{}\n",
            TypeErrorHumanizer::new(&err, catalog).to_string().trim(),
        ),
    }
}

#[allow(clippy::disallowed_types)]
fn handle_apply(
    catalog: &TestCatalog,
    input: &str,
    args: &std::collections::HashMap<String, Vec<String>>,
) -> String {
    let Some(pipeline) = args.get("pipeline") else {
        return "missing required `pipeline` argument for `apply` directive".to_string();
    };

    if pipeline.len() != 1 {
        return "unexpected `pipeline` arguments for `apply` directive".to_string();
    }

    let result = match pipeline[0].as_str() {
        // Pseudo-transforms.
        "identity" => {
            // noop
            let transform = Identity::default();
            apply_transform(transform, catalog, input)
        }
        // Actual transforms.
        "anf" => {
            use mz_transform::cse::anf::ANF;
            let transform = ANF::default();
            apply_transform(transform, catalog, input)
        }
        "column_knowledge" => {
            use mz_transform::column_knowledge::ColumnKnowledge;
            let transform = ColumnKnowledge::default();
            apply_transform(transform, catalog, input)
        }
        "literal_lifting" => {
            use mz_transform::literal_lifting::LiteralLifting;
            let transform = LiteralLifting::default();
            apply_transform(transform, catalog, input)
        }
        "non_null_requirements" => {
            use mz_transform::non_null_requirements::NonNullRequirements;
            let transform = NonNullRequirements::default();
            apply_transform(transform, catalog, input)
        }
        "predicate_pushdown" => {
            use mz_transform::predicate_pushdown::PredicatePushdown;
            let transform = PredicatePushdown::default();
            apply_transform(transform, catalog, input)
        }
        "projection_lifting" => {
            use mz_transform::movement::ProjectionLifting;
            let transform = ProjectionLifting::default();
            apply_transform(transform, catalog, input)
        }
        "projection_pushdown" => {
            use mz_transform::movement::ProjectionPushdown;
            let transform = ProjectionPushdown::default();
            apply_transform(transform, catalog, input)
        }
        "normalize_lets" => {
            use mz_transform::normalize_lets::NormalizeLets;
            let transform = NormalizeLets::new(false);
            apply_transform(transform, catalog, input)
        }
        "redundant_join" => {
            use mz_transform::redundant_join::RedundantJoin;
            let transform = RedundantJoin::default();
            apply_transform(transform, catalog, input)
        }
        "relation_cse" => {
            use mz_transform::cse::relation_cse::RelationCSE;
            let transform = RelationCSE::new(false);
            apply_transform(transform, catalog, input)
        }
        "semijoin_idempotence" => {
            use mz_transform::semijoin_idempotence::SemijoinIdempotence;
            let transform = SemijoinIdempotence::default();
            apply_transform(transform, catalog, input)
        }
        transform => Err(format!("unsupported pipeline transform: {transform}")),
    };

    result.unwrap_or_else(|err| err)
}

fn apply_transform<T: mz_transform::Transform>(
    transform: T,
    catalog: &TestCatalog,
    input: &str,
) -> Result<String, String> {
    // Parse the relation, returning early on parse error.
    let mut relation = try_parse_mir(catalog, input)?;

    // Apply the transformation, returning early on TransformError.
    transform
        .transform(&mut relation, EMPTY_ARGS)
        .map_err(|e| format!("{}\n", e.to_string().trim()))?;

    // Serialize and return the transformed relation.
    Ok(relation.explain(&ExplainConfig::default(), Some(catalog)))
}

#[derive(Debug, Default)]
struct Identity;

impl mz_transform::Transform for Identity {
    fn transform(
        &self,
        _relation: &mut mz_expr::MirRelationExpr,
        _args: mz_transform::TransformArgs,
    ) -> Result<(), mz_transform::TransformError> {
        Ok(())
    }
}

const EMPTY_ARGS: mz_transform::TransformArgs<'static> = mz_transform::TransformArgs {
    indexes: &mz_transform::EmptyIndexOracle,
    stats: &mz_transform::EmptyStatisticsOracle,
    global_id: None,
};
