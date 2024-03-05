// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_expr::explain::{enforce_linear_chains, ExplainContext};
use mz_expr_parser::{handle_define, try_parse_mir, TestCatalog};
use mz_ore::str::Indent;
use mz_repr::explain::text::text_string_at;
use mz_repr::explain::{ExplainConfig, PlanRenderingContext};
use mz_transform::attribute::annotate_plan;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::typecheck::TypeErrorHumanizer;

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn run_tests() {
    // Interpret datadriven tests.
    datadriven::walk("tests/test_transforms", |f| {
        let mut catalog = TestCatalog::default();
        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "define" => handle_define(&mut catalog, &test_case.input),
                "explain" => handle_explain(&catalog, &test_case.input, &test_case.args),
                "typecheck" => handle_typecheck(&catalog, &test_case.input, &test_case.args),
                "apply" => handle_apply(&catalog, &test_case.input, &test_case.args),
                _ => format!("unknown directive: {}", test_case.directive),
            }
        })
    });
}

#[allow(clippy::disallowed_types)]
fn handle_explain(
    catalog: &TestCatalog,
    input: &str,
    args: &std::collections::HashMap<String, Vec<String>>,
) -> String {
    let with = match args.get("with") {
        Some(with) => with.iter().cloned().collect::<BTreeSet<String>>(),
        None => return "missing required `with` argument for `explain` directive".to_string(),
    };

    // Create the ExplainConfig from the given `with` set of strings.
    let config = match parse_explain_config(with) {
        Ok(config) => config,
        Err(e) => return format!("ExplainConfig::try_from error\n{}\n", e.to_string().trim()),
    };

    let context = ExplainContext {
        config: &config,
        humanizer: catalog,
        used_indexes: Default::default(),
        finishing: Default::default(),
        duration: Default::default(),
        optimizer_notices: Vec::default(),
    };

    // Parse the relation, returning early on parse error.
    let mut relation = match try_parse_mir(catalog, input) {
        Ok(relation) => relation,
        Err(e) => return format!("try_parse_mir error:\n{}\n", e.to_string().trim()),
    };

    // normalize the representation as linear chains
    // (this implies !context.config.raw_plans by construction)
    if context.config.linear_chains {
        match enforce_linear_chains(&mut relation) {
            Ok(_) => {}
            Err(e) => return format!("enforce_linear_chains error:\n{}\n", e.to_string().trim()),
        };
    };

    // We deliberately don't interpret the `raw_plans` config option here,
    // because we might want to test the output of things that are reset when it
    // is set. For test purposes we never want to implicitly normalize the plan
    // as part this statement.

    let annotated_plan = match annotate_plan(&relation, &context) {
        Ok(annotated_plan) => annotated_plan,
        Err(e) => return format!("annotate_plan error:\n{}\n", e.to_string().trim()),
    };

    text_string_at(annotated_plan.plan, || PlanRenderingContext {
        indent: Indent::default(),
        humanizer: context.humanizer,
        annotations: annotated_plan.annotations.clone(),
        config: &config,
    })
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

    let tc = Typecheck::new(std::sync::Arc::clone(&ctx));

    let res = tc.typecheck(&relation, &ctx.lock().expect("typecheck ctx"));

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
        "fold_constants" => {
            use mz_transform::fold_constants::FoldConstants;
            let transform = FoldConstants { limit: None };
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
        "fusion_top_k" => {
            use mz_transform::fusion::top_k::TopK;
            let transform = TopK;
            apply_transform(transform, catalog, input)
        }
        "fusion_join" => {
            use mz_transform::fusion::join::Join;
            let transform = Join;
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

    let mut dataflow_metainfo = DataflowMetainfo::default();
    let mut empty_args: mz_transform::TransformCtx = mz_transform::TransformCtx {
        indexes: &mz_transform::EmptyIndexOracle,
        stats: &mz_transform::EmptyStatisticsOracle,
        global_id: None,
        enable_eager_delta_joins: false,
        dataflow_metainfo: &mut dataflow_metainfo,
    };

    // Apply the transformation, returning early on TransformError.
    transform
        .transform(&mut relation, &mut empty_args)
        .map_err(|e| format!("{}\n", e.to_string().trim()))?;

    // Serialize and return the transformed relation.
    Ok(relation.explain(&ExplainConfig::default(), Some(catalog)))
}

fn parse_explain_config(mut flags: BTreeSet<String>) -> Result<ExplainConfig, String> {
    let result = ExplainConfig {
        arity: flags.remove("arity"),
        humanized_exprs: flags.remove("humanized_exprs"),
        column_names: flags.remove("column_names"),
        types: flags.remove("types"),
        redacted: false,
        join_impls: false,
        no_fast_path: false,
        raw_plans: false,
        ..ExplainConfig::default()
    };

    if flags.is_empty() {
        Ok(result)
    } else {
        let err = format!(
            "parse_explain_config\n\
             unsupported 'with' option: {flags:?}\n"
        );
        Err(err)
    }
}

#[derive(Debug, Default)]
struct Identity;

impl mz_transform::Transform for Identity {
    fn transform(
        &self,
        _relation: &mut mz_expr::MirRelationExpr,
        _ctx: &mut mz_transform::TransformCtx,
    ) -> Result<(), mz_transform::TransformError> {
        Ok(())
    }
}
