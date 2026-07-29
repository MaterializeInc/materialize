// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_expr::explain::{ExplainContext, enforce_linear_chains};
use mz_expr_parser::{TestCatalog, handle_define, try_parse_mir};
use mz_ore::str::Indent;
use mz_repr::GlobalId;
use mz_repr::explain::text::text_string_at;
use mz_repr::explain::{ExplainConfig, PlanRenderingContext};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_transform::analysis::annotate_plan;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::{Transform, TransformCtx};

const TEST_GLOBAL_ID: GlobalId = GlobalId::Transient(1234567);

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
                "typecheck" => handle_typecheck(&catalog, &test_case.input),
                "apply" => handle_apply(&catalog, &test_case.input, &test_case.args),
                _ => format!("unknown directive: {}", test_case.directive),
            }
        })
    });
}

#[allow(clippy::disallowed_types)] // what testdrive provides
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

    // Create OptimizerFeatures and override from the config overrides layer.
    let features = OptimizerFeatures::default().override_from(&config.features);

    let context = ExplainContext {
        config: &config,
        features: &features,
        humanizer: catalog,
        cardinality_stats: Default::default(), // empty stats
        used_indexes: Default::default(),
        finishing: Default::default(),
        duration: Default::default(),
        target_cluster: Default::default(),
        optimizer_notices: Default::default(),
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
        ambiguous_ids: BTreeSet::default(),
    })
}

fn handle_typecheck(catalog: &TestCatalog, input: &str) -> String {
    // Parse the relation, returning early on parse error.
    let relation = match try_parse_mir(catalog, input) {
        Ok(relation) => relation,
        Err(err) => return err,
    };

    // Apply the transformation, returning early on TransformError.
    use mz_transform::typecheck::{Typecheck, columns_pretty};
    let ctx = mz_transform::typecheck::empty_typechecking_context();

    let tc = Typecheck::new(std::sync::Arc::clone(&ctx));

    let res = tc.typecheck(&relation, &ctx.lock().expect("typecheck ctx"));

    match res {
        Ok(typ) => format!("{}\n", columns_pretty(&typ, catalog).trim()),
        Err(err) => format!(
            "{}\n",
            mz_transform::typecheck::TypeErrorHumanizer::new(&err, catalog)
                .to_string()
                .trim(),
        ),
    }
}

#[allow(clippy::disallowed_types)] // what testdrive provides
fn handle_apply(
    catalog: &TestCatalog,
    input: &str,
    args: &std::collections::HashMap<String, Vec<String>>,
) -> String {
    let Some(pipeline) = args.get("pipeline") else {
        return "missing required `pipeline` argument for `apply` directive".to_string();
    };

    if pipeline.is_empty() {
        return "empty `pipeline` argument for `apply` directive".to_string();
    }

    let mut transforms = vec![];
    for name in pipeline {
        match get_transforms(name) {
            Ok(ts) => transforms.extend(ts),
            Err(err) => return err,
        }
    }

    apply_transforms(transforms, catalog, input, args).unwrap_or_else(|err| err)
}

/// Resolves a `pipeline` entry of the `apply` directive to a transform
/// sequence. Most names map to a single transform, `optimize` expands to the
/// full optimizer pipeline.
fn get_transforms(name: &str) -> Result<Vec<Box<dyn Transform>>, String> {
    use mz_transform::*;
    let transform: Box<dyn Transform> = match name {
        // Pseudo-transforms.
        "identity" => Box::new(Identity),
        "optimize" => return Ok(full_transform_list()),
        // Actual transforms.
        "anf" => Box::new(cse::anf::ANF::default()),
        "canonicalize_mfp" => Box::new(canonicalize_mfp::CanonicalizeMfp),
        "case_literal" => Box::new(case_literal::CaseLiteralTransform),
        "coalesce_case" => Box::new(coalesce_case::CoalesceCase),
        "equivalence_propagation" => {
            Box::new(equivalence_propagation::EquivalencePropagation::default())
        }
        "flat_map_elimination" => Box::new(canonicalization::FlatMapElimination),
        "fold_constants" => Box::new(fold_constants::FoldConstants { limit: None }),
        "fusion" => Box::new(fusion::Fusion),
        "fusion_join" => Box::new(fusion::join::Join),
        "fusion_top_k" => Box::new(fusion::top_k::TopK),
        "literal_lifting" => Box::new(literal_lifting::LiteralLifting::default()),
        "non_null_requirements" => Box::new(non_null_requirements::NonNullRequirements::default()),
        "normalize_lets" => Box::new(normalize_lets::NormalizeLets::new(false)),
        "predicate_pushdown" => Box::new(predicate_pushdown::PredicatePushdown::default()),
        "projection_extraction" => Box::new(canonicalization::ProjectionExtraction),
        "projection_lifting" => Box::new(movement::ProjectionLifting::default()),
        "projection_pushdown" => Box::new(movement::ProjectionPushdown::default()),
        "reduction_pushdown" => Box::new(reduction_pushdown::ReductionPushdown),
        "redundant_join" => Box::new(redundant_join::RedundantJoin::default()),
        "relation_cse" => Box::new(cse::relation_cse::RelationCSE::new(false)),
        "semijoin_idempotence" => Box::new(semijoin_idempotence::SemijoinIdempotence::default()),
        "threshold_elision" => Box::new(threshold_elision::ThresholdElision),
        "union_branch_cancellation" => Box::new(union_cancel::UnionBranchCancellation),
        "union_fusion" => Box::new(fusion::union::Union),
        "union_negate_fusion" => Box::new(compound::UnionNegateFusion),
        "will_distinct" => Box::new(will_distinct::WillDistinct),
        transform => return Err(format!("unsupported pipeline transform: {transform}")),
    };
    Ok(vec![transform])
}

/// The full optimizer pipeline, as applied by the `optimize` pipeline name.
fn full_transform_list() -> Vec<Box<dyn Transform>> {
    use mz_transform::{Optimizer, TransformCtx, typecheck};

    let features = OptimizerFeatures::default();
    let typecheck_ctx = typecheck::empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut transform_ctx = TransformCtx::local(
        &features,
        &typecheck_ctx,
        &mut df_meta,
        None,
        Some(TEST_GLOBAL_ID),
    );

    #[allow(deprecated)]
    Optimizer::logical_optimizer(&mut transform_ctx)
        .transforms
        .into_iter()
        .chain(std::iter::once::<Box<dyn Transform>>(Box::new(
            mz_transform::movement::ProjectionPushdown::default(),
        )))
        .chain(std::iter::once::<Box<dyn Transform>>(Box::new(
            mz_transform::normalize_lets::NormalizeLets::new(false),
        )))
        .chain(Optimizer::logical_cleanup_pass(&mut transform_ctx, false).transforms)
        .chain(Optimizer::physical_optimizer(&mut transform_ctx).transforms)
        .collect::<Vec<_>>()
}

#[allow(clippy::disallowed_types)] // what testdrive provides
fn apply_transforms(
    transforms: Vec<Box<dyn Transform>>,
    catalog: &TestCatalog,
    input: &str,
    args: &std::collections::HashMap<String, Vec<String>>,
) -> Result<String, String> {
    // Parse the relation, returning early on parse error.
    let mut relation = try_parse_mir(catalog, input)?;

    let mut features = mz_repr::optimize::OptimizerFeatures::default();
    // Apply a non-default feature flag to test the right implementation.
    features.enable_letrec_fixpoint_analysis = true;
    features.enable_dequadratic_eqprop_map = true;
    features.enable_eq_classes_withholding_errors = true;
    // Tests opt into flag-gated transform behavior via directive args, e.g.
    // `apply pipeline=will_distinct enable_will_distinct_propagation=true`.
    if args.contains_key("enable_will_distinct_propagation") {
        features.enable_will_distinct_propagation = true;
    }
    let typecheck_ctx = mz_transform::typecheck::empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut transform_ctx = TransformCtx::local(
        &features,
        &typecheck_ctx,
        &mut df_meta,
        None,
        Some(TEST_GLOBAL_ID),
    );

    // Apply the transformations, returning early on TransformError.
    for transform in transforms {
        transform
            .transform(&mut relation, &mut transform_ctx)
            .map_err(|e| format!("{}\n", e.to_string().trim()))?;
    }

    // Serialize and return the transformed relation.
    Ok(relation.debug_explain(&ExplainConfig::default(), Some(catalog)))
}

fn parse_explain_config(mut flags: BTreeSet<String>) -> Result<ExplainConfig, String> {
    let result = ExplainConfig {
        arity: flags.remove("arity"),
        humanized_exprs: flags.remove("humanized_exprs"),
        column_names: flags.remove("column_names"),
        keys: flags.remove("keys"),
        types: flags.remove("types"),
        redacted: false,
        join_impls: false,
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

impl Transform for Identity {
    fn name(&self) -> &'static str {
        "Identity"
    }

    fn actually_perform_transform(
        &self,
        _relation: &mut mz_expr::MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), mz_transform::TransformError> {
        Ok(())
    }
}
