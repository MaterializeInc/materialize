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
/// sequence. Names are the transforms' canonical `Transform::name` strings and
/// map to a single transform, `optimize` expands to the full optimizer
/// pipeline.
fn get_transforms(name: &str) -> Result<Vec<Box<dyn Transform>>, String> {
    use mz_transform::*;
    let transform: Box<dyn Transform> = match name {
        // Pseudo-transforms.
        "Identity" => Box::new(Identity),
        "optimize" => return Ok(full_transform_list()),
        // Actual transforms.
        "ANF" => Box::new(cse::anf::ANF::default()),
        "CanonicalizeMfp" => Box::new(canonicalize_mfp::CanonicalizeMfp),
        "CaseLiteralTransform" => Box::new(case_literal::CaseLiteralTransform),
        "CoalesceCase" => Box::new(coalesce_case::CoalesceCase),
        "EquivalencePropagation" => {
            Box::new(equivalence_propagation::EquivalencePropagation::default())
        }
        "FlatMapElimination" => Box::new(canonicalization::FlatMapElimination),
        "FoldConstants" => Box::new(fold_constants::FoldConstants { limit: None }),
        "Fusion" => Box::new(fusion::Fusion),
        "JoinFusion" => Box::new(fusion::join::Join),
        "LiteralLifting" => Box::new(literal_lifting::LiteralLifting::default()),
        "NonNullRequirements" => Box::new(non_null_requirements::NonNullRequirements::default()),
        "NormalizeLets" => Box::new(normalize_lets::NormalizeLets::new(false)),
        "PredicatePushdown" => Box::new(predicate_pushdown::PredicatePushdown::default()),
        "ProjectionExtraction" => Box::new(canonicalization::ProjectionExtraction),
        "ProjectionLifting" => Box::new(movement::ProjectionLifting::default()),
        "ProjectionPushdown" => Box::new(movement::ProjectionPushdown::default()),
        "ReductionPushdown" => Box::new(reduction_pushdown::ReductionPushdown),
        "RedundantJoin" => Box::new(redundant_join::RedundantJoin::default()),
        "RelationCSE" => Box::new(cse::relation_cse::RelationCSE::new(false)),
        "SemijoinIdempotence" => Box::new(semijoin_idempotence::SemijoinIdempotence::default()),
        "ThresholdElision" => Box::new(threshold_elision::ThresholdElision),
        "TopKFusion" => Box::new(fusion::top_k::TopK),
        "UnionBranchCancellation" => Box::new(union_cancel::UnionBranchCancellation),
        "UnionFusion" => Box::new(fusion::union::Union),
        "UnionNegateFusion" => Box::new(compound::UnionNegateFusion),
        "WillDistinct" => Box::new(will_distinct::WillDistinct),
        transform => return Err(format!("unsupported pipeline transform: {transform}")),
    };
    Ok(vec![transform])
}

/// Checks that every single-transform pipeline name accepted by
/// [`get_transforms`] is the canonical [`Transform::name`] of the transform it
/// resolves to. Names added to the `match` in [`get_transforms`] must also be
/// added here. `optimize` is exempt, it expands to the full optimizer pipeline
/// rather than naming a single transform.
#[mz_ore::test]
fn pipeline_names_are_canonical() {
    const PIPELINE_NAMES: &[&str] = &[
        "Identity",
        "ANF",
        "CanonicalizeMfp",
        "CaseLiteralTransform",
        "CoalesceCase",
        "EquivalencePropagation",
        "FlatMapElimination",
        "FoldConstants",
        "Fusion",
        "JoinFusion",
        "LiteralLifting",
        "NonNullRequirements",
        "NormalizeLets",
        "PredicatePushdown",
        "ProjectionExtraction",
        "ProjectionLifting",
        "ProjectionPushdown",
        "ReductionPushdown",
        "RedundantJoin",
        "RelationCSE",
        "SemijoinIdempotence",
        "ThresholdElision",
        "TopKFusion",
        "UnionBranchCancellation",
        "UnionFusion",
        "UnionNegateFusion",
        "WillDistinct",
    ];

    for name in PIPELINE_NAMES {
        let transforms = get_transforms(name).expect("known pipeline name");
        assert_eq!(
            transforms.len(),
            1,
            "`{name}` must resolve to one transform"
        );
        assert_eq!(
            transforms[0].name(),
            *name,
            "pipeline name `{name}` must match the transform's canonical name",
        );
    }
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
    // `apply pipeline=WillDistinct enable_will_distinct_propagation=true`.
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
