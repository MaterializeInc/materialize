// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines a small language for directly constructing RelationExprs and running
//! various optimizations on them. It uses datadriven, so the output of each test can be rewritten
//! by setting the REWRITE environment variable.
//! TODO(justin):
//! * It's currently missing a mechanism to run just a single test file
//! * There is some duplication between this and the SQL planner

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Write;

    use anyhow::{Error, anyhow};
    use mz_expr::explain::ExplainContext;
    use mz_expr::{Id, MirRelationExpr};
    use mz_expr_test_util::{
        MirRelationExprDeserializeContext, TestCatalog, build_rel, json_to_spec,
    };
    use mz_lowertest::{deserialize, tokenize};
    use mz_ore::collections::HashMap;
    use mz_ore::str::separated;
    use mz_repr::GlobalId;
    use mz_repr::explain::{Explain, ExplainConfig, ExplainFormat};
    use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
    use mz_transform::dataflow::{
        DataflowMetainfo, optimize_dataflow_demand_inner, optimize_dataflow_filters_inner,
    };
    use mz_transform::{Optimizer, Transform, TransformCtx, typecheck};
    use proc_macro2::TokenTree;

    use crate::explain::Explainable;

    // Global options
    const IN: &str = "in";
    const FORMAT: &str = "format";
    // Values that can be supplied for global options
    const JSON: &str = "json";
    const TEST: &str = "test";

    const TEST_GLOBAL_ID: GlobalId = GlobalId::Transient(1234567);

    fn full_transform_list() -> Vec<Box<dyn Transform>> {
        let features = OptimizerFeatures::default();
        let typecheck_ctx = typecheck::empty_context();
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

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum FormatType<'a> {
        Explain(Option<&'a Vec<String>>),
        Json,
        Test,
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum TestType {
        Build,
        Opt,
        Steps,
    }

    /// Parses the output format from `args[format]`.
    fn get_format_type<'a>(args: &'a HashMap<String, Vec<String>>) -> FormatType<'a> {
        if let Some(format) = args.get(FORMAT) {
            if format.iter().any(|s| s == TEST) {
                FormatType::Test
            } else if format.iter().any(|s| s == JSON) {
                FormatType::Json
            } else {
                FormatType::Explain(args.get(FORMAT))
            }
        } else {
            FormatType::Explain(args.get(FORMAT))
        }
    }

    // Converts string to MirRelationExpr. `args[in]` specifies which input
    // format is being used.
    fn parse_relation(
        s: &str,
        cat: &TestCatalog,
        args: &HashMap<String, Vec<String>>,
    ) -> Result<MirRelationExpr, Error> {
        if let Some(input_format) = args.get(IN) {
            if input_format.iter().any(|s| s == JSON) {
                return serde_json::from_str::<MirRelationExpr>(s).map_err(|e| anyhow!(e));
            }
        }
        build_rel(s, cat).map_err(|e| anyhow!(e))
    }

    /// Converts MirRelationExpr to `format_type`.
    fn convert_rel_to_string(
        rel: &MirRelationExpr,
        cat: &TestCatalog,
        format_type: &FormatType,
    ) -> String {
        match format_type {
            FormatType::Test => format!(
                "{}\n",
                json_to_spec(&serde_json::to_string(rel).unwrap(), cat).0
            ),
            FormatType::Json => format!("{}\n", serde_json::to_string(rel).unwrap()),
            FormatType::Explain(format) => {
                let format_contains = |key: &str| {
                    format
                        .map(|format| format.contains(&key.to_string()))
                        .unwrap_or(false)
                };

                let config = ExplainConfig {
                    arity: false,
                    join_impls: true,
                    keys: format_contains("types"), // FIXME: use `keys`
                    linear_chains: false,
                    non_negative: false,
                    raw_plans: false,
                    raw_syntax: false,
                    subtree_size: false,
                    equivalences: false,
                    timing: false,
                    types: format_contains("types"),
                    ..ExplainConfig::default()
                };

                // Create OptimizerFeatures and override from the config overrides layer.
                let features = OptimizerFeatures::default().override_from(&config.features);

                let context = ExplainContext {
                    config: &config,
                    features: &features,
                    humanizer: cat,
                    cardinality_stats: Default::default(), // empty stats
                    used_indexes: Default::default(),
                    finishing: Default::default(),
                    duration: Default::default(),
                    target_cluster: Default::default(),
                    optimizer_notices: Default::default(),
                };

                Explainable(&mut rel.clone())
                    .explain(&ExplainFormat::VerboseText, &context)
                    .unwrap()
            }
        }
    }

    #[mz_ore::instrument(fields(s))]
    fn run_single_view_testcase(
        s: &str,
        cat: &TestCatalog,
        args: &HashMap<String, Vec<String>>,
        test_type: TestType,
    ) -> Result<String, Error> {
        let features = OptimizerFeatures::default();
        let typecheck_ctx = typecheck::empty_context();
        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::local(
            &features,
            &typecheck_ctx,
            &mut df_meta,
            None,
            Some(TEST_GLOBAL_ID),
        );
        let mut rel = parse_relation(s, cat, args)?;
        for t in args.get("apply").cloned().unwrap_or_else(Vec::new).iter() {
            get_transform(t)?.transform(&mut rel, &mut transform_ctx)?;
        }

        let format_type = get_format_type(args);

        let out = match test_type {
            TestType::Opt => {
                let transforms = full_transform_list();
                for transform in transforms.iter() {
                    transform.transform(&mut rel, &mut transform_ctx)?;
                }
                convert_rel_to_string(&rel, cat, &format_type)
            }
            TestType::Build => convert_rel_to_string(&rel, cat, &format_type),
            TestType::Steps => {
                // TODO(justin): this thing does not currently peek into fixpoints, so it's not
                // that helpful for optimizations that involve those (which is most of them).
                let mut out = String::new();
                // Buffer of the names of the transformations that have been applied with no changes.
                let mut no_change: Vec<String> = Vec::new();

                writeln!(out, "{}", convert_rel_to_string(&rel, cat, &format_type))?;
                writeln!(out, "====")?;

                let transforms = full_transform_list();
                for transform in transforms {
                    let prev = rel.clone();
                    transform.transform(&mut rel, &mut transform_ctx)?;

                    if rel != prev {
                        if no_change.len() > 0 {
                            write!(out, "No change:")?;
                            let mut sep = " ";
                            for t in no_change.iter() {
                                write!(out, "{}{}", sep, t)?;
                                sep = ", ";
                            }
                            writeln!(out, "\n====")?;
                        }
                        no_change = vec![];

                        write!(out, "Applied {:?}:", transform)?;
                        writeln!(out, "\n{}", convert_rel_to_string(&rel, cat, &format_type))?;
                        writeln!(out, "====")?;
                    } else {
                        no_change.push(format!("{:?}", transform));
                    }
                }

                if no_change.len() > 0 {
                    write!(out, "No change:")?;
                    let mut sep = " ";
                    for t in no_change {
                        write!(out, "{}{}", sep, t)?;
                        sep = ", ";
                    }
                    writeln!(out, "\n====")?;
                }

                writeln!(out, "Final:")?;
                writeln!(out, "{}", convert_rel_to_string(&rel, cat, &format_type))?;
                writeln!(out, "====")?;

                out
            }
        };
        if let FormatType::Test = format_type {
            let source_defs = json_to_spec(&serde_json::to_string(&rel).unwrap(), cat).1;
            if !source_defs.is_empty() {
                return Ok(format!(
                    "{}====\nCatalog defs:\n{}\n",
                    out,
                    separated("\n", source_defs)
                ));
            }
        }
        Ok(out)
    }

    fn get_transform(name: &str) -> Result<Box<dyn Transform>, Error> {
        // TODO(justin): is there a way to just extract these from the Optimizer list of
        // transforms?
        match name {
            "CanonicalizeMfp" => Ok(Box::new(mz_transform::canonicalize_mfp::CanonicalizeMfp)),
            "EquivalencePropagation" => Ok(Box::new(
                mz_transform::equivalence_propagation::EquivalencePropagation::default(),
            )),
            "Demand" => Ok(Box::new(mz_transform::demand::Demand::default())),
            "Fusion" => Ok(Box::new(mz_transform::fusion::Fusion)),
            "FoldConstants" => Ok(Box::new(mz_transform::fold_constants::FoldConstants {
                limit: None,
            })),
            "FlatMapToMap" => Ok(Box::new(mz_transform::canonicalization::FlatMapToMap)),
            "JoinFusion" => Ok(Box::new(mz_transform::fusion::join::Join)),
            "LiteralLifting" => Ok(Box::new(
                mz_transform::literal_lifting::LiteralLifting::default(),
            )),
            "NonNullRequirements" => Ok(Box::new(
                mz_transform::non_null_requirements::NonNullRequirements::default(),
            )),
            "PredicatePushdown" => Ok(Box::new(
                mz_transform::predicate_pushdown::PredicatePushdown::default(),
            )),
            "ProjectionExtraction" => Ok(Box::new(
                mz_transform::canonicalization::ProjectionExtraction,
            )),
            "ProjectionLifting" => Ok(Box::new(
                mz_transform::movement::ProjectionLifting::default(),
            )),
            "ProjectionPushdown" => Ok(Box::new(
                mz_transform::movement::ProjectionPushdown::default(),
            )),
            "ReductionPushdown" => Ok(Box::new(
                mz_transform::reduction_pushdown::ReductionPushdown,
            )),
            "ReduceElision" => Ok(Box::new(mz_transform::reduce_elision::ReduceElision)),
            "RedundantJoin" => Ok(Box::new(
                mz_transform::redundant_join::RedundantJoin::default(),
            )),
            "RelationCSE" => Ok(Box::new(mz_transform::cse::relation_cse::RelationCSE::new(
                false,
            ))),
            "ThresholdElision" => Ok(Box::new(mz_transform::threshold_elision::ThresholdElision)),
            "UnionBranchCancellation" => Ok(Box::new(
                mz_transform::union_cancel::UnionBranchCancellation,
            )),
            "UnionNegateFusion" => Ok(Box::new(mz_transform::compound::UnionNegateFusion)),
            "UnionFusion" => Ok(Box::new(mz_transform::fusion::union::Union)),
            _ => Err(anyhow!(
                "no transform named {} (you might have to add it to get_transform)",
                name
            )),
        }
    }

    // TODO: have multiview test case accept the "in" argument
    fn run_multiview_testcase(
        s: &str,
        cat: &mut TestCatalog,
        args: &HashMap<String, Vec<String>>,
        test_type: TestType,
    ) -> Result<String, String> {
        let mut input_stream = tokenize(s)?.into_iter();
        let mut dataflow = Vec::new();
        while let Some(token) = input_stream.next() {
            match token {
                TokenTree::Group(group) => {
                    let mut inner_iter = group.stream().into_iter();
                    let name = match inner_iter.next() {
                        Some(TokenTree::Ident(ident)) => ident.to_string(),
                        other => {
                            return Err(format!("Could not parse {:?} as view name", other));
                        }
                    };
                    let rel: MirRelationExpr = deserialize(
                        &mut inner_iter,
                        "MirRelationExpr",
                        &mut MirRelationExprDeserializeContext::new(cat),
                    )?;
                    let id = cat.insert(&name, rel.typ(), true)?;
                    dataflow.push((id, rel));
                }
                other => return Err(format!("Could not parse {:?} as view", other)),
            }
        }
        if dataflow.is_empty() {
            return Err("Empty dataflow".to_string());
        }
        let mut out = String::new();
        if test_type == TestType::Opt {
            let features = OptimizerFeatures::default();
            let typecheck_ctx = typecheck::empty_context();
            let mut df_meta = DataflowMetainfo::default();
            let mut transform_ctx = TransformCtx::local(
                &features,
                &typecheck_ctx,
                &mut df_meta,
                None,
                Some(TEST_GLOBAL_ID),
            );

            #[allow(deprecated)]
            let optimizer = Optimizer::logical_optimizer(&mut transform_ctx);
            dataflow = dataflow
                .into_iter()
                .map(|(id, rel)| {
                    transform_ctx.set_global_id(id);
                    (
                        id,
                        optimizer
                            .optimize(rel, &mut transform_ctx)
                            .unwrap()
                            .into_inner(),
                    )
                })
                .collect();
        }
        match test_type {
            TestType::Build => {
                for t in args.get("apply").cloned().unwrap_or_else(Vec::new).iter() {
                    out.push_str(&apply_cross_view_transform(t, &mut dataflow, cat)?[..]);
                }
            }
            TestType::Opt => {
                for t in ["filter", "project"] {
                    out.push_str(&apply_cross_view_transform(t, &mut dataflow, cat)?[..]);
                }
            }
            _ => {}
        };
        if test_type == TestType::Opt {
            let features = OptimizerFeatures::default();
            let typecheck_ctx = typecheck::empty_context();
            let mut df_meta = DataflowMetainfo::default();
            let mut transform_ctx = TransformCtx::local(
                &features,
                &typecheck_ctx,
                &mut df_meta,
                None,
                Some(TEST_GLOBAL_ID),
            );

            let log_optimizer = Optimizer::logical_cleanup_pass(&mut transform_ctx, true);
            let phys_optimizer = Optimizer::physical_optimizer(&mut transform_ctx);
            dataflow = dataflow
                .into_iter()
                .map(|(id, rel)| {
                    transform_ctx.set_global_id(id);
                    let local_mir_plan = log_optimizer
                        .optimize(rel, &mut transform_ctx)
                        .unwrap()
                        .into_inner();
                    let global_mir_plan = phys_optimizer
                        .optimize(local_mir_plan, &mut transform_ctx)
                        .unwrap()
                        .into_inner();
                    (id, global_mir_plan)
                })
                .collect();
        }
        let format_type = get_format_type(args);
        out = format!(
            "{}\n====\n{}",
            out,
            separated(
                "====\n",
                dataflow.into_iter().map(|(id, rel)| format!(
                    "View {}:\n{}\n",
                    cat.get_source_name(&id).unwrap(),
                    convert_rel_to_string(&rel, cat, &format_type)
                ))
            )
        );
        cat.remove_transient_objects();
        Ok(out)
    }

    /// Applies a transform across the set of `MirRelationExpr`.
    ///
    /// Returns a string describing information pushed down to sources.
    fn apply_cross_view_transform(
        transform: &str,
        dataflow: &mut Vec<(GlobalId, MirRelationExpr)>,
        cat: &TestCatalog,
    ) -> Result<String, String> {
        match transform {
            "filter" => {
                let mut predicates = BTreeMap::new();
                match optimize_dataflow_filters_inner(
                    dataflow
                        .iter_mut()
                        .map(|(id, rel)| (Id::Global(*id), rel))
                        .rev(),
                    &mut predicates,
                ) {
                    Ok(()) => Ok(format!(
                        "Pushed-down predicates:\n{}",
                        log_pushed_outside_of_dataflow(predicates, cat)
                    )),
                    Err(e) => Err(e.to_string()),
                }
            }
            "project" => {
                let mut demand = BTreeMap::new();
                if let Some((id, rel)) = dataflow.last() {
                    demand.insert(Id::Global(*id), (0..rel.arity()).collect());
                }
                match optimize_dataflow_demand_inner(
                    dataflow
                        .iter_mut()
                        .map(|(id, rel)| (Id::Global(*id), rel))
                        .rev(),
                    &mut demand,
                ) {
                    Ok(()) => Ok(format!(
                        "Pushed-down demand:\n{}",
                        log_pushed_outside_of_dataflow(demand, cat)
                    )),
                    Err(e) => Err(e.to_string()),
                }
            }
            _ => Err(format!(
                "no cross-view transform named {} (you might have to add it to apply_cross_view_transform)",
                transform
            )),
        }
    }

    /// Converts a map of (source) -> (information pushed to source) into a string.
    fn log_pushed_outside_of_dataflow<D>(map: BTreeMap<Id, D>, cat: &TestCatalog) -> String
    where
        D: std::fmt::Debug + Clone,
    {
        let mut result = String::new();
        for (id, obj) in map {
            if let Id::Global(GlobalId::User(id)) = id {
                result.push_str(
                    &format!(
                        "Source {}: {:?}",
                        cat.get_source_name(&GlobalId::User(id)).unwrap(),
                        obj
                    )[..],
                );
                result.push('\n');
            }
        }
        result
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn run() {
        datadriven::walk("tests/testdata", |f| {
            let mut catalog = TestCatalog::default();
            f.run(move |s| -> String {
                let args = s.args.clone().into();
                match s.directive.as_str() {
                    "cat" => match catalog.handle_test_command(&s.input) {
                        Ok(()) => String::from("ok\n"),
                        Err(err) => format!("error: {}\n", err),
                    },
                    "build" => {
                        match run_single_view_testcase(&s.input, &catalog, &args, TestType::Build) {
                            // Generally, explanations for fully optimized queries
                            // are not allowed to have whitespace at the end;
                            // however, a partially optimized query can.
                            // Since clippy rejects test results with trailing
                            // whitespace, remove whitespace before comparing results.
                            Ok(msg) => {
                                format!(
                                    "{}",
                                    separated("\n", msg.split('\n').map(|s| s.trim_end()))
                                )
                            }
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "opt" => {
                        match run_single_view_testcase(&s.input, &catalog, &args, TestType::Opt) {
                            Ok(msg) => msg,
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "steps" => {
                        match run_single_view_testcase(&s.input, &catalog, &args, TestType::Steps) {
                            Ok(msg) => msg,
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "crossview" => {
                        match run_multiview_testcase(&s.input, &mut catalog, &args, TestType::Build)
                        {
                            Ok(msg) => format!(
                                "{}",
                                separated("\n", msg.split('\n').map(|s| s.trim_end()))
                            ),
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "crossviewopt" => {
                        match run_multiview_testcase(&s.input, &mut catalog, &args, TestType::Build)
                        {
                            Ok(msg) => msg,
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}

/// This duplicates code from `mz_adapter` as we don't want to move
/// [`mz_transform::analysis`] and [`mz_transform::normalize_lets`] to
/// [`mz_expr`].
mod explain {
    use mz_expr::MirRelationExpr;
    use mz_expr::explain::{ExplainContext, ExplainSinglePlan, enforce_linear_chains};
    use mz_repr::explain::{Explain, ExplainError, UnsupportedFormat};
    use mz_transform::analysis::annotate_plan;
    use mz_transform::normalize_lets::normalize_lets;

    /// Newtype struct for wrapping types that should
    /// implement the [`mz_repr::explain::Explain`] trait.
    pub(crate) struct Explainable<'a, T>(pub &'a mut T);

    impl<'a> Explain<'a> for Explainable<'a, MirRelationExpr> {
        type Context = ExplainContext<'a>;

        type Text = UnsupportedFormat;

        type VerboseText = ExplainSinglePlan<'a, MirRelationExpr>;

        type Json = UnsupportedFormat;

        type Dot = UnsupportedFormat;

        fn explain_verbose_text(
            &'a mut self,
            context: &'a Self::Context,
        ) -> Result<Self::VerboseText, ExplainError> {
            self.as_explain_single_plan(context)
        }
    }

    impl<'a> Explainable<'a, MirRelationExpr> {
        fn as_explain_single_plan(
            &'a mut self,
            context: &'a ExplainContext<'a>,
        ) -> Result<ExplainSinglePlan<'a, MirRelationExpr>, ExplainError> {
            // normalize the representation as linear chains
            // (this implies !context.config.raw_plans by construction)
            if context.config.linear_chains {
                enforce_linear_chains(self.0)?;
            };
            // unless raw plans are explicitly requested
            // normalize the representation of nested Let bindings
            // and enforce sequential Let binding IDs
            if !context.config.raw_plans {
                normalize_lets(self.0, context.features)
                    .map_err(|e| ExplainError::UnknownError(e.to_string()))?;
            }

            Ok(ExplainSinglePlan {
                context,
                plan: annotate_plan(self.0, context)?,
            })
        }
    }
}
