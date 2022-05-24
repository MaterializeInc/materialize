// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// This module defines a small language for directly constructing RelationExprs and running
/// various optimizations on them. It uses datadriven, so the output of each test can be rewritten
/// by setting the REWRITE environment variable.
/// TODO(justin):
/// * It's currently missing a mechanism to run just a single test file
/// * There is some duplication between this and the SQL planner

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Write;

    use anyhow::{anyhow, Error};
    use mz_expr::{Id, MirRelationExpr};
    use mz_expr_test_util::{
        build_rel, generate_explanation, json_to_spec, MirRelationExprDeserializeContext,
        TestCatalog,
    };
    use mz_lowertest::{deserialize, tokenize};
    use mz_ore::str::separated;
    use mz_repr::GlobalId;
    use mz_transform::dataflow::{optimize_dataflow_demand_inner, optimize_dataflow_filters_inner};
    use mz_transform::{EmptyIndexOracle, Optimizer, Transform, TransformArgs};
    use proc_macro2::TokenTree;

    // Global options
    const IN: &str = "in";
    const FORMAT: &str = "format";
    // Values that can be supplied for global options
    const JSON: &str = "json";
    const TEST: &str = "test";

    thread_local! {
        static FULL_TRANSFORM_LIST: Vec<Box<dyn Transform>> =
            Optimizer::logical_optimizer()
                .transforms
                .into_iter()
                .chain(std::iter::once(
                    Box::new(mz_transform::projection_pushdown::ProjectionPushdown)
                        as Box<dyn Transform>,
                ))
                .chain(std::iter::once(
                    Box::new(mz_transform::update_let::UpdateLet::default()) as Box<dyn Transform>
                ))
                .chain(Optimizer::logical_cleanup_pass().transforms.into_iter())
                .chain(Optimizer::physical_optimizer().transforms.into_iter())
                .collect::<Vec<_>>();
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
            FormatType::Explain(format) => generate_explanation(cat, rel, *format),
        }
    }

    fn run_single_view_testcase(
        s: &str,
        cat: &TestCatalog,
        args: &HashMap<String, Vec<String>>,
        test_type: TestType,
    ) -> Result<String, Error> {
        let mut rel = parse_relation(s, cat, args)?;
        let mut id_gen = Default::default();
        for t in args.get("apply").cloned().unwrap_or_else(Vec::new).iter() {
            get_transform(t)?.transform(
                &mut rel,
                TransformArgs {
                    id_gen: &mut id_gen,
                    indexes: &EmptyIndexOracle,
                },
            )?;
        }

        let format_type = get_format_type(args);

        let out = match test_type {
            TestType::Opt => FULL_TRANSFORM_LIST.with(|transforms| -> Result<_, Error> {
                for transform in transforms.iter() {
                    transform.transform(
                        &mut rel,
                        TransformArgs {
                            id_gen: &mut id_gen,
                            indexes: &EmptyIndexOracle,
                        },
                    )?;
                }
                Ok(convert_rel_to_string(&rel, &cat, &format_type))
            })?,
            TestType::Build => convert_rel_to_string(&rel, &cat, &format_type),
            TestType::Steps => {
                // TODO(justin): this thing does not currently peek into fixpoints, so it's not
                // that helpful for optimizations that involve those (which is most of them).
                let mut out = String::new();
                // Buffer of the names of the transformations that have been applied with no changes.
                let mut no_change: Vec<String> = Vec::new();

                writeln!(out, "{}", convert_rel_to_string(&rel, &cat, &format_type))?;
                writeln!(out, "====")?;

                FULL_TRANSFORM_LIST.with(|transforms| -> Result<_, Error> {
                    for transform in transforms {
                        let prev = rel.clone();
                        transform.transform(
                            &mut rel,
                            TransformArgs {
                                id_gen: &mut id_gen,
                                indexes: &EmptyIndexOracle,
                            },
                        )?;

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
                            writeln!(out, "\n{}", convert_rel_to_string(&rel, &cat, &format_type))?;
                            writeln!(out, "====")?;
                        } else {
                            no_change.push(format!("{:?}", transform));
                        }
                    }
                    Ok(())
                })?;

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
                writeln!(out, "{}", convert_rel_to_string(&rel, &cat, &format_type))?;
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
            "ColumnKnowledge" => Ok(Box::new(
                mz_transform::column_knowledge::ColumnKnowledge::default(),
            )),
            "Demand" => Ok(Box::new(mz_transform::demand::Demand::default())),
            "FilterFusion" => Ok(Box::new(mz_transform::fusion::filter::Filter)),
            "FoldConstants" => Ok(Box::new(mz_transform::reduction::FoldConstants {
                limit: None,
            })),
            "FlatMapToMap" => Ok(Box::new(mz_transform::fusion::flatmap_to_map::FlatMapToMap)),
            "JoinFusion" => Ok(Box::new(mz_transform::fusion::join::Join)),
            "LiteralLifting" => Ok(Box::new(
                mz_transform::map_lifting::LiteralLifting::default(),
            )),
            "NonNullRequirements" => Ok(Box::new(
                mz_transform::nonnull_requirements::NonNullRequirements::default(),
            )),
            "PredicatePushdown" => Ok(Box::new(
                mz_transform::predicate_pushdown::PredicatePushdown::default(),
            )),
            "ProjectionExtraction" => Ok(Box::new(
                mz_transform::projection_extraction::ProjectionExtraction,
            )),
            "ProjectionLifting" => Ok(Box::new(
                mz_transform::projection_lifting::ProjectionLifting::default(),
            )),
            "ProjectionPushdown" => Ok(Box::new(
                mz_transform::projection_pushdown::ProjectionPushdown,
            )),
            "ReductionPushdown" => Ok(Box::new(
                mz_transform::reduction_pushdown::ReductionPushdown,
            )),
            "RedundantJoin" => Ok(Box::new(
                mz_transform::redundant_join::RedundantJoin::default(),
            )),
            "TopKFusion" => Ok(Box::new(mz_transform::fusion::top_k::TopK)),
            "UnionBranchCancellation" => Ok(Box::new(
                mz_transform::union_cancel::UnionBranchCancellation,
            )),
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
        let mut input_stream = tokenize(&s)?.into_iter();
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
            let mut optimizer = Optimizer::logical_optimizer();
            dataflow = dataflow
                .into_iter()
                .map(|(id, rel)| (id, optimizer.optimize(rel).unwrap().into_inner()))
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
            let mut log_optimizer = Optimizer::logical_cleanup_pass();
            let mut phys_optimizer = Optimizer::physical_optimizer();
            dataflow = dataflow
                .into_iter()
                .map(|(id, rel)| {
                    (
                        id,
                        phys_optimizer
                            .optimize(log_optimizer.optimize(rel).unwrap().into_inner())
                            .unwrap()
                            .into_inner(),
                    )
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
                let mut predicates = HashMap::new();
                match optimize_dataflow_filters_inner(dataflow.iter_mut().map(|(id, rel)| (Id::Global(*id), rel)).rev(), &mut predicates) {
                    Ok(()) => Ok(format!("Pushed-down predicates:\n{}", log_pushed_outside_of_dataflow(predicates, cat))),
                    Err(e) => Err(e.to_string()),
                }
            }
            "project" => {
                let mut demand = HashMap::new();
                if let Some((id, rel)) = dataflow.last() {
                    demand.insert(Id::Global(*id), (0..rel.arity()).collect());
                }
                match optimize_dataflow_demand_inner(dataflow.iter_mut().map(|(id, rel)| (Id::Global(*id), rel)).rev(), &mut demand) {
                    Ok(()) => Ok(format!("Pushed-down demand:\n{}", log_pushed_outside_of_dataflow(demand, cat))),
                    Err(e) => Err(e.to_string()),
                }
            }
            _ => return Err(format!(
                "no cross-view transform named {} (you might have to add it to apply_cross_view_transform)",
                transform
            ))
        }
    }

    /// Converts a map of (source) -> (information pushed to source) into a string.
    fn log_pushed_outside_of_dataflow<D>(map: HashMap<Id, D>, cat: &TestCatalog) -> String
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

    #[test]
    fn run() {
        datadriven::walk("tests/testdata", |f| {
            let mut catalog = TestCatalog::default();
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    "cat" => match catalog.handle_test_command(&s.input) {
                        Ok(()) => String::from("ok\n"),
                        Err(err) => format!("error: {}\n", err),
                    },
                    "build" => {
                        match run_single_view_testcase(&s.input, &catalog, &s.args, TestType::Build)
                        {
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
                        match run_single_view_testcase(&s.input, &catalog, &s.args, TestType::Opt) {
                            Ok(msg) => msg,
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "steps" => {
                        match run_single_view_testcase(&s.input, &catalog, &s.args, TestType::Steps)
                        {
                            Ok(msg) => msg,
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "crossview" => {
                        match run_multiview_testcase(
                            &s.input,
                            &mut catalog,
                            &s.args,
                            TestType::Build,
                        ) {
                            Ok(msg) => format!(
                                "{}",
                                separated("\n", msg.split('\n').map(|s| s.trim_end()))
                            ),
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "crossviewopt" => {
                        match run_multiview_testcase(
                            &s.input,
                            &mut catalog,
                            &s.args,
                            TestType::Build,
                        ) {
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
