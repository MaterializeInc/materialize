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
    use expr::MirRelationExpr;
    use expr_test_util::{build_rel, json_to_spec, TestCatalog};
    use ore::str::separated;
    use transform::{Optimizer, Transform, TransformArgs};

    // Global options
    const IN: &str = "in";
    const FORMAT: &str = "format";
    // Values that can be supplied for global options
    const JSON: &str = "json";
    const TEST: &str = "test";

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

    // Converts MirRelationExpr back. `args[format]` specifies which output
    // format to use.
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
            FormatType::Explain(format) => cat.generate_explanation(rel, *format),
        }
    }

    fn run_testcase(
        s: &str,
        cat: &TestCatalog,
        args: &HashMap<String, Vec<String>>,
        test_type: TestType,
    ) -> Result<String, Error> {
        let mut rel = parse_relation(s, cat, args)?;
        let mut id_gen = Default::default();
        let indexes = HashMap::new();
        for t in args.get("apply").cloned().unwrap_or_else(Vec::new).iter() {
            get_transform(t)?.transform(
                &mut rel,
                TransformArgs {
                    id_gen: &mut id_gen,
                    indexes: &indexes,
                },
            )?;
        }

        let format_type = if let Some(format) = args.get(FORMAT) {
            if format.iter().any(|s| s == TEST) {
                FormatType::Test
            } else if format.iter().any(|s| s == JSON) {
                FormatType::Json
            } else {
                FormatType::Explain(args.get(FORMAT))
            }
        } else {
            FormatType::Explain(args.get(FORMAT))
        };

        let mut logical_opt = Optimizer::logical_optimizer();
        let mut physical_opt = Optimizer::physical_optimizer();

        let out = match test_type {
            TestType::Opt => {
                rel = logical_opt.optimize(rel).unwrap().into_inner();
                rel = physical_opt.optimize(rel).unwrap().into_inner();

                convert_rel_to_string(&rel, &cat, &format_type)
            }
            TestType::Build => convert_rel_to_string(&rel, &cat, &format_type),
            TestType::Steps => {
                // TODO(justin): this thing does not currently peek into fixpoints, so it's not
                // that helpful for optimizations that involve those (which is most of them).
                let mut out = String::new();
                // Buffer of the names of the transformations that have been applied with no changes.
                let mut no_change: Vec<String> = Vec::new();

                writeln!(out, "{}", convert_rel_to_string(&rel, &cat, &format_type))?;
                writeln!(out, "====")?;

                for transform in logical_opt
                    .transforms
                    .iter()
                    .chain(physical_opt.transforms.iter())
                {
                    let prev = rel.clone();
                    transform.transform(
                        &mut rel,
                        TransformArgs {
                            id_gen: &mut id_gen,
                            indexes: &indexes,
                        },
                    )?;

                    if rel != prev {
                        if no_change.len() > 0 {
                            write!(out, "No change:")?;
                            let mut sep = " ";
                            for t in no_change {
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
            "CanonicalizeMfp" => Ok(Box::new(transform::canonicalize_mfp::CanonicalizeMfp)),
            "ColumnKnowledge" => Ok(Box::new(transform::column_knowledge::ColumnKnowledge)),
            "Demand" => Ok(Box::new(transform::demand::Demand)),
            "FilterFusion" => Ok(Box::new(transform::fusion::filter::Filter)),
            "FoldConstants" => Ok(Box::new(transform::reduction::FoldConstants {
                limit: None,
            })),
            "JoinFusion" => Ok(Box::new(transform::fusion::join::Join)),
            "LiteralLifting" => Ok(Box::new(transform::map_lifting::LiteralLifting)),
            "NonNullRequirements" => Ok(Box::new(
                transform::nonnull_requirements::NonNullRequirements,
            )),
            "PredicatePushdown" => Ok(Box::new(transform::predicate_pushdown::PredicatePushdown)),
            "ProjectionExtraction" => Ok(Box::new(
                transform::projection_extraction::ProjectionExtraction,
            )),
            "ProjectionLifting" => Ok(Box::new(transform::projection_lifting::ProjectionLifting)),
            "ProjectionPushdown" => {
                Ok(Box::new(transform::projection_pushdown::ProjectionPushdown))
            }
            "ReductionPushdown" => Ok(Box::new(transform::reduction_pushdown::ReductionPushdown)),
            "UnionBranchCancellation" => {
                Ok(Box::new(transform::union_cancel::UnionBranchCancellation))
            }
            "UnionFusion" => Ok(Box::new(transform::fusion::union::Union)),
            _ => Err(anyhow!(
                "no transform named {} (you might have to add it to get_transform)",
                name
            )),
        }
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
                    "build" => match run_testcase(&s.input, &catalog, &s.args, TestType::Build) {
                        // Generally, explanations for fully optimized queries
                        // are not allowed to have whitespace at the end;
                        // however, a partially optimized query can.
                        // Since clippy rejects test results with trailing
                        // whitespace, remove whitespace before comparing results.
                        Ok(msg) => {
                            format!("{}", separated("\n", msg.split('\n').map(|s| s.trim_end())))
                        }
                        Err(err) => format!("error: {}\n", err),
                    },
                    "opt" => match run_testcase(&s.input, &catalog, &s.args, TestType::Opt) {
                        Ok(msg) => msg,
                        Err(err) => format!("error: {}\n", err),
                    },
                    "steps" => match run_testcase(&s.input, &catalog, &s.args, TestType::Steps) {
                        Ok(msg) => msg,
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
