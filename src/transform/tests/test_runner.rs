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
    use expr_test_util::{build_rel, TestCatalog};
    use transform::{Optimizer, Transform, TransformArgs};

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum TestType {
        Build,
        Opt,
        Steps,
    }

    fn run_testcase(
        s: &str,
        cat: &TestCatalog,
        args: &HashMap<String, Vec<String>>,
        test_type: TestType,
    ) -> Result<String, Error> {
        let mut rel = build_rel(s, cat).map_err(|e| anyhow!(e))?;

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

        match test_type {
            TestType::Opt => {
                let mut opt = Optimizer::for_dataflow();
                rel = opt.optimize(rel, &HashMap::new()).unwrap().into_inner();

                Ok(cat.generate_explanation(&rel, args.get("format")))
            }
            TestType::Build => Ok(cat.generate_explanation(&rel, args.get("format"))),
            TestType::Steps => {
                // TODO(justin): this thing does not currently peek into fixpoints, so it's not
                // that helpful for optimizations that involve those (which is most of them).
                let opt = Optimizer::for_dataflow();
                let mut out = String::new();
                // Buffer of the names of the transformations that have been applied with no changes.
                let mut no_change: Vec<String> = Vec::new();

                writeln!(
                    out,
                    "{}",
                    cat.generate_explanation(&rel, args.get("format"))
                )?;
                writeln!(out, "====")?;

                for transform in opt.transforms.iter() {
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
                        writeln!(
                            out,
                            "\n{}",
                            cat.generate_explanation(&rel, args.get("format"))
                        )?;
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
                writeln!(
                    out,
                    "{}",
                    cat.generate_explanation(&rel, args.get("format"))
                )?;
                writeln!(out, "====")?;

                Ok(out)
            }
        }
    }

    fn get_transform(name: &str) -> Result<Box<dyn Transform>, Error> {
        // TODO(justin): is there a way to just extract these from the Optimizer list of
        // transforms?
        match name {
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
            "PredicatePullup" => Ok(Box::new(transform::predicate_pullup::PredicatePullup)),
            "PredicatePushdown" => Ok(Box::new(transform::predicate_pushdown::PredicatePushdown)),
            "ProjectionExtraction" => Ok(Box::new(
                transform::projection_extraction::ProjectionExtraction,
            )),
            "ProjectionLifting" => Ok(Box::new(transform::projection_lifting::ProjectionLifting)),
            "ReductionPushdown" => Ok(Box::new(transform::reduction_pushdown::ReductionPushdown)),
            "RedundantJoin" => Ok(Box::new(transform::redundant_join::RedundantJoin)),
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
                        Ok(msg) => format!("{}\n", msg.trim_end().to_string()),
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
