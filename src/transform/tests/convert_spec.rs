// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One-shot converter from the lowertest-based `tests/testdata` files to the
//! spec-based `tests/test_transforms/*.spec` files consumed by
//! `test_transforms.rs`.
//!
//! Inputs are parsed with the old lowertest machinery and printed in the spec
//! syntax. Two checks guard against semantic drift:
//!
//! * Every converted relation is parsed back with the spec parser and
//!   asserted equal to the original, so the input plans cannot change.
//! * The directive's transform sequence is applied to the original plan both
//!   with the old harness's optimizer features and with the new harness's
//!   (which enables three extra flags), asserting equal results. This proves
//!   the feature-flag difference between the harnesses does not change the
//!   behavior of the converted tests.
//!
//! Expected outputs are carried over verbatim. The old and the new harness
//! print through different explain paths, so goldens need a REWRITE run
//! afterwards, and the resulting text diff must be formatting-only (the MIR
//! equality checks above are the semantic safety net).
//!
//! Run with `cargo test -p mz-transform --test convert_spec -- --ignored`.

use std::fmt::Write;

use mz_expr::{AggregateExpr, AggregateFunc, Id, JoinImplementation, MirRelationExpr, TableFunc};
use mz_expr_parser::print_scalar;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, Diff, GlobalId, ReprColumnType, ReprScalarType, Row};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::{Transform, TransformCtx, typecheck};

const TEST_GLOBAL_ID: GlobalId = GlobalId::Transient(1234567);

/// A single datadriven test case, with surrounding comments and blank lines
/// preserved verbatim.
struct Case {
    preamble: String,
    directive: String,
    input: String,
    expected: String,
}

fn split_cases(contents: &str) -> Vec<Case> {
    let mut cases = vec![];
    let mut lines = contents.lines().peekable();
    loop {
        let mut preamble = String::new();
        while let Some(line) = lines.peek() {
            if line.trim().is_empty() || line.trim_start().starts_with('#') {
                writeln!(preamble, "{}", line).unwrap();
                lines.next();
            } else {
                break;
            }
        }
        let Some(directive) = lines.next() else {
            // Trailing preamble (end-of-file comments).
            if !preamble.is_empty() {
                cases.push(Case {
                    preamble,
                    directive: String::new(),
                    input: String::new(),
                    expected: String::new(),
                });
            }
            break;
        };
        let mut input = String::new();
        for line in lines.by_ref() {
            if line.trim() == "----" {
                break;
            }
            writeln!(input, "{}", line).unwrap();
        }
        let mut expected = String::new();
        for line in lines.by_ref() {
            if line.trim().is_empty() {
                break;
            }
            writeln!(expected, "{}", line).unwrap();
        }
        cases.push(Case {
            preamble,
            directive: directive.to_string(),
            input,
            expected,
        });
    }
    cases
}

/// Parses an old directive line into its name and `key=value` arguments.
fn parse_directive_line(line: &str) -> (String, Vec<(String, String)>) {
    let mut words = line.split_whitespace();
    let directive = words.next().expect("nonempty directive line").to_string();
    let args = words
        .map(|w| {
            let (k, v) = w.split_once('=').unwrap_or((w, ""));
            (k.to_string(), v.to_string())
        })
        .collect();
    (directive, args)
}

/// Converts a CamelCase transform name to the new harness's snake_case
/// pipeline name.
fn to_snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for c in name.chars() {
        if c.is_ascii_uppercase() {
            if !out.is_empty() {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn print_repr_scalar_type(typ: &ReprScalarType) -> Result<String, String> {
    let name = match typ {
        ReprScalarType::Bool => "boolean",
        ReprScalarType::Int16 => "smallint",
        ReprScalarType::Int32 => "integer",
        ReprScalarType::Int64 => "bigint",
        ReprScalarType::Float64 => "double precision",
        ReprScalarType::Numeric => "numeric",
        ReprScalarType::String => "text",
        ReprScalarType::Jsonb => "jsonb",
        typ => Err(format!("cannot print scalar type {typ:?}"))?,
    };
    Ok(name.to_string())
}

fn print_column_types(types: &[ReprColumnType]) -> Result<String, String> {
    let parts = types
        .iter()
        .map(|t| {
            Ok(format!(
                "{}{}",
                print_repr_scalar_type(&t.scalar_type)?,
                if t.nullable { "?" } else { "" }
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(parts.join(", "))
}

fn print_datum(datum: &Datum) -> Result<String, String> {
    match datum {
        Datum::Null => Ok("null".to_string()),
        Datum::True => Ok("true".to_string()),
        Datum::False => Ok("false".to_string()),
        Datum::Int16(i) => Ok(i.to_string()),
        Datum::Int32(i) => Ok(i.to_string()),
        Datum::Int64(i) => Ok(i.to_string()),
        Datum::Float64(f) => Ok(format!("{:?}", f.into_inner())),
        Datum::String(s) => Ok(format!("{s:?}")),
        datum => Err(format!("cannot print datum {datum:?}")),
    }
}

fn print_row(row: &Row) -> Result<String, String> {
    let datums = row
        .iter()
        .map(|d| print_datum(&d))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(datums.join(", "))
}

fn print_aggregate(agg: &AggregateExpr) -> Result<String, String> {
    use AggregateFunc::*;
    if agg.func == Count && !agg.distinct && agg.expr == mz_expr::MirScalarExpr::literal_true() {
        return Ok("count(*)".to_string());
    }
    let name = match agg.func {
        Count => "count",
        Any => "any",
        All => "all",
        MaxInt32 => "max_int32",
        MaxInt64 => "max_int64",
        MinInt32 => "min_int32",
        MinInt64 => "min_int64",
        SumInt16 => "sum_int16",
        SumInt32 => "sum_int32",
        SumInt64 => "sum_int64",
        ref func => Err(format!("cannot print aggregate {func:?}"))?,
    };
    Ok(format!(
        "{name}({}{})",
        if agg.distinct { "distinct " } else { "" },
        print_scalar(&agg.expr)?
    ))
}

fn print_scalars(exprs: &[mz_expr::MirScalarExpr]) -> Result<String, String> {
    let exprs = exprs
        .iter()
        .map(print_scalar)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(exprs.join(", "))
}

/// Prints a [`MirRelationExpr`] in the syntax accepted by
/// [`mz_expr_parser::try_parse_mir`].
fn print_mir(
    catalog: &mz_expr_parser::TestCatalog,
    expr: &MirRelationExpr,
    indent: usize,
    out: &mut String,
) -> Result<(), String> {
    use MirRelationExpr::*;
    let pad = " ".repeat(indent);
    macro_rules! line {
        ($($arg:tt)*) => {{
            writeln!(out, "{pad}{}", format!($($arg)*)).unwrap();
        }};
    }
    match expr {
        Constant { rows, typ } => {
            let types = print_column_types(&typ.column_types)?;
            let mut analyses = format!("types: \"({types})\"");
            if !typ.keys.is_empty() {
                let keys = typ
                    .keys
                    .iter()
                    .map(|key| {
                        let cols = key
                            .iter()
                            .map(|c| c.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("[{cols}]")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(analyses, ", keys: \"({keys})\"").unwrap();
            }
            let rows = match rows {
                Ok(rows) => rows,
                Err(err) => Err(format!("cannot print error Constant: {err}"))?,
            };
            if rows.is_empty() {
                line!("Constant <empty> // {{ {analyses} }}");
            } else {
                line!("Constant // {{ {analyses} }}");
                for (row, diff) in rows {
                    if *diff == Diff::ONE {
                        line!("  - ({})", print_row(row)?);
                    } else if diff.is_positive() {
                        line!("  - (({}) x {})", print_row(row)?, diff);
                    } else {
                        Err(format!("cannot print row with diff {diff}"))?
                    }
                }
            }
        }
        Get { id, .. } => match id {
            Id::Global(id) => match catalog.get_source_name(id) {
                Some(name) => line!("Get {name}"),
                None => Err(format!("no name for {id} in catalog"))?,
            },
            Id::Local(id) => line!("Get {id}"),
        },
        Let { .. } => {
            // Collect the chain of Let bindings. The parser requires cte ids
            // in ascending order, which holds for lowertest-built plans
            // (local ids are assigned in order of appearance).
            let mut ctes = vec![];
            let mut body = expr;
            while let Let {
                id,
                value,
                body: next,
            } = body
            {
                ctes.push((id, value));
                body = next;
            }
            line!("With");
            for (id, value) in ctes {
                line!("  cte {id} =");
                print_mir(catalog, value, indent + 4, out)?;
            }
            line!("Return");
            print_mir(catalog, body, indent + 2, out)?;
        }
        LetRec { .. } => Err("cannot print LetRec".to_string())?,
        Project { input, outputs } => {
            let outputs = outputs
                .iter()
                .map(|o| format!("#{o}"))
                .collect::<Vec<_>>()
                .join(", ");
            line!("Project ({outputs})");
            print_mir(catalog, input, indent + 2, out)?;
        }
        Map { input, scalars } => {
            line!("Map ({})", print_scalars(scalars)?);
            print_mir(catalog, input, indent + 2, out)?;
        }
        FlatMap { input, func, exprs } => {
            let func = match func {
                TableFunc::GenerateSeriesInt64 => "generate_series",
                TableFunc::GenerateSeriesInt32 => "generate_series_i32",
                TableFunc::JsonbObjectKeys => "jsonb_object_keys",
                func => Err(format!("cannot print table function {func:?}"))?,
            };
            line!("FlatMap {func}({})", print_scalars(exprs)?);
            print_mir(catalog, input, indent + 2, out)?;
        }
        Filter { input, predicates } => {
            // The parser splits a top-level AND into the predicate list, so a
            // predicate that is itself an AND call cannot be reconstructed.
            let is_and = |p: &mz_expr::MirScalarExpr| {
                matches!(
                    p,
                    mz_expr::MirScalarExpr::CallVariadic {
                        func: mz_expr::VariadicFunc::And(_),
                        ..
                    }
                )
            };
            if predicates.iter().any(is_and) {
                Err("cannot print Filter with a nested AND predicate".to_string())?
            }
            let predicates = predicates
                .iter()
                .map(print_scalar)
                .collect::<Result<Vec<_>, _>>()?;
            line!("Filter {}", predicates.join(" AND "));
            print_mir(catalog, input, indent + 2, out)?;
        }
        Join {
            inputs,
            equivalences,
            implementation,
        } => {
            if !matches!(implementation, JoinImplementation::Unimplemented) {
                Err("cannot print an implemented Join".to_string())?
            }
            if equivalences.is_empty() {
                line!("CrossJoin");
            } else {
                let equivalences = equivalences
                    .iter()
                    .map(|class| {
                        let class = class
                            .iter()
                            .map(print_scalar)
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok(class.join(" = "))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                line!("Join on=({})", equivalences.join(" AND "));
            }
            for input in inputs {
                print_mir(catalog, input, indent + 2, out)?;
            }
        }
        Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } => {
            let mut opts = String::new();
            if *monotonic {
                opts.push_str(" monotonic");
            }
            if let Some(size) = expected_group_size {
                write!(opts, " exp_group_size={size}").unwrap();
            }
            if aggregates.is_empty() {
                line!("Distinct project=[{}]{opts}", print_scalars(group_key)?);
            } else {
                let aggregates = aggregates
                    .iter()
                    .map(print_aggregate)
                    .collect::<Result<Vec<_>, _>>()?;
                let group_by = if group_key.is_empty() {
                    String::new()
                } else {
                    format!("group_by=[{}] ", print_scalars(group_key)?)
                };
                line!(
                    "Reduce {group_by}aggregates=[{}]{opts}",
                    aggregates.join(", ")
                );
            }
            print_mir(catalog, input, indent + 2, out)?;
        }
        TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
            expected_group_size,
        } => {
            let mut opts = String::new();
            if !group_key.is_empty() {
                let group_key = group_key
                    .iter()
                    .map(|k| format!("#{k}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(opts, " group_by=[{group_key}]").unwrap();
            }
            if !order_key.is_empty() {
                let order_key = order_key
                    .iter()
                    .map(|o| {
                        format!(
                            "#{} {} {}",
                            o.column,
                            if o.desc { "desc" } else { "asc" },
                            if o.nulls_last {
                                "nulls_last"
                            } else {
                                "nulls_first"
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(opts, " order_by=[{order_key}]").unwrap();
            }
            if let Some(limit) = limit {
                write!(opts, " limit={}", print_scalar(limit)?).unwrap();
            }
            if *offset > 0 {
                write!(opts, " offset={offset}").unwrap();
            }
            if *monotonic {
                opts.push_str(" monotonic");
            }
            if let Some(size) = expected_group_size {
                write!(opts, " exp_group_size={size}").unwrap();
            }
            line!("TopK{opts}");
            print_mir(catalog, input, indent + 2, out)?;
        }
        Negate { input } => {
            line!("Negate");
            print_mir(catalog, input, indent + 2, out)?;
        }
        Threshold { input } => {
            line!("Threshold");
            print_mir(catalog, input, indent + 2, out)?;
        }
        Union { base, inputs } => {
            line!("Union");
            print_mir(catalog, base, indent + 2, out)?;
            for input in inputs {
                print_mir(catalog, input, indent + 2, out)?;
            }
        }
        ArrangeBy { input, keys } => {
            let keys = keys
                .iter()
                .map(|key| Ok(format!("[{}]", print_scalars(key)?)))
                .collect::<Result<Vec<_>, String>>()?;
            line!("ArrangeBy keys=[{}]", keys.join(", "));
            print_mir(catalog, input, indent + 2, out)?;
        }
    }
    Ok(())
}

/// The transform sequences the old harness can run, resolved from directive
/// arguments the same way `get_transform` in the old `test_runner.rs` does.
fn get_transforms(names: &[String]) -> Vec<Box<dyn Transform>> {
    names
        .iter()
        .map(|name| -> Box<dyn Transform> {
            match name.as_str() {
                "CanonicalizeMfp" => Box::new(mz_transform::canonicalize_mfp::CanonicalizeMfp),
                "Fusion" => Box::new(mz_transform::fusion::Fusion),
                "FoldConstants" => {
                    Box::new(mz_transform::fold_constants::FoldConstants { limit: None })
                }
                "LiteralLifting" => {
                    Box::new(mz_transform::literal_lifting::LiteralLifting::default())
                }
                "NonNullRequirements" => {
                    Box::new(mz_transform::non_null_requirements::NonNullRequirements::default())
                }
                "PredicatePushdown" => {
                    Box::new(mz_transform::predicate_pushdown::PredicatePushdown::default())
                }
                "ProjectionExtraction" => {
                    Box::new(mz_transform::canonicalization::ProjectionExtraction)
                }
                "ProjectionLifting" => {
                    Box::new(mz_transform::movement::ProjectionLifting::default())
                }
                "ProjectionPushdown" => {
                    Box::new(mz_transform::movement::ProjectionPushdown::default())
                }
                "ReductionPushdown" => {
                    Box::new(mz_transform::reduction_pushdown::ReductionPushdown)
                }
                "RedundantJoin" => Box::new(mz_transform::redundant_join::RedundantJoin::default()),
                "UnionFusion" => Box::new(mz_transform::fusion::union::Union),
                "UnionNegateFusion" => Box::new(mz_transform::compound::UnionNegateFusion),
                "WillDistinct" => Box::new(mz_transform::will_distinct::WillDistinct),
                name => panic!("unknown transform {name}"),
            }
        })
        .collect()
}

/// The full optimizer pipeline, as the old harness's `opt` directive ran it.
fn full_transform_list() -> Vec<Box<dyn Transform>> {
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
    mz_transform::Optimizer::logical_optimizer(&mut transform_ctx)
        .transforms
        .into_iter()
        .chain(std::iter::once::<Box<dyn Transform>>(Box::new(
            mz_transform::movement::ProjectionPushdown::default(),
        )))
        .chain(std::iter::once::<Box<dyn Transform>>(Box::new(
            mz_transform::normalize_lets::NormalizeLets::new(false),
        )))
        .chain(mz_transform::Optimizer::logical_cleanup_pass(&mut transform_ctx, false).transforms)
        .chain(mz_transform::Optimizer::physical_optimizer(&mut transform_ctx).transforms)
        .collect::<Vec<_>>()
}

fn apply_transforms(
    transforms: &[Box<dyn Transform>],
    features: &OptimizerFeatures,
    mut relation: MirRelationExpr,
) -> Result<MirRelationExpr, String> {
    let typecheck_ctx = typecheck::empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut transform_ctx = TransformCtx::local(
        features,
        &typecheck_ctx,
        &mut df_meta,
        None,
        Some(TEST_GLOBAL_ID),
    );
    for transform in transforms {
        transform
            .transform(&mut relation, &mut transform_ctx)
            .map_err(|e| e.to_string())?;
    }
    Ok(relation)
}

/// Asserts that running the given transforms with the old harness's optimizer
/// features and with the new harness's features produces the same plan.
fn check_feature_flag_drift(
    context: &str,
    transforms: &[Box<dyn Transform>],
    will_distinct: bool,
    relation: &MirRelationExpr,
) {
    let mut old_features = OptimizerFeatures::default();
    old_features.enable_will_distinct_propagation = will_distinct;

    let mut new_features = OptimizerFeatures::default();
    new_features.enable_letrec_fixpoint_analysis = true;
    new_features.enable_dequadratic_eqprop_map = true;
    new_features.enable_eq_classes_withholding_errors = true;
    new_features.enable_will_distinct_propagation = will_distinct;

    let old_result = apply_transforms(transforms, &old_features, relation.clone());
    let new_result = apply_transforms(transforms, &new_features, relation.clone());
    assert_eq!(old_result, new_result, "feature flag drift in {context}");
}

struct Converter {
    old_cat: mz_expr_test_util::TestCatalog,
    /// Replays `handle_define` exactly as the new harness will at runtime,
    /// providing the golden output text for `define` blocks.
    new_cat_runtime: mz_expr_parser::TestCatalog,
    /// Registers the same sources non-transiently, so its `GlobalId`s match
    /// the old catalog's `User` ids. Used for printing and the roundtrip
    /// check against the old-parsed plans.
    new_cat_check: mz_expr_parser::TestCatalog,
}

impl Converter {
    /// Converts a `cat` block, returning the replacement `define` blocks
    /// (directive, input, output) for each newly defined source.
    fn convert_cat(&mut self, input: &str) -> Result<Vec<(String, String, String)>, String> {
        let before: Vec<GlobalId> = ids(&self.old_cat);
        self.old_cat.handle_test_command(input)?;
        let after: Vec<GlobalId> = ids(&self.old_cat);

        let mut blocks = vec![];
        for id in after.into_iter().filter(|id| !before.contains(id)) {
            let name = self.old_cat.get_source_name(&id).expect("just defined");
            let (_, typ) = get_source(&self.old_cat, name);

            let mut def = format!("DefSource name={name}");
            if !typ.keys.is_empty() {
                let keys = typ
                    .keys
                    .iter()
                    .map(|key| {
                        let cols = key
                            .iter()
                            .map(|c| format!("#{c}"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("[{cols}]")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(def, " keys=[{keys}]").unwrap();
            }
            def.push('\n');
            for (i, col) in typ.column_types.iter().enumerate() {
                let col = ReprColumnType::from(col);
                writeln!(
                    def,
                    "  - c{i}: {}{}",
                    print_repr_scalar_type(&col.scalar_type)?,
                    if col.nullable { "?" } else { "" }
                )
                .unwrap();
            }

            let output = mz_expr_parser::handle_define(&mut self.new_cat_runtime, &def);
            let cols = (0..typ.column_types.len())
                .map(|i| format!("c{i}"))
                .collect();
            self.new_cat_check.insert(name, cols, typ.clone(), false)?;
            blocks.push(("define".to_string(), def, output));
        }
        Ok(blocks)
    }

    /// Converts a `build`/`opt` case, returning the new directive line and
    /// input block.
    fn convert_rel_case(
        &self,
        context: &str,
        directive: &str,
        args: &[(String, String)],
        input: &str,
    ) -> Result<(String, String), String> {
        let relation = mz_expr_test_util::build_rel(input, &self.old_cat)?;

        // Print the plan in spec syntax and prove the roundtrip.
        let mut printed = String::new();
        print_mir(&self.new_cat_check, &relation, 0, &mut printed)
            .map_err(|err| format!("{err}\nwhile printing:\n{input}"))?;
        let reparsed = mz_expr_parser::try_parse_mir(&self.new_cat_check, &printed)
            .map_err(|err| format!("cannot reparse:\n{printed}\n{err}"))?;
        assert_eq!(
            reparsed, relation,
            "roundtrip drift in {context}:\n{printed}"
        );

        // Map the directive to the new harness.
        let apply_names: Vec<String> = args
            .iter()
            .find(|(k, _)| k == "apply")
            .map(|(_, v)| {
                v.trim_matches(|c| c == '(' || c == ')')
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect()
            })
            .unwrap_or_default();
        let will_distinct = args
            .iter()
            .any(|(k, v)| k == "enable_will_distinct_propagation" && v == "true");
        let format_types = args.iter().any(|(k, v)| k == "format" && v == "types");

        let new_directive = match directive {
            "opt" => {
                check_feature_flag_drift(context, &full_transform_list(), will_distinct, &relation);
                "apply pipeline=optimize".to_string()
            }
            "build" if format_types && apply_names.is_empty() => {
                "explain with=(types, keys)".to_string()
            }
            "build" => {
                let transforms = get_transforms(&apply_names);
                check_feature_flag_drift(context, &transforms, will_distinct, &relation);
                let mut directive = match apply_names.len() {
                    0 => "apply pipeline=identity".to_string(),
                    1 => format!("apply pipeline={}", to_snake_case(&apply_names[0])),
                    _ => {
                        let names = apply_names
                            .iter()
                            .map(|n| to_snake_case(n))
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("apply pipeline=({names})")
                    }
                };
                if will_distinct {
                    directive.push_str(" enable_will_distinct_propagation=true");
                }
                directive
            }
            directive => Err(format!("unknown directive {directive}"))?,
        };

        Ok((new_directive, printed))
    }
}

fn ids(cat: &mz_expr_test_util::TestCatalog) -> Vec<GlobalId> {
    // The old catalog has no id iterator, so probe the dense User id space.
    (0u64..)
        .map(GlobalId::User)
        .take_while(|id| cat.get_source_name(id).is_some())
        .collect()
}

fn get_source<'a>(
    cat: &'a mz_expr_test_util::TestCatalog,
    name: &str,
) -> &'a (GlobalId, mz_repr::SqlRelationType) {
    cat.get(name).expect("source exists")
}

#[mz_ore::test]
#[ignore = "one-shot conversion tool"]
fn convert() {
    let files = [
        "filter",
        "join-implementation",
        "let-get",
        "lifting",
        "non_null_requirements",
        "predicate-pushdown",
        "projection-extraction",
        "redundant_join",
        "t",
        "topk",
        "topk_fusion",
        "typ",
        "union-fusion",
        "will_distinct",
        "canonicalize_mfp",
        "reduction-pushdown",
        "partial-reduction-pushdown",
        "projection-pushdown",
    ];
    for file in files {
        let contents = std::fs::read_to_string(format!("tests/testdata/{file}")).unwrap();
        let mut converter = Converter {
            old_cat: mz_expr_test_util::TestCatalog::default(),
            new_cat_runtime: mz_expr_parser::TestCatalog::default(),
            new_cat_check: mz_expr_parser::TestCatalog::default(),
        };
        let mut out = String::new();
        for (i, case) in split_cases(&contents).into_iter().enumerate() {
            let context = format!("{file}:{i}");
            out.push_str(&case.preamble);
            if case.directive.is_empty() {
                continue;
            }
            let (directive, args) = parse_directive_line(&case.directive);
            match directive.as_str() {
                "cat" => {
                    let blocks = converter
                        .convert_cat(&case.input)
                        .unwrap_or_else(|err| panic!("{context}: {err}"));
                    for (directive, input, output) in blocks {
                        writeln!(out, "{directive}").unwrap();
                        out.push_str(&input);
                        writeln!(out, "----").unwrap();
                        out.push_str(&output);
                        writeln!(out).unwrap();
                    }
                }
                "build" | "opt" => {
                    let (directive, input) = converter
                        .convert_rel_case(&context, &directive, &args, &case.input)
                        .unwrap_or_else(|err| panic!("{context}: {err}"));
                    writeln!(out, "{directive}").unwrap();
                    out.push_str(&input);
                    writeln!(out, "----").unwrap();
                    out.push_str(&case.expected);
                    writeln!(out).unwrap();
                }
                directive => panic!("{context}: unknown directive {directive}"),
            }
        }
        // Drop the extra trailing blank line added after the last case.
        if out.ends_with("\n\n") {
            out.pop();
        }
        // Migrated cases go to their own file. The prefix is only added when
        // a hand-written spec file of the same name already exists.
        let name = file.replace('-', "_");
        let path = format!("tests/test_transforms/{name}.spec");
        let path = if std::fs::exists(&path).unwrap() {
            format!("tests/test_transforms/migrated_mzreflect_{name}.spec")
        } else {
            path
        };
        std::fs::write(path, out).unwrap();
    }
}
