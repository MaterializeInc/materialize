// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Arity and shape rules.
//!
//! Each function checks one operator's column references and parallel structures against
//! the arities of its inputs, and returns the operator's output arity. An input arity of
//! `None` stands for an input that produces no rows, whose arity the plan does not fix.
//! Checks that need such an arity are skipped, because no datum ever reaches them.
//!
//! An operator that reads an input through an arrangement sees the arranged layout, the key
//! columns followed by the thinned value columns. Its width comes from
//! [`arranged_width`], which assumes the canonical thinning of
//! `permutation_for_arrangement`. That covers the arranged inputs of `Get`, `Mfp`,
//! `FlatMap`, `Reduce`, `ArrangeBy`, and join sources, and the lookup value layout of each
//! join stage, whose width is the lookup key plus the canonical thinning of the lookup input.
//! A join stage's stream layout instead comes from its stated `stream_thinning`. The
//! arrangement rules check that the triples of `ArrangeBy` forms and threshold plans, and
//! every join `stream_thinning`, are canonical, so every arrangement has the canonical
//! layout.

use std::collections::BTreeSet;

use mz_expr::{Columns, MapFilterProject, MfpPlan, OptimizableExpr, TableFunc};

use crate::plan::join::{DeltaJoinPlan, JoinClosure, JoinPlan, LinearJoinPlan};
use crate::plan::reduce::{
    AccumulablePlan, BasicPlan, HierarchicalPlan, KeyValPlan, LirAggregateExpr, ReducePlan,
};
use crate::plan::scalar::LirScalarExpr;
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::{AvailableCollections, GetPlan};

use super::Rule;

/// Rule violations found in one operator.
pub(super) type Issues = Vec<(Rule, String)>;

/// The width of the rows an arrangement of a collection with `arity` columns by `key`
/// presents: the key columns followed by the input columns the key does not reference.
///
/// Matches `key.len() + thinning.len()` for the thinning `permutation_for_arrangement`
/// produces, without allocating `arity` entries.
pub(super) fn arranged_width(key: &[LirScalarExpr], arity: usize) -> usize {
    let in_key: BTreeSet<usize> = key
        .iter()
        .filter_map(|k| k.as_column())
        .filter(|c| *c < arity)
        .collect();
    key.len().saturating_add(arity - in_key.len())
}

/// Reports every column `expr` references at or beyond `bound`.
pub(super) fn check_columns<E: Columns>(issues: &mut Issues, what: &str, expr: &E, bound: usize) {
    if let Some(column) = expr.support().into_iter().find(|c| *c >= bound) {
        issues.push((
            Rule::Arity,
            format!("{what} references column {column}, which is out of range for arity {bound}"),
        ));
    }
}

/// Checks a key over a collection of the given arity.
fn check_key(issues: &mut Issues, what: &str, key: &[LirScalarExpr], arity: Option<usize>) {
    if let Some(arity) = arity {
        for k in key {
            check_columns(issues, what, k, arity);
        }
    }
}

/// Checks the column references of `(key, permutation, thinning)` triples over a
/// collection of the given arity.
fn check_triples(
    issues: &mut Issues,
    what: &str,
    keys: &AvailableCollections,
    arity: Option<usize>,
) {
    let Some(arity) = arity else { return };
    for (key, permutation, thinning) in &keys.arranged {
        check_key(issues, &format!("{what} key"), key, Some(arity));
        if let Some(c) = thinning.iter().find(|c| **c >= arity) {
            issues.push((
                Rule::Arity,
                format!(
                    "{what} thinning references column {c}, which is out of range for arity {arity}"
                ),
            ));
        }
        let width = key.len().saturating_add(thinning.len());
        if let Some(c) = permutation.iter().find(|c| **c >= width) {
            issues.push((
                Rule::Arity,
                format!(
                    "{what} permutation references position {c}, which is out of range for an arrangement of width {width}"
                ),
            ));
        }
    }
}

/// Checks a `MapFilterProject` against the width of the rows it is applied to, and returns
/// the number of columns available to its projection.
///
/// Evaluation pushes expressions in order and evaluates each predicate once the row has as
/// many columns as the predicate's position, indexing `datums` without bounds checks of its
/// own. So an expression may reference only earlier columns, and a predicate only columns
/// before its position.
pub(super) fn check_mfp<E: OptimizableExpr>(
    issues: &mut Issues,
    what: &str,
    mfp: &MapFilterProject<E>,
    input_width: Option<usize>,
) -> usize {
    if let Some(width) = input_width {
        if mfp.input_arity != width {
            issues.push((
                Rule::Arity,
                format!(
                    "{what} expects input arity {}, but its input has arity {width}",
                    mfp.input_arity
                ),
            ));
        }
    }
    let base = mfp.input_arity;
    for (i, expr) in mfp.expressions.iter().enumerate() {
        check_columns(
            issues,
            &format!("{what} expression {i}"),
            expr,
            base.saturating_add(i),
        );
    }
    let total = base.saturating_add(mfp.expressions.len());
    for (position, predicate) in &mfp.predicates {
        if *position > total {
            issues.push((
                Rule::Shape,
                format!("{what} predicate position {position} is beyond its {total} columns"),
            ));
        }
        if let Some(column) = predicate.support().into_iter().find(|c| c >= position) {
            issues.push((
                Rule::Shape,
                format!(
                    "{what} predicate at position {position} references column {column}, which is not computed until later"
                ),
            ));
        }
    }
    if let Some(c) = mfp.projection.iter().find(|c| **c >= total) {
        issues.push((
            Rule::Arity,
            format!(
                "{what} projection references column {c}, which is out of range for arity {total}"
            ),
        ));
    }
    total
}

/// Checks an `MfpPlan`, whose temporal bounds are evaluated after all its expressions.
fn check_mfp_plan(
    issues: &mut Issues,
    what: &str,
    plan: &MfpPlan<LirScalarExpr>,
    input_width: Option<usize>,
) {
    let (mfp, lower, upper) = plan.as_parts();
    let total = check_mfp(issues, what, &**mfp, input_width);
    for bound in lower.iter().chain(upper) {
        check_columns(issues, &format!("{what} temporal bound"), bound, total);
    }
}

/// The width an operator reading `input_key` of an input with `arity` columns sees.
pub(super) fn read_width(
    input_key: Option<&[LirScalarExpr]>,
    arity: Option<usize>,
) -> Option<usize> {
    match input_key {
        None => arity,
        Some(key) => arity.map(|a| arranged_width(key, a)),
    }
}

/// `Get`: returns the output arity, given the arity of the bound collection.
pub(super) fn get(
    issues: &mut Issues,
    keys: &AvailableCollections,
    plan: &GetPlan,
    arity: Option<usize>,
) -> Option<usize> {
    check_triples(issues, "Get", keys, arity);
    match plan {
        GetPlan::PassArrangements => arity,
        GetPlan::Arrangement(key, _val, mfp) => {
            check_key(issues, "Get arrangement key", key, arity);
            check_mfp_plan(issues, "Get MFP", mfp, read_width(Some(key), arity));
            Some(projection_len(mfp))
        }
        GetPlan::Collection(mfp) => {
            check_mfp_plan(issues, "Get MFP", mfp, arity);
            Some(projection_len(mfp))
        }
    }
}

/// `Mfp`: returns the output arity.
pub(super) fn mfp(
    issues: &mut Issues,
    mfp: &MfpPlan<LirScalarExpr>,
    input_key_val: Option<&[LirScalarExpr]>,
    arity: Option<usize>,
) -> Option<usize> {
    check_key(
        issues,
        "Mfp input key",
        input_key_val.unwrap_or_default(),
        arity,
    );
    check_mfp_plan(issues, "Mfp", mfp, read_width(input_key_val, arity));
    // Rendering hands the input through unchanged when the MFP is the identity.
    if mfp.is_identity() {
        arity
    } else {
        Some(projection_len(mfp))
    }
}

/// The number of columns an `MfpPlan` produces.
pub(super) fn projection_len(plan: &MfpPlan<LirScalarExpr>) -> usize {
    plan.as_parts().0.projection.len()
}

/// `FlatMap`: returns the output arity.
pub(super) fn flat_map(
    issues: &mut Issues,
    input_key: Option<&[LirScalarExpr]>,
    exprs: &[LirScalarExpr],
    func: &TableFunc,
    mfp_after: &MfpPlan<LirScalarExpr>,
    arity: Option<usize>,
) -> Option<usize> {
    check_key(
        issues,
        "FlatMap input key",
        input_key.unwrap_or_default(),
        arity,
    );
    let width = read_width(input_key, arity);
    if let Some(width) = width {
        for (i, expr) in exprs.iter().enumerate() {
            check_columns(issues, &format!("FlatMap argument {i}"), expr, width);
        }
    }
    let Some(func_arity) = func.checked_output_arity() else {
        issues.push((
            Rule::Arity,
            "FlatMap table function arity overflows".to_string(),
        ));
        return None;
    };
    // The MFP sees the input row followed by the table function's output.
    let mfp_width = width.map(|w| w.saturating_add(func_arity));
    check_mfp_plan(issues, "FlatMap MFP", mfp_after, mfp_width);
    Some(projection_len(mfp_after))
}

/// Checks a join closure applied to rows of `width` columns, and returns its output arity.
fn check_closure(
    issues: &mut Issues,
    what: &str,
    closure: &JoinClosure,
    width: Option<usize>,
) -> usize {
    for class in &closure.ready_equivalences {
        // `JoinClosure::apply` reads the first expression of each class unconditionally.
        if class.is_empty() {
            issues.push((
                Rule::Shape,
                format!("{what} has an empty equivalence class"),
            ));
        }
        if let Some(width) = width {
            for expr in class {
                check_columns(issues, &format!("{what} equivalence"), expr, width);
            }
        }
    }
    check_mfp(issues, what, &*closure.before, width);
    closure.before.projection.len()
}

/// The arity of join input `index`, reporting an index that names no input.
fn join_input(
    issues: &mut Issues,
    what: &str,
    index: usize,
    inputs: &[Option<usize>],
) -> Result<Option<usize>, ()> {
    match inputs.get(index) {
        Some(arity) => Ok(*arity),
        None => {
            issues.push((
                Rule::Shape,
                format!(
                    "{what} {index} does not exist, the join has {} inputs",
                    inputs.len()
                ),
            ));
            Err(())
        }
    }
}

/// One join path: a source, optionally arranged, and stages that each look up one input.
pub(super) struct JoinPath<'a> {
    pub(super) source_relation: usize,
    pub(super) source_key: Option<&'a [LirScalarExpr]>,
    pub(super) initial_closure: Option<&'a JoinClosure>,
    pub(super) stages: Vec<JoinStage<'a>>,
    pub(super) final_closure: Option<&'a JoinClosure>,
}

/// One stage of a join path.
pub(super) struct JoinStage<'a> {
    pub(super) lookup_relation: usize,
    pub(super) stream_key: &'a [LirScalarExpr],
    pub(super) stream_thinning: &'a [usize],
    pub(super) lookup_key: &'a [LirScalarExpr],
    pub(super) closure: &'a JoinClosure,
}

impl<'a> JoinPath<'a> {
    pub(super) fn linear(plan: &'a LinearJoinPlan) -> Self {
        JoinPath {
            source_relation: plan.source_relation,
            source_key: plan.source_key.as_deref(),
            initial_closure: plan.initial_closure.as_ref(),
            stages: plan
                .stage_plans
                .iter()
                .map(|s| JoinStage {
                    lookup_relation: s.lookup_relation,
                    stream_key: &s.stream_key,
                    stream_thinning: &s.stream_thinning,
                    lookup_key: &s.lookup_key,
                    closure: &s.closure,
                })
                .collect(),
            final_closure: plan.final_closure.as_ref(),
        }
    }

    pub(super) fn delta(plan: &'a DeltaJoinPlan) -> Vec<Self> {
        plan.path_plans
            .iter()
            .map(|p| JoinPath {
                source_relation: p.source_relation,
                source_key: p.source_key.as_deref(),
                initial_closure: Some(&p.initial_closure),
                stages: p
                    .stage_plans
                    .iter()
                    .map(|s| JoinStage {
                        lookup_relation: s.lookup_relation,
                        stream_key: &s.stream_key,
                        stream_thinning: &s.stream_thinning,
                        lookup_key: &s.lookup_key,
                        closure: &s.closure,
                    })
                    .collect(),
                final_closure: p.final_closure.as_ref(),
            })
            .collect()
    }

    /// Checks the path and returns the arity of its output.
    ///
    /// The stream starts as the source's rows, raw or in the arranged layout of
    /// `source_key`. A stage arranges the stream by `stream_key` with values
    /// `stream_thinning`, and its closure sees the key, the stream values, and the lookup
    /// input's thinned values, in that order.
    fn check(&self, issues: &mut Issues, what: &str, inputs: &[Option<usize>]) -> Option<usize> {
        let source = join_input(
            issues,
            &format!("{what} source relation"),
            self.source_relation,
            inputs,
        )
        .ok()?;
        check_key(
            issues,
            &format!("{what} source key"),
            self.source_key.unwrap_or_default(),
            source,
        );
        let mut stream = read_width(self.source_key, source);
        if let Some(closure) = self.initial_closure {
            stream = Some(check_closure(
                issues,
                &format!("{what} initial closure"),
                closure,
                stream,
            ));
        }
        for (i, stage) in self.stages.iter().enumerate() {
            let what = format!("{what} stage {i}");
            let lookup = join_input(
                issues,
                &format!("{what} lookup relation"),
                stage.lookup_relation,
                inputs,
            )
            .ok()?;
            check_key(
                issues,
                &format!("{what} stream key"),
                stage.stream_key,
                stream,
            );
            check_key(
                issues,
                &format!("{what} lookup key"),
                stage.lookup_key,
                lookup,
            );
            if let Some(stream) = stream {
                if let Some(c) = stage.stream_thinning.iter().find(|c| **c >= stream) {
                    issues.push((
                        Rule::Arity,
                        format!(
                            "{what} stream thinning references column {c}, which is out of range for arity {stream}"
                        ),
                    ));
                }
            }
            if stage.stream_key.len() != stage.lookup_key.len() {
                issues.push((
                    Rule::Shape,
                    format!(
                        "{what} stream key has {} columns, but the lookup key has {}",
                        stage.stream_key.len(),
                        stage.lookup_key.len()
                    ),
                ));
            }
            // The lookup arrangement's values are its row minus the key columns.
            let lookup_values =
                lookup.map(|a| arranged_width(stage.lookup_key, a) - stage.lookup_key.len());
            let width = lookup_values.map(|v| {
                stage
                    .stream_key
                    .len()
                    .saturating_add(stage.stream_thinning.len())
                    .saturating_add(v)
            });
            stream = Some(check_closure(
                issues,
                &format!("{what} closure"),
                stage.closure,
                width,
            ));
        }
        if let Some(closure) = self.final_closure {
            stream = Some(check_closure(
                issues,
                &format!("{what} final closure"),
                closure,
                stream,
            ));
        }
        stream
    }
}

/// `Join`: returns the output arity, given the arities of the inputs.
pub(super) fn join(
    issues: &mut Issues,
    plan: &JoinPlan,
    inputs: &[Option<usize>],
) -> Option<usize> {
    match plan {
        JoinPlan::Linear(plan) => JoinPath::linear(plan).check(issues, "linear join", inputs),
        JoinPlan::Delta(plan) => {
            let mut output = None;
            for (i, path) in JoinPath::delta(plan).iter().enumerate() {
                let what = format!("delta join path {i}");
                let Some(arity) = path.check(issues, &what, inputs) else {
                    continue;
                };
                match output {
                    None => output = Some(arity),
                    Some(expected) if expected != arity => issues.push((
                        Rule::Arity,
                        format!(
                            "{what} produces arity {arity}, but path 0 produces arity {expected}"
                        ),
                    )),
                    Some(_) => {}
                }
            }
            output
        }
    }
}

/// The aggregates a `ReducePlan` computes, in output order.
fn reduce_aggregates(plan: &ReducePlan) -> Vec<Option<&LirAggregateExpr>> {
    match plan {
        ReducePlan::Distinct => vec![],
        ReducePlan::Accumulable(AccumulablePlan { full_aggrs, .. }) => {
            full_aggrs.iter().map(Some).collect()
        }
        // Hierarchical plans keep only the functions.
        ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(p)) => vec![None; p.aggr_funcs.len()],
        ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(p)) => vec![None; p.aggr_funcs.len()],
        ReducePlan::Basic(BasicPlan::Single(p)) => vec![Some(&p.expr)],
        ReducePlan::Basic(BasicPlan::Multiple(aggrs)) => aggrs.iter().map(Some).collect(),
    }
}

/// `Reduce`: returns the output arity.
///
/// The key plan and the value plan read the input row. The value plan produces one column
/// per aggregate, which the plans read by position. The output is the key followed by the
/// aggregates, subjected to `mfp_after`.
pub(super) fn reduce(
    issues: &mut Issues,
    input_key: Option<&[LirScalarExpr]>,
    key_val_plan: &KeyValPlan,
    plan: &ReducePlan,
    mfp_after: &MapFilterProject<LirScalarExpr>,
    arity: Option<usize>,
) -> Option<usize> {
    check_key(
        issues,
        "Reduce input key",
        input_key.unwrap_or_default(),
        arity,
    );
    let width = read_width(input_key, arity);
    check_mfp(issues, "Reduce key plan", &*key_val_plan.key_plan, width);
    check_mfp(issues, "Reduce value plan", &*key_val_plan.val_plan, width);
    let key_arity = key_val_plan.key_plan.projection.len();

    let aggregates = reduce_aggregates(plan);
    let count = aggregates.len();
    // `LirAggregateExpr::expr` is the aggregate's argument over the logical input row. Rendering
    // reads the value plan's output instead, but the expression is still shown in `EXPLAIN`.
    if let Some(arity) = arity {
        for (i, aggregate) in aggregates.iter().enumerate() {
            if let Some(aggregate) = aggregate {
                check_columns(
                    issues,
                    &format!("Reduce aggregate {i}"),
                    &aggregate.expr,
                    arity,
                );
            }
        }
    }
    let values = key_val_plan.val_plan.projection.len();
    if values != count {
        issues.push((
            Rule::Arity,
            format!(
                "Reduce value plan produces {values} columns, but the plan has {count} aggregates"
            ),
        ));
    }
    if let ReducePlan::Accumulable(p) = plan {
        // Rendering walks the value row forward to each simple aggregate's position.
        let mut previous: Option<usize> = None;
        for (index, _) in &p.simple_aggrs {
            if previous.is_some_and(|p| p >= *index) {
                issues.push((
                    Rule::Shape,
                    "Reduce simple aggregate positions are not strictly increasing".to_string(),
                ));
            }
            previous = Some(*index);
        }
        for (index, _) in p.simple_aggrs.iter().chain(&p.distinct_aggrs) {
            if *index >= count {
                issues.push((
                    Rule::Arity,
                    format!(
                        "Reduce aggregate position {index} is out of range for {count} aggregates"
                    ),
                ));
            }
        }
    }

    let mfp_width = key_arity.saturating_add(count);
    check_mfp(issues, "Reduce MFP", mfp_after, Some(mfp_width));
    // The output arrangement is keyed by the first `key_arity` columns, which rendering takes
    // from the key and not from `mfp_after`.
    if !mfp_after
        .projection
        .iter()
        .copied()
        .take(key_arity)
        .eq(0..key_arity)
    {
        issues.push((
            Rule::Shape,
            format!("Reduce MFP does not preserve the {key_arity} key columns as a prefix"),
        ));
    }
    if matches!(plan, ReducePlan::Distinct) && mfp_after.projection.len() != key_arity {
        // A distinct reduction applies `mfp_after` as a filter and outputs only the key.
        issues.push((
            Rule::Shape,
            "Reduce MFP of a distinct reduction projects more than the key".to_string(),
        ));
    }
    Some(mfp_after.projection.len())
}

/// `TopK`: returns the output arity, which is the input arity.
pub(super) fn top_k(issues: &mut Issues, plan: &TopKPlan, arity: Option<usize>) -> Option<usize> {
    let (group_key, order_key, limit, plan_arity) = match plan {
        TopKPlan::MonotonicTop1(p) => (&p.group_key, &p.order_key, None, p.arity),
        TopKPlan::MonotonicTopK(p) => (&p.group_key, &p.order_key, p.limit.as_ref(), p.arity),
        TopKPlan::Basic(p) => (&p.group_key, &p.order_key, p.limit.as_ref(), p.arity),
    };
    let Some(arity) = arity else {
        return Some(plan_arity);
    };
    if plan_arity != arity {
        issues.push((
            Rule::Arity,
            format!("TopK plan states arity {plan_arity}, but its input has arity {arity}"),
        ));
    }
    if let Some(c) = group_key.iter().find(|c| **c >= arity) {
        issues.push((
            Rule::Arity,
            format!(
                "TopK group key references column {c}, which is out of range for arity {arity}"
            ),
        ));
    }
    if let Some(o) = order_key.iter().find(|o| o.column >= arity) {
        issues.push((
            Rule::Arity,
            format!(
                "TopK order key references column {}, which is out of range for arity {arity}",
                o.column
            ),
        ));
    }
    if let Some(limit) = limit {
        check_columns(issues, "TopK limit", limit, arity);
        // Rendering rewrites the limit to read the group key, and indexes a map of the group key
        // columns with each column the limit references.
        if let Some(c) = limit.support().into_iter().find(|c| !group_key.contains(c)) {
            issues.push((
                Rule::Arity,
                format!("TopK limit references column {c}, which is not in the group key"),
            ));
        }
    }
    Some(arity)
}

/// `Threshold`: returns the output arity, which is the input arity.
pub(super) fn threshold(
    issues: &mut Issues,
    plan: &ThresholdPlan,
    arity: Option<usize>,
) -> Option<usize> {
    let ThresholdPlan::Basic(plan) = plan;
    let (key, permutation, thinning) = &plan.ensure_arrangement;
    let keys = AvailableCollections {
        raw: false,
        arranged: vec![(key.clone(), permutation.clone(), thinning.clone())],
    };
    check_triples(issues, "Threshold arrangement", &keys, arity);
    arity
}

/// `ArrangeBy`: returns the output arity, which is the input arity.
///
/// Rendering applies `input_mfp` only when it has to form the raw collection from the
/// arrangement named by `input_key`, and the formed rows replace the input's rows.
pub(super) fn arrange_by(
    issues: &mut Issues,
    input_key: Option<&[LirScalarExpr]>,
    input_mfp: &MfpPlan<LirScalarExpr>,
    forms: &AvailableCollections,
    arity: Option<usize>,
) -> Option<usize> {
    check_key(
        issues,
        "ArrangeBy input key",
        input_key.unwrap_or_default(),
        arity,
    );
    check_mfp_plan(
        issues,
        "ArrangeBy input MFP",
        input_mfp,
        read_width(input_key, arity),
    );
    if let Some(arity) = arity {
        let produced = projection_len(input_mfp);
        if produced != arity {
            issues.push((
                Rule::Arity,
                format!(
                    "ArrangeBy input MFP produces arity {produced}, but its input has arity {arity}"
                ),
            ));
        }
    }
    check_triples(issues, "ArrangeBy", forms, arity);
    arity
}

/// Checks an export's arrangement key over the arity of the exported collection.
pub(super) fn export_key<E: Columns>(issues: &mut Issues, what: &str, key: &[E], arity: usize) {
    for k in key {
        check_columns(issues, what, k, arity);
    }
}

/// Checks a sink's `from_arrangement`, which reconstructs rows of `arity` columns from an
/// arrangement of the sinked collection by `key`.
pub(super) fn sink_arrangement(
    issues: &mut Issues,
    key: &[LirScalarExpr],
    mfp: &MfpPlan<LirScalarExpr>,
    arity: usize,
) {
    check_key(issues, "sink arrangement key", key, Some(arity));
    check_mfp_plan(issues, "sink MFP", mfp, Some(arranged_width(key, arity)));
    let produced = projection_len(mfp);
    if produced != arity {
        issues.push((
            Rule::Arity,
            format!(
                "sink MFP produces arity {produced}, but the sinked collection has arity {arity}"
            ),
        ));
    }
}
