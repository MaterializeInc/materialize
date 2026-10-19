// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Arrangement availability rules.
//!
//! Rendering hands each operator's output to its consumers as a `CollectionBundle`, which
//! holds the raw collection, arrangements looked up by exact key equality, or both. A
//! consumer that reads a form the bundle lacks panics while the dataflow is built, whether or
//! not the collection has data, so these rules run for empty inputs too. Each function
//! simulates one operator: it checks the operator's reads against the [`Available`] forms of
//! its inputs and returns the forms rendering gives its output.
//!
//! Arrangements are tracked by key alone, because rendering never reads the permutation and
//! thinning a plan states for an existing arrangement. Every `(key, permutation, thinning)`
//! triple a plan states must instead be the one `permutation_for_arrangement` produces, and so
//! must every join stream thinning. Index imports are assumed to have that layout too, as
//! lowering assumes. Every arrangement then has the canonical layout of its key, which is what
//! the arity rules compute widths from.

use std::collections::BTreeSet;

use mz_expr::{Columns, permutation_for_arrangement};

use crate::plan::join::JoinPlan;
use crate::plan::scalar::LirScalarExpr;
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::{AvailableCollections, GetPlan};

use super::Rule;
use super::arity::{Issues, JoinPath, arranged_width, read_width};

/// The forms in which a collection is available to its consumers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Available {
    /// Whether the raw collection exists.
    pub raw: bool,
    /// The keys of the existing arrangements.
    pub keys: BTreeSet<Vec<LirScalarExpr>>,
}

impl Available {
    pub fn raw() -> Self {
        Available {
            raw: true,
            keys: BTreeSet::new(),
        }
    }

    pub fn arranged(key: Vec<LirScalarExpr>) -> Self {
        Available {
            raw: false,
            keys: BTreeSet::from([key]),
        }
    }

    fn columns(columns: impl IntoIterator<Item = usize>) -> Self {
        Self::arranged(columns.into_iter().map(LirScalarExpr::column).collect())
    }
}

/// A key for error messages, without printing expressions, whose formatting recurses.
pub(super) fn key_name(key: &[LirScalarExpr]) -> String {
    let parts: Vec<String> = key
        .iter()
        .map(|k| match k.as_column() {
            Some(c) => format!("#{c}"),
            None => "<expression>".to_string(),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

/// Reports a read of the raw collection of an input that has none.
fn require_raw(issues: &mut Issues, what: &str, input: &Available) {
    if !input.raw {
        issues.push((
            Rule::Arrangement,
            format!("{what} reads the raw collection of its input, but the input is only arranged"),
        ));
    }
}

/// Reports a read of an arrangement an input does not have.
fn require_key(issues: &mut Issues, what: &str, input: &Available, key: &[LirScalarExpr]) {
    if !input.keys.contains(key) {
        let key = key_name(key);
        issues.push((
            Rule::Arrangement,
            format!(
                "{what} reads the arrangement by {key}, but its input is not arranged by {key}"
            ),
        ));
    }
}

/// Reports a read of `input_key` of an input, or of its raw collection if `None`.
fn require_form(
    issues: &mut Issues,
    what: &str,
    input: &Available,
    input_key: Option<&[LirScalarExpr]>,
) {
    match input_key {
        Some(key) => require_key(issues, what, input, key),
        None => require_raw(issues, what, input),
    }
}

/// The canonical thinning of an arrangement by `key` of rows with `arity` columns, or `None`
/// if it does not have `len` entries.
///
/// Compares lengths before allocating, so an `arity` taken from a plan's stated field never
/// allocates more than `len` plus the key's length.
fn canonical_thinning(key: &[LirScalarExpr], arity: usize, len: usize) -> Option<Vec<usize>> {
    if arranged_width(key, arity) - key.len() != len {
        return None;
    }
    Some(permutation_for_arrangement(key, arity).1)
}

/// Checks that every triple of `forms` is the canonical layout of its key over `arity` columns.
pub(super) fn check_triples(
    issues: &mut Issues,
    what: &str,
    forms: &AvailableCollections,
    arity: Option<usize>,
) {
    let Some(arity) = arity else { return };
    for (key, permutation, thinning) in &forms.arranged {
        // The permutation has one entry per column, so its length bounds `arity` before
        // `permutation_for_arrangement` allocates that many.
        let canonical = (permutation.len() == arity)
            .then(|| permutation_for_arrangement(key, arity))
            .filter(|(p, t)| p == permutation && t == thinning);
        if canonical.is_none() {
            let expected = if permutation.len() == arity {
                let (p, t) = permutation_for_arrangement(key, arity);
                format!("permutation {p:?} and thinning {t:?}")
            } else {
                format!("a permutation of {arity} entries")
            };
            issues.push((
                Rule::Arrangement,
                format!(
                    "{what} arrangement by {} has permutation {permutation:?} and thinning {thinning:?}, but the canonical layout for arity {arity} has {expected}",
                    key_name(key)
                ),
            ));
        }
    }
}

/// `Get`: returns the forms of the output, given those of the bound collection.
pub(super) fn get(
    issues: &mut Issues,
    keys: &AvailableCollections,
    plan: &GetPlan,
    input: &Available,
    arity: Option<usize>,
) -> Available {
    check_triples(issues, "Get", keys, arity);
    match plan {
        GetPlan::PassArrangements => {
            for (key, _, _) in &keys.arranged {
                require_key(issues, "Get", input, key);
            }
            if keys.raw {
                require_raw(issues, "Get", input);
            }
            // Rendering keeps the raw collection whether or not `keys` asks for it, and drops
            // the arrangements `keys` does not name.
            let output = Available {
                raw: input.raw,
                keys: input
                    .keys
                    .iter()
                    .filter(|key| keys.arranged.iter().any(|(k, _, _)| k == *key))
                    .cloned()
                    .collect(),
            };
            // Rendering attaches hydration logging to some form of every node's output, and
            // panics if there is none.
            if !output.raw && output.keys.is_empty() {
                issues.push((
                    Rule::Arrangement,
                    "Get passes on no form of its input".to_string(),
                ));
            }
            output
        }
        GetPlan::Arrangement(key, _, _) => {
            require_key(issues, "Get", input, key);
            Available::raw()
        }
        GetPlan::Collection(_) => {
            require_raw(issues, "Get", input);
            Available::raw()
        }
    }
}

/// `Mfp`: returns the forms of the output.
pub(super) fn mfp(
    issues: &mut Issues,
    is_identity: bool,
    input_key: Option<&[LirScalarExpr]>,
    input: &Available,
) -> Available {
    // Rendering hands the input through unchanged when the MFP is the identity, without
    // reading `input_key`.
    if is_identity {
        return input.clone();
    }
    require_form(issues, "Mfp", input, input_key);
    Available::raw()
}

/// `FlatMap`: returns the forms of the output.
pub(super) fn flat_map(
    issues: &mut Issues,
    input_key: Option<&[LirScalarExpr]>,
    input: &Available,
) -> Available {
    require_form(issues, "FlatMap", input, input_key);
    Available::raw()
}

/// `Join`: returns the forms of the output, given those of the inputs.
///
/// Reports nothing for input indices out of range, which the arity rules report.
pub(super) fn join(
    issues: &mut Issues,
    plan: &JoinPlan,
    inputs: &[&Available],
    arities: &[Option<usize>],
) -> Available {
    match plan {
        JoinPlan::Linear(plan) => join_path(
            issues,
            "linear join",
            &JoinPath::linear(plan),
            true,
            inputs,
            arities,
        ),
        JoinPlan::Delta(plan) => {
            for (i, path) in JoinPath::delta(plan).iter().enumerate() {
                let what = format!("delta join path {i}");
                join_path(issues, &what, path, false, inputs, arities);
            }
        }
    }
    Available::raw()
}

/// Checks the reads of one join path.
fn join_path(
    issues: &mut Issues,
    what: &str,
    path: &JoinPath<'_>,
    linear: bool,
    inputs: &[&Available],
    arities: &[Option<usize>],
) {
    let (Some(source), Some(source_arity)) = (
        inputs.get(path.source_relation),
        arities.get(path.source_relation).copied(),
    ) else {
        return;
    };
    // A linear join without an initial closure streams an existing arrangement of its source
    // by the first stage's stream key, whatever `source_key` says, and reads `source_key`
    // only if there is none.
    let streams_arrangement = linear
        && path.initial_closure.is_none()
        && path
            .stages
            .first()
            .is_some_and(|stage| source.keys.contains(stage.stream_key));
    let source_what = format!("{what} source");
    if !streams_arrangement {
        require_form(issues, &source_what, source, path.source_key);
    }

    // The width of the stream each stage arranges by its stream key.
    let mut stream = match path.initial_closure {
        Some(closure) => Some(closure.before.projection.len()),
        // The streamed arrangement has the canonical layout of the source.
        None if streams_arrangement => source_arity,
        None => read_width(path.source_key, source_arity),
    };
    for (i, stage) in path.stages.iter().enumerate() {
        let Some(lookup) = inputs.get(stage.lookup_relation) else {
            return;
        };
        if !lookup.keys.contains(stage.lookup_key) {
            let (r, key) = (stage.lookup_relation, key_name(stage.lookup_key));
            issues.push((
                Rule::Arrangement,
                format!(
                    "{what} stage {i} reads input {r} arranged by {key}, but input {r} is not arranged by {key}"
                ),
            ));
        }
        if let Some(width) = stream {
            let canonical =
                canonical_thinning(stage.stream_key, width, stage.stream_thinning.len());
            if canonical.as_deref() != Some(stage.stream_thinning) {
                issues.push((
                    Rule::Arrangement,
                    format!(
                        "{what} stage {i} has stream thinning {:?}, which is not the canonical thinning of its stream key over {width} columns",
                        stage.stream_thinning
                    ),
                ));
            }
        }
        stream = Some(stage.closure.before.projection.len());
    }
}

/// `Reduce`: returns the forms of the output, an arrangement by its `key_arity` key columns.
pub(super) fn reduce(
    issues: &mut Issues,
    input_key: Option<&[LirScalarExpr]>,
    key_arity: usize,
    input: &Available,
) -> Available {
    require_form(issues, "Reduce", input, input_key);
    Available::columns(0..key_arity)
}

/// `TopK`: returns the forms of the output.
pub(super) fn top_k(issues: &mut Issues, plan: &TopKPlan, input: &Available) -> Available {
    require_raw(issues, "TopK", input);
    match plan {
        TopKPlan::MonotonicTop1(p) => Available::columns(p.group_key.iter().copied()),
        TopKPlan::MonotonicTopK(_) | TopKPlan::Basic(_) => Available::raw(),
    }
}

/// `Threshold`: returns the forms of the output, the arrangement it thresholds.
pub(super) fn threshold(
    issues: &mut Issues,
    plan: &ThresholdPlan,
    input: &Available,
    arity: Option<usize>,
) -> Available {
    let ThresholdPlan::Basic(plan) = plan;
    let (key, permutation, thinning) = &plan.ensure_arrangement;
    let forms = AvailableCollections {
        raw: false,
        arranged: vec![(key.clone(), permutation.clone(), thinning.clone())],
    };
    check_triples(issues, "Threshold", &forms, arity);
    require_key(issues, "Threshold", input, key);
    Available::arranged(key.clone())
}

/// `Negate` and every `Union` input read the raw collection.
pub(super) fn raw_input(issues: &mut Issues, what: &str, input: &Available) {
    require_raw(issues, what, input);
}

/// `ArrangeBy`: returns the forms of the output.
pub(super) fn arrange_by(
    issues: &mut Issues,
    input_key: Option<&[LirScalarExpr]>,
    forms: &AvailableCollections,
    input: &Available,
    arity: Option<usize>,
) -> Available {
    check_triples(issues, "ArrangeBy", forms, arity);
    let mut output = input.clone();
    for (key, _, _) in &forms.arranged {
        // Rendering soft-asserts this, which panics where soft assertions are enabled.
        if input.keys.contains(key) {
            let key = key_name(key);
            issues.push((
                Rule::Arrangement,
                format!(
                    "ArrangeBy forms the arrangement by {key}, but its input is already arranged by {key}"
                ),
            ));
        }
        output.keys.insert(key.clone());
    }
    // Rendering forms the raw collection to build new arrangements from, and keeps it.
    let form_raw = forms.raw
        || forms
            .arranged
            .iter()
            .any(|(key, _, _)| !input.keys.contains(key));
    if form_raw && !input.raw {
        match input_key {
            Some(key) => require_key(issues, "ArrangeBy", input, key),
            None => issues.push((
                Rule::Arrangement,
                "ArrangeBy reads the raw collection of its input to form its output, but the input is only arranged and the ArrangeBy names no input key".to_string(),
            )),
        }
    }
    output.raw |= form_raw;
    output
}
