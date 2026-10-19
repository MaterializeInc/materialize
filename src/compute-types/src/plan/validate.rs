// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Validation of LIR dataflows against what rendering trusts.
//!
//! Rendering assumes that a plan is well formed and panics where it is not. The validator
//! checks a dataflow before it is rendered, so that a plan that passes cannot panic a
//! replica. It never panics itself, on any input.
//!
//! One walk visits every node of every object once, in post-order, and computes a
//! [`NodeInfo`] that describes the node's output. Each rule family checks one aspect of a
//! node against the infos of its inputs. Scoping lives in this module, arity and shape in
//! [`arity`]. Errors accumulate, so one call reports every problem it can find. A node
//! whose input broke a rule is skipped, so a problem is reported once, where it occurs.

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::{Id, LocalId, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_repr::{GlobalId, ReprColumnType, ReprRelationType};

use crate::dataflows::LirDataflowDescription;
use crate::plan::{AvailableCollections, LirId, LirRelationExpr, LirRelationNode};

mod arity;

/// The rule a [`ValidationError`] reports a violation of.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rule {
    /// Identifiers resolve to bindings, imports, or built objects.
    Scoping,
    /// Column references are in range, and arities agree.
    Arity,
    /// Structures have the shape rendering expects, for example equal lengths.
    Shape,
    /// Requested arrangements are available.
    Arrangement,
    /// Expressions are well typed.
    Type,
    /// Functions are admitted for user plans.
    Function,
    /// Operator-specific requirements hold.
    Operator,
    /// Operators that require monotonic input receive it.
    Monotonicity,
    /// The output matches the declared schema.
    Output,
}

/// A violation of a validation rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationError {
    /// The node that breaks the rule, or `None` for a problem of the dataflow as a whole.
    pub lir_id: Option<LirId>,
    /// The name of the node's operator.
    pub operator: &'static str,
    /// The rule that is broken.
    pub rule: Rule,
    /// What is wrong.
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.lir_id {
            Some(lir_id) => write!(
                f,
                "LIR node {lir_id} ({}): {:?}: {}",
                self.operator, self.rule, self.message
            ),
            None => write!(f, "dataflow: {:?}: {}", self.rule, self.message),
        }
    }
}

impl std::error::Error for ValidationError {}

/// Options for [`validate_dataflow`].
#[derive(Clone, Debug, Default)]
pub struct ValidateConfig {
    /// Whether every function application must be admitted for user plans.
    pub enforce_function_allowlist: bool,
}

/// Validates a dataflow, and returns the output type of every built object.
///
/// An object whose output is empty and that has no export has no type of its own and is
/// left out of the result.
///
/// Returns every violation found if the dataflow breaks a rule. Never panics.
pub fn validate_dataflow(
    df: &LirDataflowDescription,
    config: &ValidateConfig,
) -> Result<BTreeMap<GlobalId, ReprRelationType>, Vec<ValidationError>> {
    // No rule in this module consults the function allowlist.
    let ValidateConfig {
        enforce_function_allowlist: _,
    } = config;
    let mut validator = Validator::new();
    let types = match validator.dataflow(df) {
        Ok(types) => types,
        Err(RecursionLimitError { .. }) => {
            validator.errors.push(ValidationError {
                lir_id: None,
                operator: "dataflow",
                rule: Rule::Shape,
                message: format!("plan nests deeper than {RECURSION_LIMIT} operators"),
            });
            BTreeMap::new()
        }
    };
    if validator.errors.is_empty() {
        Ok(types)
    } else {
        Err(validator.errors)
    }
}

/// What the walk knows about the output of a node.
#[derive(Clone, Debug)]
struct NodeInfo {
    /// The number of columns.
    arity: usize,
    /// The type of each column. Empty until type inference fills it.
    types: Vec<ReprColumnType>,
    /// The forms in which the output is available. Raw-less and empty until arrangement
    /// tracking fills it.
    #[allow(dead_code)]
    available: AvailableCollections,
    /// Whether the output never retracts a row. `false` until monotonicity analysis fills it.
    #[allow(dead_code)]
    monotonic: bool,
}

impl NodeInfo {
    fn with_arity(arity: usize) -> Self {
        NodeInfo {
            arity,
            types: Vec::new(),
            available: AvailableCollections::default(),
            monotonic: false,
        }
    }
}

/// The result of visiting a node.
#[derive(Clone, Debug)]
enum Output {
    /// The node or one of its inputs broke a rule, which has been reported.
    Failed,
    /// The node produces no rows, because every path to its output starts at an empty or
    /// error `Constant`. The plan does not fix its arity, and since no datum reaches its
    /// consumers, they check only what does not depend on it.
    Empty,
    /// The node produces rows as described.
    Rows(NodeInfo),
}

impl Output {
    /// The arity of the output, `None` for an empty output, or `Err` for a failed one.
    fn arity(&self) -> Result<Option<usize>, ()> {
        match self {
            Output::Failed => Err(()),
            Output::Empty => Ok(None),
            Output::Rows(info) => Ok(Some(info.arity)),
        }
    }
}

/// The name of an operator in error messages.
fn operator_name(node: &LirRelationNode) -> &'static str {
    match node {
        LirRelationNode::Constant { .. } => "Constant",
        LirRelationNode::Get { .. } => "Get",
        LirRelationNode::Let { .. } => "Let",
        LirRelationNode::LetRec { .. } => "LetRec",
        LirRelationNode::Mfp { .. } => "Mfp",
        LirRelationNode::FlatMap { .. } => "FlatMap",
        LirRelationNode::Join { .. } => "Join",
        LirRelationNode::Reduce { .. } => "Reduce",
        LirRelationNode::TopK { .. } => "TopK",
        LirRelationNode::Negate { .. } => "Negate",
        LirRelationNode::Threshold { .. } => "Threshold",
        LirRelationNode::Union { .. } => "Union",
        LirRelationNode::ArrangeBy { .. } => "ArrangeBy",
    }
}

/// The state of one validation.
struct Validator {
    errors: Vec<ValidationError>,
    recursion_guard: RecursionGuard,
    /// What `Get`s of global ids resolve to: the imported collections, and the objects built
    /// so far.
    globals: BTreeMap<GlobalId, Output>,
    /// The ids of built objects, including those built later than the current one.
    objects: BTreeSet<GlobalId>,
    /// Local bindings in scope, innermost last.
    locals: Vec<(LocalId, Output)>,
    /// Local ids bound so far in the current object.
    bound_locals: BTreeSet<LocalId>,
    /// `LirId`s seen so far in the dataflow.
    lir_ids: BTreeSet<LirId>,
    /// Global ids read by `Get`s that resolved.
    read_globals: BTreeSet<GlobalId>,
}

impl CheckedRecursion for Validator {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl Validator {
    fn new() -> Self {
        Validator {
            errors: Vec::new(),
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
            globals: BTreeMap::new(),
            objects: BTreeSet::new(),
            locals: Vec::new(),
            bound_locals: BTreeSet::new(),
            lir_ids: BTreeSet::new(),
            read_globals: BTreeSet::new(),
        }
    }

    fn dataflow_error(&mut self, rule: Rule, message: String) {
        self.errors.push(ValidationError {
            lir_id: None,
            operator: "dataflow",
            rule,
            message,
        });
    }

    fn node_error(&mut self, expr: &LirRelationExpr, rule: Rule, message: String) {
        self.errors.push(ValidationError {
            lir_id: Some(expr.lir_id),
            operator: operator_name(&expr.node),
            rule,
            message,
        });
    }

    /// Reports `issues` against `expr`, and returns whether there were none.
    fn report(&mut self, expr: &LirRelationExpr, issues: arity::Issues) -> bool {
        let clean = issues.is_empty();
        for (rule, message) in issues {
            self.node_error(expr, rule, message);
        }
        clean
    }

    /// Validates the dataflow and returns the output types of the built objects.
    fn dataflow(
        &mut self,
        df: &LirDataflowDescription,
    ) -> Result<BTreeMap<GlobalId, ReprRelationType>, RecursionLimitError> {
        self.imports(df);

        self.objects = df.objects_to_build.iter().map(|o| o.id).collect();
        let mut types = BTreeMap::new();
        let mut empty_objects = Vec::new();
        for object in &df.objects_to_build {
            self.bound_locals.clear();
            let mut output = self.visit(&object.plan, true)?;
            if let Output::Empty = output {
                match Self::declared_type(df, object.id) {
                    Some(typ) => {
                        let mut info = NodeInfo::with_arity(typ.arity());
                        info.types = typ.column_types;
                        output = Output::Rows(info);
                    }
                    // Consumers of the object may still fix its arity.
                    None => empty_objects.push(object.id),
                }
            }
            if let Output::Rows(info) = &output {
                types.insert(object.id, ReprRelationType::new(info.types.clone()));
            }
            // `DataflowDescription::depends_on`, which rendering calls, asserts that object ids
            // are unique.
            if self.globals.contains_key(&object.id) {
                self.dataflow_error(
                    Rule::Scoping,
                    format!(
                        "object {} has the same id as an import or an earlier object",
                        object.id
                    ),
                );
            } else {
                self.globals.insert(object.id, output);
            }
        }

        // An empty object without export and without a reader has nothing to fix its arity.
        for object in &df.objects_to_build {
            if empty_objects.contains(&object.id) && !self.read_globals.contains(&object.id) {
                self.node_error(
                    &object.plan,
                    Rule::Arity,
                    "cannot infer the arity of an empty constant".to_string(),
                );
            }
        }

        self.exports(df);
        Ok(types)
    }

    /// Registers the imported collections in `globals`.
    ///
    /// Sources are bound at their own ids, and indexes at the ids of the collections they
    /// index. Several imports of one collection must agree on its arity.
    fn imports(&mut self, df: &LirDataflowDescription) {
        let mut arities: BTreeMap<GlobalId, usize> = BTreeMap::new();
        for (id, import) in &df.source_imports {
            let typ_arity = import.desc.typ.arity();
            // The source's operators run inside the source, so `Get`s see their output.
            let arity = match &import.desc.arguments.operators {
                None => typ_arity,
                Some(mfp) => {
                    let mut issues = Vec::new();
                    arity::check_mfp(&mut issues, "source MFP", mfp, Some(typ_arity));
                    for (rule, message) in issues {
                        self.dataflow_error(rule, format!("source import {id}: {message}"));
                    }
                    mfp.projection.len()
                }
            };
            arities.insert(*id, arity);
        }
        for (id, import) in &df.index_imports {
            let on_id = import.desc.on_id;
            let arity = import.typ.arity();
            let mut issues = Vec::new();
            arity::export_key(&mut issues, "index key", &import.desc.key, arity);
            for (rule, message) in issues {
                self.dataflow_error(rule, format!("index import {id}: {message}"));
            }
            match arities.get(&on_id) {
                Some(existing) if *existing != arity => self.dataflow_error(
                    Rule::Arity,
                    format!(
                        "index import {id} gives {on_id} arity {arity}, but another import gives it arity {existing}"
                    ),
                ),
                Some(_) => {}
                None => {
                    arities.insert(on_id, arity);
                }
            }
        }
        for (id, arity) in arities {
            self.globals
                .insert(id, Output::Rows(NodeInfo::with_arity(arity)));
        }
    }

    /// The type the exports of object `id` declare for it, if it has an export.
    fn declared_type(df: &LirDataflowDescription, id: GlobalId) -> Option<ReprRelationType> {
        df.index_exports
            .get(&id)
            .map(|(_, typ)| typ.clone())
            .or_else(|| {
                df.sink_exports
                    .values()
                    .find(|sink| sink.from == id)
                    .map(|sink| ReprRelationType::from(sink.from_desc.typ()))
            })
    }

    /// Checks that exports refer to collections that exist and have the expected arity.
    fn exports(&mut self, df: &LirDataflowDescription) {
        for (id, (desc, typ)) in &df.index_exports {
            // Rendering looks up the arrangement to export at the id of the index.
            if !self.objects.contains(id) {
                self.dataflow_error(
                    Rule::Scoping,
                    format!("index export {id} does not refer to a built object"),
                );
                continue;
            }
            if !self.globals.contains_key(&desc.on_id) {
                self.dataflow_error(
                    Rule::Scoping,
                    format!(
                        "index export {id} is on {}, which is neither an import nor a built object",
                        desc.on_id
                    ),
                );
            }
            let Ok(Some(arity)) = self.globals.get(id).map_or(Err(()), Output::arity) else {
                continue;
            };
            if typ.arity() != arity {
                self.dataflow_error(
                    Rule::Arity,
                    format!(
                        "index export {id} has arity {}, but object {id} has arity {arity}",
                        typ.arity()
                    ),
                );
            }
            let mut issues = Vec::new();
            arity::export_key(&mut issues, "index key", &desc.key, arity);
            for (rule, message) in issues {
                self.dataflow_error(rule, format!("index export {id}: {message}"));
            }
        }
        for (id, sink) in &df.sink_exports {
            let Some(output) = self.globals.get(&sink.from) else {
                self.dataflow_error(
                    Rule::Scoping,
                    format!(
                        "sink export {id} is from {}, which is neither an import nor a built object",
                        sink.from
                    ),
                );
                continue;
            };
            let Ok(Some(arity)) = output.arity() else {
                continue;
            };
            let mut issues = Vec::new();
            let declared = sink.from_desc.arity();
            if declared != arity {
                issues.push((
                    Rule::Arity,
                    format!(
                        "sink has arity {declared}, but {} has arity {arity}",
                        sink.from
                    ),
                ));
            }
            if let Some(c) = sink.non_null_assertions.iter().find(|c| **c >= arity) {
                issues.push((
                    Rule::Arity,
                    format!(
                        "non-null assertion on column {c}, which is out of range for arity {arity}"
                    ),
                ));
            }
            if let Some((key, mfp)) = &sink.from_arrangement {
                arity::sink_arrangement(&mut issues, key, mfp, arity);
            }
            for (rule, message) in issues {
                self.dataflow_error(rule, format!("sink export {id}: {message}"));
            }
        }
    }

    /// Visits `expr` and its inputs, and returns what is known about its output.
    ///
    /// `binding_allowed` says whether `expr` is in a position where rendering accepts `Let`
    /// and `LetRec`. `RenderPlan::try_from` accepts them only as a prefix of bindings above a
    /// binding-free body, with `Let` values binding-free and `LetRec` values again of that
    /// form, and the controller panics on any other plan.
    fn visit(
        &mut self,
        expr: &LirRelationExpr,
        binding_allowed: bool,
    ) -> Result<Output, RecursionLimitError> {
        self.checked_recur_mut(|this| this.visit_inner(expr, binding_allowed))
    }

    fn visit_inner(
        &mut self,
        expr: &LirRelationExpr,
        binding_allowed: bool,
    ) -> Result<Output, RecursionLimitError> {
        // `LetFreePlan` stores nodes in a map by `LirId`, so a duplicate replaces a node.
        if !self.lir_ids.insert(expr.lir_id) {
            self.node_error(
                expr,
                Rule::Scoping,
                format!("LIR id {} is used more than once", expr.lir_id),
            );
        }
        let mut issues = Vec::new();
        let output = match &expr.node {
            LirRelationNode::Constant { rows } => match rows {
                Ok(rows) if !rows.is_empty() => {
                    let arity = rows[0].0.iter().count();
                    if let Some((i, (row, _, _))) = rows
                        .iter()
                        .enumerate()
                        .find(|(_, (row, _, _))| row.iter().count() != arity)
                    {
                        issues.push((
                            Rule::Arity,
                            format!(
                                "constant row {i} has {} columns, but row 0 has {arity}",
                                row.iter().count()
                            ),
                        ));
                    }
                    Some(Output::Rows(NodeInfo::with_arity(arity)))
                }
                _ => Some(Output::Empty),
            },
            LirRelationNode::Get { id, keys, plan } => {
                let binding = self.resolve(expr, id);
                match binding.arity() {
                    Err(()) => None,
                    Ok(arity) => {
                        let out = arity::get(&mut issues, keys, plan, arity);
                        Some(Self::unary(out))
                    }
                }
            }
            LirRelationNode::Let { id, value, body } => {
                if !binding_allowed {
                    issues.push((Rule::Shape, Self::misplaced_binding()));
                }
                let value = self.visit(value, false)?;
                self.bind(expr, *id);
                self.locals.push((*id, value));
                let body = self.visit(body, true)?;
                self.locals.pop();
                Some(body)
            }
            LirRelationNode::LetRec {
                ids,
                values,
                limits,
                body,
            } => {
                if !binding_allowed {
                    issues.push((Rule::Shape, Self::misplaced_binding()));
                }
                // Rendering zips the three with `zip_eq`.
                if ids.len() != values.len() || ids.len() != limits.len() {
                    issues.push((
                        Rule::Shape,
                        format!(
                            "LetRec has {} ids, {} values, and {} limits",
                            ids.len(),
                            values.len(),
                            limits.len()
                        ),
                    ));
                }
                Some(self.let_rec(expr, ids, values, body)?)
            }
            LirRelationNode::Mfp {
                input,
                mfp,
                input_key_val,
            } => {
                let input = self.visit(input, false)?;
                input.arity().ok().map(|arity| {
                    let key = input_key_val.as_ref().map(|(key, _)| key.as_slice());
                    Self::unary(arity::mfp(&mut issues, mfp, key, arity))
                })
            }
            LirRelationNode::FlatMap {
                input_key,
                input,
                exprs,
                func,
                mfp_after,
            } => {
                let input = self.visit(input, false)?;
                input.arity().ok().map(|arity| {
                    let out = arity::flat_map(
                        &mut issues,
                        input_key.as_deref(),
                        exprs,
                        func,
                        mfp_after,
                        arity,
                    );
                    Self::unary(out)
                })
            }
            LirRelationNode::Join { inputs, plan } => {
                let mut arities = Vec::with_capacity(inputs.len());
                let mut failed = false;
                for input in inputs {
                    match self.visit(input, false)?.arity() {
                        Ok(arity) => arities.push(arity),
                        Err(()) => failed = true,
                    }
                }
                if failed {
                    None
                } else {
                    let out = arity::join(&mut issues, plan, &arities);
                    if arities.iter().any(Option::is_none) {
                        Some(Output::Empty)
                    } else {
                        out.map(|arity| Output::Rows(NodeInfo::with_arity(arity)))
                    }
                }
            }
            LirRelationNode::Reduce {
                input_key,
                input,
                key_val_plan,
                plan,
                mfp_after,
                temporal_bucketing_strategy: _,
            } => {
                let input = self.visit(input, false)?;
                input.arity().ok().map(|arity| {
                    let out = arity::reduce(
                        &mut issues,
                        input_key.as_deref(),
                        key_val_plan,
                        plan,
                        mfp_after,
                        arity,
                    );
                    Self::unary(out)
                })
            }
            LirRelationNode::TopK {
                input,
                top_k_plan,
                temporal_bucketing_strategy: _,
            } => {
                let input = self.visit(input, false)?;
                input
                    .arity()
                    .ok()
                    .map(|arity| Self::unary(arity::top_k(&mut issues, top_k_plan, arity)))
            }
            LirRelationNode::Negate { input } => {
                let input = self.visit(input, false)?;
                input.arity().ok().map(Self::unary)
            }
            LirRelationNode::Threshold {
                input,
                threshold_plan,
            } => {
                let input = self.visit(input, false)?;
                input
                    .arity()
                    .ok()
                    .map(|arity| Self::unary(arity::threshold(&mut issues, threshold_plan, arity)))
            }
            LirRelationNode::Union {
                inputs,
                consolidate_output: _,
                temporal_bucketing_strategies,
            } => {
                // Rendering zips the two with `zip_eq`.
                if temporal_bucketing_strategies.len() != inputs.len() {
                    issues.push((
                        Rule::Shape,
                        format!(
                            "Union has {} inputs, but {} temporal bucketing strategies",
                            inputs.len(),
                            temporal_bucketing_strategies.len()
                        ),
                    ));
                }
                let mut failed = false;
                let mut output = None;
                for (i, input) in inputs.iter().enumerate() {
                    match self.visit(input, false)?.arity() {
                        Err(()) => failed = true,
                        Ok(None) => {}
                        Ok(Some(arity)) => match output {
                            None => output = Some((i, arity)),
                            Some((first, expected)) if expected != arity => issues.push((
                                Rule::Arity,
                                format!(
                                    "Union input {i} has arity {arity}, but input {first} has arity {expected}"
                                ),
                            )),
                            Some(_) => {}
                        },
                    }
                }
                if failed {
                    None
                } else {
                    Some(match output {
                        Some((_, arity)) => Output::Rows(NodeInfo::with_arity(arity)),
                        None => Output::Empty,
                    })
                }
            }
            LirRelationNode::ArrangeBy {
                input_key,
                input,
                input_mfp,
                forms,
                strategy: _,
            } => {
                let input = self.visit(input, false)?;
                input.arity().ok().map(|arity| {
                    let out = arity::arrange_by(
                        &mut issues,
                        input_key.as_deref(),
                        input_mfp,
                        forms,
                        arity,
                    );
                    Self::unary(out)
                })
            }
        };
        let clean = self.report(expr, issues);
        Ok(match output {
            Some(output) if clean => output,
            _ => Output::Failed,
        })
    }

    /// The output of a unary operator, given the output arity the arity rules computed.
    ///
    /// An operator that states its output arity has that arity even over an empty input,
    /// which subjects its consumers to the full checks. One that passes its input's arity
    /// through is empty over an empty input.
    fn unary(output: Option<usize>) -> Output {
        match output {
            Some(arity) => Output::Rows(NodeInfo::with_arity(arity)),
            None => Output::Empty,
        }
    }

    fn misplaced_binding() -> String {
        "Let and LetRec may appear only as a prefix of bindings above a binding-free body"
            .to_string()
    }

    /// Resolves the collection a `Get` reads.
    fn resolve(&mut self, expr: &LirRelationExpr, id: &Id) -> Output {
        let found = match id {
            Id::Local(local) => self
                .locals
                .iter()
                .rev()
                .find(|(l, _)| l == local)
                .map(|(_, output)| output.clone()),
            Id::Global(global) => {
                let found = self.globals.get(global).cloned();
                if found.is_some() {
                    self.read_globals.insert(*global);
                }
                found
            }
        };
        if let Some(output) = found {
            return output;
        }
        let message = match id {
            Id::Local(local) => {
                format!("local id {local} is not bound by an enclosing Let or LetRec")
            }
            Id::Global(global) if self.objects.contains(global) => {
                format!("global id {global} refers to an object that is not built before this one")
            }
            Id::Global(global) => {
                format!("global id {global} is neither an import nor a built object")
            }
        };
        self.node_error(expr, Rule::Scoping, message);
        Output::Failed
    }

    /// Records that `expr` binds `id`.
    ///
    /// Rendering keeps every binding of an object in one map without scopes, so a repeated
    /// id would replace a binding that other parts of the plan still read, and a repeated id
    /// within one `LetRec` makes rendering panic.
    fn bind(&mut self, expr: &LirRelationExpr, id: LocalId) {
        if !self.bound_locals.insert(id) {
            self.node_error(
                expr,
                Rule::Scoping,
                format!("local id {id} is bound more than once"),
            );
        }
    }

    /// Visits a `LetRec`, whose bindings are visible in all values and in the body.
    ///
    /// A value can read its own binding, so the arities of the bindings come from
    /// [`structural_arity`] before the values are visited, and are then checked against
    /// the visited values.
    fn let_rec(
        &mut self,
        expr: &LirRelationExpr,
        ids: &[LocalId],
        values: &[LirRelationExpr],
        body: &LirRelationExpr,
    ) -> Result<Output, RecursionLimitError> {
        for id in ids {
            self.bind(expr, *id);
        }

        // Iterate to a fixpoint, since a value's arity can depend on other bindings.
        let mut known: BTreeMap<LocalId, usize> = BTreeMap::new();
        loop {
            let mut changed = false;
            // A length mismatch with `values` has been reported, so ids without a value are
            // skipped.
            for (i, id) in ids.iter().enumerate() {
                let Some(value) = values.get(i) else { break };
                if known.contains_key(id) {
                    continue;
                }
                if let Some(arity) = self.structural_arity(value, &known) {
                    known.insert(*id, arity);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        let scope = self.locals.len();
        for (i, id) in ids.iter().enumerate() {
            let binding = match (known.get(id), values.get(i)) {
                (Some(arity), _) => Output::Rows(NodeInfo::with_arity(*arity)),
                (None, Some(value)) if Self::is_empty_constant(value) => Output::Empty,
                (None, Some(_)) => {
                    self.node_error(
                        expr,
                        Rule::Arity,
                        format!("cannot infer the arity of recursive binding {id}"),
                    );
                    Output::Failed
                }
                // The length mismatch has been reported.
                (None, None) => Output::Failed,
            };
            self.locals.push((*id, binding));
        }

        let mut clean = true;
        for (i, value) in values.iter().enumerate() {
            let output = self.visit(value, true)?;
            let (Some(id), Ok(Some(arity))) = (ids.get(i), output.arity()) else {
                continue;
            };
            if let Some(expected) = known.get(id) {
                if *expected != arity {
                    self.node_error(
                        expr,
                        Rule::Arity,
                        format!(
                            "recursive binding {id} has arity {expected}, but its value has arity {arity}"
                        ),
                    );
                    clean = false;
                }
            }
        }

        let body = self.visit(body, true)?;
        self.locals.truncate(scope);
        Ok(if clean { body } else { Output::Failed })
    }

    fn is_empty_constant(expr: &LirRelationExpr) -> bool {
        match &expr.node {
            LirRelationNode::Constant { rows } => rows.as_ref().map_or(true, Vec::is_empty),
            _ => false,
        }
    }

    /// The arity of `expr` as stated by the plan's own fields, without checking anything.
    ///
    /// Most operators state their output arity, for example in the projection of an MFP.
    /// For those that pass their input through, this looks at the input, iteratively.
    /// `recursive` holds the arities of the enclosing `LetRec`'s bindings found so far.
    /// Returns `None` where the arity cannot be determined this way.
    fn structural_arity(
        &self,
        expr: &LirRelationExpr,
        recursive: &BTreeMap<LocalId, usize>,
    ) -> Option<usize> {
        let mut todo = vec![expr];
        while let Some(expr) = todo.pop() {
            match &expr.node {
                LirRelationNode::Constant { rows } => {
                    if let Some((row, _, _)) = rows.as_ref().ok().and_then(|rows| rows.first()) {
                        return Some(row.iter().count());
                    }
                }
                LirRelationNode::Get { id, plan, .. } => {
                    if let crate::plan::GetPlan::Arrangement(_, _, mfp)
                    | crate::plan::GetPlan::Collection(mfp) = plan
                    {
                        return Some(arity::projection_len(mfp));
                    }
                    let binding = match id {
                        Id::Local(local) => recursive.get(local).copied().or_else(|| {
                            self.locals
                                .iter()
                                .rev()
                                .find(|(l, _)| l == local)
                                .and_then(|(_, output)| output.arity().ok().flatten())
                        }),
                        Id::Global(global) => self
                            .globals
                            .get(global)
                            .and_then(|output| output.arity().ok().flatten()),
                    };
                    if binding.is_some() {
                        return binding;
                    }
                }
                LirRelationNode::Mfp { mfp, .. } if !mfp.is_identity() => {
                    return Some(arity::projection_len(mfp));
                }
                LirRelationNode::FlatMap { mfp_after, .. } => {
                    return Some(arity::projection_len(mfp_after));
                }
                LirRelationNode::Reduce { mfp_after, .. } => {
                    return Some(mfp_after.projection.len());
                }
                LirRelationNode::TopK { top_k_plan, .. } => {
                    return Some(match top_k_plan {
                        crate::plan::top_k::TopKPlan::MonotonicTop1(p) => p.arity,
                        crate::plan::top_k::TopKPlan::MonotonicTopK(p) => p.arity,
                        crate::plan::top_k::TopKPlan::Basic(p) => p.arity,
                    });
                }
                LirRelationNode::Join { inputs, plan } => {
                    let mut issues = Vec::new();
                    // With unknown input arities, the join rules check nothing and compute
                    // the arity from the closures, if the plan has any.
                    let unknown = vec![None; inputs.len()];
                    match arity::join(&mut issues, plan, &unknown) {
                        Some(arity) => return Some(arity),
                        None => todo.extend(inputs.iter().rev()),
                    }
                }
                LirRelationNode::Mfp { input, .. }
                | LirRelationNode::Negate { input }
                | LirRelationNode::Threshold { input, .. }
                | LirRelationNode::ArrangeBy { input, .. } => todo.push(input),
                LirRelationNode::Union { inputs, .. } => todo.extend(inputs.iter().rev()),
                LirRelationNode::Let { body, .. } | LirRelationNode::LetRec { body, .. } => {
                    todo.push(body)
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests;
