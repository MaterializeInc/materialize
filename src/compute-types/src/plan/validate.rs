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
//! [`arity`], arrangement availability in [`arrangements`], types in [`types`], with the
//! functions user plans may apply in [`allowlist`], operator-specific requirements in
//! [`operators`], and monotonicity in [`monotonic`]. Errors accumulate, so one call
//! reports every problem it can find. A node whose input broke a rule is skipped, so a
//! problem is reported once, where it occurs.

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::{Id, LocalId, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_ore::vec::PartialOrdVecExt;
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};
use timely::PartialOrder;

use crate::dataflows::LirDataflowDescription;
use crate::plan::scalar::LirScalarExpr;
use crate::plan::{LirId, LirRelationExpr, LirRelationNode};

pub mod allowlist;
mod arity;
mod arrangements;
mod monotonic;
mod operators;
mod types;

use arrangements::Available;
use monotonic::Monotonicity;
use types::{ColType, Typer};

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
    /// User plan mode. Enables the function allowlist and the rules that hold for user plans
    /// only.
    ///
    /// In user plan mode, function applications must be admitted by [`allowlist`], every
    /// column must have an inferable type, and every recursive binding must state an
    /// iteration limit. Otherwise functions are typed by their output types only, columns
    /// whose type cannot be inferred, such as all-null constant columns, are accepted, and
    /// recursive bindings may lack a limit, as optimizer plans of recursive queries without
    /// one do.
    pub enforce_function_allowlist: bool,
}

/// Validates a dataflow, and returns the output type of every built object.
///
/// An object with a column whose type is not inferred, for example because no row reaches
/// it, takes its type from its export, and is left out of the result if it has none.
///
/// Returns every violation found if the dataflow breaks a rule. Never panics.
pub fn validate_dataflow(
    df: &LirDataflowDescription,
    config: &ValidateConfig,
) -> Result<BTreeMap<GlobalId, ReprRelationType>, Vec<ValidationError>> {
    let ValidateConfig {
        enforce_function_allowlist,
    } = config;
    let mut validator = Validator::new(Typer {
        enforce: *enforce_function_allowlist,
    });
    let types = match validator.dataflow(df) {
        Ok(types) => types,
        Err(abort) => {
            let message = match abort {
                Abort::Recursion => format!("plan nests deeper than {RECURSION_LIMIT} operators"),
                Abort::Budget => {
                    format!("validating the plan takes more than {VISIT_BUDGET} operator visits")
                }
            };
            validator.errors.push(ValidationError {
                lir_id: None,
                operator: "dataflow",
                rule: Rule::Shape,
                message,
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
    /// The type of each column, or `None` if no row reaches the output. Has `arity` entries.
    types: Option<Vec<ColType>>,
    /// The forms in which the output is available.
    available: Available,
    /// Whether the output never retracts a row. See [`monotonic`].
    monotonic: bool,
    /// Whether the output's update stream has no negative diffs. See [`monotonic`].
    physically_monotonic: bool,
}

impl NodeInfo {
    /// Describes an output that is not monotonic. The walk sets the monotonicity of each
    /// node after its rules have run.
    fn new(arity: usize, available: Available, types: Option<Vec<ColType>>) -> Self {
        NodeInfo {
            arity,
            // Types disagree with the arity only where the arity rules reported a problem.
            types: types.filter(|t| t.len() == arity),
            available,
            monotonic: false,
            physically_monotonic: false,
        }
    }

    fn set_monotonicity(&mut self, monotonicity: Monotonicity) {
        self.monotonic = monotonicity.logical;
        self.physically_monotonic = monotonicity.physical;
    }
}

/// The result of visiting a node.
#[derive(Clone, Debug)]
enum Output {
    /// The node or one of its inputs broke a rule, which has been reported.
    Failed,
    /// The node produces no rows, because every path to its output starts at an empty or
    /// error `Constant`. The plan does not fix its arity, and since no datum reaches its
    /// consumers, they check only what does not depend on it. Rendering still builds the
    /// forms it is available in.
    Empty(Available),
    /// The node produces rows as described.
    Rows(NodeInfo),
}

impl Output {
    /// The arity of the output, `None` for an empty output, or `Err` for a failed one.
    fn arity(&self) -> Result<Option<usize>, ()> {
        match self {
            Output::Failed => Err(()),
            Output::Empty(_) => Ok(None),
            Output::Rows(info) => Ok(Some(info.arity)),
        }
    }

    /// The forms the output is available in, or `None` for a failed one.
    fn available(&self) -> Option<&Available> {
        match self {
            Output::Failed => None,
            Output::Empty(available) => Some(available),
            Output::Rows(info) => Some(&info.available),
        }
    }

    /// The column types of the output, or `None` if no row reaches it or it failed.
    fn types(&self) -> Option<&[ColType]> {
        match self {
            Output::Rows(info) => info.types.as_deref(),
            Output::Failed | Output::Empty(_) => None,
        }
    }

    /// Whether the output retracts rows. An empty output has no rows to retract, and a failed
    /// one is judged as empty, because its consumers are not checked.
    fn monotonicity(&self) -> Monotonicity {
        match self {
            Output::Rows(info) => Monotonicity {
                logical: info.monotonic,
                physical: info.physically_monotonic,
            },
            Output::Failed | Output::Empty(_) => Monotonicity::NO_ROWS,
        }
    }

    /// Replaces the column types of an output with rows.
    fn set_types(&mut self, types: Option<Vec<ColType>>) {
        if let Output::Rows(info) = self {
            info.types = types.filter(|t| t.len() == info.arity);
        }
    }

    /// Replaces the forms of a non-failed output.
    fn set_available(&mut self, forms: Available) {
        match self {
            Output::Failed => {}
            Output::Empty(available) => *available = forms,
            Output::Rows(info) => info.available = forms,
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

/// The most times the values of one `LetRec` are visited to find the column types of its
/// bindings. Each visit that does not reach the fixpoint makes some column of some binding
/// less precise.
const LET_REC_TYPE_ITERATIONS: usize = 64;

/// The most operator visits one validation makes.
///
/// A plan without `LetRec` visits each operator once. A `LetRec` visits its values until the
/// types of its bindings reach a fixpoint, and once more, so nested `LetRec`s multiply the
/// visits of the values inside them. The budget bounds that work. It leaves room for plans
/// of 100,000 operators visited ten times each, and a validation that used it up took 3 s
/// in a debug build.
const VISIT_BUDGET: usize = 1_000_000;

/// Why validation stopped before visiting every operator.
#[derive(Debug)]
enum Abort {
    /// The plan nests deeper than [`RECURSION_LIMIT`].
    Recursion,
    /// The walk used up [`VISIT_BUDGET`].
    Budget,
}

impl From<RecursionLimitError> for Abort {
    fn from(_: RecursionLimitError) -> Self {
        Abort::Recursion
    }
}

/// The result of visiting the values of a `LetRec` once.
struct LetRecPass {
    /// Whether the values agree with their bindings.
    clean: bool,
    /// Whether the column types of a binding changed.
    changed: bool,
    /// The first binding column whose value has another scalar type than the binding.
    mismatch: Option<(LocalId, usize, ReprScalarType, ReprScalarType)>,
}

/// The state of [`Validator`] that visits change besides `locals`.
struct Snapshot {
    errors: usize,
    lir_ids: BTreeSet<LirId>,
    bound_locals: BTreeSet<LocalId>,
    read_globals: BTreeSet<GlobalId>,
}

/// The state of one validation.
struct Validator {
    typer: Typer,
    /// The operator visits made so far, including discarded ones.
    visits: usize,
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
    /// The keys of index imports that exclude their snapshot, by the collection they index.
    snapshotless: BTreeMap<GlobalId, BTreeSet<Vec<LirScalarExpr>>>,
    monotonicity: monotonic::Analysis,
    /// The number of `LetRec` values the current node is inside of.
    rec_depth: usize,
}

impl CheckedRecursion for Validator {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl Validator {
    fn new(typer: Typer) -> Self {
        Validator {
            typer,
            visits: 0,
            errors: Vec::new(),
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
            globals: BTreeMap::new(),
            objects: BTreeSet::new(),
            locals: Vec::new(),
            bound_locals: BTreeSet::new(),
            lir_ids: BTreeSet::new(),
            read_globals: BTreeSet::new(),
            snapshotless: BTreeMap::new(),
            monotonicity: monotonic::Analysis::default(),
            rec_depth: 0,
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
    ) -> Result<BTreeMap<GlobalId, ReprRelationType>, Abort> {
        // `is_single_time` soft-asserts that the as-of is not beyond the until, which a plan
        // need not satisfy.
        let single_time = df
            .as_of
            .as_ref()
            .is_some_and(|as_of| PartialOrder::less_equal(as_of, &df.until))
            && df.is_single_time();
        // The same imports `refine_single_time_consolidation` passes to the single-time
        // analysis.
        let monotonic_ids = df
            .source_imports
            .iter()
            .filter(|(_, import)| import.monotonic)
            .map(|(id, _)| *id)
            .chain(
                df.index_imports
                    .values()
                    .filter(|import| import.monotonic)
                    .map(|import| import.desc.on_id),
            )
            .collect();
        self.monotonicity = monotonic::Analysis::new(single_time, monotonic_ids);
        self.imports(df);

        self.objects = df.objects_to_build.iter().map(|o| o.id).collect();
        let mut types = BTreeMap::new();
        let mut empty_objects = Vec::new();
        for object in &df.objects_to_build {
            self.bound_locals.clear();
            let mut output = self.visit(&object.plan, true)?;
            let declared = Self::declared_type(df, object.id);
            if let Output::Empty(available) = &output {
                match &declared {
                    Some(typ) => {
                        output = Output::Rows(NodeInfo::new(typ.arity(), available.clone(), None))
                    }
                    // Consumers of the object may still fix its arity.
                    None => empty_objects.push(object.id),
                }
            }
            if let Output::Rows(info) = &output {
                if let Some(typ) = Self::object_type(info, declared.as_ref()) {
                    types.insert(object.id, typ);
                }
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
    ///
    /// A source import provides the raw collection, and an index import its arrangement. An
    /// index import that excludes its snapshot provides either its arrangement or the raw
    /// collection, depending on the replica's configuration (`import_index` in rendering), so
    /// it provides neither for certain and is recorded in `snapshotless` instead.
    fn imports(&mut self, df: &LirDataflowDescription) {
        let mut arities: BTreeMap<GlobalId, usize> = BTreeMap::new();
        let mut available: BTreeMap<GlobalId, Available> = BTreeMap::new();
        let mut types: BTreeMap<GlobalId, Vec<ColType>> = BTreeMap::new();
        // Collections whose source operators have a temporal predicate.
        let mut temporal = BTreeSet::new();
        for (id, import) in &df.source_imports {
            if let Some(mfp) = &import.desc.arguments.operators {
                if mfp.predicates.iter().any(|(_, p)| p.contains_temporal()) {
                    temporal.insert(*id);
                }
            }
            let typ_arity = import.desc.typ.arity();
            let typ: Vec<ColType> = ReprRelationType::from(&import.desc.typ)
                .column_types
                .into_iter()
                .map(ColType::Known)
                .collect();
            // The source's operators run inside the source, so `Get`s see their output.
            let (arity, typ) = match &import.desc.arguments.operators {
                None => (typ_arity, typ),
                Some(mfp) => {
                    let mut issues = Vec::new();
                    arity::check_mfp(&mut issues, "source MFP", mfp, Some(typ_arity));
                    // Rendering plans the operators with `MfpPlan::create_from` and panics if
                    // that fails, for example on a temporal predicate it cannot express as a
                    // bound. Planning optimizes the operators first, which is only safe for
                    // operators whose columns are in range.
                    if issues.is_empty() && mz_expr::MfpPlan::create_from(mfp.clone()).is_err() {
                        issues.push((
                            Rule::Shape,
                            "operators are not a valid MFP plan".to_string(),
                        ));
                    }
                    let typ = self.source_operator_types(&mut issues, mfp, &typ);
                    for (rule, message) in issues {
                        self.dataflow_error(rule, format!("source import {id}: {message}"));
                    }
                    (mfp.projection.len(), typ)
                }
            };
            arities.insert(*id, arity);
            available.insert(*id, Available::raw());
            types.insert(*id, typ);
        }
        for (id, import) in &df.index_imports {
            let on_id = import.desc.on_id;
            let arity = import.typ.arity();
            let mut issues = Vec::new();
            arity::export_key(&mut issues, "index key", &import.desc.key, arity);
            for (rule, message) in issues {
                self.dataflow_error(rule, format!("index import {id}: {message}"));
            }
            if let Some(key) = self.lir_key(&format!("index import {id}"), &import.desc.key) {
                let forms = available.entry(on_id).or_default();
                if import.with_snapshot {
                    forms.keys.insert(key);
                } else {
                    self.snapshotless.entry(on_id).or_default().insert(key);
                }
            }
            types.entry(on_id).or_insert_with(|| {
                import
                    .typ
                    .column_types
                    .iter()
                    .cloned()
                    .map(ColType::Known)
                    .collect()
            });
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
            let forms = available.remove(&id).unwrap_or_default();
            let types = types.remove(&id);
            let mut info = NodeInfo::new(arity, forms, types);
            let monotonicity = self.monotonicity.import(id, temporal.contains(&id));
            info.set_monotonicity(monotonicity);
            self.globals.insert(id, Output::Rows(info));
        }
    }

    /// The output types of a source's operators, given the types of the source.
    ///
    /// The operators are MIR expressions, typed through their LIR translation.
    fn source_operator_types(
        &self,
        issues: &mut arity::Issues,
        mfp: &mz_expr::MapFilterProject,
        input: &[ColType],
    ) -> Vec<ColType> {
        let lir = |exprs: &[mz_expr::MirScalarExpr]| {
            exprs
                .iter()
                .map(|e| LirScalarExpr::try_from(e).ok())
                .collect::<Option<Vec<_>>>()
        };
        let predicates = mfp
            .predicates
            .iter()
            .map(|(position, p)| Some((*position, LirScalarExpr::try_from(p).ok()?)))
            .collect::<Option<Vec<_>>>();
        let (Some(expressions), Some(predicates)) = (lir(&mfp.expressions), predicates) else {
            // Such operators include temporal filters, which `mz_now()` makes inexpressible
            // in LIR.
            if self.typer.enforce {
                issues.push((
                    Rule::Function,
                    "operators use a function that LIR cannot express".to_string(),
                ));
            }
            return mfp.projection.iter().map(|_| ColType::Unknown).collect();
        };
        let lir_mfp = mz_expr::MapFilterProject {
            expressions,
            predicates,
            projection: mfp.projection.clone(),
            input_arity: mfp.input_arity,
        };
        self.typer.mfp(issues, "source MFP", &lir_mfp, &|c| {
            types::col(Some(input), c)
        })
    }

    /// Translates an index key to the LIR key rendering looks the arrangement up by.
    ///
    /// Rendering translates index keys with `IndexDesc::as_lir`, which panics on functions
    /// that LIR cannot express. Reports those and returns `None`.
    fn lir_key(
        &mut self,
        what: &str,
        key: &[mz_expr::MirScalarExpr],
    ) -> Option<Vec<LirScalarExpr>> {
        match key
            .iter()
            .map(LirScalarExpr::try_from)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(key) => Some(key),
            Err(_) => {
                self.dataflow_error(
                    Rule::Function,
                    format!("{what} key uses a function that LIR cannot express"),
                );
                None
            }
        }
    }

    /// The output type of an object, with the columns whose types are not inferred taken
    /// from the type its exports declare, or `None` if that does not determine every column.
    fn object_type(
        info: &NodeInfo,
        declared: Option<&ReprRelationType>,
    ) -> Option<ReprRelationType> {
        let declared = declared.filter(|typ| typ.arity() == info.arity);
        let Some(types) = &info.types else {
            return declared.cloned();
        };
        let column_types = types
            .iter()
            .enumerate()
            .map(|(c, t)| match t {
                ColType::Known(t) => Some(t.clone()),
                ColType::Bottom | ColType::Unknown => {
                    declared.and_then(|typ| typ.column_types.get(c).cloned())
                }
            })
            .collect::<Option<Vec<_>>>()?;
        Some(ReprRelationType::new(column_types))
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
            // Rendering exports the arrangement of the object by the index key, and panics if
            // there is none.
            let forms = self.globals.get(id).and_then(Output::available).cloned();
            if let (Some(forms), Some(key)) = (
                forms,
                self.lir_key(&format!("index export {id}"), &desc.key),
            ) {
                if !forms.keys.contains(&key) {
                    let key = arrangements::key_name(&key);
                    self.dataflow_error(
                        Rule::Arrangement,
                        format!(
                            "index export {id} reads the arrangement by {key}, but object {id} is not arranged by {key}"
                        ),
                    );
                }
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
            let Some(output) = self.globals.get(&sink.from).cloned() else {
                self.dataflow_error(
                    Rule::Scoping,
                    format!(
                        "sink export {id} is from {}, which is neither an import nor a built object",
                        sink.from
                    ),
                );
                continue;
            };
            // Rendering reads the raw collection if there is one, and otherwise the arrangement
            // `from_arrangement` names. An arrangement of a snapshotless index import serves
            // too, because the raw collection replaces it where the import omits it.
            if let Some(forms) = output.available() {
                if !forms.raw {
                    let message = match &sink.from_arrangement {
                        None => Some(format!(
                            "sink export {id} reads the raw collection of {}, which has none, and names no arrangement",
                            sink.from
                        )),
                        Some((key, _))
                            if !forms.keys.contains(key)
                                && !self
                                    .snapshotless
                                    .get(&sink.from)
                                    .is_some_and(|keys| keys.contains(key)) =>
                        {
                            let key = arrangements::key_name(key);
                            Some(format!(
                                "sink export {id} reads the arrangement by {key}, but {} is not arranged by {key}",
                                sink.from
                            ))
                        }
                        Some(_) => None,
                    };
                    if let Some(message) = message {
                        self.dataflow_error(Rule::Arrangement, message);
                    }
                }
            }
            // A soft assertion in rendering, which panics where soft assertions are enabled.
            if !sink.non_null_assertions.is_strictly_sorted() {
                self.dataflow_error(
                    Rule::Shape,
                    format!("sink export {id}: non-null assertions are not strictly sorted"),
                );
            }
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
                self.typer
                    .sink_arrangement(&mut issues, key, mfp, output.types());
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
    fn visit(&mut self, expr: &LirRelationExpr, binding_allowed: bool) -> Result<Output, Abort> {
        self.visits += 1;
        if self.visits > VISIT_BUDGET {
            return Err(Abort::Budget);
        }
        self.checked_recur_mut(|this| this.visit_inner(expr, binding_allowed))
    }

    fn visit_inner(
        &mut self,
        expr: &LirRelationExpr,
        binding_allowed: bool,
    ) -> Result<Output, Abort> {
        // `LetFreePlan` stores nodes in a map by `LirId`, so a duplicate replaces a node.
        if !self.lir_ids.insert(expr.lir_id) {
            self.node_error(
                expr,
                Rule::Scoping,
                format!("LIR id {} is used more than once", expr.lir_id),
            );
        }
        let mut issues = Vec::new();
        let typer = self.typer;
        // The monotonicity of the inputs in order, or of the binding of a `Get`.
        let mut input_monotonicity = Vec::new();
        let mut output = match &expr.node {
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
                    let types = typer.constant(&mut issues, rows);
                    Some(Output::Rows(NodeInfo::new(arity, Available::raw(), types)))
                }
                _ => Some(Output::Empty(Available::raw())),
            },
            LirRelationNode::Get { id, keys, plan } => {
                let binding = self.resolve(expr, id);
                input_monotonicity.push(binding.monotonicity());
                Self::inputs(&binding).map(|(arity, input, types)| {
                    let out = arity::get(&mut issues, keys, plan, arity);
                    let forms = arrangements::get(&mut issues, keys, plan, input, arity);
                    let types = typer.get(&mut issues, plan, types);
                    Self::unary(out, forms, types)
                })
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
                if self.typer.enforce {
                    operators::let_rec_limits(&mut issues, ids, limits);
                }
                Some(self.let_rec(expr, ids, values, body)?)
            }
            LirRelationNode::Mfp {
                input,
                mfp,
                input_key_val,
            } => {
                let input = self.visit(input, false)?;
                input_monotonicity.push(input.monotonicity());
                Self::inputs(&input).map(|(arity, forms, types)| {
                    let key = input_key_val.as_ref().map(|(key, _)| key.as_slice());
                    let out = arity::mfp(&mut issues, mfp, key, arity);
                    let forms = arrangements::mfp(&mut issues, mfp.is_identity(), key, forms);
                    let types = typer.mfp_node(&mut issues, mfp, key, types);
                    Self::unary(out, forms, types)
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
                input_monotonicity.push(input.monotonicity());
                Self::inputs(&input).map(|(arity, forms, types)| {
                    let out = arity::flat_map(
                        &mut issues,
                        input_key.as_deref(),
                        exprs,
                        func,
                        mfp_after,
                        arity,
                    );
                    let forms = arrangements::flat_map(&mut issues, input_key.as_deref(), forms);
                    let types = typer.flat_map(
                        &mut issues,
                        input_key.as_deref(),
                        exprs,
                        func,
                        mfp_after,
                        types,
                    );
                    Self::unary(out, forms, Some(types))
                })
            }
            LirRelationNode::Join { inputs, plan } => {
                let mut arities = Vec::with_capacity(inputs.len());
                let mut forms = Vec::with_capacity(inputs.len());
                let mut input_types = Vec::with_capacity(inputs.len());
                let mut failed = false;
                let single_path = self.monotonicity.single_time() && self.rec_depth == 0;
                operators::join(&mut issues, plan, inputs.len(), single_path);
                for input in inputs {
                    let input = self.visit(input, false)?;
                    input_monotonicity.push(input.monotonicity());
                    match Self::inputs(&input) {
                        Some((arity, available, types)) => {
                            arities.push(arity);
                            forms.push(available.clone());
                            input_types.push(types.map(<[ColType]>::to_vec));
                        }
                        None => failed = true,
                    }
                }
                if failed {
                    None
                } else {
                    let out = arity::join(&mut issues, plan, &arities);
                    let inputs: Vec<&Available> = forms.iter().collect();
                    let output_forms = arrangements::join(&mut issues, plan, &inputs, &arities);
                    if arities.iter().any(Option::is_none) {
                        Some(Output::Empty(output_forms))
                    } else {
                        let input_types: Vec<_> =
                            input_types.iter().map(Option::as_deref).collect();
                        let types = typer.join(&mut issues, plan, &input_types, &inputs);
                        out.map(|arity| Output::Rows(NodeInfo::new(arity, output_forms, types)))
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
                operators::reduce(&mut issues, key_val_plan, plan);
                let input = self.visit(input, false)?;
                input_monotonicity.push(input.monotonicity());
                Self::inputs(&input).map(|(arity, forms, types)| {
                    let out = arity::reduce(
                        &mut issues,
                        input_key.as_deref(),
                        key_val_plan,
                        plan,
                        mfp_after,
                        arity,
                    );
                    let key_arity = key_val_plan.key_plan.projection.len();
                    let forms =
                        arrangements::reduce(&mut issues, input_key.as_deref(), key_arity, forms);
                    let types = typer.reduce(
                        &mut issues,
                        input_key.as_deref(),
                        key_val_plan,
                        plan,
                        mfp_after,
                        types,
                    );
                    Self::unary(out, forms, Some(types))
                })
            }
            LirRelationNode::TopK {
                input,
                top_k_plan,
                temporal_bucketing_strategy,
            } => {
                operators::top_k(&mut issues, top_k_plan, temporal_bucketing_strategy);
                let input = self.visit(input, false)?;
                input_monotonicity.push(input.monotonicity());
                Self::inputs(&input).map(|(arity, forms, types)| {
                    let out = arity::top_k(&mut issues, top_k_plan, arity);
                    let forms = arrangements::top_k(&mut issues, top_k_plan, forms);
                    let types = typer.top_k(&mut issues, top_k_plan, types);
                    Self::unary(out, forms, types)
                })
            }
            LirRelationNode::Negate { input } => {
                let input = self.visit(input, false)?;
                input_monotonicity.push(input.monotonicity());
                Self::inputs(&input).map(|(arity, forms, types)| {
                    arrangements::require_raw(&mut issues, "Negate", forms);
                    Self::unary(arity, Available::raw(), types.map(<[ColType]>::to_vec))
                })
            }
            LirRelationNode::Threshold {
                input,
                threshold_plan,
            } => {
                let input = self.visit(input, false)?;
                input_monotonicity.push(input.monotonicity());
                Self::inputs(&input).map(|(arity, forms, types)| {
                    let out = arity::threshold(&mut issues, threshold_plan, arity);
                    let forms = arrangements::threshold(&mut issues, threshold_plan, forms, arity);
                    Self::unary(out, forms, types.map(<[ColType]>::to_vec))
                })
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
                let mut types = None;
                for (i, input) in inputs.iter().enumerate() {
                    let input = self.visit(input, false)?;
                    input_monotonicity.push(input.monotonicity());
                    if let Some(forms) = input.available() {
                        arrangements::require_raw(&mut issues, &format!("Union input {i}"), forms);
                    }
                    if let Err((c, expected, t)) = types::union_into(&mut types, input.types()) {
                        issues.push((
                            Rule::Type,
                            format!(
                                "Union input {i} column {c} has type {t:?}, but an earlier input has type {expected:?}"
                            ),
                        ));
                    }
                    match input.arity() {
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
                        Some((_, arity)) => {
                            Output::Rows(NodeInfo::new(arity, Available::raw(), types))
                        }
                        None => Output::Empty(Available::raw()),
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
                input_monotonicity.push(input.monotonicity());
                Self::inputs(&input).map(|(arity, available, types)| {
                    let out = arity::arrange_by(
                        &mut issues,
                        input_key.as_deref(),
                        input_mfp,
                        forms,
                        arity,
                    );
                    let types = typer.arrange_by(
                        &mut issues,
                        input_key.as_deref(),
                        input_mfp,
                        forms,
                        available,
                        types,
                    );
                    let available = arrangements::arrange_by(
                        &mut issues,
                        input_key.as_deref(),
                        forms,
                        available,
                        arity,
                    );
                    Self::unary(out, available, types)
                })
            }
        };
        if let Some(Output::Rows(info)) = output.as_mut() {
            if let Some(monotonicity) = self.monotonicity.node(
                &mut issues,
                &expr.node,
                &input_monotonicity,
                self.rec_depth > 0,
            ) {
                info.set_monotonicity(monotonicity);
            }
        }
        let clean = self.report(expr, issues);
        Ok(match output {
            Some(output) if clean => output,
            _ => Output::Failed,
        })
    }

    /// The arity, forms and column types of a visited input, or `None` if it failed.
    fn inputs(input: &Output) -> Option<(Option<usize>, &Available, Option<&[ColType]>)> {
        Some((input.arity().ok()?, input.available()?, input.types()))
    }

    /// The output of a unary operator, given the output arity the arity rules computed, the
    /// forms the arrangement rules computed, and the column types the type rules computed.
    ///
    /// An operator that states its output arity has that arity even over an empty input,
    /// which subjects its consumers to the full checks. One that passes its input's arity
    /// through is empty over an empty input.
    fn unary(output: Option<usize>, available: Available, types: Option<Vec<ColType>>) -> Output {
        match output {
            Some(arity) => Output::Rows(NodeInfo::new(arity, available, types)),
            None => Output::Empty(available),
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
    /// the visited values. The column types of the bindings come from visiting the values
    /// until their types reach a fixpoint.
    fn let_rec(
        &mut self,
        expr: &LirRelationExpr,
        ids: &[LocalId],
        values: &[LirRelationExpr],
        body: &LirRelationExpr,
    ) -> Result<Output, Abort> {
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

        // The column types of the bindings are a fixpoint too. The values are visited with the
        // binding types found so far, starting from no row reaching any binding, until the
        // types they produce add nothing. A binding takes its value's types as soon as the
        // value is visited, so later values in the same pass see them. These visits only
        // collect types, so their errors and bookkeeping are discarded. A last visit checks
        // the values against the fixpoint.
        //
        // The same passes find the greatest fixpoint of the bindings' monotonicity, starting
        // from every binding being monotonic.
        let mut types: Vec<Option<Vec<ColType>>> = vec![None; ids.len()];
        let mut monotonic = vec![true; ids.len()];
        let mut clean = true;
        let mut converged = false;
        self.rec_depth += 1;
        for _ in 0..LET_REC_TYPE_ITERATIONS {
            let snapshot = self.snapshot();
            let scope = self.locals.len();
            let pass =
                self.let_rec_values(expr, ids, values, &known, &mut types, &mut monotonic, true)?;
            self.locals.truncate(scope);
            self.restore(snapshot);
            if let Some((id, c, a, b)) = pass.mismatch {
                self.node_error(
                    expr,
                    Rule::Type,
                    format!("recursive binding {id} column {c} has type {a:?} and type {b:?}"),
                );
                clean = false;
                break;
            }
            if !pass.changed {
                converged = true;
                break;
            }
        }
        if clean && !converged {
            self.node_error(
                expr,
                Rule::Type,
                format!(
                    "the column types or monotonicity of the recursive bindings do not converge within {LET_REC_TYPE_ITERATIONS} iterations"
                ),
            );
            clean = false;
        }

        let scope = self.locals.len();
        let values_clean = self
            .let_rec_values(expr, ids, values, &known, &mut types, &mut monotonic, false)?
            .clean;
        self.rec_depth -= 1;
        for (_, binding) in self.locals.iter_mut().skip(scope) {
            binding.set_available(Available::raw());
        }
        let body = self.visit(body, true)?;
        self.locals.truncate(scope);
        Ok(if clean && values_clean {
            body
        } else {
            Output::Failed
        })
    }

    /// Binds the ids of a `LetRec` with the given arities, column types and logical
    /// monotonicity, and visits its values.
    ///
    /// Leaves the bindings in `locals`, each with the forms of its value. With `update`, merges
    /// each value's column types into `types` and its monotonicity into `monotonic`, and both
    /// into its binding, right after visiting it.
    fn let_rec_values(
        &mut self,
        expr: &LirRelationExpr,
        ids: &[LocalId],
        values: &[LirRelationExpr],
        known: &BTreeMap<LocalId, usize>,
        types: &mut [Option<Vec<ColType>>],
        monotonic: &mut [bool],
        update: bool,
    ) -> Result<LetRecPass, Abort> {
        // Rendering binds each id to a raw-only `Variable` until its value is rendered, then to
        // the value's forms for the values that follow, and to its raw collection alone for
        // the body, because arrangements cannot leave the iterative scope.
        let scope = self.locals.len();
        for (i, id) in ids.iter().enumerate() {
            let binding = match (known.get(id), values.get(i)) {
                (Some(arity), _) => {
                    let types = types.get(i).cloned().flatten();
                    let mut info = NodeInfo::new(*arity, Available::raw(), types);
                    // A recursive binding changes from one iteration to the next, so its
                    // update stream may have negative diffs. `SingleTimeMonotonic` judges
                    // every recursive binding so.
                    info.set_monotonicity(Monotonicity {
                        logical: monotonic.get(i).copied().unwrap_or(false),
                        physical: false,
                    });
                    Output::Rows(info)
                }
                (None, Some(value)) if Self::is_empty_constant(value) => {
                    Output::Empty(Available::raw())
                }
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

        let mut pass = LetRecPass {
            clean: true,
            changed: false,
            mismatch: None,
        };
        for (i, value) in values.iter().enumerate() {
            let output = self.visit(value, true)?;
            // A length mismatch between ids and values has been reported.
            let Some(id) = ids.get(i) else { continue };
            pass.clean &= self.let_rec_value(expr, scope + i, *id, known.get(id), &output);
            if let Some(m) = monotonic.get_mut(i).filter(|m| update && **m) {
                if !output.monotonicity().logical {
                    *m = false;
                    pass.changed = true;
                    if let Some((_, Output::Rows(info))) = self.locals.get_mut(scope + i) {
                        info.monotonic = false;
                    }
                }
            }
            let Some(acc) = types.get_mut(i).filter(|_| update) else {
                continue;
            };
            let before = acc.clone();
            if let Err((c, a, b)) = types::union_into(acc, output.types()) {
                pass.mismatch.get_or_insert((*id, c, a, b));
            }
            if *acc != before {
                pass.changed = true;
                if let Some((_, binding)) = self.locals.get_mut(scope + i) {
                    binding.set_types(acc.clone());
                }
            }
        }
        Ok(pass)
    }

    /// Checks the output of the value of the `LetRec` binding at `locals[index]`, and makes
    /// the value's forms the binding's. Returns whether the value agrees with the binding.
    fn let_rec_value(
        &mut self,
        expr: &LirRelationExpr,
        index: usize,
        id: LocalId,
        expected: Option<&usize>,
        output: &Output,
    ) -> bool {
        let mut clean = true;
        if let Some(forms) = output.available() {
            // Rendering unwraps the raw collection of each value to bind it.
            if !forms.raw {
                self.node_error(
                    expr,
                    Rule::Arrangement,
                    format!("LetRec value of {id} lacks the raw collection"),
                );
                clean = false;
            }
            let forms = forms.clone();
            if let Some((_, binding)) = self.locals.get_mut(index) {
                binding.set_available(forms);
            }
        }
        if let (Ok(Some(arity)), Some(expected)) = (output.arity(), expected) {
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
        clean
    }

    /// The state that visiting a node changes besides `locals`, for visits whose results
    /// are discarded.
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            errors: self.errors.len(),
            lir_ids: self.lir_ids.clone(),
            bound_locals: self.bound_locals.clone(),
            read_globals: self.read_globals.clone(),
        }
    }

    fn restore(&mut self, snapshot: Snapshot) {
        let Snapshot {
            errors,
            lir_ids,
            bound_locals,
            read_globals,
        } = snapshot;
        self.errors.truncate(errors);
        self.lir_ids = lir_ids;
        self.bound_locals = bound_locals;
        self.read_globals = read_globals;
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
