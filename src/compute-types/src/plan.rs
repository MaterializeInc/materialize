// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An explicit representation of a rendering plan for provided dataflows.

#![warn(missing_debug_implementations)]

use std::collections::{BTreeMap, BTreeSet};

use columnar::Columnar;
use mz_expr::{
    CollectionPlan, EvalError, Id, LetRecLimit, LocalId, MapFilterProject, MfpPlan, MirScalarExpr,
    OptimizedMirRelationExpr, SafeMfpPlan, TableFunc,
};
use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::metrics::raw::IntCounterVec;
use mz_ore::soft_assert_eq_no_log;
use mz_ore::str::Indent;
use mz_repr::explain::text::text_string_at;
use mz_repr::explain::{DummyHumanizer, ExplainConfig, ExprHumanizer, PlanRenderingContext};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use serde::{Deserialize, Serialize};

use crate::dataflows::DataflowDescription;
use crate::plan::join::JoinPlan;
use crate::plan::reduce::{KeyValPlan, ReducePlan};
use crate::plan::scalar::{LirScalarExpr, mfp_mir_to_lir_plan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::transform::{Transform, TransformConfig};

mod lowering;

pub mod interpret;
pub mod join;
pub mod reduce;
pub mod render_plan;
pub mod scalar;
pub mod threshold;
pub mod top_k;
pub mod transform;

/// Metrics collected during MIR to LIR lowering.
#[derive(Debug, Clone)]
pub struct LoweringMetrics {
    /// Counts non-`None` results of `MapFilterProject::literal_constraints` during lowering,
    /// labeled by the call site (`"get"` or `"mfp"`).
    literal_constraints: IntCounterVec,
}

impl LoweringMetrics {
    /// Registers the lowering metrics into `registry`.
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            literal_constraints: registry.register(metric!(
                name: "mz_optimizer_lowering_literal_constraints_total",
                help: "How often the MFP-based literal-constraint detector succeeded, by call site.",
                var_labels: ["case"],
            )),
        }
    }

    /// Records that a `literal_constraints` call at `case` produced a usable constraint.
    pub fn inc_literal_constraints(&self, case: &str) {
        self.literal_constraints.with_label_values(&[case]).inc();
    }
}

/// The forms in which an operator's output is available.
///
/// These forms may include "raw", meaning as a streamed collection, but also any
/// number of "arranged" representations.
///
/// Each arranged representation is described by a `KeyValRowMapping`, or rather
/// at the moment by its three fields in a triple. These fields explain how to form
/// a "key" by applying some expressions to each row, how to select "values" from
/// columns not explicitly captured by the key, and how to return to the original
/// row from the concatenation of key and value. Further explanation is available
/// in the documentation for `KeyValRowMapping`.
#[derive(
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize
)]
pub struct AvailableCollections {
    /// Whether the collection exists in unarranged form.
    pub raw: bool,
    /// The list of available arrangements, presented as a `KeyValRowMapping`,
    /// but here represented by a triple `(to_key, to_val, to_row)` instead.
    /// The documentation for `KeyValRowMapping` explains these fields better.
    pub arranged: Vec<(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>)>,
}

impl AvailableCollections {
    /// Represent a collection that has no arrangements.
    pub fn new_raw() -> Self {
        Self {
            raw: true,
            arranged: Vec::new(),
        }
    }

    /// Represent a collection that is arranged in the specified ways.
    pub fn new_arranged(arranged: Vec<(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>)>) -> Self {
        assert!(
            !arranged.is_empty(),
            "Invariant violated: at least one collection must exist"
        );
        Self {
            raw: false,
            arranged,
        }
    }

    /// Get some arrangement, if one exists.
    pub fn arbitrary_arrangement(&self) -> Option<&(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>)> {
        assert!(
            self.raw || !self.arranged.is_empty(),
            "Invariant violated: at least one collection must exist"
        );
        self.arranged.get(0)
    }
}

/// How to render the arrangements requested by an `ArrangeBy`.
///
/// Decided during LIR lowering and consumed by the renderer. The variant says what the
/// renderer will do, not what it knows about the input.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize
)]
pub enum ArrangementStrategy {
    /// Form arrangements directly from the input collection.
    Direct,
    /// Insert temporal bucketing in front of the arrangement, to delay future-stamped
    /// updates (e.g., from `mz_now()` MFPs) until their bucket boundary releases them.
    /// Honoured only when `ENABLE_COMPUTE_TEMPORAL_BUCKETING` is set; otherwise behaves like
    /// `Direct`.
    TemporalBucketing,
}

impl std::fmt::Display for ArrangementStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrangementStrategy::Direct => write!(f, "Direct"),
            ArrangementStrategy::TemporalBucketing => write!(f, "TemporalBucketing"),
        }
    }
}

/// An identifier for an LIR node.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Columnar
)]
pub struct LirId(u64);

impl LirId {
    fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<LirId> for u64 {
    fn from(value: LirId) -> Self {
        value.as_u64()
    }
}

impl std::fmt::Display for LirId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A rendering plan with as much conditional logic as possible removed.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LirRelationExpr {
    /// A dataflow-local identifier.
    pub lir_id: LirId,
    /// The underlying operator.
    pub node: LirRelationNode,
}

/// The actual AST node of the `LirRelationExpr`.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum LirRelationNode {
    /// A collection containing a pre-determined collection.
    Constant {
        /// Explicit update triples for the collection.
        rows: Result<Vec<(Row, Timestamp, Diff)>, EvalError>,
    },
    /// A reference to a bound collection.
    ///
    /// This is commonly either an external reference to an existing source or
    /// maintained arrangement, or an internal reference to a `Let` identifier.
    Get {
        /// A global or local identifier naming the collection.
        id: Id,
        /// Arrangements that will be available.
        ///
        /// The collection will also be loaded if available, which it will
        /// not be for imported data, but which it may be for locally defined
        /// data.
        // TODO: Be more explicit about whether a collection is available,
        // although one can always produce it from an arrangement, and it
        // seems generally advantageous to do that instead (to avoid cloning
        // rows, by using `mfp` first on borrowed data).
        keys: AvailableCollections,
        /// The actions to take when introducing the collection.
        plan: GetPlan,
    },
    /// Binds `value` to `id`, and then results in `body` with that binding.
    ///
    /// This stage has the effect of sharing `value` across multiple possible
    /// uses in `body`, and is the only mechanism we have for sharing collection
    /// information across parts of a dataflow.
    ///
    /// The binding is not available outside of `body`.
    Let {
        /// The local identifier to be used, available to `body` as `Id::Local(id)`.
        id: LocalId,
        /// The collection that should be bound to `id`.
        value: Box<LirRelationExpr>,
        /// The collection that results, which is allowed to contain `Get` stages
        /// that reference `Id::Local(id)`.
        body: Box<LirRelationExpr>,
    },
    /// Binds `values` to `ids`, evaluates them potentially recursively, and returns `body`.
    ///
    /// All bindings are available to all bindings, and to `body`.
    /// The contents of each binding are initially empty, and then updated through a sequence
    /// of iterations in which each binding is updated in sequence, from the most recent values
    /// of all bindings.
    LetRec {
        /// The local identifiers to be used, available to `body` as `Id::Local(id)`.
        ids: Vec<LocalId>,
        /// The collection that should be bound to `id`.
        values: Vec<LirRelationExpr>,
        /// Maximum number of iterations. See further info on the MIR `LetRec`.
        limits: Vec<Option<LetRecLimit>>,
        /// The collection that results, which is allowed to contain `Get` stages
        /// that reference `Id::Local(id)`.
        body: Box<LirRelationExpr>,
    },
    /// Map, Filter, and Project operators.
    ///
    /// This stage contains work that we would ideally like to fuse to other plan
    /// stages, but for practical reasons cannot. For example: threshold, topk,
    /// and sometimes reduce stages are not able to absorb this operator.
    Mfp {
        /// The input collection.
        input: Box<LirRelationExpr>,
        /// Linear operator to apply to each record.
        mfp: MfpPlan<LirScalarExpr>,
        /// Whether the input is from an arrangement, and if so,
        /// whether we can seek to a specific value therein
        input_key_val: Option<(Vec<LirScalarExpr>, Option<Row>)>,
    },
    /// A variable number of output records for each input record.
    ///
    /// This stage is a bit of a catch-all for logic that does not easily fit in
    /// map stages. This includes table valued functions, but also functions of
    /// multiple arguments, and functions that modify the sign of updates.
    ///
    /// This stage allows a `MapFilterProject` operator to be fused to its output,
    /// and this can be very important as otherwise the output of `func` is just
    /// appended to the input record, for as many outputs as it has. This has the
    /// unpleasant default behavior of repeating potentially large records that
    /// are being unpacked, producing quadratic output in those cases. Instead,
    /// in these cases use a `mfp` member that projects away these large fields.
    FlatMap {
        /// The particular arrangement of the input we expect to use,
        /// if any
        input_key: Option<Vec<LirScalarExpr>>,
        /// The input collection.
        input: Box<LirRelationExpr>,
        /// Expressions that for each row prepare the arguments to `func`.
        exprs: Vec<LirScalarExpr>,
        /// The variable-record emitting function.
        func: TableFunc,
        /// Linear operator to apply to each record produced by `func`.
        mfp_after: MfpPlan<LirScalarExpr>,
    },
    /// A multiway relational equijoin, with fused map, filter, and projection.
    ///
    /// This stage performs a multiway join among `inputs`, using the equality
    /// constraints expressed in `plan`. The plan also describes the implementation
    /// strategy we will use, and any pushed down per-record work.
    Join {
        /// An ordered list of inputs that will be joined.
        inputs: Vec<LirRelationExpr>,
        /// Detailed information about the implementation of the join.
        ///
        /// This includes information about the implementation strategy, but also
        /// any map, filter, project work that we might follow the join with, but
        /// potentially pushed down into the implementation of the join.
        plan: JoinPlan,
    },
    /// Aggregation by key.
    Reduce {
        /// The particular arrangement of the input we expect to use,
        /// if any
        input_key: Option<Vec<LirScalarExpr>>,
        /// The input collection.
        input: Box<LirRelationExpr>,
        /// A plan for changing input records into key, value pairs.
        key_val_plan: KeyValPlan,
        /// A plan for performing the reduce.
        ///
        /// The implementation of reduction has several different strategies based
        /// on the properties of the reduction, and the input itself. Please check
        /// out the documentation for this type for more detail.
        plan: ReducePlan,
        /// An MFP that must be applied to results. The projection part of this
        /// MFP must preserve the key for the reduction; otherwise, the results
        /// become undefined. Additionally, the MFP is guaranteed to be free from
        /// temporal predicates so that it can be readily evaluated.
        mfp_after: SafeMfpPlan<LirScalarExpr>,
        /// Strategy for forming the internal input arrangement built by `Reduce`
        /// (materialized via `key_val_plan`).
        ///
        /// Set by the lowering from the input's `has_future_updates` flag. The
        /// renderer applies it to the keyed `(key, val)` stream feeding the
        /// reduce. See `render_reduce` for the rationale on why this is
        /// plumbed through `Reduce` rather than handled at the arrangement site.
        ///
        /// Note: unrelated to the hash buckets used by hierarchical reductions
        /// (e.g. `ReducePlan::Hierarchical`'s `buckets`), which are an internal
        /// sharding scheme for `min`/`max`-style aggregations. Here "bucketing"
        /// refers exclusively to temporal (time-domain) bucketing of
        /// future-stamped updates.
        temporal_bucketing_strategy: ArrangementStrategy,
    },
    /// Key-based "Top K" operator, retaining the first K records in each group.
    TopK {
        /// The input collection.
        input: Box<LirRelationExpr>,
        /// A plan for performing the Top-K.
        ///
        /// The implementation of reduction has several different strategies based
        /// on the properties of the reduction, and the input itself. Please check
        /// out the documentation for this type for more detail.
        top_k_plan: TopKPlan,
        /// Strategy for bucketing the input collection ahead of the Top-K operator.
        ///
        /// Set by the lowering from the input's `has_future_updates` flag. The
        /// renderer applies it to the per-row input stream at the top of
        /// `render_topk`, covering all three `TopKPlan` arms uniformly. See
        /// `LirRelationNode::Reduce::temporal_bucketing_strategy` for the underlying
        /// convention.
        temporal_bucketing_strategy: ArrangementStrategy,
    },
    /// Inverts the sign of each update.
    Negate {
        /// The input collection.
        input: Box<LirRelationExpr>,
    },
    /// Filters records that accumulate negatively.
    ///
    /// Although the operator suppresses updates, it is a stateful operator taking
    /// resources proportional to the number of records with non-zero accumulation.
    Threshold {
        /// The input collection.
        input: Box<LirRelationExpr>,
        /// A plan for performing the threshold.
        ///
        /// The implementation of reduction has several different strategies based
        /// on the properties of the reduction, and the input itself. Please check
        /// out the documentation for this type for more detail.
        threshold_plan: ThresholdPlan,
    },
    /// Adds the contents of the input collections.
    ///
    /// Importantly, this is *multiset* union, so the multiplicities of records will
    /// add. This is in contrast to *set* union, where the multiplicities would be
    /// capped at one. A set union can be formed with `Union` followed by `Reduce`
    /// implementing the "distinct" operator.
    Union {
        /// The input collections
        inputs: Vec<LirRelationExpr>,
        /// Whether to consolidate the output, e.g., cancel negated records.
        consolidate_output: bool,
        /// Per-input bucketing strategies. Lockstep with `inputs`: index `i` is the
        /// strategy applied to `inputs[i]` before concatenation.
        ///
        /// Set by the lowering from each input's `has_future_updates` flag. Only
        /// consolidating Unions (`consolidate_output: true`) carry non-`Direct`
        /// entries, because bucketing only pays off ahead of a consolidating
        /// downstream operator. See `LirRelationNode::Reduce::temporal_bucketing_strategy`
        /// for the underlying convention.
        temporal_bucketing_strategies: Vec<ArrangementStrategy>,
    },
    /// Computes `Threshold(base - subtract)` reading two inputs co-arranged on
    /// the difference key. Both inputs must be available arranged by the key
    /// columns of `ensure_arrangement`. Produces one output arrangement keyed
    /// by those columns with empty value, matching the `Threshold` it replaces.
    SetDifference {
        /// The positive input (minuend), arranged by the difference key.
        base: Box<LirRelationExpr>,
        /// The negated input (subtrahend), arranged by the difference key.
        subtract: Box<LirRelationExpr>,
        /// Key/permutation/thinning of the output arrangement (thinning is empty).
        ensure_arrangement: (Vec<LirScalarExpr>, Vec<usize>, Vec<usize>),
    },
    /// The `input` plan, but with additional arrangements.
    ///
    /// This operator does not change the logical contents of `input`, but ensures
    /// that certain arrangements are available in the results. This operator can
    /// be important for e.g. the `Join` stage which benefits from multiple arrangements
    /// or to cap a `LirRelationExpr` so that indexes can be exported.
    ArrangeBy {
        /// The key that must be used to access the input.
        input_key: Option<Vec<LirScalarExpr>>,
        /// The input collection.
        input: Box<LirRelationExpr>,
        /// The MFP that must be applied to the input.
        input_mfp: MfpPlan<LirScalarExpr>,
        /// A list of arrangement keys, and possibly a raw collection,
        /// that will be added to those of the input. Does not include
        /// any other existing arrangements.
        forms: AvailableCollections,
        /// How the renderer should form the arrangements requested by `forms`.
        strategy: ArrangementStrategy,
    },
}

impl LirRelationNode {
    /// Iterates through references to child expressions.
    pub fn children(&self) -> impl Iterator<Item = &LirRelationExpr> {
        let mut first = None;
        let mut second = None;
        let mut rest = None;
        let mut last = None;

        use LirRelationNode::*;
        match self {
            Constant { .. } | Get { .. } => (),
            Let { value, body, .. } => {
                first = Some(&**value);
                second = Some(&**body);
            }
            LetRec { values, body, .. } => {
                rest = Some(values);
                last = Some(&**body);
            }
            Mfp { input, .. }
            | FlatMap { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input, .. }
            | Threshold { input, .. }
            | ArrangeBy { input, .. } => {
                first = Some(&**input);
            }
            SetDifference { base, subtract, .. } => {
                first = Some(&**base);
                second = Some(&**subtract);
            }
            Join { inputs, .. } | Union { inputs, .. } => {
                rest = Some(inputs);
            }
        }

        first
            .into_iter()
            .chain(second)
            .chain(rest.into_iter().flatten())
            .chain(last)
    }

    /// Iterates through mutable references to child expressions.
    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut LirRelationExpr> {
        let mut first = None;
        let mut second = None;
        let mut rest = None;
        let mut last = None;

        use LirRelationNode::*;
        match self {
            Constant { .. } | Get { .. } => (),
            Let { value, body, .. } => {
                first = Some(&mut **value);
                second = Some(&mut **body);
            }
            LetRec { values, body, .. } => {
                rest = Some(values);
                last = Some(&mut **body);
            }
            Mfp { input, .. }
            | FlatMap { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input, .. }
            | Threshold { input, .. }
            | ArrangeBy { input, .. } => {
                first = Some(&mut **input);
            }
            SetDifference { base, subtract, .. } => {
                first = Some(&mut **base);
                second = Some(&mut **subtract);
            }
            Join { inputs, .. } | Union { inputs, .. } => {
                rest = Some(inputs);
            }
        }

        first
            .into_iter()
            .chain(second)
            .chain(rest.into_iter().flatten())
            .chain(last)
    }
}

impl LirRelationNode {
    /// Attach an `lir_id` to a `LirRelationNode` to make a complete `LirRelationExpr`.
    pub fn as_plan(self, lir_id: LirId) -> LirRelationExpr {
        LirRelationExpr { lir_id, node: self }
    }
}

impl LirRelationExpr {
    /// Pretty-print this [LirRelationExpr] to a string.
    pub fn pretty(&self) -> String {
        let config = ExplainConfig::default();
        self.debug_explain(&config, None)
    }

    /// Pretty-print this [LirRelationExpr] to a string using a custom
    /// [ExplainConfig] and an optionally provided [ExprHumanizer].
    /// This is intended for debugging and tests, not users.
    pub fn debug_explain(
        &self,
        config: &ExplainConfig,
        humanizer: Option<&dyn ExprHumanizer>,
    ) -> String {
        text_string_at(self, || PlanRenderingContext {
            indent: Indent::default(),
            humanizer: humanizer.unwrap_or(&DummyHumanizer),
            annotations: BTreeMap::default(),
            config,
            ambiguous_ids: BTreeSet::default(),
        })
    }
}

/// How a `Get` stage will be rendered.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub enum GetPlan {
    /// Simply pass input arrangements on to the next stage.
    PassArrangements,
    /// Using the supplied key, optionally seek the row, and apply the MFP.
    Arrangement(Vec<LirScalarExpr>, Option<Row>, MfpPlan<LirScalarExpr>),
    /// Scan the input collection (unarranged) and apply the MFP.
    Collection(MfpPlan<LirScalarExpr>),
}

impl LirRelationExpr {
    /// Convert the dataflow description into one that uses render plans.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "finalize_dataflow")
    )]
    pub fn finalize_dataflow(
        desc: DataflowDescription<OptimizedMirRelationExpr>,
        features: &OptimizerFeatures,
        metrics: Option<&LoweringMetrics>,
    ) -> Result<DataflowDescription<Self>, String> {
        // First, we lower the dataflow description from MIR to LIR.
        let mut dataflow = Self::lower_dataflow(desc, features, metrics)?;

        // Subsequently, we perform plan refinements for the dataflow.
        Self::refine_source_mfps(&mut dataflow);

        // Recognize the co-arranged anti-side shape and fuse it into a single
        // `SetDifference` node. Gated behind a feature flag, dormant otherwise.
        if features.enable_fused_set_difference {
            Self::refine_set_difference(&mut dataflow);
        }

        // Note: `consolidate_output` for `Union` and per-input
        // `temporal_bucketing_strategies` are decided at lowering time (see the
        // `Union` arm of `lower_mir_expr_stack_safe`). The pre-existing
        // `refine_union_negate_consolidation` pass — which used to flip
        // `consolidate_output` to `true` for Unions with a `Negate` child — has
        // been folded into the lowering, since lowering is the only point where
        // the bucketing decision (which depends on `has_future_updates`) is
        // available.

        if dataflow.is_single_time() {
            // The relaxation of the `must_consolidate` flag performs an LIR-based
            // analysis and transform under checked recursion. By a similar argument
            // made in `from_mir`, we do not expect the recursion limit to be hit.
            // However, if that happens, we propagate an error to the caller.
            // To apply the transform, we first obtain monotonic source and index
            // global IDs and add them to a `TransformConfig` instance.
            let monotonic_ids = dataflow
                .source_imports
                .iter()
                .filter_map(|(id, source_import)| source_import.monotonic.then_some(*id))
                .chain(
                    dataflow
                        .index_imports
                        .iter()
                        .filter_map(|(_id, index_import)| {
                            if index_import.monotonic {
                                Some(index_import.desc.on_id)
                            } else {
                                None
                            }
                        }),
                )
                .collect::<BTreeSet<_>>();

            let config = TransformConfig { monotonic_ids };
            Self::refine_single_time_consolidation(&mut dataflow, &config)?;
        }

        soft_assert_eq_no_log!(dataflow.check_invariants(), Ok(()));

        mz_repr::explain::trace_plan(&dataflow);

        Ok(dataflow)
    }

    /// Lowers the dataflow description from MIR to LIR. To this end, the
    /// method collects all available arrangements and based on this information
    /// creates plans for every object to be built for the dataflow.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment ="mir_to_lir")
    )]
    fn lower_dataflow(
        desc: DataflowDescription<OptimizedMirRelationExpr>,
        features: &OptimizerFeatures,
        metrics: Option<&LoweringMetrics>,
    ) -> Result<DataflowDescription<Self>, String> {
        let context = lowering::Context::new(desc.debug_name.clone(), features, metrics);
        let dataflow = context.lower(desc)?;

        mz_repr::explain::trace_plan(&dataflow);

        Ok(dataflow)
    }

    /// Refines the source instance descriptions for sources imported by `dataflow` to
    /// push down common MFP expressions.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "refine_source_mfps")
    )]
    fn refine_source_mfps(dataflow: &mut DataflowDescription<Self>) {
        use crate::plan::scalar::mfp_plan_lir_to_mir;

        for (source_id, source_import) in dataflow.source_imports.iter_mut() {
            let source = &mut source_import.desc;
            let source_id = *source_id;
            let mut identity_present = false;

            // First pass: swap MfpPlans out of GetPlan::Collection nodes,
            // recording their LirId so we can put them back.
            let mut taken: Vec<(LirId, MfpPlan<LirScalarExpr>)> = Vec::new();
            for build_desc in dataflow.objects_to_build.iter_mut() {
                let mut todo = vec![&mut build_desc.plan];
                while let Some(expression) = todo.pop() {
                    let lir_id = expression.lir_id;
                    let node = &mut expression.node;
                    if let LirRelationNode::Get { id, plan, .. } = node {
                        if *id == mz_expr::Id::Global(source_id) {
                            match plan {
                                GetPlan::Collection(mfp_plan) => {
                                    let arity = mfp_plan.safe_mfp().projection.len();
                                    let placeholder = MfpPlan::from_parts(
                                        mz_expr::SafeMfpPlan::from_mfp(MapFilterProject::new(
                                            arity,
                                        )),
                                        Vec::new(),
                                        Vec::new(),
                                    );
                                    taken.push((lir_id, std::mem::replace(mfp_plan, placeholder)));
                                }
                                GetPlan::PassArrangements => {
                                    identity_present = true;
                                }
                                GetPlan::Arrangement(..) => {
                                    panic!("Surprising `GetPlan` for imported source: {:?}", plan);
                                }
                            }
                        }
                    } else {
                        todo.extend(node.children_mut());
                    }
                }
            }

            // Direct exports of sources are possible, and prevent pushdown.
            identity_present |= dataflow
                .index_exports
                .values()
                .any(|(x, _)| x.on_id == source_id);
            identity_present |= dataflow.sink_exports.values().any(|x| x.from == source_id);

            // Build a map from LirId → new MfpPlan to put back.
            let replacements: BTreeMap<LirId, MfpPlan<LirScalarExpr>> =
                if !identity_present && !taken.is_empty() {
                    // Convert LIR MfpPlans → MIR MapFilterProjects by folding
                    // temporal bounds back as mz_now() predicates, so that
                    // extract_common's column remapping applies uniformly.
                    let mut mir_mfps: Vec<(LirId, MapFilterProject<MirScalarExpr>)> = taken
                        .into_iter()
                        .map(|(lir_id, lir_plan)| {
                            let mir_mfp = mfp_plan_lir_to_mir(lir_plan).into_map_filter_project();
                            (lir_id, mir_mfp)
                        })
                        .collect();
                    let mut mfp_refs: Vec<&mut MapFilterProject<MirScalarExpr>> =
                        mir_mfps.iter_mut().map(|(_, mfp)| mfp).collect();

                    let common = MapFilterProject::extract_common(&mut mfp_refs[..]);
                    let mut source_mfp = if let Some(mfp) = source.arguments.operators.take() {
                        MapFilterProject::compose(mfp, common)
                    } else {
                        common
                    };
                    source_mfp.optimize();
                    source.arguments.operators = Some(source_mfp);

                    // Convert mutated MIR MFPs back to LIR MfpPlans.
                    mir_mfps
                        .into_iter()
                        .map(|(lir_id, mir_mfp)| (lir_id, mfp_mir_to_lir_plan(mir_mfp)))
                        .collect()
                } else {
                    taken.into_iter().collect()
                };

            // Second pass: put the MfpPlans back by LirId.
            for build_desc in dataflow.objects_to_build.iter_mut() {
                let mut todo = vec![&mut build_desc.plan];
                while let Some(expression) = todo.pop() {
                    if let Some(replacement) = replacements.get(&expression.lir_id) {
                        if let LirRelationNode::Get {
                            plan: GetPlan::Collection(mfp_plan),
                            ..
                        } = &mut expression.node
                        {
                            *mfp_plan = replacement.clone();
                        } else {
                            panic!(
                                "LirId {:?} was a GetPlan::Collection but is now {:?}",
                                expression.lir_id, expression.node
                            );
                        }
                    }
                    todo.extend(expression.node.children_mut());
                }
            }
        }
        mz_repr::explain::trace_plan(dataflow);
    }

    /// Recognizes the co-arranged anti-side shape
    /// `Threshold::Basic(ArrangeBy(Union(base, Negate(subtract))))` and rewrites it in place
    /// to a single [`LirRelationNode::SetDifference`] node.
    ///
    /// The rewrite fires only when both difference inputs are genuinely arranged on the
    /// threshold key and neither carries temporal bucketing. See [`match_anti_side`] for the
    /// full set of decline conditions.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "refine_set_difference")
    )]
    fn refine_set_difference(dataflow: &mut DataflowDescription<Self>) {
        for build_desc in dataflow.objects_to_build.iter_mut() {
            let mut todo = vec![&mut build_desc.plan];
            while let Some(expression) = todo.pop() {
                if let Some((base, subtract, ensure_arrangement)) =
                    match_anti_side(&expression.node)
                {
                    *expression = LirRelationNode::SetDifference {
                        base,
                        subtract,
                        ensure_arrangement,
                    }
                    .as_plan(expression.lir_id);
                }
                todo.extend(expression.node.children_mut());
            }
        }
        mz_repr::explain::trace_plan(dataflow);
    }

    /// Refines the plans of objects to be built as part of a single-time `dataflow` to relax
    /// the setting of the `must_consolidate` attribute of monotonic operators, if necessary,
    /// whenever the input is deemed to be physically monotonic.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "refine_single_time_consolidation")
    )]
    fn refine_single_time_consolidation(
        dataflow: &mut DataflowDescription<Self>,
        config: &TransformConfig,
    ) -> Result<(), String> {
        // We should only reach here if we have a one-shot SELECT query, i.e.,
        // a single-time dataflow.
        assert!(dataflow.is_single_time());

        let transform = transform::RelaxMustConsolidate;
        for build_desc in dataflow.objects_to_build.iter_mut() {
            transform
                .transform(config, &mut build_desc.plan)
                .map_err(|_| "Maximum recursion limit error in consolidation relaxation.")?;
        }
        mz_repr::explain::trace_plan(dataflow);
        Ok(())
    }
}

/// Matches the co-arranged anti-side shape
/// `Threshold::Basic(ArrangeBy(raw=false)(Union([base, Negate(subtract)])))` and, when every
/// decline condition holds, returns the `(base, subtract, ensure_arrangement)` components of a
/// [`LirRelationNode::SetDifference`] rewrite.
///
/// `base` is the positive `Union` arm taken as-is; `subtract` is the child of the `Negate` arm
/// (the render reintroduces the negation internally). `ensure_arrangement` is copied from the
/// matched `Threshold` so the fused node advertises the same output arrangement.
///
/// Returns `None` (decline, leaving the plan unchanged) unless all of the following hold:
/// * the node is a `Threshold::Basic`,
/// * its input is an `ArrangeBy` whose `forms` are arranged (not raw),
/// * that `ArrangeBy`'s input is a `Union` with exactly two inputs, exactly one a `Negate`,
/// * neither `Union` arm carries a non-`Direct` temporal bucketing strategy, and
/// * both arms are arranged on the threshold key columns (see [`arm_arrangement_matches`]).
///
/// `consolidate_output` need not be checked: lowering sets it precisely when a `Union` arm is a
/// `Negate`, so the two-input-one-`Negate` match already implies it.
fn match_anti_side(
    node: &LirRelationNode,
) -> Option<(
    Box<LirRelationExpr>,
    Box<LirRelationExpr>,
    (Vec<LirScalarExpr>, Vec<usize>, Vec<usize>),
)> {
    let LirRelationNode::Threshold {
        input,
        threshold_plan: ThresholdPlan::Basic(basic),
    } = node
    else {
        return None;
    };
    let ensure_arrangement = basic.ensure_arrangement.clone();
    let threshold_key = &ensure_arrangement.0;

    let LirRelationNode::ArrangeBy {
        input: union_expr,
        forms,
        ..
    } = &input.node
    else {
        return None;
    };
    if forms.raw {
        return None;
    }

    let LirRelationNode::Union {
        inputs,
        temporal_bucketing_strategies,
        ..
    } = &union_expr.node
    else {
        return None;
    };
    if inputs.len() != 2 {
        return None;
    }
    // The `SetDifference` node has no slot for a temporal bucketing strategy and the phase-1
    // render reconstructs a plain consolidating concat that does not reproduce per-input
    // bucketing. Fusing a bucketable arm would silently drop the bucketing and regress, so
    // decline whenever any arm requests a non-`Direct` (the no-op default) strategy.
    if temporal_bucketing_strategies
        .iter()
        .any(|strategy| !matches!(strategy, ArrangementStrategy::Direct))
    {
        return None;
    }

    // Exactly one arm must be a `Negate`.
    let mut negate_arms = inputs
        .iter()
        .enumerate()
        .filter(|(_, arm)| matches!(arm.node, LirRelationNode::Negate { .. }));
    let (negate_idx, negate_arm) = negate_arms.next()?;
    if negate_arms.next().is_some() {
        return None;
    }
    let LirRelationNode::Negate { input: subtract } = &negate_arm.node else {
        return None;
    };
    let base_arm = &inputs[1 - negate_idx];

    if !arm_arrangement_matches(base_arm, threshold_key)
        || !arm_arrangement_matches(subtract, threshold_key)
    {
        return None;
    }

    Some((
        Box::new(base_arm.clone()),
        subtract.clone(),
        ensure_arrangement,
    ))
}

/// Reports whether a `Union` arm is available arranged on the `key` columns, permitting an
/// optional leading projection-to-key MFP.
///
/// The arm may be wrapped in a single `Mfp` node whose plan only projects (no map, filter, or
/// temporal bound); such a projection is stripped before inspecting the underlying node. A
/// non-projection `Mfp` is not stripped, leaving `Mfp` as the node type, which is rejected.
///
/// The underlying node must be a `Get` or `ArrangeBy`, the only nodes that advertise their
/// arrangements at the node level. Any read MFP the node applies (a `GetPlan` MFP or an
/// `ArrangeBy`'s `input_mfp`) must itself be projection-only, since a filter or map between the
/// arm and its arrangement changes the collection the operator would consume. Only the key
/// columns must match `key`; permutation and thinning may differ between arms, because the
/// operator sums diffs per key and ignores value contents.
fn arm_arrangement_matches(arm: &LirRelationExpr, key: &[LirScalarExpr]) -> bool {
    let node = match &arm.node {
        LirRelationNode::Mfp { input, mfp, .. } if is_projection_only(mfp) => &input.node,
        node => node,
    };

    let advertises_key = |arranged: &[(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>)]| -> bool {
        arranged.iter().any(|(k, _, _)| k.as_slice() == key)
    };

    match node {
        LirRelationNode::Get { keys, plan, .. } => {
            get_plan_projection_only(plan) && advertises_key(&keys.arranged)
        }
        LirRelationNode::ArrangeBy {
            input_key,
            input_mfp,
            forms,
            ..
        } => {
            // The arrangement on `key` is either built by this node (its `forms`) or the input
            // arrangement it reads through (`input_key`).
            is_projection_only(input_mfp)
                && (advertises_key(&forms.arranged)
                    || input_key
                        .as_deref()
                        .is_some_and(|input_key| input_key == key))
        }
        _ => false,
    }
}

/// Reports whether a `GetPlan` reads its collection with at most a projection: no seek, no
/// filter, no map.
fn get_plan_projection_only(plan: &GetPlan) -> bool {
    match plan {
        GetPlan::PassArrangements => true,
        // A seek is a literal equality constraint, so it disqualifies the plan.
        GetPlan::Arrangement(_key, seek_row, mfp) => seek_row.is_none() && is_projection_only(mfp),
        GetPlan::Collection(mfp) => is_projection_only(mfp),
    }
}

/// Reports whether an `MfpPlan` only projects columns: no map, no filter, no temporal bound.
fn is_projection_only(mfp: &MfpPlan<LirScalarExpr>) -> bool {
    let safe_mfp = mfp.safe_mfp();
    safe_mfp.expressions.is_empty() && safe_mfp.predicates.is_empty() && !mfp.has_temporal_bounds()
}

impl CollectionPlan for LirRelationNode {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        match self {
            LirRelationNode::Constant { rows: _ } => (),
            LirRelationNode::Get {
                id,
                keys: _,
                plan: _,
            } => match id {
                Id::Global(id) => {
                    out.insert(*id);
                }
                Id::Local(_) => (),
            },
            LirRelationNode::Let { id: _, value, body } => {
                value.depends_on_into(out);
                body.depends_on_into(out);
            }
            LirRelationNode::LetRec {
                ids: _,
                values,
                limits: _,
                body,
            } => {
                for value in values.iter() {
                    value.depends_on_into(out);
                }
                body.depends_on_into(out);
            }
            LirRelationNode::Join { inputs, plan: _ }
            | LirRelationNode::Union {
                inputs,
                consolidate_output: _,
                temporal_bucketing_strategies: _,
            } => {
                for input in inputs {
                    input.depends_on_into(out);
                }
            }
            LirRelationNode::SetDifference {
                base,
                subtract,
                ensure_arrangement: _,
            } => {
                base.depends_on_into(out);
                subtract.depends_on_into(out);
            }
            LirRelationNode::Mfp {
                input,
                mfp: _,
                input_key_val: _,
            }
            | LirRelationNode::FlatMap {
                input_key: _,
                input,
                exprs: _,
                func: _,
                mfp_after: _,
            }
            | LirRelationNode::ArrangeBy {
                input_key: _,
                input,
                input_mfp: _,
                forms: _,
                strategy: _,
            }
            | LirRelationNode::Reduce {
                input_key: _,
                input,
                key_val_plan: _,
                plan: _,
                mfp_after: _,
                temporal_bucketing_strategy: _,
            }
            | LirRelationNode::TopK {
                input,
                top_k_plan: _,
                temporal_bucketing_strategy: _,
            }
            | LirRelationNode::Negate { input }
            | LirRelationNode::Threshold {
                input,
                threshold_plan: _,
            } => {
                input.depends_on_into(out);
            }
        }
    }
}

impl CollectionPlan for LirRelationExpr {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        self.node.depends_on_into(out);
    }
}

/// Returns bucket sizes, descending, suitable for hierarchical decomposition of an operator, based
/// on the expected number of rows that will have the same group key.
fn bucketing_of_expected_group_size(expected_group_size: Option<u64>) -> Vec<u64> {
    // NOTE(vmarcos): The fan-in of 16 defined below is used in the tuning advice built-in view
    // mz_introspection.mz_expected_group_size_advice.
    let mut buckets = vec![];
    let mut current = 16;

    // Plan for 4B records in the expected case if the user didn't specify a group size.
    let limit = expected_group_size.unwrap_or(4_000_000_000);

    // Distribute buckets in powers of 16, so that we can strike a balance between how many inputs
    // each layer gets from the preceding layer, while also limiting the number of layers.
    while current < limit {
        buckets.push(current);
        current = current.saturating_mul(16);
    }

    buckets.reverse();
    buckets
}
