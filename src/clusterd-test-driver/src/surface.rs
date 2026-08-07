// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The compute rendering surface, enumerated as [`SurfaceCell`]s.
//!
//! A cell is one point in the space of render code paths: an LIR operator plus
//! the variant choices that select a distinct path through `mz_compute::render`.
//! [`cells_of_plan`] reports which cells a [`RenderPlan`] realizes, which serves
//! two purposes:
//!
//!  * **Coverage accounting.** A generated suite can report which cells it
//!    reached and fail on the ones it did not, so "covers the whole surface" is a
//!    measured claim rather than an aspiration.
//!  * **Claim checking.** A workload declares the cells it intends to exercise
//!    and the runner asserts the realized plan matches. Without this, a generator
//!    that stops producing (say) bucketed hierarchical reductions keeps passing
//!    while silently testing less. That failure mode is invisible by
//!    construction, so it has to be checked rather than assumed.
//!
//! # Granularity contract
//!
//! A cell distinguishes exactly what the *plan* distinguishes, because that is
//! both what this module can observe and what a generator can target. Two
//! consequences worth naming:
//!
//!  * Concrete keys, ids, and literal values are not part of a cell. Folding them
//!    in would make the space unbounded and coverage meaningless.
//!  * [`Expr::FlatMap`] does not split by [`TableFunc`]: `render::flat_map` calls
//!    `func.eval` generically and never branches on the function, so the set of
//!    table functions is `mz-expr`'s surface, not compute's render surface. The
//!    FlatMap cells are the input arrangement and the fused MFP, which are what
//!    the renderer actually branches on.
//!
//! # Exhaustiveness
//!
//! Every `match` here is exhaustive with no wildcard arm, over [`Expr`] and over
//! each plan sub-enum ([`GetPlan`], [`JoinPlan`], [`ReducePlan`],
//! [`HierarchicalPlan`], [`BasicPlan`], [`TopKPlan`], [`ThresholdPlan`],
//! [`ArrangementStrategy`]). Adding an LIR variant therefore fails to compile
//! until it is classified, which is the mechanism that keeps the taxonomy honest
//! as the surface grows. Resist adding a `_ =>` arm.

use std::collections::BTreeSet;
use std::fmt;

use mz_compute_types::plan::reduce::{BasicPlan, HierarchicalPlan, ReducePlan};
use mz_compute_types::plan::render_plan::{Expr, RenderPlan};
use mz_compute_types::plan::threshold::ThresholdPlan;
use mz_compute_types::plan::top_k::TopKPlan;
use mz_compute_types::plan::{ArrangementStrategy, AvailableCollections, GetPlan};
use mz_expr::TableFunc;
use serde::{Deserialize, Serialize};

/// How a `Get` obtains its collection.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum GetKind {
    /// Pass through an unarranged stream.
    PassRaw,
    /// Pass through existing arrangements.
    PassArranged,
    /// Read an arrangement by key, scanning it.
    ArrangementScan,
    /// Seek a specific value in an arrangement.
    ArrangementLookup,
    /// Read the unarranged collection and apply an MFP.
    Collection,
}

/// Whether an operator reads its input as a stream or through an arrangement.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum InputKind {
    /// An unarranged stream.
    Stream,
    /// An arrangement, read by key.
    Arranged,
    /// An arrangement, seeking one value.
    Lookup,
}

/// The join implementation strategy.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum JoinKind {
    /// A linear (differential) join.
    Linear,
    /// A delta join.
    Delta,
}

/// The reduction strategy.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum ReduceKind {
    /// Distinct keys, no aggregates.
    Distinct,
    /// Invertible, associative aggregates accumulated in the diff field.
    Accumulable,
    /// Hierarchical aggregates over a monotonic input.
    HierarchicalMonotonic,
    /// Hierarchical aggregates over a monotonic input, with a consolidating stage.
    HierarchicalMonotonicConsolidating,
    /// Hierarchical aggregates reduced through a bucket tree.
    HierarchicalBucketed,
    /// A single non-incremental aggregate.
    BasicSingle,
    /// Several non-incremental aggregates, collated.
    BasicMultiple,
}

/// The Top-K strategy.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum TopKKind {
    /// `LIMIT 1` over a monotonic input.
    MonotonicTop1,
    /// A monotonic-input Top-K with no limit.
    MonotonicTopK,
    /// A monotonic-input Top-K with a limit.
    MonotonicTopKLimited,
    /// The general, non-monotonic Top-K.
    Basic,
}

/// How arrangements are formed, which is the `ArrangementStrategy` axis threaded
/// through `Reduce`, `TopK`, `Union`, and `ArrangeBy`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum StrategyKind {
    /// Arrange the input directly.
    Direct,
    /// Insert a temporal bucketing operator first.
    TemporalBucketing,
}

/// What an `ArrangeBy` produces.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum FormsKind {
    /// No arrangement, a raw stream only.
    RawOnly,
    /// One arrangement keyed by an empty key, so a single group.
    EmptyKey,
    /// One keyed arrangement.
    One,
    /// Several keyed arrangements over the same input.
    Several,
}

/// Whether a recursive binding bounds its iteration.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum LetRecLimitKind {
    /// Iterate to fixpoint.
    Unbounded,
    /// Error out at the iteration limit.
    Limited,
    /// Return the partial result at the iteration limit.
    LimitedReturnAt,
}

/// One point in the compute rendering surface.
///
/// [`fmt::Display`] renders the cell name (`Reduce/Bucketed/Direct/NoMfp` and so
/// on). The name is the identity used in corpora and coverage reports, so treat
/// it as a wire format: renaming a variant invalidates every committed workload
/// that claims it.
///
/// The derived `Ord` sorts by variant declaration order, so a `BTreeSet` of cells
/// groups by LIR operator in plan-definition order rather than alphabetically.
/// That is the useful order for a coverage report, since it reads like the
/// operator list itself. Keep the variants in [`Expr`] order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceCell {
    /// A literal collection, of rows or of an error.
    Constant {
        /// Whether the constant is an error rather than rows.
        error: bool,
    },
    /// A reference to a bound or imported collection.
    Get {
        /// How the collection is obtained.
        kind: GetKind,
    },
    /// Map, filter, and project.
    Mfp {
        /// Whether the MFP carries `mz_now()` temporal bounds.
        temporal: bool,
        /// How the input is read.
        input: InputKind,
    },
    /// A table function with a fused output MFP.
    FlatMap {
        /// How the input is read.
        input: InputKind,
        /// Whether a non-trivial MFP is fused onto the output.
        mfp_after: bool,
    },
    /// A multiway equijoin.
    Join {
        /// The implementation strategy.
        kind: JoinKind,
        /// The number of joined inputs, capped (see [`JOIN_ARITY_CAP`]) so the
        /// cell space stays closed.
        inputs: usize,
    },
    /// Aggregation by key.
    Reduce {
        /// The reduction strategy.
        kind: ReduceKind,
        /// How the internal input arrangement is formed.
        strategy: StrategyKind,
        /// Whether a non-trivial MFP is applied to the results.
        mfp_after: bool,
    },
    /// Top-K within each group.
    TopK {
        /// The Top-K strategy.
        kind: TopKKind,
        /// How the input is bucketed.
        strategy: StrategyKind,
    },
    /// Sign inversion.
    Negate,
    /// Suppression of negatively-accumulating records.
    Threshold,
    /// Multiset union.
    Union {
        /// Whether the output is consolidated.
        consolidate: bool,
        /// Whether any input is temporally bucketed.
        bucketed: bool,
    },
    /// Additional arrangements over an unchanged collection.
    ArrangeBy {
        /// What is produced.
        forms: FormsKind,
        /// How the arrangements are formed.
        strategy: StrategyKind,
        /// How the input is read.
        input: InputKind,
    },
    /// A non-recursive local binding.
    Let,
    /// A recursive local binding.
    LetRec {
        /// Whether iteration is bounded.
        limit: LetRecLimitKind,
    },
}

/// Joins of more inputs than this collapse into one cell.
///
/// The renderer's per-input work is uniform past a small arity, so an uncapped
/// input count would grow the cell space without covering new code. Two and
/// three inputs are worth separating: a two-input join is always linear, and
/// three is where delta joins and multi-stage linear plans appear.
pub const JOIN_ARITY_CAP: usize = 4;

impl fmt::Display for SurfaceCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A slash-separated path, most significant part first, so cell names sort
        // into operator families.
        match self {
            SurfaceCell::Constant { error } => {
                write!(f, "Constant/{}", if *error { "Error" } else { "Rows" })
            }
            SurfaceCell::Get { kind } => write!(f, "Get/{}", kind.label()),
            SurfaceCell::Mfp { temporal, input } => write!(
                f,
                "Mfp/{}/{}",
                if *temporal { "Temporal" } else { "Plain" },
                input.label()
            ),
            SurfaceCell::FlatMap { input, mfp_after } => write!(
                f,
                "FlatMap/{}/{}",
                input.label(),
                if *mfp_after { "MfpAfter" } else { "NoMfp" }
            ),
            SurfaceCell::Join { kind, inputs } => {
                write!(f, "Join/{}/{inputs}", kind.label())
            }
            SurfaceCell::Reduce {
                kind,
                strategy,
                mfp_after,
            } => write!(
                f,
                "Reduce/{}/{}/{}",
                kind.label(),
                strategy.label(),
                if *mfp_after { "MfpAfter" } else { "NoMfp" }
            ),
            SurfaceCell::TopK { kind, strategy } => {
                write!(f, "TopK/{}/{}", kind.label(), strategy.label())
            }
            SurfaceCell::Negate => write!(f, "Negate"),
            SurfaceCell::Threshold => write!(f, "Threshold"),
            SurfaceCell::Union {
                consolidate,
                bucketed,
            } => write!(
                f,
                "Union/{}/{}",
                if *consolidate {
                    "Consolidating"
                } else {
                    "Plain"
                },
                if *bucketed { "Bucketed" } else { "Direct" }
            ),
            SurfaceCell::ArrangeBy {
                forms,
                strategy,
                input,
            } => write!(
                f,
                "ArrangeBy/{}/{}/{}",
                forms.label(),
                strategy.label(),
                input.label()
            ),
            SurfaceCell::Let => write!(f, "Let"),
            SurfaceCell::LetRec { limit } => write!(f, "LetRec/{}", limit.label()),
        }
    }
}

impl GetKind {
    fn label(self) -> &'static str {
        match self {
            GetKind::PassRaw => "PassRaw",
            GetKind::PassArranged => "PassArranged",
            GetKind::ArrangementScan => "ArrangementScan",
            GetKind::ArrangementLookup => "ArrangementLookup",
            GetKind::Collection => "Collection",
        }
    }
}

impl InputKind {
    fn label(self) -> &'static str {
        match self {
            InputKind::Stream => "Stream",
            InputKind::Arranged => "Arranged",
            InputKind::Lookup => "Lookup",
        }
    }
}

impl JoinKind {
    fn label(self) -> &'static str {
        match self {
            JoinKind::Linear => "Linear",
            JoinKind::Delta => "Delta",
        }
    }
}

impl ReduceKind {
    fn label(self) -> &'static str {
        match self {
            ReduceKind::Distinct => "Distinct",
            ReduceKind::Accumulable => "Accumulable",
            ReduceKind::HierarchicalMonotonic => "Monotonic",
            ReduceKind::HierarchicalMonotonicConsolidating => "MonotonicConsolidating",
            ReduceKind::HierarchicalBucketed => "Bucketed",
            ReduceKind::BasicSingle => "BasicSingle",
            ReduceKind::BasicMultiple => "BasicMultiple",
        }
    }
}

impl TopKKind {
    fn label(self) -> &'static str {
        match self {
            TopKKind::MonotonicTop1 => "MonotonicTop1",
            TopKKind::MonotonicTopK => "MonotonicTopK",
            TopKKind::MonotonicTopKLimited => "MonotonicTopKLimited",
            TopKKind::Basic => "Basic",
        }
    }
}

impl StrategyKind {
    fn label(self) -> &'static str {
        match self {
            StrategyKind::Direct => "Direct",
            StrategyKind::TemporalBucketing => "Bucketed",
        }
    }
}

impl FormsKind {
    fn label(self) -> &'static str {
        match self {
            FormsKind::RawOnly => "RawOnly",
            FormsKind::EmptyKey => "EmptyKey",
            FormsKind::One => "One",
            FormsKind::Several => "Several",
        }
    }
}

impl LetRecLimitKind {
    fn label(self) -> &'static str {
        match self {
            LetRecLimitKind::Unbounded => "Unbounded",
            LetRecLimitKind::Limited => "Limited",
            LetRecLimitKind::LimitedReturnAt => "LimitedReturnAt",
        }
    }
}

/// Classify an `ArrangementStrategy`.
///
/// Exhaustive by design: a new strategy must be given a cell.
fn strategy_kind(strategy: &ArrangementStrategy) -> StrategyKind {
    match strategy {
        ArrangementStrategy::Direct => StrategyKind::Direct,
        ArrangementStrategy::TemporalBucketing => StrategyKind::TemporalBucketing,
    }
}

/// Classify how an operator reads its input, from the optional input key and an
/// optional seek value.
fn input_kind(input_key: Option<&Vec<mz_compute_types::plan::scalar::LirScalarExpr>>) -> InputKind {
    match input_key {
        None => InputKind::Stream,
        Some(_) => InputKind::Arranged,
    }
}

/// Classify what a set of requested arrangement forms produces.
fn forms_kind(forms: &AvailableCollections) -> FormsKind {
    match forms.arranged.len() {
        // `ArrangeBy` with no arrangements is the raw-stream passthrough the
        // humanizer prints as "Unarranged Raw Stream".
        0 => FormsKind::RawOnly,
        1 if forms.arranged[0].0.is_empty() => FormsKind::EmptyKey,
        1 => FormsKind::One,
        _ => FormsKind::Several,
    }
}

/// Whether a `TableFunc` is classified. Present so the granularity decision is
/// stated at the call site rather than implied: the renderer evaluates every
/// table function through the same operator, so the function does not select a
/// render path and is deliberately not a cell axis.
fn _table_func_is_not_a_cell_axis(_func: &TableFunc) {}

/// Classify a single [`Expr`] into its surface cell.
///
/// The `match` is exhaustive with no wildcard arm. That is the enforcement
/// mechanism described in the module docs: a new LIR variant breaks this
/// function until it is classified.
pub fn classify_expr(expr: &Expr) -> SurfaceCell {
    match expr {
        Expr::Constant { rows } => SurfaceCell::Constant {
            error: rows.is_err(),
        },
        Expr::Get { id: _, keys, plan } => SurfaceCell::Get {
            kind: match plan {
                GetPlan::PassArrangements => {
                    // The humanizer draws the same distinction: a raw-only
                    // passthrough is a stream, anything with arrangements is not.
                    if keys.raw && keys.arranged.is_empty() {
                        GetKind::PassRaw
                    } else {
                        GetKind::PassArranged
                    }
                }
                GetPlan::Arrangement(_key, Some(_val), _mfp) => GetKind::ArrangementLookup,
                GetPlan::Arrangement(_key, None, _mfp) => GetKind::ArrangementScan,
                GetPlan::Collection(_mfp) => GetKind::Collection,
            },
        },
        Expr::Mfp {
            input: _,
            mfp,
            input_key_val,
        } => {
            let (_safe, lower, upper) = mfp.as_parts();
            SurfaceCell::Mfp {
                temporal: !lower.is_empty() || !upper.is_empty(),
                input: match input_key_val {
                    None => InputKind::Stream,
                    Some((_key, None)) => InputKind::Arranged,
                    Some((_key, Some(_val))) => InputKind::Lookup,
                },
            }
        }
        Expr::FlatMap {
            input_key,
            input: _,
            exprs: _,
            func,
            mfp_after,
        } => {
            _table_func_is_not_a_cell_axis(func);
            SurfaceCell::FlatMap {
                input: input_kind(input_key.as_ref()),
                mfp_after: !mfp_after.safe_mfp().is_identity(),
            }
        }
        Expr::Join { inputs, plan } => SurfaceCell::Join {
            kind: match plan {
                mz_compute_types::plan::join::JoinPlan::Linear(_) => JoinKind::Linear,
                mz_compute_types::plan::join::JoinPlan::Delta(_) => JoinKind::Delta,
            },
            inputs: inputs.len().min(JOIN_ARITY_CAP),
        },
        Expr::Reduce {
            input_key: _,
            input: _,
            key_val_plan: _,
            plan,
            mfp_after,
            temporal_bucketing_strategy,
        } => SurfaceCell::Reduce {
            kind: reduce_kind(plan),
            strategy: strategy_kind(temporal_bucketing_strategy),
            mfp_after: !mfp_after.is_identity(),
        },
        Expr::TopK {
            input: _,
            top_k_plan,
            temporal_bucketing_strategy,
        } => SurfaceCell::TopK {
            kind: match top_k_plan {
                TopKPlan::MonotonicTop1(_) => TopKKind::MonotonicTop1,
                TopKPlan::MonotonicTopK(plan) if plan.limit.is_some() => {
                    TopKKind::MonotonicTopKLimited
                }
                TopKPlan::MonotonicTopK(_) => TopKKind::MonotonicTopK,
                TopKPlan::Basic(_) => TopKKind::Basic,
            },
            strategy: strategy_kind(temporal_bucketing_strategy),
        },
        Expr::Negate { input: _ } => SurfaceCell::Negate,
        Expr::Threshold {
            input: _,
            threshold_plan,
        } => match threshold_plan {
            // One variant today. Kept as a match so a second one must be
            // classified rather than silently folding into this cell.
            ThresholdPlan::Basic(_) => SurfaceCell::Threshold,
        },
        Expr::Union {
            inputs: _,
            consolidate_output,
            temporal_bucketing_strategies,
        } => SurfaceCell::Union {
            consolidate: *consolidate_output,
            // Per-input strategies collapse to "any input bucketed": the render
            // path is the same operator inserted per input, so the interesting
            // distinction is whether it appears at all.
            bucketed: temporal_bucketing_strategies
                .iter()
                .any(|s| strategy_kind(s) == StrategyKind::TemporalBucketing),
        },
        Expr::ArrangeBy {
            input_key,
            input: _,
            input_mfp: _,
            forms,
            strategy,
        } => SurfaceCell::ArrangeBy {
            forms: forms_kind(forms),
            strategy: strategy_kind(strategy),
            input: input_kind(input_key.as_ref()),
        },
    }
}

/// Classify a [`ReducePlan`], including its nested strategy enums.
fn reduce_kind(plan: &ReducePlan) -> ReduceKind {
    match plan {
        ReducePlan::Distinct => ReduceKind::Distinct,
        ReducePlan::Accumulable(_) => ReduceKind::Accumulable,
        ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(plan)) => {
            if plan.must_consolidate {
                ReduceKind::HierarchicalMonotonicConsolidating
            } else {
                ReduceKind::HierarchicalMonotonic
            }
        }
        ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(_)) => ReduceKind::HierarchicalBucketed,
        ReducePlan::Basic(BasicPlan::Single(_)) => ReduceKind::BasicSingle,
        ReducePlan::Basic(BasicPlan::Multiple(_)) => ReduceKind::BasicMultiple,
    }
}

/// Every surface cell realized by `plan`, including the cells of its `Let` and
/// `LetRec` binding structure and of nested recursive plans.
///
/// Returns a set, not a multiset: coverage asks whether a path was exercised at
/// all, and a plan that renders the same operator twice covers no more of the
/// surface than one that renders it once.
pub fn cells_of_plan(plan: &RenderPlan) -> BTreeSet<SurfaceCell> {
    let mut cells = BTreeSet::new();
    collect_plan(plan, &mut cells);
    cells
}

fn collect_plan(plan: &RenderPlan, cells: &mut BTreeSet<SurfaceCell>) {
    for stage in &plan.binds {
        for bind in &stage.lets {
            cells.insert(SurfaceCell::Let);
            collect_let_free(&bind.value, cells);
        }
        for bind in &stage.recs {
            cells.insert(SurfaceCell::LetRec {
                limit: match &bind.limit {
                    None => LetRecLimitKind::Unbounded,
                    Some(limit) if limit.return_at_limit => LetRecLimitKind::LimitedReturnAt,
                    Some(_) => LetRecLimitKind::Limited,
                },
            });
            // A recursive binding's value is itself a `RenderPlan`, so recurse.
            collect_plan(&bind.value, cells);
        }
    }
    collect_let_free(&plan.body, cells);
}

fn collect_let_free(
    plan: &mz_compute_types::plan::render_plan::LetFreePlan,
    cells: &mut BTreeSet<SurfaceCell>,
) {
    // `LetFreePlan` keeps its node map private and exposes it only by consuming
    // `destruct`, so walking it costs a clone. Acceptable in a test driver, and
    // it keeps the plan invariants owned by `compute-types`.
    let (nodes, _root, _order) = plan.clone().destruct();
    for node in nodes.values() {
        cells.insert(classify_expr(&node.expr));
    }
}

/// Render a set of cells as sorted, newline-separated names, for a golden block
/// or a coverage report.
pub fn render_cells(cells: &BTreeSet<SurfaceCell>) -> String {
    cells
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    use mz_persist_types::{PersistLocation, ShardId};
    use mz_repr::{GlobalId, Timestamp};

    /// The single-index dataflow realizes exactly the cells its shape implies: a
    /// raw-passthrough `Get` of the source and a one-form `ArrangeBy` over it.
    /// This is the classifier's end-to-end check against a real lowered plan.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn index_dataflow_cells() {
        let loc = PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        };
        let df = crate::dataflow::index_dataflow(
            GlobalId::User(1000),
            GlobalId::User(1001),
            ShardId::new(),
            loc,
            crate::data::sample_desc(),
            vec![0],
            Timestamp::from(0),
            Timestamp::from(1),
        )
        .unwrap();

        let cells = cells_of_plan(&df.objects_to_build[0].plan);
        // `Get` before `ArrangeBy`: cells sort in `Expr` declaration order.
        assert_eq!(
            render_cells(&cells),
            "Get/PassRaw\nArrangeBy/One/Direct/Stream",
        );
    }

    /// Cell names stay stable and sort in operator-declaration order, which is
    /// what lets a committed workload name the cells it claims.
    #[mz_ore::test]
    fn cell_names_are_stable() {
        let cells: BTreeSet<SurfaceCell> = [
            SurfaceCell::Negate,
            SurfaceCell::Threshold,
            SurfaceCell::Reduce {
                kind: ReduceKind::HierarchicalBucketed,
                strategy: StrategyKind::Direct,
                mfp_after: false,
            },
            SurfaceCell::TopK {
                kind: TopKKind::MonotonicTop1,
                strategy: StrategyKind::TemporalBucketing,
            },
            SurfaceCell::Join {
                kind: JoinKind::Delta,
                inputs: 3,
            },
        ]
        .into_iter()
        .collect();
        assert_eq!(
            render_cells(&cells),
            "Join/Delta/3\n\
             Reduce/Bucketed/Direct/NoMfp\n\
             TopK/MonotonicTop1/Bucketed\n\
             Negate\n\
             Threshold"
        );
    }
}
