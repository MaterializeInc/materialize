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
use std::num::NonZeroU64;

use columnar::Columnar;
use mz_expr::{
    CollectionPlan, EvalError, Id, LetRecLimit, LocalId, MapFilterProject, MirScalarExpr,
    OptimizedMirRelationExpr, TableFunc,
};
use mz_ore::soft_assert_eq_no_log;
use mz_ore::str::Indent;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::explain::text::text_string_at;
use mz_repr::explain::{DummyHumanizer, ExplainConfig, ExprHumanizer, PlanRenderingContext};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{ColumnType, Diff, GlobalId, Row};
use proptest::arbitrary::Arbitrary;
use proptest::prelude::*;
use proptest::strategy::Strategy;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::dataflows::DataflowDescription;
use crate::plan::join::JoinPlan;
use crate::plan::proto_available_collections::ProtoColumnTypes;
use crate::plan::reduce::{KeyValPlan, ReducePlan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::transform::{Transform, TransformConfig};

mod lowering;

pub mod interpret;
pub mod join;
pub mod reduce;
pub mod render_plan;
pub mod threshold;
pub mod top_k;
pub mod transform;

include!(concat!(env!("OUT_DIR"), "/mz_compute_types.plan.rs"));

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
    Arbitrary, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct AvailableCollections {
    /// Whether the collection exists in unarranged form.
    pub raw: bool,
    /// The list of available arrangements, presented as a `KeyValRowMapping`,
    /// but here represented by a triple `(to_key, to_val, to_row)` instead.
    /// The documentation for `KeyValRowMapping` explains these fields better.
    #[proptest(strategy = "prop::collection::vec(any_arranged_thin(), 0..3)")]
    pub arranged: Vec<(Vec<MirScalarExpr>, Vec<usize>, Vec<usize>)>,
    /// The types of the columns in the raw form of the collection, if known. We
    /// only capture types when necessary to support arrangement specialization,
    /// so this only done for specific LIR operators during lowering.
    pub types: Option<Vec<ColumnType>>,
}

/// A strategy that produces arrangements that are thinner than the default. That is
/// the number of direct children is limited to a maximum of 3.
pub(crate) fn any_arranged_thin()
-> impl Strategy<Value = (Vec<MirScalarExpr>, Vec<usize>, Vec<usize>)> {
    (
        prop::collection::vec(MirScalarExpr::arbitrary(), 0..3),
        Vec::<usize>::arbitrary(),
        Vec::<usize>::arbitrary(),
    )
}

impl RustType<ProtoColumnTypes> for Vec<ColumnType> {
    fn into_proto(&self) -> ProtoColumnTypes {
        ProtoColumnTypes {
            types: self.into_proto(),
        }
    }

    fn from_proto(proto: ProtoColumnTypes) -> Result<Self, TryFromProtoError> {
        proto.types.into_rust()
    }
}

impl RustType<ProtoAvailableCollections> for AvailableCollections {
    fn into_proto(&self) -> ProtoAvailableCollections {
        ProtoAvailableCollections {
            raw: self.raw,
            arranged: self.arranged.into_proto(),
            types: self.types.into_proto(),
        }
    }

    fn from_proto(x: ProtoAvailableCollections) -> Result<Self, TryFromProtoError> {
        Ok({
            Self {
                raw: x.raw,
                arranged: x.arranged.into_rust()?,
                types: x.types.into_rust()?,
            }
        })
    }
}

impl AvailableCollections {
    /// Represent a collection that has no arrangements.
    pub fn new_raw() -> Self {
        Self {
            raw: true,
            arranged: Vec::new(),
            types: None,
        }
    }

    /// Represent a collection that is arranged in the
    /// specified ways, with optionally given types describing
    /// the rows that would be in the raw form of the collection.
    pub fn new_arranged(
        arranged: Vec<(Vec<MirScalarExpr>, Vec<usize>, Vec<usize>)>,
        types: Option<Vec<ColumnType>>,
    ) -> Self {
        assert!(
            !arranged.is_empty(),
            "Invariant violated: at least one collection must exist"
        );
        Self {
            raw: false,
            arranged,
            types,
        }
    }

    /// Get some arrangement, if one exists.
    pub fn arbitrary_arrangement(&self) -> Option<&(Vec<MirScalarExpr>, Vec<usize>, Vec<usize>)> {
        assert!(
            self.raw || !self.arranged.is_empty(),
            "Invariant violated: at least one collection must exist"
        );
        self.arranged.get(0)
    }
}

/// An identifier for an LIR node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, Columnar)]
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

impl RustType<u64> for LirId {
    fn into_proto(&self) -> u64 {
        self.0
    }

    fn from_proto(proto: u64) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(Self(proto))
    }
}

/// A rendering plan with as much conditional logic as possible removed.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Plan<T = mz_repr::Timestamp> {
    /// A dataflow-local identifier.
    pub lir_id: LirId,
    /// The underlying operator.
    pub node: PlanNode<T>,
}

/// The actual AST node of the `Plan`.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum PlanNode<T = mz_repr::Timestamp> {
    /// A collection containing a pre-determined collection.
    Constant {
        /// Explicit update triples for the collection.
        rows: Result<Vec<(Row, T, Diff)>, EvalError>,
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
        value: Box<Plan<T>>,
        /// The collection that results, which is allowed to contain `Get` stages
        /// that reference `Id::Local(id)`.
        body: Box<Plan<T>>,
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
        values: Vec<Plan<T>>,
        /// Maximum number of iterations. See further info on the MIR `LetRec`.
        limits: Vec<Option<LetRecLimit>>,
        /// The collection that results, which is allowed to contain `Get` stages
        /// that reference `Id::Local(id)`.
        body: Box<Plan<T>>,
    },
    /// Map, Filter, and Project operators.
    ///
    /// This stage contains work that we would ideally like to fuse to other plan
    /// stages, but for practical reasons cannot. For example: threshold, topk,
    /// and sometimes reduce stages are not able to absorb this operator.
    Mfp {
        /// The input collection.
        input: Box<Plan<T>>,
        /// Linear operator to apply to each record.
        mfp: MapFilterProject,
        /// Whether the input is from an arrangement, and if so,
        /// whether we can seek to a specific value therein
        input_key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
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
        /// The input collection.
        input: Box<Plan<T>>,
        /// The variable-record emitting function.
        func: TableFunc,
        /// Expressions that for each row prepare the arguments to `func`.
        exprs: Vec<MirScalarExpr>,
        /// Linear operator to apply to each record produced by `func`.
        mfp_after: MapFilterProject,
        /// The particular arrangement of the input we expect to use,
        /// if any
        input_key: Option<Vec<MirScalarExpr>>,
    },
    /// A multiway relational equijoin, with fused map, filter, and projection.
    ///
    /// This stage performs a multiway join among `inputs`, using the equality
    /// constraints expressed in `plan`. The plan also describes the implementation
    /// strategy we will use, and any pushed down per-record work.
    Join {
        /// An ordered list of inputs that will be joined.
        inputs: Vec<Plan<T>>,
        /// Detailed information about the implementation of the join.
        ///
        /// This includes information about the implementation strategy, but also
        /// any map, filter, project work that we might follow the join with, but
        /// potentially pushed down into the implementation of the join.
        plan: JoinPlan,
    },
    /// Aggregation by key.
    Reduce {
        /// The input collection.
        input: Box<Plan<T>>,
        /// A plan for changing input records into key, value pairs.
        key_val_plan: KeyValPlan,
        /// A plan for performing the reduce.
        ///
        /// The implementation of reduction has several different strategies based
        /// on the properties of the reduction, and the input itself. Please check
        /// out the documentation for this type for more detail.
        plan: ReducePlan,
        /// The particular arrangement of the input we expect to use,
        /// if any
        input_key: Option<Vec<MirScalarExpr>>,
        /// An MFP that must be applied to results. The projection part of this
        /// MFP must preserve the key for the reduction; otherwise, the results
        /// become undefined. Additionally, the MFP must be free from temporal
        /// predicates so that it can be readily evaluated.
        mfp_after: MapFilterProject,
    },
    /// Key-based "Top K" operator, retaining the first K records in each group.
    TopK {
        /// The input collection.
        input: Box<Plan<T>>,
        /// A plan for performing the Top-K.
        ///
        /// The implementation of reduction has several different strategies based
        /// on the properties of the reduction, and the input itself. Please check
        /// out the documentation for this type for more detail.
        top_k_plan: TopKPlan,
    },
    /// Inverts the sign of each update.
    Negate {
        /// The input collection.
        input: Box<Plan<T>>,
    },
    /// Filters records that accumulate negatively.
    ///
    /// Although the operator suppresses updates, it is a stateful operator taking
    /// resources proportional to the number of records with non-zero accumulation.
    Threshold {
        /// The input collection.
        input: Box<Plan<T>>,
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
        inputs: Vec<Plan<T>>,
        /// Whether to consolidate the output, e.g., cancel negated records.
        consolidate_output: bool,
    },
    /// The `input` plan, but with additional arrangements.
    ///
    /// This operator does not change the logical contents of `input`, but ensures
    /// that certain arrangements are available in the results. This operator can
    /// be important for e.g. the `Join` stage which benefits from multiple arrangements
    /// or to cap a `Plan` so that indexes can be exported.
    ArrangeBy {
        /// The input collection.
        input: Box<Plan<T>>,
        /// A list of arrangement keys, and possibly a raw collection,
        /// that will be added to those of the input. Does not include
        /// any other existing arrangements.
        forms: AvailableCollections,
        /// The key that must be used to access the input.
        input_key: Option<Vec<MirScalarExpr>>,
        /// The MFP that must be applied to the input.
        input_mfp: MapFilterProject,
    },
}

impl<T> PlanNode<T> {
    /// Iterates through references to child expressions.
    pub fn children(&self) -> impl Iterator<Item = &Plan<T>> {
        let mut first = None;
        let mut second = None;
        let mut rest = None;
        let mut last = None;

        use PlanNode::*;
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
    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut Plan<T>> {
        let mut first = None;
        let mut second = None;
        let mut rest = None;
        let mut last = None;

        use PlanNode::*;
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

impl<T> PlanNode<T> {
    /// Attach an `lir_id` to a `PlanNode` to make a complete `Plan`.
    pub fn as_plan(self, lir_id: LirId) -> Plan<T> {
        Plan { lir_id, node: self }
    }
}

impl Plan {
    /// Pretty-print this [Plan] to a string.
    pub fn pretty(&self) -> String {
        let config = ExplainConfig::default();
        self.explain(&config, None)
    }

    /// Pretty-print this [Plan] to a string using a custom
    /// [ExplainConfig] and an optionally provided [ExprHumanizer].
    pub fn explain(&self, config: &ExplainConfig, humanizer: Option<&dyn ExprHumanizer>) -> String {
        text_string_at(self, || PlanRenderingContext {
            indent: Indent::default(),
            humanizer: humanizer.unwrap_or(&DummyHumanizer),
            annotations: BTreeMap::default(),
            config,
        })
    }
}

impl Arbitrary for LirId {
    type Strategy = BoxedStrategy<LirId>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let lir_id = u64::arbitrary();
        lir_id.prop_map(LirId).boxed()
    }
}

impl Arbitrary for Plan {
    type Strategy = BoxedStrategy<Plan>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let row_diff = prop::collection::vec(
            (
                Row::arbitrary_with((1..5).into()),
                mz_repr::Timestamp::arbitrary(),
                Diff::arbitrary(),
            ),
            0..2,
        );
        let rows = prop::result::maybe_ok(row_diff, EvalError::arbitrary());
        let constant = (rows, any::<LirId>()).prop_map(|(rows, lir_id)| {
            PlanNode::<mz_repr::Timestamp>::Constant { rows }.as_plan(lir_id)
        });

        let get = (
            any::<GlobalId>(),
            any::<AvailableCollections>(),
            any::<GetPlan>(),
            any::<LirId>(),
        )
            .prop_map(|(id, keys, plan, lir_id)| {
                PlanNode::<mz_repr::Timestamp>::Get {
                    id: Id::Global(id),
                    keys,
                    plan,
                }
                .as_plan(lir_id)
            });

        let leaf = prop::strategy::Union::new(vec![constant.boxed(), get.boxed()]).boxed();

        leaf.prop_recursive(2, 4, 5, |inner| {
            prop::strategy::Union::new(vec![
                //Plan::Let
                (
                    any::<LocalId>(),
                    inner.clone(),
                    inner.clone(),
                    any::<LirId>(),
                )
                    .prop_map(|(id, value, body, lir_id)| {
                        PlanNode::<mz_repr::Timestamp>::Let {
                            id,
                            value: value.into(),
                            body: body.into(),
                        }
                        .as_plan(lir_id)
                    })
                    .boxed(),
                //Plan::Mfp
                (
                    inner.clone(),
                    any::<MapFilterProject>(),
                    any::<Option<(Vec<MirScalarExpr>, Option<Row>)>>(),
                    any::<LirId>(),
                )
                    .prop_map(|(input, mfp, input_key_val, lir_id)| {
                        PlanNode::Mfp {
                            input: input.into(),
                            mfp,
                            input_key_val,
                        }
                        .as_plan(lir_id)
                    })
                    .boxed(),
                //Plan::FlatMap
                (
                    inner.clone(),
                    any::<TableFunc>(),
                    any::<Vec<MirScalarExpr>>(),
                    any::<MapFilterProject>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                    any::<LirId>(),
                )
                    .prop_map(|(input, func, exprs, mfp, input_key, lir_id)| {
                        PlanNode::FlatMap {
                            input: input.into(),
                            func,
                            exprs,
                            mfp_after: mfp,
                            input_key,
                        }
                        .as_plan(lir_id)
                    })
                    .boxed(),
                //Plan::Join
                (
                    prop::collection::vec(inner.clone(), 0..2),
                    any::<JoinPlan>(),
                    any::<LirId>(),
                )
                    .prop_map(|(inputs, plan, lir_id)| {
                        PlanNode::Join { inputs, plan }.as_plan(lir_id)
                    })
                    .boxed(),
                //Plan::Reduce
                (
                    inner.clone(),
                    any::<KeyValPlan>(),
                    any::<ReducePlan>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                    any::<MapFilterProject>(),
                    any::<LirId>(),
                )
                    .prop_map(
                        |(input, key_val_plan, plan, input_key, mfp_after, lir_id)| {
                            PlanNode::Reduce {
                                input: input.into(),
                                key_val_plan,
                                plan,
                                input_key,
                                mfp_after,
                            }
                            .as_plan(lir_id)
                        },
                    )
                    .boxed(),
                //Plan::TopK
                (inner.clone(), any::<TopKPlan>(), any::<LirId>())
                    .prop_map(|(input, top_k_plan, lir_id)| {
                        PlanNode::TopK {
                            input: input.into(),
                            top_k_plan,
                        }
                        .as_plan(lir_id)
                    })
                    .boxed(),
                //Plan::Negate
                (inner.clone(), any::<LirId>())
                    .prop_map(|(x, lir_id)| PlanNode::Negate { input: x.into() }.as_plan(lir_id))
                    .boxed(),
                //Plan::Threshold
                (inner.clone(), any::<ThresholdPlan>(), any::<LirId>())
                    .prop_map(|(input, threshold_plan, lir_id)| {
                        PlanNode::Threshold {
                            input: input.into(),
                            threshold_plan,
                        }
                        .as_plan(lir_id)
                    })
                    .boxed(),
                // Plan::Union
                (
                    prop::collection::vec(inner.clone(), 0..2),
                    any::<bool>(),
                    any::<LirId>(),
                )
                    .prop_map(|(x, b, lir_id)| {
                        PlanNode::Union {
                            inputs: x,
                            consolidate_output: b,
                        }
                        .as_plan(lir_id)
                    })
                    .boxed(),
                //Plan::ArrangeBy
                (
                    inner,
                    any::<AvailableCollections>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                    any::<MapFilterProject>(),
                    any::<LirId>(),
                )
                    .prop_map(|(input, forms, input_key, input_mfp, lir_id)| {
                        PlanNode::ArrangeBy {
                            input: input.into(),
                            forms,
                            input_key,
                            input_mfp,
                        }
                        .as_plan(lir_id)
                    })
                    .boxed(),
            ])
        })
        .boxed()
    }
}

/// How a `Get` stage will be rendered.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub enum GetPlan {
    /// Simply pass input arrangements on to the next stage.
    PassArrangements,
    /// Using the supplied key, optionally seek the row, and apply the MFP.
    Arrangement(
        #[proptest(strategy = "prop::collection::vec(MirScalarExpr::arbitrary(), 0..3)")]
        Vec<MirScalarExpr>,
        Option<Row>,
        MapFilterProject,
    ),
    /// Scan the input collection (unarranged) and apply the MFP.
    Collection(MapFilterProject),
}

impl RustType<ProtoGetPlan> for GetPlan {
    fn into_proto(&self) -> ProtoGetPlan {
        use proto_get_plan::Kind::*;

        ProtoGetPlan {
            kind: Some(match self {
                GetPlan::PassArrangements => PassArrangements(()),
                GetPlan::Arrangement(k, s, m) => {
                    Arrangement(proto_get_plan::ProtoGetPlanArrangement {
                        key: k.into_proto(),
                        seek: s.into_proto(),
                        mfp: Some(m.into_proto()),
                    })
                }
                GetPlan::Collection(mfp) => Collection(mfp.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoGetPlan) -> Result<Self, TryFromProtoError> {
        use proto_get_plan::Kind::*;
        use proto_get_plan::ProtoGetPlanArrangement;
        match proto.kind {
            Some(PassArrangements(())) => Ok(GetPlan::PassArrangements),
            Some(Arrangement(ProtoGetPlanArrangement { key, seek, mfp })) => {
                Ok(GetPlan::Arrangement(
                    key.into_rust()?,
                    seek.into_rust()?,
                    mfp.into_rust_if_some("ProtoGetPlanArrangement::mfp")?,
                ))
            }
            Some(Collection(mfp)) => Ok(GetPlan::Collection(mfp.into_rust()?)),
            None => Err(TryFromProtoError::missing_field("ProtoGetPlan::kind")),
        }
    }
}

impl RustType<ProtoLetRecLimit> for LetRecLimit {
    fn into_proto(&self) -> ProtoLetRecLimit {
        ProtoLetRecLimit {
            max_iters: self.max_iters.get(),
            return_at_limit: self.return_at_limit,
        }
    }

    fn from_proto(proto: ProtoLetRecLimit) -> Result<Self, TryFromProtoError> {
        Ok(LetRecLimit {
            max_iters: NonZeroU64::new(proto.max_iters).expect("max_iters > 0"),
            return_at_limit: proto.return_at_limit,
        })
    }
}

impl<T: timely::progress::Timestamp> Plan<T> {
    /// Convert the dataflow description into one that uses render plans.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "finalize_dataflow")
    )]
    pub fn finalize_dataflow(
        desc: DataflowDescription<OptimizedMirRelationExpr>,
        features: &OptimizerFeatures,
    ) -> Result<DataflowDescription<Self>, String> {
        // First, we lower the dataflow description from MIR to LIR.
        let mut dataflow = Self::lower_dataflow(desc, features)?;

        // Subsequently, we perform plan refinements for the dataflow.
        Self::refine_source_mfps(&mut dataflow);

        if features.enable_consolidate_after_union_negate {
            Self::refine_union_negate_consolidation(&mut dataflow);
        }

        if dataflow.is_single_time() {
            Self::refine_single_time_operator_selection(&mut dataflow);

            // The relaxation of the `must_consolidate` flag performs an LIR-based
            // analysis and transform under checked recursion. By a similar argument
            // made in `from_mir`, we do not expect the recursion limit to be hit.
            // However, if that happens, we propagate an error to the caller.
            // To apply the transform, we first obtain monotonic source and index
            // global IDs and add them to a `TransformConfig` instance.
            let monotonic_ids = dataflow
                .source_imports
                .iter()
                .filter_map(|(id, (_, monotonic, _upper))| if *monotonic { Some(id) } else { None })
                .chain(
                    dataflow
                        .index_imports
                        .iter()
                        .filter_map(|(id, index_import)| {
                            if index_import.monotonic {
                                Some(id)
                            } else {
                                None
                            }
                        }),
                )
                .cloned()
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
    ) -> Result<DataflowDescription<Self>, String> {
        let context = lowering::Context::new(desc.debug_name.clone(), features);
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
        // Extract MFPs from Get operators for sources, and extract what we can for the source.
        // For each source, we want to find `&mut MapFilterProject` for each `Get` expression.
        for (source_id, (source, _monotonic, _upper)) in dataflow.source_imports.iter_mut() {
            let mut identity_present = false;
            let mut mfps = Vec::new();
            for build_desc in dataflow.objects_to_build.iter_mut() {
                let mut todo = vec![&mut build_desc.plan];
                while let Some(expression) = todo.pop() {
                    let node = &mut expression.node;
                    if let PlanNode::Get { id, plan, .. } = node {
                        if *id == mz_expr::Id::Global(*source_id) {
                            match plan {
                                GetPlan::Collection(mfp) => mfps.push(mfp),
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
                .any(|(x, _)| x.on_id == *source_id);
            identity_present |= dataflow.sink_exports.values().any(|x| x.from == *source_id);

            if !identity_present && !mfps.is_empty() {
                // Extract a common prefix `MapFilterProject` from `mfps`.
                let common = MapFilterProject::extract_common(&mut mfps[..]);
                // Apply common expressions to the source's `MapFilterProject`.
                let mut mfp = if let Some(mfp) = source.arguments.operators.take() {
                    MapFilterProject::compose(mfp, common)
                } else {
                    common
                };
                mfp.optimize();
                source.arguments.operators = Some(mfp);
            }
        }
        mz_repr::explain::trace_plan(dataflow);
    }

    /// Changes the `consolidate_output` flag of such Unions that have at least one Negated input.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "refine_union_negate_consolidation")
    )]
    fn refine_union_negate_consolidation(dataflow: &mut DataflowDescription<Self>) {
        for build_desc in dataflow.objects_to_build.iter_mut() {
            let mut todo = vec![&mut build_desc.plan];
            while let Some(expression) = todo.pop() {
                let node = &mut expression.node;
                match node {
                    PlanNode::Union {
                        inputs,
                        consolidate_output,
                        ..
                    } => {
                        if inputs
                            .iter()
                            .any(|input| matches!(input.node, PlanNode::Negate { .. }))
                        {
                            *consolidate_output = true;
                        }
                    }
                    _ => {}
                }
                todo.extend(node.children_mut());
            }
        }
        mz_repr::explain::trace_plan(dataflow);
    }

    /// Refines the plans of objects to be built as part of `dataflow` to take advantage
    /// of monotonic operators if the dataflow refers to a single-time, i.e., is for a
    /// one-shot SELECT query.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "refine_single_time_operator_selection")
    )]
    fn refine_single_time_operator_selection(dataflow: &mut DataflowDescription<Self>) {
        // We should only reach here if we have a one-shot SELECT query, i.e.,
        // a single-time dataflow.
        assert!(dataflow.is_single_time());

        // Upgrade single-time plans to monotonic.
        for build_desc in dataflow.objects_to_build.iter_mut() {
            let mut todo = vec![&mut build_desc.plan];
            while let Some(expression) = todo.pop() {
                let node = &mut expression.node;
                match node {
                    PlanNode::Reduce { plan, .. } => {
                        // Upgrade non-monotonic hierarchical plans to monotonic with mandatory consolidation.
                        match plan {
                            ReducePlan::Collation(collation) => {
                                collation.as_monotonic(true);
                            }
                            ReducePlan::Hierarchical(hierarchical) => {
                                hierarchical.as_monotonic(true);
                            }
                            _ => {
                                // Nothing to do for other plans, and doing nothing is safe for future variants.
                            }
                        }
                        todo.extend(node.children_mut());
                    }
                    PlanNode::TopK { top_k_plan, .. } => {
                        top_k_plan.as_monotonic(true);
                        todo.extend(node.children_mut());
                    }
                    PlanNode::LetRec { body, .. } => {
                        // Only the non-recursive `body` is restricted to a single time.
                        todo.push(body);
                    }
                    _ => {
                        // Nothing to do for other expressions, and doing nothing is safe for future expressions.
                        todo.extend(node.children_mut());
                    }
                }
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

        let transform = transform::RelaxMustConsolidate::<T>::new();
        for build_desc in dataflow.objects_to_build.iter_mut() {
            transform
                .transform(config, &mut build_desc.plan)
                .map_err(|_| "Maximum recursion limit error in consolidation relaxation.")?;
        }
        mz_repr::explain::trace_plan(dataflow);
        Ok(())
    }
}

impl<T> CollectionPlan for PlanNode<T> {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        match self {
            PlanNode::Constant { rows: _ } => (),
            PlanNode::Get {
                id,
                keys: _,
                plan: _,
            } => match id {
                Id::Global(id) => {
                    out.insert(*id);
                }
                Id::Local(_) => (),
            },
            PlanNode::Let { id: _, value, body } => {
                value.depends_on_into(out);
                body.depends_on_into(out);
            }
            PlanNode::LetRec {
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
            PlanNode::Join { inputs, plan: _ }
            | PlanNode::Union {
                inputs,
                consolidate_output: _,
            } => {
                for input in inputs {
                    input.depends_on_into(out);
                }
            }
            PlanNode::Mfp {
                input,
                mfp: _,
                input_key_val: _,
            }
            | PlanNode::FlatMap {
                input,
                func: _,
                exprs: _,
                mfp_after: _,
                input_key: _,
            }
            | PlanNode::ArrangeBy {
                input,
                forms: _,
                input_key: _,
                input_mfp: _,
            }
            | PlanNode::Reduce {
                input,
                key_val_plan: _,
                plan: _,
                input_key: _,
                mfp_after: _,
            }
            | PlanNode::TopK {
                input,
                top_k_plan: _,
            }
            | PlanNode::Negate { input }
            | PlanNode::Threshold {
                input,
                threshold_plan: _,
            } => {
                input.depends_on_into(out);
            }
        }
    }
}

impl<T> CollectionPlan for Plan<T> {
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

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
        fn available_collections_protobuf_roundtrip(expect in any::<AvailableCollections>() ) {
            let actual = protobuf_roundtrip::<_, ProtoAvailableCollections>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
        fn get_plan_protobuf_roundtrip(expect in any::<GetPlan>()) {
            let actual = protobuf_roundtrip::<_, ProtoGetPlan>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
