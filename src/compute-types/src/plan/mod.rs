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

use itertools::Itertools;
use mz_expr::{
    CollectionPlan, EvalError, Id, LetRecLimit, LocalId, MapFilterProject, MirScalarExpr,
    OptimizedMirRelationExpr, TableFunc,
};
use mz_ore::soft_assert_eq_no_log;
use mz_ore::str::Indent;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::explain::text::text_string_at;
use mz_repr::explain::{DummyHumanizer, ExplainConfig, ExprHumanizer, PlanRenderingContext};
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
pub mod threshold;
pub mod top_k;
pub mod transform;

include!(concat!(env!("OUT_DIR"), "/mz_compute_types.plan.rs"));

/// The forms in which an operator's output is available;
/// it can be considered the plan-time equivalent of
/// `render::context::CollectionBundle`.
///
/// These forms are either "raw", representing an unarranged collection,
/// or "arranged", representing one that has been arranged by some key.
///
/// The raw collection, if it exists, may be consumed directly.
///
/// The arranged collections are slightly more complicated:
/// Each key here is attached to a description of how the corresponding
/// arrangement is permuted to remove value columns
/// that are redundant with key columns. Thus, the first element in each
/// tuple of `arranged` is the arrangement key; the second is the map of
/// logical output columns to columns in the key or value of the deduplicated
/// representation, and the third is a "thinning expression",
/// or list of columns to include in the value
/// when arranging.
///
/// For example, assume a 5-column collection is to be arranged by the key
/// `[Column(2), Column(0) + Column(3), Column(1)]`.
/// Then `Column(1)` and `Column(2)` in the value are redundant with the key, and
/// only columns 0, 3, and 4 need to be stored separately.
/// The thinning expression will then be `[0, 3, 4]`.
///
/// The permutation represents how to recover the
/// original values (logically `[Column(0), Column(1), Column(2), Column(3), Column(4)]`)
/// from the key and value of the arrangement, logically
/// `[Column(2), Column(0) + Column(3), Column(1), Column(0), Column(3), Column(4)]`.
/// Thus, the permutation in this case should be `{0: 3, 1: 2, 2: 0, 3: 4, 4: 5}`.
///
/// Note that this description, while true at the time of writing, is merely illustrative;
/// users of this struct should not rely on the exact strategy used for generating
/// the permutations. As long as clients apply the thinning expression
/// when creating arrangements, and permute by the hashmap when reading them,
/// the contract of the function where they are generated (`mz_expr::permutation_for_arrangement`)
/// ensures that the correct values will be read.
#[derive(
    Arbitrary, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct AvailableCollections {
    /// Whether the collection exists in unarranged form.
    pub raw: bool,
    /// The set of arrangements of the collection, along with a
    /// column permutation mapping
    #[proptest(strategy = "prop::collection::vec(any_arranged_thin(), 0..3)")]
    pub arranged: Vec<(Vec<MirScalarExpr>, BTreeMap<usize, usize>, Vec<usize>)>,
    /// The types of the columns in the raw form of the collection, if known. We
    /// only capture types when necessary to support arrangement specialization,
    /// so this only done for specific LIR operators during lowering.
    pub types: Option<Vec<ColumnType>>,
}

/// A strategy that produces arrangements that are thinner than the default. That is
/// the number of direct children is limited to a maximum of 3.
pub(crate) fn any_arranged_thin(
) -> impl Strategy<Value = (Vec<MirScalarExpr>, BTreeMap<usize, usize>, Vec<usize>)> {
    (
        prop::collection::vec(MirScalarExpr::arbitrary(), 0..3),
        BTreeMap::<usize, usize>::arbitrary(),
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
        arranged: Vec<(Vec<MirScalarExpr>, BTreeMap<usize, usize>, Vec<usize>)>,
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
    pub fn arbitrary_arrangement(
        &self,
    ) -> Option<&(Vec<MirScalarExpr>, BTreeMap<usize, usize>, Vec<usize>)> {
        assert!(
            self.raw || !self.arranged.is_empty(),
            "Invariant violated: at least one collection must exist"
        );
        self.arranged.get(0)
    }
}

/// An identifier for a `Plan` node.
pub type NodeId = u64;

/// A rendering plan with as much conditional logic as possible removed.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Plan<T = mz_repr::Timestamp> {
    /// A collection containing a pre-determined collection.
    Constant {
        /// Explicit update triples for the collection.
        rows: Result<Vec<(Row, T, Diff)>, EvalError>,
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
    },
    /// Inverts the sign of each update.
    Negate {
        /// The input collection.
        input: Box<Plan<T>>,
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// A dataflow-local identifier.
        node_id: NodeId,
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
        /// that will be added to those of the input.
        ///
        /// If any of these collection forms are already present in the input, they have no effect.
        forms: AvailableCollections,
        /// The key that must be used to access the input.
        input_key: Option<Vec<MirScalarExpr>>,
        /// The MFP that must be applied to the input.
        input_mfp: MapFilterProject,
        /// A dataflow-local identifier.
        node_id: NodeId,
    },
}

impl<T> Plan<T> {
    /// Iterates through references to child expressions.
    pub fn children(&self) -> impl Iterator<Item = &Self> {
        let mut first = None;
        let mut second = None;
        let mut rest = None;
        let mut last = None;

        use Plan::*;
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
    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut Self> {
        let mut first = None;
        let mut second = None;
        let mut rest = None;
        let mut last = None;

        use Plan::*;
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

impl<T> Plan<T> {
    /// Return this plan's `NodeId`.
    pub fn node_id(&self) -> NodeId {
        use Plan::*;
        match self {
            Constant { node_id, .. }
            | Get { node_id, .. }
            | Let { node_id, .. }
            | LetRec { node_id, .. }
            | Mfp { node_id, .. }
            | FlatMap { node_id, .. }
            | Join { node_id, .. }
            | Reduce { node_id, .. }
            | TopK { node_id, .. }
            | Negate { node_id, .. }
            | Threshold { node_id, .. }
            | Union { node_id, .. }
            | ArrangeBy { node_id, .. } => *node_id,
        }
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
        let constant =
            (rows, any::<NodeId>()).prop_map(|(rows, node_id)| Plan::Constant { rows, node_id });

        let get = (
            any::<Id>(),
            any::<AvailableCollections>(),
            any::<GetPlan>(),
            any::<NodeId>(),
        )
            .prop_map(
                |(id, keys, plan, node_id)| Plan::<mz_repr::Timestamp>::Get {
                    id,
                    keys,
                    plan,
                    node_id,
                },
            );

        let leaf = prop::strategy::Union::new(vec![constant.boxed(), get.boxed()]).boxed();

        leaf.prop_recursive(2, 4, 5, |inner| {
            prop::strategy::Union::new(vec![
                //Plan::Let
                (
                    any::<LocalId>(),
                    inner.clone(),
                    inner.clone(),
                    any::<NodeId>(),
                )
                    .prop_map(|(id, value, body, node_id)| Plan::Let {
                        id,
                        value: value.into(),
                        body: body.into(),
                        node_id,
                    })
                    .boxed(),
                //Plan::Mfp
                (
                    inner.clone(),
                    any::<MapFilterProject>(),
                    any::<Option<(Vec<MirScalarExpr>, Option<Row>)>>(),
                    any::<NodeId>(),
                )
                    .prop_map(|(input, mfp, input_key_val, node_id)| Plan::Mfp {
                        input: input.into(),
                        mfp,
                        input_key_val,
                        node_id,
                    })
                    .boxed(),
                //Plan::FlatMap
                (
                    inner.clone(),
                    any::<TableFunc>(),
                    any::<Vec<MirScalarExpr>>(),
                    any::<MapFilterProject>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                    any::<NodeId>(),
                )
                    .prop_map(
                        |(input, func, exprs, mfp, input_key, node_id)| Plan::FlatMap {
                            input: input.into(),
                            func,
                            exprs,
                            mfp_after: mfp,
                            input_key,
                            node_id,
                        },
                    )
                    .boxed(),
                //Plan::Join
                (
                    prop::collection::vec(inner.clone(), 0..2),
                    any::<JoinPlan>(),
                    any::<NodeId>(),
                )
                    .prop_map(|(inputs, plan, node_id)| Plan::Join {
                        inputs,
                        plan,
                        node_id,
                    })
                    .boxed(),
                //Plan::Reduce
                (
                    inner.clone(),
                    any::<KeyValPlan>(),
                    any::<ReducePlan>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                    any::<MapFilterProject>(),
                    any::<NodeId>(),
                )
                    .prop_map(
                        |(input, key_val_plan, plan, input_key, mfp_after, node_id)| Plan::Reduce {
                            input: input.into(),
                            key_val_plan,
                            plan,
                            input_key,
                            mfp_after,
                            node_id,
                        },
                    )
                    .boxed(),
                //Plan::TopK
                (inner.clone(), any::<TopKPlan>(), any::<NodeId>())
                    .prop_map(|(input, top_k_plan, node_id)| Plan::TopK {
                        input: input.into(),
                        top_k_plan,
                        node_id,
                    })
                    .boxed(),
                //Plan::Negate
                (inner.clone(), any::<NodeId>())
                    .prop_map(|(x, node_id)| Plan::Negate {
                        input: x.into(),
                        node_id,
                    })
                    .boxed(),
                //Plan::Threshold
                (inner.clone(), any::<ThresholdPlan>(), any::<NodeId>())
                    .prop_map(|(input, threshold_plan, node_id)| Plan::Threshold {
                        input: input.into(),
                        threshold_plan,
                        node_id,
                    })
                    .boxed(),
                // Plan::Union
                (
                    prop::collection::vec(inner.clone(), 0..2),
                    any::<bool>(),
                    any::<NodeId>(),
                )
                    .prop_map(|(x, b, node_id)| Plan::Union {
                        inputs: x,
                        consolidate_output: b,
                        node_id,
                    })
                    .boxed(),
                //Plan::ArrangeBy
                (
                    inner,
                    any::<AvailableCollections>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                    any::<MapFilterProject>(),
                    any::<NodeId>(),
                )
                    .prop_map(
                        |(input, forms, input_key, input_mfp, node_id)| Plan::ArrangeBy {
                            input: input.into(),
                            forms,
                            input_key,
                            input_mfp,
                            node_id,
                        },
                    )
                    .boxed(),
            ])
        })
        .boxed()
    }
}

impl RustType<proto_plan::ProtoPlanRows>
    for Result<Vec<(Row, mz_repr::Timestamp, i64)>, EvalError>
{
    fn into_proto(&self) -> proto_plan::ProtoPlanRows {
        use proto_plan::proto_plan_rows::Result;
        proto_plan::ProtoPlanRows {
            result: Some(match self {
                Ok(ok) => Result::Ok(ok.into_proto()),
                Err(err) => Result::Err(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: proto_plan::ProtoPlanRows) -> Result<Self, TryFromProtoError> {
        use proto_plan::proto_plan_rows::Result;
        match proto.result {
            Some(Result::Ok(ok)) => Ok(Ok(ok.into_rust()?)),
            Some(Result::Err(err)) => Ok(Err(err.into_rust()?)),
            None => Err(TryFromProtoError::missing_field("ProtoPlanRows::result")),
        }
    }
}

impl RustType<ProtoPlan> for Plan {
    fn into_proto(&self) -> ProtoPlan {
        use proto_plan::Kind::*;
        use proto_plan::*;

        fn input_kv_into(
            x: &Option<(Vec<MirScalarExpr>, Option<Row>)>,
        ) -> Option<ProtoPlanInputKeyVal> {
            x.as_ref().map(|(key, val)| ProtoPlanInputKeyVal {
                key: key.into_proto(),
                val: val.into_proto(),
            })
        }

        fn input_k_into(
            input_key: Option<&Vec<MirScalarExpr>>,
        ) -> Option<proto_plan::ProtoPlanInputKey> {
            input_key.map(|vec| ProtoPlanInputKey {
                key: vec.into_proto(),
            })
        }

        ProtoPlan {
            kind: Some(match self {
                Plan::Constant { rows, node_id } => Constant(ProtoPlanConstant {
                    rows: Some(rows.into_proto()),
                    node_id: node_id.into_proto(),
                }),
                Plan::Get {
                    id,
                    keys,
                    plan,
                    node_id,
                } => Get(ProtoPlanGet {
                    id: Some(id.into_proto()),
                    keys: Some(keys.into_proto()),
                    plan: Some(plan.into_proto()),
                    node_id: node_id.into_proto(),
                }),
                Plan::Let {
                    id,
                    value,
                    body,
                    node_id,
                } => Let(ProtoPlanLet {
                    id: Some(id.into_proto()),
                    value: Some(value.into_proto()),
                    body: Some(body.into_proto()),
                    node_id: node_id.into_proto(),
                }
                .into()),
                Plan::LetRec {
                    ids,
                    values,
                    limits,
                    body,
                    node_id,
                } => LetRec(
                    ProtoPlanLetRec {
                        ids: ids.into_proto(),
                        limits: limits
                            .clone()
                            .into_iter()
                            .map(|limit| match limit {
                                Some(limit) => limit,
                                None => LetRecLimit {
                                    // The actual value doesn't matter here, because the limit_is_some
                                    // field will be false, so we won't read this value when converting
                                    // back.
                                    max_iters: NonZeroU64::new(1).unwrap(),
                                    return_at_limit: false,
                                },
                            })
                            .collect_vec()
                            .into_proto(),
                        limit_is_some: limits
                            .clone()
                            .into_iter()
                            .map(|limit| limit.is_some())
                            .collect_vec()
                            .into_proto(),
                        values: values.into_proto(),
                        body: Some(body.into_proto()),
                        node_id: node_id.into_proto(),
                    }
                    .into(),
                ),
                Plan::Mfp {
                    input,
                    mfp,
                    input_key_val,
                    node_id,
                } => Mfp(ProtoPlanMfp {
                    input: Some(input.into_proto()),
                    mfp: Some(mfp.into_proto()),
                    input_key_val: input_kv_into(input_key_val),
                    node_id: node_id.into_proto(),
                }
                .into()),
                Plan::FlatMap {
                    input,
                    func,
                    exprs,
                    mfp_after,
                    input_key,
                    node_id,
                } => FlatMap(
                    ProtoPlanFlatMap {
                        input: Some(input.into_proto()),
                        func: Some(func.into_proto()),
                        exprs: exprs.into_proto(),
                        mfp_after: Some(mfp_after.into_proto()),
                        input_key: input_k_into(input_key.as_ref()),
                        node_id: node_id.into_proto(),
                    }
                    .into(),
                ),
                Plan::Join {
                    inputs,
                    plan,
                    node_id,
                } => Join(ProtoPlanJoin {
                    inputs: inputs.into_proto(),
                    plan: Some(plan.into_proto()),
                    node_id: node_id.into_proto(),
                }),
                Plan::Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                    mfp_after,
                    node_id,
                } => Reduce(
                    ProtoPlanReduce {
                        input: Some(input.into_proto()),
                        key_val_plan: Some(key_val_plan.into_proto()),
                        plan: Some(plan.into_proto()),
                        input_key: input_k_into(input_key.as_ref()),
                        mfp_after: Some(mfp_after.into_proto()),
                        node_id: node_id.into_proto(),
                    }
                    .into(),
                ),
                Plan::TopK {
                    input,
                    top_k_plan,
                    node_id,
                } => TopK(
                    ProtoPlanTopK {
                        input: Some(input.into_proto()),
                        top_k_plan: Some(top_k_plan.into_proto()),
                        node_id: node_id.into_proto(),
                    }
                    .into(),
                ),
                Plan::Negate { input, node_id } => Negate(
                    ProtoPlanNegate {
                        input: Some(input.into_proto()),
                        node_id: node_id.into_proto(),
                    }
                    .into(),
                ),
                Plan::Threshold {
                    input,
                    threshold_plan,
                    node_id,
                } => Threshold(
                    ProtoPlanThreshold {
                        input: Some(input.into_proto()),
                        threshold_plan: Some(threshold_plan.into_proto()),
                        node_id: node_id.into_proto(),
                    }
                    .into(),
                ),
                Plan::Union {
                    inputs,
                    consolidate_output,
                    node_id,
                } => Union(ProtoPlanUnion {
                    inputs: inputs.into_proto(),
                    consolidate_output: consolidate_output.into_proto(),
                    node_id: node_id.into_proto(),
                }),
                Plan::ArrangeBy {
                    input,
                    forms,
                    input_key,
                    input_mfp,
                    node_id,
                } => ArrangeBy(
                    ProtoPlanArrangeBy {
                        input: Some(input.into_proto()),
                        forms: Some(forms.into_proto()),
                        input_key: input_k_into(input_key.as_ref()),
                        input_mfp: Some(input_mfp.into_proto()),
                        node_id: node_id.into_proto(),
                    }
                    .into(),
                ),
            }),
        }
    }

    fn from_proto(proto: ProtoPlan) -> Result<Self, TryFromProtoError> {
        use proto_plan::Kind::*;
        use proto_plan::*;

        fn input_k_try_into(
            input_key: Option<ProtoPlanInputKey>,
        ) -> Result<Option<Vec<MirScalarExpr>>, TryFromProtoError> {
            Ok(match input_key {
                Some(proto_plan::ProtoPlanInputKey { key }) => Some(key.into_rust()?),
                None => None,
            })
        }

        fn input_kv_try_into(
            input_key_val: Option<ProtoPlanInputKeyVal>,
        ) -> Result<Option<(Vec<MirScalarExpr>, Option<Row>)>, TryFromProtoError> {
            Ok(match input_key_val {
                Some(inner) => Some((inner.key.into_rust()?, inner.val.into_rust()?)),
                None => None,
            })
        }

        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoPlan::kind"))?;

        Ok(match kind {
            Constant(proto) => Plan::Constant {
                rows: proto.rows.into_rust_if_some("ProtoPlanConstant::rows")?,
                node_id: proto.node_id.into_rust()?,
            },
            Get(proto) => Plan::Get {
                id: proto.id.into_rust_if_some("ProtoPlanGet::id")?,
                keys: proto.keys.into_rust_if_some("ProtoPlanGet::keys")?,
                plan: proto.plan.into_rust_if_some("ProtoPlanGet::plan")?,
                node_id: proto.node_id.into_rust()?,
            },
            Let(proto) => Plan::Let {
                id: proto.id.into_rust_if_some("ProtoPlanLet::id")?,
                value: proto.value.into_rust_if_some("ProtoPlanLet::value")?,
                body: proto.body.into_rust_if_some("ProtoPlanLet::body")?,
                node_id: proto.node_id.into_rust()?,
            },
            LetRec(proto) => {
                let ids: Vec<LocalId> = proto.ids.into_rust()?;
                let values: Vec<Plan> = proto.values.into_rust()?;
                let limits_raw: Vec<LetRecLimit> = proto.limits.into_rust()?;
                let limit_is_some: Vec<bool> = proto.limit_is_some.into_rust()?;
                assert_eq!(ids.len(), values.len());
                assert_eq!(ids.len(), limits_raw.len());
                assert_eq!(ids.len(), limit_is_some.len());
                let limits = limits_raw
                    .into_iter()
                    .zip_eq(limit_is_some)
                    .map(
                        |(limit_raw, is_some)| {
                            if is_some {
                                Some(limit_raw)
                            } else {
                                None
                            }
                        },
                    )
                    .collect_vec();
                assert_eq!(ids.len(), limits.len());
                Plan::LetRec {
                    ids,
                    values,
                    limits,
                    body: proto.body.into_rust_if_some("ProtoPlanLetRec::body")?,
                    node_id: proto.node_id.into_rust()?,
                }
            }
            Mfp(proto) => Plan::Mfp {
                input: proto.input.into_rust_if_some("ProtoPlanMfp::input")?,
                input_key_val: input_kv_try_into(proto.input_key_val)?,
                mfp: proto.mfp.into_rust_if_some("ProtoPlanMfp::mfp")?,
                node_id: proto.node_id.into_rust()?,
            },
            FlatMap(proto) => Plan::FlatMap {
                input: proto.input.into_rust_if_some("ProtoPlanFlatMap::input")?,
                func: proto.func.into_rust_if_some("ProtoPlanFlatMap::func")?,
                exprs: proto.exprs.into_rust()?,
                mfp_after: proto
                    .mfp_after
                    .into_rust_if_some("ProtoPlanFlatMap::mfp_after")?,
                input_key: input_k_try_into(proto.input_key)?,
                node_id: proto.node_id.into_rust()?,
            },
            Join(proto) => Plan::Join {
                inputs: proto.inputs.into_rust()?,
                plan: proto.plan.into_rust_if_some("")?,
                node_id: proto.node_id.into_rust()?,
            },
            Reduce(proto) => Plan::Reduce {
                input: proto.input.into_rust_if_some("ProtoPlanReduce::input")?,
                key_val_plan: proto
                    .key_val_plan
                    .into_rust_if_some("ProtoPlanReduce::key_val_plan")?,
                plan: proto.plan.into_rust_if_some("ProtoPlanReduce::plan")?,
                input_key: input_k_try_into(proto.input_key)?,
                mfp_after: proto
                    .mfp_after
                    .into_rust_if_some("ProtoPlanReduce::mfp_after")?,
                node_id: proto.node_id.into_rust()?,
            },
            TopK(proto) => Plan::TopK {
                input: proto.input.into_rust_if_some("ProtoPlanTopK::input")?,
                top_k_plan: proto
                    .top_k_plan
                    .into_rust_if_some("ProtoPlanTopK::top_k_plan")?,
                node_id: proto.node_id.into_rust()?,
            },
            Negate(proto) => Plan::Negate {
                input: proto.input.into_rust_if_some("ProtoPlanNegate::input")?,
                node_id: proto.node_id.into_rust()?,
            },
            Threshold(proto) => Plan::Threshold {
                input: proto.input.into_rust_if_some("ProtoPlanThreshold::input")?,
                threshold_plan: proto
                    .threshold_plan
                    .into_rust_if_some("ProtoPlanThreshold::threshold_plan")?,
                node_id: proto.node_id.into_rust()?,
            },
            Union(proto) => Plan::Union {
                inputs: proto.inputs.into_rust()?,
                consolidate_output: proto.consolidate_output.into_rust()?,
                node_id: proto.node_id.into_rust()?,
            },
            ArrangeBy(proto) => Plan::ArrangeBy {
                input: proto.input.into_rust_if_some("ProtoPlanArrangeBy::input")?,
                forms: proto.forms.into_rust_if_some("ProtoPlanArrangeBy::forms")?,
                input_key: input_k_try_into(proto.input_key)?,
                input_mfp: proto
                    .input_mfp
                    .into_rust_if_some("ProtoPlanArrangeBy::input_mfp")?,
                node_id: proto.node_id.into_rust()?,
            },
        })
    }
}

impl RustType<proto_plan::ProtoRowDiff> for (Row, mz_repr::Timestamp, i64) {
    fn into_proto(&self) -> proto_plan::ProtoRowDiff {
        proto_plan::ProtoRowDiff {
            row: Some(self.0.into_proto()),
            timestamp: self.1.into(),
            diff: self.2.clone(),
        }
    }

    fn from_proto(proto: proto_plan::ProtoRowDiff) -> Result<Self, TryFromProtoError> {
        Ok((
            proto.row.into_rust_if_some("ProtoRowDiff::row")?,
            proto.timestamp.into(),
            proto.diff,
        ))
    }
}

impl RustType<proto_plan::ProtoRowDiffVec> for Vec<(Row, mz_repr::Timestamp, i64)> {
    fn into_proto(&self) -> proto_plan::ProtoRowDiffVec {
        proto_plan::ProtoRowDiffVec {
            rows: self.into_proto(),
        }
    }

    fn from_proto(proto: proto_plan::ProtoRowDiffVec) -> Result<Self, TryFromProtoError> {
        proto.rows.into_rust()
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
        enable_consolidate_after_union_negate: bool,
        enable_reduce_mfp_fusion: bool,
    ) -> Result<DataflowDescription<Self>, String> {
        // First, we lower the dataflow description from MIR to LIR.
        let mut dataflow = Self::lower_dataflow(desc, enable_reduce_mfp_fusion)?;

        // Subsequently, we perform plan refinements for the dataflow.
        Self::refine_source_mfps(&mut dataflow);

        if enable_consolidate_after_union_negate {
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
                .filter_map(|(id, (_, monotonic))| if *monotonic { Some(id) } else { None })
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
        enable_reduce_mfp_fusion: bool,
    ) -> Result<DataflowDescription<Self>, String> {
        let context = lowering::Context::new(desc.debug_name.clone(), enable_reduce_mfp_fusion);
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
        for (source_id, (source, _monotonic)) in dataflow.source_imports.iter_mut() {
            let mut identity_present = false;
            let mut mfps = Vec::new();
            for build_desc in dataflow.objects_to_build.iter_mut() {
                let mut todo = vec![&mut build_desc.plan];
                while let Some(expression) = todo.pop() {
                    if let Plan::Get { id, plan, .. } = expression {
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
                        todo.extend(expression.children_mut());
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
                match expression {
                    Plan::Union {
                        inputs,
                        consolidate_output,
                        ..
                    } => {
                        if inputs
                            .iter()
                            .any(|input| matches!(input, Plan::Negate { .. }))
                        {
                            *consolidate_output = true;
                        }
                    }
                    _ => {}
                }
                todo.extend(expression.children_mut());
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
                match expression {
                    Plan::Reduce { plan, .. } => {
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
                        todo.extend(expression.children_mut());
                    }
                    Plan::TopK { top_k_plan, .. } => {
                        top_k_plan.as_monotonic(true);
                        todo.extend(expression.children_mut());
                    }
                    Plan::LetRec { body, .. } => {
                        // Only the non-recursive `body` is restricted to a single time.
                        todo.push(body);
                    }
                    _ => {
                        // Nothing to do for other expressions, and doing nothing is safe for future expressions.
                        todo.extend(expression.children_mut());
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

    /// Partitions the plan into `parts` many disjoint pieces.
    ///
    /// This is used to partition `Plan::Constant` stages so that the work
    /// can be distributed across many workers.
    pub fn partition_among(self, parts: usize) -> Vec<Self> {
        if parts == 0 {
            Vec::new()
        } else if parts == 1 {
            vec![self]
        } else {
            match self {
                // For constants, balance the rows across the workers.
                Plan::Constant { rows, node_id } => match rows {
                    Ok(rows) => {
                        let mut rows_parts = vec![Vec::new(); parts];
                        for (index, row) in rows.into_iter().enumerate() {
                            rows_parts[index % parts].push(row);
                        }
                        rows_parts
                            .into_iter()
                            .map(|rows| Plan::Constant {
                                rows: Ok(rows),
                                node_id,
                            })
                            .collect()
                    }
                    Err(err) => {
                        let mut result = vec![
                            Plan::Constant {
                                rows: Ok(Vec::new()),
                                node_id,
                            };
                            parts
                        ];
                        result[0] = Plan::Constant {
                            rows: Err(err),
                            node_id,
                        };
                        result
                    }
                },

                // For all other variants, just replace inputs with appropriately sharded versions.
                // This is surprisingly verbose, but that is all it is doing.
                Plan::Get {
                    id,
                    keys,
                    plan,
                    node_id,
                } => vec![
                    Plan::Get {
                        id,
                        keys,
                        plan,
                        node_id
                    };
                    parts
                ],
                Plan::Let {
                    value,
                    body,
                    id,
                    node_id,
                } => {
                    let value_parts = value.partition_among(parts);
                    let body_parts = body.partition_among(parts);
                    value_parts
                        .into_iter()
                        .zip(body_parts)
                        .map(|(value, body)| Plan::Let {
                            value: Box::new(value),
                            body: Box::new(body),
                            id,
                            node_id,
                        })
                        .collect()
                }
                Plan::LetRec {
                    ids,
                    values,
                    limits,
                    body,
                    node_id,
                } => {
                    let mut values_parts: Vec<Vec<Self>> = vec![Vec::new(); parts];
                    for value in values.into_iter() {
                        for (index, part) in value.partition_among(parts).into_iter().enumerate() {
                            values_parts[index].push(part);
                        }
                    }
                    let body_parts = body.partition_among(parts);
                    values_parts
                        .into_iter()
                        .zip(body_parts)
                        .map(|(values, body)| Plan::LetRec {
                            values,
                            body: Box::new(body),
                            limits: limits.clone(),
                            ids: ids.clone(),
                            node_id,
                        })
                        .collect()
                }
                Plan::Mfp {
                    input,
                    input_key_val,
                    mfp,
                    node_id,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Mfp {
                        input: Box::new(input),
                        mfp: mfp.clone(),
                        input_key_val: input_key_val.clone(),
                        node_id,
                    })
                    .collect(),
                Plan::FlatMap {
                    input,
                    input_key,
                    func,
                    exprs,
                    mfp_after: mfp,
                    node_id,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::FlatMap {
                        input: Box::new(input),
                        input_key: input_key.clone(),
                        func: func.clone(),
                        exprs: exprs.clone(),
                        mfp_after: mfp.clone(),
                        node_id,
                    })
                    .collect(),
                Plan::Join {
                    inputs,
                    plan,
                    node_id,
                } => {
                    let mut inputs_parts = vec![Vec::new(); parts];
                    for input in inputs.into_iter() {
                        for (index, input_part) in
                            input.partition_among(parts).into_iter().enumerate()
                        {
                            inputs_parts[index].push(input_part);
                        }
                    }
                    inputs_parts
                        .into_iter()
                        .map(|inputs| Plan::Join {
                            inputs,
                            plan: plan.clone(),
                            node_id,
                        })
                        .collect()
                }
                Plan::Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                    mfp_after,
                    node_id,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Reduce {
                        input: Box::new(input),
                        input_key: input_key.clone(),
                        key_val_plan: key_val_plan.clone(),
                        plan: plan.clone(),
                        mfp_after: mfp_after.clone(),
                        node_id,
                    })
                    .collect(),
                Plan::TopK {
                    input,
                    top_k_plan,
                    node_id,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::TopK {
                        input: Box::new(input),
                        top_k_plan: top_k_plan.clone(),
                        node_id,
                    })
                    .collect(),
                Plan::Negate { input, node_id } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Negate {
                        input: Box::new(input),
                        node_id,
                    })
                    .collect(),
                Plan::Threshold {
                    input,
                    threshold_plan,
                    node_id,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Threshold {
                        input: Box::new(input),
                        threshold_plan: threshold_plan.clone(),
                        node_id,
                    })
                    .collect(),
                Plan::Union {
                    inputs,
                    consolidate_output,
                    node_id,
                } => {
                    let mut inputs_parts = vec![Vec::new(); parts];
                    for input in inputs.into_iter() {
                        for (index, input_part) in
                            input.partition_among(parts).into_iter().enumerate()
                        {
                            inputs_parts[index].push(input_part);
                        }
                    }
                    inputs_parts
                        .into_iter()
                        .map(|inputs| Plan::Union {
                            inputs,
                            consolidate_output,
                            node_id,
                        })
                        .collect()
                }
                Plan::ArrangeBy {
                    input,
                    forms: keys,
                    input_key,
                    input_mfp,
                    node_id,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::ArrangeBy {
                        input: Box::new(input),
                        forms: keys.clone(),
                        input_key: input_key.clone(),
                        input_mfp: input_mfp.clone(),
                        node_id,
                    })
                    .collect(),
            }
        }
    }
}

impl<T> CollectionPlan for Plan<T> {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        match self {
            Plan::Constant {
                rows: _,
                node_id: _,
            } => (),
            Plan::Get {
                id,
                keys: _,
                plan: _,
                node_id: _,
            } => match id {
                Id::Global(id) => {
                    out.insert(*id);
                }
                Id::Local(_) => (),
            },
            Plan::Let {
                id: _,
                value,
                body,
                node_id: _,
            } => {
                value.depends_on_into(out);
                body.depends_on_into(out);
            }
            Plan::LetRec {
                ids: _,
                values,
                limits: _,
                body,
                node_id: _,
            } => {
                for value in values.iter() {
                    value.depends_on_into(out);
                }
                body.depends_on_into(out);
            }
            Plan::Join {
                inputs,
                plan: _,
                node_id: _,
            }
            | Plan::Union {
                inputs,
                consolidate_output: _,
                node_id: _,
            } => {
                for input in inputs {
                    input.depends_on_into(out);
                }
            }
            Plan::Mfp {
                input,
                mfp: _,
                input_key_val: _,
                node_id: _,
            }
            | Plan::FlatMap {
                input,
                func: _,
                exprs: _,
                mfp_after: _,
                input_key: _,
                node_id: _,
            }
            | Plan::ArrangeBy {
                input,
                forms: _,
                input_key: _,
                input_mfp: _,
                node_id: _,
            }
            | Plan::Reduce {
                input,
                key_val_plan: _,
                plan: _,
                input_key: _,
                mfp_after: _,
                node_id: _,
            }
            | Plan::TopK {
                input,
                top_k_plan: _,
                node_id: _,
            }
            | Plan::Negate { input, node_id: _ }
            | Plan::Threshold {
                input,
                threshold_plan: _,
                node_id: _,
            } => {
                input.depends_on_into(out);
            }
        }
    }
}

/// Returns bucket sizes, descending, suitable for hierarchical decomposition of an operator, based
/// on the expected number of rows that will have the same group key.
fn bucketing_of_expected_group_size(expected_group_size: Option<u64>) -> Vec<u64> {
    // NOTE(vmarcos): The fan-in of 16 defined below is used in the tuning advice built-in view
    // mz_internal.mz_expected_group_size_advice.
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
    use mz_proto::protobuf_roundtrip;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
        fn available_collections_protobuf_roundtrip(expect in any::<AvailableCollections>() ) {
            let actual = protobuf_roundtrip::<_, ProtoAvailableCollections>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
        fn get_plan_protobuf_roundtrip(expect in any::<GetPlan>()) {
            let actual = protobuf_roundtrip::<_, ProtoGetPlan>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[mz_ore::test]
        fn plan_protobuf_roundtrip(expect in any::<Plan>()) {
            let actual = protobuf_roundtrip::<_, ProtoPlan>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

    }
}
