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

use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use mz_expr::JoinImplementation::{DeltaQuery, Differential, IndexedFilter, Unimplemented};
use mz_expr::{
    permutation_for_arrangement, CollectionPlan, EvalError, Id, JoinInputMapper, LetRecLimit,
    LocalId, MapFilterProject, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, TableFunc,
};
use mz_ore::soft_panic_or_log;
use mz_ore::str::Indent;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::explain::text::text_string_at;
use mz_repr::explain::{DummyHumanizer, ExplainConfig, ExprHumanizer, PlanRenderingContext};
use mz_repr::{Diff, GlobalId, Row};
use proptest::arbitrary::Arbitrary;
use proptest::prelude::*;
use proptest::strategy::Strategy;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::plan::join::{DeltaJoinPlan, JoinPlan, LinearJoinPlan};
use crate::plan::reduce::{KeyValPlan, ReducePlan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::types::dataflows::{BuildDesc, DataflowDescription};

pub mod join;
pub mod reduce;
pub mod threshold;
pub mod top_k;

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.plan.rs"));

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
#[derive(Arbitrary, Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AvailableCollections {
    /// Whether the collection exists in unarranged form.
    pub raw: bool,
    /// The set of arrangements of the collection, along with a
    /// column permutation mapping
    #[proptest(strategy = "prop::collection::vec(any_arranged_thin(), 0..3)")]
    pub arranged: Vec<(Vec<MirScalarExpr>, BTreeMap<usize, usize>, Vec<usize>)>,
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

impl RustType<ProtoAvailableCollections> for AvailableCollections {
    fn into_proto(&self) -> ProtoAvailableCollections {
        ProtoAvailableCollections {
            raw: self.raw,
            arranged: self.arranged.into_proto(),
        }
    }

    fn from_proto(x: ProtoAvailableCollections) -> Result<Self, TryFromProtoError> {
        Ok({
            Self {
                raw: x.raw,
                arranged: x.arranged.into_rust()?,
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
        }
    }

    /// Represent a collection that is arranged in the
    /// specified ways.
    pub fn new_arranged(
        arranged: Vec<(Vec<MirScalarExpr>, BTreeMap<usize, usize>, Vec<usize>)>,
    ) -> Self {
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

/// A rendering plan with as much conditional logic as possible removed.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Plan<T = mz_repr::Timestamp> {
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
    /// stages, but for practical reasons cannot. For example: reduce, threshold,
    /// and topk stages are not able to absorb this operator.
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
        mfp: MapFilterProject,
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
    },
}

impl<T> Plan<T> {
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
            | Negate { input }
            | Threshold { input, .. }
            | ArrangeBy { input, .. } => {
                first = Some(&mut **input);
            }
            Join { inputs, .. } | Union { inputs } => {
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
        let constant = prop::result::maybe_ok(row_diff, EvalError::arbitrary())
            .prop_map(|rows| Plan::Constant { rows });

        let get = (any::<Id>(), any::<AvailableCollections>(), any::<GetPlan>())
            .prop_map(|(id, keys, plan)| Plan::<mz_repr::Timestamp>::Get { id, keys, plan });

        let leaf = prop::strategy::Union::new(vec![constant.boxed(), get.boxed()]).boxed();

        leaf.prop_recursive(2, 4, 5, |inner| {
            prop::strategy::Union::new(vec![
                //Plan::Let
                (any::<LocalId>(), inner.clone(), inner.clone())
                    .prop_map(|(id, value, body)| Plan::Let {
                        id,
                        value: value.into(),
                        body: body.into(),
                    })
                    .boxed(),
                //Plan::Mfp
                (
                    inner.clone(),
                    any::<MapFilterProject>(),
                    any::<Option<(Vec<MirScalarExpr>, Option<Row>)>>(),
                )
                    .prop_map(|(input, mfp, input_key_val)| Plan::Mfp {
                        input: input.into(),
                        mfp,
                        input_key_val,
                    })
                    .boxed(),
                //Plan::FlatMap
                (
                    inner.clone(),
                    any::<TableFunc>(),
                    any::<Vec<MirScalarExpr>>(),
                    any::<MapFilterProject>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                )
                    .prop_map(|(input, func, exprs, mfp, input_key)| Plan::FlatMap {
                        input: input.into(),
                        func,
                        exprs,
                        mfp,
                        input_key,
                    })
                    .boxed(),
                //Plan::Join
                (
                    prop::collection::vec(inner.clone(), 0..2),
                    any::<JoinPlan>(),
                )
                    .prop_map(|(inputs, plan)| Plan::Join { inputs, plan })
                    .boxed(),
                //Plan::Reduce
                (
                    inner.clone(),
                    any::<KeyValPlan>(),
                    any::<ReducePlan>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                )
                    .prop_map(|(input, key_val_plan, plan, input_key)| Plan::Reduce {
                        input: input.into(),
                        key_val_plan,
                        plan,
                        input_key,
                    })
                    .boxed(),
                //Plan::TopK
                (inner.clone(), any::<TopKPlan>())
                    .prop_map(|(input, top_k_plan)| Plan::TopK {
                        input: input.into(),
                        top_k_plan,
                    })
                    .boxed(),
                //Plan::Negate
                inner
                    .clone()
                    .prop_map(|x| Plan::Negate { input: x.into() })
                    .boxed(),
                //Plan::Threshold
                (inner.clone(), any::<ThresholdPlan>())
                    .prop_map(|(input, threshold_plan)| Plan::Threshold {
                        input: input.into(),
                        threshold_plan,
                    })
                    .boxed(),
                // Plan::Union
                prop::collection::vec(inner.clone(), 0..2)
                    .prop_map(|x| Plan::Union { inputs: x })
                    .boxed(),
                //Plan::ArrangeBy
                (
                    inner,
                    any::<AvailableCollections>(),
                    any::<Option<Vec<MirScalarExpr>>>(),
                    any::<MapFilterProject>(),
                )
                    .prop_map(|(input, forms, input_key, input_mfp)| Plan::ArrangeBy {
                        input: input.into(),
                        forms,
                        input_key,
                        input_mfp,
                    })
                    .boxed(),
            ])
        })
        .boxed()
    }
}

impl RustType<proto_plan::ProtoPlanConstant>
    for Result<Vec<(Row, mz_repr::Timestamp, i64)>, EvalError>
{
    fn into_proto(&self) -> proto_plan::ProtoPlanConstant {
        use proto_plan::proto_plan_constant::Result;
        proto_plan::ProtoPlanConstant {
            result: Some(match self {
                Ok(rows) => Result::Rows(rows.into_proto()),
                Err(err) => Result::Err(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: proto_plan::ProtoPlanConstant) -> Result<Self, TryFromProtoError> {
        use proto_plan::proto_plan_constant::Result;
        match proto.result {
            Some(Result::Rows(rows)) => Ok(Ok(rows.into_rust()?)),
            Some(Result::Err(err)) => Ok(Err(err.into_rust()?)),
            None => Err(TryFromProtoError::missing_field(
                "ProtoPlanConstant::result",
            )),
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
                Plan::Constant { rows } => Constant(rows.into_proto()),
                Plan::Get { id, keys, plan } => Get(ProtoPlanGet {
                    id: Some(id.into_proto()),
                    keys: Some(keys.into_proto()),
                    plan: Some(plan.into_proto()),
                }),
                Plan::Let { id, value, body } => Let(ProtoPlanLet {
                    id: Some(id.into_proto()),
                    value: Some(value.into_proto()),
                    body: Some(body.into_proto()),
                }
                .into()),
                Plan::LetRec {
                    ids,
                    values,
                    limits,
                    body,
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
                    }
                    .into(),
                ),
                Plan::Mfp {
                    input,
                    mfp,
                    input_key_val,
                } => Mfp(ProtoPlanMfp {
                    input: Some(input.into_proto()),
                    mfp: Some(mfp.into_proto()),
                    input_key_val: input_kv_into(input_key_val),
                }
                .into()),
                Plan::FlatMap {
                    input,
                    func,
                    exprs,
                    mfp,
                    input_key,
                } => FlatMap(
                    ProtoPlanFlatMap {
                        input: Some(input.into_proto()),
                        func: Some(func.into_proto()),
                        exprs: exprs.into_proto(),
                        mfp: Some(mfp.into_proto()),
                        input_key: input_k_into(input_key.as_ref()),
                    }
                    .into(),
                ),
                Plan::Join { inputs, plan } => Join(ProtoPlanJoin {
                    inputs: inputs.into_proto(),
                    plan: Some(plan.into_proto()),
                }),
                Plan::Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                } => Reduce(
                    ProtoPlanReduce {
                        input: Some(input.into_proto()),
                        key_val_plan: Some(key_val_plan.into_proto()),
                        plan: Some(plan.into_proto()),
                        input_key: input_k_into(input_key.as_ref()),
                    }
                    .into(),
                ),
                Plan::TopK { input, top_k_plan } => TopK(
                    ProtoPlanTopK {
                        input: Some(input.into_proto()),
                        top_k_plan: Some(top_k_plan.into_proto()),
                    }
                    .into(),
                ),
                Plan::Negate { input } => Negate(input.into_proto()),
                Plan::Threshold {
                    input,
                    threshold_plan,
                } => Threshold(
                    ProtoPlanThreshold {
                        input: Some(input.into_proto()),
                        threshold_plan: Some(threshold_plan.into_proto()),
                    }
                    .into(),
                ),
                Plan::Union { inputs } => Union(ProtoPlanUnion {
                    inputs: inputs.into_proto(),
                }),
                Plan::ArrangeBy {
                    input,
                    forms,
                    input_key,
                    input_mfp,
                } => ArrangeBy(
                    ProtoPlanArrangeBy {
                        input: Some(input.into_proto()),
                        forms: Some(forms.into_proto()),
                        input_key: input_k_into(input_key.as_ref()),
                        input_mfp: Some(input_mfp.into_proto()),
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
            Constant(ProtoPlanConstant { result }) => {
                let result = result
                    .ok_or_else(|| TryFromProtoError::missing_field("ProtoPlanConstant::result"))?;

                Plan::Constant {
                    rows: match result {
                        proto_plan_constant::Result::Rows(rows) => Ok(rows.into_rust()?),
                        proto_plan_constant::Result::Err(eval_err) => Err(eval_err.into_rust()?),
                    },
                }
            }
            Get(proto) => Plan::Get {
                id: proto.id.into_rust_if_some("ProtoPlanGet::id")?,
                keys: proto.keys.into_rust_if_some("ProtoPlanGet::keys")?,
                plan: proto.plan.into_rust_if_some("ProtoPlanGet::plan")?,
            },
            Let(proto) => Plan::Let {
                id: proto.id.into_rust_if_some("ProtoPlanLet::id")?,
                value: proto.value.into_rust_if_some("ProtoPlanLet::value")?,
                body: proto.body.into_rust_if_some("ProtoPlanLet::body")?,
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
                    .zip_eq(limit_is_some.into_iter())
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
                }
            }
            Mfp(proto) => Plan::Mfp {
                input: proto.input.into_rust_if_some("ProtoPlanMfp::input")?,
                input_key_val: input_kv_try_into(proto.input_key_val)?,
                mfp: proto.mfp.into_rust_if_some("ProtoPlanMfp::mfp")?,
            },
            FlatMap(proto) => Plan::FlatMap {
                input: proto.input.into_rust_if_some("ProtoPlanFlatMap::input")?,
                func: proto.func.into_rust_if_some("ProtoPlanFlatMap::func")?,
                exprs: proto.exprs.into_rust()?,
                mfp: proto.mfp.into_rust_if_some("ProtoPlanFlatMap::mfp")?,
                input_key: input_k_try_into(proto.input_key)?,
            },
            Join(proto) => Plan::Join {
                inputs: proto.inputs.into_rust()?,
                plan: proto.plan.into_rust_if_some("")?,
            },
            Reduce(proto) => Plan::Reduce {
                input: proto.input.into_rust_if_some("ProtoPlanReduce::input")?,
                key_val_plan: proto
                    .key_val_plan
                    .into_rust_if_some("ProtoPlanReduce::key_val_plan")?,
                plan: proto.plan.into_rust_if_some("ProtoPlanReduce::plan")?,
                input_key: input_k_try_into(proto.input_key)?,
            },
            TopK(proto) => Plan::TopK {
                input: proto.input.into_rust_if_some("ProtoPlanTopK::input")?,
                top_k_plan: proto
                    .top_k_plan
                    .into_rust_if_some("ProtoPlanTopK::top_k_plan")?,
            },
            Negate(proto) => Plan::Negate {
                input: proto.into_rust()?,
            },
            Threshold(proto) => Plan::Threshold {
                input: proto.input.into_rust_if_some("ProtoPlanThreshold::input")?,
                threshold_plan: proto
                    .threshold_plan
                    .into_rust_if_some("ProtoPlanThreshold::threshold_plan")?,
            },
            Union(proto) => Plan::Union {
                inputs: proto.inputs.into_rust()?,
            },
            ArrangeBy(proto) => Plan::ArrangeBy {
                input: proto.input.into_rust_if_some("ProtoPlanArrangeBy::input")?,
                forms: proto.forms.into_rust_if_some("ProtoPlanArrangeBy::forms")?,
                input_key: input_k_try_into(proto.input_key)?,
                input_mfp: proto
                    .input_mfp
                    .into_rust_if_some("ProtoPlanArrangeBy::input_mfp")?,
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
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
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

/// Various bits of state to print along with error messages during LIR planning,
/// to aid debugging.
#[derive(Copy, Clone, Debug)]
pub struct LirDebugInfo<'a> {
    debug_name: &'a str,
    id: GlobalId,
}

impl<'a> std::fmt::Display for LirDebugInfo<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Debug name: {}; id: {}", self.debug_name, self.id)
    }
}

impl<T: timely::progress::Timestamp> Plan<T> {
    /// Replace the plan with another one
    /// that has the collection in some additional forms.
    pub fn arrange_by(
        self,
        collections: AvailableCollections,
        old_collections: &AvailableCollections,
        arity: usize,
    ) -> Self {
        let new_self = if let Self::ArrangeBy {
            input,
            mut forms,
            input_key,
            input_mfp,
        } = self
        {
            forms.raw |= collections.raw;
            forms.arranged.extend(collections.arranged.into_iter());
            forms.arranged.sort_by(|k1, k2| k1.0.cmp(&k2.0));
            forms.arranged.dedup_by(|k1, k2| k1.0 == k2.0);
            Self::ArrangeBy {
                input,
                forms,
                input_key,
                input_mfp,
            }
        } else {
            let (input_key, input_mfp) = if let Some((input_key, permutation, thinning)) =
                old_collections.arbitrary_arrangement()
            {
                let mut mfp = MapFilterProject::new(arity);
                mfp.permute(permutation.clone(), thinning.len() + input_key.len());
                (Some(input_key.clone()), mfp)
            } else {
                (None, MapFilterProject::new(arity))
            };
            Self::ArrangeBy {
                input: Box::new(self),
                forms: collections,
                input_key,
                input_mfp,
            }
        };
        new_self
    }
    /// This method converts a MirRelationExpr into a plan that can be directly rendered.
    ///
    /// The rough structure is that we repeatedly extract map/filter/project operators
    /// from each expression we see, bundle them up as a `MapFilterProject` object, and
    /// then produce a plan for the combination of that with the next operator.
    ///
    /// The method takes as an argument the existing arrangements for each bound identifier,
    /// which it will locally add to and remove from for `Let` bindings (by the end of the
    /// call it should contain the same bindings as when it started).
    ///
    /// The result of the method is both a `Plan`, but also a list of arrangements that
    /// are certain to be produced, which can be relied on by the next steps in the plan.
    /// Each of the arrangement keys is associated with an MFP that must be applied if that arrangement is used,
    /// to back out the permutation associated with that arrangement.
    ///
    /// An empty list of arrangement keys indicates that only a `Collection` stream can
    /// be assumed to exist.
    fn from_mir(
        expr: &MirRelationExpr,
        arrangements: &mut BTreeMap<Id, AvailableCollections>,
        debug_info: LirDebugInfo<'_>,
    ) -> Result<(Self, AvailableCollections), String> {
        // This function is recursive and can overflow its stack, so grow it if
        // needed. The growth here is unbounded. Our general solution for this problem
        // is to use [`ore::stack::RecursionGuard`] to additionally limit the stack
        // depth. That however requires upstream error handling. This function is
        // currently called by the Coordinator after calls to `catalog_transact`,
        // and thus are not allowed to fail. Until that allows errors, we choose
        // to allow the unbounded growth here. We are though somewhat protected by
        // higher levels enforcing their own limits on stack depth (in the parser,
        // transformer/desugarer, and planner).
        mz_ore::stack::maybe_grow(|| Plan::from_mir_stack_safe(expr, arrangements, debug_info))
    }

    fn from_mir_stack_safe(
        expr: &MirRelationExpr,
        arrangements: &mut BTreeMap<Id, AvailableCollections>,
        debug_info: LirDebugInfo<'_>,
    ) -> Result<(Self, AvailableCollections), String> {
        // Extract a maximally large MapFilterProject from `expr`.
        // We will then try and push this in to the resulting expression.
        //
        // Importantly, `mfp` may contain temporal operators and not be a "safe" MFP.
        // While we would eventually like all plan stages to be able to absorb such
        // general operators, not all of them can.
        let (mut mfp, expr) = MapFilterProject::extract_from_expression(expr);
        // We attempt to plan what we have remaining, in the context of `mfp`.
        // We may not be able to do this, and must wrap some operators with a `Mfp` stage.
        let (mut plan, mut keys) = match expr {
            // These operators should have been extracted from the expression.
            MirRelationExpr::Map { .. } => {
                panic!("This operator should have been extracted");
            }
            MirRelationExpr::Filter { .. } => {
                panic!("This operator should have been extracted");
            }
            MirRelationExpr::Project { .. } => {
                panic!("This operator should have been extracted");
            }
            // These operators may not have been extracted, and need to result in a `Plan`.
            MirRelationExpr::Constant { rows, typ: _ } => {
                let plan = Plan::Constant {
                    rows: rows.clone().map(|rows| {
                        rows.into_iter()
                            .map(|(row, diff)| (row, T::minimum(), diff))
                            .collect()
                    }),
                };
                // The plan, not arranged in any way.
                (plan, AvailableCollections::new_raw())
            }
            MirRelationExpr::Get { id, typ: _ } => {
                // This stage can absorb arbitrary MFP operators.
                let mut mfp = mfp.take();
                // If `mfp` is the identity, we can surface all imported arrangements.
                // Otherwise, we apply `mfp` and promise no arrangements.
                let mut in_keys = arrangements
                    .get(id)
                    .cloned()
                    .unwrap_or_else(AvailableCollections::new_raw);

                // Seek out an arrangement key that might be constrained to a literal.
                // TODO: Improve key selection heuristic.
                // Note that most (actually all, as far as I know) of the cases that used to be
                // handled by this code are instead handled by `CanonicalizeMfp`.
                let key_val = in_keys
                    .arranged
                    .iter()
                    .filter_map(|key| {
                        mfp.literal_constraints(&key.0)
                            .map(|val| (key.clone(), val))
                    })
                    .max_by_key(|(key, _val)| key.0.len());

                // Determine the plan of action for the `Get` stage.
                let plan = if let Some(((key, permutation, thinning), val)) = &key_val {
                    mfp.permute(permutation.clone(), thinning.len() + key.len());
                    in_keys.arranged = vec![(key.clone(), permutation.clone(), thinning.clone())];
                    GetPlan::Arrangement(key.clone(), Some(val.clone()), mfp)
                } else if !mfp.is_identity() {
                    // We need to ensure a collection exists, which means we must form it.
                    if let Some((key, permutation, thinning)) =
                        in_keys.arbitrary_arrangement().cloned()
                    {
                        mfp.permute(permutation.clone(), thinning.len() + key.len());
                        in_keys.arranged = vec![(key.clone(), permutation, thinning)];
                        GetPlan::Arrangement(key, None, mfp)
                    } else {
                        GetPlan::Collection(mfp)
                    }
                } else {
                    // By default, just pass input arrangements through.
                    GetPlan::PassArrangements
                };

                let out_keys = if let GetPlan::PassArrangements = plan {
                    in_keys.clone()
                } else {
                    AvailableCollections::new_raw()
                };

                // Return the plan, and any keys if an identity `mfp`.
                (
                    Plan::Get {
                        id: id.clone(),
                        keys: in_keys,
                        plan,
                    },
                    out_keys,
                )
            }
            MirRelationExpr::Let { id, value, body } => {
                // It would be unfortunate to have a non-trivial `mfp` here, as we hope
                // that they would be pushed down. I am not sure if we should take the
                // initiative to push down the `mfp` ourselves.

                // Plan the value using only the initial arrangements, but
                // introduce any resulting arrangements bound to `id`.
                let (value, v_keys) = Plan::from_mir(value, arrangements, debug_info)?;
                let pre_existing = arrangements.insert(Id::Local(*id), v_keys);
                assert!(pre_existing.is_none());
                // Plan the body using initial and `value` arrangements,
                // and then remove reference to the value arrangements.
                let (body, b_keys) = Plan::from_mir(body, arrangements, debug_info)?;
                arrangements.remove(&Id::Local(*id));
                // Return the plan, and any `body` arrangements.
                (
                    Plan::Let {
                        id: id.clone(),
                        value: Box::new(value),
                        body: Box::new(body),
                    },
                    b_keys,
                )
            }
            MirRelationExpr::LetRec {
                ids,
                values,
                limits,
                body,
            } => {
                assert_eq!(ids.len(), values.len());
                assert_eq!(ids.len(), limits.len());
                // Plan the values using only the available arrangements, but
                // introduce any resulting arrangements bound to each `id`.
                // Arrangements made available cannot be used by prior bindings,
                // as we cannot circulate an arrangement through a `Variable` yet.
                let mut lir_values = Vec::with_capacity(values.len());
                for (id, value) in ids.iter().zip(values) {
                    let (mut lir_value, mut v_keys) =
                        Plan::from_mir(value, arrangements, debug_info)?;
                    // If `v_keys` does not contain an unarranged collection, we must form it.
                    if !v_keys.raw {
                        // Choose an "arbitrary" arrangement; TODO: prefer a specific one.
                        let (input_key, permutation, thinning) =
                            v_keys.arbitrary_arrangement().unwrap();
                        let mut input_mfp = MapFilterProject::new(value.arity());
                        input_mfp.permute(permutation.clone(), thinning.len() + input_key.len());
                        let input_key = Some(input_key.clone());

                        let forms = AvailableCollections::new_raw();

                        // We just want to insert an `ArrangeBy` to form an unarranged collection,
                        // but there is a complication: We shouldn't break the invariant (created by
                        // `NormalizeLets`, and relied upon by the rendering) that there isn't
                        // anything between two `LetRec`s. So if `lir_value` is itself a `LetRec`,
                        // then we insert the `ArrangeBy` on the `body` of the inner `LetRec`,
                        // instead of on top of the inner `LetRec`.
                        lir_value = match lir_value {
                            Plan::LetRec {
                                ids,
                                values,
                                limits,
                                body,
                            } => Plan::LetRec {
                                ids,
                                values,
                                limits,
                                body: Box::new(Plan::ArrangeBy {
                                    input: body,
                                    forms,
                                    input_key,
                                    input_mfp,
                                }),
                            },
                            lir_value => Plan::ArrangeBy {
                                input: Box::new(lir_value),
                                forms,
                                input_key,
                                input_mfp,
                            },
                        };
                        v_keys.raw = true;
                    }
                    let pre_existing = arrangements.insert(Id::Local(*id), v_keys);
                    assert!(pre_existing.is_none());
                    lir_values.push(lir_value);
                }
                // As we exit the iterative scope, we must leave all arrangements behind,
                // as they reference a timestamp coordinate that must be stripped off.
                for id in ids.iter() {
                    arrangements.insert(Id::Local(*id), AvailableCollections::new_raw());
                }
                // Plan the body using initial and `value` arrangements,
                // and then remove reference to the value arrangements.
                let (body, b_keys) = Plan::from_mir(body, arrangements, debug_info)?;
                for id in ids.iter() {
                    arrangements.remove(&Id::Local(*id));
                }
                // Return the plan, and any `body` arrangements.
                (
                    Plan::LetRec {
                        ids: ids.clone(),
                        values: lir_values,
                        limits: limits.clone(),
                        body: Box::new(body),
                    },
                    b_keys,
                )
            }
            MirRelationExpr::FlatMap { input, func, exprs } => {
                let (input, keys) = Plan::from_mir(input, arrangements, debug_info)?;
                // This stage can absorb arbitrary MFP instances.
                let mfp = mfp.take();
                let mut exprs = exprs.clone();
                let input_key = if let Some((k, permutation, _)) = keys.arbitrary_arrangement() {
                    // We don't permute the MFP here, because it runs _after_ the table function,
                    // whose output is in a fixed order.
                    //
                    // We _do_, however, need to permute the `expr`s that provide input to the
                    // `func`.
                    for expr in &mut exprs {
                        expr.permute_map(permutation);
                    }

                    Some(k.clone())
                } else {
                    None
                };
                // Return the plan, and no arrangements.
                (
                    Plan::FlatMap {
                        input: Box::new(input),
                        func: func.clone(),
                        exprs: exprs.clone(),
                        mfp,
                        input_key,
                    },
                    AvailableCollections::new_raw(),
                )
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                implementation,
            } => {
                let input_mapper = JoinInputMapper::new(inputs);

                // Plan each of the join inputs independently.
                // The `plans` get surfaced upwards, and the `input_keys` should
                // be used as part of join planning / to validate the existing
                // plans / to aid in indexed seeding of update streams.
                let mut plans = Vec::new();
                let mut input_keys = Vec::new();
                let mut input_arities = Vec::new();
                for input in inputs.iter() {
                    let (plan, keys) = Plan::from_mir(input, arrangements, debug_info)?;
                    input_arities.push(input.arity());
                    plans.push(plan);
                    input_keys.push(keys);
                }

                // Extract temporal predicates as joins cannot currently absorb them.
                let (plan, missing) = match implementation {
                    IndexedFilter(_id, key, _val) => {
                        // Start with the constant input. (This used to be important before #14059
                        // was fixed.)
                        let start: usize = 1;
                        let order = vec![(0usize, key.clone(), None)];
                        // All columns of the constant input will be part of the arrangement key.
                        // Note that currently nothing else would make this arrangement exist, so
                        // this will end up in `missing`, and thus we'll insert an LIR ArrangeBy
                        // later.
                        let source_arrangement = (
                            (0..key.len())
                                .map(MirScalarExpr::Column)
                                .collect::<Vec<_>>(),
                            (0..key.len()).map(|i| (i, i)).collect::<BTreeMap<_, _>>(),
                            Vec::<usize>::new(),
                        );
                        let (ljp, missing) = LinearJoinPlan::create_from(
                            start,
                            Some(&source_arrangement),
                            equivalences,
                            &order,
                            input_mapper,
                            &mut mfp,
                            &input_keys,
                        );
                        (JoinPlan::Linear(ljp), missing)
                    }
                    Differential((start, start_arr, _start_characteristic), order) => {
                        let source_arrangement = start_arr.as_ref().and_then(|key| {
                            input_keys[*start]
                                .arranged
                                .iter()
                                .find(|(k, _, _)| k == key)
                                .clone()
                        });
                        let (ljp, missing) = LinearJoinPlan::create_from(
                            *start,
                            source_arrangement,
                            equivalences,
                            order,
                            input_mapper,
                            &mut mfp,
                            &input_keys,
                        );
                        (JoinPlan::Linear(ljp), missing)
                    }
                    DeltaQuery(orders) => {
                        let (djp, missing) = DeltaJoinPlan::create_from(
                            equivalences,
                            orders,
                            input_mapper,
                            &mut mfp,
                            &input_keys,
                        );
                        (JoinPlan::Delta(djp), missing)
                    }
                    // Other plans are errors, and should be reported as such.
                    Unimplemented => return Err("unimplemented join".to_string()),
                };
                // The renderer will expect certain arrangements to exist; if any of those are not available, the join planning functions above should have returned them in
                // `missing`. We thus need to plan them here so they'll exist.
                let is_delta = matches!(plan, JoinPlan::Delta(_));
                for (((input_plan, input_keys), missing), arity) in plans
                    .iter_mut()
                    .zip(input_keys.iter())
                    .zip(missing.into_iter())
                    .zip(input_arities.iter().cloned())
                {
                    if missing != Default::default() {
                        if is_delta {
                            // join_implementation.rs produced a sub-optimal plan here;
                            // we shouldn't plan delta joins at all if not all of the required arrangements
                            // are available. Print an error message, to increase the chances that
                            // the user will tell us about this.
                            soft_panic_or_log!("Arrangements depended on by delta join alarmingly absent: {:?}
Dataflow info: {}
This is not expected to cause incorrect results, but could indicate a performance issue in Materialize.", missing, debug_info);
                        } else {
                            // It's fine and expected that linear joins don't have all their arrangements available up front,
                            // so no need to print an error here.
                        }
                        let raw_plan = std::mem::replace(
                            input_plan,
                            Plan::Constant {
                                rows: Ok(Vec::new()),
                            },
                        );
                        *input_plan = raw_plan.arrange_by(missing, input_keys, arity);
                    }
                }
                // Return the plan, and no arrangements.
                (
                    Plan::Join {
                        inputs: plans,
                        plan,
                    },
                    AvailableCollections::new_raw(),
                )
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => {
                let input_arity = input.arity();
                let output_arity = group_key.len() + aggregates.len();
                let (input, keys) = Self::from_mir(input, arrangements, debug_info)?;
                let (input_key, permutation_and_new_arity) = if let Some((
                    input_key,
                    permutation,
                    thinning,
                )) = keys.arbitrary_arrangement()
                {
                    (
                        Some(input_key.clone()),
                        Some((permutation.clone(), thinning.len() + input_key.len())),
                    )
                } else {
                    (None, None)
                };
                let key_val_plan = KeyValPlan::new(
                    input_arity,
                    group_key,
                    aggregates,
                    permutation_and_new_arity,
                );
                let reduce_plan =
                    ReducePlan::create_from(aggregates.clone(), *monotonic, *expected_group_size);
                let output_keys = reduce_plan.keys(group_key.len(), output_arity);
                // Return the plan, and the keys it produces.
                (
                    Plan::Reduce {
                        input: Box::new(input),
                        key_val_plan,
                        plan: reduce_plan,
                        input_key,
                    },
                    output_keys,
                )
            }
            MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
                monotonic,
                expected_group_size,
            } => {
                let arity = input.arity();
                let (input, keys) = Self::from_mir(input, arrangements, debug_info)?;

                let top_k_plan = TopKPlan::create_from(
                    group_key.clone(),
                    order_key.clone(),
                    *offset,
                    *limit,
                    arity,
                    *monotonic,
                    *expected_group_size,
                );

                // We don't have an MFP here -- install an operator to permute the
                // input, if necessary.
                let input = if !keys.raw {
                    input.arrange_by(AvailableCollections::new_raw(), &keys, arity)
                } else {
                    input
                };
                // Return the plan, and no arrangements.
                (
                    Plan::TopK {
                        input: Box::new(input),
                        top_k_plan,
                    },
                    AvailableCollections::new_raw(),
                )
            }
            MirRelationExpr::Negate { input } => {
                let arity = input.arity();
                let (input, keys) = Self::from_mir(input, arrangements, debug_info)?;

                // We don't have an MFP here -- install an operator to permute the
                // input, if necessary.
                let input = if !keys.raw {
                    input.arrange_by(AvailableCollections::new_raw(), &keys, arity)
                } else {
                    input
                };
                // Return the plan, and no arrangements.
                (
                    Plan::Negate {
                        input: Box::new(input),
                    },
                    AvailableCollections::new_raw(),
                )
            }
            MirRelationExpr::Threshold { input } => {
                let arity = input.arity();
                let (input, keys) = Self::from_mir(input, arrangements, debug_info)?;
                // We don't have an MFP here -- install an operator to permute the
                // input, if necessary.
                let input = if !keys.raw {
                    input.arrange_by(AvailableCollections::new_raw(), &keys, arity)
                } else {
                    input
                };
                let (threshold_plan, required_arrangement) = ThresholdPlan::create_from(arity);
                let input = if !keys
                    .arranged
                    .iter()
                    .any(|(key, _, _)| key == &required_arrangement.0)
                {
                    input.arrange_by(
                        AvailableCollections::new_arranged(vec![required_arrangement]),
                        &keys,
                        arity,
                    )
                } else {
                    input
                };

                let output_keys = threshold_plan.keys();
                // Return the plan, and any produced keys.
                (
                    Plan::Threshold {
                        input: Box::new(input),
                        threshold_plan,
                    },
                    output_keys,
                )
            }
            MirRelationExpr::Union { base, inputs } => {
                let arity = base.arity();
                let mut plans_keys = Vec::with_capacity(1 + inputs.len());
                let (plan, keys) = Self::from_mir(base, arrangements, debug_info)?;
                plans_keys.push((plan, keys));
                for input in inputs.iter() {
                    let (plan, keys) = Self::from_mir(input, arrangements, debug_info)?;
                    plans_keys.push((plan, keys));
                }
                let plans = plans_keys
                    .into_iter()
                    .map(|(plan, keys)| {
                        // We don't have an MFP here -- install an operator to permute the
                        // input, if necessary.
                        if !keys.raw {
                            plan.arrange_by(AvailableCollections::new_raw(), &keys, arity)
                        } else {
                            plan
                        }
                    })
                    .collect();
                // Return the plan and no arrangements.
                let plan = Plan::Union { inputs: plans };
                (plan, AvailableCollections::new_raw())
            }
            MirRelationExpr::ArrangeBy { input, keys } => {
                let arity = input.arity();
                let (input, mut input_keys) = Self::from_mir(input, arrangements, debug_info)?;
                // Determine keys that are not present in `input_keys`.
                let new_keys = keys
                    .iter()
                    .filter(|k1| !input_keys.arranged.iter().any(|(k2, _, _)| k1 == &k2))
                    .cloned()
                    .collect::<Vec<_>>();
                if new_keys.is_empty() {
                    (input, input_keys)
                } else {
                    let new_keys = new_keys.iter().cloned().map(|k| {
                        let (permutation, thinning) = permutation_for_arrangement(&k, arity);
                        (k, permutation, thinning)
                    });
                    let (input_key, input_mfp) = if let Some((input_key, permutation, thinning)) =
                        input_keys.arbitrary_arrangement()
                    {
                        let mut mfp = MapFilterProject::new(arity);
                        mfp.permute(permutation.clone(), thinning.len() + input_key.len());
                        (Some(input_key.clone()), mfp)
                    } else {
                        (None, MapFilterProject::new(arity))
                    };
                    input_keys.arranged.extend(new_keys);
                    input_keys.arranged.sort_by(|k1, k2| k1.0.cmp(&k2.0));

                    // Return the plan and extended keys.
                    (
                        Plan::ArrangeBy {
                            input: Box::new(input),
                            forms: input_keys.clone(),
                            input_key,
                            input_mfp,
                        },
                        input_keys,
                    )
                }
            }
        };

        // If the plan stage did not absorb all linear operators, introduce a new stage to implement them.
        if !mfp.is_identity() {
            // Seek out an arrangement key that might be constrained to a literal.
            // TODO: Improve key selection heuristic.
            let key_val = keys
                .arranged
                .iter()
                .filter_map(|(key, permutation, thinning)| {
                    let mut mfp = mfp.clone();
                    mfp.permute(permutation.clone(), thinning.len() + key.len());
                    mfp.literal_constraints(key)
                        .map(|val| (key.clone(), permutation, thinning, val))
                })
                .max_by_key(|(key, _, _, _)| key.len());

            // Input key selection strategy:
            // (1) If we can read a key at a particular value, do so
            // (2) Otherwise, if there is a key that causes the MFP to be the identity, and
            // therefore allows us to avoid discarding the arrangement, use that.
            // (3) Otherwise, if there is _some_ key, use that,
            // (4) Otherwise just read the raw collection.
            let input_key_val = if let Some((key, permutation, thinning, val)) = key_val {
                mfp.permute(permutation.clone(), thinning.len() + key.len());

                Some((key, Some(val)))
            } else if let Some((key, permutation, thinning)) =
                keys.arranged.iter().find(|(key, permutation, thinning)| {
                    let mut mfp = mfp.clone();
                    mfp.permute(permutation.clone(), thinning.len() + key.len());
                    mfp.is_identity()
                })
            {
                mfp.permute(permutation.clone(), thinning.len() + key.len());
                Some((key.clone(), None))
            } else if let Some((key, permutation, thinning)) = keys.arbitrary_arrangement() {
                mfp.permute(permutation.clone(), thinning.len() + key.len());
                Some((key.clone(), None))
            } else {
                None
            };

            if mfp.is_identity() {
                // We have discovered a key
                // whose permutation causes the MFP to actually
                // be the identity! We can keep it around,
                // but without its permutation this time,
                // and with a trivial thinning of the right length.
                let (key, val) = input_key_val.unwrap();
                let (_old_key, old_permutation, old_thinning) = keys
                    .arranged
                    .iter_mut()
                    .find(|(key2, _, _)| key2 == &key)
                    .unwrap();
                *old_permutation = (0..mfp.input_arity).map(|i| (i, i)).collect();
                let old_thinned_arity = old_thinning.len();
                *old_thinning = (0..old_thinned_arity).collect();
                // Get rid of all other forms, as this is now the only one known to be valid.
                // TODO[btv] we can probably save the other arrangements too, if we adjust their permutations.
                // This is not hard to do, but leaving it for a quick follow-up to avoid making the present diff too unwieldy.
                keys.arranged.retain(|(key2, _, _)| key2 == &key);
                keys.raw = false;

                // Creating a Plan::Mfp node is now logically unnecessary, but we
                // should do so anyway when `val` is populated, so that
                // the `key_val` optimization gets applied.
                if val.is_some() {
                    plan = Plan::Mfp {
                        input: Box::new(plan),
                        mfp,
                        input_key_val: Some((key, val)),
                    }
                }
            } else {
                plan = Plan::Mfp {
                    input: Box::new(plan),
                    mfp,
                    input_key_val,
                };
                keys = AvailableCollections::new_raw();
            }
        }

        Ok((plan, keys))
    }

    /// Convert the dataflow description into one that uses render plans.
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
        fields(path.segment = "finalize_dataflow")
    )]
    pub fn finalize_dataflow(
        desc: DataflowDescription<OptimizedMirRelationExpr>,
        enable_monotonic_oneshot_selects: bool,
    ) -> Result<DataflowDescription<Self>, String> {
        // First, we lower the dataflow description from MIR to LIR.
        let mut dataflow = Self::lower_dataflow(desc)?;

        // Subsequently, we perform plan refinements for the dataflow.
        Self::refine_source_mfps(&mut dataflow);

        if enable_monotonic_oneshot_selects {
            Self::refine_single_time_dataflow(&mut dataflow);
        }

        mz_repr::explain::trace_plan(&dataflow);

        Ok(dataflow)
    }

    /// Lowers the dataflow description from MIR to LIR. To this end, the
    /// method collects all available arrangements and based on this information
    /// creates plans for every object to be built for the dataflow.
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
        fields(path.segment ="mir_to_lir")
    )]
    fn lower_dataflow(
        desc: DataflowDescription<OptimizedMirRelationExpr>,
    ) -> Result<DataflowDescription<Self>, String> {
        // Collect available arrangements by identifier.
        let mut arrangements = BTreeMap::new();
        // Sources might provide arranged forms of their data, in the future.
        // Indexes provide arranged forms of their data.
        for (index_desc, r#type, _monotonic) in desc.index_imports.values() {
            let key = index_desc.key.clone();
            // TODO[btv] - We should be told the permutation by
            // `index_desc`, and it should have been generated
            // at the same point the thinning logic was.
            //
            // We should for sure do that soon, but it requires
            // a bit of a refactor, so for now we just
            // _assume_ that they were both generated by `permutation_for_arrangement`,
            // and recover it here.
            let (permutation, thinning) = permutation_for_arrangement(&key, r#type.arity());
            arrangements
                .entry(Id::Global(index_desc.on_id))
                .or_insert_with(AvailableCollections::default)
                .arranged
                .push((key, permutation, thinning));
        }
        for id in desc.source_imports.keys() {
            arrangements
                .entry(Id::Global(*id))
                .or_insert_with(AvailableCollections::new_raw);
        }
        // Build each object in order, registering the arrangements it forms.
        let mut objects_to_build = Vec::with_capacity(desc.objects_to_build.len());
        for build in desc.objects_to_build.into_iter() {
            let (plan, keys) = Self::from_mir(
                &build.plan,
                &mut arrangements,
                LirDebugInfo {
                    debug_name: &desc.debug_name,
                    id: build.id,
                },
            )?;
            arrangements.insert(Id::Global(build.id), keys);
            objects_to_build.push(BuildDesc { id: build.id, plan });
        }

        let dataflow = DataflowDescription {
            source_imports: desc.source_imports,
            index_imports: desc.index_imports,
            objects_to_build,
            index_exports: desc.index_exports,
            sink_exports: desc.sink_exports,
            as_of: desc.as_of,
            until: desc.until,
            debug_name: desc.debug_name,
        };

        mz_repr::explain::trace_plan(&dataflow);

        Ok(dataflow)
    }

    /// Refines the source instance descriptions for sources imported by `dataflow` to
    /// push down common MFP expressions.
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
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

    /// Refines the plans of objects to be built as part of `dataflow` to take advantage
    /// of monotonic operators if the dataflow refers to a single-time, i.e., is for a
    /// one-shot SELECT query.
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
        fields(path.segment = "refine_single_time_dataflow")
    )]
    fn refine_single_time_dataflow(dataflow: &mut DataflowDescription<Self>) {
        // Check if we have a one-shot SELECT query, i.e., a single-time dataflow.
        if !dataflow.is_single_time() {
            return;
        }

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
                Plan::Constant { rows } => match rows {
                    Ok(rows) => {
                        let mut rows_parts = vec![Vec::new(); parts];
                        for (index, row) in rows.into_iter().enumerate() {
                            rows_parts[index % parts].push(row);
                        }
                        rows_parts
                            .into_iter()
                            .map(|rows| Plan::Constant { rows: Ok(rows) })
                            .collect()
                    }
                    Err(err) => {
                        let mut result = vec![
                            Plan::Constant {
                                rows: Ok(Vec::new())
                            };
                            parts
                        ];
                        result[0] = Plan::Constant { rows: Err(err) };
                        result
                    }
                },

                // For all other variants, just replace inputs with appropriately sharded versions.
                // This is surprisingly verbose, but that is all it is doing.
                Plan::Get { id, keys, plan } => vec![Plan::Get { id, keys, plan }; parts],
                Plan::Let { value, body, id } => {
                    let value_parts = value.partition_among(parts);
                    let body_parts = body.partition_among(parts);
                    value_parts
                        .into_iter()
                        .zip(body_parts)
                        .map(|(value, body)| Plan::Let {
                            value: Box::new(value),
                            body: Box::new(body),
                            id,
                        })
                        .collect()
                }
                Plan::LetRec {
                    ids,
                    values,
                    limits,
                    body,
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
                        })
                        .collect()
                }
                Plan::Mfp {
                    input,
                    input_key_val,
                    mfp,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Mfp {
                        input: Box::new(input),
                        mfp: mfp.clone(),
                        input_key_val: input_key_val.clone(),
                    })
                    .collect(),
                Plan::FlatMap {
                    input,
                    input_key,
                    func,
                    exprs,
                    mfp,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::FlatMap {
                        input: Box::new(input),
                        input_key: input_key.clone(),
                        func: func.clone(),
                        exprs: exprs.clone(),
                        mfp: mfp.clone(),
                    })
                    .collect(),
                Plan::Join { inputs, plan } => {
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
                        })
                        .collect()
                }
                Plan::Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Reduce {
                        input: Box::new(input),
                        input_key: input_key.clone(),
                        key_val_plan: key_val_plan.clone(),
                        plan: plan.clone(),
                    })
                    .collect(),
                Plan::TopK { input, top_k_plan } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::TopK {
                        input: Box::new(input),
                        top_k_plan: top_k_plan.clone(),
                    })
                    .collect(),
                Plan::Negate { input } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Negate {
                        input: Box::new(input),
                    })
                    .collect(),
                Plan::Threshold {
                    input,
                    threshold_plan,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::Threshold {
                        input: Box::new(input),
                        threshold_plan: threshold_plan.clone(),
                    })
                    .collect(),
                Plan::Union { inputs } => {
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
                        .map(|inputs| Plan::Union { inputs })
                        .collect()
                }
                Plan::ArrangeBy {
                    input,
                    forms: keys,
                    input_key,
                    input_mfp,
                } => input
                    .partition_among(parts)
                    .into_iter()
                    .map(|input| Plan::ArrangeBy {
                        input: Box::new(input),
                        forms: keys.clone(),
                        input_key: input_key.clone(),
                        input_mfp: input_mfp.clone(),
                    })
                    .collect(),
            }
        }
    }
}

impl<T> CollectionPlan for Plan<T> {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        match self {
            Plan::Constant { rows: _ } => (),
            Plan::Get {
                id,
                keys: _,
                plan: _,
            } => match id {
                Id::Global(id) => {
                    out.insert(*id);
                }
                Id::Local(_) => (),
            },
            Plan::Let { id: _, value, body } => {
                value.depends_on_into(out);
                body.depends_on_into(out);
            }
            Plan::LetRec {
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
            Plan::Join { inputs, plan: _ } | Plan::Union { inputs } => {
                for input in inputs {
                    input.depends_on_into(out);
                }
            }
            Plan::Mfp {
                input,
                mfp: _,
                input_key_val: _,
            }
            | Plan::FlatMap {
                input,
                func: _,
                exprs: _,
                mfp: _,
                input_key: _,
            }
            | Plan::ArrangeBy {
                input,
                forms: _,
                input_key: _,
                input_mfp: _,
            }
            | Plan::Reduce {
                input,
                key_val_plan: _,
                plan: _,
                input_key: _,
            }
            | Plan::TopK {
                input,
                top_k_plan: _,
            }
            | Plan::Negate { input }
            | Plan::Threshold {
                input,
                threshold_plan: _,
            } => {
                input.depends_on_into(out);
            }
        }
    }
}

/// Returns bucket sizes, descending, suitable for hierarchical decomposition of an operator, based
/// on the expected number of rows that will have the same group key.
fn bucketing_of_expected_group_size(expected_group_size: Option<u64>) -> Vec<u64> {
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
        #[test]
        #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
        fn available_collections_protobuf_roundtrip(expect in any::<AvailableCollections>() ) {
            let actual = protobuf_roundtrip::<_, ProtoAvailableCollections>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn get_plan_protobuf_roundtrip(expect in any::<GetPlan>()) {
            let actual = protobuf_roundtrip::<_, ProtoGetPlan>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn plan_protobuf_roundtrip(expect in any::<Plan>()) {
            let actual = protobuf_roundtrip::<_, ProtoPlan>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

    }
}
