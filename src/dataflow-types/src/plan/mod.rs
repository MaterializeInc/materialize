// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An explicit representation of a rendering plan for provided dataflows.

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]
#![warn(missing_debug_implementations)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use mz_ore::soft_panic_or_log;
use mz_repr::proto::newapi::IntoRustIfSome;
use mz_repr::proto::newapi::ProtoType;
use mz_repr::proto::newapi::RustType;
use mz_repr::proto::TryFromProtoError;
use mz_repr::proto::TryIntoIfSome;
use proptest::arbitrary::Arbitrary;
use proptest::prelude::*;
use proptest::strategy::Strategy;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize, Serializer};

use mz_expr::{
    permutation_for_arrangement, CollectionPlan, EvalError, Id, JoinInputMapper, LocalId,
    MapFilterProject, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, TableFunc,
};
use mz_repr::{Datum, Diff, GlobalId, Row};

use self::join::{DeltaJoinPlan, JoinPlan, LinearJoinPlan};
use self::reduce::{KeyValPlan, ReducePlan};
use self::threshold::ThresholdPlan;
use self::top_k::TopKPlan;
use crate::DataflowDescription;

pub mod join;
pub mod reduce;
pub mod threshold;
pub mod top_k;

include!(concat!(env!("OUT_DIR"), "/mz_dataflow_types.plan.rs"));

// This function exists purely to convert the HashMap into a BTreeMap,
// so that the value will be stable, for the benefit of tests
// that print out the physical plan.
fn serialize_arranged<S: Serializer>(
    arranged: &Vec<(Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>)>,
    s: S,
) -> Result<S::Ok, S::Error> {
    let to_serialize = arranged.iter().map(|(key, permutation, thinning)| {
        let permutation = permutation.iter().collect::<BTreeMap<_, _>>();
        (key, permutation, thinning)
    });
    s.collect_seq(to_serialize)
}

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
    #[serde(serialize_with = "serialize_arranged")]
    #[proptest(strategy = "prop::collection::vec(any_arranged_thin(), 0..3)")]
    pub arranged: Vec<(Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>)>,
}

/// A strategy that produces arrangements that are thinner than the default. That is
/// the number of direct children is limited to a maximum of 3.
pub(crate) fn any_arranged_thin(
) -> impl Strategy<Value = (Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>)> {
    (
        prop::collection::vec(MirScalarExpr::arbitrary(), 0..3),
        HashMap::<usize, usize>::arbitrary(),
        Vec::<usize>::arbitrary(),
    )
}

impl From<&AvailableCollections> for ProtoAvailableCollections {
    fn from(x: &AvailableCollections) -> Self {
        Self {
            raw: x.raw,
            arranged: x.arranged.into_proto(),
        }
    }
}

impl TryFrom<ProtoAvailableCollections> for AvailableCollections {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoAvailableCollections) -> Result<Self, Self::Error> {
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
        arranged: Vec<(Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>)>,
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
    ) -> Option<&(Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>)> {
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
impl From<&(Row, u64, i64)> for proto_plan::ProtoRowDiff {
    fn from(x: &(Row, u64, i64)) -> Self {
        proto_plan::ProtoRowDiff {
            row: Some(x.0.into_proto()),
            timestamp: x.1,
            diff: x.2,
        }
    }
}

impl From<&Vec<(Row, u64, i64)>> for proto_plan::ProtoRowDiffVec {
    fn from(x: &Vec<(Row, u64, i64)>) -> Self {
        Self {
            rows: x.iter().map(Into::into).collect(),
        }
    }
}

impl From<&Result<Vec<(Row, mz_repr::Timestamp, i64)>, EvalError>>
    for proto_plan::ProtoPlanConstant
{
    fn from(x: &Result<Vec<(Row, mz_repr::Timestamp, i64)>, EvalError>) -> Self {
        proto_plan::ProtoPlanConstant {
            result: Some(match x {
                Ok(rows) => proto_plan::proto_plan_constant::Result::Rows(rows.into()),
                Err(err) => proto_plan::proto_plan_constant::Result::Err(err.into_proto()),
            }),
        }
    }
}

impl From<&Box<Plan>> for Option<Box<ProtoPlan>> {
    fn from(x: &Box<Plan>) -> Self {
        Some(Into::<ProtoPlan>::into(&**x).into())
    }
}

impl From<&Plan> for ProtoPlan {
    fn from(x: &Plan) -> Self {
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

        Self {
            kind: Some(match x {
                Plan::Constant { rows } => Constant(rows.into()),
                Plan::Get { id, keys, plan } => Get(ProtoPlanGet {
                    id: Some(id.into_proto()),
                    keys: Some(keys.into()),
                    plan: Some(plan.into()),
                }),
                Plan::Let { id, value, body } => Let(ProtoPlanLet {
                    id: Some(id.into_proto()),
                    value: value.into(),
                    body: body.into(),
                }
                .into()),
                Plan::Mfp {
                    input,
                    mfp,
                    input_key_val,
                } => Mfp(ProtoPlanMfp {
                    input: input.into(),
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
                        input: input.into(),
                        func: Some(func.into_proto()),
                        exprs: exprs.into_proto(),
                        mfp: Some(mfp.into_proto()),
                        input_key: input_k_into(input_key.as_ref()),
                    }
                    .into(),
                ),
                Plan::Join { inputs, plan } => Join(ProtoPlanJoin {
                    inputs: inputs.iter().map(Into::into).collect(),
                    plan: Some(plan.into_proto()),
                }),
                Plan::Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                } => Reduce(
                    ProtoPlanReduce {
                        input: input.into(),
                        key_val_plan: Some(key_val_plan.into_proto()),
                        plan: Some(plan.into_proto()),
                        input_key: input_k_into(input_key.as_ref()),
                    }
                    .into(),
                ),
                Plan::TopK { input, top_k_plan } => TopK(
                    ProtoPlanTopK {
                        input: input.into(),
                        top_k_plan: Some(top_k_plan.into_proto()),
                    }
                    .into(),
                ),
                Plan::Negate { input } => Negate(Into::<ProtoPlan>::into(&**input).into()),
                Plan::Threshold {
                    input,
                    threshold_plan,
                } => Threshold(
                    ProtoPlanThreshold {
                        input: input.into(),
                        threshold_plan: Some(threshold_plan.into_proto()),
                    }
                    .into(),
                ),
                Plan::Union { inputs } => Union(ProtoPlanUnion {
                    inputs: inputs.iter().map(Into::into).collect(),
                }),
                Plan::ArrangeBy {
                    input,
                    forms,
                    input_key,
                    input_mfp,
                } => ArrangeBy(
                    ProtoPlanArrangeBy {
                        input: input.into(),
                        forms: Some(forms.into()),
                        input_key: input_k_into(input_key.as_ref()),
                        input_mfp: Some(input_mfp.into_proto()),
                    }
                    .into(),
                ),
            }),
        }
    }
}

impl TryFrom<proto_plan::ProtoRowDiff> for (Row, u64, i64) {
    type Error = TryFromProtoError;

    fn try_from(x: proto_plan::ProtoRowDiff) -> Result<Self, Self::Error> {
        Ok((
            x.row.into_rust_if_some("ProtoRowDiff::row")?,
            x.timestamp,
            x.diff,
        ))
    }
}

impl TryFrom<proto_plan::ProtoRowDiffVec> for Vec<(Row, u64, i64)> {
    type Error = TryFromProtoError;

    fn try_from(x: proto_plan::ProtoRowDiffVec) -> Result<Self, Self::Error> {
        Ok(x.rows
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?)
    }
}

impl TryFrom<Box<ProtoPlan>> for Box<Plan> {
    type Error = TryFromProtoError;

    fn try_from(value: Box<ProtoPlan>) -> Result<Self, Self::Error> {
        Ok(Box::new((*value).try_into()?))
    }
}

impl TryFrom<ProtoPlan> for Plan {
    type Error = TryFromProtoError;

    fn try_from(value: ProtoPlan) -> Result<Self, Self::Error> {
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

        let kind = value
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoPlan::kind"))?;

        Ok(match kind {
            Constant(ProtoPlanConstant { result }) => {
                let result = result
                    .ok_or_else(|| TryFromProtoError::missing_field("ProtoPlanConstant::result"))?;

                Plan::Constant {
                    rows: match result {
                        proto_plan_constant::Result::Rows(rows) => Ok(rows.try_into()?),
                        proto_plan_constant::Result::Err(eval_err) => Err(eval_err.into_rust()?),
                    },
                }
            }
            Get(proto) => Plan::Get {
                id: proto.id.into_rust_if_some("ProtoPlanGet::id")?,
                keys: proto.keys.try_into_if_some("ProtoPlanGet::keys")?,
                plan: proto.plan.try_into_if_some("ProtoPlanGet::plan")?,
            },
            Let(proto) => Plan::Let {
                id: proto.id.into_rust_if_some("ProtoPlanLet::id")?,
                value: proto.value.try_into_if_some("ProtoPlanLet::value")?,
                body: proto.body.try_into_if_some("ProtoPlanLet::body")?,
            },
            Mfp(proto) => Plan::Mfp {
                input: proto.input.try_into_if_some("ProtoPlanMfp::input")?,
                input_key_val: input_kv_try_into(proto.input_key_val)?,
                mfp: proto.mfp.into_rust_if_some("ProtoPlanMfp::mfp")?,
            },
            FlatMap(proto) => Plan::FlatMap {
                input: proto.input.try_into_if_some("")?,
                func: proto.func.into_rust_if_some("")?,
                exprs: proto.exprs.into_rust()?,
                mfp: proto.mfp.into_rust_if_some("")?,
                input_key: input_k_try_into(proto.input_key)?,
            },
            Join(proto) => Plan::Join {
                inputs: proto
                    .inputs
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
                plan: proto.plan.into_rust_if_some("")?,
            },
            Reduce(proto) => Plan::Reduce {
                input: proto.input.try_into_if_some("ProtoPlanReduce::input")?,
                key_val_plan: proto
                    .key_val_plan
                    .into_rust_if_some("ProtoPlanReduce::key_val_plan")?,
                plan: proto.plan.into_rust_if_some("ProtoPlanReduce::plan")?,
                input_key: input_k_try_into(proto.input_key)?,
            },
            TopK(proto) => Plan::TopK {
                input: proto.input.try_into_if_some("ProtoPlanTopK::input")?,
                top_k_plan: proto
                    .top_k_plan
                    .into_rust_if_some("ProtoPlanTopK::top_k_plan")?,
            },
            Negate(proto) => Plan::Negate {
                input: proto.try_into()?,
            },
            Threshold(proto) => Plan::Threshold {
                input: proto.input.try_into_if_some("ProtoPlanThreshold::input")?,
                threshold_plan: proto
                    .threshold_plan
                    .into_rust_if_some("ProtoPlanThreshold::threshold_plan")?,
            },
            Union(proto) => Plan::Union {
                inputs: proto
                    .inputs
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            },
            ArrangeBy(proto) => Plan::ArrangeBy {
                input: proto.input.try_into_if_some("ProtoPlanArrangeBy::input")?,
                forms: proto.forms.try_into_if_some("ProtoPlanArrangeBy::forms")?,
                input_key: input_k_try_into(proto.input_key)?,
                input_mfp: proto
                    .input_mfp
                    .into_rust_if_some("ProtoPlanArrangeBy::input_mfp")?,
            },
        })
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

impl From<&GetPlan> for ProtoGetPlan {
    fn from(x: &GetPlan) -> Self {
        use proto_get_plan::Kind::*;

        ProtoGetPlan {
            kind: Some(match x {
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
}

impl TryFrom<ProtoGetPlan> for GetPlan {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoGetPlan) -> Result<Self, Self::Error> {
        use proto_get_plan::Kind::*;
        use proto_get_plan::ProtoGetPlanArrangement;
        match x.kind {
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

/// Various bits of state to print along with error messages during LIR planning,
/// to aid debugging.
#[derive(Copy, Clone, Debug)]
pub struct LirDebugInfo<'a> {
    debug_name: &'a str,
    id: GlobalId,
    dataflow_uuid: uuid::Uuid,
}

impl<'a> std::fmt::Display for LirDebugInfo<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Debug name: {}; id: {}; dataflow UUID: {}",
            self.debug_name, self.id, self.dataflow_uuid
        )
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

    /// An empty list of arrangement keys indicates that only a `Collection` stream can
    /// be assumed to exist.
    pub fn from_mir(
        expr: &MirRelationExpr,
        arrangements: &mut BTreeMap<Id, AvailableCollections>,
        debug_info: LirDebugInfo<'_>,
    ) -> Result<(Self, AvailableCollections), ()> {
        // This function is recursive and can overflow its stack, so grow it if
        // needed. The growth here is unbounded. Our general solution for this problem
        // is to use [`ore::stack::RecursionGuard`] to additionally limit the stack
        // depth. That however requires upstream error handling. This function is
        // currently called by the Coordinator after calls to `catalog_transact`,
        // and thus are not allowed to fail. Until that allows errors, we choose
        // to allow the unbounded growth here. We are though somewhat protected by
        // higher levels enforcing their own limits on stack depth (in the parser,
        // transformer/desugarer, and planner).
        mz_ore::stack::maybe_grow(|| Plan::from_mir_inner(expr, arrangements, debug_info))
    }

    fn from_mir_inner(
        expr: &MirRelationExpr,
        arrangements: &mut BTreeMap<Id, AvailableCollections>,
        debug_info: LirDebugInfo<'_>,
    ) -> Result<(Self, AvailableCollections), ()> {
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
                    mz_expr::JoinImplementation::Differential((start, _start_arr), order) => {
                        let source_arrangement = input_keys[*start].arbitrary_arrangement();
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
                    mz_expr::JoinImplementation::DeltaQuery(orders) => {
                        let (djp, missing) = DeltaJoinPlan::create_from(
                            equivalences,
                            &orders[..],
                            input_mapper,
                            &mut mfp,
                            &input_keys,
                        );
                        (JoinPlan::Delta(djp), missing)
                    }
                    // Other plans are errors, and should be reported as such.
                    _ => return Err(()),
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
                let (threshold_plan, required_arrangement) =
                    ThresholdPlan::create_from(arity, false);
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
                let keys = keys.iter().cloned().map(|k| {
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
                input_keys.arranged.extend(keys);
                input_keys.arranged.sort_by(|k1, k2| k1.0.cmp(&k2.0));
                input_keys.arranged.dedup_by(|k1, k2| k1.0 == k2.0);

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
    pub fn finalize_dataflow(
        desc: DataflowDescription<OptimizedMirRelationExpr>,
    ) -> Result<DataflowDescription<Self>, ()> {
        // Collect available arrangements by identifier.
        let mut arrangements = BTreeMap::new();
        // Sources might provide arranged forms of their data, in the future.
        // Indexes provide arranged forms of their data.
        for (index_desc, r#type) in desc.index_imports.values() {
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
                    dataflow_uuid: desc.id,
                },
            )?;
            arrangements.insert(Id::Global(build.id), keys);
            objects_to_build.push(crate::BuildDesc { id: build.id, plan });
        }

        Ok(DataflowDescription {
            source_imports: desc.source_imports,
            index_imports: desc.index_imports,
            objects_to_build,
            index_exports: desc.index_exports,
            sink_exports: desc.sink_exports,
            as_of: desc.as_of,
            debug_name: desc.debug_name,
            id: desc.id,
        })
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

/// Helper method to convert linear operators to MapFilterProject instances.
///
/// This method produces a `MapFilterProject` instance that first applies any predicates,
/// and then introduces `Datum::Dummy` literals in columns that are not demanded.
/// The `RelationType` is required so that we can fill in the correct type of `Datum::Dummy`.
pub fn linear_to_mfp(
    linear: crate::LinearOperator,
    typ: &mz_repr::RelationType,
) -> MapFilterProject {
    let crate::types::LinearOperator {
        predicates,
        projection,
    } = linear;

    let arity = typ.arity();
    let mut dummies = Vec::new();
    let mut demand_projection = Vec::new();
    for (column, typ) in typ.column_types.iter().enumerate() {
        if projection.contains(&column) {
            demand_projection.push(column);
        } else {
            demand_projection.push(arity + dummies.len());
            dummies.push(MirScalarExpr::literal_ok(
                Datum::Dummy,
                typ.scalar_type.clone(),
            ));
        }
    }

    // First filter, then introduce and reposition `Datum::Dummy` values.
    MapFilterProject::new(arity)
        .filter(predicates)
        .map(dummies)
        .project(demand_projection)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;

    proptest! {
       #![proptest_config(ProptestConfig::with_cases(10))]
       #[test]
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
