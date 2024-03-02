// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`FlatPlan`], a flat representation of LIR plans used in the compute protocol and rendering,
//! and support for for flatting [`Plan`]s into this representation.

use std::collections::BTreeMap;

use mz_expr::{EvalError, Id, LetRecLimit, LocalId, MapFilterProject, MirScalarExpr, TableFunc};
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, Row};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::plan::join::JoinPlan;
use crate::plan::reduce::{KeyValPlan, ReducePlan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::{AvailableCollections, GetPlan, NodeId, Plan, ProtoLetRecLimit};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_compute_types.plan.flat_plan.rs"
));

/// A flat representation of LIR plans.
///
/// In contrast to [`Plan`], which recursively contains its subplans, this type encodes references
/// between nodes with their unique [`NodeId`]. Doing so has some benefits:
///
///  * Operations that visit all nodes in a [`FlatPlan`] have natural iterative implementations,
///    avoiding the risk of stack overflows.
///  * Because nodes can be referenced multiple times, a [`FlatPlan`] can represent DAGs, not just
///    trees. This better matches the structure of rendered dataflows and avoids the need for a
///    `Let` construct.
///
/// Note that the `LetRec` operator is still present in [`FlatPlan`]s. While we could replace it
/// with cyclic references between nodes, keeping it simplifies rendering as the sources of
/// recursion are clearly marked.
///
/// A [`FlatPlan`] can be constructed from a [`Plan`] using the corresponding [`From`] impl.
///
/// # Invariants
///
/// A [`FlatPlan`] maintains the following internal invariants:
///
/// Validity of node references:
///
///  (1) The `root` ID is contained in the `nodes` map.
///  (2) For each node in the `nodes` map, each [`NodeId`] contained within the node is contained
///      in the `nodes` map.
///
/// Constrained recursive structure:
///
///  (3) `LetRec` nodes only occur at the root or as inputs of other `LetRec` nodes.
///  (4) Only `LetRec` nodes may introduce cycles into the plan.
///  (5) Each input to a `LetRec` node is the root of an independent subplan.
///
/// The implementation of [`FlatPlan`] must ensure that all its methods uphold these invariants and
/// that users are not able to break them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlatPlan<T = mz_repr::Timestamp> {
    /// The nodes in the plan.
    nodes: BTreeMap<NodeId, FlatPlanNode<T>>,
    /// The ID of the root node.
    root: NodeId,
}

/// A node in a [`FlatPlan`].
///
/// Variants mostly match the ones of [`Plan`], except:
///
///  * Recursive references are replaced with [`NodeId`]s.
///  * The `node_id` fields are removed.
///  * The `Let` variant is removed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FlatPlanNode<T = mz_repr::Timestamp> {
    /// A collection containing a pre-determined collection.
    Constant {
        /// Explicit update triples for the collection.
        rows: Result<Vec<(Row, T, Diff)>, EvalError>,
    },
    /// A reference to a bound collection.
    ///
    /// This is commonly either an external reference to an existing source or maintained
    /// arrangement, or an internal reference to a `LetRec` identifier.
    Get {
        /// A global or local identifier naming the collection.
        id: Id,
        /// Arrangements that will be available.
        ///
        /// The collection will also be loaded if available, which it will not be for imported
        /// data, but which it may be for locally defined data.
        keys: AvailableCollections,
        /// The actions to take when introducing the collection.
        plan: GetPlan,
    },
    /// Binds `values` to `ids`, evaluates them potentially recursively, and returns `body`.
    ///
    /// All bindings are available to all bindings, and to `body`. The contents of each binding are
    /// initially empty, and then updated through a sequence of iterations in which each binding is
    /// updated in sequence, from the most recent values of all bindings.
    LetRec {
        /// The local identifiers to be used, available to `body` as `Id::Local(id)`.
        ids: Vec<LocalId>,
        /// The collections that should be bound to `ids`.
        values: Vec<NodeId>,
        /// Maximum number of iterations. See further info on the MIR `LetRec`.
        limits: Vec<Option<LetRecLimit>>,
        /// The collection that results, which is allowed to contain `Get` stages
        /// that reference `Id::Local(id)`.
        body: NodeId,
    },
    /// Map, Filter, and Project operators.
    ///
    /// This stage contains work that we would ideally like to fuse to other plan stages, but for
    /// practical reasons cannot. For example: threshold, topk, and sometimes reduce stages are not
    /// able to absorb this operator.
    Mfp {
        /// The input collection.
        input: NodeId,
        /// Linear operator to apply to each record.
        mfp: MapFilterProject,
        /// Whether the input is from an arrangement, and if so, whether we can seek to a specific
        /// value therein.
        input_key_val: Option<(Vec<MirScalarExpr>, Option<Row>)>,
    },
    /// A variable number of output records for each input record.
    ///
    /// This stage is a bit of a catch-all for logic that does not easily fit in map stages. This
    /// includes table valued functions, but also functions of multiple arguments, and functions
    /// that modify the sign of updates.
    ///
    /// This stage allows a `MapFilterProject` operator to be fused to its output, and this can be
    /// very important as otherwise the output of `func` is just appended to the input record, for
    /// as many outputs as it has. This has the unpleasant default behavior of repeating
    /// potentially large records that are being unpacked, producing quadratic output in those
    /// cases. Instead, in these cases use a `mfp` member that projects away these large fields.
    FlatMap {
        /// The input collection.
        input: NodeId,
        /// The variable-record emitting function.
        func: TableFunc,
        /// Expressions that for each row prepare the arguments to `func`.
        exprs: Vec<MirScalarExpr>,
        /// Linear operator to apply to each record produced by `func`.
        mfp_after: MapFilterProject,
        /// The particular arrangement of the input we expect to use, if any.
        input_key: Option<Vec<MirScalarExpr>>,
    },
    /// A multiway relational equijoin, with fused map, filter, and projection.
    ///
    /// This stage performs a multiway join among `inputs`, using the equality constraints
    /// expressed in `plan`. The plan also describes the implementation strategy we will use, and
    /// any pushed down per-record work.
    Join {
        /// An ordered list of inputs that will be joined.
        inputs: Vec<NodeId>,
        /// Detailed information about the implementation of the join.
        ///
        /// This includes information about the implementation strategy, but also any map, filter,
        /// project work that we might follow the join with, but potentially pushed down into the
        /// implementation of the join.
        plan: JoinPlan,
    },
    /// Aggregation by key.
    Reduce {
        /// The input collection.
        input: NodeId,
        /// A plan for changing input records into key, value pairs.
        key_val_plan: KeyValPlan,
        /// A plan for performing the reduce.
        ///
        /// The implementation of reduction has several different strategies based on the
        /// properties of the reduction, and the input itself. Please check out the documentation
        /// for this type for more detail.
        plan: ReducePlan,
        /// The particular arrangement of the input we expect to use, if any.
        input_key: Option<Vec<MirScalarExpr>>,
        /// An MFP that must be applied to results. The projection part of this MFP must preserve
        /// the key for the reduction; otherwise, the results become undefined. Additionally, the
        /// MFP must be free from temporal predicates so that it can be readily evaluated.
        mfp_after: MapFilterProject,
    },
    /// Key-based "Top K" operator, retaining the first K records in each group.
    TopK {
        /// The input collection.
        input: NodeId,
        /// A plan for performing the Top-K.
        ///
        /// The implementation of Top-K has several different strategies based on the properties of
        /// the Top-K, and the input itself. Please check out the documentation for this type for
        /// more detail.
        top_k_plan: TopKPlan,
    },
    /// Inverts the sign of each update.
    Negate {
        /// The input collection.
        input: NodeId,
    },
    /// Filters records that accumulate negatively.
    ///
    /// Although the operator suppresses updates, it is a stateful operator taking resources
    /// proportional to the number of records with non-zero accumulation.
    Threshold {
        /// The input collection.
        input: NodeId,
        /// A plan for performing the threshold.
        ///
        /// The implementation of threshold has several different strategies based on the
        /// properties of the threshold, and the input itself. Please check out the documentation
        /// for this type for more detail.
        threshold_plan: ThresholdPlan,
    },
    /// Adds the contents of the input collections.
    ///
    /// Importantly, this is *multiset* union, so the multiplicities of records will add. This is
    /// in contrast to *set* union, where the multiplicities would be capped at one. A set union
    /// can be formed with `Union` followed by `Reduce` implementing the "distinct" operator.
    Union {
        /// The input collections.
        inputs: Vec<NodeId>,
        /// Whether to consolidate the output, e.g., cancel negated records.
        consolidate_output: bool,
    },
    /// The `input` plan, but with additional arrangements.
    ///
    /// This operator does not change the logical contents of `input`, but ensures that certain
    /// arrangements are available in the results. This operator can be important for e.g. the
    /// `Join` stage which benefits from multiple arrangements or to cap a `Plan` so that indexes
    /// can be exported.
    ArrangeBy {
        /// The input collection.
        input: NodeId,
        /// A list of arrangement keys, and possibly a raw collection, that will be added to those
        /// of the input.
        ///
        /// If any of these collection forms are already present in the input, they have no effect.
        forms: AvailableCollections,
        /// The key that must be used to access the input.
        input_key: Option<Vec<MirScalarExpr>>,
        /// The MFP that must be applied to the input.
        input_mfp: MapFilterProject,
    },
}

impl<T> From<Plan<T>> for FlatPlan<T> {
    /// Flatten the given [`Plan`] into a [`FlatPlan`].
    fn from(plan: Plan<T>) -> Self {
        use FlatPlanNode::*;

        // The strategy is to walk walk through the `Plan` in right-to-left pre-order and for each
        // node (a) produce a corresponding `FlatPlanNode` and (b) push the contained subplans onto
        // the work stack. We do this until no further subplans remain.
        //
        // Special cases exist for `Let`s and `Get`s: We want to always remove the former and
        // remove the latter if it references a `Let` node. For example:
        //
        //         +-------+                             +-------+
        //         | Root  |                             | Root  |
        //         +-------+                             +-------+
        //             |                                     |
        //         +-------+                             +-------+
        //         |  Let  |                             | Body  |
        //         +-------+                             +-------+
        //           /   \                 ===>            /   \
        //       /---     ---\                            /     \
        //  +-------+     +-------+                       \     /
        //  | Value |     | Body  |                      +-------+
        //  +-------+     +-------+                      | Value |
        //                  /   \                        +-------+
        //               /--     --\
        //          +-------+   +-------+
        //          |  Get  |   |  Get  |
        //          +-------+   +-------+
        //
        // For this, whenever we encounter a `Let`, we insert a insert a binding for Value, instead
        // of producing a new `FlatPlanNode`. We then flatten Value and Body as usual.
        //
        // When we encounter a `Get` that references a `LocalId`, and we find the `LocalId` in the
        // current bindings, we replace the `Get` with an optional `Mfp` that references the
        // binding value instead.

        let mut root = plan.node_id();

        // Stack of nodes to flatten.
        let mut todo: Vec<Plan<T>> = vec![plan];
        // Flat nodes produced so far.
        let mut nodes: BTreeMap<u64, FlatPlanNode<T>> = Default::default();
        // Bindings resulting from `Plan::Let` nodes.
        let mut bindings: BTreeMap<LocalId, NodeId> = Default::default();
        // Node references to replace at the end.
        let mut replacements: BTreeMap<NodeId, NodeId> = Default::default();

        while let Some(plan) = todo.pop() {
            match plan {
                Plan::Constant { rows, node_id } => {
                    let node = Constant { rows };
                    nodes.insert(node_id, node);
                }
                Plan::Get {
                    id,
                    keys,
                    plan,
                    node_id,
                } => {
                    // If this `Get` references another node directly, remove it. If it contains an
                    // MFP, leave an `Mfp` node in its place.
                    if let Id::Local(local_id) = id {
                        if let Some(input_id) = bindings.get(&local_id) {
                            match plan {
                                GetPlan::PassArrangements => {
                                    replacements.insert(node_id, *input_id);
                                }
                                GetPlan::Arrangement(keys, row, mfp) => {
                                    let node = Mfp {
                                        input: *input_id,
                                        mfp,
                                        input_key_val: Some((keys, row)),
                                    };
                                    nodes.insert(node_id, node);
                                }
                                GetPlan::Collection(mfp) => {
                                    let node = Mfp {
                                        input: *input_id,
                                        mfp,
                                        input_key_val: None,
                                    };
                                    nodes.insert(node_id, node);
                                }
                            }
                            continue;
                        }
                    }

                    let node = Get { id, keys, plan };
                    nodes.insert(node_id, node);
                }
                Plan::Let {
                    id,
                    value,
                    body,
                    node_id,
                } => {
                    // Insert a binding for the value and replace the `Let` node with the body.
                    bindings.insert(id, value.node_id());
                    replacements.insert(node_id, body.node_id());

                    todo.extend([*value, *body]);
                }
                Plan::LetRec {
                    ids,
                    values,
                    limits,
                    body,
                    node_id,
                } => {
                    let node = LetRec {
                        ids,
                        values: values.iter().map(|v| v.node_id()).collect(),
                        limits,
                        body: body.node_id(),
                    };
                    nodes.insert(node_id, node);

                    todo.extend(values);
                    todo.push(*body);
                }
                Plan::Mfp {
                    input,
                    mfp,
                    input_key_val,
                    node_id,
                } => {
                    let node = Mfp {
                        input: input.node_id(),
                        mfp,
                        input_key_val,
                    };
                    nodes.insert(node_id, node);

                    todo.push(*input);
                }
                Plan::FlatMap {
                    input,
                    func,
                    exprs,
                    mfp_after,
                    input_key,
                    node_id,
                } => {
                    let node = FlatMap {
                        input: input.node_id(),
                        func,
                        exprs,
                        mfp_after,
                        input_key,
                    };
                    nodes.insert(node_id, node);

                    todo.push(*input);
                }
                Plan::Join {
                    inputs,
                    plan,
                    node_id,
                } => {
                    let node = Join {
                        inputs: inputs.iter().map(|i| i.node_id()).collect(),
                        plan,
                    };
                    nodes.insert(node_id, node);

                    todo.extend(inputs.into_iter());
                }
                Plan::Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                    mfp_after,
                    node_id,
                } => {
                    let node = Reduce {
                        input: input.node_id(),
                        key_val_plan,
                        plan,
                        input_key,
                        mfp_after,
                    };
                    nodes.insert(node_id, node);

                    todo.push(*input);
                }
                Plan::TopK {
                    input,
                    top_k_plan,
                    node_id,
                } => {
                    let node = TopK {
                        input: input.node_id(),
                        top_k_plan,
                    };
                    nodes.insert(node_id, node);

                    todo.push(*input);
                }
                Plan::Negate { input, node_id } => {
                    let node = Negate {
                        input: input.node_id(),
                    };
                    nodes.insert(node_id, node);

                    todo.push(*input);
                }
                Plan::Threshold {
                    input,
                    threshold_plan,
                    node_id,
                } => {
                    let node = Threshold {
                        input: input.node_id(),
                        threshold_plan,
                    };
                    nodes.insert(node_id, node);

                    todo.push(*input);
                }
                Plan::Union {
                    inputs,
                    consolidate_output,
                    node_id,
                } => {
                    let node = Union {
                        inputs: inputs.iter().map(|i| i.node_id()).collect(),
                        consolidate_output,
                    };
                    nodes.insert(node_id, node);

                    todo.extend(inputs.into_iter());
                }
                Plan::ArrangeBy {
                    input,
                    forms,
                    input_key,
                    input_mfp,
                    node_id,
                } => {
                    let node = ArrangeBy {
                        input: input.node_id(),
                        forms,
                        input_key,
                        input_mfp,
                    };
                    nodes.insert(node_id, node);

                    todo.push(*input);
                }
            };
        }

        // Apply replacements.
        let replace = |id: &mut NodeId| {
            // The replacement targets might have been replaced themselves, so iterate.
            while let Some(target_id) = replacements.get(id) {
                *id = *target_id;
            }
        };

        for node in nodes.values_mut() {
            for id in node.input_node_ids_mut() {
                replace(id);
            }
        }
        replace(&mut root);

        Self { nodes, root }
    }
}

impl<T> FlatPlanNode<T> {
    /// Returns mutable references to the IDs of input nodes to this node.
    fn input_node_ids_mut(&mut self) -> impl Iterator<Item = &mut NodeId> {
        use FlatPlanNode::*;

        let mut list = None;
        let mut last = None;

        match self {
            Constant { .. } | Get { .. } => (),
            LetRec { values, body, .. } => {
                list = Some(values);
                last = Some(body);
            }
            Mfp { input, .. }
            | FlatMap { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input, .. }
            | Threshold { input, .. }
            | ArrangeBy { input, .. } => {
                last = Some(input);
            }
            Join { inputs, .. } | Union { inputs, .. } => {
                list = Some(inputs);
            }
        }

        list.into_iter().flatten().chain(last)
    }
}

impl Arbitrary for FlatPlan {
    type Strategy = BoxedStrategy<FlatPlan>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any::<Plan>().prop_map(FlatPlan::from).boxed()
    }
}

impl RustType<ProtoFlatPlan> for FlatPlan {
    fn into_proto(&self) -> ProtoFlatPlan {
        ProtoFlatPlan {
            nodes: self.nodes.into_proto(),
            root: self.root,
        }
    }

    fn from_proto(proto: ProtoFlatPlan) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(Self {
            nodes: proto.nodes.into_rust()?,
            root: proto.root,
        })
    }
}

impl ProtoMapEntry<NodeId, FlatPlanNode> for proto_flat_plan::ProtoNode {
    fn from_rust(entry: (&NodeId, &FlatPlanNode)) -> Self {
        Self {
            id: *entry.0,
            node: Some(entry.1.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(NodeId, FlatPlanNode), TryFromProtoError> {
        Ok((self.id, self.node.into_rust_if_some("ProtoNode::node")?))
    }
}

impl RustType<ProtoFlatPlanNode> for FlatPlanNode {
    fn into_proto(&self) -> ProtoFlatPlanNode {
        use proto_flat_plan_node::*;

        fn input_kv_into(
            input_key_val: &Option<(Vec<MirScalarExpr>, Option<Row>)>,
        ) -> Option<ProtoInputKeyVal> {
            input_key_val.as_ref().map(|(key, val)| ProtoInputKeyVal {
                key: key.into_proto(),
                val: val.into_proto(),
            })
        }

        fn input_k_into(input_key: &Option<Vec<MirScalarExpr>>) -> Option<ProtoInputKey> {
            input_key.as_ref().map(|vec| ProtoInputKey {
                key: vec.into_proto(),
            })
        }

        let kind = match self {
            Self::Constant { rows } => Kind::Constant(ProtoConstant {
                rows: Some(rows.into_proto()),
            }),
            Self::Get { id, keys, plan } => Kind::Get(ProtoGet {
                id: Some(id.into_proto()),
                keys: Some(keys.into_proto()),
                plan: Some(plan.into_proto()),
            }),
            Self::LetRec {
                ids,
                values,
                limits,
                body,
            } => {
                let mut proto_limits = Vec::with_capacity(limits.len());
                let mut proto_limit_is_some = Vec::with_capacity(limits.len());
                for limit in limits {
                    match limit {
                        Some(limit) => {
                            proto_limits.push(limit.into_proto());
                            proto_limit_is_some.push(true);
                        }
                        None => {
                            // The actual value doesn't matter here, because the limit_is_some
                            // field will be false, so we won't read this value when converting
                            // back.
                            proto_limits.push(ProtoLetRecLimit {
                                max_iters: 1,
                                return_at_limit: false,
                            });
                            proto_limit_is_some.push(false);
                        }
                    }
                }

                Kind::LetRec(ProtoLetRec {
                    ids: ids.into_proto(),
                    values: values.into_proto(),
                    limits: proto_limits,
                    limit_is_some: proto_limit_is_some,
                    body: *body,
                })
            }
            Self::Mfp {
                input,
                mfp,
                input_key_val,
            } => Kind::Mfp(ProtoMfp {
                input: *input,
                mfp: Some(mfp.into_proto()),
                input_key_val: input_kv_into(input_key_val),
            }),
            Self::FlatMap {
                input,
                func,
                exprs,
                mfp_after,
                input_key,
            } => Kind::FlatMap(ProtoFlatMap {
                input: *input,
                func: Some(func.into_proto()),
                exprs: exprs.into_proto(),
                mfp_after: Some(mfp_after.into_proto()),
                input_key: input_k_into(input_key),
            }),
            Self::Join { inputs, plan } => Kind::Join(ProtoJoin {
                inputs: inputs.into_proto(),
                plan: Some(plan.into_proto()),
            }),
            Self::Reduce {
                input,
                key_val_plan,
                plan,
                input_key,
                mfp_after,
            } => Kind::Reduce(ProtoReduce {
                input: *input,
                key_val_plan: Some(key_val_plan.into_proto()),
                plan: Some(plan.into_proto()),
                input_key: input_k_into(input_key),
                mfp_after: Some(mfp_after.into_proto()),
            }),
            Self::TopK { input, top_k_plan } => Kind::TopK(ProtoTopK {
                input: *input,
                top_k_plan: Some(top_k_plan.into_proto()),
            }),
            Self::Negate { input } => Kind::Negate(ProtoNegate { input: *input }),
            Self::Threshold {
                input,
                threshold_plan,
            } => Kind::Threshold(ProtoThreshold {
                input: *input,
                threshold_plan: Some(threshold_plan.into_proto()),
            }),
            Self::Union {
                inputs,
                consolidate_output,
            } => Kind::Union(ProtoUnion {
                inputs: inputs.into_proto(),
                consolidate_output: *consolidate_output,
            }),
            Self::ArrangeBy {
                input,
                forms,
                input_key,
                input_mfp,
            } => Kind::ArrangeBy(ProtoArrangeBy {
                input: *input,
                forms: Some(forms.into_proto()),
                input_key: input_k_into(input_key),
                input_mfp: Some(input_mfp.into_proto()),
            }),
        };

        ProtoFlatPlanNode { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoFlatPlanNode) -> Result<Self, TryFromProtoError> {
        use proto_flat_plan_node::*;

        fn input_kv_try_into(
            input_key_val: Option<ProtoInputKeyVal>,
        ) -> Result<Option<(Vec<MirScalarExpr>, Option<Row>)>, TryFromProtoError> {
            let option = match input_key_val {
                Some(kv) => Some((kv.key.into_rust()?, kv.val.into_rust()?)),
                None => None,
            };
            Ok(option)
        }

        fn input_k_try_into(
            input_key: Option<ProtoInputKey>,
        ) -> Result<Option<Vec<MirScalarExpr>>, TryFromProtoError> {
            let option = match input_key {
                Some(k) => Some(k.key.into_rust()?),
                None => None,
            };
            Ok(option)
        }

        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoPlan::kind"))?;

        let result = match kind {
            Kind::Constant(proto) => Self::Constant {
                rows: proto.rows.into_rust_if_some("ProtoConstant::rows")?,
            },
            Kind::Get(proto) => Self::Get {
                id: proto.id.into_rust_if_some("ProtoGet::id")?,
                keys: proto.keys.into_rust_if_some("ProtoGet::keys")?,
                plan: proto.plan.into_rust_if_some("ProtoGet::plan")?,
            },
            Kind::LetRec(proto) => {
                let mut limits = Vec::with_capacity(proto.limits.len());
                for (limit, is_some) in proto.limits.into_iter().zip(proto.limit_is_some) {
                    let limit = match is_some {
                        true => Some(limit.into_rust()?),
                        false => None,
                    };
                    limits.push(limit);
                }

                Self::LetRec {
                    ids: proto.ids.into_rust()?,
                    values: proto.values.into_rust()?,
                    limits,
                    body: proto.body,
                }
            }
            Kind::Mfp(proto) => Self::Mfp {
                input: proto.input,
                mfp: proto.mfp.into_rust_if_some("ProtoMfp::mfp")?,
                input_key_val: input_kv_try_into(proto.input_key_val)?,
            },
            Kind::FlatMap(proto) => Self::FlatMap {
                input: proto.input,
                func: proto.func.into_rust_if_some("ProtoFlatMap::func")?,
                exprs: proto.exprs.into_rust()?,
                mfp_after: proto
                    .mfp_after
                    .into_rust_if_some("ProtoFlatMap::mfp_after")?,
                input_key: input_k_try_into(proto.input_key)?,
            },
            Kind::Join(proto) => Self::Join {
                inputs: proto.inputs.into_rust()?,
                plan: proto.plan.into_rust_if_some("ProtoJoin::plan")?,
            },
            Kind::Reduce(proto) => Self::Reduce {
                input: proto.input,
                key_val_plan: proto
                    .key_val_plan
                    .into_rust_if_some("ProtoReduce::key_val_plan")?,
                plan: proto.plan.into_rust_if_some("ProtoReduce::plan")?,
                input_key: input_k_try_into(proto.input_key)?,
                mfp_after: proto.mfp_after.into_rust_if_some("Proto::mfp_after")?,
            },
            Kind::TopK(proto) => Self::TopK {
                input: proto.input,
                top_k_plan: proto
                    .top_k_plan
                    .into_rust_if_some("ProtoTopK::top_k_plan")?,
            },
            Kind::Negate(proto) => Self::Negate { input: proto.input },
            Kind::Threshold(proto) => Self::Threshold {
                input: proto.input,
                threshold_plan: proto
                    .threshold_plan
                    .into_rust_if_some("ProtoThreshold::threshold_plan")?,
            },
            Kind::Union(proto) => Self::Union {
                inputs: proto.inputs.into_rust()?,
                consolidate_output: proto.consolidate_output,
            },
            Kind::ArrangeBy(proto) => Self::ArrangeBy {
                input: proto.input,
                forms: proto.forms.into_rust_if_some("ProtoArrangeBy::forms")?,
                input_key: input_k_try_into(proto.input_key)?,
                input_mfp: proto
                    .input_mfp
                    .into_rust_if_some("ProtoArrangeBy::input_mfp")?,
            },
        };

        Ok(result)
    }
}

impl RustType<proto_flat_plan_node::ProtoConstantRows>
    for Result<Vec<(Row, mz_repr::Timestamp, i64)>, EvalError>
{
    fn into_proto(&self) -> proto_flat_plan_node::ProtoConstantRows {
        use proto_flat_plan_node::proto_constant_rows::Result;

        proto_flat_plan_node::ProtoConstantRows {
            result: Some(match self {
                Ok(ok) => Result::Ok(ok.into_proto()),
                Err(err) => Result::Err(err.into_proto()),
            }),
        }
    }

    fn from_proto(
        proto: proto_flat_plan_node::ProtoConstantRows,
    ) -> Result<Self, TryFromProtoError> {
        use proto_flat_plan_node::proto_constant_rows::Result;

        match proto.result {
            Some(Result::Ok(ok)) => Ok(Ok(ok.into_rust()?)),
            Some(Result::Err(err)) => Ok(Err(err.into_rust()?)),
            None => Err(TryFromProtoError::missing_field(
                "ProtoConstantRows::result",
            )),
        }
    }
}

impl RustType<proto_flat_plan_node::ProtoUpdateVec> for Vec<(Row, mz_repr::Timestamp, i64)> {
    fn into_proto(&self) -> proto_flat_plan_node::ProtoUpdateVec {
        proto_flat_plan_node::ProtoUpdateVec {
            rows: self.into_proto(),
        }
    }

    fn from_proto(proto: proto_flat_plan_node::ProtoUpdateVec) -> Result<Self, TryFromProtoError> {
        proto.rows.into_rust()
    }
}

impl RustType<proto_flat_plan_node::ProtoUpdate> for (Row, mz_repr::Timestamp, i64) {
    fn into_proto(&self) -> proto_flat_plan_node::ProtoUpdate {
        proto_flat_plan_node::ProtoUpdate {
            row: Some(self.0.into_proto()),
            timestamp: self.1.into(),
            diff: self.2,
        }
    }

    fn from_proto(proto: proto_flat_plan_node::ProtoUpdate) -> Result<Self, TryFromProtoError> {
        Ok((
            proto.row.into_rust_if_some("ProtoUpdate::row")?,
            proto.timestamp.into(),
            proto.diff,
        ))
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[mz_ore::test]
        fn flat_plan_protobuf_roundtrip(expect in any::<FlatPlan>()) {
            let actual = protobuf_roundtrip::<_, ProtoFlatPlan>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

    }
}
