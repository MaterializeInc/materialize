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
use mz_repr::{Diff, Row};
use serde::{Deserialize, Serialize};

use crate::plan::join::JoinPlan;
use crate::plan::reduce::{KeyValPlan, ReducePlan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::{AvailableCollections, GetPlan, NodeId, Plan};

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
