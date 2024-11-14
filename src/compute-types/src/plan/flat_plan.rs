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

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::explain::{HumanizedExplain, HumanizerMode};
use mz_expr::{
    CollectionPlan, EvalError, Id, LetRecLimit, LocalId, MapFilterProject, MirScalarExpr, TableFunc,
};
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::explain::ExprHumanizer;
use mz_repr::{Diff, GlobalId, Row};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::plan::join::{DeltaJoinPlan, JoinPlan, LinearJoinPlan};
use crate::plan::reduce::{BucketedPlan, HierarchicalPlan, KeyValPlan, ReducePlan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::{MonotonicTopKPlan, TopKPlan};
use crate::plan::{AvailableCollections, GetPlan, LirId, Plan, PlanNode, ProtoLetRecLimit};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_compute_types.plan.flat_plan.rs"
));

/// A flat representation of LIR plans.
///
/// In contrast to [`Plan`], which recursively contains its subplans, this type encodes references
/// between nodes with their unique [`LirId`]. Doing so has the benefit that operations that visit
/// all nodes in a [`FlatPlan`] have natural iterative implementations, avoiding the risk of stack
/// overflows.
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
///  (2) For each node in the `nodes` map, each [`LirId`] contained within the node is contained
///      in the `nodes` map.
///  (3) Each [`LirId`] contained within `topological_order` is contained in the `nodes` map.
///
/// Constrained graph structure:
///
///  (4) `LetRec` nodes only occur at the root or as inputs of other `LetRec` nodes.
///  (5) Only `LetRec` nodes may introduce cycles into the plan.
///  (6) Each input to a `LetRec` node is the root of an independent subplan.
///  (7) `Let` nodes only occur at the root, as inputs of `LetRec` nodes, or as `body` inputs of
///      other `Let` nodes.
///  (8) Each input to a `Let` node is the root of an independent subplan.
///
/// Topological order:
///
///  (9) `topological_order` lists the nodes in a valid topological order.
///
/// The implementation of [`FlatPlan`] must ensure that all its methods uphold these invariants and
/// that users are not able to break them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlatPlan<T = mz_repr::Timestamp> {
    /// The nodes in the plan.
    steps: BTreeMap<LirId, FlatPlanStep<T>>,
    /// The ID of the root node.
    root: LirId,
    /// The topological order of nodes (dependencies before dependants).
    topological_order: Vec<LirId>,
}

/// A step of a `FlatPlan`, comprising a `FlatPlanNode` and some metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlatPlanStep<T = mz_repr::Timestamp> {
    /// The AST node for this step of the `FlatPlan`.
    pub node: FlatPlanNode<T>,
    /// The LirId of the parent of this node (for tree reconstruction).
    pub parent: Option<LirId>,
    /// The nesting level of each node (for pretty printing).
    pub nesting: u8,
}

/// A node in a [`FlatPlan`].
///
/// Variants mostly match the ones of [`Plan`], except:
///
///  * Recursive references are replaced with [`LirId`]s.
///  * The `lir_id` fields are removed.
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
        value: LirId,
        /// The collection that results, which is allowed to contain `Get` stages
        /// that reference `Id::Local(id)`.
        body: LirId,
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
        values: Vec<LirId>,
        /// Maximum number of iterations. See further info on the MIR `LetRec`.
        limits: Vec<Option<LetRecLimit>>,
        /// The collection that results, which is allowed to contain `Get` stages
        /// that reference `Id::Local(id)`.
        body: LirId,
    },
    /// Map, Filter, and Project operators.
    ///
    /// This stage contains work that we would ideally like to fuse to other plan stages, but for
    /// practical reasons cannot. For example: threshold, topk, and sometimes reduce stages are not
    /// able to absorb this operator.
    Mfp {
        /// The input collection.
        input: LirId,
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
        input: LirId,
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
        inputs: Vec<LirId>,
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
        input: LirId,
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
        input: LirId,
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
        input: LirId,
    },
    /// Filters records that accumulate negatively.
    ///
    /// Although the operator suppresses updates, it is a stateful operator taking resources
    /// proportional to the number of records with non-zero accumulation.
    Threshold {
        /// The input collection.
        input: LirId,
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
        inputs: Vec<LirId>,
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
        input: LirId,
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
    ///
    /// The ids in `FlatPlan.nodes` are the same as the original LirIds.
    fn from(plan: Plan<T>) -> Self {
        use FlatPlanNode::*;

        // The strategy is to walk walk through the `Plan` in right-to-left pre-order and for each
        // node (a) produce a corresponding `FlatPlanNode` and (b) push the contained subplans onto
        // the work stack. We do this until no further subplans remain.

        let root = plan.lir_id;

        // Stack of nodes to flatten, with their parent id and nesting.
        let mut todo: Vec<(Plan<T>, Option<LirId>, u8)> = vec![(plan, None, 0)];
        // Flat nodes produced so far.
        let mut steps: BTreeMap<LirId, FlatPlanStep<T>> = Default::default();
        // A list remembering the order in which nodes were flattened.
        // Because nodes are flatten in right-to-left pre-order, reversing this list at the end
        // yields a valid topological order.
        let mut flatten_order: Vec<LirId> = Default::default();

        let mut insert_node = |id, parent, node, nesting| {
            steps.insert(
                id,
                FlatPlanStep {
                    node,
                    parent,
                    nesting,
                },
            );
            flatten_order.push(id);
        };

        while let Some((Plan { node, lir_id }, parent, nesting)) = todo.pop() {
            match node {
                PlanNode::Constant { rows } => {
                    let node = Constant { rows };
                    insert_node(lir_id, parent, node, nesting);
                }
                PlanNode::Get { id, keys, plan } => {
                    let node = Get { id, keys, plan };
                    insert_node(lir_id, parent, node, nesting);
                }
                PlanNode::Let { id, value, body } => {
                    let node = Let {
                        id,
                        value: value.lir_id,
                        body: body.lir_id,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.extend([
                        (*value, Some(lir_id), nesting.saturating_add(1)),
                        (*body, Some(lir_id), nesting),
                    ]);
                }
                PlanNode::LetRec {
                    ids,
                    values,
                    limits,
                    body,
                } => {
                    let node = LetRec {
                        ids,
                        values: values.iter().map(|v| v.lir_id).collect(),
                        limits,
                        body: body.lir_id,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.extend(
                        values
                            .into_iter()
                            .map(|plan| (plan, Some(lir_id), nesting.saturating_add(1))),
                    );
                    todo.push((*body, Some(lir_id), nesting));
                }
                PlanNode::Mfp {
                    input,
                    mfp,
                    input_key_val,
                } => {
                    let node = Mfp {
                        input: input.lir_id,
                        mfp,
                        input_key_val,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::FlatMap {
                    input,
                    func,
                    exprs,
                    mfp_after,
                    input_key,
                } => {
                    let node = FlatMap {
                        input: input.lir_id,
                        func,
                        exprs,
                        mfp_after,
                        input_key,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Join { inputs, plan } => {
                    let node = Join {
                        inputs: inputs.iter().map(|i| i.lir_id).collect(),
                        plan,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.extend(
                        inputs
                            .into_iter()
                            .map(|plan| (plan, Some(lir_id), nesting.saturating_add(1))),
                    );
                }
                PlanNode::Reduce {
                    input,
                    key_val_plan,
                    plan,
                    input_key,
                    mfp_after,
                } => {
                    let node = Reduce {
                        input: input.lir_id,
                        key_val_plan,
                        plan,
                        input_key,
                        mfp_after,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::TopK { input, top_k_plan } => {
                    let node = TopK {
                        input: input.lir_id,
                        top_k_plan,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Negate { input } => {
                    let node = Negate {
                        input: input.lir_id,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Threshold {
                    input,
                    threshold_plan,
                } => {
                    let node = Threshold {
                        input: input.lir_id,
                        threshold_plan,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Union {
                    inputs,
                    consolidate_output,
                } => {
                    let node = Union {
                        inputs: inputs.iter().map(|i| i.lir_id).collect(),
                        consolidate_output,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.extend(
                        inputs
                            .into_iter()
                            .map(|plan| (plan, Some(lir_id), nesting.saturating_add(1))),
                    );
                }
                PlanNode::ArrangeBy {
                    input,
                    forms,
                    input_key,
                    input_mfp,
                } => {
                    let node = ArrangeBy {
                        input: input.lir_id,
                        forms,
                        input_key,
                        input_mfp,
                    };
                    insert_node(lir_id, parent, node, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
            };
        }

        flatten_order.reverse();
        let topological_order = flatten_order;

        Self {
            steps,
            root,
            topological_order,
        }
    }
}

impl<T> CollectionPlan for FlatPlan<T> {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        for FlatPlanStep { node, .. } in self.steps.values() {
            if let FlatPlanNode::Get { id, .. } = node {
                if let Id::Global(id) = id {
                    out.insert(*id);
                }
            }
        }
    }
}

impl<T> FlatPlan<T> {
    /// Return the ID of the root node.
    pub fn root_id(&self) -> LirId {
        self.root
    }

    /// Return the root node.
    fn root(&self) -> &FlatPlanNode<T> {
        &self.steps.get(&self.root).expect("invariant (1)").node
    }

    /// Return whether the plan contains recursion.
    pub fn is_recursive(&self) -> bool {
        // Because of invariant (4), every recursive plan must have a `LetRec` at its root.
        matches!(self.root(), FlatPlanNode::LetRec { .. })
    }

    /// Split a recursive plan into its constituent (values, body) subplans.
    ///
    /// # Panics
    ///
    /// Panics if this plan does not have a `LetRec` at the root.
    pub fn split_recursive(self) -> (Vec<(LocalId, Self, Option<LetRecLimit>)>, Self) {
        let (mut steps, root_id, mut topological_order) = self.destruct();
        let root = steps.remove(&root_id).expect("invariant (1)");
        let FlatPlanNode::LetRec {
            ids,
            values,
            limits,
            body,
        } = root.node
        else {
            panic!("attempt to split a non-recursive plan");
        };

        // For each value and the body, find the (transitively) referenced nodes and split them out
        // into new plan. We know that nodes cannot be shared between these subplans because of
        // invariant (6).

        assert_eq!(ids.len(), values.len());
        assert_eq!(ids.len(), limits.len());
        let value_iter = ids.into_iter().zip(values).zip(limits);

        let value_plans = value_iter
            .map(|((id, root_id), limit)| {
                let mut value_steps = BTreeMap::new();
                let mut todo = vec![root_id];
                while let Some(lir_id) = todo.pop() {
                    let step = steps.remove(&lir_id).expect("FlatPlan invariants");
                    todo.extend(step.node.input_lir_ids());
                    value_steps.insert(lir_id, step);
                }

                let value_order = topological_order
                    .iter()
                    .filter(|id| value_steps.contains_key(id))
                    .copied()
                    .collect();
                let plan = FlatPlan {
                    steps: value_steps,
                    root: root_id,
                    topological_order: value_order,
                };
                (id, plan, limit)
            })
            .collect();

        topological_order.retain(|id| steps.contains_key(id));

        let body_plan = FlatPlan {
            steps,
            root: body,
            topological_order,
        };

        (value_plans, body_plan)
    }

    /// Split a non-recursive plan into its constituent let-free subplans.
    ///
    /// The returned value is a list of `value` plans, each with the `LocalId` it is referenced by,
    /// as well as the let-free `body` plan. The `value` plans are in topological order.
    pub fn split_lets(self) -> (Vec<(LocalId, Self)>, Self) {
        let (mut steps, mut root_id, mut topological_order) = self.destruct();

        // Descend the chain of `Let` nodes and build for each one the subplan rooted in its
        // `value`. We know that the subplans are let-free because of invariant (7). We know that
        // nodes cannot be shared between the subplans because of invariant (8).
        let mut value_plans = Vec::new();
        loop {
            let root = steps.remove(&root_id).expect("FlatPlan invariants");
            let FlatPlanNode::Let {
                id: local_id,
                value: value_id,
                body: body_id,
            } = root.node
            else {
                // Reached the end of the `Let` chain.
                steps.insert(root_id, root);
                break;
            };

            let mut value_steps = BTreeMap::new();
            let mut todo = vec![value_id];
            while let Some(lir_id) = todo.pop() {
                let step = steps.remove(&lir_id).expect("FlatPlan invariants");
                todo.extend(step.node.input_lir_ids());
                value_steps.insert(lir_id, step);
            }

            let value_order = topological_order
                .iter()
                .filter(|id| value_steps.contains_key(id))
                .copied()
                .collect();
            let plan = FlatPlan {
                steps: value_steps,
                root: value_id,
                topological_order: value_order,
            };
            value_plans.push((local_id, plan));

            root_id = body_id;
        }

        topological_order.retain(|id| steps.contains_key(id));

        let body_plan = FlatPlan {
            steps,
            root: root_id,
            topological_order,
        };

        (value_plans, body_plan)
    }

    /// Destruct the plan and return its raw parts (the steps (which contain the nodes), the root node, and the topological order).
    ///
    /// This allows consuming the plan without being required to uphold the [`FlatPlan`]
    /// invariants.
    pub fn destruct(self) -> (BTreeMap<LirId, FlatPlanStep<T>>, LirId, Vec<LirId>) {
        (self.steps, self.root, self.topological_order)
    }

    /// Replace references to global IDs by the result of `func`.
    pub fn replace_ids<F>(&mut self, mut func: F)
    where
        F: FnMut(GlobalId) -> GlobalId,
    {
        for step in self.steps.values_mut() {
            if let FlatPlanNode::Get {
                id: Id::Global(id), ..
            } = &mut step.node
            {
                *id = func(*id);
            }
        }
    }

    /// Enumerate all identifiers referenced in `Get` operators.
    pub fn depends(&self) -> BTreeSet<Id> {
        self.steps
            .values()
            .filter_map(|step| {
                if let FlatPlanNode::Get { id, .. } = step.node {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl<T: Clone> FlatPlan<T> {
    /// Partitions the plan into `parts` many disjoint pieces.
    ///
    /// This is used to partition `PlanNode::Constant` stages so that the work
    /// can be distributed across many workers.
    pub fn partition_among(self, parts: usize) -> Vec<Self> {
        if parts == 0 {
            return Vec::new();
        } else if parts == 1 {
            return vec![self];
        }

        let mut part_plans = vec![
            Self {
                steps: BTreeMap::new(),
                root: self.root,
                topological_order: self.topological_order,
            };
            parts
        ];

        for (id, step) in self.steps {
            let partition = step.node.partition_among(parts);
            for (plan, partial_node) in part_plans.iter_mut().zip(partition) {
                plan.steps.insert(
                    id,
                    FlatPlanStep {
                        node: partial_node,
                        parent: step.parent,
                        nesting: step.nesting,
                    },
                );
            }
        }

        part_plans
    }
}

impl<T: Clone> FlatPlanNode<T> {
    /// Partitions the node into `parts` many disjoint pieces.
    ///
    /// This is used to partition `PlanNode::Constant` stages so that the work
    /// can be distributed across many workers.
    fn partition_among(self, parts: usize) -> Vec<Self> {
        use FlatPlanNode::Constant;

        // For constants, balance the rows across the workers.
        // For all other variants, just create copies.
        if let Constant { rows } = self {
            match rows {
                Ok(rows) => {
                    let mut rows_parts = vec![Vec::new(); parts];
                    for (index, row) in rows.into_iter().enumerate() {
                        rows_parts[index % parts].push(row);
                    }
                    rows_parts
                        .into_iter()
                        .map(|rows| Constant { rows: Ok(rows) })
                        .collect()
                }
                Err(err) => {
                    let mut result = vec![
                        Constant {
                            rows: Ok(Vec::new()),
                        };
                        parts
                    ];
                    result[0] = Constant { rows: Err(err) };
                    result
                }
            }
        } else {
            vec![self; parts]
        }
    }
}

impl<T> FlatPlanNode<T> {
    /// Returns the IDs of input nodes to this node.
    fn input_lir_ids(&self) -> impl Iterator<Item = LirId> {
        use FlatPlanNode::*;

        let mut first = None;
        let mut list = Vec::new();
        let mut last = None;

        match self {
            Constant { .. } | Get { .. } => (),
            Let { value, body, .. } => {
                first = Some(*value);
                last = Some(*body);
            }
            LetRec { values, body, .. } => {
                list.clone_from(values);
                last = Some(*body);
            }
            Mfp { input, .. }
            | FlatMap { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input, .. }
            | Threshold { input, .. }
            | ArrangeBy { input, .. } => {
                last = Some(*input);
            }
            Join { inputs, .. } | Union { inputs, .. } => {
                list.clone_from(inputs);
            }
        }

        first.into_iter().chain(list).chain(last)
    }

    /// Renders a single `FlatPlanNode` as a string.
    ///
    /// Typically of the format "{NodeName}::{Detail} {input LirID} ({options})"
    ///
    /// See `FlatPlanNodeHumanizer` and its `std::fmt::Display` instance for implementation details.
    pub fn humanize(&self, humanizer: &dyn ExprHumanizer) -> String {
        FlatPlanNodeHumanizer::new(self, humanizer).to_string()
    }
}

/// Packages a `FlatPlanNode` with an `ExprHumanizer` to render human readable strings.
///
/// Invariant: the `std::fmt::Display` instance should produce a single line for a given node.
#[derive(Debug)]
pub struct FlatPlanNodeHumanizer<'a, T> {
    node: &'a FlatPlanNode<T>,
    humanizer: &'a dyn ExprHumanizer,
}

impl<'a, T> FlatPlanNodeHumanizer<'a, T> {
    /// Creates a `FlatPlanNodeHumanizer` (which simply holds the references).
    ///
    /// Use the `std::fmt::Display` instance.
    pub fn new(node: &'a FlatPlanNode<T>, humanizer: &'a dyn ExprHumanizer) -> Self {
        Self { node, humanizer }
    }
}

impl<'a, T> std::fmt::Display for FlatPlanNodeHumanizer<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.node {
            FlatPlanNode::Constant { rows } => {
                write!(f, "Constant ")?;

                match rows {
                    Ok(rows) => write!(f, "{} rows", rows.len()),
                    Err(err) => write!(f, "error ({err})"),
                }
            }
            FlatPlanNode::Get { id, keys: _, plan } => {
                write!(f, "Get::")?;

                match plan {
                    GetPlan::PassArrangements => write!(f, "PassArrangements")?,
                    GetPlan::Arrangement(_key, Some(val), _mfp) => write!(
                        f,
                        "Arrangement (val={})",
                        HumanizedExplain::new(false).expr(val, None)
                    )?,
                    GetPlan::Arrangement(_key, None, _mfp) => write!(f, "Arrangement")?,
                    GetPlan::Collection(_mfp) => write!(f, "Collection")?,
                };

                write!(f, " ")?;

                match id {
                    Id::Local(id) => write!(f, "{id}"),
                    Id::Global(id) => {
                        if let Some(id) = self.humanizer.humanize_id(*id) {
                            write!(f, "{id}")
                        } else {
                            write!(f, "{id}")
                        }
                    }
                }
            }
            FlatPlanNode::Let { id, value, body } => write!(f, "Let {id}={value} Returning {body}"),
            FlatPlanNode::LetRec {
                ids,
                values,
                limits,
                body,
            } => {
                write!(f, "LetRec ")?;

                for ((id, value), limit) in ids.iter().zip_eq(values).zip_eq(limits) {
                    write!(f, "{id} = {value} ")?;

                    if let Some(limit) = limit {
                        write!(f, "({limit}) ")?;
                    }
                }

                write!(f, "Returning {body}")
            }
            FlatPlanNode::Mfp {
                input,
                mfp: _,
                input_key_val: _,
            } => {
                // TODO(mgree) show MFP detail
                write!(f, "MapFilterProject {input}")
            }
            FlatPlanNode::FlatMap {
                input,
                func,
                exprs: _,
                mfp_after: _,
                input_key: _,
            } => {
                // TODO(mgree) show FlatMap detail
                write!(f, "FlatMap {input} ({func})")
            }
            FlatPlanNode::Join { inputs, plan } => match plan {
                JoinPlan::Linear(LinearJoinPlan {
                    source_relation,
                    stage_plans,
                    ..
                }) => {
                    write!(f, "Join::Differential ")?;

                    write!(f, "{}", inputs[*source_relation])?;
                    for dsp in stage_plans {
                        write!(f, "» {}", inputs[dsp.lookup_relation])?;
                    }

                    Ok(())
                }
                JoinPlan::Delta(DeltaJoinPlan { path_plans }) => {
                    write!(f, "Join::Delta")?;

                    for dpp in path_plans {
                        write!(f, "[{}", inputs[dpp.source_relation])?;

                        for dsp in &dpp.stage_plans {
                            write!(f, "» {}", inputs[dsp.lookup_relation])?;
                        }
                        write!(f, "] ")?;
                    }

                    Ok(())
                }
            },
            FlatPlanNode::Reduce {
                input,
                key_val_plan: _key_val_plan,
                plan,
                input_key: _input_key,
                mfp_after: _mfp_after,
            } => {
                write!(f, "Reduce::")?;

                match plan {
                    ReducePlan::Distinct => write!(f, "Distinct {input}"),
                    ReducePlan::Accumulable(..) => write!(f, "Accumulable {input}"),
                    ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(..)) => {
                        write!(f, "Hierarchical {input} (monotonic)")
                    }
                    ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(BucketedPlan {
                        buckets,
                        ..
                    })) => {
                        write!(f, "Hierarchical {input} (buckets:")?;

                        for bucket in buckets {
                            write!(f, " {bucket}")?;
                        }
                        write!(f, ")")
                    }
                    ReducePlan::Basic(..) => write!(f, "Basic"),
                    ReducePlan::Collation(..) => write!(f, "Collation"),
                }
            }
            FlatPlanNode::TopK { input, top_k_plan } => {
                write!(f, "TopK::")?;
                match top_k_plan {
                    TopKPlan::MonotonicTop1(..) => write!(f, "MonotonicTop1")?,
                    TopKPlan::MonotonicTopK(MonotonicTopKPlan {
                        limit: Some(limit), ..
                    }) => write!(
                        f,
                        "MonotonicTopK({})",
                        HumanizedExplain::new(false).expr(limit, None)
                    )?,
                    TopKPlan::MonotonicTopK(MonotonicTopKPlan { .. }) => {
                        write!(f, "MonotonicTopK")?
                    }
                    TopKPlan::Basic(..) => write!(f, "Basic")?,
                };
                write!(f, " {input}")
            }
            FlatPlanNode::Negate { input } => write!(f, "Negate {input}"),
            FlatPlanNode::Threshold {
                input,
                threshold_plan: _,
            } => write!(f, "Threshold {input}"),
            FlatPlanNode::Union {
                inputs,
                consolidate_output,
            } => {
                write!(f, "Union")?;

                for input in inputs {
                    write!(f, " {input}")?;
                }

                if *consolidate_output {
                    write!(f, " (consolidates output)")?;
                }

                Ok(())
            }
            FlatPlanNode::ArrangeBy {
                input,
                forms: _,
                input_key: _,
                input_mfp: _,
            } => write!(f, "Arrange {input}"),
        }
    }
}

impl Arbitrary for FlatPlan {
    type Strategy = BoxedStrategy<FlatPlan>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any::<Plan>().prop_map(FlatPlan::from).boxed()
    }
}

impl RustType<u64> for LirId {
    fn into_proto(&self) -> u64 {
        u64::from(self.0)
    }

    fn from_proto(proto: u64) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(Self(proto.try_into()?))
    }
}

impl RustType<ProtoFlatPlan> for FlatPlan {
    fn into_proto(&self) -> ProtoFlatPlan {
        ProtoFlatPlan {
            steps: self.steps.into_proto(),
            root: self.root.into_proto(),
            topological_order: self.topological_order.into_proto(),
        }
    }

    fn from_proto(proto: ProtoFlatPlan) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(Self {
            steps: proto.steps.into_rust()?,
            root: LirId::from_proto(proto.root)?,
            topological_order: proto.topological_order.into_rust()?,
        })
    }
}

impl ProtoMapEntry<LirId, FlatPlanStep> for proto_flat_plan::ProtoStep {
    fn from_rust(entry: (&LirId, &FlatPlanStep)) -> Self {
        Self {
            id: entry.0.into_proto(),
            step: Some(entry.1.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(LirId, FlatPlanStep), TryFromProtoError> {
        Ok((
            LirId::from_proto(self.id)?,
            self.step.into_rust_if_some("ProtoStep::step")?,
        ))
    }
}

impl RustType<ProtoFlatPlanStep> for FlatPlanStep {
    fn into_proto(&self) -> ProtoFlatPlanStep {
        ProtoFlatPlanStep {
            node: Some(self.node.into_proto()),
            parent: self.parent.into_proto(),
            nesting: u32::from(self.nesting),
        }
    }

    fn from_proto(proto: ProtoFlatPlanStep) -> Result<Self, mz_proto::TryFromProtoError> {
        Ok(Self {
            node: proto.node.into_rust_if_some("node")?,
            parent: proto.parent.into_rust()?,
            nesting: u8::try_from(proto.nesting).unwrap_or(u8::MAX),
        })
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
            Self::Let { id, value, body } => Kind::Let(ProtoLet {
                id: Some(id.into_proto()),
                value: value.into_proto(),
                body: body.into_proto(),
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
                    body: body.into_proto(),
                })
            }
            Self::Mfp {
                input,
                mfp,
                input_key_val,
            } => Kind::Mfp(ProtoMfp {
                input: input.into_proto(),
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
                input: input.into_proto(),
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
                input: input.into_proto(),
                key_val_plan: Some(key_val_plan.into_proto()),
                plan: Some(plan.into_proto()),
                input_key: input_k_into(input_key),
                mfp_after: Some(mfp_after.into_proto()),
            }),
            Self::TopK { input, top_k_plan } => Kind::TopK(ProtoTopK {
                input: input.into_proto(),
                top_k_plan: Some(top_k_plan.into_proto()),
            }),
            Self::Negate { input } => Kind::Negate(ProtoNegate {
                input: input.into_proto(),
            }),
            Self::Threshold {
                input,
                threshold_plan,
            } => Kind::Threshold(ProtoThreshold {
                input: input.into_proto(),
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
                input: input.into_proto(),
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
            Kind::Let(proto) => Self::Let {
                id: proto.id.into_rust_if_some("ProtoLet::id")?,
                value: LirId::from_proto(proto.value)?,
                body: LirId::from_proto(proto.body)?,
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
                    body: LirId::from_proto(proto.body)?,
                }
            }
            Kind::Mfp(proto) => Self::Mfp {
                input: LirId::from_proto(proto.input)?,
                mfp: proto.mfp.into_rust_if_some("ProtoMfp::mfp")?,
                input_key_val: input_kv_try_into(proto.input_key_val)?,
            },
            Kind::FlatMap(proto) => Self::FlatMap {
                input: LirId::from_proto(proto.input)?,
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
                input: LirId::from_proto(proto.input)?,
                key_val_plan: proto
                    .key_val_plan
                    .into_rust_if_some("ProtoReduce::key_val_plan")?,
                plan: proto.plan.into_rust_if_some("ProtoReduce::plan")?,
                input_key: input_k_try_into(proto.input_key)?,
                mfp_after: proto.mfp_after.into_rust_if_some("Proto::mfp_after")?,
            },
            Kind::TopK(proto) => Self::TopK {
                input: LirId::from_proto(proto.input)?,
                top_k_plan: proto
                    .top_k_plan
                    .into_rust_if_some("ProtoTopK::top_k_plan")?,
            },
            Kind::Negate(proto) => Self::Negate {
                input: LirId::from_proto(proto.input)?,
            },
            Kind::Threshold(proto) => Self::Threshold {
                input: LirId::from_proto(proto.input)?,
                threshold_plan: proto
                    .threshold_plan
                    .into_rust_if_some("ProtoThreshold::threshold_plan")?,
            },
            Kind::Union(proto) => Self::Union {
                inputs: proto.inputs.into_rust()?,
                consolidate_output: proto.consolidate_output,
            },
            Kind::ArrangeBy(proto) => Self::ArrangeBy {
                input: LirId::from_proto(proto.input)?,
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
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[mz_ore::test]
        fn flat_plan_protobuf_roundtrip(expect in any::<FlatPlan>()) {
            let actual = protobuf_roundtrip::<_, ProtoFlatPlan>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }

    }
}
