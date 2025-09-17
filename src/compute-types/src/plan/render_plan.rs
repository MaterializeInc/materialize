// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`RenderPlan`], a representation of LIR plans used in the compute protocol and rendering,
//! and support for converting [`Plan`]s into this representation.

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::explain::{HumanizedExplain, HumanizerMode};
use mz_expr::{
    CollectionPlan, EvalError, Id, LetRecLimit, LocalId, MapFilterProject, MirScalarExpr, TableFunc,
};
use mz_ore::soft_assert_or_log;
use mz_repr::explain::{CompactScalars, ExprHumanizer};
use mz_repr::{Diff, GlobalId, Row};
use serde::{Deserialize, Serialize};

use crate::plan::join::{DeltaJoinPlan, JoinPlan, LinearJoinPlan};
use crate::plan::reduce::{BucketedPlan, HierarchicalPlan, KeyValPlan, MonotonicPlan, ReducePlan};
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::{MonotonicTopKPlan, TopKPlan};
use crate::plan::{AvailableCollections, GetPlan, LirId, Plan, PlanNode};

/// A representation of LIR plans used for rendering.
///
/// In contrast to [`Plan`], which recursively contains its subplans, this type encodes references
/// between nodes with their unique [`LirId`]. Doing so has the benefit that operations that visit
/// all nodes in a [`RenderPlan`] have natural iterative implementations, avoiding the risk of
/// stack overflows. An exception are recursive bindings, which are defined through nesting of
/// [`RenderPlan`]s. We expect the depth of these nestings to be low for reasonable plans.
///
/// A [`RenderPlan`] can be constructed from a [`Plan`] using the corresponding [`TryFrom`] impl.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RenderPlan<T = mz_repr::Timestamp> {
    /// Stages of bindings to render in order.
    pub binds: Vec<BindStage<T>>,
    /// The binding-free body.
    pub body: LetFreePlan<T>,
}

/// A set of bindings to render in order.
///
/// Each binding assigns a collection to a [`LocalId`] through which the collection can then be
/// referenced in other bindings and the plan body.
///
/// Let bindings in `lets` are rendered first. Each one has access to previous Let bindings in the
/// same stage, as well as any bindings from previous stages.
/// Rec bindings in `recs` are rendered second. Each one has access to _all_ Let and Rec bindings
/// in the same stage, including itself, as well as any bindings from previous stages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BindStage<T = mz_repr::Timestamp> {
    /// Non-recursive bindings.
    pub lets: Vec<LetBind<T>>,
    /// Potentially recursive bindings.
    pub recs: Vec<RecBind<T>>,
}

/// Binds a collection to a [`LocalId`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LetBind<T = mz_repr::Timestamp> {
    /// The identifier through which the collection can be referenced.
    pub id: LocalId,
    /// The collection that is bound to `id`.
    pub value: LetFreePlan<T>,
}

/// Binds a potentially recursively defined collection to a [`LocalId`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecBind<T = mz_repr::Timestamp> {
    /// The identifier through which the collection can be referenced.
    pub id: LocalId,
    /// The collection that is bound to `id`.
    pub value: RenderPlan<T>,
    /// Limits imposed on recursive iteration.
    pub limit: Option<LetRecLimit>,
}

/// A plan free of any binding definitions.
///
/// # Invariants
///
/// A [`LetFreePlan`] maintains the following internal invariants:
///
///  (1) The `root` ID is contained in the `nodes` map.
///  (2) For each node in the `nodes` map, each [`LirId`] contained within the node is contained in
///  the `nodes` map.
///  (3) Each [`LirId`] contained within `topological_order` is contained in the `nodes` map.
///  (4) `topological_order` lists the nodes in a valid topological order.
///
/// The implementation of [`LetFreePlan`] must ensure that all its methods uphold these invariants
/// and that users are not able to break them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LetFreePlan<T = mz_repr::Timestamp> {
    /// The nodes in the plan.
    nodes: BTreeMap<LirId, Node<T>>,
    /// The ID of the root node.
    root: LirId,
    /// The topological order of nodes (dependencies before dependants).
    topological_order: Vec<LirId>,
}

/// A node of a [`RenderPlan`], comprising an [`Expr`] and some metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node<T = mz_repr::Timestamp> {
    /// The relation expression for this node.
    pub expr: Expr<T>,
    /// The [`LirId`] of the parent of this node (for tree reconstruction).
    pub parent: Option<LirId>,
    /// The nesting level of this node (for pretty printing).
    pub nesting: u8,
}

/// A relation expression in a [`RenderPlan`].
///
/// Variants mostly match the ones of [`Plan`], except:
///
///  * The `Let` and `LetRec` variants are removed.
///  * The `lir_id` fields are removed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expr<T = mz_repr::Timestamp> {
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
        /// The particular arrangement of the input we expect to use, if any.
        input_key: Option<Vec<MirScalarExpr>>,
        /// The input collection.
        input: LirId,
        /// Expressions that for each row prepare the arguments to `func`.
        exprs: Vec<MirScalarExpr>,
        /// The variable-record emitting function.
        func: TableFunc,
        /// Linear operator to apply to each record produced by `func`.
        mfp_after: MapFilterProject,
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
        /// The particular arrangement of the input we expect to use, if any.
        input_key: Option<Vec<MirScalarExpr>>,
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
        /// The key that must be used to access the input.
        input_key: Option<Vec<MirScalarExpr>>,
        /// The input collection.
        input: LirId,
        /// The MFP that must be applied to the input.
        input_mfp: MapFilterProject,
        /// A list of arrangement keys, and possibly a raw collection, that will be added to those
        /// of the input. Does not include any other existing arrangements.
        forms: AvailableCollections,
    },
}

impl<T> TryFrom<Plan<T>> for RenderPlan<T> {
    /// The only error is "invalid input plan".
    type Error = ();

    /// Convert the given [`Plan`] into a [`RenderPlan`].
    ///
    /// The ids in [`Node`]s are the same as the original [`LirId`]s.
    ///
    /// # Preconditions
    ///
    /// A [`RenderPlan`] requires certain structure of the [`Plan`] it is converted from.
    ///
    /// Informally, each valid plan is a sequence of Let and LetRec bindings atop a let-free
    /// expression. Each Let binding is to a let-free expression, and each LetRec binding is to a
    /// similarly valid plan.
    ///
    /// ```ignore
    ///   valid_plan := <let_free>
    ///              |  LET v = <let_free> IN <valid_plan>
    ///              |  LETREC (v = <valid_plan>)* IN <valid_plan>
    /// ```
    ///
    /// Input [`Plan`]s that do not satisfy this requirement will result in errors.
    fn try_from(mut plan: Plan<T>) -> Result<Self, Self::Error> {
        use PlanNode::{Let, LetRec};

        // Peel off stages of bindings. Each stage is constructed of an arbitrary amount of leading
        // `Plan::Let` nodes, and at most one `Plan::LetRec` node.
        let mut binds = Vec::new();
        while matches!(plan.node, Let { .. } | LetRec { .. }) {
            let mut lets = Vec::new();
            while let Let { id, value, body } = plan.node {
                let value = LetFreePlan::try_from(*value)?;
                lets.push(LetBind { id, value });
                plan = *body;
            }

            let mut recs = Vec::new();
            if let LetRec {
                ids,
                values,
                limits,
                body,
            } = plan.node
            {
                for ((id, value), limit) in ids.into_iter().zip_eq(values).zip_eq(limits) {
                    let value = RenderPlan::try_from(value)?;
                    recs.push(RecBind { id, value, limit })
                }
                plan = *body;
            }

            binds.push(BindStage { lets, recs });
        }

        // The rest of the plan must be let-free.
        let body = LetFreePlan::try_from(plan)?;

        Ok(Self { binds, body })
    }
}

impl<T> TryFrom<Plan<T>> for LetFreePlan<T> {
    /// The only error is "invalid input plan".
    type Error = ();

    /// Convert the given [`Plan`] into a [`LetFreePlan`].
    ///
    /// Returns an error if the given [`Plan`] contains `Let` or `LetRec` nodes.
    fn try_from(plan: Plan<T>) -> Result<Self, Self::Error> {
        use Expr::*;

        // The strategy is to walk walk through the `Plan` in right-to-left pre-order and for each
        // node (a) produce a corresponding `Node` and (b) push the contained subplans onto the
        // work stack. We do this until no further subplans remain.

        let root = plan.lir_id;

        // Stack of nodes to flatten, with their parent id and nesting.
        let mut todo: Vec<(Plan<T>, Option<LirId>, u8)> = vec![(plan, None, 0)];
        // `RenderPlan` nodes produced so far.
        let mut nodes: BTreeMap<LirId, Node<T>> = Default::default();
        // A list remembering the order in which nodes were flattened.
        // Because nodes are flatten in right-to-left pre-order, reversing this list at the end
        // yields a valid topological order.
        let mut flatten_order: Vec<LirId> = Default::default();

        let mut insert_node = |id, parent, expr, nesting| {
            nodes.insert(
                id,
                Node {
                    expr,
                    parent,
                    nesting,
                },
            );
            flatten_order.push(id);
        };

        while let Some((Plan { node, lir_id }, parent, nesting)) = todo.pop() {
            match node {
                PlanNode::Constant { rows } => {
                    let expr = Constant { rows };
                    insert_node(lir_id, parent, expr, nesting);
                }
                PlanNode::Get { id, keys, plan } => {
                    let expr = Get { id, keys, plan };
                    insert_node(lir_id, parent, expr, nesting);
                }
                PlanNode::Mfp {
                    input,
                    mfp,
                    input_key_val,
                } => {
                    let expr = Mfp {
                        input: input.lir_id,
                        mfp,
                        input_key_val,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::FlatMap {
                    input_key,
                    input,
                    exprs,
                    func,
                    mfp_after,
                } => {
                    let expr = FlatMap {
                        input_key,
                        input: input.lir_id,
                        exprs,
                        func,
                        mfp_after,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Join { inputs, plan } => {
                    let expr = Join {
                        inputs: inputs.iter().map(|i| i.lir_id).collect(),
                        plan,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.extend(
                        inputs
                            .into_iter()
                            .map(|plan| (plan, Some(lir_id), nesting.saturating_add(1))),
                    );
                }
                PlanNode::Reduce {
                    input_key,
                    input,
                    key_val_plan,
                    plan,
                    mfp_after,
                } => {
                    let expr = Reduce {
                        input_key,
                        input: input.lir_id,
                        key_val_plan,
                        plan,
                        mfp_after,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::TopK { input, top_k_plan } => {
                    let expr = TopK {
                        input: input.lir_id,
                        top_k_plan,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Negate { input } => {
                    let expr = Negate {
                        input: input.lir_id,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Threshold {
                    input,
                    threshold_plan,
                } => {
                    let expr = Threshold {
                        input: input.lir_id,
                        threshold_plan,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Union {
                    inputs,
                    consolidate_output,
                } => {
                    let expr = Union {
                        inputs: inputs.iter().map(|i| i.lir_id).collect(),
                        consolidate_output,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.extend(
                        inputs
                            .into_iter()
                            .map(|plan| (plan, Some(lir_id), nesting.saturating_add(1))),
                    );
                }
                PlanNode::ArrangeBy {
                    input_key,
                    input,
                    input_mfp,
                    forms,
                } => {
                    let expr = ArrangeBy {
                        input_key,
                        input: input.lir_id,
                        input_mfp,
                        forms,
                    };
                    insert_node(lir_id, parent, expr, nesting);

                    todo.push((*input, Some(lir_id), nesting.saturating_add(1)));
                }
                PlanNode::Let { .. } | PlanNode::LetRec { .. } => return Err(()),
            };
        }

        flatten_order.reverse();
        let topological_order = flatten_order;

        Ok(Self {
            nodes,
            root,
            topological_order,
        })
    }
}

impl<T> CollectionPlan for RenderPlan<T> {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        for stage in &self.binds {
            for LetBind { value, .. } in &stage.lets {
                value.depends_on_into(out);
            }
            for RecBind { value, .. } in &stage.recs {
                value.depends_on_into(out);
            }
        }

        self.body.depends_on_into(out);
    }
}

impl<T> CollectionPlan for LetFreePlan<T> {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        for Node { expr, .. } in self.nodes.values() {
            if let Expr::Get { id, .. } = expr {
                if let Id::Global(id) = id {
                    out.insert(*id);
                }
            }
        }
    }
}

impl<T> RenderPlan<T> {
    /// Return whether the plan contains recursion.
    pub fn is_recursive(&self) -> bool {
        self.binds.iter().any(|b| !b.recs.is_empty())
    }

    /// Replace references to global IDs by the result of `func`.
    pub fn replace_ids<F>(&mut self, func: &mut F)
    where
        F: FnMut(GlobalId) -> GlobalId,
    {
        for stage in &mut self.binds {
            for LetBind { value, .. } in &mut stage.lets {
                value.replace_ids(&mut *func);
            }
            for RecBind { value, .. } in &mut stage.recs {
                value.replace_ids(&mut *func);
            }
        }
        self.body.replace_ids(&mut *func);
    }

    /// Enumerate all identifiers referenced in `Get` operators.
    pub fn depends(&self) -> BTreeSet<Id> {
        let mut result = BTreeSet::new();
        for stage in &self.binds {
            for LetBind { value, .. } in &stage.lets {
                result.append(&mut value.depends());
            }
            for RecBind { value, .. } in &stage.recs {
                result.append(&mut value.depends());
            }
        }
        result.append(&mut self.body.depends());
        result
    }
}

impl<T> LetFreePlan<T> {
    /// Return the ID of the root node.
    pub fn root_id(&self) -> LirId {
        self.root
    }

    /// Destruct the plan and return its raw parts (the nodes, the root node, and the topological
    /// order).
    ///
    /// This allows consuming the plan without being required to uphold the [`LetFreePlan`]
    /// invariants.
    pub fn destruct(self) -> (BTreeMap<LirId, Node<T>>, LirId, Vec<LirId>) {
        (self.nodes, self.root, self.topological_order)
    }

    /// Replace references to global IDs by the result of `func`.
    pub fn replace_ids<F>(&mut self, mut func: F)
    where
        F: FnMut(GlobalId) -> GlobalId,
    {
        for node in self.nodes.values_mut() {
            if let Expr::Get {
                id: Id::Global(id), ..
            } = &mut node.expr
            {
                *id = func(*id);
            }
        }
    }

    /// Enumerate all identifiers referenced in `Get` operators.
    pub fn depends(&self) -> BTreeSet<Id> {
        self.nodes
            .values()
            .filter_map(|node| {
                if let Expr::Get { id, .. } = node.expr {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl<T: Clone> RenderPlan<T> {
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

        // Partition the body first and use the result to initialize the per-part `RenderPlan`s, to
        // which we can then push the partitioned binds directly, avoiding a vec allocation.
        let bodies = self.body.partition_among(parts);
        let mut part_plans: Vec<_> = bodies
            .into_iter()
            .map(|body| Self {
                binds: Vec::with_capacity(self.binds.len()),
                body,
            })
            .collect();

        for stage in self.binds {
            let partition = stage.partition_among(parts);
            for (plan, stage) in part_plans.iter_mut().zip_eq(partition) {
                plan.binds.push(stage);
            }
        }

        part_plans
    }
}

impl<T: Clone> BindStage<T> {
    /// Partitions the stage into `parts` many disjoint pieces.
    ///
    /// This is used to partition `PlanNode::Constant` stages so that the work
    /// can be distributed across many workers.
    fn partition_among(self, parts: usize) -> Vec<Self> {
        let mut part_binds = vec![
            BindStage {
                lets: Vec::with_capacity(self.lets.len()),
                recs: Vec::with_capacity(self.recs.len())
            };
            parts
        ];

        for LetBind { id, value } in self.lets {
            let partition = value.partition_among(parts);
            for (stage, value) in part_binds.iter_mut().zip_eq(partition) {
                stage.lets.push(LetBind { id, value });
            }
        }
        for RecBind { id, value, limit } in self.recs {
            let partition = value.partition_among(parts);
            for (stage, value) in part_binds.iter_mut().zip_eq(partition) {
                stage.recs.push(RecBind { id, value, limit });
            }
        }

        part_binds
    }
}

impl<T: Clone> LetFreePlan<T> {
    /// Partitions the plan into `parts` many disjoint pieces.
    ///
    /// This is used to partition `PlanNode::Constant` stages so that the work
    /// can be distributed across many workers.
    fn partition_among(self, parts: usize) -> Vec<Self> {
        let mut part_plans = vec![
            Self {
                nodes: BTreeMap::new(),
                root: self.root,
                topological_order: self.topological_order,
            };
            parts
        ];

        for (id, node) in self.nodes {
            let partition = node.expr.partition_among(parts);
            for (plan, expr) in part_plans.iter_mut().zip_eq(partition) {
                plan.nodes.insert(
                    id,
                    Node {
                        expr,
                        parent: node.parent,
                        nesting: node.nesting,
                    },
                );
            }
        }

        part_plans
    }
}

impl<T: Clone> Expr<T> {
    /// Partitions the expr into `parts` many disjoint pieces.
    ///
    /// This is used to partition `PlanNode::Constant` stages so that the work
    /// can be distributed across many workers.
    fn partition_among(self, parts: usize) -> Vec<Self> {
        use Expr::Constant;

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

impl<T> Expr<T> {
    /// Renders a single [`Expr`] as a string.
    ///
    /// Typically of the format "{ExprName}::{Detail} {input LirID} ({options})"
    ///
    /// See `RenderPlanExprHumanizer` and its [`std::fmt::Display`] instance for implementation
    /// details.
    pub fn humanize(&self, humanizer: &dyn ExprHumanizer) -> String {
        RenderPlanExprHumanizer::new(self, humanizer).to_string()
    }
}

/// Packages an [`Expr`] with an [`ExprHumanizer`] to render human readable strings.
///
/// Invariant: the [`std::fmt::Display`] instance should produce a single line for a given expr.
#[derive(Debug)]
pub struct RenderPlanExprHumanizer<'a, T> {
    /// The [`Expr`] to be rendered.
    expr: &'a Expr<T>,
    /// Humanization information.
    humanizer: &'a dyn ExprHumanizer,
}

impl<'a, T> RenderPlanExprHumanizer<'a, T> {
    /// Creates a [`RenderPlanExprHumanizer`] (which simply holds the references).
    ///
    /// Use the [`std::fmt::Display`] instance.
    pub fn new(expr: &'a Expr<T>, humanizer: &'a dyn ExprHumanizer) -> Self {
        Self { expr, humanizer }
    }
}

impl<'a, T> std::fmt::Display for RenderPlanExprHumanizer<'a, T> {
    // NOTE: This code needs to be kept in sync with `Plan::fmt_default_text`.
    //
    // This code determines what you see in `mz_lir_mapping`; that other code
    // determine what you see when you run `EXPLAIN`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expr::*;

        match self.expr {
            Constant { rows } => {
                write!(f, "Constant ")?;

                match rows {
                    Ok(rows) => write!(f, "({} rows)", rows.len()),
                    Err(err) => write!(f, "(error: {err})"),
                }
            }
            Get { id, keys, plan } => {
                let id = match id {
                    Id::Local(id) => id.to_string(),
                    Id::Global(id) => self
                        .humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| id.to_string()),
                };

                match plan {
                    GetPlan::PassArrangements => {
                        if keys.raw && keys.arranged.is_empty() {
                            write!(f, "Stream {id}")
                        } else {
                            write!(f, "Arranged {id}")
                        }
                    }
                    GetPlan::Arrangement(_key, Some(val), _mfp) => write!(
                        f,
                        "Index Lookup on {id} (val={})",
                        HumanizedExplain::new(false).expr(val, None)
                    ),
                    GetPlan::Arrangement(_key, None, _mfp) => write!(f, "Arranged {id}"),
                    GetPlan::Collection(_mfp) => write!(f, "Read {id}"),
                }
            }
            Mfp {
                input: _,
                mfp: _,
                input_key_val: _,
            } => {
                // TODO(mgree) show MFP detail
                write!(f, "Map/Filter/Project")
            }
            FlatMap {
                input_key: _,
                input: _,
                exprs,
                func,
                mfp_after: _,
            } => {
                let mode = HumanizedExplain::new(false);
                let exprs = CompactScalars(mode.seq(exprs, None));

                write!(f, "Table Function {func}({exprs})")
            }
            Join { inputs: _, plan } => match plan {
                JoinPlan::Linear(LinearJoinPlan {
                    source_relation,
                    stage_plans,
                    ..
                }) => {
                    write!(f, "Differential Join ")?;

                    write!(f, "%{}", source_relation)?;
                    for dsp in stage_plans {
                        write!(f, " » %{}", dsp.lookup_relation)?;
                    }

                    Ok(())
                }
                JoinPlan::Delta(DeltaJoinPlan { path_plans }) => {
                    write!(f, "Delta Join ")?;

                    let mut first = true;
                    for dpp in path_plans {
                        if !first {
                            write!(f, " ")?;
                            first = false;
                        }
                        write!(f, "[%{}", dpp.source_relation)?;

                        for dsp in &dpp.stage_plans {
                            write!(f, " » %{}", dsp.lookup_relation)?;
                        }
                        write!(f, "]")?;
                    }

                    Ok(())
                }
            },
            Reduce {
                input_key: _input_key,
                input: _,
                key_val_plan: _key_val_plan,
                plan,
                mfp_after: _mfp_after,
            } => match plan {
                ReducePlan::Distinct => write!(f, "Distinct GroupAggregate"),
                ReducePlan::Accumulable(..) => write!(f, "Accumulable GroupAggregate"),
                ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(MonotonicPlan {
                    must_consolidate,
                    ..
                })) => {
                    if *must_consolidate {
                        write!(f, "Consolidating ")?;
                    }
                    write!(f, "Monotonic GroupAggregate")
                }
                ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(BucketedPlan {
                    buckets,
                    ..
                })) => {
                    write!(f, "Bucketed Hierarchical GroupAggregate (buckets:")?;

                    for bucket in buckets {
                        write!(f, " {bucket}")?;
                    }
                    write!(f, ")")
                }
                ReducePlan::Basic(..) => write!(f, "Non-incremental GroupAggregate`"),
                ReducePlan::Collation(..) => write!(f, "Collated Multi-GroupAggregate"),
            },
            TopK {
                input: _,
                top_k_plan,
            } => {
                match top_k_plan {
                    TopKPlan::MonotonicTop1(..) => write!(f, "Monotonic Top1")?,
                    TopKPlan::MonotonicTopK(MonotonicTopKPlan {
                        limit: Some(limit), ..
                    }) => write!(
                        f,
                        "MonotonicTopK({})",
                        HumanizedExplain::new(false).expr(limit, None)
                    )?,
                    TopKPlan::MonotonicTopK(MonotonicTopKPlan { .. }) => {
                        write!(f, "Monotonic TopK")?
                    }
                    TopKPlan::Basic(..) => write!(f, "Non-monotonic TopK")?,
                };

                Ok(())
            }
            Negate { input: _ } => write!(f, "Negate Diffs"),
            Threshold {
                input: _,
                threshold_plan: _,
            } => write!(f, "Threshold Diffs"),
            Union {
                inputs: _,
                consolidate_output,
            } => {
                if *consolidate_output {
                    write!(f, "Consolidating ")?;
                }

                write!(f, "Union")?;

                Ok(())
            }
            ArrangeBy {
                input_key: _,
                input: _,
                input_mfp: _,
                forms,
            } => {
                if forms.arranged.is_empty() {
                    soft_assert_or_log!(forms.raw, "raw stream with no arrangements");
                    write!(f, "Unarranged Raw Stream")
                } else {
                    write!(f, "Arrange")?;
                    if forms.arranged.len() > 0 {
                        let mode = HumanizedExplain::new(false);
                        for (key, _, _) in &forms.arranged {
                            if !key.is_empty() {
                                let key = mode.seq(key, None);
                                let key = CompactScalars(key);
                                write!(f, " ({key})")?;
                            } else {
                                write!(f, " (empty key)")?;
                            }
                        }
                    }
                    Ok(())
                }
            }
        }
    }
}
