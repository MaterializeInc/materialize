// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(missing_docs)]

use std::cmp::Ordering;
use std::fmt;

use failure::ResultExt;
use serde::{Deserialize, Serialize};

use repr::{ColumnType, Datum, RelationType, Row};

use self::func::{AggregateFunc, TableFunc};
use crate::id::DummyHumanizer;
use crate::{GlobalId, Id, IdHumanizer, LocalId, ScalarExpr};

pub mod func;

/// An abstract syntax tree which defines a collection.
///
/// The AST is meant reflect the capabilities of the [`differential_dataflow::Collection`] type,
/// written generically enough to avoid run-time compilation work.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum RelationExpr {
    /// A constant relation containing specified rows.
    ///
    /// The runtime memory footprint of this operator is zero.
    Constant {
        /// Rows of the constant collection and their multiplicities.
        rows: Vec<(Row, isize)>,
        /// Schema of the collection.
        typ: RelationType,
    },
    /// Get an existing dataflow.
    ///
    /// The runtime memory footprint of this operator is zero.
    Get {
        /// The identifier for the collection to load.
        id: Id,
        /// Schema of the collection.
        typ: RelationType,
    },
    /// Introduce a temporary dataflow.
    ///
    /// The runtime memory footprint of this operator is zero.
    Let {
        /// The identifier to be used in `Get` variants to retrieve `value`.
        id: LocalId,
        /// The collection to be bound to `name`.
        value: Box<RelationExpr>,
        /// The result of the `Let`, evaluated with `name` bound to `value`.
        body: Box<RelationExpr>,
    },
    /// Project out some columns from a dataflow
    ///
    /// The runtime memory footprint of this operator is zero.
    Project {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Indices of columns to retain.
        outputs: Vec<usize>,
    },
    /// Append new columns to a dataflow
    ///
    /// The runtime memory footprint of this operator is zero.
    Map {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Expressions which determine values to append to each row.
        /// An expression may refer to columns in `input` or
        /// expressions defined earlier in the vector
        scalars: Vec<ScalarExpr>,
    },
    /// Like Map, but yields zero-or-more output rows per input row
    ///
    /// The runtime memory footprint of this operator is zero.
    FlatMap {
        /// The source collection
        input: Box<RelationExpr>,
        /// The table func to apply
        func: TableFunc,
        /// The argument to the table func
        exprs: Vec<ScalarExpr>,
        /// Output columns demanded by the surrounding expression.
        ///
        /// The input columns are often discarded and can be very
        /// expensive to reproduce, so restricting what we produce
        /// as output can be a substantial win.
        ///
        /// See [`expr::transform::Demand`] for more details.
        demand: Option<Vec<usize>>,
    },
    /// Keep rows from a dataflow where all the predicates are true
    ///
    /// The runtime memory footprint of this operator is zero.
    Filter {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Predicates, each of which must be true.
        predicates: Vec<ScalarExpr>,
    },
    /// Join several collections, where some columns must be equal.
    ///
    /// For further details consult the documentation for [`RelationExpr::join`].
    ///
    /// The runtime memory footprint of this operator can be proportional to
    /// the sizes of all inputs and the size of all joins of prefixes.
    /// This may be reduced due to arrangements available at rendering time.
    Join {
        /// A sequence of input relations.
        inputs: Vec<RelationExpr>,
        /// A sequence of equivalence classes of expressions on the cross product of inputs.
        ///
        /// Each equivalence class is a list of scalar expressions, where for each class the
        /// intended interpretation is that all evaluated expressions should be equal.
        ///
        /// Each scalar expression is to be evaluated over the cross-product of all records
        /// from all inputs. In many cases this may just be column selection from specific
        /// inputs, but more general cases exist (e.g. complex functions of multiple columns
        /// from multiple inputs, or just constant literals).
        equivalences: Vec<Vec<ScalarExpr>>,
        /// This optional field is a hint for which columns are
        /// actually used by operators that use this collection. Although the
        /// join does not have permission to change the schema, it can introduce
        /// dummy values at the end of its computation, avoiding the maintenance of values
        /// not present in this list (when it is non-None).
        ///
        /// See [`expr::transform::Demand`] for more details.
        demand: Option<Vec<usize>>,
        /// Join implementation information.
        implementation: JoinImplementation,
    },
    /// Group a dataflow by some columns and aggregate over each group
    ///
    /// The runtime memory footprint of this operator is at most proportional to the
    /// number of distinct records in the input and output. The actual requirements
    /// can be less: the number of distinct inputs to each aggregate, summed across
    /// each aggregate, plus the output size. For more details consult the code that
    /// builds the associated dataflow.
    Reduce {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<ScalarExpr>,
        /// Expressions which determine values to append to each row, after the group keys.
        aggregates: Vec<AggregateExpr>,
    },
    /// Groups and orders within each group, limiting output.
    ///
    /// The runtime memory footprint of this operator is proportional to its input and output.
    TopK {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<usize>,
        /// Column indices used to order rows within groups.
        order_key: Vec<ColumnOrder>,
        /// Number of records to retain
        limit: Option<usize>,
        /// Number of records to skip
        offset: usize,
    },
    /// Return a dataflow where the row counts are negated
    ///
    /// The runtime memory footprint of this operator is zero.
    Negate {
        /// The source collection.
        input: Box<RelationExpr>,
    },
    /// Keep rows from a dataflow where the row counts are positive
    ///
    /// The runtime memory footprint of this operator is proportional to its input and output.
    Threshold {
        /// The source collection.
        input: Box<RelationExpr>,
    },
    /// Adds the frequencies of elements in both sets.
    ///
    /// The runtime memory footprint of this operator is zero.
    Union {
        /// A source collection.
        left: Box<RelationExpr>,
        /// A source collection.
        right: Box<RelationExpr>,
    },
    /// Technically a no-op. Used to render an index. Will be used to optimize queries
    /// on finer grain
    ///
    /// The runtime memory footprint of this operator is proportional to its input.
    ArrangeBy {
        /// The source collection
        input: Box<RelationExpr>,
        /// Columns to arrange `input` by, in order of decreasing primacy
        keys: Vec<Vec<ScalarExpr>>,
    },
}

impl RelationExpr {
    /// Reports the schema of the relation.
    ///
    /// This method determines the type through recursive traversal of the
    /// relation expression, drawing from the types of base collections.
    /// As such, this is not an especially cheap method, and should be used
    /// judiciously.
    pub fn typ(&self) -> RelationType {
        match self {
            RelationExpr::Constant { rows, typ } => {
                for (row, _diff) in rows {
                    for (datum, column_typ) in row.iter().zip(typ.column_types.iter()) {
                        assert!(
                            datum.is_instance_of(column_typ),
                            "Expected datum of type {:?}, got value {:?}",
                            column_typ,
                            datum
                        );
                    }
                }
                let result = typ.clone();
                if rows.len() == 0 || (rows.len() == 1 && rows[0].1 == 1) {
                    result.add_keys(Vec::new())
                } else {
                    result
                }
            }
            RelationExpr::Get { typ, .. } => typ.clone(),
            RelationExpr::Let { body, .. } => body.typ(),
            RelationExpr::Project { input, outputs } => {
                let input_typ = input.typ();
                let mut output_typ = RelationType::new(
                    outputs
                        .iter()
                        .map(|&i| input_typ.column_types[i].clone())
                        .collect(),
                );
                for keys in input_typ.keys {
                    if keys.iter().all(|k| outputs.contains(k)) {
                        output_typ = output_typ.add_keys(
                            keys.iter()
                                .map(|c| outputs.iter().position(|o| o == c).unwrap())
                                .collect(),
                        );
                    }
                }
                output_typ
            }
            RelationExpr::Map { input, scalars } => {
                let mut typ = input.typ();
                let arity = typ.column_types.len();

                let mut remappings = Vec::new();
                for (column, scalar) in scalars.iter().enumerate() {
                    typ.column_types.push(scalar.typ(&typ));
                    // assess whether the scalar preserves uniqueness,
                    // and could participate in a key!

                    fn uniqueness(expr: &ScalarExpr) -> Option<usize> {
                        match expr {
                            ScalarExpr::CallUnary { func, expr } => {
                                if func.preserves_uniqueness() {
                                    uniqueness(expr)
                                } else {
                                    None
                                }
                            }
                            ScalarExpr::Column(c) => Some(*c),
                            _ => None,
                        }
                    }

                    if let Some(c) = uniqueness(scalar) {
                        remappings.push((c, column + arity));
                    }
                }

                // Any column in `remappings` could be replaced in a key
                // by the corresponding c. This could lead to combinatorial
                // explosion using our current representation, so we wont
                // do that. Instead, we'll handle the case of one remapping.
                if remappings.len() == 1 {
                    let (old, new) = remappings.pop().unwrap();
                    let mut new_keys = Vec::new();
                    for key in typ.keys.iter() {
                        if key.contains(&old) {
                            let mut new_key: Vec<usize> =
                                key.iter().cloned().filter(|k| k != &old).collect();
                            new_key.push(new);
                            new_key.sort();
                            new_keys.push(new_key);
                        }
                    }
                    for new_key in new_keys {
                        typ = typ.add_keys(new_key);
                    }
                }

                typ
            }
            RelationExpr::FlatMap {
                input,
                func,
                exprs: _,
                demand: _,
            } => {
                let mut typ = input.typ();
                typ.column_types.extend(func.output_type().column_types);
                // FlatMap can add duplicate rows, so input keys are no longer valid
                RelationType::new(typ.column_types)
            }
            RelationExpr::Filter { input, .. } => input.typ(),
            RelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();

                // Iterating and cloning types inside the flat_map() avoids allocating Vec<>,
                // as clones are directly added to column_types Vec<>.
                let column_types = input_types
                    .iter()
                    .flat_map(|i| i.column_types.iter().cloned())
                    .collect::<Vec<_>>();
                let mut typ = RelationType::new(column_types);

                let input_arities = input_types
                    .iter()
                    .map(|i| i.column_types.len())
                    .collect::<Vec<_>>();

                let mut offset = 0;
                let mut prior_arities = Vec::new();
                for input in 0..inputs.len() {
                    prior_arities.push(offset);
                    offset += input_arities[input];
                }

                let input_relation = input_arities
                    .iter()
                    .enumerate()
                    .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                    .collect::<Vec<_>>();

                // A relation's uniqueness constraint holds if there is a
                // sequence of the other relations such that each one has
                // a uniqueness constraint whose columns are used in join
                // constraints with relations prior in the sequence.
                //
                // We are going to use the uniqueness constraints of the
                // first relation, and attempt to use the presented order.
                let remains_unique = (1..inputs.len()).all(|index| {
                    let mut prior_bound = Vec::new();
                    for equivalence in equivalences {
                        if equivalence.iter().any(|expr| {
                            expr.support()
                                .into_iter()
                                .all(|i| input_relation[i] < index)
                        }) {
                            for expr in equivalence {
                                if let ScalarExpr::Column(c) = expr {
                                    prior_bound.push(c);
                                }
                            }
                        }
                    }
                    input_types[index].keys.iter().any(|ks| {
                        ks.iter()
                            .all(|k| prior_bound.contains(&&(prior_arities[index] + k)))
                    })
                });
                if remains_unique && !inputs.is_empty() {
                    for keys in input_types[0].keys.iter() {
                        typ = typ.add_keys(keys.clone());
                    }
                }
                typ
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let input_typ = input.typ();
                let mut column_types = group_key
                    .iter()
                    .map(|e| e.typ(&input_typ))
                    .collect::<Vec<_>>();
                for agg in aggregates {
                    column_types.push(agg.typ(&input_typ));
                }
                let mut result = RelationType::new(column_types);
                // The group key should form a key, but we might already have
                // keys that are subsets of the group key, and should retain
                // those instead, if so.
                let mut keys = Vec::new();
                for key in input_typ.keys.iter() {
                    if key
                        .iter()
                        .all(|k| group_key.contains(&ScalarExpr::Column(*k)))
                    {
                        keys.push(
                            key.iter()
                                .map(|i| {
                                    group_key
                                        .iter()
                                        .position(|k| k == &ScalarExpr::Column(*i))
                                        .unwrap()
                                })
                                .collect::<Vec<_>>(),
                        );
                    }
                }
                if keys.is_empty() {
                    keys.push((0..group_key.len()).collect());
                }
                for key in keys {
                    result = result.add_keys(key);
                }
                result
            }
            RelationExpr::TopK { input, .. } => input.typ(),
            RelationExpr::Negate { input } => input.typ(),
            RelationExpr::Threshold { input } => input.typ(),
            RelationExpr::Union { left, right } => {
                let left_typ = left.typ();
                let right_typ = right.typ();
                assert_eq!(left_typ.column_types.len(), right_typ.column_types.len());
                RelationType::new(
                    left_typ
                        .column_types
                        .iter()
                        .zip(right_typ.column_types.iter())
                        .map(|(l, r)| l.union(r))
                        .collect::<Result<Vec<_>, _>>()
                        .with_context(|e| format!("{}\nIn {:#?}", e, self))
                        .unwrap(),
                )
                // Important: do not inherit keys of either input, as not unique.
            }
            RelationExpr::ArrangeBy { input, .. } => input.typ(),
        }
    }

    /// The number of columns in the relation.
    ///
    /// This number is determined from the type, which is determined recursively
    /// at non-trivial cost.
    pub fn arity(&self) -> usize {
        self.typ().column_types.len()
    }

    /// Constructs a constant collection from specific rows and schema, where
    /// each row will have a multiplicity of one.
    pub fn constant(rows: Vec<Vec<Datum>>, typ: RelationType) -> Self {
        let rows = rows.into_iter().map(|row| (row, 1)).collect();
        RelationExpr::constant_diff(rows, typ)
    }

    /// Constructs a constant collection from specific rows and schema, where
    /// each row can have an arbitrary multiplicity.
    pub fn constant_diff(rows: Vec<(Vec<Datum>, isize)>, typ: RelationType) -> Self {
        for (row, _diff) in &rows {
            for (datum, column_typ) in row.iter().zip(typ.column_types.iter()) {
                assert!(
                    datum.is_instance_of(column_typ),
                    "Expected datum of type {:?}, got value {:?}",
                    column_typ,
                    datum
                );
            }
        }
        let rows = rows
            .into_iter()
            .map(|(row, diff)| (Row::pack(row), diff))
            .collect();
        RelationExpr::Constant { rows, typ }
    }

    /// Constructs the expression for getting a global collection
    pub fn global_get(id: GlobalId, typ: RelationType) -> Self {
        RelationExpr::Get {
            id: Id::Global(id),
            typ,
        }
    }

    /// Retains only the columns specified by `output`.
    pub fn project(self, outputs: Vec<usize>) -> Self {
        RelationExpr::Project {
            input: Box::new(self),
            outputs,
        }
    }

    /// Append to each row the results of applying elements of `scalar`.
    pub fn map(self, scalars: Vec<ScalarExpr>) -> Self {
        RelationExpr::Map {
            input: Box::new(self),
            scalars,
        }
    }

    /// Like `map`, but yields zero-or-more output rows per input row
    pub fn flat_map(self, func: TableFunc, exprs: Vec<ScalarExpr>) -> Self {
        RelationExpr::FlatMap {
            input: Box::new(self),
            func,
            exprs,
            demand: None,
        }
    }

    /// Retain only the rows satisifying each of several predicates.
    pub fn filter(self, predicates: Vec<ScalarExpr>) -> Self {
        RelationExpr::Filter {
            input: Box::new(self),
            predicates,
        }
    }

    /// Form the Cartesian outer-product of rows in both inputs.
    pub fn product(self, right: Self) -> Self {
        RelationExpr::join(vec![self, right], vec![])
    }

    /// Performs a relational equijoin among the input collections.
    ///
    /// The sequence `inputs` each describe different input collections, and the sequence `variables` describes
    /// equality constraints that some of their columns must satisfy. Each element in `variable` describes a set
    /// of pairs  `(input_index, column_index)` where every value described by that set must be equal.
    ///
    /// For example, the pair `(input, column)` indexes into `inputs[input][column]`, extracting the `input`th
    /// input collection and for each row examining its `column`th column.
    ///
    /// # Example
    ///
    /// ```rust
    /// use repr::{Datum, ColumnType, RelationType, ScalarType};
    /// use expr::RelationExpr;
    ///
    /// // A common schema for each input.
    /// let schema = RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Int32),
    ///     ColumnType::new(ScalarType::Int32),
    /// ]);
    ///
    /// // the specific data are not important here.
    /// let data = vec![Datum::Int32(0), Datum::Int32(1)];
    ///
    /// // Three collections that could have been different.
    /// let input0 = RelationExpr::constant(vec![data.clone()], schema.clone());
    /// let input1 = RelationExpr::constant(vec![data.clone()], schema.clone());
    /// let input2 = RelationExpr::constant(vec![data.clone()], schema.clone());
    ///
    /// // Join the three relations looking for triangles, like so.
    /// //
    /// //     Output(A,B,C) := Input0(A,B), Input1(B,C), Input2(A,C)
    /// let joined = RelationExpr::join(
    ///     vec![input0, input1, input2],
    ///     vec![
    ///         vec![(0,0), (2,0)], // fields A of inputs 0 and 2.
    ///         vec![(0,1), (1,0)], // fields B of inputs 0 and 1.
    ///         vec![(1,1), (2,1)], // fields C of inputs 1 and 2.
    ///     ],
    /// );
    ///
    /// // Technically the above produces `Output(A,B,B,C,A,C)` because the columns are concatenated.
    /// // A projection resolves this and produces the correct output.
    /// let result = joined.project(vec![0, 1, 3]);
    /// ```
    pub fn join(inputs: Vec<RelationExpr>, variables: Vec<Vec<(usize, usize)>>) -> Self {
        let arities = inputs.iter().map(|input| input.arity()).collect::<Vec<_>>();

        let mut offset = 0;
        let mut prior_arities = Vec::new();
        for input in 0..inputs.len() {
            prior_arities.push(offset);
            offset += arities[input];
        }

        let equivalences = variables
            .into_iter()
            .map(|vs| {
                vs.into_iter()
                    .map(|(r, c)| ScalarExpr::Column(prior_arities[r] + c))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Self::join_scalars(inputs, equivalences)
    }

    /// Constructs a join operator from inputs and required-equal scalar expressions.
    pub fn join_scalars(inputs: Vec<RelationExpr>, equivalences: Vec<Vec<ScalarExpr>>) -> Self {
        RelationExpr::Join {
            inputs,
            equivalences,
            demand: None,
            implementation: JoinImplementation::Unimplemented,
        }
    }

    /// Perform a key-wise reduction / aggregation.
    ///
    /// The `group_key` argument indicates columns in the input collection that should
    /// be grouped, and `aggregates` lists aggregation functions each of which produces
    /// one output column in addition to the keys.
    pub fn reduce(self, group_key: Vec<usize>, aggregates: Vec<AggregateExpr>) -> Self {
        RelationExpr::Reduce {
            input: Box::new(self),
            group_key: group_key.into_iter().map(ScalarExpr::Column).collect(),
            aggregates,
        }
    }

    /// Perform a key-wise reduction order by and limit.
    ///
    /// The `group_key` argument indicates columns in the input collection that should
    /// be grouped, the `order_key` argument indicates columns that should be further
    /// used to order records within groups, and the `limit` argument constrains the
    /// total number of records that should be produced in each group.
    pub fn top_k(
        self,
        group_key: Vec<usize>,
        order_key: Vec<ColumnOrder>,
        limit: Option<usize>,
        offset: usize,
    ) -> Self {
        RelationExpr::TopK {
            input: Box::new(self),
            group_key,
            order_key,
            limit,
            offset,
        }
    }

    /// Negates the occurrences of each row.
    pub fn negate(self) -> Self {
        RelationExpr::Negate {
            input: Box::new(self),
        }
    }

    /// Removes all but the first occurrence of each row.
    pub fn distinct(self) -> Self {
        let arity = self.arity();
        self.distinct_by((0..arity).collect())
    }

    /// Removes all but the first occurrence of each key. Columns not included
    /// in the `group_key` are discarded.
    pub fn distinct_by(self, group_key: Vec<usize>) -> Self {
        self.reduce(group_key, vec![])
    }

    /// Discards rows with a negative frequency.
    pub fn threshold(self) -> Self {
        RelationExpr::Threshold {
            input: Box::new(self),
        }
    }

    /// Produces one collection where each row is present with the sum of its frequencies in each input.
    pub fn union(self, other: Self) -> Self {
        RelationExpr::Union {
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Arranges the collection by the specified columns
    pub fn arrange_by(self, keys: &[Vec<ScalarExpr>]) -> Self {
        RelationExpr::ArrangeBy {
            input: Box::new(self),
            keys: keys.to_owned(),
        }
    }

    /// Indicates if this is a constant empty collection.
    ///
    /// A false value does not mean the collection is known to be non-empty,
    /// only that we cannot currently determine that it is statically empty.
    pub fn is_empty(&self) -> bool {
        if let RelationExpr::Constant { rows, .. } = self {
            rows.is_empty()
        } else {
            false
        }
    }

    /// Appends global identifiers on which this expression depends to `out`.
    pub fn global_uses(&self, out: &mut Vec<GlobalId>) {
        if let RelationExpr::Get {
            id: Id::Global(id), ..
        } = self
        {
            out.push(*id);
        }
        self.visit1(|e| e.global_uses(out))
    }

    /// Applies a fallible `f` to each child `RelationExpr`.
    pub fn try_visit1<'a, F, E>(&'a self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a Self) -> Result<(), E>,
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Let { value, body, .. } => {
                f(value)?;
                f(body)?;
            }
            RelationExpr::Project { input, .. } => {
                f(input)?;
            }
            RelationExpr::Map { input, .. } => {
                f(input)?;
            }
            RelationExpr::FlatMap { input, .. } => {
                f(input)?;
            }
            RelationExpr::Filter { input, .. } => {
                f(input)?;
            }
            RelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input)?;
                }
            }
            RelationExpr::Reduce { input, .. } => {
                f(input)?;
            }
            RelationExpr::TopK { input, .. } => {
                f(input)?;
            }
            RelationExpr::Negate { input } => f(input)?,
            RelationExpr::Threshold { input } => f(input)?,
            RelationExpr::Union { left, right } => {
                f(left)?;
                f(right)?;
            }
            RelationExpr::ArrangeBy { input, .. } => {
                f(input)?;
            }
        }
        Ok(())
    }

    /// Applies an infallible `f` to each child `RelationExpr`.
    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        self.try_visit1(|e| {
            f(e);
            Ok::<_, ()>(())
        })
        .unwrap()
    }

    /// Post-order fallible visitor for each `RelationExpr`.
    pub fn try_visit<'a, F, E>(&'a self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a Self) -> Result<(), E>,
    {
        self.try_visit1(|e| e.try_visit(f))?;
        f(self)
    }

    /// Post-order infallible visitor for each `RelationExpr`.
    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit(f));
        f(self)
    }

    /// Applies fallible `f` to each child `RelationExpr`.
    pub fn try_visit1_mut<'a, F, E>(&'a mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a mut Self) -> Result<(), E>,
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Let { value, body, .. } => {
                f(value)?;
                f(body)?;
            }
            RelationExpr::Project { input, .. } => {
                f(input)?;
            }
            RelationExpr::Map { input, .. } => {
                f(input)?;
            }
            RelationExpr::FlatMap { input, .. } => {
                f(input)?;
            }
            RelationExpr::Filter { input, .. } => {
                f(input)?;
            }
            RelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input)?;
                }
            }
            RelationExpr::Reduce { input, .. } => {
                f(input)?;
            }
            RelationExpr::TopK { input, .. } => {
                f(input)?;
            }
            RelationExpr::Negate { input } => f(input)?,
            RelationExpr::Threshold { input } => f(input)?,
            RelationExpr::Union { left, right } => {
                f(left)?;
                f(right)?;
            }
            RelationExpr::ArrangeBy { input, .. } => {
                f(input)?;
            }
        }
        Ok(())
    }

    /// Applies infallible `f` to each child `RelationExpr`.
    pub fn visit1_mut<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        self.try_visit1_mut(|e| {
            f(e);
            Ok::<_, ()>(())
        })
        .unwrap()
    }

    /// Post-order fallible visitor for each `RelationExpr`.
    pub fn try_visit_mut<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
    {
        self.try_visit1_mut(|e| e.try_visit_mut(f))?;
        f(self)
    }

    /// Post-order infallible visitor for each `RelationExpr`.
    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut(f));
        f(self)
    }

    /// Pre-order fallible visitor for each `RelationExpr`.
    pub fn try_visit_mut_pre<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
    {
        f(self)?;
        self.try_visit1_mut(|e| e.try_visit_mut_pre(f))
    }

    /// Pre-order fallible visitor for each `RelationExpr`.
    pub fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e| e.visit_mut_pre(f))
    }

    /// Mutably visit all [`ScalarExpr`]s in the relation expression.
    pub fn visit_scalars_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut ScalarExpr),
    {
        // Match written out explicitly to reduce the possibility of adding a
        // new field with a `ScalarExpr` within and forgetting to account for it
        // here.
        self.visit_mut(&mut |e| match e {
            RelationExpr::Map { scalars, input: _ }
            | RelationExpr::Filter {
                predicates: scalars,
                input: _,
            } => {
                for s in scalars {
                    s.visit_mut(f);
                }
            }
            RelationExpr::FlatMap {
                exprs,
                input: _,
                func: _,
                demand: _,
            } => {
                for expr in exprs {
                    f(expr);
                }
            }
            RelationExpr::Join {
                equivalences: keys,
                inputs: _,
                demand: _,
                implementation: _,
            }
            | RelationExpr::ArrangeBy { input: _, keys } => {
                for key in keys {
                    for s in key {
                        s.visit_mut(f);
                    }
                }
            }
            RelationExpr::Reduce {
                group_key,
                aggregates,
                input: _,
            } => {
                for s in group_key {
                    s.visit_mut(f);
                }
                for agg in aggregates {
                    agg.expr.visit_mut(f);
                }
            }
            RelationExpr::Constant { rows: _, typ: _ }
            | RelationExpr::Get { id: _, typ: _ }
            | RelationExpr::Let {
                id: _,
                value: _,
                body: _,
            }
            | RelationExpr::Project {
                input: _,
                outputs: _,
            }
            | RelationExpr::TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | RelationExpr::Negate { input: _ }
            | RelationExpr::Threshold { input: _ }
            | RelationExpr::Union { left: _, right: _ } => (),
        })
    }

    /// Pretty-print this RelationExpr to a string.
    ///
    /// This method allows an additional IdHumanizer which can annotate
    /// identifiers with additional information, perhaps human-meaningful names
    /// for the identifiers.
    pub fn pretty_humanized(&self, id_humanizer: &impl IdHumanizer) -> String {
        self.explain(id_humanizer).to_string()
    }

    /// Pretty-print this RelationExpr to a string.
    pub fn pretty(&self) -> String {
        self.explain(&DummyHumanizer).to_string()
    }

    /// Take ownership of `self`, leaving an empty `RelationExpr::Constant` with the correct type.
    pub fn take_safely(&mut self) -> RelationExpr {
        let typ = self.typ();
        std::mem::replace(self, RelationExpr::Constant { rows: vec![], typ })
    }
    /// Take ownership of `self`, leaving an empty `RelationExpr::Constant` with an **incorrect** type.
    ///
    /// This should only be used if `self` is about to be dropped or otherwise overwritten.
    pub fn take_dangerous(&mut self) -> RelationExpr {
        let empty = RelationExpr::Constant {
            rows: vec![],
            typ: RelationType::new(Vec::new()),
        };
        std::mem::replace(self, empty)
    }

    /// Replaces `self` with some logic applied to `self`.
    pub fn replace_using<F>(&mut self, logic: F)
    where
        F: FnOnce(RelationExpr) -> RelationExpr,
    {
        let empty = RelationExpr::Constant {
            rows: vec![],
            typ: RelationType::new(Vec::new()),
        };
        let expr = std::mem::replace(self, empty);
        *self = logic(expr);
    }

    /// Store `self` in a `Let` and pass the corresponding `Get` to `body`
    pub fn let_in<Body>(
        self,
        id_gen: &mut IdGen,
        body: Body,
    ) -> Result<RelationExpr, failure::Error>
    where
        Body: FnOnce(&mut IdGen, RelationExpr) -> Result<super::RelationExpr, failure::Error>,
    {
        if let RelationExpr::Get { .. } = self {
            // already done
            body(id_gen, self)
        } else {
            let id = LocalId::new(id_gen.allocate_id());
            let get = RelationExpr::Get {
                id: Id::Local(id),
                typ: self.typ(),
            };
            let body = (body)(id_gen, get)?;
            Ok(RelationExpr::Let {
                id,
                value: Box::new(self),
                body: Box::new(body),
            })
        }
    }

    /// Return every row in `self` that does not have a matching row in the first columns of `keys_and_values`, using `default` to fill in the remaining columns
    /// (If `default` is a row of nulls, this is the 'outer' part of LEFT OUTER JOIN)
    pub fn anti_lookup(
        self,
        id_gen: &mut IdGen,
        keys_and_values: RelationExpr,
        default: Vec<(Datum, ColumnType)>,
    ) -> RelationExpr {
        assert_eq!(keys_and_values.arity() - self.arity(), default.len());
        self.let_in(id_gen, |_id_gen, get_keys| {
            Ok(RelationExpr::join(
                vec![
                    // all the missing keys (with count 1)
                    keys_and_values
                        .distinct_by((0..get_keys.arity()).collect())
                        .negate()
                        .union(get_keys.clone().distinct()),
                    // join with keys to get the correct counts
                    get_keys.clone(),
                ],
                (0..get_keys.arity())
                    .map(|i| vec![(0, i), (1, i)])
                    .collect(),
            )
            // get rid of the extra copies of columns from keys
            .project((0..get_keys.arity()).collect())
            // This join is logically equivalent to
            // `.map(<default_expr>)`, but using a join allows for
            // potential predicate pushdown and elision in the
            // optimizer.
            .product(RelationExpr::constant(
                vec![default.iter().map(|(datum, _)| *datum).collect()],
                RelationType::new(default.iter().map(|(_, typ)| typ.clone()).collect()),
            )))
        })
        .unwrap()
    }

    /// Return:
    /// * every row in keys_and_values
    /// * every row in `self` that does not have a matching row in the first columns of `keys_and_values`, using `default` to fill in the remaining columns
    /// (If `default` is a row of nulls, this is LEFT OUTER JOIN)
    pub fn lookup(
        self,
        id_gen: &mut IdGen,
        keys_and_values: RelationExpr,
        default: Vec<(Datum<'static>, ColumnType)>,
    ) -> RelationExpr {
        keys_and_values
            .let_in(id_gen, |id_gen, get_keys_and_values| {
                Ok(get_keys_and_values.clone().union(self.anti_lookup(
                    id_gen,
                    get_keys_and_values,
                    default,
                )))
            })
            .unwrap()
    }
}

/// Specification for an ordering by a column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ColumnOrder {
    /// The column index.
    pub column: usize,
    /// Whether to sort in descending order.
    pub desc: bool,
}

impl fmt::Display for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "#{} {}",
            self.column.to_string(),
            if self.desc { "desc" } else { "asc" }
        )
    }
}

/// Manages the allocation of locally unique IDs when building a [`RelationExpr`].
#[derive(Debug, Default)]
pub struct IdGen {
    id: u64,
}

impl IdGen {
    /// Allocates a new identifier and advances the generator.
    pub fn allocate_id(&mut self) -> u64 {
        let id = self.id;
        self.id += 1;
        id
    }
}

/// Describes an aggregation expression.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AggregateExpr {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    pub expr: ScalarExpr,
    /// Should the aggregation be applied only to distinct results in each group.
    pub distinct: bool,
}

impl AggregateExpr {
    /// Computes the type of this `AggregateExpr`.
    pub fn typ(&self, relation_type: &RelationType) -> ColumnType {
        self.func.output_type(self.expr.typ(relation_type))
    }
}

/// Describe a join implementation in dataflow.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum JoinImplementation {
    /// Perform a sequence of binary differential dataflow joins.
    ///
    /// The first argument indicates the index of the starting collection, and
    /// the sequence that follows lists other relation indexes, and the key for
    /// the arrangement we should use when joining it in.
    ///
    /// Each collection index should occur exactly once, either in the first
    /// position or somewhere in the list.
    Differential(usize, Vec<(usize, Vec<ScalarExpr>)>),
    /// Perform independent delta query dataflows for each input.
    ///
    /// The argument is a sequence of plans, for the input collections in order.
    /// Each plan starts from the corresponding index, and then in sequence joins
    /// against collections identified by index and with the specified arrangement key.
    DeltaQuery(Vec<Vec<(usize, Vec<ScalarExpr>)>>),
    /// No implementation yet selected.
    Unimplemented,
}

/// Instructions for finishing the result of a query.
///
/// The primary reason for the existence of this structure and attendant code
/// is that SQL's ORDER BY requires sorting rows (as already implied by the
/// keywords), whereas much of the rest of SQL is defined in terms of unordered
/// multisets. But as it turns out, the same idea can be used to optimize
/// trivial peeks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowSetFinishing {
    /// Order rows by the given columns.
    pub order_by: Vec<ColumnOrder>,
    /// Include only as many rows (after offset).
    pub limit: Option<usize>,
    /// Omit as many rows.
    pub offset: usize,
    /// Include only given columns.
    pub project: Vec<usize>,
}

impl RowSetFinishing {
    /// True if the finishing does nothing to any result set.
    pub fn is_trivial(&self) -> bool {
        (self.limit == None) && self.order_by.is_empty() && self.offset == 0
    }
    /// Applies finishing actions to a row set.
    pub fn finish(&self, rows: &mut Vec<Row>) {
        let mut sort_by = |left: &Row, right: &Row| {
            compare_columns(&self.order_by, &left.unpack(), &right.unpack(), || {
                left.cmp(right)
            })
        };
        let offset = self.offset;
        if offset > rows.len() {
            *rows = Vec::new();
        } else {
            if let Some(limit) = self.limit {
                let offset_plus_limit = offset + limit;
                if rows.len() > offset_plus_limit {
                    pdqselect::select_by(rows, offset_plus_limit, &mut sort_by);
                    rows.truncate(offset_plus_limit);
                }
            }
            if offset > 0 {
                pdqselect::select_by(rows, offset, &mut sort_by);
                rows.drain(..offset);
            }
            rows.sort_by(&mut sort_by);
            for row in rows {
                let datums = row.unpack();
                let new_row = Row::pack(self.project.iter().map(|i| &datums[*i]));
                *row = new_row;
            }
        }
    }
}

/// Compare `left` and `right` using `order`. If that doesn't produce a strict ordering, call `tiebreaker`.
pub fn compare_columns<F>(
    order: &[ColumnOrder],
    left: &[Datum],
    right: &[Datum],
    tiebreaker: F,
) -> Ordering
where
    F: Fn() -> Ordering,
{
    for order in order {
        let (lval, rval) = (&left[order.column], &right[order.column]);
        let cmp = if order.desc {
            rval.cmp(&lval)
        } else {
            lval.cmp(&rval)
        };
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    tiebreaker()
}
