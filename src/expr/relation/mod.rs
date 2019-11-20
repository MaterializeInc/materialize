// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![deny(missing_docs)]
// Clippy is wrong.
#![allow(clippy::op_ref, clippy::len_zero)]

use failure::ResultExt;
use pretty::Doc::Space;
use pretty::{BoxDoc, Doc};
use serde::{Deserialize, Serialize};

use self::func::AggregateFunc;
use crate::pretty_pretty::{
    compact_intersperse_doc, tighten_outputs, to_braced_doc, to_tightly_braced_doc,
};
use crate::{GlobalId, Id, IdHumanizer, LocalId, ScalarExpr};
use repr::{ColumnType, Datum, RelationType, Row};

pub mod func;

/// An abstract syntax tree which defines a collection.
///
/// The AST is meant reflect the capabilities of the [`differential_dataflow::Collection`] type,
/// written generically enough to avoid run-time compilation work.
#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum RelationExpr {
    /// Always return the same value
    Constant {
        /// Rows of the constant collection and their multiplicities.
        rows: Vec<(Row, isize)>,
        /// Schema of the collection.
        typ: RelationType,
    },
    /// Get an existing dataflow
    Get {
        /// The identifier for the collection to load.
        id: Id,
        /// Schema of the collection.
        typ: RelationType,
    },
    /// Introduce a temporary dataflow
    Let {
        /// The identifier to be used in `Get` variants to retrieve `value`.
        id: LocalId,
        /// The collection to be bound to `name`.
        value: Box<RelationExpr>,
        /// The result of the `Let`, evaluated with `name` bound to `value`.
        body: Box<RelationExpr>,
    },
    /// Project out some columns from a dataflow
    Project {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Indices of columns to retain.
        outputs: Vec<usize>,
    },
    /// Append new columns to a dataflow
    Map {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Expressions which determine values to append to each row.
        scalars: Vec<ScalarExpr>,
    },
    /// Keep rows from a dataflow where all the predicates are true
    Filter {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Predicates, each of which must be true.
        predicates: Vec<ScalarExpr>,
    },
    /// Join several collections, where some columns must be equal.
    ///
    /// For further details consult the documentation for [`RelationExpr::join`].
    Join {
        /// A sequence of input relations.
        inputs: Vec<RelationExpr>,
        /// A sequence of equivalence classes of `(input_index, column_index)`.
        ///
        /// Each element of the sequence is a set of pairs, where the values described by each pair must
        /// be equal to all other values in the same set.
        variables: Vec<Vec<(usize, usize)>>,
        /// This optional field is a hint for which columns from each input are
        /// actually used by operators that use this collection. Although the
        /// join does not have permission to change the schema, it can introduce
        /// dummy values at the end of its computation, avoiding the maintenance of values
        /// not present in this list (when it is non-None).
        demand: Option<Vec<Vec<usize>>>,
    },
    /// Group a dataflow by some columns and aggregate over each group
    Reduce {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<usize>,
        /// Expressions which determine values to append to each row, after the group keys.
        aggregates: Vec<AggregateExpr>,
    },
    /// Groups and orders within each group, limiting output.
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
    Negate {
        /// The source collection.
        input: Box<RelationExpr>,
    },
    /// Keep rows from a dataflow where the row counts are positive
    Threshold {
        /// The source collection.
        input: Box<RelationExpr>,
    },
    /// Adds the frequencies of elements in both sets.
    Union {
        /// A source collection.
        left: Box<RelationExpr>,
        /// A source collection.
        right: Box<RelationExpr>,
    },
    /// Technically a no-op. Used to render an index. Will be used to optimize queries
    /// on finer grain
    ArrangeBy {
        /// The source collection
        input: Box<RelationExpr>,
        /// Columns to arrange `input` by, in order of decreasing primacy
        keys: Vec<usize>,
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
                            datum.is_instance_of(*column_typ),
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
                let mut output_typ =
                    RelationType::new(outputs.iter().map(|&i| input_typ.column_types[i]).collect());
                for keys in input_typ.keys {
                    if keys.iter().all(|k| outputs.contains(k)) {
                        output_typ = output_typ.add_keys(keys);
                    }
                }
                output_typ
            }
            RelationExpr::Map { input, scalars } => {
                let mut typ = input.typ();
                for scalar in scalars {
                    typ.column_types.push(scalar.typ(&typ));
                }
                typ
            }
            RelationExpr::Filter { input, .. } => input.typ(),
            RelationExpr::Join {
                inputs, variables, ..
            } => {
                let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();

                // Iterating and cloning types inside the flat_map() avoids allocating Vec<>,
                // as clones are directly added to column_types Vec<>.
                let column_types = input_types
                    .iter()
                    .flat_map(|i| i.column_types.iter().cloned())
                    .collect::<Vec<_>>();
                let mut typ = RelationType::new(column_types);

                // A relation's uniqueness constraint holds if there is a
                // sequence of the other relations such that each one has
                // a uniqueness constraint whose columns are used in join
                // constraints with relations prior in the sequence.
                //
                // We are going to use the uniqueness constraints of the
                // first relation, and attempt to use the presented order.
                let remains_unique = (1..inputs.len()).all(|index| {
                    let mut prior_bound = Vec::new();
                    for variable in variables {
                        if variable.iter().any(|(r, _c)| r < &index) {
                            for (r, c) in variable {
                                if r == &index {
                                    prior_bound.push(c);
                                }
                            }
                        }
                    }
                    input_types[index]
                        .keys
                        .iter()
                        .any(|ks| ks.iter().all(|k| prior_bound.contains(&k)))
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
                    .map(|&i| input_typ.column_types[i])
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
                    if key.iter().all(|k| group_key.contains(k)) {
                        keys.push(
                            key.iter()
                                .map(|i| group_key.iter().position(|k| k == i).unwrap())
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
                        .map(|(l, r)| l.union(*r))
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
                    datum.is_instance_of(*column_typ),
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
        RelationExpr::Join {
            inputs,
            variables,
            demand: None,
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
            group_key,
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
    pub fn arrange_by(self, keys: &[usize]) -> Self {
        RelationExpr::ArrangeBy {
            input: Box::new(self),
            keys: keys.to_vec(),
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

    /// Applies `f` to each child `RelationExpr`.
    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Let { value, body, .. } => {
                f(value);
                f(body);
            }
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input);
                }
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => f(input),
            RelationExpr::Threshold { input } => f(input),
            RelationExpr::Union { left, right } => {
                f(left);
                f(right);
            }
            RelationExpr::ArrangeBy { input, .. } => {
                f(input);
            }
        }
    }

    /// Post-order visitor for each `RelationExpr`.
    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit(f));
        f(self);
    }

    /// Applies `f` to each child `RelationExpr`.
    pub fn visit1_mut<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Let { value, body, .. } => {
                f(value);
                f(body);
            }
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input);
                }
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => f(input),
            RelationExpr::Threshold { input } => f(input),
            RelationExpr::Union { left, right } => {
                f(left);
                f(right);
            }
            RelationExpr::ArrangeBy { input, .. } => {
                f(input);
            }
        }
    }

    /// Post-order visitor for each `RelationExpr`.
    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut(f));
        f(self);
    }

    /// Pre-order visitor for each `RelationExpr`.
    pub fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e| e.visit_mut_pre(f));
    }

    /// Convert this [`RelationExpr`] to a [`Doc`] or document for pretty printing. This
    /// function formats each dataflow operator instance as
    /// ```text
    /// <operator-name> { <argument-1>, ..., <argument-n> }
    /// ```
    /// Arguments that are *not* dataflow operator instances are listed first and arguments
    /// that are dataflow operator instances, i.e., the input dataflows, are listed second.
    /// The former are prefixed with their names, while the latter are not. For example:
    /// ```text
    /// Project { outputs: [1], Map { scalars: [665], "X" } }
    /// ```
    /// identifies the outputs for `Project` and the scalars for `Map` by name, whereas the
    /// sole inputs are nameless.
    pub fn to_doc(&self, id_humanizer: &impl IdHumanizer) -> Doc<BoxDoc<()>> {
        let doc = match self {
            RelationExpr::Constant { rows, .. } => {
                let rows = Doc::intersperse(
                    rows.iter().map(|(row, diff)| {
                        let row = Doc::intersperse(row, to_doc!(",", Space));
                        let row = to_tightly_braced_doc("[", row, "]").group();
                        if *diff != 1 {
                            row.append(to_doc!("*", diff.to_string()))
                        } else {
                            row
                        }
                    }),
                    to_doc!(",", Space),
                );
                to_tightly_braced_doc("Constant [", rows, "]")
            }
            RelationExpr::Get { id, typ: _ } => {
                let id = match id_humanizer.humanize_id(*id) {
                    Some(s) => format!("{} ({})", s, id),
                    None => id.to_string(),
                };
                to_braced_doc("Get {", id, "}")
            }
            RelationExpr::Let { id, value, body } => {
                let value = value.to_doc(id_humanizer);
                let body = body.to_doc(id_humanizer);
                // NB: We don't include the body inside the curly braces, so that recursively
                // nested Let expressions do *not* increase the indentation.
                let binding =
                    to_braced_doc("Let {", to_doc!(id.to_string(), " = ", value), "} in").group();
                to_doc!(binding, Space, body)
            }
            RelationExpr::Project { input, outputs } => {
                let input = input.to_doc(id_humanizer);
                let outputs =
                    compact_intersperse_doc(tighten_outputs(outputs), to_doc!(",", Space));
                let outputs = to_tightly_braced_doc("outputs: [", outputs, "]").group();
                to_braced_doc("Project {", to_doc!(outputs, ",", Space, input), "}")
            }
            RelationExpr::Map { input, scalars } => {
                let input = input.to_doc(id_humanizer);
                let scalars = Doc::intersperse(scalars.iter(), to_doc!(",", Space));
                let scalars = to_tightly_braced_doc("scalars: [", scalars, "]").group();
                to_braced_doc("Map {", to_doc!(scalars, ",", Space, input), "}")
            }
            RelationExpr::Filter { input, predicates } => {
                let input = input.to_doc(id_humanizer);
                let predicates = Doc::intersperse(predicates, to_doc!(",", Space));
                let predicates = to_tightly_braced_doc("predicates: [", predicates, "]").group();
                to_braced_doc("Filter {", to_doc!(predicates, ",", Space, input), "}")
            }
            RelationExpr::Join {
                inputs, variables, ..
            } => {
                fn pair_to_doc(p: &(usize, usize)) -> Doc<BoxDoc<()>, ()> {
                    to_doc!("(", p.0.to_string(), ", ", p.1.to_string(), ")")
                }

                let variables = Doc::intersperse(
                    variables.iter().map(|ps| {
                        let ps = Doc::intersperse(ps.iter().map(pair_to_doc), to_doc!(",", Space));
                        to_tightly_braced_doc("[", ps, "]").group()
                    }),
                    to_doc!(",", Space),
                );
                let variables = to_tightly_braced_doc("variables: [", variables, "]").group();

                let inputs = Doc::intersperse(
                    inputs.iter().map(|inp| inp.to_doc(id_humanizer)),
                    to_doc!(",", Space),
                );

                to_braced_doc("Join {", to_doc!(variables, ",", Space, inputs), "}")
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let input = input.to_doc(id_humanizer);
                let keys = compact_intersperse_doc(tighten_outputs(group_key), to_doc!(",", Space));
                let keys = to_tightly_braced_doc("group_key: [", keys, "]").group();

                if aggregates.is_empty() {
                    to_braced_doc("Distinct {", to_doc!(keys, ",", Space, input), "}")
                } else {
                    let aggregates = Doc::intersperse(aggregates, to_doc!(",", Space));
                    let aggregates =
                        to_tightly_braced_doc("aggregates: [", aggregates, "]").group();

                    to_braced_doc(
                        "Reduce {",
                        to_doc!(keys, ",", Space, aggregates, ",", Space, input),
                        "}",
                    )
                }
            }
            RelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
            } => {
                let input = input.to_doc(id_humanizer);
                let group_keys =
                    compact_intersperse_doc(tighten_outputs(group_key), to_doc!(",", Space));
                let group_keys = to_tightly_braced_doc("group_key: [", group_keys, "]").group();
                let order_keys = compact_intersperse_doc(order_key, to_doc!(",", Space));
                let order_keys = to_tightly_braced_doc("order_key: [", order_keys, "]").group();
                let limit_doc = format!(
                    "limit: {}",
                    if let Some(limit_num) = limit {
                        limit_num.to_string()
                    } else {
                        "None".to_string()
                    }
                );
                let offset_doc = format!("offset: {}", offset);
                to_braced_doc(
                    "TopK {",
                    to_doc!(
                        group_keys, ",", Space, order_keys, ",", Space, limit_doc, ",", Space,
                        offset_doc, ",", Space, input
                    ),
                    "}",
                )
            }
            RelationExpr::Negate { input } => {
                to_braced_doc("Negate {", input.to_doc(id_humanizer), "}")
            }
            RelationExpr::Threshold { input } => {
                to_braced_doc("Threshold {", input.to_doc(id_humanizer), "}")
            }
            RelationExpr::Union { left, right } => {
                let left = left.to_doc(id_humanizer);
                let right = right.to_doc(id_humanizer);
                to_braced_doc("Union {", to_doc!(left, ",", Space, right), "}")
            }
            RelationExpr::ArrangeBy { input, keys } => {
                let input = input.to_doc(id_humanizer);
                let keys = compact_intersperse_doc(tighten_outputs(keys), to_doc!(",", Space));
                let keys = to_tightly_braced_doc("columns: [", keys, "]").group();
                to_braced_doc("ArrangeBy {", to_doc!(keys, ",", Space, input), "}")
            }
        };

        // INVARIANT: RelationExpr's document is grouped. Much of the code above depends on this!
        doc.group()
    }

    /// Pretty-print this RelationExpr to a string with 70 columns maximum.
    pub fn pretty(&self, id_humanizer: &impl IdHumanizer) -> String {
        format!("{}", self.to_doc(id_humanizer).pretty(70))
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
        let keys = self;
        assert_eq!(keys_and_values.arity() - keys.arity(), default.len());
        keys_and_values
            .let_in(id_gen, |_id_gen, get_keys_and_values| {
                Ok(get_keys_and_values
                    .clone()
                    .distinct_by((0..keys.arity()).collect())
                    .negate()
                    .union(keys)
                    // This join is logically equivalent to
                    // `.map(<default_expr>)`, but using a join allows for
                    // potential predicate pushdown and elision in the
                    // optimizer.
                    .product(RelationExpr::constant(
                        vec![default.iter().map(|(datum, _)| datum.clone()).collect()],
                        RelationType::new(default.iter().map(|(_, typ)| *typ).collect()),
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

///Specification for an order by column
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ColumnOrder {
    /// the column number
    pub column: usize,
    /// Whether to sort in descending order
    pub desc: bool,
}

impl<'a> From<&'a ColumnOrder> for Doc<'a, BoxDoc<'a, ()>> {
    fn from(column_order: &'a ColumnOrder) -> Doc<'a, BoxDoc<'a, ()>> {
        Doc::text(format!(
            "#{} {}",
            column_order.column.to_string(),
            if column_order.desc { "desc" } else { "asc" }
        ))
    }
}

/// Manages the allocation of locally unique IDs when building a [`RelationExpr`].
#[derive(Debug, Default)]
pub struct IdGen {
    id: usize,
}

impl IdGen {
    fn allocate_id(&mut self) -> usize {
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

    /// Converts this [`AggregateExpr`] to a [`Doc`] or document for pretty
    /// printing. See [`RelationExpr::to_doc`] for details on the approach.
    pub fn to_doc(&self) -> Doc<BoxDoc<()>> {
        let args = if self.distinct {
            to_doc!("distinct", Doc::space(), &self.expr)
        } else {
            self.expr.to_doc()
        };
        let call = to_tightly_braced_doc("(", args, ")").group();
        to_doc!(&self.func, call)
    }
}

impl<'a> From<&'a AggregateExpr> for Doc<'a, BoxDoc<'a, ()>, ()> {
    fn from(s: &'a AggregateExpr) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        s.to_doc()
    }
}

#[cfg(test)]
mod tests {
    use repr::ScalarType;

    use super::*;
    use crate::id::test_utils::DummyHumanizer;

    fn constant(rows: Vec<Vec<i64>>) -> RelationExpr {
        let rows = rows
            .into_iter()
            .map(|row| row.into_iter().map(|i| Datum::Int64(i)).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let types = if rows.is_empty() {
            RelationType::new(Vec::new())
        } else {
            RelationType::new(
                (0..rows[0].len())
                    .map(|_| ColumnType::new(ScalarType::Int64))
                    .collect(),
            )
        };
        RelationExpr::constant(rows, types)
    }

    fn base() -> RelationExpr {
        constant(vec![])
    }

    #[test]
    fn test_pretty_constant() {
        assert_eq!(
            constant(vec![])
                .to_doc(&DummyHumanizer)
                .pretty(72)
                .to_string(),
            "Constant []"
        );
        assert_eq!(
            constant(vec![vec![]])
                .to_doc(&DummyHumanizer)
                .pretty(72)
                .to_string(),
            "Constant [[]]"
        );

        assert_eq!(
            constant(vec![vec![1]])
                .to_doc(&DummyHumanizer)
                .pretty(72)
                .to_string(),
            "Constant [[1]]"
        );

        assert_eq!(
            constant(vec![vec![1], vec![2]])
                .to_doc(&DummyHumanizer)
                .pretty(72)
                .to_string(),
            "Constant [[1], [2]]"
        );

        assert_eq!(
            constant(vec![vec![1, 2]])
                .to_doc(&DummyHumanizer)
                .pretty(72)
                .to_string(),
            "Constant [[1, 2]]"
        );

        assert_eq!(
            constant(vec![vec![1, 2], vec![1, 2]])
                .to_doc(&DummyHumanizer)
                .pretty(72)
                .to_string(),
            "Constant [[1, 2], [1, 2]]"
        );

        assert_eq!(
            constant(vec![vec![1, 2], vec![1, 2]])
                .to_doc(&DummyHumanizer)
                .pretty(16)
                .to_string(),
            "Constant [
  [1, 2],
  [1, 2]
]"
        );
    }

    #[test]
    fn test_pretty_let() {
        let c1 = constant(vec![vec![13]]);
        let c2 = constant(vec![vec![42]]);
        let c3 = constant(vec![vec![665]]);
        let binding = RelationExpr::Let {
            id: LocalId::new(1),
            value: Box::new(RelationExpr::Let {
                id: LocalId::new(2),
                value: Box::new(c1),
                body: Box::new(c2),
            }),
            body: Box::new(c3),
        };

        assert_eq!(
            binding.to_doc(&DummyHumanizer).pretty(100).to_string(),
            r#"Let { l1 = Let { l2 = Constant [[13]] } in Constant [[42]] } in Constant [[665]]"#
        );

        assert_eq!(
            binding.to_doc(&DummyHumanizer).pretty(28).to_string(),
            r#"Let {
  l1 = Let {
    l2 = Constant [[13]]
  } in
  Constant [[42]]
} in
Constant [[665]]"#
        );
    }

    #[test]
    fn test_pretty_project() {
        let project = RelationExpr::Project {
            outputs: vec![0, 1, 2, 3, 4],
            input: Box::new(base()),
        };

        assert_eq!(
            project.to_doc(&DummyHumanizer).pretty(82).to_string(),
            "Project { outputs: [0 .. 4], Constant [] }",
        );

        assert_eq!(
            project.to_doc(&DummyHumanizer).pretty(14).to_string(),
            "Project {
  outputs: [
    0 .. 4
  ],
  Constant []
}",
        );
    }

    #[test]
    fn test_pretty_map() {
        let map = RelationExpr::Map {
            scalars: vec![(ScalarExpr::Column(0)), (ScalarExpr::Column(1))],
            input: Box::new(base()),
        };

        assert_eq!(
            map.to_doc(&DummyHumanizer).pretty(82).to_string(),
            "Map { scalars: [#0, #1], Constant [] }",
        );

        assert_eq!(
            map.to_doc(&DummyHumanizer).pretty(16).to_string(),
            "Map {
  scalars: [
    #0,
    #1
  ],
  Constant []
}",
        );
    }

    #[test]
    fn test_pretty_filter() {
        let filter = RelationExpr::Filter {
            predicates: vec![ScalarExpr::Column(0), ScalarExpr::Column(1)],
            input: Box::new(base()),
        };

        assert_eq!(
            filter.to_doc(&DummyHumanizer).pretty(82).to_string(),
            "Filter { predicates: [#0, #1], Constant [] }",
        );

        assert_eq!(
            filter.to_doc(&DummyHumanizer).pretty(20).to_string(),
            "Filter {
  predicates: [
    #0,
    #1
  ],
  Constant []
}",
        );
    }

    #[test]
    fn test_pretty_join() {
        let join = RelationExpr::join(
            vec![base(), base()],
            vec![vec![(0, 0), (1, 0)], vec![(0, 1), (1, 1)]],
        );

        assert_eq!(
            join.to_doc(&DummyHumanizer).pretty(82).to_string(),
            "Join { variables: [[(0, 0), (1, 0)], [(0, 1), (1, 1)]], Constant [], Constant [] }",
        );

        assert_eq!(
            join.to_doc(&DummyHumanizer).pretty(48).to_string(),
            "Join {
  variables: [
    [(0, 0), (1, 0)],
    [(0, 1), (1, 1)]
  ],
  Constant [],
  Constant []
}",
        );
    }

    #[test]
    fn test_pretty_reduce() {
        let agg0 = AggregateExpr {
            func: AggregateFunc::SumInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let agg1 = AggregateExpr {
            func: AggregateFunc::MaxInt64,
            expr: ScalarExpr::Column(1),
            distinct: true,
        };

        let reduce = RelationExpr::Reduce {
            input: Box::new(base()),
            group_key: vec![1, 2],
            aggregates: vec![agg0, agg1],
        };

        assert_eq!(
            reduce.to_doc(&DummyHumanizer).pretty(84).to_string(),
            "Reduce { group_key: [1, 2], aggregates: [sum(#0), max(distinct #1)], Constant [] }",
        );

        assert_eq!(
            reduce.to_doc(&DummyHumanizer).pretty(16).to_string(),
            "Reduce {
  group_key: [
    1, 2
  ],
  aggregates: [
    sum(#0),
    max(
      distinct
      #1
    )
  ],
  Constant []
}",
        );
    }
}
