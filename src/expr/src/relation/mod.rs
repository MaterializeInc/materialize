// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(missing_docs)]

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use lowertest::MzEnumReflect;
use ore::collections::CollectionExt;
use repr::{ColumnType, Datum, RelationType, Row};

use self::func::{AggregateFunc, TableFunc};
use crate::explain::ViewExplanation;
use crate::{
    DummyHumanizer, EvalError, ExprHumanizer, GlobalId, Id, LocalId, MirScalarExpr, UnaryFunc,
};

pub mod canonicalize;
pub mod func;
pub mod join_input_mapper;

/// An abstract syntax tree which defines a collection.
///
/// The AST is meant reflect the capabilities of the `differential_dataflow::Collection` type,
/// written generically enough to avoid run-time compilation work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzEnumReflect)]
pub enum MirRelationExpr {
    /// A constant relation containing specified rows.
    ///
    /// The runtime memory footprint of this operator is zero.
    Constant {
        /// Rows of the constant collection and their multiplicities.
        rows: Result<Vec<(Row, isize)>, EvalError>,
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
        value: Box<MirRelationExpr>,
        /// The result of the `Let`, evaluated with `name` bound to `value`.
        body: Box<MirRelationExpr>,
    },
    /// Project out some columns from a dataflow
    ///
    /// The runtime memory footprint of this operator is zero.
    Project {
        /// The source collection.
        input: Box<MirRelationExpr>,
        /// Indices of columns to retain.
        outputs: Vec<usize>,
    },
    /// Append new columns to a dataflow
    ///
    /// The runtime memory footprint of this operator is zero.
    Map {
        /// The source collection.
        input: Box<MirRelationExpr>,
        /// Expressions which determine values to append to each row.
        /// An expression may refer to columns in `input` or
        /// expressions defined earlier in the vector
        scalars: Vec<MirScalarExpr>,
    },
    /// Like Map, but yields zero-or-more output rows per input row
    ///
    /// The runtime memory footprint of this operator is zero.
    FlatMap {
        /// The source collection
        input: Box<MirRelationExpr>,
        /// The table func to apply
        func: TableFunc,
        /// The argument to the table func
        exprs: Vec<MirScalarExpr>,
        /// Output columns demanded by the surrounding expression.
        ///
        /// The input columns are often discarded and can be very
        /// expensive to reproduce, so restricting what we produce
        /// as output can be a substantial win.
        ///
        /// See `transform::Demand` for more details.
        #[serde(default)]
        demand: Option<Vec<usize>>,
    },
    /// Keep rows from a dataflow where all the predicates are true
    ///
    /// The runtime memory footprint of this operator is zero.
    Filter {
        /// The source collection.
        input: Box<MirRelationExpr>,
        /// Predicates, each of which must be true.
        predicates: Vec<MirScalarExpr>,
    },
    /// Join several collections, where some columns must be equal.
    ///
    /// For further details consult the documentation for [`MirRelationExpr::join`].
    ///
    /// The runtime memory footprint of this operator can be proportional to
    /// the sizes of all inputs and the size of all joins of prefixes.
    /// This may be reduced due to arrangements available at rendering time.
    Join {
        /// A sequence of input relations.
        inputs: Vec<MirRelationExpr>,
        /// A sequence of equivalence classes of expressions on the cross product of inputs.
        ///
        /// Each equivalence class is a list of scalar expressions, where for each class the
        /// intended interpretation is that all evaluated expressions should be equal.
        ///
        /// Each scalar expression is to be evaluated over the cross-product of all records
        /// from all inputs. In many cases this may just be column selection from specific
        /// inputs, but more general cases exist (e.g. complex functions of multiple columns
        /// from multiple inputs, or just constant literals).
        equivalences: Vec<Vec<MirScalarExpr>>,
        /// This optional field is a hint for which columns are
        /// actually used by operators that use this collection. Although the
        /// join does not have permission to change the schema, it can introduce
        /// dummy values at the end of its computation, avoiding the maintenance of values
        /// not present in this list (when it is non-None).
        ///
        /// See `transform::Demand` for more details.
        #[serde(default)]
        demand: Option<Vec<usize>>,
        /// Join implementation information.
        #[serde(default)]
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
        input: Box<MirRelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<MirScalarExpr>,
        /// Expressions which determine values to append to each row, after the group keys.
        aggregates: Vec<AggregateExpr>,
        /// True iff the input is known to monotonically increase (only addition of records).
        #[serde(default)]
        monotonic: bool,
        /// User hint: expected number of values per group key. Used to optimize physical rendering.
        #[serde(default)]
        expected_group_size: Option<usize>,
    },
    /// Groups and orders within each group, limiting output.
    ///
    /// The runtime memory footprint of this operator is proportional to its input and output.
    TopK {
        /// The source collection.
        input: Box<MirRelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<usize>,
        /// Column indices used to order rows within groups.
        order_key: Vec<ColumnOrder>,
        /// Number of records to retain
        #[serde(default)]
        limit: Option<usize>,
        /// Number of records to skip
        #[serde(default)]
        offset: usize,
        /// True iff the input is known to monotonically increase (only addition of records).
        #[serde(default)]
        monotonic: bool,
    },
    /// Return a dataflow where the row counts are negated
    ///
    /// The runtime memory footprint of this operator is zero.
    Negate {
        /// The source collection.
        input: Box<MirRelationExpr>,
    },
    /// Keep rows from a dataflow where the row counts are positive
    ///
    /// The runtime memory footprint of this operator is proportional to its input and output.
    Threshold {
        /// The source collection.
        input: Box<MirRelationExpr>,
    },
    /// Adds the frequencies of elements in contained sets.
    ///
    /// The runtime memory footprint of this operator is zero.
    Union {
        /// A source collection.
        base: Box<MirRelationExpr>,
        /// Source collections to union.
        inputs: Vec<MirRelationExpr>,
    },
    /// Technically a no-op. Used to render an index. Will be used to optimize queries
    /// on finer grain
    ///
    /// The runtime memory footprint of this operator is proportional to its input.
    ArrangeBy {
        /// The source collection
        input: Box<MirRelationExpr>,
        /// Columns to arrange `input` by, in order of decreasing primacy
        keys: Vec<Vec<MirScalarExpr>>,
    },
    /// Declares that `keys` are primary keys for `input`.
    /// Should be used *very* sparingly, and only if there's no plausible
    /// way to derive the key information from the underlying expression.
    /// The result of declaring a key that isn't actually a key for the underlying expression is undefined.
    ///
    /// There is no operator rendered for this IR node; thus, its runtime memory footprint is zero.
    DeclareKeys {
        /// The source collection
        input: Box<MirRelationExpr>,
        /// The set of columns in the source collection that form a key.
        keys: Vec<Vec<usize>>,
    },
}

impl MirRelationExpr {
    /// Reports the schema of the relation.
    ///
    /// This method determines the type through recursive traversal of the
    /// relation expression, drawing from the types of base collections.
    /// As such, this is not an especially cheap method, and should be used
    /// judiciously.
    pub fn typ(&self) -> RelationType {
        match self {
            MirRelationExpr::Constant { rows, typ } => {
                if let Ok(rows) = rows {
                    let n_cols = typ.arity();
                    // If the `i`th entry is `Some`, then we have not yet observed non-uniqueness in the `i`th column.
                    let mut unique_values_per_col = vec![Some(HashSet::<Datum>::default()); n_cols];
                    for (row, _diff) in rows {
                        for (i, (datum, column_typ)) in
                            row.iter().zip(typ.column_types.iter()).enumerate()
                        {
                            // If the record will be observed, we should validate its type.
                            if datum != Datum::Dummy {
                                assert!(
                                    datum.is_instance_of(column_typ),
                                    "Expected datum of type {:?}, got value {:?}",
                                    column_typ,
                                    datum
                                );
                                if let Some(unique_vals) = &mut unique_values_per_col[i] {
                                    let is_dupe = !unique_vals.insert(datum);
                                    if is_dupe {
                                        unique_values_per_col[i] = None;
                                    }
                                }
                            }
                        }
                    }
                    if rows.len() == 0 || (rows.len() == 1 && rows[0].1 == 1) {
                        RelationType::new(typ.column_types.clone()).with_key(vec![])
                    } else {
                        // XXX - Multi-column keys are not detected.
                        typ.clone().with_keys(
                            unique_values_per_col
                                .into_iter()
                                .enumerate()
                                .filter(|(_idx, unique_vals)| unique_vals.is_some())
                                .map(|(idx, _)| vec![idx])
                                .collect(),
                        )
                    }
                } else {
                    typ.clone()
                }
            }
            MirRelationExpr::Get { typ, .. } => typ.clone(),
            MirRelationExpr::Let { body, .. } => body.typ(),
            MirRelationExpr::Project { input, outputs } => {
                let input_typ = input.typ();
                let mut output_typ = RelationType::new(
                    outputs
                        .iter()
                        .map(|&i| input_typ.column_types[i].clone())
                        .collect(),
                );
                for keys in input_typ.keys {
                    if keys.iter().all(|k| outputs.contains(k)) {
                        output_typ = output_typ.with_key(
                            keys.iter()
                                .map(|c| outputs.iter().position(|o| o == c).unwrap())
                                .collect(),
                        );
                    }
                }
                output_typ
            }
            MirRelationExpr::Map { input, scalars } => {
                let mut typ = input.typ();
                let arity = typ.column_types.len();

                let mut remappings = Vec::new();
                for (column, scalar) in scalars.iter().enumerate() {
                    typ.column_types.push(scalar.typ(&typ));
                    // assess whether the scalar preserves uniqueness,
                    // and could participate in a key!

                    fn uniqueness(expr: &MirScalarExpr) -> Option<usize> {
                        match expr {
                            MirScalarExpr::CallUnary { func, expr } => {
                                if func.preserves_uniqueness() {
                                    uniqueness(expr)
                                } else {
                                    None
                                }
                            }
                            MirScalarExpr::Column(c) => Some(*c),
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
                            new_key.sort_unstable();
                            new_keys.push(new_key);
                        }
                    }
                    for new_key in new_keys {
                        typ = typ.with_key(new_key);
                    }
                }

                typ
            }
            MirRelationExpr::FlatMap {
                input,
                func,
                exprs: _,
                demand: _,
            } => {
                let mut input_typ = input.typ();
                input_typ
                    .column_types
                    .extend(func.output_type().column_types);
                // FlatMap can add duplicate rows, so input keys are no longer valid
                let typ = RelationType::new(input_typ.column_types);
                typ
            }
            MirRelationExpr::Filter { input, predicates } => {
                // A filter inherits the keys of its input unless the filters
                // have reduced the input to a single row, in which case the
                // keys of the input are `()`.
                let mut input_typ = input.typ();
                let cols_equal_to_literal = predicates
                    .iter()
                    .filter_map(|p| {
                        if let MirScalarExpr::CallBinary {
                            func: crate::BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = p
                        {
                            if let MirScalarExpr::Column(c) = &**expr1 {
                                if expr2.is_literal_ok() {
                                    return Some(c);
                                }
                            }
                        }
                        None
                    })
                    .collect::<Vec<_>>();
                for key_set in &mut input_typ.keys {
                    key_set.retain(|k| !cols_equal_to_literal.contains(&k));
                }
                // Augment non-nullability of columns, by observing either
                // 1. Predicates that explicitly test for null values, and
                // 2. Columns that if null would make a predicate be null.
                let mut nonnull_required_columns = HashSet::new();
                for predicate in predicates {
                    // Add any columns that being null would force the predicate to be null.
                    // Should that happen, the row would be discarded.
                    predicate.non_null_requirements(&mut nonnull_required_columns);
                    // Test for explicit checks that a column is non-null.
                    if let MirScalarExpr::CallUnary {
                        func: UnaryFunc::Not,
                        expr,
                    } = predicate
                    {
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::IsNull,
                            expr,
                        } = &**expr
                        {
                            if let MirScalarExpr::Column(c) = &**expr {
                                input_typ.column_types[*c].nullable = false;
                            }
                        }
                    }
                }
                // Set as nonnull any columns where null values would cause
                // any predicate to evaluate to null.
                for column in nonnull_required_columns.into_iter() {
                    input_typ.column_types[column].nullable = false;
                }
                input_typ
            }
            MirRelationExpr::Join {
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

                // It is important the `new_from_input_types` constructor is
                // used. Otherwise, Materialize may potentially end up in an
                // infinite loop.
                let input_mapper =
                    join_input_mapper::JoinInputMapper::new_from_input_types(&input_types);

                let global_keys = input_mapper.global_keys(
                    &input_types
                        .iter()
                        .map(|t| t.keys.clone())
                        .collect::<Vec<_>>(),
                    equivalences,
                );

                for keys in global_keys {
                    typ = typ.with_key(keys.clone());
                }
                typ
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                ..
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
                        .all(|k| group_key.contains(&MirScalarExpr::Column(*k)))
                    {
                        keys.push(
                            key.iter()
                                .map(|i| {
                                    group_key
                                        .iter()
                                        .position(|k| k == &MirScalarExpr::Column(*i))
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
                    result = result.with_key(key);
                }
                result
            }
            MirRelationExpr::TopK {
                input,
                group_key,
                limit,
                ..
            } => {
                // If `limit` is `Some(1)` then the group key will become
                // a unique key, as there will be only one record with that key.
                let mut typ = input.typ();
                if limit == &Some(1) {
                    typ = typ.with_key(group_key.clone())
                }
                typ
            }
            MirRelationExpr::Negate { input } => {
                // Although negate may have distinct records for each key,
                // the multiplicity is -1 rather than 1. This breaks many
                // of the optimization uses of "keys".
                let mut typ = input.typ();
                typ.keys.clear();
                typ
            }
            MirRelationExpr::Threshold { input } => input.typ(),
            MirRelationExpr::Union { base, inputs } => {
                let mut base_cols = base.typ().column_types;
                for input in inputs {
                    for (base_col, col) in base_cols.iter_mut().zip_eq(input.typ().column_types) {
                        *base_col = base_col
                            .union(&col)
                            .map_err(|e| format!("{}\nIn {:#?}", e, self))
                            .unwrap();
                    }
                }

                // Generally, unions do not have any unique keys, because
                // each input might duplicate some. However, there is at
                // least one idiomatic structure that does preserve keys,
                // which results from SQL aggregations that must populate
                // absent records with default values. In that pattern,
                // the union of one GET with its negation, which has first
                // been subjected to a projection and map, we can remove
                // their influence on the key structure.
                //
                // If there are A, B, each with a unique `key` such that
                // we are looking at
                //
                //     A.proj(set_containg_key) + (B - A.proj(key)).map(stuff)
                //
                // Then we can report `key` as a unique key.
                //
                // TODO: make unique key structure an optimization analysis
                // rather than part of the type information.
                // TODO: perhaps ensure that (above) A.proj(key) is a
                // subset of B, as otherwise there are negative records
                // and who knows what is true (not expected, but again
                // who knows what the query plan might look like).

                let (base_projection, base_with_project_stripped) =
                    if let MirRelationExpr::Project { input, outputs } = &**base {
                        (outputs.clone(), &**input)
                    } else {
                        // A input without a project is equivalent to an input
                        // with the project being all columns in the input in order.
                        ((0..base_cols.len()).collect::<Vec<_>>(), &**base)
                    };
                let mut keys = Vec::new();
                if let MirRelationExpr::Get {
                    id: first_id,
                    typ: _,
                } = base_with_project_stripped
                {
                    if inputs.len() == 1 {
                        if let MirRelationExpr::Map { input, .. } = &inputs[0] {
                            if let MirRelationExpr::Union { base, inputs } = &**input {
                                if inputs.len() == 1 {
                                    if let MirRelationExpr::Project { input, outputs } = &**base {
                                        if let MirRelationExpr::Negate { input } = &**input {
                                            if let MirRelationExpr::Get {
                                                id: second_id,
                                                typ: _,
                                            } = &**input
                                            {
                                                if first_id == second_id {
                                                    keys.extend(
                                                        inputs[0].typ().keys.drain(..).filter(
                                                            |key| {
                                                                key.iter().all(|c| {
                                                                    outputs.get(*c) == Some(c)
                                                                        && base_projection.get(*c)
                                                                            == Some(c)
                                                                })
                                                            },
                                                        ),
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                RelationType::new(base_cols).with_keys(keys)
                // Important: do not inherit keys of either input, as not unique.
            }
            MirRelationExpr::ArrangeBy { input, .. } => input.typ(),
            MirRelationExpr::DeclareKeys { input, keys } => input.typ().with_keys(keys.clone()),
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
        MirRelationExpr::constant_diff(rows, typ)
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
        let rows = Ok(rows
            .into_iter()
            .map(move |(row, diff)| (Row::pack_slice(&row), diff))
            .collect());
        MirRelationExpr::Constant { rows, typ }
    }

    /// Constructs the expression for getting a global collection
    pub fn global_get(id: GlobalId, typ: RelationType) -> Self {
        MirRelationExpr::Get {
            id: Id::Global(id),
            typ,
        }
    }

    /// Retains only the columns specified by `output`.
    pub fn project(self, outputs: Vec<usize>) -> Self {
        MirRelationExpr::Project {
            input: Box::new(self),
            outputs,
        }
    }

    /// Append to each row the results of applying elements of `scalar`.
    pub fn map(self, scalars: Vec<MirScalarExpr>) -> Self {
        MirRelationExpr::Map {
            input: Box::new(self),
            scalars,
        }
    }

    /// Like `map`, but yields zero-or-more output rows per input row
    pub fn flat_map(self, func: TableFunc, exprs: Vec<MirScalarExpr>) -> Self {
        MirRelationExpr::FlatMap {
            input: Box::new(self),
            func,
            exprs,
            demand: None,
        }
    }

    /// Retain only the rows satisifying each of several predicates.
    pub fn filter<I>(self, predicates: I) -> Self
    where
        I: IntoIterator<Item = MirScalarExpr>,
    {
        MirRelationExpr::Filter {
            input: Box::new(self),
            predicates: predicates.into_iter().collect(),
        }
    }

    /// Form the Cartesian outer-product of rows in both inputs.
    pub fn product(self, right: Self) -> Self {
        MirRelationExpr::join(vec![self, right], vec![])
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
    /// use expr::MirRelationExpr;
    ///
    /// // A common schema for each input.
    /// let schema = RelationType::new(vec![
    ///     ScalarType::Int32.nullable(false),
    ///     ScalarType::Int32.nullable(false),
    /// ]);
    ///
    /// // the specific data are not important here.
    /// let data = vec![Datum::Int32(0), Datum::Int32(1)];
    ///
    /// // Three collections that could have been different.
    /// let input0 = MirRelationExpr::constant(vec![data.clone()], schema.clone());
    /// let input1 = MirRelationExpr::constant(vec![data.clone()], schema.clone());
    /// let input2 = MirRelationExpr::constant(vec![data.clone()], schema.clone());
    ///
    /// // Join the three relations looking for triangles, like so.
    /// //
    /// //     Output(A,B,C) := Input0(A,B), Input1(B,C), Input2(A,C)
    /// let joined = MirRelationExpr::join(
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
    pub fn join(inputs: Vec<MirRelationExpr>, variables: Vec<Vec<(usize, usize)>>) -> Self {
        let input_mapper = join_input_mapper::JoinInputMapper::new(&inputs);

        let equivalences = variables
            .into_iter()
            .map(|vs| {
                vs.into_iter()
                    .map(|(r, c)| input_mapper.map_expr_to_global(MirScalarExpr::Column(c), r))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Self::join_scalars(inputs, equivalences)
    }

    /// Constructs a join operator from inputs and required-equal scalar expressions.
    pub fn join_scalars(
        inputs: Vec<MirRelationExpr>,
        equivalences: Vec<Vec<MirScalarExpr>>,
    ) -> Self {
        MirRelationExpr::Join {
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
    pub fn reduce(
        self,
        group_key: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
        expected_group_size: Option<usize>,
    ) -> Self {
        MirRelationExpr::Reduce {
            input: Box::new(self),
            group_key: group_key.into_iter().map(MirScalarExpr::Column).collect(),
            aggregates,
            monotonic: false,
            expected_group_size,
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
        MirRelationExpr::TopK {
            input: Box::new(self),
            group_key,
            order_key,
            limit,
            offset,
            monotonic: false,
        }
    }

    /// Negates the occurrences of each row.
    pub fn negate(self) -> Self {
        MirRelationExpr::Negate {
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
        self.reduce(group_key, vec![], None)
    }

    /// Discards rows with a negative frequency.
    pub fn threshold(self) -> Self {
        MirRelationExpr::Threshold {
            input: Box::new(self),
        }
    }

    /// Unions together any number inputs.
    ///
    /// If `inputs` is empty, then an empty relation of type `typ` is
    /// constructed.
    pub fn union_many(mut inputs: Vec<Self>, typ: RelationType) -> Self {
        if inputs.len() == 0 {
            MirRelationExpr::Constant {
                rows: Ok(vec![]),
                typ,
            }
        } else if inputs.len() == 1 {
            inputs.into_element()
        } else {
            MirRelationExpr::Union {
                base: Box::new(inputs.remove(0)),
                inputs,
            }
        }
    }

    /// Produces one collection where each row is present with the sum of its frequencies in each input.
    pub fn union(self, other: Self) -> Self {
        MirRelationExpr::Union {
            base: Box::new(self),
            inputs: vec![other],
        }
    }

    /// Arranges the collection by the specified columns
    pub fn arrange_by(self, keys: &[Vec<MirScalarExpr>]) -> Self {
        MirRelationExpr::ArrangeBy {
            input: Box::new(self),
            keys: keys.to_owned(),
        }
    }

    /// Indicates if this is a constant empty collection.
    ///
    /// A false value does not mean the collection is known to be non-empty,
    /// only that we cannot currently determine that it is statically empty.
    pub fn is_empty(&self) -> bool {
        if let MirRelationExpr::Constant { rows: Ok(rows), .. } = self {
            rows.is_empty()
        } else {
            false
        }
    }

    /// Returns the distinct global identifiers on which this expression
    /// depends.
    ///
    /// See [`MirRelationExpr::global_uses_into`] to reuse an existing vector.
    pub fn global_uses(&self) -> Vec<GlobalId> {
        let mut out = vec![];
        self.global_uses_into(&mut out);
        out.sort();
        out.dedup();
        out
    }

    /// Appends global identifiers on which this expression depends to `out`.
    ///
    /// Unlike [`MirRelationExpr::global_uses`], this method does not deduplicate
    /// the global identifiers.
    pub fn global_uses_into(&self, out: &mut Vec<GlobalId>) {
        if let MirRelationExpr::Get {
            id: Id::Global(id), ..
        } = self
        {
            out.push(*id);
        }
        self.visit1(|expr| expr.global_uses_into(out))
    }

    /// Applies a fallible `f` to each child `MirRelationExpr`.
    pub fn try_visit1<'a, F, E>(&'a self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a Self) -> Result<(), E>,
    {
        match self {
            MirRelationExpr::Constant { .. } | MirRelationExpr::Get { .. } => (),
            MirRelationExpr::Let { value, body, .. } => {
                f(value)?;
                f(body)?;
            }
            MirRelationExpr::Project { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::Map { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::FlatMap { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::Filter { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input)?;
                }
            }
            MirRelationExpr::Reduce { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::TopK { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::Negate { input } => f(input)?,
            MirRelationExpr::Threshold { input } => f(input)?,
            MirRelationExpr::Union { base, inputs } => {
                f(base)?;
                for input in inputs {
                    f(input)?;
                }
            }
            MirRelationExpr::ArrangeBy { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::DeclareKeys { input, .. } => {
                f(input)?;
            }
        }
        Ok(())
    }

    /// Applies an infallible `f` to each child `MirRelationExpr`.
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

    /// Post-order fallible visitor for each `MirRelationExpr`.
    pub fn try_visit<'a, F, E>(&'a self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a Self) -> Result<(), E>,
    {
        self.try_visit1(|e| e.try_visit(f))?;
        f(self)
    }

    /// Post-order infallible visitor for each `MirRelationExpr`.
    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit(f));
        f(self)
    }

    /// Applies fallible `f` to each child `MirRelationExpr`.
    pub fn try_visit1_mut<'a, F, E>(&'a mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a mut Self) -> Result<(), E>,
    {
        match self {
            MirRelationExpr::Constant { .. } | MirRelationExpr::Get { .. } => (),
            MirRelationExpr::Let { value, body, .. } => {
                f(value)?;
                f(body)?;
            }
            MirRelationExpr::Project { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::Map { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::FlatMap { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::Filter { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input)?;
                }
            }
            MirRelationExpr::Reduce { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::TopK { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::Negate { input } => f(input)?,
            MirRelationExpr::Threshold { input } => f(input)?,
            MirRelationExpr::Union { base, inputs } => {
                f(base)?;
                for input in inputs {
                    f(input)?;
                }
            }
            MirRelationExpr::ArrangeBy { input, .. } => {
                f(input)?;
            }
            MirRelationExpr::DeclareKeys { input, .. } => {
                f(input)?;
            }
        }
        Ok(())
    }

    /// Applies infallible `f` to each child `MirRelationExpr`.
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

    /// Post-order fallible visitor for each `MirRelationExpr`.
    pub fn try_visit_mut<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
    {
        self.try_visit1_mut(|e| e.try_visit_mut(f))?;
        f(self)
    }

    /// Post-order infallible visitor for each `MirRelationExpr`.
    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut(f));
        f(self)
    }

    /// Pre-order fallible visitor for each `MirRelationExpr`.
    pub fn try_visit_mut_pre<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
    {
        f(self)?;
        self.try_visit1_mut(|e| e.try_visit_mut_pre(f))
    }

    /// Pre-order fallible visitor for each `MirRelationExpr`.
    pub fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e| e.visit_mut_pre(f))
    }

    /// Fallible visitor for the [`MirScalarExpr`]s in the relation expression.
    /// Note that this does not recurse into the `MirScalarExpr`s themselves.
    pub fn try_visit_scalars_mut<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut MirScalarExpr) -> Result<(), E>,
    {
        // Match written out explicitly to reduce the possibility of adding a
        // new field with a `MirScalarExpr` within and forgetting to account for it
        // here.
        self.try_visit_mut(&mut |e| e.try_visit_scalars_mut1(f))
    }

    /// Fallible visitor for the [`MirScalarExpr`]s directly owned by this relation expression.
    /// This does not recursively descend into owned [`MirRelationExpr`]s.
    pub fn try_visit_scalars_mut1<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut MirScalarExpr) -> Result<(), E>,
    {
        match self {
            MirRelationExpr::Map { scalars, input: _ }
            | MirRelationExpr::Filter {
                predicates: scalars,
                input: _,
            } => {
                for s in scalars {
                    f(s)?;
                }
                Ok(())
            }
            MirRelationExpr::FlatMap {
                exprs,
                input: _,
                func: _,
                demand: _,
            } => {
                for expr in exprs {
                    f(expr)?;
                }
                Ok(())
            }
            MirRelationExpr::Join {
                equivalences: keys,
                inputs: _,
                demand: _,
                implementation: _,
            }
            | MirRelationExpr::ArrangeBy { input: _, keys } => {
                for key in keys {
                    for s in key {
                        f(s)?;
                    }
                }
                Ok(())
            }
            MirRelationExpr::Reduce {
                group_key,
                aggregates,
                ..
            } => {
                for s in group_key {
                    f(s)?;
                }
                for agg in aggregates {
                    f(&mut agg.expr)?;
                }
                Ok(())
            }
            MirRelationExpr::Constant { rows: _, typ: _ }
            | MirRelationExpr::Get { id: _, typ: _ }
            | MirRelationExpr::Let {
                id: _,
                value: _,
                body: _,
            }
            | MirRelationExpr::Project {
                input: _,
                outputs: _,
            }
            | MirRelationExpr::TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
                monotonic: _,
            }
            | MirRelationExpr::Negate { input: _ }
            | MirRelationExpr::Threshold { input: _ }
            | MirRelationExpr::DeclareKeys { input: _, keys: _ }
            | MirRelationExpr::Union { base: _, inputs: _ } => Ok(()),
        }
    }

    /// Like `try_visit_scalars_mut`, but the closure must be infallible.
    pub fn visit_scalars_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut MirScalarExpr),
    {
        self.try_visit_scalars_mut(&mut |s| {
            f(s);
            Ok::<_, ()>(())
        })
        .unwrap()
    }

    /// Pretty-print this MirRelationExpr to a string.
    ///
    /// This method allows an additional `ExprHumanizer` which can annotate
    /// identifiers with human-meaningful names for the identifiers.
    pub fn pretty_humanized(&self, id_humanizer: &impl ExprHumanizer) -> String {
        ViewExplanation::new(self, id_humanizer).to_string()
    }

    /// Pretty-print this MirRelationExpr to a string.
    pub fn pretty(&self) -> String {
        ViewExplanation::new(self, &DummyHumanizer).to_string()
    }

    /// Pretty-print this MirRelationExpr to a string with type information.
    pub fn pretty_typed(&self) -> String {
        let mut explanation = ViewExplanation::new(self, &DummyHumanizer);
        explanation.explain_types();
        explanation.to_string()
    }

    /// Take ownership of `self`, leaving an empty `MirRelationExpr::Constant` with the correct type.
    pub fn take_safely(&mut self) -> MirRelationExpr {
        let typ = self.typ();
        std::mem::replace(
            self,
            MirRelationExpr::Constant {
                rows: Ok(vec![]),
                typ,
            },
        )
    }
    /// Take ownership of `self`, leaving an empty `MirRelationExpr::Constant` with an **incorrect** type.
    ///
    /// This should only be used if `self` is about to be dropped or otherwise overwritten.
    pub fn take_dangerous(&mut self) -> MirRelationExpr {
        let empty = MirRelationExpr::Constant {
            rows: Ok(vec![]),
            typ: RelationType::new(Vec::new()),
        };
        std::mem::replace(self, empty)
    }

    /// Replaces `self` with some logic applied to `self`.
    pub fn replace_using<F>(&mut self, logic: F)
    where
        F: FnOnce(MirRelationExpr) -> MirRelationExpr,
    {
        let empty = MirRelationExpr::Constant {
            rows: Ok(vec![]),
            typ: RelationType::new(Vec::new()),
        };
        let expr = std::mem::replace(self, empty);
        *self = logic(expr);
    }

    /// Store `self` in a `Let` and pass the corresponding `Get` to `body`
    pub fn let_in<Body>(self, id_gen: &mut IdGen, body: Body) -> super::MirRelationExpr
    where
        Body: FnOnce(&mut IdGen, MirRelationExpr) -> super::MirRelationExpr,
    {
        if let MirRelationExpr::Get { .. } = self {
            // already done
            body(id_gen, self)
        } else {
            let id = LocalId::new(id_gen.allocate_id());
            let get = MirRelationExpr::Get {
                id: Id::Local(id),
                typ: self.typ(),
            };
            let body = (body)(id_gen, get);
            MirRelationExpr::Let {
                id,
                value: Box::new(self),
                body: Box::new(body),
            }
        }
    }

    /// Return every row in `self` that does not have a matching row in the first columns of `keys_and_values`, using `default` to fill in the remaining columns
    /// (If `default` is a row of nulls, this is the 'outer' part of LEFT OUTER JOIN)
    pub fn anti_lookup(
        self,
        id_gen: &mut IdGen,
        keys_and_values: MirRelationExpr,
        default: Vec<(Datum, ColumnType)>,
    ) -> MirRelationExpr {
        assert_eq!(keys_and_values.arity() - self.arity(), default.len());
        self.let_in(id_gen, |_id_gen, get_keys| {
            MirRelationExpr::join(
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
            .product(MirRelationExpr::constant(
                vec![default.iter().map(|(datum, _)| *datum).collect()],
                RelationType::new(default.iter().map(|(_, typ)| typ.clone()).collect()),
            ))
        })
    }

    /// Return:
    /// * every row in keys_and_values
    /// * every row in `self` that does not have a matching row in the first columns of `keys_and_values`, using `default` to fill in the remaining columns
    /// (If `default` is a row of nulls, this is LEFT OUTER JOIN)
    pub fn lookup(
        self,
        id_gen: &mut IdGen,
        keys_and_values: MirRelationExpr,
        default: Vec<(Datum<'static>, ColumnType)>,
    ) -> MirRelationExpr {
        keys_and_values.let_in(id_gen, |id_gen, get_keys_and_values| {
            get_keys_and_values.clone().union(self.anti_lookup(
                id_gen,
                get_keys_and_values,
                default,
            ))
        })
    }

    /// Passes the collection through unchanged, but informs the optimizer that `keys` are primary keys.
    pub fn declare_keys(self, keys: Vec<Vec<usize>>) -> Self {
        Self::DeclareKeys {
            input: Box::new(self),
            keys,
        }
    }

    /// Returns whether this collection is just a `Get` wrapping an underlying bare source.
    pub fn is_trivial_source(&self) -> bool {
        matches!(
            self,
            MirRelationExpr::Get {
                id: Id::LocalBareSource,
                ..
            }
        )
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

/// Manages the allocation of locally unique IDs when building a [`MirRelationExpr`].
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AggregateExpr {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    pub expr: MirScalarExpr,
    /// Should the aggregation be applied only to distinct results in each group.
    pub distinct: bool,
}

impl AggregateExpr {
    /// Computes the type of this `AggregateExpr`.
    pub fn typ(&self, relation_type: &RelationType) -> ColumnType {
        self.func.output_type(self.expr.typ(relation_type))
    }

    /// Returns whether the expression has a constant result.
    pub fn is_constant(&self) -> bool {
        match self.func {
            AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxDecimal
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinDecimal
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
            | AggregateFunc::Any
            | AggregateFunc::All => self.expr.is_literal(),
            AggregateFunc::Count => self.expr.is_literal_null(),
            _ => self.expr.is_literal_err(),
        }
    }
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}({}{})",
            self.func,
            if self.distinct { "distinct " } else { "" },
            self.expr
        )
    }
}

/// Describe a join implementation in dataflow.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzEnumReflect)]
pub enum JoinImplementation {
    /// Perform a sequence of binary differential dataflow joins.
    ///
    /// The first argument indicates 1) the index of the starting collection
    /// and 2) if it should be arranged, the keys to arrange it by.
    /// The sequence that follows lists other relation indexes, and the key for
    /// the arrangement we should use when joining it in.
    ///
    /// Each collection index should occur exactly once, either in the first
    /// position or somewhere in the list.
    Differential(
        (usize, Option<Vec<MirScalarExpr>>),
        Vec<(usize, Vec<MirScalarExpr>)>,
    ),
    /// Perform independent delta query dataflows for each input.
    ///
    /// The argument is a sequence of plans, for the input collections in order.
    /// Each plan starts from the corresponding index, and then in sequence joins
    /// against collections identified by index and with the specified arrangement key.
    DeltaQuery(Vec<Vec<(usize, Vec<MirScalarExpr>)>>),
    /// No implementation yet selected.
    Unimplemented,
}

impl Default for JoinImplementation {
    fn default() -> Self {
        JoinImplementation::Unimplemented
    }
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
    pub fn is_trivial(&self, arity: usize) -> bool {
        self.limit.is_none()
            && self.order_by.is_empty()
            && self.offset == 0
            && self.project.iter().copied().eq(0..arity)
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
            let mut row_packer = Row::default();
            for row in rows {
                let datums = row.unpack();
                row_packer.extend(self.project.iter().map(|i| &datums[*i]));
                *row = row_packer.finish_and_reuse();
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
