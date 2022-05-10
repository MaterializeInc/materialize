// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::num::NonZeroUsize;

use itertools::Itertools;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::IdGen;
use mz_ore::stack::RecursionLimitError;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::proto::{ProtoRepr, TryFromProtoError, TryIntoIfSome};
use mz_repr::{ColumnName, ColumnType, Datum, Diff, GlobalId, RelationType, Row, ScalarType};

use self::func::{AggregateFunc, LagLeadType, TableFunc};
use crate::explain::ViewExplanation;
use crate::visit::{Visit, VisitChildren};
use crate::{
    func as scalar_func, DummyHumanizer, EvalError, ExprHumanizer, Id, LocalId, MirScalarExpr,
    UnaryFunc, VariadicFunc,
};

pub mod canonicalize;
pub mod func;
pub mod join_input_mapper;

include!(concat!(env!("OUT_DIR"), "/mz_expr.relation.rs"));

/// A recursion limit to be used for stack-safe traversals of [`MirRelationExpr`] trees.
///
/// The recursion limit must be large enough to accommodate for the linear representation
/// of some pathological but frequently occurring query fragments.
///
/// For example, in MIR we could have long chains of
/// - (1) `Let` bindings,
/// - (2) `CallBinary` calls with associative functions such as `OR` and `+`
///
/// Until we fix those, we need to stick with the larger recursion limit.
pub const RECURSION_LIMIT: usize = 2048;

/// A trait for types that describe how to build a collection.
pub trait CollectionPlan {
    /// Appends global identifiers on which this plan depends to `out`.
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>);

    /// Returns the global identifiers on which this plan depends.
    ///
    /// See [`CollectionPlan::depends_on_into`] to reuse an existing `BTreeSet`.
    fn depends_on(&self) -> BTreeSet<GlobalId> {
        let mut out = BTreeSet::new();
        self.depends_on_into(&mut out);
        out
    }
}

/// An abstract syntax tree which defines a collection.
///
/// The AST is meant reflect the capabilities of the `differential_dataflow::Collection` type,
/// written generically enough to avoid run-time compilation work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub enum MirRelationExpr {
    /// A constant relation containing specified rows.
    ///
    /// The runtime memory footprint of this operator is zero.
    Constant {
        /// Rows of the constant collection and their multiplicities.
        rows: Result<Vec<(Row, Diff)>, EvalError>,
        /// Schema of the collection.
        typ: RelationType,
    },
    /// Get an existing dataflow.
    ///
    /// The runtime memory footprint of this operator is zero.
    Get {
        /// The identifier for the collection to load.
        #[mzreflect(ignore)]
        id: Id,
        /// Schema of the collection.
        typ: RelationType,
    },
    /// Introduce a temporary dataflow.
    ///
    /// The runtime memory footprint of this operator is zero.
    Let {
        /// The identifier to be used in `Get` variants to retrieve `value`.
        #[mzreflect(ignore)]
        id: LocalId,
        /// The collection to be bound to `id`.
        value: Box<MirRelationExpr>,
        /// The result of the `Let`, evaluated with `id` bound to `value`.
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
}

impl MirRelationExpr {
    /// Reports the schema of the relation.
    ///
    /// This method determines the type through recursive traversal of the
    /// relation expression, drawing from the types of base collections.
    /// As such, this is not an especially cheap method, and should be used
    /// judiciously.
    ///
    /// The relation type is computed incrementally with a recursive post-order
    /// traversal, that accumulates the input types for the relations yet to be
    /// visited in `type_stack`.
    pub fn typ(&self) -> RelationType {
        let mut type_stack = Vec::new();
        #[allow(deprecated)]
        self.visit_pre_post_nolimit(
            &mut |e: &MirRelationExpr| -> Option<Vec<&MirRelationExpr>> {
                if let MirRelationExpr::Let { body, .. } = &e {
                    // Do not traverse the value sub-graph, since it's not relevant for
                    // determining the relation type of Let operators.
                    Some(vec![&*body])
                } else {
                    None
                }
            },
            &mut |e: &MirRelationExpr| {
                if let MirRelationExpr::Let { .. } = &e {
                    let body_typ = type_stack.pop().unwrap();
                    // Insert a dummy relation type for the value, since `typ_with_input_types`
                    // won't look at it, but expects the relation type of the body to be second.
                    type_stack.push(RelationType::empty());
                    type_stack.push(body_typ);
                }
                let num_inputs = e.num_inputs();
                let relation_type =
                    e.typ_with_input_types(&type_stack[type_stack.len() - num_inputs..]);
                type_stack.truncate(type_stack.len() - num_inputs);
                type_stack.push(relation_type);
            },
        );
        assert_eq!(type_stack.len(), 1);
        type_stack.pop().unwrap()
    }

    /// Reports the schema of the relation given the schema of the input relations.
    ///
    /// `input_types` is required to contain the schemas for the input relations of
    /// the current relation in the same order as they are visited by `try_visit1`
    /// method, even though not all may be used for computing the schema of the
    /// current relation. For example, `Let` expects two input types, one for the
    /// value relation and one for the body, in that order, but only the one for the
    /// body is used to determine the type of the `Let` relation.
    ///
    /// It is meant to be used during post-order traversals to compute relation
    /// schemas incrementally.
    pub fn typ_with_input_types(&self, input_types: &[RelationType]) -> RelationType {
        assert_eq!(self.num_inputs(), input_types.len());
        match self {
            MirRelationExpr::Constant { rows, typ } => {
                if let Ok(rows) = rows {
                    let n_cols = typ.arity();
                    // If the `i`th entry is `Some`, then we have not yet observed non-uniqueness in the `i`th column.
                    let mut unique_values_per_col = vec![Some(HashSet::<Datum>::default()); n_cols];
                    for (row, diff) in rows {
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
                                    let is_dupe = *diff != 1 || !unique_vals.insert(datum);
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
            MirRelationExpr::Let { .. } => input_types.last().unwrap().clone(),
            MirRelationExpr::Project { input: _, outputs } => {
                let input_typ = &input_types[0];
                let mut output_typ = RelationType::new(
                    outputs
                        .iter()
                        .map(|&i| input_typ.column_types[i].clone())
                        .collect(),
                );
                for keys in input_typ.keys.iter() {
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
            MirRelationExpr::Map { scalars, .. } => {
                let mut typ = input_types[0].clone();
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
            MirRelationExpr::FlatMap { func, .. } => {
                let mut input_typ = input_types[0].clone();
                input_typ
                    .column_types
                    .extend(func.output_type().column_types);
                // FlatMap can add duplicate rows, so input keys are no longer valid
                let typ = RelationType::new(input_typ.column_types);
                typ
            }
            MirRelationExpr::Filter { predicates, .. } => {
                // A filter inherits the keys of its input unless the filters
                // have reduced the input to a single row, in which case the
                // keys of the input are `()`.
                let mut input_typ = input_types[0].clone();
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
                if !input_typ.keys.is_empty() {
                    // If `[0 1]` is an input key and there `#0 = #1` exists as a
                    // predicate, we should present both `[0]` and `[1]` as keys
                    // of the output. Also, if there is a key involving X column
                    // and an equality between X and another column Y, a variant
                    // of that key with Y instead of X should be presented as
                    // a key of the output.

                    // First, we build an iterator over the equivalences
                    let classes = predicates.iter().filter_map(|p| {
                        if let MirScalarExpr::CallBinary {
                            func: crate::BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = p
                        {
                            if let Some(c1) = expr1.as_column() {
                                if let Some(c2) = expr2.as_column() {
                                    return Some((c1, c2));
                                }
                            }
                        }
                        None
                    });

                    // Keep doing replacements until the number of keys settles
                    let mut prev_keys: HashSet<_> = input_typ.keys.drain(..).collect();
                    let mut prev_keys_size = 0;
                    while prev_keys_size != prev_keys.len() {
                        prev_keys_size = prev_keys.len();
                        for (c1, c2) in classes.clone() {
                            let mut new_keys = HashSet::new();
                            for key in prev_keys.into_iter() {
                                let contains_c1 = key.contains(&c1);
                                let contains_c2 = key.contains(&c2);
                                if contains_c1 && contains_c2 {
                                    new_keys.insert(
                                        key.iter().filter(|c| **c != c1).cloned().collect_vec(),
                                    );
                                    new_keys.insert(
                                        key.iter().filter(|c| **c != c2).cloned().collect_vec(),
                                    );
                                } else {
                                    if contains_c1 {
                                        new_keys.insert(
                                            key.iter()
                                                .map(|c| if *c == c1 { c2 } else { *c })
                                                .sorted()
                                                .collect_vec(),
                                        );
                                    } else if contains_c2 {
                                        new_keys.insert(
                                            key.iter()
                                                .map(|c| if *c == c2 { c1 } else { *c })
                                                .sorted()
                                                .collect_vec(),
                                        );
                                    }
                                    new_keys.insert(key);
                                }
                            }
                            prev_keys = new_keys;
                        }
                    }

                    input_typ.keys = prev_keys.into_iter().sorted().collect_vec();
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
                        func: UnaryFunc::Not(scalar_func::Not),
                        expr,
                    } = predicate
                    {
                        if let MirScalarExpr::CallUnary {
                            func: UnaryFunc::IsNull(scalar_func::IsNull),
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
            MirRelationExpr::Join { equivalences, .. } => {
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
                    join_input_mapper::JoinInputMapper::new_from_input_types(input_types);

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
                group_key,
                aggregates,
                ..
            } => {
                let input_typ = &input_types[0];
                let mut column_types = group_key
                    .iter()
                    .map(|e| e.typ(input_typ))
                    .collect::<Vec<_>>();
                for agg in aggregates {
                    column_types.push(agg.typ(input_typ));
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
                group_key, limit, ..
            } => {
                // If `limit` is `Some(1)` then the group key will become
                // a unique key, as there will be only one record with that key.
                let mut typ = input_types[0].clone();
                if limit == &Some(1) {
                    typ = typ.with_key(group_key.clone())
                }
                typ
            }
            MirRelationExpr::Negate { input: _ } => {
                // Although negate may have distinct records for each key,
                // the multiplicity is -1 rather than 1. This breaks many
                // of the optimization uses of "keys".
                let mut typ = input_types[0].clone();
                typ.keys.clear();
                typ
            }
            MirRelationExpr::Threshold { .. } => input_types[0].clone(),
            MirRelationExpr::Union { base, inputs } => {
                let mut base_cols = input_types[0].column_types.clone();
                for input_type in input_types.iter().skip(1) {
                    for (base_col, col) in
                        base_cols.iter_mut().zip_eq(input_type.column_types.iter())
                    {
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
                //     A.proj(set_containing_key) + (B - A.proj(key)).map(stuff)
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
            MirRelationExpr::ArrangeBy { .. } => input_types[0].clone(),
        }
    }

    /// The number of columns in the relation.
    ///
    /// This number is determined from the type, which is determined recursively
    /// at non-trivial cost.
    pub fn arity(&self) -> usize {
        match self {
            MirRelationExpr::Constant { rows: _, typ } => typ.arity(),
            MirRelationExpr::Get { typ, .. } => typ.arity(),
            MirRelationExpr::Let { body, .. } => body.arity(),
            MirRelationExpr::Project { input: _, outputs } => outputs.len(),
            MirRelationExpr::Map { input, scalars } => input.arity() + scalars.len(),
            MirRelationExpr::FlatMap { input, func, .. } => {
                input.arity() + func.output_type().column_types.len()
            }
            MirRelationExpr::Filter { input, .. } => input.arity(),
            MirRelationExpr::Join { inputs, .. } => inputs.iter().map(|i| i.arity()).sum(),
            MirRelationExpr::Reduce {
                input: _,
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
            MirRelationExpr::TopK { input, .. } => input.arity(),
            MirRelationExpr::Negate { input } => input.arity(),
            MirRelationExpr::Threshold { input } => input.arity(),
            MirRelationExpr::Union { base, inputs: _ } => base.arity(),
            MirRelationExpr::ArrangeBy { input, .. } => input.arity(),
        }
    }

    /// The number of child relations this relation has.
    pub fn num_inputs(&self) -> usize {
        let mut count = 0;

        self.visit_children(|_| count += 1);

        count
    }

    /// Constructs a constant collection from specific rows and schema, where
    /// each row will have a multiplicity of one.
    pub fn constant(rows: Vec<Vec<Datum>>, typ: RelationType) -> Self {
        let rows = rows.into_iter().map(|row| (row, 1)).collect();
        MirRelationExpr::constant_diff(rows, typ)
    }

    /// Constructs a constant collection from specific rows and schema, where
    /// each row can have an arbitrary multiplicity.
    pub fn constant_diff(rows: Vec<(Vec<Datum>, Diff)>, typ: RelationType) -> Self {
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

    /// Append to each row a single `scalar`.
    pub fn map_one(self, scalar: MirScalarExpr) -> Self {
        MirRelationExpr::Map {
            input: Box::new(self),
            scalars: vec![scalar],
        }
    }

    /// Like `map`, but yields zero-or-more output rows per input row
    pub fn flat_map(self, func: TableFunc, exprs: Vec<MirScalarExpr>) -> Self {
        MirRelationExpr::FlatMap {
            input: Box::new(self),
            func,
            exprs,
        }
    }

    /// Retain only the rows satisfying each of several predicates.
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
    /// use mz_repr::{Datum, ColumnType, RelationType, ScalarType};
    /// use mz_expr::MirRelationExpr;
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

    /// Print this MirRelationExpr to a JSON-formatted string.
    pub fn json(&self) -> String {
        serde_json::to_string(self).unwrap()
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
        default: Vec<(Datum, ScalarType)>,
    ) -> MirRelationExpr {
        let (data, column_types): (Vec<_>, Vec<_>) = default
            .into_iter()
            .map(|(datum, scalar_type)| (datum, scalar_type.nullable(datum.is_null())))
            .unzip();
        assert_eq!(keys_and_values.arity() - self.arity(), data.len());
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
                vec![data],
                RelationType::new(column_types),
            ))
        })
    }

    /// Return:
    /// * every row in keys_and_values
    /// * every row in `self` that does not have a matching row in the first columns of `keys_and_values`, using `default` to fill in the remaining columns
    /// (This is LEFT OUTER JOIN if:
    /// 1) `default` is a row of null
    /// 2) matching rows in `keys_and_values` and `self` have the same multiplicity.)
    pub fn lookup(
        self,
        id_gen: &mut IdGen,
        keys_and_values: MirRelationExpr,
        default: Vec<(Datum<'static>, ScalarType)>,
    ) -> MirRelationExpr {
        keys_and_values.let_in(id_gen, |id_gen, get_keys_and_values| {
            get_keys_and_values.clone().union(self.anti_lookup(
                id_gen,
                get_keys_and_values,
                default,
            ))
        })
    }

    /// True iff the expression contains a `NullaryFunc::MzLogicalTimestamp`.
    pub fn contains_temporal(&mut self) -> bool {
        let mut contains = false;
        self.visit_scalars_mut(&mut |e| contains = contains || e.contains_temporal());
        contains
    }

    /// Fallible visitor for the [`MirScalarExpr`]s directly owned by this relation expression.
    ///
    /// The `f` visitor should not recursively descend into owned [`MirRelationExpr`]s.
    pub fn try_visit_scalars_mut1<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut MirScalarExpr) -> Result<(), E>,
    {
        use MirRelationExpr::*;
        match self {
            Map { scalars, .. } => {
                for s in scalars {
                    f(s)?;
                }
            }
            Filter { predicates, .. } => {
                for p in predicates {
                    f(p)?;
                }
            }
            FlatMap { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            Join { equivalences, .. } => {
                for equivalence in equivalences {
                    for expr in equivalence {
                        f(expr)?;
                    }
                }
            }
            ArrangeBy { keys, .. } => {
                for key in keys {
                    for s in key {
                        f(s)?;
                    }
                }
            }
            Reduce {
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
            }
            Constant { .. }
            | Get { .. }
            | Let { .. }
            | Project { .. }
            | TopK { .. }
            | Negate { .. }
            | Threshold { .. }
            | Union { .. } => (),
        }
        Ok(())
    }

    /// Fallible mutable visitor for the [`MirScalarExpr`]s in the [`MirRelationExpr`] subtree rooted at `self`.
    ///
    /// Note that this does not recurse into [`MirRelationExpr`] subtrees within [`MirScalarExpr`] nodes.
    pub fn try_visit_scalars_mut<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut MirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        self.try_visit_mut_post(&mut |expr| expr.try_visit_scalars_mut1(f))
    }

    /// Infallible mutable visitor for the [`MirScalarExpr`]s in the [`MirRelationExpr`] subtree rooted at at `self`.
    ///
    /// Note that this does not recurse into [`MirRelationExpr`] subtrees within [`MirScalarExpr`] nodes.
    pub fn visit_scalars_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut MirScalarExpr),
    {
        self.try_visit_scalars_mut(&mut |s| {
            f(s);
            Ok::<_, RecursionLimitError>(())
        })
        .expect("Unexpected error in `visit_scalars_mut` call");
    }
}

impl CollectionPlan for MirRelationExpr {
    fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
        if let MirRelationExpr::Get {
            id: Id::Global(id), ..
        } = self
        {
            out.insert(*id);
        }
        self.visit_children(|expr| expr.depends_on_into(out))
    }
}

impl VisitChildren for MirRelationExpr {
    fn visit_children<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        use MirRelationExpr::*;
        match self {
            Constant { .. } | Get { .. } => (),
            Let { value, body, .. } => {
                f(value);
                f(body);
            }
            Project { input, .. }
            | Map { input, .. }
            | FlatMap { input, .. }
            | Filter { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input }
            | Threshold { input }
            | ArrangeBy { input, .. } => {
                f(input);
            }
            Join { inputs, .. } => {
                for input in inputs {
                    f(input);
                }
            }
            Union { base, inputs } => {
                f(base);
                for input in inputs {
                    f(input);
                }
            }
        }
    }

    fn visit_mut_children<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        use MirRelationExpr::*;
        match self {
            Constant { .. } | Get { .. } => (),
            Let { value, body, .. } => {
                f(value);
                f(body);
            }
            Project { input, .. }
            | Map { input, .. }
            | FlatMap { input, .. }
            | Filter { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input }
            | Threshold { input }
            | ArrangeBy { input, .. } => {
                f(input);
            }
            Join { inputs, .. } => {
                for input in inputs {
                    f(input);
                }
            }
            Union { base, inputs } => {
                f(base);
                for input in inputs {
                    f(input);
                }
            }
        }
    }

    fn try_visit_children<'a, F, E>(&'a self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a Self) -> Result<(), E>,
    {
        use MirRelationExpr::*;
        match self {
            Constant { .. } | Get { .. } => (),
            Let { value, body, .. } => {
                f(value)?;
                f(body)?;
            }
            Project { input, .. }
            | Map { input, .. }
            | FlatMap { input, .. }
            | Filter { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input }
            | Threshold { input }
            | ArrangeBy { input, .. } => {
                f(input)?;
            }
            Join { inputs, .. } => {
                for input in inputs {
                    f(input)?;
                }
            }
            Union { base, inputs } => {
                f(base)?;
                for input in inputs {
                    f(input)?;
                }
            }
        }
        Ok(())
    }

    fn try_visit_mut_children<'a, F, E>(&'a mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a mut Self) -> Result<(), E>,
    {
        use MirRelationExpr::*;
        match self {
            Constant { .. } | Get { .. } => (),
            Let { value, body, .. } => {
                f(value)?;
                f(body)?;
            }
            Project { input, .. }
            | Map { input, .. }
            | FlatMap { input, .. }
            | Filter { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input }
            | Threshold { input }
            | ArrangeBy { input, .. } => {
                f(input)?;
            }
            Join { inputs, .. } => {
                for input in inputs {
                    f(input)?;
                }
            }
            Union { base, inputs } => {
                f(base)?;
                for input in inputs {
                    f(input)?;
                }
            }
        }
        Ok(())
    }
}

/// Specification for an ordering by a column.
#[derive(Arbitrary, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, MzReflect)]
pub struct ColumnOrder {
    /// The column index.
    pub column: usize,
    /// Whether to sort in descending order.
    #[serde(default)]
    pub desc: bool,
}

impl From<&ColumnOrder> for ProtoColumnOrder {
    fn from(x: &ColumnOrder) -> Self {
        ProtoColumnOrder {
            column: x.column.into_proto(),
            desc: x.desc,
        }
    }
}

impl TryFrom<ProtoColumnOrder> for ColumnOrder {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoColumnOrder) -> Result<Self, Self::Error> {
        Ok(ColumnOrder {
            column: usize::from_proto(x.column)?,
            desc: x.desc,
        })
    }
}

impl fmt::Display for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "#{} {}",
            self.column,
            if self.desc { "desc" } else { "asc" }
        )
    }
}

/// Describes an aggregation expression.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct AggregateExpr {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    pub expr: MirScalarExpr,
    /// Should the aggregation be applied only to distinct results in each group.
    #[serde(default)]
    pub distinct: bool,
}

impl From<&AggregateExpr> for ProtoAggregateExpr {
    fn from(x: &AggregateExpr) -> Self {
        ProtoAggregateExpr {
            func: Some((&x.func).into()),
            expr: Some((&x.expr).into()),
            distinct: x.distinct,
        }
    }
}

impl TryFrom<ProtoAggregateExpr> for AggregateExpr {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoAggregateExpr) -> Result<Self, Self::Error> {
        Ok(Self {
            func: x.func.try_into_if_some("ProtoAggregateExpr::func")?,
            expr: x.expr.try_into_if_some("ProtoAggregateExpr::expr")?,
            distinct: x.distinct,
        })
    }
}

impl AggregateExpr {
    /// Computes the type of this `AggregateExpr`.
    pub fn typ(&self, relation_type: &RelationType) -> ColumnType {
        self.func.output_type(self.expr.typ(relation_type))
    }

    /// Returns whether the expression has a constant result.
    pub fn is_constant(&self) -> bool {
        match self.func {
            AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
            | AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::Dummy => self.expr.is_literal(),
            AggregateFunc::Count => self.expr.is_literal_null(),
            _ => self.expr.is_literal_err(),
        }
    }

    /// Extracts unique input from aggregate type
    pub fn on_unique(&self, input_type: &RelationType) -> MirScalarExpr {
        match &self.func {
            // Count is one if non-null, and zero if null.
            AggregateFunc::Count => self
                .expr
                .clone()
                .call_unary(UnaryFunc::IsNull(crate::func::IsNull))
                .if_then_else(
                    MirScalarExpr::literal_ok(Datum::Int64(0), ScalarType::Int64),
                    MirScalarExpr::literal_ok(Datum::Int64(1), ScalarType::Int64),
                ),

            // SumInt16 takes Int16s as input, but outputs Int64s.
            AggregateFunc::SumInt16 => self
                .expr
                .clone()
                .call_unary(UnaryFunc::CastInt16ToInt64(scalar_func::CastInt16ToInt64)),

            // SumInt32 takes Int32s as input, but outputs Int64s.
            AggregateFunc::SumInt32 => self
                .expr
                .clone()
                .call_unary(UnaryFunc::CastInt32ToInt64(scalar_func::CastInt32ToInt64)),

            // SumInt64 takes Int64s as input, but outputs numerics.
            AggregateFunc::SumInt64 => self.expr.clone().call_unary(UnaryFunc::CastInt64ToNumeric(
                scalar_func::CastInt64ToNumeric(Some(NumericMaxScale::ZERO)),
            )),

            // JsonbAgg takes _anything_ as input, but must output a Jsonb array.
            AggregateFunc::JsonbAgg { .. } => MirScalarExpr::CallVariadic {
                func: VariadicFunc::JsonbBuildArray,
                exprs: vec![self
                    .expr
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)))],
            },

            // JsonbAgg takes _anything_ as input, but must output a Jsonb object.
            AggregateFunc::JsonbObjectAgg { .. } => {
                let record = self
                    .expr
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::JsonbBuildObject,
                    exprs: (0..2)
                        .map(|i| {
                            record
                                .clone()
                                .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(i)))
                        })
                        .collect(),
                }
            }

            // StringAgg takes nested records of strings and outputs a string
            AggregateFunc::StringAgg { .. } => self
                .expr
                .clone()
                .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)))
                .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0))),

            // ListConcat and ArrayConcat take a single level of records and output a list containing exactly 1 element
            AggregateFunc::ListConcat { .. } | AggregateFunc::ArrayConcat { .. } => self
                .expr
                .clone()
                .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0))),

            // RowNumber takes a list of records and outputs a list containing exactly 1 element
            AggregateFunc::RowNumber { .. } => {
                let list = self
                    .expr
                    .clone()
                    // extract the list within the record
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));

                // extract the expression within the list
                let record = MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListIndex,
                    exprs: vec![
                        list,
                        MirScalarExpr::literal_ok(Datum::Int64(1), ScalarType::Int64),
                    ],
                };

                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListCreate {
                        elem_type: self
                            .typ(input_type)
                            .scalar_type
                            .unwrap_list_element_type()
                            .clone(),
                    },
                    exprs: vec![MirScalarExpr::CallVariadic {
                        func: VariadicFunc::RecordCreate {
                            field_names: vec![
                                ColumnName::from("?row_number?"),
                                ColumnName::from("?record?"),
                            ],
                        },
                        exprs: vec![
                            MirScalarExpr::literal_ok(Datum::Int64(1), ScalarType::Int64),
                            record,
                        ],
                    }],
                }
            }

            // DenseRank takes a list of records and outputs a list containing exactly 1 element
            AggregateFunc::DenseRank { .. } => {
                let list = self
                    .expr
                    .clone()
                    // extract the list within the record
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));

                // extract the expression within the list
                let record = MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListIndex,
                    exprs: vec![
                        list,
                        MirScalarExpr::literal_ok(Datum::Int64(1), ScalarType::Int64),
                    ],
                };

                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListCreate {
                        elem_type: self
                            .typ(input_type)
                            .scalar_type
                            .unwrap_list_element_type()
                            .clone(),
                    },
                    exprs: vec![MirScalarExpr::CallVariadic {
                        func: VariadicFunc::RecordCreate {
                            field_names: vec![
                                ColumnName::from("?dense_rank?"),
                                ColumnName::from("?record?"),
                            ],
                        },
                        exprs: vec![
                            MirScalarExpr::literal_ok(Datum::Int64(1), ScalarType::Int64),
                            record,
                        ],
                    }],
                }
            }

            // The input type for LagLead is a ((OriginalRow, (InputValue, Offset, Default)), OrderByExprs...)
            AggregateFunc::LagLead { lag_lead, .. } => {
                let tuple = self
                    .expr
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));

                // Get the overall return type
                let return_type = self
                    .typ(input_type)
                    .scalar_type
                    .unwrap_list_element_type()
                    .clone();
                let lag_lead_return_type = return_type.unwrap_record_element_type()[0].clone();

                // Extract the original row
                let original_row = tuple
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));

                let column_name = match lag_lead {
                    LagLeadType::Lag => "?lag?",
                    LagLeadType::Lead => "?lead?",
                };

                // Extract the encoded args
                let encoded_args =
                    tuple.call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(1)));
                let expr = encoded_args
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));
                let offset = encoded_args
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(1)));
                let default_value =
                    encoded_args.call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(2)));

                // In this case, the window always has only one element, so if the offset is not null and
                // not zero, the default value should be returned instead.
                let value = offset
                    .clone()
                    .call_binary(
                        MirScalarExpr::literal_ok(Datum::Int32(0), ScalarType::Int32),
                        crate::BinaryFunc::Eq,
                    )
                    .if_then_else(expr, default_value);
                let null_offset_check = offset
                    .call_unary(crate::UnaryFunc::IsNull(crate::func::IsNull))
                    .if_then_else(MirScalarExpr::literal_null(lag_lead_return_type), value);

                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListCreate {
                        elem_type: return_type,
                    },
                    exprs: vec![MirScalarExpr::CallVariadic {
                        func: VariadicFunc::RecordCreate {
                            field_names: vec![
                                ColumnName::from(column_name),
                                ColumnName::from("?record?"),
                            ],
                        },
                        exprs: vec![null_offset_check, original_row],
                    }],
                }
            }

            // The input type for FirstValue is a ((OriginalRow, InputValue), OrderByExprs...)
            AggregateFunc::FirstValue { window_frame, .. } => {
                let tuple = self
                    .expr
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));

                // Get the overall return type
                let return_type = self
                    .typ(input_type)
                    .scalar_type
                    .unwrap_list_element_type()
                    .clone();
                let first_value_return_type = return_type.unwrap_record_element_type()[0].clone();

                // Extract the original row
                let original_row = tuple
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));

                // Extract the input value
                let expr = tuple.call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(1)));

                // If the window frame includes the current (single) row, return its value, null otherwise
                let value = if window_frame.includes_current_row() {
                    expr
                } else {
                    MirScalarExpr::literal_null(first_value_return_type)
                };

                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListCreate {
                        elem_type: return_type,
                    },
                    exprs: vec![MirScalarExpr::CallVariadic {
                        func: VariadicFunc::RecordCreate {
                            field_names: vec![
                                ColumnName::from("?first_value?"),
                                ColumnName::from("?record?"),
                            ],
                        },
                        exprs: vec![value, original_row],
                    }],
                }
            }

            // The input type for LastValue is a ((OriginalRow, InputValue), OrderByExprs...)
            AggregateFunc::LastValue { window_frame, .. } => {
                let tuple = self
                    .expr
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));

                // Get the overall return type
                let return_type = self
                    .typ(input_type)
                    .scalar_type
                    .unwrap_list_element_type()
                    .clone();
                let last_value_return_type = return_type.unwrap_record_element_type()[0].clone();

                // Extract the original row
                let original_row = tuple
                    .clone()
                    .call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(0)));

                // Extract the input value
                let expr = tuple.call_unary(UnaryFunc::RecordGet(scalar_func::RecordGet(1)));

                // If the window frame includes the current (single) row, return its value, null otherwise
                let value = if window_frame.includes_current_row() {
                    expr
                } else {
                    MirScalarExpr::literal_null(last_value_return_type)
                };

                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListCreate {
                        elem_type: return_type,
                    },
                    exprs: vec![MirScalarExpr::CallVariadic {
                        func: VariadicFunc::RecordCreate {
                            field_names: vec![
                                ColumnName::from("?last_value?"),
                                ColumnName::from("?record?"),
                            ],
                        },
                        exprs: vec![value, original_row],
                    }],
                }
            }

            // All other variants should return the argument to the aggregation.
            AggregateFunc::MaxNumeric
            | AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz
            | AggregateFunc::MinNumeric
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
            | AggregateFunc::SumFloat32
            | AggregateFunc::SumFloat64
            | AggregateFunc::SumNumeric
            | AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::Dummy => self.expr.clone(),
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
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
#[derive(Arbitrary, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

impl From<&RowSetFinishing> for ProtoRowSetFinishing {
    fn from(x: &RowSetFinishing) -> Self {
        ProtoRowSetFinishing {
            order_by: x.order_by.iter().map(Into::into).collect(),
            limit: x.limit.as_ref().map(|x| usize::into_proto(*x)),
            offset: x.offset.into_proto(),
            project: x.project.iter().map(|x| usize::into_proto(*x)).collect(),
        }
    }
}

impl TryFrom<ProtoRowSetFinishing> for RowSetFinishing {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoRowSetFinishing) -> Result<Self, Self::Error> {
        Ok(RowSetFinishing {
            order_by: x
                .order_by
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, _>>()?,
            limit: x.limit.map(usize::from_proto).transpose()?,
            offset: usize::from_proto(x.offset)?,
            project: x
                .project
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl RowSetFinishing {
    /// True if the finishing does nothing to any result set.
    pub fn is_trivial(&self, arity: usize) -> bool {
        self.limit.is_none()
            && self.order_by.is_empty()
            && self.offset == 0
            && self.project.iter().copied().eq(0..arity)
    }
    /// Determines the index of the (Row, count) pair, and the
    /// index into the count within that pair, corresponding to a particular offset.
    ///
    /// For example, if `self.offset` is 42, and `rows` is
    /// `&[(_, 10), (_, 17), (_, 20), (_, 11)]`,
    /// then this function will return `(2, 15)`, because after applying
    /// the offset, we will start at the row/diff pair in position 2,
    /// and skip 15 of the 20 rows that that entry represents.
    fn find_offset(&self, rows: &[(Row, NonZeroUsize)]) -> (usize, usize) {
        let mut offset_remaining = self.offset;
        for (i, (_, count)) in rows.iter().enumerate() {
            let count = count.get();
            if count > offset_remaining {
                return (i, offset_remaining);
            }
            offset_remaining -= count;
        }
        // The offset is past the end of the rows; we will
        // return nothing.
        (rows.len(), 0)
    }
    /// Applies finishing actions to a row set,
    /// and unrolls it to a unary representation.
    pub fn finish(&self, mut rows: Vec<(Row, NonZeroUsize)>) -> Vec<Row> {
        let mut left_datum_vec = mz_repr::DatumVec::new();
        let mut right_datum_vec = mz_repr::DatumVec::new();
        let sort_by = |(left, _): &(Row, _), (right, _): &(Row, _)| {
            let left_datums = left_datum_vec.borrow_with(left);
            let right_datums = right_datum_vec.borrow_with(right);
            compare_columns(&self.order_by, &left_datums, &right_datums, || {
                left.cmp(&right)
            })
        };
        rows.sort_by(sort_by);

        let (offset_nth_row, offset_kth_copy) = self.find_offset(&rows);

        // Adjust the first returned row's count to account for the offset.
        if let Some((_, nth_diff)) = rows.get_mut(offset_nth_row) {
            // Justification for `unwrap`:
            // we only get here if we return from `get_offset`
            // having found something to return,
            // which can only happen if the count of
            // this row is greater than the offset remaining.
            *nth_diff = NonZeroUsize::new(nth_diff.get() - offset_kth_copy).unwrap();
        }

        let limit = self.limit.unwrap_or(std::usize::MAX);

        // The code below is logically equivalent to:
        //
        // let mut total = 0;
        // for (_, count) in &rows[offset_nth_row..] {
        //     total += count.get();
        // }
        // let return_size = std::cmp::min(total, limit);
        //
        // but it breaks early if the limit is reached, instead of scanning the entire code.
        let return_size = rows[offset_nth_row..]
            .iter()
            .try_fold(0, |sum, (_, count)| {
                let new_sum = sum + count.get();
                if new_sum > limit {
                    None
                } else {
                    Some(new_sum)
                }
            })
            .unwrap_or(limit);

        let mut ret = Vec::with_capacity(return_size);
        let mut remaining = limit;
        let mut row_buf = Row::default();
        let mut datum_vec = mz_repr::DatumVec::new();
        for (row, count) in &rows[offset_nth_row..] {
            if remaining == 0 {
                break;
            }
            let count = std::cmp::min(count.get(), remaining);
            for _ in 0..count {
                let new_row = {
                    let datums = datum_vec.borrow_with(&row);
                    row_buf
                        .packer()
                        .extend(self.project.iter().map(|i| &datums[*i]));
                    row_buf.clone()
                };
                ret.push(new_row);
            }
            remaining -= count;
        }

        ret
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

/// Describe a window frame, e.g. `RANGE UNBOUNDED PRECEDING` or
/// `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Window frames define a subset of the partition , and only a subset of
/// window functions make use of the window frame.
#[derive(Arbitrary, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, MzReflect)]
pub struct WindowFrame {
    /// ROWS, RANGE or GROUPS
    pub units: WindowFrameUnits,
    /// Where the frame starts
    pub start_bound: WindowFrameBound,
    /// Where the frame ends
    pub end_bound: WindowFrameBound,
}

impl WindowFrame {
    /// Return the default window frame used when one is not explicitly defined
    pub fn default() -> Self {
        WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::CurrentRow,
        }
    }

    fn includes_current_row(&self) -> bool {
        use WindowFrameBound::*;
        match self.start_bound {
            UnboundedPreceding => match self.end_bound {
                UnboundedPreceding => false,
                OffsetPreceding(0) => true,
                OffsetPreceding(_) => false,
                CurrentRow => true,
                OffsetFollowing(_) => true,
                UnboundedFollowing => true,
            },
            OffsetPreceding(0) => match self.end_bound {
                UnboundedPreceding => unreachable!(),
                OffsetPreceding(0) => true,
                // Any nonzero offsets here will create an empty window
                OffsetPreceding(_) => false,
                CurrentRow => true,
                OffsetFollowing(_) => true,
                UnboundedFollowing => true,
            },
            OffsetPreceding(_) => match self.end_bound {
                UnboundedPreceding => unreachable!(),
                // Window ends at the current row
                OffsetPreceding(0) => true,
                OffsetPreceding(_) => false,
                CurrentRow => true,
                OffsetFollowing(_) => true,
                UnboundedFollowing => true,
            },
            CurrentRow => true,
            OffsetFollowing(0) => match self.end_bound {
                UnboundedPreceding => unreachable!(),
                OffsetPreceding(_) => unreachable!(),
                CurrentRow => unreachable!(),
                OffsetFollowing(_) => true,
                UnboundedFollowing => true,
            },
            OffsetFollowing(_) => match self.end_bound {
                UnboundedPreceding => unreachable!(),
                OffsetPreceding(_) => unreachable!(),
                CurrentRow => unreachable!(),
                OffsetFollowing(_) => false,
                UnboundedFollowing => false,
            },
            UnboundedFollowing => false,
        }
    }
}

impl From<&WindowFrame> for ProtoWindowFrame {
    fn from(x: &WindowFrame) -> Self {
        ProtoWindowFrame {
            units: Some((&x.units).into()),
            start_bound: Some((&x.start_bound).into()),
            end_bound: Some((&x.end_bound).into()),
        }
    }
}

impl TryFrom<ProtoWindowFrame> for WindowFrame {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoWindowFrame) -> Result<Self, Self::Error> {
        Ok(WindowFrame {
            units: x.units.try_into_if_some("ProtoWindowFrame::units")?,
            start_bound: x
                .start_bound
                .try_into_if_some("ProtoWindowFrame::start_bound")?,
            end_bound: x
                .end_bound
                .try_into_if_some("ProtoWindowFrame::end_bound")?,
        })
    }
}

/// Describe how frame bounds are interpreted
#[derive(Arbitrary, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, MzReflect)]
pub enum WindowFrameUnits {
    /// Each row is treated as the unit of work for bounds
    Rows,
    /// Each peer group is treated as the unit of work for bounds,
    /// and offset-based bounds use the value of the ORDER BY expression
    Range,
    /// Each peer group is treated as the unit of work for bounds.
    /// Groups is currently not supported, and it is rejected during planning.
    Groups,
}

impl From<&WindowFrameUnits> for proto_window_frame::ProtoWindowFrameUnits {
    fn from(x: &WindowFrameUnits) -> Self {
        proto_window_frame::ProtoWindowFrameUnits {
            kind: Some(match x {
                WindowFrameUnits::Rows => {
                    proto_window_frame::proto_window_frame_units::Kind::Rows(())
                }
                WindowFrameUnits::Range => {
                    proto_window_frame::proto_window_frame_units::Kind::Range(())
                }
                WindowFrameUnits::Groups => {
                    proto_window_frame::proto_window_frame_units::Kind::Groups(())
                }
            }),
        }
    }
}

impl TryFrom<proto_window_frame::ProtoWindowFrameUnits> for WindowFrameUnits {
    type Error = TryFromProtoError;

    fn try_from(x: proto_window_frame::ProtoWindowFrameUnits) -> Result<Self, Self::Error> {
        Ok(match x.kind {
            Some(proto_window_frame::proto_window_frame_units::Kind::Rows(())) => {
                WindowFrameUnits::Rows
            }
            Some(proto_window_frame::proto_window_frame_units::Kind::Range(())) => {
                WindowFrameUnits::Range
            }
            Some(proto_window_frame::proto_window_frame_units::Kind::Groups(())) => {
                WindowFrameUnits::Groups
            }
            None => {
                return Err(TryFromProtoError::MissingField(
                    "ProtoWindowFrameUnits::kind".into(),
                ))
            }
        })
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
///
/// The order between frame bounds is significant, as Postgres enforces
/// some restrictions there.
#[derive(
    Arbitrary, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, MzReflect, PartialOrd, Ord,
)]
pub enum WindowFrameBound {
    /// `UNBOUNDED PRECEDING`
    UnboundedPreceding,
    /// `<N> PRECEDING`
    OffsetPreceding(u64),
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> FOLLOWING`
    OffsetFollowing(u64),
    /// `UNBOUNDED FOLLOWING`.
    UnboundedFollowing,
}

impl From<&WindowFrameBound> for proto_window_frame::ProtoWindowFrameBound {
    fn from(x: &WindowFrameBound) -> Self {
        proto_window_frame::ProtoWindowFrameBound {
            kind: Some(match x {
                WindowFrameBound::UnboundedPreceding => {
                    proto_window_frame::proto_window_frame_bound::Kind::UnboundedPreceding(())
                }
                WindowFrameBound::OffsetPreceding(offset) => {
                    proto_window_frame::proto_window_frame_bound::Kind::OffsetPreceding(*offset)
                }
                WindowFrameBound::CurrentRow => {
                    proto_window_frame::proto_window_frame_bound::Kind::CurrentRow(())
                }
                WindowFrameBound::OffsetFollowing(offset) => {
                    proto_window_frame::proto_window_frame_bound::Kind::OffsetFollowing(*offset)
                }
                WindowFrameBound::UnboundedFollowing => {
                    proto_window_frame::proto_window_frame_bound::Kind::UnboundedFollowing(())
                }
            }),
        }
    }
}

impl TryFrom<proto_window_frame::ProtoWindowFrameBound> for WindowFrameBound {
    type Error = TryFromProtoError;

    fn try_from(x: proto_window_frame::ProtoWindowFrameBound) -> Result<Self, Self::Error> {
        Ok(match x.kind {
            Some(proto_window_frame::proto_window_frame_bound::Kind::UnboundedPreceding(())) => {
                WindowFrameBound::UnboundedPreceding
            }
            Some(proto_window_frame::proto_window_frame_bound::Kind::OffsetPreceding(offset)) => {
                WindowFrameBound::OffsetPreceding(offset)
            }
            Some(proto_window_frame::proto_window_frame_bound::Kind::CurrentRow(())) => {
                WindowFrameBound::CurrentRow
            }
            Some(proto_window_frame::proto_window_frame_bound::Kind::OffsetFollowing(offset)) => {
                WindowFrameBound::OffsetFollowing(offset)
            }
            Some(proto_window_frame::proto_window_frame_bound::Kind::UnboundedFollowing(())) => {
                WindowFrameBound::UnboundedFollowing
            }
            None => {
                return Err(TryFromProtoError::MissingField(
                    "ProtoWindowFrameBound::kind".into(),
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn column_order_protobuf_roundtrip(expect in any::<ColumnOrder>()) {
            let actual = protobuf_roundtrip::<_, ProtoColumnOrder>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn aggregate_expr_protobuf_roundtrip(expect in any::<AggregateExpr>()) {
            let actual = protobuf_roundtrip::<_, ProtoAggregateExpr>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn window_frame_units_protobuf_roundtrip(expect in any::<WindowFrameUnits>()) {
            let actual = protobuf_roundtrip::<_, proto_window_frame::ProtoWindowFrameUnits>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn window_frame_bound_protobuf_roundtrip(expect in any::<WindowFrameBound>()) {
            let actual = protobuf_roundtrip::<_, proto_window_frame::ProtoWindowFrameBound>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn window_frame_protobuf_roundtrip(expect in any::<WindowFrame>()) {
            let actual = protobuf_roundtrip::<_, ProtoWindowFrame>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
