// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

use std::cmp::{max, Ordering};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Display;
use std::num::{NonZeroU64, NonZeroUsize};

use bytesize::ByteSize;
use itertools::Itertools;
use mz_lowertest::MzReflect;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::IdGen;
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::Indent;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::explain::text::text_string_at;
use mz_repr::explain::{DummyHumanizer, ExplainConfig, ExprHumanizer, PlanRenderingContext};
use mz_repr::{ColumnName, ColumnType, Datum, Diff, GlobalId, RelationType, Row, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::relation::func::{AggregateFunc, LagLeadType, TableFunc};
use crate::visit::{Visit, VisitChildren};
use crate::Id::Local;
use crate::{
    func as scalar_func, EvalError, FilterCharacteristics, Id, LocalId, MirScalarExpr, UnaryFunc,
    VariadicFunc,
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
/// - (2) `CallBinary` calls with associative functions such as `+`
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
/// The AST is meant to reflect the capabilities of the `differential_dataflow::Collection` type,
/// written generically enough to avoid run-time compilation work.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect)]
pub enum MirRelationExpr {
    /// A constant relation containing specified rows.
    ///
    /// The runtime memory footprint of this operator is zero.
    ///
    /// When you would like to pattern match on this, consider using `MirRelationExpr::as_const`
    /// instead, which looks behind `ArrangeBy`s. You might want this matching behavior because
    /// constant folding doesn't remove `ArrangeBy`s.
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
    /// Introduce mutually recursive bindings.
    ///
    /// Each `LocalId` is immediately bound to an initially empty  collection
    /// with the type of its corresponding `MirRelationExpr`. Repeatedly, each
    /// binding is evaluated using the current contents of each other binding,
    /// and is refreshed to contain the new evaluation. This process continues
    /// through all bindings, and repeats as long as changes continue to occur.
    ///
    /// The resulting value of the expression is `body` evaluated once in the
    /// context of the final iterates.
    ///
    /// A zero-binding instance can be replaced by `body`.
    /// A single-binding instance is equivalent to `MirRelationExpr::Let`.
    ///
    /// The runtime memory footprint of this operator is zero.
    LetRec {
        /// The identifiers to be used in `Get` variants to retrieve each `value`.
        #[mzreflect(ignore)]
        ids: Vec<LocalId>,
        /// The collections to be bound to each `id`.
        values: Vec<MirRelationExpr>,
        /// Maximum number of iterations, after which we should artificially force a fixpoint.
        /// (We don't error when reaching the limit, just return the current state as final result.)
        /// The per-`LetRec` limit that the user specified is initially copied to each binding to
        /// accommodate slicing and merging of `LetRec`s in MIR transforms (e.g., `NormalizeLets`).
        #[mzreflect(ignore)]
        limits: Vec<Option<LetRecLimit>>,
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
        expected_group_size: Option<u64>,
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
        /// User-supplied hint: how many rows will have the same group key.
        #[serde(default)]
        expected_group_size: Option<u64>,
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
    /// on finer grain. Each `keys` item represents a different index that should be
    /// produced from the `keys`.
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
                match &e {
                    MirRelationExpr::Let { body, .. } => {
                        // Do not traverse the value sub-graph, since it's not relevant for
                        // determining the relation type of Let operators.
                        Some(vec![&*body])
                    }
                    MirRelationExpr::LetRec { body, .. } => {
                        // Do not traverse the value sub-graph, since it's not relevant for
                        // determining the relation type of Let operators.
                        Some(vec![&*body])
                    }
                    _ => None,
                }
            },
            &mut |e: &MirRelationExpr| {
                match e {
                    MirRelationExpr::Let { .. } => {
                        let body_typ = type_stack.pop().unwrap();
                        // Insert a dummy relation type for the value, since `typ_with_input_types`
                        // won't look at it, but expects the relation type of the body to be second.
                        type_stack.push(RelationType::empty());
                        type_stack.push(body_typ);
                    }
                    MirRelationExpr::LetRec { values, .. } => {
                        let body_typ = type_stack.pop().unwrap();
                        // Insert dummy relation types for the values, since `typ_with_input_types`
                        // won't look at them, but expects the relation type of the body to be last.
                        type_stack
                            .extend(std::iter::repeat(RelationType::empty()).take(values.len()));
                        type_stack.push(body_typ);
                    }
                    _ => {}
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
    /// the current relation in the same order as they are visited by `try_visit_children`
    /// method, even though not all may be used for computing the schema of the
    /// current relation. For example, `Let` expects two input types, one for the
    /// value relation and one for the body, in that order, but only the one for the
    /// body is used to determine the type of the `Let` relation.
    ///
    /// It is meant to be used during post-order traversals to compute relation
    /// schemas incrementally.
    pub fn typ_with_input_types(&self, input_types: &[RelationType]) -> RelationType {
        let column_types = self.col_with_input_cols(input_types.iter().map(|i| &i.column_types));
        let unique_keys = self.keys_with_input_keys(
            input_types.iter().map(|i| i.arity()),
            input_types.iter().map(|i| &i.keys),
        );
        RelationType::new(column_types).with_keys(unique_keys)
    }

    /// Reports the column types of the relation given the column types of the input relations.
    ///
    /// `input_types` is required to contain the column types for the input relations of
    /// the current relation in the same order as they are visited by `try_visit_children`
    /// method, even though not all may be used for computing the schema of the
    /// current relation. For example, `Let` expects two input types, one for the
    /// value relation and one for the body, in that order, but only the one for the
    /// body is used to determine the type of the `Let` relation.
    ///
    /// It is meant to be used during post-order traversals to compute column types
    /// incrementally.
    pub fn col_with_input_cols<'a, I>(&self, mut input_types: I) -> Vec<ColumnType>
    where
        I: Iterator<Item = &'a Vec<ColumnType>>,
    {
        use MirRelationExpr::*;

        match self {
            Constant { rows, typ } => {
                let mut col_types = typ.column_types.clone();
                let mut seen_null = vec![false; typ.arity()];
                if let Ok(rows) = rows {
                    for (row, _diff) in rows {
                        for (datum, i) in row.iter().zip_eq(0..typ.arity()) {
                            if datum.is_null() {
                                seen_null[i] = true;
                            }
                        }
                    }
                }
                for (&seen_null, i) in seen_null.iter().zip_eq(0..typ.arity()) {
                    if !seen_null {
                        col_types[i].nullable = false;
                    } else {
                        assert!(col_types[i].nullable);
                    }
                }
                col_types
            }
            Get { typ, .. } => typ.column_types.clone(),
            Project { outputs, .. } => {
                let input = input_types.next().unwrap();
                outputs.iter().map(|&i| input[i].clone()).collect()
            }
            Map { scalars, .. } => {
                let mut result = input_types.next().unwrap().clone();
                for scalar in scalars.iter() {
                    result.push(scalar.typ(&result))
                }
                result
            }
            FlatMap { func, .. } => {
                let mut result = input_types.next().unwrap().clone();
                result.extend(func.output_type().column_types);
                result
            }
            Filter { predicates, .. } => {
                let mut result = input_types.next().unwrap().clone();

                // Set as nonnull any columns where null values would cause
                // any predicate to evaluate to null.
                for column in non_nullable_columns(predicates) {
                    result[column].nullable = false;
                }
                result
            }
            Join { equivalences, .. } => {
                // Concatenate input column types
                let mut types = input_types.flat_map(|cols| cols.to_owned()).collect_vec();
                // In an equivalence class, if any column is non-null, then make all non-null
                for equivalence in equivalences {
                    let col_inds = equivalence
                        .iter()
                        .filter_map(|expr| match expr {
                            MirScalarExpr::Column(col) => Some(*col),
                            _ => None,
                        })
                        .collect_vec();
                    if col_inds.iter().any(|i| !types.get(*i).unwrap().nullable) {
                        for i in col_inds {
                            types.get_mut(i).unwrap().nullable = false;
                        }
                    }
                }
                types
            }
            Reduce {
                group_key,
                aggregates,
                ..
            } => {
                let input = input_types.next().unwrap();
                group_key
                    .iter()
                    .map(|e| e.typ(input))
                    .chain(aggregates.iter().map(|agg| agg.typ(input)))
                    .collect()
            }
            TopK { .. } | Negate { .. } | Threshold { .. } | ArrangeBy { .. } => {
                input_types.next().unwrap().clone()
            }
            Let { .. } => {
                // skip over the input types for `value`.
                input_types.nth(1).unwrap().clone()
            }
            LetRec { values, .. } => {
                // skip over the input types for `values`.
                input_types.nth(values.len()).unwrap().clone()
            }
            Union { .. } => {
                let mut result = input_types.next().unwrap().clone();
                for input_col_types in input_types {
                    for (base_col, col) in result.iter_mut().zip_eq(input_col_types) {
                        *base_col = base_col
                            .union(col)
                            .map_err(|e| format!("{}\nIn {:#?}", e, self))
                            .unwrap();
                    }
                }
                result
            }
        }
    }

    /// Reports the unique keys of the relation given the arities and the unique
    /// keys of the input relations.
    ///
    /// `input_arities` and `input_keys` are required to contain the
    /// corresponding info for the input relations of
    /// the current relation in the same order as they are visited by `try_visit_children`
    /// method, even though not all may be used for computing the schema of the
    /// current relation. For example, `Let` expects two input types, one for the
    /// value relation and one for the body, in that order, but only the one for the
    /// body is used to determine the type of the `Let` relation.
    ///
    /// It is meant to be used during post-order traversals to compute unique keys
    /// incrementally.
    pub fn keys_with_input_keys<'a, I, J>(
        &self,
        mut input_arities: I,
        mut input_keys: J,
    ) -> Vec<Vec<usize>>
    where
        I: Iterator<Item = usize>,
        J: Iterator<Item = &'a Vec<Vec<usize>>>,
    {
        use MirRelationExpr::*;

        let mut keys = match self {
            Constant {
                rows: Ok(rows),
                typ,
            } => {
                let n_cols = typ.arity();
                // If the `i`th entry is `Some`, then we have not yet observed non-uniqueness in the `i`th column.
                let mut unique_values_per_col = vec![Some(BTreeSet::<Datum>::default()); n_cols];
                for (row, diff) in rows {
                    for (i, datum) in row.iter().enumerate() {
                        if datum != Datum::Dummy {
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
                    vec![vec![]]
                } else {
                    // XXX - Multi-column keys are not detected.
                    typ.keys
                        .iter()
                        .cloned()
                        .chain(
                            unique_values_per_col
                                .into_iter()
                                .enumerate()
                                .filter(|(_idx, unique_vals)| unique_vals.is_some())
                                .map(|(idx, _)| vec![idx]),
                        )
                        .collect()
                }
            }
            Constant { rows: Err(_), typ } | Get { typ, .. } => typ.keys.clone(),
            Threshold { .. } | ArrangeBy { .. } => input_keys.next().unwrap().clone(),
            Let { .. } => {
                // skip over the unique keys for value
                input_keys.nth(1).unwrap().clone()
            }
            LetRec { values, .. } => {
                // skip over the unique keys for value
                input_keys.nth(values.len()).unwrap().clone()
            }
            Project { outputs, .. } => {
                let input = input_keys.next().unwrap();
                input
                    .iter()
                    .filter_map(|key_set| {
                        if key_set.iter().all(|k| outputs.contains(k)) {
                            Some(
                                key_set
                                    .iter()
                                    .map(|c| outputs.iter().position(|o| o == c).unwrap())
                                    .collect(),
                            )
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            Map { scalars, .. } => {
                let mut remappings = Vec::new();
                let arity = input_arities.next().unwrap();
                for (column, scalar) in scalars.iter().enumerate() {
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

                let mut result = input_keys.next().unwrap().clone();
                let mut new_keys = Vec::new();
                // Any column in `remappings` could be replaced in a key
                // by the corresponding c. This could lead to combinatorial
                // explosion using our current representation, so we wont
                // do that. Instead, we'll handle the case of one remapping.
                if remappings.len() == 1 {
                    let (old, new) = remappings.pop().unwrap();
                    for key in &result {
                        if key.contains(&old) {
                            let mut new_key: Vec<usize> =
                                key.iter().cloned().filter(|k| k != &old).collect();
                            new_key.push(new);
                            new_key.sort_unstable();
                            new_keys.push(new_key);
                        }
                    }
                    result.append(&mut new_keys);
                }
                result
            }
            FlatMap { .. } => {
                // FlatMap can add duplicate rows, so input keys are no longer
                // valid
                vec![]
            }
            Negate { .. } => {
                // Although negate may have distinct records for each key,
                // the multiplicity is -1 rather than 1. This breaks many
                // of the optimization uses of "keys".
                vec![]
            }
            Filter { predicates, .. } => {
                // A filter inherits the keys of its input unless the filters
                // have reduced the input to a single row, in which case the
                // keys of the input are `()`.
                let mut input = input_keys.next().unwrap().clone();
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
                for key_set in &mut input {
                    key_set.retain(|k| !cols_equal_to_literal.contains(&k));
                }
                if !input.is_empty() {
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
                                    if c1 != c2 {
                                        return Some((c1, c2));
                                    }
                                }
                            }
                        }
                        None
                    });

                    // Keep doing replacements until the number of keys settles
                    let mut prev_keys: BTreeSet<_> = input.drain(..).collect();
                    let mut prev_keys_size = 0;
                    while prev_keys_size != prev_keys.len() {
                        prev_keys_size = prev_keys.len();
                        for (c1, c2) in classes.clone() {
                            let mut new_keys = BTreeSet::new();
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

                    prev_keys.into_iter().sorted().collect()
                } else {
                    input
                }
            }
            Join { equivalences, .. } => {
                // It is important the `new_from_input_arities` constructor is
                // used. Otherwise, Materialize may potentially end up in an
                // infinite loop.
                let input_mapper = crate::JoinInputMapper::new_from_input_arities(input_arities);

                input_mapper.global_keys(input_keys, equivalences)
            }
            Reduce { group_key, .. } => {
                // The group key should form a key, but we might already have
                // keys that are subsets of the group key, and should retain
                // those instead, if so.
                let mut result = Vec::new();
                for key_set in input_keys.next().unwrap() {
                    if key_set
                        .iter()
                        .all(|k| group_key.contains(&MirScalarExpr::Column(*k)))
                    {
                        result.push(
                            key_set
                                .iter()
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
                if result.is_empty() {
                    result.push((0..group_key.len()).collect());
                }
                result
            }
            TopK {
                group_key, limit, ..
            } => {
                // If `limit` is `Some(1)` then the group key will become
                // a unique key, as there will be only one record with that key.
                let mut result = input_keys.next().unwrap().clone();
                if limit == &Some(1) {
                    result.push(group_key.clone())
                }
                result
            }
            Union { base, inputs } => {
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

                let arity = input_arities.next().unwrap();
                let (base_projection, base_with_project_stripped) =
                    if let MirRelationExpr::Project { input, outputs } = &**base {
                        (outputs.clone(), &**input)
                    } else {
                        // A input without a project is equivalent to an input
                        // with the project being all columns in the input in order.
                        ((0..arity).collect::<Vec<_>>(), &**base)
                    };
                let mut result = Vec::new();
                if let MirRelationExpr::Get {
                    id: first_id,
                    typ: _,
                } = base_with_project_stripped
                {
                    if inputs.len() == 1 {
                        if let MirRelationExpr::Map { input, .. } = &inputs[0] {
                            if let MirRelationExpr::Union { base, inputs } = &**input {
                                if inputs.len() == 1 {
                                    if let Some((input, outputs)) = base.is_negated_project() {
                                        if let MirRelationExpr::Get {
                                            id: second_id,
                                            typ: _,
                                        } = input
                                        {
                                            if first_id == second_id {
                                                result.extend(
                                                    inputs[0].typ().keys.drain(..).filter(|key| {
                                                        key.iter().all(|c| {
                                                            outputs.get(*c) == Some(c)
                                                                && base_projection.get(*c)
                                                                    == Some(c)
                                                        })
                                                    }),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                // Important: do not inherit keys of either input, as not unique.
                result
            }
        };
        keys.sort();
        keys.dedup();
        keys
    }

    /// The number of columns in the relation.
    ///
    /// This number is determined from the type, which is determined recursively
    /// at non-trivial cost.
    ///
    /// The arity is computed incrementally with a recursive post-order
    /// traversal, that accumulates the arities for the relations yet to be
    /// visited in `arity_stack`.
    pub fn arity(&self) -> usize {
        let mut arity_stack = Vec::new();
        #[allow(deprecated)]
        self.visit_pre_post_nolimit(
            &mut |e: &MirRelationExpr| -> Option<Vec<&MirRelationExpr>> {
                match &e {
                    MirRelationExpr::Let { body, .. } => {
                        // Do not traverse the value sub-graph, since it's not relevant for
                        // determining the arity of Let operators.
                        Some(vec![&*body])
                    }
                    MirRelationExpr::LetRec { body, .. } => {
                        // Do not traverse the value sub-graph, since it's not relevant for
                        // determining the arity of Let operators.
                        Some(vec![&*body])
                    }
                    _ => None,
                }
            },
            &mut |e: &MirRelationExpr| {
                match &e {
                    MirRelationExpr::Let { .. } => {
                        let body_arity = arity_stack.pop().unwrap();
                        arity_stack.push(0);
                        arity_stack.push(body_arity);
                    }
                    MirRelationExpr::LetRec { values, .. } => {
                        let body_arity = arity_stack.pop().unwrap();
                        arity_stack.extend(std::iter::repeat(0).take(values.len()));
                        arity_stack.push(body_arity);
                    }
                    _ => {}
                }
                let num_inputs = e.num_inputs();
                let arity = e
                    .arity_with_input_arities(arity_stack[arity_stack.len() - num_inputs..].iter());
                arity_stack.truncate(arity_stack.len() - num_inputs);
                arity_stack.push(arity);
            },
        );
        assert_eq!(arity_stack.len(), 1);
        arity_stack.pop().unwrap()
    }

    /// Reports the arity of the relation given the schema of the input relations.
    ///
    /// `input_arities` is required to contain the arities for the input relations of
    /// the current relation in the same order as they are visited by `try_visit_children`
    /// method, even though not all may be used for computing the schema of the
    /// current relation. For example, `Let` expects two input types, one for the
    /// value relation and one for the body, in that order, but only the one for the
    /// body is used to determine the type of the `Let` relation.
    ///
    /// It is meant to be used during post-order traversals to compute arities
    /// incrementally.
    pub fn arity_with_input_arities<'a, I>(&self, mut input_arities: I) -> usize
    where
        I: Iterator<Item = &'a usize>,
    {
        use MirRelationExpr::*;

        match self {
            Constant { rows: _, typ } => typ.arity(),
            Get { typ, .. } => typ.arity(),
            Let { .. } => {
                input_arities.next();
                *input_arities.next().unwrap()
            }
            LetRec { values, .. } => {
                for _ in 0..values.len() {
                    input_arities.next();
                }
                *input_arities.next().unwrap()
            }
            Project { outputs, .. } => outputs.len(),
            Map { scalars, .. } => *input_arities.next().unwrap() + scalars.len(),
            FlatMap { func, .. } => {
                *input_arities.next().unwrap() + func.output_type().column_types.len()
            }
            Join { .. } => input_arities.map(|a| *a).sum(),
            Reduce {
                input: _,
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
            Filter { .. }
            | TopK { .. }
            | Negate { .. }
            | Threshold { .. }
            | Union { .. }
            | ArrangeBy { .. } => *input_arities.next().unwrap(),
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

    /// If self is a constant, return the value and the type, otherwise `None`.
    /// Looks behind `ArrangeBy`s.
    pub fn as_const(&self) -> Option<(&Result<Vec<(Row, Diff)>, EvalError>, &RelationType)> {
        match self {
            MirRelationExpr::Constant { rows, typ } => Some((rows, typ)),
            MirRelationExpr::ArrangeBy { input, .. } => input.as_const(),
            _ => None,
        }
    }

    /// If self is a constant, mutably return the value and the type, otherwise `None`.
    /// Looks behind `ArrangeBy`s.
    pub fn as_const_mut(
        &mut self,
    ) -> Option<(&mut Result<Vec<(Row, Diff)>, EvalError>, &mut RelationType)> {
        match self {
            MirRelationExpr::Constant { rows, typ } => Some((rows, typ)),
            MirRelationExpr::ArrangeBy { input, .. } => input.as_const_mut(),
            _ => None,
        }
    }

    /// If self is a constant error, return the error, otherwise `None`.
    /// Looks behind `ArrangeBy`s.
    pub fn as_const_err(&self) -> Option<&EvalError> {
        match self {
            MirRelationExpr::Constant { rows: Err(e), .. } => Some(e),
            MirRelationExpr::ArrangeBy { input, .. } => input.as_const_err(),
            _ => None,
        }
    }

    /// Checks if `self` is the single element collection with no columns.
    pub fn is_constant_singleton(&self) -> bool {
        if let Some((Ok(rows), typ)) = self.as_const() {
            rows.len() == 1 && typ.column_types.len() == 0 && rows[0].1 == 1
        } else {
            false
        }
    }

    /// Constructs the expression for getting a global collection
    pub fn global_get(id: GlobalId, typ: RelationType) -> Self {
        MirRelationExpr::Get {
            id: Id::Global(id),
            typ,
        }
    }

    /// Retains only the columns specified by `output`.
    pub fn project(mut self, mut outputs: Vec<usize>) -> Self {
        if let MirRelationExpr::Project {
            outputs: columns, ..
        } = &mut self
        {
            // Update `outputs` to reference base columns of `input`.
            for column in outputs.iter_mut() {
                *column = columns[*column];
            }
            *columns = outputs;
            self
        } else {
            MirRelationExpr::Project {
                input: Box::new(self),
                outputs,
            }
        }
    }

    /// Append to each row the results of applying elements of `scalar`.
    pub fn map(mut self, scalars: Vec<MirScalarExpr>) -> Self {
        if let MirRelationExpr::Map { scalars: s, .. } = &mut self {
            s.extend(scalars);
            self
        } else if !scalars.is_empty() {
            MirRelationExpr::Map {
                input: Box::new(self),
                scalars,
            }
        } else {
            self
        }
    }

    /// Append to each row a single `scalar`.
    pub fn map_one(self, scalar: MirScalarExpr) -> Self {
        self.map(vec![scalar])
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
    pub fn filter<I>(mut self, predicates: I) -> Self
    where
        I: IntoIterator<Item = MirScalarExpr>,
    {
        // Extract existing predicates
        let mut new_predicates = if let MirRelationExpr::Filter { input, predicates } = self {
            self = *input;
            predicates
        } else {
            Vec::new()
        };
        // Normalize collection of predicates.
        new_predicates.extend(predicates);
        new_predicates.sort();
        new_predicates.dedup();
        // Introduce a `Filter` only if we have predicates.
        if !new_predicates.is_empty() {
            self = MirRelationExpr::Filter {
                input: Box::new(self),
                predicates: new_predicates,
            };
        }

        self
    }

    /// Form the Cartesian outer-product of rows in both inputs.
    pub fn product(mut self, right: Self) -> Self {
        if right.is_constant_singleton() {
            self
        } else if self.is_constant_singleton() {
            right
        } else if let MirRelationExpr::Join { inputs, .. } = &mut self {
            inputs.push(right);
            self
        } else {
            MirRelationExpr::join(vec![self, right], vec![])
        }
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
        expected_group_size: Option<u64>,
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
        expected_group_size: Option<u64>,
    ) -> Self {
        MirRelationExpr::TopK {
            input: Box::new(self),
            group_key,
            order_key,
            limit,
            offset,
            expected_group_size,
            monotonic: false,
        }
    }

    /// Negates the occurrences of each row.
    pub fn negate(self) -> Self {
        if let MirRelationExpr::Negate { input } = self {
            *input
        } else {
            MirRelationExpr::Negate {
                input: Box::new(self),
            }
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
        if let MirRelationExpr::Threshold { .. } = &self {
            self
        } else {
            MirRelationExpr::Threshold {
                input: Box::new(self),
            }
        }
    }

    /// Unions together any number inputs.
    ///
    /// If `inputs` is empty, then an empty relation of type `typ` is
    /// constructed.
    pub fn union_many(mut inputs: Vec<Self>, typ: RelationType) -> Self {
        // Deconstruct `inputs` as `Union`s and reconstitute.
        let mut flat_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            if let MirRelationExpr::Union { base, inputs } = input {
                flat_inputs.push(*base);
                flat_inputs.extend(inputs);
            } else {
                flat_inputs.push(input);
            }
        }
        inputs = flat_inputs;
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
        // Deconstruct `self` and `other` as `Union`s and reconstitute.
        let mut flat_inputs = Vec::with_capacity(2);
        if let MirRelationExpr::Union { base, inputs } = self {
            flat_inputs.push(*base);
            flat_inputs.extend(inputs);
        } else {
            flat_inputs.push(self);
        }
        if let MirRelationExpr::Union { base, inputs } = other {
            flat_inputs.push(*base);
            flat_inputs.extend(inputs);
        } else {
            flat_inputs.push(other);
        }

        MirRelationExpr::Union {
            base: Box::new(flat_inputs.remove(0)),
            inputs: flat_inputs,
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
        if let Some((Ok(rows), ..)) = self.as_const() {
            rows.is_empty()
        } else {
            false
        }
    }

    /// If the expression is a negated project, return the input and the projection.
    pub fn is_negated_project(&self) -> Option<(&MirRelationExpr, &[usize])> {
        if let MirRelationExpr::Negate { input } = self {
            if let MirRelationExpr::Project { input, outputs } = &**input {
                return Some((&**input, outputs));
            }
        }
        if let MirRelationExpr::Project { input, outputs } = self {
            if let MirRelationExpr::Negate { input } = &**input {
                return Some((&**input, outputs));
            }
        }
        None
    }

    /// Pretty-print this [MirRelationExpr] to a string.
    pub fn pretty(&self) -> String {
        let config = ExplainConfig::default();
        self.explain(&config, None)
    }

    /// Pretty-print this [MirRelationExpr] to a string using a custom
    /// [ExplainConfig] and an optionally provided [ExprHumanizer].
    pub fn explain(&self, config: &ExplainConfig, humanizer: Option<&dyn ExprHumanizer>) -> String {
        text_string_at(self, || PlanRenderingContext {
            indent: Indent::default(),
            humanizer: humanizer.unwrap_or(&DummyHumanizer),
            annotations: BTreeMap::default(),
            config,
        })
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

    /// Like [MirRelationExpr::let_in], but with a fallible return type.
    pub fn let_in_fallible<Body, E>(
        self,
        id_gen: &mut IdGen,
        body: Body,
    ) -> Result<MirRelationExpr, E>
    where
        Body: FnOnce(&mut IdGen, MirRelationExpr) -> Result<MirRelationExpr, E>,
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
            let body = (body)(id_gen, get)?;
            Ok(MirRelationExpr::Let {
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
    pub fn contains_temporal(&self) -> bool {
        let mut contains = false;
        self.visit_scalars(&mut |e| contains = contains || e.contains_temporal());
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
            Join {
                equivalences,
                implementation,
                ..
            } => {
                for equivalence in equivalences {
                    for expr in equivalence {
                        f(expr)?;
                    }
                }
                match implementation {
                    JoinImplementation::Differential((_, start_key, _), order) => {
                        for start_key in start_key {
                            for k in start_key {
                                f(k)?;
                            }
                        }
                        for (_, lookup_key, _) in order {
                            for k in lookup_key {
                                f(k)?;
                            }
                        }
                    }
                    JoinImplementation::DeltaQuery(paths) => {
                        for path in paths {
                            for (_, lookup_key, _) in path {
                                for k in lookup_key {
                                    f(k)?;
                                }
                            }
                        }
                    }
                    JoinImplementation::IndexedFilter(_, index_key, _) => {
                        for k in index_key {
                            f(k)?;
                        }
                    }
                    JoinImplementation::Unimplemented => {} // No scalar exprs
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
            | LetRec { .. }
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

    /// Fallible visitor for the [`MirScalarExpr`]s directly owned by this relation expression.
    ///
    /// The `f` visitor should not recursively descend into owned [`MirRelationExpr`]s.
    pub fn try_visit_scalars_1<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&MirScalarExpr) -> Result<(), E>,
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
                    f(&agg.expr)?;
                }
            }
            Constant { .. }
            | Get { .. }
            | Let { .. }
            | LetRec { .. }
            | Project { .. }
            | TopK { .. }
            | Negate { .. }
            | Threshold { .. }
            | Union { .. } => (),
        }
        Ok(())
    }

    /// Fallible immutable visitor for the [`MirScalarExpr`]s in the [`MirRelationExpr`] subtree rooted at `self`.
    ///
    /// Note that this does not recurse into [`MirRelationExpr`] subtrees within [`MirScalarExpr`] nodes.
    pub fn try_visit_scalars<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&MirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        self.try_visit_post(&mut |expr| expr.try_visit_scalars_1(f))
    }

    /// Infallible immutable visitor for the [`MirScalarExpr`]s in the [`MirRelationExpr`] subtree rooted at at `self`.
    ///
    /// Note that this does not recurse into [`MirRelationExpr`] subtrees within [`MirScalarExpr`] nodes.
    pub fn visit_scalars<F>(&self, f: &mut F)
    where
        F: FnMut(&MirScalarExpr),
    {
        self.try_visit_scalars(&mut |s| {
            f(s);
            Ok::<_, RecursionLimitError>(())
        })
        .expect("Unexpected error in `visit_scalars` call");
    }

    /// Clears the contents of `self` even if it's so deep that simply dropping it would cause a
    /// stack overflow in `drop_in_place`.
    ///
    /// Leaves `self` in an unusable state, so this should only be used if `self` is about to be
    /// dropped or otherwise overwritten.
    pub fn destroy_carefully(&mut self) {
        #[allow(deprecated)] // Having `maybe_grow` and no limit is the point here
        self.visit_mut_post_nolimit(&mut |e| {
            e.take_dangerous();
        });
    }

    /// Computes the size (total number of nodes) and maximum depth of a MirRelationExpr for
    /// debug printing purposes. Might grow the stack to a size proportional to the maximum depth.
    pub fn debug_size_and_depth(&self) -> (usize, usize) {
        let mut size = 0;
        let mut max_depth = 0;
        fn dfs(expr: &MirRelationExpr, size: &mut usize, max_depth: &mut usize, cur_depth: usize) {
            mz_ore::stack::maybe_grow(|| {
                *size += 1;
                *max_depth = max(*max_depth, cur_depth);
                expr.visit_children(|child| dfs(child, size, max_depth, cur_depth + 1));
            });
        }
        dfs(self, &mut size, &mut max_depth, 1);
        (size, max_depth)
    }
}

// `LetRec` helpers
impl MirRelationExpr {
    /// True when `expr` contains a `LetRec` AST node.
    pub fn is_recursive(self: &MirRelationExpr) -> bool {
        let mut worklist = vec![self];
        while let Some(expr) = worklist.pop() {
            if let MirRelationExpr::LetRec { .. } = expr {
                return true;
            }
            worklist.extend(expr.children());
        }
        false
    }

    /// Given the ids and values of a LetRec, it computes the subset of ids that are used across
    /// iterations. These are those ids that have a reference before they are defined, when reading
    /// all the bindings in order.
    ///
    /// For example:
    /// ```SQL
    /// WITH MUTUALLY RECURSIVE
    ///     x(...) AS f(z),
    ///     y(...) AS g(x),
    ///     z(...) AS h(y)
    /// ...;
    /// ```
    /// Here, only `z` is returned, because `x` and `y` are referenced only within the same
    /// iteration.
    ///
    /// Note that if a binding references itself, that is also returned.
    pub fn recursive_ids(
        ids: &[LocalId],
        values: &[MirRelationExpr],
    ) -> Result<BTreeSet<LocalId>, RecursionLimitError> {
        let mut used_across_iterations = BTreeSet::new();
        let mut defined = BTreeSet::new();
        for (binding_id, value) in itertools::zip_eq(ids.iter(), values.iter()) {
            value.visit_post(&mut |expr| {
                if let MirRelationExpr::Get {
                    id: Local(get_id), ..
                } = expr
                {
                    // If we haven't seen a definition for it yet, then this will refer
                    // to the previous iteration.
                    // The `ids.contains` part of the condition is needed to exclude
                    // those ids that are not really in this LetRec, but either an inner
                    // or outer one.
                    if !defined.contains(get_id) && ids.contains(get_id) {
                        used_across_iterations.insert(*get_id);
                    }
                }
            })?;
            defined.insert(*binding_id);
        }
        Ok(used_across_iterations)
    }

    /// Replaces `LetRec` nodes with a stack of `Let` nodes.
    ///
    /// In each `Let` binding, uses of `Get` in `value` that are not at strictly greater
    /// identifiers are rewritten to be the constant collection.
    /// This makes the computation perform exactly "one" iteration.
    ///
    /// This was used only temporarily while developing `LetRec`.
    pub fn make_nonrecursive(self: &mut MirRelationExpr) {
        let mut deadlist = BTreeSet::new();
        let mut worklist = vec![self];
        while let Some(expr) = worklist.pop() {
            if let MirRelationExpr::LetRec {
                ids,
                values,
                limits: _,
                body,
            } = expr
            {
                let ids_values = values
                    .drain(..)
                    .zip(ids)
                    .map(|(value, id)| (*id, value))
                    .collect::<Vec<_>>();
                *expr = body.take_dangerous();
                for (id, mut value) in ids_values.into_iter().rev() {
                    // Remove references to potentially recursive identifiers.
                    deadlist.insert(id);
                    value.visit_pre_mut(|e| {
                        if let MirRelationExpr::Get {
                            id: crate::Id::Local(id),
                            ..
                        } = e
                        {
                            if deadlist.contains(id) {
                                e.take_safely();
                            }
                        }
                    });
                    *expr = MirRelationExpr::Let {
                        id,
                        value: Box::new(value),
                        body: Box::new(expr.take_dangerous()),
                    };
                }
                worklist.push(expr);
            } else {
                worklist.extend(expr.children_mut().rev());
            }
        }
    }

    /// For each Id `id'` referenced in `expr`, if it is larger or equal than `id`, then record in
    /// `expire_whens` that when `id'` is redefined, then we should expire the information that
    /// we are holding about `id`. Call `do_expirations` with `expire_whens` at each Id
    /// redefinition.
    ///
    /// IMPORTANT: Relies on the numbering of Ids to be what `renumber_bindings` gives.
    pub fn collect_expirations(
        id: LocalId,
        expr: &MirRelationExpr,
        expire_whens: &mut BTreeMap<LocalId, Vec<LocalId>>,
    ) {
        expr.visit_pre(|e| {
            if let MirRelationExpr::Get {
                id: Id::Local(referenced_id),
                ..
            } = e
            {
                // The following check needs `renumber_bindings` to have run recently
                if referenced_id >= &id {
                    expire_whens
                        .entry(*referenced_id)
                        .or_insert_with(Vec::new)
                        .push(id);
                }
            }
        });
    }

    /// Call this function when `id` is redefined. It modifies `id_infos` by removing information
    /// about such Ids whose information depended on the earlier definition of `id`, according to
    /// `expire_whens`. Also modifies `expire_whens`: it removes the currently processed entry.
    pub fn do_expirations<I>(
        redefined_id: LocalId,
        expire_whens: &mut BTreeMap<LocalId, Vec<LocalId>>,
        id_infos: &mut BTreeMap<LocalId, I>,
    ) -> Vec<(LocalId, I)> {
        let mut expired_infos = Vec::new();
        if let Some(expirations) = expire_whens.remove(&redefined_id) {
            for expired_id in expirations.into_iter() {
                if let Some(offer) = id_infos.remove(&expired_id) {
                    expired_infos.push((expired_id, offer));
                }
            }
        }
        expired_infos
    }
}
/// Augment non-nullability of columns, by observing either
/// 1. Predicates that explicitly test for null values, and
/// 2. Columns that if null would make a predicate be null.
pub fn non_nullable_columns(predicates: &[MirScalarExpr]) -> BTreeSet<usize> {
    let mut nonnull_required_columns = BTreeSet::new();
    for predicate in predicates {
        // Add any columns that being null would force the predicate to be null.
        // Should that happen, the row would be discarded.
        predicate.non_null_requirements(&mut nonnull_required_columns);

        /*
        Test for explicit checks that a column is non-null.

        This analysis is ad hoc, and will miss things:

        materialize=> create table a(x int, y int);
        CREATE TABLE
        materialize=> explain with(types) select x from a where (y=x and y is not null) or x is not null;
        Optimized Plan
        --------------------------------------------------------------------------------------------------------
        Explained Query:                                                                                      +
        Project (#0) // { types: "(integer?)" }                                                             +
        Filter ((#0) IS NOT NULL OR ((#1) IS NOT NULL AND (#0 = #1))) // { types: "(integer?, integer?)" }+
        Get materialize.public.a // { types: "(integer?, integer?)" }                                   +
                                                                                  +
        Source materialize.public.a                                                                           +
        filter=(((#0) IS NOT NULL OR ((#1) IS NOT NULL AND (#0 = #1))))                                     +

        (1 row)
        */

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
                    nonnull_required_columns.insert(*c);
                }
            }
        }
    }

    nonnull_required_columns
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

impl MirRelationExpr {
    /// Iterates through references to child expressions.
    pub fn children(&self) -> impl DoubleEndedIterator<Item = &Self> {
        let mut first = None;
        let mut second = None;
        let mut rest = None;
        let mut last = None;

        use MirRelationExpr::*;
        match self {
            Constant { .. } | Get { .. } => (),
            Let { value, body, .. } => {
                first = Some(&**value);
                second = Some(&**body);
            }
            LetRec { values, body, .. } => {
                rest = Some(values);
                last = Some(&**body);
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
                first = Some(&**input);
            }
            Join { inputs, .. } => {
                rest = Some(inputs);
            }
            Union { base, inputs } => {
                first = Some(&**base);
                rest = Some(inputs);
            }
        }

        first
            .into_iter()
            .chain(second)
            .chain(rest.into_iter().flatten())
            .chain(last)
    }

    /// Iterates through mutable references to child expressions.
    pub fn children_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut Self> {
        let mut first = None;
        let mut second = None;
        let mut rest = None;
        let mut last = None;

        use MirRelationExpr::*;
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
            Project { input, .. }
            | Map { input, .. }
            | FlatMap { input, .. }
            | Filter { input, .. }
            | Reduce { input, .. }
            | TopK { input, .. }
            | Negate { input }
            | Threshold { input }
            | ArrangeBy { input, .. } => {
                first = Some(&mut **input);
            }
            Join { inputs, .. } => {
                rest = Some(inputs);
            }
            Union { base, inputs } => {
                first = Some(&mut **base);
                rest = Some(inputs);
            }
        }

        first
            .into_iter()
            .chain(second)
            .chain(rest.into_iter().flatten())
            .chain(last)
    }

    /// Iterative pre-order visitor.
    pub fn visit_pre<'a, F: FnMut(&'a Self)>(&'a self, mut f: F) {
        let mut worklist = vec![self];
        while let Some(expr) = worklist.pop() {
            f(expr);
            worklist.extend(expr.children().rev());
        }
    }

    /// Iterative pre-order visitor.
    pub fn visit_pre_mut<F: FnMut(&mut Self)>(&mut self, mut f: F) {
        let mut worklist = vec![self];
        while let Some(expr) = worklist.pop() {
            f(expr);
            worklist.extend(expr.children_mut().rev());
        }
    }

    /// Return a vector of references to the subtrees of this expression
    /// in post-visit order (the last element is `&self`).
    pub fn post_order_vec(&self) -> Vec<&Self> {
        let mut stack = vec![self];
        let mut result = vec![];
        while let Some(expr) = stack.pop() {
            result.push(expr);
            stack.extend(expr.children());
        }
        result.reverse();
        result
    }
}

impl VisitChildren<Self> for MirRelationExpr {
    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&Self),
    {
        for child in self.children() {
            f(child)
        }
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        for child in self.children_mut() {
            f(child)
        }
    }

    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        for child in self.children() {
            f(child)?
        }
        Ok(())
    }

    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        for child in self.children_mut() {
            f(child)?
        }
        Ok(())
    }
}

/// Specification for an ordering by a column.
#[derive(
    Arbitrary, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct ColumnOrder {
    /// The column index.
    pub column: usize,
    /// Whether to sort in descending order.
    #[serde(default)]
    pub desc: bool,
    /// Whether to sort nulls last.
    #[serde(default)]
    pub nulls_last: bool,
}

impl RustType<ProtoColumnOrder> for ColumnOrder {
    fn into_proto(&self) -> ProtoColumnOrder {
        ProtoColumnOrder {
            column: self.column.into_proto(),
            desc: self.desc,
            nulls_last: self.nulls_last,
        }
    }

    fn from_proto(proto: ProtoColumnOrder) -> Result<Self, TryFromProtoError> {
        Ok(ColumnOrder {
            column: proto.column.into_rust()?,
            desc: proto.desc,
            nulls_last: proto.nulls_last,
        })
    }
}

impl fmt::Display for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "#{} {} {}",
            self.column,
            if self.desc { "desc" } else { "asc" },
            if self.nulls_last {
                "nulls_last"
            } else {
                "nulls_first"
            },
        )
    }
}

/// Describes an aggregation expression.
#[derive(
    Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct AggregateExpr {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    pub expr: MirScalarExpr,
    /// Should the aggregation be applied only to distinct results in each group.
    #[serde(default)]
    pub distinct: bool,
}

impl RustType<ProtoAggregateExpr> for AggregateExpr {
    fn into_proto(&self) -> ProtoAggregateExpr {
        ProtoAggregateExpr {
            func: Some(self.func.into_proto()),
            expr: Some(self.expr.into_proto()),
            distinct: self.distinct,
        }
    }

    fn from_proto(proto: ProtoAggregateExpr) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            func: proto.func.into_rust_if_some("ProtoAggregateExpr::func")?,
            expr: proto.expr.into_rust_if_some("ProtoAggregateExpr::expr")?,
            distinct: proto.distinct,
        })
    }
}

impl AggregateExpr {
    /// Computes the type of this `AggregateExpr`.
    pub fn typ(&self, column_types: &[ColumnType]) -> ColumnType {
        self.func.output_type(self.expr.typ(column_types))
    }

    /// Returns whether the expression has a constant result.
    pub fn is_constant(&self) -> bool {
        match self.func {
            AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxMzTimestamp
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
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinMzTimestamp
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
    pub fn on_unique(&self, input_type: &[ColumnType]) -> MirScalarExpr {
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

            // SumUInt16 takes UInt16s as input, but outputs UInt64s.
            AggregateFunc::SumUInt16 => self.expr.clone().call_unary(
                UnaryFunc::CastUint16ToUint64(scalar_func::CastUint16ToUint64),
            ),

            // SumUInt32 takes UInt32s as input, but outputs UInt64s.
            AggregateFunc::SumUInt32 => self.expr.clone().call_unary(
                UnaryFunc::CastUint32ToUint64(scalar_func::CastUint32ToUint64),
            ),

            // SumUInt64 takes UInt64s as input, but outputs numerics.
            AggregateFunc::SumUInt64 => {
                self.expr.clone().call_unary(UnaryFunc::CastUint64ToNumeric(
                    scalar_func::CastUint64ToNumeric(Some(NumericMaxScale::ZERO)),
                ))
            }

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
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxMzTimestamp
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
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinMzTimestamp
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

    /// Returns whether the expression is COUNT(*) or not.  Note that
    /// when we define the count builtin in sql::func, we convert
    /// COUNT(*) to COUNT(true), making it indistinguishable from
    /// literal COUNT(true), but we prefer to consider this as the
    /// former.
    pub fn is_count_asterisk(&self) -> bool {
        match self {
            AggregateExpr {
                func: AggregateFunc::Count,
                expr:
                    MirScalarExpr::Literal(
                        Ok(row),
                        mz_repr::ColumnType {
                            scalar_type: mz_repr::ScalarType::Bool,
                            nullable: false,
                        },
                    ),
                ..
            } => row.unpack_first() == mz_repr::Datum::True,
            _ => false,
        }
    }
}

/// Describe a join implementation in dataflow.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect)]
pub enum JoinImplementation {
    /// Perform a sequence of binary differential dataflow joins.
    ///
    /// The first argument indicates
    /// 1) the index of the starting collection,
    /// 2) if it should be arranged, the keys to arrange it by, and
    /// 3) the characteristics of the starting collection (for EXPLAINing).
    /// The sequence that follows lists other relation indexes, and the key for
    /// the arrangement we should use when joining it in.
    /// The JoinInputCharacteristics are for EXPLAINing the characteristics that
    /// were used for join ordering.
    ///
    /// Each collection index should occur exactly once, either as the starting collection
    /// or somewhere in the list.
    Differential(
        (
            usize,
            Option<Vec<MirScalarExpr>>,
            Option<JoinInputCharacteristics>,
        ),
        Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>,
    ),
    /// Perform independent delta query dataflows for each input.
    ///
    /// The argument is a sequence of plans, for the input collections in order.
    /// Each plan starts from the corresponding index, and then in sequence joins
    /// against collections identified by index and with the specified arrangement key.
    /// The JoinInputCharacteristics are for EXPLAINing the characteristics that were
    /// used for join ordering.
    DeltaQuery(Vec<Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>>),
    /// Join a user-created index with a constant collection to speed up the evaluation of a
    /// predicate such as `(f1 = 3 AND f2 = 5) OR (f1 = 7 AND f2 = 9)`.
    /// This gets translated to a Differential join during MIR -> LIR lowering, but we still want
    /// to represent it in MIR, because the fast path detection wants to match on this.
    ///
    /// Consists of (`<view id>`, `<keys of index>`, `<constants>`)
    IndexedFilter(GlobalId, Vec<MirScalarExpr>, #[mzreflect(ignore)] Vec<Row>),
    /// No implementation yet selected.
    Unimplemented,
}

impl Default for JoinImplementation {
    fn default() -> Self {
        JoinImplementation::Unimplemented
    }
}

impl JoinImplementation {
    /// Returns `true` iff the value is not [`JoinImplementation::Unimplemented`].
    pub fn is_implemented(&self) -> bool {
        match self {
            Self::Unimplemented => false,
            _ => true,
        }
    }

    /// Returns an optional implementation name if the value is not [`JoinImplementation::Unimplemented`].
    pub fn name(&self) -> Option<&'static str> {
        match self {
            Self::Differential(..) => Some("differential"),
            Self::DeltaQuery(..) => Some("delta"),
            Self::IndexedFilter(..) => Some("indexed_filter"),
            Self::Unimplemented => None,
        }
    }
}

/// Characteristics of a join order candidate collection.
///
/// A candidate is described by a collection and a key, and may have various liabilities.
/// Primarily, the candidate may risk substantial inflation of records, which is something
/// that concerns us greatly. Additionally the candidate may be unarranged, and we would
/// prefer candidates that do not require additional memory. Finally, we prefer lower id
/// collections in the interest of consistent tie-breaking.
#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Serialize, Deserialize, Hash, MzReflect)]
pub struct JoinInputCharacteristics {
    /// An excellent indication that record count will not increase.
    pub unique_key: bool,
    /// A weaker signal that record count will not increase.
    pub key_length: usize,
    /// Indicates that there will be no additional in-memory footprint.
    pub arranged: bool,
    /// Characteristics of the filter that is applied at this input.
    pub filters: FilterCharacteristics,
    /// We want to prefer input earlier in the input list, for stability of ordering.
    pub input: std::cmp::Reverse<usize>,
}

impl JoinInputCharacteristics {
    /// Creates a new instance with the given characteristics.
    pub fn new(
        unique_key: bool,
        key_length: usize,
        arranged: bool,
        filters: FilterCharacteristics,
        input: usize,
    ) -> Self {
        Self {
            unique_key,
            key_length,
            arranged,
            filters,
            input: std::cmp::Reverse(input),
        }
    }

    /// Turns the instance into a String to be printed in EXPLAIN.
    pub fn explain(&self) -> String {
        let mut e = "".to_owned();
        if self.unique_key {
            e.push_str("U");
        }
        for _ in 0..self.key_length {
            e.push_str("K");
        }
        if self.arranged {
            e.push_str("A");
        }
        e.push_str(&self.filters.explain());
        e
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

impl RustType<ProtoRowSetFinishing> for RowSetFinishing {
    fn into_proto(&self) -> ProtoRowSetFinishing {
        ProtoRowSetFinishing {
            order_by: self.order_by.into_proto(),
            limit: self.limit.into_proto(),
            offset: self.offset.into_proto(),
            project: self.project.into_proto(),
        }
    }

    fn from_proto(x: ProtoRowSetFinishing) -> Result<Self, TryFromProtoError> {
        Ok(RowSetFinishing {
            order_by: x.order_by.into_rust()?,
            limit: x.limit.into_rust()?,
            offset: x.offset.into_rust()?,
            project: x.project.into_rust()?,
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
    pub fn finish(
        &self,
        mut rows: Vec<(Row, NonZeroUsize)>,
        // TODO(jkosh44) Eventually we want to be able to return arbitrary sized results.
        max_result_size: u32,
    ) -> Result<Vec<Row>, String> {
        let max_result_size = usize::cast_from(max_result_size);
        let mut left_datum_vec = mz_repr::DatumVec::new();
        let mut right_datum_vec = mz_repr::DatumVec::new();
        let sort_by = |(left, _): &(Row, _), (right, _): &(Row, _)| {
            let left_datums = left_datum_vec.borrow_with(left);
            let right_datums = right_datum_vec.borrow_with(right);
            compare_columns(&self.order_by, &left_datums, &right_datums, || {
                left.cmp(right)
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

        let limit = self.limit.unwrap_or(usize::MAX);

        // Count how many rows we'd expand into, returning early from the whole function
        // if we don't have enough memory to expand the result, or break early from the
        // iteration once we pass our limit.
        let mut num_rows = 0;
        let mut num_bytes: usize = 0;
        for (row, count) in &rows[offset_nth_row..] {
            num_rows += count.get();
            num_bytes = num_bytes.saturating_add(count.get().saturating_mul(row.byte_len()));

            // Check that result fits into max_result_size.
            if num_bytes > max_result_size {
                return Err(format!(
                    "result exceeds max size of {}",
                    ByteSize::b(u64::cast_from(max_result_size))
                ));
            }

            // Stop iterating if we've passed limit.
            if num_rows > limit {
                break;
            }
        }
        let return_size = std::cmp::min(num_rows, limit);

        let mut ret = Vec::with_capacity(return_size);
        let mut remaining = limit;
        let mut row_buf = Row::default();
        let mut datum_vec = mz_repr::DatumVec::new();
        let mut total_bytes = 0;
        for (row, count) in &rows[offset_nth_row..] {
            if remaining == 0 {
                break;
            }
            let count = std::cmp::min(count.get(), remaining);
            for _ in 0..count {
                let new_row = {
                    let datums = datum_vec.borrow_with(row);
                    row_buf
                        .packer()
                        .extend(self.project.iter().map(|i| &datums[*i]));
                    row_buf.clone()
                };
                total_bytes += new_row.byte_len();
                if total_bytes > max_result_size {
                    return Err(format!(
                        "result exceeds max size of {}",
                        ByteSize::b(u64::cast_from(max_result_size))
                    ));
                }
                ret.push(new_row);
            }
            remaining -= count;
        }

        Ok(ret)
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
        let cmp = match (&left[order.column], &right[order.column]) {
            (Datum::Null, Datum::Null) => Ordering::Equal,
            (Datum::Null, _) => {
                if order.nulls_last {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (_, Datum::Null) => {
                if order.nulls_last {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (lval, rval) => {
                if order.desc {
                    rval.cmp(lval)
                } else {
                    lval.cmp(rval)
                }
            }
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
#[derive(
    Arbitrary, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
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

impl RustType<ProtoWindowFrame> for WindowFrame {
    fn into_proto(&self) -> ProtoWindowFrame {
        ProtoWindowFrame {
            units: Some(self.units.into_proto()),
            start_bound: Some(self.start_bound.into_proto()),
            end_bound: Some(self.end_bound.into_proto()),
        }
    }

    fn from_proto(proto: ProtoWindowFrame) -> Result<Self, TryFromProtoError> {
        Ok(WindowFrame {
            units: proto.units.into_rust_if_some("ProtoWindowFrame::units")?,
            start_bound: proto
                .start_bound
                .into_rust_if_some("ProtoWindowFrame::start_bound")?,
            end_bound: proto
                .end_bound
                .into_rust_if_some("ProtoWindowFrame::end_bound")?,
        })
    }
}

/// Describe how frame bounds are interpreted
#[derive(
    Arbitrary, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
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

impl RustType<proto_window_frame::ProtoWindowFrameUnits> for WindowFrameUnits {
    fn into_proto(&self) -> proto_window_frame::ProtoWindowFrameUnits {
        use proto_window_frame::proto_window_frame_units::Kind::*;
        proto_window_frame::ProtoWindowFrameUnits {
            kind: Some(match self {
                WindowFrameUnits::Rows => Rows(()),
                WindowFrameUnits::Range => Range(()),
                WindowFrameUnits::Groups => Groups(()),
            }),
        }
    }

    fn from_proto(
        proto: proto_window_frame::ProtoWindowFrameUnits,
    ) -> Result<Self, TryFromProtoError> {
        use proto_window_frame::proto_window_frame_units::Kind::*;
        Ok(match proto.kind {
            Some(Rows(())) => WindowFrameUnits::Rows,
            Some(Range(())) => WindowFrameUnits::Range,
            Some(Groups(())) => WindowFrameUnits::Groups,
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoWindowFrameUnits::kind",
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

impl RustType<proto_window_frame::ProtoWindowFrameBound> for WindowFrameBound {
    fn into_proto(&self) -> proto_window_frame::ProtoWindowFrameBound {
        use proto_window_frame::proto_window_frame_bound::Kind::*;
        proto_window_frame::ProtoWindowFrameBound {
            kind: Some(match self {
                WindowFrameBound::UnboundedPreceding => UnboundedPreceding(()),
                WindowFrameBound::OffsetPreceding(offset) => OffsetPreceding(*offset),
                WindowFrameBound::CurrentRow => CurrentRow(()),
                WindowFrameBound::OffsetFollowing(offset) => OffsetFollowing(*offset),
                WindowFrameBound::UnboundedFollowing => UnboundedFollowing(()),
            }),
        }
    }

    fn from_proto(x: proto_window_frame::ProtoWindowFrameBound) -> Result<Self, TryFromProtoError> {
        use proto_window_frame::proto_window_frame_bound::Kind::*;
        Ok(match x.kind {
            Some(UnboundedPreceding(())) => WindowFrameBound::UnboundedPreceding,
            Some(OffsetPreceding(offset)) => WindowFrameBound::OffsetPreceding(offset),
            Some(CurrentRow(())) => WindowFrameBound::CurrentRow,
            Some(OffsetFollowing(offset)) => WindowFrameBound::OffsetFollowing(offset),
            Some(UnboundedFollowing(())) => WindowFrameBound::UnboundedFollowing,
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoWindowFrameBound::kind",
                ))
            }
        })
    }
}

/// Maximum iterations for a LetRec.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LetRecLimit {
    /// Maximum number of iterations to evaluate.
    pub max_iters: NonZeroU64,
    /// Whether to throw an error when reaching the above limit.
    /// If true, we simply use the current contents of each Id as the final result.
    pub return_at_limit: bool,
}

impl LetRecLimit {
    /// Compute the smallest limit from a Vec of `LetRecLimit`s.
    pub fn min_max_iter(limits: &Vec<Option<LetRecLimit>>) -> Option<u64> {
        limits
            .iter()
            .filter_map(|l| l.as_ref().map(|l| l.max_iters.get()))
            .min()
    }

    /// The default value of `LetRecLimit::return_at_limit` when using the RECURSION LIMIT option of
    /// WMR without ERROR AT or RETURN AT.
    pub const RETURN_AT_LIMIT_DEFAULT: bool = false;
}

impl Display for LetRecLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[recursion_limit={}", self.max_iters)?;
        if self.return_at_limit != LetRecLimit::RETURN_AT_LIMIT_DEFAULT {
            write!(f, ", return_at_limit")?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use mz_repr::explain::text::text_string_at;
    use proptest::prelude::*;

    use super::*;

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

    #[test]
    fn test_row_set_finishing_as_text() {
        let finishing = RowSetFinishing {
            order_by: vec![ColumnOrder {
                column: 4,
                desc: true,
                nulls_last: true,
            }],
            limit: Some(7),
            offset: Default::default(),
            project: vec![1, 3, 4, 5],
        };

        let act = text_string_at(&finishing, mz_ore::str::Indent::default);

        let exp = {
            use mz_ore::fmt::FormatBuffer;
            let mut s = String::new();
            write!(&mut s, "Finish");
            write!(&mut s, " order_by=[#4 desc nulls_last]");
            write!(&mut s, " limit=7");
            write!(&mut s, " output=[#1, #3..=#5]");
            writeln!(&mut s, "");
            s
        };

        assert_eq!(act, exp);
    }
}
