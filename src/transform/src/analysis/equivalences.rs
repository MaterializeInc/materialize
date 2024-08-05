// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An analysis that reports all known-equivalent expressions for each relation.
//!
//! Expressions are equivalent at a relation if they are certain to evaluate to
//! the same `Datum` for all records in the relation.
//!
//! Equivalences are recorded in an `EquivalenceClasses`, which lists all known
//! equivalences classes, each a list of equivalent expressions.

use mz_expr::canonicalize::EquivalenceClasses;
use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_repr::Datum;

use crate::analysis::{Analysis, Lattice};
use crate::analysis::{Arity, RelationType};
use crate::analysis::{Derived, DerivedBuilder};

/// Pulls up and pushes down predicate information represented as equivalences
#[derive(Debug, Default)]
pub struct Equivalences;

impl Analysis for Equivalences {
    // A `Some(list)` indicates a list of classes of equivalent expressions.
    // A `None` indicates all expressions are equivalent, including contradictions;
    // this is only possible for the empty collection, and as an initial result for
    // unconstrained recursive terms.
    type Value = Option<EquivalenceClasses>;

    fn announce_dependencies(builder: &mut DerivedBuilder) {
        builder.require(Arity);
        builder.require(RelationType); // needed for expression reduction.
    }

    fn derive(
        &self,
        expr: &MirRelationExpr,
        index: usize,
        results: &[Self::Value],
        depends: &Derived,
    ) -> Self::Value {
        let mut equivalences = match expr {
            MirRelationExpr::Constant { rows, typ } => {
                // Trawl `rows` for any constant information worth recording.
                // Literal columns may be valuable; non-nullability could be too.
                let mut equivalences = EquivalenceClasses::default();
                if let Ok([(row, _cnt), rows @ ..]) = rows.as_deref() {
                    // Vector of `Option<Datum>` which becomes `None` once a column has a second datum.
                    let len = row.iter().count();
                    let mut common = Vec::with_capacity(len);
                    common.extend(row.iter().map(Some));

                    for (row, _cnt) in rows.iter() {
                        for (datum, common) in row.iter().zip(common.iter_mut()) {
                            if Some(datum) != *common {
                                *common = None;
                            }
                        }
                    }
                    for (index, common) in common.into_iter().enumerate() {
                        if let Some(datum) = common {
                            equivalences.classes.push(vec![
                                MirScalarExpr::Column(index),
                                MirScalarExpr::literal_ok(
                                    datum,
                                    typ.column_types[index].scalar_type.clone(),
                                ),
                            ]);
                        }
                    }
                }
                Some(equivalences)
            }
            MirRelationExpr::Get { id, typ, .. } => {
                let mut equivalences = Some(EquivalenceClasses::default());
                // Find local identifiers, but nothing for external identifiers.
                if let Id::Local(id) = id {
                    if let Some(offset) = depends.bindings().get(id) {
                        // It is possible we have derived nothing for a recursive term
                        if let Some(result) = results.get(*offset) {
                            equivalences.clone_from(result);
                        } else {
                            // No top element was prepared.
                            // This means we are executing pessimistically,
                            // but perhaps we must because optimism is off.
                        }
                    }
                }
                // Incorporate statements about column nullability.
                let mut non_null_cols = vec![MirScalarExpr::literal_false()];
                for (index, col_type) in typ.column_types.iter().enumerate() {
                    if !col_type.nullable {
                        non_null_cols.push(MirScalarExpr::column(index).call_is_null());
                    }
                }
                if non_null_cols.len() > 1 {
                    if let Some(equivalences) = equivalences.as_mut() {
                        equivalences.classes.push(non_null_cols);
                    }
                }

                equivalences
            }
            MirRelationExpr::Let { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::LetRec { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Project { outputs, .. } => {
                // restrict equivalences, and introduce equivalences for repeated outputs.
                let mut equivalences = results.get(index - 1).unwrap().clone();
                equivalences
                    .as_mut()
                    .map(|e| e.project(outputs.iter().cloned()));
                equivalences
            }
            MirRelationExpr::Map { scalars, .. } => {
                // introduce equivalences for new columns and expressions that define them.
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    let input_arity = depends.results::<Arity>().unwrap()[index - 1];
                    for (pos, expr) in scalars.iter().enumerate() {
                        equivalences
                            .classes
                            .push(vec![MirScalarExpr::Column(input_arity + pos), expr.clone()]);
                    }
                }
                equivalences
            }
            MirRelationExpr::FlatMap { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Filter { predicates, .. } => {
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    let mut class = predicates.clone();
                    class.push(MirScalarExpr::literal_ok(
                        Datum::True,
                        mz_repr::ScalarType::Bool,
                    ));
                    equivalences.classes.push(class);
                }
                equivalences
            }
            MirRelationExpr::Join { equivalences, .. } => {
                // Collect equivalences from all inputs;
                let expr_index = index;
                let mut children = depends
                    .children_of_rev(expr_index, expr.children().count())
                    .collect::<Vec<_>>();
                children.reverse();

                let arity = depends.results::<Arity>().unwrap();
                let mut columns = 0;
                let mut result = Some(EquivalenceClasses::default());
                for child in children.into_iter() {
                    let input_arity = arity[child];
                    let equivalences = results[child].clone();
                    if let Some(mut equivalences) = equivalences {
                        let permutation = (columns..(columns + input_arity)).collect::<Vec<_>>();
                        equivalences.permute(&permutation);
                        result
                            .as_mut()
                            .map(|e| e.classes.extend(equivalences.classes));
                    } else {
                        result = None;
                    }
                    columns += input_arity;
                }

                // Fold join equivalences into our results.
                result
                    .as_mut()
                    .map(|e| e.classes.extend(equivalences.iter().cloned()));
                result
            }
            MirRelationExpr::Reduce {
                group_key,
                aggregates: _,
                ..
            } => {
                let input_arity = depends.results::<Arity>().unwrap()[index - 1];
                let mut equivalences = results.get(index - 1).unwrap().clone();
                if let Some(equivalences) = &mut equivalences {
                    // Introduce keys column equivalences as a map, then project to them as a projection.
                    for (pos, expr) in group_key.iter().enumerate() {
                        equivalences
                            .classes
                            .push(vec![MirScalarExpr::Column(input_arity + pos), expr.clone()]);
                    }

                    // Having added classes to `equivalences`, we should minimize the classes to fold the
                    // information in before applying the `project`, to set it up for success.
                    equivalences.minimize(&None);

                    // TODO: MIN, MAX, ANY, ALL aggregates pass through all certain properties of their columns.
                    // They also pass through equivalences of them and other constant columns (e.g. key columns).
                    // However, it is not correct to simply project onto these columns, as relationships amongst
                    // aggregate columns may no longer be preserved. MAX(col) != MIN(col) even though col = col.
                    // TODO: COUNT ensures a non-null value.
                    equivalences.project(input_arity..(input_arity + group_key.len()));
                }
                equivalences
            }
            MirRelationExpr::TopK { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Negate { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Threshold { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Union { .. } => {
                // TODO: `EquivalenceClasses::union` takes references, and we could probably skip much of this cloning.
                let expr_index = index;
                depends
                    .children_of_rev(expr_index, expr.children().count())
                    .flat_map(|c| results[c].clone())
                    .reduce(|e1, e2| e1.union(&e2))
            }
            MirRelationExpr::ArrangeBy { .. } => results.get(index - 1).unwrap().clone(),
        };

        let expr_type = depends.results::<RelationType>().unwrap()[index].clone();
        equivalences.as_mut().map(|e| e.minimize(&expr_type));
        equivalences
    }

    fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
        Some(Box::new(EQLattice))
    }
}

struct EQLattice;

impl Lattice<Option<EquivalenceClasses>> for EQLattice {
    fn top(&self) -> Option<EquivalenceClasses> {
        None
    }

    fn meet_assign(
        &self,
        a: &mut Option<EquivalenceClasses>,
        b: Option<EquivalenceClasses>,
    ) -> bool {
        match (&mut *a, b) {
            (_, None) => false,
            (None, b) => {
                *a = b;
                true
            }
            (Some(a), Some(b)) => {
                let mut c = a.union(&b);
                std::mem::swap(a, &mut c);
                a != &mut c
            }
        }
    }
}
