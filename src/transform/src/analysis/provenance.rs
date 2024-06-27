// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An analysis that describes the possible values of columns as a function
//! of the columns of let-bound identifiers.
//!
//! This analysis is meant to support "subsumption" of one set of values by
//! another, and enable the removal of semijoins that do not restrict the
//! terms they are joined against.

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};

use crate::analysis::Analysis;
use crate::analysis::{Arity, RelationType};
use crate::analysis::{Derived, DerivedBuilder};

/// An analysis that relates a collection to other named collections.
#[derive(Debug, Default)]
pub struct Provenance;

impl Analysis for Provenance {
    /// A list of potential relationships to other collections.
    type Value = Vec<ProvenanceInfo>;

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
        let result = match expr {
            MirRelationExpr::Constant { .. } => Vec::new(),
            MirRelationExpr::Get { id, .. } => {
                // The `id` may already have provenence information, which we should clone.
                let mut provenance = Vec::new();
                if let Id::Local(id) = id {
                    if let Some(offset) = depends.bindings().get(id) {
                        // It is possible we have derived nothing for a recursive term
                        if let Some(result) = results.get(*offset) {
                            provenance.clone_from(result);
                        }
                    }
                }
                let arity = depends.results::<Arity>().unwrap()[index];
                provenance.push(ProvenanceInfo::new(*id, arity));
                provenance
            }
            MirRelationExpr::Let { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::LetRec { .. } => results.get(index - 1).unwrap().clone(),
            MirRelationExpr::Project { outputs, .. } => {
                let mut input_provenance = results.get(index - 1).unwrap().clone();
                for provenance in input_provenance.iter_mut() {
                    // Projections may reorder, drop, and duplicate columns.
                    let columns = outputs
                        .iter()
                        .map(|c| provenance.columns[*c].clone())
                        .collect();
                    provenance.columns = columns;
                    // Predicates may need to be updated or discarded outright.
                    if let Some(predicates) = &mut provenance.filters {
                        // TODO: This can be made more efficient for long `outputs` and `predicates`.
                        // TODO: EQProp could provide a more relaxed test, if we know that columns that are
                        // projected away are equal to other columns / expressions that are retained.
                        if predicates
                            .iter()
                            .all(|expr| expr.support().iter().all(|c| outputs.contains(c)))
                        {
                            for expr in predicates.iter_mut() {
                                expr.visit_pre_mut(|e| {
                                    if let MirScalarExpr::Column(c) = e {
                                        *c = outputs.iter().position(|k| k == c).unwrap();
                                    }
                                })
                            }
                        } else {
                            provenance.filters = None;
                        }
                    }
                }
                input_provenance
            }

            MirRelationExpr::Map { scalars, .. } => {
                let input_arity = depends.results::<Arity>().unwrap()[index - 1];
                let mut input_provenance = results.get(index - 1).unwrap().clone();
                for provenance in input_provenance.iter_mut() {
                    assert_eq!(provenance.columns.len(), input_arity);
                    for scalar in scalars.iter() {
                        // TODO: This information is redundant with EQProp, and were we to rely on it as well,
                        // we could avoid any work here (and potential explosion of tracked information).
                        if !scalar.support().is_empty()
                            && scalar
                                .support()
                                .iter()
                                .all(|c| provenance.columns[*c].is_some())
                        {
                            let mut expr = scalar.clone();
                            let mut todo = vec![&mut expr];
                            while let Some(expr) = todo.pop() {
                                if let MirScalarExpr::Column(c) = expr {
                                    *expr = provenance.columns[*c].clone().unwrap();
                                } else {
                                    todo.extend(expr.children_mut());
                                }
                            }
                            provenance.columns.push(Some(expr));
                        } else {
                            provenance.columns.push(None);
                        }
                    }
                }
                input_provenance
            }
            MirRelationExpr::Filter { predicates, .. } => {
                let mut input_provenance = results.get(index - 1).unwrap().clone();
                for provenance in input_provenance.iter_mut() {
                    // If we already have a known list of filters, perhaps we can extend it.
                    // We only do this if we know that the support of the new predicates is
                    // present in `provenance.columns`, as otherwise the predicates do not
                    // relate to the referenced collection.
                    if let Some(preds) = &mut provenance.filters {
                        if predicates.iter().all(|expr| {
                            expr.support()
                                .iter()
                                .all(|c| provenance.columns[*c].is_some())
                        }) {
                            preds.extend(predicates.iter().cloned())
                        } else {
                            provenance.filters = None;
                        }
                    }
                }
                input_provenance
            }
            MirRelationExpr::Join { .. } => {
                // TODO: EQProp will be a much more effective way to carry information derived from equivalent
                // columns and expressions of columns.

                // A join is a cross-product of inputs, with some filters applied to the results.
                // However, we do not currently know that each input is non-empty, and so we must conservatively
                // set the `filter` field to `None`.
                // TODO: If all inputs are known non-empty, we could set the `filter` field to something that
                // derives from `equivalences`.

                let mut input_provenance = depends
                    .children_of_rev(index, expr.children().count())
                    .map(|c| results[c].clone())
                    .collect::<Vec<_>>();
                input_provenance.reverse();

                let mut input_arities = depends
                    .children_of_rev(index, expr.children().count())
                    .map(|c| depends.results::<Arity>().unwrap()[c].clone())
                    .collect::<Vec<_>>();
                input_arities.reverse();

                // TODO: This information is weak when we have related provenance from multiple inputs.
                // We could strengthen it by consolidating provenance information for the same source.
                let columns = vec![None; input_arities.iter().cloned().sum()];
                let mut provenance = Vec::new();
                for (index, input) in input_provenance.iter().enumerate() {
                    let prior_arity: usize = input_arities[..index].iter().sum();
                    for input_prov in input.iter() {
                        // Modify the column references and unset the `filter` field.
                        let mut columns = columns.clone();
                        for c in 0..input_arities[index] {
                            columns[c + prior_arity] = input_prov.columns[c].clone();
                        }
                        provenance.push(ProvenanceInfo {
                            id: input_prov.id,
                            columns,
                            filters: None,
                        });
                    }
                }
                provenance
            }
            MirRelationExpr::Reduce {
                group_key,
                aggregates,
                ..
            } => {
                // Reduce produces rows that have expressions as their keys, and also produces aggregates
                // that may reflect actual input columns, in the case of `MIN` and `MAX` for example.
                // The keys are not subset, as every present value produces an output row, but the aggregate
                // values may experience subsetting we cannot describe with a filter.

                let input_provenance = results.get(index - 1).unwrap();
                let mut provenance = Vec::with_capacity(input_provenance.len());
                for input_prov in input_provenance.iter() {
                    // Columns are those of `group_key` if their support is present, and then the aggregates.
                    // For now, we will not attempt to describe the aggregates, but we will describe the keys.
                    let mut columns = group_key
                        .iter()
                        .map(|key| {
                            if !key.support().is_empty()
                                && key
                                    .support()
                                    .iter()
                                    .all(|c| input_prov.columns[*c].is_some())
                            {
                                let mut expr = key.clone();
                                let mut todo = vec![&mut expr];
                                while let Some(expr) = todo.pop() {
                                    if let MirScalarExpr::Column(c) = expr {
                                        *expr = input_prov.columns[*c].clone().unwrap();
                                    } else {
                                        todo.extend(expr.children_mut());
                                    }
                                }
                                Some(expr)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    columns.extend((0..aggregates.len()).map(|_| None));

                    let filters = input_prov.filters.as_ref().and_then(|predicates| {
                        if predicates.iter().all(|expr| {
                            expr.support()
                                .iter()
                                .all(|c| group_key.contains(&MirScalarExpr::Column(*c)))
                        }) {
                            // Rewrite all predicates to refer to the columns of the output.
                            Some(
                                predicates
                                    .iter()
                                    .cloned()
                                    .map(|mut expr| {
                                        expr.visit_pre_mut(|e| {
                                            if let MirScalarExpr::Column(c) = e {
                                                *c = group_key
                                                    .iter()
                                                    .position(|k| k == &MirScalarExpr::Column(*c))
                                                    .unwrap();
                                            }
                                        });
                                        expr
                                    })
                                    .collect(),
                            )
                        } else {
                            None
                        }
                    });

                    provenance.push(ProvenanceInfo {
                        id: input_prov.id,
                        columns,
                        filters,
                    });
                }
                provenance
            }

            MirRelationExpr::FlatMap { func, .. } => {
                // These operators do not change the structure of a row, but may drop rows (including negating them).
                let mut input_provenance = results.get(index - 1).unwrap().clone();
                for input_prov in input_provenance.iter_mut() {
                    input_prov
                        .columns
                        .extend((0..func.output_arity()).map(|_| None));
                    input_prov.filters = None;
                }
                input_provenance
            }
            MirRelationExpr::TopK { .. }
            | MirRelationExpr::Negate { .. }
            | MirRelationExpr::Threshold { .. } => {
                // These operators do not change the structure of a row, but may drop rows (including negating them).
                let mut input_provenance = results.get(index - 1).unwrap().clone();
                for input_prov in input_provenance.iter_mut() {
                    input_prov.filters = None;
                }
                input_provenance
            }
            MirRelationExpr::Union { .. } => depends
                .children_of_rev(index, expr.children().count())
                .map(|c| results[c].clone())
                .reduce(|mut e1, e2| {
                    meet_prov_vecs(&mut e1, &e2);
                    e1
                })
                .unwrap(),
            MirRelationExpr::ArrangeBy { .. } => results.get(index - 1).unwrap().clone(),
        };

        for prov in result.iter() {
            assert_eq!(
                prov.columns.len(),
                depends.results::<Arity>().unwrap()[index],
                "{:?}",
                expr
            );
        }
        result
    }
}

/// A relationship to another collection indicated by `id`.
///
/// The relationship is expressed by two forms of restriction:
/// 1. columns: some columns are functions of columns in the other collection.
/// 2. rows: some rows may have been filtered as a function of their columns.
/// Nothing is expressed about the preservatino of the cardinality of rows.
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct ProvenanceInfo {
    /// The identifier of the other collection.
    pub id: Id,
    /// For each colunm of the subject collection, an optional expression that
    /// when present indicates that the column equals the expression applied to
    /// the collection identified by `id`.
    pub columns: Vec<Option<MirScalarExpr>>,
    /// When set, a guarantee that all values present in `id` are also present
    /// in this relation with cardinality at least one, subject to the predicates
    /// in the list. Also a guarantee that all values failing these predicates
    /// have multiplicity zero.
    ///
    /// When unset this indicates that no such predicates are known, and the
    /// subject collection may be an arbitrary subset (but it will be a subset).
    ///
    /// When set to `Some(Vec::new())` this indicates that the subject collection
    /// has been unfiltered, and reflects all values from the other collection.
    /// No guarantees about the cardinalities, but the set of values is the same.
    pub filters: Option<Vec<MirScalarExpr>>,
}

impl ProvenanceInfo {
    /// Create a new `Self` directly referencing `id`.
    ///
    /// Each column is exactly the source column, and no filters have been applied.
    fn new(id: Id, arity: usize) -> Self {
        ProvenanceInfo {
            id,
            columns: (0..arity).map(|c| Some(MirScalarExpr::Column(c))).collect(),
            filters: Some(Vec::new()),
        }
    }
}

/// Updates `this` to reflect the meet of `this` and `that`.
///
/// The meet of two `ProvenanceInfo` values is the most conservative information
/// that is true about both of the inputs.
///
/// Each list represents a disjunction of possibilities, but each resulting possibility
/// must involve at least one element from each list. We can take the cross product of
/// each list, and then discard any elements that speak about different collections,
/// or which have incompatible column expressions and
fn meet_prov_vecs(this: &mut Vec<ProvenanceInfo>, that: &[ProvenanceInfo]) {
    let mut meet = Vec::new();
    for prov1 in this.iter() {
        for prov2 in that.iter() {
            if prov1.id == prov2.id {
                // Intersect columns.
                let columns: Vec<_> = prov1
                    .columns
                    .iter()
                    .zip(prov2.columns.iter())
                    .map(|(c1, c2)| if c1 == c2 { c1.clone() } else { None })
                    .collect();
                let filters = match (&prov1.filters, &prov2.filters) {
                    (Some(f1), Some(f2)) if f1 == f2 => Some(f1.clone()),
                    _ => None,
                };
                if columns.iter().any(Option::is_some) {
                    meet.push(ProvenanceInfo {
                        id: prov1.id,
                        columns,
                        filters,
                    });
                }
            }
        }
    }
    *this = meet;
}
