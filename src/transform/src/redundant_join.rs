// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove redundant collections of distinct elements from joins.
//!
//! This analysis looks for joins in which one collection contains distinct
//! elements, and it can be determined that the join would only restrict the
//! results, and that the restriction is redundant (the other results would
//! not be reduced by the join).
//!
//! This type of optimization shows up often in subqueries, where distinct
//! collections are used in decorrelation, and afterwards often distinct
//! collections are then joined against the results.

// If statements seem a bit clearer in this case. Specialized methods
// that replace simple and common alternatives frustrate developers.
#![allow(clippy::comparison_chain, clippy::filter_next)]

use std::collections::HashMap;

use expr::Id;

use crate::{RelationExpr, ScalarExpr, TransformArgs};

/// Remove redundant collections of distinct elements from joins.
#[derive(Debug)]
pub struct RedundantJoin;

impl crate::Transform for RedundantJoin {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(relation, &mut HashMap::new());
        Ok(())
    }
}

impl RedundantJoin {
    /// Remove redundant collections of distinct elements from joins.
    ///
    /// This method tracks "provenance" information for each collections,
    /// those being column-wise relationships to identified collections
    /// (either imported collections, or let-bound collections). These
    /// relationships state that when projected on to these columns, the
    /// records of the one collection are contained in the records of the
    /// identified collection.
    ///
    /// This provenance information is then used for the `RelationExpr::Join`
    /// variant to remove "redundant" joins, those that can be determined to
    /// neither restrict nor augment one of the input relations. Consult the
    /// `find_redundancy` method and its documentation for more detail.
    pub fn action(
        &self,
        relation: &mut RelationExpr,
        lets: &mut HashMap<Id, Vec<ProvInfo>>,
    ) -> Vec<ProvInfo> {
        match relation {
            RelationExpr::Let { id, value, body } => {
                // Recursively determine provenance of the value.
                let value_prov = self.action(value, lets);
                let old = lets.insert(Id::Local(*id), value_prov);
                let result = self.action(body, lets);
                if let Some(old) = old {
                    lets.insert(Id::Local(*id), old);
                } else {
                    lets.remove(&Id::Local(*id));
                }
                result
            }
            RelationExpr::Get { id, typ } => {
                // Extract the value provenance, or an empty list if unavailable.
                let mut val_info = lets.get(id).cloned().unwrap_or_default();
                // Add information about being exactly this let binding too.
                val_info.push(ProvInfo {
                    id: *id,
                    binding: (0..typ.arity()).map(|c| (c, c)).collect::<Vec<_>>(),
                    exact: true,
                });
                val_info
            }

            RelationExpr::Join {
                inputs,
                equivalences,
                demand,
                implementation,
            } => {
                // This logic first applies what it has learned about its input provenance,
                // and if it finds a redundant join input it removes it. In that case, it
                // also fails to produce exciting provenance information, partly out of
                // laziness and the challenge of ensuring it is correct. Instead, if it is
                // unable to find a rendundant join it produces meaningful provenance information.

                // Recursively apply transformation, and determine the provenance of inputs.
                let input_prov = inputs
                    .iter_mut()
                    .map(|i| self.action(i, lets))
                    .collect::<Vec<_>>();

                // Determine useful information about the structure of the inputs.
                let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
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

                // If we find an input that can be removed, we should do so!
                // We only do this once per invocation to keep our sanity, but we could
                // rewrite it to iterate. We can avoid looking for any relation that
                // does not have keys, as it cannot be redundant in that case.
                if let Some((input, bindings)) = (0..input_types.len())
                    .rev()
                    .filter(|i| !input_types[*i].keys.is_empty())
                    .flat_map(|i| {
                        find_redundancy(
                            i,
                            &input_types[i].keys,
                            &input_arities[..],
                            &prior_arities[..],
                            equivalences,
                            &input_prov[..],
                        )
                        .map(|b| (i, b))
                    })
                    .next()
                {
                    // From `binding`, we produce the projection we will apply to the join
                    // once `input` is removed. This is valuable also to rewrite expressions
                    // in the join constraints.
                    let mut columns = 0;
                    let mut projection = Vec::new();
                    for i in 0..input_arities.len() {
                        if i != input {
                            projection.extend(columns..columns + input_arities[i]);
                            columns += input_arities[i];
                        } else {
                            // When we reach the removed relation, we should introduce
                            // references to the columns that are meant to replace these.
                            // This should happen only once, and `.drain(..)` would be correct.
                            projection.extend(bindings.clone());
                        }
                    }
                    // The references introduced from `bindings` need to be refreshed, now that we
                    // know where each target column will be. This is important because they could
                    // have been to columns *after* `input`, and our original take on where they
                    // would be is no longer correct. References before `input` should stay as they
                    // are, and references afterwards will likely be decreased.
                    for c in prior_arities[input]..prior_arities[input] + input_arities[input] {
                        projection[c] = projection[projection[c]];
                    }

                    // Tidy up equivalences rewriting column references with `projection` and
                    // removing any equivalence classes that are now redundant (e.g. those that
                    // related the columns of `input` with the relation that showed its redundancy)
                    for equivalence in equivalences.iter_mut() {
                        for expr in equivalence.iter_mut() {
                            expr.permute(&projection[..]);
                        }
                        equivalence.sort();
                        equivalence.dedup();
                    }
                    equivalences.retain(|es| es.len() > 1);

                    inputs.remove(input);

                    // Unset demand and implementation, as irrevocably hosed by this transformation.
                    *demand = None;
                    *implementation = expr::JoinImplementation::Unimplemented;

                    *relation = relation.take_dangerous().project(projection);
                    // The projection will gum up provenance reasoning anyhow, so don't work hard.
                    // We will return to this expression again with the same analysis.
                    Vec::new()
                } else {
                    // Provenance information should be the union of input provenance information,
                    // with columns updated. Because rows may be dropped in the join, all `exact`
                    // bits should be un-set.
                    let mut results = Vec::new();
                    for (input, input_prov) in input_prov.into_iter().enumerate() {
                        for mut prov in input_prov {
                            prov.exact = false;
                            for (_src, inp) in prov.binding.iter_mut() {
                                *inp += prior_arities[input];
                            }
                            results.push(prov);
                        }
                    }
                    results
                }
            }

            RelationExpr::Filter { input, .. } => {
                // Filter may drop records, and so we unset `exact`.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::Map { input, .. } => self.action(input, lets),

            RelationExpr::Union { left, right } => {
                let prov_l = self.action(left, lets);
                let prov_r = self.action(right, lets);
                let mut result = Vec::new();
                for l in prov_l {
                    result.extend(prov_r.iter().flat_map(|r| l.meet(r)))
                }
                result
            }

            RelationExpr::Constant { .. } => Vec::new(),

            RelationExpr::Reduce {
                input, group_key, ..
            } => {
                // Reduce yields its first few columns as a key, and produces
                // all key tuples that were present in its input.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    // update the bindings. no need to update `exact`.
                    let new_bindings = group_key
                        .iter()
                        .enumerate()
                        .filter_map(|(i, e)| {
                            if let ScalarExpr::Column(c) = e {
                                Some((i, c))
                            } else {
                                None
                            }
                        })
                        .filter_map(|(i, c)| {
                            // output column `i` corresponds to input column `c`.
                            prov.binding
                                .iter()
                                .find(|(_src, inp)| inp == c)
                                .map(|(src, _inp)| (*src, i))
                        })
                        .collect::<Vec<_>>();
                    prov.binding = new_bindings;
                }
                result.retain(|p| !p.binding.is_empty());
                // TODO: For min, max aggregates, we could preserve provenance
                // if the expression references a column. We would need to un-set
                // the `exact` bit in that case, and so we would want to keep both
                // sets of provenance information.
                result
            }

            RelationExpr::Threshold { input } => {
                // Threshold may drop records, and so we unset `exact`.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::TopK { input, .. } => {
                // TopK may drop records, and so we unset `exact`.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::Project { input, outputs } => {
                // Projections re-order, drop, and duplicate columns,
                // but they neither drop rows nor invent values.
                let mut result = self.action(input, lets);
                for provenance in result.iter_mut() {
                    let new_binding = outputs
                        .iter()
                        .enumerate()
                        .flat_map(|(i, c)| {
                            provenance
                                .binding
                                .iter()
                                .find(|(_, l)| l == c)
                                .map(|(s, _)| (*s, i))
                        })
                        .collect::<Vec<_>>();

                    provenance.binding = new_binding;
                }
                result
            }

            RelationExpr::FlatMap { input, .. } => {
                // FlatMap may drop records, and so we unset `exact`.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::Negate { input } => {
                // Negate does not guarantee that the multiplicity of
                // each source record it at least one. This could have
                // been a problem in `Union`, where we might report
                // that the union of positive and negative records is
                // "exact": cancellations would make this false.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::ArrangeBy { input, .. } => self.action(input, lets),
        }
    }
}

/// A relationship between a collections columns and some source columns.
///
/// An instance of this type indicates that some of the bearer's columns
/// derive from `id`. In particular, each column in the second element of
/// `binding` is derived from the column of `id` found in the corresponding
/// first element.
///
/// The guarantee is that projected on to these columns, the distinct values
/// of the bearer are contained in the set of distinct values of projected
/// columns of `id`. In the case that `exact` is set, the two sets are equal.
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct ProvInfo {
    // The Id (local or global) of the source.
    id: Id,
    // A list of (source column, current column) associations.
    // There should be at most one occurrence of each number in the second position.
    binding: Vec<(usize, usize)>,
    // If true, all distinct projected source rows are present in the rows of
    // the projection of the current collection. This constraint is lost as soon
    // as a transformation may drop records.
    exact: bool,
}

impl ProvInfo {
    /// Merge two constraints to find a constraint that satisfies both inputs.
    ///
    /// This method returns nothing if no columns are in common (either because
    /// difference sources are identified, or just no columns in common) and it
    /// intersects bindings and the `exact` bit.
    fn meet(&self, other: &Self) -> Option<Self> {
        if self.id == other.id {
            let mut result = self.clone();
            result.binding.retain(|b| other.binding.contains(b));
            result.exact &= other.exact;
            if !result.binding.is_empty() {
                Some(result)
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Attempts to find column bindings that make `input` redundant.
///
/// This method attempts to determine that `input` may be redundant by searching
/// the join structure for another relation `other` with provenance that contains some
/// provenance of `input`, and keys for `input` that are equated by the join to the
/// corresponding columns of `other` under their provenance. The `input` provenance
/// must also have its `exact` bit set.
///
/// In these circumstances, the claim is that because the key columns are equated and
/// determine non-key columns, any matches between `input` and
/// `other` will neither introduce new information to `other`, nor restrict the rows
/// of `other`, nor alter their multplicity.
fn find_redundancy(
    input: usize,
    keys: &[Vec<usize>],
    input_arities: &[usize],
    prior_arities: &[usize],
    equivalences: &[Vec<ScalarExpr>],
    input_prov: &[Vec<ProvInfo>],
) -> Option<Vec<usize>> {
    for provenance in input_prov[input].iter() {
        // We can only elide if the input contains all records, and binds all columns.
        if provenance.exact && provenance.binding.len() == input_arities[input] {
            // examine all *other* inputs that have not been removed...
            for other in (0..input_arities.len()).filter(|other| other != &input) {
                for other_prov in input_prov[other].iter().filter(|p| p.id == provenance.id) {
                    // We need to find each column of `input` bound in `other` with this provenance.
                    let mut bindings = HashMap::new();
                    for (src, input_col) in provenance.binding.iter() {
                        for (src2, other_col) in other_prov.binding.iter() {
                            if src == src2 {
                                bindings.insert(input_col, *other_col);
                            }
                        }
                    }

                    // True iff `col = binding[col]` is in `equivalences` for all `col` in `cols`.
                    let all_columns_equated = |cols: &Vec<usize>| {
                        cols.iter().all(|input_col| {
                            let other_col = bindings[&input_col];
                            equivalences.iter().any(|e| {
                                e.contains(&ScalarExpr::Column(prior_arities[input] + input_col))
                                    && e.contains(&ScalarExpr::Column(
                                        prior_arities[other] + other_col,
                                    ))
                            })
                        })
                    };

                    // If all columns of `input` are bound, and any key columns of `input` are equated,
                    // the binding can be returned as mapping replacements for each input column.
                    if bindings.len() == input_arities[input]
                        && keys.iter().any(|key| all_columns_equated(key))
                    {
                        let binding = (0..input_arities[input])
                            .map(|c| prior_arities[other] + bindings[&c])
                            .collect::<Vec<_>>();
                        return Some(binding);
                    }
                }
            }
        }
    }

    None
}
