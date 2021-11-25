// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr, RECURSION_LIMIT};
use ore::stack::{CheckedRecursion, RecursionGuard};

use crate::TransformArgs;

/// Remove redundant collections of distinct elements from joins.
#[derive(Debug)]
pub struct RedundantJoin {
    recursion_guard: RecursionGuard,
}

impl Default for RedundantJoin {
    fn default() -> RedundantJoin {
        RedundantJoin {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for RedundantJoin {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for RedundantJoin {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(relation, &mut HashMap::new()).map(|_| ())
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
    /// This provenance information is then used for the `MirRelationExpr::Join`
    /// variant to remove "redundant" joins, those that can be determined to
    /// neither restrict nor augment one of the input relations. Consult the
    /// `find_redundancy` method and its documentation for more detail.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        lets: &mut HashMap<Id, Vec<ProvInfo>>,
    ) -> Result<Vec<ProvInfo>, crate::TransformError> {
        self.checked_recur(|_| {
            match relation {
                MirRelationExpr::Let { id, value, body } => {
                    // Recursively determine provenance of the value.
                    let value_prov = self.action(value, lets)?;
                    let old = lets.insert(Id::Local(*id), value_prov);
                    let result = self.action(body, lets)?;
                    if let Some(old) = old {
                        lets.insert(Id::Local(*id), old);
                    } else {
                        lets.remove(&Id::Local(*id));
                    }
                    Ok(result)
                }
                MirRelationExpr::Get { id, typ } => {
                    // Extract the value provenance, or an empty list if unavailable.
                    let mut val_info = lets.get(id).cloned().unwrap_or_default();
                    // Add information about being exactly this let binding too.
                    val_info.push(ProvInfo {
                        id: *id,
                        binding: (0..typ.arity()).map(|c| (c, c)).collect::<Vec<_>>(),
                        exact: true,
                    });
                    Ok(val_info)
                }

                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    implementation,
                } => {
                    // This logic first applies what it has learned about its input provenance,
                    // and if it finds a redundant join input it removes it. In that case, it
                    // also fails to produce exciting provenance information, partly out of
                    // laziness and the challenge of ensuring it is correct. Instead, if it is
                    // unable to find a rendundant join it produces meaningful provenance information.

                    // Recursively apply transformation, and determine the provenance of inputs.
                    let mut input_prov = Vec::new();
                    for i in inputs.iter_mut() {
                        input_prov.push(self.action(i, lets)?);
                    }

                    // Determine useful information about the structure of the inputs.
                    let mut input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                    let old_input_mapper = JoinInputMapper::new_from_input_types(&input_types);

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
                                &old_input_mapper,
                                equivalences,
                                &input_prov[..],
                            )
                            .map(|b| (i, b))
                        })
                        .next()
                    {
                        inputs.remove(input);
                        input_types.remove(input);

                        let new_input_mapper = JoinInputMapper::new_from_input_types(&input_types);
                        // From `binding`, we produce the projection we will apply to the join
                        // once `input` is removed. This is valuable also to rewrite expressions
                        // in the join constraints.
                        let mut projection = Vec::new();
                        for i in 0..old_input_mapper.total_inputs() {
                            if i != input {
                                projection.extend(new_input_mapper.global_columns(if i < input {
                                    i
                                } else {
                                    i - 1
                                }));
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
                        for c in old_input_mapper.global_columns(input) {
                            projection[c] = projection[projection[c]];
                        }

                        // Tidy up equivalences rewriting column references with `projection` and
                        // removing any equivalence classes that are now redundant (e.g. those that
                        // related the columns of `input` with the relation that showed its redundancy)
                        for equivalence in equivalences.iter_mut() {
                            for expr in equivalence.iter_mut() {
                                expr.permute(&projection[..]);
                            }
                        }
                        expr::canonicalize::canonicalize_equivalences(equivalences, &input_types);

                        // Unset implementation, as irrevocably hosed by this transformation.
                        *implementation = expr::JoinImplementation::Unimplemented;

                        *relation = relation.take_dangerous().project(projection);
                        // The projection will gum up provenance reasoning anyhow, so don't work hard.
                        // We will return to this expression again with the same analysis.
                        Ok(Vec::new())
                    } else {
                        // Provenance information should be the union of input provenance information,
                        // with columns updated. Because rows may be dropped in the join, all `exact`
                        // bits should be un-set.
                        let mut results = Vec::new();
                        for (input, input_prov) in input_prov.into_iter().enumerate() {
                            for mut prov in input_prov {
                                prov.exact = false;
                                for (_src, inp) in prov.binding.iter_mut() {
                                    *inp = old_input_mapper.map_column_to_global(*inp, input);
                                }
                                results.push(prov);
                            }
                        }
                        Ok(results)
                    }
                }

                MirRelationExpr::Filter { input, .. } => {
                    // Filter may drop records, and so we unset `exact`.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::Map { input, .. } => self.action(input, lets),
                MirRelationExpr::DeclareKeys { input, .. } => self.action(input, lets),

                MirRelationExpr::Union { base, inputs } => {
                    let mut prov = self.action(base, lets)?;
                    for input in inputs {
                        let input_prov = self.action(input, lets)?;
                        // To merge a new list of provenances, we look at the cross
                        // produce of things we might know about each source.
                        // TODO(mcsherry): this can be optimized to use datastructures
                        // keyed by the source identifier.
                        let mut new_prov = Vec::new();
                        for l in prov {
                            new_prov.extend(input_prov.iter().flat_map(|r| l.meet(r)))
                        }
                        prov = new_prov;
                    }
                    Ok(prov)
                }

                MirRelationExpr::Constant { .. } => Ok(Vec::new()),

                MirRelationExpr::Reduce {
                    input, group_key, ..
                } => {
                    // Reduce yields its first few columns as a key, and produces
                    // all key tuples that were present in its input.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        // update the bindings. no need to update `exact`.
                        let new_bindings = group_key
                            .iter()
                            .enumerate()
                            .filter_map(|(i, e)| {
                                if let MirScalarExpr::Column(c) = e {
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
                    Ok(result)
                }

                MirRelationExpr::Threshold { input } => {
                    // Threshold may drop records, and so we unset `exact`.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::TopK { input, .. } => {
                    // TopK may drop records, and so we unset `exact`.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::Project { input, outputs } => {
                    // Projections re-order, drop, and duplicate columns,
                    // but they neither drop rows nor invent values.
                    let mut result = self.action(input, lets)?;
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
                    Ok(result)
                }

                MirRelationExpr::FlatMap { input, .. } => {
                    // FlatMap may drop records, and so we unset `exact`.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::Negate { input } => {
                    // Negate does not guarantee that the multiplicity of
                    // each source record it at least one. This could have
                    // been a problem in `Union`, where we might report
                    // that the union of positive and negative records is
                    // "exact": cancellations would make this false.
                    let mut result = self.action(input, lets)?;
                    for prov in result.iter_mut() {
                        prov.exact = false;
                    }
                    Ok(result)
                }

                MirRelationExpr::ArrangeBy { input, .. } => self.action(input, lets),
            }
        })
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
    input_mapper: &JoinInputMapper,
    equivalences: &[Vec<MirScalarExpr>],
    input_prov: &[Vec<ProvInfo>],
) -> Option<Vec<usize>> {
    for provenance in input_prov[input].iter() {
        // We can only elide if the input contains all records, and binds all columns.
        if provenance.exact && provenance.binding.len() == input_mapper.input_arity(input) {
            // examine all *other* inputs that have not been removed...
            for other in (0..input_mapper.total_inputs()).filter(|other| other != &input) {
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
                    let all_columns_equated =
                        |cols: &Vec<usize>| {
                            cols.iter().all(|input_col| {
                                let other_col = bindings[&input_col];
                                equivalences.iter().any(|e| {
                                    e.contains(&input_mapper.map_expr_to_global(
                                        MirScalarExpr::Column(*input_col),
                                        input,
                                    )) && e.contains(&input_mapper.map_expr_to_global(
                                        MirScalarExpr::Column(other_col),
                                        other,
                                    ))
                                })
                            })
                        };

                    // If all columns of `input` are bound, and any key columns of `input` are equated,
                    // the binding can be returned as mapping replacements for each input column.
                    if bindings.len() == input_mapper.input_arity(input)
                        && keys.iter().any(|key| all_columns_equated(key))
                    {
                        let binding = input_mapper
                            .local_columns(input)
                            .map(|c| input_mapper.map_column_to_global(bindings[&c], other))
                            .collect::<Vec<_>>();
                        return Some(binding);
                    }
                }
            }
        }
    }

    None
}
