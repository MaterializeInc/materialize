// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove redundant collections of distinct elements from joins.

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
        // println!("PRE: {}", relation.pretty());
        self.action(relation, &mut HashMap::new());
        // println!("PST: {}", relation.pretty());
        Ok(())
    }
}

impl RedundantJoin {
    /// Remove redundant collections of distinct elements from joins.
    ///
    /// This method recursively determines "provenance" information for the relation, that being a set of
    /// pairs of `Id` and column correspondences, indicating that some columns in this relation correspond
    /// to columns in the bound `Id`; specifically, that projected on to these columns, the distinct rows
    /// of this relation would be contained in the distinct rows of the identified relation.
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
                let mut val_info = lets.get(id).map(|x| x.clone()).unwrap_or(Vec::new());
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
                // Recursively apply transformation, and determine input provenance.
                let mut input_prov = inputs
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
                // We only do this once per invocation to keep our sanity, but we could rewrite it to iterate.
                if let Some((input, bindings)) = (0..input_types.len())
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
                    // println!("Found redundancy: \n\tinput {:?}\n\tbinding {:?}", input, bindings);

                    let mut columns = 0;
                    let mut projection = Vec::new();
                    for i in 0..input_arities.len() {
                        if i != input {
                            projection.extend(columns..columns + input_arities[i]);
                            columns += input_arities[i];
                        } else {
                            // This should happen only once, and `.drain(..)` could work.
                            projection.extend(bindings.clone());
                        }
                    }
                    // Bindings need to be refreshed, now that we know where each target column will be.
                    // This is important because they could have been to columns *after* the removed
                    // relation, and our original take on where they would be is no longer correct.
                    for c in prior_arities[input]..prior_arities[input] + input_arities[input] {
                        projection[c] = projection[projection[c]];
                    }

                    for equivalence in equivalences.iter_mut() {
                        for expr in equivalence.iter_mut() {
                            expr.permute(&projection[..]);
                        }
                        equivalence.sort();
                        equivalence.dedup();
                    }
                    equivalences.retain(|es| es.len() > 1);

                    inputs.remove(input);
                    input_prov.remove(input);

                    // Unset demand and implementation, as irrevocably hosed by this transformation.
                    *demand = None;
                    *implementation = expr::JoinImplementation::Unimplemented;

                    *relation = relation.take_dangerous().project(projection);
                    // The projection will gum up provenance reasoning anyhow, so don't work hard.
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
                // Filter drops records, and so is not set to `exact`.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::Map { input, .. } => self.action(input, lets),

            RelationExpr::Union { left, right } => {
                self.action(left, lets);
                self.action(right, lets);
                Vec::new()
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
                // TODO: For min, max aggregates, we could preserve provenance
                // if the expression references a column.
                result
            }

            RelationExpr::Threshold { input } => {
                // Threshold may drop records, and so is not set to `exact`.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::TopK { input, .. } => {
                // TopK may drop records, and so is not exact.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::Project { input, .. } => {
                self.action(input, lets);
                // Projections generally get hoisted, and we can
                // wait until that happens to implement this analysis.
                Vec::new()
            }

            RelationExpr::FlatMap { input, .. } => {
                // FlatMap may drop records, and so is not set to `exact`.
                let mut result = self.action(input, lets);
                for prov in result.iter_mut() {
                    prov.exact = false;
                }
                result
            }

            RelationExpr::Negate { input } => {
                // Negate changes the sign on its multiplicities,
                // which means "distinct" counts would now be -1.
                // We set `exact` to false to inhibit the optimization,
                // but should probably fix `.keys` instead.
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
#[derive(Clone, Debug)]
pub struct ProvInfo {
    // The Id (local or global) of the source
    id: Id,
    // A list of source column, current column associations.
    // There should be at most one occurrence of each number in the second position.
    binding: Vec<(usize, usize)>,
    // Are these the exact set of columns, with none omitted?
    exact: bool,
}

// Attempts to discover a replacement relation for `input`, and replacement column bindings for its columns.
fn find_redundancy(
    input: usize,
    keys: &[Vec<usize>],
    input_arities: &[usize],
    prior_arities: &[usize],
    equivalences: &[Vec<ScalarExpr>],
    input_prov: &[Vec<ProvInfo>],
) -> Option<Vec<usize>> {
    // println!();
    // println!("Input: {}", input);
    // println!("Arities: {:?}", input_arities);
    // println!("Prior: {:?}", prior_arities);
    // println!("Equiv: {:?}", equivalences);
    // println!("Prov: {:?}", input_prov);

    // A join input can be removed if
    //   1. it contains distinct records (i.e. has a key),
    //   2. it has some `exact` provenance for all columns,
    //   3. each of its key columns are equated to another input whose provenance contains it.
    // If the input is removed, all references to its columns should be replaced by references
    // to the corresponding columns in the other input.

    for provenance in input_prov[input].iter() {
        // We can only elide if the input contains all records, and binds all columns.
        if provenance.exact && provenance.binding.len() == input_arities[input] {
            // examine all *other* inputs that have not been removed...
            for other in 0..input_arities.len() {
                if other != input {
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
                        if bindings.len() == input_arities[input] {
                            for key in keys.iter() {
                                if key.iter().all(|input_col| {
                                    let other_col = bindings[&input_col];
                                    equivalences.iter().any(|e| {
                                        e.contains(&ScalarExpr::Column(
                                            prior_arities[input] + input_col,
                                        )) && e.contains(&ScalarExpr::Column(
                                            prior_arities[other] + other_col,
                                        ))
                                    })
                                }) {
                                    let binding = (0..input_arities[input])
                                        .map(|c| prior_arities[other] + bindings[&c])
                                        .collect::<Vec<_>>();
                                    // println!("Found redundancy for {:?}", input);
                                    // println!("\tother: {:?}", other);
                                    // println!("\tbinding: {:?}", binding);
                                    return Some(binding);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    None
}
