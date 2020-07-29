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

use expr::{GlobalId, Id, LocalId};

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
    pub fn action(&self, relation: &mut RelationExpr, lets: &mut HashMap<LocalId, RelationExpr>) {
        if let RelationExpr::Let { id, value, body } = relation {
            self.action(value, lets);
            let old = lets.insert(*id, (**value).clone());
            self.action(body, lets);
            if let Some(old) = old {
                lets.insert(*id, old);
            } else {
                lets.remove(id);
            }
        } else {
            relation.visit1_mut(|relation| self.action(relation, lets))
        }

        if let RelationExpr::Join {
            inputs,
            equivalences,
            demand,
            implementation,
        } = relation
        {
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

            let provenance: Vec<_> = inputs
                .iter()
                .map(|inp| compute_provenance(inp, lets))
                .collect();

            let are_cols_equiv = |(rel1, col1), (rel2, col2)| {
                let expr1 = ScalarExpr::Column(prior_arities[rel1] + col1);
                let expr2 = ScalarExpr::Column(prior_arities[rel2] + col2);
                equivalences
                    .iter()
                    .any(|equiv| equiv.contains(&expr1) && equiv.contains(&expr2))
            };

            let can_elide_self = |index: usize, prior: usize| {
                let keys = inputs[index].typ().keys;
                inputs[index] == inputs[prior]
                    && keys
                        .iter()
                        .any(|key| key.iter().all(|k| are_cols_equiv((index, *k), (prior, *k))))
            };

            // Check whether we've already joined a distinct copy of the same
            // collection, in which case the join is a no-op and the prior
            // collection can be elided.
            let can_elide_prior = |index: usize, prior: usize| {
                let prov_me = provenance[index].as_ref()?;
                let prov_prior = provenance[prior].as_ref()?;
                if !prov_prior.known_distinct {
                    return None;
                }
                let mut permutation = vec![];
                for (pos_prior, prov) in prov_prior.columns.iter().enumerate() {
                    let pos_me = prov_me.columns.iter().position(|p| p == prov)?;
                    if !are_cols_equiv((index, pos_me), (prior, pos_prior)) {
                        return None;
                    }
                    permutation.push(pos_me);
                }
                Some((prior, permutation))
            };

            // It is possible that two inputs are the same, and joined on columns that form a key for them.
            // If so, we can remove one of them, and replace references to it with corresponding references
            // to the other.
            let mut columns = 0;
            let mut projection = Vec::new();
            let mut to_remove = Vec::new();
            for index in 0..inputs.len() {
                let priors = (0..index).filter(|i| !to_remove.contains(i));
                if let Some(prior) = priors.clone().find(|prior| can_elide_self(index, *prior)) {
                    projection.extend(
                        prior_arities[prior]..(prior_arities[prior] + input_arities[prior]),
                    );
                    to_remove.push(index);
                // TODO: check for relation repetition in any variable.
                } else if let Some((prior, permutation)) = priors
                    .clone()
                    .find_map(|prior| can_elide_prior(index, prior))
                {
                    // NOTE(benesch): this logic is maximally sketchy and is
                    // probably wrong. Using physical column indices makes this
                    // adjustment really hard to do.
                    let shift_point = projection
                        .splice(
                            prior_arities[prior]..(prior_arities[prior] + input_arities[prior]),
                            permutation.iter().map(|p| columns + p),
                        )
                        .max();
                    projection.extend(columns..(columns + input_arities[index]));
                    if let Some(shift_point) = shift_point {
                        for p in &mut projection {
                            if *p > shift_point {
                                *p -= input_arities[prior];
                            }
                        }
                    }
                    columns = columns - input_arities[prior] + input_arities[index];
                    to_remove.push(prior);
                } else {
                    projection.extend(columns..(columns + input_arities[index]));
                    columns += input_arities[index];
                }
            }

            // Update constraints to reference `prior`. Shift subsequent references.
            if !to_remove.is_empty() {
                // remove in reverse order.
                while let Some(index) = to_remove.pop() {
                    inputs.remove(index);
                }
                for equivalence in equivalences.iter_mut() {
                    for expr in equivalence.iter_mut() {
                        expr.permute(&projection[..]);
                    }
                    equivalence.sort();
                    equivalence.dedup();
                }
                equivalences.retain(|v| v.len() > 1);
                *demand = None;
            }

            // Implement a projection if the projection removed any columns.
            let orig_arity = input_arities.iter().sum::<usize>();
            if projection.len() != orig_arity || projection.iter().enumerate().any(|(i, p)| i != *p)
            {
                *implementation = expr::JoinImplementation::Unimplemented;
                *relation = relation.take_dangerous().project(projection);
            }
        }
    }
}

/// For each column in a relation, tracks the upstream relation from which it
/// came.
///
/// This presently only works for columns that come directly from a global
/// collection. With functioning unbind/deduplicate transformations, we'd
/// probably want to track this at the level of LocalIds, but at the moment
/// plans often have several let bindings for identical collections.
#[derive(Debug)]
struct Provenance {
    columns: Vec<(GlobalId, usize)>,
    known_distinct: bool,
}

impl Provenance {
    fn permute(mut self, permutation: &[usize]) -> Self {
        self.columns = permutation.iter().map(|i| self.columns[*i]).collect();
        self
    }

    fn distinct(mut self) -> Self {
        self.known_distinct = true;
        self
    }
}

fn compute_provenance(
    relation: &RelationExpr,
    lets: &HashMap<LocalId, RelationExpr>,
) -> Option<Provenance> {
    match relation {
        RelationExpr::Get { id, typ } => match id {
            Id::Global(id) => Some(Provenance {
                columns: (0..typ.arity()).map(|i| (*id, i)).collect(),
                known_distinct: false,
            }),
            Id::Local(id) => compute_provenance(&lets[id], lets),
        },

        RelationExpr::Reduce {
            input,
            aggregates,
            group_key,
        } if aggregates.is_empty() => {
            let group_key = extract_simple_group_key(group_key)?;
            let prov = compute_provenance(input, lets)?;
            Some(prov.permute(&group_key).distinct())
        }

        RelationExpr::TopK { input, .. } => compute_provenance(input, lets),

        RelationExpr::ArrangeBy { input, .. } => compute_provenance(input, lets),

        _ => None,
    }
}

fn extract_simple_group_key(key: &[ScalarExpr]) -> Option<Vec<usize>> {
    let mut out = vec![];
    for expr in key {
        match expr {
            ScalarExpr::Column(i) => out.push(*i),
            _ => return None,
        }
    }
    Some(out)
}
