// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{GlobalId, Id, RelationExpr, ScalarExpr};

/// Hoist literal maps wherever possible.
///
/// This transform specifically looks for `RelationExpr::Map` operators
/// where any of the `ScalarExpr` expressions are literals. Whenever it
/// can, it lifts those expressions through or around operators.
#[derive(Debug)]
pub struct LiteralLifting;

impl crate::transform::Transform for LiteralLifting {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) -> Result<(), super::TransformError> {
        self.transform(relation);
        Ok(())
    }
}

impl LiteralLifting {
    pub fn transform(&self, relation: &mut RelationExpr) {
        let scalars = self.action(relation, &mut HashMap::new());
        if !scalars.is_empty() {
            *relation = relation.take_dangerous().map(scalars);
        }
    }
    // Lift literals out from under `relation`.
    // Returns a list of literal scalar expressions that must be appended
    // to the result before it is used.
    pub fn action(
        &self,
        relation: &mut RelationExpr,
        // Map from names to literals required for appending.
        gets: &mut HashMap<Id, Vec<ScalarExpr>>,
    ) -> Vec<ScalarExpr> {
        match relation {
            RelationExpr::Constant { rows, typ } => {
                // From the back to the front, check if all values are identical
                let mut the_same = vec![true; typ.arity()];
                if let Some((row, _cnt)) = rows.first() {
                    let mut data = row.unpack();
                    assert_eq!(the_same.len(), data.len());
                    for (row, _cnt) in rows[1..].iter() {
                        let other = row.unpack();
                        assert_eq!(the_same.len(), other.len());
                        for index in 0..the_same.len() {
                            the_same[index] = the_same[index] && (data[index] == other[index]);
                        }
                    }
                    let mut literals = Vec::new();
                    while the_same.last() == Some(&true) {
                        the_same.pop();
                        let datum = data.pop().unwrap();
                        let typum = typ.column_types.pop().unwrap();
                        literals.push(ScalarExpr::literal_ok(datum, typum));
                    }
                    literals.reverse();

                    if !literals.is_empty() {
                        // Tidy up the type information of `relation`.
                        for key in typ.keys.iter_mut() {
                            key.retain(|k| k < &data.len());
                        }
                        typ.keys.sort();
                        typ.keys.dedup();

                        for (row, _cnt) in rows.iter_mut() {
                            *row = repr::Row::pack(row.unpack().into_iter().take(typ.arity()));
                        }
                    }
                    literals
                } else {
                    Vec::new()
                }
            }
            RelationExpr::Get { id, typ } => {
                // A get expression may need to have literal expressions appended to it.
                let literals = gets.get(id).cloned().unwrap_or_else(Vec::new);
                if !literals.is_empty() {
                    for _ in 0..literals.len() {
                        typ.column_types.pop();
                    }
                    let columns = typ.column_types.len();
                    for key in typ.keys.iter_mut() {
                        key.retain(|k| k < &columns);
                    }
                    typ.keys.sort();
                    typ.keys.dedup();
                }
                literals
            }
            RelationExpr::Let { id, value, body } => {
                let literals = self.action(value, gets);
                let id = Id::Local(*id);
                if !literals.is_empty() {
                    let prior = gets.insert(id, literals);
                    assert!(!prior.is_some());
                }
                let result = self.action(body, gets);
                gets.remove(&id);
                result
            }
            RelationExpr::Project { input, outputs } => {
                // For stability reasons, we do not want to lift
                // literals around projection. Projections are the
                // highest lifted operator and lifting literals
                // around them could cause oscillations.
                let mut literals = self.action(input, gets);
                if !literals.is_empty() {
                    let input_arity = input.arity();
                    if let Some(project_max) = outputs.iter().max() {
                        // Discard literals that are not projected.
                        // TODO(frank): this could also discard intermediate
                        // literals that are not projected, with more thougt.
                        while *project_max + 1 < input_arity + literals.len()
                            && !literals.is_empty()
                        {
                            literals.pop();
                        }
                    }
                    // If the literals need to be re-interleaved,
                    // we don't have much choice but to install a
                    // Map operator to do that under the project.
                    // Ideally this doesn't happen much, as projects
                    // get lifted too.
                    if !literals.is_empty() {
                        **input = input.take_dangerous().map(literals);
                    }
                }
                // Policy: Do not lift literals around projects.
                Vec::new()
            }
            RelationExpr::Map { input, scalars } => {
                let mut literals = self.action(input, gets);

                // Make the map properly formed again.
                literals.extend(scalars.iter().cloned());
                *scalars = literals;

                // TODO(frank) propagate evaluation through expressions.

                // Strip off literals at the end of `scalars`.
                // TODO(frank) permute columns to put literals at end, hope project lifted.
                let mut result = Vec::new();
                while scalars.last().map(|e| e.is_literal()) == Some(true) {
                    result.push(scalars.pop().unwrap());
                }
                result.reverse();

                if scalars.is_empty() {
                    *relation = input.take_dangerous();
                }

                result
            }
            RelationExpr::FlatMap {
                input,
                func: _,
                exprs,
                demand: _,
            } => {
                let literals = self.action(input, gets);
                if !literals.is_empty() {
                    let input_arity = input.arity();
                    for expr in exprs.iter_mut() {
                        expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                if *c >= input_arity {
                                    *e = literals[*c - input_arity].clone();
                                }
                            }
                        });
                    }

                    **input = input.take_dangerous().map(literals);
                }
                Vec::new()
            }
            RelationExpr::Filter { input, predicates } => {
                let literals = self.action(input, gets);
                if !literals.is_empty() {
                    // We should be able to instantiate all uses of `literals`
                    // in predicates and then lift the `map` around the filter.
                    let input_arity = input.arity();
                    for expr in predicates.iter_mut() {
                        expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                if *c >= input_arity {
                                    *e = literals[*c - input_arity].clone();
                                }
                            }
                        });
                    }
                }
                literals
            }
            RelationExpr::Join {
                inputs,
                equivalences,
                demand,
                implementation,
            } => {
                let mut input_literals = Vec::new();
                for input in inputs.iter_mut() {
                    input_literals.push(self.action(input, gets));
                }

                if input_literals.iter().any(|l| !l.is_empty()) {
                    *demand = None;
                    *implementation = crate::JoinImplementation::Unimplemented;

                    // We should be able to install any literals in the
                    // equivalence relations, and then lift all literals
                    // around the join using a project to re-order colunms.

                    let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                    let input_arities = input_types
                        .iter()
                        .zip(input_literals.iter())
                        .map(|(i, l)| i.column_types.len() + l.len())
                        .collect::<Vec<_>>();

                    let mut offset = 0;
                    let mut prior_arities = Vec::new();
                    for input in 0..inputs.len() {
                        prior_arities.push(offset);
                        offset += input_arities[input];
                    }

                    let input_relation = input_arities
                        .iter()
                        .enumerate()
                        .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                        .collect::<Vec<_>>();

                    // Visit each expression in each equivalence class to either
                    // inline literals or update column references. Each column
                    // reference should subtract off the total length of literals
                    // returned for strictly prior input relations.
                    for equivalence in equivalences.iter_mut() {
                        for expr in equivalence.iter_mut() {
                            expr.visit_mut(&mut |e| {
                                if let ScalarExpr::Column(c) = e {
                                    let input = input_relation[*c];
                                    let input_arity = input_types[input].column_types.len();
                                    if *c >= prior_arities[input] + input_arity {
                                        // Inline any input literal that has been promoted.
                                        *e = input_literals[input]
                                            [*c - (prior_arities[input] + input_arity)]
                                            .clone()
                                    } else {
                                        // Subtract off columns that have been promoted.
                                        *c -= input_literals[..input]
                                            .iter()
                                            .map(|l| l.len())
                                            .sum::<usize>();
                                    }
                                }
                            });
                        }
                    }

                    let old_arity = input_arities.iter().sum();
                    let new_arity =
                        old_arity - input_literals.iter().map(|l| l.len()).sum::<usize>();
                    let mut projection = Vec::new();
                    for column in 0..old_arity {
                        let input = input_relation[column];
                        let input_arity = input_types[input].column_types.len();
                        let prior_literals = input_literals[..input]
                            .iter()
                            .map(|l| l.len())
                            .sum::<usize>();
                        if column >= prior_arities[input] + input_arity {
                            projection.push(
                                new_arity
                                    + prior_literals
                                    + ((column - prior_arities[input]) - input_arity),
                            );
                        } else {
                            projection.push(column - prior_literals);
                        }
                    }
                    let literals = input_literals.into_iter().flatten().collect::<Vec<_>>();
                    *relation = relation.take_dangerous().map(literals).project(projection)
                }
                Vec::new()
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let literals = self.action(input, gets);
                if !literals.is_empty() {
                    // Reduce absorbs maps, and we should in-line literals.
                    let input_arity = input.arity();
                    // Inline literals into group key expressions.
                    for expr in group_key.iter_mut() {
                        expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                if *c >= input_arity {
                                    *e = literals[*c - input_arity].clone();
                                }
                            }
                        });
                    }
                    // Inline literals into aggregate value selector expressions.
                    for aggr in aggregates.iter_mut() {
                        aggr.expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                if *c >= input_arity {
                                    *e = literals[*c - input_arity].clone();
                                }
                            }
                        });
                    }
                }

                let mut result = Vec::new();
                while aggregates.last().map(|a| {
                    (a.func == crate::AggregateFunc::Any || a.func == crate::AggregateFunc::All)
                        && a.expr.is_literal_ok()
                }) == Some(true)
                {
                    let aggr = aggregates.pop().unwrap();
                    let temp = repr::RowArena::new();
                    let eval = aggr
                        .func
                        .eval(Some(aggr.expr.eval(&[], &temp).unwrap()), &temp);
                    result.push(ScalarExpr::literal_ok(
                        eval,
                        repr::ColumnType::new(repr::ScalarType::Bool),
                    ));
                }
                result.reverse();
                // TODO(frank): extract non-terminal literals.
                result
            }
            RelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit: _,
                offset: _,
            } => {
                let literals = self.action(input, gets);
                if !literals.is_empty() {
                    // We should be able to lift literals out, as they affect neither
                    // grouping nor ordering. We should discard grouping and ordering
                    // that references the columns, though.
                    let input_arity = input.arity();
                    group_key.retain(|c| *c < input_arity);
                    order_key.retain(|o| o.column < input_arity);
                }
                literals
            }
            RelationExpr::Negate { input } => {
                // Literals can just be lifted out of negate.
                self.action(input, gets)
            }
            RelationExpr::Threshold { input } => {
                // Literals can just be lifted out of threshold.
                self.action(input, gets)
            }
            RelationExpr::Union { left, right } => {
                // We cannot, in general, lift literals out of unions.
                let mut l_literals = self.action(left, gets);
                let mut r_literals = self.action(right, gets);

                let mut results = Vec::new();
                while !l_literals.is_empty() && l_literals.last() == r_literals.last() {
                    l_literals.pop();
                    results.push(r_literals.pop().unwrap());
                }
                results.reverse();

                if !l_literals.is_empty() {
                    **left = left.take_dangerous().map(l_literals);
                }
                if !r_literals.is_empty() {
                    **right = right.take_dangerous().map(r_literals);
                }

                results
            }
            RelationExpr::ArrangeBy { input, keys } => {
                // TODO(frank): Not sure if this is the right behavior,
                // as we disrupt the set of used arrangements. Though,
                // we are probably most likely to use arranged `Get`
                // operators rather than those decorated with maps.
                let literals = self.action(input, gets);
                if !literals.is_empty() {
                    let input_arity = input.arity();
                    for key in keys.iter_mut() {
                        for expr in key.iter_mut() {
                            expr.visit_mut(&mut |e| {
                                if let ScalarExpr::Column(c) = e {
                                    if *c >= input_arity {
                                        *e = literals[*c - input_arity].clone();
                                    }
                                }
                            });
                        }
                    }
                }
                literals
            }
        }
    }
}
