// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hoist literal values from maps wherever possible.
//!
//! This transform specifically looks for `MirRelationExpr::Map` operators
//! where any of the `ScalarExpr` expressions are literals. Whenever it
//! can, it lifts those expressions through or around operators.
//!
//! The main feature of this operator is that it allows transformations
//! to locally change the shape of operators, presenting fewer columns
//! when they are unused and replacing them with mapped default values.
//! The mapped default values can then be lifted and ideally absorbed.
//! This type of transformation is difficult to make otherwise, as it
//! is not easy to locally change the shape of relations.

use std::collections::HashMap;

use itertools::Itertools;

use expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr};

use crate::TransformArgs;

/// Hoist literal values from maps wherever possible.
#[derive(Debug)]
pub struct LiteralLifting;

impl crate::Transform for LiteralLifting {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let literals = self.action(relation, &mut HashMap::new());
        if !literals.is_empty() {
            // Literals return up the root should be re-installed.
            *relation = relation.take_dangerous().map(literals);
        }
        Ok(())
    }
}

impl LiteralLifting {
    /// Hoist literal values from maps wherever possible.
    ///
    /// Returns a list of literal scalar expressions that must be appended
    /// to the result before it can be correctly used. The intent is that
    /// this action extracts a maximal set of literals from `relation`,
    /// which can then often be propagated further up and inlined in any
    /// expressions as it goes.
    ///
    /// In several cases, we only manage to extract literals from the final
    /// columns. This could be improved using permutations to move all of
    /// the literals to the final columns, and then rely on projection
    /// hoisting to allow the these literals to move up the AST.
    // TODO(frank): Fix this.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        // Map from names to literals required for appending.
        gets: &mut HashMap<Id, Vec<MirScalarExpr>>,
    ) -> Vec<MirScalarExpr> {
        match relation {
            MirRelationExpr::Constant { rows, typ } => {
                // From the back to the front, check if all values are identical.
                // TODO(frank): any subset of constant values can be extracted with a permute.
                let mut the_same = vec![true; typ.arity()];
                if let Ok([(row, _cnt), rows @ ..]) = rows.as_deref_mut() {
                    let mut data = row.unpack();
                    assert_eq!(the_same.len(), data.len());
                    for (row, _cnt) in rows.iter() {
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
                        literals.push(MirScalarExpr::literal_ok(datum, typum.scalar_type));
                    }
                    literals.reverse();

                    if !literals.is_empty() {
                        // Tidy up the type information of `relation`.
                        for key in typ.keys.iter_mut() {
                            key.retain(|k| k < &data.len());
                        }
                        typ.keys.sort();
                        typ.keys.dedup();

                        row.truncate_datums(typ.arity());
                        for (row, _cnt) in rows.iter_mut() {
                            row.truncate_datums(typ.arity());
                        }
                    }
                    literals
                } else {
                    Vec::new()
                }
            }
            MirRelationExpr::Get { id, typ } => {
                // A get expression may need to have literal expressions appended to it.
                let literals = gets.get(id).cloned().unwrap_or_else(Vec::new);
                if !literals.is_empty() {
                    // Correct the type of the `Get`, which has fewer columns,
                    // and not the same fields in its keys. It is ok to remove
                    // any columns from the keys, as them being literals meant
                    // that their distinctness was not what made anything a key.
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
            MirRelationExpr::Let { id, value, body } => {
                // Any literals appended to the `value` should be used
                // at corresponding `Get`s throughout the `body`.
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
            MirRelationExpr::Project { input, outputs } => {
                // We do not want to lift literals around projections.
                // Projections are the highest lifted operator and lifting
                // literals around projections could cause us to fail to
                // reach a fixed point under the transformations.
                let mut literals = self.action(input, gets);
                if !literals.is_empty() {
                    let input_arity = input.arity();
                    if let Some(project_max) = outputs.iter().max() {
                        // Discard literals that are not projected.
                        // TODO(frank): this could also discard intermediate
                        // literals that are not projected, with more thought.
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
            MirRelationExpr::Map { input, scalars } => {
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
            MirRelationExpr::FlatMap {
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
                            if let MirScalarExpr::Column(c) = e {
                                if *c >= input_arity {
                                    *e = literals[*c - input_arity].clone();
                                }
                            }
                        });
                    }
                    // We need to re-install literals, as we don't know what flatmap does.
                    // TODO(frank): We could permute the literals around the columns
                    // added by FlatMap.
                    **input = input.take_dangerous().map(literals);
                }
                Vec::new()
            }
            MirRelationExpr::Filter { input, predicates } => {
                let literals = self.action(input, gets);
                if !literals.is_empty() {
                    // We should be able to instantiate all uses of `literals`
                    // in predicates and then lift the `map` around the filter.
                    let input_arity = input.arity();
                    for expr in predicates.iter_mut() {
                        expr.visit_mut(&mut |e| {
                            if let MirScalarExpr::Column(c) = e {
                                if *c >= input_arity {
                                    *e = literals[*c - input_arity].clone();
                                }
                            }
                        });
                    }
                }
                literals
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                demand,
                implementation,
            } => {
                // before lifting, save the original shape of the inputs
                let old_input_mapper = JoinInputMapper::new(inputs);

                // lift literals from each input
                let mut input_literals = Vec::new();
                for input in inputs.iter_mut() {
                    input_literals.push(self.action(input, gets));
                }

                if input_literals.iter().any(|l| !l.is_empty()) {
                    *demand = None;
                    *implementation = expr::JoinImplementation::Unimplemented;

                    // We should be able to install any literals in the
                    // equivalence relations, and then lift all literals
                    // around the join using a project to re-order columns.

                    // Visit each expression in each equivalence class to either
                    // inline literals or update column references.
                    let new_input_mapper = JoinInputMapper::new(inputs);
                    for equivalence in equivalences.iter_mut() {
                        for expr in equivalence.iter_mut() {
                            expr.visit_mut(&mut |e| {
                                if let MirScalarExpr::Column(c) = e {
                                    let (col, input) = old_input_mapper.map_column_to_local(*c);
                                    if col >= new_input_mapper.input_arity(input) {
                                        // the column refers to a literal that
                                        // has been promoted. inline it
                                        *e = input_literals[input]
                                            [col - new_input_mapper.input_arity(input)]
                                        .clone()
                                    } else {
                                        // localize to the new join
                                        *c = new_input_mapper.map_column_to_global(col, input);
                                    }
                                }
                            });
                        }
                    }

                    // We now determine a projection to shovel around all of
                    // the columns that puts the literals last. Where this is optional
                    // for other operators, it is mandatory here if we want to lift the
                    // literals through the join.

                    // The first literal column number starts at the last column
                    // of the new join. Increment the column number as literals
                    // get added.
                    let mut literal_column_number = new_input_mapper.total_columns();
                    let mut projection = Vec::new();
                    for input in 0..old_input_mapper.total_inputs() {
                        for column in old_input_mapper.local_columns(input) {
                            if column >= new_input_mapper.input_arity(input) {
                                projection.push(literal_column_number);
                                literal_column_number += 1;
                            } else {
                                projection
                                    .push(new_input_mapper.map_column_to_global(column, input));
                            }
                        }
                    }

                    let literals = input_literals.into_iter().flatten().collect::<Vec<_>>();
                    *relation = relation.take_dangerous().map(literals).project(projection)
                }
                Vec::new()
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                let literals = self.action(input, gets);
                if !literals.is_empty() {
                    // Reduce absorbs maps, and we should inline literals.
                    let input_arity = input.arity();
                    // Inline literals into group key expressions.
                    for expr in group_key.iter_mut() {
                        expr.visit_mut(&mut |e| {
                            if let MirScalarExpr::Column(c) = e {
                                if *c >= input_arity {
                                    *e = literals[*c - input_arity].clone();
                                }
                            }
                        });
                    }
                    // Inline literals into aggregate value selector expressions.
                    for aggr in aggregates.iter_mut() {
                        aggr.expr.visit_mut(&mut |e| {
                            if let MirScalarExpr::Column(c) = e {
                                if *c >= input_arity {
                                    *e = literals[*c - input_arity].clone();
                                }
                            }
                        });
                    }
                }

                // The only literals we think we can lift are those that are
                // independent of the number of records; things like `Any`, `All`,
                // `Min`, and `Max`.
                // TODO(frank): extract non-terminal literals.
                let mut result = Vec::new();
                while aggregates.last().map(|a| {
                    (a.func == expr::AggregateFunc::Any || a.func == expr::AggregateFunc::All)
                        && a.expr.is_literal_ok()
                }) == Some(true)
                {
                    let aggr = aggregates.pop().unwrap();
                    let temp = repr::RowArena::new();
                    let eval = aggr
                        .func
                        .eval(Some(aggr.expr.eval(&[], &temp).unwrap()), &temp);
                    result.push(MirScalarExpr::literal_ok(
                        eval,
                        // This type information should be available in the `a.expr` literal,
                        // but extracting it with pattern matching seems awkward.
                        aggr.func
                            .output_type(aggr.expr.typ(&repr::RelationType::empty()))
                            .scalar_type,
                    ));
                }
                result.reverse();
                result
            }
            MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit: _,
                offset: _,
                monotonic: _,
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
            MirRelationExpr::Negate { input } => {
                // Literals can just be lifted out of negate.
                self.action(input, gets)
            }
            MirRelationExpr::Threshold { input } => {
                // Literals can just be lifted out of threshold.
                self.action(input, gets)
            }
            MirRelationExpr::DeclareKeys { input, .. } => self.action(input, gets),
            MirRelationExpr::Union { base, inputs } => {
                let mut base_literals = self.action(base, gets);
                let mut input_literals = inputs
                    .iter_mut()
                    .map(|input| self.action(input, gets))
                    .collect::<Vec<Vec<MirScalarExpr>>>();

                // We need to find the longest common suffix between all the arms of the union.
                let mut suffix = Vec::new();
                while !base_literals.is_empty()
                    && input_literals
                        .iter()
                        .all(|lits| lits.last() == base_literals.last())
                {
                    // Every arm agrees on the last value, so push it onto the shared suffix and
                    // remove it from each arm.
                    suffix.push(base_literals.last().unwrap().clone());
                    base_literals.pop();
                    for lits in input_literals.iter_mut() {
                        lits.pop();
                    }
                }

                // Because we pushed stuff onto the vector like a stack, we need to reverse it now.
                suffix.reverse();

                // Any remaining literals for each expression must be appended to that expression,
                // while the shared suffix is returned to continue traveling upwards.
                if !base_literals.is_empty() {
                    **base = base.take_dangerous().map(base_literals);
                }
                for (input, literals) in inputs.iter_mut().zip_eq(input_literals) {
                    if !literals.is_empty() {
                        *input = input.take_dangerous().map(literals);
                    }
                }
                suffix
            }
            MirRelationExpr::ArrangeBy { input, keys } => {
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
                                if let MirScalarExpr::Column(c) = e {
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
