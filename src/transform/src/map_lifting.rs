// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use mz_expr::visit::Visit;
use mz_expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::{Row, RowPacker};

use crate::TransformArgs;

/// Hoist literal values from maps wherever possible.
#[derive(Debug)]
pub struct LiteralLifting {
    recursion_guard: RecursionGuard,
}

impl Default for LiteralLifting {
    fn default() -> LiteralLifting {
        LiteralLifting {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for LiteralLifting {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for LiteralLifting {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let literals = self.action(relation, &mut HashMap::new())?;
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
    /// columns. But in those cases where it is possible, permutations are
    /// used to move all of the literals to the final columns, and then rely
    /// on projection hoisting to allow the these literals to move up the AST.
    ///
    /// TODO: The literals from the final columns are returned as the result
    /// of this method, whereas literals in intermediate columns are extracted
    /// using permutations. The reason for this different treatment is that in
    /// some cases it is not possible to remove the projection of the
    /// permutation, preventing the lifting of a literal that could otherwise
    /// be lifted, the following example being of them:
    ///
    /// %0 =
    /// | Constant (1, 2, 3) (2, 2, 3)
    ///
    /// %1 =
    /// | Constant (4, 3, 3) (4, 5, 3)
    ///
    /// %2 =
    /// | Union %0 %1
    ///
    /// If final literals weren't treated differently, the example above would
    /// lead to the following transformed plan:
    ///
    /// %0 =
    /// | Constant (1) (2)
    /// | Map 2, 3
    /// | Project (#0..#2)
    ///
    /// %1 =
    /// | Constant (3) (5)
    /// | Map 4, 3
    /// | Project (#1, #0, #2)
    ///
    /// %2 =
    /// | Union %0 %1
    ///
    /// Since the union branches have different projections, they cannot be
    /// removed, preventing literal 3 from being lifted further.
    ///
    /// In theory, all literals could be treated in the same way if this method
    /// returned both a list of literals and a projection vector, making the
    /// caller have to deal with the reshuffling.
    /// (see <https://github.com/MaterializeInc/materialize/issues/6598>)
    ///
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        // Map from names to literals required for appending.
        gets: &mut HashMap<Id, Vec<MirScalarExpr>>,
    ) -> Result<Vec<MirScalarExpr>, crate::TransformError> {
        self.checked_recur(|_| {
            match relation {
                MirRelationExpr::Constant { rows, typ } => {
                    // From the back to the front, check if all values are identical.
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

                        // Any subset of constant values can be extracted with a permute.
                        let remaining_common_literals = the_same.iter().filter(|e| **e).count();
                        if remaining_common_literals > 0 {
                            let final_arity = the_same.len() - remaining_common_literals;
                            let mut projected_literals = Vec::new();
                            let mut projection = Vec::new();
                            let mut new_column_types = Vec::new();
                            for (i, sameness) in the_same.iter().enumerate() {
                                if *sameness {
                                    projection.push(final_arity + projected_literals.len());
                                    projected_literals.push(MirScalarExpr::literal_ok(
                                        data[i],
                                        typ.column_types[i].scalar_type.clone(),
                                    ));
                                } else {
                                    projection.push(new_column_types.len());
                                    new_column_types.push(typ.column_types[i].clone());
                                }
                            }
                            typ.column_types = new_column_types;

                            // Tidy up the type information of `relation`.
                            for key in typ.keys.iter_mut() {
                                *key = key
                                    .iter()
                                    .filter(|x| !the_same.get(**x).unwrap_or(&true))
                                    .map(|x| projection[*x])
                                    .collect::<Vec<usize>>();
                            }
                            typ.keys.sort();
                            typ.keys.dedup();

                            let remove_extracted_literals = |row: &mut Row| {
                                let mut new_row = Row::default();
                                let mut packer = new_row.packer();
                                let data = row.unpack();
                                for i in 0..the_same.len() {
                                    if !the_same[i] {
                                        packer.push(data[i]);
                                    }
                                }
                                *row = new_row;
                            };

                            remove_extracted_literals(row);
                            for (row, _cnt) in rows.iter_mut() {
                                remove_extracted_literals(row);
                            }

                            *relation = relation
                                .take_dangerous()
                                .map(projected_literals)
                                .project(projection);
                        } else if !literals.is_empty() {
                            // Tidy up the type information of `relation`.
                            for key in typ.keys.iter_mut() {
                                key.retain(|k| k < &data.len());
                            }
                            typ.keys.sort();
                            typ.keys.dedup();

                            RowPacker::for_existing_row(row).truncate_datums(typ.arity());
                            for (row, _cnt) in rows.iter_mut() {
                                RowPacker::for_existing_row(row).truncate_datums(typ.arity());
                            }
                        }

                        Ok(literals)
                    } else {
                        Ok(Vec::new())
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
                    Ok(literals)
                }
                MirRelationExpr::Let { id, value, body } => {
                    // Any literals appended to the `value` should be used
                    // at corresponding `Get`s throughout the `body`.
                    let literals = self.action(value, gets)?;
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
                    let mut literals = self.action(input, gets)?;
                    if !literals.is_empty() {
                        let input_arity = input.arity();
                        // For each input literal contains a vector with the `output` positions
                        // that references it. By putting data into a Vec and sorting, we
                        // guarantee a reliable order.
                        let mut used_literals = outputs
                            .iter()
                            .enumerate()
                            .filter(|(_, x)| **x >= input_arity)
                            .map(|(out_col, old_in_col)| (old_in_col - input_arity, out_col))
                            // group them to avoid adding duplicated literals
                            .into_group_map()
                            .drain()
                            .collect::<Vec<_>>();

                        if used_literals.len() != literals.len() {
                            used_literals.sort();
                            // Discard literals that are not projected
                            literals = used_literals
                                .iter()
                                .map(|(old_in_col, _)| literals[*old_in_col].clone())
                                .collect::<Vec<_>>();
                            // Update the references to the literal in `output`
                            for (new_in_col, (_old_in_col, out_cols)) in
                                used_literals.iter().enumerate()
                            {
                                for out_col in out_cols {
                                    outputs[*out_col] = input_arity + new_in_col;
                                }
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
                    Ok(Vec::new())
                }
                MirRelationExpr::Map { input, scalars } => {
                    let mut literals = self.action(input, gets)?;

                    // Make the map properly formed again.
                    literals.extend(scalars.iter().cloned());
                    *scalars = literals;

                    // Strip off literals at the end of `scalars`.
                    let mut result = Vec::new();
                    while scalars.last().map(|e| e.is_literal()) == Some(true) {
                        result.push(scalars.pop().unwrap());
                    }
                    result.reverse();

                    if scalars.is_empty() {
                        *relation = input.take_dangerous();
                    } else {
                        // Permute columns to put literals at end, if any, hope project lifted.
                        let literal_count = scalars.iter().filter(|e| e.is_literal()).count();
                        if literal_count != 0 {
                            let input_arity = input.arity();
                            let first_literal_id = input_arity + scalars.len() - literal_count;
                            let mut new_scalars = Vec::new();
                            let mut projected_literals = Vec::new();
                            let mut projection = (0..input_arity).collect::<Vec<usize>>();
                            for scalar in scalars.iter_mut() {
                                if scalar.is_literal() {
                                    projection.push(first_literal_id + projected_literals.len());
                                    projected_literals.push(scalar.clone());
                                } else {
                                    let mut cloned_scalar = scalar.clone();
                                    // Propagate literals through expressions and remap columns.
                                    cloned_scalar.visit_mut_post(&mut |e| {
                                        if let MirScalarExpr::Column(old_id) = e {
                                            let new_id = projection[*old_id];
                                            if new_id >= first_literal_id {
                                                *e = projected_literals[new_id - first_literal_id]
                                                    .clone();
                                            } else {
                                                *old_id = new_id;
                                            }
                                        }
                                    })?;
                                    projection.push(input_arity + new_scalars.len());
                                    new_scalars.push(cloned_scalar);
                                }
                            }
                            new_scalars.extend(projected_literals);
                            *relation = input.take_dangerous().map(new_scalars).project(projection);
                        }
                    }

                    Ok(result)
                }
                MirRelationExpr::FlatMap { input, func, exprs } => {
                    let literals = self.action(input, gets)?;
                    if !literals.is_empty() {
                        let input_arity = input.arity();
                        for expr in exprs.iter_mut() {
                            expr.visit_mut_post(&mut |e| {
                                if let MirScalarExpr::Column(c) = e {
                                    if *c >= input_arity {
                                        *e = literals[*c - input_arity].clone();
                                    }
                                }
                            })?;
                        }
                        // Permute the literals around the columns added by FlatMap
                        let mut projection = (0..input_arity).collect::<Vec<usize>>();
                        let func_arity = func.output_arity();
                        projection
                            .extend((0..literals.len()).map(|x| input_arity + func_arity + x));
                        projection.extend((0..func_arity).map(|x| input_arity + x));

                        *relation = relation.take_dangerous().map(literals).project(projection);
                    }
                    Ok(Vec::new())
                }
                MirRelationExpr::Filter { input, predicates } => {
                    let literals = self.action(input, gets)?;
                    if !literals.is_empty() {
                        // We should be able to instantiate all uses of `literals`
                        // in predicates and then lift the `map` around the filter.
                        let input_arity = input.arity();
                        for expr in predicates.iter_mut() {
                            expr.visit_mut_post(&mut |e| {
                                if let MirScalarExpr::Column(c) = e {
                                    if *c >= input_arity {
                                        *e = literals[*c - input_arity].clone();
                                    }
                                }
                            })?;
                        }
                    }
                    Ok(literals)
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    implementation,
                } => {
                    // before lifting, save the original shape of the inputs
                    let old_input_mapper = JoinInputMapper::new(inputs);

                    // lift literals from each input
                    let mut input_literals = Vec::new();
                    for mut input in inputs.iter_mut() {
                        let literals = self.action(input, gets)?;

                        // Do not propagate error literals beyond join inputs, since that may result
                        // in them being propagated to other inputs of the join and evaluated when
                        // they should not.
                        if literals.iter().any(|l| l.is_literal_err()) {
                            // Push the literal errors beyond any arrangement since otherwise JoinImplementation
                            // would add another arrangement on top leading to an infinite loop/stack overflow.
                            if let MirRelationExpr::ArrangeBy { input, .. } = &mut input {
                                **input = input.take_dangerous().map(literals);
                            } else {
                                *input = input.take_dangerous().map(literals);
                            }
                            input_literals.push(Vec::new());
                        } else {
                            input_literals.push(literals);
                        }
                    }

                    if input_literals.iter().any(|l| !l.is_empty()) {
                        *implementation = mz_expr::JoinImplementation::Unimplemented;

                        // We should be able to install any literals in the
                        // equivalence relations, and then lift all literals
                        // around the join using a project to re-order columns.

                        // Visit each expression in each equivalence class to either
                        // inline literals or update column references.
                        let new_input_mapper = JoinInputMapper::new(inputs);
                        for equivalence in equivalences.iter_mut() {
                            for expr in equivalence.iter_mut() {
                                expr.visit_mut_post(&mut |e| {
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
                                })?;
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
                    Ok(Vec::new())
                }
                MirRelationExpr::Reduce {
                    input,
                    group_key,
                    aggregates,
                    monotonic: _,
                    expected_group_size: _,
                } => {
                    let literals = self.action(input, gets)?;
                    if !literals.is_empty() {
                        // Reduce absorbs maps, and we should inline literals.
                        let input_arity = input.arity();
                        // Inline literals into group key expressions.
                        for expr in group_key.iter_mut() {
                            expr.visit_mut_post(&mut |e| {
                                if let MirScalarExpr::Column(c) = e {
                                    if *c >= input_arity {
                                        *e = literals[*c - input_arity].clone();
                                    }
                                }
                            })?;
                        }
                        // Inline literals into aggregate value selector expressions.
                        for aggr in aggregates.iter_mut() {
                            aggr.expr.visit_mut_post(&mut |e| {
                                if let MirScalarExpr::Column(c) = e {
                                    if *c >= input_arity {
                                        *e = literals[*c - input_arity].clone();
                                    }
                                }
                            })?;
                        }
                    }

                    let eval_constant_aggr = |aggr: &mz_expr::AggregateExpr| {
                        let temp = mz_repr::RowArena::new();
                        let mut eval = aggr.expr.eval(&[], &temp);
                        if let Ok(param) = eval {
                            eval = Ok(aggr.func.eval(Some(param), &temp));
                        }
                        MirScalarExpr::literal(
                            eval,
                            // This type information should be available in the `a.expr` literal,
                            // but extracting it with pattern matching seems awkward.
                            aggr.func
                                .output_type(aggr.expr.typ(&mz_repr::RelationType::empty()))
                                .scalar_type,
                        )
                    };

                    // The only literals we think we can lift are those that are
                    // independent of the number of records; things like `Any`, `All`,
                    // `Min`, and `Max`.
                    let mut result = Vec::new();
                    while aggregates.last().map(|a| a.is_constant()) == Some(true) {
                        let aggr = aggregates.pop().unwrap();
                        result.push(eval_constant_aggr(&aggr));
                    }
                    if aggregates.is_empty() {
                        while group_key.last().map(|k| k.is_literal()) == Some(true) {
                            let key = group_key.pop().unwrap();
                            result.push(key);
                        }
                    }
                    result.reverse();

                    // Add a Map operator with the remaining literals so that they are lifted in
                    // the next invocation of this transform.
                    let non_literal_keys = group_key.iter().filter(|x| !x.is_literal()).count();
                    let non_constant_aggr = aggregates.iter().filter(|x| !x.is_constant()).count();
                    if non_literal_keys != group_key.len() || non_constant_aggr != aggregates.len()
                    {
                        let first_projected_literal: usize = non_literal_keys + non_constant_aggr;
                        let mut projection = Vec::new();
                        let mut projected_literals = Vec::new();

                        let mut new_group_key = Vec::new();
                        for key in group_key.drain(..) {
                            if key.is_literal() {
                                projection.push(first_projected_literal + projected_literals.len());
                                projected_literals.push(key);
                            } else {
                                projection.push(new_group_key.len());
                                new_group_key.push(key);
                            }
                        }
                        // The new group key without literals
                        *group_key = new_group_key;

                        let mut new_aggregates = Vec::new();
                        for aggr in aggregates.drain(..) {
                            if aggr.is_constant() {
                                projection.push(first_projected_literal + projected_literals.len());
                                projected_literals.push(eval_constant_aggr(&aggr));
                            } else {
                                projection.push(group_key.len() + new_aggregates.len());
                                new_aggregates.push(aggr);
                            }
                        }
                        // The new aggregates without constant ones
                        *aggregates = new_aggregates;

                        *relation = relation
                            .take_dangerous()
                            .map(projected_literals)
                            .project(projection);
                    }
                    Ok(result)
                }
                MirRelationExpr::TopK {
                    input,
                    group_key,
                    order_key,
                    limit: _,
                    offset: _,
                    monotonic: _,
                } => {
                    let literals = self.action(input, gets)?;
                    if !literals.is_empty() {
                        // We should be able to lift literals out, as they affect neither
                        // grouping nor ordering. We should discard grouping and ordering
                        // that references the columns, though.
                        let input_arity = input.arity();
                        group_key.retain(|c| *c < input_arity);
                        order_key.retain(|o| o.column < input_arity);
                    }
                    Ok(literals)
                }
                MirRelationExpr::Negate { input } => {
                    // Literals can just be lifted out of negate.
                    self.action(input, gets)
                }
                MirRelationExpr::Threshold { input } => {
                    // Literals can just be lifted out of threshold.
                    self.action(input, gets)
                }
                MirRelationExpr::Union { base, inputs } => {
                    let mut base_literals = self.action(base, gets)?;

                    let mut input_literals = vec![];
                    for input in inputs.iter_mut() {
                        input_literals.push(self.action(input, gets)?)
                    }

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
                    Ok(suffix)
                }
                MirRelationExpr::ArrangeBy { input, keys } => {
                    // TODO(frank): Not sure if this is the right behavior,
                    // as we disrupt the set of used arrangements. Though,
                    // we are probably most likely to use arranged `Get`
                    // operators rather than those decorated with maps.
                    let literals = self.action(input, gets)?;
                    if !literals.is_empty() {
                        let input_arity = input.arity();
                        for key in keys.iter_mut() {
                            for expr in key.iter_mut() {
                                expr.visit_mut_post(&mut |e| {
                                    if let MirScalarExpr::Column(c) = e {
                                        if *c >= input_arity {
                                            *e = literals[*c - input_arity].clone();
                                        }
                                    }
                                })?;
                            }
                        }
                    }
                    Ok(literals)
                }
            }
        })
    }
}
