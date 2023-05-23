// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove semijoins that are applied multiple times to no further effect.
//!
//! Mechanically, this transform looks for instances of `A join B` and replaces
//! `B` with a simpler `C`. It does this in the restricted setting that each `join`
//! would be a "semijoin": a multiplicity preserving restriction.
//!
//! The approach we use here is to restrict our attention to cases where
//!
//! 1. `A` is a potentially filtered instance of some `Get{id}`,
//! 2. `A join B` equate columns of `A` to all columns of `B`,
//! 3. The multiplicity of any record in `B` is at most one.
//! 4. The values in these records are exactly `Get{id} join C`.
//!
//! We find a candidate `C` by descending `B` looking for another semijoin between
//! `Get{id}` and some other collection `D` on the same columns as `A` means to join `B`.
//! Should we find such, allowing arbitrary filters of `Get{id}` on the equated columns,
//! which we will transfer to the columns of `D` thereby forming `C`.

use std::collections::BTreeMap;

use mz_expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};

use crate::TransformArgs;

/// Remove redundant semijoin operators
#[derive(Debug)]
pub struct SemijoinIdempotence {
    recursion_guard: RecursionGuard,
}

impl Default for SemijoinIdempotence {
    fn default() -> SemijoinIdempotence {
        SemijoinIdempotence {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for SemijoinIdempotence {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for SemijoinIdempotence {
    fn recursion_safe(&self) -> bool {
        true
    }

    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "semijoin_idempotence")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let mut let_replacements = BTreeMap::<Id, Vec<Replacement>>::new();
        let mut gets_behind_gets = BTreeMap::<Id, Vec<(Id, Vec<MirScalarExpr>)>>::new();
        self.action(relation, &mut let_replacements, &mut gets_behind_gets)?;

        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl SemijoinIdempotence {
    fn action(
        &self,
        expr: &mut MirRelationExpr,
        let_replacements: &mut BTreeMap<Id, Vec<Replacement>>,
        gets_behind_gets: &mut BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
    ) -> Result<(), crate::TransformError> {
        // At each node, either gather info about Let bindings or attempt to simplify a join.
        Ok(self.checked_recur(|_| {
            match expr {
                MirRelationExpr::Let { id, value, body } => {
                    let_replacements.insert(
                        Id::Local(*id),
                        list_replacements(&*value, &let_replacements, &gets_behind_gets),
                    );
                    gets_behind_gets
                        .insert(Id::Local(*id), as_filtered_get(value, &gets_behind_gets));
                    self.action(value, let_replacements, gets_behind_gets)?;
                    self.action(body, let_replacements, gets_behind_gets)?;
                }
                MirRelationExpr::Join {
                    inputs,
                    equivalences,
                    implementation,
                    ..
                } => {
                    attempt_join_simplification(
                        inputs,
                        equivalences,
                        implementation,
                        &let_replacements,
                        &gets_behind_gets,
                    );
                    for input in inputs {
                        self.action(input, let_replacements, gets_behind_gets)?;
                    }
                }
                _ => {
                    for child in expr.children_mut() {
                        self.action(child, let_replacements, gets_behind_gets)?;
                    }
                }
            }
            Ok::<(), crate::TransformError>(())
        })?)
    }
}

/// Attempt to simplify the join using local information and let bindings.
fn attempt_join_simplification(
    inputs: &mut [MirRelationExpr],
    equivalences: &mut Vec<Vec<MirScalarExpr>>,
    implementation: &mut mz_expr::JoinImplementation,
    let_replacements: &BTreeMap<Id, Vec<Replacement>>,
    gets_behind_gets: &BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
) {
    // Useful join manipulation helper.
    let input_mapper = JoinInputMapper::new(inputs);

    if let Some((ltr, rtl)) = semijoin_bijection(inputs, equivalences) {
        // Collect the `Get` identifiers each input might present as.
        let ids0 = as_filtered_get(&inputs[0], gets_behind_gets)
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        let ids1 = as_filtered_get(&inputs[1], gets_behind_gets)
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();

        // Consider replacing the second input for the benefit of the first.
        if distinct_on_keys_of(&inputs[1], &rtl)
            && input_mapper.input_arity(1) == equivalences.len()
        {
            for mut candidate in list_replacements(&inputs[1], let_replacements, gets_behind_gets) {
                if ids0.contains(&candidate.id) {
                    if let Some(permutation) = validate_replacement(&ltr, &mut candidate) {
                        inputs[1] = candidate.replacement.project(permutation);
                        *implementation = mz_expr::JoinImplementation::Unimplemented;

                        // Take a moment to think about pushing down `IS NOT NULL` tests.
                        // The pushdown is for the benefit of CSE on the `A` expressions,
                        // in the not uncommon case of nullable foreign keys in outer joins.
                        // TODO: Discover the transform that would not require this code.
                        let typ0 = inputs[0].typ().column_types;
                        let typ1 = inputs[1].typ().column_types;
                        let mut is_not_nulls = Vec::new();
                        for (col0, col1) in ltr.iter() {
                            if !typ1[*col1].nullable && typ0[*col0].nullable {
                                is_not_nulls.push(MirScalarExpr::Column(*col0).call_is_null().not())
                            }
                        }
                        if !is_not_nulls.is_empty() {
                            // Canonicalize otherwise arbitrary predicate order.
                            is_not_nulls.sort();
                            inputs[0] = inputs[0].take_dangerous().filter(is_not_nulls);
                        }

                        // GTFO because things are now crazy.
                        return;
                    }
                }
            }
        }
        // Consider replacing the first input for the benefit of the second.
        if distinct_on_keys_of(&inputs[0], &ltr)
            && input_mapper.input_arity(0) == equivalences.len()
        {
            for mut candidate in list_replacements(&inputs[0], let_replacements, gets_behind_gets) {
                if ids1.contains(&candidate.id) {
                    if let Some(permutation) = validate_replacement(&rtl, &mut candidate) {
                        inputs[0] = candidate.replacement.project(permutation);
                        *implementation = mz_expr::JoinImplementation::Unimplemented;

                        // Take a moment to think about pushing down `IS NOT NULL` tests.
                        // The pushdown is for the benefit of CSE on the `A` expressions,
                        // in the not uncommon case of nullable foreign keys in outer joins.
                        // TODO: Discover the transform that would not require this code.
                        let typ0 = inputs[0].typ().column_types;
                        let typ1 = inputs[1].typ().column_types;
                        let mut is_not_nulls = Vec::new();
                        for (col1, col0) in rtl.iter() {
                            if !typ0[*col0].nullable && typ1[*col1].nullable {
                                is_not_nulls.push(MirScalarExpr::Column(*col1).call_is_null().not())
                            }
                        }
                        if !is_not_nulls.is_empty() {
                            inputs[1] = inputs[1].take_dangerous().filter(is_not_nulls);
                        }

                        // GTFO because things are now crazy.
                        return;
                    }
                }
            }
        }
    }
}

/// Evaluates the viability of a `candidate` to drive the replacement at `semijoin`.
///
/// Returns a projection to apply to `candidate.replacement` if everything checks out.
fn validate_replacement(
    map: &BTreeMap<usize, usize>,
    candidate: &mut Replacement,
) -> Option<Vec<usize>> {
    if candidate.columns.len() == map.len()
        && candidate
            .columns
            .iter()
            .all(|(c0, c1, _c2)| map.get(c0) == Some(c1))
    {
        candidate.columns.sort_by_key(|(_, c, _)| *c);
        Some(
            candidate
                .columns
                .iter()
                .map(|(_, _, c)| *c)
                .collect::<Vec<_>>(),
        )
    } else {
        None
    }
}

/// A restricted form of a semijoin idempotence information.
///
/// A `Replacement` may be offered up by any `MirRelationExpr`, meant to be `B` from above or similar,
/// and indicates that the offered expression can be projected onto columns such that it then exactly equals
/// a column projection of `Get{id} semijoin replacement`.

/// Specifically,
/// the `columns` member lists indexes `(a, b, c)` where column `b` of the offering expression corresponds to
/// columns `a` in `Get{id}` and `c` in `replacement`, and for which the semijoin requires `a = c`. The values
/// of the projection of the offering expression onto the `b` indexes exactly equal the intersection of the
/// projection of `Get{id}` onto the `a` indexes and the projection of `replacement` onto the `c` columns.
#[derive(Clone, Debug)]
struct Replacement {
    id: Id,
    columns: Vec<(usize, usize, usize)>,
    replacement: MirRelationExpr,
}

/// Return a list of potential semijoin replacements for `expr`.
///
/// This method descends recursively, traversing `Get`, `Project`, `Reduce`, and `ArrangeBy` operators
/// looking for a `Join` operator, at which point it defers to the `list_replacements_join` method.
fn list_replacements(
    expr: &MirRelationExpr,
    let_replacements: &BTreeMap<Id, Vec<Replacement>>,
    gets_behind_gets: &BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
) -> Vec<Replacement> {
    let mut results = Vec::new();
    match expr {
        MirRelationExpr::Get { id, .. } => {
            // The `Get` may reference an `id` that offers semijoin replacements.
            if let Some(replacements) = let_replacements.get(id) {
                results.extend(replacements.iter().cloned());
            }
        }
        MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } => {
            results.extend(list_replacements_join(
                inputs,
                equivalences,
                gets_behind_gets,
            ));
        }
        MirRelationExpr::Project { input, outputs } => {
            // If the columns are preserved by projection ..
            results.extend(
                list_replacements(input, let_replacements, gets_behind_gets)
                    .into_iter()
                    .filter_map(|mut replacement| {
                        let new_cols = replacement
                            .columns
                            .iter()
                            .filter_map(|(c0, c1, c2)| {
                                outputs.iter().position(|o| o == c1).map(|c| (*c0, c, *c2))
                            })
                            .collect::<Vec<_>>();
                        if new_cols.len() == replacement.columns.len() {
                            replacement.columns = new_cols;
                            Some(replacement)
                        } else {
                            None
                        }
                    }),
            );
        }
        MirRelationExpr::Reduce {
            input, group_key, ..
        } => {
            // If the columns are preserved by `group_key` ..
            results.extend(
                list_replacements(input, let_replacements, gets_behind_gets)
                    .into_iter()
                    .filter_map(|mut replacement| {
                        let new_cols = replacement
                            .columns
                            .iter()
                            .filter_map(|(c0, c1, c2)| {
                                group_key
                                    .iter()
                                    .position(|o| o == &MirScalarExpr::Column(*c1))
                                    .map(|c| (*c0, c, *c2))
                            })
                            .collect::<Vec<_>>();
                        if new_cols.len() == replacement.columns.len() {
                            replacement.columns = new_cols;
                            Some(replacement)
                        } else {
                            None
                        }
                    }),
            );
        }
        MirRelationExpr::ArrangeBy { input, .. } => {
            results.extend(list_replacements(input, let_replacements, gets_behind_gets));
        }
        _ => {}
    }
    results
}

/// Return a list of potential semijoin replacements for `expr`.
fn list_replacements_join(
    inputs: &[MirRelationExpr],
    equivalences: &Vec<Vec<MirScalarExpr>>,
    gets_behind_gets: &BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
) -> Vec<Replacement> {
    // Result replacements.
    let mut results = Vec::new();

    // If we are a binary join whose equivalence classes equate columns in the two inputs.
    if let Some((ltr, rtl)) = semijoin_bijection(inputs, equivalences) {
        // Each unique key could be a semijoin candidate.
        // We want to check that the join equivalences exactly match the key,
        // and then transcribe the corresponding columns in the other input.
        if distinct_on_keys_of(&inputs[1], &rtl) {
            let columns = ltr
                .iter()
                .map(|(k0, k1)| (*k0, *k0, *k1))
                .collect::<Vec<_>>();

            for (id, mut predicates) in as_filtered_get(&inputs[0], gets_behind_gets) {
                if predicates
                    .iter()
                    .all(|e| e.support().iter().all(|c| ltr.contains_key(c)))
                {
                    for predicate in predicates.iter_mut() {
                        predicate.permute_map(&ltr);
                    }

                    let mut replacement = inputs[1].clone();
                    if !predicates.is_empty() {
                        replacement = replacement.filter(predicates.clone());
                    }
                    results.push(Replacement {
                        id,
                        columns: columns.clone(),
                        replacement,
                    })
                }
            }
        }
        // Each unique key could be a semijoin candidate.
        // We want to check that the join equivalences exactly match the key,
        // and then transcribe the corresponding columns in the other input.
        if distinct_on_keys_of(&inputs[0], &ltr) {
            let columns = ltr
                .iter()
                .map(|(k0, k1)| (*k1, *k0, *k0))
                .collect::<Vec<_>>();

            for (id, mut predicates) in as_filtered_get(&inputs[1], gets_behind_gets) {
                if predicates
                    .iter()
                    .all(|e| e.support().iter().all(|c| rtl.contains_key(c)))
                {
                    for predicate in predicates.iter_mut() {
                        predicate.permute_map(&rtl);
                    }

                    let mut replacement = inputs[0].clone();
                    if !predicates.is_empty() {
                        replacement = replacement.filter(predicates.clone());
                    }
                    results.push(Replacement {
                        id,
                        columns: columns.clone(),
                        replacement,
                    })
                }
            }
        }
    }

    results
}

/// True iff some unique key of `input` is contained in the keys of `map`.
fn distinct_on_keys_of(expr: &MirRelationExpr, map: &BTreeMap<usize, usize>) -> bool {
    expr.typ()
        .keys
        .iter()
        .any(|key| key.iter().all(|k| map.contains_key(k)))
}

/// Attempts to interpret `expr` as filters applied to a `Get`.
///
/// Returns a list of such interpretations, potentially spanning `Let` bindings.
fn as_filtered_get(
    mut expr: &MirRelationExpr,
    gets_behind_gets: &BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
) -> Vec<(Id, Vec<MirScalarExpr>)> {
    let mut results = Vec::new();
    while let MirRelationExpr::Filter { input, predicates } = expr {
        results.extend(predicates.iter().cloned());
        expr = &**input;
    }
    if let MirRelationExpr::Get { id, .. } = expr {
        let mut output = Vec::new();
        if let Some(bound) = gets_behind_gets.get(id) {
            for (id, list) in bound.iter() {
                let mut predicates = list.clone();
                predicates.extend(results.iter().cloned());
                output.push((*id, predicates));
            }
        }
        output.push((*id, results));
        output
    } else {
        Vec::new()
    }
}

/// Determines bijection between equated columns of a binary join.
///
/// Returns nothing if not a binary join, or if any equivalences are not of two opposing columns.
/// Returned maps go from the column of the first input to those of the second, and vice versa.
fn semijoin_bijection(
    inputs: &[MirRelationExpr],
    equivalences: &Vec<Vec<MirScalarExpr>>,
) -> Option<(BTreeMap<usize, usize>, BTreeMap<usize, usize>)> {
    // Useful join manipulation helper.
    let input_mapper = JoinInputMapper::new(inputs);

    // Pairs of equated columns localized to inputs 0 and 1.
    let mut equiv_pairs = Vec::with_capacity(equivalences.len());

    // Populate `equiv_pairs`, ideally finding exactly one pair for each equivalence class.
    for eq in equivalences.iter() {
        if eq.len() == 2 {
            // The equivalence class could reference the inputs in either order, or be some
            // tangle of references (e.g. to both) that we want to avoid reacting to.
            match (
                input_mapper.single_input(&eq[0]),
                input_mapper.single_input(&eq[1]),
            ) {
                (Some(0), Some(1)) => {
                    let expr0 = input_mapper.map_expr_to_local(eq[0].clone());
                    let expr1 = input_mapper.map_expr_to_local(eq[1].clone());
                    if let (MirScalarExpr::Column(col0), MirScalarExpr::Column(col1)) =
                        (expr0, expr1)
                    {
                        equiv_pairs.push((col0, col1));
                    }
                }
                (Some(1), Some(0)) => {
                    let expr0 = input_mapper.map_expr_to_local(eq[1].clone());
                    let expr1 = input_mapper.map_expr_to_local(eq[0].clone());
                    if let (MirScalarExpr::Column(col0), MirScalarExpr::Column(col1)) =
                        (expr0, expr1)
                    {
                        equiv_pairs.push((col0, col1));
                    }
                }
                _ => {}
            }
        }
    }

    if inputs.len() == 2 && equiv_pairs.len() == equivalences.len() {
        let ltr = equiv_pairs.iter().cloned().collect();
        let rtl = equiv_pairs.iter().map(|(c0, c1)| (*c1, *c0)).collect();

        Some((ltr, rtl))
    } else {
        None
    }
}
