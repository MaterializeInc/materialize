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
//! When we observe a candidate `A semijoin B`, in which the columns of `B`
//! are unique keys and are all equated to columns of A, we:
//!
//! 1. Look for a `Get{id}` that populates the columns of `A`, perhaps with some filtering.
//! 2. Chase down `B` looking for another semijoin between `Get{id}` and some `S`.
//! 3. After some careful tests, and futzing around, replace `B` by `S`.
//!
//! The intent is that we only perform the replacement when we are certain that the
//! columns surfaced by `B` are those of `Get{id}` restricted by `S`, permuted so that
//! the `A semijoin B` equates like columns of `Get{id}`.
//!
//! We give ourselves permission to allow `A` to be an arbitrarily filtered `Get{id}`:
//! any restrictions we apply to `Get{id}` in producing `A` do not invalidate the transform.
//! If we find in `B` a filtered `Get{id}` we attempt to transform those filters to `S`,
//! which we can only do if the predicates are on the equated columns.
//!
//! Much more work could be done to incorporate projections, and potentially other ways
//! that the colunms of `A` or `B` could derive from `Get{id}`.
use std::collections::{BTreeMap, HashMap};

use crate::TransformArgs;

use mz_expr::Id;
use mz_expr::JoinInputMapper;
use mz_expr::MirRelationExpr;
use mz_expr::MirScalarExpr;

/// Remove redundant semijoin operators
#[derive(Debug)]
pub struct SemijoinIdempotence;

impl crate::Transform for SemijoinIdempotence {
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
        {
            // Iteratively descend the expression, at each node attempting to simplify a join.
            let mut bindings = BTreeMap::<Id, Vec<Replacement>>::new();
            let mut rebindings = BTreeMap::<Id, Vec<(Id, Vec<MirScalarExpr>)>>::new();
            let mut worklist = vec![&mut *relation];
            while let Some(expr) = worklist.pop() {
                match expr {
                    MirRelationExpr::Let { id, value, .. } => {
                        bindings.insert(
                            Id::Local(*id),
                            semijoin_replacements(&*value, &bindings, &rebindings),
                        );
                        rebindings.insert(Id::Local(*id), as_filtered_get(value, &rebindings));
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
                            &bindings,
                            &rebindings,
                        );
                    }
                    _ => {}
                }

                // Continue to process the children.
                worklist.extend(expr.children_mut());
            }
        }

        mz_repr::explain_new::trace_plan(&*relation);
        Ok(())
    }
}

/// Attempt to simplify the join using local information and let bindings.
fn attempt_join_simplification(
    inputs: &mut [MirRelationExpr],
    equivalences: &mut Vec<Vec<MirScalarExpr>>,
    implementation: &mut mz_expr::JoinImplementation,
    bindings: &BTreeMap<Id, Vec<Replacement>>,
    rebindings: &BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
) {
    // Useful join manipulation helper.
    let input_mapper = JoinInputMapper::new(inputs);

    if let Some((ltr, rtl)) = semijoin_bijection(inputs, equivalences) {
        // Collect the `Get` identifiers each input might present as.
        let ids0 = as_filtered_get(&inputs[0], rebindings)
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        let ids1 = as_filtered_get(&inputs[1], rebindings)
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();

        // Consider replacing the second input for the benefit of the first.
        if distinct_on_keys_of(&inputs[1], &rtl)
            && input_mapper.input_arity(1) == equivalences.len()
        {
            for mut candidate in semijoin_replacements(&inputs[1], bindings, rebindings) {
                if ids0.contains(&candidate.id) {
                    if let Some(permutation) = validate_replacement(&ltr, &mut candidate) {
                        inputs[1] = candidate.replacement.project(permutation);
                        *implementation = mz_expr::JoinImplementation::Unimplemented;

                        // Take a moment to think about pushing down `IS NOT NULL` tests.
                        let typ0 = inputs[0].typ().column_types;
                        let typ1 = inputs[1].typ().column_types;
                        let mut is_not_nulls = Vec::new();
                        for (col0, col1) in ltr.iter() {
                            if !typ1[*col1].nullable && typ0[*col0].nullable {
                                use mz_expr::UnaryFunc;
                                is_not_nulls.push(
                                    MirScalarExpr::Column(*col0)
                                        .call_unary(UnaryFunc::IsNull(mz_expr::func::IsNull))
                                        .call_unary(UnaryFunc::Not(mz_expr::func::Not)),
                                )
                            }
                        }
                        if !is_not_nulls.is_empty() {
                            inputs[0] = inputs[0].take_dangerous().filter(is_not_nulls);
                        }

                        // GTFO because things are now crazy.
                        return;
                    }
                }
            }
        }
        // Consider replacing the second input for the benefit of the first.
        if distinct_on_keys_of(&inputs[0], &ltr)
            && input_mapper.input_arity(0) == equivalences.len()
        {
            for mut candidate in semijoin_replacements(&inputs[0], bindings, rebindings) {
                if ids1.contains(&candidate.id) {
                    if let Some(permutation) = validate_replacement(&rtl, &mut candidate) {
                        inputs[0] = candidate.replacement.project(permutation);
                        *implementation = mz_expr::JoinImplementation::Unimplemented;

                        // Take a moment to think about pushing down `IS NOT NULL` tests.
                        let typ0 = inputs[0].typ().column_types;
                        let typ1 = inputs[1].typ().column_types;
                        let mut is_not_nulls = Vec::new();
                        for (col1, col0) in rtl.iter() {
                            if !typ0[*col0].nullable && typ1[*col1].nullable {
                                use mz_expr::UnaryFunc;
                                is_not_nulls.push(
                                    MirScalarExpr::Column(*col1)
                                        .call_unary(UnaryFunc::IsNull(mz_expr::func::IsNull))
                                        .call_unary(UnaryFunc::Not(mz_expr::func::Not)),
                                )
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
/// Returns a projection to apply to `candidate.replacement` if everything checks out,
/// as well as the equivalence between columns of semijoin and the replacement.
fn validate_replacement(
    map: &HashMap<usize, usize>,
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
/// This identifies two inputs to a semijoin, one of which is a `Get{id}` and the other an arbitrary relation.
/// We also indicate which columns were equated, from each inputs, as well as a permutation to the columns of
/// the latter which can allow us to substitute it in for the relation that offers this information.
#[derive(Clone, Debug)]
struct Replacement {
    id: Id,
    // matching columns from `id`, `other`, and `replacement` respectively.
    columns: Vec<(usize, usize, usize)>,
    replacement: MirRelationExpr,
}

/// Return a list of potential semijoin fits, expressed as a list of
/// 1. candidate let binding identifier,
/// 2. expressions on which the semijoin has occurred.
/// 3. replacement `MirRelationExpr`
fn semijoin_replacements(
    expr: &MirRelationExpr,
    bindings: &BTreeMap<Id, Vec<Replacement>>,
    rebindings: &BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
) -> Vec<Replacement> {
    let mut results = Vec::new();
    match expr {
        MirRelationExpr::Get { id, .. } => {
            // The `Get` may reference an `id` that offers semijoin replacements.
            if let Some(replacements) = bindings.get(id) {
                results.extend(replacements.iter().cloned());
            }
        }
        MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } => {
            results.extend(list_replacements(inputs, equivalences, rebindings));
        }
        MirRelationExpr::Project { input, outputs } => {
            // If the columns are preserved by projection ..
            results.extend(
                semijoin_replacements(input, bindings, rebindings)
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
                semijoin_replacements(input, bindings, rebindings)
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
            results.extend(semijoin_replacements(input, bindings, rebindings));
        }
        _ => {}
    }
    results
}

/// Attempts to re-cast a binary join as a certain form of semijoin, where it can identify:
///
/// 1. A `Get(id)` input,
/// 2. An other input with unique keys (columns),
/// 3. Expressions that equate those keys to columns in the first input.
///
/// If the join is exactly these things (no additional inputs or equivalences) it is bundled
/// up as a `Replacement` identifying each. A join could potentially have multiple replacements,
/// although maybe math prevents that from happening, with some deeper thought.
fn list_replacements(
    inputs: &[MirRelationExpr],
    equivalences: &Vec<Vec<MirScalarExpr>>,
    rebindings: &BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
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

            for (id, mut predicates) in as_filtered_get(&inputs[0], rebindings) {
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

            for (id, mut predicates) in as_filtered_get(&inputs[1], rebindings) {
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
fn distinct_on_keys_of(expr: &MirRelationExpr, map: &HashMap<usize, usize>) -> bool {
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
    bindings: &BTreeMap<Id, Vec<(Id, Vec<MirScalarExpr>)>>,
) -> Vec<(Id, Vec<MirScalarExpr>)> {
    let mut results = Vec::new();
    while let MirRelationExpr::Filter { input, predicates } = expr {
        results.extend(predicates.iter().cloned());
        expr = &**input;
    }
    if let MirRelationExpr::Get { id, .. } = expr {
        let mut output = Vec::new();
        if let Some(bound) = bindings.get(id) {
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
) -> Option<(HashMap<usize, usize>, HashMap<usize, usize>)> {
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
        let ltr: std::collections::HashMap<_, _> = equiv_pairs.iter().cloned().collect();
        let rtl: std::collections::HashMap<_, _> =
            equiv_pairs.iter().map(|(c0, c1)| (*c1, *c0)).collect();

        Some((ltr, rtl))
    } else {
        None
    }
}
