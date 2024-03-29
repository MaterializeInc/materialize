// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use mz_expr::{MirRelationExpr, MirScalarExpr};

use crate::plan::expr::{HirRelationExpr, HirScalarExpr};
use crate::plan::PlanError;

use crate::plan::lowering::{ColumnMap, Config, CteMap};

/// Attempt to render a stack of left joins as an inner join against "enriched" right relations.
///
/// This optimization applies for a contiguous block of left joins where the `right` term is not
/// correlated, and where the `on` constraints equate columns in `right` to expressions over some
/// single prior joined relation (`left`, or a prior `right`).
///
/// The plan is to enrich each `right` with any missing key values, extracted by applying the equated
/// expressions to the source collection and then introducing them to an "augmented" right relation.
/// The introduced records are augmented with null values where missing, and an additional column that
/// indicates whether the data are original or augmented (important for masking out introduced keys).
///
/// Importantly, we need to introduce the constraints that equate columns and expressions in the `Join`,
/// as a `Filter` will still use SQL's equality, which treats NULL as unequal (we want them to match).
/// We could replace each `(col = expr)` with `(col = expr OR (col IS NULL AND expr IS NULL))`.
pub(crate) fn attempt_left_join_magic(
    left: &HirRelationExpr,
    rights: Vec<(&HirRelationExpr, &HirScalarExpr)>,
    id_gen: &mut mz_ore::id_gen::IdGen,
    get_outer: MirRelationExpr,
    col_map: &ColumnMap,
    cte_map: &mut CteMap,
    config: &Config,
) -> Result<Option<MirRelationExpr>, PlanError> {
    use mz_expr::LocalId;

    tracing::debug!(
        inputs = rights.len() + 1,
        outer_arity = get_outer.arity(),
        "attempt_left_join_magic"
    );

    let oa = get_outer.arity();
    if oa > 0 {
        // Bail out in correlated contexts for now. Even though the code below
        // supports them, we want to test this code path more thoroughly before
        // enabling this.
        tracing::debug!(case = 1, oa, "attempt_left_join_magic");
        return Ok(None);
    }

    // Will contain a list of let binding obligations.
    // We may modify the values if we find promising prior values.
    let mut bindings = Vec::new();
    let mut augmented = Vec::new();
    // A vector associating result columns with their corresponding input number
    // (where 0 idicates columns from the outer context).
    let mut bound_to = (0..oa).map(|_| 0).collect::<Vec<_>>();
    // A vector associating inputs with their arities (where the [0] entry
    // correponds to the arity of the outer context).
    let mut arities = vec![oa];

    // Left relation, its type, and its arity.
    let left = left
        .clone()
        .applied_to(id_gen, get_outer.clone(), col_map, cte_map, config)?;
    let lt = left.typ().column_types.into_iter().skip(oa).collect_vec();
    let la = lt.len();

    // Create a new let binding to use as input.
    // We may use these relations multiple times to extract augmenting values.
    let id = LocalId::new(id_gen.allocate_id());
    // The join body that we will iteratively develop.
    let mut body = MirRelationExpr::local_get(id, left.typ());
    bindings.push((id, body.clone(), left));
    bound_to.extend((0..la).map(|_| 1));
    arities.push(la);

    // "body arity": number of columns in `body`; the join we are building.
    let mut ba = la;

    // For each LEFT JOIN, there is a `right` input and an `on` constraint.
    // We want to decorrelate them, failing if there are subqueries because omg no,
    // and then check to see if the decorrelated `on` equates RHS columns with values
    // in one prior input. If so; bring those values into the mix, and bind that as
    // the value of the `Let` binding.
    for (index, (right, on)) in rights.into_iter().rev().enumerate() {
        // Correlated right expressions are handled in a different branch than standard
        // outer join lowering, and I don't know what they mean. Fail conservatively.
        if right.is_correlated() {
            tracing::debug!(case = 2, index, "attempt_left_join_magic");
            return Ok(None);
        }

        // Decorrelate `right`.
        let right_col_map = col_map.enter_scope(0);
        let right = right
            .clone()
            .map(vec![HirScalarExpr::literal_true()]) // add a bit to mark "real" rows.
            .applied_to(id_gen, get_outer.clone(), &right_col_map, cte_map, config)?;
        let rt = right.typ().column_types.into_iter().skip(oa).collect_vec();
        let ra = rt.len() - 1; // don't count the new column

        let mut right_type = right.typ();
        // Create a binding for `right`, unadulterated.
        let id = LocalId::new(id_gen.allocate_id());
        let get_right = MirRelationExpr::local_get(id, right_type.clone());
        // Create a binding for the augmented right, which we will form here but use before we do.
        // We want the join to be based off of the augmented relation, but we don't yet know how
        // to augment it until we decorrelate `on`. So, we use a `Get` binding that we backfill.
        for column in right_type.column_types.iter_mut() {
            column.nullable = true;
        }
        right_type.keys.clear();
        let aug_id = LocalId::new(id_gen.allocate_id());
        let aug_right = MirRelationExpr::local_get(aug_id, right_type);

        bindings.push((id, get_right.clone(), right));
        bound_to.extend((0..ra).map(|_| 2 + index));
        arities.push(ra);

        // Cartesian join but equating the outer columns.
        let mut product = MirRelationExpr::join(
            vec![body, aug_right.clone()],
            (0..oa).map(|i| vec![(0, i), (1, i)]).collect(),
        )
        // ... remove the second copy of the outer columns.
        .project(
            (0..(oa + ba))
                .chain((oa + ba + oa)..(oa + ba + oa + ra + 1)) // include new column
                .collect(),
        );

        // Decorrelate and lower the `on` clause.
        let on = on
            .clone()
            .applied_to(id_gen, col_map, cte_map, config, &mut product, &None)?;

        // if `on` added any new columns, .. no clue what to do.
        // Return with failure, to avoid any confusion.
        if product.typ().column_types.len() > oa + ba + ra + 1 {
            tracing::debug!(case = 3, index, "attempt_left_join_magic");
            return Ok(None);
        }

        // If `on` equates columns in `right` with columns in some input,
        // not just "any columns in `body`" but some single specific input,
        // then we can fish out values from that input. If it equates values
        // across multiple inputs, we would need to fish out valid tuples and
        // no idea how we would get those w/o doing a join or a cartesian product.
        let equations = if let Some(list) = decompose_equations(&on) {
            list
        } else {
            tracing::debug!(case = 4, index, "attempt_left_join_magic");
            return Ok(None);
        };

        // We now need to see if all left columns exist in some input relation,
        // and that all right columns are actually in the right relation. Idk.
        // Left columns less than `oa` do not bind to an input, as they are for
        // columns present in all inputs.
        let mut bound_input = None;
        for (left, right) in equations.iter().cloned() {
            // If the right reference is not actually to `right`, bail out.
            if right < oa + ba {
                tracing::debug!(case = 5, index, "attempt_left_join_magic");
                return Ok(None);
            }
            // Only columns not from the outer scope introduce bindings.
            if left >= oa {
                if let Some(bound) = bound_input {
                    if bound_to[left] != bound {
                        tracing::debug!(case = 6, index, "attempt_left_join_magic");
                        return Ok(None);
                    }
                }
                bound_input = Some(bound_to[left]);
            }
        }

        if let Some(bound) = bound_input {
            // This is great news; we have an input `bound` that we can augment,
            // and just need to pull those values in to the definition of `right`.

            // Add up prior arities, to learn what to subtract from left references.
            // Don't subtract anything from left references less than `oa`!
            let offset: usize = arities[0..bound].iter().sum();

            // We now want to grab the `Get` for both left and right relations,
            // which we will project to get distinct values, then difference and
            // threshold to find those present in left but missing in right.
            let get_left = &bindings[bound - 1].1;
            // Set up a type for the all-nulls row we need to introduce.
            let mut left_typ = get_left.typ();
            for col in left_typ.column_types.iter_mut() {
                col.nullable = true;
            }
            left_typ.keys.clear();
            // `get_right` is already bound.
            let left_vals = get_left
                .clone()
                .union(MirRelationExpr::Constant {
                    rows: Ok(vec![(
                        mz_repr::Row::pack(
                            std::iter::repeat(mz_repr::Datum::Null).take(get_left.arity()),
                        ),
                        1,
                    )]),
                    typ: left_typ,
                })
                .project(
                    equations
                        .iter()
                        .map(|(l, _)| if l < &oa { *l } else { l - offset })
                        .collect::<Vec<_>>(),
                )
                .distinct();

            // We skip the distinct because the eventual `threshold` protects us.
            let right_vals = get_right.clone().project(
                equations
                    .iter()
                    .map(|(_, r)| r - oa - ba)
                    .collect::<Vec<_>>(),
            );

            // Now we need to permute them into place, and leave `Datum::Null` values behind.
            // We should also add an all `Null` row, so that any null values match with nulls.

            // By default, we'll place post-pended nulls in each location.
            // We will overwrite this with instructions to find augmenting values.
            let mut projection = (equations.len()..equations.len() + rt.len()).collect::<Vec<_>>();
            for (index, (_, right)) in equations.iter().enumerate() {
                projection[*right - oa - ba] = index;
            }

            let additions = right_vals
                .negate()
                .union(left_vals)
                .threshold()
                .map(
                    rt.iter()
                        .map(|t| MirScalarExpr::literal_null(t.scalar_type.clone()))
                        .collect::<Vec<_>>(),
                )
                .project(projection);

            // This is where we should add a boolean column to indicate that the row is augmented,
            // so that after the join is done we can overwrite all values for `right` with null values.
            // This is a quirk of how outer joins work: the matched columns are left as null.

            // Record the binding we'll need to make for `aug_id`.
            augmented.push((
                aug_id,
                aug_right,
                bindings[index + 1].1.clone().union(additions),
            ));

            // Update `body` to reflect the product, filtered by `on`.
            body = product.filter(recompose_equations(equations));

            // Update `body` so that each new column consults its final column, and if null sets all columns to null.
            let scalars = (oa + ba..oa + ba + ra)
                .map(|col| MirScalarExpr::If {
                    cond: Box::new(MirScalarExpr::Column(oa + ba + ra).call_is_null()),
                    then: Box::new(MirScalarExpr::literal_null(
                        rt[col - (oa + ba)].scalar_type.clone(),
                    )),
                    els: Box::new(MirScalarExpr::Column(col)),
                })
                .collect::<Vec<_>>();

            body = body.map(scalars).project(
                (0..oa + ba)
                    .chain(oa + ba + ra + 1..oa + ba + ra + 1 + ra)
                    .collect(),
            );

            ba += ra;

            assert_eq!(oa + ba, body.arity());
        } else {
            tracing::debug!(case = 7, index, "attempt_left_join_magic");
            return Ok(None);
        }
    }

    // If we've gotten this for, we've populated `bindings` with various let bindings
    // we must now create, all wrapped around `body`.
    while let Some((id, _get, value)) = augmented.pop() {
        body = MirRelationExpr::Let {
            id,
            value: Box::new(value),
            body: Box::new(body),
        };
    }
    while let Some((id, _get, value)) = bindings.pop() {
        body = MirRelationExpr::Let {
            id,
            value: Box::new(value),
            body: Box::new(body),
        };
    }

    tracing::debug!(case = 0, "attempt_left_join_magic");
    Ok(Some(body))
}

use mz_expr::{BinaryFunc, VariadicFunc};

/// If `predicate` can be decomposed as any number of `col(x) = col(y)` expressions anded together, return them.
fn decompose_equations(predicate: &MirScalarExpr) -> Option<Vec<(usize, usize)>> {
    let mut equations = Vec::new();

    let mut todo = vec![predicate];
    while let Some(expr) = todo.pop() {
        match expr {
            MirScalarExpr::CallVariadic {
                func: VariadicFunc::And,
                exprs,
            } => {
                todo.extend(exprs.iter());
            }
            MirScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1,
                expr2,
            } => {
                if let (MirScalarExpr::Column(c1), MirScalarExpr::Column(c2)) = (&**expr1, &**expr2)
                {
                    if c1 < c2 {
                        equations.push((*c1, *c2));
                    } else {
                        equations.push((*c2, *c1));
                    }
                } else {
                    return None;
                }
            }
            e if e.is_literal_true() => (), // `USING(c1,...,cN)` translates to `true && c1 = c1 ... cN = cN`.
            _ => return None,
        }
    }
    Some(equations)
}

/// Turns column equation into idiomatic Rust equation, where nulls equate.
fn recompose_equations(pairs: Vec<(usize, usize)>) -> Vec<MirScalarExpr> {
    pairs
        .iter()
        .map(|(x, y)| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or,
            exprs: vec![
                MirScalarExpr::CallBinary {
                    func: BinaryFunc::Eq,
                    expr1: Box::new(MirScalarExpr::Column(*x)),
                    expr2: Box::new(MirScalarExpr::Column(*y)),
                },
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::And,
                    exprs: vec![
                        MirScalarExpr::Column(*x).call_is_null(),
                        MirScalarExpr::Column(*y).call_is_null(),
                    ],
                },
            ],
        })
        .collect()
}
