// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Post-order rewrites for `CallVariadic` nodes.

use std::collections::BTreeSet;
use std::mem;

use mz_ore::collections::CollectionExt;
use mz_pgtz::timezone::TimezoneSpec;
use mz_repr::{Datum, ReprColumnType, ReprScalarType, RowArena, SqlScalarType};

use crate::scalar::func::variadic::{Coalesce, ListCreate, ListIndex};
use crate::scalar::func::{
    self, BinaryFunc, UnaryFunc, VariadicFunc, parse_timezone, regexp_replace_parse_flags,
};
use crate::{Eval, MirScalarExpr};

pub(super) fn reduce_call_variadic(
    e: &mut MirScalarExpr,
    column_types: &[ReprColumnType],
    temp_storage: &RowArena,
) {
    // Flatten chains of associative variadic calls before any per-`func`
    // dispatch. `undistribute_and_or` below relies on this having run.
    e.flatten_associative();

    let MirScalarExpr::CallVariadic { func, exprs } = e else {
        unreachable!("`flatten_associative` shouldn't change node type");
    };

    // Coalesce has its own simplification routine that handles null/error
    // propagation internally — bail out to it.
    if *func == Coalesce.into() {
        simplify_coalesce(e, column_types);
        return;
    }

    // CaseLiteral is lazy: only the selected arm is evaluated, so a literal
    // NULL or error in a *result branch* must not propagate to the whole call.
    // Constant-fold only when every argument is a literal (eval is correct then),
    // otherwise normalize to canonical arm order.
    if let VariadicFunc::CaseLiteral(cl) = func {
        if exprs.iter().all(|x| x.is_literal()) {
            *e = MirScalarExpr::literal(e.eval(&[], temp_storage), e.typ(column_types).scalar_type);
        } else {
            cl.canonicalize(exprs);
        }
        return;
    }

    // Generic folds: constant-fold, null-propagate, error-propagate.
    if exprs.iter().all(|x| x.is_literal()) {
        *e = MirScalarExpr::literal(e.eval(&[], temp_storage), e.typ(column_types).scalar_type);
        return;
    }
    if func.propagates_nulls() && exprs.iter().any(|x| x.is_literal_null()) {
        *e = MirScalarExpr::literal_null(e.typ(column_types).scalar_type);
        return;
    }
    if let Some(err) = exprs.iter().find_map(|x| x.as_literal_err()) {
        *e = MirScalarExpr::literal(Err(err.clone()), e.typ(column_types).scalar_type);
        return;
    }

    // Per-function dispatch. Arms are mutually exclusive on discriminant; the
    // bodies only fire when their literal-argument guards hold.
    match func {
        VariadicFunc::Greatest(_) | VariadicFunc::Least(_) => {
            reduce_greatest_least(e, column_types);
        }
        VariadicFunc::Substr(_)
            if exprs.len() == 2 && matches!(exprs[1].as_literal(), Some(Ok(Datum::Int32(1)))) =>
        {
            // `substr(s, 1)` — the two-argument form — keeps the entire
            // string, and its evaluation at a start of one is infallible.
            *e = exprs.swap_remove(0);
        }
        VariadicFunc::RegexpMatch(_)
            if exprs[1].is_literal() && exprs.get(2).map_or(true, |e| e.is_literal()) =>
        {
            let needle = exprs[1].as_literal_str().unwrap();
            let flags = if exprs.len() == 3 {
                exprs[2].as_literal_str().unwrap()
            } else {
                ""
            };
            *e = match func::build_regex(needle, flags) {
                Ok(regex) => mem::take(exprs)
                    .into_first()
                    .call_unary(UnaryFunc::RegexpMatch(func::RegexpMatch(regex))),
                Err(err) => MirScalarExpr::literal(Err(err), e.typ(column_types).scalar_type),
            };
        }
        VariadicFunc::RegexpReplace(_)
            if exprs[1].is_literal() && exprs.get(3).map_or(true, |e| e.is_literal()) =>
        {
            let pattern = exprs[1].as_literal_str().unwrap();
            let flags = exprs
                .get(3)
                .map_or("", |expr| expr.as_literal_str().unwrap());
            let (limit, flags) = regexp_replace_parse_flags(flags);

            // The behavior of `regexp_replace` is that if the data is `NULL`, the
            // function returns `NULL`, independently of whether the pattern or
            // flags are correct. We need to check for this case and introduce an
            // if-then-else on the error path to only surface the error if both
            // the source and replacement inputs are non-NULL.
            *e = match func::build_regex(pattern, &flags) {
                Ok(regex) => {
                    let mut exprs = mem::take(exprs);
                    let replacement = exprs.swap_remove(2);
                    let source = exprs.swap_remove(0);
                    source.call_binary(
                        replacement,
                        BinaryFunc::from(func::RegexpReplace { regex, limit }),
                    )
                }
                Err(err) => {
                    let mut exprs = mem::take(exprs);
                    let replacement = exprs.swap_remove(2);
                    let source = exprs.swap_remove(0);
                    let scalar_type = e.typ(column_types).scalar_type;
                    // We need to return `NULL` on `NULL` input, and error otherwise.
                    source
                        .call_is_null()
                        .or(replacement.call_is_null())
                        .if_then_else(
                            MirScalarExpr::literal_null(scalar_type.clone()),
                            MirScalarExpr::literal(Err(err), scalar_type),
                        )
                }
            };
        }
        VariadicFunc::RegexpSplitToArray(_)
            if exprs[1].is_literal() && exprs.get(2).map_or(true, |e| e.is_literal()) =>
        {
            let needle = exprs[1].as_literal_str().unwrap();
            let flags = if exprs.len() == 3 {
                exprs[2].as_literal_str().unwrap()
            } else {
                ""
            };
            *e = match func::build_regex(needle, flags) {
                Ok(regex) => {
                    mem::take(exprs)
                        .into_first()
                        .call_unary(UnaryFunc::RegexpSplitToArray(func::RegexpSplitToArray(
                            regex,
                        )))
                }
                Err(err) => MirScalarExpr::literal(Err(err), e.typ(column_types).scalar_type),
            };
        }
        VariadicFunc::ListIndex(_) if is_list_create_call(&exprs[0]) => {
            // We are looking for ListIndex(ListCreate, literal), and eliminate
            // both the ListIndex and the ListCreate. E.g.: `LIST[f1,f2][2]` --> `f2`
            let ind_exprs = exprs.split_off(1);
            let top_list_create = exprs.swap_remove(0);
            *e = reduce_list_create_list_index_literal(top_list_create, ind_exprs);
        }
        VariadicFunc::And(_) | VariadicFunc::Or(_) => {
            // Note: It's important that we have called `flatten_associative` above.
            e.undistribute_and_or();
            e.reduce_and_canonicalize_and_or();
        }
        VariadicFunc::TimezoneTimeVariadic(_)
            if exprs[0].is_literal() && exprs[2].is_literal_ok() =>
        {
            let tz = exprs[0].as_literal_str().unwrap();
            *e = match parse_timezone(tz, TimezoneSpec::Posix) {
                Ok(tz) => MirScalarExpr::CallUnary {
                    func: UnaryFunc::TimezoneTime(func::TimezoneTime {
                        tz,
                        wall_time: exprs[2]
                            .as_literal()
                            .unwrap()
                            .unwrap()
                            .unwrap_timestamptz()
                            .naive_utc(),
                    }),
                    expr: Box::new(exprs[1].take()),
                },
                Err(err) => MirScalarExpr::literal(Err(err), e.typ(column_types).scalar_type),
            };
        }
        _ => {}
    }
}

/// Simplifies a `Greatest`/`Least` call:
/// 1. Deduplicate structurally equal operands, keeping first occurrences.
///    Scalar evaluation is deterministic (the `And`/`Or` and `Coalesce`
///    reducers already rely on this when they deduplicate), so duplicates of
///    an expression contribute the same value — over which `greatest`/`least`
///    are idempotent — or the same error; keeping the first occurrence leaves
///    unchanged which error surfaces.
/// 2. Drop literal null operands: both functions ignore null inputs (they
///    return the max/min of the non-null inputs, and null only when every
///    input is null).
/// 3. A call left with a single operand is the identity on it — the call
///    evaluates the operand once and returns it, null or not — and a call
///    left with none is null.
fn reduce_greatest_least(e: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    let typ = e.typ(column_types).scalar_type;
    let MirScalarExpr::CallVariadic { exprs, .. } = e else {
        unreachable!()
    };
    let mut seen = BTreeSet::new();
    exprs.retain(|x| seen.insert(x.clone()));
    exprs.retain(|x| !x.is_literal_null());
    match exprs.len() {
        0 => *e = MirScalarExpr::literal_null(typ),
        1 => *e = exprs.swap_remove(0),
        _ => {}
    }
}

/// Simplifies a `Coalesce`:
/// 1. If all arguments are null, the result is null.
/// 2. Drop null arguments (none of them can be the result).
/// 3. Truncate after the first argument known to be non-null (a literal or a
///    non-nullable column).
/// 4. Deduplicate arguments (e.g. `coalesce(#0, #0) → coalesce(#0)`).
/// 5. Unwrap a single-argument `coalesce`.
fn simplify_coalesce(e: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    let MirScalarExpr::CallVariadic { exprs, .. } = e else {
        unreachable!()
    };

    // If all inputs are null, output is null. This check must
    // be done before `exprs.retain...` because `e.typ` requires
    // > 0 `exprs` remain.
    if exprs.iter().all(|x| x.is_literal_null()) {
        *e = MirScalarExpr::literal_null(e.typ(column_types).scalar_type);
        return;
    }

    // Remove any null values if not all values are null.
    exprs.retain(|x| !x.is_literal_null());

    // Find the first argument that is a literal or non-nullable
    // column. All arguments after it get ignored, so throw them
    // away. This intentionally throws away errors that can
    // never happen.
    if let Some(i) = exprs
        .iter()
        .position(|x| x.is_literal() || !x.typ(column_types).nullable)
    {
        exprs.truncate(i + 1);
    }

    // Deduplicate arguments in cases like `coalesce(#0, #0)`.
    let mut seen = BTreeSet::new();
    exprs.retain(|x| seen.insert(x.clone()));

    if exprs.len() == 1 {
        // Only one argument, so the coalesce is a no-op.
        *e = exprs[0].take();
    }
}

fn is_list_create_call(expr: &MirScalarExpr) -> bool {
    matches!(
        expr,
        MirScalarExpr::CallVariadic {
            func: VariadicFunc::ListCreate(..),
            ..
        }
    )
}

fn list_create_type(list_create: &MirScalarExpr) -> ReprScalarType {
    if let MirScalarExpr::CallVariadic {
        func: VariadicFunc::ListCreate(ListCreate { elem_type: typ }),
        ..
    } = list_create
    {
        ReprScalarType::from(typ)
    } else {
        unreachable!()
    }
}

/// Partial-evaluates a list indexing with a literal directly after a list creation.
///
/// Multi-dimensional lists are handled by a single call to this function, with multiple
/// elements in index_exprs (of which not all need to be literals), and nested ListCreates
/// in list_create_to_reduce.
///
/// # Examples
///
/// `LIST[f1,f2][2]` --> `f2`.
///
/// A multi-dimensional list, with only some of the indexes being literals:
/// `LIST[[[f1, f2], [f3, f4]], [[f5, f6], [f7, f8]]] [2][n][2]` --> `LIST[f6, f8] [n]`
///
/// See more examples in list.slt.
fn reduce_list_create_list_index_literal(
    mut list_create_to_reduce: MirScalarExpr,
    mut index_exprs: Vec<MirScalarExpr>,
) -> MirScalarExpr {
    // We iterate over the index_exprs and remove literals, but keep non-literals.
    // When we encounter a non-literal, we need to dig into the nested ListCreates:
    // `list_create_mut_refs` will contain all the ListCreates of the current level. If an
    // element of `list_create_mut_refs` is not actually a ListCreate, then we break out of
    // the loop. When we remove a literal, we need to partial-evaluate all ListCreates
    // that are at the current level (except those that disappeared due to
    // literals at earlier levels), index into them with the literal, and change each
    // element in `list_create_mut_refs` to the result.
    // We also record mut refs to all the earlier `element_type` references that we have
    // seen in ListCreate calls, because when we process a literal index, we need to remove
    // one layer of list type from all these earlier ListCreate `element_type`s.
    let mut list_create_mut_refs = vec![&mut list_create_to_reduce];
    let mut earlier_list_create_types: Vec<&mut SqlScalarType> = vec![];
    let mut i = 0;
    while i < index_exprs.len()
        && list_create_mut_refs
            .iter()
            .all(|lc| is_list_create_call(lc))
    {
        if index_exprs[i].is_literal_ok() {
            // We can remove this index.
            let removed_index = index_exprs.remove(i);
            let index_i64 = match removed_index.as_literal().unwrap().unwrap() {
                Datum::Int64(sql_index_i64) => sql_index_i64 - 1,
                _ => unreachable!(), // always an Int64, see plan_index_list
            };
            // For each list_create referenced by list_create_mut_refs, substitute it by its
            // `index`th argument (or null).
            for list_create in &mut list_create_mut_refs {
                let list_create_args = match list_create {
                    MirScalarExpr::CallVariadic {
                        func: VariadicFunc::ListCreate(ListCreate { elem_type: _ }),
                        exprs,
                    } => exprs,
                    _ => unreachable!(), // func cannot be anything else than a ListCreate
                };
                // ListIndex gives null on an out-of-bounds index
                if index_i64 >= 0 && index_i64 < list_create_args.len().try_into().unwrap() {
                    let index: usize = index_i64.try_into().unwrap();
                    **list_create = list_create_args.swap_remove(index);
                } else {
                    let typ = list_create_type(list_create);
                    **list_create = MirScalarExpr::literal_null(typ);
                }
            }
            // Peel one layer off of each of the earlier element types.
            for t in earlier_list_create_types.iter_mut() {
                if let SqlScalarType::List {
                    element_type,
                    custom_id: _,
                } = t
                {
                    **t = *element_type.clone();
                    // These are not the same types anymore, so remove custom_ids all the
                    // way down.
                    let mut u = &mut **t;
                    while let SqlScalarType::List {
                        element_type,
                        custom_id,
                    } = u
                    {
                        *custom_id = None;
                        u = &mut **element_type;
                    }
                } else {
                    unreachable!("already matched below");
                }
            }
        } else {
            // We can't remove this index, so we can't reduce any of the ListCreates at this
            // level. So we change list_create_mut_refs to refer to all the arguments of all
            // the ListCreates currently referenced by list_create_mut_refs.
            list_create_mut_refs = list_create_mut_refs
                .into_iter()
                .flat_map(|list_create| match list_create {
                    MirScalarExpr::CallVariadic {
                        func: VariadicFunc::ListCreate(ListCreate { elem_type }),
                        exprs: list_create_args,
                    } => {
                        earlier_list_create_types.push(elem_type);
                        list_create_args
                    }
                    // func cannot be anything else than a ListCreate
                    _ => unreachable!(),
                })
                .collect();
            i += 1;
        }
    }
    // If all list indexes have been evaluated, return the reduced expression.
    // Otherwise, rebuild the ListIndex call with the remaining ListCreates and indexes.
    if index_exprs.is_empty() {
        assert_eq!(list_create_mut_refs.len(), 1);
        list_create_to_reduce
    } else {
        MirScalarExpr::call_variadic(
            ListIndex,
            std::iter::once(list_create_to_reduce)
                .chain(index_exprs)
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, ReprScalarType};

    use crate::MirScalarExpr;
    use crate::scalar::func::variadic::{Greatest, Least, Substr};

    #[mz_ore::test]
    fn greatest_least_null_operand_drop() {
        let types = [
            ReprScalarType::Int32.nullable(true),
            ReprScalarType::Int32.nullable(true),
        ];
        let null = || MirScalarExpr::literal_null(ReprScalarType::Int32);
        let col = MirScalarExpr::column;

        // Null operands drop; a single survivor is the result.
        let mut e = MirScalarExpr::call_variadic(Greatest, vec![col(0), null()]);
        e.reduce(&types);
        assert_eq!(e, col(0));

        let mut e = MirScalarExpr::call_variadic(Least, vec![col(0), null(), col(1)]);
        e.reduce(&types);
        assert_eq!(e, MirScalarExpr::call_variadic(Least, vec![col(0), col(1)]));

        // Structurally equal operands deduplicate (scalar evaluation is
        // deterministic and greatest/least are idempotent), keeping first
        // occurrences.
        let mut e = MirScalarExpr::call_variadic(Greatest, vec![col(0), col(0)]);
        e.reduce(&types);
        assert_eq!(e, col(0));

        let mut e = MirScalarExpr::call_variadic(Least, vec![col(1), col(0), col(1)]);
        e.reduce(&types);
        assert_eq!(e, MirScalarExpr::call_variadic(Least, vec![col(1), col(0)]));

        // Dedup and null-drop compose down to the bare operand.
        let mut e = MirScalarExpr::call_variadic(Greatest, vec![col(0), null(), col(0)]);
        e.reduce(&types);
        assert_eq!(e, col(0));

        // All-null evaluates to null.
        let mut e = MirScalarExpr::call_variadic(Greatest, vec![null(), null()]);
        e.reduce(&types);
        assert!(e.is_literal_null());
    }

    #[mz_ore::test]
    fn substr_from_one() {
        let types = [ReprScalarType::String.nullable(true)];
        let col = || MirScalarExpr::column(0);
        let lit = |v| MirScalarExpr::literal_ok(Datum::Int32(v), ReprScalarType::Int32);

        // The two-argument form starting at one is the identity.
        let mut e = MirScalarExpr::call_variadic(Substr, vec![col(), lit(1)]);
        e.reduce(&types);
        assert_eq!(e, col());

        // The three-argument form truncates and must stay.
        let mut e = MirScalarExpr::call_variadic(Substr, vec![col(), lit(1), lit(5)]);
        e.reduce(&types);
        assert_ne!(e, col());
    }
}

#[cfg(test)]
mod case_literal_tests {
    use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row, SqlColumnType, SqlScalarType};

    use crate::scalar::func::{CaseLiteral, CaseLiteralEntry};
    use crate::{EvalError, MirScalarExpr, VariadicFunc};

    /// A CaseLiteral whose only matching arm is an error must NOT be folded to
    /// that error: the error is reachable only when input == 1.
    #[mz_ore::test]
    fn case_literal_error_branch_not_hoisted() {
        // input = column 0 (i64, nullable so it is not constant-folded away)
        let input = MirScalarExpr::column(0);
        let err_branch =
            MirScalarExpr::literal(Err(EvalError::DivisionByZero), ReprScalarType::Int64);
        let fallback = MirScalarExpr::literal_ok(Datum::Int64(0), ReprScalarType::Int64);
        let cl = CaseLiteral {
            lookup: vec![CaseLiteralEntry {
                literal: Row::pack_slice(&[Datum::Int64(1)]),
                expr_index: 1,
            }],
            return_type: SqlColumnType {
                scalar_type: SqlScalarType::Int64,
                nullable: true,
            },
        };
        let mut expr = MirScalarExpr::CallVariadic {
            func: VariadicFunc::CaseLiteral(cl),
            exprs: vec![input, err_branch, fallback],
        };
        let col_types = vec![ReprColumnType {
            scalar_type: ReprScalarType::Int64,
            nullable: true,
        }];
        crate::scalar::reduce::reduce(
            &mut expr,
            &col_types,
            &mz_repr::optimize::OptimizerFeatures::default(),
        );
        // Must remain a CaseLiteral, not collapse to the error literal.
        assert!(
            matches!(
                expr,
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::CaseLiteral(_),
                    ..
                }
            ),
            "CaseLiteral was incorrectly folded: {expr:?}"
        );
    }

    #[mz_ore::test]
    fn reduce_canonicalizes_case_literal_arm_order() {
        use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row, SqlColumnType, SqlScalarType};
        let input = MirScalarExpr::column(0);
        // exprs: [input, result@1=100, result@2=200, fallback=0]
        // lookup sorted by literal: (1 -> idx 2), (2 -> idx 1)  -- out of slot order
        let cl = CaseLiteral {
            lookup: vec![
                CaseLiteralEntry {
                    literal: Row::pack_slice(&[Datum::Int64(1)]),
                    expr_index: 2,
                },
                CaseLiteralEntry {
                    literal: Row::pack_slice(&[Datum::Int64(2)]),
                    expr_index: 1,
                },
            ],
            return_type: SqlColumnType {
                scalar_type: SqlScalarType::Int64,
                nullable: true,
            },
        };
        let mut expr = MirScalarExpr::CallVariadic {
            func: VariadicFunc::CaseLiteral(cl),
            exprs: vec![
                input,
                MirScalarExpr::literal_ok(Datum::Int64(100), ReprScalarType::Int64),
                MirScalarExpr::literal_ok(Datum::Int64(200), ReprScalarType::Int64),
                MirScalarExpr::literal_ok(Datum::Int64(0), ReprScalarType::Int64),
            ],
        };
        let col_types = vec![ReprColumnType {
            scalar_type: ReprScalarType::Int64,
            nullable: true,
        }];
        crate::scalar::reduce::reduce(
            &mut expr,
            &col_types,
            &mz_repr::optimize::OptimizerFeatures::default(),
        );
        let MirScalarExpr::CallVariadic {
            func: VariadicFunc::CaseLiteral(cl),
            exprs,
        } = &expr
        else {
            panic!("expected CaseLiteral, got {expr:?}");
        };
        assert_eq!(cl.lookup[0].expr_index, 1);
        assert_eq!(cl.lookup[1].expr_index, 2);
        // exprs[1] is now the result for literal 1 (=200).
        assert_eq!(
            exprs[1],
            MirScalarExpr::literal_ok(Datum::Int64(200), ReprScalarType::Int64)
        );
    }
}
