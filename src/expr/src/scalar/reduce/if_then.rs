// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Post-order rewrites for `If` nodes.

use mz_repr::{Datum, ReprColumnType, ReprScalarType};

use crate::MirScalarExpr;

pub(super) fn reduce_if(e: &mut MirScalarExpr, column_types: &[ReprColumnType]) {
    let MirScalarExpr::If { cond, then, els } = e else {
        unreachable!()
    };

    if let Some(literal) = cond.as_literal() {
        match literal {
            Ok(Datum::True) => *e = then.take(),
            Ok(Datum::False) | Ok(Datum::Null) => *e = els.take(),
            Err(err) => {
                *e = MirScalarExpr::Literal(
                    Err(err.clone()),
                    then.typ(column_types)
                        .union(&els.typ(column_types))
                        .unwrap(),
                );
            }
            _ => unreachable!(),
        }
        return;
    }
    if then == els {
        *e = then.take();
        return;
    }
    if then.is_literal_ok()
        && els.is_literal_ok()
        && then.typ(column_types).scalar_type == ReprScalarType::Bool
        && els.typ(column_types).scalar_type == ReprScalarType::Bool
    {
        match (then.as_literal(), els.as_literal()) {
            // Note: NULLs from the condition should not be propagated to the result
            // of the expression.
            (Some(Ok(Datum::True)), _) => {
                // Rewritten as ((<cond> IS NOT NULL) AND (<cond>)) OR (<els>)
                // NULL <cond> results in: (FALSE AND NULL) OR (<els>) => (<els>)
                *e = cond
                    .clone()
                    .call_is_null()
                    .not()
                    .and(cond.take())
                    .or(els.take());
            }
            (Some(Ok(Datum::False)), _) => {
                // Rewritten as ((NOT <cond>) OR (<cond> IS NULL)) AND (<els>)
                // NULL <cond> results in: (NULL OR TRUE) AND (<els>) => TRUE AND (<els>) => (<els>)
                *e = cond
                    .clone()
                    .not()
                    .or(cond.take().call_is_null())
                    .and(els.take());
            }
            (_, Some(Ok(Datum::True))) => {
                // Rewritten as (NOT <cond>) OR (<cond> IS NULL) OR (<then>)
                // NULL <cond> results in: NULL OR TRUE OR (<then>) => TRUE
                *e = cond
                    .clone()
                    .not()
                    .or(cond.take().call_is_null())
                    .or(then.take());
            }
            (_, Some(Ok(Datum::False))) => {
                // Rewritten as (<cond> IS NOT NULL) AND (<cond>) AND (<then>)
                // NULL <cond> results in: FALSE AND NULL AND (<then>) => FALSE
                *e = cond
                    .clone()
                    .call_is_null()
                    .not()
                    .and(cond.take())
                    .and(then.take());
            }
            _ => {}
        }
        return;
    }

    // Equivalent expression structure would allow us to push the `If` into the expression.
    // For example, `IF <cond> THEN x = y ELSE x = z` becomes `x = IF <cond> THEN y ELSE z`.
    //
    // We have to also make sure that the expressions that will end up in
    // the two `If` branches have unionable types. Otherwise, the `If` could
    // not be typed by `typ`. An example where this could cause an issue is
    // when pulling out `cast_jsonbable_to_jsonb`, which accepts a wide
    // range of input types. (In theory, we could still do the optimization
    // in this case by inserting appropriate casts, but this corner case is
    // not worth the complication for now.)
    // See https://github.com/MaterializeInc/database-issues/issues/9182
    match (&mut **then, &mut **els) {
        (
            MirScalarExpr::CallUnary { func: f1, expr: e1 },
            MirScalarExpr::CallUnary { func: f2, expr: e2 },
        ) if f1 == f2 && e1.typ(column_types) == e2.typ(column_types) => {
            *e = cond
                .take()
                .if_then_else(e1.take(), e2.take())
                .call_unary(f1.clone());
        }
        (
            MirScalarExpr::CallBinary {
                func: f1,
                expr1: e1a,
                expr2: e2a,
            },
            MirScalarExpr::CallBinary {
                func: f2,
                expr1: e1b,
                expr2: e2b,
            },
        ) if f1 == f2 && e1a == e1b && e2a.typ(column_types) == e2b.typ(column_types) => {
            *e = e1a
                .take()
                .call_binary(cond.take().if_then_else(e2a.take(), e2b.take()), f1.clone());
        }
        (
            MirScalarExpr::CallBinary {
                func: f1,
                expr1: e1a,
                expr2: e2a,
            },
            MirScalarExpr::CallBinary {
                func: f2,
                expr1: e1b,
                expr2: e2b,
            },
        ) if f1 == f2 && e2a == e2b && e1a.typ(column_types) == e1b.typ(column_types) => {
            *e = cond
                .take()
                .if_then_else(e1a.take(), e1b.take())
                .call_binary(e2a.take(), f1.clone());
        }
        _ => {}
    }
}
