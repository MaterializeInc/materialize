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
        // Rewrite `IF cond THEN <bool-lit> ELSE <bool-lit>` using AND/OR/IS NULL
        // so a NULL `cond` is not propagated to the result.
        match (then.as_literal(), els.as_literal()) {
            (Some(Ok(Datum::True)), _) => {
                // ((cond IS NOT NULL) AND cond) OR els.
                *e = cond
                    .clone()
                    .call_is_null()
                    .not()
                    .and(cond.take())
                    .or(els.take());
            }
            (Some(Ok(Datum::False)), _) => {
                // ((NOT cond) OR (cond IS NULL)) AND els.
                *e = cond
                    .clone()
                    .not()
                    .or(cond.take().call_is_null())
                    .and(els.take());
            }
            (_, Some(Ok(Datum::True))) => {
                // (NOT cond) OR (cond IS NULL) OR then.
                *e = cond
                    .clone()
                    .not()
                    .or(cond.take().call_is_null())
                    .or(then.take());
            }
            (_, Some(Ok(Datum::False))) => {
                // (cond IS NOT NULL) AND cond AND then.
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

    // Lift a common operator out of the two `If` branches. For example,
    // `IF cond THEN x = y ELSE x = z` becomes `x = IF cond THEN y ELSE z`.
    //
    // We must ensure that the two `If` branches have unionable types,
    // otherwise the lifted `If` could not be typed. See
    // https://github.com/MaterializeInc/database-issues/issues/9182.
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
