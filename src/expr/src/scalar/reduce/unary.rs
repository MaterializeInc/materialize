// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Post-order rewrites for `CallUnary` nodes.

use mz_repr::{ReprColumnType, RowArena};

use crate::scalar::func::{self, UnaryFunc, VariadicFunc};
use crate::{Eval, MirScalarExpr};

pub(super) fn reduce_call_unary(
    e: &mut MirScalarExpr,
    column_types: &[ReprColumnType],
    temp_storage: &RowArena,
) {
    let MirScalarExpr::CallUnary { func, expr } = e else {
        unreachable!()
    };

    if expr.is_literal() && *func != UnaryFunc::Panic(func::Panic) {
        *e = MirScalarExpr::literal(e.eval(&[], temp_storage), e.typ(column_types).scalar_type);
        return;
    }

    // `RecordGet(i)(RecordCreate(args))` → `args[i]`.
    if let UnaryFunc::RecordGet(func::RecordGet(i)) = *func {
        if let MirScalarExpr::CallVariadic {
            func: VariadicFunc::RecordCreate(..),
            exprs,
        } = &mut **expr
        {
            *e = exprs.swap_remove(i);
        }
    }
}
