// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the PostgreSQL project. The original
// source code is subject to the terms of the PostgreSQL license, a copy of
// which can be found in the LICENSE file at the root of this repository.

//! A trait for things that can evaluate (`MirScalarExpr`, `LirScalarExpr`).

use mz_repr::{Datum, RowArena};

use crate::EvalError;

pub trait Eval {
    /// Evaluates, where `datums` are column references and `temp_storage` is used for allocation.
    ///
    /// Should not panic, but instead return an appropriate `EvalError`.
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError>;

    /// True iff evaluation could possibly error on non-error input `Datum`.
    fn could_error(&self) -> bool;
}
