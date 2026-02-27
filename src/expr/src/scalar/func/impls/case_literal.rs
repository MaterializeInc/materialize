// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A lookup-based evaluation of `CASE expr WHEN lit1 THEN res1 ... ELSE els END`.
//!
//! [`CaseLiteral`] replaces chains of `If(Eq(expr, literal), result, If(...))`
//! with a single `BTreeMap` lookup, turning O(n) evaluation into O(log n).

use std::collections::BTreeMap;
use std::fmt;

use mz_lowertest::MzReflect;
use mz_repr::{Datum, Row, RowArena, SqlColumnType};
use serde::{Deserialize, Serialize};

use crate::scalar::func::LazyUnaryFunc;
use crate::{EvalError, MirScalarExpr};

/// Evaluates a CASE expression by looking up the input datum in a `BTreeMap`.
///
/// The input expression is evaluated once, packed into a temporary `Row`,
/// and looked up in `cases`. If found, the corresponding result expression
/// is evaluated; otherwise `els` is evaluated. NULL inputs go straight to `els`
/// (since SQL `NULL = x` is always NULL/falsy).
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct CaseLiteral {
    /// Map from literal values (as single-datum `Row`s) to result expressions.
    pub cases: BTreeMap<Row, MirScalarExpr>,
    /// The fallback expression, evaluated when no case matches or the input is NULL.
    pub els: Box<MirScalarExpr>,
    /// The output type of this CASE expression.
    pub return_type: SqlColumnType,
}

impl LazyUnaryFunc for CaseLiteral {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let input = a.eval(datums, temp_storage)?;
        // SQL NULL = x is always NULL/falsy, so go straight to `els`.
        if input.is_null() {
            return self.els.eval(datums, temp_storage);
        }
        let key = Row::pack_slice(&[input]);
        if let Some(result_expr) = self.cases.get(&key) {
            result_expr.eval(datums, temp_storage)
        } else {
            self.els.eval(datums, temp_storage)
        }
    }

    fn output_sql_type(&self, _input_type: SqlColumnType) -> SqlColumnType {
        self.return_type.clone()
    }

    fn propagates_nulls(&self) -> bool {
        // NULL input goes to `els`, not automatically to NULL output.
        false
    }

    fn introduces_nulls(&self) -> bool {
        // Branch results or `els` may be NULL.
        true
    }

    fn could_error(&self) -> bool {
        true
    }

    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }
}

impl fmt::Display for CaseLiteral {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "case_literal[{} cases]", self.cases.len())
    }
}
