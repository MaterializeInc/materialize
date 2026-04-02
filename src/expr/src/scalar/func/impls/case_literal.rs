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
//! with a sorted `Vec` + binary-search lookup, turning O(n) evaluation into O(log n).
//!
//! Represented as a `CallVariadic { func: CaseLiteral { lookup, return_type }, exprs }`
//! where:
//! * `exprs[0]` = input expression (the `x` in `CASE x WHEN ...`)
//! * `exprs[1..n]` = case result expressions
//! * `exprs[last]` = `els` (fallback)
//! * `lookup: Vec<CaseLiteralEntry>` maps literal values to indices in `exprs` (sorted by `Row`)

use std::fmt;

use mz_lowertest::MzReflect;
use mz_repr::{Datum, Row, RowArena, SqlColumnType};
use serde::{Deserialize, Serialize};

use crate::scalar::func::variadic::LazyVariadicFunc;
use crate::{EvalError, MirScalarExpr};

/// A single entry in a [`CaseLiteral`] lookup table: a literal `Row` value
/// paired with the index of the corresponding result expression in `exprs`.
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub struct CaseLiteralEntry {
    /// The literal value (as a single-datum `Row`).
    #[mzreflect(ignore)]
    pub literal: Row,
    /// Index into the `exprs` vector of the corresponding result expression.
    pub expr_index: usize,
}

/// Evaluates a CASE expression by looking up the input datum in a sorted `Vec`.
///
/// The input expression (`exprs[0]`) is evaluated once, packed into a temporary
/// `Row`, and looked up in `lookup` via binary search. If found, the corresponding
/// result expression (`exprs[idx]`) is evaluated; otherwise the fallback
/// (`exprs.last()`) is evaluated.
/// NULL inputs go straight to the fallback (since SQL `NULL = x` is always NULL/falsy).
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
    /// Sorted vec of literal-to-index entries for binary-search lookup.
    pub lookup: Vec<CaseLiteralEntry>,
    /// The output type of this CASE expression.
    pub return_type: SqlColumnType,
}

impl LazyVariadicFunc for CaseLiteral {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        let input = exprs[0].eval(datums, temp_storage)?;
        // SQL NULL = x is always NULL/falsy, so go straight to the fallback.
        if input.is_null() {
            return exprs.last().unwrap().eval(datums, temp_storage);
        }
        let key = Row::pack_slice(&[input]);
        if let Ok(pos) = self
            .lookup
            .binary_search_by(|entry| entry.literal.cmp(&key))
        {
            exprs[self.lookup[pos].expr_index].eval(datums, temp_storage)
        } else {
            exprs.last().unwrap().eval(datums, temp_storage)
        }
    }

    fn output_type(&self, _input_types: &[SqlColumnType]) -> SqlColumnType {
        self.return_type.clone()
    }

    fn propagates_nulls(&self) -> bool {
        // NULL input goes to the fallback, not automatically to NULL output.
        false
    }

    fn introduces_nulls(&self) -> bool {
        // Branch results or the fallback may be NULL.
        true
    }

    fn could_error(&self) -> bool {
        // The function itself does not error; errors in sub-expressions are
        // checked separately by MirScalarExpr::could_error.
        false
    }

    fn is_monotone(&self) -> bool {
        false
    }

    fn is_associative(&self) -> bool {
        false
    }
}

// Note: this Display impl is unused at runtime because CaseLiteral has
// custom printing in src/expr/src/explain/text.rs.
impl CaseLiteral {
    /// Look up a key in the sorted lookup vec. Returns the expr index if found.
    pub fn get(&self, key: &Row) -> Option<usize> {
        self.lookup
            .binary_search_by(|entry| entry.literal.cmp(key))
            .ok()
            .map(|pos| self.lookup[pos].expr_index)
    }

    /// Insert an entry, maintaining sorted order.
    /// If the literal already exists, overwrites the index and returns the old one.
    pub fn insert(&mut self, literal: Row, expr_index: usize) -> Option<usize> {
        match self
            .lookup
            .binary_search_by(|entry| entry.literal.cmp(&literal))
        {
            Ok(pos) => {
                let old = self.lookup[pos].expr_index;
                self.lookup[pos].expr_index = expr_index;
                Some(old)
            }
            Err(pos) => {
                self.lookup
                    .insert(pos, CaseLiteralEntry { literal, expr_index });
                None
            }
        }
    }
}

impl fmt::Display for CaseLiteral {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "case_literal[{} cases]", self.lookup.len())
    }
}
