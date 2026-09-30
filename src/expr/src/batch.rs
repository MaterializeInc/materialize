// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Evaluation of scalar expressions and MFPs over batches of rows.
//!
//! [`MirScalarExpr::eval_batch`] walks an expression once per batch rather
//! than once per row, handing each function whole columns. A function or
//! literal type without a batch form makes the whole expression report
//! `None`, and the caller evaluates it row by row instead.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use mz_repr::batch::{DatumBatch, Kind, TypedVec};
use mz_repr::{Row, RowArena};

use crate::linear::plan::{MfpPlan, SafeMfpPlan};
use crate::{Columns, EvalError, MirScalarExpr};

impl MirScalarExpr {
    /// Evaluates this expression over `columns`, each of `len` rows.
    ///
    /// Returns `None` when some function or literal in the expression has no
    /// batch form. A bare column reference borrows its column.
    pub fn eval_batch<'a>(
        &self,
        columns: &[&'a DatumBatch<EvalError>],
        len: usize,
        temp_storage: &'a RowArena,
    ) -> Option<Cow<'a, DatumBatch<EvalError>>> {
        match self {
            MirScalarExpr::Column(index, _name) => Some(Cow::Borrowed(columns[*index])),
            MirScalarExpr::Literal(Ok(row), typ) => {
                DatumBatch::repeat(row.unpack_first(), len, &typ.scalar_type).map(Cow::Owned)
            }
            MirScalarExpr::Literal(Err(err), typ) => {
                DatumBatch::repeat_error(err.clone(), len, &typ.scalar_type).map(Cow::Owned)
            }
            MirScalarExpr::CallUnary { func, expr } => {
                let input = expr.eval_batch(columns, len, temp_storage)?;
                func.eval_batch(&input).map(Cow::Owned)
            }
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                let input1 = expr1.eval_batch(columns, len, temp_storage)?;
                let input2 = expr2.eval_batch(columns, len, temp_storage)?;
                func.eval_batch(&[&*input1, &*input2], temp_storage)
                    .map(Cow::Owned)
            }
            // Unmaterializable functions must be transformed away before
            // evaluation. Variadic functions and conditionals have no batch
            // form yet.
            MirScalarExpr::CallUnmaterializable(_)
            | MirScalarExpr::CallVariadic { .. }
            | MirScalarExpr::If { .. } => None,
        }
    }
}

/// Marks the live rows with an error in `batch` as failed with that error.
fn absorb_errors(
    batch: &DatumBatch<EvalError>,
    live: &mut [bool],
    failed: &mut BTreeMap<usize, EvalError>,
) {
    for (row, err) in batch.iter_errors() {
        if live[row] {
            live[row] = false;
            failed.insert(row, err.clone());
        }
    }
}

impl SafeMfpPlan<MirScalarExpr> {
    /// Evaluates the plan over `len` rows, given as one batch per input
    /// column, calling `emit` with each row's index and its projected row or
    /// its error. Rows a predicate filters out are not emitted.
    ///
    /// Returns `None`, having emitted nothing, when some expression has no
    /// batch form. That depends only on the plan and the column types, so a
    /// caller can probe once with an empty batch.
    ///
    /// Row-at-a-time evaluation stops at a row's first failing predicate or
    /// error, in support order, so an expression it never reaches cannot fail
    /// that row. This evaluates every expression for every row, but charges
    /// an error to a row only if the row is still live at that point in the
    /// same order, which yields the same outcome.
    pub fn evaluate_batch(
        &self,
        inputs: &[DatumBatch<EvalError>],
        len: usize,
        mut emit: impl FnMut(usize, Result<&Row, &EvalError>),
    ) -> Option<()> {
        let temp_storage = RowArena::new();
        let mut live = vec![true; len];
        let mut failed = BTreeMap::new();
        for input in inputs {
            absorb_errors(input, &mut live, &mut failed);
        }

        let mut computed: Vec<DatumBatch<EvalError>> =
            Vec::with_capacity(self.mfp.expressions.len());
        let eval = |expr: &MirScalarExpr, computed: &[DatumBatch<EvalError>]| {
            let columns: Vec<&DatumBatch<EvalError>> =
                inputs.iter().chain(computed.iter()).collect();
            expr.eval_batch(&columns, len, &temp_storage)
                .map(Cow::into_owned)
        };

        let mut expression = 0;
        for (support, predicate) in self.mfp.predicates.iter() {
            while self.mfp.input_arity + expression < *support {
                let batch = eval(&self.mfp.expressions[expression], &computed)?;
                absorb_errors(&batch, &mut live, &mut failed);
                computed.push(batch);
                expression += 1;
            }
            let batch = eval(predicate, &computed)?;
            absorb_errors(&batch, &mut live, &mut failed);
            let TypedVec::Bool(values) = batch.values() else {
                return None;
            };
            let mut value = 0;
            for row in 0..len {
                match batch.kind(row) {
                    Kind::Value => {
                        if !values[value] {
                            live[row] = false;
                        }
                        value += 1;
                    }
                    Kind::Null => live[row] = false,
                    Kind::Error => {}
                }
            }
        }
        while expression < self.mfp.expressions.len() {
            let batch = eval(&self.mfp.expressions[expression], &computed)?;
            absorb_errors(&batch, &mut live, &mut failed);
            computed.push(batch);
            expression += 1;
        }

        let columns: Vec<&DatumBatch<EvalError>> = inputs.iter().chain(computed.iter()).collect();
        let mut projected: Vec<_> = self
            .mfp
            .projection
            .iter()
            .map(|column| columns[*column].iter())
            .collect();
        let mut row_buf = Row::default();
        for row in 0..len {
            let datums = projected
                .iter_mut()
                .map(|column| column.next().expect("every batch has `len` rows"));
            if live[row] {
                {
                    let mut packer = row_buf.packer();
                    for datum in datums {
                        packer.push(datum.expect("errors on live rows were absorbed"));
                    }
                }
                emit(row, Ok(&row_buf));
            } else {
                // Advance the column cursors past this row.
                datums.for_each(drop);
                if let Some(err) = failed.get(&row) {
                    emit(row, Err(err));
                }
            }
        }
        Some(())
    }
}

impl MfpPlan<MirScalarExpr> {
    /// The input columns that some expression, predicate, temporal bound, or
    /// the projection reads.
    pub fn referenced_inputs(&self) -> BTreeSet<usize> {
        let mfp = &self.mfp.mfp;
        let mut referenced = BTreeSet::new();
        let exprs = mfp
            .expressions
            .iter()
            .chain(mfp.predicates.iter().map(|(_, predicate)| predicate))
            .chain(&self.lower_bounds)
            .chain(&self.upper_bounds);
        for expr in exprs {
            referenced.extend(expr.support());
        }
        referenced.extend(mfp.projection.iter().copied());
        referenced.retain(|column| *column < mfp.input_arity);
        referenced
    }

    /// Batch form of [`Self::evaluate`] for plans without temporal predicates,
    /// see [`SafeMfpPlan::evaluate_batch`]. Returns `None` for a temporal plan.
    pub fn evaluate_batch(
        &self,
        inputs: &[DatumBatch<EvalError>],
        len: usize,
        emit: impl FnMut(usize, Result<&Row, &EvalError>),
    ) -> Option<()> {
        if !self.lower_bounds.is_empty() || !self.upper_bounds.is_empty() {
            return None;
        }
        self.mfp.evaluate_batch(inputs, len, emit)
    }
}

#[cfg(test)]
mod tests;
