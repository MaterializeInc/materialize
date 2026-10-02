// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Batch evaluation of eager functions, derived from their argument and
//! result types.
//!
//! An eager function declares its argument type `Input<'a>` and its result
//! type `Output<'a>`. When both have batch forms, see
//! [`InputDatumType::iter_batch`] and [`OutputDatumType::new_batch`], the
//! function runs over whole columns: its argument batches are aligned to the
//! rows it may see, it is called once per such row, and its results are
//! collected into one output batch.

use mz_repr::batch::DatumBatch;
use mz_repr::{InputDatumType, OutputDatumType, RowArena};

use crate::EvalError;
use crate::scalar::func::binary::EagerBinaryFunc;
use crate::scalar::func::unary::EagerUnaryFunc;

/// Whether a kernel with argument type `I` sees null rows.
///
/// `Some(false)` withholds them, as the function propagates nulls.
/// `Some(true)` passes them through. `None` means the argument positions
/// disagree, and the function has no batch form.
fn pass_nulls<'a, I: InputDatumType<'a, EvalError>>() -> Option<bool> {
    match (I::nullable(), I::all_nullable()) {
        (false, _) => Some(false),
        (true, true) => Some(true),
        (true, false) => None,
    }
}

/// Batch form of [`EagerUnaryFunc::call`].
pub(crate) fn call_unary<'a, F: EagerUnaryFunc + ?Sized>(
    func: &F,
    batch: &'a DatumBatch<EvalError>,
) -> Option<DatumBatch<EvalError>> {
    let output = <F::Output<'a> as OutputDatumType<'a, EvalError>>::new_batch()?;
    let pass_nulls = pass_nulls::<F::Input<'a>>()?;
    let aligned = DatumBatch::align(&[batch], pass_nulls);
    let output = run_unary(func, &aligned.inputs(), output)?;
    Some(aligned.finish(output))
}

/// Calls `func` once per row of the aligned `inputs`, appending to `output`.
fn run_unary<'b, F: EagerUnaryFunc + ?Sized>(
    func: &F,
    inputs: &[&'b DatumBatch<EvalError>],
    mut output: DatumBatch<EvalError>,
) -> Option<DatumBatch<EvalError>> {
    for input in <F::Input<'b> as InputDatumType<'b, EvalError>>::iter_batch(inputs)? {
        func.call(input).push_batch(&mut output);
    }
    Some(output)
}

/// Batch form of [`EagerBinaryFunc::call`].
pub(crate) fn call_binary<'a, F: EagerBinaryFunc + ?Sized>(
    func: &F,
    batches: &[&'a DatumBatch<EvalError>],
    temp_storage: &'a RowArena,
) -> Option<DatumBatch<EvalError>> {
    let output = <F::Output<'a> as OutputDatumType<'a, EvalError>>::new_batch()?;
    let pass_nulls = pass_nulls::<F::Input<'a>>()?;
    let aligned = DatumBatch::align(batches, pass_nulls);
    let output = run_binary(func, &aligned.inputs(), temp_storage, output)?;
    Some(aligned.finish(output))
}

/// Calls `func` once per row of the aligned `inputs`, appending to `output`.
fn run_binary<'b, F: EagerBinaryFunc + ?Sized>(
    func: &F,
    inputs: &[&'b DatumBatch<EvalError>],
    temp_storage: &'b RowArena,
    mut output: DatumBatch<EvalError>,
) -> Option<DatumBatch<EvalError>> {
    for input in <F::Input<'b> as InputDatumType<'b, EvalError>>::iter_batch(inputs)? {
        func.call(input, temp_storage).push_batch(&mut output);
    }
    Some(output)
}
