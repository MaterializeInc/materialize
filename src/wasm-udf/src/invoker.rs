// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Batched invocation with per-row semantics.
//!
//! The outcome for a row is defined as the outcome of calling the function on
//! a batch holding only that row, in a fresh instance, under the function's
//! limits. Larger batches are an optimization that must not change any
//! outcome:
//!
//! * Every call, whatever its size, runs in a fresh instance, so no state
//!   carries between batches.
//! * Per-row errors the guest returns in its error column are final.
//! * A failure of a whole call (trap, fuel, memory, a `-1` return, an ABI
//!   violation) splits the batch in half and retries each half, down to
//!   single rows, whose failure is that row's error.
//! * The same fuel and memory limits apply to every call. A batch that
//!   finishes within them shows that each of its rows would finish alone, as
//!   long as a row costs no more alone than in any batch containing it.
//!
//! What the host cannot check is that the guest's output for a row depends
//! only on that row. That is part of the function's contract.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use itertools::Itertools;
use mz_expr::EvalError;
use mz_expr::func::{WasmErrorKind, WasmInvoker, WasmLimits};
use mz_repr::{Datum, RowArena};

use crate::call::{CallError, CompiledModule, MAX_MESSAGE_BYTES, truncated_str};
use crate::codec::{self, RowResult, ValueType};

/// Upper bounds on the size of one guest call, shared by every invoker of a
/// runtime.
#[derive(Debug)]
pub struct BatchConfig {
    max_rows: AtomicUsize,
    max_bytes: AtomicUsize,
}

impl BatchConfig {
    /// Large enough that vectorized guests (arrow-udf `batch_fn`s, or the
    /// macro's SIMD kernels for primitive functions) see whole containers.
    pub const DEFAULT_MAX_ROWS: usize = 16 << 10;
    /// Keeps a full batch's input and output buffers, and the guest's decoded
    /// copies of them, well within the default memory limit.
    pub const DEFAULT_MAX_BYTES: usize = 8 << 20;

    pub fn set(&self, max_rows: usize, max_bytes: usize) {
        self.max_rows.store(max_rows.max(1), Ordering::Relaxed);
        self.max_bytes.store(max_bytes.max(1), Ordering::Relaxed);
    }

    pub fn max_rows(&self) -> usize {
        self.max_rows.load(Ordering::Relaxed)
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes.load(Ordering::Relaxed)
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        BatchConfig {
            max_rows: AtomicUsize::new(Self::DEFAULT_MAX_ROWS),
            max_bytes: AtomicUsize::new(Self::DEFAULT_MAX_BYTES),
        }
    }
}

/// Receives the details of every guest call an [`Invoker`] makes.
pub trait CallObserver: Send + Sync {
    fn observe(
        &self,
        rows: usize,
        fuel_consumed: u64,
        peak_memory: usize,
        output: &[u8],
        failed: bool,
    );
}

/// One bound function: a module export with its SQL types and limits.
pub struct Invoker {
    name: String,
    module: Arc<CompiledModule>,
    export: String,
    args: Vec<ValueType>,
    ret: ValueType,
    limits: WasmLimits,
    batch: Arc<BatchConfig>,
    /// The fuel the most recent successful call used per row, which sizes the
    /// next batch. Batch sizes never change results, so this estimate need
    /// not be deterministic.
    fuel_per_row: AtomicU64,
    observer: Option<Arc<dyn CallObserver>>,
}

impl fmt::Debug for Invoker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Invoker")
            .field("name", &self.name)
            .field("export", &self.export)
            .finish_non_exhaustive()
    }
}

impl Invoker {
    pub fn new(
        name: String,
        module: Arc<CompiledModule>,
        export: String,
        args: Vec<ValueType>,
        ret: ValueType,
        limits: WasmLimits,
        batch: Arc<BatchConfig>,
    ) -> Self {
        Invoker {
            name,
            module,
            export,
            args,
            ret,
            limits,
            batch,
            fuel_per_row: AtomicU64::new(0),
            observer: None,
        }
    }

    /// Reports every guest call to `observer`.
    pub fn with_observer<O: CallObserver + 'static>(mut self, observer: Arc<O>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// An error of this function. Errors are persisted, so every message is
    /// capped here, whatever its source. Sources that copy guest data cap it
    /// as they copy too, so a large message is never built in full.
    fn error(&self, kind: WasmErrorKind, message: String) -> EvalError {
        EvalError::WasmFunction {
            name: self.name.clone().into(),
            kind,
            message: truncated_str(&message, MAX_MESSAGE_BYTES).into(),
        }
    }

    /// The most values one call's output may decode to, nested values
    /// included.
    ///
    /// Each value becomes at least a `Datum` on the host, so a compact output,
    /// such as a list of booleans at one bit each, could otherwise decode to
    /// many times the guest's memory limit. Exceeding the budget fails the
    /// whole call, which bisection narrows to the rows responsible, so whether
    /// a row exceeds it does not depend on its batch.
    fn max_decoded_values(&self) -> usize {
        // The floor keeps a full batch of scalars within budget for a small
        // memory limit.
        usize::try_from(self.limits.memory_bytes / 16)
            .unwrap_or(usize::MAX)
            .max(1 << 16)
    }

    /// The most rows the next call should carry, given the fuel rows have
    /// recently cost. Heavier functions get smaller batches so that full
    /// batches rarely exhaust their fuel and bisect.
    fn rows_per_call(&self) -> usize {
        let max_rows = self.batch.max_rows();
        match self.fuel_per_row.load(Ordering::Relaxed) {
            0 => max_rows,
            per_row => {
                let fit = self.limits.fuel / per_row.saturating_mul(2).max(1);
                usize::try_from(fit)
                    .unwrap_or(usize::MAX)
                    .clamp(1, max_rows)
            }
        }
    }

    /// Calls the guest on `rows` and appends one result per row, bisecting
    /// on whole-call failures.
    fn call_bisecting<'a>(
        &self,
        rows: &[&[Datum<'a>]],
        arena: &'a RowArena,
        out: &mut Vec<Result<Datum<'a>, EvalError>>,
    ) {
        match self.call_once(rows, arena) {
            Ok(results) => out.extend(results),
            Err(e) if rows.len() == 1 => out.push(Err(e)),
            Err(_) => {
                let (left, right) = rows.split_at(rows.len() / 2);
                self.call_bisecting(left, arena, out);
                self.call_bisecting(right, arena, out);
            }
        }
    }

    /// Makes one guest call for `rows`. Errors are failures of the whole call.
    fn call_once<'a>(
        &self,
        rows: &[&[Datum<'a>]],
        arena: &'a RowArena,
    ) -> Result<Vec<Result<Datum<'a>, EvalError>>, EvalError> {
        let input = codec::encode(&self.args, rows)
            .map_err(|e| self.error(WasmErrorKind::Conversion, e))?;
        let report = self.module.call(&self.export, &input, self.limits);
        if let Some(observer) = &self.observer {
            observer.observe(
                rows.len(),
                report.fuel_consumed,
                report.peak_memory,
                &report.output,
                report.result.is_err(),
            );
        }
        // Guest output can echo row data, which logs must not carry. `mz udf`
        // shows the output itself when a call is replayed locally.
        if !report.output.is_empty() {
            tracing::debug!(
                function = %self.name,
                bytes = report.output.len(),
                "WebAssembly function wrote output",
            );
        }
        let output = report
            .result
            .map_err(|e: CallError| self.error(e.kind(), e.message(&report.output)))?;
        let decoded = codec::decode(
            &output,
            &self.ret,
            rows.len(),
            self.max_decoded_values(),
            arena,
        )
        .map_err(|e| self.error(WasmErrorKind::CallFailed, e))?;

        let rows_u64 = u64::try_from(rows.len()).expect("usize fits in u64");
        self.fuel_per_row.store(
            report.fuel_consumed.div_ceil(rows_u64.max(1)),
            Ordering::Relaxed,
        );

        Ok(decoded
            .into_iter()
            .map(|r| match r {
                RowResult::Value(d) => Ok(d),
                RowResult::GuestError(m) => Err(self.error(WasmErrorKind::Guest, m)),
                RowResult::Invalid(m) => Err(self.error(WasmErrorKind::Conversion, m)),
            })
            .collect())
    }
}

impl WasmInvoker for Invoker {
    fn call_batch<'a>(
        &self,
        args: &[&[Datum<'a>]],
        arena: &'a RowArena,
        out: &mut Vec<Result<Datum<'a>, EvalError>>,
    ) {
        // Rows that cannot be encoded get their error up front, so that
        // encoding the rest cannot fail partway through a batch.
        let mut results: Vec<Option<Result<Datum<'a>, EvalError>>> = Vec::with_capacity(args.len());
        let mut encodable = Vec::with_capacity(args.len());
        for (i, row) in args.iter().enumerate() {
            let checked = self
                .args
                .iter()
                .zip_eq(row.iter())
                .try_for_each(|(typ, d)| codec::check(typ, *d));
            match checked {
                Ok(()) => {
                    results.push(None);
                    encodable.push(i);
                }
                Err(e) => results.push(Some(Err(self.error(WasmErrorKind::Conversion, e)))),
            }
        }

        let max_bytes = self.batch.max_bytes();
        let mut chunk: Vec<&[Datum<'a>]> = Vec::new();
        let mut chunk_rows: Vec<usize> = Vec::new();
        let mut chunk_bytes = 0;
        let mut chunk_results = Vec::new();
        let mut flush = |chunk: &mut Vec<&[Datum<'a>]>,
                         chunk_rows: &mut Vec<usize>,
                         results: &mut Vec<Option<_>>| {
            if chunk.is_empty() {
                return;
            }
            chunk_results.clear();
            self.call_bisecting(chunk, arena, &mut chunk_results);
            for (i, r) in chunk_rows.drain(..).zip_eq(chunk_results.drain(..)) {
                results[i] = Some(r);
            }
            chunk.clear();
        };
        for i in encodable {
            let row = args[i];
            let bytes: usize = row.iter().map(|d| codec::encoded_size(*d)).sum();
            if !chunk.is_empty()
                && (chunk.len() >= self.rows_per_call() || chunk_bytes + bytes > max_bytes)
            {
                flush(&mut chunk, &mut chunk_rows, &mut results);
                chunk_bytes = 0;
            }
            chunk.push(row);
            chunk_rows.push(i);
            chunk_bytes += bytes;
        }
        flush(&mut chunk, &mut chunk_rows, &mut results);

        out.extend(
            results
                .into_iter()
                .map(|r| r.expect("every row has a result")),
        );
    }
}
