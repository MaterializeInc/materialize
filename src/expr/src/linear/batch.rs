// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Batched evaluation of MFPs that call WebAssembly functions.
//!
//! A call into a guest has a fixed cost (instantiation, encoding, the call
//! itself) that only amortizes over many rows. [`BatchedSafeMfpPlan`]
//! evaluates an MFP over a batch of rows in the order
//! [`SafeMfpPlan::evaluate_inner`] uses, one step (a mapped expression or a
//! predicate) at a time. Before each step it precomputes every WebAssembly
//! call the step contains, innermost first, with one guest call per call site
//! for all rows still live. It then evaluates the step row by row, and each
//! call replays its row's precomputed outcome instead of calling the guest.
//!
//! The results are exactly those of per-row evaluation. A call replays the
//! outcome it would have computed, at the point where per-row evaluation
//! reaches it, so error precedence among sibling expressions and
//! short-circuiting in `If`, `COALESCE`, `AND` and `OR` behave the same.
//! Calls in positions that a row never reaches are computed and discarded:
//! that costs guest work but never changes an outcome. Rows that an earlier
//! predicate rejected, or that already failed, are not sent to the guest.

use itertools::Itertools;
use mz_repr::{Datum, Diff, Row, RowArena};

use crate::linear::plan::{MfpPlan, SafeMfpPlan, evaluate_temporal};
use crate::scalar::func::WasmFunc;
use crate::scalar::func::impls::replay::FrameGuard;
use crate::scalar::optimizable::OptimizableExpr;
use crate::visit::Visit;
use crate::{Eval, EvalError, MapFilterProject, MirScalarExpr};

/// A [`SafeMfpPlan`] that calls WebAssembly functions, prepared for batched
/// evaluation.
#[derive(Clone, Debug)]
pub struct BatchedSafeMfpPlan<E: OptimizableExpr = MirScalarExpr> {
    mfp: MapFilterProject<E>,
}

/// The WebAssembly calls in `expr`, innermost first, so that a call's
/// arguments only contain calls that come before it.
fn wasm_calls<E: OptimizableExpr>(expr: &E) -> Vec<(&WasmFunc, &[E])> {
    let mut calls = Vec::new();
    expr.visit_post(&mut |e: &E| {
        if let Some(call) = e.as_wasm_call() {
            calls.push(call);
        }
    });
    calls
}

impl<E: OptimizableExpr + Eval> BatchedSafeMfpPlan<E> {
    /// Returns `None` if `mfp` calls no WebAssembly function.
    fn from_mfp(mfp: &MapFilterProject<E>) -> Option<Self> {
        let calls_wasm = mfp
            .expressions
            .iter()
            .chain(mfp.predicates.iter().map(|(_, p)| p))
            .any(|e| !wasm_calls(e).is_empty());
        calls_wasm.then(|| BatchedSafeMfpPlan { mfp: mfp.clone() })
    }

    /// Evaluates the plan on a batch of rows.
    ///
    /// `rows[i]` holds the input datums of row `i`. On return, `outcomes[i]`
    /// is `Ok(true)` if the row passed every predicate, in which case
    /// `rows[i]` holds all of its columns and [`Self::projection`] selects
    /// the output, `Ok(false)` if a predicate rejected it, or the error it
    /// produced.
    pub fn evaluate_batch<'a>(
        &'a self,
        rows: &mut [Vec<Datum<'a>>],
        arena: &'a RowArena,
        outcomes: &mut Vec<Result<bool, EvalError>>,
    ) {
        outcomes.clear();
        outcomes.resize(rows.len(), Ok(true));
        let mut live: Vec<usize> = (0..rows.len()).collect();
        let frame = FrameGuard::install();
        let mfp = &self.mfp;

        let mut expression = 0;
        for (support, predicate) in mfp.predicates.iter() {
            while mfp.input_arity + expression < *support {
                let expr = &mfp.expressions[expression];
                precompute(expr, rows, &live, arena, &frame);
                for &i in &live {
                    frame.set_row(i);
                    match expr.eval(&rows[i], arena) {
                        Ok(datum) => rows[i].push(datum),
                        Err(e) => outcomes[i] = Err(e),
                    }
                }
                live.retain(|&i| outcomes[i].is_ok());
                expression += 1;
            }
            precompute(predicate, rows, &live, arena, &frame);
            for &i in &live {
                frame.set_row(i);
                match predicate.eval(&rows[i], arena) {
                    Ok(Datum::True) => {}
                    Ok(_) => outcomes[i] = Ok(false),
                    Err(e) => outcomes[i] = Err(e),
                }
            }
            live.retain(|&i| matches!(outcomes[i], Ok(true)));
        }
        while expression < mfp.expressions.len() {
            let expr = &mfp.expressions[expression];
            precompute(expr, rows, &live, arena, &frame);
            for &i in &live {
                frame.set_row(i);
                match expr.eval(&rows[i], arena) {
                    Ok(datum) => rows[i].push(datum),
                    Err(e) => outcomes[i] = Err(e),
                }
            }
            live.retain(|&i| outcomes[i].is_ok());
            expression += 1;
        }
    }

    /// The columns of a row's datums that form its output.
    pub fn projection(&self) -> &[usize] {
        &self.mfp.projection
    }
}

/// Records, in `frame`, the outcome of every WebAssembly call in `expr` for
/// each live row, with one guest call per call site.
///
/// A call's outcome includes its arguments: a row whose arguments fail to
/// evaluate records that error and is not sent to the guest. Arguments are
/// evaluated with `frame` replaying the calls already recorded, which are
/// the calls nested inside them.
fn precompute<'a, E: OptimizableExpr + Eval>(
    expr: &'a E,
    rows: &[Vec<Datum<'a>>],
    live: &[usize],
    arena: &'a RowArena,
    frame: &FrameGuard,
) {
    frame.clear();
    let mut args = Vec::new();
    let mut callers = Vec::new();
    let mut results = Vec::new();
    for (func, exprs) in wasm_calls(expr) {
        args.clear();
        callers.clear();
        let mut failed = Vec::new();
        for &i in live {
            frame.set_row(i);
            let row_args: Result<Vec<_>, _> =
                exprs.iter().map(|e| e.eval(&rows[i], arena)).collect();
            match row_args {
                Ok(row_args) => {
                    args.push(row_args);
                    callers.push(i);
                }
                Err(e) => failed.push((i, Err(e))),
            }
        }
        let slices: Vec<&[Datum<'a>]> = args.iter().map(Vec::as_slice).collect();
        results.clear();
        func.call_batch(&slices, arena, &mut results);
        let called = callers
            .iter()
            .zip_eq(results.drain(..))
            .map(|(&i, result)| (i, result.map(|d| Row::pack_slice(&[d]))));
        frame.record(func, rows.len(), failed.into_iter().chain(called));
    }
}

impl<E: OptimizableExpr + Eval> SafeMfpPlan<E> {
    /// Returns a batched form of this plan, or `None` if it calls no
    /// WebAssembly function.
    pub fn batched(&self) -> Option<BatchedSafeMfpPlan<E>> {
        BatchedSafeMfpPlan::from_mfp(&self.mfp)
    }
}

/// An [`MfpPlan`] prepared for batched evaluation.
#[derive(Clone, Debug)]
pub struct BatchedMfpPlan<E: OptimizableExpr = MirScalarExpr> {
    mfp: BatchedSafeMfpPlan<E>,
    lower_bounds: Vec<E>,
    upper_bounds: Vec<E>,
}

impl<E: OptimizableExpr + Eval> MfpPlan<E> {
    /// Returns a batched form of this plan, or `None` if it calls no
    /// WebAssembly function.
    ///
    /// Calls inside temporal bounds are evaluated per row.
    pub fn batched(&self) -> Option<BatchedMfpPlan<E>> {
        let (safe, lower, upper) = self.as_parts();
        Some(BatchedMfpPlan {
            mfp: BatchedSafeMfpPlan::from_mfp(&safe.mfp)?,
            lower_bounds: lower.to_vec(),
            upper_bounds: upper.to_vec(),
        })
    }
}

impl<E: OptimizableExpr + Eval> BatchedMfpPlan<E> {
    /// Evaluates the non-temporal steps on a batch of rows. See
    /// [`BatchedSafeMfpPlan::evaluate_batch`].
    pub fn evaluate_batch<'a>(
        &'a self,
        rows: &mut [Vec<Datum<'a>>],
        arena: &'a RowArena,
        outcomes: &mut Vec<Result<bool, EvalError>>,
    ) {
        self.mfp.evaluate_batch(rows, arena, outcomes)
    }

    /// Finishes one row of a batch: applies temporal bounds and projects it,
    /// with the same results as [`MfpPlan::evaluate`].
    pub fn finish<'b, 'a: 'b, Err: From<EvalError>, V: Fn(&mz_repr::Timestamp) -> bool>(
        &'a self,
        outcome: Result<bool, EvalError>,
        datums: &'b [Datum<'a>],
        arena: &'a RowArena,
        time: mz_repr::Timestamp,
        diff: Diff,
        valid_time: V,
        row_builder: &mut Row,
    ) -> impl Iterator<
        Item = Result<(Row, mz_repr::Timestamp, Diff), (Err, mz_repr::Timestamp, Diff)>,
    > + use<Err, V, E> {
        match outcome {
            Err(e) => Some(Err((e.into(), time, diff))).into_iter().chain(None),
            Ok(false) => None.into_iter().chain(None),
            Ok(true) => evaluate_temporal(
                &self.lower_bounds,
                &self.upper_bounds,
                self.mfp.projection(),
                datums,
                arena,
                time,
                diff,
                valid_time,
                row_builder,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use itertools::Itertools;
    use mz_repr::{Datum, ReprScalarType, RowArena, SqlScalarType};

    use crate::func::{
        self, InvokerCell, WasmErrorKind, WasmFunc, WasmInvoker, WasmLimits, WasmModuleHash,
        WasmRuntime, install_wasm_runtime,
    };
    use crate::{EvalError, MapFilterProject, MirScalarExpr, SafeMfpPlan, VariadicFunc};

    /// Every batch the fake guest received: `(export, rows)`.
    static CALLS: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

    /// Doubles its `bigint` argument, and errors on negative input.
    #[derive(Debug)]
    struct Doubler(String);

    impl WasmInvoker for Doubler {
        fn call_batch<'a>(
            &self,
            args: &[&[Datum<'a>]],
            _arena: &'a RowArena,
            out: &mut Vec<Result<Datum<'a>, EvalError>>,
        ) {
            CALLS.lock().unwrap().push((self.0.clone(), args.len()));
            out.extend(args.iter().map(|row| match row[0] {
                Datum::Null => Ok(Datum::Null),
                Datum::Int64(i) if i < 0 => Err(EvalError::WasmFunction {
                    name: self.0.clone().into(),
                    kind: WasmErrorKind::Guest,
                    message: "negative".into(),
                }),
                Datum::Int64(i) => Ok(Datum::Int64(i * 2)),
                d => panic!("unexpected datum {d:?}"),
            }));
        }
    }

    struct FakeRuntime;

    impl WasmRuntime for FakeRuntime {
        fn bind(&self, func: &WasmFunc) -> Result<Arc<dyn WasmInvoker>, EvalError> {
            Ok(Arc::new(Doubler(func.export.clone())))
        }
    }

    fn double(export: &str, arg: MirScalarExpr) -> MirScalarExpr {
        let _ = install_wasm_runtime(Arc::new(FakeRuntime));
        MirScalarExpr::CallVariadic {
            func: VariadicFunc::Wasm(WasmFunc {
                name: export.into(),
                module: WasmModuleHash([0; 32]),
                export: export.into(),
                arg_types: vec![SqlScalarType::Int64],
                return_type: SqlScalarType::Int64,
                strict: false,
                limits: WasmLimits {
                    fuel: 0,
                    memory_bytes: 0,
                },
                invoker: InvokerCell::default(),
            }),
            exprs: vec![arg],
        }
    }

    fn int(i: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(i), ReprScalarType::Int64)
    }

    fn calls(export: &str) -> Vec<usize> {
        CALLS
            .lock()
            .unwrap()
            .iter()
            .filter(|(e, _)| e == export)
            .map(|(_, n)| *n)
            .collect()
    }

    fn safe_plan(mfp: MapFilterProject) -> SafeMfpPlan {
        mfp.into_plan().unwrap().into_nontemporal().unwrap()
    }

    /// Evaluates `plan` both per row and batched over `inputs`, asserts that
    /// the results agree, and returns them.
    fn evaluate_both(
        plan: &SafeMfpPlan,
        inputs: &[Datum<'static>],
    ) -> Vec<Result<Option<Vec<Datum<'static>>>, EvalError>> {
        let arena = RowArena::new();
        let per_row: Vec<_> = inputs
            .iter()
            .map(|d| {
                let mut datums = vec![*d];
                plan.evaluate_iter(&mut datums, &arena)
                    .map(|out| out.map(|it| it.map(owned).collect::<Vec<_>>()))
            })
            .collect();

        let batched_plan = plan.batched().expect("plan calls a WebAssembly function");
        let mut rows: Vec<Vec<Datum>> = inputs.iter().map(|d| vec![*d]).collect();
        let mut outcomes = Vec::new();
        batched_plan.evaluate_batch(&mut rows, &arena, &mut outcomes);
        let batched: Vec<_> = rows
            .iter()
            .zip_eq(outcomes)
            .map(|(row, outcome)| {
                outcome.map(|passed| {
                    passed.then(|| {
                        batched_plan
                            .projection()
                            .iter()
                            .map(|c| owned(row[*c]))
                            .collect()
                    })
                })
            })
            .collect();

        assert_eq!(per_row, batched);
        batched
    }

    /// The tests only use `Copy` datums, which need no arena.
    fn owned(d: Datum) -> Datum<'static> {
        match d {
            Datum::Null => Datum::Null,
            Datum::True => Datum::True,
            Datum::False => Datum::False,
            Datum::Int64(i) => Datum::Int64(i),
            d => panic!("unexpected datum {d:?}"),
        }
    }

    #[mz_ore::test]
    fn batched_matches_per_row() {
        // #1 = double(#0), #2 = double(#1) + 1, keep rows where #2 > 10.
        let mfp = MapFilterProject::new(1)
            .map([
                double("batched_matches_per_row", MirScalarExpr::column(0)),
                double("batched_matches_per_row", MirScalarExpr::column(1))
                    .call_binary(int(1), func::AddInt64),
            ])
            .filter([MirScalarExpr::column(2).call_binary(int(10), func::Gt)])
            .project([0, 2]);
        let inputs = [
            Datum::Int64(1),
            Datum::Int64(3),
            Datum::Int64(-1),
            Datum::Null,
            Datum::Int64(100),
        ];
        let results = evaluate_both(&safe_plan(mfp), &inputs);
        assert_eq!(
            results[1],
            Ok(Some(vec![Datum::Int64(3), Datum::Int64(13)]))
        );
        assert!(results[2].is_err());
        assert_eq!(results[3], Ok(None));
    }

    #[mz_ore::test]
    fn each_call_site_is_one_batch() {
        let export = "each_call_site_is_one_batch";
        let mfp = MapFilterProject::new(1)
            .map([double(export, double(export, MirScalarExpr::column(0)))]);
        let inputs = [Datum::Int64(1), Datum::Int64(2), Datum::Int64(3)];
        let results = evaluate_both(&safe_plan(mfp), &inputs);
        assert_eq!(
            results[2],
            Ok(Some(vec![Datum::Int64(3), Datum::Int64(12)]))
        );
        // Three rows of two nested per-row calls, then one three-row call for
        // each call site.
        assert_eq!(calls(export), vec![1, 1, 1, 1, 1, 1, 3, 3]);
    }

    #[mz_ore::test]
    fn filters_before_a_call_keep_rows_from_the_guest() {
        let export = "filters_before_a_call_keep_rows_from_the_guest";
        let mfp = MapFilterProject::new(1)
            .filter([MirScalarExpr::column(0).call_binary(int(0), func::Gt)])
            .map([double(export, MirScalarExpr::column(0))]);
        let plan = safe_plan(mfp).batched().unwrap();
        let arena = RowArena::new();
        let mut rows: Vec<Vec<Datum>> = [-2, -1, 0, 1, 2]
            .into_iter()
            .map(|i| vec![Datum::Int64(i)])
            .collect();
        let mut outcomes = Vec::new();
        plan.evaluate_batch(&mut rows, &arena, &mut outcomes);
        assert_eq!(
            outcomes,
            vec![Ok(false), Ok(false), Ok(false), Ok(true), Ok(true)]
        );
        assert_eq!(calls(export), vec![2]);
    }

    /// `(#0 + 1) / (#0 + 1) + double(#0)` fails twice at `#0 = -1`. Per-row
    /// evaluation reports the division's error, the earlier argument.
    #[mz_ore::test]
    fn sibling_error_precedence_is_preserved() {
        let export = "sibling_error_precedence_is_preserved";
        let succ = MirScalarExpr::column(0).call_binary(int(1), func::AddInt64);
        let quotient = succ.clone().call_binary(succ, func::DivInt64);
        let expr = quotient.call_binary(double(export, MirScalarExpr::column(0)), func::AddInt64);
        let mfp = MapFilterProject::new(1).map([expr]);
        let results = evaluate_both(&safe_plan(mfp), &[Datum::Int64(-1), Datum::Int64(2)]);
        assert_eq!(results[0], Err(EvalError::DivisionByZero));
        assert_eq!(results[1], Ok(Some(vec![Datum::Int64(2), Datum::Int64(5)])));
    }

    #[mz_ore::test]
    fn calls_in_lazy_positions_are_batched() {
        let export = "calls_in_lazy_positions_are_batched";
        // Negative rows take the else branch, so the guest's error for them
        // is computed but never reported.
        let guarded = MirScalarExpr::column(0)
            .call_binary(int(0), func::Gt)
            .if_then_else(double(export, MirScalarExpr::column(0)), int(0));
        let mfp = MapFilterProject::new(1).map([guarded]);
        let inputs = [Datum::Int64(-1), Datum::Int64(4)];
        let results = evaluate_both(&safe_plan(mfp), &inputs);
        assert_eq!(
            results[0],
            Ok(Some(vec![Datum::Int64(-1), Datum::Int64(0)]))
        );
        assert_eq!(calls(export).last(), Some(&2));

        // A false conjunct absorbs the guest's error.
        let absorbed = MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(func::variadic::And),
            exprs: vec![
                MirScalarExpr::column(0).call_binary(int(0), func::Gt),
                double(export, MirScalarExpr::column(0)).call_binary(int(0), func::Gt),
            ],
        };
        let mfp = MapFilterProject::new(1).map([absorbed]);
        let results = evaluate_both(&safe_plan(mfp), &inputs);
        assert_eq!(results[0], Ok(Some(vec![Datum::Int64(-1), Datum::False])));
    }
}
