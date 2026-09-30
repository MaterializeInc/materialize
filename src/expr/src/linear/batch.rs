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
//! restructures an MFP into a sequence of per-row steps separated by batched
//! WebAssembly calls: it evaluates every row up to a call, makes one call for
//! all rows still live, and continues.
//!
//! Only calls in *strict* positions are lifted into batched steps (see
//! [`OptimizableExpr::strict_children_mut`]): positions that are evaluated
//! whenever their enclosing expression is, and whose errors always surface.
//! Lifting such a call cannot change whether a row errors, only which of
//! several errors it reports, which is the same latitude MFP memoization
//! already takes. Calls in other positions stay inline and are evaluated one
//! row at a time.
//!
//! Each row sees the same predicates in the same order as
//! [`SafeMfpPlan::evaluate_inner`], so a filter placed before a call still
//! keeps rows it rejects away from the guest.

use itertools::Itertools;
use mz_repr::{Datum, Diff, Row, RowArena};

use crate::linear::plan::{MfpPlan, SafeMfpPlan, evaluate_temporal};
use crate::scalar::func::WasmFunc;
use crate::scalar::optimizable::OptimizableExpr;
use crate::{Eval, EvalError, MapFilterProject};

#[derive(Clone, Debug)]
enum Step<E> {
    /// Evaluate an expression per row and append it as a column.
    Map(E),
    /// Evaluate a predicate per row and drop rows for which it is not true.
    Filter(E),
    /// Evaluate the arguments per row, call the function once for all live
    /// rows, and append the results as a column.
    Wasm { func: WasmFunc, args: Vec<E> },
}

/// A [`SafeMfpPlan`] restructured for batched evaluation.
#[derive(Clone, Debug)]
pub struct BatchedSafeMfpPlan<E> {
    steps: Vec<Step<E>>,
    projection: Vec<usize>,
}

impl<E: OptimizableExpr + Eval> BatchedSafeMfpPlan<E> {
    /// Restructures `mfp`, or returns `None` if it has no WebAssembly call in
    /// a position that can be batched.
    ///
    /// The returned permutation maps each column of `mfp` (inputs followed
    /// by mapped expressions) to its position in the batched plan's datums.
    fn from_mfp(mfp: &MapFilterProject<E>) -> Option<(Self, Vec<usize>)> {
        let mut builder = Builder {
            steps: Vec::new(),
            next_column: mfp.input_arity,
            lifted: false,
        };
        let mut permutation: Vec<usize> = (0..mfp.input_arity).collect();

        let mut expression = 0;
        for (support, predicate) in mfp.predicates.iter() {
            while mfp.input_arity + expression < *support {
                let column = builder.map(&mfp.expressions[expression], &permutation);
                permutation.push(column);
                expression += 1;
            }
            builder.filter(predicate, &permutation);
        }
        while expression < mfp.expressions.len() {
            let column = builder.map(&mfp.expressions[expression], &permutation);
            permutation.push(column);
            expression += 1;
        }

        if !builder.lifted {
            return None;
        }
        let projection = mfp.projection.iter().map(|c| permutation[*c]).collect();
        Some((
            BatchedSafeMfpPlan {
                steps: builder.steps,
                projection,
            },
            permutation,
        ))
    }

    /// Evaluates the plan's steps on a batch of rows.
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
        let mut args = Vec::new();
        let mut callers = Vec::new();
        let mut results = Vec::new();

        for step in &self.steps {
            match step {
                Step::Map(expr) => {
                    for &i in &live {
                        match expr.eval(&rows[i], arena) {
                            Ok(datum) => rows[i].push(datum),
                            Err(e) => outcomes[i] = Err(e),
                        }
                    }
                }
                Step::Filter(predicate) => {
                    for &i in &live {
                        match predicate.eval(&rows[i], arena) {
                            Ok(Datum::True) => {}
                            Ok(_) => outcomes[i] = Ok(false),
                            Err(e) => outcomes[i] = Err(e),
                        }
                    }
                }
                Step::Wasm { func, args: exprs } => {
                    args.clear();
                    callers.clear();
                    for &i in &live {
                        let row_args: Result<Vec<_>, _> =
                            exprs.iter().map(|e| e.eval(&rows[i], arena)).collect();
                        match row_args {
                            Ok(row_args) => {
                                args.push(row_args);
                                callers.push(i);
                            }
                            Err(e) => outcomes[i] = Err(e),
                        }
                    }
                    let slices: Vec<&[Datum<'a>]> = args.iter().map(Vec::as_slice).collect();
                    results.clear();
                    func.call_batch(&slices, arena, &mut results);
                    for (&i, result) in callers.iter().zip_eq(results.drain(..)) {
                        match result {
                            Ok(datum) => rows[i].push(datum),
                            Err(e) => outcomes[i] = Err(e),
                        }
                    }
                }
            }
            live.retain(|&i| matches!(outcomes[i], Ok(true)));
            if live.is_empty() {
                break;
            }
        }
    }

    /// The columns of a row's datums that form its output.
    pub fn projection(&self) -> &[usize] {
        &self.projection
    }
}

struct Builder<E> {
    steps: Vec<Step<E>>,
    next_column: usize,
    lifted: bool,
}

impl<E: OptimizableExpr> Builder<E> {
    /// Appends steps that compute `expr`, and returns its column.
    fn map(&mut self, expr: &E, permutation: &[usize]) -> usize {
        let mut expr = expr.clone();
        expr.permute(permutation);
        self.lift(&mut expr);
        self.steps.push(Step::Map(expr));
        self.push_column()
    }

    /// Appends steps that apply `predicate`.
    fn filter(&mut self, predicate: &E, permutation: &[usize]) {
        let mut predicate = predicate.clone();
        predicate.permute(permutation);
        self.lift(&mut predicate);
        self.steps.push(Step::Filter(predicate));
    }

    /// Replaces each WebAssembly call in a strict position of `expr` with a
    /// reference to a column computed by a preceding batched step. Calls are
    /// lifted innermost first, so a call's arguments never contain a
    /// liftable call.
    fn lift(&mut self, expr: &mut E) {
        for child in expr.strict_children_mut() {
            self.lift(child);
        }
        if let Some((func, args)) = expr.as_wasm_call() {
            self.steps.push(Step::Wasm {
                func: func.clone(),
                args: args.to_vec(),
            });
            self.lifted = true;
            *expr = E::column(self.push_column());
        }
    }

    fn push_column(&mut self) -> usize {
        let column = self.next_column;
        self.next_column += 1;
        column
    }
}

impl<E: OptimizableExpr + Eval> SafeMfpPlan<E> {
    /// Returns a batched form of this plan, or `None` if it has no
    /// WebAssembly call that batching would help.
    pub fn batched(&self) -> Option<BatchedSafeMfpPlan<E>> {
        BatchedSafeMfpPlan::from_mfp(&self.mfp).map(|(plan, _)| plan)
    }
}

/// An [`MfpPlan`] restructured for batched evaluation.
#[derive(Clone, Debug)]
pub struct BatchedMfpPlan<E> {
    mfp: BatchedSafeMfpPlan<E>,
    lower_bounds: Vec<E>,
    upper_bounds: Vec<E>,
}

impl<E: OptimizableExpr + Eval> MfpPlan<E> {
    /// Returns a batched form of this plan, or `None` if it has no
    /// WebAssembly call that batching would help.
    ///
    /// Calls inside temporal bounds are not batched.
    pub fn batched(&self) -> Option<BatchedMfpPlan<E>> {
        let (safe, lower, upper) = self.as_parts();
        let (mfp, permutation) = BatchedSafeMfpPlan::from_mfp(&safe.mfp)?;
        let permute = |bounds: &[E]| {
            bounds
                .iter()
                .map(|b| {
                    let mut b = b.clone();
                    b.permute(&permutation);
                    b
                })
                .collect()
        };
        Some(BatchedMfpPlan {
            mfp,
            lower_bounds: permute(lower),
            upper_bounds: permute(upper),
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
                &self.mfp.projection,
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

        let batched_plan = plan.batched().expect("plan has a batchable call");
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
        // Three single-row calls from the per-row evaluator's nested call,
        // twice, then one three-row call per lifted call site.
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

    #[mz_ore::test]
    fn calls_in_lazy_positions_are_not_batched() {
        let export = "calls_in_lazy_positions_are_not_batched";
        let guarded = MirScalarExpr::column(0)
            .call_binary(int(0), func::Gt)
            .if_then_else(double(export, MirScalarExpr::column(0)), int(0));
        let mfp = MapFilterProject::new(1).map([guarded]);
        assert!(safe_plan(mfp).batched().is_none());

        let absorbed = MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(func::variadic::And),
            exprs: vec![
                MirScalarExpr::column(0).call_binary(int(0), func::Gt),
                double(export, MirScalarExpr::column(0)).call_binary(int(0), func::Gt),
            ],
        };
        let mfp = MapFilterProject::new(1).map([absorbed]);
        assert!(safe_plan(mfp).batched().is_none());
    }
}
