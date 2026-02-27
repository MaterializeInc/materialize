// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-row compiled expression evaluation via cached WASM instances.
//!
//! This module provides [`CompiledExprSession`] for evaluating a single compiled
//! expression one row at a time, and [`CompiledMfp`] for evaluating an entire
//! [`MapFilterProject`] with compiled expressions replacing the interpreter
//! where possible.

use mz_expr::{EvalError, MapFilterProject, MfpPlan};
use mz_repr::{Datum, Diff, Row, RowArena};

use crate::engine::{CompiledExpr, ExprEngine};

/// A cached WASM session for evaluating a single compiled expression repeatedly.
///
/// Created once per operator, reused across all rows. The WASM memory is
/// pre-allocated for single-row evaluation, and parameter values (memory
/// offsets) are precomputed.
pub struct CompiledExprSession {
    store: wasmtime::Store<()>,
    memory: wasmtime::Memory,
    eval_func: wasmtime::Func,
    /// Precomputed WASM function parameters (num_rows=1, column pointers, output pointers).
    params: Vec<wasmtime::Val>,
    /// Memory offsets for input columns: (values_ptr, validity_ptr) per input column.
    input_col_offsets: Vec<(usize, usize)>,
    output_values_ptr: usize,
    output_validity_ptr: usize,
    output_errors_ptr: usize,
    /// Original column indices this expression reads, in WASM parameter order.
    input_columns: Vec<usize>,
}

impl CompiledExprSession {
    /// Creates a new session from a compiled expression.
    ///
    /// Allocates WASM memory for single-row evaluation and instantiates
    /// the WASM module. The instance is reused across all `eval` calls.
    pub fn new(compiled: &CompiledExpr) -> Result<Self, wasmtime::Error> {
        let engine = compiled.module().engine().clone();
        let mut store = wasmtime::Store::new(&engine, ());

        // For 1 row: each input col = 8 bytes (value) + 1 byte (validity).
        // Output = 8 bytes (value) + 1 byte (validity) + 1 byte (error).
        let num_input_cols = compiled.layout().num_input_cols;
        let total_bytes = num_input_cols * 9 + 10;
        let num_pages = (total_bytes + 65535) / 65536;

        let memory_type = wasmtime::MemoryType::new(u32::try_from(num_pages).unwrap_or(1), None);
        let memory = wasmtime::Memory::new(&mut store, memory_type)?;

        // Compute fixed memory layout offsets for 1 row.
        let mut offset = 0usize;
        let mut input_col_offsets = Vec::with_capacity(num_input_cols);
        for _ in 0..num_input_cols {
            let values_ptr = offset;
            offset += 8; // 1 row * 8 bytes per i64
            let validity_ptr = offset;
            offset += 1; // 1 row * 1 byte
            input_col_offsets.push((values_ptr, validity_ptr));
        }
        let output_values_ptr = offset;
        offset += 8;
        let output_validity_ptr = offset;
        offset += 1;
        let output_errors_ptr = offset;

        // Build precomputed parameter values.
        let mut params = Vec::with_capacity(1 + num_input_cols * 2 + 3);
        params.push(wasmtime::Val::I32(1)); // num_rows = 1
        #[allow(clippy::as_conversions)]
        for &(vp, validp) in &input_col_offsets {
            params.push(wasmtime::Val::I32(vp as i32));
            params.push(wasmtime::Val::I32(validp as i32));
        }
        #[allow(clippy::as_conversions)]
        {
            params.push(wasmtime::Val::I32(output_values_ptr as i32));
            params.push(wasmtime::Val::I32(output_validity_ptr as i32));
            params.push(wasmtime::Val::I32(output_errors_ptr as i32));
        }

        // Instantiate the WASM module.
        let instance = wasmtime::Instance::new(&mut store, compiled.module(), &[memory.into()])?;
        let eval_func = instance
            .get_func(&mut store, "eval")
            .expect("eval function must exist in compiled module");

        Ok(Self {
            store,
            memory,
            eval_func,
            params,
            input_col_offsets,
            output_values_ptr,
            output_validity_ptr,
            output_errors_ptr,
            input_columns: compiled.input_columns().to_vec(),
        })
    }

    /// Evaluates the compiled expression on a single row's datums.
    ///
    /// Writes input values into WASM memory, calls the compiled function,
    /// and reads the result. Returns `Datum::Int64`, `Datum::Null`, or
    /// an `EvalError` (e.g., numeric overflow).
    pub fn eval<'a>(&mut self, datums: &[Datum<'a>]) -> Result<Datum<'static>, EvalError> {
        // Write input datum values to WASM memory.
        {
            let mem_data = self.memory.data_mut(&mut self.store);
            for (param_idx, &col_idx) in self.input_columns.iter().enumerate() {
                let (values_ptr, validity_ptr) = self.input_col_offsets[param_idx];
                match datums[col_idx] {
                    Datum::Int64(v) => {
                        mem_data[values_ptr..values_ptr + 8].copy_from_slice(&v.to_le_bytes());
                        mem_data[validity_ptr] = 1;
                    }
                    Datum::Null => {
                        mem_data[values_ptr..values_ptr + 8].copy_from_slice(&0i64.to_le_bytes());
                        mem_data[validity_ptr] = 0;
                    }
                    _ => {
                        return Err(EvalError::Internal(
                            "unsupported datum type for compiled WASM eval"
                                .to_string()
                                .into_boxed_str(),
                        ));
                    }
                }
            }
        }

        // Call the WASM eval function.
        let mut results = [];
        self.eval_func
            .call(&mut self.store, &self.params, &mut results)
            .map_err(|e| EvalError::Internal(format!("WASM eval failed: {e}").into_boxed_str()))?;

        // Read the result from WASM memory.
        let mem_data = self.memory.data(&self.store);
        let result_bytes: [u8; 8] = mem_data[self.output_values_ptr..self.output_values_ptr + 8]
            .try_into()
            .expect("8 bytes");
        let result_value = i64::from_le_bytes(result_bytes);
        let is_valid = mem_data[self.output_validity_ptr] != 0;
        let error_code = mem_data[self.output_errors_ptr];

        if error_code != 0 {
            Err(match error_code {
                1 => EvalError::NumericFieldOverflow,
                2 => EvalError::DivisionByZero,
                3 => EvalError::Int64OutOfRange("integer out of range".into()),
                _ => EvalError::Internal(
                    format!("unknown WASM error code: {error_code}").into_boxed_str(),
                ),
            })
        } else if !is_valid {
            Ok(Datum::Null)
        } else {
            Ok(Datum::Int64(result_value))
        }
    }
}

/// A compiled MFP that evaluates expressions via WASM where possible.
///
/// Wraps an [`MfpPlan`] and adds compiled WASM sessions for expressions that
/// were successfully compiled. Predicates, temporal bounds, and projection
/// remain interpreted.
pub struct CompiledMfp {
    /// One entry per expression in the MFP. `Some` = compiled, `None` = interpreted fallback.
    compiled_sessions: Vec<Option<CompiledExprSession>>,
    /// The underlying plan, used for predicates, temporal bounds, and projection.
    plan: MfpPlan,
}

impl CompiledMfp {
    /// Attempts to create a compiled MFP from the given plan.
    ///
    /// Returns `Ok(Self)` if at least one expression was compiled to WASM.
    /// Returns `Err(plan)` if no expressions could be compiled, returning
    /// ownership of the plan for interpreted fallback.
    pub fn try_new(plan: MfpPlan) -> Result<Self, MfpPlan> {
        let engine = match ExprEngine::new() {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("failed to create WASM engine: {e}");
                return Err(plan);
            }
        };

        let mfp: &MapFilterProject = plan.non_temporal();
        let mut compiled_sessions = Vec::with_capacity(mfp.expressions.len());
        let mut any_compiled = false;

        for expr in &mfp.expressions {
            if crate::analyze::is_compilable(expr) {
                let input_types = vec![];
                match engine.compile(expr, &input_types) {
                    Ok(Some(compiled)) => match CompiledExprSession::new(&compiled) {
                        Ok(session) => {
                            compiled_sessions.push(Some(session));
                            any_compiled = true;
                            continue;
                        }
                        Err(e) => {
                            tracing::debug!("WASM session creation failed: {e}");
                        }
                    },
                    Ok(None) => {}
                    Err(e) => {
                        tracing::debug!("WASM compilation failed for expression: {e}");
                    }
                }
            }
            compiled_sessions.push(None);
        }

        if !any_compiled {
            return Err(plan);
        }

        let compiled_count = compiled_sessions.iter().filter(|s| s.is_some()).count();
        let total = compiled_sessions.len();
        tracing::debug!("compiled MFP: {compiled_count}/{total} expressions compiled to WASM");

        Ok(Self {
            compiled_sessions,
            plan,
        })
    }

    /// Evaluates the MFP on the given datums, returning results as a `Vec`.
    ///
    /// Mirrors [`MfpPlan::evaluate`] but uses WASM for compiled expressions.
    /// Predicates, temporal bounds, and projection remain interpreted.
    pub fn evaluate<'b, 'a: 'b, E: From<EvalError>, V: Fn(&mz_repr::Timestamp) -> bool>(
        &'a mut self,
        datums: &'b mut Vec<Datum<'a>>,
        arena: &'a RowArena,
        time: mz_repr::Timestamp,
        diff: Diff,
        valid_time: V,
        row_builder: &mut Row,
    ) -> Vec<Result<(Row, mz_repr::Timestamp, Diff), (E, mz_repr::Timestamp, Diff)>> {
        // Evaluate expressions and predicates (compiled + interpreted).
        match Self::evaluate_inner(
            &mut self.compiled_sessions,
            self.plan.non_temporal(),
            datums,
            arena,
        ) {
            Err(e) => {
                return vec![Err((e.into(), time, diff))];
            }
            Ok(true) => {}
            Ok(false) => {
                return vec![];
            }
        }

        // Lower and upper temporal bounds.
        let mut lower_bound = time;
        let mut upper_bound = None;

        // Track whether we have seen a null in either bound.
        let mut null_eval = false;

        for l in self.plan.lower_bounds() {
            match l.eval(datums, arena) {
                Err(e) => {
                    return vec![Err((e.into(), time, diff))];
                }
                Ok(Datum::MzTimestamp(d)) => {
                    lower_bound = lower_bound.max(d);
                }
                Ok(Datum::Null) => {
                    null_eval = true;
                }
                x => {
                    panic!("Non-mz_timestamp value in temporal predicate: {:?}", x);
                }
            }
        }

        // If the lower bound exceeds our `until` frontier, suppress output.
        if !valid_time(&lower_bound) {
            return vec![];
        }

        for u in self.plan.upper_bounds() {
            if upper_bound != Some(lower_bound) {
                match u.eval(datums, arena) {
                    Err(e) => {
                        return vec![Err((e.into(), time, diff))];
                    }
                    Ok(Datum::MzTimestamp(d)) => {
                        if let Some(upper) = upper_bound {
                            upper_bound = Some(upper.min(d));
                        } else {
                            upper_bound = Some(d);
                        };
                        if upper_bound.is_some() && upper_bound < Some(lower_bound) {
                            upper_bound = Some(lower_bound);
                        }
                    }
                    Ok(Datum::Null) => {
                        null_eval = true;
                    }
                    x => {
                        panic!("Non-mz_timestamp value in temporal predicate: {:?}", x);
                    }
                }
            }
        }

        // If the upper bound exceeds our `until` frontier, suppress it.
        if let Some(upper) = &mut upper_bound {
            if !valid_time(upper) {
                upper_bound = None;
            }
        }

        // Produce output if upper bound exceeds lower bound and no null was seen.
        if Some(lower_bound) != upper_bound && !null_eval {
            let mfp: &MapFilterProject = self.plan.non_temporal();
            row_builder
                .packer()
                .extend(mfp.projection.iter().map(|c| datums[*c]));
            let mut results = Vec::with_capacity(2);
            results.push(Ok((row_builder.clone(), lower_bound, diff)));
            if let Some(upper_bound) = upper_bound {
                results.push(Ok((row_builder.clone(), upper_bound, -diff)));
            }
            results
        } else {
            vec![]
        }
    }

    /// Evaluates expressions and predicates, appending results to datums.
    ///
    /// Mirrors [`SafeMfpPlan::evaluate_inner`] but uses WASM for compiled
    /// expressions and falls back to the interpreter for the rest.
    fn evaluate_inner<'b, 'a: 'b>(
        sessions: &mut [Option<CompiledExprSession>],
        mfp: &'a MapFilterProject,
        datums: &'b mut Vec<Datum<'a>>,
        arena: &'a RowArena,
    ) -> Result<bool, EvalError> {
        let mut expression = 0;
        for (support, predicate) in mfp.predicates.iter() {
            while mfp.input_arity + expression < *support {
                let datum = if let Some(ref mut session) = sessions[expression] {
                    session.eval(datums)?
                } else {
                    mfp.expressions[expression].eval(&datums[..], arena)?
                };
                datums.push(datum);
                expression += 1;
            }
            if predicate.eval(&datums[..], arena)? != Datum::True {
                return Ok(false);
            }
        }
        while expression < mfp.expressions.len() {
            let datum = if let Some(ref mut session) = sessions[expression] {
                session.eval(datums)?
            } else {
                mfp.expressions[expression].eval(&datums[..], arena)?
            };
            datums.push(datum);
            expression += 1;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{BinaryFunc, MapFilterProject, MirScalarExpr};
    use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row, RowArena};

    fn col(idx: usize) -> MirScalarExpr {
        MirScalarExpr::Column(idx, Default::default())
    }

    fn lit_i64(v: i64) -> MirScalarExpr {
        MirScalarExpr::Literal(
            Ok(Row::pack_slice(&[Datum::Int64(v)])),
            ReprColumnType {
                scalar_type: ReprScalarType::Int64,
                nullable: false,
            },
        )
    }

    fn add_i64(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallBinary {
            func: BinaryFunc::AddInt64(mz_expr::func::AddInt64),
            expr1: Box::new(a),
            expr2: Box::new(b),
        }
    }

    #[mz_ore::test]
    fn test_session_simple_add() {
        let engine = ExprEngine::new().unwrap();
        let expr = add_i64(col(0), col(1));
        let input_types = vec![];
        let compiled = engine.compile(&expr, &input_types).unwrap().unwrap();
        let mut session = CompiledExprSession::new(&compiled).unwrap();

        let result = session.eval(&[Datum::Int64(1), Datum::Int64(2)]).unwrap();
        assert_eq!(result, Datum::Int64(3));

        let result = session.eval(&[Datum::Int64(10), Datum::Int64(20)]).unwrap();
        assert_eq!(result, Datum::Int64(30));
    }

    #[mz_ore::test]
    fn test_session_null_propagation() {
        let engine = ExprEngine::new().unwrap();
        let expr = add_i64(col(0), col(1));
        let input_types = vec![];
        let compiled = engine.compile(&expr, &input_types).unwrap().unwrap();
        let mut session = CompiledExprSession::new(&compiled).unwrap();

        let result = session.eval(&[Datum::Null, Datum::Int64(2)]).unwrap();
        assert_eq!(result, Datum::Null);

        let result = session.eval(&[Datum::Int64(1), Datum::Null]).unwrap();
        assert_eq!(result, Datum::Null);
    }

    #[mz_ore::test]
    fn test_session_with_literal() {
        let engine = ExprEngine::new().unwrap();
        let expr = add_i64(col(0), lit_i64(100));
        let input_types = vec![];
        let compiled = engine.compile(&expr, &input_types).unwrap().unwrap();
        let mut session = CompiledExprSession::new(&compiled).unwrap();

        let result = session.eval(&[Datum::Int64(42)]).unwrap();
        assert_eq!(result, Datum::Int64(142));
    }

    #[mz_ore::test]
    fn test_session_reuse_across_rows() {
        let engine = ExprEngine::new().unwrap();
        let expr = add_i64(col(0), col(1));
        let input_types = vec![];
        let compiled = engine.compile(&expr, &input_types).unwrap().unwrap();
        let mut session = CompiledExprSession::new(&compiled).unwrap();

        for i in 0..100 {
            let result = session
                .eval(&[Datum::Int64(i), Datum::Int64(i * 2)])
                .unwrap();
            assert_eq!(result, Datum::Int64(i + i * 2));
        }
    }

    #[mz_ore::test]
    fn test_compiled_mfp_evaluate_inner_simple() {
        // MFP with one expression: col(0) + col(1), no predicates
        let mfp = MapFilterProject {
            expressions: vec![add_i64(col(0), col(1))],
            predicates: vec![],
            projection: vec![0, 1, 2],
            input_arity: 2,
        };
        let plan = mfp.into_plan().unwrap();
        let mut compiled = CompiledMfp::try_new(plan).unwrap();

        let arena = RowArena::new();
        let mut datums: Vec<Datum<'_>> = vec![Datum::Int64(3), Datum::Int64(4)];
        let result = CompiledMfp::evaluate_inner(
            &mut compiled.compiled_sessions,
            compiled.plan.non_temporal(),
            &mut datums,
            &arena,
        )
        .unwrap();

        assert!(result);
        assert_eq!(datums.len(), 3);
        assert_eq!(datums[2], Datum::Int64(7));
    }

    #[mz_ore::test]
    fn test_compiled_mfp_evaluate_nontemporal() {
        // MFP: map col(0) + col(1), project [2]
        let mfp = MapFilterProject {
            expressions: vec![add_i64(col(0), col(1))],
            predicates: vec![],
            projection: vec![2],
            input_arity: 2,
        };
        let plan = mfp.into_plan().unwrap();
        let mut compiled = CompiledMfp::try_new(plan).unwrap();

        let arena = RowArena::new();
        let mut datums: Vec<Datum<'_>> = vec![Datum::Int64(5), Datum::Int64(10)];
        let mut row_builder = Row::default();
        let results: Vec<Result<(Row, _, _), (EvalError, _, _)>> = compiled.evaluate(
            &mut datums,
            &arena,
            0u64.into(),
            Diff::from(1),
            |_| true,
            &mut row_builder,
        );

        assert_eq!(results.len(), 1);
        let (row, _, _) = results[0].as_ref().unwrap();
        assert_eq!(row.unpack_first(), Datum::Int64(15));
    }

    #[mz_ore::test]
    fn test_compiled_mfp_mixed_expressions() {
        // MFP with two expressions: one compilable (add), one not (coalesce).
        // Only the add should be compiled; coalesce falls back to interpreter.
        let add_expr = add_i64(col(0), col(1));
        // A non-compilable expression that just wraps a column reference.
        let coalesce_expr = MirScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce),
            exprs: vec![col(0)],
        };

        let mfp = MapFilterProject {
            expressions: vec![add_expr, coalesce_expr],
            predicates: vec![],
            projection: vec![2, 3],
            input_arity: 2,
        };
        let plan = mfp.into_plan().unwrap();
        let mut compiled = CompiledMfp::try_new(plan).unwrap();

        // Verify first session is compiled, second is not.
        assert!(compiled.compiled_sessions[0].is_some());
        assert!(compiled.compiled_sessions[1].is_none());

        let arena = RowArena::new();
        let mut datums: Vec<Datum<'_>> = vec![Datum::Int64(3), Datum::Int64(7)];
        let mut row_builder = Row::default();
        let results: Vec<Result<(Row, _, _), (EvalError, _, _)>> = compiled.evaluate(
            &mut datums,
            &arena,
            0u64.into(),
            Diff::from(1),
            |_| true,
            &mut row_builder,
        );

        assert_eq!(results.len(), 1);
        let (row, _, _) = results[0].as_ref().unwrap();
        let unpacked: Vec<_> = row.iter().collect();
        assert_eq!(unpacked[0], Datum::Int64(10)); // 3 + 7
        assert_eq!(unpacked[1], Datum::Int64(3)); // coalesce(3) = 3
    }

    #[mz_ore::test]
    fn test_compiled_mfp_no_compilable_expressions() {
        // MFP with only non-compilable expressions returns Err(plan).
        let coalesce_expr = MirScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce),
            exprs: vec![col(0)],
        };
        let mfp = MapFilterProject {
            expressions: vec![coalesce_expr],
            predicates: vec![],
            projection: vec![0, 1],
            input_arity: 1,
        };
        let plan = mfp.into_plan().unwrap();
        let result = CompiledMfp::try_new(plan);
        assert!(result.is_err());
    }
}
