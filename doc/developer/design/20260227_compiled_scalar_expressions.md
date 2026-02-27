# WASM-compiled scalar expression evaluation

## The problem

Materialize evaluates `MirScalarExpr` trees by tree-walking interpretation.
Each node dispatches through match arms, unpacks and repacks `Datum` values, and recurses — per row, per expression, per operator.
For tight per-row evaluation in `MapFilterProject`, this overhead dominates execution time relative to the actual arithmetic or comparison.

Compiling expressions to native code via WebAssembly eliminates the per-node dispatch and datum packing overhead.
The WASM approach also enables batch evaluation, where a single WASM call processes thousands of rows in a loop over columnar memory.

## Success criteria

* Expressions composed of Int64 arithmetic, comparisons, boolean logic, and null handling compile to WASM and produce results identical to the interpreter.
* Per-row compiled evaluation is competitive with interpretation (within 2x).
* Batch compiled evaluation is at least 5x faster than interpretation for supported expressions.
* The system falls back gracefully to interpretation for any expression that cannot be compiled.

## Out of scope

* Non-Int64 types (String, Float64, Timestamp, etc.).
* `MirScalarExpr::If` (conditional/lazy evaluation).
* Aggregations or join logic.
* Replacing the interpreter entirely — this is an acceleration layer for a subset of expressions.

## Solution proposal

### Architecture

The `mz-expr-compiler` crate has five modules:

```
analyze  → determines if an expression tree is compilable, infers types
codegen  → emits WASM bytecode via wasm-encoder
engine   → compiles modules via wasmtime, provides a stateless evaluation API
columnar → converts between Row format and columnar arrays
eval     → per-row (CompiledExprSession) and MFP-level (CompiledMfp) evaluation
```

#### Analysis (`analyze.rs`)

`is_compilable(expr, input_types)` recursively checks whether every node in the expression tree maps to a supported WASM operation.
`infer_type(expr, input_types)` returns the output `SqlScalarType`.
`referenced_columns(expr)` collects the set of column indices read by the expression.

Supported operations:

| Category | Operations |
|---|---|
| Int64 arithmetic | Add, Sub, Mul, Div, Mod |
| Int64 bitwise | BitAnd, BitOr, BitXor, BitNot |
| Int64 unary | Neg, Abs |
| Comparisons (Int64 inputs) | Eq, NotEq, Lt, Lte, Gt, Gte |
| Boolean logic | And, Or, Not |
| Null handling | IsNull, IsTrue, IsFalse |

`infer_input_types_from_mfp(mfp)` scans all expressions and predicates in a `MapFilterProject` to infer column types from operation context.
For example, `AddInt64(col(0), col(1))` constrains both columns to Int64.
This enables compilation at the compute layer call site, which does not carry explicit column type information.

#### Code generation (`codegen.rs`)

`generate_wasm(expr, input_types)` produces a WASM module using `wasm-encoder`.
The module imports a host-provided linear memory and exports a single `eval(num_rows, col0_ptr, col0_valid_ptr, ..., out_ptr, out_valid_ptr, err_ptr)` function.

Memory layout is column-major:
* Each input column: `num_rows * 8` bytes for i64 values, then `num_rows` bytes for validity flags.
* Output: `num_rows * 8` bytes for values, `num_rows` bytes for validity, `num_rows` bytes for error codes.

The generated function contains a row loop that:
1. Reads input values and validity flags from column arrays.
2. Evaluates the expression tree using WASM stack operations.
3. Writes the result value, validity, and error code to the output arrays.

Null propagation follows SQL semantics.
Arithmetic operations propagate nulls — if either operand is null, the result is null.
And/Or implement three-valued logic: `FALSE AND NULL = FALSE`, `TRUE OR NULL = TRUE`.
`IsNull`, `IsTrue`, `IsFalse` consume nulls (their output is never null).
Overflow detection uses widening multiplication and explicit range checks.

Error handling uses per-row error codes: 0=ok, 1=overflow, 2=division-by-zero, 3=out-of-range.

#### Engine (`engine.rs`)

`ExprEngine` owns a shared `wasmtime::Engine` and provides `compile(expr, input_types) -> CompiledExpr`.
`CompiledExpr::evaluate(datums)` creates a fresh WASM instance per call — suitable for one-off evaluation but not for hot loops.

#### Evaluation (`eval.rs`)

`CompiledExprSession` caches a WASM instance (store, memory, function handle) and reuses it across calls.
It provides two evaluation modes:

`eval(datums)` — per-row evaluation.
Writes a single row's datums into WASM memory, calls the function with `num_rows=1`, reads back one result.

`eval_batch(datums_per_row)` — batch evaluation.
Transposes row-major datums into column-major WASM memory, calls the function with `num_rows=N`, reads back N results.
Memory is grown automatically via `memory.grow()` and reused across calls.

`CompiledMfp` wraps an `MfpPlan` and replaces individual expressions and predicates with compiled sessions where possible.
Expressions that fail compilation fall through to the interpreter.
The MFP evaluation loop calls compiled sessions for supported expressions and delegates to `MfpPlan::evaluate_into` for the rest.

### Integration

In `src/compute/src/render/context.rs`, the `MfpEvaluator` enum dispatches between interpreted and compiled paths:

```rust
enum MfpEvaluator {
    Interpreted(MfpPlan),
    Compiled(CompiledMfp),
}
```

At operator construction time, input types are inferred from the MFP and compilation is attempted:

```rust
let input_types = infer_input_types_from_mfp(mfp_plan.non_temporal());
match CompiledMfp::try_new(mfp_plan, &input_types) {
    Ok(compiled) => MfpEvaluator::Compiled(compiled),
    Err(plan) => MfpEvaluator::Interpreted(plan),
}
```

### WASM local variables

The generated function uses 10 local variables beyond the function parameters:

| Local | Type | Purpose |
|---|---|---|
| `i` | i32 | Row loop counter |
| `result_val` | i64 | Expression result value |
| `is_null` | i32 | Null flag for current subexpression |
| `is_error` | i32 | Error code (0=ok) |
| `operand_a` | i64 | Saved operand for overflow detection |
| `operand_b` | i64 | Saved operand for overflow detection |
| `saved_null` | i32 | Saved null flag across subtrees |
| `any_dominant` | i32 | And/Or: tracks dominant value (FALSE for And, TRUE for Or) |
| `any_null` | i32 | And/Or: tracks whether any child was null |
| `saved_caller_null` | i32 | And/Or: saves outer null flag for nesting |

## Benchmark results

Benchmarks use criterion with 7 scenarios at three batch sizes (100, 1000, 10000 rows).
Three evaluation modes are compared: interpreted (full MFP), compiled per-row (full MFP), and compiled batch (single expression, single WASM call).

Representative results at 10,000 rows:

| Scenario | Interpreted | Compiled per-row | Compiled batch |
|---|---|---|---|
| `add_two_cols` | 137 µs | 260 µs | 16 µs |
| `arith_chain` | 237 µs | 419 µs | 14 µs |
| `where_gt_42` | 89 µs | 225 µs | 12 µs |
| `complex_bool` | 202 µs | 373 µs | 16 µs |

Per-row compiled is ~2x slower than interpreted, due to the overhead of crossing the WASM boundary per row and transposing each datum individually.
Batch compiled is 8-17x faster than interpreted and 15-40x faster than per-row compiled.

### Profiling breakdown

Profiling the batch path (perf record + annotate) at 10,000 rows shows:
* ~66% of time: building the `Vec<Result<Datum, EvalError>>` result vector (56-byte Result structs, per-row branching on error/null/valid).
* ~34% of time: writing input data (per-row datum matching, column-major memory writes).
* ~0% of time: WASM execution itself.

The bottleneck is entirely in Rust-side data marshalling, not WASM computation.

## Alternatives

**Cranelift JIT directly (without WASM).**
Would avoid the WASM abstraction layer and linear memory marshalling.
However, `wasmtime` already uses Cranelift internally, provides memory sandboxing for free, and offers a stable API.
The WASM approach also enables future possibilities like shipping precompiled modules across nodes.

**Vectorized Rust evaluation (no WASM).**
Evaluate expressions in Rust using columnar arrays (like Arrow kernels).
Would avoid WASM marshalling overhead entirely.
However, this requires writing and maintaining specialized kernels for each operation, losing the composability of expression-tree compilation.

**Arrow/DataFusion integration.**
Materialize's internal representation uses `Row`/`Datum`, not Arrow arrays.
Converting to Arrow for evaluation and back would add its own marshalling cost.
A native approach that works directly with the existing data model has lower integration friction.

## Open questions

* **Columnar input path.** The current batch interface accepts `&[&[Datum]]` (row-major) and transposes to column-major in Rust.
  Accepting columnar input directly would eliminate the 34% input-writing overhead.
  This requires changes upstream where data enters the MFP evaluation loop.
* **Columnar output path.** Returning columnar results (separate value/validity/error arrays) instead of `Vec<Result<Datum, EvalError>>` would eliminate the 66% result-building overhead.
  The downstream consumer would need to accept columnar data.
* **Type coverage.** Adding Int32, Float64, and String support would cover a larger fraction of real-world expressions.
  String operations (comparison, concatenation) require variable-length data in WASM memory.
* **MFP-level batch evaluation.** Currently batch evaluation applies to single expressions.
  Compiling an entire MFP (map + filter + project) into one WASM module would avoid multiple passes over the data.
