# Columnar Rendering: Incremental Migration Plan

## Overview

Convert Materialize's rendering layer from row-first (`Vec<(Row, T, Diff)>`) to column-first
representation using the `columnar` crate. The goal is to reduce pointer chasing, improve cache
locality, and enable vectorized scalar evaluation.

### Architecture

**Current state**: Data flows as `VecCollection<S, Row, Diff>` between operators. Each `Row` is a
separate allocation. Arrangements use `DatumContainer` (dictionary-compressed row bytes).
`CollectionBundle` wraps both unarranged collections and arrangements keyed by expression.

**Target state**: Unarranged edges carry columnar containers (`Column<(Row, T, Diff)>` or a
future column-of-datums layout). Arrangements remain as-is initially (they already use
`DatumContainer`). Operators that apply scalar expressions can optionally use vectorized
evaluation when data is in columnar form.

### Constraints

- Each step must compile cleanly (`bin/lint`, `cargo clippy`, `cargo test`).
- Do not modify `src/storage` unless absolutely needed.
- Arrangements (Row→Row spines) are already reasonably efficient; focus on unarranged edges first.
- The vectorized evaluation PR (#35464) defines `ColumnDatum`/`DatumColumn` as the evaluation-time
  layout. We don't need the same layout for edges, but conversion must be cheap.

### Key types

| Type | Location | Role |
|------|----------|------|
| `CollectionBundle<S, T>` | `render/context.rs` | Edge between operators: optional collection + arrangements |
| `ArrangementFlavor<S, T>` | `render/context.rs` | Local or imported arrangement (RowRow spine) |
| `VecCollection<S, Row, Diff>` | differential_dataflow | Unarranged data stream (row-first) |
| `Column<C>` | `timely-util/src/columnar.rs` | Columnar container (typed/bytes/aligned) |
| `Col2ValBatcher` | `timely-util/src/columnar.rs` | Columnar merge batcher |
| `ColumnBuilder` | `timely-util/src/columnar/builder.rs` | Builds columnar containers |
| `ColumnDatum` / `DatumColumn` | PR #35464 `expr/src/vectorized.rs` | Vectorized eval layout |

---

## Phase 0: Foundation

### Prompt 0.1: Introduce `ColumnarCollection` type alias and `ColumnarBundle`

[*] Introduce a type alias `ColumnarCollection<S, D, R>` for a collection backed by columnar
containers (`StreamCore<S, Column<(D, S::Timestamp, R)>>` or equivalent).

[*] Add a `columnar_collection` field to `CollectionBundle<S, T>` alongside the existing
`collection` field. Initially always `None`.

[*] Add helper methods: `from_columnar_collections(oks, errs)` and `columnar_collection()` that
return the columnar variant if present, falling back to converting the Vec variant.

[*] Add a method `ensure_vec_collection()` that materializes the `VecCollection` from the columnar
collection if needed (the "escape hatch" for operators not yet converted).

[*] Ensure all existing code continues to compile and pass tests unchanged. The new field is
always `None` at this point.

**Files**: `src/compute/src/render/context.rs`, `src/compute/src/typedefs.rs`

---

### Prompt 0.2: Columnar ↔ Vec conversion utilities

[*] Implement `vec_to_columnar` and `columnar_to_vec` stream operators that convert between
`VecCollection<S, Row, Diff>` and the columnar equivalent.

[*] The `vec_to_columnar` operator should batch rows into columnar containers using
`ColumnBuilder`. Use a configurable batch size (default 1024 or container-size-driven).

[*] The `columnar_to_vec` operator should iterate the columnar container and emit individual
`(Row, T, Diff)` tuples.

[*] Add unit tests that round-trip data through both conversions.

**Files**: `src/compute/src/render/context.rs` or a new `src/compute/src/render/columnar.rs`

---

## Phase 1: Source ingestion as columnar

### Prompt 1.1: Persist source emits columnar collections

[*] Investigate `persist_source::persist_source()` in `src/storage-operators/src/persist_source.rs`.
It already produces batches of rows. Determine whether it can emit `Column<(Row, T, Diff)>`
directly or whether a `vec_to_columnar` conversion at the boundary is more practical.

[*] If the persist source already produces columnar-friendly batches (it uses `ColumnBuilder` in
PR #35464), wire the columnar output into the `imported_sources` in `render.rs` so that
`CollectionBundle` carries the columnar collection.

[*] Ensure that all downstream operators still work by having `ensure_vec_collection()` as the
fallback. Run tests.

**Files**: `src/compute/src/render.rs`, `src/storage-operators/src/persist_source.rs` (read-only
if possible)

---

## Phase 2: Simple pass-through operators

### Prompt 2.1: Negate

[*] Convert the `Negate` operator to propagate columnar collections. Negation flips the sign of
`Diff`, which can be done in-place on a columnar container without unpacking rows.

[*] If the input `CollectionBundle` has a columnar collection, produce a columnar output. Otherwise
fall back to the existing Vec path.

**Files**: `src/compute/src/render.rs` (the Negate match arm in `render_plan_expr`)

---

### Prompt 2.2: Union

[*] Convert the `Union` operator to propagate columnar collections. Union concatenates streams,
which works identically for columnar containers.

[*] If all inputs have columnar collections, produce a columnar output. If some inputs are Vec,
either convert them or fall back to Vec for all.

**Files**: `src/compute/src/render.rs` (the Union match arm)

---

### Prompt 2.3: Constant

[*] Convert the `Constant` operator to emit columnar collections. Constants are small, so this
is primarily for uniformity.

[*] Pack the constant rows into a columnar container and set the `columnar_collection` field.

**Files**: `src/compute/src/render.rs` (the Constant match arm)

---

## Phase 3: MFP (Map/Filter/Project)

### Prompt 3.1: Columnar `as_collection_core`

[*] This is the core method that applies `MapFilterProject` to collections or arrangements and
produces a `VecCollection`. Add a columnar variant `as_columnar_collection_core` that:
  - Accepts columnar input
  - For now, converts to Vec internally and applies the existing MfpPlan row-at-a-time
  - Returns a columnar collection
  - This is the incremental step; vectorized eval comes later

[*] Wire `render_plan_expr` for `Get` and `Mfp` to prefer the columnar path when available.

**Files**: `src/compute/src/render/context.rs`

---

### Prompt 3.2: Vectorized MFP evaluation on columnar data

[ ] Integrate the vectorized evaluation from PR #35464 into `as_columnar_collection_core`.
When the `MfpPlan` is suitable for vectorized evaluation (non-temporal, supported expression
types):
  - Convert columnar `Row` batches to `Vec<DatumColumn>` (the `rows_to_columns` function)
  - Evaluate using `MfpPlan::evaluate_batch`
  - Convert results back to columnar `Row` containers

[ ] Add a dyncfg flag to enable/disable vectorized evaluation, defaulting to off.

[ ] Keep the row-at-a-time path as fallback for unsupported expressions.

**Files**: `src/compute/src/render/context.rs`, `src/expr/src/vectorized.rs`

---

## Phase 4: FlatMap

### Prompt 4.1: Columnar FlatMap

[*] Convert `render_flat_map` to accept columnar input. Since `FlatMap` applies table functions
that can produce variable numbers of output rows per input row, the output is naturally a
stream and may not benefit from columnar representation on the output side.

[*] Accept columnar input, convert batch-at-a-time for table function evaluation, and emit
columnar output if practical.

**Files**: `src/compute/src/render/flat_map.rs`

---

## Phase 5: Arrangement creation (ArrangeBy)

### Prompt 5.1: Columnar input to arrangements

[ ] The `ensure_collections` method in `CollectionBundle` arranges data by key expressions. It
currently calls `as_specific_collection` to get a `VecCollection` and then arranges it.

[ ] When columnar input is available, use it to feed into arrangement creation. The arrangement
spines (`RowRowSpine`) use `DatumContainer` which accepts `Row` input, so we need to unpack
columnar → Row at this boundary.

[ ] This is primarily a plumbing step: accept columnar, unpack to Row for arrangement.

**Files**: `src/compute/src/render/context.rs`, `src/compute/src/extensions/arrange.rs`

---

## Phase 6: Stateful operators

### Prompt 6.1: Columnar Reduce input

[ ] `render_reduce` currently calls `flat_map` to selectively unpack demanded columns and
evaluate key/value plans. Convert to accept columnar input.

[ ] The reduce operator must create arrangements from its output. The key extraction and
aggregation logic operates row-at-a-time for now; the columnar input is unpacked at the
operator boundary.

**Files**: `src/compute/src/render/reduce.rs`

---

### Prompt 6.2: Columnar TopK input

[ ] `render_top_k` calls `as_specific_collection` to get a Vec collection. Convert to accept
columnar input, unpacking at the operator boundary.

**Files**: `src/compute/src/render/top_k.rs`

---

### Prompt 6.3: Columnar Threshold input

[ ] Threshold works directly on arrangements (`arrangement(&key)`), not on unarranged
collections. No changes needed for the main path.

[ ] Verify that threshold continues to work when upstream operators produce columnar collections
(it should, since it only uses arrangements).

**Files**: `src/compute/src/render/threshold.rs` (verification only)

---

## Phase 7: Joins

### Prompt 7.1: Columnar Linear Join input

[ ] `render_linear_join` gets its initial input via `as_specific_collection`. Convert to accept
columnar input for the initial collection.

[ ] Join stages operate on arrangements, which remain unchanged. The initial closure application
and the final output can use columnar.

[ ] Update `LinearJoinImpl` to support columnar output accumulation.

**Files**: `src/compute/src/render/join/linear_join.rs`

---

### Prompt 7.2: Columnar Delta Join input

[ ] Delta joins build update streams from arrangements. The join output is accumulated in
collections. Convert the output path to produce columnar collections.

[ ] The join closure application happens per-result-tuple and may not benefit from columnar
within the join itself, but the output stream should be columnar.

**Files**: `src/compute/src/render/join/delta_join.rs`

---

## Phase 8: Sinks and exports

### Prompt 8.1: Columnar sink input

[ ] Sinks receive data via `as_collection_core`. Convert to accept columnar input.

[ ] The sink export boundary (writing to persist) may need Row-format data. Unpack columnar
at this boundary.

**Files**: `src/compute/src/render/sinks.rs`

---

## Phase 9: Remove Vec fallbacks

### Prompt 9.1: Audit and remove dead Vec paths

[ ] Once all operators produce and consume columnar collections, the `collection` field in
`CollectionBundle` (the Vec variant) should be unused for the unarranged data path.

[ ] Add metrics/logging to track how often the Vec fallback is used.

[ ] Gradually remove the Vec paths, starting with operators where the columnar path is proven
stable.

**Files**: All render files

---

### Prompt 9.2: Remove `ensure_vec_collection` calls

[ ] Once all operators are converted, `ensure_vec_collection` should have zero callers.
Remove it and the `collection` field from `CollectionBundle`.

**Files**: `src/compute/src/render/context.rs`, all render files

---

## Phase 10: Columnar-native arrangements (future)

### Prompt 10.1: Investigate columnar arrangement spines

[ ] The current `RowRowSpine` uses `DatumContainer` for dictionary-compressed row bytes. This is
already reasonably cache-friendly but does not enable vectorized access by column.

[ ] Investigate whether arrangement spines can store data in a column-of-datums layout for
direct vectorized evaluation from arrangements without materializing collections.

[ ] This is a research/design step, not an implementation step.

**Files**: `src/compute/src/row_spine.rs`, `src/compute/src/typedefs.rs`

---

## Notes

### Conversion cost awareness

The PR #35464 benchmarks show:
- Row→Column transpose: ~58μs per 1024 rows
- Column→Row packing: ~10μs per 1024 rows
- Vectorized arithmetic: ~0.7μs per 1024 rows

The conversion cost dominates. The strategy is:
1. First, get columnar containers flowing through edges (even if Row-packed inside)
2. Then, reduce conversions by keeping data columnar across operator boundaries
3. Finally, enable vectorized evaluation where conversion is amortized

### Arrangement boundary

Arrangements remain Row-based throughout this plan. They use `DatumContainer` which stores
row bytes contiguously with dictionary compression. Converting arrangements to true columnar
is Phase 10 (future work) and depends on changes to differential-dataflow spine infrastructure.

### Error streams

Error streams (`VecCollection<S, DataflowError, Diff>`) are kept as Vec throughout. Errors are
rare and the `DataflowError` type is not amenable to columnar representation.
