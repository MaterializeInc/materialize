# Memory Optimization Log

## Session 1: Shrink MirScalarExpr and UnaryFunc via boxing

**Date:** 2026-02-27

### Changes

Two boxing optimizations to reduce the size of key expression types:

1. **Box `like_pattern::Matcher` in `IsLikeMatch` struct** (`src/expr/src/scalar/func/impls/string.rs`)
   - Changed `IsLikeMatch(Matcher)` to `IsLikeMatch(Box<Matcher>)`
   - `Matcher` is 72 bytes (contains `String` + `MatcherImpl` enum with `Vec<Subpattern>` or `Regex`)
   - LIKE patterns are rare; boxing has negligible runtime cost
   - **UnaryFunc: 72 → 56 bytes (22% reduction)**

2. **Box `Result<Row, EvalError>` in `MirScalarExpr::Literal`** (`src/expr/src/scalar.rs`)
   - Changed `Literal(Result<Row, EvalError>, ReprColumnType)` to `Literal(Box<Result<Row, EvalError>>, ReprColumnType)`
   - `Result<Row, EvalError>` is 56 bytes (dominated by `EvalError` at 56 bytes), inflating the Literal variant to 88 bytes
   - Literal values are constructed once during planning and read during eval; the extra Box indirection is negligible
   - **MirScalarExpr: 88 → 72 bytes (18% reduction)**

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| MirScalarExpr | 88 | 72 | 16 bytes (18%) |
| UnaryFunc | 72 | 56 | 16 bytes (22%) |
| AggregateExpr | 184 | 168 | 16 bytes (9%, cascading) |

### Cascading effects

- Every `Vec<MirScalarExpr>` element saves 16 bytes
- `AggregateExpr` (which contains `MirScalarExpr` inline) shrinks from 184 to 168 bytes
- All plan structures containing `MirScalarExpr` benefit

### Files changed

- `src/expr/src/scalar.rs` — Literal variant type + all construction/matching sites
- `src/expr/src/scalar/func/impls/string.rs` — IsLikeMatch struct
- `src/expr/src/scalar/func/unary.rs` — test
- `src/expr/src/interpret.rs` — Literal matching
- `src/expr/src/explain/text.rs` — Literal display
- `src/expr-parser/src/parser.rs` — Literal construction
- `src/sql/src/plan/hir.rs` — Literal matching
- `src/sql/src/plan/lowering.rs` — Literal construction
- `src/transform/src/analysis/equivalences.rs` — Literal matching
- `src/transform/src/column_knowledge.rs` — Literal construction/matching
- `src/transform/src/demand.rs` — Literal construction
- `src/transform/src/typecheck.rs` — Literal matching
- `src/adapter/src/coord/sequencer/inner/copy_from.rs` — Literal matching

## Session 2: Shrink AggregateFunc via boxing WindowFrame

**Date:** 2026-02-27

### Changes

Box `WindowFrame` (40 bytes) in the 4 `AggregateFunc` variants that embed it inline:

1. **`FirstValue { window_frame: WindowFrame }` → `FirstValue { window_frame: Box<WindowFrame> }`**
2. **`LastValue { window_frame: WindowFrame }` → `LastValue { window_frame: Box<WindowFrame> }`**
3. **`WindowAggregate { window_frame: WindowFrame }` → `WindowAggregate { window_frame: Box<WindowFrame> }`**
4. **`FusedWindowAggregate { window_frame: WindowFrame }` → `FusedWindowAggregate { window_frame: Box<WindowFrame> }`**

`WindowFrame` is 40 bytes (contains `WindowFrameUnits` enum + two `WindowFrameBound` values). It's constructed once during planning and read during evaluation; the extra Box indirection is negligible.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| AggregateFunc | 88 | 64 | 24 bytes (27%) |
| AggregateExpr | 168 | 144 | 24 bytes (14%) |

### Variant size breakdown

The `FusedWindowAggregate` variant drove the enum to 88 bytes with `Vec<AggregateFunc>(24) + Vec<ColumnOrder>(24) + WindowFrame(40) = 88`. After boxing `WindowFrame`, it becomes `24 + 24 + 8 = 56`, and the enum (with discriminant and alignment) fits in 64 bytes.

### Cascading effects

- Every `Vec<AggregateExpr>` element saves 24 bytes (in `Reduce` nodes)
- All plan structures containing `AggregateExpr` benefit
- Queries with window functions (first_value, last_value, window aggregates) use less memory during planning

### Benchmark results

Window function queries run correctly. Planning + execution for `first_value`, `last_value`, and window aggregate queries with 1000 rows averages ~11ms, consistent with baseline (no regression from the extra Box indirection).

### Files changed

- `src/expr/src/relation/func.rs` — WindowFrame field type in 4 variants + 3 display comparison fixes
- `src/expr/src/scalar.rs` — Added size assertions for AggregateFunc (64) and AggregateExpr (144)
- `src/sql/src/plan/hir.rs` — 4 construction sites wrapped with Box::new()

## Session 3: Shrink MirRelationExpr from 176 to 104 bytes via boxing

**Date:** 2026-02-27

### Changes

Box large fields in the three biggest `MirRelationExpr` variants:

1. **`FlatMap { func: TableFunc }` → `FlatMap { func: Box<TableFunc> }`**
   - `TableFunc` is 80 bytes; boxing it saves 72 bytes in the variant (80 → 8)
   - FlatMap variant: 112 → 40 bytes

2. **`Join { implementation: JoinImplementation }` → `Join { implementation: Box<JoinImplementation> }`**
   - `JoinImplementation` is 120 bytes; boxing it saves 112 bytes in the variant (120 → 8)
   - Join variant: 168 → 56 bytes

3. **`TopK { limit: Option<MirScalarExpr> }` → `TopK { limit: Option<Box<MirScalarExpr>> }`**
   - `Option<MirScalarExpr>` is 72 bytes; boxing saves 64 bytes (72 → 8)
   - TopK variant: ~160 → ~96 bytes

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| MirRelationExpr | 176 | 104 | 72 bytes (41%) |

### Benchmark results

All query types work correctly with no performance regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| 3-way join (1000 rows each) | ~14ms | Exercises boxed JoinImplementation |
| TopK with LIMIT | ~4.4ms | Exercises boxed limit |
| FlatMap via generate_series | ~6.8ms | Exercises boxed TableFunc |
| Complex join with filters | ~15ms | Combines multiple optimized paths |

### Cascading effects

- Every `Box<MirRelationExpr>` pointer now points to 104 instead of 176 bytes
- `MirRelationExpr` is the core IR type — it's stored recursively in trees, so savings compound at every node
- All optimizer passes, planning, and serialization benefit from the reduced size
- Better cache locality due to smaller node size

### Files changed (18 files)

- `src/expr/src/relation.rs` — FlatMap, Join, TopK field types + constructor methods + visit methods + size assertion
- `src/expr/src/explain/text.rs` — Deref adjustments for boxed fields
- `src/expr-parser/src/parser.rs` — Construction adjustments for boxed fields
- `src/adapter/src/coord/peek.rs` — Pattern matching adjustment
- `src/compute-types/src/plan/lowering.rs` — Pattern matching + construction adjustments
- `src/transform/src/analysis.rs` — Deref for boxed implementation
- `src/transform/src/canonicalization/flat_map_elimination.rs` — Deref for boxed func
- `src/transform/src/column_knowledge.rs` — Deref for boxed func
- `src/transform/src/dataflow.rs` — Deref for boxed func
- `src/transform/src/fusion/top_k.rs` — Deref for boxed limit
- `src/transform/src/join_implementation.rs` — Deref for boxed implementation
- `src/transform/src/literal_constraints.rs` — Pattern matching adjustment
- `src/transform/src/literal_lifting.rs` — Pattern matching adjustment
- `src/transform/src/movement/projection_lifting.rs` — Pattern matching + deref adjustments
- `src/transform/src/movement/projection_pushdown.rs` — Pattern matching + deref adjustments
- `src/transform/src/redundant_join.rs` — Deref for boxed implementation
- `src/transform/src/semijoin_idempotence.rs` — Pattern matching adjustments
- `src/transform/src/typecheck.rs` — Deref for boxed func

## Session 4: Shrink TableFunc from 80 to 40 bytes via boxing

**Date:** 2026-02-27

### Changes

Box two large fields in `TableFunc` variants that were inflating the enum:

1. **`RegexpExtract(AnalyzedRegex)` → `RegexpExtract(Box<AnalyzedRegex>)`**
   - `AnalyzedRegex` is 72 bytes (contains `Regex` + `Vec<Option<String>>`)
   - This variant was the largest, driving the enum to 80 bytes

2. **`TabletizedScalar { relation: SqlRelationType }` → `TabletizedScalar { relation: Box<SqlRelationType> }`**
   - `SqlRelationType` is 48 bytes (contains two `Vec`s)
   - Second largest contributor to enum size

Both types are constructed once during planning and read during evaluation; the extra Box indirection is negligible.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| TableFunc | 80 | 40 | 40 bytes (50%) |

### Why this matters beyond Session 3

In Session 3, we already boxed `TableFunc` inside `MirRelationExpr::FlatMap`. This session shrinks `TableFunc` *itself*, so the boxed allocation is now half the size. This also benefits any other location where `TableFunc` is stored or passed by value.

### Benchmark results

All FlatMap-related query types work correctly with no regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| generate_series (100K rows) | ~29ms | Exercises FlatMap with non-boxed variant |
| regexp_extract | ~3.4ms | Exercises boxed AnalyzedRegex |
| VALUES clause (TabletizedScalar) | ~1.2ms | Exercises boxed SqlRelationType |

### Exploration note: ColumnOrder column usize→u32

Before settling on TableFunc boxing, we explored a non-boxing optimization: changing `ColumnOrder::column` from `usize` to `u32` (would shrink ColumnOrder from 16→8 bytes). This compiled cleanly across 13+ files but failed in the `mz_lowertest` test framework, which generates invalid JSON (`#0` instead of `0`) when deserializing column references. The framework happens to work with `usize` but not `u32` due to how it tokenizes and reconstructs JSON for serde. This approach was abandoned.

### Files changed

- `src/expr/src/relation/func.rs` — Box AnalyzedRegex and SqlRelationType in two variants
- `src/expr/src/relation.rs` — Update size assertion (80→40)
- `src/sql/src/func.rs` — Box::new() at RegexpExtract construction site
- `src/sql/src/plan/query.rs` — Box::new() at TabletizedScalar construction site

## Session 5: Shrink UnaryFunc from 56 to 48 bytes via String→Box<str> and Vec→Box<[T]>

**Date:** 2026-02-27

### Changes

Eliminate wasted capacity fields in `ToCharTimestamp` and `ToCharTimestampTz` by converting heap types to their unsized equivalents:

1. **`ToCharTimestamp.format_string: String` → `Box<str>`** (saves 8 bytes: 24→16)
   - `String` stores `(ptr, len, capacity)` but format strings are never modified after creation
   - `Box<str>` stores `(ptr, len)` — no wasted capacity field

2. **`ToCharTimestampTz.format_string: String` → `Box<str>`** (saves 8 bytes: 24→16)
   - Same reasoning as above

3. **`DateTimeFormat(Vec<DateTimeFormatNode>)` → `DateTimeFormat(Box<[DateTimeFormatNode]>)`** (saves 8 bytes: 24→16)
   - The format node list is compiled once and never modified
   - `Box<[T]>` stores `(ptr, len)` vs `Vec<T>`'s `(ptr, len, capacity)`

Each `ToCharTimestamp`/`ToCharTimestampTz` struct saves 16 bytes (8 from format_string + 8 from DateTimeFormat). These were among the largest UnaryFunc variants, driving the enum from 56 → 48 bytes.

### Key insight

Unlike boxing (which adds an indirection), `String` → `Box<str>` and `Vec<T>` → `Box<[T]>` are strictly better: same heap allocation, same indirection, just without the wasted capacity field. There's no tradeoff — just free memory savings for data that's constructed once and never grown.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| UnaryFunc | 56 | 48 | 8 bytes (14%) |
| DateTimeFormat | 24 | 16 | 8 bytes (33%) |
| ToCharTimestamp | ~56 | ~40 | ~16 bytes (29%) |
| ToCharTimestampTz | ~56 | ~40 | ~16 bytes (29%) |

### Benchmark results

All to_char queries work correctly with no performance regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| to_char(timestamp, ...) 10K rows | ~580ms | Exercises ToCharTimestamp |
| to_char(timestamptz, ...) 10K rows | ~615ms | Exercises ToCharTimestampTz |
| General cast queries 10K rows | ~8.6ms | Exercises UnaryFunc in expression trees |

### Files changed

- `src/expr/src/scalar/func/impls/timestamp.rs` — `format_string: String` → `Box<str>` in ToCharTimestamp and ToCharTimestampTz
- `src/expr/src/scalar/func/format.rs` — `DateTimeFormat(Vec<DateTimeFormatNode>)` → `DateTimeFormat(Box<[DateTimeFormatNode]>)`
- `src/expr/src/scalar.rs` — Updated size assertion (UnaryFunc 56→48), `.to_string()` → `.into()` at construction sites

### Future optimization ideas (roadmap)

- **Box large `EvalError` variants**: `DateDiffOverflow` and `OutOfDomain` at 48 bytes each drive EvalError to 56 bytes. Boxing these would reduce EvalError to ~32 bytes, which further shrinks Result<Row, EvalError> and could enable un-boxing the Literal Result.
- **`Vec<T>` → `Box<[T]>` conversions**: Many plan struct fields use `Vec<T>` but never grow after construction. Converting to `Box<[T]>` saves 8 bytes per field (24→16). Key targets: `SqlRelationType::column_types`, `SqlRelationType::keys`, `LinearMfp` fields, various plan structures.
- **`JoinImplementation` (120 bytes)**: The `Differential` variant contains `Option<JoinInputCharacteristics>` (64 bytes niche-optimized). Boxing this or the entire variant could shrink it significantly.
- **`ReprRelationType` (48 bytes)**: Contains two Vecs (column_types + keys) that never grow. Converting to `Box<[T]>` would save 16 bytes.
- **`like_pattern::Matcher` internals**: `pattern: String` → `Box<str>`, `Subpattern.suffix: String` → `Box<str>`, `Vec<Subpattern>` → `Box<[Subpattern]>` — all are immutable after construction.
