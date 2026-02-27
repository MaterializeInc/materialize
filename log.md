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

## Session 6: Shrink EvalError from 56 to 40 bytes via boxing large variants

**Date:** 2026-02-27

### Changes

Box three large `EvalError` variants that were inflating the enum:

1. **`Parse(ParseError)` → `Parse(Box<ParseError>)`**
   - `ParseError` is ~40 bytes (contains `ParseKind` enum + 3 string fields)
   - Parse errors are only created on malformed input — rare hot path

2. **`OutOfDomain(DomainLimit, DomainLimit, Box<str>)` → `OutOfDomain(Box<(DomainLimit, DomainLimit, Box<str>)>)`**
   - The tuple is ~48 bytes inline; boxing reduces to 8 bytes
   - Only triggered for domain violations (e.g., `acos(2.0)`)

3. **`DateDiffOverflow { unit, a, b }` → `DateDiffOverflow(Box<(Box<str>, Box<str>, Box<str>)>)`**
   - The struct variant is ~48 bytes (3 × Box<str> = 3 × 16); boxing reduces to 8 bytes
   - Only triggered on extreme date ranges

All three are error-path only — boxing has zero cost on the happy path since the Box is never allocated.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| EvalError | 56 | 40 | 16 bytes (29%) |
| MirRelationExpr | 104 | 96 | 8 bytes (8%, cascading) |

### Cascading effects

- **MirRelationExpr::Constant** contains `Result<Vec<(Row, Diff)>, EvalError>` inline — when EvalError shrinks by 16 bytes, the Constant variant shrinks, reducing MirRelationExpr from 104 to 96 bytes
- Every `Box<MirRelationExpr>` allocation now uses 96 instead of 104 bytes
- MirRelationExpr is the core IR type stored recursively, so savings compound at every tree node
- `MirScalarExpr::Literal` contains `Box<Result<Row, EvalError>>` — the boxed allocation is also smaller

### Benchmark results

All query types and error paths work correctly with no regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| Trig functions (acos, asin, atanh, acosh) | instant | OutOfDomain error path tested |
| VALUES clause (20 rows) | ~1.2ms | Exercises Constant nodes |
| Scalar expressions (10K rows) | ~411ms | Exercises expression evaluation |
| Parse errors (`'text'::int`) | instant | Parse error formatting correct |

### Files changed (5 files)

- `src/expr/src/scalar.rs` — Box Parse, OutOfDomain, DateDiffOverflow variants + Display/proto impls + size assertion
- `src/expr/src/scalar/func/impls/float64.rs` — 4 OutOfDomain construction sites (acos, asin, acosh, atanh)
- `src/expr/src/relation.rs` — MirRelationExpr size assertion (104→96)
- `src/storage-types/src/errors.rs` — Columnation impl for boxed variants
- `src/adapter/src/catalog.rs` — Pattern matching adjustment for boxed Literal

### Future optimization ideas (roadmap)

- **`Vec<T>` → `Box<[T]>` conversions**: Many plan struct fields use `Vec<T>` but never grow after construction. Converting to `Box<[T]>` saves 8 bytes per field (24→16). Key targets: `SqlRelationType::column_types`, `SqlRelationType::keys`, `LinearMfp` fields, various plan structures.
- **`JoinImplementation` (120 bytes)**: The `Differential` variant contains `Option<JoinInputCharacteristics>` (64 bytes niche-optimized). Boxing this or the entire variant could shrink it significantly.
- **`ReprRelationType` (48 bytes)**: Contains two Vecs (column_types + keys) that never grow. Converting to `Box<[T]>` would save 16 bytes.
- **`like_pattern::Matcher` internals**: `pattern: String` → `Box<str>`, `Subpattern.suffix: String` → `Box<str>`, `Vec<Subpattern>` → `Box<[Subpattern]>` — all are immutable after construction.

## Session 7: Shrink AggregateFunc from 48 to 48 bytes via Vec→Box<[T]> (net: 64→48 from session 2)

**Date:** 2026-02-27

### Changes

Convert all `Vec<T>` fields in `AggregateFunc` to `Box<[T]>`, eliminating the unused capacity field. These fields are constructed once during planning and never grown afterward.

**15 `Vec<ColumnOrder>` → `Box<[ColumnOrder]>` conversions** across all variants that have an `order_by` field:
- `JsonbAgg`, `JsonbObjectAgg`, `MapAgg`, `ArrayConcat`, `ListConcat`, `StringAgg`
- `RowNumber`, `Rank`, `DenseRank`, `LagLead`
- `FirstValue`, `LastValue`, `FusedValueWindowFunc`
- `WindowAggregate`, `FusedWindowAggregate`

**2 `Vec<AggregateFunc>` → `Box<[AggregateFunc]>` conversions**:
- `FusedValueWindowFunc::funcs`
- `FusedWindowAggregate::wrapped_aggregates`

Additionally, function signatures were improved from `&Vec<T>` to `&[T]` (idiomatic Rust).

### Key insight

Unlike boxing (which adds a new indirection), `Vec<T>` → `Box<[T]>` is strictly better for immutable data: same heap allocation, same pointer+length, just without the wasted 8-byte capacity field. There's no tradeoff — data that's constructed once and never grown doesn't need the capacity.

### Size measurements (before → after)

| Type | Before (session 2) | After | Savings |
|------|---------------------|-------|---------|
| AggregateFunc | 64 | 48 | 16 bytes (25%) |
| AggregateExpr | 144 | 128 | 16 bytes (11%) |

The largest variant was `FusedWindowAggregate` with `Vec<AggregateFunc>(24) + Vec<ColumnOrder>(24) + Box<WindowFrame>(8) = 56`. After converting to `Box<[T]>`: `Box<[AggregateFunc]>(16) + Box<[ColumnOrder]>(16) + Box<WindowFrame>(8) = 40`, fitting the enum in 48 bytes.

### Cumulative AggregateFunc savings (sessions 2 + 7)

| Type | Original | After session 2 | After session 7 | Total savings |
|------|----------|-----------------|-----------------|---------------|
| AggregateFunc | 88 | 64 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 144 | 128 | 56 bytes (30%) |

### Benchmark results

All aggregate and window function queries work correctly with no regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| string_agg with ORDER BY (5000 rows) | ~16ms | Exercises StringAgg |
| jsonb_agg with ORDER BY | ~16ms | Exercises JsonbAgg |
| array_agg with ORDER BY | ~12ms | Exercises ArrayConcat |
| row_number OVER | ~26ms | Exercises RowNumber |
| first_value OVER | ~23ms | Exercises FirstValue |
| last_value OVER | ~21ms | Exercises LastValue |
| rank + dense_rank OVER | ~36ms | Exercises Rank + DenseRank |
| lag + lead OVER | ~27ms | Exercises LagLead (fused) |
| sum OVER (window aggregate) | ~26ms | Exercises WindowAggregate |

### Files changed

- `src/expr/src/relation/func.rs` — 15 `order_by` fields + 2 `funcs`/`wrapped_aggregates` fields converted to `Box<[T]>`, function signatures `&Vec<T>` → `&[T]`, 3 assert_eq adjustments for Box deref
- `src/expr/src/scalar.rs` — Updated size assertions (AggregateFunc 64→48, AggregateExpr 144→128)
- `src/sql/src/plan/hir.rs` — Construction sites converted to use `.into_boxed_slice()` and `.collect::<Vec<_>>().into_boxed_slice()`

## Session 8: Shrink like_pattern::Matcher from 72 to 64 bytes via String→Box<str> and Vec→Box<[T]>

**Date:** 2026-02-27

### Changes

Convert immutable heap types in `like_pattern::Matcher` and its internals to their unsized equivalents, eliminating unused capacity fields:

1. **`Matcher::pattern: String` → `Box<str>`** (saves 8 bytes: 24→16)
   - Pattern string is set at compile time and never modified
   - `Box<str>` stores `(ptr, len)` vs `String`'s `(ptr, len, capacity)`

2. **`MatcherImpl::String(Vec<Subpattern>)` → `MatcherImpl::String(Box<[Subpattern]>)`** (saves 8 bytes: 24→16)
   - The subpattern list is built once and never grown
   - `Box<[T]>` stores `(ptr, len)` vs `Vec<T>`'s `(ptr, len, capacity)`

3. **`Subpattern::suffix: String` → `Box<str>`** (saves 8 bytes per Subpattern: 40→32)
   - Each suffix literal is set once during pattern compilation
   - Removed `Default` derive from `Subpattern` (no longer needed with `Box<str>`)
   - Refactored `build_subpatterns` to construct Subpattern values directly instead of using `mem::take()`

### Key insight

Same as sessions 5 and 7: `String` → `Box<str>` and `Vec<T>` → `Box<[T]>` are strictly better for immutable data — same heap allocation, same indirection, just without the wasted 8-byte capacity field. The `build_subpatterns` refactor also removes 2 `shrink_to_fit()` calls that were trying to reclaim the unused capacity, since `Box<str>` and `Box<[Subpattern]>` never have excess capacity in the first place.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| Matcher | 72 | 64 | 8 bytes (11%) |
| Subpattern (heap element) | 40 | 32 | 8 bytes (20%) |
| MatcherImpl::String variant data | 24 | 16 | 8 bytes (33%) |

### Cascading effects

- `Matcher` is boxed inside `IsLikeMatch` in `UnaryFunc`, so the boxed heap allocation shrinks from 72 to 64 bytes
- Each `Subpattern` on the heap is 20% smaller, improving cache utilization for complex LIKE patterns with multiple wildcards
- The `build_subpatterns` function is cleaner: no more `shrink_to_fit()` calls or `Default` trait requirement

### Benchmark results

All LIKE and ILIKE queries work correctly with no regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| Prefix LIKE (`'John%'`) 10K rows | ~9.1ms | Simple prefix match |
| Suffix LIKE (`'%Smith'`) 10K rows | ~12.4ms | Suffix search |
| Contains LIKE (`'%oh%'`) 10K rows | ~9.5ms | Substring search |
| Multi-wildcard LIKE (`'%o%n%'`) 10K rows | ~8.2ms | Complex pattern |
| Single-char wildcard (`'J_n_ %'`) 10K rows | ~7.4ms | Underscore wildcard |
| ILIKE (`'%SMITH%'`) 10K rows | ~9.6ms | Case-insensitive (regex path) |
| Mixed LIKE + ILIKE | ~11.9ms | Combined patterns |

### Files changed

- `src/expr/src/scalar/like_pattern.rs` — `pattern: String` → `Box<str>`, `Vec<Subpattern>` → `Box<[Subpattern]>`, `suffix: String` → `Box<str>`, removed `Default` derive, refactored `build_subpatterns`
- `src/expr/src/scalar.rs` — Added size assertion for `Matcher` (64 bytes)

## Session 9: Shrink JoinImplementation from 120 to 64 bytes via boxing + Vec→Box<[T]>

**Date:** 2026-02-27

### Changes

Combined two techniques to shrink `JoinImplementation`:

1. **Box the Differential first tuple** (96→8 bytes inline)
   - `(usize, Option<Vec<MirScalarExpr>>, Option<JoinInputCharacteristics>)` is ~96 bytes due to `JoinInputCharacteristics` being ~64 bytes
   - This tuple is constructed once during join planning and only read during EXPLAIN/rendering
   - Boxing reduces the inline footprint from 96 to 8 bytes

2. **Convert outer `Vec<T>` → `Box<[T]>` in all variants** (saves 8 bytes per field, no new indirection)
   - `Differential` second arg: `Vec<(...)>` → `Box<[(...)]>` (24→16)
   - `DeltaQuery` outer arg: `Vec<Vec<(...)>>` → `Box<[Vec<(...)>]>` (24→16)
   - `IndexedFilter` key: `Vec<MirScalarExpr>` → `Box<[MirScalarExpr]>` (24→16)
   - `IndexedFilter` vals: `Vec<Row>` → `Box<[Row]>` (24→16)

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| JoinImplementation | 120 | 64 | 56 bytes (47%) |

The 64-byte result is better than the naive 72-byte estimate because the compiler found a niche optimization (Box's non-null pointer encodes the `Unimplemented` variant discriminant).

### Variant size breakdown (after)

- `Differential`: Box(8) + Box<[T]>(16) = 24 bytes
- `DeltaQuery`: Box<[T]>(16) = 16 bytes
- `IndexedFilter`: GlobalId(16) + GlobalId(16) + Box<[T]>(16) + Box<[T]>(16) = 64 bytes
- `Unimplemented`: 0 bytes (encoded via niche)

### Cascading effects

- `JoinImplementation` is stored as `Box<JoinImplementation>` in `MirRelationExpr::Join`, so each boxed allocation shrinks from 120 to 64 bytes
- All optimizer passes that construct/inspect join implementations benefit from smaller allocations
- The `Vec→Box<[T]>` conversions eliminate unused capacity fields — these are immutable after construction

### Benchmark results

All join types work correctly with no performance regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| 2-way join (1000 rows each) | ~7ms | Exercises Differential |
| 3-way join (1000 rows each) | ~14ms | Exercises DeltaQuery |
| IndexedFilter IN (1,5,10,50,100) | ~1.8ms | Exercises IndexedFilter with Box<[T]> |
| Join with filters | ~8ms | Complex Differential with filters |
| EXPLAIN 3-way join | instant | Differential/DeltaQuery plan rendering |
| EXPLAIN indexed filter | instant | IndexedFilter plan rendering |

### Files changed (7 files)

- `src/expr/src/relation.rs` — JoinImplementation enum: Box Differential first tuple, Vec→Box<[T]> in all variants, size assertion 120→64, visit methods updated for Box patterns
- `src/expr/src/explain/text.rs` — Differential match pattern updated for Box, join_order closure param `&Vec<T>` → `&[T]`, IndexedFilter `.to_vec()` for explain output
- `src/transform/src/join_implementation.rs` — Differential/DeltaQuery construction with Box::new() and .into_boxed_slice()
- `src/transform/src/literal_constraints.rs` — IndexedFilter construction with .into_boxed_slice()
- `src/transform/src/typecheck.rs` — Differential match pattern updated for Box
- `src/compute-types/src/plan/lowering.rs` — Differential match pattern updated, IndexedFilter key.to_vec()
- `src/adapter/src/coord/peek.rs` — IndexedFilter vals.to_vec() for FastPathPlan

## Session 10: Shrink MirScalarExpr from 72 to 56 bytes via shrinking BinaryFunc and VariadicFunc

**Date:** 2026-02-27

### Changes

Three complementary optimizations that shrink BinaryFunc, VariadicFunc, and cascadingly MirScalarExpr:

1. **Box `Regex` in `RegexpReplace` struct** (BinaryFunc 48 → 24 bytes)
   - Changed `pub regex: Regex` to `pub regex: Box<Regex>` in `RegexpReplace`
   - `mz_repr::adt::regex::Regex` is 40 bytes (wraps `regex::Regex` at 32 + 2 bools)
   - Boxing saves 32 bytes (40 → 8), making RegexpReplace shrink from 48 to 16 bytes
   - `RegexpReplace` was the single largest BinaryFunc variant; all ~180 other variants are unit structs (0 bytes)
   - `regexp_replace` patterns are compiled once during planning — boxing has zero cost

2. **Box `SqlScalarType` in 6 VariadicFunc variants** (VariadicFunc 40 → 24 bytes)
   - `ArrayCreate`, `ListCreate`, `MapBuild`, `RangeCreate`, `ArrayFill`, `ArrayToString` all had `elem_type: SqlScalarType` (32 bytes)
   - Changed to `elem_type: Box<SqlScalarType>` (8 bytes)
   - These types are constructed once during planning and never modified — boxing has zero cost

3. **Convert `RecordCreate::field_names` from `Vec<ColumnName>` to `Box<[ColumnName]>`** (24 → 16 bytes)
   - Field names are set once during planning and never modified
   - Eliminates the unused capacity field (same allocation, no tradeoff)

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| RegexpReplace | 48 | 16 | 32 bytes (67%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| MirScalarExpr | 72 | 56 | 16 bytes (22%) |
| AggregateExpr | 128 | 112 | 16 bytes (13%) |

### Why MirScalarExpr shrank so dramatically

Before: the two largest variants were:
- `CallBinary { func: BinaryFunc(48), expr1: Box(8), expr2: Box(8) }` = 64 bytes
- `CallVariadic { func: VariadicFunc(40), exprs: Vec(24) }` = 64 bytes

After:
- `CallBinary { func: BinaryFunc(24), expr1: Box(8), expr2: Box(8) }` = 40 bytes
- `CallVariadic { func: VariadicFunc(24), exprs: Vec(24) }` = 48 bytes
- `CallUnary { func: UnaryFunc(48), expr: Box(8) }` = 56 bytes (now the largest variant)

MirScalarExpr = 56 bytes (with niche optimization, the discriminant fits without extra padding).

### Cascading effects

- **MirScalarExpr** is the core expression type — it's stored recursively in trees. Every node saves 16 bytes.
- **AggregateExpr** contains MirScalarExpr inline, cascading from 128 → 112 bytes
- Every `Vec<MirScalarExpr>` element saves 16 bytes (in Map, Filter, Join, FlatMap, etc.)
- Every `Vec<AggregateExpr>` element saves 16 bytes (in Reduce nodes)
- All plan structures, optimizer passes, and serialization benefit

### Benchmark results

All query types work correctly with no performance regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| ARRAY[x, x+1, x+2] 10K rows | ~32ms | Exercises ArrayCreate with boxed SqlScalarType |
| LIST[x, x+1, x+2] 10K rows | ~22ms | Exercises ListCreate with boxed SqlScalarType |
| map['a'=>..., 'b'=>...] 10K rows | ~19ms | Exercises MapBuild with boxed SqlScalarType |
| ROW(x, x+1, x::text) 10K rows | ~17ms | Exercises RecordCreate with Box<[ColumnName]> |
| regexp_replace(x, pattern, repl) 10K rows | ~1.4s | Exercises boxed Regex in BinaryFunc |
| 5 binary ops (+ - * / %) 10K rows | ~15ms | Exercises smaller BinaryFunc in expression trees |
| int4range(x, x+10) 10K rows | ~13ms | Exercises RangeCreate with boxed SqlScalarType |

### Files changed (10 files)

- `src/expr/src/scalar/func/impls/string.rs` — Box Regex field in RegexpReplace struct
- `src/expr/src/scalar/func/variadic.rs` — Box SqlScalarType in 6 variants, Box<[ColumnName]> in RecordCreate, deref adjustments
- `src/expr/src/scalar.rs` — Updated size assertions, Box::new at RegexpReplace construction, deref for ListCreate pattern match
- `src/expr/src/relation.rs` — Box::new() at ListCreate/RecordCreate/MapBuild construction sites (18 changes)
- `src/expr-parser/src/parser.rs` — Box::new at ArrayCreate/ListCreate construction sites
- `src/sql/src/func.rs` — Box::new at all RangeCreate/ArrayFill/ArrayToString/RecordCreate construction sites (~25 changes)
- `src/sql/src/plan/query.rs` — Box::new at ArrayCreate/ListCreate/MapBuild/RecordCreate construction sites
- `src/sql/src/plan/lowering.rs` — Box::new at ListCreate construction, .into_boxed_slice() at RecordCreate construction

### Cumulative MirScalarExpr savings (sessions 1 + 10)

| Type | Original | After session 1 | After session 10 | Total savings |
|------|----------|-----------------|------------------|---------------|
| MirScalarExpr | 88 | 72 | 56 | 32 bytes (36%) |
| AggregateExpr | 184 | 168 | 112 | 72 bytes (39%) |

## Session 11: Shrink SqlScalarType from 32 to 24 bytes via boxing Record and Map variants

**Date:** 2026-02-27

### Changes

Box the `Record` and `Map` variants of `SqlScalarType` to enable niche optimization:

1. **`Record { fields: Box<[(ColumnName, SqlColumnType)]>, custom_id: Option<CatalogItemId> }` → `Record(Box<RecordType>)`**
   - Introduced `RecordType` struct with `fields` and `custom_id`
   - Record variant was the largest at 32 bytes (16 + 16); boxing reduces to 8 bytes

2. **`Map { value_type: Box<SqlScalarType>, custom_id: Option<CatalogItemId> }` → `Map(Box<MapType>)`**
   - Introduced `MapType` struct with `value_type` and `custom_id`
   - Map variant was 24 bytes (8 + 16); boxing reduces to 8 bytes

### Key insight: niche optimization requires a unique largest variant

The `List` variant contains `Option<CatalogItemId>`, which provides 251 spare niche values from `CatalogItemId`'s discriminant byte (4 of 256 values used, minus 1 for `Option::None`). The Rust compiler can store the outer `SqlScalarType` discriminant in one of these niches, avoiding an extra 8-byte discriminant+padding.

However, this only works when **exactly one variant** occupies the maximum size at the niche byte position. With both `List` and `Map` having `Option<CatalogItemId>` at the same offset, the compiler cannot distinguish between them using the niche byte alone, so it falls back to adding an explicit discriminant — wasting 8 bytes.

Boxing `Map` (and `Record`) ensures `List` is the sole 24-byte variant with the niche field, enabling the optimization. This was verified experimentally: a test enum with 2 niche-bearing variants was 32 bytes, but the same enum with 1 niche variant + 1 boxed was 24 bytes.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |

### Cascading effects

- **SqlColumnType** embeds `SqlScalarType` inline, shrinking from 40 to 32 bytes
- **SqlRelationType** contains `Box<[(ColumnName, SqlColumnType)]>` — each element on the heap saves 8 bytes
- `SqlScalarType` is pervasive: it appears in every column type, relation type, function signature, and plan node
- Every `Box<SqlScalarType>` allocation (used in `List`, `Array`, `Range`) is now 24 instead of 32 bytes
- Record and Map types are relatively uncommon compared to primitive scalar types, so the extra Box indirection on access has minimal impact

### Benchmark results

All query types work correctly with no regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| 18-column SELECT (varied types) | ~3.5ms | Exercises SqlScalarType allocation |
| jsonb operations | ~0.7ms | Exercises type inference |

### Files changed (~25 files)

- `src/repr/src/scalar.rs` — RecordType/MapType structs, variant changes, proto, arbitrary, methods, size assertions
- `src/repr/src/lib.rs` — Export RecordType, MapType
- `src/repr/src/relation.rs` — sql_union Record matching
- `src/repr/src/row/encode.rs` — Record/Map encoding/decoding
- `src/repr/src/stats.rs` — Map wildcard pattern
- `src/arrow-util/src/builder.rs` — Record/Map arrow conversion
- `src/arrow-util/src/reader.rs` — Record arrow reader
- `src/pgrepr/src/types.rs` — Record/Map PG type conversion
- `src/pgrepr/src/value.rs` — Record/Map value conversion
- `src/pgwire/src/protocol.rs` — Map binary encoding guard
- `src/sql/src/func.rs` — Record/Map function categories
- `src/sql/src/plan/query.rs` — Record/Map construction/matching (~8 sites)
- `src/sql/src/plan/hir.rs` — MapAgg return type
- `src/sql/src/plan/statement/ddl.rs` — Map type construction
- `src/sql/src/plan/typeconv.rs` — Record/Map conversions
- `src/expr/src/scalar/func.rs` — Record/Map stringify
- `src/expr/src/scalar/func/impls/map.rs` — MapGetValue return type
- `src/expr/src/scalar/func/impls/record.rs` — RecordGet return type
- `src/expr/src/scalar/func/impls/string.rs` — Map wildcard pattern
- `src/expr/src/scalar/func/variadic.rs` — MapBuild return type
- `src/expr/src/scalar/func/unmaterializable.rs` — Map type construction
- `src/expr/src/relation/func.rs` — MapAgg return type
- `src/interchange/src/json.rs` — Record/Map JSON conversion
- `src/interchange/src/avro/encode.rs` — Map avro encoding
- `src/interchange/src/avro/schema.rs` — Map avro schema
- `src/adapter/src/catalog.rs` — Record/Map humanization
- `src/adapter/src/catalog/builtin_table_updates.rs` — Map custom_id extraction
- `src/storage-types/src/sources/envelope.rs` — Record construction

## Session 12: Shrink HirScalarExpr from 192 to 80 bytes and HirRelationExpr from 456 to 88 bytes via boxing

**Date:** 2026-02-27

### Changes

Three complementary boxing optimizations that dramatically shrink the two core HIR (High-level Intermediate Representation) types:

1. **Box `WindowExpr` in `HirScalarExpr::Windowing`** (HirScalarExpr: 192 → 80 bytes)
   - `WindowExpr` is 176 bytes (contains `WindowExprType(128)` + two `Vec<HirScalarExpr>(24)` each)
   - The `Windowing` variant was 192 bytes (176 + 16 for `NameMetadata`), dwarfing all other variants
   - After boxing: `Box<WindowExpr>(8) + NameMetadata(16) = 24` bytes
   - Next-largest variants are `CallUnary(72)` and `Literal(72)`, so the enum settles at 80 bytes
   - `Windowing` is only constructed for window function calls — relatively rare vs Column/CallUnary/CallBinary

2. **Box `HirScalarExpr` in `HirRelationExpr::Join::on`** (cascading savings)
   - `on: HirScalarExpr` was 192 bytes inline; now `on: Box<HirScalarExpr>` is 8 bytes
   - Join variant shrinks from ~224 to ~40 bytes

3. **Box `HirScalarExpr` in `HirRelationExpr::TopK::limit` and `offset`** (HirRelationExpr: 456 → 88 bytes)
   - `limit: Option<HirScalarExpr>` was ~200 bytes; now `limit: Option<Box<HirScalarExpr>>` is 8 bytes (niche-optimized)
   - `offset: HirScalarExpr` was 192 bytes; now `offset: Box<HirScalarExpr>` is 8 bytes
   - TopK was the largest variant at ~464 bytes; after boxing: `8 + 24 + 24 + 8 + 8 + 16 = 88` bytes
   - The next-largest variant is `Reduce` at ~72 bytes, so the enum settles at 88 bytes

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 88 | 368 bytes (81%) |

### Why this is the highest-impact optimization yet

These are the two core HIR types — every SQL query is first represented as a tree of `HirScalarExpr` and `HirRelationExpr` nodes. Unlike the MIR types (which were already optimized in sessions 1-11), the HIR types were extremely bloated:

- **HirScalarExpr** was 192 bytes (vs MirScalarExpr at 56 bytes) — every scalar expression in every query used 3.4x more memory than needed
- **HirRelationExpr** was 456 bytes (vs MirRelationExpr at 96 bytes) — every relational node used 4.8x more memory than needed

The savings compound because:
- `HirScalarExpr` is stored recursively: every `Vec<HirScalarExpr>` element (in Map, Filter, CallTable, CallVariadic, WindowExpr) saves 112 bytes
- `HirRelationExpr` is stored recursively: every `Box<HirRelationExpr>` allocation saves 368 bytes
- HIR trees exist throughout planning (parsing → name resolution → type checking → optimization → lowering to MIR)

### Benchmark results

All query types work correctly with no regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| Inner Join (1000 rows) | ~33ms | Exercises boxed `on` field |
| Left Join (1000 rows) | ~39ms | Exercises variadic left join lowering with boxed `on` |
| 3-way Join (1000 rows) | ~35ms | Exercises multiple Join nodes |
| TopK LIMIT+OFFSET | ~25ms | Exercises boxed `limit` and `offset` |
| DISTINCT ON | ~32ms | Exercises TopK variant (limit=1) |
| Window: row_number | ~455ms | Exercises boxed WindowExpr (Scalar) |
| Window: first_value | ~455ms | Exercises boxed WindowExpr (Value) |
| Window: sum aggregate | ~538ms | Exercises boxed WindowExpr (Aggregate) |
| Fused: row_number+rank+dense_rank | ~455ms | Exercises fusion of boxed WindowExprs |
| Complex: Join+TopK+Window | ~43ms | Combines all optimized paths |

### Files changed (5 files)

- `src/sql/src/plan/hir.rs` — Box WindowExpr in Windowing, Box HirScalarExpr in Join::on and TopK::limit/offset, windowing() constructor, top_k() constructor, size assertions
- `src/sql/src/plan/lowering.rs` — Deref adjustments for boxed `on` in Join (2 sites in variadic left join extraction)
- `src/sql/src/plan/transform_hir.rs` — Pattern matching adjustments for boxed WindowExpr (6 sites: extract_options, fused value, fused aggregate, is_value_or_agg_window_func_call)
- `src/sql/src/plan/statement/dml.rs` — Deref adjustment for boxed `offset` assignment
- `src/sql/src/plan/explain/text.rs` — No changes needed (auto-deref through Box)

### Cumulative savings across all sessions

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| MirScalarExpr | 88 | 56 | 32 bytes (36%) |
| MirRelationExpr | 176 | 96 | 80 bytes (45%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 88 | 368 bytes (81%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 112 | 72 bytes (39%) |
| UnaryFunc | 72 | 48 | 24 bytes (33%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
