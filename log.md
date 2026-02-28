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

## Session 13: Shrink HirRelationExpr from 88 to 72 bytes via Vec→Box<[T]> and boxing SqlRelationType

**Date:** 2026-02-27

### Changes

Five field type changes that combine to reduce HirRelationExpr from 88 to 72 bytes:

1. **`Constant::typ: SqlRelationType` → `Box<SqlRelationType>`** (Constant variant: 72→32 bytes)
   - SqlRelationType is 48 bytes (contains two Vecs); boxing reduces inline to 8 bytes
   - Constant values are constructed once during planning and immutable

2. **`Get::typ: SqlRelationType` → `Box<SqlRelationType>`** (Get variant: 64→24 bytes)
   - Same reasoning — Get nodes are constructed once and the typ is read-only

3. **`TopK::group_key: Vec<usize>` → `Box<[usize]>`** (saves 8 bytes)
   - Group keys are set during planning and never grown
   - Box<[T]> eliminates the unused capacity field

4. **`TopK::order_key: Vec<ColumnOrder>` → `Box<[ColumnOrder]>`** (saves 8 bytes; TopK: 88→72 bytes)
   - Order keys are set during planning and never grown

5. **`CallTable::exprs: Vec<HirScalarExpr>` → `Box<[HirScalarExpr]>`** (CallTable: 64→56 bytes)
   - Table function arguments are set during planning and never grown

### Key insight: niche optimization preservation

The critical constraint is that `Option<u64>` (in TopK::expected_group_size and Reduce::expected_group_size) provides a niche value at byte offset 56 that the compiler uses to store the enum discriminant. For this to work, all variants must have either a niche-bearing field or padding at byte offset 56.

Boxing SqlRelationType in Constant/Get ensures those variants are small enough (≤32 and ≤24 bytes respectively) that byte 56 is pure padding. Converting CallTable's Vec to Box<[T]> reduces it to 56 bytes, so byte 56 is again padding. The TopK Vec→Box<[T]> changes shrink TopK from 88→72 bytes, which becomes the new maximum. The niche at offset 56 in TopK/Reduce stores the discriminant, so no explicit discriminant field is needed.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| HirRelationExpr | 88 | 72 | 16 bytes (18%) |

### Variant size breakdown (after)

- TopK: 72 bytes (input:8 + group_key:16 + order_key:16 + limit:8 + offset:8 + expected_group_size:16)
- Reduce: 72 bytes (input:8 + group_key:24 + aggregates:24 + expected_group_size:16)
- Constant: 32 bytes (rows:24 + typ:8)
- Get: 24 bytes (id:16 + typ:8)
- CallTable: 56 bytes (func:40 + exprs:16)

### Benchmark results

All query types work correctly with no regression:

| Query Type | Avg Time (200 iter) | Notes |
|-----------|---------------------|-------|
| Multi-CTE join (5 CTEs) | ~33ms/query | Exercises Get + Constant nodes |
| Window function (row_number) | ~30ms/query | Exercises Reduce-like planning |
| Triple TopK join | ~34ms/query | Exercises TopK with Box<[T]> fields |
| generate_series CallTable | ~37ms/query | Exercises CallTable with Box<[T]> exprs |
| VALUES Constant | ~65ms/query | Exercises Constant with Box<SqlRelationType> |
| Union TopK | ~52ms/query | Exercises multiple TopK nodes |

### Files changed (5 files)

- `src/sql/src/plan/hir.rs` — Field type changes in Constant, Get, TopK, CallTable + constructor updates + typ()/as_const() deref adjustments + size assertion 88→72
- `src/sql/src/plan/query.rs` — Construction sites: Box::new() for Constant/Get typ, .into_boxed_slice() for CallTable exprs (7 sites)
- `src/sql/src/plan/lowering.rs` — `ReprRelationType::from(&typ)` → `from(&*typ)` for boxed typ fields (3 sites)
- `src/sql/src/plan/explain.rs` — Box::new() for Get typ construction (1 site)
- `src/sql/src/plan.rs` — Box::new() for SelectPlan::immediate Constant typ

### Cumulative savings across all sessions

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| MirScalarExpr | 88 | 56 | 32 bytes (36%) |
| MirRelationExpr | 176 | 96 | 80 bytes (45%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
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
| ReprRelationType | 48 | 40 | 8 bytes (17%) |

## Session 14: Shrink ReprRelationType via Vec→Box<[T]>

**Date:** 2026-02-28

### Changes

Changed `ReprRelationType::column_types` from `Vec<ReprColumnType>` to `Box<[ReprColumnType]>`:
- `Vec<T>` is 24 bytes (ptr + len + capacity); `Box<[T]>` is 16 bytes (ptr + len)
- `column_types` is constructed once and never mutated in `ReprRelationType` (unlike `SqlRelationType` which uses `.push()`, `.retain()`, etc.)
- The `Box<[T]>` still supports iteration, indexing, `.len()`, `.clone()`, Serialize/Deserialize
- A few mutation sites in `literal_lifting.rs` use `std::mem::take().into_vec()` pattern for temporary mutation

**ReprRelationType: 48 → 40 bytes (17% reduction)**

### Size measurements

| Type | Before | After | Savings |
|------|--------|-------|---------|
| ReprRelationType | 48 | 40 | 8 bytes (17%) |

### Cascading effects

- `ReprRelationType` is embedded inline in `MirRelationExpr::Get` and `MirRelationExpr::Constant` variants
- Every MIR plan node that references table types benefits from reduced memory per `ReprRelationType` instance
- `MirRelationExpr` remains at 96 bytes (the Get variant at 88 bytes is below the 96-byte threshold set by other variants)

### Benchmark

Query planning latency (200 iterations each, 3 trials averaged):

| Query | Avg per query |
|-------|--------------|
| Simple SELECT | 0.107 ms |
| 5-table JOIN | 0.129 ms |
| EXPLAIN JOIN | 0.122 ms |
| UNION ALL (5 tables) | 0.126 ms |
| View chain | 0.110 ms |

The optimization is structural — it saves 8 bytes per `ReprRelationType` instance in memory, benefiting workloads with many concurrent plans or large catalogs. Per-query latency is already sub-millisecond so timing improvements are not measurable at this scale.

### Files changed

- `src/repr/src/relation.rs` — Field type change + constructor updates
- `src/expr/src/relation.rs` — `col_with_input_cols` and `try_col_with_input_cols` signatures + callers
- `src/expr/src/relation/canonicalize.rs` — `canonicalize_equivalences` signature
- `src/transform/src/literal_constraints.rs` — Direct construction
- `src/transform/src/literal_lifting.rs` — Mutation sites (pop, assign)
- `src/transform/src/column_knowledge.rs` — Type conversions
- `src/transform/src/typecheck.rs` — Type conversion
- `src/transform/src/join_implementation.rs` — Deref for canonicalize call
- `src/transform/src/predicate_pushdown.rs` — Deref for canonicalize calls
- `src/transform/src/redundant_join.rs` — Deref for canonicalize call
- `src/transform/src/analysis.rs` — Slice conversion for try_col_with_input_cols call
- `src/sql/src/plan/lowering.rs` — `.drain()` → slice + `.to_vec()`
- `src/expr/tests/test_runner.rs` — Test adaptation

## Session 15: Shrink UnaryFunc from 48 to 32 bytes via boxing Regex, SqlScalarType, and ToChar internals

**Date:** 2026-02-28

### Changes

Three complementary boxing optimizations that shrink `UnaryFunc` from 48 to 32 bytes:

1. **Box `Regex` in 3 regex-related UnaryFunc structs** (each 40→8 bytes inline)
   - `IsRegexpMatch(Regex)` → `IsRegexpMatch(Box<Regex>)` — regex match test
   - `RegexpMatch(Regex)` → `RegexpMatch(Box<Regex>)` — regex capture groups
   - `RegexpSplitToArray(Regex)` → `RegexpSplitToArray(Box<Regex>)` — regex split
   - `mz_repr::adt::regex::Regex` is 40 bytes; boxing reduces to 8 bytes
   - Regex patterns are compiled once during planning — boxing has zero runtime cost

2. **Box `SqlScalarType` in 7 cast UnaryFunc structs** (each 24→8 bytes for the field)
   - `CastStringToArray.return_ty`, `CastStringToList.return_ty`, `CastStringToMap.return_ty`, `CastStringToRange.return_ty` — string-to-collection casts
   - `CastArrayToArray.return_ty`, `CastList1ToList2.return_ty`, `CastRecord1ToRecord2.return_ty` — collection-to-collection casts
   - `SqlScalarType` is 24 bytes; boxing reduces to 8 bytes per field
   - Cast functions are constructed once during planning and never modified

3. **Box `ToCharTimestamp`/`ToCharTimestampTz` internals into inner structs** (each ~40→8 bytes)
   - `ToCharTimestamp { format_string: Box<str>, format: DateTimeFormat }` → `ToCharTimestamp { inner: Box<ToCharTimestampInner> }`
   - `ToCharTimestampTz { format_string: Box<str>, format: DateTimeFormat }` → `ToCharTimestampTz { inner: Box<ToCharTimestampTzInner> }`
   - The inner struct is ~32 bytes (`Box<str>(16) + DateTimeFormat(16)`); boxing to 8 bytes
   - Used `#[serde(transparent)]` to maintain wire-compatible serialization

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| UnaryFunc | 48 | 32 | 16 bytes (33%) |
| MirScalarExpr | 56 | 48 | 8 bytes (14%) |
| AggregateExpr | 112 | 104 | 8 bytes (7%) |

### Why UnaryFunc shrank to 32 bytes

Before: the largest "lazy" UnaryFunc variants (structs with data) were:
- `ToCharTimestamp`: `Box<str>(16) + DateTimeFormat(16) = 32 bytes` inline → drove enum to 48 bytes (with alignment + discriminant)
- Various cast structs: `SqlScalarType(24) + Box<MirScalarExpr>(8) = 32 bytes`
- Regex structs: `Regex(40)` was largest individual field

After: all heavy data is boxed behind pointers:
- `ToCharTimestamp { inner: Box(8) }` = 8 bytes
- `CastStringToArray { return_ty: Box(8), cast_expr: Box(8) }` = 16 bytes
- `CastRecord1ToRecord2 { return_ty: Box(8), cast_exprs: Box<[T]>(16) }` = 24 bytes (new largest)
- `IsRegexpMatch(Box(8))` = 8 bytes

The largest variant is now `CastRecord1ToRecord2` at 24 bytes. With discriminant and alignment: 24 + padding → 32 bytes.

### Cascading effects

- **MirScalarExpr** contains `UnaryFunc` in the `CallUnary` variant. `CallUnary { func: UnaryFunc(32), expr: Box(8) }` = 40 bytes. The new largest variant is `CallVariadic { func: VariadicFunc(24), exprs: Vec(24) }` = 48 bytes. MirScalarExpr shrinks from 56 to 48 bytes.
- **AggregateExpr** contains `MirScalarExpr` inline, cascading from 112 → 104 bytes
- Every `Vec<MirScalarExpr>` element saves 8 bytes (in Map, Filter, Join, FlatMap, etc.)
- Every `Vec<AggregateExpr>` element saves 8 bytes (in Reduce nodes)

### Benchmark results

All query types work correctly with no performance regression:

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| regexp_match 10K rows | ~7.4ms | Exercises boxed Regex in RegexpMatch |
| regexp_split_to_array 10K rows | ~5.9ms | Exercises boxed Regex in RegexpSplitToArray |
| to_char(timestamp) 10K rows | ~7.1ms | Exercises boxed ToCharTimestampInner |
| to_char(timestamptz) 10K rows | ~6.8ms | Exercises boxed ToCharTimestampTzInner |
| '{1,2,3}'::int[] cast 10K rows | ~6.2ms | Exercises boxed SqlScalarType in CastStringToArray |
| 13-expression tree 10K rows | ~6.5ms | Exercises smaller MirScalarExpr nodes |

### Files changed (7 files)

- `src/expr/src/scalar.rs` — Box::new at regex/ToChar construction sites, size assertions (UnaryFunc 48→32, MirScalarExpr 56→48, AggregateExpr 112→104)
- `src/expr/src/scalar/func/impls/string.rs` — Box Regex in IsRegexpMatch/RegexpMatch/RegexpSplitToArray, Box SqlScalarType in CastStringToArray/CastStringToList/CastStringToMap/CastStringToRange, deref adjustments
- `src/expr/src/scalar/func/impls/timestamp.rs` — Box ToCharTimestamp/ToCharTimestampTz internals into inner structs with #[serde(transparent)]
- `src/expr/src/scalar/func/impls/array.rs` — Box SqlScalarType in CastArrayToArray
- `src/expr/src/scalar/func/impls/list.rs` — Box SqlScalarType in CastList1ToList2
- `src/expr/src/scalar/func/impls/record.rs` — Box SqlScalarType in CastRecord1ToRecord2, deref adjustments
- `src/sql/src/plan/typeconv.rs` — Box::new() at 8 cast construction sites

### Cumulative MirScalarExpr/UnaryFunc savings (sessions 1 + 5 + 10 + 15)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |

### Cumulative savings across all sessions (after session 15)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 96 | 80 bytes (45%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |

## Session 16: Shrink ColumnOrder from 16 to 8 bytes via usize→u32

**Date:** 2026-02-28

### Changes

Changed `ColumnOrder::column` from `usize` (8 bytes) to `u32` (4 bytes):

- `ColumnOrder` previously contained `column: usize` (8 bytes) + `desc: bool` (1 byte) + `nulls_last: bool` (1 byte) + padding = 16 bytes
- With `column: u32` (4 bytes) + `desc: bool` (1 byte) + `nulls_last: bool` (1 byte) + padding = 8 bytes
- No table can have more than ~4 billion columns, so `u32` is more than sufficient
- Unlike boxing, this has **zero overhead** — no extra indirection, no extra allocation

### Key insight: non-boxing size reduction

This is a rare opportunity to shrink a type *without* boxing. Most previous sessions achieved savings by boxing large fields behind `Box<T>`, which adds indirection. Here, we simply use a smaller integer type for a field that never needs 64 bits. The savings are "pure" — every `ColumnOrder` in memory is half the size with no tradeoffs.

### Previous attempt (Session 4)

This optimization was first attempted in Session 4 but abandoned because the `mz_lowertest` test framework failed: it generates invalid JSON (`#0` instead of `0`) when deserializing column references, and the tokenizer happened to work with `usize` but not `u32`. The fix was a one-line change in `src/expr-test-util/src/lib.rs` to handle both `"usize"` and `"u32"` type names in the deserialization context.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| ColumnOrder | 16 | 8 | 8 bytes (50%) |

### Cascading effects

`ColumnOrder` is stored in arrays/slices throughout the system. Every element in every `Vec<ColumnOrder>` or `Box<[ColumnOrder]>` saves 8 bytes:

- **AggregateFunc**: 15+ variants have `order_by: Box<[ColumnOrder]>` fields — each element saves 8 bytes
- **MirRelationExpr::TopK**: `order_key: Vec<ColumnOrder>` — each element saves 8 bytes
- **HirRelationExpr::TopK**: `order_key: Box<[ColumnOrder]>` — each element saves 8 bytes
- **JoinImplementation::DeltaQuery**: inner `Vec<ColumnOrder>` elements — each saves 8 bytes
- **Finishing::order_by**: `Vec<ColumnOrder>` — each element saves 8 bytes (affects every query with ORDER BY)

For a query with `ORDER BY a, b, c`, this saves 24 bytes per TopK/Finishing node. For window functions with `PARTITION BY ... ORDER BY ...`, the per-aggregate savings multiply.

### Benchmark results

All query types work correctly with no regression (200 iterations, 3 trials averaged):

| Query Type | Avg Time (200 iter) | Notes |
|-----------|---------------------|-------|
| ORDER BY multi-column TopK | ~22ms | Exercises TopK with 3 ColumnOrder elements |
| TopK with OFFSET | ~21ms | Exercises TopK with LIMIT + OFFSET |
| row_number OVER (ORDER BY) | ~23ms | Exercises window functions with ColumnOrder |
| string_agg ORDER BY | ~24ms | Exercises AggregateFunc with order_by |
| first_value OVER (ORDER BY) | ~23ms | Exercises window aggregate with ColumnOrder |

### Files changed (15 files)

- `src/expr/src/relation.rs` — `ColumnOrder::column: usize` → `u32`, display and comparison adjustments
- `src/expr/src/scalar.rs` — Size assertion for ColumnOrder (8 bytes)
- `src/expr-parser/src/parser.rs` — Parse `column` as `u32` instead of `usize`
- `src/expr-test-util/src/lib.rs` — Handle `"u32"` type name in deserializer (the fix for the Session 4 failure)
- `src/adapter/src/active_compute_sink.rs` — 8 `column as usize` casts for array indexing
- `src/adapter/src/coord/peek.rs` — `column as usize` for projection indexing
- `src/sql/src/plan/query.rs` — `column as usize` / `as u32` casts at 6+ sites
- `src/sql/src/plan/explain/text.rs` — `column as usize` for comparison
- `src/sql/src/plan/lowering.rs` — `arity() as u32` for arithmetic
- `src/transform/src/demand.rs` — `column as usize` for column set extension
- `src/transform/src/fold_constants.rs` — `*column as u32` for ColumnOrder construction
- `src/transform/src/literal_lifting.rs` — `column as usize` for comparison
- `src/transform/src/movement/projection_lifting.rs` — `as u32` / `as usize` for permutation
- `src/transform/src/movement/projection_pushdown.rs` — Separate reverse permutation for u32 column field
- `src/transform/src/typecheck.rs` — `column as usize` for bounds check

### Cumulative savings across all sessions (after session 16)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 96 | 80 bytes (45%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |

## Session 17: Shrink MirRelationExpr from 96 to 88 bytes via TopK Vec→Box<[T]>

**Date:** 2026-02-28

### Changes

Convert two `Vec<T>` fields in `MirRelationExpr::TopK` to `Box<[T]>`, eliminating the unused capacity field. These fields are constructed once during planning and never grown afterward.

1. **`TopK::group_key: Vec<usize>` → `Box<[usize]>`** (saves 8 bytes: 24→16)
   - Group keys are set during planning and never modified
   - `Box<[T]>` eliminates the unused capacity field

2. **`TopK::order_key: Vec<ColumnOrder>` → `Box<[ColumnOrder]>`** (saves 8 bytes: 24→16)
   - Order keys are set during planning and never modified
   - Same `Vec→Box<[T]>` pattern used in sessions 7, 9, 13, 14

### Key insight

TopK was the dominant variant in `MirRelationExpr` at 96 bytes. After shrinking both Vec fields by 8 bytes each, TopK drops to ~80 bytes (matching the Reduce variant), and the enum shrinks from 96 to 88 bytes. The `expected_group_size: Option<u64>` field provides a niche value that the compiler uses to store the enum discriminant, avoiding an explicit 8-byte discriminant.

A few mutation sites in `literal_lifting.rs` use the `std::mem::take().into_vec()` → mutate → `.into_boxed_slice()` pattern for temporary mutation (`.retain()` on group_key and order_key).

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| MirRelationExpr | 96 | 88 | 8 bytes (8%) |

### Cascading effects

- `MirRelationExpr` is the core MIR type — stored recursively in trees. Every `Box<MirRelationExpr>` allocation shrinks from 96 to 88 bytes.
- All optimizer passes, planning, and serialization benefit from reduced node size
- Better cache locality due to smaller node size
- Every element in `Vec<MirRelationExpr>` (e.g., in Union inputs, Join inputs) saves 8 bytes

### Benchmark results

All TopK query types work correctly with no regression (5000 rows):

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| Simple ORDER BY LIMIT 10 | ~9ms | Basic TopK |
| Multi-column ORDER BY (3 cols) LIMIT 20 | ~9ms | Exercises order_key with 3 ColumnOrder elements |
| DISTINCT ON (100 groups) | ~9ms | Exercises group_key |
| ORDER BY LIMIT 10 OFFSET 100 | ~13ms | TopK with offset |

### Files changed (8 files)

- `src/expr/src/relation.rs` — `group_key: Vec<usize>` → `Box<[usize]>`, `order_key: Vec<ColumnOrder>` → `Box<[ColumnOrder]>`, `.into_boxed_slice()` at construction, `.to_vec()` for cloning, size assertion 96→88
- `src/adapter/src/coord/peek.rs` — Deref adjustment for `order_key` comparison (`**order_key == *finishing.order_by`)
- `src/compute-types/src/plan/lowering.rs` — `.clone()` → `.to_vec()` for group_key/order_key
- `src/expr-parser/src/parser.rs` — `.into_boxed_slice()` at TopK construction
- `src/lowertest/src/lib.rs` — Handle boxed slice types (`[T]`) in deserialization framework
- `src/transform/src/analysis.rs` — `&Vec<usize>` → `&[usize]` in topk method signature
- `src/transform/src/literal_lifting.rs` — Mutation via `take().into_vec()` → mutate → `.into_boxed_slice()` pattern
- `src/transform/src/movement/projection_lifting.rs` — `.to_vec()` for TopK reconstruction

### Cumulative MirRelationExpr savings (sessions 3 + 6 + 17)

| Type | Original | After session 3 | After session 6 | After session 17 | Total savings |
|------|----------|-----------------|-----------------|------------------|---------------|
| MirRelationExpr | 176 | 104 | 96 | 88 | 88 bytes (50%) |

### Cumulative savings across all sessions (after session 17)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 88 | 88 bytes (50%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |

## Session 18: Shrink Plan enum from ~1888 to 184 bytes via boxing large variants and Statement<Raw>

**Date:** 2026-02-28

### Changes

Two complementary optimizations that dramatically shrink the `Plan` enum:

1. **Box `Statement<Raw>` in `DeclarePlan` and `PreparePlan`** (Plan: ~1888 → 992 bytes)
   - `Statement<Raw>` is ~832 bytes — it's the AST representation of an entire SQL statement
   - `PreparePlan` was ~888 bytes; after boxing `stmt`: 160 bytes
   - `DeclarePlan` was ~880 bytes; after boxing `stmt`: 128 bytes
   - These are the `PREPARE` and `DECLARE CURSOR` statements — the Statement is constructed once during parsing and only read during execution

2. **Box 17 large Plan variants (>200 bytes)** (Plan: 992 → 184 bytes)
   - Boxed variants: `CreateConnection`(992), `CreateSource`(792), `CreateTable`(784), `CopyTo`(712), `CreateSink`(672), `AlterSink`(648), `CopyFrom`(488), `ExplainPlan`(464), `CreateContinualTask`(456), `ExplainPushdown`(400), `CreateMaterializedView`(392), `ValidateConnection`(336), `CreateView`(264), `Subscribe`(232), `ShowColumns`(224), `ReadThenWrite`(224), `AlterCluster`(208)
   - Kept `SelectPlan`(184) unboxed as the hot-path variant (every SELECT query)
   - `InsertPlan` and other smaller variants also kept unboxed

### Key insight: Plan is one of the largest enums in the codebase

The `Plan` enum represents every possible SQL statement as a single type. Before optimization, its size was dominated by DDL plan variants (which carry full AST nodes, type information, and connection details). Since the enum must be as large as its largest variant, even a simple `SELECT 1` allocated ~1888 bytes for the `Plan` enum on the stack.

After optimization, `SelectPlan`(184 bytes) is the largest remaining unboxed variant, so the enum is 184 bytes. DDL/EXPLAIN/SUBSCRIBE operations pay one extra Box allocation, but these are inherently heavyweight operations where a single heap allocation is negligible.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| Plan | ~1888 | 184 | ~1704 bytes (90%) |
| PreparePlan | ~888 | 160 | ~728 bytes (82%) |
| DeclarePlan | ~880 | 128 | ~752 bytes (86%) |

### Why this matters

- **Every SQL statement** flows through the `Plan` enum — it's created by the planner and consumed by the coordinator/sequencer
- Shrinking it from ~1888 to 184 bytes means every statement uses ~1700 fewer bytes on the stack
- For high-throughput SELECT workloads, the `SelectPlan` variant is unboxed, so there's zero overhead
- For DDL operations, the extra Box allocation is negligible compared to the catalog transaction cost

### Cascading effects

- The `Plan` enum is matched in `rbac.rs` (permissions checking), `sequencer.rs` (execution), and `catalog_serving.rs` — all these code paths now work with a much smaller enum on the stack
- Function calls that pass `Plan` by value move fewer bytes
- Less stack pressure for deeply nested async call chains in the coordinator

### Benchmark results

All query types work correctly with no regression:

| Query Type | Avg Time (200 iter) | Notes |
|-----------|---------------------|-------|
| Simple SELECT | ~5.4ms/query | SelectPlan (unboxed, hot path) |
| SELECT with JOIN | ~8.0ms/query | SelectPlan (unboxed) |
| EXPLAIN | ~2.1ms/query | ExplainPlanPlan (boxed) |
| INSERT | ~2.4ms/query | InsertPlan (unboxed) |
| SHOW COLUMNS | ~1.3ms/query | ShowColumnsPlan (boxed) |
| CREATE+DROP VIEW cycle | ~85ms/cycle | CreateViewPlan (boxed) |
| CREATE+DROP TABLE cycle | ~92ms/cycle | CreateTablePlan (boxed) |

### Files changed (17 files)

- `src/sql/src/plan.rs` — Box 17 Plan variants, Box `Statement<Raw>` in DeclarePlan/PreparePlan, size assertion tests
- `src/sql/src/plan/hir.rs` — Adjusted top_k construction
- `src/sql/src/plan/statement/ddl.rs` — Box::new() at 8+ DDL plan construction sites
- `src/sql/src/plan/statement/dml.rs` — Box::new() at DML plan construction sites
- `src/sql/src/plan/statement/scl.rs` — Box::new() at ShowColumns construction
- `src/sql/src/plan/statement/show.rs` — Box::new() at ShowColumns construction
- `src/sql/src/plan/statement/validate.rs` — Box::new() at ValidateConnection construction
- `src/sql/src/rbac.rs` — Pattern matching simplified for boxed variants
- `src/adapter/src/catalog/state.rs` — Deref adjustments for boxed Plan variants
- `src/adapter/src/coord/appends.rs` — Deref adjustments for boxed Plan variants
- `src/adapter/src/coord/catalog_serving.rs` — Deref adjustments for boxed Plan variants
- `src/adapter/src/coord/introspection.rs` — Deref adjustment
- `src/adapter/src/coord/sequencer.rs` — Deref adjustments in sequencer dispatch
- `src/adapter/src/coord/sequencer/inner.rs` — Deref adjustments in inner sequencer
- `src/adapter/src/coord/sequencer/inner/create_materialized_view.rs` — Deref adjustment
- `src/adapter/src/coord/sequencer/inner/create_view.rs` — Deref adjustment
- `src/adapter/src/frontend_peek.rs` — Deref adjustments in frontend peek path

### Cumulative savings across all sessions (after session 18)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| Plan | ~1888 | 184 | ~1704 bytes (90%) |
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 88 | 88 bytes (50%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |

## Session 19: Shrink Ident from 24 to 16 bytes via String→Box<str>

**Date:** 2026-02-28

### Changes

Changed `Ident` (SQL identifier type) from `String` (24 bytes: ptr+len+capacity) to `Box<str>` (16 bytes: ptr+len). Identifiers are immutable after construction — they're created during SQL parsing and never grown — so the capacity field provided by `String` is pure waste.

1. **`Ident(pub(crate) String)` → `Ident(pub(crate) Box<str>)`** (`src/sql-parser/src/ast/defs/name.rs`)
   - Updated all factory methods (`new`, `new_lossy`, `new_unchecked`, `append_lossy`, `into_string`)
   - `append_lossy` converts to String temporarily for mutation, then back to Box<str>
   - **Ident: 24 → 16 bytes (33% reduction)**

2. **Walkabout codegen fix** (`src/walkabout/src/ir.rs`, `src/walkabout/src/generated.rs`)
   - Added `"str"` to the primitive type list so `Box<str>` is recognized as `Box<Primitive>`
   - Updated fold/visit code generation: `Box<primitive>` passes through unchanged (nothing to fold/visit)
   - Without this fix, the AST folder would try to recursively fold into `Box<str>` which doesn't make sense

3. **`From<Ident> for Value`** (`src/sql-parser/src/ast/defs/value.rs`)
   - `Self::String(ident.0)` → `Self::String(ident.0.into())` to convert Box<str> → String

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| Ident | 24 | 16 | 8 bytes (33%) |
| UnresolvedDatabaseName | 24 | 16 | 8 bytes (33%) |
| Option\<Ident\> | 24 | 16 | 8 bytes (33%) |

### Why this is high-impact

Ident is the most numerous AST node type in any SQL query. A simple `SELECT a, b FROM t WHERE c = 1` contains 4+ identifiers. More complex queries with qualified names (`schema.table.column`), aliases, joins, and subqueries easily contain 50-100+ identifiers.

The savings compound in multiple ways:
- **Direct inline savings**: Every struct with an `Ident` field saves 8 bytes (e.g., `TableAlias`, `ColumnDef`, 26+ `Statement` variants)
- **Vec element savings**: Each element in `Vec<Ident>` saves 8 bytes. Qualified names like `UnresolvedItemName(Vec<Ident>)` save 8 bytes per path component (e.g., `db.schema.table` = 24 bytes saved)
- **Option savings**: `Option<Ident>` shrinks from 24 to 16 bytes (e.g., `SelectItem::Expr { alias: Option<Ident> }`)
- **No runtime cost**: `Box<str>` eliminates wasted capacity that `String` may allocate; there's no extra indirection since both are heap-allocated

### Benchmark results

All query types work correctly with no regression:

| Query Type | Avg (ms) |
|-----------|----------|
| Simple SELECT (4 columns) | 3.70 |
| Column aliases | 3.78 |
| Qualified names | 4.96 |
| Subquery with aliases | 5.34 |
| UNION with aliases | 6.00 |
| Self-join (many idents) | 10.38 |
| CASE expression | 4.23 |
| ORDER BY + LIMIT | 4.41 |
| GROUP BY | 5.87 |

### Files changed (4 files)

- `src/sql-parser/src/ast/defs/name.rs` — Ident type + all factory methods + size test
- `src/sql-parser/src/ast/defs/value.rs` — From<Ident> conversion
- `src/walkabout/src/ir.rs` — Added "str" to primitive types
- `src/walkabout/src/generated.rs` — Handle Box<primitive> in fold/visit codegen

### Cumulative savings across all sessions (after session 19)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| Ident | 24 | 16 | 8 bytes (33%) |
| Plan | ~1888 | 184 | ~1704 bytes (90%) |
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 88 | 88 bytes (50%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |

---

## Session 20: Shrink PlanNode (compute LIR) via boxing large sub-types

**Date:** 2026-02-28

### Changes

Boxed 9 large inline fields in `PlanNode<T>` (the compute LIR plan enum). These fields are constructed once during plan lowering and read during dataflow rendering/explain. The extra Box indirection is negligible since these are not on hot eval paths.

Fields boxed:
1. `Get { plan: Box<GetPlan> }` — GetPlan is 136 bytes
2. `Mfp { mfp: Box<MapFilterProject> }` — MapFilterProject is 80 bytes
3. `FlatMap { mfp_after: Box<MapFilterProject> }` — 80 bytes
4. `Join { plan: Box<JoinPlan> }` — JoinPlan is 264 bytes (largest sub-type)
5. `Reduce { key_val_plan: Box<KeyValPlan> }` — KeyValPlan is 160 bytes
6. `Reduce { plan: Box<ReducePlan> }` — ReducePlan is 112 bytes
7. `Reduce { mfp_after: Box<MapFilterProject> }` — 80 bytes
8. `TopK { top_k_plan: Box<TopKPlan> }` — TopKPlan is 136 bytes
9. `ArrangeBy { input_mfp: Box<MapFilterProject> }` — 80 bytes

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| PlanNode\<Timestamp\> | 384 | 104 | 280 bytes (73%) |
| Plan\<Timestamp\> | 392 | 112 | 280 bytes (71%) |

### Why this is high-impact

`PlanNode` is the LIR (Low-level IR) stored per-dataflow on every compute worker. Each dataflow's plan tree consists of many PlanNode instances that persist in memory for the lifetime of the dataflow. With many dataflows active, the cumulative savings are significant.

The FlatMap variant (104 bytes with 3 Vecs + TableFunc + Box<MapFilterProject>) is now the largest variant, determining the overall enum size.

### Files changed (5 files)

- `src/compute-types/src/plan.rs` — Field type changes + size assertion test
- `src/compute-types/src/plan/lowering.rs` — Box::new() at ~12 construction sites
- `src/compute-types/src/plan/render_plan.rs` — Dereference boxed fields in PlanNode→RenderPlanNode conversion
- `src/compute-types/src/explain/text.rs` — Dereference in pattern matches and mode.expr() calls
- `src/compute-types/src/plan/transform/relax_must_consolidate.rs` — Restructured match to use nested if-let for boxed enums

### Cumulative savings across all sessions (after session 20)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| PlanNode | 384 | 104 | 280 bytes (73%) |
| Ident | 24 | 16 | 8 bytes (33%) |
| Plan (sql) | ~1888 | 184 | ~1704 bytes (90%) |
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 88 | 88 bytes (50%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |

## Session 21: Shrink Value from 48 to 40 bytes and Expr<Raw> from 240 to 72 bytes

**Date:** 2026-02-28

### Changes

Three complementary optimizations that shrink the core AST types used during SQL parsing:

1. **`Value::Number(String)` → `Number(Box<str>)`, `Value::String(String)` → `String(Box<str>)`, `Value::HexString(String)` → `HexString(Box<str>)`** (Value: 48 → 40 bytes)
   - String literals, numeric literals, and hex strings are parsed once and never modified
   - `Box<str>` stores `(ptr, len)` vs `String`'s `(ptr, len, capacity)` — eliminates the wasted capacity field
   - `IntervalValue::value: String` → `Box<str>` as well (IntervalValue: 48 → 40 bytes)

2. **`Expr::Function(Function<T>)` → `Function(Box<Function<T>>)`** (Expr<Raw>: 240 → 72 bytes)
   - `Function<Raw>` is 240 bytes (contains `name`, `args: FunctionArgs<T>`, `filter: Option<Box<Expr<T>>>`, `over: Option<WindowSpec<T>>`, `distinct: bool`)
   - This was the single largest Expr variant, inflating the entire enum to 240 bytes
   - Boxing reduces the inline footprint from 240 to 8 bytes
   - Function calls are constructed once during parsing and read during planning — boxing has negligible cost

3. **`Expr::Cast { data_type: T::DataType }` → `Cast { data_type: Box<T::DataType> }`**
   - `RawDataType` can be up to ~48 bytes (the `Other` variant contains `RawItemName` + `Vec<i64>`)
   - Boxing reduces the inline footprint from ~48 to 8 bytes
   - Cast expressions are constructed once during parsing and read during planning

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| Value | 48 | 40 | 8 bytes (17%) |
| IntervalValue | 48 | 40 | 8 bytes (17%) |
| Expr\<Raw\> | 240 | 72 | 168 bytes (70%) |

### Why Expr<Raw> shrank so dramatically

Before: `Function<Raw>` (240 bytes) was the sole driver of the enum size — it's a large struct with multiple fields including `FunctionArgs<T>` and `Option<WindowSpec<T>>`.

After: The largest remaining variants are:
- `Case { operand(8) + conditions(24) + results(24) + else_result(8) }` = 64 bytes
- `Op { op(48) + expr1(8) + expr2(8) }` = 64 bytes (Op contains `namespace: Option<Vec<Ident>>` + `op: String`)

With discriminant and alignment, the enum settles at 72 bytes.

### Why this is high-impact

`Expr<T>` is the most numerous AST node type. Every expression in every SQL query — columns, literals, function calls, operators, casts — is represented as an `Expr<T>`. A moderately complex query like `SELECT upper(name), id + 1, val::text FROM t WHERE id > 0` contains 8+ `Expr` nodes.

The savings compound massively because:
- `Expr<T>` is stored recursively: every `Vec<Expr<T>>` element saves 168 bytes (in function args, CASE conditions/results, IN lists, etc.)
- Every `Box<Expr<T>>` allocation saves 168 bytes (in unary/binary ops, subquery arms, etc.)
- `Value` appears inside `Expr::Value(Value)` — shrinking Value from 48→40 bytes contributes to the smaller Expr

### Cascading effects

- **Statement\<Raw\>** embeds `Expr<Raw>` in many variants via Vec/Box — all statement types that contain expressions benefit from smaller heap allocations
- **SelectStatement**, **InsertStatement**, **CreateTableStatement**, etc. all contain `Expr<Raw>` in WHERE, GROUP BY, HAVING, ORDER BY, DEFAULT, CHECK constraints
- Every SQL query parsed allocates fewer bytes for its AST representation
- The parser allocates less total memory per query

### Benchmark results

All query types work correctly with no regression (200K rows, 3 trials averaged):

| Query Type | Avg Time | Notes |
|-----------|----------|-------|
| String literals (3 per row) | ~8ms | Exercises Value::String with Box<str> |
| Numeric literals + math | ~8ms | Exercises Value::Number with Box<str> |
| Function calls (5 per row) | ~8.5ms | Exercises boxed Function<T> |
| CAST expressions (5 per row) | ~7.9ms | Exercises boxed data_type |
| Complex mixed query | ~10.6ms | Exercises all optimized paths |
| INTERVAL literals | ~8.2ms | Exercises IntervalValue with Box<str> |
| CREATE/DROP TABLE cycle | ~38ms | Exercises Statement through parser |

### Files changed (21 files)

- `src/sql-parser/src/ast/defs/value.rs` — String→Box<str> for Number, String, HexString, IntervalValue::value + size assertions
- `src/sql-parser/src/ast/defs/expr.rs` — Box Function<T>, Box data_type in Cast + size assertion + helper method updates
- `src/sql-parser/src/ast/defs/statement.rs` — Pattern matching adjustment for Value::String
- `src/sql-parser/src/parser.rs` — `.into()` conversions at ~30 Value construction sites, Box::new for Function/Cast
- `src/sql/src/plan/with_options.rs` — `.into()` conversions for Value construction/extraction
- `src/sql/src/plan/literal.rs` — Value::String extraction
- `src/sql/src/plan/query.rs` — Value construction + Function pattern matching
- `src/sql/src/plan/side_effecting_func.rs` — Restructured Function pattern matching for boxed Function
- `src/sql/src/plan/statement/ddl.rs` — Value construction
- `src/sql/src/plan/statement/show.rs` — Value construction
- `src/sql/src/plan/transform_ast.rs` — Value/Function pattern matching
- `src/sql/src/kafka_util.rs` — Value construction
- `src/sql/src/pure.rs` — Value construction at ~10 sites
- `src/sql/src/pure/mysql.rs` — Value construction
- `src/sql/src/pure/postgres.rs` — Value construction
- `src/sql/src/pure/sql_server.rs` — Value construction
- `src/adapter/src/catalog/migrate.rs` — Value construction/matching
- `src/adapter/src/coord/sequencer/inner.rs` — Value construction
- `src/adapter/src/coord/sequencer/inner/secret.rs` — Value construction
- `src/adapter/src/util.rs` — Value extraction
- `src/sqllogictest/src/runner.rs` — Value construction

### Cumulative savings across all sessions (after session 21)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| Expr\<Raw\> | 240 | 72 | 168 bytes (70%) |
| Value | 48 | 40 | 8 bytes (17%) |
| PlanNode | 384 | 104 | 280 bytes (73%) |
| Ident | 24 | 16 | 8 bytes (33%) |
| Plan (sql) | ~1888 | 184 | ~1704 bytes (90%) |
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 88 | 88 bytes (50%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |

## Session 22: Shrink Expr\<Raw\> from 72 to 64 bytes via boxing Case and Op::op String→Box\<str\>

**Date:** 2026-02-28

### Changes

Two complementary optimizations that shrink `Expr<Raw>` from 72 to 64 bytes:

1. **Box `Case` variant into `CaseExpr<T>` struct** (Case variant: 64 → 8 bytes inline)
   - Introduced `CaseExpr<T>` struct with `operand`, `conditions`, `results`, `else_result`
   - `Case { operand: Option<Box<Expr<T>>>, conditions: Vec<Expr<T>>, results: Vec<Expr<T>>, else_result: Option<Box<Expr<T>>> }` was 64 bytes inline
   - Boxing reduces to `Case(Box<CaseExpr<T>>)` = 8 bytes
   - CASE expressions are constructed once during parsing and read during planning — boxing has negligible cost

2. **`Op::op: String` → `Box<str>`** (Op: 48 → 40 bytes)
   - Operator strings ("+", "-", "=", etc.) are parsed once and never modified
   - `Box<str>` stores `(ptr, len)` vs `String`'s `(ptr, len, capacity)` — eliminates the wasted capacity field
   - Op shrinks from 48 bytes (`Option<Vec<Ident>>(24) + String(24)`) to 40 bytes (`Option<Vec<Ident>>(24) + Box<str>(16)`)

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| Expr\<Raw\> | 72 | 64 | 8 bytes (11%) |
| Op | 48 | 40 | 8 bytes (17%) |

### Why Expr\<Raw\> shrank to 64 bytes

Before: the two largest variants were:
- `Case { operand(8) + conditions(24) + results(24) + else_result(8) }` = 64 bytes
- `Op { op: Op(48) + expr1: Box(8) + expr2: Option<Box>(8) }` = 64 bytes

After:
- `Case(Box<CaseExpr<T>>)` = 8 bytes
- `Op { op: Op(40) + expr1: Box(8) + expr2: Option<Box>(8) }` = 56 bytes (now the largest variant)

With discriminant and alignment, the enum settles at 64 bytes.

### Why this is valuable

`Expr<T>` is the most numerous AST node type. Every expression in every SQL query is represented as an `Expr<T>`. The savings compound because:
- `Expr<T>` is stored recursively: every `Vec<Expr<T>>` element saves 8 bytes
- Every `Box<Expr<T>>` allocation saves 8 bytes
- CASE expressions are relatively uncommon compared to column refs, function calls, and operators — boxing the rare variant while keeping common variants unboxed is the right tradeoff
- Op's `String→Box<str>` is strictly better: no new indirection, just eliminates the unused capacity field

### Tests

All unit tests pass:
- `mz-sql-parser` lib tests: 3/3 passed (including `ast_expr_sizes` size assertion)
- `mz-sql-parser` integration tests (`sqlparser_common`): 5/5 passed
- `mz-sql-pretty` parser tests: 1/1 passed
- Full compilation: `mz-sql-parser`, `mz-sql`, `mz-sql-pretty`, `mz-adapter`, `mz-environmentd` all compile cleanly

Note: Live Materialize benchmarks could not be run due to a pre-existing stack overflow in `persist_cdc::UnopenedPersistCatalogState::open_inner` during startup (reproduces on stashed code as well). This is an infrastructure issue unrelated to our AST changes.

### Files changed (6 files)

- `src/sql-parser/src/ast/defs/expr.rs` — Introduced `CaseExpr<T>` struct, boxed Case variant, `Op::op: String` → `Box<str>`, size assertions (Op 40, Expr\<Raw\> 64)
- `src/sql-parser/src/parser.rs` — `Expr::Case(Box::new(CaseExpr { ... }))` construction, `Op::bare` takes `Into<Box<str>>`, operator string conversion
- `src/sql-parser/tests/testdata/scalar` — Updated expected test output for boxed Case debug format
- `src/sql-pretty/src/doc.rs` — Pattern matching adjustment for `Expr::Case(case)`
- `src/sql/src/plan/query.rs` — Pattern matching adjustments for boxed Case (2 sites)
- `src/sql/src/plan/transform_ast.rs` — `Expr::Case(Box::new(CaseExpr { ... }))` construction (3 sites)

### Cumulative Expr\<Raw\> savings (sessions 21 + 22)

| Type | Original | After session 21 | After session 22 | Total savings |
|------|----------|------------------|------------------|---------------|
| Expr\<Raw\> | 240 | 72 | 64 | 176 bytes (73%) |

### Cumulative savings across all sessions (after session 22)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| Expr\<Raw\> | 240 | 64 | 176 bytes (73%) |
| Value | 48 | 40 | 8 bytes (17%) |
| PlanNode | 384 | 104 | 280 bytes (73%) |
| Ident | 24 | 16 | 8 bytes (33%) |
| Plan (sql) | ~1888 | 184 | ~1704 bytes (90%) |
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 88 | 88 bytes (50%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |
| Op | 48 | 32 | 16 bytes (33%) |

## Session 23: Shrink Op from 40 to 32 bytes and Expr\<Raw\> from 64 to 56 bytes via boxed slice namespace + walkabout slice support

**Date:** 2026-02-28

### Changes

Two complementary changes:

1. **`Op::namespace: Option<Vec<Ident>>` → `Option<Box<[Ident]>>`** (Op: 40 → 32 bytes)
   - `Vec<Ident>` is 24 bytes (ptr + len + capacity); `Box<[Ident]>` is 16 bytes (ptr + len)
   - `Option<Vec<Ident>>` uses the null pointer niche: 24 bytes; `Option<Box<[Ident]>>` uses the null pointer niche: 16 bytes
   - Op shrinks from 40 bytes (`Option<Vec<Ident>>(24) + Box<str>(16)`) to 32 bytes (`Option<Box<[Ident]>>(16) + Box<str>(16)`)
   - `namespace` is almost always `None` (only used for `OPERATOR(schema.op)` syntax) and never modified after construction

2. **Added `Box<[T]>` (boxed slice) support to the walkabout AST code generator**
   - Added `Type::Slice(Box<Type>)` variant to the walkabout IR
   - Added `syn::Type::Slice` handling in `analyze_type` to recognize `[T]` types
   - Updated fold codegen: `Box<[T]>` folds via `.into_vec().into_iter().map(fold).collect::<Vec<_>>().into_boxed_slice()`
   - Updated visit/visit_mut codegen: `Box<[T]>` iterates via `.iter()` / `.iter_mut()`
   - This is a one-time investment that enables `Box<[T]>` in any future AST type optimizations

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| Op | 40 | 32 | 8 bytes (20%) |
| Expr\<Raw\> | 64 | 56 | 8 bytes (12.5%) |

### Why Expr\<Raw\> shrank to 56 bytes

Before: 5 variants embedded `Op(40)` inline, making them 56 bytes each (the largest):
- `Op { op: Op(40), expr1: Box(8), expr2: Option<Box>(8) }` = 56 bytes
- `AnySubquery { left: Box(8), op: Op(40), right: Box(8) }` = 56 bytes
- `AnyExpr`, `AllSubquery`, `AllExpr` — same pattern

After: All 5 variants shrink to 48 bytes:
- `Op { op: Op(32), expr1: Box(8), expr2: Option<Box>(8) }` = 48 bytes
- `AnySubquery { left: Box(8), op: Op(32), right: Box(8) }` = 48 bytes

With discriminant and alignment, the enum settles at 56 bytes.

### Why this is high-impact

`Expr<T>` is the most numerous AST node type — every expression in every SQL query. The savings compound because:
- `Expr<T>` is stored recursively: every `Vec<Expr<T>>` element saves 8 bytes (function args, IN lists, CASE conditions/results, etc.)
- Every `Box<Expr<T>>` allocation saves 8 bytes (unary/binary ops, subquery arms, etc.)
- A moderately complex query contains 10-50+ Expr nodes, saving 80-400+ bytes per query

### Cascading effects

- **Statement\<Raw\>** embeds `Expr<Raw>` in many variants — all statement types that contain expressions benefit
- **SelectStatement**, **InsertStatement**, **CreateTableStatement**, etc. all benefit
- Every SQL query parsed allocates fewer bytes for its AST
- The walkabout `Box<[T]>` support enables future AST type optimizations without additional codegen changes

### Tests

All tests pass:
- `mz-sql-parser` lib tests: all 3 passed (including `ast_expr_sizes` size assertion: Op=32, Expr\<Raw\>=56)
- `mz-sql-parser` integration tests: 5/5 passed
- `mz-sql-parser` doc tests: 13/13 passed (including fold, visit, visit_mut — exercises walkabout-generated code)
- `mz-sql-pretty` tests: 1/1 passed
- `mz-sql` lib tests: 7/7 passed (including `type_size_assertions` and HIR size tests)
- Full compilation: `mz-sql-parser`, `mz-sql`, `mz-sql-pretty`, `mz-adapter`, `mz-environmentd` all compile cleanly

Note: Live Materialize benchmarks could not be run due to a pre-existing panic in `persist_cdc` during startup (reproduces on stashed code as well). This is an infrastructure issue unrelated to our AST changes.

### Files changed (4 files)

- `src/sql-parser/src/ast/defs/expr.rs` — `Op::namespace: Option<Vec<Ident>>` → `Option<Box<[Ident]>>`, size assertions updated (Op 40→32, Expr\<Raw\> 64→56)
- `src/sql-parser/src/parser.rs` — `namespace.into_boxed_slice()` at `parse_operator` construction site
- `src/walkabout/src/ir.rs` — Added `Type::Slice(Box<Type>)` variant, `syn::Type::Slice` handling in `analyze_type`, added `Slice` to the container unwrap chain
- `src/walkabout/src/generated.rs` — Fold/visit/visit_mut codegen for `Type::Slice` and `Type::Box(Type::Slice(...))`: fold via `.into_vec().into_iter().map(fold).collect().into_boxed_slice()`, visit via `.iter()`/`.iter_mut()`

### Cumulative Expr\<Raw\> savings (sessions 21 + 22 + 23)

| Type | Original | After session 21 | After session 22 | After session 23 | Total savings |
|------|----------|------------------|------------------|------------------|---------------|
| Expr\<Raw\> | 240 | 72 | 64 | 56 | 184 bytes (77%) |

### Cumulative savings across all sessions (after session 23)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| Expr\<Raw\> | 240 | 56 | 184 bytes (77%) |
| Value | 48 | 40 | 8 bytes (17%) |
| PlanNode | 384 | 104 | 280 bytes (73%) |
| Ident | 24 | 16 | 8 bytes (33%) |
| Plan (sql) | ~1888 | 184 | ~1704 bytes (90%) |
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 88 | 88 bytes (50%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |
| Op | 48 | 32 | 16 bytes (33%) |

## Session 24: Shrink RowSetFinishing from 72 to 56 bytes via Vec→Box<[T]>

**Date:** 2026-02-28

### Changes

Convert two `Vec<T>` fields in `RowSetFinishing` to `Box<[T]>`, eliminating the unused capacity field. These fields are constructed once during planning and never grown afterward.

1. **`order_by: Vec<ColumnOrder>` → `Box<[ColumnOrder]>`** (saves 8 bytes: 24→16)
   - Order-by columns are set during planning and never modified after construction
   - `Box<[T]>` stores `(ptr, len)` vs `Vec<T>`'s `(ptr, len, capacity)`

2. **`project: Vec<usize>` → `Box<[usize]>`** (saves 8 bytes: 24→16)
   - Projection columns are set during planning and never modified after construction
   - Same `Vec→Box<[T]>` pattern used successfully in sessions 7, 9, 13, 14, 17

### Key insight: cascading effect on Plan enum

`RowSetFinishing` is stored inline in `SelectPlan`, which was the largest unboxed variant of the `Plan` enum at 184 bytes. Shrinking RowSetFinishing by 16 bytes shrinks SelectPlan from 184 to 168 bytes. However, `CreateClusterPlan` (176 bytes, also unboxed) is now the largest variant, so the Plan enum settles at 176 bytes instead of 168.

### Size measurements (before → after)

| Type | Before | After | Savings |
|------|--------|-------|---------|
| RowSetFinishing | 72 | 56 | 16 bytes (22%) |
| SelectPlan | 184 | 168 | 16 bytes (9%) |
| Plan (sql) | 184 | 176 | 8 bytes (4%) |

### Implementation note: preserving try_push_projection_order_by

The `try_push_projection_order_by` function mutates `project` and `order_by` fields via `&mut Vec<T>`. Rather than changing its signature (which would also affect an internal query planner call site with local `Vec<T>` variables), the RowSetFinishing construction was restructured: `try_push_projection_order_by` is now called *before* constructing the finishing, while the `project` and `order_by` variables are still `Vec<T>`. The results are then converted to `Box<[T]>` via `.into_boxed_slice()` when constructing the finishing.

### Cascading effects

- **SelectPlan** shrinks from 184 to 168 bytes — every `SELECT` statement uses less stack space
- **Plan enum** shrinks from 184 to 176 bytes — every SQL statement uses 8 fewer bytes
- Every `RowSetFinishing` instance (in optimizer, coordinator, compute workers) saves 16 bytes
- `RowSetFinishing` is stored in `PeekContext`, `ReadThenWritePlan`, and passed to compute workers — all benefit

### Tests

All unit tests pass:
- `mz-expr` lib tests: 50/50 passed (including `type_size_assertions` with RowSetFinishing=56)
- `mz-sql` lib tests: 7/7 passed (including `type_size_assertions` with Plan=176, SelectPlan=168)
- Full compilation: `mz-expr`, `mz-sql`, `mz-adapter`, `mz-compute`, `mz-environmentd` all compile cleanly

Note: Live Materialize benchmarks could not be run due to CockroachDB not being available (same infrastructure issue as sessions 22-23).

### Files changed (7 files)

- `src/expr/src/relation.rs` — `order_by: Vec<ColumnOrder>` → `Box<[ColumnOrder]>`, `project: Vec<usize>` → `Box<[usize]>`, trivial() constructor, `.to_vec()` for with_projection, size assertion
- `src/sql/src/plan.rs` — Plan size assertion 184→176, added SelectPlan=168 assertion
- `src/sql/src/plan/query.rs` — Restructured plan_root_query/plan_ct_query to call try_push_projection_order_by before constructing finishing, `.into_boxed_slice()` at 3 construction sites
- `src/sql/src/plan/hir.rs` — trivial_row_set_finishing_hir Box::default()/into_boxed_slice(), `.into_vec()` for top_k/project consumption
- `src/adapter/src/coord/sequencer.rs` — Box::default()/into_boxed_slice() at construction site
- `src/adapter/src/coord/sequencer/inner.rs` — Box::default()/into_boxed_slice() at construction site
- `src/adapter/src/coord/peek.rs` — `.into_vec()` for RowSetFinishingIncremental::new

### Cumulative savings across all sessions (after session 24)

| Type | Original | After all sessions | Total savings |
|------|----------|-------------------|---------------|
| Expr\<Raw\> | 240 | 56 | 184 bytes (77%) |
| Value | 48 | 40 | 8 bytes (17%) |
| PlanNode | 384 | 104 | 280 bytes (73%) |
| Ident | 24 | 16 | 8 bytes (33%) |
| Plan (sql) | ~1888 | 176 | ~1712 bytes (91%) |
| SelectPlan | 184 | 168 | 16 bytes (9%) |
| RowSetFinishing | 72 | 56 | 16 bytes (22%) |
| MirScalarExpr | 88 | 48 | 40 bytes (45%) |
| MirRelationExpr | 176 | 88 | 88 bytes (50%) |
| HirScalarExpr | 192 | 80 | 112 bytes (58%) |
| HirRelationExpr | 456 | 72 | 384 bytes (84%) |
| AggregateFunc | 88 | 48 | 40 bytes (45%) |
| AggregateExpr | 184 | 104 | 80 bytes (43%) |
| UnaryFunc | 72 | 32 | 40 bytes (56%) |
| BinaryFunc | 48 | 24 | 24 bytes (50%) |
| VariadicFunc | 40 | 24 | 16 bytes (40%) |
| TableFunc | 80 | 40 | 40 bytes (50%) |
| EvalError | 56 | 40 | 16 bytes (29%) |
| JoinImplementation | 120 | 64 | 56 bytes (47%) |
| SqlScalarType | 32 | 24 | 8 bytes (25%) |
| SqlColumnType | 40 | 32 | 8 bytes (20%) |
| Matcher | 72 | 64 | 8 bytes (11%) |
| ReprRelationType | 48 | 40 | 8 bytes (17%) |
| ColumnOrder | 16 | 8 | 8 bytes (50%) |
| Op | 48 | 32 | 16 bytes (33%) |
