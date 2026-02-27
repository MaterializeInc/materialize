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

### Future optimization ideas (roadmap)

- **Box large `AggregateFunc` variants**: `FusedWindowAggregate` (88 bytes), `WindowAggregate` (72 bytes), `FirstValue`/`LastValue` (64 bytes) drive AggregateFunc to 88 bytes. Boxing these 4 window-frame variants would reduce AggregateFunc from 88 to ~64 bytes (27%) and AggregateExpr from 168 to ~144 bytes.
- **Box large `EvalError` variants**: `DateDiffOverflow` and `OutOfDomain` at 48 bytes each drive EvalError to 56 bytes. Boxing these would reduce EvalError to ~32 bytes, which further shrinks Result<Row, EvalError> and could enable un-boxing the Literal Result.
- **`Vec<T>` → `Box<[T]>` conversions**: Many plan struct fields use `Vec<T>` but never grow after construction. Converting to `Box<[T]>` saves 8 bytes per field (24→16). Key targets: `SqlRelationType::column_types`, `SqlRelationType::keys`, `LinearMfp` fields, various plan structures.
- **`TableFunc` (80 bytes)**: Likely has large variants that could be boxed.
- **`MirRelationExpr` (176 bytes)**: The `LetRec`, `Join`, and `Reduce` variants are very large and could be boxed.
