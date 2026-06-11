---
source: src/expr/src/scalar/reduce/variadic.rs
revision: ed05cf7584
---

# mz-expr::scalar::reduce::variadic

Post-order rewrites for `CallVariadic` nodes, called from `reduce::reduce_post`.

## Entry point

`reduce_call_variadic(e, column_types, temp_storage)` requires `e` to be a `CallVariadic` node. It applies rewrites in the following order:

### Associative flattening

`flatten_associative` is called first to merge nested calls of the same associative function (e.g. `And(And(a, b), c)` → `And(a, b, c)`). This normalization is a prerequisite for the `undistribute_and_or` step below.

### Coalesce (early exit)

If the function is `Coalesce`, control is handed off to `simplify_coalesce` and the function returns. `simplify_coalesce` applies five steps in sequence:
1. If all arguments are null, replace with a typed `NULL`.
2. Drop null arguments (they can never be the result).
3. Truncate after the first argument known to be non-null (a literal or a non-nullable column).
4. Deduplicate arguments (e.g. `coalesce(#0, #0)` → `coalesce(#0)`).
5. Unwrap a single-argument `coalesce` to the argument itself.

### Generic folds

- **Constant folding** — if all arguments are literals, evaluate and replace with a literal.
- **Null propagation** — if the function propagates nulls and any argument is `NULL`, replace with a typed `NULL`.
- **Error propagation** — if any argument is a literal error, propagate that error.

### Per-function specializations

Applied when the relevant arguments are literals:

- `Greatest` / `Least` — calls `reduce_greatest_least`: deduplicates structurally equal operands (keeping the first occurrence), drops literal null operands (both functions ignore nulls), and collapses a single-operand call to the identity and a zero-operand call to a typed `NULL`.
- `Substr` (two-argument form) with a literal start of `1` — the call keeps the entire string and is infallible at that start, so the call is replaced with the string operand.
- `RegexpMatch` with literal pattern (and optional flags) — compiles the regex via `build_regex` and converts to `UnaryFunc::RegexpMatch(regex)`, or a literal error.
- `RegexpReplace` with a literal pattern (and optional flags) — compiles the regex and reduces to a `CallBinary(RegexpReplace { regex, limit }, source, replacement)`. On a regex compilation error, produces an `if_then_else` that returns `NULL` when source or replacement is `NULL` and the error otherwise, preserving the SQL semantics that `NULL` input yields `NULL` output.
- `RegexpSplitToArray` with a literal pattern (and optional flags) — compiles the regex and converts to `UnaryFunc::RegexpSplitToArray(regex)`.
- `ListIndex(ListCreate, ...)` with at least one literal index — partially evaluates the list indexing via `reduce_list_create_list_index_literal`, which walks through multi-dimensional nested `ListCreate` expressions and collapses as many literal index levels as possible in a single pass, leaving a residual `ListIndex` if non-literal indices remain.
- `And` / `Or` — applies `undistribute_and_or` to factor out common subexpressions, followed by `reduce_and_canonicalize_and_or` to remove duplicates and absorb dominating literals (`true` for `Or`, `false` for `And`).
- `TimezoneTimeVariadic` with a literal timezone and a literal-ok wall-time argument — parses the timezone (POSIX spec) and converts to `UnaryFunc::TimezoneTime { tz, wall_time }` applied to the time argument.
