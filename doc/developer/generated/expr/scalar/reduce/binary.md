---
source: src/expr/src/scalar/reduce/binary.rs
revision: 5d046b3ab6
---

# mz-expr::scalar::reduce::binary

Post-order rewrites for `CallBinary` nodes, called from `reduce::reduce_post`.

## Entry point

`reduce_call_binary(e, column_types, temp_storage)` requires `e` to be a `CallBinary` node. It applies rewrites in two stages:

### Generic folds (applied first)

These fire before any per-function dispatch and short-circuit with a `return`:

- **Constant folding** — if both operands are literals, evaluate the entire expression and replace it with a literal.
- **Null propagation** — if either operand is `NULL` and the function propagates nulls, replace with a typed `NULL` literal.
- **Error propagation** — if either operand is a literal error, propagate that error as a literal.

### Per-function specializations

Applied only when the relevant operand is a literal. Each converts the two-argument binary call into a cheaper unary call with the parsed/compiled value baked in:

- `IsLikeMatch{CaseSensitive,CaseInsensitive}` with a literal pattern — precompiles the LIKE pattern via `like_pattern::compile` into `IsLikeMatch(matcher)`.
- `IsRegexpMatch{CaseSensitive,CaseInsensitive}` with a literal pattern — compiles the regex and converts to `IsRegexpMatch(regex)`.
- `Extract{Interval,Time,Timestamp,TimestampTz,Date}` and `DatePart{Interval,Time,Timestamp,TimestampTz}` and `DateTrunc{Timestamp,TimestampTz}` with a literal units string — parses the `DateTimeUnits` and converts to the corresponding unary form, or a literal `UnknownUnits` error.
- `TimezoneTimestamp{,Tz}Binary` with a literal timezone string — parses the `Timezone` (POSIX spec) and converts to the unary `TimezoneTimestamp{,Tz}` form.
- `ToChar{Timestamp,TimestampTz}` with a literal format string — compiles the `DateTimeFormat` and converts to the unary form with the format string and compiled format baked in.
- `Eq` / `NotEq` when operands are out of canonical order — swaps them so that deduplication and the record-equality matching below work correctly.
- All other cases — delegates to `reduce_call_binary_eq_record`.

### Record equality decomposition (`reduce_call_binary_eq_record`)

Handles two structural patterns for `Eq`:

- `Literal([c1, c2, ...]) = RecordCreate(e1, e2, ...)` → `c1 = e1 AND c2 = e2 AND ...`. This is required by `MapFilterProject::literal_constraints` because `(e1, e2) IN ((1, 2))` is desugared via `RecordCreate`.
- `RecordCreate(a1, a2, ...) = RecordCreate(b1, b2, ...)` → `a1 = b1 AND a2 = b2 AND ...`. Enables literal-constraint discovery even when record fields are not all literals.
