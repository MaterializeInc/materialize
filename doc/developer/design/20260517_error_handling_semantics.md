# Error handling semantics

* Associated: TBD (no open issue yet; this doc establishes the model)

## The problem

Materialize today has two error pathways and a gap between them.
`DataflowError` propagates row-level failures through a parallel error collection, allowing dataflows to surface failed records without halting computation.
`EvalError` describes scalar failures but, when raised, escalates the whole row to `DataflowError::EvalError` and removes the row from the output collection.
There is no first-class representation of a cell-level error inside a `Row`, and there is no representation of a collection-level error attached to a logical operator output.

The absence of cell-level errors forces ingestion and casting paths to either reject records, coerce values to types they cannot represent, or route columns through `text` and defer parsing.
Concrete cases this hurts include MySQL/TiDB `0000-00-00` zero-dates that cannot be represented as `Datum::Date`, JSON casts that fail mid-row, decimal overflow inside a `SELECT` list, and any user-defined coercion that may fail on a subset of inputs.
A user who wants the rest of the row preserved must today either filter the data upstream or model the column as `text` and reparse it at query time.
This is the same friction that motivated `try_cast`, but applied at the storage layer rather than at the cast site.

Globally scoped errors — failures whose blast radius is the whole collection rather than a single row — also lack a uniform representation.
A `WHERE` predicate that errors on a single row is a row-level error today, but the same predicate evaluated as part of a join condition or aggregation can produce semantics that escape any single row.
Differential dataflow's natural locus for "this collection is invalid at time `t`" is the `diff` field, which Materialize currently does not use this way.
A spec for global errors is needed so that future work has a consistent target rather than an ad-hoc encoding per operator.

## Success criteria

A solution is successful when the following hold.

* Cell-level failures can be represented inside a `Row` without forcing the row out of the output collection.
* Row-level failures continue to be represented by the existing error collection without behavior change for in-place dataflows.
* Collection-level failures (global errors) have a defined semantics and a defined encoding, even if the encoding is not yet implemented.
* SQL evaluation rules for `NULL`, errors, and short-circuiting are written down in one place and match PostgreSQL where reasonable, with deviations called out explicitly.
* Existing data and existing dataflows continue to read and run after the new variant is added.

## Out of scope

The following are intentionally not addressed here.

* The wire-format migration plan for adding a new `Datum` tag.
This is implementation work whose shape depends on which encoding option is chosen.
* The exact set of operators that should be made error-aware in the first iteration.
That is a planning concern, not a semantic one.
* User-facing SQL syntax for introspecting or filtering on errors (`is_error`, `error_message`, `try_*`).
The semantics defined here support such syntax, but choosing the surface is a separate design.
* Cancellation of in-flight queries due to global errors.
A dataflow exposing a global error is a steady-state concept; cancellation is orthogonal.

## Solution proposal

The proposal introduces three error scopes and assigns each a representation.

### Error scopes

Errors are classified by the smallest unit of output they invalidate.

* **Cell-scoped**: the error invalidates a single `Datum` within a single `Row`.
The rest of the row is still well-defined.
Example: cast overflow on a single column inside a `SELECT` list.
* **Row-scoped**: the error invalidates a single row.
No `Datum` in the row is well-defined, but the rest of the collection is.
Example: a decoding error on a Kafka record, or a key-conflict in an upsert envelope.
* **Global-scoped**: the error invalidates the entire output collection at some time `t`.
No row at that time is well-defined.
Example: a `WHERE clause` whose evaluation depends on collection-wide state that has become invalid, or a sink whose downstream contract has been violated.

Classification is a property of the operator producing the error, not of the underlying `EvalError`.
The same arithmetic overflow is cell-scoped when raised inside a `SELECT` projection and row-scoped when raised inside a `WHERE` predicate.
Operators are responsible for choosing the smallest scope that faithfully represents their semantics.

### Cell-scoped errors: `Datum::Error`

A new `Datum::Error(Box<EvalError>)` variant is added.
The variant participates in all `Row` encoding paths and is propagated by expressions according to the rules in the SQL semantics section.
Operators that produce a row may produce `Datum::Error` in any position where a value of any type is expected.
Operators that consume a row must either propagate the error, trap it via an explicit operator (`try_*`, `coalesce`-style), or escalate it to a row-scoped error.

The type system treats `Datum::Error` as inhabiting every `ScalarType`.
This mirrors the way `NULL` inhabits every nullable type.
The variant carries an `EvalError`, not a string, so that error introspection functions can be added later without a format break.

### Row-scoped errors: `DataflowError` plus diff-encoded multiplicities

`DataflowError` continues to carry row-scoped errors through the existing error collection.
The semantics of the existing collection are unchanged.
An operator that wishes to escalate a `Datum::Error` to row scope does so by emitting a `DataflowError::EvalError` and dropping the row from the data collection, as today.

Operators that produce row-scoped errors from per-row evaluation — `WHERE`, join predicates, projection scalars — face a tension that the existing `DataflowError` pathway does not resolve.
The erroring expression's result (for example `1/(a+b)` with `a+b = 0`) has no column in the row to live in.
Replacing the row with an opaque error marker loses the row's content, which downstream operators may still need to count, consolidate, or retract.
Routing the failure to `DataflowError` and dropping the row from the data collection has the same effect: the data row is gone, even though the row's data was otherwise well-defined.

The spec therefore extends the `diff` field with a per-error-kind multiplicity component so that row-scoped errors retain row content and participate in normal diff arithmetic.
A diff is the pair

```
Diff = Int × ErrCount
ErrCount = EvalError -fin-> Int
```

where `Int` is the valid-copy multiplicity and `ErrCount` is the finite-support map from error payloads to error-copy multiplicities.
Addition is pointwise on both axes.
Multiplication (used by join and cross) is `(a, m) * (b, n) = (a*b, a • n + b • m)` where `a • n` scales every err-count in `n` by the scalar `a`; this is the unique extension that distributes over addition and preserves the absorbing role of `0`.
Negation is pointwise: `-(a, m) = (-a, -m)`, where `-m` is the pointwise negation of the err-count map.
Zero is `(0, ∅)`; one is `(1, ∅)`.

Under this encoding, a `WHERE` predicate that errors with `EvalError::DivisionByZero` on a row carrying diff `(a, m)` produces an output record with the row content preserved and diff `(0, m ∪ {DivisionByZero ↦ a})`.
A later retraction of the same row produces the negated diff and the err-count cancels through ordinary differential-dataflow consolidation.
Two distinct rows that happen to produce the same `EvalError` accumulate in the same err-count slot; an `IS DISTINCT FROM`-style query that distinguishes them can still do so via the row content, which the carrier has not touched.

`DataflowError` and the diff-encoded multiplicities are complementary, not redundant.
A sink that cannot emit error rows still escalates to `DataflowError` and drops the row.
An intermediate operator that wants to keep counting through an erroring evaluation uses the diff-encoded component and leaves `DataflowError` alone.
The escalation rule is explicit: an operator opts into `DataflowError` when its downstream contract requires it.

### Global-scoped errors: absorbing diff marker (specification only)

A global error at time `t` is encoded as a distinguished record whose diff carries an absorbing element.
The absorbing element sits outside the `Int × ErrCount` pair, layered as

```
DiffWithGlobal = val(Diff) | global
```

with `global` absorbing both addition and multiplication: `global + d = global`, `d + global = global`, `global * d = global`, `d * global = global`.
Any sum involving `global` is itself `global`, which is exactly the propagation rule a downstream operator needs.
Unlike the row-scoped err-count component, `global` is terminal: a `global` marker at time `t` cannot be retracted, because the claim "this collection is invalid at `t`" is one-way.

The intent is that any downstream operator observing a `global` record at time `t` treats the entire input collection at `t` as invalid, propagating the global error to its own output.
The natural locus is still the diff field, because the data field is per-row and the time field is per-update, and an absorbing monoid extension of the diff semiring captures exactly the propagation rule required.

Implementation is out of scope.
The spec exists so that future operator work targets this two-layer encoding (`Int × ErrCount` for retractable row-scoped, `global` for terminal collection-scoped) rather than inventing alternates.

### SQL error semantics

The rules below define how `NULL` and `Datum::Error` interact in expression evaluation.
The intent is to match PostgreSQL behavior where PostgreSQL has behavior, and to extend it where PostgreSQL has none because PostgreSQL has no first-class cell error.

**Scalar function evaluation.**
A strict function applied to any `Datum::Error` argument returns `Datum::Error`.
A strict function applied to `NULL` returns `NULL`, as today.
If a strict function receives both `NULL` and `Datum::Error` arguments, it returns `Datum::Error`.
This rule matches the principle that errors are stronger than `NULL`: `NULL` denotes "unknown value", error denotes "the value cannot exist".

**Non-strict functions.**
`coalesce(a, b)` returns the first non-`NULL`, non-error argument, evaluating left to right.
If all arguments are `NULL` or error, the result is `NULL` if any argument was `NULL` and all errors were unreached, otherwise the first error.
This generalizes PostgreSQL `coalesce` so that a fallback can rescue an error in the same way it rescues a `NULL`.
Short-circuit boolean operators evaluate per the truth table below.

**Boolean three-valued logic, extended.**
`AND` and `OR` are extended from PostgreSQL's three-valued logic to four values: `TRUE`, `FALSE`, `NULL`, `ERROR`.
The extension is conservative: any cell that PostgreSQL would have produced as `NULL` is still `NULL`, and `ERROR` participates only when an operand is an actual error.

| `AND`   | TRUE  | FALSE | NULL  | ERROR |
|---------|-------|-------|-------|-------|
| TRUE    | TRUE  | FALSE | NULL  | ERROR |
| FALSE   | FALSE | FALSE | FALSE | FALSE |
| NULL    | NULL  | FALSE | NULL  | NULL  |
| ERROR   | ERROR | FALSE | NULL  | ERROR |

| `OR`    | TRUE | FALSE | NULL  | ERROR |
|---------|------|-------|-------|-------|
| TRUE    | TRUE | TRUE  | TRUE  | TRUE  |
| FALSE   | TRUE | FALSE | NULL  | ERROR |
| NULL    | TRUE | NULL  | NULL  | NULL  |
| ERROR   | TRUE | ERROR | NULL  | ERROR |

`FALSE AND ERROR` is `FALSE`, and `TRUE OR ERROR` is `TRUE`, because the result is determined without inspecting the erroring operand.
`NULL AND ERROR` and `NULL OR ERROR` collapse to `NULL`, preserving PostgreSQL's bias toward `NULL` when ignorance subsumes the question.

**Predicates.**
A `WHERE` clause emits a row when its predicate evaluates to `TRUE`.
It drops the row when the predicate is `FALSE` or `NULL`, as today.
When the predicate is `ERROR`, the row is recorded with diff `(0, {e ↦ a})` where `e` is the error and `a` is the input row's valid-copy multiplicity; the row content is preserved and the err-count participates in retraction via the row-scoped diff encoding described above.
A downstream operator that must escalate to `DataflowError` does so explicitly; the default behavior preserves the row.

**Comparison.**
`=`, `<`, `>`, etc., applied to `Datum::Error` return `Datum::Error`.
`IS DISTINCT FROM` treats `Datum::Error` as distinct from any other value including another `Datum::Error` carrying the same inner error, on the grounds that the equality of two errors is itself ill-defined.
`IS NULL` returns `FALSE` on `Datum::Error`, mirroring the rule that error and `NULL` are distinct.
A future `IS ERROR` predicate would return `TRUE` on `Datum::Error` and `FALSE` otherwise.

**Aggregates.**
`COUNT(*)` counts rows regardless of cell contents; `COUNT(expr)` counts rows where `expr` is neither `NULL` nor error.
`SUM`, `AVG`, `MIN`, `MAX`, and similar reductions return `Datum::Error` if any input cell is `Datum::Error`, with the inner `EvalError` chosen by an operator-defined rule (typically the first error in scan order; the rule must be deterministic given a fixed input).
This matches the principle that errors are stronger than `NULL` and the principle that aggregates should not silently hide failures.
An explicit opt-out is provided by future `try_sum`-style aggregates.

**Grouping.**
`GROUP BY` treats `Datum::Error` as a distinct group key, with the same equality semantics as `IS DISTINCT FROM`: every error is its own group.
This avoids accidentally collapsing unrelated failures into a single aggregate output.

**Joins.**
A join predicate evaluating to `ERROR` produces an output row whose diff carries the err-count component, symmetric with the `WHERE` rule and using the multiplicative rule on diffs to combine the contributions of the two input sides.
A downstream operator that must escalate to `DataflowError` does so explicitly.
Join keys containing `Datum::Error` do not match any other key, including identical `Datum::Error` values, mirroring the grouping rule.

**Casts and `try_cast`.**
A cast that would today raise `EvalError` now also has the option of producing `Datum::Error` when invoked from a context that has opted in to cell-scoped failures.
`try_cast` continues to return `NULL` on failure for backward compatibility.
A new variant such as `try_cast_error` could return `Datum::Error`; choosing the surface is out of scope.

### Operator obligations

Each operator falls into one of three categories.

* **Error-transparent**: passes `Datum::Error` through unchanged in the cells where it appears.
Most projection-style operators are transparent.
* **Error-aware**: inspects `Datum::Error` and produces a defined result.
Examples: `coalesce`, `IS NULL`, future `IS ERROR`, `try_*`.
* **Error-escalating**: converts a cell-scoped error to a row-scoped error.
Examples: `WHERE`, join predicates, sink output (a sink cannot emit a row containing `Datum::Error` to a downstream system, so it must escalate).

Operators document which category they fall into.
Default for a new operator is transparent unless it has a reason to be aware or escalating.

### Worked example: TiDB zero-date

MySQL and TiDB allow `0000-00-00` as a fallback when permissive `sql_mode` rejects an invalid date.
The value is not equivalent to `NULL`: a `NOT NULL` column can contain `0000-00-00` alongside `NULL` columns elsewhere.
Materialize's `Date` type cannot represent `0000-00-00` (no year 0, no month 0, no day 0).

Under the proposed model the MySQL source decodes `Value::Date(0, 0, 0, ...)` into `Datum::Error(EvalError::InvalidDate)` for `SqlScalarType::Date` columns.
The row is emitted intact; downstream queries that touch only other columns succeed.
A query that projects the date column sees `Datum::Error`, which propagates per the rules above.
A user who wants to coalesce can write `coalesce(d, DATE '1970-01-01')` if they have opted into error-aware coalesce, preserving the distinction between zero-date and `NULL` while still producing a usable timestamp downstream.
The TEXT COLUMN escape hatch remains available but is no longer the only correct ingestion path.

## Minimal viable prototype

A prototype consists of three steps, none of which require the full migration to land.

* Add `Datum::Error(Box<EvalError>)` behind a feature flag, wired through `Row` packing and `RowArena` allocation, gated so that existing rows cannot contain the variant.
* Implement the strict-function propagation rule and the extended boolean truth tables in `MirScalarExpr` evaluation, with unit tests covering each cell of each table.
* Wire the MySQL source decoder to emit `Datum::Error` on `Value::Date(0, 0, 0, ...)` for `SqlScalarType::Date` columns, and verify end-to-end that a `SELECT` over an unaffected column returns the row while a `SELECT` over the date column returns the error.

The prototype intentionally omits global errors, sinks, and aggregates; those land in subsequent PRs with their own tests.

## Alternatives

**Status quo plus TEXT COLUMN guidance.**
Document that columns with permissive upstream `sql_mode` must be ingested as `text`.
Cheap, but pushes parsing to query time forever and does not generalize to other cell-failure sources (overflow, JSON cast, decimal precision).

**Datum-level `NULL` overload.**
Coerce zero-dates to `NULL` at ingestion.
Loses the distinction between user-intended `NULL` and ingestion-rejected value.
Violates the spec rule that error is stronger than `NULL`.
Rejected on correctness grounds.

**String error wrapper instead of `EvalError`.**
Store `Datum::Error(Box<str>)` rather than `Datum::Error(Box<EvalError>)`.
Simpler to encode but loses structured error information; introspection functions would parse a string.
Rejected on extensibility grounds.

**Error rows in the data collection rather than a separate variant.**
Tag the row itself with a per-column error bitmask, leaving `Row` as today.
Saves a `Datum` variant but introduces an out-of-band channel that operators must thread through every transformation.
Rejected on uniformity grounds; the `Datum` variant is the same place every existing operator already inspects.

**Global errors via a sidecar collection rather than the `diff` field.**
Carry global errors in a separate timely stream.
Works, but requires every operator to be aware of two error inputs.
The `diff`-field encoding leverages differential dataflow's existing fan-in and is the natural extension of the semiring.

**Row-scoped errors via carrier replacement rather than a diff component.**
Encode a row-scoped error by replacing the row with an opaque error marker in the data field while keeping the diff a plain `Int`.
This is the simplest extension but loses the row's content at the carrier flip, so any downstream operator that needed the original row data (for join, group-by on other columns, or counting) cannot recover it.
The `diff`-encoded `ErrCount` keeps the row and counts the error as a parallel multiplicity, which preserves both pieces of information and stays retractable through ordinary diff arithmetic.

**Row-scoped errors via an absorbing diff marker like global errors.**
Use the same absorbing-monoid extension as the global-scoped case.
Rejected because per-row evaluation errors must be retractable: a row that errs on `WHERE 1/(a+b) > 5` at time `t` and is retracted at time `t'` must net to zero in the consolidated view.
An absorbing marker cannot retract.
The two-layer encoding (`Int × ErrCount` for row-scoped, `global` for collection-scoped) keeps the algebra of each scope faithful to its semantics.

## Open questions

* What is the exact set of `EvalError` payloads that operators may produce as `Datum::Error`?
Some `EvalError` variants (out-of-memory, environment errors) make no sense at cell scope.
* How does `Datum::Error` interact with persisted state and version skew?
A reader on an older binary that encounters `Datum::Error` must have a defined behavior; the default proposal is to surface the row as a row-scoped error in the read path.
* Should ordering operators have a defined sort position for `Datum::Error`?
PostgreSQL has no precedent; candidates are "errors sort last", "errors sort like `NULL`", and "errors are unordered and produce a sort-key error".
* What is the storage cost of widening `Row` encoding to include the new tag, and how does it compare to the current cost of routing failures through the error collection?
* For global errors, what is the precise specification of the absorbing element in the `diff` semiring, and how does it interact with consolidation and arrangement?
* For row-scoped errors carried in the diff's `ErrCount` component, what is the storage / wire cost of a finite map from `EvalError` to `Int` versus the existing single-`Int` diff, and is the cost acceptable for collections in which errors are rare?
* For row-scoped errors carried in the diff, how does the encoding interact with existing arrangement layouts that assume `Int` diffs, and what is the minimum change required for the runtime to round-trip the new diff type?
