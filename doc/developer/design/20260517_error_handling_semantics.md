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

### Stream shape: two-diff records

The unit of denotation is a stream of records of shape

```
(row : List Datum, diff : Int, err_diff : Int)
```

For each `(row, time)` bucket, `diff` is the multiplicity of `row` as a valid output and `err_diff` is the multiplicity of `row` as an erred output.
Both components are ordinary `Int` diffs that participate in differential dataflow's standard consolidation arithmetic.
Both retract.

A row with `(diff, err_diff) = (1, 0)` is a normal valid row.
A row with `(0, 1)` is a single erred row that exists in the error collection.
A row with `(1, 1)` exists in both collections (rare but representable).
Retractions are the natural `(−1, 0)` or `(0, −1)`.

The single-stream two-diff form is denotationally equivalent to the (`data`, `errors`) two-collection form used elsewhere in Materialize today.
The two views are interchangeable via the obvious bijection: `(r, d, e) ↦ (r appears `d` times in `data`, `r` appears `e` times in `errors`)`.
Choosing the single-stream form here means operators have one signature `Stream → Stream` instead of two, and proofs reason about one carrier rather than a pair.

### Row-scoped errors: `err_diff` and `DataflowError`

`DataflowError` continues to carry the structured payload of every erred row.
The row content lives in the data field of the stream record; the `err_diff` is its multiplicity in the error side.
`DataflowError::EvalError(EvalError)` is the structured payload for predicate-error / join-predicate-error / aggregate-error escalations; other `DataflowError` variants (source-decoder errors, key-conflict errors, etc.) carry their own structured payloads alongside the row.

An operator escalates a row to row scope by moving its multiplicity from `diff` to `err_diff`.
Concrete operator rules are listed in *SQL error semantics* below.

### Cell-scoped errors: `Datum::Error` inside the row

`Datum::Error(EvalError)` lives inside the row's cells.
A row `[Datum::Int 5, Datum::Error EvalError::DivByZero, Datum::Null]` is a perfectly valid row of width three whose middle column happens to be an error.
The row participates in the stream as a normal `(diff, err_diff) = (1, 0)` record.
Downstream operators that consume the column propagate the error per the strict-function rules in *SQL error semantics*.

An alternative form extends the diff with a per-`EvalError` multiplicity component (`Diff = Int × (EvalError → Int)`) so that the err side tracks *which* errors caused row escalation at the diff level.
See *Per-error-payload diff component* under Alternatives; the two-diff baseline and the per-payload form are both viable and the choice is not yet settled.

### Global-scoped errors: absorbing diff marker (specification only)

A global error at time `t` is encoded as a distinguished record whose `diff` field carries an absorbing element.
The diff field for global-error records carries a value from the extended semiring

```
DiffWithGlobal = val(Int) | global
```

with `global` absorbing both addition and multiplication: `global + d = global`, `d + global = global`, `global * d = global`, `d * global = global`.
Any sum involving `global` is itself `global`, which is exactly the propagation rule a downstream operator needs.
The `global` marker is terminal: it cannot be retracted, because the claim "this collection is invalid at `t`" is one-way.

The intent is that any downstream operator observing a `global` record at time `t` treats the entire input collection at `t` as invalid, propagating the global error to its own output.
The natural locus is the diff field, because the data field is per-row and the time field is per-update, and an absorbing monoid extension of the diff semiring captures exactly the propagation rule required.

The data side and the `DataflowError` side both carry `Int` diffs in the ordinary case; only the global-error pathway uses `DiffWithGlobal`.
Implementation is out of scope.
The spec exists so that future operator work targets this absorbing-marker encoding rather than inventing alternates.

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
A `WHERE` clause acts on a record `(r, diff, err_diff)` according to the value of `eval r pred`:

* `TRUE`     → `(r, diff, err_diff)` unchanged (the row passes through with both multiplicities preserved).
* `FALSE`    → `(r, 0, err_diff)` — the data multiplicity drops to zero; the err side is untouched.
* `NULL`     → `(r, 0, err_diff)` — same as `FALSE`.
* `ERROR e`  → `(r, 0, err_diff + diff)` — the data multiplicity migrates to the err side, added to whatever err multiplicity was already there.
              The structured `EvalError` payload `e` is carried via `DataflowError::EvalError(e)` alongside the err-side multiplicity.

Retractions are the natural negation: a row that was escalated to err side with multiplicity `+1` and is later retracted produces an `err_diff = -1`, cancelling in the consolidated view.
The row content is preserved across the escalation; downstream operators that consume the row see the same `r` whether it lives in the data side or the err side.

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
Cross product on two-diff records combines multiplicities multiplicatively, applying the standard differential-dataflow rule on each component:

```
(rL, dL, eL) × (rR, dR, eR) ⟹ (rL ++ rR, dL · dR, dL · eR + eL · dR + eL · eR)
```

Valid×valid produces valid (`dL · dR`).
Valid×err and err×valid produce err (each multiplied by the other side's data count).
Err×err also produces err (an err record crossed with an err record is still err).

A join predicate evaluating to `ERROR` on a row `(r, d, e)` of the cross product follows the `WHERE` rule: the data multiplicity migrates to the err side, with the structured `EvalError` payload carried via `DataflowError::EvalError`.

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

### Schema-driven soundness

Several rewrites are unsound on the err side of the two-diff model in their general form but become sound under a column-schema precondition.
The optimizer already carries a `RelationType` (per-column SQL type plus a `nullable` flag); adding a per-column `errable` bit and a collection-level `rowErrFree` flag turns these schema facts into discharge conditions for the rewrites.

The Lean model in `doc/developer/semantics/Mz/Schema.lean` mechanizes the structural skeleton (`ColSchema { nullable, errable }`, `Schema n { cols, rowErrFree }`, `RowSatisfies`, `Update.Satisfies`).
The first concrete payoffs riding on it are:

* **Predicate pushdown across cross under strict equality.**
  `filter p (cross sL sR) = cross (filter p sL) sR` is unsound in general because cross's err-diff bilinear formula carries `recL.diff · recR.err_diff`, which filter zeroes out before the cross when the predicate evaluates to false on `recL` (`Mz/Collection.lean` `filterOne_cross_pushdown_left_unsound`).
  Under the schema fact `sR.rowErrFree = true`, the offending term is zero by hypothesis and the pushdown closes at strict equality (`filter_cross_pushdown_left_strict`).
  Materialize's optimizer can cite the right-side schema rather than carrying an ad-hoc err-multiplicity hypothesis.

* **Coalesce identity on non-null non-err columns.**
  `coalesce(a, b) = a` when `a` is statically non-null and non-err.
  In schema terms: `outputType(a).nullable = false ∧ outputType(a).errable = false`.
  The Datum-level lemma (`evalCoalesce_cons_of_concrete`) and its `Expr`-level corollary (`eval_coalesce_pair_of_a_concrete`) close once the schema discharges the concrete-value hypothesis.

* **Cross preserves row-err-freedom.**
  `NoRowErr_cross`: when both inputs have `rowErrFree`, the product does too.
  This is the schema-propagation rule that lets an optimizer carry `rowErrFree` through a join plan rather than recomputing it.

Open obligations on the schema side that the prototype must close:

* Filter preserves `rowErrFree` only when the predicate is statically err-free on the input cell schema.
  Connects to the existing `might_error` analysis lifted from rows to streams.
* Output-schema propagation for `Expr` — given input schema, derive the column metadata of the result.
  Today every rewrite cites bare predicates; the propagation rules let one schema fact discharge multiple operators.
* A cell-error-free row schema (every column has `errable = false`), distinct from `rowErrFree` (no `err_diff > 0`).
  Both are honest schema facts and gate different rewrites; the optimizer needs both.

The optimizer's `RelationType` is the existing carrier; extending it with `errable` per column and a `rowErrFree` flag is purely additive on the schema side, with the operator obligations enumerated above pacing the rollout.

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

The design space has four semi-orthogonal dimensions, plus a cross-cutting choice about runtime channel structure.

* **Cell encoding** — how a per-cell error sits inside a row.
* **Row / collection encoding** — how a row's error multiplicity is represented at the stream record.
* **Global encoding** — how a collection-wide error is represented.
* **Evaluation-order equivalence** — what relation we treat as "the same" result when two execution orders disagree on whether to err.
* **Channel structure and wire layout** — whether errors flow in-band with data or in a sidecar, and how they're serialized.

The current proposal picks `Datum::Error` (cell), two-diff `(diff, err_diff)` (row), absorbing `DiffWithGlobal` (global), strict equality (equivalence — provisional, see *Evaluation-order equivalence*), and an in-band single-stream layout.
Each of these is one point in the larger design space; the subsections below enumerate the others.
Rejected options are retained for the historical record so that future iterations don't relitigate ground that's already been covered.

### Cell encoding

**Status quo plus TEXT COLUMN guidance.**
Document that columns with permissive upstream `sql_mode` must be ingested as `text`.
Cheap, but pushes parsing to query time forever and does not generalize to other cell-failure sources (overflow, JSON cast, decimal precision).
Rejected as a general solution; remains a valid escape hatch.

**Datum-level `NULL` overload.**
Coerce zero-dates and other unrepresentable values to `NULL` at ingestion.
Loses the distinction between user-intended `NULL` and ingestion-rejected value.
Violates the spec rule that error is stronger than `NULL`.
Rejected on correctness grounds.

**String error wrapper instead of `EvalError`.**
Store `Datum::Error(Box<str>)` rather than `Datum::Error(Box<EvalError>)`.
Simpler to encode but loses structured error information; introspection functions would parse a string.
Rejected on extensibility grounds.

**Per-column error vector inside `Row` (sidecar metadata).**
Keep `Row` as today; attach a parallel `error_vec : List (ColIdx, EvalError)` per row.
Saves a `Datum` variant but introduces an out-of-band channel that operators must thread through every transformation: every projection has to rewrite the index map, every filter has to read both the row and the vector to decide propagation.
Rejected on uniformity grounds; the `Datum` variant is the same place every existing operator already inspects.

**Severity-graded `Datum::Error` (open).**
Extend the variant to `Datum::Error(severity : Severity, EvalError)` where `Severity ∈ {Warning, Error}` and warnings do not escalate under `WHERE`, joins, or sinks.
A warning denotes "this value may be wrong but the operator chose not to fail"; e.g. a lossy cast that succeeded under a permissive cast rule but whose result the user may want to inspect.
The strict-function rule generalizes from "any err → err" to "join the severity lattice of arguments"; the boolean truth tables extend with a third row that collapses to the existing four-value behavior when severity is bottom.
Sink output downgrades warnings to data and only escalates errors.
This is a strict extension of the current proposal (the existing semantics is the case `Severity = Error` everywhere) and could be added incrementally.
The cost is one extra tag per `Datum::Error` and one extra column of every boolean / strict-function truth table; the benefit is a path to "permissive but observable" SQL surface for casts and ingestion paths that today have only "reject row" or "produce NULL".
Open: whether the warning level is needed before MVP ships, or can be added later if a concrete use case (e.g. lossy CAST, MySQL strict-mode toggles) demands it.

**Chosen: `Datum::Error(Box<EvalError>)`** as specified in the main proposal.

### Row / collection encoding

**Row-scoped errors via carrier replacement rather than the err-side diff (rejected).**
Encode a row-scoped error by replacing the row with an opaque error marker in the data field while keeping a plain `Int` diff.
This is the simplest extension but loses the row's content at the carrier flip, so any downstream operator that needed the original row data (for join, group-by on other columns, or counting) cannot recover it.
The two-diff `(diff, err_diff)` form keeps the row and tracks the err multiplicity in a parallel `Int` component, which preserves both pieces of information and stays retractable through ordinary diff arithmetic.

**Row-scoped errors via an absorbing diff marker like global errors (rejected).**
Use the same absorbing-monoid extension as the global-scoped case.
Rejected because per-row evaluation errors must be retractable: a row that errs on `WHERE 1/(a+b) > 5` at time `t` and is retracted at time `t'` must net to zero in the consolidated view.
An absorbing marker cannot retract.
The two-diff encoding (`Int` data multiplicity + `Int` err multiplicity, with absorbing `global` layered on top) keeps the algebra of each scope faithful to its semantics.

**Sum-type row carrier with a single `Int` diff (open).**
Make the row position itself a sum: `Payload = Row(List Datum) | Err(DataflowError)`, with one ordinary `Int` diff per record.
A valid row is `(Row r, 1)`; an erred row is `(Err e, 1)`; retractions are `(_, -1)`.
This is the encoding closest to Materialize's *current* two-collection model — `data` and `errors` are each a `Stream<Time, (X, Int)>` with `X = Row` on one side and `X = DataflowError` on the other.
The sum-type carrier merges those two streams into one signature `Stream → Stream` without adding a second diff component.

Properties of the sum-type form that the two-diff form does not have:

* Single `Int` diff means every existing differential-dataflow arrangement layout, batcher, and consolidator applies bit-for-bit.
  No wire-format change to the `diff` representation.

* Error records always carry their structured `DataflowError` payload as part of the carrier.
  There is no "err multiplicity without a payload" state — the two-diff form admits `(r, d, e)` with `e ≠ 0` whose payload is recoverable only via a separate `DataflowError` companion stream.

Properties of the two-diff form that the sum-type form does not have:

* A single row that exists simultaneously in both data and error collections (e.g. a row whose ingestion succeeded into data but whose subsequent reprocessing errored) is one record `(r, 1, 1)` in two-diff, but must be two records `(Row r, 1)` and `(Err e, 1)` in sum-type.
  Two-diff keeps a single arrangement key; sum-type splits across keys.

* Joins multiply two `Int` diffs per component: data×data and the three err cross-terms.
  Under sum-type, the err side has its own arrangement and joins err×err on a separate key.
  The two-diff form keeps both sides' multiplicities discoverable from one record under the same carrier.

* `which` error caused escalation lives in the carrier under sum-type, which forces every escalation site to construct a payload.
  Under two-diff, an operator can migrate `diff → err_diff` while deferring the payload to a parallel `DataflowError` stream — useful if the payload is expensive (e.g. carries the full input row) and most consumers don't need it.

The sum-type form and the two-diff form are denotationally equivalent under the natural conversion: `(r, d, e)` maps to two records `(Row r, d)` and `(Err _, e)` (the payload comes from a companion `DataflowError` lookup), and the inverse projects the diff side and discards the carrier.
The conversion is *lossy* in one direction: the inverse needs a payload witness for the err side.

Sketched in `doc/developer/semantics/Mz/AltSumStream.lean` with parallel definitions of `filter`, `project`, `negate`, `unionAll`, and `cross`, plus a `toBaseline` projection back to the two-diff form.

**Per-error-payload diff component (Diff = Int × ErrCount) (open).**
An earlier iteration of this document proposed extending the diff with a finite map from `EvalError` payloads to multiplicities, so that *which* error caused row escalation would be tracked at the diff level alongside the data multiplicity.
This is an open alternative, not a rejection — the two-diff `(Int, Int)` form is the current working baseline, but the per-payload form has properties the baseline does not, and the choice is not yet settled.

Properties of the per-payload form that the baseline does not have:

* `which` error caused row escalation is observable at the diff level rather than only through the `DataflowError` companion payload.
  A future SQL surface that wants to filter or aggregate by error kind without joining on `DataflowError` can read the err-count map directly.

* Multiple errors arising at the same `(row, time)` bucket are distinguished.
  Under the baseline, two distinct error events for the same row collapse to `err_diff = 2` and the per-event payload information is recoverable only via `DataflowError`.
  Under the per-payload form, they live in separate keys of the `ErrCount` map.

Properties of the baseline that the per-payload form does not have:

* The err-side diff is an ordinary `Int`, so every existing diff-semiring lemma and runtime arrangement layout applies without modification.

* Multiplication has the simple rule `(a, b) * (a', b') = (a·a', a·b' + b·a' + b·b')` per component, and `predicate_pushdown` over `Cross(L, R)` is sound under strict list equality.

* No stream-equivalence quotient is needed.
  Under the per-payload form, the multiplication rule `(a, m) * (b, n) = (a·b, a • n + b • m)` makes several optimizer rewrites change err-count distributions without changing data multiplicities; soundness for those rewrites requires the quotient.

The Lean mechanization on this branch carried out the `Int × ErrCount` design and surfaced both the equivalence-relation requirement and the `predicate_pushdown` interaction concretely.
That work is preserved as a worked alternative.
Resolving which form to standardize on requires answering the open question about a user-facing error-kind surface (see *Open questions*).

**N-component labeled diff (open).**
Generalize the diff to a labeled product `(d_valid, d_eval_err, d_decode_err, d_overflow, ...)` where each component is an `Int` and the label set is fixed at compile time.
The two-diff form is the case `Labels = {valid, err}`; the per-payload form is the case `Labels = EvalError` with a sparse map representation.
A fixed finite label set sits between them: more error-kind discrimination than two-diff, less than per-payload, and still ordinary-`Int` arithmetic per component.

Properties:

* Each component retracts independently.
  An operator that escalates a row to `decode_err` does not pollute the `eval_err` channel; downstream operators that filter by err-kind can read the relevant component directly.

* Multiplication generalizes via the obvious distributive rule on the full label set, expanding `(|L| · |L|)` cross-terms per join.
  For `|L| = 2` (two-diff baseline), this is the four-term rule already in the spec.
  For `|L| = k`, it is `k²` terms; manageable while `k` stays small.

* Storage cost is `k · sizeof(Int)` per record — fixed and predictable, unlike the per-payload form whose map size scales with payload diversity.

* The set of labels must be chosen up front and is part of the wire format.
  Adding a new label is a format change, unlike the per-payload form which is open-universe.

Open: whether two `Int`s is sufficient discrimination, whether a fixed `k = 4` or `k = 8` covers the operator-error-kind axes we expect, or whether the open-universe per-payload form is needed.
The Lean mechanization can carry out the N-component form as a follow-up to the per-payload work; the diff-semiring laws factor through the labeled product the same way they factor through the pair.

**Chosen: two-diff `(diff, err_diff)`** as specified in the main proposal; per-payload, sum-type, and N-component remain open alternatives.

### Evaluation-order equivalence

SQL evaluation order is unspecified outside of `CASE`, `AND`/`OR` short-circuit, and a few other explicitly-ordered constructs.
A conforming implementation is free to evaluate `x + 1 - 1` as either `(x + 1) - 1` or as `x + (1 - 1)`.
On an `INT` `x` near the type's maximum, the first form raises overflow and the second does not.
Two equally-valid implementations therefore disagree on whether the same expression errs.

The semantics we mechanize has to admit this freedom, or every interesting rewrite — associativity of `+`, predicate pushdown across `AND`, constant folding under arithmetic — is unsound the moment errors are reachable.
The pushdown counterexample documented as a PR comment on the prior `Int × ErrCount` iteration was a specific case of this general problem: even the *err multiplicity* of the output of a rewritten pipeline can change while every successful evaluation gives the same value, and a soundness proof based on strict list-of-error-payload equality cannot close.

The choice is which equivalence relation `≡` on `Datum` (and lifted to stream records) we treat as "the same result".

**Strict equality (the trivial choice).**
`a ≡ b` iff `a = b` as `Datum` values.
Under this relation, every error payload is distinct, the order in which errors arise is observable, and most useful rewrites fail to hold.
This is the relation the current Lean skeleton uses for `eval` — and the reason `predicate_pushdown` over `Cross(L, R)` had to be carefully phrased on the data side only, with the err side left out of scope.
This is the relation a strict reading of SQL semantics would require if SQL pinned evaluation order — but SQL does not, so this relation is *too fine* to model what SQL actually says.

**Error-set equivalence.**
`a ≡ b` iff both are the same non-error datum, or both are errors (with the inner `EvalError` payloads allowed to differ).
This collapses all errors at a given position to a single equivalence class.
Useful because it makes associativity of arithmetic hold under reasonable side conditions: `(x + 1) - 1 ≡ x + (1 - 1)` whenever `x` does not overflow, and *both* err (just possibly with different specific overflow witnesses) when `x` is near the boundary.
Loses payload information at the equivalence layer, but the payload is still concretely produced by `eval`; the relation just doesn't demand it match.

**Refinement preorder (errors as bottom).**
A partial order rather than an equivalence: `a ⊑ b` iff `a = b` or `a` is an error.
"`b` is at least as defined as `a`" with errors as the least-defined element.
Two values are *equivalent* iff each refines the other, which collapses to error-set equivalence; but the preorder lets us state the asymmetric law that an optimizer may rewrite `e1 → e2` if `eval e1 ⊑ eval e2` pointwise — i.e., the rewrite never *adds* an error.
This matches the "spurious errors are bugs" posture: the optimizer is permitted to drop an error that the original evaluation order would have produced (e.g., constant folding `1/0` inside a `WHERE FALSE` arm), but is not permitted to introduce an error that no admissible evaluation order would have produced.
PostgreSQL's posture is closer to the opposite — implementations may produce additional errors the user didn't ask for — and rewrites in that style are sound under the *dual* preorder (`a ⊒ b` iff `a = b` or `b` is an error).
Mechanizing both directions lets the doc state precisely which rewrites belong to which posture.

In Materialize's setting, pushdown across `Cross(L, R)` of a predicate that may error is the canonical case that distinguishes the postures.
Under "no spurious errors" the rewrite is gated by `Expr.might_error` (analyzed in `Mz/MightError.lean`); when the analyzer rules out errors the rewrite is sound under `=`, and the preorder discussion is unnecessary.
Under "spurious errors permitted" the rewrite is sound unconditionally under `⊒` (the dual preorder), and the optimizer is free to apply it without static analysis.

**Value-only equivalence under non-determinism.**
`a ≡ b` iff there exist evaluation orders for both expressions under which they produce the same non-error value, *or* both expressions err under every admissible evaluation order.
The strongest equivalence that still permits all the rewrites listed above.
The cost is that the semantic object stops being a function `Expr → Datum` and becomes a relation `Expr → Set Datum` — every expression denotes a *set* of admissible outcomes, one per evaluation order.
Properly modeling this requires lifting the entire `eval` function to a non-deterministic semantics, which is a significant restructure of the skeleton.
The payoff is that pushdown over errors becomes a tractable theorem rather than a counterexample.

The current Lean mechanization uses strict equality and therefore states all its laws as "value-side agrees; err-side out of scope", which is sound but unsatisfying.
Moving to error-set equivalence is the cheapest upgrade: every existing theorem stated as `=` lifts to `≡` for free, several rewrites that currently fail acquire conditional forms, and the rewrites that need the refinement preorder can be stated as one-directional inclusions instead of equalities.

#### Counterexamples under strict equality

Concrete cases where strict equality fails and a coarser relation is required:

1. **Associativity of `plus` over `Int`.**
   `eval r ((x.plus 1).plus (-1)) = eval r (x.plus (1.plus (-1)))`?
   This counterexample requires modeling bounded ints; the current Lean skeleton uses unbounded `Int` (`evalPlus` on `.int n` and `.int m` is `.int (n + m)` without overflow), so the counterexample is moot in the mechanization today.
   The intended counterexample on a bounded `Int32` is: a row `r` where `x` evaluates to `MAX_INT32`.
   LHS: `MAX_INT32 + 1` raises `OverflowError`, so the whole expression is `.err OverflowError`.
   RHS: `1 + (-1) = 0`, then `MAX_INT32 + 0 = MAX_INT32`, so the whole expression is `.int MAX_INT32`.
   The two are not equal under strict equality.
   Under error-set equivalence, they are still not equal (one is an err, one is not).
   Under the refinement preorder (errors as bottom), LHS ⊑ RHS holds (LHS errs, RHS doesn't, RHS is "more defined"), so an optimizer rewrite from LHS-shape to RHS-shape is sound — it removes a spurious error.
   The reverse rewrite RHS → LHS is sound under the dual preorder (PostgreSQL posture) but not under the no-spurious-errors posture.
   To exercise this in the mechanization, the skeleton would need an `evalPlusBounded` with explicit overflow; that is one of the natural next-step extensions if the bounded case becomes a forcing function.

2. **Predicate pushdown across `Cross(L, R)` with an erroring predicate.**
   `filter (p AND q) (cross L R) = cross (filter p L) (filter q R)`?
   Counterexample: a single row pair `(rL, rR)` where `eval rL p` errs and `eval rR q` errs.
   LHS evaluates the conjunction on the joined env and produces one error from the AND truth table (`ERROR AND ERROR = ERROR`), so one record migrates to the err side.
   RHS errs once on the left (filter on `L` migrates `rL` to err side) and once on the right (filter on `R` migrates `rR` to err side), then cross-products: under the two-diff multiplication rule, the err-side combines as `dL · eR + eL · dR + eL · eR`, which is `0 + 0 + 1 = 1` here.
   The data sides agree; the err multiplicity also coincidentally agrees at 1, but the *carrier* of the err — which row content went with which payload — diverges.
   This is the case the per-payload form surfaced as a soundness gap.
   Strict equality fails; error-set equivalence on the err side fails (different payloads); error-set equivalence on err-*multiplicity* (forgetting carriers) succeeds.

3. **Coalesce with reorderable operands.**
   `coalesce(a, b)` is left-to-right by SQL specification, so no counterexample here — but `coalesce` is the exception that proves the rule.
   It exists precisely because the default rewriting freedom is too broad to express "rescue the first non-error".
   Conversely, `least(a, b)` (which SQL leaves order-unspecified) is *not* equivalent to a specific evaluation order, and a strict-equality model proves false equalities involving `least` over erroring inputs.

4. **Constant folding inside a discarded `AND` branch.**
   `eval r (FALSE AND (1 / 0))` — the boolean truth table absorbs `FALSE` on the left and the result is `.bool false` (the existing `evalAnd` pattern `.bool false, _ => .bool false` matches first).
   `eval r ((1 / 0) AND FALSE)` — same expression with swapped arguments — *also* evaluates to `.bool false` in the current skeleton because `evalAnd` pattern-matches `.bool false` on the right before evaluating the left operand at all (pattern arm `_, .bool false => .bool false`).
   So in the *current* `evalAnd` this is not a counterexample.
   But a *different* implementation strategy — strict left-to-right evaluation that always evaluates both operands before consulting the truth table — would produce `.err divisionByZero` for the swapped form.
   Two SQL-conformant implementations of `AND` disagree on whether `(1/0) AND FALSE` errs.
   Under strict equality, the two implementations cannot both be modeled by the same `eval` function.
   Under error-set equivalence, the eager implementation produces `.err _` and the lazy one produces `.bool false`; not equivalent.
   Under the refinement preorder, the lazy implementation's `.bool false` *refines* the eager implementation's `.err _` (errors are bottom; `.err _ ⊑ .bool false`), so a rewrite from eager to lazy is sound.
   Under value-only equivalence under non-determinism, the two are equivalent because there exists an admissible evaluation order (the lazy one) under which the expression succeeds with `.bool false`.
   The current `evalAnd` *is* the lazy form, so the eager-to-lazy rewrite is implicit in the model; if a future `evalAndEager` is added (e.g. to mirror a different operator), the preorder lets us prove the two implementations interchangeable up to error introduction.

The cases stack: strict equality breaks the most rewrites; error-set equivalence recovers commutativity and some rearrangements; refinement preorder recovers optimizer-introduced rewrites (push down a filter even though it might add an err for an unreachable row); value-only equivalence under non-determinism recovers everything but requires non-deterministic semantics.

The decision is which equivalence the *Lean mechanization* should mechanize.
A pragmatic path: ship the skeleton on error-set equivalence, with a quotient on `EvalError` payloads at the `Datum` level.
State the refinement preorder as a separate predicate `Datum.refines` and prove the optimizer-correctness laws against it.
Defer non-deterministic semantics to a follow-up when a concrete rewrite demands it.

#### Counterexamples I tried and could not prove

These are statements where I expected the proof to go through under one of the relations above but did not.
Documented here so a future iteration knows the terrain.

* **Associativity of `plus` under error-set equivalence.**
  Statement: `(x + 1) + (-1) ≡ x + (1 + (-1))`.
  Counterexample (bounded ints, not in the current skeleton): as above, with `x = MAX_INT32`.
  Under error-set equivalence the LHS is `err` and the RHS is `.int MAX_INT32`; the relation `≡` requires both sides to be errors or both to be the same value, and one is not.
  Needed: refinement preorder with the direction LHS ⊑ RHS (the err refines the value) — a *symmetric* equivalence is not strong enough.

* **`filter p ∘ negate = negate ∘ filter p` under error-set equivalence on streams.**
  Statement: filter commutes with negate at the stream level.
  Counterexample: a record `(r, 1, 0)` where `eval r p = .err`.
  Filter then negate: filter migrates to err, producing `(r, 0, 1)`, then negate produces `(r, 0, -1)`.
  Negate then filter: negate produces `(r, -1, 0)`, then filter migrates with `err_diff + diff = 0 + (-1) = -1`, producing `(r, 0, -1)`.
  These match.
  But a record `(r, 1, 1)` (which the two-diff form permits): filter then negate gives `(r, 0, -2)`; negate then filter gives the same `(r, 0, -2)`.
  These match too.
  The statement *does* hold under strict equality — I include it here only because I attempted to prove it on the err side under the per-payload diff form and failed: the payload combined into `err_diff` by filter is the payload of `p`, but the payload visible after negate-then-filter has a sign flip on the multiplicity but the same payload, so under per-payload semantics the two err-counts differ in their key when `p`'s payload is not present in the input.
  This is a per-payload-form artifact, not a two-diff artifact.

* **`predicate_pushdown` (filter across `cross`) on the err side under error-set equivalence.**
  Statement: `filter p (cross l r) ≡ cross (filter p l) r` (where `p` is bounded by left widths and `r` is err-free).
  Data side proven in `Mz/JoinPushdown.lean` under strict equality.
  Err side fails under strict equality (counterexample 2 above) and also under error-set equivalence on per-record err multiplicities (the err-side multiplicity differs by the right-side's data cardinality on the LHS vs the RHS in cases where `p` errs).
  Holds under value-only equivalence under non-determinism, because both sides are valid evaluation orders of the same SQL expression.
  Mechanizing this rewrite on the err side is the canonical use case for moving to non-deterministic semantics.

### Global encoding

**Global errors via a sidecar collection rather than the `diff` field (rejected).**
Carry global errors in a separate timely stream parallel to the data stream.
Works, but requires every operator to be aware of two error inputs and to fan-in the global stream by hand.
The `diff`-field encoding leverages differential dataflow's existing fan-in and is the natural extension of the semiring.
Rejected.

**Chosen: `DiffWithGlobal = val(Int) | global`** as specified in the main proposal.

### Channel structure and wire layout

These choices are largely independent of the encoding choices above and could be revisited even after the encoding is settled.

**Sidecar error stream (open).**
Run a separate timely stream `Stream<Time, (RowKey, DataflowError, Int)>` keyed by something derivable from each data record — e.g. `RowKey = hash(row) ⊕ source_offset`.
The data stream carries plain `(Row, Int)` records, never `Datum::Error` and never `err_diff`.
A downstream consumer that wants per-row error context joins the two streams on `RowKey`.

Properties:

* Zero storage cost on error-free records.
  The data stream is bit-for-bit identical to today's data stream.

* Operators that don't care about errors don't pay for them.
  An optimizer-introduced `consolidate` over a column projection of error-free data never touches the error side.

* Cross-stream joins between data and errors are not free.
  Every operator that *does* need to know per-row errors (sink output, `try_*`, error introspection functions) pays a join cost.

* The key has to be well-defined.
  A row-mutating operator like `project` changes the row's hash, so the key has to be a stable identifier through the dataflow — likely a `(source, offset)` pair or a synthetic per-record id.

Open: whether the synthetic-id requirement is acceptable.
Most existing operators don't generate synthetic ids; adding them is a substantial change to the carrier.

**Sparse two-diff wire format (open optimization on the chosen encoding).**
Keep the two-diff semantics but omit `err_diff = 0` records from the wire and arrangement.
A serialized record is either `(row, diff)` (the common case) or `(row, diff, err_diff)` (the rare error case), distinguished by a tag bit.
In-memory representation can keep the two-diff form uniformly; only the persisted and shuffled form is sparse.

Properties:

* Wire cost on error-free streams is exactly today's cost.
  The two-diff form's "extra `Int` per record" overhead applies only to records that actually carry a nonzero `err_diff`.

* Decode is one tag bit per record plus an optional `Int` read.

* Arrangement layout in memory is unaffected — the sparse form is a serialization concern only, so existing in-memory operators don't care.

This is the obvious optimization on the chosen encoding and resolves the storage-cost concern in *Open questions*.
It should be a default rather than an alternative: the two-diff form should ship with a sparse wire format unless there's a measured reason not to.

**Errors at distinguished timestamps (rejected).**
Encode errors by emitting them at a virtual "error time" namespace disjoint from the data timeline.
A `WHERE` failure at time `t` becomes a data emission at time `t.error_ns`.
Reuses the time dimension as the error channel.
Rejected because timestamps in differential dataflow are part of the consistency contract: every operator and arrangement expects the time dimension to be a partial order with a defined meet, and overloading it with an "error namespace" breaks frontier reasoning.

**Chosen: in-band two-diff stream**, with sparse wire format as a default-on optimization.

### Summary table

| Dimension | Option | Retract | Cell-error expressivity | Per-record cost | Open-universe payloads | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Cell | TEXT COLUMN | n/a | none | 0 | n/a | escape hatch, not general |
| Cell | NULL overload | n/a | none | 0 | n/a | rejected: collapses NULL vs error |
| Cell | `Datum::Error(str)` | yes | string only | 1 ptr | yes | rejected: not introspectable |
| Cell | `Datum::Error(EvalError)` | yes | structured | 1 ptr | extensible | **chosen** |
| Cell | severity-graded | yes | + warning level | 1 ptr + 1 byte | extensible | open extension |
| Cell | per-row error vec | yes | structured | per-row sidecar | extensible | rejected: out-of-band |
| Row | carrier replacement | no (lossy) | n/a | 0 | yes | rejected: loses row content |
| Row | absorbing diff marker | **no** | n/a | 0 | n/a | rejected: not retractable |
| Row | two-diff `(d, e)` | yes | n/a | + 1 `Int` | n/a | **chosen** |
| Row | sum-type carrier | yes | n/a | + 1 tag | yes | open: matches today's two-collection model |
| Row | per-payload diff | yes | n/a | + map | yes (open universe) | open: error-kind observable at diff |
| Row | N-component diff | yes | n/a | + k `Int` | no (fixed labels) | open: middle ground |
| Global | sidecar collection | n/a | n/a | + 1 stream | n/a | rejected |
| Global | `DiffWithGlobal` | one-way | n/a | wide diff | n/a | **chosen** |
| Equivalence | strict `=` | n/a | n/a | n/a | n/a | currently mechanized; too fine to model SQL |
| Equivalence | error-set | n/a | n/a | n/a | n/a | open: minimal upgrade, recovers commutativity |
| Equivalence | refinement preorder | n/a | n/a | n/a | n/a | open: supports may-add-error rewrites |
| Equivalence | value-only / non-det | n/a | n/a | n/a | n/a | open: full pushdown soundness; needs non-det semantics |
| Channel | in-band | n/a | n/a | per encoding | n/a | **chosen** |
| Channel | sidecar error stream | n/a | n/a | 0 on hot path | n/a | open: requires stable RowKey |
| Channel | sparse wire | n/a | n/a | 0 on err-free | n/a | open: default-on optimization |
| Channel | distinguished time | n/a | n/a | 0 | n/a | rejected: breaks frontier |

## Open questions

* What is the exact set of `EvalError` payloads that operators may produce as `Datum::Error`?
Some `EvalError` variants (out-of-memory, environment errors) make no sense at cell scope.
* How does `Datum::Error` interact with persisted state and version skew?
A reader on an older binary that encounters `Datum::Error` must have a defined behavior; the default proposal is to surface the row as a row-scoped error in the read path.
* Should ordering operators have a defined sort position for `Datum::Error`?
PostgreSQL has no precedent; candidates are "errors sort last", "errors sort like `NULL`", and "errors are unordered and produce a sort-key error".
* What is the storage cost of widening `Row` encoding to include the new tag, and how does it compare to the current cost of routing failures through the error collection?
* For global errors, what is the precise specification of the absorbing element in the `diff` semiring, and how does it interact with consolidation and arrangement?
* The two-diff `(diff, err_diff)` form has the runtime carrying two `Int`s per record where today it carries one.
What is the storage / wire cost, and is it acceptable for collections in which errors are rare?
A sparse representation that omits records with `err_diff = 0` is the obvious optimization.
* For source-decoder errors where the row's columns cannot be fully produced, what row content goes into the data field?
Candidates: a width-zero placeholder row, a width-N row of all-`Datum::Error` cells, or a `DataflowError::DecodeError` payload that lives alongside the err multiplicity but does not require a row at all.
The choice affects whether the data-side carrier is always a `List Datum` or whether the stream needs a sum-type carrier after all.
* Should the err-side multiplicity be a single `Int` (the two-diff baseline) or a per-`EvalError` map `EvalError → Int` (the per-payload alternative)?
This is the question that resolves whether the `Int × ErrCount` alternative is adopted.
The answer turns on a downstream concern: is there a user-facing SQL surface that needs to distinguish error kinds at the row level without joining on `DataflowError`?
If yes, the per-payload form is the natural fit.
If no, the two-diff form is the simpler home.
Until that surface is decided, both forms remain on the table.
* Which equivalence relation on `Datum` (and stream records) does the Lean mechanization commit to: strict equality, error-set equivalence, refinement preorder, or value-only equivalence under non-deterministic evaluation?
The choice gates which rewrites the optimizer can claim to be sound under errors.
The current skeleton uses strict equality, and several rewrites (predicate pushdown across `Cross`, associativity of arithmetic, constant folding inside discarded boolean arms) provably do *not* hold on the err side under that relation.
A pragmatic first step is to lift `Datum` equality to an error-set equivalence quotient and re-state the existing laws against it; concrete counterexamples and the larger discussion are in *Alternatives → Evaluation-order equivalence*.
