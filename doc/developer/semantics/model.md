# Semantic model: layers, errors, and equivalence variants

This document is the working reference for the data model and the
equivalence relations the Lean skeleton at `doc/developer/semantics/`
is exploring.
It complements the design doc at
`../design/20260517_error_handling_semantics.md`, which is more
discursive; this file is a layered catalog plus honest notes on what
each variant buys and what it costs.

The model has four layers — `Datum`, `Expression`, `Row`, `Stream` —
and a separate dimension of error semantics that cuts across all of
them.
After the layers, this doc summarizes the equivalence relations the
optimizer can pick from and the rewrites each enables.

## Datum

`Datum` is the cell-level value type.
The skeleton currently models the four-valued lattice
`{TRUE, FALSE, NULL, ERROR}` plus integers:

```
inductive Datum
  | bool (b : Bool)
  | int  (n : Int)
  | null
  | err  (e : EvalError)
```

`EvalError` is the cell-scoped error payload — currently
`.divisionByZero` and `.overflow`, with the production list
(`src/expr/src/scalar.rs`) much larger.
Numeric, string, and temporal types are intentionally omitted.

The four-valued absorption order is `FALSE > ERROR > NULL > TRUE` for
`AND` (and the dual for `OR`), encoded in `evalAnd` / `evalOr`.
The `Datum.IsErr` predicate is the propositional witness for "this
cell errored".

## Expression

`Expr` is the AST for scalar computations:

```
inductive Expr
  | lit (d : Datum)
  | col (i : Nat)
  | not (a : Expr)
  | ifThen (c t e : Expr)
  | andN (args : List Expr)
  | orN  (args : List Expr)
  | coalesce (args : List Expr)
  | plus / minus / times / divide (a b : Expr)
  | eq / lt (a b : Expr)
```

Binary `Expr.and` / `Expr.or` are sugar over the variadic `andN [a, b]`
/ `orN [a, b]` to match the Rust `MirScalarExpr` variadic-only shape.
Evaluation is the big-step `eval : Env → Expr → Datum`, with
`Env := List Datum` as the positional column lookup.

Static analyses:
* `might_error` — conservatively decides whether an expression can
  raise a cell error.
  Short-circuits literal-false / literal-true absorption and
  literal-nonzero divisors.
* `colReferencesBoundedBy n` — every `.col i` has `i < n`.
  Used as the column-bound hypothesis for predicate pushdown across
  joins.
* `colShift k` — adds `k` to every column reference.
  Used to realign a right-side predicate against the joined env.
* `colReferencesUnused n` — column `n` is never read.
  Used for column-pruning rewrites.

## Row

A row is a positional list of `Datum`s.
Two encodings coexist in the skeleton:

* `Row = List Datum` (untyped) in `Mz/Eval.lean` (alias for `Env`)
  used by `Mz/Stream.lean` — arity lives in a conventional invariant.
  Operators preserve it by construction but the type system gives no
  feedback.
* `RowN n = List.Vector Datum n` (indexed) in `Mz/StreamN.lean` —
  arity in the type.
  `filter` is `StreamN n → StreamN n`, `project` is
  `StreamN n → StreamN m` (driven by a length-`m` expression vector),
  `cross` is `StreamN n → StreamN m → StreamN (n + m)`.

The indexed form makes arity mismatches unspeakable and surfaces
arity-rewriting obligations (`cross_assoc` needs the cast
`n + m + k = n + (m + k)` via `List.Vector.congr`).
The untyped form is cheaper to write but cannot witness arity bugs at
type-check time.

## Schema

Per-column nullability and errability bits plus a stream-level
row-error flag, modeled in `Mz/Schema.lean`. The structural
counterpart to Materialize's `RelationType` on the Rust side.

* `ColSchema { nullable, errable : Bool }` — per-column metadata.
* `Schema n { cols : Vector ColSchema n, rowErrFree : Bool }` — the
  schema as a whole.
* `Schema.free n` — information-free starting point.
* `Schema.append a b` — concatenation, produced by `cross`.

Propositional satisfaction:

* `RowSatisfies sch row` — `¬nullable → row[i] ≠ .null` and
  `¬errable → ¬(row[i]).IsErr` per column.
* `StreamRecordN.Satisfies sch rec` — row satisfies the column
  bits *and* `sch.rowErrFree → rec.err_diff = 0`.
* `StreamN.Satisfies sch s` — every record satisfies.

Schema discharges optimizer obligations whose soundness depends on
column or row-level invariants:

* `NoRowErr_of_satisfies_rowErrFree` — bridge to
  `filter_cross_pushdown_left_strict`'s precondition.
* `evalCoalesce_cons_of_concrete` + `eval_coalesce_pair_of_a_concrete`
  — `coalesce(a, b) = a` when `a` evaluates to a concrete (non-null,
  non-err) value. Schema rider: when `a`'s output column has
  `nullable = false` and `errable = false`, the precondition is
  immediate.
* `NoRowErr_cross` — `cross` preserves row-err-freedom.

Open obligations on the schema side:

* `NoRowErr_filter` — filter preserves `rowErrFree` only when the
  predicate is statically err-free on the input cell schema.
  Connects to `might_error_sound` lifted from rows to streams.
* Output-schema propagation for `Expr` and stream operators —
  given input schema, derive the output schema. Currently every
  rewrite cites bare predicates; the propagation rules would let
  the optimizer reuse one schema fact across multiple operators.
* Cell-error-free row schema: an analogue of `NoRowErr` for the
  per-cell `Datum.IsErr` condition. Distinct from `NoRowErr`
  (row-level err multiplicity) — both are honest schema facts and
  both gate different rewrites.

## Stream

A stream is a list of records carrying multiplicities and (optionally)
timestamps.
The current baseline ("two-diff") encodes each record as

```
structure StreamRecord where
  row : Row
  diff : Int      -- data multiplicity, retractable
  err_diff : Int  -- err multiplicity, retractable
```

Both diffs are ordinary `Int`s, retractable to model
differential-dataflow consolidation.
A record with `(diff, err_diff) = (1, 0)` is a valid output;
`(0, 1)` is an erred output; `(1, 1)` is both (rare but representable
under the encoding).

Operators on streams:
* `filter` — preserves `row`, zeroes `diff`, migrates to `err_diff` on
  an `.err` predicate result.
* `project` — applies the projection expression vector pointwise,
  preserving multiplicities.
* `cross` — concatenates rows; multiplies `diff`s; the cross's
  `err_diff` is the bilinear sum
  `dL · eR + eL · dR + eL · eR`.
* `negate`, `unionAll` — pointwise negation and concatenation.

Time is not yet modeled.
Consolidation, distinct, and aggregate are open.

## Errors

Three error scopes, mostly orthogonal:

* **Cell-scoped** — `Datum::Error(EvalError)`.
  Lives inside a row.
  Carries a structured payload.
  Surface examples: division by zero, integer overflow, decode error
  on a single column.
* **Row-scoped** — `err_diff` on the stream record (two-diff model)
  *or* a row-carrier variant `(Row | DataflowError)`.
  The row failed as a whole.
  Surface examples: a `Datum::Error` in a projected column that the
  optimizer chose to escalate; a row whose decoder failed before any
  cell was reached.
* **Collection-scoped** — an absorbing element on the diff
  (`DiffWithError`) or a separate flag.
  Once introduced, the entire collection is poisoned.
  Surface examples: a source that lost its prefix; a fatal indexing
  failure during aggregation.
  Currently spec-only; not mechanized in the post-restart skeleton.

The two-diff baseline carries cell and row scopes natively (cell as
`Datum::Error` in the row, row as `err_diff` multiplicity).
Collection scope is documented in the design doc and historically had
a `DiffWithError` mechanization that was removed at restart; it can be
reintroduced as a flag if a forcing function appears.

## Equivalence relations explored

SQL leaves evaluation order unspecified outside `CASE`, `AND` / `OR`
short-circuit, and a few other places.
Any optimizer rewrite that touches errors must therefore live above
strict equality on `Datum`.
The skeleton catalogs four candidate relations in `Mz/Equiv.lean` plus
two more discovered during the indexed-arity pilot.

### Strict equality (`=`)

Reference relation.
Closes the data-side laws cleanly: filter / project fuse, identity
laws of `AND` / `OR`, idempotence, `evalPlus` associativity over
unbounded `Int`.

Counterexamples (mechanized in `Mz/EquivBounded.lean`,
`Mz/Equiv.lean`):
* `evalAnd` not commutative on err / err inputs (left-bias).
* `evalPlusBounded` not associative at the bounded-int boundary.
* `filter_cross_pushdown_left` unsound when right stream has
  `err_diff > 0` (witnessed by
  `filterOne_cross_pushdown_left_unsound`).

### Error-set equivalence (`eqErrSet`)

`a.eqErrSet b := a = b ∨ (a.IsErr ∧ b.IsErr)`.
Collapses all `.err` payloads into one equivalence class.

What it buys: recovers `evalAnd` commutativity on err / err
(`evalAnd_err_err_eqErrSet_comm`).
What it does not buy: bounded-int associativity (one side is err, the
other value; relation requires both errs or strict equality);
predicate pushdown over `cross` on the err side (record-level carrier
mismatch survives the relation).

### Refinement preorder (`refines`, errors as bottom)

`a.refines b := a = b ∨ a.IsErr`.
Asymmetric.
"Transformed result loses an error" is admissible.

Posture: "no spurious errors" — the transformed result has no errors
the original did not have.
Pushdown `filter_cross_pushdown_left` is sound in the LHS → RHS
direction (LHS errs, RHS doesn't) but unsound in reverse.

### Dual refinement preorder (`refinesDual`, errors as top)

`a.refinesDual b := a = b ∨ b.IsErr`.
The reverse direction of `refines`.

Posture: "spurious errors permitted" (PostgreSQL).
Transformed result may add errors the original did not have.
Pushdown `filter_cross_pushdown_left` is unsound under this posture
(RHS loses an error LHS had, which the posture forbids).

### Data-side erasure (`eraseErr`)

`recA.eraseErr.diff = recB.eraseErr.diff ∧ rows equal`, ignoring
`err_diff`.
Equivalence relation.

What it buys: `filter_cross_pushdown_left_data` closes for every
branch of the predicate evaluation
(`filterOne_cross_pushdown_left_data` per record, lifted via
`List.map`).
What it costs: erases the user-visible distinction between "row
filtered out" and "row errored".
For Materialize, errors are observable, so full erasure is too
coarse for the user-facing surface.
It is a useful interior relation for proving that a rewrite preserves
the data side, with the err side handled separately.

### Per-error-payload diff (open alternative)

`Diff = Int × (EvalError → Int)` with the bilinear cross rule.
Retains structured error payloads through multiplicities.
Open question in the design doc.

Lean evidence accumulated before the restart:
* The bilinear multiplication is a semiring (proved).
* `predicate_pushdown` over `Cross(L, R)` is provably *unsound* under
  this encoding (counterexample mechanized then preserved in branch
  history; documented in `Mz/Equiv.lean` counterexamples docstring).
  This is the same finding the two-diff `eraseErr` analysis
  re-derives: the err side does not commute with cross under any
  encoding that multiplies err multiplicity against data multiplicity.

## Summary table

| Layer / variant            | Mechanization                | Sound under                | Open under                                |
| -------------------------- | ---------------------------- | -------------------------- | ----------------------------------------- |
| `Datum`                    | `Mz/Datum.lean`              | —                          | overflow / decode / division-by-zero only |
| `Expr.eval`                | `Mz/Eval.lean`               | `=`                        | strict eval order (no non-determinism)    |
| `Row = List Datum`         | `Mz/Eval.lean`               | `=`                        | arity by convention                       |
| `RowN n = Vector Datum n`  | `Mz/StreamN.lean`            | `=`                        | indexed-arity reasoning verified          |
| `Stream` (two-diff)        | `Mz/Stream.lean`             | `=` on data side           | err side under `eraseErr` / `refines`     |
| `eqErrSet`                 | `Mz/Equiv.lean`              | err / err commutativity    | bounded-int assoc, pushdown over cross    |
| `refines`                  | `Mz/Equiv.lean`              | pushdown LHS → RHS         | rewrites that add err                     |
| `refinesDual`              | `Mz/Equiv.lean`              | rewrites that add err      | pushdown over cross                       |
| `eraseErr` (data-only)     | `Mz/StreamN.lean`            | filter / cross pushdown    | err-side surfacing                        |
| `NoRowErr` precondition    | `Mz/StreamN.lean`            | pushdown under `=`         | filter preservation needs static pred     |
| `Schema n` (sketch)        | `Mz/Schema.lean`             | coalesce id, cross row-err | filter / project output-schema rules      |
| Per-payload `(Int×ErrCnt)` | preserved in branch history  | `=` on commutative-monoid  | pushdown over cross                       |
| Collection-scoped diff     | spec-only post-restart       | —                          | not currently mechanized                  |

The remaining open obligations live primarily on the err side of
operators that mix records — `cross`, `join`, aggregates over erred
inputs.
The catalog above is the map of which equivalence relation makes which
rewrite go through; the open question driving the design doc is which
of these to settle on as the user-facing surface.

## See also

* `../design/20260517_error_handling_semantics.md` — design doc with
  the discursive treatment of the same material plus alternatives.
* `transforms.md` — catalog of mechanized equational and inclusion
  laws for optimizer rewrites.
* `Mz/Equiv.lean` and `Mz/EquivBounded.lean` — the equivalence
  relations and the live bounded-arithmetic counterexample.
* `Mz/StreamN.lean` — the indexed-arity pilot and its
  `filter_cross_pushdown_left` finding.
