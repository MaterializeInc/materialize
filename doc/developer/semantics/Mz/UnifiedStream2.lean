import Mz.DiffErrCount
import Mz.DiffWithGlobal
import Mz.Expr
import Mz.Eval
import Mz.Bag
import Mz.ErrStream

/-!
# Unified stream, take two

`Mz/UnifiedStream.lean` carries a sum-typed `UnifiedRow := row | err`
carrier. Filter and project replace the row with `.err e` on
evaluation error, which loses the row's content — a row that errs
inside a `WHERE` predicate no longer exists as data even though its
columns were otherwise well-defined.

The design doc
(`doc/developer/design/20260517_error_handling_semantics.md`,
sections "Row-scoped errors" and "Predicates") fixes this by moving
row-scoped errs out of the carrier and into the diff's `ErrCount`
component:

* the carrier is just `Row` — no `err` constructor;
* row-scoped errs live in `Diff.errs : ErrCount` (retractable);
* collection-scoped errs live in `DiffWithGlobal.global` (terminal).

This module is the new model. The old `UnifiedStream` is retained
unchanged because downstream files (Join, SetOps, Consolidate, etc.)
still cite it; migration is incremental.

## Encoding

`UnifiedStream2 := List (Row × DiffWithGlobal)`. Operators consume
and produce `UnifiedStream2`s. A `WHERE` predicate that errors on a
row carrying diff `(a, m)` produces the same row with diff
`(0, m ∪ {e ↦ a})` — the row content is preserved and the err-count
participates in retraction through ordinary diff arithmetic. -/

namespace Mz

/-- New unified stream. Carrier is just `Row` — no err marker.
Row-scoped errs live in the diff's `ErrCount` component;
collection-scoped errs live in the `DiffWithGlobal.global` marker. -/
abbrev UnifiedStream2 := List (Row × DiffWithGlobal)

namespace UnifiedStream2

/-! ## Filter

Per the design doc:

* `eval r pred = .bool true`           → diff unchanged.
* `eval r pred = .bool false / .null / .int _`
                                       → diff valid count zeroed,
                                          errs kept.
* `eval r pred = .err e`               → diff valid count zeroed,
                                          errs gets `e` added with
                                          count `d.val`.
* `diff = .global`                     → passes through unchanged.

For `.bool false / .null / .int` we always emit the record even
when the diff's errs are zero — semantic clarity over output-size
optimization. The downstream consolidator drops zero-diff records
if it cares. -/

/-- Per-`Diff` filter action. Splits on the predicate result on `r`.
Always returns exactly one record (with the same row, possibly a
zeroed-out diff). -/
@[inline] private def filterOne (pred : Expr) (r : Row) (d : Diff) :
    Row × DiffWithGlobal :=
  match eval r pred with
  | .bool true =>
    -- predicate true: pass row through with its original diff.
    (r, DiffWithGlobal.val d)
  | .err e =>
    -- predicate errs: zero the valid count, add e ↦ d.val to errs.
    (r, DiffWithGlobal.val { val := 0
                            , errs := d.errs + ErrCount.single e d.val })
  | _ =>
    -- predicate false / null / int: zero the valid count, errs kept.
    (r, DiffWithGlobal.val { val := 0, errs := d.errs })

/-- Diff-aware filter on the new unified stream. -/
def filter (pred : Expr) (us : UnifiedStream2) : UnifiedStream2 :=
  us.map fun rd => match rd with
    | (r, DiffWithGlobal.val d) => filterOne pred r d
    | (_, DiffWithGlobal.global) => rd

/-! ### Reduction lemmas

Named per-list-shape reductions for `filter`. Downstream proofs
cite these instead of unfolding `map` inline. -/

theorem filter_nil (pred : Expr) :
    filter pred [] = [] := rfl

theorem filter_append (pred : Expr) (a b : UnifiedStream2) :
    filter pred (a ++ b) = filter pred a ++ filter pred b := by
  show (a ++ b).map _ = a.map _ ++ b.map _
  exact List.map_append

/-! ## Project

Per the design doc:

* `rowAllSafe es r = true`              → row replaced by
                                          `es.map (eval r)`,
                                          diff unchanged.
* `rowAllSafe es r = false`             → row preserved, diff valid
                                          count zeroed, errs gets one
                                          entry per erroring scalar
                                          with count `d.val`.
* `diff = .global`                      → passes through unchanged.

This mirrors the old module's `rowProjectRecords` shape but stays in
the new diff encoding: a per-row evaluation error keeps the row in
the carrier and routes the failure through the err-count component
of `Diff` rather than replacing the row with an `.err` carrier. -/

/-- Build an `ErrCount` accumulating each erroring scalar's payload
with count `n`. Order matches the expression order within the row;
duplicates accumulate via `ErrCount.add`. -/
def rowErrCount (es : List Expr) (r : Row) (n : Int) : ErrCount :=
  (rowErrs es r).foldr (fun e acc => ErrCount.single e n + acc) 0

/-- Per-`Diff` project action on a single row. -/
@[inline] private def projectOne (es : List Expr) (r : Row) (d : Diff) :
    Row × DiffWithGlobal :=
  if rowAllSafe es r then
    -- every scalar evaluates cleanly: replace row by projected row,
    -- keep the diff.
    (es.map (eval r), DiffWithGlobal.val d)
  else
    -- some scalar errs: preserve the row, zero the valid count, add
    -- one err-count entry per erroring scalar with count d.val.
    (r, DiffWithGlobal.val { val := 0
                            , errs := d.errs + rowErrCount es r d.val })

/-- Diff-aware projection on the new unified stream. -/
def project (es : List Expr) (us : UnifiedStream2) : UnifiedStream2 :=
  us.map fun rd => match rd with
    | (r, DiffWithGlobal.val d) => projectOne es r d
    | (_, DiffWithGlobal.global) => rd

/-! ### Reduction lemmas -/

theorem project_nil_stream (es : List Expr) :
    project es [] = [] := rfl

theorem project_append (es : List Expr) (a b : UnifiedStream2) :
    project es (a ++ b) = project es a ++ project es b := by
  show (a ++ b).map _ = a.map _ ++ b.map _
  exact List.map_append

end UnifiedStream2

end Mz
