import Mz.Eval
import Mz.Bag
import Mz.ErrStream

/-!
# Unified data / error stream

`BagStream` (`Mz/ErrStream.lean`) carries a `(data, errors)` pair —
two collections, threaded through every operator. That split
mirrors Materialize's current runtime but is a *pragmatic* choice
rather than a semantic one. The spec target is a single unified
stream where data rows and errors flow through the same carrier.

This file gives the unified model and the conversion to / from the
split form.

## Encoding

`UnifiedRow` is a sum type: either an honest `row` or a bare `err`
without a row. The absence of a row in the `err` variant preserves
the current property that errors carry no row context (the runtime
`DataflowError` is the same way). A future refinement could attach
optional row provenance.

`UnifiedStream := List UnifiedRow`. Operators consume and produce
`UnifiedStream`s; errors propagate through the carrier
automatically.

## Diff-aware view

The encoding here uses a plain list. The next refinement attaches a
`DiffWithError ℤ` to each record (see `Mz/DiffSemiring.lean`) so the
absorbing `error` diff captures collection-scoped global errors
alongside row-scoped errors. The conversion lemmas below transport
to that refinement when it lands.

## Semantic differences with the split form

`UnifiedStream.ofBag` concatenates data rows first and errors
second, fixing an order. Operators that process records left-to-
right will see data before errors. The split form makes no such
commitment between data and errors. Equivalence between unified
and split is therefore exact on the round trip (`split (ofBag s) =
s`) but only up to multiset equality on the cross-direction
`(filter ∘ ofBag) ≈ (ofBag ∘ filter)`. The skeleton states the
round trip; the cross-equivalence is left for a future iteration
that introduces multiset machinery on `List EvalError`.
-/

namespace Mz

inductive UnifiedRow where
  | row (r : Row)
  | err (e : EvalError)
  deriving Inhabited

abbrev UnifiedStream := List UnifiedRow

/-- Pick the row payload of a `UnifiedRow`, or `none` for errors. -/
@[inline] private def pickRow : UnifiedRow → Option Row
  | .row r => some r
  | .err _ => none

/-- Pick the error payload of a `UnifiedRow`, or `none` for rows. -/
@[inline] private def pickErr : UnifiedRow → Option EvalError
  | .row _ => none
  | .err e => some e

/-- Pack a `BagStream` into a single unified stream: data rows
first, error payloads second. -/
def UnifiedStream.ofBag (s : BagStream) : UnifiedStream :=
  s.data.map UnifiedRow.row ++ s.errors.map UnifiedRow.err

/-- Split a unified stream back into the `(data, errors)` pair. -/
def UnifiedStream.split (us : UnifiedStream) : BagStream :=
  { data   := us.filterMap pickRow
  , errors := us.filterMap pickErr }

/-- Filter on the unified stream. Predicate is evaluated on every
real `row`; survivors stay, erroring rows become `err` records,
non-true / non-error results are dropped. Existing `err` records
pass through unchanged. -/
def UnifiedStream.filter (pred : Expr) (us : UnifiedStream) : UnifiedStream :=
  us.flatMap fun u => match u with
    | .row r =>
      match eval r pred with
      | .bool true => [.row r]
      | .err e     => [.err e]
      | _          => []
    | .err e => [.err e]

/-! ## Helper lemmas for filterMap over the packed concatenation -/

private theorem filterMap_pickRow_rowMap (rs : List Row) :
    (rs.map UnifiedRow.row).filterMap pickRow = rs := by
  induction rs with
  | nil => rfl
  | cons hd tl ih => simp [List.map, pickRow, ih]

private theorem filterMap_pickRow_errMap (es : List EvalError) :
    (es.map UnifiedRow.err).filterMap pickRow = ([] : Relation) := by
  induction es with
  | nil => rfl
  | cons _ tl ih => simp [List.map, pickRow, ih]

private theorem filterMap_pickErr_rowMap (rs : List Row) :
    (rs.map UnifiedRow.row).filterMap pickErr = ([] : List EvalError) := by
  induction rs with
  | nil => rfl
  | cons _ tl ih => simp [List.map, pickErr, ih]

private theorem filterMap_pickErr_errMap (es : List EvalError) :
    (es.map UnifiedRow.err).filterMap pickErr = es := by
  induction es with
  | nil => rfl
  | cons hd tl ih => simp [List.map, pickErr, ih]

/-! ## Round-trip lemmas -/

theorem UnifiedStream.split_data_ofBag (s : BagStream) :
    (UnifiedStream.split (UnifiedStream.ofBag s)).data = s.data := by
  show (s.data.map UnifiedRow.row ++ s.errors.map UnifiedRow.err).filterMap pickRow = s.data
  induction s.data with
  | nil =>
    simp only [List.map_nil, List.nil_append]
    exact filterMap_pickRow_errMap s.errors
  | cons hd tl ih =>
    simp [List.map, pickRow, ih]

theorem UnifiedStream.split_errors_ofBag (s : BagStream) :
    (UnifiedStream.split (UnifiedStream.ofBag s)).errors = s.errors := by
  show (s.data.map UnifiedRow.row ++ s.errors.map UnifiedRow.err).filterMap pickErr = s.errors
  induction s.data with
  | nil =>
    simp only [List.map_nil, List.nil_append]
    exact filterMap_pickErr_errMap s.errors
  | cons _ tl ih =>
    simp [List.map, pickErr, ih]

/-- Full round trip on the structure level. -/
theorem UnifiedStream.split_ofBag (s : BagStream) :
    UnifiedStream.split (UnifiedStream.ofBag s) = s := by
  apply BagStream.ext
  · exact UnifiedStream.split_data_ofBag s
  · exact UnifiedStream.split_errors_ofBag s

end Mz
