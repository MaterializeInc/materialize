import Mz.Eval
import Mz.Bag
import Mz.ErrStream
import Mz.DiffSemiring

/-!
# Unified data / error / diff stream

`BagStream` (`Mz/ErrStream.lean`) carries a `(data, errors)` pair —
two collections, threaded through every operator. That split
mirrors Materialize's current runtime but is a *pragmatic* choice
rather than a semantic one. The spec target is a single unified
stream where data rows, row-scoped errors, and collection-scoped
("global") errors all flow through one carrier.

This file gives that unified model. The carrier pairs a
`UnifiedRow` (data row or row-scoped error) with a
`DiffWithError Int` (the differential-dataflow multiplicity,
augmented with an absorbing `error` marker that encodes
collection-scoped errors).

## Encoding

`UnifiedRow` is a sum type — either an honest `row` or a bare `err`
without a row. The diff component is from `DiffWithError Int`:
* `.val n` — ordinary differential dataflow multiplicity (positive
  for inserts, negative for retractions, zero for cancellation);
* `.error` — collection-scoped error marker that absorbs through
  addition and multiplication.

`UnifiedStream := List (UnifiedRow × DiffWithError Int)`. Operators
consume and produce `UnifiedStream`s. Row-scoped errors propagate
through the carrier; collection-scoped errors propagate through
diff multiplication / addition.

## Semantic differences with the split form

`UnifiedStream.ofBag` concatenates data rows first and errors
second, fixing an order, and assigns every record a diff of
`.val 1`. The split form makes no commitment between data and
errors. Equivalence between unified and split is therefore exact
on the round trip (`split (ofBag s) = s`) but only up to multiset
equality on the cross-direction `(filter ∘ ofBag) ≈ (ofBag ∘ filter)`.
The skeleton states the round trip; the cross-equivalence is left
for a future iteration that introduces multiset machinery on
`List EvalError`.

`split` discards the diff component, mapping every carrier record
to one bag row regardless of multiplicity. This is lossy for diffs
other than `.val 1` (duplicate rows, retractions, or
collection-scoped errors). The round trip still goes through
because `ofBag` only ever produces `.val 1` diffs.
-/

namespace Mz

inductive UnifiedRow where
  | row (r : Row)
  | err (e : EvalError)
  deriving DecidableEq, Inhabited

/-- A unified-stream record pairs a row-or-error carrier with a
differential-dataflow diff augmented by the absorbing `error`
element. -/
abbrev UnifiedStream := List (UnifiedRow × DiffWithError Int)

/-- Pick the row payload of a unified record, or `none` for
errors. Diff component is discarded. -/
@[inline] private def pickRow : UnifiedRow × DiffWithError Int → Option Row
  | (.row r, _) => some r
  | (.err _, _) => none

/-- Pick the row-scoped error payload of a unified record, or
`none` for data rows. Diff component is discarded. -/
@[inline] private def pickErr : UnifiedRow × DiffWithError Int → Option EvalError
  | (.row _, _) => none
  | (.err e, _) => some e

/-- Pack a `BagStream` into a unified stream: data rows first,
error payloads second, each with diff `.val 1`. -/
def UnifiedStream.ofBag (s : BagStream) : UnifiedStream :=
  s.data.map (fun r => (UnifiedRow.row r, (1 : DiffWithError Int)))
  ++ s.errors.map (fun e => (UnifiedRow.err e, (1 : DiffWithError Int)))

/-- Split a unified stream back into the `(data, errors)` pair.
Diff multiplicities and `.error` diffs are dropped. -/
def UnifiedStream.split (us : UnifiedStream) : BagStream :=
  { data   := us.filterMap pickRow
  , errors := us.filterMap pickErr }

/-- Filter on the unified stream. Records carrying a collection-
scoped `.error` diff pass through unconditionally — the absorbing
diff marker cannot be filtered away without violating the
semiring laws. For other diffs, the predicate is evaluated on
every real `row`: survivors stay with their original diff, rows
whose predicate errs become `err` records (diff unchanged —
multiplicity is preserved through the error route), non-true /
non-error results are dropped. Existing row-scoped `err` records
pass through unchanged. -/
def UnifiedStream.filter (pred : Expr) (us : UnifiedStream) : UnifiedStream :=
  us.flatMap fun ud => match ud with
    | (_,      .error)        => [ud]
    | (.err e, d)             => [(.err e, d)]
    | (.row r, d)             =>
      match eval r pred with
      | .bool true => [(.row r, d)]
      | .err e     => [(.err e, d)]
      | _          => []

/-! ## Helper lemmas for filterMap over the packed concatenation -/

private theorem filterMap_pickRow_rowMap (rs : List Row) :
    (rs.map (fun r => (UnifiedRow.row r, (1 : DiffWithError Int)))).filterMap pickRow
      = rs := by
  induction rs with
  | nil => rfl
  | cons hd tl ih => simp [List.map, pickRow, ih]

private theorem filterMap_pickRow_errMap (es : List EvalError) :
    (es.map (fun e => (UnifiedRow.err e, (1 : DiffWithError Int)))).filterMap pickRow
      = ([] : Relation) := by
  induction es with
  | nil => rfl
  | cons _ tl ih => simp [List.map, pickRow, ih]

private theorem filterMap_pickErr_rowMap (rs : List Row) :
    (rs.map (fun r => (UnifiedRow.row r, (1 : DiffWithError Int)))).filterMap pickErr
      = ([] : List EvalError) := by
  induction rs with
  | nil => rfl
  | cons _ tl ih => simp [List.map, pickErr, ih]

private theorem filterMap_pickErr_errMap (es : List EvalError) :
    (es.map (fun e => (UnifiedRow.err e, (1 : DiffWithError Int)))).filterMap pickErr
      = es := by
  induction es with
  | nil => rfl
  | cons hd tl ih => simp [List.map, pickErr, ih]

/-! ## Round-trip lemmas -/

theorem UnifiedStream.split_data_ofBag (s : BagStream) :
    (UnifiedStream.split (UnifiedStream.ofBag s)).data = s.data := by
  show ((s.data.map (fun r => (UnifiedRow.row r, (1 : DiffWithError Int))))
        ++ (s.errors.map (fun e => (UnifiedRow.err e, (1 : DiffWithError Int))))
       ).filterMap pickRow = s.data
  induction s.data with
  | nil =>
    simp only [List.map_nil, List.nil_append]
    exact filterMap_pickRow_errMap s.errors
  | cons hd tl ih =>
    simp [List.map, pickRow, ih]

theorem UnifiedStream.split_errors_ofBag (s : BagStream) :
    (UnifiedStream.split (UnifiedStream.ofBag s)).errors = s.errors := by
  show ((s.data.map (fun r => (UnifiedRow.row r, (1 : DiffWithError Int))))
        ++ (s.errors.map (fun e => (UnifiedRow.err e, (1 : DiffWithError Int))))
       ).filterMap pickErr = s.errors
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
