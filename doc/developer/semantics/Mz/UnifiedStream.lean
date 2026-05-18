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

/-! ### Reduction lemmas

Named per-list-shape reductions for `filter`. Downstream proofs
cite these instead of unfolding `flatMap` inline. -/

theorem UnifiedStream.filter_nil (pred : Expr) :
    UnifiedStream.filter pred [] = [] := rfl

theorem UnifiedStream.filter_append (pred : Expr) (a b : UnifiedStream) :
    UnifiedStream.filter pred (a ++ b)
      = UnifiedStream.filter pred a ++ UnifiedStream.filter pred b := by
  show (a ++ b).flatMap _ = a.flatMap _ ++ b.flatMap _
  exact List.flatMap_append

-- `project_append` is stated after `project`'s definition below.

/-! ## Project

Diff-aware projection. Each non-error record splits on its carrier:

* `.error` diff: pass through unchanged (the absorbing marker
  cannot be transformed away).
* `.err e` carrier with `.val` diff: pass through unchanged (the
  row-scoped err already represents a failed row; projection has
  nothing to evaluate).
* `.row r` carrier with `.val n` diff: evaluate `es` on `r`. If
  every scalar succeeds, emit `(.row (es.map ...), .val n)`. If any
  scalar errs, emit one `(.err e, .val n)` per erroring scalar —
  multiplicity is preserved per err, mirroring `BagStream.project`'s
  `projectErrs` but lifted into the carrier.

The split between `.row` and `.err` is the unified analogue of
`BagStream.project`'s `(data, errors)` split: the same record kind
holds both, distinguished by the carrier tag. -/

/-- Project a single `.row` record through `es`, returning the
list of unified records the row contributes. Diff is preserved on
every produced record (rows-share-diff, errs-share-diff). Exposed
(not `private`) so cross-file commute theorems like
`project_negate` in `Mz/SetOps.lean` can reason about it directly. -/
def rowProjectRecords (es : List Expr) (d : DiffWithError Int) (r : Row) :
    UnifiedStream :=
  if rowAllSafe es r then
    [(UnifiedRow.row (es.map (eval r)), d)]
  else
    (rowErrs es r).map (fun e => (UnifiedRow.err e, d))

/-- Diff-aware projection. -/
def UnifiedStream.project (es : List Expr) (us : UnifiedStream) : UnifiedStream :=
  us.flatMap fun ud => match ud with
    | (_,      .error) => [ud]
    | (.err e, d)      => [(.err e, d)]
    | (.row r, d)      => rowProjectRecords es d r

/-! ### Trivial cases -/

theorem UnifiedStream.project_nil_stream (es : List Expr) :
    UnifiedStream.project es [] = [] := rfl

theorem UnifiedStream.project_append (es : List Expr) (a b : UnifiedStream) :
    UnifiedStream.project es (a ++ b)
      = UnifiedStream.project es a ++ UnifiedStream.project es b := by
  show (a ++ b).flatMap _ = a.flatMap _ ++ b.flatMap _
  exact List.flatMap_append

/-- The empty projection list cannot error on any row, so every
record passes through with the row collapsed to width zero. -/
theorem UnifiedStream.project_nil_es (us : UnifiedStream) :
    UnifiedStream.project [] us =
      us.map (fun ud => match ud with
        | (_,      .error) => ud
        | (.err e, d)      => (.err e, d)
        | (.row _, d)      => (.row [], d)) := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases d with
    | error =>
      show ([(uc, DiffWithError.error)] : UnifiedStream)
              ++ UnifiedStream.project [] tl
          = (uc, DiffWithError.error)
              :: tl.map (fun ud => match ud with
                | (_,      .error) => ud
                | (.err e, d)      => (.err e, d)
                | (.row _, d)      => (.row [], d))
      simp [ih]
    | val n =>
      cases uc with
      | row r =>
        show rowProjectRecords [] (DiffWithError.val n) r
              ++ UnifiedStream.project [] tl
            = (UnifiedRow.row [], DiffWithError.val n)
              :: tl.map (fun ud => match ud with
                | (_,      .error) => ud
                | (.err e, d)      => (.err e, d)
                | (.row _, d)      => (.row [], d))
        have hSafe : rowAllSafe [] r = true := rfl
        show (if rowAllSafe [] r then
                  [(UnifiedRow.row (([] : List Expr).map (eval r)),
                    DiffWithError.val n)]
                else
                  (rowErrs [] r).map (fun e =>
                    (UnifiedRow.err e, DiffWithError.val n)))
              ++ UnifiedStream.project [] tl
            = (UnifiedRow.row [], DiffWithError.val n)
              :: tl.map (fun ud => match ud with
                | (_,      .error) => ud
                | (.err e, d)      => (.err e, d)
                | (.row _, d)      => (.row [], d))
        rw [if_pos hSafe]
        simp [ih]
      | err e =>
        show ([(UnifiedRow.err e, DiffWithError.val n)] : UnifiedStream)
              ++ UnifiedStream.project [] tl
            = (UnifiedRow.err e, DiffWithError.val n)
              :: tl.map (fun ud => match ud with
                | (_,      .error) => ud
                | (.err e, d)      => (.err e, d)
                | (.row _, d)      => (.row [], d))
        simp [ih]

/-! ### `.error` absorption

A record carrying the absorbing `.error` diff passes through
projection unchanged. Combined with no-error preservation below,
this means `.error` remains the only source of absorbing diffs. -/

theorem UnifiedStream.project_preserves_error_diff
    (es : List Expr) (us : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ us) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.project es us := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc₀, d₀⟩ := hd
    rcases List.mem_cons.mp h with hEq | hTail
    · have hUc : uc = uc₀ := (Prod.mk.injEq _ _ _ _).mp hEq |>.1
      have hD : (DiffWithError.error : DiffWithError Int) = d₀ :=
        (Prod.mk.injEq _ _ _ _).mp hEq |>.2
      subst hUc; subst hD
      show (uc, DiffWithError.error)
          ∈ (([(uc, DiffWithError.error)] : UnifiedStream)
              ++ UnifiedStream.project es tl)
      exact List.mem_append.mpr (Or.inl List.mem_cons_self)
    · have ihResult := ih hTail
      -- The head splits into a list (possibly empty for row + non-safe);
      -- in every shape, `mem_append.mpr (Or.inr ihResult)` discharges the goal.
      show (uc, DiffWithError.error)
          ∈ ((match (uc₀, d₀) with
                | (_,      .error) => [(uc₀, d₀)]
                | (.err e, d)      => [(.err e, d)]
                | (.row r, d)      => rowProjectRecords es d r)
              ++ UnifiedStream.project es tl)
      exact List.mem_append.mpr (Or.inr ihResult)

/-! ### No-error preservation

If every input diff is `.val`, every output diff is `.val`. The
row-projection helper only emits records whose diff is the input
record's diff, so no `.error` is introduced. -/

private theorem rowProjectRecords_no_error
    (es : List Expr) (n : Int) (r : Row) :
    ∀ rec ∈ rowProjectRecords es (DiffWithError.val n) r,
      ∃ m : Int, rec.2 = DiffWithError.val m := by
  intro rec hMem
  unfold rowProjectRecords at hMem
  split at hMem
  · -- branch: all safe; the singleton has diff `.val n`.
    have : rec = (UnifiedRow.row (es.map (eval r)), DiffWithError.val n) :=
      List.mem_singleton.mp hMem
    exact ⟨n, by rw [this]⟩
  · -- branch: some err; every produced record has diff `.val n`.
    obtain ⟨e, _, hRec⟩ := List.mem_map.mp hMem
    exact ⟨n, by rw [← hRec]⟩

theorem UnifiedStream.project_no_error
    (es : List Expr) (us : UnifiedStream)
    (h : ∀ r ∈ us, ∃ n : Int, r.2 = DiffWithError.val n) :
    ∀ r ∈ UnifiedStream.project es us,
      ∃ n : Int, r.2 = DiffWithError.val n := by
  induction us with
  | nil => intro r hMem; exact absurd hMem List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hHd : ∃ n : Int, d = DiffWithError.val n := h (uc, d) List.mem_cons_self
    have hTl : ∀ r ∈ tl, ∃ n : Int, r.2 = DiffWithError.val n :=
      fun r hMem => h r (List.mem_cons_of_mem _ hMem)
    obtain ⟨n, hN⟩ := hHd
    subst hN
    intro rec hMem
    cases uc with
    | row r =>
      have hMem' : rec ∈ rowProjectRecords es (DiffWithError.val n) r
                       ++ UnifiedStream.project es tl := hMem
      rcases List.mem_append.mp hMem' with hHead | hTail
      · exact rowProjectRecords_no_error es n r rec hHead
      · exact ih hTl rec hTail
    | err e =>
      have hMem' : rec ∈ ([(UnifiedRow.err e, DiffWithError.val n)] : UnifiedStream)
                       ++ UnifiedStream.project es tl := hMem
      rcases List.mem_append.mp hMem' with hHead | hTail
      · have : rec = (UnifiedRow.err e, DiffWithError.val n) :=
          List.mem_singleton.mp hHead
        exact ⟨n, by rw [this]⟩
      · exact ih hTl rec hTail

/-! ## Error-scope extractors

The skeleton distinguishes three error scopes:

* **Cell-scoped** (`Datum.err e`): a single cell-value invalid.
  Propagates through scalar evaluators.
* **Row-scoped** (`UnifiedRow.err e`): a whole record carries an
  error instead of a row. Captured in the carrier.
* **Collection-scoped** (`.error` diff in `DiffWithError`): the
  collection itself is invalid at this time/state. Captured in
  the diff.

`errCarriers` and `errorDiffCarriers` project a `UnifiedStream`
to the two record-level error sets, giving the spec a vocabulary
for "how many row-errs and collection-errs in this stream". -/

/-- The list of row-scoped error payloads carried by this stream.
Order matches the input. -/
def UnifiedStream.errCarriers (us : UnifiedStream) : List EvalError :=
  us.filterMap fun ud => match ud.1 with
    | .err e => some e
    | _      => none

/-- The list of carriers whose diff is collection-scoped `.error`.
Order matches the input. -/
def UnifiedStream.errorDiffCarriers (us : UnifiedStream) : List UnifiedRow :=
  us.filterMap fun ud => match ud.2 with
    | .error => some ud.1
    | _      => none

theorem UnifiedStream.errCarriers_nil :
    UnifiedStream.errCarriers [] = [] := rfl

theorem UnifiedStream.errorDiffCarriers_nil :
    UnifiedStream.errorDiffCarriers [] = [] := rfl

theorem UnifiedStream.errCarriers_append (a b : UnifiedStream) :
    UnifiedStream.errCarriers (a ++ b)
      = UnifiedStream.errCarriers a ++ UnifiedStream.errCarriers b := by
  show (a ++ b).filterMap _ = a.filterMap _ ++ b.filterMap _
  exact List.filterMap_append

theorem UnifiedStream.errorDiffCarriers_append (a b : UnifiedStream) :
    UnifiedStream.errorDiffCarriers (a ++ b)
      = UnifiedStream.errorDiffCarriers a ++ UnifiedStream.errorDiffCarriers b := by
  show (a ++ b).filterMap _ = a.filterMap _ ++ b.filterMap _
  exact List.filterMap_append

/-! ### Membership characterizations

Bridge the extractor membership predicates to the underlying
record-membership predicates. These let downstream proofs reason
about error scopes via the carrier and diff projections of
individual records without unfolding the `filterMap`. -/

theorem UnifiedStream.mem_errCarriers (us : UnifiedStream) (e : EvalError) :
    e ∈ UnifiedStream.errCarriers us
      ↔ ∃ d, (UnifiedRow.err e, d) ∈ us := by
  induction us with
  | nil =>
    constructor
    · intro h; exact absurd h List.not_mem_nil
    · intro ⟨_, h⟩; exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases uc with
    | row r =>
      -- Head contributes nothing to errCarriers; reduce to tail.
      have hRed : UnifiedStream.errCarriers ((UnifiedRow.row r, d) :: tl)
                = UnifiedStream.errCarriers tl := rfl
      rw [hRed]
      constructor
      · intro h; obtain ⟨d', hMem⟩ := ih.mp h
        exact ⟨d', List.mem_cons_of_mem _ hMem⟩
      · intro ⟨d', hMem⟩
        rcases List.mem_cons.mp hMem with hHead | hTail
        · -- hHead : (UnifiedRow.err e, d') = (UnifiedRow.row r, d). Impossible.
          have hCarrier : UnifiedRow.err e = UnifiedRow.row r :=
            (Prod.mk.injEq _ _ _ _).mp hHead |>.1
          exact UnifiedRow.noConfusion hCarrier
        · exact ih.mpr ⟨d', hTail⟩
    | err e0 =>
      -- Head contributes `e0` to errCarriers.
      have hRed : UnifiedStream.errCarriers ((UnifiedRow.err e0, d) :: tl)
                = e0 :: UnifiedStream.errCarriers tl := rfl
      rw [hRed]
      constructor
      · intro h
        rcases List.mem_cons.mp h with hHead | hTail
        · subst hHead; exact ⟨d, List.mem_cons_self⟩
        · obtain ⟨d', hMem⟩ := ih.mp hTail
          exact ⟨d', List.mem_cons_of_mem _ hMem⟩
      · intro ⟨d', hMem⟩
        rcases List.mem_cons.mp hMem with hHead | hTail
        · have hErr : UnifiedRow.err e = UnifiedRow.err e0 :=
            (Prod.mk.injEq _ _ _ _).mp hHead |>.1
          have : e = e0 := UnifiedRow.err.inj hErr
          rw [this]; exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (ih.mpr ⟨d', hTail⟩)

theorem UnifiedStream.mem_errorDiffCarriers (us : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers us
      ↔ (uc, (DiffWithError.error : DiffWithError Int)) ∈ us := by
  induction us with
  | nil =>
    constructor
    · intro h; exact absurd h List.not_mem_nil
    · intro h; exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc0, d⟩ := hd
    cases d with
    | val n =>
      -- `.val n` head contributes nothing.
      have hRed : UnifiedStream.errorDiffCarriers ((uc0, DiffWithError.val n) :: tl)
                = UnifiedStream.errorDiffCarriers tl := rfl
      rw [hRed]
      constructor
      · intro h; exact List.mem_cons_of_mem _ (ih.mp h)
      · intro h
        rcases List.mem_cons.mp h with hHead | hTail
        · -- (uc, .error) = (uc0, .val n) impossible (diff mismatch).
          have hDiff : (DiffWithError.error : DiffWithError Int)
                     = DiffWithError.val n :=
            (Prod.mk.injEq _ _ _ _).mp hHead |>.2
          cases hDiff
        · exact ih.mpr hTail
    | error =>
      -- `.error` head contributes `uc0`.
      have hRed : UnifiedStream.errorDiffCarriers ((uc0, DiffWithError.error) :: tl)
                = uc0 :: UnifiedStream.errorDiffCarriers tl := rfl
      rw [hRed]
      constructor
      · intro h
        rcases List.mem_cons.mp h with hHead | hTail
        · subst hHead; exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (ih.mp hTail)
      · intro h
        rcases List.mem_cons.mp h with hHead | hTail
        · have : uc = uc0 := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
          rw [this]; exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (ih.mpr hTail)

/-! ## `project` and error scopes

`project` mirrors `filter` on error-scope behavior:

* Collection-err set is preserved exactly. The `(_, .error)` arm
  passes the absorbing diff through; every other arm produces
  `.val`-diff output only (`rowProjectRecords` preserves the
  input diff verbatim onto each emitted record).
* Row-err set is monotone (grows). The `(.row r, .val n)` arm
  may emit zero or more `.err e` records when scalar evaluation
  fails — the cell-to-row promotion site. -/

private theorem errorDiffCarriers_eq_nil_of_all_val
    (us : UnifiedStream)
    (h : ∀ rec ∈ us, ∃ n : Int, rec.2 = DiffWithError.val n) :
    UnifiedStream.errorDiffCarriers us = [] := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨n, hN⟩ := h hd List.mem_cons_self
    obtain ⟨uc, d⟩ := hd
    have hD : d = DiffWithError.val n := hN
    subst hD
    have hRed : UnifiedStream.errorDiffCarriers
                  ((uc, DiffWithError.val n) :: tl)
              = UnifiedStream.errorDiffCarriers tl := rfl
    rw [hRed]
    exact ih (fun rec hMem => h rec (List.mem_cons_of_mem _ hMem))

private theorem project_singleton_errorDiffCarriers
    (es : List Expr) (uc : UnifiedRow) (d : DiffWithError Int) :
    UnifiedStream.errorDiffCarriers (UnifiedStream.project es [(uc, d)])
      = UnifiedStream.errorDiffCarriers [(uc, d)] := by
  cases d with
  | error =>
    -- First arm: project passes (uc, .error) through.
    cases uc <;> rfl
  | val n =>
    cases uc with
    | err _ => rfl
    | row r =>
      have hP : UnifiedStream.project es
                  [(UnifiedRow.row r, DiffWithError.val n)]
              = rowProjectRecords es (DiffWithError.val n) r := by
        show List.flatMap _ _ = _
        simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      rw [hP]
      have hRhs : UnifiedStream.errorDiffCarriers
                    [(UnifiedRow.row r, DiffWithError.val n)] = [] := rfl
      rw [hRhs]
      exact errorDiffCarriers_eq_nil_of_all_val _
        (rowProjectRecords_no_error es n r)

theorem UnifiedStream.project_errorDiffCarriers
    (es : List Expr) (us : UnifiedStream) :
    UnifiedStream.errorDiffCarriers (UnifiedStream.project es us)
      = UnifiedStream.errorDiffCarriers us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hCons : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
    rw [hCons, UnifiedStream.project_append,
        UnifiedStream.errorDiffCarriers_append,
        UnifiedStream.errorDiffCarriers_append, ih,
        project_singleton_errorDiffCarriers es uc d]

private theorem project_singleton_errCarriers_mono
    (es : List Expr) (uc : UnifiedRow) (d : DiffWithError Int) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers [(uc, d)]) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.project es [(uc, d)]) := by
  cases uc with
  | row r =>
    have hEmpty : UnifiedStream.errCarriers [(UnifiedRow.row r, d)] = [] := rfl
    rw [hEmpty] at h
    exact absurd h List.not_mem_nil
  | err e0 =>
    have hSingle : UnifiedStream.errCarriers [(UnifiedRow.err e0, d)] = [e0] := rfl
    rw [hSingle] at h
    have hEq : e = e0 := List.mem_singleton.mp h
    subst hEq
    cases d with
    | error =>
      have hP : UnifiedStream.project es
                  [(UnifiedRow.err e, DiffWithError.error)]
              = [(UnifiedRow.err e, DiffWithError.error)] := rfl
      rw [hP]
      exact List.mem_singleton.mpr rfl
    | val n =>
      have hP : UnifiedStream.project es
                  [(UnifiedRow.err e, DiffWithError.val n)]
              = [(UnifiedRow.err e, DiffWithError.val n)] := rfl
      rw [hP]
      exact List.mem_singleton.mpr rfl

theorem UnifiedStream.project_errCarriers_mono
    (es : List Expr) (us : UnifiedStream) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers us) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.project es us) := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hCons : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
    rw [hCons, UnifiedStream.errCarriers_append] at h
    rw [hCons, UnifiedStream.project_append, UnifiedStream.errCarriers_append]
    rcases List.mem_append.mp h with hHead | hTail
    · exact List.mem_append.mpr
        (Or.inl (project_singleton_errCarriers_mono es uc d e hHead))
    · exact List.mem_append.mpr (Or.inr (ih hTail))

/-! ## Error-scope escalation

`escalateRowErrs` promotes every row-scoped error to a
collection-scoped error: each `(.err e, _)` record has its diff
overwritten to `.error`. The `.row r` records are untouched.

This is the canonical operator for the "row err means the whole
collection is broken at this point" semantics. The companion
`escalateRowErrs_idem` says re-escalating is a no-op. -/

def UnifiedStream.escalateRowErrs (us : UnifiedStream) : UnifiedStream :=
  us.map fun ud => match ud.1 with
    | .err e => (UnifiedRow.err e, DiffWithError.error)
    | .row _ => ud

theorem UnifiedStream.escalateRowErrs_nil :
    UnifiedStream.escalateRowErrs [] = [] := rfl

theorem UnifiedStream.escalateRowErrs_length (us : UnifiedStream) :
    (UnifiedStream.escalateRowErrs us).length = us.length :=
  List.length_map _

theorem UnifiedStream.escalateRowErrs_idem (us : UnifiedStream) :
    UnifiedStream.escalateRowErrs (UnifiedStream.escalateRowErrs us)
      = UnifiedStream.escalateRowErrs us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases uc with
    | row r =>
      show ((UnifiedRow.row r, d) ::
              (tl.map (fun ud => match ud.1 with
                | .err e => (UnifiedRow.err e, DiffWithError.error)
                | .row _ => ud))).map _
          = (UnifiedRow.row r, d) :: _
      simp only [List.map_cons]
      show (UnifiedRow.row r, d) ::
              (tl.map (fun ud => match ud.1 with
                | .err e => (UnifiedRow.err e, DiffWithError.error)
                | .row _ => ud)).map _
          = (UnifiedRow.row r, d) :: _
      exact congrArg (fun t => (UnifiedRow.row r, d) :: t) ih
    | err e =>
      show ((UnifiedRow.err e, DiffWithError.error) ::
              (tl.map (fun ud => match ud.1 with
                | .err e => (UnifiedRow.err e, DiffWithError.error)
                | .row _ => ud))).map _
          = (UnifiedRow.err e, DiffWithError.error) :: _
      simp only [List.map_cons]
      exact congrArg (fun t => (UnifiedRow.err e, DiffWithError.error) :: t) ih

/-- After escalation, every row-err in the input is also a
collection-err carrier in the output. The row-err set is
preserved (escalation does not delete carriers, only overwrites
their diff). -/
theorem UnifiedStream.escalateRowErrs_errCarriers (us : UnifiedStream) :
    UnifiedStream.errCarriers (UnifiedStream.escalateRowErrs us)
      = UnifiedStream.errCarriers us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases uc with
    | row r =>
      show UnifiedStream.errCarriers ((UnifiedRow.row r, d)
                :: UnifiedStream.escalateRowErrs tl)
          = UnifiedStream.errCarriers ((UnifiedRow.row r, d) :: tl)
      show (UnifiedStream.escalateRowErrs tl).filterMap _ = tl.filterMap _
      exact ih
    | err e =>
      show UnifiedStream.errCarriers
            ((UnifiedRow.err e, DiffWithError.error)
                :: UnifiedStream.escalateRowErrs tl)
          = UnifiedStream.errCarriers ((UnifiedRow.err e, d) :: tl)
      show e :: (UnifiedStream.escalateRowErrs tl).filterMap _
          = e :: tl.filterMap _
      exact congrArg _ ih

/-- After escalation, every row-err carrier from input appears in
the output's collection-err set: the escalation is observable on
`errorDiffCarriers`. -/
theorem UnifiedStream.escalateRowErrs_errCarriers_in_errorDiff
    (us : UnifiedStream) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers us) :
    UnifiedRow.err e
      ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.escalateRowErrs us) := by
  obtain ⟨d, hMem⟩ := (UnifiedStream.mem_errCarriers us e).mp h
  -- Build the membership witness in escalateRowErrs us.
  have hMemMap : (UnifiedRow.err e, (DiffWithError.error : DiffWithError Int))
                  ∈ UnifiedStream.escalateRowErrs us := by
    show (UnifiedRow.err e, DiffWithError.error)
          ∈ us.map (fun ud => match ud.1 with
            | .err e => (UnifiedRow.err e, DiffWithError.error)
            | .row _ => ud)
    refine List.mem_map.mpr ⟨(UnifiedRow.err e, d), hMem, ?_⟩
    rfl
  exact (UnifiedStream.mem_errorDiffCarriers _ _).mpr hMemMap

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
