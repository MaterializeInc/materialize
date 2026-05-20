import Mz.UnifiedStream2
import Mz.DiffErrCount
import Mz.DiffWithGlobal
import Mz.Boolean
import Mz.Expr
import Mz.Eval
import Mz.Laws

/-!
# Filter fusion on the new unified stream

Mirror of `Mz/FilterFusion.lean` over `UnifiedStream2`. The new stream
keeps the row in the carrier and routes row-scoped predicate errors
through the `Diff.errs` axis (see
`doc/developer/design/20260517_error_handling_semantics.md`).
Collection-scoped errors live in the absorbing `.global` branch of
`DiffWithGlobal`.

The Rust optimizer's `fusion/filter.rs` pass collapses adjacent
filters: `filter p ∘ filter q ↝ filter (q ∧ p)`. As in the old model,
the denotational statement holds under a row-level err-freedom side
condition: neither predicate may evaluate to `.err _` on any data row
whose diff is `.val _`.

The new filter never drops a record — it preserves the row and only
mutates the diff's `val` count (zeroing it on non-`.bool true`
results) and `errs` count (adding `e ↦ d.val` on `.err e`).
Consequently the side condition is purely about lining up the err
case with `evalAnd`'s `.bool false`-absorbs-everything clause: a
predicate `.err e` followed by `.bool false` (or vice versa) would
disagree with the fused form, which collapses to `.bool false` and
loses the err entry.

`.global` diffs pass through both pipelines unchanged, so the err-
freedom hypothesis is only required on records whose diff is
`.val _`. -/

namespace Mz

namespace UnifiedStream2

/-- Predicate err-freedom on a stream's data rows. A predicate is
*data-err-free* on `us` when, for every record `(r, .val d)` in `us`,
`eval r e` is not an `.err _`. Records with a `.global` diff carry no
constraint — the collection is already invalid, so the row's err
status is irrelevant to downstream consolidation. -/
def predNoRowErr (e : Expr) (us : UnifiedStream2) : Prop :=
  ∀ rd ∈ us, ∀ d, rd.2 = DiffWithGlobal.val d → ∀ ev, eval rd.1 e ≠ Datum.err ev

theorem predNoRowErr.tail {e : Expr}
    {hd : Row × DiffWithGlobal} {tl : UnifiedStream2}
    (h : predNoRowErr e (hd :: tl)) :
    predNoRowErr e tl :=
  fun rd hMem => h rd (List.mem_cons_of_mem _ hMem)

theorem predNoRowErr.head {e : Expr}
    {hd : Row × DiffWithGlobal} {tl : UnifiedStream2}
    (h : predNoRowErr e (hd :: tl)) :
    ∀ d, hd.2 = DiffWithGlobal.val d → ∀ ev, eval hd.1 e ≠ Datum.err ev :=
  h hd List.mem_cons_self

/-! ## Per-record fusion at a data row

Single-record filter pipelines line up with `filter (.and q p)` when
err-freedom holds at the row. The proof walks every non-err `Datum`
shape `eval r q` can produce; in each branch it walks every non-err
shape `eval r p` can produce. Unlike the old filter, the new filter
keeps the row in every branch — only the diff's `val` count and
`errs` count change — so the per-record case analysis only has to
match diff components, not list shapes. -/

/-- Filter reduces on a singleton `.val` record to a `match eval r _`
on `filterOne`. -/
private theorem filter_val_singleton (e : Expr) (r : Row) (d : Diff) :
    filter e [(r, DiffWithGlobal.val d)]
      = [(match eval r e with
          | .bool true => (r, DiffWithGlobal.val d)
          | .err ev    => (r, DiffWithGlobal.val
                              { val := 0
                              , errs := d.errs + ErrCount.single ev d.val })
          | _          => (r, DiffWithGlobal.val { val := 0, errs := d.errs }))] := by
  show (match eval r e with
          | .bool true => _
          | .err ev    => _
          | _          => _) :: [] = _
  rfl

/-- Filter passes a singleton `.global` record through unchanged. -/
private theorem filter_global_singleton (e : Expr) (r : Row) :
    filter e [(r, DiffWithGlobal.global)] = [(r, DiffWithGlobal.global)] := rfl

private theorem filter_fusion_val
    (q p : Expr) (r : Row) (d : Diff)
    (hQ : ∀ ev, eval r q ≠ Datum.err ev)
    (hP : ∀ ev, eval r p ≠ Datum.err ev) :
    filter p (filter q [(r, DiffWithGlobal.val d)])
      = filter (Expr.and q p) [(r, DiffWithGlobal.val d)] := by
  rw [filter_val_singleton q r d, filter_val_singleton (Expr.and q p) r d]
  have hEvalAnd : eval r (Expr.and q p) = evalAnd (eval r q) (eval r p) := by
    simp only [eval]
  rw [hEvalAnd]
  -- The RHS is now a singleton list whose head is determined by
  -- `evalAnd (eval r q) (eval r p)`. The LHS still needs `filter p`
  -- to reduce on the singleton produced by the inner `filter q`.
  cases hQ' : eval r q with
  | err e => exact absurd hQ' (hQ e)
  | bool b =>
    cases b with
    | true =>
      -- Inner result: `(r, .val d)`. Outer filter on the same row.
      rw [filter_val_singleton p r d]
      cases hP' : eval r p with
      | err e => exact absurd hP' (hP e)
      | bool b' => cases b' with | true => rfl | false => rfl
      | null => rfl
      | int _ => rfl
    | false =>
      -- Inner result: `(r, .val { val := 0, errs := d.errs })`.
      -- Outer filter sees the same row again.
      rw [filter_val_singleton p r { val := 0, errs := d.errs }]
      cases hP' : eval r p with
      | err e => exact absurd hP' (hP e)
      | bool b' => cases b' with | true => rfl | false => rfl
      | null => rfl
      | int _ => rfl
  | null =>
    rw [filter_val_singleton p r { val := 0, errs := d.errs }]
    cases hP' : eval r p with
    | err e => exact absurd hP' (hP e)
    | bool b => cases b with | true => rfl | false => rfl
    | null => rfl
    | int _ => rfl
  | int k =>
    rw [filter_val_singleton p r { val := 0, errs := d.errs }]
    cases hP' : eval r p with
    | err e => exact absurd hP' (hP e)
    | bool b =>
      cases b with
      | true =>
        -- `evalAnd (.int k) (.bool true) = .int k`; falls in the `_` arm.
        rfl
      | false => rfl
    | null => rfl
    | int m =>
      by_cases hKM : k = m
      · have hEA : evalAnd (Datum.int k) (Datum.int m) = Datum.int k := by
          show (if k = m then Datum.int k else Datum.null) = Datum.int k
          rw [if_pos hKM]
        rw [hEA]
      · have hEA : evalAnd (Datum.int k) (Datum.int m) = Datum.null := by
          show (if k = m then Datum.int k else Datum.null) = Datum.null
          rw [if_neg hKM]
        rw [hEA]

/-! ## Main fusion theorem -/

/-- Adjacent filters fuse: `filter p ∘ filter q = filter (.and q p)`
when neither predicate triggers an `.err` on any `.val`-diff record
of the input stream. `.global`-diff records pass through both
pipelines unchanged. -/
theorem filter_filter_fuse
    (q p : Expr) (us : UnifiedStream2)
    (hQ : predNoRowErr q us)
    (hP : predNoRowErr p us) :
    filter p (filter q us) = filter (Expr.and q p) us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨r, dg⟩ := hd
    have hTlQ : predNoRowErr q tl := hQ.tail
    have hTlP : predNoRowErr p tl := hP.tail
    have hConsAsApp : ((r, dg) :: tl : UnifiedStream2) = [(r, dg)] ++ tl := rfl
    rw [hConsAsApp, filter_append, filter_append, filter_append, ih hTlQ hTlP]
    congr 1
    cases dg with
    | global =>
      -- `.global` passes through both pipelines, including the
      -- fused-predicate one.
      rfl
    | val d =>
      have hQr : ∀ ev, eval r q ≠ Datum.err ev :=
        hQ.head d rfl
      have hPr : ∀ ev, eval r p ≠ Datum.err ev :=
        hP.head d rfl
      exact filter_fusion_val q p r d hQr hPr

/-! ## Idempotence (no hypothesis required)

Unlike the old `UnifiedStream.filter`, the new filter never drops a
record — every input record produces exactly one output record with
the same row. Idempotence holds unconditionally: the second pass
sees the same `eval r pred`, and the diff transformation under
`val := 0` is fixed by another application of itself. -/

/-- Helper: `ErrCount.single e 0 = 0` pointwise. The conditional
returns `0` on both branches. -/
private theorem errCount_single_zero (e : EvalError) :
    ErrCount.single e 0 = (0 : ErrCount) := by
  funext e'
  show (if e = e' then (0 : Int) else 0) = 0
  split <;> rfl

/-- Helper: the err-arm output of `filterOne` is a fixed point of
the err-arm of `filterOne`. After one application, the diff's
`val` is zero, so the next err-arm adds `ErrCount.single e 0 = 0`. -/
private theorem filterOne_err_idem (r : Row) (d : Diff) (e : EvalError) :
    (r, DiffWithGlobal.val
          { val := 0
          , errs := (d.errs + ErrCount.single e d.val) + ErrCount.single e 0 })
      = (r, DiffWithGlobal.val
              { val := 0
              , errs := d.errs + ErrCount.single e d.val }) := by
  congr 2
  rw [errCount_single_zero, ErrCount.add_zero]

/-- Filter is idempotent: applying the same predicate twice equals
applying it once. Holds unconditionally because the new filter
preserves rows in every branch and the diff transformation is
fixed by re-application. -/
theorem filter_idem (pred : Expr) (us : UnifiedStream2) :
    filter pred (filter pred us) = filter pred us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨r, dg⟩ := hd
    have hConsAsApp : ((r, dg) :: tl : UnifiedStream2) = [(r, dg)] ++ tl := rfl
    rw [hConsAsApp, filter_append, filter_append, ih]
    congr 1
    cases dg with
    | global => rfl
    | val d =>
      rw [filter_val_singleton pred r d]
      cases hEval : eval r pred with
      | bool b =>
        cases b with
        | true =>
          -- First filter keeps `(r, .val d)`; second filter sees the
          -- same `eval r pred = .bool true` and keeps it again.
          rw [filter_val_singleton pred r d, hEval]
        | false =>
          -- First filter outputs `(r, .val { val := 0, errs := d.errs })`;
          -- second filter on `eval r pred = .bool false` zeros the val
          -- again, leaving the errs unchanged.
          rw [filter_val_singleton pred r { val := 0, errs := d.errs }, hEval]
      | err e =>
        -- First filter outputs `(r, .val { val := 0, errs := d.errs + single e d.val })`;
        -- second filter sees `eval r pred = .err e` and adds
        -- `single e 0 = 0`, leaving the diff unchanged.
        rw [filter_val_singleton pred r { val := 0
                                        , errs := d.errs + ErrCount.single e d.val },
            hEval]
        show [_] = [_]
        congr 1
        exact filterOne_err_idem r d e
      | null =>
        rw [filter_val_singleton pred r { val := 0, errs := d.errs }, hEval]
      | int _ =>
        rw [filter_val_singleton pred r { val := 0, errs := d.errs }, hEval]

/-! ## Filter under eval-equivalent predicates

Two predicates that agree on every `.val`-diff row produce equal
outputs. The proof walks the stream and reduces to per-record
equality via `filter_val_singleton` for `.val` and definitional
equality for `.global`. -/

/-- Filters with eval-equivalent predicates on every `.val`-diff
row produce equal outputs. Useful for re-associating or re-ordering
fused predicates without re-running the full filter analysis. -/
theorem filter_eval_eq
    (p q : Expr) (us : UnifiedStream2)
    (h : ∀ rd ∈ us, ∀ d, rd.2 = DiffWithGlobal.val d → eval rd.1 p = eval rd.1 q) :
    filter p us = filter q us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨r, dg⟩ := hd
    have hTl : ∀ rd ∈ tl, ∀ d, rd.2 = DiffWithGlobal.val d →
        eval rd.1 p = eval rd.1 q :=
      fun rd hMem => h rd (List.mem_cons_of_mem _ hMem)
    have hConsAsApp : ((r, dg) :: tl : UnifiedStream2) = [(r, dg)] ++ tl := rfl
    rw [hConsAsApp, filter_append, filter_append, ih hTl]
    congr 1
    cases dg with
    | global => rfl
    | val d =>
      have hRow : eval r p = eval r q :=
        h (r, DiffWithGlobal.val d) List.mem_cons_self d rfl
      rw [filter_val_singleton p r d, filter_val_singleton q r d, hRow]

/-! ## Filter commutativity (under err-freedom)

Two filters commute when neither predicate errors on any `.val`-diff
row. Reduces to `filter_filter_fuse` applied both ways, then equates
`.and q p` with `.and p q` via `evalAnd_comm_of_no_err`. -/

/-- `Datum` is not an err iff none of its `err _` matches. Restates
the err-freedom hypothesis used here in the form
`evalAnd_comm_of_no_err` expects (`¬d.IsErr`). -/
private theorem datum_not_isErr_of_no_err {d : Datum}
    (h : ∀ ev, d ≠ Datum.err ev) : ¬d.IsErr := by
  cases d with
  | err e => exact absurd rfl (h e)
  | bool _ => exact id
  | int _ => exact id
  | null => exact id

/-- Filters commute when neither predicate errors on any `.val`-diff
row of the input. -/
theorem filter_comm
    (q p : Expr) (us : UnifiedStream2)
    (hQ : predNoRowErr q us)
    (hP : predNoRowErr p us) :
    filter p (filter q us) = filter q (filter p us) := by
  rw [filter_filter_fuse q p us hQ hP, filter_filter_fuse p q us hP hQ]
  apply filter_eval_eq
  intro rd hMem d hVal
  have hQr : ∀ ev, eval rd.1 q ≠ Datum.err ev := hQ rd hMem d hVal
  have hPr : ∀ ev, eval rd.1 p ≠ Datum.err ev := hP rd hMem d hVal
  have hEvalAndQP : eval rd.1 (Expr.and q p) = evalAnd (eval rd.1 q) (eval rd.1 p) := by
    simp only [eval]
  have hEvalAndPQ : eval rd.1 (Expr.and p q) = evalAnd (eval rd.1 p) (eval rd.1 q) := by
    simp only [eval]
  rw [hEvalAndQP, hEvalAndPQ]
  exact evalAnd_comm_of_no_err
    (datum_not_isErr_of_no_err hQr)
    (datum_not_isErr_of_no_err hPr)

end UnifiedStream2

end Mz
