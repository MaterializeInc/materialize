import Mz.UnifiedStream
import Mz.Boolean
import Mz.Expr
import Mz.Eval

/-!
# Filter fusion

The Rust optimizer's `fusion/filter.rs` pass collapses adjacent
filters: `filter p ∘ filter q ↝ filter (q ∧ p)`. The denotational
statement holds under a row-level err-freedom side condition:
neither predicate may evaluate to `.err _` on any data row of the
input stream.

The side condition is forced by an interaction between filter's
err-promotion rule (a predicate `err` on a `.row r` record routes
the err into the carrier, keeping the record) and `evalAnd`'s
clause ordering (a `.bool false` argument absorbs everything,
including `.err`). Without err-freedom the fusion fails on, e.g.,
`(eval r q, eval r p) = (.err e, .bool false)`: `filter q` first
produces `(.err e, _)`, which `filter p` keeps via the err-carrier
arm; whereas `filter (.and q p)` reduces to `.bool false` and
drops the record.

Carrier-err records (`.err`) and collection-err records
(`.error` diff) flow through both pipelines unchanged — the err-
freedom hypothesis is only required at `.row` records. -/

namespace Mz

/-- Predicate err-freedom on a stream's data rows. A predicate is
*data-err-free* on `us` when, for every `.row r` carrier in `us`,
`eval r e` is not an `.err _`. This is the precise hypothesis the
filter-fusion theorem needs at the `.row` arm: it rules out the
ordering mismatch between filter's err-promotion and `evalAnd`'s
`.bool false`-absorbs-everything clause. -/
def UnifiedStream.predNoRowErr (e : Expr) (us : UnifiedStream) : Prop :=
  ∀ ud ∈ us, ∀ r, ud.1 = UnifiedRow.row r → ∀ ev, eval r e ≠ Datum.err ev

theorem UnifiedStream.predNoRowErr.tail {e : Expr}
    {hd : UnifiedRow × DiffWithError Int} {tl : UnifiedStream}
    (h : UnifiedStream.predNoRowErr e (hd :: tl)) :
    UnifiedStream.predNoRowErr e tl :=
  fun ud hMem => h ud (List.mem_cons_of_mem _ hMem)

theorem UnifiedStream.predNoRowErr.head {e : Expr}
    {hd : UnifiedRow × DiffWithError Int} {tl : UnifiedStream}
    (h : UnifiedStream.predNoRowErr e (hd :: tl)) :
    ∀ r, hd.1 = UnifiedRow.row r → ∀ ev, eval r e ≠ Datum.err ev :=
  h hd List.mem_cons_self

/-! ## Per-record fusion at a data row

Single-record filter pipelines line up with `filter (.and q p)`
when err-freedom holds at the row. The proof walks every non-err
`Datum` shape `eval r q` can produce; in the `.bool true`
keep-arm, it walks every non-err shape `eval r p` can produce. -/

/-- Filter reduces on a `.row` singleton to a `match eval r _`. -/
private theorem filter_row_singleton (e : Expr) (r : Row) (n : Int) :
    UnifiedStream.filter e [(UnifiedRow.row r, DiffWithError.val n)]
      = (match eval r e with
          | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
          | .err ev    => [(UnifiedRow.err ev, DiffWithError.val n)]
          | _          => []) := by
  show (match eval r e with
          | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
          | .err ev    => [(UnifiedRow.err ev, DiffWithError.val n)]
          | _          => []) ++ [] = _
  rw [List.append_nil]

private theorem filter_fusion_row
    (q p : Expr) (r : Row) (n : Int)
    (hQ : ∀ ev, eval r q ≠ Datum.err ev)
    (hP : ∀ ev, eval r p ≠ Datum.err ev) :
    UnifiedStream.filter p
        (UnifiedStream.filter q [(UnifiedRow.row r, DiffWithError.val n)])
      = UnifiedStream.filter (Expr.and q p)
          [(UnifiedRow.row r, DiffWithError.val n)] := by
  rw [filter_row_singleton q r n, filter_row_singleton (Expr.and q p) r n]
  have hEvalAnd : eval r (Expr.and q p) = evalAnd (eval r q) (eval r p) := by
    simp only [eval]
  rw [hEvalAnd]
  cases hQ' : eval r q with
  | err e => exact absurd hQ' (hQ e)
  | bool b =>
    cases b with
    | true =>
      -- LHS becomes `filter p [(.row r, .val n)]`.
      rw [filter_row_singleton p r n]
      cases hP' : eval r p with
      | err e => exact absurd hP' (hP e)
      | bool b' => cases b' with | true => rfl | false => rfl
      | null => rfl
      | int _ => rfl
    | false =>
      -- LHS becomes `filter p []` which is `[]`.
      -- RHS match on `evalAnd .bool false _ = .bool false` → `[]`.
      cases hP' : eval r p with
      | err e => exact absurd hP' (hP e)
      | bool _ => rfl
      | null => rfl
      | int _ => rfl
  | null =>
    cases hP' : eval r p with
    | err e => exact absurd hP' (hP e)
    | bool b =>
      cases b with
      | true => rfl
      | false => rfl
    | null => rfl
    | int _ => rfl
  | int k =>
    cases hP' : eval r p with
    | err e => exact absurd hP' (hP e)
    | bool b =>
      cases b with
      | true =>
        -- `evalAnd (.int k) (.bool true) = .int k`; match drops via `_`.
        rfl
      | false => rfl
    | null => rfl
    | int m =>
      by_cases hKM : k = m
      · have hEA : evalAnd (Datum.int k) (Datum.int m) = Datum.int k := by
          show (if k = m then Datum.int k else Datum.null) = Datum.int k
          rw [if_pos hKM]
        rw [hEA]; rfl
      · have hEA : evalAnd (Datum.int k) (Datum.int m) = Datum.null := by
          show (if k = m then Datum.int k else Datum.null) = Datum.null
          rw [if_neg hKM]
        rw [hEA]; rfl

/-! ## Main fusion theorem -/

/-- Adjacent filters fuse: `filter p ∘ filter q = filter (.and q p)`
when neither predicate triggers an `.err` on any data row of the
input stream. The hypothesis is sharp — see file docstring for the
ordering corner where err-freedom is required. -/
theorem UnifiedStream.filter_filter_fuse
    (q p : Expr) (us : UnifiedStream)
    (hQ : UnifiedStream.predNoRowErr q us)
    (hP : UnifiedStream.predNoRowErr p us) :
    UnifiedStream.filter p (UnifiedStream.filter q us)
      = UnifiedStream.filter (Expr.and q p) us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hTlQ : UnifiedStream.predNoRowErr q tl := hQ.tail
    have hTlP : UnifiedStream.predNoRowErr p tl := hP.tail
    have hConsAsApp : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
    rw [hConsAsApp, UnifiedStream.filter_append,
        UnifiedStream.filter_append, UnifiedStream.filter_append, ih hTlQ hTlP]
    congr 1
    cases d with
    | error => rfl
    | val n =>
      cases uc with
      | err e => rfl
      | row r =>
        have hQr : ∀ ev, eval r q ≠ Datum.err ev :=
          hQ.head r rfl
        have hPr : ∀ ev, eval r p ≠ Datum.err ev :=
          hP.head r rfl
        exact filter_fusion_row q p r n hQr hPr

end Mz
