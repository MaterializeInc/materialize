import Mz.UnifiedStream
import Mz.Pushdown

/-!
# Project fusion

The Rust optimizer's `fusion/map.rs` pass collapses adjacent
projections: `project es' ∘ project es ↝ project (es'[es])`, where
`e' ∈ es'` is rewritten by substituting each `col i` in `e'` with
`es[i]`.

The denotational statement holds when every projection scalar in
`es` succeeds on every data row of the input (`rowAllSafe es r =
true`). The hypothesis is sharp: if `es` errors on some row,
`project es` emits one err record per erroring scalar of `es`;
the fused `project (es'.map (·.subst es))` instead emits one err
record per `e' ∈ es'` whose substituted form errors. Those err
sets agree only in the safe-input case.

Carrier-err records (`.err`) and collection-err records (`.error`
diff) flow through both pipelines unchanged — the safety
hypothesis is only required at `.row` records.

Substitution machinery (`Expr.subst`, `eval_subst`) is imported
from `Mz/Pushdown.lean`. -/

namespace Mz

/-- Safety of `es` on every data row of `us`. The exact hypothesis
the projection-fusion theorem needs at the `.row` arm. -/
def UnifiedStream.projsAllSafe (es : List Expr) (us : UnifiedStream) : Prop :=
  ∀ ud ∈ us, ∀ r, ud.1 = UnifiedRow.row r → rowAllSafe es r = true

theorem UnifiedStream.projsAllSafe.tail {es : List Expr}
    {hd : UnifiedRow × DiffWithError Int} {tl : UnifiedStream}
    (h : UnifiedStream.projsAllSafe es (hd :: tl)) :
    UnifiedStream.projsAllSafe es tl :=
  fun ud hMem => h ud (List.mem_cons_of_mem _ hMem)

theorem UnifiedStream.projsAllSafe.head {es : List Expr}
    {hd : UnifiedRow × DiffWithError Int} {tl : UnifiedStream}
    (h : UnifiedStream.projsAllSafe es (hd :: tl)) :
    ∀ r, hd.1 = UnifiedRow.row r → rowAllSafe es r = true :=
  h hd List.mem_cons_self

/-! ## Substitution preserves the row-projection record set

Under no hypothesis, projecting `es'` against the row produced by
projecting `es` first agrees with projecting the substituted
expression list `es'.map (·.subst es)` against the original row.
This is the row-level fusion law; the stream-level fusion lifts it
under the safety hypothesis. -/

private theorem rowAllSafe_substList (es es' : List Expr) (r : Row) :
    rowAllSafe (es'.map (·.subst es)) r
      = rowAllSafe es' (es.map (eval r)) := by
  unfold rowAllSafe
  rw [List.all_map]
  congr 1
  funext e'
  show (match eval r (e'.subst es) with | .err _ => false | _ => true)
        = (match eval (es.map (eval r)) e' with | .err _ => false | _ => true)
  rw [eval_subst r es e']

private theorem rowErrs_substList (es es' : List Expr) (r : Row) :
    rowErrs (es'.map (·.subst es)) r
      = rowErrs es' (es.map (eval r)) := by
  unfold rowErrs
  rw [List.filterMap_map]
  congr 1
  funext e'
  show (match eval r (e'.subst es) with | .err err => some err | _ => none)
        = (match eval (es.map (eval r)) e' with | .err err => some err | _ => none)
  rw [eval_subst r es e']

private theorem evalList_substList (es es' : List Expr) (r : Row) :
    (es'.map (·.subst es)).map (eval r) = es'.map (eval (es.map (eval r))) := by
  rw [List.map_map]
  apply List.map_congr_left
  intro e' _
  exact eval_subst r es e'

private theorem rowProjectRecords_substList
    (es es' : List Expr) (d : DiffWithError Int) (r : Row) :
    rowProjectRecords (es'.map (·.subst es)) d r
      = rowProjectRecords es' d (es.map (eval r)) := by
  unfold rowProjectRecords
  rw [rowAllSafe_substList, rowErrs_substList]
  by_cases hSafe : rowAllSafe es' (es.map (eval r)) = true
  · rw [if_pos hSafe, if_pos hSafe, evalList_substList]
  · rw [if_neg hSafe, if_neg hSafe]

/-! ## Main fusion theorem -/

/-- Adjacent projections fuse: `project es' ∘ project es =
project (es'.map (·.subst es))` when `es` is safe on every data
row of the input (no scalar in `es` errors). The hypothesis is
sharp — see file docstring for the err-set divergence. -/
theorem UnifiedStream.project_project_fuse
    (es es' : List Expr) (us : UnifiedStream)
    (hSafe : UnifiedStream.projsAllSafe es us) :
    UnifiedStream.project es' (UnifiedStream.project es us)
      = UnifiedStream.project (es'.map (·.subst es)) us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hTl : UnifiedStream.projsAllSafe es tl := hSafe.tail
    have hConsAsApp : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
    rw [hConsAsApp, UnifiedStream.project_append,
        UnifiedStream.project_append, UnifiedStream.project_append,
        ih hTl]
    congr 1
    cases d with
    | error =>
      -- Both pipelines pass `.error`-diff through unchanged.
      rfl
    | val n =>
      cases uc with
      | err e =>
        -- Both pipelines pass `.err` carrier through unchanged.
        rfl
      | row r =>
        have hHd : rowAllSafe es r = true := hSafe.head r rfl
        -- Step 1: `project es [(.row r, .val n)]` reduces via the
        -- safe branch to `[(.row (es.map (eval r)), .val n)]`.
        have hStep1 : UnifiedStream.project es [(UnifiedRow.row r, DiffWithError.val n)]
                    = [(UnifiedRow.row (es.map (eval r)), DiffWithError.val n)] := by
          show rowProjectRecords es (DiffWithError.val n) r ++ [] = _
          unfold rowProjectRecords
          rw [if_pos hHd, List.append_nil]
        -- Step 2: `project es'` of step 1 = rowProjectRecords es' against
        -- the projected row.
        have hStep2 : UnifiedStream.project es'
                        [(UnifiedRow.row (es.map (eval r)), DiffWithError.val n)]
                    = rowProjectRecords es' (DiffWithError.val n)
                        (es.map (eval r)) := by
          show rowProjectRecords es' (DiffWithError.val n) (es.map (eval r)) ++ [] = _
          rw [List.append_nil]
        -- Fused: same value via the row-level fusion lemma.
        have hFused : UnifiedStream.project (es'.map (·.subst es))
                        [(UnifiedRow.row r, DiffWithError.val n)]
                    = rowProjectRecords (es'.map (·.subst es))
                        (DiffWithError.val n) r := by
          show rowProjectRecords (es'.map (·.subst es)) (DiffWithError.val n) r ++ [] = _
          rw [List.append_nil]
        rw [hStep1, hStep2, hFused,
            rowProjectRecords_substList es es' (DiffWithError.val n) r]

end Mz
