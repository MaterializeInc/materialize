import Mz.UnifiedStream2
import Mz.Pushdown

/-!
# Project fusion on the new unified stream

Mirror of `Mz/ProjectFusion.lean` over `UnifiedStream2`. The new
stream keeps the row in the carrier (no `.err` row marker) and routes
row-scoped projection errors through the `Diff.errs` axis (see
`doc/developer/design/20260517_error_handling_semantics.md`).

The Rust optimizer's `fusion/map.rs` pass collapses adjacent
projections: `project es' ∘ project es ↝ project (es'[es])`, where
`e' ∈ es'` is rewritten by substituting each `col i` in `e'` with
`es[i]`.

The denotational statement holds when every projection scalar in
`es` succeeds on every data row of the input (`rowAllSafe es r =
true`). In the new model the unsafe branch of `projectOne` preserves
the input row in the carrier — so when both pipelines take the
unsafe branch on the second projection, they keep different rows
(LHS keeps `es.map (eval r)`, RHS keeps `r`). To avoid that
divergence, we additionally require the substituted projection list
`es'.map (·.subst es)` to be safe on every data row of the input,
which (by `rowAllSafe_substList`) is equivalent to `es'` being safe
on every projected row. Under both hypotheses, the second projection
also lands in the safe branch and the pipelines agree.

Substitution machinery (`Expr.subst`, `eval_subst`) is imported
from `Mz/Pushdown.lean`. -/

namespace Mz

/-- Safety of `es` on every data row of `us`. Same shape as the old
`UnifiedStream.projsAllSafe` but quantifies over `Diff` instead of
`DiffWithError Int` and projects `rd.1` (the row) instead of
case-splitting on a `.row` carrier. -/
def UnifiedStream2.projsAllSafe (es : List Expr) (us : UnifiedStream2) : Prop :=
  ∀ rd ∈ us, ∀ d : Diff, rd.2 = DiffWithGlobal.val d → rowAllSafe es rd.1 = true

theorem UnifiedStream2.projsAllSafe.tail {es : List Expr}
    {hd : Row × DiffWithGlobal} {tl : UnifiedStream2}
    (h : UnifiedStream2.projsAllSafe es (hd :: tl)) :
    UnifiedStream2.projsAllSafe es tl :=
  fun rd hMem => h rd (List.mem_cons_of_mem _ hMem)

theorem UnifiedStream2.projsAllSafe.head {es : List Expr}
    {hd : Row × DiffWithGlobal} {tl : UnifiedStream2}
    (h : UnifiedStream2.projsAllSafe es (hd :: tl)) :
    ∀ d : Diff, hd.2 = DiffWithGlobal.val d → rowAllSafe es hd.1 = true :=
  h hd List.mem_cons_self

/-! ## Substitution preserves the row-level projection record

Carry-over from `ProjectFusion.lean`: `rowAllSafe`, `rowErrs`, and
`eval` are shared with the old module, so the substitution-equivalence
lemmas transfer verbatim. -/

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

/-! ## Main fusion theorem

In the new model the projection's unsafe branch preserves the input
row in the carrier. So a `project es'` after a `project es` that
fails on the projected row would keep `es.map (eval r)`, while the
fused `project (es'.map (·.subst es))` failing on `r` keeps `r`.
Those carriers are not equal in general, so the law requires both
pipelines to land in the safe branch on the second projection.

We capture that with a second hypothesis `projsAllSafe (es'.map
(·.subst es)) us` — by `rowAllSafe_substList`, equivalent to `es'`
being safe on every projected row. Combined with `hSafe`, the result
is the obvious analogue of the old fusion theorem. -/

/-- Adjacent projections fuse: `project es' ∘ project es =
project (es'.map (·.subst es))` when both `es` and the substituted
list `es'.map (·.subst es)` are safe on every data row of the input.
The second hypothesis is required because the new model's unsafe
branch of projection preserves the input row in the carrier; without
it the two pipelines diverge on the carrier in the unsafe case (see
file docstring). -/
theorem UnifiedStream2.project_project_fuse
    (es es' : List Expr) (us : UnifiedStream2)
    (hSafe : UnifiedStream2.projsAllSafe es us)
    (hSafe' : UnifiedStream2.projsAllSafe (es'.map (·.subst es)) us) :
    UnifiedStream2.project es' (UnifiedStream2.project es us)
      = UnifiedStream2.project (es'.map (·.subst es)) us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨r, dg⟩ := hd
    have hTl : UnifiedStream2.projsAllSafe es tl := hSafe.tail
    have hTl' : UnifiedStream2.projsAllSafe (es'.map (·.subst es)) tl := hSafe'.tail
    have hConsAsApp : ((r, dg) :: tl : UnifiedStream2) = [(r, dg)] ++ tl := rfl
    rw [hConsAsApp, UnifiedStream2.project_append,
        UnifiedStream2.project_append, UnifiedStream2.project_append,
        ih hTl hTl']
    congr 1
    cases dg with
    | global =>
      -- `.global` passes through every pipeline unchanged.
      rfl
    | val d =>
      have hHd : rowAllSafe es r = true := hSafe.head d rfl
      have hHd' : rowAllSafe (es'.map (·.subst es)) r = true := hSafe'.head d rfl
      -- By `rowAllSafe_substList`, `hHd'` says `es'` is safe on the
      -- projected row.
      have hHdProj : rowAllSafe es' (es.map (eval r)) = true := by
        rw [← rowAllSafe_substList]; exact hHd'
      -- Step 1: `project es [(r, val d)]` reduces to
      -- `[(es.map (eval r), val d)]` via the safe branch.
      have hStep1 : UnifiedStream2.project es [(r, DiffWithGlobal.val d)]
                  = [(es.map (eval r), DiffWithGlobal.val d)] := by
        show (if rowAllSafe es r then
                (es.map (eval r), DiffWithGlobal.val d)
              else
                (r, DiffWithGlobal.val { val := 0
                                        , errs := d.errs
                                                  + UnifiedStream2.rowErrCount es r d.val })) :: [] = _
        rw [if_pos hHd]
      -- Step 2: `project es' [(es.map (eval r), val d)]` reduces to
      -- `[(es'.map (eval (es.map (eval r))), val d)]` via the safe branch.
      have hStep2 : UnifiedStream2.project es'
                      [(es.map (eval r), DiffWithGlobal.val d)]
                  = [(es'.map (eval (es.map (eval r))), DiffWithGlobal.val d)] := by
        show (if rowAllSafe es' (es.map (eval r)) then
                (es'.map (eval (es.map (eval r))), DiffWithGlobal.val d)
              else
                (es.map (eval r),
                  DiffWithGlobal.val
                    { val := 0
                    , errs := d.errs
                              + UnifiedStream2.rowErrCount es' (es.map (eval r)) d.val })) :: [] = _
        rw [if_pos hHdProj]
      -- Fused: `project (es'.map (·.subst es)) [(r, val d)]` reduces to
      -- `[((es'.map (·.subst es)).map (eval r), val d)]` via the safe branch.
      have hFused : UnifiedStream2.project (es'.map (·.subst es))
                      [(r, DiffWithGlobal.val d)]
                  = [((es'.map (·.subst es)).map (eval r), DiffWithGlobal.val d)] := by
        show (if rowAllSafe (es'.map (·.subst es)) r then
                ((es'.map (·.subst es)).map (eval r), DiffWithGlobal.val d)
              else
                (r,
                  DiffWithGlobal.val
                    { val := 0
                    , errs := d.errs
                              + UnifiedStream2.rowErrCount (es'.map (·.subst es)) r d.val })) :: [] = _
        rw [if_pos hHd']
      rw [hStep1, hStep2, hFused, evalList_substList]

/-! ## Filter ∘ project pushdown

Lifts `filterRel_pushdown_project` to `UnifiedStream2` under the same
safety hypothesis the fusion theorem starts with. Filtering after
projecting equals substituting through the projection and filtering
before projecting. Unlike fusion, this law does not need a second
safety hypothesis: in the new model the filter preserves the input
row in every branch, so after the inner `filter (p.subst es)` the
carrier is still `r`, and `project es` then lands in its safe branch
by `hSafe`. -/

/-- Per-`.val` helper. Filter applied to a single projected row record
equals project applied to a filter-substituted singleton. Under
safety, both sides reduce through the projection's safe branch and
`eval_subst` bridges the predicates. -/
private theorem filter_project_pushdown_val
    (p : Expr) (es : List Expr) (d : Diff) (r : Row)
    (hSafe : rowAllSafe es r = true) :
    UnifiedStream2.filter p
        (UnifiedStream2.project es [(r, DiffWithGlobal.val d)])
      = UnifiedStream2.project es
          (UnifiedStream2.filter (p.subst es)
            [(r, DiffWithGlobal.val d)]) := by
  -- Project the singleton via the safe branch.
  have hProj : UnifiedStream2.project es [(r, DiffWithGlobal.val d)]
             = [(es.map (eval r), DiffWithGlobal.val d)] := by
    show (if rowAllSafe es r then
            (es.map (eval r), DiffWithGlobal.val d)
          else
            (r, DiffWithGlobal.val { val := 0
                                    , errs := d.errs
                                              + UnifiedStream2.rowErrCount es r d.val })) :: [] = _
    rw [if_pos hSafe]
  -- Filter the projected singleton.
  have hFilterProj :
      UnifiedStream2.filter p [(es.map (eval r), DiffWithGlobal.val d)]
        = [(match eval (es.map (eval r)) p with
            | .bool true => (es.map (eval r), DiffWithGlobal.val d)
            | .err e     => (es.map (eval r),
                              DiffWithGlobal.val
                                { val := 0
                                , errs := d.errs + ErrCount.single e d.val })
            | _          => (es.map (eval r),
                              DiffWithGlobal.val { val := 0, errs := d.errs }))] := by
    show (match eval (es.map (eval r)) p with
          | .bool true => _
          | .err _     => _
          | _          => _) :: [] = _
    rfl
  -- Filter the original singleton with the substituted predicate.
  have hFilterSubst :
      UnifiedStream2.filter (p.subst es) [(r, DiffWithGlobal.val d)]
        = [(match eval r (p.subst es) with
            | .bool true => (r, DiffWithGlobal.val d)
            | .err e     => (r, DiffWithGlobal.val
                                  { val := 0
                                  , errs := d.errs + ErrCount.single e d.val })
            | _          => (r, DiffWithGlobal.val
                                  { val := 0, errs := d.errs }))] := by
    show (match eval r (p.subst es) with
          | .bool true => _
          | .err _     => _
          | _          => _) :: [] = _
    rfl
  have hEvalSubst : eval r (p.subst es) = eval (es.map (eval r)) p :=
    eval_subst r es p
  rw [hProj, hFilterProj, hFilterSubst, hEvalSubst]
  -- Both sides branch on the same `eval (es.map (eval r)) p`.
  -- After filtering, the RHS still needs the outer `project es` to
  -- reduce on the resulting singleton; we use `hSafe` to land in the
  -- safe branch in every arm.
  cases eval (es.map (eval r)) p with
  | bool b =>
    cases b with
    | true =>
      -- LHS: `[(es.map (eval r), val d)]`.
      -- RHS: `project es [(r, val d)]` = `[(es.map (eval r), val d)]` by hProj.
      show [(es.map (eval r), DiffWithGlobal.val d)]
            = UnifiedStream2.project es [(r, DiffWithGlobal.val d)]
      rw [hProj]
    | false =>
      -- LHS: `[(es.map (eval r), val { val := 0, errs := d.errs })]`.
      -- RHS: `project es [(r, val { val := 0, errs := d.errs })]` lands in
      -- the safe branch (rowAllSafe es r still holds) giving
      -- `[(es.map (eval r), val { val := 0, errs := d.errs })]`.
      show [(es.map (eval r), DiffWithGlobal.val { val := 0, errs := d.errs })]
            = UnifiedStream2.project es
                [(r, DiffWithGlobal.val { val := 0, errs := d.errs })]
      show _ = (if rowAllSafe es r then
                  (es.map (eval r), DiffWithGlobal.val ({ val := 0, errs := d.errs } : Diff))
                else
                  (r, DiffWithGlobal.val { val := 0
                                          , errs := (({ val := 0, errs := d.errs } : Diff).errs)
                                                    + UnifiedStream2.rowErrCount es r
                                                        (({ val := 0, errs := d.errs } : Diff).val) })) :: []
      rw [if_pos hSafe]
  | err e =>
    -- LHS: `[(es.map (eval r), val { val := 0, errs := d.errs + single e d.val })]`.
    -- RHS: `project es [(r, val { val := 0, errs := d.errs + single e d.val })]`
    -- safe branch gives same.
    show [(es.map (eval r),
            DiffWithGlobal.val { val := 0
                                , errs := d.errs + ErrCount.single e d.val })]
          = UnifiedStream2.project es
              [(r, DiffWithGlobal.val
                    { val := 0
                    , errs := d.errs + ErrCount.single e d.val })]
    show _ = (if rowAllSafe es r then _ else _) :: []
    rw [if_pos hSafe]
  | int _ =>
    show [(es.map (eval r), DiffWithGlobal.val { val := 0, errs := d.errs })]
          = UnifiedStream2.project es
              [(r, DiffWithGlobal.val { val := 0, errs := d.errs })]
    show _ = (if rowAllSafe es r then _ else _) :: []
    rw [if_pos hSafe]
  | null =>
    show [(es.map (eval r), DiffWithGlobal.val { val := 0, errs := d.errs })]
          = UnifiedStream2.project es
              [(r, DiffWithGlobal.val { val := 0, errs := d.errs })]
    show _ = (if rowAllSafe es r then _ else _) :: []
    rw [if_pos hSafe]

/-- Filter pushes through project: `filter p ∘ project es =
project es ∘ filter (p.subst es)`, under `projsAllSafe es us`. The
new model's filter preserves the row in every branch, so the outer
`project es` always sees a row on which `es` is safe (by hypothesis),
and the `eval_subst` bridge from the substituted predicate to the
projected row's predicate finishes the equality. -/
theorem UnifiedStream2.filter_project_pushdown
    (p : Expr) (es : List Expr) (us : UnifiedStream2)
    (hSafe : UnifiedStream2.projsAllSafe es us) :
    UnifiedStream2.filter p (UnifiedStream2.project es us)
      = UnifiedStream2.project es (UnifiedStream2.filter (p.subst es) us) := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨r, dg⟩ := hd
    have hTl : UnifiedStream2.projsAllSafe es tl := hSafe.tail
    have hConsAsApp : ((r, dg) :: tl : UnifiedStream2) = [(r, dg)] ++ tl := rfl
    rw [hConsAsApp, UnifiedStream2.project_append,
        UnifiedStream2.filter_append, UnifiedStream2.filter_append,
        UnifiedStream2.project_append, ih hTl]
    congr 1
    cases dg with
    | global =>
      -- `.global` passes through both pipelines unchanged.
      rfl
    | val d =>
      have hHd : rowAllSafe es r = true := hSafe.head d rfl
      exact filter_project_pushdown_val p es d r hHd

end Mz
