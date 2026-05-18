import Mz.Join
import Mz.ColRefs

/-!
# Filter pushdown for cross products

When a join predicate `pred` only references columns of the left
input (`pred.colReferencesBoundedBy N = true` where `N` bounds
every left row's width), `filter pred ∘ cross` may be rewritten
as `cross ∘ (filter pred)` on the left input. This is the
optimizer's canonical join-pushdown rule: shrink the cross
product before the join filter runs.

The full rewrite requires the right input to be free of
row-scoped errors (`.err` carriers) and collection-scoped errors
(`.error` diffs). Without this restriction, `combineCarrier`'s
left-wins rule and the absorbing `.error * d = .error` rule cause
the two sides to disagree on which err payload ends up on the
output (and on whether `.error`-diff rows survive past the
filter). With a clean right input, the pushdown is exact.

The proof is by structural induction on the left input, with a
per-record helper that handles the three left-record shapes
(`.error` diff, `.err` carrier, `.row` with `.val` diff).
-/

namespace Mz

/-- Predicate: every record in `r` has a `.row` carrier and a
`.val` diff. Captures "no row-scoped error, no collection-scoped
error" — the precondition for filter pushdown to commute with
cross. -/
def UnifiedStream.IsPureData (us : UnifiedStream) : Prop :=
  ∀ ud ∈ us, (∃ rb, ud.1 = UnifiedRow.row rb) ∧ (∃ m : Int, ud.2 = DiffWithError.val m)

theorem UnifiedStream.IsPureData.tail {hd : UnifiedRow × DiffWithError Int}
    {tl : UnifiedStream} (h : UnifiedStream.IsPureData (hd :: tl)) :
    UnifiedStream.IsPureData tl :=
  fun ud hMem => h ud (List.mem_cons_of_mem _ hMem)

theorem UnifiedStream.IsPureData.head {hd : UnifiedRow × DiffWithError Int}
    {tl : UnifiedStream} (h : UnifiedStream.IsPureData (hd :: tl)) :
    (∃ rb, hd.1 = UnifiedRow.row rb) ∧ (∃ m : Int, hd.2 = DiffWithError.val m) :=
  h hd List.mem_cons_self

/-- Reduction: `cross [(uc, d)] r` flattens to a per-record map
over `r`. Stated with carrier / diff split out separately so the
expanded body uses concrete projections, not `.fst`/`.snd` on a
tuple — keeps downstream `rfl`s working under induction. -/
theorem UnifiedStream.cross_singleton
    (uc : UnifiedRow) (d : DiffWithError Int) (r : UnifiedStream) :
    UnifiedStream.cross [(uc, d)] r
      = r.map fun rd => (combineCarrier uc rd.1, d * rd.2) := by
  show ([(uc, d)] : UnifiedStream).flatMap _ = _
  simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]

/-! ## Per-record helpers

Each helper handles one shape of left record `(uc, d)` against a
pure-data right input. Combined in the main theorem via case
analysis. The helpers state the equation in the
`filter pred (r.map …) = cross … r` form directly, then use
`cross_singleton` to expand the cross side on demand. -/

/-- For `(uc, .error)` left record: filter passes every cross
output through (`.error` diff absorbs and filter's first arm
matches). Equals the cross of the filtered singleton. -/
private theorem filter_map_error_diff
    (pred : Expr) (uc : UnifiedRow) (r : UnifiedStream) :
    UnifiedStream.filter pred
        (r.map fun rd => (combineCarrier uc rd.1,
          (DiffWithError.error : DiffWithError Int) * rd.2))
      = UnifiedStream.cross
          (UnifiedStream.filter pred
            [(uc, (DiffWithError.error : DiffWithError Int))]) r := by
  have hFilterL : UnifiedStream.filter pred
                    [(uc, (DiffWithError.error : DiffWithError Int))]
                = [(uc, (DiffWithError.error : DiffWithError Int))] := by
    cases uc <;> rfl
  rw [hFilterL, UnifiedStream.cross_singleton]
  induction r with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨ru, rd⟩ := hd
    show UnifiedStream.filter pred
            ((combineCarrier uc ru,
              (DiffWithError.error : DiffWithError Int) * rd) :: tl.map _)
        = (combineCarrier uc ru,
            (DiffWithError.error : DiffWithError Int) * rd) :: tl.map _
    have hCons : ((combineCarrier uc ru,
                    (DiffWithError.error : DiffWithError Int) * rd)
                    :: tl.map (fun rd' => (combineCarrier uc rd'.1,
                                            (DiffWithError.error : DiffWithError Int) * rd'.2))
                  : UnifiedStream)
                = [(combineCarrier uc ru,
                     (DiffWithError.error : DiffWithError Int) * rd)]
                    ++ tl.map (fun rd' => (combineCarrier uc rd'.1,
                                            (DiffWithError.error : DiffWithError Int) * rd'.2)) := rfl
    rw [hCons, UnifiedStream.filter_append, ih]
    rw [DiffWithError.error_mul_left]
    rfl

/-- For `(.err e, .val n)` left record on a pure-data right
input: cross output carriers are `.err e` (left wins), diffs are
`.val (n*m)`. Filter passes `.err`-carrier `.val`-diff records
through via second arm. Equals cross of the filtered singleton. -/
private theorem filter_map_err_carrier
    (pred : Expr) (e : EvalError) (n : Int) (r : UnifiedStream)
    (hR : UnifiedStream.IsPureData r) :
    UnifiedStream.filter pred
        (r.map fun rd => (combineCarrier (UnifiedRow.err e) rd.1,
                           DiffWithError.val n * rd.2))
      = UnifiedStream.cross
          (UnifiedStream.filter pred [(UnifiedRow.err e, DiffWithError.val n)]) r := by
  have hFilterL : UnifiedStream.filter pred
                    [(UnifiedRow.err e, DiffWithError.val n)]
                = [(UnifiedRow.err e, DiffWithError.val n)] := rfl
  rw [hFilterL, UnifiedStream.cross_singleton]
  induction r with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨ru, rd⟩ := hd
    obtain ⟨_, ⟨m, hM⟩⟩ := hR (ru, rd) List.mem_cons_self
    have hTl : UnifiedStream.IsPureData tl := hR.tail
    have hM' : rd = DiffWithError.val m := hM
    subst hM'
    have hComb : combineCarrier (UnifiedRow.err e) ru = UnifiedRow.err e := by
      cases ru <;> rfl
    show UnifiedStream.filter pred
            ((combineCarrier (UnifiedRow.err e) ru,
              DiffWithError.val n * DiffWithError.val m) :: tl.map _)
        = (combineCarrier (UnifiedRow.err e) ru,
            DiffWithError.val n * DiffWithError.val m) :: tl.map _
    rw [hComb]
    have hCons : ((UnifiedRow.err e,
                    DiffWithError.val n * DiffWithError.val m)
                    :: tl.map (fun rd' => (combineCarrier (UnifiedRow.err e) rd'.1,
                                            DiffWithError.val n * rd'.2))
                  : UnifiedStream)
              = [(UnifiedRow.err e,
                   DiffWithError.val n * DiffWithError.val m)]
                  ++ tl.map (fun rd' => (combineCarrier (UnifiedRow.err e) rd'.1,
                                           DiffWithError.val n * rd'.2)) := rfl
    rw [hCons, UnifiedStream.filter_append, ih hTl]
    rfl

/-- `(.row la, .val n)` left with `eval la pred = .bool true` on
a pure-data right input. Filter keeps every cross output:
`eval (la ++ rb) pred = eval la pred = .bool true` by
`eval_append_left_of_bounded`. -/
private theorem filter_map_row_val_keep
    (pred : Expr) (la : Row) (n : Int) (r : UnifiedStream)
    (hBound' : pred.colReferencesBoundedBy la.length = true)
    (hEval : eval la pred = Datum.bool true)
    (hR : UnifiedStream.IsPureData r) :
    UnifiedStream.filter pred
        (r.map fun rd => (combineCarrier (UnifiedRow.row la) rd.1,
                           DiffWithError.val n * rd.2))
      = UnifiedStream.cross
          (UnifiedStream.filter pred [(UnifiedRow.row la, DiffWithError.val n)]) r := by
  have hFilterL : UnifiedStream.filter pred
                    [(UnifiedRow.row la, DiffWithError.val n)]
                = [(UnifiedRow.row la, DiffWithError.val n)] := by
    show (match eval la pred with
            | .bool true => [(UnifiedRow.row la, DiffWithError.val n)]
            | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
            | _          => []) ++ [] = _
    rw [hEval]; rfl
  rw [hFilterL, UnifiedStream.cross_singleton]
  induction r with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨ru, rd⟩ := hd
    obtain ⟨⟨rb, hRb⟩, ⟨m, hM⟩⟩ := hR (ru, rd) List.mem_cons_self
    have hTl : UnifiedStream.IsPureData tl := hR.tail
    subst hRb; subst hM
    have hComb : combineCarrier (UnifiedRow.row la) (UnifiedRow.row rb)
              = UnifiedRow.row (la ++ rb) := rfl
    have hMul : (DiffWithError.val n : DiffWithError Int) * DiffWithError.val m
              = DiffWithError.val (n * m) := rfl
    show UnifiedStream.filter pred
            (((UnifiedRow.row rb, DiffWithError.val m) :: tl).map _)
        = ((UnifiedRow.row rb, DiffWithError.val m) :: tl).map _
    simp only [List.map_cons]
    rw [hComb, hMul]
    have hConsLhs : ((UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))
                      :: tl.map (fun rd' =>
                          (combineCarrier (UnifiedRow.row la) rd'.1,
                            DiffWithError.val n * rd'.2))
                    : UnifiedStream)
                = [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
                    ++ tl.map (fun rd' =>
                          (combineCarrier (UnifiedRow.row la) rd'.1,
                            DiffWithError.val n * rd'.2)) := rfl
    rw [hConsLhs, UnifiedStream.filter_append, ih hTl]
    have hEvalApp : eval (la ++ rb) pred = Datum.bool true := by
      rw [eval_append_left_of_bounded la rb pred hBound', hEval]
    have hFilterHd : UnifiedStream.filter pred
                      [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
                  = [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))] := by
      show (match eval (la ++ rb) pred with
              | .bool true => [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
              | .err e     => [(UnifiedRow.err e, DiffWithError.val (n * m))]
              | _          => []) ++ [] = _
      rw [hEvalApp]; rfl
    rw [hFilterHd]

/-- `(.row la, .val n)` left with `eval la pred = .err e_pred`:
filter promotes every cross output to `.err e_pred` carrier. -/
private theorem filter_map_row_val_err
    (pred : Expr) (la : Row) (n : Int) (r : UnifiedStream) (e_pred : EvalError)
    (hBound' : pred.colReferencesBoundedBy la.length = true)
    (hEval : eval la pred = Datum.err e_pred)
    (hR : UnifiedStream.IsPureData r) :
    UnifiedStream.filter pred
        (r.map fun rd => (combineCarrier (UnifiedRow.row la) rd.1,
                           DiffWithError.val n * rd.2))
      = UnifiedStream.cross
          (UnifiedStream.filter pred [(UnifiedRow.row la, DiffWithError.val n)]) r := by
  have hFilterL : UnifiedStream.filter pred
                    [(UnifiedRow.row la, DiffWithError.val n)]
                = [(UnifiedRow.err e_pred, DiffWithError.val n)] := by
    show (match eval la pred with
            | .bool true => [(UnifiedRow.row la, DiffWithError.val n)]
            | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
            | _          => []) ++ [] = _
    rw [hEval]; rfl
  rw [hFilterL, UnifiedStream.cross_singleton]
  induction r with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨ru, rd⟩ := hd
    obtain ⟨⟨rb, hRb⟩, ⟨m, hM⟩⟩ := hR (ru, rd) List.mem_cons_self
    have hTl : UnifiedStream.IsPureData tl := hR.tail
    subst hRb; subst hM
    have hCombRow : combineCarrier (UnifiedRow.row la) (UnifiedRow.row rb)
                 = UnifiedRow.row (la ++ rb) := rfl
    have hCombErr : combineCarrier (UnifiedRow.err e_pred) (UnifiedRow.row rb)
                 = UnifiedRow.err e_pred := rfl
    have hMul : (DiffWithError.val n : DiffWithError Int) * DiffWithError.val m
              = DiffWithError.val (n * m) := rfl
    show UnifiedStream.filter pred
            (((UnifiedRow.row rb, DiffWithError.val m) :: tl).map _)
        = ((UnifiedRow.row rb, DiffWithError.val m) :: tl).map _
    simp only [List.map_cons]
    rw [hCombRow, hCombErr, hMul]
    have hConsLhs : ((UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))
                      :: tl.map (fun rd' =>
                          (combineCarrier (UnifiedRow.row la) rd'.1,
                            DiffWithError.val n * rd'.2))
                    : UnifiedStream)
                = [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
                    ++ tl.map (fun rd' =>
                          (combineCarrier (UnifiedRow.row la) rd'.1,
                            DiffWithError.val n * rd'.2)) := rfl
    rw [hConsLhs, UnifiedStream.filter_append, ih hTl]
    have hEvalApp : eval (la ++ rb) pred = Datum.err e_pred := by
      rw [eval_append_left_of_bounded la rb pred hBound', hEval]
    have hFilterHd : UnifiedStream.filter pred
                      [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
                  = [(UnifiedRow.err e_pred, DiffWithError.val (n * m))] := by
      show (match eval (la ++ rb) pred with
              | .bool true => [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
              | .err e     => [(UnifiedRow.err e, DiffWithError.val (n * m))]
              | _          => []) ++ [] = _
      rw [hEvalApp]; rfl
    rw [hFilterHd]
    rfl

/-- `(.row la, .val n)` left when `eval la pred` is neither
`.bool true` nor `.err _`: filter drops both at the singleton
level and at every cross-output level. -/
private theorem filter_map_row_val_drop
    (pred : Expr) (la : Row) (n : Int) (r : UnifiedStream)
    (hBound' : pred.colReferencesBoundedBy la.length = true)
    (hDrop : UnifiedStream.filter pred [(UnifiedRow.row la, DiffWithError.val n)] = [])
    (hR : UnifiedStream.IsPureData r) :
    UnifiedStream.filter pred
        (r.map fun rd => (combineCarrier (UnifiedRow.row la) rd.1,
                           DiffWithError.val n * rd.2))
      = UnifiedStream.cross
          (UnifiedStream.filter pred [(UnifiedRow.row la, DiffWithError.val n)]) r := by
  rw [hDrop]
  show _ = ([] : UnifiedStream).flatMap _
  rw [List.flatMap_nil]
  have hDropExpand : (match eval la pred with
                       | .bool true => [(UnifiedRow.row la, DiffWithError.val n)]
                       | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                       | _          => ([] : UnifiedStream))
                  = [] := by
    have : UnifiedStream.filter pred [(UnifiedRow.row la, DiffWithError.val n)]
         = (match eval la pred with
              | .bool true => [(UnifiedRow.row la, DiffWithError.val n)]
              | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
              | _          => []) ++ [] := rfl
    rw [this] at hDrop
    rw [List.append_nil] at hDrop
    exact hDrop
  induction r with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨ru, rd⟩ := hd
    obtain ⟨⟨rb, hRb⟩, ⟨m, hM⟩⟩ := hR (ru, rd) List.mem_cons_self
    have hTl : UnifiedStream.IsPureData tl := hR.tail
    subst hRb; subst hM
    have hCombRow : combineCarrier (UnifiedRow.row la) (UnifiedRow.row rb)
                 = UnifiedRow.row (la ++ rb) := rfl
    have hMul : (DiffWithError.val n : DiffWithError Int) * DiffWithError.val m
              = DiffWithError.val (n * m) := rfl
    show UnifiedStream.filter pred
            (((UnifiedRow.row rb, DiffWithError.val m) :: tl).map _) = []
    simp only [List.map_cons]
    rw [hCombRow, hMul]
    have hConsLhs : ((UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))
                      :: tl.map (fun rd' =>
                          (combineCarrier (UnifiedRow.row la) rd'.1,
                            DiffWithError.val n * rd'.2))
                    : UnifiedStream)
                = [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
                    ++ tl.map (fun rd' =>
                          (combineCarrier (UnifiedRow.row la) rd'.1,
                            DiffWithError.val n * rd'.2)) := rfl
    rw [hConsLhs, UnifiedStream.filter_append, ih hTl]
    have hEvalApp : eval (la ++ rb) pred = eval la pred :=
      eval_append_left_of_bounded la rb pred hBound'
    have hFilterHd : UnifiedStream.filter pred
                      [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
                  = [] := by
      show (match eval (la ++ rb) pred with
              | .bool true => [(UnifiedRow.row (la ++ rb), DiffWithError.val (n * m))]
              | .err e     => [(UnifiedRow.err e, DiffWithError.val (n * m))]
              | _          => []) ++ [] = _
      rw [hEvalApp, List.append_nil]
      cases hC : eval la pred with
      | bool b =>
        cases b with
        | true =>
          exfalso
          rw [hC] at hDropExpand
          exact List.cons_ne_nil _ _ hDropExpand
        | false => rfl
      | err e =>
        exfalso
        rw [hC] at hDropExpand
        exact List.cons_ne_nil _ _ hDropExpand
      | int _ => rfl
      | null  => rfl
    rw [hFilterHd]
    rfl

/-! ## Main pushdown theorem -/

/-- Filter pushdown for cross products. When the join predicate
references only left-input columns (bounded by `N`, where every
left row's width is at least `N`), and the right input is pure
data (no row-errs, no collection-errs), filtering the cross
product equals crossing the filtered left input with the right.

The right-pure hypothesis is essential: without it,
`combineCarrier`'s left-wins rule disagrees with filter's
cell-to-row promotion on which err payload to keep, and a
`.error` diff in `r` would interact with filter's first arm to
keep records that would be dropped post-pushdown. -/
theorem UnifiedStream.filter_cross_pushdown_left
    (pred : Expr) (N : Nat) (l r : UnifiedStream)
    (hBound : pred.colReferencesBoundedBy N = true)
    (hLWidth : ∀ ud ∈ l, ∀ la, ud.1 = UnifiedRow.row la → la.length ≥ N)
    (hRPure : UnifiedStream.IsPureData r) :
    UnifiedStream.filter pred (UnifiedStream.cross l r)
      = UnifiedStream.cross (UnifiedStream.filter pred l) r := by
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hTlWidth : ∀ ud ∈ tl, ∀ la, ud.1 = UnifiedRow.row la → la.length ≥ N :=
      fun ud hMem la hUc => hLWidth ud (List.mem_cons_of_mem _ hMem) la hUc
    have hHdWidth : ∀ la, uc = UnifiedRow.row la → la.length ≥ N :=
      fun la hUc => hLWidth (uc, d) List.mem_cons_self la hUc
    rw [UnifiedStream.cross_cons_left,
        UnifiedStream.filter_append, ih hTlWidth]
    have hFilterCons :
        UnifiedStream.filter pred ((uc, d) :: tl)
          = UnifiedStream.filter pred [(uc, d)]
              ++ UnifiedStream.filter pred tl := by
      have : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
      rw [this, UnifiedStream.filter_append]
    rw [hFilterCons, UnifiedStream.cross_append_left]
    congr 1
    cases d with
    | error => exact filter_map_error_diff pred uc r
    | val n =>
      cases uc with
      | err e => exact filter_map_err_carrier pred e n r hRPure
      | row la =>
        have hBound' : pred.colReferencesBoundedBy la.length = true :=
          Expr.colReferencesBoundedBy_mono pred hBound (hHdWidth la rfl)
        cases hEval : eval la pred with
        | bool b =>
          cases b with
          | true =>
            exact filter_map_row_val_keep pred la n r hBound' hEval hRPure
          | false =>
            apply filter_map_row_val_drop pred la n r hBound' _ hRPure
            show (match eval la pred with
                    | .bool true => [(UnifiedRow.row la, DiffWithError.val n)]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                    | _          => []) ++ [] = []
            rw [hEval]; rfl
        | int _ =>
          apply filter_map_row_val_drop pred la n r hBound' _ hRPure
          show (match eval la pred with
                  | .bool true => [(UnifiedRow.row la, DiffWithError.val n)]
                  | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                  | _          => []) ++ [] = []
          rw [hEval]; rfl
        | null =>
          apply filter_map_row_val_drop pred la n r hBound' _ hRPure
          show (match eval la pred with
                  | .bool true => [(UnifiedRow.row la, DiffWithError.val n)]
                  | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                  | _          => []) ++ [] = []
          rw [hEval]; rfl
        | err e_pred =>
          exact filter_map_row_val_err pred la n r e_pred hBound' hEval hRPure

end Mz
