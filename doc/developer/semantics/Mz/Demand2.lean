import Mz.UnifiedStream2
import Mz.ColRefs

/-!
# Demand for UnifiedStream2: unused-column invariance

Port of `Mz/Demand.lean` to the new `UnifiedStream2` carrier
(`Row × DiffWithGlobal`). The model is simpler than the old
`UnifiedStream`:

* The carrier is just `Row` — there is no `.err` constructor to
  carve around. `replaceAtRow` always applies `Env.replaceAt` to
  the row component.
* `project` keeps the row on evaluation failure (the failure is
  routed into the diff's `errs` component), so the
  `IsPureData`-style hypothesis that gated the old project proof
  is no longer required.

The headline theorems mirror the old file: `filter` commutes with
`replaceAtRow` under `colReferencesUnused`, and `project`
commutes with `replaceAtRow` under
`Expr.argsColRefUnusedList2` together with the projection-width
side condition `es.length ≤ n` (so the output row has no column
`n` to disturb).
-/

namespace Mz

/-- Replace column `n` of every row in `us` with `v`. With the new
carrier there is no `.err` branch — every record's first
component is a `Row`, so the substitution applies uniformly. The
`DiffWithGlobal` component (whether `.val` or `.global`) is
preserved on every record. -/
def UnifiedStream2.replaceAtRow (n : Nat) (v : Datum) (us : UnifiedStream2) :
    UnifiedStream2 :=
  us.map fun rd => (Env.replaceAt rd.1 n v, rd.2)

theorem UnifiedStream2.replaceAtRow_nil (n : Nat) (v : Datum) :
    UnifiedStream2.replaceAtRow n v [] = [] := rfl

theorem UnifiedStream2.replaceAtRow_append
    (n : Nat) (v : Datum) (a b : UnifiedStream2) :
    UnifiedStream2.replaceAtRow n v (a ++ b)
      = UnifiedStream2.replaceAtRow n v a
          ++ UnifiedStream2.replaceAtRow n v b := by
  unfold UnifiedStream2.replaceAtRow
  exact List.map_append

/-! ## Filter invariance under unused-column replacement -/

/-- Filter commutes with `replaceAtRow n v` when the predicate
does not reference column `n`. Replacing the unused column on the
input then filtering equals filtering then replacing on the
output. Models `demand.rs`: an unused column is free to be
overwritten without affecting the filter result.

Compared with the old `UnifiedStream` proof this version is
strictly simpler — every record has carrier `Row`, so there is no
`.err` carve-out, and the `.global` diff case becomes a clean
pass-through (the carrier is still a row that `replaceAtRow`
touches on both sides). -/
theorem UnifiedStream2.filter_replaceAtRow_of_unused
    (pred : Expr) (n : Nat) (v : Datum) (us : UnifiedStream2)
    (h : pred.colReferencesUnused n = true) :
    UnifiedStream2.filter pred (UnifiedStream2.replaceAtRow n v us)
      = UnifiedStream2.replaceAtRow n v
          (UnifiedStream2.filter pred us) := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨r, d⟩ := hd
    have hConsAsApp : ((r, d) :: tl : UnifiedStream2) = [(r, d)] ++ tl := rfl
    rw [hConsAsApp, UnifiedStream2.replaceAtRow_append,
        UnifiedStream2.filter_append, UnifiedStream2.filter_append,
        UnifiedStream2.replaceAtRow_append, ih]
    congr 1
    cases d with
    | global =>
      -- `.global` diff: filter passes through unchanged; the
      -- carrier on both sides is `Env.replaceAt r n v`.
      rfl
    | val d =>
      -- Both pipelines reduce to a single record produced by
      -- `filter`'s inner `match eval _ pred with ...`. The
      -- `eval_replaceAt_of_unused` lemma bridges the input
      -- substitution to the predicate result.
      have hEval : eval (Env.replaceAt r n v) pred = eval r pred :=
        eval_replaceAt_of_unused r n v pred h
      show [match eval (Env.replaceAt r n v) pred with
              | .bool true =>
                (Env.replaceAt r n v, DiffWithGlobal.val d)
              | .err e =>
                (Env.replaceAt r n v, DiffWithGlobal.val
                  { val := 0
                  , errs := d.errs + ErrCount.single e d.val })
              | _ =>
                (Env.replaceAt r n v, DiffWithGlobal.val
                  { val := 0, errs := d.errs })] ++ []
            = UnifiedStream2.replaceAtRow n v
                ([match eval r pred with
                    | .bool true =>
                      (r, DiffWithGlobal.val d)
                    | .err e =>
                      (r, DiffWithGlobal.val
                        { val := 0
                        , errs := d.errs + ErrCount.single e d.val })
                    | _ =>
                      (r, DiffWithGlobal.val
                        { val := 0, errs := d.errs })] ++ [])
      rw [hEval]
      -- Match on `eval r pred`: each branch produces a single
      -- record whose row component on the LHS is already
      -- `Env.replaceAt r n v` and on the RHS becomes so after
      -- `replaceAtRow n v`.
      cases eval r pred with
      | bool b =>
        cases b with
        | true => rfl
        | false => rfl
      | err _ => rfl
      | int _ => rfl
      | null => rfl

/-! ## Project invariance under unused-column replacement -/

/-- Every expression in `es` has column `n` unused. Same predicate
as in the old `Mz/Demand.lean`. -/
def Expr.argsColRefUnusedList2 (n : Nat) (es : List Expr) : Prop :=
  ∀ e ∈ es, e.colReferencesUnused n = true

private theorem rowAllSafe_replaceAt_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (r : Row)
    (h : Expr.argsColRefUnusedList2 n es) :
    rowAllSafe es (Env.replaceAt r n v) = rowAllSafe es r := by
  induction es with
  | nil => rfl
  | cons hd tl ih =>
    have hHd : hd.colReferencesUnused n = true := h hd List.mem_cons_self
    have hTl : Expr.argsColRefUnusedList2 n tl :=
      fun e hMem => h e (List.mem_cons_of_mem _ hMem)
    have hEval : eval (Env.replaceAt r n v) hd = eval r hd :=
      eval_replaceAt_of_unused r n v hd hHd
    unfold rowAllSafe at ih ⊢
    rw [List.all_cons, List.all_cons, hEval, ih hTl]

private theorem rowErrs_replaceAt_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (r : Row)
    (h : Expr.argsColRefUnusedList2 n es) :
    rowErrs es (Env.replaceAt r n v) = rowErrs es r := by
  induction es with
  | nil => rfl
  | cons hd tl ih =>
    have hHd : hd.colReferencesUnused n = true := h hd List.mem_cons_self
    have hTl : Expr.argsColRefUnusedList2 n tl :=
      fun e hMem => h e (List.mem_cons_of_mem _ hMem)
    have hEval : eval (Env.replaceAt r n v) hd = eval r hd :=
      eval_replaceAt_of_unused r n v hd hHd
    unfold rowErrs at ih ⊢
    rw [List.filterMap_cons, List.filterMap_cons, hEval, ih hTl]

private theorem evalMap_replaceAt_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (r : Row)
    (h : Expr.argsColRefUnusedList2 n es) :
    es.map (eval (Env.replaceAt r n v)) = es.map (eval r) := by
  induction es with
  | nil => rfl
  | cons hd tl ih =>
    have hHd : hd.colReferencesUnused n = true := h hd List.mem_cons_self
    have hTl : Expr.argsColRefUnusedList2 n tl :=
      fun e hMem => h e (List.mem_cons_of_mem _ hMem)
    have hEval : eval (Env.replaceAt r n v) hd = eval r hd :=
      eval_replaceAt_of_unused r n v hd hHd
    show eval (Env.replaceAt r n v) hd :: tl.map _
        = eval r hd :: tl.map _
    rw [hEval, ih hTl]

private theorem rowErrCount_replaceAt_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (r : Row) (k : Int)
    (h : Expr.argsColRefUnusedList2 n es) :
    UnifiedStream2.rowErrCount es (Env.replaceAt r n v) k
      = UnifiedStream2.rowErrCount es r k := by
  unfold UnifiedStream2.rowErrCount
  rw [rowErrs_replaceAt_of_unused es n v r h]

/-- Out-of-bounds replacement is a no-op. -/
private theorem replaceAt_of_length_le :
    ∀ (env : Env) (n : Nat) (v : Datum), env.length ≤ n →
      Env.replaceAt env n v = env
  | [],          _,     _, _ => rfl
  | _ :: _,      0,     _, h => by
    -- `(_ :: _).length = _ + 1`, which cannot be `≤ 0`.
    exact absurd h (Nat.not_succ_le_zero _)
  | hd :: tl,    n + 1, v, h => by
    show hd :: Env.replaceAt tl n v = hd :: tl
    have hTl : tl.length ≤ n := Nat.le_of_succ_le_succ h
    rw [replaceAt_of_length_le tl n v hTl]

/-- Length of `List.map` is the length of the underlying list. -/
private theorem length_map_eval (es : List Expr) (r : Row) :
    (es.map (eval r)).length = es.length := List.length_map _

/-- Project commutes with `replaceAtRow n v` when every expression
in `es` has column `n` unused and the projected row is too short
to contain a column at index `n` (`es.length ≤ n`).

The width side condition handles project's row-rewrite asymmetry:
when `rowAllSafe es r = true` the output row is `es.map (eval r)`,
and `replaceAtRow n v` on the *output* side touches position `n` of
that projected row. The unused-column hypothesis makes the
projected row independent of the input's column `n` but says
nothing about what value happens to live at the *output's* column
`n`. Requiring `es.length ≤ n` puts the would-be substitution out
of bounds, where `Env.replaceAt` is the identity.

The unsafe branch needs no width side condition: project preserves
the row there, so `replaceAtRow` on input and output coincide. -/
theorem UnifiedStream2.project_replaceAtRow_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (us : UnifiedStream2)
    (h : Expr.argsColRefUnusedList2 n es) (hLen : es.length ≤ n) :
    UnifiedStream2.project es (UnifiedStream2.replaceAtRow n v us)
      = UnifiedStream2.replaceAtRow n v
          (UnifiedStream2.project es us) := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨r, d⟩ := hd
    have hConsAsApp : ((r, d) :: tl : UnifiedStream2) = [(r, d)] ++ tl := rfl
    rw [hConsAsApp, UnifiedStream2.replaceAtRow_append,
        UnifiedStream2.project_append, UnifiedStream2.project_append,
        UnifiedStream2.replaceAtRow_append, ih]
    congr 1
    cases d with
    | global =>
      -- `.global`: project passes the record through. Both
      -- pipelines apply `replaceAtRow n v` exactly once.
      rfl
    | val d =>
      -- Both pipelines reduce to a single record produced by
      -- `project`'s inner `if rowAllSafe es _ then ... else ...`.
      show [if rowAllSafe es (Env.replaceAt r n v) then
              (es.map (eval (Env.replaceAt r n v)), DiffWithGlobal.val d)
            else
              (Env.replaceAt r n v, DiffWithGlobal.val
                { val := 0
                , errs := d.errs
                          + UnifiedStream2.rowErrCount es
                              (Env.replaceAt r n v) d.val })] ++ []
            = UnifiedStream2.replaceAtRow n v
                ([if rowAllSafe es r then
                    (es.map (eval r), DiffWithGlobal.val d)
                  else
                    (r, DiffWithGlobal.val
                      { val := 0
                      , errs := d.errs
                                + UnifiedStream2.rowErrCount es r d.val })]
                  ++ [])
      rw [rowAllSafe_replaceAt_of_unused es n v r h]
      by_cases hSafe : rowAllSafe es r = true
      · -- Safe branch: output row is `es.map (eval _)`, diff kept.
        rw [if_pos hSafe, if_pos hSafe]
        show [(es.map (eval (Env.replaceAt r n v)), DiffWithGlobal.val d)]
              ++ []
            = UnifiedStream2.replaceAtRow n v
                ([(es.map (eval r), DiffWithGlobal.val d)] ++ [])
        rw [List.append_nil, List.append_nil]
        show [(es.map (eval (Env.replaceAt r n v)), DiffWithGlobal.val d)]
            = [(Env.replaceAt (es.map (eval r)) n v, DiffWithGlobal.val d)]
        rw [evalMap_replaceAt_of_unused es n v r h]
        -- `Env.replaceAt` on the projected row is a no-op because
        -- the row is `es.length`-long and `n ≥ es.length`.
        have hOob : (es.map (eval r)).length ≤ n := by
          rw [length_map_eval]; exact hLen
        rw [replaceAt_of_length_le (es.map (eval r)) n v hOob]
      · -- Unsafe branch: row is preserved, diff valid count zeroed,
        -- err-count entries added per erroring scalar.
        rw [if_neg hSafe, if_neg hSafe]
        show [(Env.replaceAt r n v,
              DiffWithGlobal.val
                { val := 0
                , errs := d.errs
                          + UnifiedStream2.rowErrCount es
                              (Env.replaceAt r n v) d.val })] ++ []
            = UnifiedStream2.replaceAtRow n v
                ([(r, DiffWithGlobal.val
                    { val := 0
                    , errs := d.errs
                              + UnifiedStream2.rowErrCount es r d.val })]
                  ++ [])
        rw [rowErrCount_replaceAt_of_unused es n v r d.val h]
        rfl

end Mz
