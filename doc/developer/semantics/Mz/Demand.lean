import Mz.UnifiedStream
import Mz.ColRefs
import Mz.JoinPushdown

/-!
# Demand: unused-column invariance

The Rust optimizer's `demand.rs` pass tracks which columns are
required downstream and rewrites the plan to avoid materializing
unused columns. The denotational counterpart: if a column is
unused by an operator, replacing the column with any value in
every row of the input leaves the operator output unchanged
(modulo the same replacement on the output).

`Mz/ColRefs.lean` supplies the row-level invariance
(`eval_replaceAt_of_unused`). This file lifts it to
`UnifiedStream` for the two operators that consume a row at the
predicate / expression level: `filter` and `project`.

`replaceAtRow n v` is the column-`n` substitution applied to every
`.row` carrier in the stream. `.err` carriers and `.error` diffs
flow through unchanged. -/

namespace Mz

/-- Replace column `n` of every `.row` carrier in `us` with `v`.
`.err` carriers pass through unchanged; the diff is preserved on
every record. -/
def UnifiedStream.replaceAtRow (n : Nat) (v : Datum) (us : UnifiedStream) : UnifiedStream :=
  us.map fun ud => match ud.1 with
    | UnifiedRow.row r => (UnifiedRow.row (Env.replaceAt r n v), ud.2)
    | UnifiedRow.err _ => ud

theorem UnifiedStream.replaceAtRow_nil (n : Nat) (v : Datum) :
    UnifiedStream.replaceAtRow n v [] = [] := rfl

theorem UnifiedStream.replaceAtRow_append
    (n : Nat) (v : Datum) (a b : UnifiedStream) :
    UnifiedStream.replaceAtRow n v (a ++ b)
      = UnifiedStream.replaceAtRow n v a
          ++ UnifiedStream.replaceAtRow n v b := by
  unfold UnifiedStream.replaceAtRow
  exact List.map_append

/-! ## Filter invariance under unused-column replacement -/

/-- Filter commutes with `replaceAtRow n v` when the predicate
does not reference column `n`. Replacing the unused column on the
input then filtering equals filtering then replacing on the
output. Models `demand.rs`: an unused column is free to be
overwritten without affecting the filter result. -/
theorem UnifiedStream.filter_replaceAtRow_of_unused
    (pred : Expr) (n : Nat) (v : Datum) (us : UnifiedStream)
    (h : pred.colReferencesUnused n = true) :
    UnifiedStream.filter pred (UnifiedStream.replaceAtRow n v us)
      = UnifiedStream.replaceAtRow n v (UnifiedStream.filter pred us) := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hConsAsApp : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
    rw [hConsAsApp, UnifiedStream.replaceAtRow_append,
        UnifiedStream.filter_append, UnifiedStream.filter_append,
        UnifiedStream.replaceAtRow_append, ih]
    congr 1
    cases d with
    | error =>
      -- `.error` diff: filter keeps; replaceAtRow ignores diff;
      -- carrier handled identically on both pipelines.
      cases uc with
      | row r => rfl
      | err _ => rfl
    | val n' =>
      cases uc with
      | err _ => rfl
      | row r =>
        -- Both pipelines reduce to `(match eval … pred with …) ++ []`.
        -- `eval_replaceAt_of_unused` bridges the inner match.
        have hEval : eval (Env.replaceAt r n v) pred = eval r pred :=
          eval_replaceAt_of_unused r n v pred h
        show (match eval (Env.replaceAt r n v) pred with
                | .bool true => [(UnifiedRow.row (Env.replaceAt r n v),
                                   DiffWithError.val n')]
                | .err e     => [(UnifiedRow.err e, DiffWithError.val n')]
                | _          => []) ++ []
              = UnifiedStream.replaceAtRow n v
                  ((match eval r pred with
                      | .bool true => [(UnifiedRow.row r, DiffWithError.val n')]
                      | .err e     => [(UnifiedRow.err e, DiffWithError.val n')]
                      | _          => []) ++ [])
        rw [hEval]
        cases eval r pred with
        | bool b =>
          cases b with
          | true => rfl
          | false => rfl
        | err _ => rfl
        | int _ => rfl
        | null => rfl

/-! ## Project invariance under unused-column replacement -/

/-- Every expression in `es` has column `n` unused. -/
def Expr.argsColRefUnusedList (n : Nat) (es : List Expr) : Prop :=
  ∀ e ∈ es, e.colReferencesUnused n = true

private theorem rowAllSafe_replaceAt_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (r : Row)
    (h : Expr.argsColRefUnusedList n es) :
    rowAllSafe es (Env.replaceAt r n v) = rowAllSafe es r := by
  induction es with
  | nil => rfl
  | cons hd tl ih =>
    have hHd : hd.colReferencesUnused n = true := h hd List.mem_cons_self
    have hTl : Expr.argsColRefUnusedList n tl :=
      fun e hMem => h e (List.mem_cons_of_mem _ hMem)
    have hEval : eval (Env.replaceAt r n v) hd = eval r hd :=
      eval_replaceAt_of_unused r n v hd hHd
    unfold rowAllSafe at ih ⊢
    rw [List.all_cons, List.all_cons, hEval, ih hTl]

private theorem rowErrs_replaceAt_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (r : Row)
    (h : Expr.argsColRefUnusedList n es) :
    rowErrs es (Env.replaceAt r n v) = rowErrs es r := by
  induction es with
  | nil => rfl
  | cons hd tl ih =>
    have hHd : hd.colReferencesUnused n = true := h hd List.mem_cons_self
    have hTl : Expr.argsColRefUnusedList n tl :=
      fun e hMem => h e (List.mem_cons_of_mem _ hMem)
    have hEval : eval (Env.replaceAt r n v) hd = eval r hd :=
      eval_replaceAt_of_unused r n v hd hHd
    unfold rowErrs at ih ⊢
    rw [List.filterMap_cons, List.filterMap_cons, hEval, ih hTl]

private theorem evalMap_replaceAt_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (r : Row)
    (h : Expr.argsColRefUnusedList n es) :
    es.map (eval (Env.replaceAt r n v)) = es.map (eval r) := by
  induction es with
  | nil => rfl
  | cons hd tl ih =>
    have hHd : hd.colReferencesUnused n = true := h hd List.mem_cons_self
    have hTl : Expr.argsColRefUnusedList n tl :=
      fun e hMem => h e (List.mem_cons_of_mem _ hMem)
    have hEval : eval (Env.replaceAt r n v) hd = eval r hd :=
      eval_replaceAt_of_unused r n v hd hHd
    show eval (Env.replaceAt r n v) hd :: tl.map _
        = eval r hd :: tl.map _
    rw [hEval, ih hTl]

/-- Project is invariant under input replacement of an unused
column, when the input is pure data (no `.err` carriers, no
`.error` diffs). The hypothesis rules out the err / error
passthrough that *would* preserve the replaced carrier on the
output and break the simpler invariance form.

This is the project-side counterpart of
`filter_replaceAtRow_of_unused`. The reason the filter version
admits replacement on both sides (no purity needed) and this one
doesn't: filter preserves the row content of surviving records,
so column `n` of the output equals column `n` of the input;
project rewrites the row content via `es.map (eval r)`, so column
`n` of the output is unrelated to column `n` of the input. -/
theorem UnifiedStream.project_replaceAtRow_eq_of_unused
    (es : List Expr) (n : Nat) (v : Datum) (us : UnifiedStream)
    (hPure : UnifiedStream.IsPureData us)
    (h : Expr.argsColRefUnusedList n es) :
    UnifiedStream.project es (UnifiedStream.replaceAtRow n v us)
      = UnifiedStream.project es us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hTlPure : UnifiedStream.IsPureData tl := hPure.tail
    obtain ⟨⟨r, hUc⟩, ⟨n', hD⟩⟩ := hPure.head
    subst hUc; subst hD
    have hConsAsApp : ((UnifiedRow.row r, DiffWithError.val n') :: tl : UnifiedStream)
                    = [(UnifiedRow.row r, DiffWithError.val n')] ++ tl := rfl
    rw [hConsAsApp, UnifiedStream.replaceAtRow_append,
        UnifiedStream.project_append, UnifiedStream.project_append,
        ih hTlPure]
    congr 1
    -- LHS: project es [(.row (Env.replaceAt r n v), .val n')].
    -- RHS: project es [(.row r, .val n')]. Bottoms out at
    -- `rowProjectRecords es (.val n') (...)`. The two agree under
    -- the unused-column hypothesis.
    show rowProjectRecords es (DiffWithError.val n') (Env.replaceAt r n v) ++ []
          = rowProjectRecords es (DiffWithError.val n') r ++ []
    rw [List.append_nil, List.append_nil]
    unfold rowProjectRecords
    rw [rowAllSafe_replaceAt_of_unused es n v r h,
        rowErrs_replaceAt_of_unused es n v r h,
        evalMap_replaceAt_of_unused es n v r h]

end Mz
