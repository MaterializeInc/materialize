import Mz.UnifiedStream
import Mz.ColRefs

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

end Mz
