import Mz.Indexed.PrimEval
import Mz.Indexed.Equiv

/-!
# Bounded-arithmetic counterexample (indexed)

The boundedness counterexample for `evalPlus` associativity on
`Datum .int`. Indexed counterpart of `Mz/EquivBounded.lean`.

The two evaluation orders of `x + 1 + (-1)` at `x = max` disagree:
left-leaning errs on overflow at `max + 1`; right-leaning folds
`1 + (-1) = 0` and returns `.int max`. Under `=` they're distinct;
under `refines` LHS ⊑ RHS (err refines value); the reverse fails.

Same shape as the untyped version, restricted to `Datum .int`. -/

namespace Mz.Indexed

open Mz

theorem evalPlusBounded_int_int (max n m : Int) :
    evalPlusBounded max (.int n) (.int m)
      = (if n + m > max ∨ n + m < -max then (.err .overflow : Datum .int)
         else .int (n + m)) :=
  rfl

private theorem evalPlusBounded_max_one (max : Int) :
    evalPlusBounded max (.int max) (.int 1) = (.err .overflow : Datum .int) := by
  rw [evalPlusBounded_int_int]
  exact if_pos (Or.inl (by omega : max + 1 > max))

private theorem evalPlusBounded_lhs (max : Int) :
    evalPlusBounded max
        (evalPlusBounded max (.int max) (.int 1)) (.int (-1))
      = (.err .overflow : Datum .int) := by
  rw [evalPlusBounded_max_one]; rfl

private theorem evalPlusBounded_one_neg_one (max : Int) (hmax : 0 ≤ max) :
    evalPlusBounded max (.int 1) (.int (-1)) = (.int 0 : Datum .int) := by
  rw [evalPlusBounded_int_int]
  have h : ¬((1 : Int) + (-1) > max ∨ (1 : Int) + (-1) < -max) := by
    intro h
    cases h with
    | inl h => omega
    | inr h => omega
  rw [if_neg h]
  show (Datum.int ((1 : Int) + (-1)) : Datum .int) = Datum.int 0
  norm_num

private theorem evalPlusBounded_rhs (max : Int) (hmax : 0 ≤ max) :
    evalPlusBounded max (.int max)
        (evalPlusBounded max (.int 1) (.int (-1)))
      = (.int max : Datum .int) := by
  rw [evalPlusBounded_one_neg_one max hmax, evalPlusBounded_int_int]
  have h : ¬(max + 0 > max ∨ max + 0 < -max) := by
    intro h
    cases h with
    | inl h => omega
    | inr h => omega
  rw [if_neg h]
  show (Datum.int (max + 0) : Datum .int) = Datum.int max
  congr 1
  omega

/-- Strict equality fails: LHS errs, RHS returns `.int max`. -/
theorem evalPlusBounded_assoc_max_strict_ne (max : Int) (hmax : 0 ≤ max) :
    evalPlusBounded max
        (evalPlusBounded max (.int max) (.int 1)) (.int (-1))
      ≠ evalPlusBounded max (.int max)
        (evalPlusBounded max (.int 1) (.int (-1))) := by
  rw [evalPlusBounded_lhs, evalPlusBounded_rhs max hmax]
  intro h
  cases h

/-- LHS refines RHS: the err refines the value. -/
theorem evalPlusBounded_assoc_max_refines (max : Int) (hmax : 0 ≤ max) :
    Datum.refines
      (evalPlusBounded max
        (evalPlusBounded max (.int max) (.int 1)) (.int (-1)))
      (evalPlusBounded max (.int max)
        (evalPlusBounded max (.int 1) (.int (-1)))) := by
  rw [evalPlusBounded_lhs, evalPlusBounded_rhs max hmax]
  exact Datum.err_refines .overflow (.int max)

/-- Reverse direction fails: `.int max` does not refine `.err _`. -/
theorem evalPlusBounded_assoc_max_not_refines_rev (max : Int) (hmax : 0 ≤ max) :
    ¬ Datum.refines
        (evalPlusBounded max (.int max)
          (evalPlusBounded max (.int 1) (.int (-1))))
        (evalPlusBounded max
          (evalPlusBounded max (.int max) (.int 1)) (.int (-1))) := by
  rw [evalPlusBounded_lhs, evalPlusBounded_rhs max hmax]
  intro h
  cases h with
  | inl h => cases h
  | inr h => cases h

end Mz.Indexed
