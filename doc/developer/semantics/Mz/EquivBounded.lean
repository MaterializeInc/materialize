import Mz.PrimEval
import Mz.Equiv

/-!
# Bounded-arithmetic counterexample to strict associativity of `+`

This module mechanizes the bounded-int associativity counterexample
the design doc (§"Evaluation-order equivalence") names as the
motivating example for relaxing strict equality on `Datum`. Where
`Mz/Equiv.lean` discusses the counterexample at the level of prose,
this module discharges it as a live theorem:

* `evalPlusBounded_assoc_max_strict_ne` — at `x = max`, the two
  evaluation orders of `x + 1 + (-1)` are *not* strictly equal as
  `Datum`. The left-leaning order errs on overflow at `max + 1`; the
  right-leaning order folds `1 + (-1) = 0` first and returns
  `.int max`.

* `evalPlusBounded_assoc_max_refines` — the same statement under
  refinement (errors as bottom) closes in the LHS ⊑ RHS direction:
  the err-leaning order refines the value-yielding one. This is the
  shape of the soundness obligation under the "no spurious errors"
  posture.

* `evalPlusBounded_assoc_max_not_refines_rev` — the reverse direction
  fails, witnessed concretely. `.int max` does not refine `.err _`;
  this is the conjunctive bound that distinguishes a preorder from an
  equivalence relation.

* `evalPlusBounded_assoc_max_refinesDual` — the dual posture
  ("spurious errors permitted") closes in the RHS ⊑ LHS direction:
  a value-yielding order may always be replaced by an err-yielding
  one. The asymmetry between `refines` and `refinesDual` is exactly
  what determines which optimizer rewrites are sound under which
  posture.

The bound is symmetric (`[-max, max]`) for proof economy; widening
to `[MIN_INT32, MAX_INT32]` is mechanical and does not change the
counterexample.
-/

namespace Mz

/-! ## Helper reduction lemmas for `evalPlusBounded`

The `.int / .int` case carries the only non-trivial reduction; the
`.err` and `.null` cases reduce by `rfl` and are inlined where
needed. -/

/-- `.int + .int` case of `evalPlusBounded` exposed for rewriting:
the result is `.int (n + m)` when in range and `.err .overflow`
otherwise. -/
theorem evalPlusBounded_int_int (max n m : Int) :
    evalPlusBounded max (.int n) (.int m)
      = (if n + m > max ∨ n + m < -max then .err .overflow else .int (n + m)) :=
  rfl

/-! ## Live counterexample at `x = max`

Concrete values: LHS folds `max + 1` first and errs on overflow,
then propagates the err through `+ (-1)`; RHS folds `1 + (-1) = 0`
first, then `max + 0 = max`. The two `Datum` values land in
distinct constructors. -/

/-- LHS inner step: `max + 1` overflows. -/
private theorem evalPlusBounded_max_one (max : Int) :
    evalPlusBounded max (.int max) (.int 1) = .err .overflow := by
  rw [evalPlusBounded_int_int]
  exact if_pos (Or.inl (by omega : max + 1 > max))

/-- LHS outer step: propagating the inner overflow err. -/
private theorem evalPlusBounded_lhs (max : Int) :
    evalPlusBounded max
        (evalPlusBounded max (.int max) (.int 1)) (.int (-1))
      = .err .overflow := by
  rw [evalPlusBounded_max_one]; rfl

/-- RHS inner step: `1 + (-1) = 0` lies in range whenever `0 ≤ max`. -/
private theorem evalPlusBounded_one_neg_one (max : Int) (hmax : 0 ≤ max) :
    evalPlusBounded max (.int 1) (.int (-1)) = .int 0 := by
  rw [evalPlusBounded_int_int]
  have h : ¬((1 : Int) + (-1) > max ∨ (1 : Int) + (-1) < -max) := by
    intro h
    cases h with
    | inl h => omega
    | inr h => omega
  rw [if_neg h]
  show Datum.int ((1 : Int) + (-1)) = Datum.int 0
  decide

/-- RHS outer step: `max + 0 = max` lies in range whenever `0 ≤ max`. -/
private theorem evalPlusBounded_rhs (max : Int) (hmax : 0 ≤ max) :
    evalPlusBounded max (.int max)
        (evalPlusBounded max (.int 1) (.int (-1)))
      = .int max := by
  rw [evalPlusBounded_one_neg_one max hmax, evalPlusBounded_int_int]
  have h : ¬(max + 0 > max ∨ max + 0 < -max) := by
    intro h
    cases h with
    | inl h => omega
    | inr h => omega
  rw [if_neg h]
  show Datum.int (max + 0) = Datum.int max
  congr 1
  omega

/-! ## Strict-equality failure

The headline counterexample. Strict equality does not hold between
the two evaluation orders of `x + 1 + (-1)` at `x = max`. -/

/-- Strict equality fails: the left-leaning order errs on overflow,
the right-leaning order returns `.int max`. The disjoint
constructors `.err _` and `.int _` discharge the inequality. -/
theorem evalPlusBounded_assoc_max_strict_ne (max : Int) (hmax : 0 ≤ max) :
    evalPlusBounded max
        (evalPlusBounded max (.int max) (.int 1)) (.int (-1))
      ≠ evalPlusBounded max (.int max)
        (evalPlusBounded max (.int 1) (.int (-1))) := by
  rw [evalPlusBounded_lhs, evalPlusBounded_rhs max hmax]
  intro h
  cases h

/-! ## Refinement (errors as bottom)

Under the "no spurious errors" posture, the err-leaning evaluation
order refines the value-leaning one. `evalPlusBounded_assoc_max_refines`
witnesses LHS ⊑ RHS. The reverse direction
`evalPlusBounded_assoc_max_not_refines_rev` fails: `.int max` does
not refine `.err .overflow` because `.int` is not `IsErr`. -/

/-- LHS refines RHS: the err refines the value, by `err_refines`. -/
theorem evalPlusBounded_assoc_max_refines (max : Int) (hmax : 0 ≤ max) :
    Datum.refines
      (evalPlusBounded max
        (evalPlusBounded max (.int max) (.int 1)) (.int (-1)))
      (evalPlusBounded max (.int max)
        (evalPlusBounded max (.int 1) (.int (-1)))) := by
  rw [evalPlusBounded_lhs, evalPlusBounded_rhs max hmax]
  exact Datum.err_refines .overflow (.int max)

/-- The reverse direction fails: `.int max` does not refine
`.err .overflow`. Witness for the asymmetry of `refines`. -/
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

/-! ## Dual refinement (errors as top)

Under the "spurious errors permitted" (PostgreSQL) posture, a
value-yielding evaluation order may be replaced by an err-yielding
one. RHS ⊑ LHS in the dual order. -/

/-- RHS dual-refines LHS: by `refinesDual_err`. -/
theorem evalPlusBounded_assoc_max_refinesDual (max : Int) (hmax : 0 ≤ max) :
    Datum.refinesDual
      (evalPlusBounded max (.int max)
        (evalPlusBounded max (.int 1) (.int (-1))))
      (evalPlusBounded max
        (evalPlusBounded max (.int max) (.int 1)) (.int (-1))) := by
  rw [evalPlusBounded_lhs, evalPlusBounded_rhs max hmax]
  exact Datum.refinesDual_err (.int max) .overflow

end Mz
