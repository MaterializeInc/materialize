import Mz.Schema

/-!
# Output schema propagation for `Expr`

`Expr.outputType sch e` derives the output `ColSchema` for an
expression `e` evaluated on a row satisfying input schema `sch`,
with soundness theorem `eval_satisfies_outputType`.

This module lands the *foundation*: precise rules for `.lit` and
`.col`, conservative `{ nullable := true, errable := true }` for
every other constructor. The conservative rules are sound by
`DatumSatisfies.weakest`; refining them per constructor (`.not`
preserves; arithmetic `errable` propagates; `.coalesce` recovers
on a non-null non-err first argument; etc.) is mechanical and
left as a follow-up.

The conservative form is already useful as the structural carrier:
optimizer rewrites that need a precise output column schema can
cite `eval_satisfies_outputType` for the lit / col fragments, and
refinements ride on top of the same theorem statement. -/

namespace Mz

/-! ## Satisfaction predicate at the `Datum` level -/

/-- A `Datum` satisfies a `ColSchema`. The implication direction is
the active part: `cs.nullable = false` constrains, `cs.nullable = true`
is vacuous; same for `errable`. -/
def DatumSatisfies (cs : ColSchema) (d : Datum) : Prop :=
  (cs.nullable = false → d ≠ .null) ∧ (cs.errable = false → ¬d.IsErr)

/-- The weakest schema is satisfied by every datum. Discharges every
constructor that `Expr.outputType` does not yet have a precise rule
for. -/
theorem DatumSatisfies.weakest (d : Datum) :
    DatumSatisfies { nullable := true, errable := true } d := by
  refine ⟨?_, ?_⟩
  · intro h; cases h
  · intro h; cases h

/-! ## Output type computation -/

/-- Compute the output `ColSchema` for an expression. Precise on
`.lit` and `.col`; conservative on every other constructor. -/
def Expr.outputType {n : Nat} (sch : Schema n) : Expr → ColSchema
  | .lit (.bool _) => { nullable := false, errable := false }
  | .lit (.int _)  => { nullable := false, errable := false }
  | .lit .null     => { nullable := true,  errable := false }
  | .lit (.err _)  => { nullable := false, errable := true }
  | .col i =>
    if h : i < n then sch.cols.get ⟨i, h⟩
    else { nullable := true, errable := false }
  | _ => { nullable := true, errable := true }

/-! ## `Env.get` on a row that satisfies a schema -/

private theorem Env_get_eq_list_get (l : List Datum) (i : Nat) (h : i < l.length) :
    Env.get l i = l.get ⟨i, h⟩ := by
  induction l generalizing i with
  | nil => cases h
  | cons hd tl ih =>
    cases i with
    | zero => rfl
    | succ k =>
      show Env.get tl k = tl.get ⟨k, by simp [List.length_cons] at h; omega⟩
      apply ih

private theorem Env_get_eq_null_of_ge (l : List Datum) (i : Nat) (h : l.length ≤ i) :
    Env.get l i = .null := by
  induction l generalizing i with
  | nil => cases i <;> rfl
  | cons hd tl ih =>
    cases i with
    | zero => simp [List.length_cons] at h
    | succ k =>
      show Env.get tl k = .null
      apply ih
      simp [List.length_cons] at h
      omega

private theorem Env_get_row_toList_lt
    {n : Nat} (row : RowN n) (i : Nat) (h : i < n) :
    Env.get row.toList i = row.get ⟨i, h⟩ := by
  have hLen : row.toList.length = n := row.toList_length
  have hLt : i < row.toList.length := by rw [hLen]; exact h
  rw [Env_get_eq_list_get row.toList i hLt]
  show row.toList.get ⟨i, hLt⟩ = row.get ⟨i, h⟩
  rcases row with ⟨l, hl⟩; rfl

private theorem Env_get_row_toList_ge
    {n : Nat} (row : RowN n) (i : Nat) (h : ¬i < n) :
    Env.get row.toList i = .null := by
  have hLen : row.toList.length = n := row.toList_length
  apply Env_get_eq_null_of_ge
  rw [hLen]; omega

/-! ## Soundness -/

/-- Soundness: evaluating `e` on a row satisfying `sch` produces a
`Datum` satisfying `Expr.outputType sch e`. Precise on `.lit` and
`.col`; falls through to `DatumSatisfies.weakest` for every other
constructor. -/
theorem eval_satisfies_outputType {n : Nat}
    (sch : Schema n) (row : RowN n) (hsat : RowSatisfies sch row)
    (e : Expr) :
    DatumSatisfies (Expr.outputType sch e) (eval row.toList e) := by
  match e with
  | .lit d =>
    simp only [eval, Expr.outputType]
    cases d with
    | bool b =>
      refine ⟨?_, ?_⟩
      · intro _ h; cases h
      · intro _ h; cases h
    | int k =>
      refine ⟨?_, ?_⟩
      · intro _ h; cases h
      · intro _ h; cases h
    | null =>
      refine ⟨?_, ?_⟩
      · intro h; cases h
      · intro _ h; cases h
    | err e =>
      refine ⟨?_, ?_⟩
      · intro _ h; cases h
      · intro h; cases h
  | .col i =>
    simp only [eval, Expr.outputType]
    by_cases h : i < n
    · rw [Env_get_row_toList_lt row i h]
      rw [dif_pos h]
      exact hsat ⟨i, h⟩
    · rw [Env_get_row_toList_ge row i h]
      rw [dif_neg h]
      refine ⟨?_, ?_⟩
      · intro hN; cases hN
      · intro _ h; cases h
  | .not _ => exact DatumSatisfies.weakest _
  | .ifThen _ _ _ => exact DatumSatisfies.weakest _
  | .andN _ => exact DatumSatisfies.weakest _
  | .orN _ => exact DatumSatisfies.weakest _
  | .coalesce _ => exact DatumSatisfies.weakest _
  | .plus _ _ => exact DatumSatisfies.weakest _
  | .minus _ _ => exact DatumSatisfies.weakest _
  | .times _ _ => exact DatumSatisfies.weakest _
  | .divide _ _ => exact DatumSatisfies.weakest _
  | .eq _ _ => exact DatumSatisfies.weakest _
  | .lt _ _ => exact DatumSatisfies.weakest _

end Mz
