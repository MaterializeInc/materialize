import Mz.Indexed.PrimEval
import Mz.Indexed.Laws

/-!
# Variadic `AND` and `OR` (indexed)

Laws over `evalAndN` / `evalOrN` on `List (Datum .bool)`. Indexed
counterpart of `Mz/Variadic.lean`.

All `¬d.IsInt` hypotheses present in the untyped model disappear:
the indexed `Datum .bool` rules out `.int` at the type level.
-/

namespace Mz.Indexed

open Mz

/-! ## Cons recurrence -/

theorem evalAndN_cons (d : Datum .bool) (ds : List (Datum .bool)) :
    evalAndN (d :: ds) = evalAnd d (evalAndN ds) := rfl

theorem evalOrN_cons (d : Datum .bool) (ds : List (Datum .bool)) :
    evalOrN (d :: ds) = evalOr d (evalOrN ds) := rfl

/-! ## Identity cases -/

theorem evalAndN_nil : evalAndN [] = .bool true := rfl
theorem evalOrN_nil  : evalOrN  [] = .bool false := rfl

theorem evalAndN_singleton (d : Datum .bool) : evalAndN [d] = d := by
  show evalAnd d (.bool true) = d
  exact evalAnd_true_right d

theorem evalOrN_singleton (d : Datum .bool) : evalOrN [d] = d := by
  show evalOr d (.bool false) = d
  exact evalOr_false_right d

/-! ## Binary equivalence -/

theorem evalAndN_binary (a b : Datum .bool) :
    evalAndN [a, b] = evalAnd a b := by
  show evalAnd a (evalAnd b (.bool true)) = evalAnd a b
  rw [evalAnd_true_right b]

theorem evalOrN_binary (a b : Datum .bool) :
    evalOrN [a, b] = evalOr a b := by
  show evalOr a (evalOr b (.bool false)) = evalOr a b
  rw [evalOr_false_right b]

/-! ## Absorption -/

private theorem evalAnd_false_right_any (d : Datum .bool) :
    evalAnd d (.bool false) = .bool false := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

private theorem evalOr_true_right_any (d : Datum .bool) :
    evalOr d (.bool true) = .bool true := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem evalAndN_false_absorbs :
    ∀ {ds : List (Datum .bool)}, (.bool false : Datum .bool) ∈ ds → evalAndN ds = .bool false := by
  intro ds hmem
  induction ds with
  | nil => cases hmem
  | cons hd tl ih =>
    cases hmem with
    | head _ =>
      show evalAnd (.bool false) (evalAndN tl) = .bool false
      rfl
    | tail _ hmem' =>
      have htl : evalAndN tl = .bool false := ih hmem'
      show evalAnd hd (evalAndN tl) = .bool false
      rw [htl]
      exact evalAnd_false_right_any hd

theorem evalOrN_true_absorbs :
    ∀ {ds : List (Datum .bool)}, (.bool true : Datum .bool) ∈ ds → evalOrN ds = .bool true := by
  intro ds hmem
  induction ds with
  | nil => cases hmem
  | cons hd tl ih =>
    cases hmem with
    | head _ =>
      show evalOr (.bool true) (evalOrN tl) = .bool true
      rfl
    | tail _ hmem' =>
      have htl : evalOrN tl = .bool true := ih hmem'
      show evalOr hd (evalOrN tl) = .bool true
      rw [htl]
      exact evalOr_true_right_any hd

end Mz.Indexed
