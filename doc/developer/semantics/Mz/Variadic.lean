import Mz.PrimEval
import Mz.Laws

/-!
# Variadic `AND` and `OR`

`MirScalarExpr::VariadicFunc::And` and `Or` take an arbitrary number
of operands. This file proves laws about the corresponding
`List Datum → Datum` evaluators (`evalAndN`, `evalOrN`) defined in
`Mz/PrimEval.lean`, and shows that the binary `evalAnd` / `evalOr`
are the two-operand specializations of the variadic forms.

The variadic forms are defined by right-fold so the cons recurrence
holds by `rfl`. With a left-fold the recurrence would require a
separate associativity argument; the right-fold gives the same final
value because `TRUE`/`FALSE` are two-sided identities for
`evalAnd`/`evalOr` (see `Mz/Laws.lean`).

A key spec property of `MirScalarExpr` `And`/`Or` is that `FALSE`
(resp. `TRUE`) absorbs every other operand including `err`. Lifted
to the variadic form: if `FALSE` appears anywhere in the list,
`evalAndN` is `FALSE`. The corresponding `evalOrN` theorem holds for
`TRUE`. These absorption theorems justify the runtime's short-circuit
optimization and are what an optimizer cites when it folds
`x AND false` to `false` regardless of `x`'s side effects (which in
this skeleton are limited to producing `err`s).
-/

namespace Mz

/-! ## Cons recurrence -/

theorem evalAndN_cons (d : Datum) (ds : List Datum) :
    evalAndN (d :: ds) = evalAnd d (evalAndN ds) := rfl

theorem evalOrN_cons (d : Datum) (ds : List Datum) :
    evalOrN (d :: ds) = evalOr d (evalOrN ds) := rfl

/-! ## Identity cases -/

theorem evalAndN_nil : evalAndN [] = .bool true := rfl
theorem evalOrN_nil  : evalOrN  [] = .bool false := rfl

theorem evalAndN_singleton (d : Datum) : evalAndN [d] = d := by
  show evalAnd d (.bool true) = d
  exact evalAnd_true_right d

theorem evalOrN_singleton (d : Datum) : evalOrN [d] = d := by
  show evalOr d (.bool false) = d
  exact evalOr_false_right d

/-! ## Binary equivalence

The two-operand variadic forms agree with the binary evaluators.
This is the bridge that lets every binary theorem in
`Mz/Boolean.lean` and `Mz/Laws.lean` carry over to the variadic
operators on lists of length two. -/

theorem evalAndN_binary (a b : Datum) :
    evalAndN [a, b] = evalAnd a b := by
  show evalAnd a (evalAnd b (.bool true)) = evalAnd a b
  rw [evalAnd_true_right]

theorem evalOrN_binary (a b : Datum) :
    evalOrN [a, b] = evalOr a b := by
  show evalOr a (evalOr b (.bool false)) = evalOr a b
  rw [evalOr_false_right]

/-! ## Absorption

A single `FALSE` anywhere in the operand list collapses `evalAndN` to
`FALSE`. Symmetric statement for `TRUE` and `evalOrN`. The induction
is on the operand list, using the cons recurrence and the fact that
`evalAnd` returns `FALSE` whenever its right argument is `FALSE`. -/

private theorem evalAnd_false_right_any (d : Datum) :
    evalAnd d (.bool false) = .bool false := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

private theorem evalOr_true_right_any (d : Datum) :
    evalOr d (.bool true) = .bool true := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

theorem evalAndN_false_absorbs :
    ∀ {ds : List Datum}, Datum.bool false ∈ ds → evalAndN ds = .bool false := by
  intro ds hmem
  induction ds with
  | nil => cases hmem
  | cons hd tl ih =>
    cases hmem with
    | head _ =>
      -- `hd` was unified with `.bool false`. Cons recurrence reduces.
      show evalAnd (.bool false) (evalAndN tl) = .bool false
      rfl
    | tail _ hmem' =>
      have htl : evalAndN tl = .bool false := ih hmem'
      show evalAnd hd (evalAndN tl) = .bool false
      rw [htl]
      exact evalAnd_false_right_any hd

theorem evalOrN_true_absorbs :
    ∀ {ds : List Datum}, Datum.bool true ∈ ds → evalOrN ds = .bool true := by
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

end Mz
