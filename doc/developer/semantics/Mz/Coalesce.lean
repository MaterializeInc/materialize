import Mz.PrimEval

/-!
# `coalesce` and the error-rescue law

`coalesce(d₁, …, dₙ)` returns the first operand that is neither
`null` nor `err`, evaluating left to right. The proposed extension
over PostgreSQL is that `err` is rescuable in the same way `null`
is: a later non-error operand can substitute for an earlier one,
whether that earlier one was `null`, `err`, or any combination.

The evaluator (`evalCoalesce`) and its state-machine helper live in
`Mz/PrimEval.lean`. This file collects the laws.

When no concrete value is found, the result follows a `null`-beats-
`err` rule:

* If any operand was `null`, return `null`.
* Otherwise, if any operand was `err`, return the first such `err`.
* Otherwise (the empty list), return `null`.

The "`null` beats `err`" tiebreaker preserves PostgreSQL's familiar
`coalesce` behavior for the all-`null` case while extending it
cleanly. It is the dual of the strict-function rule documented in
`Mz/Strict.lean`: strict functions promote `err` above `null` in the
result of a per-cell computation; `coalesce` is non-strict and
demotes `err` below `null`.

This is a deliberate divergence from PG: PG's `coalesce(NULL, 1/0)`
evaluates `1/0` (because no non-null value has been seen) and errors;
this skeleton's `evalCoalesce [.null, .err DivByZero]` returns
`.null`. The divergence is recorded in the design doc's *SQL error
semantics → Non-strict functions* section.

Left-to-right evaluation order is part of the semantics, not an
implementation choice. `evalCoalesce` is order-sensitive: swapping
adjacent operands changes the result whenever one is `.null` and the
other is `.bool _ / .int _`. The unspecified-evaluation-order
caveats in `model.md`'s *Equivalence relations* discussion do not
apply here — `coalesce`, like `CASE`, has SQL-pinned order. -/

namespace Mz

/-! ## Base cases -/

theorem coalesce_nil : evalCoalesce [] = .null := rfl

theorem coalesce_singleton_bool (b : Bool) :
    evalCoalesce [.bool b] = .bool b := rfl

theorem coalesce_singleton_null :
    evalCoalesce [.null] = .null := rfl

theorem coalesce_singleton_err (e : EvalError) :
    evalCoalesce [.err e] = .err e := rfl

/-! ## Error-rescue laws

The defining property of the proposed `coalesce`: a later non-error,
non-null operand rescues an earlier `err` exactly as it rescues an
earlier `null`. -/

theorem coalesce_err_rescue_bool (e : EvalError) (b : Bool) :
    evalCoalesce [.err e, .bool b] = .bool b := rfl

theorem coalesce_null_rescue_bool (b : Bool) :
    evalCoalesce [.null, .bool b] = .bool b := rfl

/-! ## `null` beats `err` -/

theorem coalesce_null_then_err (e : EvalError) :
    evalCoalesce [.null, .err e] = .null := rfl

theorem coalesce_err_then_null (e : EvalError) :
    evalCoalesce [.err e, .null] = .null := rfl

/-! ## First error wins among errors -/

theorem coalesce_first_err_wins (e₁ e₂ : EvalError) :
    evalCoalesce [.err e₁, .err e₂] = .err e₁ := rfl

/-! ## Three-operand examples

These nail down the interaction between several `err`s, a `null`,
and a concrete value. They are intentionally stated as concrete
equations rather than universal laws so that a regression in
`Coalesce.go` breaks the offending equation in isolation. -/

theorem coalesce_err_err_bool (e₁ e₂ : EvalError) (b : Bool) :
    evalCoalesce [.err e₁, .err e₂, .bool b] = .bool b := rfl

theorem coalesce_err_err_null (e₁ e₂ : EvalError) :
    evalCoalesce [.err e₁, .err e₂, .null] = .null := rfl

theorem coalesce_err_null_err (e₁ e₂ : EvalError) :
    evalCoalesce [.err e₁, .null, .err e₂] = .null := rfl

theorem coalesce_null_err_bool (e : EvalError) (b : Bool) :
    evalCoalesce [.null, .err e, .bool b] = .bool b := rfl

end Mz
