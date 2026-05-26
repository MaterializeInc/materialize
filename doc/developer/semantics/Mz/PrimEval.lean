import Mz.Datum

/-!
# Primitive scalar evaluators

This file collects every evaluator that operates on `Datum` (or
`List Datum`) without reference to `Expr`. The split keeps the
defining equations available to both the algebraic-law files
(`Boolean.lean`, `Laws.lean`, `Strict.lean`, etc.) and the
expression-level evaluator (`Eval.lean`) without circular imports.

The primitives split into three groups:

* **Binary boolean and ternary if-then**: `evalAnd`, `evalOr`,
  `evalNot`, `evalIfThen`. Match the runtime in
  `src/expr/src/scalar/func/variadic.rs`.
* **Environment**: `Env`, `Env.get`. Indexed lookups for `Expr.col`.
* **Variadic primitives**: `evalAndN`, `evalOrN`, `evalCoalesce`.
  Used directly by `Expr.andN`, `Expr.orN`, `Expr.coalesce`
  evaluation in `Eval.lean`.
-/

namespace Mz

/-! ## Binary and ternary boolean evaluators -/

/-- AND evaluation table. Pattern order encodes the absorption
hierarchy `FALSE > ERROR > NULL > TRUE`. Non-boolean operands
(`.int _`) route to `.null` via the catch-all — the codomain of
`evalAnd` on a `Datum × Datum` argument is the boolean fragment
`{.bool _, .null, .err _}`.

Modeling note: in production, a type-mismatched operand to `AND`
would be caught by the planner type-checker; if it slipped past,
Materialize would panic, escalating the row to the error
collection. The skeleton routes to `.null` instead, which keeps
the row in the data collection — a sound over-approximation of
the panic-and-escalate behavior because any rewrite sound under
`.null` is also sound under panic (panic strictly removes
observable rows). Using `.null` lets the skeleton stay total
without introducing a `typeMismatch` `EvalError` variant.

Laws on arbitrary `Datum` operands (`evalAnd_true_left/right`,
`evalAnd_idem`) carry a `¬IsInt` hypothesis as a result. -/
def evalAnd : Datum → Datum → Datum
  | .bool false, _         => .bool false
  | _, .bool false         => .bool false
  | .err e, _              => .err e
  | _, .err e              => .err e
  | .null, _               => .null
  | _, .null               => .null
  | .bool true, .bool true => .bool true
  | _, _                   => .null

/-- OR evaluation table. Mirror of `evalAnd` with `TRUE` as the
dominant absorber: `TRUE > ERROR > NULL > FALSE`. Non-boolean
operands route to `.null`; codomain is the boolean fragment. -/
def evalOr : Datum → Datum → Datum
  | .bool true, _            => .bool true
  | _, .bool true            => .bool true
  | .err e, _                => .err e
  | _, .err e                => .err e
  | .null, _                 => .null
  | _, .null                 => .null
  | .bool false, .bool false => .bool false
  | _, _                     => .null

/-- NOT evaluation table. Strict on `null` and `err`. Non-boolean
operands (`.int _`) route to `.null` via the catch-all, matching
the tightened codomain of `evalAnd` / `evalOr`. The codomain of
`evalNot` is `{.bool _, .null, .err _}`. Same modeling note as
for `evalAnd` (`Mz/PrimEval.lean` above): in production a
type-mismatched operand would be caught by the planner or panic;
the skeleton's `.null` route is a sound over-approximation. -/
def evalNot : Datum → Datum
  | .bool b => .bool (!b)
  | .null   => .null
  | .err e  => .err e
  | .int _  => .null

/-- `IfThen` evaluation table. Modeled strictly; see `Mz/Eval.lean`
for the discussion of lazy vs strict in a total skeleton. -/
def evalIfThen : Datum → Datum → Datum → Datum
  | .bool true,  dt, _  => dt
  | .bool false, _,  de => de
  | .null,       _,  _  => .null
  | .err e,      _,  _  => .err e
  | _,           _,  _  => .null

/-! ## Numeric arithmetic

Binary integer arithmetic. Strict on `.err` and `.null`. Non-
numeric operands route to `.null` for totality. Division by zero
returns `.err .divisionByZero` — the canonical cell-scoped error
the design doc cites. -/

/-- Integer addition. Strict on `.err` (propagates) and `.null`
(propagates). Type-mismatched operands route to `.null`. -/
def evalPlus : Datum → Datum → Datum
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => .int (n + m)
  | _, _           => .null

/-- Bounded integer addition. Parameterized on the symmetric range
`[-max, max]`. Strict on `.err` and `.null`; type-mismatched
operands route to `.null`. An `.int + .int` result outside
`[-max, max]` returns `.err .overflow`.

The runtime counterpart is the bit-width-parameterized addition
in `src/expr/src/scalar/func/binary.rs`. The symmetric range here
keeps the bounded-arithmetic counterexample proofs trivial; the
asymmetry of two's-complement (`MIN = -MAX - 1`) is irrelevant to
the soundness argument and can be tightened when a bit-width tag
is added to `Datum`. -/
def evalPlusBounded (max : Int) : Datum → Datum → Datum
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m =>
    let r := n + m
    if r > max ∨ r < -max then .err .overflow else .int r
  | _, _           => .null

/-- Integer subtraction. Same propagation rules as `evalPlus`. -/
def evalMinus : Datum → Datum → Datum
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => .int (n - m)
  | _, _           => .null

/-- Integer multiplication. Same propagation rules. -/
def evalTimes : Datum → Datum → Datum
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => .int (n * m)
  | _, _           => .null

/-- Integer division. Strict on `.err` and `.null`. A right
operand of `.int 0` produces `.err .divisionByZero`. -/
def evalDivide : Datum → Datum → Datum
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => if m = 0 then .err .divisionByZero else .int (n / m)
  | _, _           => .null

/-! ## Comparison

Binary comparison primitives. Strict on `.err` (propagates the
left-most err) and `.null` (propagates `.null`). Mixed-type
operands route to `.null` — the skeleton does not model SQL
implicit casts. Booleans compare by SQL's `false < true` ordering;
integers compare by `Int`'s built-in `<` / `=`.

The output is always a `.bool`, `.null`, or `.err` — never a
numeric or string. This keeps comparisons compatible with the
boolean fragment as a `WHERE` predicate. -/

/-- Equality test. `.bool x = .bool y` and `.int n = .int m` use
the decidable equality of the base types; mixed types yield
`.null`. -/
def evalEq : Datum → Datum → Datum
  | .err e, _          => .err e
  | _, .err e          => .err e
  | .null, _           => .null
  | _, .null           => .null
  | .bool x, .bool y   => .bool (decide (x = y))
  | .int  n, .int  m   => .bool (decide (n = m))
  | _, _               => .null

/-- Strict less-than. Booleans compare with `false < true`;
integers compare with `Int`'s `<`. Mixed types yield `.null`. -/
def evalLt : Datum → Datum → Datum
  | .err e, _          => .err e
  | _, .err e          => .err e
  | .null, _           => .null
  | _, .null           => .null
  | .bool x, .bool y   => .bool (decide (x < y))
  | .int  n, .int  m   => .bool (decide (n < m))
  | _, _               => .null

/-! ## Environment -/

/-- Environment: a positional list of bindings for `Expr.col`. -/
abbrev Env := List Datum

/-- Reading an out-of-bounds column yields `NULL`. Defined by
primitive recursion to keep inductive proofs simple.

**Load-bearing.** The `.null`-on-OOB fallback is consumed by
`Mz/Subst.lean` (`Expr.subst` substitutes `.lit .null` for OOB
references so `eval_subst` is unconditional) and by
`Mz/OutputType.lean` (`Expr.outputType (.col i)` returns
`{nullable := true, errable := false}` on OOB indices). A future
iteration that changes `Env.get` to return `.err _` on OOB —
defensible for catching planner bugs — must synchronously flip
the OOB substitution literal in `Expr.subst` and the OOB output
schema in `Expr.outputType` from `errable := false` to
`errable := true`, plus restate `eval_subst` with a column-bound
hypothesis. The dependency is implicit; document it if a forcing
function appears. -/
def Env.get : Env → Nat → Datum
  | [],          _     => .null
  | d :: _,      0     => d
  | _ :: rest,   n + 1 => Env.get rest n

/-- `Env.get` agrees with `List.get` on in-bounds indices. -/
theorem Env.get_eq_list_get (l : List Datum) (i : Nat) (h : i < l.length) :
    Env.get l i = l.get ⟨i, h⟩ := by
  induction l generalizing i with
  | nil => cases h
  | cons hd tl ih =>
    cases i with
    | zero => rfl
    | succ k =>
      show Env.get tl k = tl.get ⟨k, by simp [List.length_cons] at h; omega⟩
      apply ih

/-- `Env.get` returns `.null` past the end of the list. -/
theorem Env.get_eq_null_of_ge (l : List Datum) (i : Nat) (h : l.length ≤ i) :
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

/-! ## Variadic primitives

Variadic `AND`, `OR`, and `COALESCE` evaluators over `List Datum`.
The fold-style definitions are exposed here so `eval` in
`Mz/Eval.lean` can refer to them by name without a forward
dependency. The theorems for these evaluators live in
`Mz/Variadic.lean` and `Mz/Coalesce.lean`. -/

/-- Right-fold variadic AND. Seed value `TRUE` is the identity for
`evalAnd`, giving the cons recurrence by `rfl`. -/
def evalAndN : List Datum → Datum
  | []        => .bool true
  | d :: rest => evalAnd d (evalAndN rest)

/-- Right-fold variadic OR. Dual of `evalAndN`. -/
def evalOrN : List Datum → Datum
  | []        => .bool false
  | d :: rest => evalOr d (evalOrN rest)

/-! ### Coalesce: two-pass evaluator

`evalCoalesce` is split into two independent helpers:

* `Coalesce.firstConcrete` finds the leftmost `.bool _` or
  `.int _` in the operand list. This pass realizes the SQL spec
  "first non-null, non-error operand".
* `Coalesce.residue` produces the no-concrete fallback: `.null` if
  any operand was `.null`, else the leftmost `.err _` if any, else
  `.null` for the empty list.

`evalCoalesce` dispatches between them. Splitting into two passes
keeps each pass's invariant local — first pass is a simple
left-to-right search; second pass is a fold with the `.null >
.err > nothing` precedence — and is a more direct transcription
of the design doc's *Non-strict functions* clause than the prior
state-machine encoding. -/

/-- Predicate Bool for `.null`. -/
def Datum.isNullB : Datum → Bool
  | .null => true
  | _ => false

/-- Pass 1: the leftmost concrete (`.bool _` or `.int _`) operand,
if any. Returns `none` when every operand is `.null` or `.err _`. -/
def Coalesce.firstConcrete : List Datum → Option Datum
  | []           => none
  | .bool b :: _ => some (.bool b)
  | .int n :: _  => some (.int n)
  | _ :: rest    => Coalesce.firstConcrete rest

/-- Pass 2: residue when no concrete operand exists. `.null` wins
over any subsequent `.err _`; the leftmost `.err _` wins over
subsequent `.err _`s; the empty list yields `.null`. -/
def Coalesce.residue : List Datum → Datum
  | []              => .null
  | .null :: _      => .null
  | .err e :: rest  =>
    if rest.any Datum.isNullB then .null else .err e
  | _ :: rest       => Coalesce.residue rest

/-- `coalesce` returns the first concrete operand, with a
`null`-beats-`err` tiebreak when none exists. See `Mz/Coalesce.lean`
for the laws. -/
def evalCoalesce (ds : List Datum) : Datum :=
  match Coalesce.firstConcrete ds with
  | some d => d
  | none   => Coalesce.residue ds

end Mz
