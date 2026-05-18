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
(`.int _`) are preserved when paired with the identity element
`.bool true` (or another `.int`), so the algebraic laws
`evalAnd_true_left/right` and `evalAnd_idem` hold universally.
SQL would reject `.int` in `AND` at type-check time; the
skeleton's semantics is a coherent total extension. -/
def evalAnd : Datum → Datum → Datum
  | .bool false, _         => .bool false
  | _, .bool false         => .bool false
  | .err e, _              => .err e
  | _, .err e              => .err e
  | .null, _               => .null
  | _, .null               => .null
  | .bool true, .bool true => .bool true
  | .bool true, .int n     => .int n
  | .int n, .bool true     => .int n
  | .int n, .int m         => if n = m then .int n else .null

/-- OR evaluation table. Mirror of `evalAnd` with `TRUE` as the
dominant absorber: `TRUE > ERROR > NULL > FALSE`. Identity on
`.int` operands paired with the identity element `.bool false`. -/
def evalOr : Datum → Datum → Datum
  | .bool true, _            => .bool true
  | _, .bool true            => .bool true
  | .err e, _                => .err e
  | _, .err e                => .err e
  | .null, _                 => .null
  | _, .null                 => .null
  | .bool false, .bool false => .bool false
  | .bool false, .int n      => .int n
  | .int n, .bool false      => .int n
  | .int n, .int m           => if n = m then .int n else .null

/-- NOT evaluation table. Strict on `null` and `err`. Numeric
operands pass through unchanged so that `evalNot` stays
involutive on `.int` even though SQL would type-reject it. -/
def evalNot : Datum → Datum
  | .bool b => .bool (!b)
  | .null   => .null
  | .err e  => .err e
  | .int n  => .int n

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

/-! ## Environment -/

/-- Environment: a positional list of bindings for `Expr.col`. -/
abbrev Env := List Datum

/-- Reading an out-of-bounds column yields `NULL`. Defined by
primitive recursion to keep inductive proofs simple. -/
def Env.get : Env → Nat → Datum
  | [],          _     => .null
  | d :: _,      0     => d
  | _ :: rest,   n + 1 => Env.get rest n

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

/-! ### Coalesce state machine

`Coalesce.go` carries the `seenNull` sticky bit and the earliest
`err` payload while walking operands. The first concrete value
(`.bool _`) short-circuits the walk. -/

def Coalesce.go (seenNull : Bool) (firstErr : Option EvalError) :
    List Datum → Datum
  | []              =>
    if seenNull then .null
    else
      match firstErr with
      | some e => .err e
      | none   => .null
  | .bool b :: _    => .bool b
  | .int n  :: _    => .int n
  | .null   :: rest => Coalesce.go true firstErr rest
  | .err e  :: rest =>
    match firstErr with
    | some _ => Coalesce.go seenNull firstErr rest
    | none   => Coalesce.go seenNull (some e) rest

/-- `coalesce` returns the first concrete operand, with a
`null`-beats-`err` tiebreak when none exists. See `Mz/Coalesce.lean`
for the laws. -/
def evalCoalesce : List Datum → Datum :=
  Coalesce.go false none

end Mz
