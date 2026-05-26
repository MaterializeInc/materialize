import Mz.Indexed.Datum

/-!
# Primitive scalar evaluators (indexed)

Each evaluator is typed by the input / output kinds. No catch-all
`_ , _ => .null` arms: the indexed `Datum k` rules out
type-mismatched operands at the type level.

The primitives split into three groups, mirroring the layout of
the untyped `Mz/PrimEval.lean`:

* **Binary boolean and ternary if-then** on `Datum .bool`:
  `evalAnd`, `evalOr`, `evalNot`, `evalIfThen`.
* **Integer arithmetic** on `Datum .int`: `evalPlus`, `evalMinus`,
  `evalTimes`, `evalDivide`, plus the `evalPlusBounded` variant
  for boundary counterexamples.
* **Same-kind comparison** producing `Datum .bool`: `evalEq`,
  `evalLt`. The kind parameter is shared by both operands.
* **Variadic** on `Datum k`: `evalAndN` / `evalOrN` (bool only),
  `evalCoalesce` (any `k`, two-pass form).

Codomain of every evaluator is now closed by construction — the
boolean evaluators return `Datum .bool`, the arithmetic ones
return `Datum .int`, comparisons return `Datum .bool`. No
type-mismatch routing, no `¬IsInt` hypotheses on downstream laws.
-/

namespace Mz.Indexed

open Mz

/-! ## Binary and ternary boolean evaluators -/

/-- AND on `Datum .bool × Datum .bool`. Pattern order encodes the
absorption hierarchy `FALSE > ERROR > NULL > TRUE`. No catch-all
— `Datum .bool` admits only `.bool _`, `.null`, `.err _`. -/
def evalAnd : Datum .bool → Datum .bool → Datum .bool
  | .bool false, _         => .bool false
  | _, .bool false         => .bool false
  | .err e, _              => .err e
  | _, .err e              => .err e
  | .null, _               => .null
  | _, .null               => .null
  | .bool true, .bool true => .bool true

/-- OR on `Datum .bool × Datum .bool`. Mirror of `evalAnd` with
`TRUE` as the dominant absorber. -/
def evalOr : Datum .bool → Datum .bool → Datum .bool
  | .bool true, _            => .bool true
  | _, .bool true            => .bool true
  | .err e, _                => .err e
  | _, .err e                => .err e
  | .null, _                 => .null
  | _, .null                 => .null
  | .bool false, .bool false => .bool false

/-- NOT on `Datum .bool`. Strict on `.null` and `.err _`. -/
def evalNot : Datum .bool → Datum .bool
  | .bool b => .bool (!b)
  | .null   => .null
  | .err e  => .err e

/-- `IfThen` with bool condition and matching-kind branches. -/
def evalIfThen {k : ColType} : Datum .bool → Datum k → Datum k → Datum k
  | .bool true,  dt, _  => dt
  | .bool false, _,  de => de
  | .null,       _,  _  => .null
  | .err e,      _,  _  => .err e

/-! ## Integer arithmetic -/

/-- Integer addition. Strict on `.err` and `.null`. -/
def evalPlus : Datum .int → Datum .int → Datum .int
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => .int (n + m)

/-- Bounded integer addition. Symmetric range `[-max, max]`. -/
def evalPlusBounded (max : Int) : Datum .int → Datum .int → Datum .int
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m =>
    let r := n + m
    if r > max ∨ r < -max then .err .overflow else .int r

def evalMinus : Datum .int → Datum .int → Datum .int
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => .int (n - m)

def evalTimes : Datum .int → Datum .int → Datum .int
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => .int (n * m)

/-- Integer division. Strict on `.err` and `.null`. Right operand
of `.int 0` produces `.err .divisionByZero`. -/
def evalDivide : Datum .int → Datum .int → Datum .int
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => if m = 0 then .err .divisionByZero else .int (n / m)

/-! ## Comparison

Both operands share the same kind via the index parameter. Mixed-
kind comparison isn't expressible at the type level — the untyped
catch-all `.null` route disappears entirely. -/

def evalEq {k : ColType} : Datum k → Datum k → Datum .bool
  | .err e, _        => .err e
  | _, .err e        => .err e
  | .null, _         => .null
  | _, .null         => .null
  | .bool x, .bool y => .bool (decide (x = y))
  | .int n,  .int m  => .bool (decide (n = m))

def evalLt {k : ColType} : Datum k → Datum k → Datum .bool
  | .err e, _        => .err e
  | _, .err e        => .err e
  | .null, _         => .null
  | _, .null         => .null
  | .bool x, .bool y => .bool (decide (x < y))
  | .int n,  .int m  => .bool (decide (n < m))

/-! ## Variadic primitives -/

/-- Right-fold variadic AND. Seed value is `.bool true`. -/
def evalAndN : List (Datum .bool) → Datum .bool
  | []        => .bool true
  | d :: rest => evalAnd d (evalAndN rest)

/-- Right-fold variadic OR. -/
def evalOrN : List (Datum .bool) → Datum .bool
  | []        => .bool false
  | d :: rest => evalOr d (evalOrN rest)

/-! ### Coalesce: two-pass evaluator (indexed)

All operands share kind `k`. The first concrete operand
(`.bool _` or `.int _`, the latter only present when `k = .int`)
short-circuits. On no concrete, residue follows the `.null > .err
> nothing` rule. -/

/-- Pass 1: leftmost concrete operand, if any. -/
def Coalesce.firstConcrete {k : ColType} : List (Datum k) → Option (Datum k)
  | []           => none
  | .bool b :: _ => some (.bool b)
  | .int n :: _  => some (.int n)
  | _ :: rest    => Coalesce.firstConcrete rest

/-- Pass 2: residue when no concrete operand exists. -/
def Coalesce.residue {k : ColType} : List (Datum k) → Datum k
  | []              => .null
  | .null :: _      => .null
  | .err e :: rest  =>
    if rest.any Datum.isNullB then .null else .err e
  | _ :: rest       => Coalesce.residue rest

/-- `coalesce` on the indexed model. -/
def evalCoalesce {k : ColType} (ds : List (Datum k)) : Datum k :=
  match Coalesce.firstConcrete ds with
  | some d => d
  | none   => Coalesce.residue ds

end Mz.Indexed
