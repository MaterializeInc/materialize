import Mz.Schema

/-!
# Monadic `Datum` — experimental parallel encoding

Pilot of a monadic representation of the `Datum k` value layer.
Existing `Mz/Datum.lean` defines `Datum : ColType → Type` as a
GADT with four explicit constructors (`.bool _`, `.int _`,
`.null`, `.err _`). This module proposes:

* `Concrete : ColType → Type` — type-family extracting the
  per-kind concrete inhabitant.
* `DatumW (α : Type)` — error-and-null monad over `α`.
* `Datum' (k : ColType) := DatumW (Concrete k)`.

So `Datum' .bool = DatumW Bool`, `Datum' .int = DatumW Int`,
`Datum' .top = DatumW Empty` (only `.null` / `.err _` inhabit).

`DatumW` is a plain `Type → Type`, so standard `Monad` /
`Applicative` instances apply (Step 2). Strict primitives
(`evalNot`, `evalPlus`, `evalEq`, …) reduce to monadic
expressions; variadic absorbing operators (`.andN`, `.orN`,
`.coalesce`) and branch-selecting `.ifThen` sit outside the
monad and stay bespoke.

**Status: pilot.** This module runs alongside `Mz/Datum.lean`
and `Mz/PrimEval.lean` for comparison. No consumers depend on
`Datum'`. If the pilot lands, a follow-up will migrate the
canonical `Datum` to this encoding. -/

namespace Mz

/-- Per-kind concrete inhabitant. `.bool` carries a `Bool`,
`.int` carries an `Int`, `.top` is unconstrained and has no
concrete inhabitant (`Empty`).

`@[reducible]` so that `Concrete .int` unfolds to `Int` for
type-class resolution (e.g. `HAdd Int Int Int` on monadic-bind
bodies). -/
@[reducible]
def Concrete : ColType → Type
  | .bool => Bool
  | .int  => Int
  | .top  => Empty

/-- Error-and-null monad over `α`. Three constructors:

* `.val a` — a concrete value of type `α`.
* `.null` — the SQL null.
* `.err e` — a cell-scoped error with payload `EvalError`.

The standard `Monad` instance (Step 2) propagates `.null` and
`.err _` through `bind`; only `.val a` runs the continuation. -/
inductive DatumW (α : Type)
  | val (a : α)
  | null
  | err (e : EvalError)
  deriving Inhabited

/-- Monadic `Datum k` — synonymous with `DatumW (Concrete k)`.
`Datum' .bool = DatumW Bool`; `Datum' .int = DatumW Int`;
`Datum' .top = DatumW Empty`. -/
def Datum' (k : ColType) : Type := DatumW (Concrete k)

/-! ## Monad instance

`DatumW` is the error-and-null monad. `pure a` injects a concrete
value; `bind d f` propagates `.null` and `.err _` and runs `f`
only on `.val a`. Three propagation rules are realized by the
single `bind` clause — exactly the strict-precedence pattern
(`err > null > val`). -/

instance : Monad DatumW where
  pure a := .val a
  bind d f := match d with
    | .val a  => f a
    | .null   => .null
    | .err e  => .err e

/-! ## Lawful monad

Standard monad laws hold by structural cases. `id_map`,
`pure_bind`, `bind_assoc` all close by `cases <;> rfl`. -/

instance : LawfulMonad DatumW := LawfulMonad.mk' DatumW
  (id_map      := fun x => by cases x <;> rfl)
  (pure_bind   := fun _ _ => rfl)
  (bind_assoc  := fun x _ _ => by cases x <;> rfl)

/-! ## Constructor aliases

Mirror the per-kind constructor names from `Mz/Datum.lean`'s GADT
form. `@[match_pattern]` lets pattern matches on `Datum' k` use
the named-constructor style: `.bool b`, `.int n`, `.null`,
`.err e`. Under the hood each unfolds to a `DatumW` constructor.

* `.bool` and `.int` are kind-specific (`Datum' .bool` and
  `Datum' .int` respectively); the type-family `Concrete` reduces
  the value type at each kind.
* `.null` and `.err _` are kind-polymorphic — every kind admits
  them. -/

@[match_pattern]
def Datum'.bool (b : Bool) : Datum' .bool := DatumW.val b

@[match_pattern]
def Datum'.int (n : Int) : Datum' .int := DatumW.val n

@[match_pattern]
def Datum'.null {k : ColType} : Datum' k := DatumW.null

@[match_pattern]
def Datum'.err {k : ColType} (e : EvalError) : Datum' k := DatumW.err e

/-! ## Sanity probe — pattern match works on `Datum' k` -/

example (d : Datum' .bool) : Nat :=
  match d with
  | .bool true  => 1
  | .bool false => 0
  | .null       => 2
  | .err _      => 3

example (d : Datum' .int) : Datum' .int :=
  match d with
  | .int n  => .int (n + 1)
  | .null   => .null
  | .err e  => .err e

example (d : Datum' .top) : Bool :=
  match d with
  | .null   => true
  | .err _  => false

/-! ## Primitives: strict ops via monad

Strict unary / binary ops express as `<$>` / `do`-notation over
`DatumW`. The monadic propagation handles `.null` and `.err _`
uniformly; the body of each primitive contains only the concrete
case. -/

/-- Boolean negation. Functorial — `not <$> d`. Propagates
`.null` and `.err _` automatically. -/
def evalNot' (d : Datum' .bool) : Datum' .bool :=
  Bool.not <$> d

/-- Integer addition. Strict on `.null` / `.err _` via monadic
bind. Body contains only the concrete-case arithmetic. -/
def evalPlus' (a b : Datum' .int) : Datum' .int := do
  let n ← a
  let m ← b
  return n + m

/-! ## Sanity probes -/

example : evalNot' (.bool true) = .bool false := rfl
example : evalNot' (.bool false) = .bool true := rfl
example : evalNot' .null = (.null : Datum' .bool) := rfl
example (e : EvalError) : evalNot' (.err e) = .err e := rfl

example : evalPlus' (.int 2) (.int 3) = .int 5 := rfl
example : evalPlus' (.int 2) .null = .null := rfl
example : evalPlus' .null (.int 3) = .null := rfl
example (e : EvalError) : evalPlus' (.err e) (.int 3) = .err e := rfl
example (e : EvalError) : evalPlus' (.int 2) (.err e) = .err e := rfl

/-! ## Kind-polymorphic comparison

`evalEq'` and `evalLt'` accept operands of any `Datum' k` and
return a `Datum' .bool`. Kind-discrimination on the concrete arms
flows through the typeclass requirement on `Concrete k`:

* `evalEq'` needs `DecidableEq (Concrete k)`. `Concrete .bool =
  Bool` and `Concrete .int = Int` both have stdlib instances;
  `Concrete .top = Empty` is vacuously decidable.
* `evalLt'` needs `Ord (Concrete k)` for the `compare` dispatch.

Empty needs an `Ord` instance for the `.top` case to type-check
even though it's vacuous. -/

instance : Ord Empty where
  compare a _ := a.elim

def evalEq' {k : ColType} [DecidableEq (Concrete k)]
    (a b : Datum' k) : Datum' .bool := do
  let x ← a
  let y ← b
  return decide (x = y)

def evalLt' {k : ColType} [Ord (Concrete k)]
    (a b : Datum' k) : Datum' .bool := do
  let x ← a
  let y ← b
  return compare x y == .lt

/-! ## Sanity probes -/

example : evalEq' (.bool true) (.bool true) = .bool true := rfl
example : evalEq' (.bool true) (.bool false) = .bool false := rfl
example : evalEq' (.int 3) (.int 3) = .bool true := rfl
example : evalEq' (.int 3) (.int 4) = .bool false := rfl
example : evalEq' (k := .int) (.int 3) .null = .null := rfl
example (e : EvalError) :
    evalEq' (k := .int) (.err e) (.int 3) = .err e := rfl

example : evalLt' (.int 2) (.int 3) = .bool true := rfl
example : evalLt' (.int 3) (.int 3) = .bool false := rfl
example : evalLt' (.bool false) (.bool true) = .bool true := rfl
example : evalLt' (k := .int) (.int 2) .null = .null := rfl
example (e : EvalError) :
    evalLt' (k := .int) (.err e) (.int 3) = .err e := rfl

/-! ## Variadic AND — outside the monad

Variadic `AND` has absorption order `FALSE > ERR > NULL > TRUE`.
Standard error-monad `bind` propagates `.err _` first; would
return `.err e` for `[.err e, .bool false]` instead of the
spec-correct `.bool false` (FALSE absorbs ERR). Monad doesn't
apply.

Direct fold via a custom binary combiner with the full
4-precedence dispatch. The combiner `datumAnd` is the AND-flavored
sibling of `evalAnd` from `Mz/PrimEval.lean`, expressed on the
monadic `Datum' .bool` representation. -/

/-- Binary AND on `Datum' .bool`. Absorption: FALSE > ERR > NULL > TRUE. -/
def datumAnd (a b : Datum' .bool) : Datum' .bool :=
  match a, b with
  | .bool false, _ => .bool false
  | _, .bool false => .bool false
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .bool true, .bool true => .bool true

/-- Variadic AND. Right-fold via `datumAnd` (mirrors
`evalAndN`). The absorption logic lives in `datumAnd`; the fold
itself is structural. -/
def evalAndN' (xs : List (Datum' .bool)) : Datum' .bool :=
  xs.foldr datumAnd (.bool true)

/-! ## Sanity probes — including the absorption story

`evalAndN' [.err e, .bool false] = .bool false` (FALSE absorbs ERR).
Standard error-monad `bind` would propagate `.err e` here. -/

example : evalAndN' [.bool true, .bool true] = .bool true := rfl
example : evalAndN' [.bool true, .bool false] = .bool false := rfl
example : evalAndN' [.null, .bool true] = .null := rfl
example (e : EvalError) :
    evalAndN' [.bool false, .err e] = .bool false := rfl
example (e : EvalError) :
    evalAndN' [.err e, .bool false] = .bool false := rfl
example (e : EvalError) :
    evalAndN' [.err e, .null] = .err e := rfl
example (e : EvalError) :
    evalAndN' [.null, .err e] = .err e := rfl

/-! ## Algebraic law pilot — idempotence of `datumAnd`

Port of `Mz/Laws.lean`'s `evalAnd_idem`. Existing proof on the
GADT-flavored `Datum .bool`:

```
theorem evalAnd_idem (d : Datum .bool) : evalAnd d d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl
```

Monadic-form proof on `Datum' .bool`: same shape, modulo the
`DatumW` constructor names (`.val` instead of `.bool`).
`@[match_pattern]` aliases let us still write `.bool b` in `match`
position; `cases` uses the underlying constructor names. -/

theorem datumAnd_idem (d : Datum' .bool) : datumAnd d d = d := by
  cases d with
  | val b => cases b <;> rfl
  | null  => rfl
  | err _ => rfl

end Mz
