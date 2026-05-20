import Mz.DiffErrCount

/-!
# Diff extended with an absorbing global-error marker

`Mz/DiffErrCount.lean` defines the row-scoped retractable diff
`Diff = Int × ErrCount`. Collection-scoped (global) errors require a
strictly stronger encoding: a marker that absorbs both addition and
multiplication and cannot be retracted.

The design doc (`doc/developer/design/20260517_error_handling_semantics.md`,
section "Global-scoped errors: absorbing diff marker") layers an
absorbing `global` marker over `Diff`:

```
DiffWithGlobal = val(Diff) | global
```

with `global + d = global`, `d + global = global`, `global * d = global`,
`d * global = global`. The `val` branch reuses `Diff`'s full
`Add`/`Mul`/`Neg` algebra; the `global` branch is terminal under every
operation.

This is the two-layer encoding the design doc prescribes: row-scoped
errors live in `Diff.errs` (retractable), collection-scoped errors live
in `DiffWithGlobal.global` (terminal). The older `DiffWithError α` in
`Mz/DiffSemiring.lean` conflated the two scopes; downstream files still
use it pending migration.

The mechanization mirrors `Mz/DiffSemiring.lean`'s proof style but
specializes the payload to `Diff`, so the laws can cite
`Diff.add_comm`, `Diff.mul_assoc`, etc., directly rather than taking
`h_*` hypotheses on a generic base. -/

namespace Mz

/-- `Diff`-valued diff augmented with an absorbing `global` marker.
The `global` element encodes "this collection is invalid at this
time". Unlike the row-scoped err-count axis inside `Diff`, `global`
is terminal: addition and multiplication preserve it, and there is
no inverse. -/
inductive DiffWithGlobal where
  | val (d : Diff)
  | global
  deriving Inhabited

namespace DiffWithGlobal

/-- Lifted addition. `global` absorbs from either side; `val`s add
pointwise via `Diff`'s `+`. -/
def add : DiffWithGlobal → DiffWithGlobal → DiffWithGlobal
  | .global, _      => .global
  | _, .global      => .global
  | .val a, .val b  => .val (a + b)

/-- Lifted multiplication. Joins/crosses on the absorbing extension. -/
def mul : DiffWithGlobal → DiffWithGlobal → DiffWithGlobal
  | .global, _      => .global
  | _, .global      => .global
  | .val a, .val b  => .val (a * b)

/-- Lifted negation. `global` absorbs (the "collection invalid"
marker cannot be negated away); `val a` negates pointwise via
`Diff`'s `Neg`. Required for bag-difference flavors of set
operations (`EXCEPT ALL`). -/
def neg : DiffWithGlobal → DiffWithGlobal
  | .global => .global
  | .val a  => .val (-a)

instance : Add DiffWithGlobal := ⟨add⟩
instance : Mul DiffWithGlobal := ⟨mul⟩
instance : Neg DiffWithGlobal := ⟨neg⟩

/-- Lifted zero (identity for `+`). -/
instance : Zero DiffWithGlobal := ⟨.val 0⟩

/-- Lifted one (identity for `*`). -/
instance : One DiffWithGlobal := ⟨.val 1⟩

/-! ## Absorption laws

The defining property of the `global` marker: any sum or product
involving it is itself `global`. -/

theorem global_add_left (y : DiffWithGlobal) :
    (global : DiffWithGlobal) + y = global := rfl

theorem global_add_right (x : DiffWithGlobal) :
    x + (global : DiffWithGlobal) = global := by
  cases x with
  | val _  => rfl
  | global => rfl

/-- Converse of absorption: `.global` can only emerge from `+` if at
least one summand was `.global`. The `.val + .val` branch always
returns `.val`, so the result `.global` rules it out. -/
theorem add_eq_global_left_or_right (a b : DiffWithGlobal)
    (h : a + b = global) :
    a = global ∨ b = global := by
  cases a with
  | global => exact Or.inl rfl
  | val x =>
    cases b with
    | global => exact Or.inr rfl
    | val y =>
      have hEq : (DiffWithGlobal.val x : DiffWithGlobal) + DiffWithGlobal.val y
               = DiffWithGlobal.val (x + y) := rfl
      rw [hEq] at h
      cases h

theorem global_mul_left (y : DiffWithGlobal) :
    (global : DiffWithGlobal) * y = global := rfl

theorem global_mul_right (x : DiffWithGlobal) :
    x * (global : DiffWithGlobal) = global := by
  cases x with
  | val _  => rfl
  | global => rfl

/-! ## Commutativity / associativity of `+` -/

theorem add_comm (a b : DiffWithGlobal) : a + b = b + a := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val (x + y) : DiffWithGlobal) = val (y + x)
      rw [Diff.add_comm]
    | global => rfl
  | global =>
    cases b with
    | val _  => rfl
    | global => rfl

theorem add_assoc (a b c : DiffWithGlobal) : (a + b) + c = a + (b + c) := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      cases c with
      | val z =>
        show (val ((x + y) + z) : DiffWithGlobal) = val (x + (y + z))
        rw [Diff.add_assoc]
      | global => rfl
    | global =>
      cases c with
      | val _  => rfl
      | global => rfl
  | global =>
    cases b with
    | val _ =>
      cases c with
      | val _  => rfl
      | global => rfl
    | global =>
      cases c with
      | val _  => rfl
      | global => rfl

/-! ## Zero identity -/

theorem zero_add_val (x : Diff) :
    (0 : DiffWithGlobal) + val x = val x := by
  show (val (0 + x) : DiffWithGlobal) = val x
  rw [Diff.zero_add]

theorem val_add_zero (x : Diff) :
    (val x : DiffWithGlobal) + 0 = val x := by
  show (val (x + 0) : DiffWithGlobal) = val x
  rw [Diff.add_zero]

/-! ## Distributivity

Left distributivity says `a * (b + c) = a * b + a * c`. With the
absorbing `global`, the law holds unconditionally: any `global` in the
inputs forces every sub-expression containing it to `global`, and
`global + global = global` restores the equality on the right. -/

theorem mul_add (a b c : DiffWithGlobal) : a * (b + c) = a * b + a * c := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      cases c with
      | val z =>
        show (val (x * (y + z)) : DiffWithGlobal) = val (x * y + x * z)
        rw [Diff.mul_add]
      | global => rfl
    | global =>
      cases c with
      | val _  => rfl
      | global => rfl
  | global =>
    cases b with
    | val _ =>
      cases c with
      | val _  => rfl
      | global => rfl
    | global =>
      cases c with
      | val _  => rfl
      | global => rfl

/-! ## Associativity / commutativity of `*` -/

theorem mul_assoc (a b c : DiffWithGlobal) : (a * b) * c = a * (b * c) := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      cases c with
      | val z =>
        show (val ((x * y) * z) : DiffWithGlobal) = val (x * (y * z))
        rw [Diff.mul_assoc]
      | global => rfl
    | global =>
      cases c with
      | val _  => rfl
      | global => rfl
  | global =>
    cases b with
    | val _ =>
      cases c with
      | val _  => rfl
      | global => rfl
    | global =>
      cases c with
      | val _  => rfl
      | global => rfl

theorem mul_comm (a b : DiffWithGlobal) : a * b = b * a := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val (x * y) : DiffWithGlobal) = val (y * x)
      rw [Diff.mul_comm]
    | global => rfl
  | global =>
    cases b with
    | val _  => rfl
    | global => rfl

/-! ## Negation laws

`global` absorbs negation, and double-negation is the identity on
`val` (lifted from `Diff.neg_neg`). -/

theorem neg_global :
    -(global : DiffWithGlobal) = global := rfl

theorem neg_val (x : Diff) :
    -(val x : DiffWithGlobal) = val (-x) := rfl

theorem neg_neg (a : DiffWithGlobal) : - -a = a := by
  cases a with
  | val x =>
    show (val (- -x) : DiffWithGlobal) = val x
    rw [Diff.neg_neg]
  | global => rfl

/-- Right-inverse on `val`: lifted from `Diff.add_neg_self`. `global`
has no inverse — the absorber is unrecoverable, which is exactly the
spec property: a collection-scoped error cannot be retracted. -/
theorem val_add_neg_val (x : Diff) :
    (val x : DiffWithGlobal) + -val x = 0 := by
  show (val (x + -x) : DiffWithGlobal) = val 0
  rw [Diff.add_neg_self]

/-- Negation distributes over addition, lifted from `Diff.neg_add`. -/
theorem neg_add (a b : DiffWithGlobal) : -(a + b) = -a + -b := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val (-(x + y)) : DiffWithGlobal) = val (-x + -y)
      rw [Diff.neg_add]
    | global => rfl
  | global =>
    cases b with
    | val _  => rfl
    | global => rfl

/-- Negation distributes over multiplication on the left, lifted from
`Diff.neg_mul`. -/
theorem neg_mul (a b : DiffWithGlobal) : (-a) * b = -(a * b) := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val ((-x) * y) : DiffWithGlobal) = val (-(x * y))
      rw [Diff.neg_mul]
    | global => rfl
  | global =>
    cases b with
    | val _  => rfl
    | global => rfl

/-- Negation distributes over multiplication on the right, lifted
from `Diff.mul_neg`. -/
theorem mul_neg (a b : DiffWithGlobal) : a * (-b) = -(a * b) := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val (x * (-y)) : DiffWithGlobal) = val (-(x * y))
      rw [Diff.mul_neg]
    | global => rfl
  | global =>
    cases b with
    | val _  => rfl
    | global => rfl

end DiffWithGlobal

end Mz
