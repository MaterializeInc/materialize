import Mz.Datum

/-!
# Diff-semiring extension for global errors

The design doc proposes that collection-scoped (global) errors be
encoded in the `diff` field of differential dataflow records: a
special marker in the diff that, when seen at time `t`, signals
"this collection is invalid at `t`". The marker must absorb the
ordinary multiset-count arithmetic — adding it to any other diff
gives the marker back, and likewise for multiplication.

This file defines `DiffWithError α`, the simplest extension of an
arbitrary diff type `α` (typically `ℤ` for multiset counts) with an
absorbing `error` element, and proves the absorption / commutativity
/ associativity laws an operator over the extended semiring relies
on.

The skeleton models only the algebraic structure; tying it to a
concrete dataflow operator is future work that requires modeling
collections-with-times. The laws here are however independent of
that — they are the proof obligations any such operator must
discharge.
-/

namespace Mz

/-- `α`-valued diff augmented with an absorbing `error` marker. The
`error` element is the algebraic encoding of "this collection is
globally invalid". -/
inductive DiffWithError (α : Type) where
  | val (x : α)
  | error
  deriving Inhabited

namespace DiffWithError

variable {α : Type}

/-- Lifted addition. `error` absorbs from either side; `val`s add
pointwise via the underlying `α`'s `+`. -/
def add [Add α] : DiffWithError α → DiffWithError α → DiffWithError α
  | .error, _      => .error
  | _, .error      => .error
  | .val x, .val y => .val (x + y)

/-- Lifted multiplication, with the same absorbing behavior. Joins
in differential dataflow are multiplicative on diffs. -/
def mul [Mul α] : DiffWithError α → DiffWithError α → DiffWithError α
  | .error, _      => .error
  | _, .error      => .error
  | .val x, .val y => .val (x * y)

/-- Lifted negation. `error` absorbs (negation cannot remove
the "collection invalid" marker); `val x` negates pointwise via
the underlying `α`'s `Neg`. Required for bag-difference flavors
of set operations (`EXCEPT ALL`): `L - R = L + (-R)` on diffs. -/
def neg [Neg α] : DiffWithError α → DiffWithError α
  | .error => .error
  | .val x => .val (-x)

instance [Add α] : Add (DiffWithError α) := ⟨add⟩
instance [Mul α] : Mul (DiffWithError α) := ⟨mul⟩
instance [Neg α] : Neg (DiffWithError α) := ⟨neg⟩

/-- Lifted zero (identity for `+`). -/
instance [Zero α] : Zero (DiffWithError α) := ⟨.val 0⟩

/-- Lifted one (identity for `*`). -/
instance [One α] : One (DiffWithError α) := ⟨.val 1⟩

/-! ## Absorption laws

The defining property of the `error` marker: any sum or product
involving it is itself `error`. -/

theorem error_add_left [Add α] (y : DiffWithError α) :
    (error : DiffWithError α) + y = error := rfl

theorem error_add_right [Add α] (x : DiffWithError α) :
    x + (error : DiffWithError α) = error := by
  cases x with
  | val _ => rfl
  | error => rfl

theorem error_mul_left [Mul α] (y : DiffWithError α) :
    (error : DiffWithError α) * y = error := rfl

theorem error_mul_right [Mul α] (x : DiffWithError α) :
    x * (error : DiffWithError α) = error := by
  cases x with
  | val _ => rfl
  | error => rfl

/-! ## Commutativity / associativity of `+` (when the base has them) -/

theorem add_comm [Add α] (h_comm : ∀ x y : α, x + y = y + x)
    (a b : DiffWithError α) : a + b = b + a := by
  cases a with
  | val x =>
    cases b with
    | val y => show (val (x + y) : DiffWithError α) = val (y + x); rw [h_comm]
    | error => rfl
  | error =>
    cases b with
    | val _ => rfl
    | error => rfl

theorem add_assoc [Add α] (h_assoc : ∀ x y z : α, (x + y) + z = x + (y + z))
    (a b c : DiffWithError α) : (a + b) + c = a + (b + c) := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      cases c with
      | val z =>
        show (val ((x + y) + z) : DiffWithError α) = val (x + (y + z))
        rw [h_assoc]
      | error => rfl
    | error =>
      cases c with
      | val _ => rfl
      | error => rfl
  | error =>
    cases b with
    | val _ =>
      cases c with
      | val _ => rfl
      | error => rfl
    | error =>
      cases c with
      | val _ => rfl
      | error => rfl

/-! ## Zero identity (when the base has it) -/

theorem zero_add_val [Add α] [Zero α] (h : ∀ x : α, 0 + x = x) (x : α) :
    (0 : DiffWithError α) + val x = val x := by
  show (val (0 + x) : DiffWithError α) = val x
  rw [h]

theorem val_add_zero [Add α] [Zero α] (h : ∀ x : α, x + 0 = x) (x : α) :
    (val x : DiffWithError α) + 0 = val x := by
  show (val (x + 0) : DiffWithError α) = val x
  rw [h]

/-! ## Distributivity (when the base has it)

Left distributivity says `a * (b + c) = a * b + a * c`. With the
absorbing `error`, the law holds unconditionally on `DiffWithError`
provided it holds on `α`: any `error` in the inputs forces every
sub-expression containing it to `error`, and `error + error = error`
restores the equality on the right. -/

theorem mul_add [Mul α] [Add α]
    (h_distrib : ∀ x y z : α, x * (y + z) = x * y + x * z)
    (a b c : DiffWithError α) : a * (b + c) = a * b + a * c := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      cases c with
      | val z =>
        show (val (x * (y + z)) : DiffWithError α) = val (x * y + x * z)
        rw [h_distrib]
      | error => rfl
    | error =>
      cases c with
      | val _ => rfl
      | error => rfl
  | error =>
    cases b with
    | val _ =>
      cases c with
      | val _ => rfl
      | error => rfl
    | error =>
      cases c with
      | val _ => rfl
      | error => rfl

/-! ## Associativity of `*` (when the base has it) -/

theorem mul_assoc [Mul α]
    (h_assoc : ∀ x y z : α, (x * y) * z = x * (y * z))
    (a b c : DiffWithError α) : (a * b) * c = a * (b * c) := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      cases c with
      | val z =>
        show (val ((x * y) * z) : DiffWithError α) = val (x * (y * z))
        rw [h_assoc]
      | error => rfl
    | error =>
      cases c with
      | val _ => rfl
      | error => rfl
  | error =>
    cases b with
    | val _ =>
      cases c with
      | val _ => rfl
      | error => rfl
    | error =>
      cases c with
      | val _ => rfl
      | error => rfl

/-- Commutativity of `*` (when the base has it). -/
theorem mul_comm [Mul α] (h_comm : ∀ x y : α, x * y = y * x)
    (a b : DiffWithError α) : a * b = b * a := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val (x * y) : DiffWithError α) = val (y * x)
      rw [h_comm]
    | error => rfl
  | error =>
    cases b with
    | val _ => rfl
    | error => rfl

/-! ## Negation laws

`error` absorbs negation, and double-negation is the identity on
`val` (when the base has the same property). -/

theorem neg_error [Neg α] :
    -(error : DiffWithError α) = error := rfl

theorem neg_val [Neg α] (x : α) :
    -(val x : DiffWithError α) = val (-x) := rfl

theorem neg_neg_val [Neg α] (h : ∀ x : α, - -x = x) (a : DiffWithError α) :
    - -a = a := by
  cases a with
  | val x => show (val (- -x) : DiffWithError α) = val x; rw [h]
  | error => rfl

/-- Right-inverse on `val`: when the base has `x + -x = 0`,
`val x + -val x = 0` in the lifted semiring. `error` does not have
an inverse — the absorber is unrecoverable, which is exactly the
spec property: a collection-scoped error cannot be "subtracted
away". -/
theorem val_add_neg_val [Add α] [Neg α] [Zero α]
    (h : ∀ x : α, x + -x = 0) (x : α) :
    (val x : DiffWithError α) + -val x = 0 := by
  show (val (x + -x) : DiffWithError α) = val 0
  rw [h]

/-- Negation distributes over addition, lifted from the base. -/
theorem neg_add [Add α] [Neg α]
    (h : ∀ x y : α, -(x + y) = -x + -y)
    (a b : DiffWithError α) : -(a + b) = -a + -b := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val (-(x + y)) : DiffWithError α) = val (-x + -y)
      rw [h]
    | error => rfl
  | error =>
    cases b with
    | val _ => rfl
    | error => rfl

/-- Negation distributes over multiplication on the left, lifted
from the base. Used by `Mz/SetOps.lean` to reason about negating
one side of a cross product. -/
theorem neg_mul [Mul α] [Neg α]
    (h : ∀ x y : α, (-x) * y = -(x * y))
    (a b : DiffWithError α) : (-a) * b = -(a * b) := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val ((-x) * y) : DiffWithError α) = val (-(x * y))
      rw [h]
    | error => rfl
  | error =>
    cases b with
    | val _ => rfl
    | error => rfl

/-- Negation distributes over multiplication on the right. -/
theorem mul_neg [Mul α] [Neg α]
    (h : ∀ x y : α, x * (-y) = -(x * y))
    (a b : DiffWithError α) : a * (-b) = -(a * b) := by
  cases a with
  | val x =>
    cases b with
    | val y =>
      show (val (x * (-y)) : DiffWithError α) = val (-(x * y))
      rw [h]
    | error => rfl
  | error =>
    cases b with
    | val _ => rfl
    | error => rfl

/-! ## Int specializations

The diff-aware operators in `Mz/UnifiedStream.lean` and
`Mz/Join.lean` instantiate the base type at `Int`. The following
specializations discharge the `h_*` hypotheses once at the type
level so downstream code can cite the named laws without
re-supplying base proofs every time. -/

theorem add_comm_int (a b : DiffWithError Int) : a + b = b + a :=
  add_comm Int.add_comm a b

theorem add_assoc_int (a b c : DiffWithError Int) :
    (a + b) + c = a + (b + c) :=
  add_assoc Int.add_assoc a b c

theorem mul_assoc_int (a b c : DiffWithError Int) :
    (a * b) * c = a * (b * c) :=
  mul_assoc Int.mul_assoc a b c

theorem mul_comm_int (a b : DiffWithError Int) : a * b = b * a :=
  mul_comm Int.mul_comm a b

theorem mul_add_int (a b c : DiffWithError Int) :
    a * (b + c) = a * b + a * c :=
  mul_add Int.mul_add a b c

theorem neg_neg_int (a : DiffWithError Int) : - -a = a :=
  neg_neg_val (fun x => Int.neg_neg x) a

theorem val_add_neg_val_int (x : Int) :
    (val x : DiffWithError Int) + -val x = 0 :=
  val_add_neg_val (fun x => by omega) x

theorem neg_add_int (a b : DiffWithError Int) : -(a + b) = -a + -b :=
  neg_add (fun _ _ => by omega) a b

theorem neg_mul_int (a b : DiffWithError Int) : (-a) * b = -(a * b) :=
  neg_mul (fun x y => Int.neg_mul x y) a b

theorem mul_neg_int (a b : DiffWithError Int) : a * (-b) = -(a * b) :=
  mul_neg (fun x y => Int.mul_neg x y) a b

end DiffWithError

end Mz
