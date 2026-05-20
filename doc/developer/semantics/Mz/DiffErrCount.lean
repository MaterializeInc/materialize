import Mz.Datum

/-!
# Row-scoped retractable diff

The skeleton's earlier `DiffWithError α` used an absorbing `error`
marker for *all* error scopes. That encoding can only express
collection-scoped (global) errors faithfully — the marker is
terminal under both addition and multiplication, so it cannot
retract.

Row-scoped errors raised inside a `WHERE` predicate or a join
condition (e.g., `1/(a+b)` on a row with `a+b = 0`) must retract:
a row that errs at time `t` and is retracted at time `t'` should
net to zero in the consolidated view. The earlier encoding's only
recourse was to drop the row from the data collection and route
the error through `DataflowError`, which loses the row's content.

The design doc (`doc/developer/design/20260517_error_handling_semantics.md`)
extends the diff with a per-`EvalError` multiplicity component:

```
Diff = Int × ErrCount
ErrCount = EvalError -fin-> Int
```

`Int` carries the valid-copy multiplicity; `ErrCount` carries the
per-`EvalError`-payload error-copy multiplicities. Addition is
pointwise on both axes. Multiplication is
`(a, m) * (b, n) = (a*b, a • n + b • m)` — the unique extension
that distributes over addition and keeps `(0, ∅)` absorbing under
`*`. Negation is pointwise, so both valid and err counts are
retractable through ordinary diff arithmetic.

This file defines `ErrCount` and `Diff` and proves the algebraic
laws. The absorbing collection-scoped marker layered on top of
`Diff` lives in `Mz/DiffWithGlobal.lean`. -/

namespace Mz

/-- Finite per-`EvalError` multiplicity. Modeled as a total
function with the understanding that we only ever construct
instances with finite support (`zero` plus finitely many
`single`-style additions). Decidable equality on `EvalError`
makes `single` definable.

A `Finsupp`-style finite-support carrier would be cleaner but
requires mathlib; the function representation suffices for the
algebra here — point-wise equality is reached via `funext` and
the laws below. -/
def ErrCount : Type := EvalError → Int

namespace ErrCount

/-- Identity for `add`: every err has count zero. -/
def zero : ErrCount := fun _ => 0

/-- Pointwise addition. -/
def add (m n : ErrCount) : ErrCount := fun e => m e + n e

/-- Pointwise negation. -/
def neg (m : ErrCount) : ErrCount := fun e => -(m e)

/-- Scalar multiplication: scale every err count by `a`. -/
def smul (a : Int) (m : ErrCount) : ErrCount := fun e => a * m e

/-- Concentrate `n` copies of err `e` and nothing else. -/
def single (e : EvalError) (n : Int) : ErrCount :=
  fun e' => if e = e' then n else 0

instance : Zero ErrCount := ⟨zero⟩
instance : Add  ErrCount := ⟨add⟩
instance : Neg  ErrCount := ⟨neg⟩

/-! ## Pointwise laws -/

theorem add_zero (m : ErrCount) : m + 0 = m := by
  funext e
  show m e + 0 = m e
  exact Int.add_zero _

theorem zero_add (m : ErrCount) : 0 + m = m := by
  funext e
  show 0 + m e = m e
  exact Int.zero_add _

theorem add_comm (m n : ErrCount) : m + n = n + m := by
  funext e
  show m e + n e = n e + m e
  exact Int.add_comm _ _

theorem add_assoc (m n p : ErrCount) : (m + n) + p = m + (n + p) := by
  funext e
  show (m e + n e) + p e = m e + (n e + p e)
  exact Int.add_assoc _ _ _

theorem neg_add_self (m : ErrCount) : -m + m = 0 := by
  funext e
  show -(m e) + m e = 0
  exact Int.add_left_neg _

theorem add_neg_self (m : ErrCount) : m + -m = 0 := by
  funext e
  show m e + -(m e) = 0
  exact Int.add_right_neg _

theorem neg_zero : -(0 : ErrCount) = 0 := by
  funext e
  show -(0 : Int) = 0
  rfl

theorem neg_add (m n : ErrCount) : -(m + n) = -m + -n := by
  funext e
  show -(m e + n e) = -(m e) + -(n e)
  exact Int.neg_add

/-! ## Scalar multiplication laws -/

theorem smul_zero (a : Int) : smul a 0 = 0 := by
  funext e
  show a * 0 = 0
  exact Int.mul_zero _

theorem zero_smul (m : ErrCount) : smul 0 m = 0 := by
  funext e
  show 0 * m e = 0
  exact Int.zero_mul _

theorem one_smul (m : ErrCount) : smul 1 m = m := by
  funext e
  show 1 * m e = m e
  exact Int.one_mul _

theorem smul_add (a : Int) (m n : ErrCount) :
    smul a (m + n) = smul a m + smul a n := by
  funext e
  show a * (m e + n e) = a * m e + a * n e
  exact Int.mul_add _ _ _

theorem add_smul (a b : Int) (m : ErrCount) :
    smul (a + b) m = smul a m + smul b m := by
  funext e
  show (a + b) * m e = a * m e + b * m e
  exact Int.add_mul _ _ _

theorem smul_smul (a b : Int) (m : ErrCount) :
    smul a (smul b m) = smul (a * b) m := by
  funext e
  show a * (b * m e) = (a * b) * m e
  rw [Int.mul_assoc]

theorem smul_neg (a : Int) (m : ErrCount) :
    smul a (-m) = -(smul a m) := by
  funext e
  show a * -(m e) = -(a * m e)
  exact Int.mul_neg _ _

theorem neg_smul (a : Int) (m : ErrCount) :
    smul (-a) m = -(smul a m) := by
  funext e
  show (-a) * m e = -(a * m e)
  exact Int.neg_mul _ _

end ErrCount

/-! ## `Diff`: pair of valid count and err counts

The carrier of row-scoped retractable multiplicities. -/

structure Diff where
  val : Int
  errs : ErrCount

namespace Diff

instance : Inhabited Diff := ⟨{ val := 0, errs := fun _ => 0 }⟩

def zero : Diff := { val := 0, errs := 0 }
def one  : Diff := { val := 1, errs := 0 }

def add (a b : Diff) : Diff :=
  { val := a.val + b.val, errs := a.errs + b.errs }

def neg (a : Diff) : Diff :=
  { val := -a.val, errs := -a.errs }

/-- Multiplicative combine. The cross/join rule: valid copies on
each side multiply normally; an err-count on one side is scaled by
the valid count on the other side and summed. The unique extension
of `*` that distributes over `+` and keeps `(0, ∅)` absorbing. -/
def mul (a b : Diff) : Diff :=
  { val  := a.val * b.val
  , errs := ErrCount.add (ErrCount.smul a.val b.errs)
                          (ErrCount.smul b.val a.errs) }

instance : Zero Diff := ⟨zero⟩
instance : One  Diff := ⟨one⟩
instance : Add  Diff := ⟨add⟩
instance : Neg  Diff := ⟨neg⟩
instance : Mul  Diff := ⟨mul⟩

/-! ## Convenience constructors -/

/-- Pure valid count, no errors. -/
@[inline] def pure (n : Int) : Diff := { val := n, errs := 0 }

/-- Pure err count for a single err payload. -/
@[inline] def err (e : EvalError) (n : Int) : Diff :=
  { val := 0, errs := ErrCount.single e n }

/-! ## Component reduction lemmas -/

theorem val_zero : (0 : Diff).val = 0 := rfl
theorem errs_zero : (0 : Diff).errs = (0 : ErrCount) := rfl
theorem val_one : (1 : Diff).val = 1 := rfl
theorem errs_one : (1 : Diff).errs = (0 : ErrCount) := rfl

theorem val_add (a b : Diff) : (a + b).val = a.val + b.val := rfl
theorem errs_add (a b : Diff) : (a + b).errs = a.errs + b.errs := rfl

theorem val_neg (a : Diff) : (-a).val = -a.val := rfl
theorem errs_neg (a : Diff) : (-a).errs = -a.errs := rfl

theorem val_mul (a b : Diff) : (a * b).val = a.val * b.val := rfl

theorem errs_mul (a b : Diff) :
    (a * b).errs
      = ErrCount.add (ErrCount.smul a.val b.errs)
                     (ErrCount.smul b.val a.errs) := rfl

/-! ## Additive monoid laws -/

theorem add_zero (a : Diff) : a + 0 = a := by
  rcases a with ⟨v, m⟩
  show (⟨v + 0, ErrCount.add m 0⟩ : Diff) = ⟨v, m⟩
  congr 1
  · exact Int.add_zero _
  · exact ErrCount.add_zero _

theorem zero_add (a : Diff) : 0 + a = a := by
  rcases a with ⟨v, m⟩
  show (⟨0 + v, ErrCount.add 0 m⟩ : Diff) = ⟨v, m⟩
  congr 1
  · exact Int.zero_add _
  · exact ErrCount.zero_add _

theorem add_comm (a b : Diff) : a + b = b + a := by
  rcases a with ⟨v, m⟩
  rcases b with ⟨w, n⟩
  show (⟨v + w, ErrCount.add m n⟩ : Diff) = ⟨w + v, ErrCount.add n m⟩
  congr 1
  · exact Int.add_comm _ _
  · exact ErrCount.add_comm _ _

theorem add_assoc (a b c : Diff) : (a + b) + c = a + (b + c) := by
  rcases a with ⟨v, m⟩
  rcases b with ⟨w, n⟩
  rcases c with ⟨u, p⟩
  show (⟨(v + w) + u, ErrCount.add (ErrCount.add m n) p⟩ : Diff)
        = ⟨v + (w + u), ErrCount.add m (ErrCount.add n p)⟩
  congr 1
  · exact Int.add_assoc _ _ _
  · exact ErrCount.add_assoc _ _ _

theorem add_neg_self (a : Diff) : a + -a = 0 := by
  rcases a with ⟨v, m⟩
  show (⟨v + -v, ErrCount.add m (-m)⟩ : Diff) = ⟨0, 0⟩
  congr 1
  · exact Int.add_right_neg _
  · exact ErrCount.add_neg_self _

theorem neg_add_self (a : Diff) : -a + a = 0 := by
  rcases a with ⟨v, m⟩
  show (⟨-v + v, ErrCount.add (-m) m⟩ : Diff) = ⟨0, 0⟩
  congr 1
  · exact Int.add_left_neg _
  · exact ErrCount.neg_add_self _

theorem neg_neg (a : Diff) : - -a = a := by
  rcases a with ⟨v, m⟩
  show (⟨- -v, ErrCount.neg (ErrCount.neg m)⟩ : Diff) = ⟨v, m⟩
  congr 1
  · exact Int.neg_neg _
  · funext e; exact Int.neg_neg _

theorem neg_zero : -(0 : Diff) = 0 := rfl

theorem neg_add (a b : Diff) : -(a + b) = -a + -b := by
  rcases a with ⟨v, m⟩
  rcases b with ⟨w, n⟩
  show (Diff.neg (Diff.add ⟨v, m⟩ ⟨w, n⟩) : Diff) = Diff.add (Diff.neg ⟨v, m⟩) (Diff.neg ⟨w, n⟩)
  show (⟨-(v + w), ErrCount.neg (ErrCount.add m n)⟩ : Diff)
        = ⟨-v + -w, ErrCount.add (-m) (-n)⟩
  congr 1
  · exact Int.neg_add
  · exact ErrCount.neg_add _ _

/-! ## Multiplicative laws -/

theorem mul_zero (a : Diff) : a * 0 = 0 := by
  rcases a with ⟨v, m⟩
  show (⟨v * 0, ErrCount.add (ErrCount.smul v (0 : ErrCount))
                              (ErrCount.smul 0 m)⟩ : Diff)
        = ⟨0, 0⟩
  congr 1
  · exact Int.mul_zero _
  · rw [ErrCount.smul_zero, ErrCount.zero_smul]
    exact ErrCount.add_zero _

theorem zero_mul (a : Diff) : 0 * a = 0 := by
  rcases a with ⟨v, m⟩
  show (⟨0 * v, ErrCount.add (ErrCount.smul 0 m) (ErrCount.smul v (0 : ErrCount))⟩
          : Diff) = ⟨0, 0⟩
  congr 1
  · exact Int.zero_mul _
  · rw [ErrCount.zero_smul, ErrCount.smul_zero]
    exact ErrCount.add_zero _

theorem mul_one (a : Diff) : a * 1 = a := by
  rcases a with ⟨v, m⟩
  show (⟨v * 1, ErrCount.add (ErrCount.smul v (0 : ErrCount))
                              (ErrCount.smul 1 m)⟩ : Diff)
        = ⟨v, m⟩
  congr 1
  · exact Int.mul_one _
  · rw [ErrCount.smul_zero, ErrCount.one_smul]
    exact ErrCount.zero_add _

theorem one_mul (a : Diff) : 1 * a = a := by
  rcases a with ⟨v, m⟩
  show (⟨1 * v, ErrCount.add (ErrCount.smul 1 m) (ErrCount.smul v (0 : ErrCount))⟩
          : Diff) = ⟨v, m⟩
  congr 1
  · exact Int.one_mul _
  · rw [ErrCount.one_smul, ErrCount.smul_zero]
    exact ErrCount.add_zero _

theorem mul_comm (a b : Diff) : a * b = b * a := by
  rcases a with ⟨v, m⟩
  rcases b with ⟨w, n⟩
  show (⟨v * w, ErrCount.add (ErrCount.smul v n) (ErrCount.smul w m)⟩ : Diff)
        = ⟨w * v, ErrCount.add (ErrCount.smul w m) (ErrCount.smul v n)⟩
  congr 1
  · exact Int.mul_comm _ _
  · exact ErrCount.add_comm _ _

theorem mul_assoc (a b c : Diff) : (a * b) * c = a * (b * c) := by
  rcases a with ⟨v, m⟩
  rcases b with ⟨w, n⟩
  rcases c with ⟨u, p⟩
  show (Diff.mul (Diff.mul ⟨v, m⟩ ⟨w, n⟩) ⟨u, p⟩)
        = Diff.mul ⟨v, m⟩ (Diff.mul ⟨w, n⟩ ⟨u, p⟩)
  show (⟨(v * w) * u,
          ErrCount.add (ErrCount.smul (v * w) p)
                       (ErrCount.smul u (ErrCount.add (ErrCount.smul v n)
                                                       (ErrCount.smul w m)))⟩ : Diff)
        = ⟨v * (w * u),
            ErrCount.add (ErrCount.smul v (ErrCount.add (ErrCount.smul w p)
                                                          (ErrCount.smul u n)))
                          (ErrCount.smul (w * u) m)⟩
  congr 1
  · exact Int.mul_assoc _ _ _
  · funext e
    show (v * w) * p e + u * (v * n e + w * m e)
          = v * (w * p e + u * n e) + (w * u) * m e
    have h1 : u * (v * n e + w * m e) = (u * v) * n e + (u * w) * m e := by
      rw [Int.mul_add, ← Int.mul_assoc, ← Int.mul_assoc]
    have h2 : v * (w * p e + u * n e) = (v * w) * p e + (v * u) * n e := by
      rw [Int.mul_add, ← Int.mul_assoc, ← Int.mul_assoc]
    rw [h1, h2]
    have hUV : u * v = v * u := Int.mul_comm _ _
    have hUW : u * w = w * u := Int.mul_comm _ _
    rw [hUV, hUW]
    -- LHS: (v*w)*p e + ((v*u)*n e + (w*u)*m e)
    -- RHS: (v*w)*p e + (v*u)*n e + (w*u)*m e = ((v*w)*p e + (v*u)*n e) + (w*u)*m e
    -- Need: a + (b + c) = (a + b) + c, i.e., ← Int.add_assoc on RHS.
    rw [← Int.add_assoc]

theorem mul_add (a b c : Diff) : a * (b + c) = a * b + a * c := by
  rcases a with ⟨v, m⟩
  rcases b with ⟨w, n⟩
  rcases c with ⟨u, p⟩
  show Diff.mul ⟨v, m⟩ (Diff.add ⟨w, n⟩ ⟨u, p⟩)
        = Diff.add (Diff.mul ⟨v, m⟩ ⟨w, n⟩) (Diff.mul ⟨v, m⟩ ⟨u, p⟩)
  show (⟨v * (w + u),
          ErrCount.add (ErrCount.smul v (ErrCount.add n p))
                       (ErrCount.smul (w + u) m)⟩ : Diff)
        = ⟨v * w + v * u,
            ErrCount.add (ErrCount.add (ErrCount.smul v n) (ErrCount.smul w m))
                          (ErrCount.add (ErrCount.smul v p) (ErrCount.smul u m))⟩
  congr 1
  · exact Int.mul_add _ _ _
  · funext e
    show v * (n e + p e) + (w + u) * m e
          = (v * n e + w * m e) + (v * p e + u * m e)
    rw [Int.mul_add, Int.add_mul]
    -- (v*n e + v*p e) + (w*m e + u*m e) = (v*n e + w*m e) + (v*p e + u*m e)
    rw [Int.add_assoc, ← Int.add_assoc (v * p e),
        Int.add_comm (v * p e) (w * m e),
        Int.add_assoc, ← Int.add_assoc]

theorem add_mul (a b c : Diff) : (a + b) * c = a * c + b * c := by
  rw [mul_comm, mul_add, mul_comm c a, mul_comm c b]

theorem neg_mul (a b : Diff) : -a * b = -(a * b) := by
  rcases a with ⟨v, m⟩
  rcases b with ⟨w, n⟩
  show Diff.mul (Diff.neg ⟨v, m⟩) ⟨w, n⟩ = Diff.neg (Diff.mul ⟨v, m⟩ ⟨w, n⟩)
  show (⟨(-v) * w,
          ErrCount.add (ErrCount.smul (-v) n) (ErrCount.smul w (-m))⟩ : Diff)
        = ⟨-(v * w),
            -(ErrCount.add (ErrCount.smul v n) (ErrCount.smul w m))⟩
  congr 1
  · exact Int.neg_mul _ _
  · rw [ErrCount.neg_smul, ErrCount.smul_neg]
    funext e
    show -(v * n e) + -(w * m e) = -(v * n e + w * m e)
    rw [Int.neg_add]

theorem mul_neg (a b : Diff) : a * -b = -(a * b) := by
  rw [mul_comm, neg_mul, mul_comm]

end Diff

end Mz
