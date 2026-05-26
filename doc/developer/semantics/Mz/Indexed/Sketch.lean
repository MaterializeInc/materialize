import Mz.Datum
import Mz.Schema

/-!
# Sketch: intrinsically-typed `Expr` (GADT) prototype

A self-contained proof-of-concept exploring whether an indexed
inductive `Expr : ColKind → Type` simplifies the model by making
ill-typed expressions structurally unconstructible.

The untyped `Mz/Expr.lean` model carries a separate `WellTyped`
predicate and a `Datum.kind` analysis. Evaluators handle
type-mismatched operands with catch-all `_ , _ => .null` arms.
This sketch lifts kind discipline into the type system:

* `Datum k` — datum indexed by its kind. `.bool`/`.int`
  constructors live in their respective indices; `.null` / `.err _`
  inhabit every index.
* `Expr k` — expression whose evaluation produces a `Datum k`.
  Constructors enforce kind-correctness at construction time:
  `Expr.not : Expr .bool → Expr .bool`,
  `Expr.plus : Expr .int → Expr .int → Expr .int`, etc.

The sketch covers the closed-expression fragment (no `.col` —
schema-indexed column references would couple `Expr` to a
`Schema n` value at the type level, which is the load-bearing
question for whether this generalizes; see *Open questions* at the
end). The point is to show that for the closed fragment the
type-mismatch routing and the `WellTyped` predicate both disappear,
and at least one demonstrator theorem closes without the awkward
hypothesis the untyped model carries.

This file is not part of the main `Mz.lean` import. It compiles
standalone for the prototype evaluation.
-/

namespace Mz.Indexed

open Mz

/-! ## Indexed `Datum`

`Datum .bool` admits `.bool _`, `.null`, `.err _`.
`Datum .int`  admits `.int _`,  `.null`, `.err _`.
`Datum .top`  admits only `.null`, `.err _` — the polymorphic case.

The `.null` / `.err` constructors are universally quantified over
the kind index. Pattern matching on a `Datum k` for known `k`
prunes the constructors not inhabiting that kind. -/

inductive Datum : ColKind → Type
  | bool (b : Bool) : Datum .bool
  | int (n : Int) : Datum .int
  | null : Datum k
  | err (e : EvalError) : Datum k

/-! ## Indexed `Expr`

Each operator's slot specifies the kind of operand it accepts.
Ill-typed expressions cannot be built — `Expr.not (.lit (.int 5))`
fails to type-check because `Expr.not` requires `Expr .bool` and
`.lit (.int 5) : Expr .int`. The `WellTyped` predicate becomes
the type-system enforcing it. -/

-- `coalesce {k} (args : List (Expr k))` would fit naturally, but
-- Lean's positivity / nested-inductive checker rejects
-- `List (Expr k)` with `k` a local variable on a constructor.
-- Define `ExprList` mutually as the kind-indexed companion. This
-- is the standard Lean 4 idiom for variadic constructors on
-- indexed inductives (`Lean.Expr` uses the same pattern).
mutual
  inductive Expr : ColKind → Type
    | lit {k : ColKind} (d : Datum k) : Expr k
    | not  : Expr .bool → Expr .bool
    | plus : Expr .int → Expr .int → Expr .int
    | minus : Expr .int → Expr .int → Expr .int
    | times : Expr .int → Expr .int → Expr .int
    | divide : Expr .int → Expr .int → Expr .int
    | eq {k : ColKind} : Expr k → Expr k → Expr .bool
    | lt {k : ColKind} : Expr k → Expr k → Expr .bool
    | andN (args : ExprList .bool) : Expr .bool
    | orN  (args : ExprList .bool) : Expr .bool
    | ifThen {k : ColKind} (c : Expr .bool) (t e : Expr k) : Expr k
    | coalesce {k : ColKind} (args : ExprList k) : Expr k

  inductive ExprList : ColKind → Type
    | nil {k : ColKind} : ExprList k
    | cons {k : ColKind} : Expr k → ExprList k → ExprList k
end

/-! ## Primitive evaluators

No catch-all `_ , _ => .null` arms — Lean's exhaustivity checker
sees that on `Datum .bool × Datum .bool` the only inhabitants are
`(.bool _ | .null | .err _) × (.bool _ | .null | .err _)`, so the
nine-cell truth table is the complete case split. Same for
`Datum .int × Datum .int`. -/

def evalNot : Datum .bool → Datum .bool
  | .bool b => .bool (!b)
  | .null   => .null
  | .err e  => .err e

def evalAnd : Datum .bool → Datum .bool → Datum .bool
  | .bool false, _         => .bool false
  | _, .bool false         => .bool false
  | .err e, _              => .err e
  | _, .err e              => .err e
  | .null, _               => .null
  | _, .null               => .null
  | .bool true, .bool true => .bool true

def evalOr : Datum .bool → Datum .bool → Datum .bool
  | .bool true, _            => .bool true
  | _, .bool true            => .bool true
  | .err e, _                => .err e
  | _, .err e                => .err e
  | .null, _                 => .null
  | _, .null                 => .null
  | .bool false, .bool false => .bool false

def evalPlus : Datum .int → Datum .int → Datum .int
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => .int (n + m)

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

def evalDivide : Datum .int → Datum .int → Datum .int
  | .err e, _      => .err e
  | _, .err e      => .err e
  | .null, _       => .null
  | _, .null       => .null
  | .int n, .int m => if m = 0 then .err .divisionByZero else .int (n / m)

/-- Equality on a single kind. Returns a `Datum .bool`. The kind
parameter is shared across both operands by construction — the
ill-typed mix `evalEq (.bool true) (.int 5)` simply doesn't type. -/
def evalEq {k : ColKind} : Datum k → Datum k → Datum .bool
  | .err e, _        => .err e
  | _, .err e        => .err e
  | .null, _         => .null
  | _, .null         => .null
  | .bool x, .bool y => .bool (decide (x = y))
  | .int n,  .int m  => .bool (decide (n = m))

def evalLt {k : ColKind} : Datum k → Datum k → Datum .bool
  | .err e, _        => .err e
  | _, .err e        => .err e
  | .null, _         => .null
  | _, .null         => .null
  | .bool x, .bool y => .bool (decide (x < y))
  | .int n,  .int m  => .bool (decide (n < m))

def evalIfThen {k : ColKind} : Datum .bool → Datum k → Datum k → Datum k
  | .bool true,  dt, _  => dt
  | .bool false, _,  de => de
  | .null,       _,  _  => .null
  | .err e,      _,  _  => .err e

/-! ## Variadic primitives -/

def evalAndN : List (Datum .bool) → Datum .bool
  | []        => .bool true
  | d :: rest => evalAnd d (evalAndN rest)

def evalOrN : List (Datum .bool) → Datum .bool
  | []        => .bool false
  | d :: rest => evalOr d (evalOrN rest)

/-- `coalesce` on the indexed model. All operands share kind `k`,
so the type-mismatch handling that plagued the untyped model is
gone. Two-pass shape mirrors `Mz/PrimEval.lean`. -/
def Datum.isNullB {k : ColKind} : Datum k → Bool
  | .null => true
  | _     => false

def Coalesce.firstConcrete {k : ColKind} : List (Datum k) → Option (Datum k)
  | []           => none
  | .bool b :: _ => some (.bool b)
  | .int n :: _  => some (.int n)
  | _ :: rest    => Coalesce.firstConcrete rest

def Coalesce.residue {k : ColKind} : List (Datum k) → Datum k
  | []              => .null
  | .null :: _      => .null
  | .err e :: rest  =>
    if rest.any Datum.isNullB then .null else .err e
  | _ :: rest       => Coalesce.residue rest

def evalCoalesce {k : ColKind} (ds : List (Datum k)) : Datum k :=
  match Coalesce.firstConcrete ds with
  | some d => d
  | none   => Coalesce.residue ds

/-! ## Big-step evaluator

No environment for now (no `.col`). Closed expressions only. -/

mutual
  def eval : {k : ColKind} → Expr k → Datum k
    | _, .lit d        => d
    | _, .not a        => evalNot (eval a)
    | _, .plus a b     => evalPlus (eval a) (eval b)
    | _, .minus a b    => evalMinus (eval a) (eval b)
    | _, .times a b    => evalTimes (eval a) (eval b)
    | _, .divide a b   => evalDivide (eval a) (eval b)
    | _, .eq a b       => evalEq (eval a) (eval b)
    | _, .lt a b       => evalLt (eval a) (eval b)
    | _, .andN args    => evalAndN (evalList args)
    | _, .orN args     => evalOrN (evalList args)
    | _, .ifThen c t e => evalIfThen (eval c) (eval t) (eval e)
    | _, .coalesce args => evalCoalesce (evalList args)

  def evalList : {k : ColKind} → ExprList k → List (Datum k)
    | _, .nil       => []
    | _, .cons a as => eval a :: evalList as
end

/-! ## Demonstrator theorems

These are statements whose untyped counterparts in `Mz/Boolean.lean`,
`Mz/Laws.lean`, etc., carry a `¬d.IsInt` hypothesis to rule out the
type-mismatch route. Here the kind discipline rules it out
structurally; the hypothesis is gone. -/

/-- Untyped counterpart: `Mz/Boolean.lean`'s `not_not` requires
`¬d.IsInt`. Here the kind discipline forces `d : Datum .bool`, so
the `.int` route doesn't exist and the hypothesis disappears. -/
theorem not_not (d : Datum .bool) : evalNot (evalNot d) = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

/-- Untyped counterpart: `Mz/Laws.lean`'s `evalAnd_idem` requires
`¬d.IsInt`. Here, no hypothesis. -/
theorem evalAnd_idem (d : Datum .bool) : evalAnd d d = d := by
  cases d with
  | bool b => cases b <;> rfl
  | null   => rfl
  | err _  => rfl

/-- `evalAnd` commutes on its full domain (`Datum .bool × Datum .bool`)
when no `.err _` payloads disagree. The untyped counterpart adds
both a `might_error` hypothesis and a `.int`-exclusion hypothesis;
here, only the err-payload disagreement remains, and that's a
genuine cell-content concern, not a type-discipline artifact. -/
theorem evalAnd_comm_of_no_err
    (d₁ d₂ : Datum .bool)
    (h₁ : ∀ e, d₁ ≠ .err e) (h₂ : ∀ e, d₂ ≠ .err e) :
    evalAnd d₁ d₂ = evalAnd d₂ d₁ := by
  cases d₁ with
  | bool b₁ =>
    cases d₂ with
    | bool b₂ => cases b₁ <;> cases b₂ <;> rfl
    | null    => cases b₁ <;> rfl
    | err e   => exact (h₂ e rfl).elim
  | null    =>
    cases d₂ with
    | bool b₂ => cases b₂ <;> rfl
    | null    => rfl
    | err e   => exact (h₂ e rfl).elim
  | err e   => exact (h₁ e rfl).elim

/-- `evalPlus` is commutative on the int fragment when no err
payloads disagree. -/
theorem evalPlus_comm_of_no_err
    (a b : Datum .int)
    (h₁ : ∀ e, a ≠ .err e) (h₂ : ∀ e, b ≠ .err e) :
    evalPlus a b = evalPlus b a := by
  cases a with
  | int n =>
    cases b with
    | int m => simp only [evalPlus]; rw [Int.add_comm]
    | null  => rfl
    | err e => exact (h₂ e rfl).elim
  | null  =>
    cases b with
    | int _ => rfl
    | null  => rfl
    | err e => exact (h₂ e rfl).elim
  | err e => exact (h₁ e rfl).elim

/-! ## Open questions for the indexed model

The closed-expression prototype above is clean. The questions
gating whether this generalizes to the full model:

* **`.col` references and schema coupling.** Column references
  carry the schema's kind for that column: `.col i : Expr
  (sch.kinds.get i)`. This makes `Expr` indexed not only by its
  output kind but also by the *schema* it references against,
  parameterizing the inductive: `inductive Expr (sch : Schema n) :
  ColKind → Type`. The cost cascades through substitution
  (`Expr.subst` must move between schemas — every projection /
  filter operator changes the input schema), column-shift
  reasoning in `Mz/ColRefs.lean`, and `Subst.lean`'s soundness.
  This is the load-bearing question. A partial mitigation: keep
  `.col` parameterized by an explicit kind argument `.col (i : Nat)
  (k : ColKind) : Expr k` with a separate runtime
  `WellSchemed sch e` predicate that says the kind argument
  agrees with `sch.kinds.get i`. That recovers the simplification
  for the operator-side machinery and pays the schema cost only
  at the column-lookup point.

* **Datum.kind ambiguity for `.null` / `.err _`.** Polymorphic
  constructors make `Datum.kind (.null : Datum .bool) = .bool`
  but `Datum.kind (.null : Datum .int) = .int`. The kind index
  is now syntactic, attached to the constructor's *type*, not
  derived from the value. `kind_of_eval`-style soundness
  theorems become trivial (the kind is the index, period).

* **Eraseable model.** The intrinsically typed model can be
  erased to the untyped one (`erase : Datum k → Mz.Datum`,
  `erase : Expr k → Mz.Expr`). Soundness statement: erased eval
  agrees with intrinsic eval. This lets the indexed model coexist
  with the untyped one — proofs against the indexed model can be
  ported back via erasure. Worth mechanizing as the next prototype
  step.

* **Migration cost vs gain.** ~5.5 kloc of existing untyped-Expr
  proof (might_error, ColRefs, Subst, refines compositionality,
  eval_satisfies_outputType, the entire `Mz/Variadic.lean` /
  `Mz/Laws.lean` boolean fragment) restated against the indexed
  form. The variadic / law restating is largely mechanical (drop
  the `¬IsInt` hypotheses). The schema-coupling cost on `.col` /
  `Subst` is the real expense. -/

end Mz.Indexed
