import Mz.Expr
import Mz.PrimEval

/-!
# Big-step evaluator

`eval (env : Env sch) (e : Expr sch k) : Datum k`. Schema-indexed.
The schema enforces:

* Column references have correct kinds (`.col i : Expr sch
  (sch.types.get i)`).
* Operators take operands of the kind they expect (no
  type-mismatch routing).

The environment is a typed lookup function:
`Env sch := (i : Fin n) → Datum (sch.types.get i)`. Each cell
returns a `Datum` whose kind agrees with the schema's declared
type for that column.

## Laziness

`.ifThen c t e` dispatches inline on `eval env c` and only
evaluates the selected branch. Eager dispatch through a
`Datum`-level `evalIfThen` primitive would compute both `t` and
`e` regardless — wasteful, and under `Datum.err _` semantics
risks producing an err in the discarded branch.

The variadic constructors `.andN` / `.orN` / `.coalesce` walk
`ExprList` directly through structurally lazy mutual partners
(`evalAndN_lazy` / `evalOrN_lazy` / `evalCoalesce_lazy`). Each
short-circuits on its absorbing element:

* `evalAndN_lazy` stops on the first `.bool false` arg —
  subsequent args are not evaluated.
* `evalOrN_lazy` stops on the first `.bool true` arg.
* `evalCoalesce_lazy` stops on the first concrete arg
  (`.bool _` / `.int _`); residue helpers track `null > err`
  for the no-concrete case (`evalCoalesce_null_residue` after a
  null head, `evalCoalesce_err_residue` after an err head).

The eager list-mediated forms (`evalAndN` / `evalOrN` /
`evalCoalesce` on `List (Datum k)`) remain in `Mz/PrimEval.lean`
as the algebraic-laws layer. Bridge lemmas
`evalAndN_lazy_eq_eager`, `evalOrN_lazy_eq_eager`, and
`evalCoalesce_lazy_eq_eager` discharge the lazy form to the
eager one — downstream proofs that already reason about
`evalAndN (evalList env args)` continue to apply via a single
rewrite. `evalList` itself is retained as a mutual partner —
`Mz/Subst.lean`'s `evalList_subst` still consumes it. -/

namespace Mz


/-- A typed environment for evaluating expressions over schema
`sch`. Cell `i` carries a `Datum` of the kind the schema declares
for column `i`. -/
def Env {n : Nat} (sch : Schema n) : Type :=
  (i : Fin n) → Datum (sch.types.get i)

/-! ## Lazy-coalesce step helpers

Step functions for the lazy coalesce mutual partners. Each takes
the evaluated head plus thunks for the recursive cases; the
thunk only forces when the head dictates recursion, which keeps
the lazy structure intact. Extraction from the mutual block lets
the inner match iota-reduce normally — Lean's compiled mutual
auxiliaries don't reduce a polymorphic-discriminant match under
`cases` on the discriminant. -/

/-- Step for `evalCoalesce_lazy`: dispatch on the head. -/
def coalesceLazyStep {k : ColType} (head : Datum k)
    (kNull : Unit → Datum k) (kErr : EvalError → Datum k) : Datum k :=
  match head with
  | .bool b => .bool b
  | .int n  => .int n
  | .null   => kNull ()
  | .err e  => kErr e

/-- Step for `evalCoalesce_null_residue`: dispatch on the head;
all non-concrete heads (null or err) recurse into `kRec`. -/
def coalesceNullStep {k : ColType} (head : Datum k)
    (kRec : Unit → Datum k) : Datum k :=
  match head with
  | .bool b => .bool b
  | .int n  => .int n
  | _       => kRec ()

/-- Step for `evalCoalesce_err_residue`: dispatch on the head;
a `.null` head crosses into the null-residue branch while a
later err keeps the tracked first err (closed over by `kErr`). -/
def coalesceErrStep {k : ColType} (head : Datum k)
    (kNull : Unit → Datum k) (kErr : Unit → Datum k) : Datum k :=
  match head with
  | .bool b => .bool b
  | .int n  => .int n
  | .null   => kNull ()
  | .err _  => kErr ()

mutual
  /-- Big-step evaluation. Mutual with `evalList` for proofs that
  reason about the list-of-datums view, and with the lazy
  variadic partners (`evalAndN_lazy`, `evalOrN_lazy`,
  `evalCoalesce_lazy`) that drive the structurally lazy
  dispatch. -/
  def eval {n : Nat} {sch : Schema n} :
      {k : ColType} → Env sch → Expr sch k → Datum k
    | _, _, .lit d        => d
    | _, env, .col i      => env i
    | _, env, .not a      => evalNot (eval env a)
    | _, env, .plus a b   => evalPlus (eval env a) (eval env b)
    | _, env, .minus a b  => evalMinus (eval env a) (eval env b)
    | _, env, .times a b  => evalTimes (eval env a) (eval env b)
    | _, env, .divide a b => evalDivide (eval env a) (eval env b)
    | _, env, .eq a b     => evalEq (eval env a) (eval env b)
    | _, env, .lt a b     => evalLt (eval env a) (eval env b)
    | _, env, .andN args  => evalAndN_lazy env args
    | _, env, .orN args   => evalOrN_lazy env args
    | _, env, .ifThen c t e =>
      match eval env c with
      | .bool true  => eval env t
      | .bool false => eval env e
      | .null       => .null
      | .err err    => .err err
    | _, env, .coalesce args => evalCoalesce_lazy env args

  /-- Evaluate an `ExprList` to a `List (Datum k)`. Kept as a
  mutual partner for proofs (`Mz/Subst.lean`'s `evalList_subst`)
  and for the eager-form bridge lemmas. -/
  def evalList {n : Nat} {sch : Schema n} :
      {k : ColType} → Env sch → ExprList sch k → List (Datum k)
    | _, _, .nil         => []
    | _, env, .cons a as => eval env a :: evalList env as

  /-- Lazy variadic AND. Walks `args` left to right; the first
  `.bool false` short-circuits — subsequent args are not
  evaluated. Other heads (`.bool true`, `.null`, `.err _`) fall
  to `evalAnd` against the recursive tail, preserving the
  `FALSE > ERROR > NULL > TRUE` absorption hierarchy. -/
  def evalAndN_lazy {n : Nat} {sch : Schema n} (env : Env sch) :
      ExprList sch .bool → Datum .bool
    | .nil       => .bool true
    | .cons a as =>
      match eval env a with
      | .bool false => .bool false
      | d           => evalAnd d (evalAndN_lazy env as)

  /-- Lazy variadic OR. Mirror of `evalAndN_lazy` with `.bool
  true` as the absorbing element. -/
  def evalOrN_lazy {n : Nat} {sch : Schema n} (env : Env sch) :
      ExprList sch .bool → Datum .bool
    | .nil       => .bool false
    | .cons a as =>
      match eval env a with
      | .bool true => .bool true
      | d          => evalOr d (evalOrN_lazy env as)

  /-- Lazy coalesce. Stops at the first concrete arg. On a null
  head, delegates to `evalCoalesce_null_residue` (tail scan
  retains `.null`). On an err head, delegates to
  `evalCoalesce_err_residue` (tail scan tracks the first err
  but a later `.null` wins per the `null > err` residue rule).

  The inner dispatch on the head's value lives in the
  step-helpers (`coalesceLazyStep`, `coalesceNullStep`,
  `coalesceErrStep`) below the mutual block. Extraction is a
  proof-engineering choice: the head match has polymorphic `k`,
  and Lean's compiled mutual auxiliaries don't iota-reduce a
  polymorphic-discriminant match under `cases`; lifting the
  match out of the mutual block sidesteps that. -/
  def evalCoalesce_lazy {n : Nat} {sch : Schema n} {k : ColType}
      (env : Env sch) : ExprList sch k → Datum k
    | .nil       => .null
    | .cons a as =>
      coalesceLazyStep (eval env a)
        (fun _ => evalCoalesce_null_residue env as)
        (fun e => evalCoalesce_err_residue env e as)

  /-- After a null head: scan for first concrete; later errs
  don't change the tracked residue (null wins). -/
  def evalCoalesce_null_residue {n : Nat} {sch : Schema n} {k : ColType}
      (env : Env sch) : ExprList sch k → Datum k
    | .nil       => .null
    | .cons a as =>
      coalesceNullStep (eval env a) (fun _ => evalCoalesce_null_residue env as)

  /-- After a first-err head: scan for first concrete; a later
  null beats the tracked err; later errs keep the first. -/
  def evalCoalesce_err_residue {n : Nat} {sch : Schema n} {k : ColType}
      (env : Env sch) (firstErr : EvalError) : ExprList sch k → Datum k
    | .nil       => .err firstErr
    | .cons a as =>
      coalesceErrStep (eval env a)
        (fun _ => evalCoalesce_null_residue env as)
        (fun _ => evalCoalesce_err_residue env firstErr as)
end

/-! ## Per-constructor equation lemmas

Mutual-block compilation makes one-step `unfold eval` expose
nested `eval env a` occurrences too, which downstream `show` /
`change` clauses fight. The per-constructor lemmas below state
the immediate reduction step in a form downstream proofs can
`rw` with surgical precision. -/

theorem eval_coalesce {n : Nat} {sch : Schema n} {k : ColType}
    (env : Env sch) (args : ExprList sch k) :
    eval env (Expr.coalesce args) = evalCoalesce_lazy env args := by
  unfold eval; rfl

theorem eval_andN {n : Nat} {sch : Schema n} (env : Env sch)
    (args : ExprList sch .bool) :
    eval env (Expr.andN args) = evalAndN_lazy env args := by
  unfold eval; rfl

theorem eval_orN {n : Nat} {sch : Schema n} (env : Env sch)
    (args : ExprList sch .bool) :
    eval env (Expr.orN args) = evalOrN_lazy env args := by
  unfold eval; rfl

/-! ## Eager-form bridges

Each bridge equates the lazy mutual partner with the
list-mediated eager form. Downstream proofs that already
reason about `evalAndN (evalList env args)` etc. continue to
apply via a single rewrite. Proofs are by structural induction
on `args` plus case analysis on the head's evaluation; the
short-circuit arms collapse via the absorbing pattern in
`evalAnd` / `evalOr` / `Coalesce.firstConcrete`. -/

theorem evalAndN_lazy_eq_eager {n : Nat} {sch : Schema n} (env : Env sch) :
    (args : ExprList sch .bool) →
    evalAndN_lazy env args = evalAndN (evalList env args)
  | .nil => by unfold evalAndN_lazy evalList; rfl
  | .cons a as => by
    unfold evalAndN_lazy evalList
    have ih := evalAndN_lazy_eq_eager env as
    cases hev : eval env a with
    | bool b =>
      cases b
      · simp [evalAndN, evalAnd]
      · simp [evalAndN, evalAnd, ih]
    | null  => simp [evalAndN, evalAnd, ih]
    | err e => simp [evalAndN, evalAnd, ih]

theorem evalOrN_lazy_eq_eager {n : Nat} {sch : Schema n} (env : Env sch) :
    (args : ExprList sch .bool) →
    evalOrN_lazy env args = evalOrN (evalList env args)
  | .nil => by unfold evalOrN_lazy evalList; rfl
  | .cons a as => by
    unfold evalOrN_lazy evalList
    have ih := evalOrN_lazy_eq_eager env as
    cases hev : eval env a with
    | bool b =>
      cases b
      · simp [evalOrN, evalOr, ih]
      · simp [evalOrN, evalOr]
    | null  => simp [evalOrN, evalOr, ih]
    | err e => simp [evalOrN, evalOr, ih]

/-- Helper: null-residue lazy = `Coalesce.residue` on the
eager list, restricted to the "head was null" entry point. -/
theorem evalCoalesce_null_residue_eq_eager {n : Nat} {sch : Schema n}
    {k : ColType} (env : Env sch) :
    (args : ExprList sch k) →
    evalCoalesce_null_residue env args = evalCoalesce (.null :: evalList env args)
  | .nil => by
    unfold evalCoalesce_null_residue evalList
    simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]
  | .cons a as => by
    unfold evalCoalesce_null_residue evalList
    have ih := evalCoalesce_null_residue_eq_eager (k := k) env as
    cases hev : eval env a with
    | bool b =>
      simp [coalesceNullStep, evalCoalesce, Coalesce.firstConcrete]
    | int n =>
      simp [coalesceNullStep, evalCoalesce, Coalesce.firstConcrete]
    | null =>
      simp [coalesceNullStep]
      rw [ih]
      simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]
    | err e =>
      simp [coalesceNullStep]
      rw [ih]
      simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]

/-- Helper: err-residue lazy = `Coalesce.residue` on the eager
list, restricted to the "head was err" entry point. -/
theorem evalCoalesce_err_residue_eq_eager {n : Nat} {sch : Schema n}
    {k : ColType} (env : Env sch) (firstErr : EvalError) :
    (args : ExprList sch k) →
    evalCoalesce_err_residue env firstErr args =
    evalCoalesce (.err firstErr :: evalList env args)
  | .nil => by
    unfold evalCoalesce_err_residue evalList
    simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]
  | .cons a as => by
    unfold evalCoalesce_err_residue evalList
    have ihN := evalCoalesce_null_residue_eq_eager (k := k) env as
    have ihE := evalCoalesce_err_residue_eq_eager (k := k) env firstErr as
    cases hev : eval env a with
    | bool b =>
      simp [coalesceErrStep, evalCoalesce, Coalesce.firstConcrete]
    | int n =>
      simp [coalesceErrStep, evalCoalesce, Coalesce.firstConcrete]
    | null =>
      simp [coalesceErrStep]
      rw [ihN]
      simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue,
            Datum.isNullB]
    | err e =>
      simp [coalesceErrStep]
      rw [ihE]
      simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue,
            Datum.isNullB]

theorem evalCoalesce_lazy_eq_eager {n : Nat} {sch : Schema n} {k : ColType}
    (env : Env sch) :
    (args : ExprList sch k) →
    evalCoalesce_lazy env args = evalCoalesce (evalList env args)
  | .nil => by
    unfold evalCoalesce_lazy evalList
    simp [evalCoalesce, Coalesce.firstConcrete, Coalesce.residue]
  | .cons a as => by
    unfold evalCoalesce_lazy evalList
    cases hev : eval env a with
    | bool b => simp [coalesceLazyStep, evalCoalesce, Coalesce.firstConcrete]
    | int n  => simp [coalesceLazyStep, evalCoalesce, Coalesce.firstConcrete]
    | null   =>
      simp [coalesceLazyStep]
      rw [evalCoalesce_null_residue_eq_eager env as]
    | err e  =>
      simp [coalesceLazyStep]
      rw [evalCoalesce_err_residue_eq_eager env e as]

end Mz
