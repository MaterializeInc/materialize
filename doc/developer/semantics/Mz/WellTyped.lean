import Mz.Schema
import Mz.OutputType
import Mathlib.Data.Vector.Defs
import Mathlib.Data.Vector.Basic

/-!
# `WellTyped`: structural type-correctness predicate for `Expr`

`Expr.WellTyped sch e` says `e` passes a plan-time type check
against schema `sch`: every operator's operand expressions
produce kinds (bool / int / top) that the operator can consume.

PG performs this check in the planner; expressions that fail
type-check never reach execution. Materialize is similar — the
optimizer rejects ill-typed expressions before lowering. The
skeleton's evaluator is *total* on `Expr` and silently routes
type-mismatched arithmetic operands to `.null`, but rewrites that
depend on the planner's type assumptions need an explicit
predicate witnessing those assumptions.

`WellTyped` is that predicate. The soundness theorem
`eval_kind_of_wellTyped` connects it to the evaluator: a well-typed
expression produces a `Datum` whose kind matches the expression's
declared output kind (modulo the universal `.null` / `.err _`
inhabitants, captured by `.top`).

## Scope

This module lands the predicate and the kind-soundness theorem.
The follow-on work — refining `Expr.outputType`'s `nullable` bit
to be precise on well-typed inputs (the precision direction
flagged in `review.md`) — rides on top and is left as a separate
step. The conservative `nullable := true` for arithmetic that
currently appears in `outputType` is sound because it admits the
type-mismatch case; with `WellTyped`, the optimizer can derive a
tighter `nullable := input-nullable-OR` rule, but that requires
re-stating `outputType` to take the well-typedness hypothesis. -/

namespace Mz

/-! ## Kind of a `Datum` -/

/-- The kind a concrete `Datum` exhibits. `.null` and `.err _`
inhabit every kind; reported as `.top`. -/
def Datum.kind : Datum → ColKind
  | .bool _ => .bool
  | .int _  => .int
  | .null   => .top
  | .err _  => .top

/-! ## Kind compatibility

`ColKind.compatible actual expected = true` reads "an operand of
kind `actual` can be used where the operator expects `expected`".

* `.top` accepts everything on either side: an operand of unknown
  kind could be the right shape; an operator that doesn't care
  about kind (variadic mixtures) accepts any operand.
* Concrete-concrete: must match exactly.

This is reflexive and symmetric (a Bool relation, decidable). -/
def ColKind.compatible : ColKind → ColKind → Bool
  | .top,  _    => true
  | _,    .top  => true
  | .bool, .bool => true
  | .int,  .int  => true
  | _,    _    => false

@[simp] theorem ColKind.compatible_top_left (k : ColKind) :
    ColKind.compatible .top k = true := by cases k <;> rfl

@[simp] theorem ColKind.compatible_top_right (k : ColKind) :
    ColKind.compatible k .top = true := by cases k <;> rfl

theorem ColKind.compatible_refl (k : ColKind) :
    ColKind.compatible k k = true := by cases k <;> rfl

/-! ## Output kind of an `Expr`

What kind a well-typed `Expr` produces. Variadic and conditional
forms whose output kind depends on operand structure default to
`.top`; tighter rules ride on a future per-operand `kindJoin`. -/

def Expr.outputKind {n : Nat} (sch : Schema n) : Expr → ColKind
  | .lit (.bool _) => .bool
  | .lit (.int _)  => .int
  | .lit .null     => .top
  | .lit (.err _)  => .top
  | .col i =>
    if h : i < n then sch.kinds.get ⟨i, h⟩ else .top
  | .not _    => .bool
  | .plus _ _  => .int
  | .minus _ _ => .int
  | .times _ _ => .int
  | .divide _ _ => .int
  | .eq _ _   => .bool
  | .lt _ _   => .bool
  | .ifThen _ _ _ => .top
  | .andN _   => .bool
  | .orN _    => .bool
  | .coalesce _ => .top

/-! ## `WellTyped` predicate

Structural recursion on `Expr`. Each operator checks that its
operands' output kinds are compatible with what the operator
consumes.

* `.not`, `.andN`, `.orN`: bool-consuming.
* `.plus`, `.minus`, `.times`, `.divide`: int-consuming.
* `.eq`, `.lt`: same-kind on both operands (compatible).
* `.ifThen`: cond bool-consuming; arms unconstrained.
* `.coalesce`: operands unconstrained (any kind admissible — null
  and err are universal). -/

mutual

def Expr.WellTyped {n : Nat} (sch : Schema n) : Expr → Prop
  | .lit _ => True
  | .col i => i < n
  | .not a =>
    Expr.WellTyped sch a
    ∧ ColKind.compatible (Expr.outputKind sch a) .bool = true
  | .ifThen c t e =>
    Expr.WellTyped sch c ∧ Expr.WellTyped sch t ∧ Expr.WellTyped sch e
    ∧ ColKind.compatible (Expr.outputKind sch c) .bool = true
  | .andN args =>
    Expr.WellTypedArgsAllBool sch args
  | .orN args =>
    Expr.WellTypedArgsAllBool sch args
  | .coalesce args =>
    Expr.WellTypedArgs sch args
  | .plus a b =>
    Expr.WellTyped sch a ∧ Expr.WellTyped sch b
    ∧ ColKind.compatible (Expr.outputKind sch a) .int = true
    ∧ ColKind.compatible (Expr.outputKind sch b) .int = true
  | .minus a b =>
    Expr.WellTyped sch a ∧ Expr.WellTyped sch b
    ∧ ColKind.compatible (Expr.outputKind sch a) .int = true
    ∧ ColKind.compatible (Expr.outputKind sch b) .int = true
  | .times a b =>
    Expr.WellTyped sch a ∧ Expr.WellTyped sch b
    ∧ ColKind.compatible (Expr.outputKind sch a) .int = true
    ∧ ColKind.compatible (Expr.outputKind sch b) .int = true
  | .divide a b =>
    Expr.WellTyped sch a ∧ Expr.WellTyped sch b
    ∧ ColKind.compatible (Expr.outputKind sch a) .int = true
    ∧ ColKind.compatible (Expr.outputKind sch b) .int = true
  | .eq a b =>
    Expr.WellTyped sch a ∧ Expr.WellTyped sch b
    ∧ ColKind.compatible (Expr.outputKind sch a) (Expr.outputKind sch b) = true
  | .lt a b =>
    Expr.WellTyped sch a ∧ Expr.WellTyped sch b
    ∧ ColKind.compatible (Expr.outputKind sch a) (Expr.outputKind sch b) = true

/-- Mutual: every operand of a variadic well-typed structurally. -/
def Expr.WellTypedArgs {n : Nat} (sch : Schema n) : List Expr → Prop
  | [] => True
  | a :: rest => Expr.WellTyped sch a ∧ Expr.WellTypedArgs sch rest

/-- Mutual: every operand well-typed AND bool-consumable. -/
def Expr.WellTypedArgsAllBool {n : Nat} (sch : Schema n) : List Expr → Prop
  | [] => True
  | a :: rest =>
    Expr.WellTyped sch a
    ∧ ColKind.compatible (Expr.outputKind sch a) .bool = true
    ∧ Expr.WellTypedArgsAllBool sch rest

end

/-! ## Kind-soundness

A row "kind-satisfies" a schema iff each cell's kind is compatible
with the schema's declared kind for that column. (Cells holding
`.null` / `.err _` always pass — they have `.top` kind, which is
compatible with any expected.) -/

def RowSatisfiesKind {n : Nat} (sch : Schema n) (row : RowN n) : Prop :=
  ∀ i : Fin n,
    ColKind.compatible (row.get i).kind (sch.kinds.get i) = true

/-! ## Primitive codomain lemmas

Each non-`.col` constructor's `outputKind` matches the codomain
of its primitive evaluator. The proofs are case-bashes on the
input `Datum` constructors; each branch yields a result whose
`kind` is compatible with the expected `outputKind`. -/

private theorem kind_evalNot (a : Datum) :
    ColKind.compatible (evalNot a).kind .bool = true := by
  cases a <;> rfl

private theorem kind_evalPlus (a b : Datum) :
    ColKind.compatible (evalPlus a b).kind .int = true := by
  cases a <;> cases b <;> rfl

private theorem kind_evalMinus (a b : Datum) :
    ColKind.compatible (evalMinus a b).kind .int = true := by
  cases a <;> cases b <;> rfl

private theorem kind_evalTimes (a b : Datum) :
    ColKind.compatible (evalTimes a b).kind .int = true := by
  cases a <;> cases b <;> rfl

private theorem kind_evalDivide (a b : Datum) :
    ColKind.compatible (evalDivide a b).kind .int = true := by
  cases a with
  | err _ => rfl
  | null => cases b <;> rfl
  | bool _ => cases b <;> rfl
  | int n =>
    cases b with
    | err _ => rfl
    | null => rfl
    | bool _ => rfl
    | int m =>
      show ColKind.compatible
          (if m = 0 then Datum.err EvalError.divisionByZero
           else Datum.int (n / m)).kind .int = true
      by_cases h : m = 0
      · rw [if_pos h]; rfl
      · rw [if_neg h]; rfl

private theorem kind_evalEq (a b : Datum) :
    ColKind.compatible (evalEq a b).kind .bool = true := by
  cases a <;> cases b <;> rfl

private theorem kind_evalLt (a b : Datum) :
    ColKind.compatible (evalLt a b).kind .bool = true := by
  cases a <;> cases b <;> rfl

/-- `evalAnd`'s output kind is always compatible with `.bool`, no
matter the inputs. The codomain of `evalAnd` is bounded by
`{.bool _, .null, .err _}` after the tightening that routes
non-boolean operands to `.null`. -/
private theorem kind_evalAnd (a b : Datum) :
    ColKind.compatible (evalAnd a b).kind .bool = true := by
  cases a with
  | bool b₁ =>
    cases b with
    | bool b₂ => cases b₁ <;> cases b₂ <;> rfl
    | int _  => cases b₁ <;> rfl
    | null   => cases b₁ <;> rfl
    | err _  => cases b₁ <;> rfl
  | int _ =>
    cases b with
    | bool b₂ => cases b₂ <;> rfl
    | int _  => rfl
    | null   => rfl
    | err _  => rfl
  | null =>
    cases b with
    | bool b₂ => cases b₂ <;> rfl
    | int _  => rfl
    | null   => rfl
    | err _  => rfl
  | err _ =>
    cases b with
    | bool b₂ => cases b₂ <;> rfl
    | int _  => rfl
    | null   => rfl
    | err _  => rfl

private theorem kind_evalOr (a b : Datum) :
    ColKind.compatible (evalOr a b).kind .bool = true := by
  cases a with
  | bool b₁ =>
    cases b with
    | bool b₂ => cases b₁ <;> cases b₂ <;> rfl
    | int _  => cases b₁ <;> rfl
    | null   => cases b₁ <;> rfl
    | err _  => cases b₁ <;> rfl
  | int _ =>
    cases b with
    | bool b₂ => cases b₂ <;> rfl
    | int _  => rfl
    | null   => rfl
    | err _  => rfl
  | null =>
    cases b with
    | bool b₂ => cases b₂ <;> rfl
    | int _  => rfl
    | null   => rfl
    | err _  => rfl
  | err _ =>
    cases b with
    | bool b₂ => cases b₂ <;> rfl
    | int _  => rfl
    | null   => rfl
    | err _  => rfl

private theorem kind_evalAndN (args : List Datum) :
    ColKind.compatible (evalAndN args).kind .bool = true := by
  induction args with
  | nil => rfl
  | cons hd tl _ih =>
    show ColKind.compatible (evalAnd hd (evalAndN tl)).kind .bool = true
    exact kind_evalAnd hd (evalAndN tl)

private theorem kind_evalOrN (args : List Datum) :
    ColKind.compatible (evalOrN args).kind .bool = true := by
  induction args with
  | nil => rfl
  | cons hd tl _ih =>
    show ColKind.compatible (evalOr hd (evalOrN tl)).kind .bool = true
    exact kind_evalOr hd (evalOrN tl)

/-! ## Kind soundness

Under `RowSatisfiesKind`, every `Expr` evaluates to a `Datum`
whose kind is compatible with the expression's `outputKind`.

The proof is structural recursion on `Expr`. `.lit` and `.col`
close from the schema; `.col`'s OOB fallback yields `.null` whose
kind `.top` is compatible with any `outputKind`. The non-foundational
constructors close via the primitive-codomain lemmas above —
their codomains are bounded by `{outputKind, .top}` independent of
operand structure.

No `WellTyped` hypothesis required: `outputKind` is conservative
enough on the variadic / conditional cases (`.ifThen`, `.coalesce`
both fall through to `.top`) that the theorem closes unconditionally
modulo `RowSatisfiesKind`. The `WellTyped` predicate's value is in
unlocking *precision* refinements of `outputType` (the
`nullable := OR-of-inputs` tightening), not in this kind-soundness
direction. -/

/-- Any datum's kind is compatible with `.top`. -/
private theorem kind_compat_top (d : Datum) :
    ColKind.compatible d.kind .top = true := by
  cases d <;> rfl

theorem Expr.kind_of_eval {n : Nat} (sch : Schema n) (row : RowN n)
    (hrk : RowSatisfiesKind sch row) :
    ∀ (e : Expr),
      ColKind.compatible (eval row.toList e).kind
        (Expr.outputKind sch e) = true
  | .lit d => by
    simp only [eval, Expr.outputKind]
    cases d <;> rfl
  | .col i => by
    simp only [eval, Expr.outputKind]
    by_cases h : i < n
    · rw [Env_get_row_toList_lt row i h, dif_pos h]
      exact hrk ⟨i, h⟩
    · rw [Env_get_row_toList_ge row i h, dif_neg h]
      rfl
  | .not a => by
    simp only [eval, Expr.outputKind]
    exact kind_evalNot (eval row.toList a)
  | .ifThen c t e => by
    simp only [eval, Expr.outputKind]
    exact kind_compat_top _
  | .andN args => by
    simp only [eval, Expr.outputKind]
    exact kind_evalAndN _
  | .orN args => by
    simp only [eval, Expr.outputKind]
    exact kind_evalOrN _
  | .coalesce args => by
    simp only [eval, Expr.outputKind]
    exact kind_compat_top _
  | .plus a b => by
    simp only [eval, Expr.outputKind]
    exact kind_evalPlus _ _
  | .minus a b => by
    simp only [eval, Expr.outputKind]
    exact kind_evalMinus _ _
  | .times a b => by
    simp only [eval, Expr.outputKind]
    exact kind_evalTimes _ _
  | .divide a b => by
    simp only [eval, Expr.outputKind]
    exact kind_evalDivide _ _
  | .eq a b => by
    simp only [eval, Expr.outputKind]
    exact kind_evalEq _ _
  | .lt a b => by
    simp only [eval, Expr.outputKind]
    exact kind_evalLt _ _

/-! ## Open follow-ups

* `Expr.kind_of_eval`. Under `RowSatisfiesKind sch row`, the eval
  result's kind is compatible with `Expr.outputKind sch e`. Each
  non-`.col` arm closes by observing that the matching primitive
  evaluator's codomain falls within
  `{outputKind, .top}` — `evalPlus` returns `.int _ / .null /
  .err _`, whose kinds are `{.int, .top, .top}`, all compatible
  with `.int`. The `.col` arm uses `RowSatisfiesKind`. The proof
  is structural recursion; the variadic and conditional cases
  default to `.top` and close trivially.

* Precision direction on `Expr.outputType` under `WellTyped`.
  The current `outputType (.plus a b) = { nullable := true,
  errable := OR-of-inputs }` carries `nullable := true` because
  the four-valued evaluator routes type-mismatched operands to
  `.null`. Under `WellTyped`, type-mismatch is ruled out: both
  operands' output kinds are compatible with `.int`, so their
  eval results are in `{.int _, .null, .err _}` and `evalPlus`
  never takes the catch-all `.null` route. The tighter rule
  `nullable := outputType(a).nullable ∨ outputType(b).nullable`
  is sound on well-typed inputs. Same for `.minus / .times / .eq
  / .lt`; `.divide` is always errable for divide-by-zero, but
  the same `nullable`-tightening applies.

* Schema-typing soundness in both directions. The existing
  `eval_satisfies_outputType` proves "output is at most as bad as
  `outputType` claims". A converse precision claim — "output is
  exactly as good as input-schema-and-expression-structure imply"
  — gates `is_null_non_nullable` and `non_null_join_key` rewrites.
  Open follow-up; rides on the precision direction above. -/

end Mz
