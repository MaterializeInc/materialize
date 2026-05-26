import Mz.Eval

/-!
# `LegalEval`: non-deterministic relational semantics

Sketch of a SQL-faithful relational eval. `LegalEval env e d` reads
"`d` is an admissible outcome of evaluating `e` under `env`, under
*some* legal SQL evaluation order."

Each `Expr` denotes a *set* of `Datum`s rather than a single value.
The deterministic `eval : Env → Expr → Datum` produces *one* legal
outcome (the soundness theorem `legal_of_eval`); other admissible
outcomes correspond to other legal evaluation orders.

## Why

SQL leaves evaluation order unspecified outside `CASE`, `AND`/`OR`
short-circuit, and `COALESCE`. Two conforming implementations can
disagree on whether the same expression errs:

* `(x + 1) - 1` and `x + (1 - 1)` over bounded `Int` near `MAX_INT`:
  one errs, one doesn't; SQL admits either evaluator.
* `(.err e₁) + (.err e₂)`: which payload reaches the row depends on
  evaluation order. The deterministic `evalPlus` picks the left
  bias; either is admissible under SQL.
* `FALSE AND (1/0)`: lazy short-circuit gives `.bool false`, eager
  evaluation gives `.err divByZero`; both admissible.

A relation `LegalEval` captures all admissible outcomes; equivalence
relations on relational sets — set equality, set inclusion (no
spurious errors), reverse inclusion (PG posture) — give the optimizer
the relations it needs to reason about rewrites that touch errors.

## Scope of this sketch

Foundation only. Scalar `Expr`; no collection lift. Variadic `.andN`
/ `.orN` / `.coalesce` are deterministic (single legal outcome via
the existing `eval`); the SQL-permitted reorderings that admit
short-circuit `.bool false` *and* `.err _` from the same operand
list are deferred — they need a per-permutation inference rule
that's mechanically painful and not yet motivated by a forcing
rewrite.

Binary arithmetic / comparison / `.not` are non-deterministic in
the err-payload axis: when both inputs are `.err _`, *either*
payload is admissible. The deterministic `evalPlus` picks the
left bias; `LegalEval` admits both.

Motivating theorem: `legal_plus_err_either` — `LegalEval env
(.plus (.lit (.err e₁)) (.lit (.err e₂)))` admits both `.err e₁`
*and* `.err e₂`. This is the canonical case where the relational
form recovers an equivalence the deterministic eval loses (strict
equality on the deterministic output picks one payload; relational
equivalence admits the symmetric form).

## Inference rules

Each rule encodes one admissible-outcome derivation. The
deterministic `eval` is one strategy; other strategies (right-
eager, lazy, reorder) appear as separate constructors. -/

namespace Mz

inductive LegalEval (env : Env) : Expr → Datum → Prop where
  | lit (d : Datum) : LegalEval env (.lit d) d
  | col (i : Nat) : LegalEval env (.col i) (Env.get env i)
  | notC {a : Expr} {da : Datum} :
      LegalEval env a da → LegalEval env (.not a) (evalNot da)
  | ifTrue {c t e : Expr} {dt : Datum} :
      LegalEval env c (.bool true) → LegalEval env t dt
      → LegalEval env (.ifThen c t e) dt
  | ifFalse {c t e : Expr} {de : Datum} :
      LegalEval env c (.bool false) → LegalEval env e de
      → LegalEval env (.ifThen c t e) de
  | ifNull {c t e : Expr} :
      LegalEval env c .null → LegalEval env (.ifThen c t e) .null
  | ifInt {c t e : Expr} {n : Int} :
      LegalEval env c (.int n) → LegalEval env (.ifThen c t e) .null
  | ifErr {c t e : Expr} {er : EvalError} :
      LegalEval env c (.err er) → LegalEval env (.ifThen c t e) (.err er)
  | plusOk {a b : Expr} {da db : Datum} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.plus a b) (evalPlus da db)
  | plusErrL {a b : Expr} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.plus a b) (.err er)
  | plusErrR {a b : Expr} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.plus a b) (.err er)

  | minusOk {a b : Expr} {da db : Datum} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.minus a b) (evalMinus da db)
  | minusErrL {a b : Expr} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.minus a b) (.err er)
  | minusErrR {a b : Expr} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.minus a b) (.err er)

  | timesOk {a b : Expr} {da db : Datum} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.times a b) (evalTimes da db)
  | timesErrL {a b : Expr} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.times a b) (.err er)
  | timesErrR {a b : Expr} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.times a b) (.err er)

  | divideOk {a b : Expr} {da db : Datum} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.divide a b) (evalDivide da db)
  | divideErrL {a b : Expr} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.divide a b) (.err er)
  | divideErrR {a b : Expr} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.divide a b) (.err er)

  | eqOk {a b : Expr} {da db : Datum} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.eq a b) (evalEq da db)
  | eqErrL {a b : Expr} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.eq a b) (.err er)
  | eqErrR {a b : Expr} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.eq a b) (.err er)

  | ltOk {a b : Expr} {da db : Datum} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.lt a b) (evalLt da db)
  | ltErrL {a b : Expr} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.lt a b) (.err er)
  | ltErrR {a b : Expr} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.lt a b) (.err er)
  | andND {args : List Expr} :
      LegalEval env (.andN args) (eval env (.andN args))
  | orND {args : List Expr} :
      LegalEval env (.orN args)  (eval env (.orN args))
  | coalesceD {args : List Expr} :
      LegalEval env (.coalesce args) (eval env (.coalesce args))

/-! ## Soundness: deterministic `eval` is a legal outcome

Every datum the deterministic evaluator produces is admissible
under `LegalEval`. The proof is structural recursion on `Expr`,
applying the matching `LegalEval` constructor at each node. -/

theorem legal_of_eval (env : Env) :
    ∀ (e : Expr), LegalEval env e (eval env e)
  | .lit d         => by simp only [eval]; exact LegalEval.lit d
  | .col i         => by simp only [eval]; exact LegalEval.col i
  | .not a         => by
      have iha := legal_of_eval env a
      simp only [eval]
      exact LegalEval.notC iha
  | .ifThen c t e  => by
      have ihc := legal_of_eval env c
      simp only [eval]
      cases h : eval env c with
      | bool b =>
        rw [h] at ihc
        cases b with
        | true =>
          have iht := legal_of_eval env t
          exact LegalEval.ifTrue ihc iht
        | false =>
          have ihe := legal_of_eval env e
          exact LegalEval.ifFalse ihc ihe
      | int _ =>
        rw [h] at ihc
        exact LegalEval.ifInt ihc
      | null =>
        rw [h] at ihc
        exact LegalEval.ifNull ihc
      | err _ =>
        rw [h] at ihc
        exact LegalEval.ifErr ihc
  | .andN _        => LegalEval.andND
  | .orN _         => LegalEval.orND
  | .coalesce _    => LegalEval.coalesceD
  | .plus a b      => by
      have iha := legal_of_eval env a
      have ihb := legal_of_eval env b
      simp only [eval]
      exact LegalEval.plusOk iha ihb
  | .minus a b     => by
      have iha := legal_of_eval env a
      have ihb := legal_of_eval env b
      simp only [eval]
      exact LegalEval.minusOk iha ihb
  | .times a b     => by
      have iha := legal_of_eval env a
      have ihb := legal_of_eval env b
      simp only [eval]
      exact LegalEval.timesOk iha ihb
  | .divide a b    => by
      have iha := legal_of_eval env a
      have ihb := legal_of_eval env b
      simp only [eval]
      exact LegalEval.divideOk iha ihb
  | .eq a b        => by
      have iha := legal_of_eval env a
      have ihb := legal_of_eval env b
      simp only [eval]
      exact LegalEval.eqOk iha ihb
  | .lt a b        => by
      have iha := legal_of_eval env a
      have ihb := legal_of_eval env b
      simp only [eval]
      exact LegalEval.ltOk iha ihb

/-! ## Motivating theorem: both err payloads admissible

For `.plus (.lit (.err e₁)) (.lit (.err e₂))`, the deterministic
`evalPlus` is left-biased and yields `.err e₁`. Under
`LegalEval`, *either* payload is admissible — `plusErrL` admits
the left, `plusErrR` admits the right.

This is the canonical case where the relational form recovers an
admissibility the deterministic eval loses: SQL leaves it
unspecified which operand of `+` is evaluated first, so an
implementation that evaluated right-first would surface `e₂`.
Strict equality on the deterministic output picks one payload;
the relational form admits the symmetric outcome. -/

theorem legal_plus_err_either (env : Env) (e₁ e₂ : EvalError) :
    LegalEval env (.plus (.lit (.err e₁)) (.lit (.err e₂))) (.err e₁)
    ∧ LegalEval env (.plus (.lit (.err e₁)) (.lit (.err e₂))) (.err e₂) := by
  refine ⟨?_, ?_⟩
  · -- Left payload via `plusErrL` from the literal-err on the left.
    exact LegalEval.plusErrL (LegalEval.lit (.err e₁))
  · -- Right payload via `plusErrR` from the literal-err on the right.
    exact LegalEval.plusErrR (LegalEval.lit (.err e₂))

/-! ## Equivalence relations on `Expr`

Three candidate equivalences induced by `LegalEval`:

* `LegalEquiv e₁ e₂` — set equality on admissible outcomes.
* `LegalSubsume e₁ e₂` — every outcome of `e₁` is also an outcome
  of `e₂` (no spurious errors when rewriting `e₂ → e₁`).
* `LegalSubsumeDual e₁ e₂` — reverse; the rewrite may add outcomes
  (PG-ish posture).

`LegalEquiv` is strictly the strongest. `LegalSubsume` is what
the optimizer cites for "removed an err"; `LegalSubsumeDual` for
"added an err under a permissible reorder". -/

def LegalEquiv (e₁ e₂ : Expr) : Prop :=
  ∀ env d, LegalEval env e₁ d ↔ LegalEval env e₂ d

def LegalSubsume (e₁ e₂ : Expr) : Prop :=
  ∀ env d, LegalEval env e₁ d → LegalEval env e₂ d

theorem LegalEquiv.refl (e : Expr) : LegalEquiv e e :=
  fun _ _ => Iff.rfl

theorem LegalEquiv.symm {e₁ e₂ : Expr} (h : LegalEquiv e₁ e₂) :
    LegalEquiv e₂ e₁ :=
  fun env d => (h env d).symm

theorem LegalEquiv.trans {e₁ e₂ e₃ : Expr}
    (h₁₂ : LegalEquiv e₁ e₂) (h₂₃ : LegalEquiv e₂ e₃) :
    LegalEquiv e₁ e₃ :=
  fun env d => Iff.trans (h₁₂ env d) (h₂₃ env d)

theorem LegalSubsume.refl (e : Expr) : LegalSubsume e e :=
  fun _ _ h => h

theorem LegalSubsume.trans {e₁ e₂ e₃ : Expr}
    (h₁₂ : LegalSubsume e₁ e₂) (h₂₃ : LegalSubsume e₂ e₃) :
    LegalSubsume e₁ e₃ :=
  fun env d h => h₂₃ env d (h₁₂ env d h)

/-! ## Motivating theorem: err-side commutativity of `.plus`

The deterministic `evalPlus` is left-biased on err / err
(`evalPlus (.err e₁) (.err e₂) = .err e₁`). Under `LegalEval`,
both payloads are admissible (`legal_plus_err_either` above), and
err-side commutativity falls out: an `.err er` outcome of
`.plus a b` derived via `plusErrL` corresponds to `.err er` of
`.plus b a` via `plusErrR`, and vice versa.

This is the half of full commutativity that the relational form
recovers cleanly. The data-side half (non-err inputs commute via
primitive arithmetic commutativity) needs a separate
`evalPlus_comm_of_no_err` lemma and is left as a follow-up.

The theorem demonstrates the core gain of the relational form:
the deterministic eval picks one payload (the left), losing the
symmetry; the relational form admits both and trivially closes
the commutativity obligation on the err-side outcomes. -/

theorem plus_comm_legal_errL_to_errR
    {env : Env} {a b : Expr} {er : EvalError}
    (h : LegalEval env a (.err er)) :
    LegalEval env (.plus b a) (.err er) :=
  LegalEval.plusErrR h

theorem plus_comm_legal_errR_to_errL
    {env : Env} {a b : Expr} {er : EvalError}
    (h : LegalEval env b (.err er)) :
    LegalEval env (.plus b a) (.err er) :=
  LegalEval.plusErrL h

end Mz
