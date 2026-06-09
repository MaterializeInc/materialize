import Mz.Eval

/-!
# `LegalEval`

Non-deterministic relational eval on indexed `Expr`.

`LegalEval env e d : Prop` reads "`d : Datum k` is an admissible
outcome of evaluating `e : Expr sch k` under `env : Env sch`,
under some legal SQL evaluation order".

The deterministic `eval` produces one legal outcome
(`legal_of_eval`); other admissible outcomes correspond to other
strategies.

Scope: binary err-payload non-determinism (both `.err _` payloads
admissible from `(.err _) + (.err _)`); variadic short-circuit,
data-side commutativity, collection lift deferred. -/

namespace Mz


inductive LegalEval {n : Nat} {sch : Schema n}
    (env : Env sch) :
    {k : ColType} → Expr sch k → Datum k → Prop where
  | lit {k : ColType} (d : Datum k) : LegalEval env (.lit d) d
  | col (i : Fin n) : LegalEval env (.col i) (env i)
  | notC {a : Expr sch .bool} {da : Datum .bool} :
      LegalEval env a da → LegalEval env (.not a) (evalNot da)
  | ifTrue {k : ColType} {c : Expr sch .bool} {t e : Expr sch k} {dt : Datum k} :
      LegalEval env c (.bool true) → LegalEval env t dt
      → LegalEval env (.ifThen c t e) dt
  | ifFalse {k : ColType} {c : Expr sch .bool} {t e : Expr sch k} {de : Datum k} :
      LegalEval env c (.bool false) → LegalEval env e de
      → LegalEval env (.ifThen c t e) de
  | ifNull {k : ColType} {c : Expr sch .bool} {t e : Expr sch k} :
      LegalEval env c .null → LegalEval env (.ifThen c t e) .null
  | ifErr {k : ColType} {c : Expr sch .bool} {t e : Expr sch k} {er : EvalError} :
      LegalEval env c (.err er) → LegalEval env (.ifThen c t e) (.err er)
  | plusOk {a b : Expr sch .int} {da db : Datum .int} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.plus a b) (evalPlus da db)
  | plusErrL {a b : Expr sch .int} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.plus a b) (.err er)
  | plusErrR {a b : Expr sch .int} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.plus a b) (.err er)
  | minusOk {a b : Expr sch .int} {da db : Datum .int} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.minus a b) (evalMinus da db)
  | minusErrL {a b : Expr sch .int} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.minus a b) (.err er)
  | minusErrR {a b : Expr sch .int} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.minus a b) (.err er)
  | timesOk {a b : Expr sch .int} {da db : Datum .int} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.times a b) (evalTimes da db)
  | timesErrL {a b : Expr sch .int} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.times a b) (.err er)
  | timesErrR {a b : Expr sch .int} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.times a b) (.err er)
  | divideOk {a b : Expr sch .int} {da db : Datum .int} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.divide a b) (evalDivide da db)
  | divideErrL {a b : Expr sch .int} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.divide a b) (.err er)
  | divideErrR {a b : Expr sch .int} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.divide a b) (.err er)
  | eqOk {k : ColType} {a b : Expr sch k} {da db : Datum k} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.eq a b) (evalEq da db)
  | eqErrL {k : ColType} {a b : Expr sch k} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.eq a b) (.err er)
  | eqErrR {k : ColType} {a b : Expr sch k} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.eq a b) (.err er)
  | ltOk {k : ColType} {a b : Expr sch k} {da db : Datum k} :
      LegalEval env a da → LegalEval env b db
      → LegalEval env (.lt a b) (evalLt da db)
  | ltErrL {k : ColType} {a b : Expr sch k} {er : EvalError} :
      LegalEval env a (.err er) → LegalEval env (.lt a b) (.err er)
  | ltErrR {k : ColType} {a b : Expr sch k} {er : EvalError} :
      LegalEval env b (.err er) → LegalEval env (.lt a b) (.err er)
  | andND {args : ExprList sch .bool} :
      LegalEval env (.andN args) (eval env (.andN args))
  | orND {args : ExprList sch .bool} :
      LegalEval env (.orN args) (eval env (.orN args))
  | coalesceD {k : ColType} {args : ExprList sch k} :
      LegalEval env (.coalesce args) (eval env (.coalesce args))

/-! ## Soundness: deterministic `eval` is one legal outcome -/

/-- The deterministic `eval` produces a `LegalEval` outcome. Bridge
between the deterministic surface and the relational semantics:
every theorem about `eval` lifts to a witness that one particular
admissible outcome is the one `eval` produces. -/
theorem legal_of_eval {n : Nat} {sch : Schema n} (env : Env sch) :
    {k : ColType} → (e : Expr sch k) → LegalEval env e (eval env e)
  | _, .lit d        => by unfold eval; exact LegalEval.lit d
  | _, .col i        => by unfold eval; exact LegalEval.col i
  | _, .not a        => by unfold eval; exact LegalEval.notC (legal_of_eval env a)
  | _, .plus a b     => by unfold eval; exact LegalEval.plusOk (legal_of_eval env a) (legal_of_eval env b)
  | _, .minus a b    => by unfold eval; exact LegalEval.minusOk (legal_of_eval env a) (legal_of_eval env b)
  | _, .times a b    => by unfold eval; exact LegalEval.timesOk (legal_of_eval env a) (legal_of_eval env b)
  | _, .divide a b   => by unfold eval; exact LegalEval.divideOk (legal_of_eval env a) (legal_of_eval env b)
  | _, .eq a b       => by unfold eval; exact LegalEval.eqOk (legal_of_eval env a) (legal_of_eval env b)
  | _, .lt a b       => by unfold eval; exact LegalEval.ltOk (legal_of_eval env a) (legal_of_eval env b)
  | _, .ifThen c t e =>
    -- ifThen dispatches lazily on eval env c.
    match h : eval env c with
    | .bool true  =>
      have hc : LegalEval env c (.bool true) := h ▸ legal_of_eval env c
      have : eval env (.ifThen c t e) = eval env t := by
        conv_lhs => unfold eval
        rw [h]
      this ▸ LegalEval.ifTrue hc (legal_of_eval env t)
    | .bool false =>
      have hc : LegalEval env c (.bool false) := h ▸ legal_of_eval env c
      have : eval env (.ifThen c t e) = eval env e := by
        conv_lhs => unfold eval
        rw [h]
      this ▸ LegalEval.ifFalse hc (legal_of_eval env e)
    | .null        =>
      have hc : LegalEval env c .null := h ▸ legal_of_eval env c
      have : eval env (.ifThen c t e) = .null := by
        conv_lhs => unfold eval
        rw [h]
      this ▸ LegalEval.ifNull hc
    | .err er      =>
      have hc : LegalEval env c (.err er) := h ▸ legal_of_eval env c
      have : eval env (.ifThen c t e) = .err er := by
        conv_lhs => unfold eval
        rw [h]
      this ▸ LegalEval.ifErr hc
  | _, .andN args    => LegalEval.andND
  | _, .orN args     => LegalEval.orND
  | _, .coalesce args => LegalEval.coalesceD

/-! ## Motivating theorem: binary err-payload non-determinism

`(.err e₁) + (.err e₂)` admits both payloads. The deterministic
`evalPlus` picks the left bias; the relational form recovers the
symmetry that strict equality on the deterministic output loses. -/

theorem legal_plus_err_either {n : Nat} {sch : Schema n}
    (env : Env sch) (e₁ e₂ : EvalError) :
    LegalEval env (Expr.plus (.lit (.err e₁)) (.lit (.err e₂))) (.err e₁)
    ∧ LegalEval env (Expr.plus (.lit (.err e₁)) (.lit (.err e₂))) (.err e₂) := by
  refine ⟨?_, ?_⟩
  · exact LegalEval.plusErrL (LegalEval.lit (.err e₁))
  · exact LegalEval.plusErrR (LegalEval.lit (.err e₂))

/-! ## Err-side commutativity of `.plus` -/

theorem plus_comm_legal_errL_to_errR {n : Nat} {sch : Schema n}
    {env : Env sch} {a b : Expr sch .int} {er : EvalError}
    (h : LegalEval env a (.err er)) :
    LegalEval env (.plus b a) (.err er) :=
  LegalEval.plusErrR h

theorem plus_comm_legal_errR_to_errL {n : Nat} {sch : Schema n}
    {env : Env sch} {a b : Expr sch .int} {er : EvalError}
    (h : LegalEval env b (.err er)) :
    LegalEval env (.plus b a) (.err er) :=
  LegalEval.plusErrL h

/-! ## Candidate equivalence relations on `Expr`

`LegalEquiv` is set-equality on `LegalEval`-admissible outcomes;
`LegalSubsume` is set-inclusion (the "no spurious errors" posture). -/

/-- Two expressions are `LegalEquiv` iff they admit the same set
of legal outcomes. -/
def LegalEquiv {n : Nat} {sch : Schema n} {k : ColType}
    (env : Env sch) (e₁ e₂ : Expr sch k) : Prop :=
  ∀ d, LegalEval env e₁ d ↔ LegalEval env e₂ d

/-- `e₁` is subsumed by `e₂` iff every admissible outcome of `e₁`
is also admissible for `e₂`. Matches the "no spurious errors"
posture: a rewrite `e₁ → e₂` is sound if every error the rewritten
form can produce was already producible by the original. -/
def LegalSubsume {n : Nat} {sch : Schema n} {k : ColType}
    (env : Env sch) (e₁ e₂ : Expr sch k) : Prop :=
  ∀ d, LegalEval env e₁ d → LegalEval env e₂ d

theorem LegalEquiv.refl {n : Nat} {sch : Schema n} {k : ColType}
    (env : Env sch) (e : Expr sch k) : LegalEquiv env e e :=
  fun _ => Iff.rfl

theorem LegalEquiv.symm {n : Nat} {sch : Schema n} {k : ColType}
    {env : Env sch} {e₁ e₂ : Expr sch k}
    (h : LegalEquiv env e₁ e₂) : LegalEquiv env e₂ e₁ :=
  fun d => (h d).symm

theorem LegalEquiv.trans {n : Nat} {sch : Schema n} {k : ColType}
    {env : Env sch} {e₁ e₂ e₃ : Expr sch k}
    (h₁ : LegalEquiv env e₁ e₂) (h₂ : LegalEquiv env e₂ e₃) :
    LegalEquiv env e₁ e₃ :=
  fun d => Iff.trans (h₁ d) (h₂ d)

end Mz
