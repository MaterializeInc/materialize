import Mz.Eval

/-!
# `LegalEval` (indexed)

Non-deterministic relational eval on indexed `Expr`. Indexed
counterpart of `Mz/Legal.lean`.

`LegalEval env e d : Prop` reads "`d : Datum k` is an admissible
outcome of evaluating `e : Expr sch k` under `env : Env sch`,
under some legal SQL evaluation order".

The deterministic `eval` produces one legal outcome
(`legal_of_eval`); other admissible outcomes correspond to other
strategies.

Scope as in the untyped version: binary err-payload
non-determinism (both `.err _` payloads admissible from
`(.err _) + (.err _)`); variadic short-circuit, data-side
commutativity, collection lift deferred. -/

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

end Mz
