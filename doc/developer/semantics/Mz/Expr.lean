import Mz.Datum
import Mz.Schema

/-!
# `Expr` — schema-indexed GADT

`Expr (sch : Schema n) : ColType → Type`. Expressions are typed
by:

* The schema `sch : Schema n` of the row they evaluate against
  (so `.col i` returns a `Datum (sch.types.get i)`).
* The output `ColType` of the expression.

Ill-typed expressions are unconstructible. The `WellTyped`
predicate is now structural; type errors at construction time
fail to type-check.

Variadics use a mutual `ExprList` to satisfy Lean's
nested-inductive restriction. -/

namespace Mz


/-! ## Schema-indexed expression GADT

Mutual with `ExprList` to support variadic constructors
(`.andN`, `.orN`, `.coalesce`). -/

mutual
  inductive Expr {n : Nat} (sch : Schema n) : ColType → Type
    | lit {k : ColType} (d : Datum k) : Expr sch k
    | col (i : Fin n) : Expr sch (sch.types.get i)
    | not  : Expr sch .bool → Expr sch .bool
    | plus : Expr sch .int → Expr sch .int → Expr sch .int
    | minus : Expr sch .int → Expr sch .int → Expr sch .int
    | times : Expr sch .int → Expr sch .int → Expr sch .int
    | divide : Expr sch .int → Expr sch .int → Expr sch .int
    | eq {k : ColType} : Expr sch k → Expr sch k → Expr sch .bool
    | lt {k : ColType} : Expr sch k → Expr sch k → Expr sch .bool
    | andN (args : ExprList sch .bool) : Expr sch .bool
    | orN  (args : ExprList sch .bool) : Expr sch .bool
    | ifThen {k : ColType} (c : Expr sch .bool) (t e : Expr sch k) : Expr sch k
    | coalesce {k : ColType} (args : ExprList sch k) : Expr sch k

  inductive ExprList {n : Nat} (sch : Schema n) : ColType → Type
    | nil {k : ColType} : ExprList sch k
    | cons {k : ColType} : Expr sch k → ExprList sch k → ExprList sch k
end

end Mz
