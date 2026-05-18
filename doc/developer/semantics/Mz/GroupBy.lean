import Mz.Eval
import Mz.Bag
import Mz.Aggregate

/-!
# `GROUP BY`

Partition a relation by the value of a key expression, evaluated on
each row. The skeleton uses Lean's derived `DecidableEq Datum` to
compare keys.

## Spec divergence on `err` keys

The error-handling spec states that `Datum.err` keys form their own
group — every error is its own bucket, so distinct failures never
collapse into the same aggregate. The skeleton's `groupBy` uses
standard `DecidableEq`, which treats two `Datum.err e` values with
the same `e` as equal. A spec-faithful variant would special-case
`err` keys to always create a new group; the spec divergence is
called out here and is the natural next refinement.

The companion `aggregateBy` runs `aggStrict` per group, producing
`(key, value)` pairs.
-/

namespace Mz

/-- Insert `row` into the group keyed by `k`. If no group with key
`k` exists yet, create one; otherwise prepend `row` to the existing
group's row list. -/
private def insertInto (k : Datum) (row : Row) :
    List (Datum × Relation) → List (Datum × Relation)
  | []                  => [(k, [row])]
  | (k', rows) :: rest =>
    if k = k' then (k', row :: rows) :: rest
    else (k', rows) :: insertInto k row rest

/-- `GROUP BY keyExpr`: partition `rel` by the value of `keyExpr`
on each row. Output is a list of `(key, rows)` pairs, one per
distinct key, in encounter order. -/
def groupBy (keyExpr : Expr) (rel : Relation) : List (Datum × Relation) :=
  rel.foldr (fun row acc => insertInto (eval row keyExpr) row acc) []

/-- Aggregate per group: run `aggStrict` over each group's evaluated
column. Models the `SELECT keyExpr, SUM(valExpr) FROM rel GROUP BY
keyExpr` flow. -/
def aggregateBy
    (keyExpr valExpr : Expr) (f : Datum → Datum → Datum)
    (rel : Relation) : List (Datum × Datum) :=
  (groupBy keyExpr rel).map fun grp =>
    (grp.1, aggStrict f (grp.2.map (fun row => eval row valExpr)))

/-! ## Trivial cases -/

theorem groupBy_nil (keyExpr : Expr) :
    groupBy keyExpr [] = [] := rfl

theorem aggregateBy_nil (keyExpr valExpr : Expr) (f : Datum → Datum → Datum) :
    aggregateBy keyExpr valExpr f [] = [] := rfl

/-- A single-row relation produces exactly one group containing that
row, keyed by the row's evaluated key. -/
theorem groupBy_singleton (keyExpr : Expr) (row : Row) :
    groupBy keyExpr [row] = [(eval row keyExpr, [row])] := rfl

end Mz
