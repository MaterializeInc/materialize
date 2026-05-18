import Mz.Eval
import Mz.Bag
import Mz.Aggregate

/-!
# `GROUP BY`

Partition a relation by the value of a key expression, evaluated on
each row. Two grouping primitives:

* `groupBy` uses Lean's derived `DecidableEq Datum`, which treats
  two `Datum.err e` values with the same payload as equal. Two
  rows whose key both evaluate to the same `err e` therefore end
  up in the same group.
* `groupByErrDistinct` uses the spec-faithful `Datum.groupKeyEq`
  predicate that returns `false` whenever either side is `.err`.
  Every error row lands in its own singleton group, matching the
  design doc's rule that distinct failures must not collapse into
  one aggregate.

The companion `aggregateBy` (and `aggregateByErrDistinct`) runs
`aggStrict` per group, producing `(key, value)` pairs.
-/

namespace Mz

/-- Spec-faithful group-key equivalence. Two err keys are never
equal — every error is its own group, regardless of payload. On
non-err keys, falls through to structural `DecidableEq`. -/
@[inline] def Datum.groupKeyEq : Datum → Datum → Bool
  | .err _, _      => false
  | _,      .err _ => false
  | a,      b      => a = b

/-- Insert `row` into the group keyed by `k`. If no group with key
`k` exists yet, create one; otherwise prepend `row` to the existing
group's row list. -/
private def insertInto (k : Datum) (row : Row) :
    List (Datum × Relation) → List (Datum × Relation)
  | []                  => [(k, [row])]
  | (k', rows) :: rest =>
    if k = k' then (k', row :: rows) :: rest
    else (k', rows) :: insertInto k row rest

/-- Err-distinct insert. Uses `Datum.groupKeyEq` instead of `=`, so
`err` keys never coalesce with any existing group. -/
private def insertIntoDistinct (k : Datum) (row : Row) :
    List (Datum × Relation) → List (Datum × Relation)
  | []                  => [(k, [row])]
  | (k', rows) :: rest =>
    if Datum.groupKeyEq k k' then (k', row :: rows) :: rest
    else (k', rows) :: insertIntoDistinct k row rest

/-- `GROUP BY keyExpr`: partition `rel` by the value of `keyExpr`
on each row. Output is a list of `(key, rows)` pairs, one per
distinct key, in encounter order. -/
def groupBy (keyExpr : Expr) (rel : Relation) : List (Datum × Relation) :=
  rel.foldr (fun row acc => insertInto (eval row keyExpr) row acc) []

/-- Spec-faithful `GROUP BY` that never merges `err` keys. -/
def groupByErrDistinct (keyExpr : Expr) (rel : Relation) :
    List (Datum × Relation) :=
  rel.foldr (fun row acc => insertIntoDistinct (eval row keyExpr) row acc) []

/-- Aggregate per group: run `aggStrict` over each group's evaluated
column. Models the `SELECT keyExpr, SUM(valExpr) FROM rel GROUP BY
keyExpr` flow. -/
def aggregateBy
    (keyExpr valExpr : Expr) (f : Datum → Datum → Datum)
    (rel : Relation) : List (Datum × Datum) :=
  (groupBy keyExpr rel).map fun grp =>
    (grp.1, aggStrict f (grp.2.map (fun row => eval row valExpr)))

/-- Err-distinct aggregate: per-group `aggStrict`, but err keys are
never merged. Since `aggStrict` of a singleton err-keyed group is
just `aggStrict` on that group's values, the practical effect is
that each err key produces its own row in the output. -/
def aggregateByErrDistinct
    (keyExpr valExpr : Expr) (f : Datum → Datum → Datum)
    (rel : Relation) : List (Datum × Datum) :=
  (groupByErrDistinct keyExpr rel).map fun grp =>
    (grp.1, aggStrict f (grp.2.map (fun row => eval row valExpr)))

/-! ## Trivial cases -/

theorem groupBy_nil (keyExpr : Expr) :
    groupBy keyExpr [] = [] := rfl

theorem groupByErrDistinct_nil (keyExpr : Expr) :
    groupByErrDistinct keyExpr [] = [] := rfl

theorem aggregateBy_nil (keyExpr valExpr : Expr) (f : Datum → Datum → Datum) :
    aggregateBy keyExpr valExpr f [] = [] := rfl

theorem aggregateByErrDistinct_nil
    (keyExpr valExpr : Expr) (f : Datum → Datum → Datum) :
    aggregateByErrDistinct keyExpr valExpr f [] = [] := rfl

/-- A single-row relation produces exactly one group containing that
row, keyed by the row's evaluated key. -/
theorem groupBy_singleton (keyExpr : Expr) (row : Row) :
    groupBy keyExpr [row] = [(eval row keyExpr, [row])] := rfl

theorem groupByErrDistinct_singleton (keyExpr : Expr) (row : Row) :
    groupByErrDistinct keyExpr [row] = [(eval row keyExpr, [row])] := rfl

/-! ## Err-distinct laws -/

/-- `Datum.groupKeyEq` is `false` whenever the left side is an err,
regardless of the right side. -/
@[simp] theorem Datum.groupKeyEq_err_left (e : EvalError) (d : Datum) :
    Datum.groupKeyEq (.err e) d = false := by
  cases d <;> rfl

/-- Inserting an err-keyed row into any group list appends the row
as a fresh singleton group at the end, since `groupKeyEq` never
matches an err key. -/
theorem insertIntoDistinct_err
    (e : EvalError) (row : Row) (groups : List (Datum × Relation)) :
    insertIntoDistinct (.err e) row groups =
      groups ++ [(.err e, [row])] := by
  induction groups with
  | nil => rfl
  | cons head tl ih =>
    obtain ⟨k', rows⟩ := head
    simp [insertIntoDistinct, Datum.groupKeyEq_err_left, ih]

/-- When every row's key evaluates to an err, the err-distinct
grouping produces exactly one group per row — no merging happens
across rows. -/
theorem groupByErrDistinct_length_of_all_err
    (keyExpr : Expr) (rel : Relation)
    (h : ∀ row ∈ rel, ∃ e, eval row keyExpr = .err e) :
    (groupByErrDistinct keyExpr rel).length = rel.length := by
  induction rel with
  | nil => rfl
  | cons head tl ih =>
    have hHead : ∃ e, eval head keyExpr = .err e :=
      h head (List.mem_cons_self)
    have hTl : ∀ row ∈ tl, ∃ e, eval row keyExpr = .err e :=
      fun row hMem => h row (List.mem_cons_of_mem _ hMem)
    obtain ⟨e, heq⟩ := hHead
    have ihApp := ih hTl
    show (insertIntoDistinct (eval head keyExpr) head
            (groupByErrDistinct keyExpr tl)).length
        = (head :: tl).length
    rw [heq, insertIntoDistinct_err, List.length_append, ihApp]
    simp [List.length_cons]

/-! ## Cardinality

The sum of group sizes equals the input relation's length: no row
is lost, no row is duplicated. Holds for both `groupBy` (which may
merge err keys) and `groupByErrDistinct` (which never does).

The proofs use an explicit `totalRows` function as the metric,
which avoids syntactic mismatches between `Prod.snd ∘ List.length`
spellings that throw off `omega`. -/

/-- Sum of `rows.length` over every group. -/
def totalRows : List (Datum × Relation) → Nat
  | []              => 0
  | (_, rs) :: rest => rs.length + totalRows rest

/-- `insertInto` adds exactly one row to the total across all
groups. The new row either joins an existing group or creates a
fresh singleton group — either way, the group-size sum grows by
one. -/
private theorem totalRows_insertInto
    (k : Datum) (row : Row) (groups : List (Datum × Relation)) :
    totalRows (insertInto k row groups) = totalRows groups + 1 := by
  induction groups with
  | nil => rfl
  | cons head tl ih =>
    obtain ⟨k', rows⟩ := head
    by_cases hEq : k = k'
    · subst hEq
      show totalRows (if k = k then (k, row :: rows) :: tl
                       else (k, rows) :: insertInto k row tl)
          = totalRows ((k, rows) :: tl) + 1
      rw [if_pos rfl]
      show (row :: rows).length + totalRows tl
          = rows.length + totalRows tl + 1
      simp [List.length_cons]
      omega
    · show totalRows (if k = k' then (k', row :: rows) :: tl
                       else (k', rows) :: insertInto k row tl)
          = totalRows ((k', rows) :: tl) + 1
      rw [if_neg hEq]
      show rows.length + totalRows (insertInto k row tl)
          = rows.length + totalRows tl + 1
      rw [ih]
      omega

/-- Analogue for the err-distinct insert: same row-count
bookkeeping, different match predicate. -/
private theorem totalRows_insertIntoDistinct
    (k : Datum) (row : Row) (groups : List (Datum × Relation)) :
    totalRows (insertIntoDistinct k row groups) = totalRows groups + 1 := by
  induction groups with
  | nil => rfl
  | cons head tl ih =>
    obtain ⟨k', rows⟩ := head
    by_cases hEq : Datum.groupKeyEq k k' = true
    · show totalRows (if Datum.groupKeyEq k k'
                       then (k', row :: rows) :: tl
                       else (k', rows) :: insertIntoDistinct k row tl)
          = totalRows ((k', rows) :: tl) + 1
      rw [if_pos hEq]
      show (row :: rows).length + totalRows tl
          = rows.length + totalRows tl + 1
      simp [List.length_cons]
      omega
    · show totalRows (if Datum.groupKeyEq k k'
                       then (k', row :: rows) :: tl
                       else (k', rows) :: insertIntoDistinct k row tl)
          = totalRows ((k', rows) :: tl) + 1
      rw [if_neg hEq]
      show rows.length + totalRows (insertIntoDistinct k row tl)
          = rows.length + totalRows tl + 1
      rw [ih]
      omega

/-- Cardinality theorem: every row in the input appears in exactly
one group's row list. -/
theorem totalRows_groupBy (keyExpr : Expr) (rel : Relation) :
    totalRows (groupBy keyExpr rel) = rel.length := by
  induction rel with
  | nil => rfl
  | cons head tl ih =>
    show totalRows (insertInto (eval head keyExpr) head
            (groupBy keyExpr tl)) = (head :: tl).length
    rw [totalRows_insertInto, ih, List.length_cons]

/-- Cardinality theorem for the err-distinct variant. -/
theorem totalRows_groupByErrDistinct (keyExpr : Expr) (rel : Relation) :
    totalRows (groupByErrDistinct keyExpr rel) = rel.length := by
  induction rel with
  | nil => rfl
  | cons head tl ih =>
    show totalRows (insertIntoDistinct (eval head keyExpr) head
            (groupByErrDistinct keyExpr tl)) = (head :: tl).length
    rw [totalRows_insertIntoDistinct, ih, List.length_cons]

end Mz
