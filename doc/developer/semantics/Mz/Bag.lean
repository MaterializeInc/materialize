import Mz.Eval

/-!
# Bag semantics: filter and project

A first sliver of relational semantics on top of the per-`Row`
evaluator. A `Relation` is modeled as a `List Row` — a bag of rows
in execution order. The skeleton supports two relational
operators:

* `filterRel pred rel` keeps the rows whose predicate evaluates to
  `.bool true`. Rows that evaluate to `.bool false`, `.null`, or
  `.err` are dropped. A real implementation would route `err` rows
  to a separate error collection; the skeleton silently drops them
  so the laws can be stated without modelling the error stream yet.
* `project es rel` evaluates each expression in `es` against every
  row and emits a list of resulting rows. The output schema width
  is `es.length`.

The two laws stated here — filter idempotence and filter
commutativity — are the simplest optimizer-relevant rewrites over
the bag. Both reduce to `List.filter_filter` plus a `Bool`
identity (`and_self`, `and_comm`); the proofs lean entirely on
existing core-library lemmas. More substantial rewrites (predicate
pushdown across projection, predicate combination, etc.) are
follow-ups that this file is intended to grow into.
-/

namespace Mz

/-- A row is a positional list of bound values. Reuses `Env`. -/
abbrev Row := Env

/-- A relation is a list of rows. Bag (multiset) semantics; row
order does not matter for filter or project laws. -/
abbrev Relation := List Row

/-- Membership-style predicate evaluator used by `filterRel`. Rows
that evaluate to `.bool true` are kept; everything else (including
`.err`) is dropped. -/
@[inline] def rowPredicate (pred : Expr) (row : Row) : Bool :=
  match eval row pred with
  | .bool true => true
  | _          => false

/-- Filter a relation by a scalar predicate. -/
def filterRel (pred : Expr) (rel : Relation) : Relation :=
  rel.filter (rowPredicate pred)

/-- Project a relation through a list of scalar expressions.
Each output row has width `es.length`. -/
def project (es : List Expr) (rel : Relation) : Relation :=
  rel.map (fun row => es.map (eval row))

/-! ## Filter laws -/

theorem filterRel_idem (pred : Expr) (rel : Relation) :
    filterRel pred (filterRel pred rel) = filterRel pred rel := by
  unfold filterRel
  rw [List.filter_filter]
  congr 1
  funext row
  exact Bool.and_self _

theorem filterRel_comm (p q : Expr) (rel : Relation) :
    filterRel p (filterRel q rel) = filterRel q (filterRel p rel) := by
  unfold filterRel
  rw [List.filter_filter, List.filter_filter]
  congr 1
  funext row
  exact Bool.and_comm _ _

/-! ## Project laws -/

theorem project_length (es : List Expr) (rel : Relation) :
    (project es rel).length = rel.length := by
  unfold project
  exact List.length_map _

/-- The empty projection collapses every row to the empty row of
width zero, so the relation becomes a list of empty rows whose
length equals the input length. -/
theorem project_nil (rel : Relation) :
    project [] rel = rel.map (fun _ => []) := by
  show rel.map (fun row => ([] : List Expr).map (eval row)) = rel.map (fun _ => [])
  congr 1

end Mz
