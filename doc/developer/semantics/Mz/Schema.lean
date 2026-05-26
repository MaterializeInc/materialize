import Mathlib.Data.Vector.Defs
import Mathlib.Data.Vector.Basic

/-!
# Schema (types only)

Type-level schema definitions reused by the indexed model in
`Mz/Indexed/`. The actual satisfaction predicates and indexed
schema-driven theorems live in `Mz/Indexed/Schema.lean` and
`Mz/Indexed/OutputType.lean`.

This file is import-only — no values that depend on the indexed
`Datum` / `Expr` / `Collection`. The GADT migration moved every
predicate that touches the value layer to `Mz/Indexed/*`. -/

namespace Mz

/-- Per-column metadata.

* `nullable` — this column may hold `Datum.null`.
* `errable`  — this column may hold `Datum.err _`.

Both default to `true` (no information). An optimizer-supplied
analyzer may tighten either bit; a runtime that does not yet
propagate the analysis treats both as `true`. -/
structure ColSchema where
  nullable : Bool
  errable : Bool
  deriving DecidableEq, Inhabited

/-- SQL type a column may hold (or expression may produce).

* `.bool` — boolean fragment (`.bool _`, plus `.null` / `.err _`).
* `.int` — integer fragment (`.int _`, plus `.null` / `.err _`).
* `.top` — unconstrained: any type (`.null` / `.err _` literals
  alone, untyped columns, expressions whose type depends on data).

`.top` is permissive: it is compatible with any expected type. A
column whose schema type is `.bool` is usable only in slots
expecting `.bool`. With the GADT migration, `Expr` is parameterized
by `ColType` and ill-typed expressions are unconstructible — the
`WellTyped` predicate of earlier iterations is now structural. -/
inductive ColType
  | bool
  | int
  | top
  deriving DecidableEq, Inhabited

/-- Schema for an `n`-arity collection:

* `cols` — per-column metadata (nullable / errable bits).
* `types` — per-column SQL type. Defaults to `.top` (unconstrained);
  an analyzer that tracks SQL types tightens to `.bool` or `.int`
  per column. After the GADT migration, the indexed `Expr`,
  `Update`, and `Collection` all carry the schema at the type
  level. -/
structure Schema (n : Nat) where
  cols : List.Vector ColSchema n
  types : List.Vector ColType n
  rowErrFree : Bool
  deriving Inhabited

namespace Schema

/-- The information-free schema: every column nullable and errable,
types unconstrained, no row-error claim. -/
def free (n : Nat) : Schema n :=
  { cols := List.Vector.replicate n { nullable := true, errable := true }
    types := List.Vector.replicate n ColType.top
    rowErrFree := false }

/-- Collection-wide projection of the per-column `errable` bits:
every column claims no `.err _` cells. -/
def cellErrFree {n : Nat} (sch : Schema n) : Prop :=
  ∀ i : Fin n, (sch.cols.get i).errable = false

/-- Concatenate two schemas. The output's `cols` is the append of
the inputs' `cols`; `rowErrFree` is preserved iff both inputs are.
This is the schema produced by `Collection.cross`. -/
def append {n m : Nat} (a : Schema n) (b : Schema m) : Schema (n + m) :=
  { cols := a.cols ++ b.cols
    types := a.types ++ b.types
    rowErrFree := a.rowErrFree && b.rowErrFree }

end Schema

/-- Cell-scoped errors raised by `Datum`-level operations. -/
inductive EvalError
  | divisionByZero
  | overflow
  deriving DecidableEq, Inhabited

end Mz
