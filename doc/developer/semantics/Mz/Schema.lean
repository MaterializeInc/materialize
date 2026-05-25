import Mz.StreamN
import Mathlib.Data.Vector.Defs
import Mathlib.Data.Vector.Basic

/-!
# Schema sketch

Per-column nullability and errability bits, plus a stream-level
row-error flag. The `Schema n` type is the structural counterpart
to Materialize's `RelationType` on the Rust side. The skeleton
keeps it minimal — just the bits and the propositional predicate
"this stream's records satisfy this schema" — and connects back
to the existing `StreamN.NoRowErr` predicate so the
`filter_cross_pushdown_left_strict` precondition can be discharged
from a schema fact.

This module is a sketch.
The proofs that ride on schema (`coalesce(a, b) = a` when `a` not
nullable, `is_null(a) = false` folding, optimizer rules that
require non-null join keys) are not yet mechanized; they are the
natural next step now that the structure is in place.
-/

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

/-- Schema for an `n`-arity stream:

* `cols` — per-column metadata.
* `rowErrFree` — the stream-level claim that no record carries
  positive `err_diff`. Modeled as a `Bool` flag so the schema can
  be combined / refined by the optimizer; the matching propositional
  fact lives in `StreamRecordN.Satisfies`.

A schema with `rowErrFree = false` makes no claim about
`err_diff`; the `Satisfies` predicate vacuously accepts records
with any `err_diff` in that case. -/
structure Schema (n : Nat) where
  cols : List.Vector ColSchema n
  rowErrFree : Bool
  deriving Inhabited

namespace Schema

/-- The information-free schema: every column nullable and errable,
no row-error claim. The starting point for any analysis. -/
def free (n : Nat) : Schema n :=
  { cols := List.Vector.replicate n { nullable := true, errable := true }
    rowErrFree := false }

/-- The schema certifies no cell-level errors. -/
def cellErrFree (sch : Schema n) : Prop :=
  ∀ i : Fin n, ¬(sch.cols.get i).errable

/-- The schema certifies no `null` values. -/
def notNullable (sch : Schema n) : Prop :=
  ∀ i : Fin n, ¬(sch.cols.get i).nullable

end Schema

/-- A row satisfies a schema iff each cell's value lies in the
declared set: no `.null` where the column is non-nullable, no
`.err _` where the column is non-errable. -/
def RowSatisfies {n : Nat} (sch : Schema n) (row : RowN n) : Prop :=
  ∀ i : Fin n,
    ((sch.cols.get i).nullable = false → row.get i ≠ .null)
    ∧ ((sch.cols.get i).errable = false → ¬(row.get i).IsErr)

/-- A stream record satisfies a schema iff its row does and the
row-level err claim is honored. -/
def StreamRecordN.Satisfies {n : Nat}
    (sch : Schema n) (rec : StreamRecordN n) : Prop :=
  RowSatisfies sch rec.row
  ∧ (sch.rowErrFree = true → rec.err_diff = 0)

namespace StreamN

variable {n : Nat}

/-- A stream satisfies a schema iff every record does. -/
def Satisfies (sch : Schema n) (s : StreamN n) : Prop :=
  ∀ rec ∈ s, rec.Satisfies sch

/-- Bridge: a stream that satisfies a schema with
`rowErrFree = true` is `NoRowErr`. Lets
`filter_cross_pushdown_left_strict` cite a schema fact rather than
the bare predicate. -/
theorem NoRowErr_of_satisfies_rowErrFree
    {sch : Schema n} {s : StreamN n}
    (hsch : sch.rowErrFree = true) (hsat : s.Satisfies sch) :
    s.NoRowErr := by
  intro rec hrec
  exact (hsat rec hrec).2 hsch

end StreamN

end Mz
