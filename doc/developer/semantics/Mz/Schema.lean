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

/-! ## Schema-driven rewrites

Once the schema is in place, optimizer rewrites whose soundness
depends on column metadata become discharged by a schema fact
instead of an ad-hoc hypothesis. The two recorded here are
representative; the broader set (`is_null` folding, non-null join
key elimination, output-schema propagation through `Expr.subst`)
follows the same shape. -/

/-- `coalesce(a :: rest)` collapses to `a` when `a` evaluates to a
concrete (non-`null`, non-`err`) value. Bare Datum form. -/
theorem evalCoalesce_cons_of_concrete (d : Datum) (rest : List Datum)
    (h_notnull : d ≠ .null) (h_noterr : ¬d.IsErr) :
    evalCoalesce (d :: rest) = d := by
  cases d with
  | bool _ => rfl
  | int _ => rfl
  | null => exact absurd rfl h_notnull
  | err _ => exact absurd True.intro h_noterr

/-- `Expr`-level corollary: `coalesce(a, b)` equals `a` whenever
`a` evaluates to a concrete value on `row`. Schema-rider: when the
schema certifies `a`'s output column is `nullable = false` and
`errable = false`, the hypothesis is discharged for every row
satisfying the input schema. -/
theorem eval_coalesce_pair_of_a_concrete
    {a b : Expr} {row : Row}
    (h_notnull : eval row a ≠ .null) (h_noterr : ¬(eval row a).IsErr) :
    eval row (.coalesce [a, b]) = eval row a := by
  simp only [eval, List.map_cons, List.map_nil]
  exact evalCoalesce_cons_of_concrete _ _ h_notnull h_noterr

/-! ## Schema combinators

The schema for a stream produced by an operator is built from the
input schemas. The combinators here cover the operators currently
proved in `Mz/StreamN.lean`. -/

namespace Schema

/-- Concatenate two schemas. The output's `cols` is the append of
the inputs' `cols`; `rowErrFree` is preserved iff both inputs are.

This is the schema produced by `StreamN.cross` and is the
structural counterpart to `Vector.append`. -/
def append {n m : Nat} (a : Schema n) (b : Schema m) : Schema (n + m) :=
  { cols := a.cols ++ b.cols
    rowErrFree := a.rowErrFree && b.rowErrFree }

end Schema

namespace StreamN

variable {n m : Nat}

/-- `cross` preserves `NoRowErr`: when both inputs are row-err-free,
the cross's `err_diff` is `0` by the bilinear formula. -/
theorem NoRowErr_cross
    {sL : StreamN n} {sR : StreamN m}
    (hL : NoRowErr sL) (hR : NoRowErr sR) :
    NoRowErr (cross sL sR) := by
  intro rec hrec
  -- Every record in `cross sL sR` is `crossOne recL recR` for some
  -- `recL ∈ sL`, `recR ∈ sR`. Both originals have err_diff = 0, so
  -- the bilinear formula gives 0 for the product.
  induction sL with
  | nil => cases hrec
  | cons recL sLR ih =>
    rw [cross_cons_left] at hrec
    rcases List.mem_append.mp hrec with hmap | htail
    · -- rec is crossOne recL recR for some recR ∈ sR.
      obtain ⟨recR, hrecR_mem, hrec_eq⟩ := List.mem_map.mp hmap
      have hL0 : recL.err_diff = 0 := hL recL (List.mem_cons_self)
      have hR0 : recR.err_diff = 0 := hR recR hrecR_mem
      rw [← hrec_eq]
      show (crossOne recL recR).err_diff = 0
      simp only [crossOne]
      rw [hL0, hR0]; ring
    · -- rec is in cross sLR sR.
      have hL' : NoRowErr sLR := by
        intro r hr
        exact hL r (List.mem_cons_of_mem _ hr)
      exact ih hL' htail

end StreamN

end Mz
