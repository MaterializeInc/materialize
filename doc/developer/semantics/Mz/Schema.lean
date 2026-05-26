import Mz.Collection
import Mz.MightError
import Mathlib.Data.Vector.Defs
import Mathlib.Data.Vector.Basic
import Mathlib.Data.Vector.Mem

/-!
# Schema sketch

Per-column nullability and errability bits, plus a collection-level
row-error flag. The `Schema n` type is the structural counterpart
to Materialize's `RelationType` on the Rust side. The skeleton
keeps it minimal — just the bits and the propositional predicate
"this collection's updates satisfy this schema" — and connects back
to the existing `Collection.NoRowErr` predicate so the
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

/-- SQL type kind a column may hold (or expression may produce).

* `.bool` — boolean fragment (`.bool _`, plus `.null` / `.err _`).
* `.int` — integer fragment (`.int _`, plus `.null` / `.err _`).
* `.top` — unconstrained: any kind (`.null` / `.err _` literals
  alone, untyped columns, expressions whose kind depends on data).

`.top` is permissive: it is compatible with any expected kind (the
caller doesn't know; SQL `NULL` and `ERROR` inhabit every SQL
type). A column whose schema kind is `.top` is therefore usable
in any operator slot — well-typing accepts it. A column whose
schema kind is `.bool` is usable only in slots expecting `.bool`. -/
inductive ColKind
  | bool
  | int
  | top
  deriving DecidableEq, Inhabited

/-- Schema for an `n`-arity collection:

* `cols` — per-column metadata (nullable / errable bits).
* `kinds` — per-column type kind. Defaults to `.top`
  (unconstrained); an analyzer that tracks SQL types tightens to
  `.bool` or `.int` per column.
* `rowErrFree` — the collection-level claim that no update carries
  positive `err_diff`. Modeled as a `Bool` flag so the schema can
  be combined / refined by the optimizer; the matching propositional
  fact lives in `Update.Satisfies`.

A schema with `rowErrFree = false` makes no claim about
`err_diff`; the `Satisfies` predicate vacuously accepts records
with any `err_diff` in that case. -/
structure Schema (n : Nat) where
  cols : List.Vector ColSchema n
  kinds : List.Vector ColKind n
  rowErrFree : Bool
  deriving Inhabited

namespace Schema

/-- The information-free schema: every column nullable and errable,
kinds unconstrained, no row-error claim. The starting point for
any analysis. -/
def free (n : Nat) : Schema n :=
  { cols := List.Vector.replicate n { nullable := true, errable := true }
    kinds := List.Vector.replicate n ColKind.top
    rowErrFree := false }

/-- Collection-wide projection of the per-column `errable` bits:
every column claims no `.err _` cells. The dual of the per-row
`RowSatisfies` predicate at the schema level — used by
`NoRowErr_filter` to bridge cell-err-freedom of inputs to row-err
absence after the predicate-err migration. -/
def cellErrFree (sch : Schema n) : Prop :=
  ∀ i : Fin n, (sch.cols.get i).errable = false

end Schema

/-- A row satisfies a schema iff each cell's value lies in the
declared set: no `.null` where the column is non-nullable, no
`.err _` where the column is non-errable. -/
def RowSatisfies {n : Nat} (sch : Schema n) (row : RowN n) : Prop :=
  ∀ i : Fin n,
    ((sch.cols.get i).nullable = false → row.get i ≠ .null)
    ∧ ((sch.cols.get i).errable = false → ¬(row.get i).IsErr)

/-- An update satisfies a schema iff its row does and the
row-level err claim is honored. -/
def Update.Satisfies {n : Nat}
    (sch : Schema n) (rec : Update n) : Prop :=
  RowSatisfies sch rec.row
  ∧ (sch.rowErrFree = true → rec.err_diff = 0)

namespace Collection

variable {n : Nat}

/-- A collection satisfies a schema iff every update does. -/
def Satisfies (sch : Schema n) (s : Collection n) : Prop :=
  ∀ rec ∈ s, rec.Satisfies sch

/-- Bridge: a collection that satisfies a schema with
`rowErrFree = true` is `NoRowErr`. Lets
`filter_cross_pushdown_left_strict` cite a schema fact rather than
the bare predicate. -/
theorem NoRowErr_of_satisfies_rowErrFree
    {sch : Schema n} {s : Collection n}
    (hsch : sch.rowErrFree = true) (hsat : s.Satisfies sch) :
    s.NoRowErr := by
  intro rec hrec
  exact (hsat rec hrec).2 hsch

end Collection

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
    {a b : Expr} {row : List Datum}
    (h_notnull : eval row a ≠ .null) (h_noterr : ¬(eval row a).IsErr) :
    eval row (.coalesce [a, b]) = eval row a := by
  simp only [eval, List.map_cons, List.map_nil]
  exact evalCoalesce_cons_of_concrete _ _ h_notnull h_noterr

/-! ## Schema combinators

The schema for a collection produced by an operator is built from
the input schemas. The combinators here cover the operators
currently proved in `Mz/Collection.lean`. -/

namespace Schema

/-- Concatenate two schemas. The output's `cols` is the append of
the inputs' `cols`; `rowErrFree` is preserved iff both inputs are.

This is the schema produced by `Collection.cross` and is the
structural counterpart to `Vector.append`. -/
def append {n m : Nat} (a : Schema n) (b : Schema m) : Schema (n + m) :=
  { cols := a.cols ++ b.cols
    kinds := a.kinds ++ b.kinds
    rowErrFree := a.rowErrFree && b.rowErrFree }

end Schema

namespace Collection

variable {n m : Nat}

/-- Bridge: a row that satisfies a cell-err-free schema has every
cell not-`IsErr`. Lets `might_error_sound` consume the schema fact
as its `Env.ErrFree` precondition. -/
theorem RowSatisfies.toList_ErrFree
    {sch : Schema n} {row : RowN n}
    (hsat : RowSatisfies sch row) (hcef : sch.cellErrFree) :
    Env.ErrFree row.toList := by
  intro d hd
  rw [List.Vector.mem_iff_get] at hd
  obtain ⟨i, hi⟩ := hd
  have herr := (hsat i).2 (hcef i)
  rw [← hi]; exact herr

/-- `filter` preserves `NoRowErr` when the predicate is statically
err-free against the input cell schema. Combines
`might_error_sound` (predicate produces no err on err-free env) with
the schema bridge above. -/
theorem NoRowErr_filter
    {sch : Schema n} {s : Collection n}
    (p : Expr) (hp : ¬p.might_error = true)
    (hcef : sch.cellErrFree) (hsat : s.Satisfies sch) (h_norow : NoRowErr s) :
    NoRowErr (filter p s) := by
  intro rec hrec
  unfold filter at hrec
  rw [List.mem_map] at hrec
  obtain ⟨rec0, hrec0_mem, hrec0_eq⟩ := hrec
  -- `rec` is `filterOne p rec0` for some `rec0 ∈ s`.
  have hsat0 : rec0.Satisfies sch := hsat rec0 hrec0_mem
  have h_norow0 : rec0.err_diff = 0 := h_norow rec0 hrec0_mem
  -- Predicate evaluates to a non-err Datum.
  have h_env : Env.ErrFree rec0.row.toList :=
    RowSatisfies.toList_ErrFree hsat0.1 hcef
  have h_not_err : ¬(eval rec0.row.toList p).IsErr :=
    might_error_sound p _ hp h_env
  -- Case-split on the eval result; every non-err branch leaves
  -- `err_diff` unchanged at 0.
  rw [← hrec0_eq]
  show (filterOne p rec0).err_diff = 0
  unfold filterOne
  cases h : eval rec0.row.toList p with
  | bool b =>
    cases b with
    | true => exact h_norow0
    | false => exact h_norow0
  | int _ => exact h_norow0
  | null => exact h_norow0
  | err _ =>
    rw [h] at h_not_err
    exact absurd True.intro h_not_err

/-- `cross` preserves `NoRowErr`: when both inputs are row-err-free,
the cross's `err_diff` is `0` by the bilinear formula. -/
theorem NoRowErr_cross
    {sL : Collection n} {sR : Collection m}
    (hL : NoRowErr sL) (hR : NoRowErr sR) :
    NoRowErr (cross sL sR) := by
  intro rec hrec
  -- Every update in `cross sL sR` is `crossOne recL recR` for some
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

/-- `project` preserves `NoRowErr`. `projectOne` rewrites `row` via
the expression vector but leaves `diff` and `err_diff` untouched, so
this is unconditional in the expressions. -/
theorem NoRowErr_project (es : VecN Expr m) {s : Collection n}
    (hs : NoRowErr s) :
    NoRowErr (project es s) := by
  intro rec hrec
  unfold project at hrec
  rw [List.mem_map] at hrec
  obtain ⟨rec0, hrec0_mem, hrec0_eq⟩ := hrec
  have h0 : rec0.err_diff = 0 := hs rec0 hrec0_mem
  rw [← hrec0_eq]
  show (projectOne es rec0).err_diff = 0
  simp only [projectOne]
  exact h0

/-- `unionAll` preserves `NoRowErr`. `unionAll` is list concatenation,
so the predicate composes conjunctively over the two inputs. -/
theorem NoRowErr_unionAll {a b : Collection n}
    (ha : NoRowErr a) (hb : NoRowErr b) :
    NoRowErr (unionAll a b) := by
  intro rec hrec
  unfold unionAll at hrec
  rcases List.mem_append.mp hrec with hL | hR
  · exact ha rec hL
  · exact hb rec hR

/-- `negate` preserves `NoRowErr`. `negate` flips `err_diff`'s sign;
`-0 = 0`, so a zero `err_diff` stays zero. -/
theorem NoRowErr_negate {s : Collection n} (hs : NoRowErr s) :
    NoRowErr (negate s) := by
  intro rec hrec
  unfold negate at hrec
  rw [List.mem_map] at hrec
  obtain ⟨rec0, hrec0_mem, hrec0_eq⟩ := hrec
  have h0 : rec0.err_diff = 0 := hs rec0 hrec0_mem
  rw [← hrec0_eq]
  simp only [h0, neg_zero]

end Collection

end Mz
