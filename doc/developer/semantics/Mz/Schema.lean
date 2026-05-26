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

/-- Block 1: cast on `List.Vector` preserves the underlying `.val`
(the cast only changes the type-level length proof). Used to factor
`cast` through `Subtype.ext`. -/
theorem _root_.List.Vector.cast_val {α : Type u} {n p : Nat}
    (h : n = p) (v : List.Vector α n) :
    (h ▸ v : List.Vector α p).val = v.val := by
  cases h
  rfl

/-- Block 2: cast of `Vector` append at `(n+m)+k` equals the
unfolded append at `n+(m+k)`. Underlying-lists agreement via
`List.append_assoc`; cast factors via `Subtype.ext` +
`Vector.cast_val`. -/
theorem _root_.List.Vector.cast_eq_append_assoc {α : Type u} {n m k : Nat}
    (a : List.Vector α n) (b : List.Vector α m) (c : List.Vector α k)
    (h : n + m + k = n + (m + k)) :
    (h ▸ ((a ++ b) ++ c) : List.Vector α (n + (m + k))) = a ++ (b ++ c) := by
  apply Subtype.ext
  rw [List.Vector.cast_val]
  rcases a with ⟨la, _⟩
  rcases b with ⟨lb, _⟩
  rcases c with ⟨lc, _⟩
  show (la ++ lb) ++ lc = la ++ (lb ++ lc)
  exact List.append_assoc la lb lc

/-- Block 3: cast on `Schema.mk` distributes through the constructor.
The `cols` / `types` fields carry the cast; `rowErrFree` is invariant.
Free-variable `cases h` reduces the cast to identity. -/
theorem cast_mk {n p : Nat} (h : n = p)
    (cols : List.Vector ColSchema n) (types : List.Vector ColType n) (b : Bool) :
    (h ▸ ({ cols := cols, types := types, rowErrFree := b } : Schema n)
      : Schema p)
    = { cols := h ▸ cols, types := h ▸ types, rowErrFree := b } := by
  cases h
  rfl

/-- Heterogeneous associativity for `Schema.append`. Composes
`Vector.cast_eq_append_assoc`, `Schema.cast_mk`, and `Bool.and_assoc`
under the `eqRec_heq` HEq↔cast trick. Arity-cast scaffolding that
`Collection.cross_assoc` consumes. -/
theorem append_assoc_heq {n m k : Nat}
    (a : Schema n) (b : Schema m) (c : Schema k) :
    HEq (Schema.append (Schema.append a b) c)
        (Schema.append a (Schema.append b c)) := by
  have hnat : n + m + k = n + (m + k) := Nat.add_assoc n m k
  -- HEq via cast trip: HEq LHS (hnat ▸ LHS) by eqRec_heq.symm, then
  -- (hnat ▸ LHS) = RHS as Eq via cast_mk + cast_eq_append_assoc.
  apply HEq.trans (HEq.symm (eqRec_heq hnat _))
  apply heq_of_eq
  show (hnat ▸ ({ cols := (a.cols ++ b.cols) ++ c.cols,
                  types := (a.types ++ b.types) ++ c.types,
                  rowErrFree := (a.rowErrFree && b.rowErrFree) && c.rowErrFree }
                : Schema (n + m + k))
        : Schema (n + (m + k)))
       = { cols := a.cols ++ (b.cols ++ c.cols),
           types := a.types ++ (b.types ++ c.types),
           rowErrFree := a.rowErrFree && (b.rowErrFree && c.rowErrFree) }
  rw [Schema.cast_mk hnat]
  congr 1
  · exact List.Vector.cast_eq_append_assoc a.cols b.cols c.cols hnat
  · exact List.Vector.cast_eq_append_assoc a.types b.types c.types hnat
  · exact Bool.and_assoc a.rowErrFree b.rowErrFree c.rowErrFree

end Schema

/-- Cell-scoped errors raised by `Datum`-level operations. -/
inductive EvalError
  | divisionByZero
  | overflow
  deriving DecidableEq, Inhabited

end Mz
