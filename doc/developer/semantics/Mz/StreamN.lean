import Mz.Eval
import Mathlib.Data.Vector.Defs
import Mathlib.Data.Vector.Basic
import Mathlib.Tactic.Ring

/-!
# Indexed-arity stream: pilot for arity-tracking stream records

The plain `Mz/Stream.lean` carries rows as `List Datum` with no arity
constraint. Filter, project, cross, etc. preserve / change row width
purely by convention; the type system gives no feedback when an
operator accidentally changes arity.

This module pilots the alternative encoding in which arity lives in
the type:

* `RowN n` is `List.Vector Datum n` — a length-`n` list of `Datum`s.
* `StreamRecordN n` carries a `row : RowN n` plus the same two `Int`
  diffs as the untyped stream record.
* `StreamN n` is `List (StreamRecordN n)`.

Operators expose arity in their signatures:

* `filter : Expr → StreamN n → StreamN n` — same arity.
* `project : List.Vector Expr m → StreamN n → StreamN m` — output
  arity is dictated by the length of the expression vector.
* `cross : StreamN n → StreamN m → StreamN (n + m)` — output arity
  is the sum.

The pilot includes the same reduction lemmas as `Mz/Stream.lean` plus
a test of indexed-arity ergonomics: `cross_assoc`. The two evaluation
orders of a triple cross live in *different* `StreamN` types
(`(n + m) + k` vs `n + (m + k)`); the law is stated through
`List.Vector.congr` (the arity-cast helper) rather than as plain
equality. Whether this dependent shape is easier or harder to prove
against than the untyped `Stream.lean` form is the question this
pilot exists to answer.

The current `Mz/Eval.lean` `eval` function expects `Env := List Datum`,
so the operators escape through `.toList` when handing rows to `eval`.
A future iteration can lift `eval` to `Vector` directly, or expose a
`Fin n`-indexed column accessor; either is additive on top of this
file. -/

namespace Mz

/-- Alias for Mathlib's `List.Vector` so the rest of this file reads
without the `List.` prefix everywhere. -/
abbrev VecN (α : Type) (n : Nat) := List.Vector α n

/-- A length-`n` row. -/
abbrev RowN (n : Nat) := VecN Datum n

/-- A stream record at arity `n`: row plus two `Int` multiplicities. -/
structure StreamRecordN (n : Nat) where
  row : RowN n
  diff : Int
  err_diff : Int
  deriving Inhabited

/-- A stream at arity `n`. -/
abbrev StreamN (n : Nat) := List (StreamRecordN n)

namespace StreamN

variable {n m k : Nat}

/-! ## Filter

Per-record filter follows the design doc rule. The row's arity is
preserved by construction — filter only zeroes multiplicities or
migrates them between data and err sides. -/

/-- Per-record filter action. -/
@[inline] def filterOne (pred : Expr) (rec : StreamRecordN n) : StreamRecordN n :=
  match eval rec.row.toList pred with
  | .bool true => rec
  | .err _     =>
    { row := rec.row, diff := 0, err_diff := rec.err_diff + rec.diff }
  | _          =>
    { row := rec.row, diff := 0, err_diff := rec.err_diff }

/-- Filter a stream. -/
def filter (pred : Expr) (s : StreamN n) : StreamN n :=
  s.map (filterOne pred)

@[simp] theorem filter_nil (pred : Expr) :
    filter pred ([] : StreamN n) = [] := rfl

theorem filter_cons (pred : Expr) (rec : StreamRecordN n) (s : StreamN n) :
    filter pred (rec :: s) = filterOne pred rec :: filter pred s := rfl

theorem filter_append (pred : Expr) (s t : StreamN n) :
    filter pred (s ++ t) = filter pred s ++ filter pred t := by
  unfold filter
  exact List.map_append

/-! ## Project

Project produces a stream at arity `m` from a length-`m` expression
vector. The output row is the pointwise application of `eval` over
the original row through the expression vector. -/

/-- Per-record projection action. -/
@[inline] def projectOne (es : VecN Expr m) (rec : StreamRecordN n) :
    StreamRecordN m :=
  { row := es.map (fun e => eval rec.row.toList e)
  , diff := rec.diff
  , err_diff := rec.err_diff }

/-- Project a stream. -/
def project (es : VecN Expr m) (s : StreamN n) : StreamN m :=
  s.map (projectOne es)

@[simp] theorem project_nil (es : VecN Expr m) :
    project es ([] : StreamN n) = [] := rfl

theorem project_append (es : VecN Expr m) (s t : StreamN n) :
    project es (s ++ t) = project es s ++ project es t := by
  unfold project
  exact List.map_append

/-! ## Cross

Cross product on streams. Per the design doc the two diff components
multiply: data×data → data, and err in either factor pollutes the
err side of the product. The output arity is `n + m` by row
concatenation. -/

/-- Per-record cross product. -/
@[inline] def crossOne (recL : StreamRecordN n) (recR : StreamRecordN m) :
    StreamRecordN (n + m) :=
  { row := recL.row ++ recR.row
  , diff := recL.diff * recR.diff
  , err_diff :=
      recL.diff * recR.err_diff
      + recL.err_diff * recR.diff
      + recL.err_diff * recR.err_diff }

/-- Cross product of two streams. -/
def cross (sL : StreamN n) (sR : StreamN m) : StreamN (n + m) :=
  sL.flatMap (fun recL => sR.map (crossOne recL))

@[simp] theorem cross_nil_left (sR : StreamN m) :
    cross ([] : StreamN n) sR = [] := rfl

@[simp] theorem cross_nil_right (sL : StreamN n) :
    cross sL ([] : StreamN m) = [] := by
  unfold cross
  induction sL with
  | nil => rfl
  | cons _ _ ih =>
    rw [List.flatMap_cons, List.map_nil, List.nil_append, ih]

theorem cross_cons_left (recL : StreamRecordN n) (sL : StreamN n) (sR : StreamN m) :
    cross (recL :: sL) sR
      = sR.map (crossOne recL) ++ cross sL sR := by
  rfl

theorem cross_append_left (s t : StreamN n) (u : StreamN m) :
    cross (s ++ t) u = cross s u ++ cross t u := by
  unfold cross
  exact List.flatMap_append

/-! ## Associativity of cross — indexed-arity test

`cross_assoc` is the canonical test of how painful arity arithmetic
is in the type. The two evaluation orders produce streams at
*different* types — `StreamN ((n + m) + k)` vs `StreamN (n + (m + k))`
— and the equality has to be stated through an arity cast.

Witnesses that `List.Vector.congr` (with `Nat.add_assoc`) closes the
arity rewrite cleanly. The data and err diff arithmetic is plain
integer associativity / distributivity. -/

/-- Cast a stream record across an arity equality. -/
@[inline] def castStreamRecordN {n m : Nat} (h : n = m)
    (rec : StreamRecordN n) : StreamRecordN m :=
  { row := rec.row.congr h
  , diff := rec.diff
  , err_diff := rec.err_diff }

/-- Cast a stream across an arity equality. -/
def castStreamN {n m : Nat} (h : n = m) (s : StreamN n) : StreamN m :=
  s.map (castStreamRecordN h)

/-! ### Cross associativity at the record level

Witnesses that a single triple-cross record at the two orders agrees
after the `n + m + k = n + (m + k)` arity cast. The row equation
reduces to `(r₁ ++ r₂) ++ r₃ = r₁ ++ (r₂ ++ r₃)` on the underlying
`List Datum`; the diff and err_diff equations are plain integer
associativity / distributivity. -/

/-- `cross` of records is associative up to the arity cast. -/
theorem crossOne_assoc
    (r₁ : StreamRecordN n) (r₂ : StreamRecordN m) (r₃ : StreamRecordN k) :
    crossOne (crossOne r₁ r₂) r₃
      = castStreamRecordN (Nat.add_assoc n m k).symm
          (crossOne r₁ (crossOne r₂ r₃)) := by
  -- Unfold both sides into the record literal so that field-by-field
  -- equality is exposed. The row equation falls out of
  -- `List.append_assoc` on the underlying `List Datum`; the diff and
  -- err_diff equations are linear integer rearrangements.
  simp only [crossOne, castStreamRecordN]
  refine StreamRecordN.mk.injEq .. |>.mpr ⟨?_, ?_, ?_⟩
  · apply Subtype.ext
    show ((r₁.row.toList ++ r₂.row.toList) ++ r₃.row.toList)
        = r₁.row.toList ++ (r₂.row.toList ++ r₃.row.toList)
    exact List.append_assoc _ _ _
  · exact mul_assoc _ _ _
  · ring

/-! ### Cross associativity at the stream level

Lifts `crossOne_assoc` through `List.flatMap`. The two evaluation
orders of a triple cross produce streams at different arities; the
equality is stated through the same arity cast applied record-wise. -/

/-- `castStreamN` distributes over `++`. -/
theorem castStreamN_append {h : n = m} (s t : StreamN n) :
    castStreamN h (s ++ t) = castStreamN h s ++ castStreamN h t := by
  unfold castStreamN
  exact List.map_append

/-- Helper: distributing a left record across a right-side cross. -/
private theorem cross_map_left
    (rs : StreamRecordN n) (t : StreamN m) (u : StreamN k) :
    cross (t.map (crossOne rs)) u
      = castStreamN (Nat.add_assoc n m k).symm ((cross t u).map (crossOne rs)) := by
  induction t with
  | nil => rfl
  | cons rt tR ih =>
    -- LHS: cross (crossOne rs rt :: tR.map (crossOne rs)) u
    -- RHS: castStreamN _ ((u.map (crossOne rt) ++ cross tR u).map (crossOne rs))
    rw [List.map_cons, cross_cons_left, cross_cons_left, List.map_append,
        castStreamN_append, ih]
    congr 1
    -- u.map (crossOne (crossOne rs rt))
    --   = castStreamN _ ((u.map (crossOne rt)).map (crossOne rs))
    unfold castStreamN
    rw [List.map_map, List.map_map]
    apply List.map_congr_left
    intro ru _
    exact crossOne_assoc rs rt ru

/-- Stream-level cross associativity. -/
theorem cross_assoc (s : StreamN n) (t : StreamN m) (u : StreamN k) :
    cross (cross s t) u
      = castStreamN (Nat.add_assoc n m k).symm (cross s (cross t u)) := by
  induction s with
  | nil => rfl
  | cons rs sR ih =>
    rw [cross_cons_left, cross_append_left, ih, cross_cons_left,
        castStreamN_append]
    congr 1
    exact cross_map_left rs t u

end StreamN

end Mz
