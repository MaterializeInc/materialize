import Mz.Eval
import Mz.ColRefs
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

/-! ## Filter / cross pushdown (left)

The classical relational pushdown `filter p (cross sL sR) = cross
(filter p sL) sR` is sound on bags. Under the two-diff stream model
(`Mz/Stream.lean` / this module), the indexed-arity formulation
makes the obligation precise enough to reveal a soundness gap on
the err side. The pilot attempted the proof on `filterOne` and was
unable to discharge the `bool false / null / int` branches: the LHS
err multiplicity includes a `recL.diff * recR.err_diff` term (from
the cross product) which the RHS drops when the filter zeroes
`recL.diff` before the cross.

**Concrete witness.** Let `recL = (rowL, diff = 1, err_diff = 0)`
and `recR = (rowR, diff = 0, err_diff = 1)`, with `eval rowL p =
.bool false` (e.g., `p` is the literal `.lit (.bool false)`).

* LHS — `filterOne p (crossOne recL recR)`:
  * `crossOne recL recR = (rowL ++ rowR, 0, 1)`.
  * `filterOne (.bool false) zeros data, leaves err: (rowL ++ rowR, 0, 1)`.
* RHS — `crossOne (filterOne p recL) recR`:
  * `filterOne (.bool false) recL = (rowL, 0, 0)`.
  * `crossOne (rowL, 0, 0) (rowR, 0, 1) = (rowL ++ rowR, 0, 0)`.

The two records differ on `err_diff` (1 vs 0). Operationally: the
right stream's error is unconditional — it surfaces regardless of
how the left's predicate evaluates. Pushdown loses that error
because the cross's err-diff formula multiplies `recL.diff` against
`recR.err_diff`, and filtering recL to `diff = 0` zeroes the product.

The branches that *do* close are the ones where filter's action
commutes with cross's multiplicative structure:

* `.bool true` — filter is the identity, both sides equal.
* `.err _` — filter migrates `diff` into `err_diff`; the
  cross-multiplied result on either order produces the same
  expanded sum of products.

These are recorded as `filterOne_cross_pushdown_left_true` and
`filterOne_cross_pushdown_left_err`. The full pushdown
`filter_cross_pushdown_left` is documented here as an open
obligation; closing it would require one of:

* a non-deterministic semantics in which the right's err
  multiplicity is moved to a separate channel that filter does not
  multiply against;
* a refinement-based equivalence that ignores err_diff drops;
* a stream encoding where errors are not multiplied through cross.

This finding mirrors the per-payload-diff soundness gap previously
documented for predicate pushdown over cross in the design doc's
*Counterexamples I tried and could not prove* section. -/

/-- Setup lemma: `eval` on the combined row equals `eval` on the
left row when the predicate's columns are bounded by `n`. Used by
both the `.bool true` and `.err _` branch lemmas. -/
private theorem eval_crossOne_left_bounded
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (recL : StreamRecordN n) (recR : StreamRecordN m) :
    eval (crossOne recL recR).row.toList p = eval recL.row.toList p := by
  have htoList :
      (crossOne recL recR).row.toList
        = recL.row.toList ++ recR.row.toList := by
    show (recL.row ++ recR.row).toList = recL.row.toList ++ recR.row.toList
    exact List.Vector.toList_append recL.row recR.row
  have hLen : recL.row.toList.length = n := recL.row.toList_length
  have hp' : p.colReferencesBoundedBy recL.row.toList.length = true := by
    rw [hLen]; exact hp
  rw [htoList]
  exact eval_append_left_of_bounded _ _ _ hp'

/-- Branch where the predicate evaluates to `.bool true` on the
left row: filter is the identity, and `filterOne` commutes with
`crossOne`. -/
theorem filterOne_cross_pushdown_left_true
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (recL : StreamRecordN n) (recR : StreamRecordN m)
    (htrue : eval recL.row.toList p = .bool true) :
    filterOne p (crossOne recL recR)
      = crossOne (filterOne p recL) recR := by
  have heval := eval_crossOne_left_bounded p hp recL recR
  unfold filterOne
  rw [heval, htrue]

/-- Branch where the predicate evaluates to `.err _` on the left
row: data multiplicity migrates to err on both sides, and the
arithmetic on err_diff rearranges to the same value. -/
theorem filterOne_cross_pushdown_left_err
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (recL : StreamRecordN n) (recR : StreamRecordN m) (e : EvalError)
    (herr : eval recL.row.toList p = .err e) :
    filterOne p (crossOne recL recR)
      = crossOne (filterOne p recL) recR := by
  have heval := eval_crossOne_left_bounded p hp recL recR
  unfold filterOne
  rw [heval, herr]
  simp only [crossOne]
  refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩ <;> ring

/-- Concrete counterexample to the full `filterOne` pushdown.
With a predicate that evaluates to `.bool false`, `recL` with
positive `diff`, and `recR` with positive `err_diff`, the two sides
differ on err multiplicity. -/
theorem filterOne_cross_pushdown_left_unsound :
    ∃ (p : Expr) (recL : StreamRecordN 0) (recR : StreamRecordN 0),
      p.colReferencesBoundedBy 0 = true
      ∧ filterOne p (crossOne recL recR)
          ≠ crossOne (filterOne p recL) recR := by
  refine ⟨.lit (.bool false),
          { row := ⟨[], rfl⟩, diff := 1, err_diff := 0 },
          { row := ⟨[], rfl⟩, diff := 0, err_diff := 1 },
          rfl, ?_⟩
  intro h
  have := congrArg StreamRecordN.err_diff h
  simp [filterOne, crossOne, eval] at this

/-! ## Pushdown under data-side equivalence

The pushdown's strict-equality form fails on `err_diff`, but the
`row` and `diff` fields agree on every branch. Mechanizing this as
"data-side equivalence" makes the precise gap explicit: the
transformation is sound under any equivalence that ignores
`err_diff`, and unsound under any equivalence that preserves it.

This matches the design doc's *Evaluation-order equivalence*
alternative §"Refinement preorder" — under an errors-as-bottom
posture, dropping an err multiplicity is admissible. The
mechanization here uses an equivalence rather than the asymmetric
preorder because data-equivalence is the cleanest stateable
relation on `Int`-valued err_diff: lifting `Datum.refines` to
`Int` doesn't fall out cleanly when multiplicities can retract
(go negative). A future iteration with `Nat`-valued or multiset-
valued err multiplicities would let the asymmetric form work too. -/

end StreamN

/-- Data-side erasure: forget the err multiplicity. Two records are
"data equivalent" iff their `eraseErr` projections coincide. -/
@[inline] def StreamRecordN.eraseErr (rec : StreamRecordN n) : StreamRecordN n :=
  { row := rec.row, diff := rec.diff, err_diff := 0 }

namespace StreamN

variable {n m k : Nat}

/-- Stream-level erasure: map `eraseErr` over every record. -/
@[inline] def eraseErrAll (s : StreamN n) : StreamN n :=
  s.map StreamRecordN.eraseErr

/-- Per-record pushdown holds under data-side erasure: the row and
data multiplicity agree on every branch of `filterOne`. -/
theorem filterOne_cross_pushdown_left_data
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (recL : StreamRecordN n) (recR : StreamRecordN m) :
    (filterOne p (crossOne recL recR)).eraseErr
      = (crossOne (filterOne p recL) recR).eraseErr := by
  have heval := eval_crossOne_left_bounded p hp recL recR
  unfold filterOne
  rw [heval]
  cases eval recL.row.toList p with
  | bool b =>
    cases b with
    | true => rfl
    | false =>
      simp only [StreamRecordN.eraseErr, crossOne]
      refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, rfl⟩
      ring
  | int _ =>
    simp only [StreamRecordN.eraseErr, crossOne]
    refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, rfl⟩
    ring
  | null =>
    simp only [StreamRecordN.eraseErr, crossOne]
    refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, rfl⟩
    ring
  | err _ =>
    simp only [StreamRecordN.eraseErr, crossOne]
    refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, rfl⟩
    ring

/-- Stream-level pushdown under data-side erasure. The two
evaluation orders of `filter p (cross sL sR)` agree exactly on row
and data multiplicity at every record, even though their err
multiplicities diverge. -/
theorem filter_cross_pushdown_left_data
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (sL : StreamN n) (sR : StreamN m) :
    eraseErrAll (filter p (cross sL sR))
      = eraseErrAll (cross (filter p sL) sR) := by
  unfold eraseErrAll
  induction sL with
  | nil => rfl
  | cons recL sLR ih =>
    rw [cross_cons_left, filter_append, List.map_append,
        filter_cons, cross_cons_left, List.map_append,
        ih]
    congr 1
    -- prefix: filter p (sR.map (crossOne recL)) ↦ eraseErr
    --      vs sR.map (crossOne (filterOne p recL)) ↦ eraseErr
    rw [filter, List.map_map, List.map_map, List.map_map]
    apply List.map_congr_left
    intro recR _
    show (filterOne p (crossOne recL recR)).eraseErr
        = (crossOne (filterOne p recL) recR).eraseErr
    exact filterOne_cross_pushdown_left_data p hp recL recR

end StreamN

/-! ## Schema predicates

The pushdown obligation `filter_cross_pushdown_left` fails under
strict equality because cross's err-diff bilinear formula carries
the term `recL.diff · recR.err_diff`; filter zeroes `recL.diff`
before the cross and drops the contribution. The simplest schema
fact that closes the obligation is "the right stream has no
row-level errors": with `recR.err_diff = 0` for every right record,
the offending term vanishes and the pushdown holds at strict
equality.

This section defines `NoRowErr` as a propositional predicate on
records and streams, and discharges the pushdown under it. The
predicate is intentionally minimal — a single equality on
`err_diff`. The full schema extension envisioned in `model.md`
("schema tracks per-column nullability and errability") is a
strictly stronger structure; this predicate is the slice of it
that the pushdown actually needs.

The natural next step is a `Schema n` structure with `nullable`
and `errable` per column, plus a `rowErrFree` flag. `NoRowErr`
would then become a derived property of the schema lifted
pointwise to records. Until that lift lands, the predicate stands
on its own. -/

/-- A record has no row-level error multiplicity. Cell-level
errors inside `row` are not constrained here — that is the
separate "cell-err-free" condition the schema's `errable` bit
will eventually carry. -/
def StreamRecordN.NoRowErr (rec : StreamRecordN n) : Prop :=
  rec.err_diff = 0

namespace StreamN

variable {n m k : Nat}

/-- A stream's records all have zero row-level err multiplicity. -/
def NoRowErr (s : StreamN n) : Prop :=
  ∀ rec ∈ s, rec.err_diff = 0

@[simp] theorem NoRowErr_nil : NoRowErr ([] : StreamN n) := by
  intro _ h; cases h

theorem NoRowErr_cons {rec : StreamRecordN n} {s : StreamN n} :
    NoRowErr (rec :: s) ↔ rec.err_diff = 0 ∧ NoRowErr s := by
  constructor
  · intro h
    refine ⟨h rec ?_, ?_⟩
    · exact List.mem_cons_self
    · intro r hr
      exact h r (List.mem_cons_of_mem _ hr)
  · rintro ⟨h0, hs⟩ r hr
    rw [List.mem_cons] at hr
    cases hr with
    | inl h => rw [h]; exact h0
    | inr h => exact hs _ h

/-! ## Pushdown under strict equality on err-free right -/

/-- Per-record pushdown under strict equality, given the right
record has no row-level err multiplicity. The offending term
`recL.diff · recR.err_diff` in cross's err-diff formula vanishes,
and every branch of `filterOne` reconciles by `ring`. -/
private theorem filterOne_cross_pushdown_left_strict
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (recL : StreamRecordN n) (recR : StreamRecordN m)
    (hR : recR.err_diff = 0) :
    filterOne p (crossOne recL recR)
      = crossOne (filterOne p recL) recR := by
  have heval := eval_crossOne_left_bounded p hp recL recR
  unfold filterOne
  rw [heval]
  cases eval recL.row.toList p with
  | bool b =>
    cases b with
    | true => rfl
    | false =>
      simp only [crossOne]
      refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩
      · ring
      · rw [hR]; ring
  | int _ =>
    simp only [crossOne]
    refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩
    · ring
    · rw [hR]; ring
  | null =>
    simp only [crossOne]
    refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩
    · ring
    · rw [hR]; ring
  | err _ =>
    simp only [crossOne]
    refine StreamRecordN.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩
    · ring
    · rw [hR]; ring

/-- Stream-level pushdown under strict equality, given the right
stream is row-err-free. -/
theorem filter_cross_pushdown_left_strict
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (sL : StreamN n) (sR : StreamN m) (hR : NoRowErr sR) :
    filter p (cross sL sR) = cross (filter p sL) sR := by
  induction sL with
  | nil => rfl
  | cons recL sLR ih =>
    rw [cross_cons_left, filter_append, filter_cons,
        cross_cons_left, ih]
    congr 1
    rw [filter, List.map_map]
    apply List.map_congr_left
    intro recR hrecR
    exact filterOne_cross_pushdown_left_strict p hp recL recR (hR recR hrecR)

end StreamN

end Mz
