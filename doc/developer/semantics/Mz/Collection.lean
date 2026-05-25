import Mz.Eval
import Mz.ColRefs
import Mathlib.Data.Vector.Defs
import Mathlib.Data.Vector.Basic
import Mathlib.Tactic.Ring

/-!
# Collection: rows with diff and err multiplicities

A collection is a multiset of rows, each carried with a data
multiplicity (`diff`) and an err multiplicity (`err_diff`). The
collection model deliberately omits the time dimension that
`doc/developer/platform/formalism.md` calls a time-varying
collection (TVC); this layer is the time-stripped slice — a single
collection version. Adding time is additive on top.

* `RowN n` is `List.Vector Datum n` — a length-`n` row.
* `Update n` carries `row : RowN n`, `diff : Int`, `err_diff : Int`.
  Named `Update` to match `formalism.md`'s update-triple
  vocabulary, minus the time field.
* `Collection n` is `List (Update n)`.

Operators expose arity in their signatures:

* `filter : Expr → Collection n → Collection n` — same arity.
* `project : List.Vector Expr m → Collection n → Collection m` —
  output arity is dictated by the length of the expression vector.
* `cross : Collection n → Collection m → Collection (n + m)` —
  output arity is the sum.
* `negate`, `unionAll` — pointwise multiplicity negation and
  list concatenation. Both preserve arity.

The current `Mz/Eval.lean` `eval` function expects
`Env := List Datum`, so the operators escape through `.toList` when
handing rows to `eval`. A future iteration can lift `eval` to
`Vector` directly, or expose a `Fin n`-indexed column accessor;
either is additive on top of this file. -/

namespace Mz

/-- Alias for Mathlib's `List.Vector` so the rest of this file reads
without the `List.` prefix everywhere. -/
abbrev VecN (α : Type) (n : Nat) := List.Vector α n

/-- A length-`n` row. -/
abbrev RowN (n : Nat) := VecN Datum n

/-- An update at arity `n`: row plus two `Int` multiplicities.
`diff` counts valid copies of `row`; `err_diff` counts erred copies.
Both are ordinary `Int` diffs that retract. -/
structure Update (n : Nat) where
  row : RowN n
  diff : Int
  err_diff : Int
  deriving Inhabited

/-- A collection at arity `n`: a list of updates. -/
abbrev Collection (n : Nat) := List (Update n)

namespace Collection

variable {n m k : Nat}

/-! ## Filter

Per-update filter follows the design doc rule. The row's arity is
preserved by construction — filter only zeroes multiplicities or
migrates them between data and err sides. -/

/-- Per-update filter action. -/
@[inline] def filterOne (pred : Expr) (rec : Update n) : Update n :=
  match eval rec.row.toList pred with
  | .bool true => rec
  | .err _     =>
    { row := rec.row, diff := 0, err_diff := rec.err_diff + rec.diff }
  | _          =>
    { row := rec.row, diff := 0, err_diff := rec.err_diff }

/-- Filter a collection. -/
def filter (pred : Expr) (s : Collection n) : Collection n :=
  s.map (filterOne pred)

@[simp] theorem filter_nil (pred : Expr) :
    filter pred ([] : Collection n) = [] := rfl

theorem filter_cons (pred : Expr) (rec : Update n) (s : Collection n) :
    filter pred (rec :: s) = filterOne pred rec :: filter pred s := rfl

theorem filter_append (pred : Expr) (s t : Collection n) :
    filter pred (s ++ t) = filter pred s ++ filter pred t := by
  unfold filter
  exact List.map_append

/-! ## Project

Project produces a collection at arity `m` from a length-`m`
expression vector. The output row is the pointwise application of
`eval` over the original row through the expression vector. -/

/-- Per-update projection action. -/
@[inline] def projectOne (es : VecN Expr m) (rec : Update n) :
    Update m :=
  { row := es.map (fun e => eval rec.row.toList e)
  , diff := rec.diff
  , err_diff := rec.err_diff }

/-- Project a collection. -/
def project (es : VecN Expr m) (s : Collection n) : Collection m :=
  s.map (projectOne es)

@[simp] theorem project_nil (es : VecN Expr m) :
    project es ([] : Collection n) = [] := rfl

theorem project_append (es : VecN Expr m) (s t : Collection n) :
    project es (s ++ t) = project es s ++ project es t := by
  unfold project
  exact List.map_append

/-! ## Negate

Pointwise negation of both multiplicities. Preserves arity. -/

/-- Negate a collection: flip every update's `diff` and `err_diff`. -/
def negate (s : Collection n) : Collection n :=
  s.map fun rec => { row := rec.row, diff := -rec.diff, err_diff := -rec.err_diff }

@[simp] theorem negate_nil : negate ([] : Collection n) = [] := rfl

theorem negate_append (a b : Collection n) :
    negate (a ++ b) = negate a ++ negate b := by
  unfold negate
  exact List.map_append

theorem negate_negate (s : Collection n) :
    negate (negate s) = s := by
  induction s with
  | nil => rfl
  | cons hd tl ih =>
    show negate (negate (hd :: tl)) = hd :: tl
    show { row := hd.row, diff := - -hd.diff, err_diff := - -hd.err_diff }
            :: negate (negate tl) = hd :: tl
    rw [ih]
    have h1 : - -hd.diff = hd.diff := Int.neg_neg _
    have h2 : - -hd.err_diff = hd.err_diff := Int.neg_neg _
    rw [h1, h2]

/-! ## UnionAll

List concatenation. Multiplicities of duplicate rows add via
downstream consolidation, which is not modeled at this layer. -/

/-- Concatenate two collections. -/
def unionAll (a b : Collection n) : Collection n := a ++ b

theorem unionAll_nil_left (s : Collection n) : unionAll [] s = s := rfl

theorem unionAll_nil_right (s : Collection n) : unionAll s [] = s :=
  List.append_nil s

theorem unionAll_assoc (a b c : Collection n) :
    unionAll (unionAll a b) c = unionAll a (unionAll b c) :=
  List.append_assoc a b c

/-! ## Cross

Cross product on collections. Per the design doc the two diff
components multiply: data×data → data, and err in either factor
pollutes the err side of the product. The output arity is `n + m`
by row concatenation. -/

/-- Per-update cross product. -/
@[inline] def crossOne (recL : Update n) (recR : Update m) :
    Update (n + m) :=
  { row := recL.row ++ recR.row
  , diff := recL.diff * recR.diff
  , err_diff :=
      recL.diff * recR.err_diff
      + recL.err_diff * recR.diff
      + recL.err_diff * recR.err_diff }

/-- Cross product of two collections. -/
def cross (sL : Collection n) (sR : Collection m) : Collection (n + m) :=
  sL.flatMap (fun recL => sR.map (crossOne recL))

@[simp] theorem cross_nil_left (sR : Collection m) :
    cross ([] : Collection n) sR = [] := rfl

@[simp] theorem cross_nil_right (sL : Collection n) :
    cross sL ([] : Collection m) = [] := by
  unfold cross
  induction sL with
  | nil => rfl
  | cons _ _ ih =>
    rw [List.flatMap_cons, List.map_nil, List.nil_append, ih]

theorem cross_cons_left (recL : Update n) (sL : Collection n) (sR : Collection m) :
    cross (recL :: sL) sR
      = sR.map (crossOne recL) ++ cross sL sR := by
  rfl

theorem cross_append_left (s t : Collection n) (u : Collection m) :
    cross (s ++ t) u = cross s u ++ cross t u := by
  unfold cross
  exact List.flatMap_append

/-! ## Associativity of cross — indexed-arity test

`cross_assoc` is the canonical test of how painful arity arithmetic
is in the type. The two evaluation orders produce collections at
*different* types — `Collection ((n + m) + k)` vs
`Collection (n + (m + k))` — and the equality has to be stated
through an arity cast.

Witnesses that `List.Vector.congr` (with `Nat.add_assoc`) closes
the arity rewrite cleanly. The data and err diff arithmetic is
plain integer associativity / distributivity. -/

/-- Cast an update across an arity equality. -/
@[inline] def castUpdate {n m : Nat} (h : n = m)
    (rec : Update n) : Update m :=
  { row := rec.row.congr h
  , diff := rec.diff
  , err_diff := rec.err_diff }

/-- Cast a collection across an arity equality. -/
def castCollection {n m : Nat} (h : n = m) (s : Collection n) : Collection m :=
  s.map (castUpdate h)

/-! ### Cross associativity at the update level

Witnesses that a single triple-cross update at the two orders agrees
after the `n + m + k = n + (m + k)` arity cast. The row equation
reduces to `(r₁ ++ r₂) ++ r₃ = r₁ ++ (r₂ ++ r₃)` on the underlying
`List Datum`; the diff and err_diff equations are plain integer
associativity / distributivity. -/

/-- `cross` of updates is associative up to the arity cast. -/
theorem crossOne_assoc
    (r₁ : Update n) (r₂ : Update m) (r₃ : Update k) :
    crossOne (crossOne r₁ r₂) r₃
      = castUpdate (Nat.add_assoc n m k).symm
          (crossOne r₁ (crossOne r₂ r₃)) := by
  -- Unfold both sides into the record literal so that field-by-field
  -- equality is exposed. The row equation falls out of
  -- `List.append_assoc` on the underlying `List Datum`; the diff and
  -- err_diff equations are linear integer rearrangements.
  simp only [crossOne, castUpdate]
  refine Update.mk.injEq .. |>.mpr ⟨?_, ?_, ?_⟩
  · apply Subtype.ext
    show ((r₁.row.toList ++ r₂.row.toList) ++ r₃.row.toList)
        = r₁.row.toList ++ (r₂.row.toList ++ r₃.row.toList)
    exact List.append_assoc _ _ _
  · exact mul_assoc _ _ _
  · ring

/-! ### Cross associativity at the collection level

Lifts `crossOne_assoc` through `List.flatMap`. The two evaluation
orders of a triple cross produce collections at different arities;
the equality is stated through the same arity cast applied
update-wise. -/

/-- `castCollection` distributes over `++`. -/
theorem castCollection_append {h : n = m} (s t : Collection n) :
    castCollection h (s ++ t) = castCollection h s ++ castCollection h t := by
  unfold castCollection
  exact List.map_append

/-- Helper: distributing a left update across a right-side cross. -/
private theorem cross_map_left
    (rs : Update n) (t : Collection m) (u : Collection k) :
    cross (t.map (crossOne rs)) u
      = castCollection (Nat.add_assoc n m k).symm ((cross t u).map (crossOne rs)) := by
  induction t with
  | nil => rfl
  | cons rt tR ih =>
    -- LHS: cross (crossOne rs rt :: tR.map (crossOne rs)) u
    -- RHS: castCollection _ ((u.map (crossOne rt) ++ cross tR u).map (crossOne rs))
    rw [List.map_cons, cross_cons_left, cross_cons_left, List.map_append,
        castCollection_append, ih]
    congr 1
    -- u.map (crossOne (crossOne rs rt))
    --   = castCollection _ ((u.map (crossOne rt)).map (crossOne rs))
    unfold castCollection
    rw [List.map_map, List.map_map]
    apply List.map_congr_left
    intro ru _
    exact crossOne_assoc rs rt ru

/-- Collection-level cross associativity. -/
theorem cross_assoc (s : Collection n) (t : Collection m) (u : Collection k) :
    cross (cross s t) u
      = castCollection (Nat.add_assoc n m k).symm (cross s (cross t u)) := by
  induction s with
  | nil => rfl
  | cons rs sR ih =>
    rw [cross_cons_left, cross_append_left, ih, cross_cons_left,
        castCollection_append]
    congr 1
    exact cross_map_left rs t u

/-! ## Filter / cross pushdown (left)

The classical relational pushdown `filter p (cross sL sR) = cross
(filter p sL) sR` is sound on bags. Under the two-diff collection
model, the indexed-arity formulation makes the obligation precise
enough to reveal a soundness gap on the err side. The pilot
attempted the proof on `filterOne` and was unable to discharge the
`bool false / null / int` branches: the LHS err multiplicity
includes a `recL.diff * recR.err_diff` term (from the cross
product) which the RHS drops when the filter zeroes `recL.diff`
before the cross.

**Concrete witness.** Let `recL = (rowL, diff = 1, err_diff = 0)`
and `recR = (rowR, diff = 0, err_diff = 1)`, with `eval rowL p =
.bool false` (e.g., `p` is the literal `.lit (.bool false)`).

* LHS — `filterOne p (crossOne recL recR)`:
  * `crossOne recL recR = (rowL ++ rowR, 0, 1)`.
  * `filterOne (.bool false) zeros data, leaves err: (rowL ++ rowR, 0, 1)`.
* RHS — `crossOne (filterOne p recL) recR`:
  * `filterOne (.bool false) recL = (rowL, 0, 0)`.
  * `crossOne (rowL, 0, 0) (rowR, 0, 1) = (rowL ++ rowR, 0, 0)`.

The two updates differ on `err_diff` (1 vs 0). Operationally: the
right collection's error is unconditional — it surfaces regardless
of how the left's predicate evaluates. Pushdown loses that error
because the cross's err-diff formula multiplies `recL.diff` against
`recR.err_diff`, and filtering recL to `diff = 0` zeroes the
product.

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
* a collection encoding where errors are not multiplied through
  cross. -/

/-- Setup lemma: `eval` on the combined row equals `eval` on the
left row when the predicate's columns are bounded by `n`. Used by
both the `.bool true` and `.err _` branch lemmas. -/
private theorem eval_crossOne_left_bounded
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (recL : Update n) (recR : Update m) :
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
    (recL : Update n) (recR : Update m)
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
    (recL : Update n) (recR : Update m) (e : EvalError)
    (herr : eval recL.row.toList p = .err e) :
    filterOne p (crossOne recL recR)
      = crossOne (filterOne p recL) recR := by
  have heval := eval_crossOne_left_bounded p hp recL recR
  unfold filterOne
  rw [heval, herr]
  simp only [crossOne]
  refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩ <;> ring

/-- Concrete counterexample to the full `filterOne` pushdown.
With a predicate that evaluates to `.bool false`, `recL` with
positive `diff`, and `recR` with positive `err_diff`, the two sides
differ on err multiplicity. -/
theorem filterOne_cross_pushdown_left_unsound :
    ∃ (p : Expr) (recL : Update 0) (recR : Update 0),
      p.colReferencesBoundedBy 0 = true
      ∧ filterOne p (crossOne recL recR)
          ≠ crossOne (filterOne p recL) recR := by
  refine ⟨.lit (.bool false),
          { row := ⟨[], rfl⟩, diff := 1, err_diff := 0 },
          { row := ⟨[], rfl⟩, diff := 0, err_diff := 1 },
          rfl, ?_⟩
  intro h
  have := congrArg Update.err_diff h
  simp [filterOne, crossOne, eval] at this

/-! ## Pushdown under data-side equivalence

The pushdown's strict-equality form fails on `err_diff`, but the
`row` and `diff` fields agree on every branch. Mechanizing this as
"data-side equivalence" makes the precise gap explicit: the
transformation is sound under any equivalence that ignores
`err_diff`, and unsound under any equivalence that preserves it. -/

end Collection

/-- Data-side erasure: forget the err multiplicity. Two updates are
"data equivalent" iff their `eraseErr` projections coincide. -/
@[inline] def Update.eraseErr (rec : Update n) : Update n :=
  { row := rec.row, diff := rec.diff, err_diff := 0 }

namespace Collection

variable {n m k : Nat}

/-- Collection-level erasure: map `eraseErr` over every update. -/
@[inline] def eraseErrAll (s : Collection n) : Collection n :=
  s.map Update.eraseErr

/-- Per-update pushdown holds under data-side erasure: the row and
data multiplicity agree on every branch of `filterOne`. -/
theorem filterOne_cross_pushdown_left_data
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (recL : Update n) (recR : Update m) :
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
      simp only [Update.eraseErr, crossOne]
      refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, rfl⟩
      ring
  | int _ =>
    simp only [Update.eraseErr, crossOne]
    refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, rfl⟩
    ring
  | null =>
    simp only [Update.eraseErr, crossOne]
    refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, rfl⟩
    ring
  | err _ =>
    simp only [Update.eraseErr, crossOne]
    refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, rfl⟩
    ring

/-- Collection-level pushdown under data-side erasure. The two
evaluation orders of `filter p (cross sL sR)` agree exactly on row
and data multiplicity at every update, even though their err
multiplicities diverge. -/
theorem filter_cross_pushdown_left_data
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (sL : Collection n) (sR : Collection m) :
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

end Collection

/-! ## Schema predicates

The pushdown obligation `filter_cross_pushdown_left` fails under
strict equality because cross's err-diff bilinear formula carries
the term `recL.diff · recR.err_diff`; filter zeroes `recL.diff`
before the cross and drops the contribution. The simplest schema
fact that closes the obligation is "the right collection has no
row-level errors": with `recR.err_diff = 0` for every right
update, the offending term vanishes and the pushdown holds at
strict equality.

This section defines `NoRowErr` as a propositional predicate on
updates and collections, and discharges the pushdown under it.
The predicate is intentionally minimal — a single equality on
`err_diff`. The full schema extension envisioned in `model.md`
("schema tracks per-column nullability and errability") is a
strictly stronger structure; this predicate is the slice of it
that the pushdown actually needs. -/

/-- An update has no row-level error multiplicity. Cell-level
errors inside `row` are not constrained here — that is the
separate "cell-err-free" condition the schema's `errable` bit
will eventually carry. -/
def Update.NoRowErr (rec : Update n) : Prop :=
  rec.err_diff = 0

namespace Collection

variable {n m k : Nat}

/-- A collection's updates all have zero row-level err multiplicity. -/
def NoRowErr (s : Collection n) : Prop :=
  ∀ rec ∈ s, rec.err_diff = 0

@[simp] theorem NoRowErr_nil : NoRowErr ([] : Collection n) := by
  intro _ h; cases h

theorem NoRowErr_cons {rec : Update n} {s : Collection n} :
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

/-- Per-update pushdown under strict equality, given the right
update has no row-level err multiplicity. The offending term
`recL.diff · recR.err_diff` in cross's err-diff formula vanishes,
and every branch of `filterOne` reconciles by `ring`. -/
private theorem filterOne_cross_pushdown_left_strict
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (recL : Update n) (recR : Update m)
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
      refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩
      · ring
      · rw [hR]; ring
  | int _ =>
    simp only [crossOne]
    refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩
    · ring
    · rw [hR]; ring
  | null =>
    simp only [crossOne]
    refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩
    · ring
    · rw [hR]; ring
  | err _ =>
    simp only [crossOne]
    refine Update.mk.injEq .. |>.mpr ⟨rfl, ?_, ?_⟩
    · ring
    · rw [hR]; ring

/-- Collection-level pushdown under strict equality, given the
right collection is row-err-free. -/
theorem filter_cross_pushdown_left_strict
    (p : Expr) (hp : p.colReferencesBoundedBy n = true)
    (sL : Collection n) (sR : Collection m) (hR : NoRowErr sR) :
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

end Collection

end Mz
