import Mz.Schema
import Mz.Eval
import Mz.Equiv
import Mz.MightError
import Mz.OutputType
import Mathlib.Tactic.Ring

/-!
# Schema-indexed `Collection`

Updates and collections parameterized by a schema. Indexed
counterpart of `Mz/Collection.lean`.

`Update sch` carries:

* `row : Env sch` — typed lookup function, each cell has the kind
  declared by `sch.types`.
* `diff : Int` — data multiplicity.
* `err_diff : Int` — err multiplicity.

`Collection sch := List (Update sch)`. Operators are typed by their
input / output schemas:

* `filter (p : Expr sch .bool) : Collection sch → Collection sch`
  — preserves schema.
* `project (es : (i : Fin m) → Expr sch_in (sch_out.types.get i))
  : Collection sch_in → Collection sch_out` — schema-transforming.
* `cross : Collection sch_l → Collection sch_r → Collection
  (Schema.append sch_l sch_r)` — produces the appended schema.
* `negate`, `unionAll` — schema-preserving.

The schema indexing means ill-formed operator composition fails to
type-check at construction time — `project` against the wrong
schema, `cross` whose result is consumed by a `filter` over a
different schema, etc. -/

namespace Mz


/-- A single update in a schema-indexed collection. -/
structure Update {n : Nat} (sch : Schema n) where
  row : Env sch
  diff : Int
  err_diff : Int

/-- A schema-indexed collection. -/
abbrev Collection {n : Nat} (sch : Schema n) := List (Update sch)

namespace Collection

variable {n m : Nat}

/-! ## Filter

`filter p s` evaluates the predicate per-update. Bool result drives
the data side; `.err _` migrates data multiplicity to the err side
(design-doc rule); `.null` / `.int` route both data and err to
zero (data only, matching the Predicates clause). -/

/-- One-update filter step. Schema-preserving. -/
def filterOne {sch : Schema n} (p : Expr sch .bool)
    (rec : Update sch) : Update sch :=
  match eval rec.row p with
  | .bool true  => rec
  | .bool false => { row := rec.row, diff := 0, err_diff := rec.err_diff }
  | .null       => { row := rec.row, diff := 0, err_diff := rec.err_diff }
  | .err _      => { row := rec.row, diff := 0, err_diff := rec.err_diff + rec.diff }

/-- Filter over a schema-preserving predicate. -/
def filter {sch : Schema n} (p : Expr sch .bool) :
    Collection sch → Collection sch :=
  List.map (filterOne p)

theorem filter_nil {sch : Schema n} (p : Expr sch .bool) :
    filter p ([] : Collection sch) = [] := rfl

theorem filter_cons {sch : Schema n} (p : Expr sch .bool)
    (rec : Update sch) (s : Collection sch) :
    filter p (rec :: s) = filterOne p rec :: filter p s := rfl

/-! ## Project

`project es s` applies the per-column projection `es` to every
update's row. The output schema's column types are induced by the
expressions in `es`. -/

/-- One-update projection step. The output row is the function
`fun i => eval rec.row (es i)`. -/
def projectOne {sch_in : Schema n} {sch_out : Schema m}
    (es : (i : Fin m) → Expr sch_in (sch_out.types.get i))
    (rec : Update sch_in) : Update sch_out :=
  { row := fun i => eval rec.row (es i)
    diff := rec.diff
    err_diff := rec.err_diff }

def project {sch_in : Schema n} {sch_out : Schema m}
    (es : (i : Fin m) → Expr sch_in (sch_out.types.get i)) :
    Collection sch_in → Collection sch_out :=
  List.map (projectOne es)

theorem project_nil {sch_in : Schema n} {sch_out : Schema m}
    (es : (i : Fin m) → Expr sch_in (sch_out.types.get i)) :
    project es ([] : Collection sch_in) = [] := rfl

/-! ## Negate

Pointwise negation of both multiplicities. Schema-preserving. -/

def negate {sch : Schema n} (s : Collection sch) : Collection sch :=
  s.map fun rec => { row := rec.row, diff := -rec.diff, err_diff := -rec.err_diff }

@[simp] theorem negate_nil {sch : Schema n} :
    negate ([] : Collection sch) = [] := rfl

theorem negate_negate {sch : Schema n} (s : Collection sch) :
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

List concatenation. Schema-preserving. -/

def unionAll {sch : Schema n} (a b : Collection sch) : Collection sch := a ++ b

theorem unionAll_nil_left {sch : Schema n} (s : Collection sch) :
    unionAll [] s = s := rfl

theorem unionAll_nil_right {sch : Schema n} (s : Collection sch) :
    unionAll s [] = s :=
  List.append_nil s

theorem unionAll_assoc {sch : Schema n} (a b c : Collection sch) :
    unionAll (unionAll a b) c = unionAll a (unionAll b c) :=
  List.append_assoc a b c

/-! ## Cross product

`crossOne` combines two updates with the bilinear diff rule
`(d, e) * (d', e') = (d*d', d*e' + e*d' + e*e')`. The result's
row spans the appended schema's columns. The Mathlib-level
helper lemmas establish that the appended schema's per-column
type agrees with the left or right schema as appropriate. -/

private theorem Schema.types_get_append_left
    {n m : Nat} (sch_l : Schema n) (sch_r : Schema m)
    (i : Fin (n + m)) (h : i.val < n) :
    (Schema.append sch_l sch_r).types.get i =
    sch_l.types.get ⟨i.val, h⟩ := by
  rcases sch_l with ⟨_, ⟨ll, hll⟩, _⟩
  rcases sch_r with ⟨_, ⟨lr, _⟩, _⟩
  rcases i with ⟨v, _⟩
  show (ll ++ lr).get ⟨v, _⟩ = ll.get ⟨v, _⟩
  simp only [List.get_eq_getElem, List.getElem_append]
  rw [dif_pos (by rw [hll]; exact h : v < ll.length)]

private theorem Schema.types_get_append_right
    {n m : Nat} (sch_l : Schema n) (sch_r : Schema m)
    (i : Fin (n + m)) (h : ¬i.val < n) :
    (Schema.append sch_l sch_r).types.get i =
    sch_r.types.get ⟨i.val - n, by have := i.isLt; omega⟩ := by
  rcases sch_l with ⟨_, ⟨ll, hll⟩, _⟩
  rcases sch_r with ⟨_, ⟨lr, hlr⟩, _⟩
  rcases i with ⟨v, hv⟩
  show (ll ++ lr).get ⟨v, _⟩ = lr.get ⟨v - n, _⟩
  simp only [List.get_eq_getElem, List.getElem_append]
  rw [dif_neg (by rw [hll]; exact h : ¬v < ll.length)]
  congr 1
  rw [hll]

/-- One-update cross step. Bilinear err rule on the diffs;
combined row spans the appended schema. -/
def crossOne {sch_l : Schema n} {sch_r : Schema m}
    (recL : Update sch_l) (recR : Update sch_r) :
    Update (Schema.append sch_l sch_r) :=
  { row := fun i =>
      if h : i.val < n then
        (Schema.types_get_append_left sch_l sch_r i h) ▸
          recL.row ⟨i.val, h⟩
      else
        (Schema.types_get_append_right sch_l sch_r i h) ▸
          recR.row ⟨i.val - n, by have := i.isLt; omega⟩
    diff := recL.diff * recR.diff
    err_diff := recL.diff * recR.err_diff
              + recL.err_diff * recR.diff
              + recL.err_diff * recR.err_diff }

/-- Cross product of two collections. Cartesian product of
updates via `crossOne`. Schema is the append. -/
def cross {sch_l : Schema n} {sch_r : Schema m}
    (l : Collection sch_l) (r : Collection sch_r) :
    Collection (Schema.append sch_l sch_r) :=
  l.flatMap (fun recL => r.map (crossOne recL))

theorem cross_nil_left {sch_l : Schema n} {sch_r : Schema m}
    (r : Collection sch_r) :
    cross ([] : Collection sch_l) r = [] := rfl

theorem cross_nil_right {sch_l : Schema n} {sch_r : Schema m}
    (l : Collection sch_l) :
    cross l ([] : Collection sch_r) = [] := by
  unfold cross
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    show List.map (crossOne hd) [] ++ tl.flatMap (fun recL => List.map (crossOne recL) []) = []
    simp

theorem cross_cons_left {sch_l : Schema n} {sch_r : Schema m}
    (recL : Update sch_l) (sL : Collection sch_l) (sR : Collection sch_r) :
    cross (recL :: sL) sR = sR.map (crossOne recL) ++ cross sL sR := rfl

/-! ## Schema cast on Update / Collection

Building blocks for stating cross associativity (and other
arity-aware Schema rearrangements) as `Eq` rather than `HEq`. Each
cast takes an arity-eq + Schema HEq pair and substitutes through. -/

/-- Cast an `Update` from `sch1` to `sch2` when the schemas are HEq
across an arity equality. Both arity and Schema substitutions reduce
the cast to identity. -/
def _root_.Mz.Update.cast {n p : Nat} {sch1 : Schema n} {sch2 : Schema p}
    (h_arity : n = p) (h_sch : HEq sch1 sch2) (u : Update sch1) :
    Update sch2 := by
  subst h_arity
  have heq : sch1 = sch2 := eq_of_heq h_sch
  subst heq
  exact u

/-- Cast a `Collection` from `sch1` to `sch2` when the schemas are
HEq across an arity equality. Defined as pointwise `Update.cast`. -/
def cast {n p : Nat} {sch1 : Schema n} {sch2 : Schema p}
    (h_arity : n = p) (h_sch : HEq sch1 sch2) (l : Collection sch1) :
    Collection sch2 :=
  l.map (Update.cast h_arity h_sch)

/-- When both arity and Schema equalities are `rfl`, the cast is
the identity (as a List). -/
theorem cast_rfl {n : Nat} {sch : Schema n} (l : Collection sch) :
    cast rfl HEq.rfl l = l := by
  show l.map (Update.cast rfl HEq.rfl) = l
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    show Update.cast rfl HEq.rfl hd :: tl.map (Update.cast rfl HEq.rfl) = hd :: tl
    rw [ih]
    congr 1

/-! ## crossOne multiplicity associativity

`crossOne`'s `diff` and `err_diff` fields are associative as `Int`
expressions — closed by `ring`. The row component is the non-trivial
part of `cross_assoc` (deferred). -/

theorem crossOne_diff_assoc (a b c : Int) :
    (a * b) * c = a * (b * c) := by ring

theorem crossOne_err_diff_assoc (dL eL dM eM dR eR : Int) :
    (dL * dM) * eR + (dL * eM + eL * dM + eL * eM) * dR
      + (dL * eM + eL * dM + eL * eM) * eR
    = dL * (dM * eR + eM * dR + eM * eR) + eL * (dM * dR)
      + eL * (dM * eR + eM * dR + eM * eR) := by ring

/-! ## Cross associativity (open — row component)

With `Schema.append_assoc_heq` + `Update.cast` / `Collection.cast`
the natural cross_assoc statement is:

```
theorem cross_assoc {n m k} {sl sm sk}
    (l : Collection sl) (mc : Collection sm) (r : Collection sk) :
    cast (Nat.add_assoc n m k) (Schema.append_assoc_heq sl sm sk)
      (cross (cross l mc) r) = cross l (cross mc r)
```

The data and err multiplicities close under Int ring laws (the
bilinear err rule is `(d, e) * (d', e') = (d*d', d*e' + e*d' + e*e')`,
which `ring` discharges). The blocker is the row component:
`(crossOne (crossOne a b) c).row` and `(crossOne a (crossOne b c)).row`
both produce `Fin (n + (m + k)) → Datum _`, but the index dispatch
splits at different positions (LHS at `n+m`, RHS at `n`). Equating
them pointwise requires Fin-index manipulation under three `▸` casts
(`Schema.types_get_append_left/right` at two nesting levels). Left
as follow-up. -/

/-! ## NoRowErr precondition -/

/-- An update is `NoRowErr` when its row-error multiplicity is
zero. Operational regimes (sources known to produce only valid
or invalid rows) discharge this on inputs. -/
def _root_.Mz.Update.NoRowErr {sch : Schema n} (rec : Update sch) : Prop :=
  rec.err_diff = 0

/-- A collection has `NoRowErr` when every update does. -/
def NoRowErr {sch : Schema n} (s : Collection sch) : Prop :=
  ∀ rec ∈ s, Update.NoRowErr rec

theorem NoRowErr_nil {sch : Schema n} : NoRowErr ([] : Collection sch) := by
  unfold NoRowErr
  intro _ hmem; cases hmem

/-! ## NoRowErr propagation through operators

If inputs have `NoRowErr`, do the outputs of schema-preserving
operators? Two answers depending on the operator:

* `negate`, `unionAll`, `project` preserve `NoRowErr` unconditionally.
* `filter` preserves it when the predicate is statically err-free
  on the schema (i.e., `Expr.might_error p = false` plus an
  err-free row condition).  Migration of data multiplicity to
  err multiplicity is the load-bearing concern.  -/

theorem NoRowErr_negate {sch : Schema n} {s : Collection sch}
    (h : NoRowErr s) : NoRowErr (negate s) := by
  intro rec hrec
  unfold negate at hrec
  rw [List.mem_map] at hrec
  obtain ⟨rec', hrec'_mem, hrec'_eq⟩ := hrec
  have h_rec' := h rec' hrec'_mem
  show rec.err_diff = 0
  rw [← hrec'_eq]
  show -rec'.err_diff = 0
  rw [h_rec']
  rfl

theorem NoRowErr_unionAll {sch : Schema n} {a b : Collection sch}
    (ha : NoRowErr a) (hb : NoRowErr b) : NoRowErr (unionAll a b) := by
  intro rec hrec
  unfold unionAll at hrec
  rcases List.mem_append.mp hrec with hL | hR
  · exact ha rec hL
  · exact hb rec hR

theorem NoRowErr_project {sch_in : Schema n} {sch_out : Schema m}
    (es : (i : Fin m) → Expr sch_in (sch_out.types.get i))
    {s : Collection sch_in} (h : NoRowErr s) :
    NoRowErr (project es s) := by
  intro rec hrec
  unfold project at hrec
  rw [List.mem_map] at hrec
  obtain ⟨rec', hrec'_mem, hrec'_eq⟩ := hrec
  have h_rec' := h rec' hrec'_mem
  show rec.err_diff = 0
  rw [← hrec'_eq]
  show rec'.err_diff = 0
  exact h_rec'

theorem NoRowErr_cross {sch_l : Schema n} {sch_r : Schema m}
    {l : Collection sch_l} {r : Collection sch_r}
    (hl : NoRowErr l) (hr : NoRowErr r) :
    NoRowErr (cross l r) := by
  intro rec hrec
  unfold cross at hrec
  rw [List.mem_flatMap] at hrec
  obtain ⟨recL, hL_mem, hrec_in⟩ := hrec
  rw [List.mem_map] at hrec_in
  obtain ⟨recR, hR_mem, hrec_eq⟩ := hrec_in
  have hL := hl recL hL_mem
  have hR := hr recR hR_mem
  show rec.err_diff = 0
  rw [← hrec_eq]
  show recL.diff * recR.err_diff
       + recL.err_diff * recR.diff
       + recL.err_diff * recR.err_diff = 0
  rw [hL, hR]; simp

/-! ## NoRowErr through filter

`filter` migrates `rec.diff` to `rec.err_diff` whenever the
predicate evaluates to `.err _`. So `NoRowErr (filter p s)`
requires the predicate to never err per-row.

Two forms:

* `NoRowErr_filter` — directly takes per-row `¬(eval rec.row p).IsErr`.
* `NoRowErr_filter_of_might_error_false` — derives the per-row
  premise from the static analyzer `Expr.might_error p = false`
  plus per-row `EnvErrFree`. The schema-driven discharge via
  `EnvErrFree_of_RowSatisfies` lives in `Mz/OutputType.lean`. -/

theorem NoRowErr_filter {sch : Schema n} (p : Expr sch .bool)
    {s : Collection sch}
    (hNoErr : NoRowErr s)
    (hPredSafe : ∀ rec ∈ s, ¬(eval rec.row p).IsErr) :
    NoRowErr (filter p s) := by
  intro rec hrec
  unfold filter at hrec
  rw [List.mem_map] at hrec
  obtain ⟨rec', hrec'_mem, hrec'_eq⟩ := hrec
  have h_err' := hNoErr rec' hrec'_mem
  have h_psafe := hPredSafe rec' hrec'_mem
  show rec.err_diff = 0
  rw [← hrec'_eq]
  show (filterOne p rec').err_diff = 0
  unfold filterOne
  cases hev : eval rec'.row p with
  | bool b =>
    cases b
    · show rec'.err_diff = 0; exact h_err'
    · show rec'.err_diff = 0; exact h_err'
  | null => show rec'.err_diff = 0; exact h_err'
  | err e =>
    exfalso
    apply h_psafe
    rw [hev]; trivial

theorem NoRowErr_filter_of_might_error_false {sch : Schema n}
    (p : Expr sch .bool) {s : Collection sch}
    (hNoErr : NoRowErr s)
    (hPred : Expr.might_error p = false)
    (hRows : ∀ rec ∈ s, EnvErrFree rec.row) :
    NoRowErr (filter p s) :=
  NoRowErr_filter p hNoErr
    (fun rec hrec => might_error_sound rec.row (hRows rec hrec) p hPred)

/-! ## Filter / cross pushdown — counterexample

The canonical soundness gap for `filter (cross l r) = cross (filter
l) r`. cross's err-diff is bilinear in data and err multiplicities:
`(dL, eL) * (dR, eR) = (dL · dR, dL · eR + eL · dR + eL · eR)`.

A filter that zeroes `dL` before the cross drops the `dL · eR`
term that the post-cross filter preserves. Witnessed concretely
on `Schema.free 0` (empty schemas), where `Env` is the unique
empty function, with a `.lit (.bool false)` predicate. -/

theorem filter_cross_pushdown_left_unsound :
    ∃ (n m : Nat) (sch_l : Schema n) (sch_r : Schema m)
      (l : Collection sch_l) (r : Collection sch_r)
      (p_comb : Expr (Schema.append sch_l sch_r) .bool)
      (p_left : Expr sch_l .bool),
      filter p_comb (cross l r) ≠
      cross (filter p_left l) r := by
  refine ⟨0, 0, Schema.free 0, Schema.free 0,
    [{ row := fun i => i.elim0, diff := 1, err_diff := 0 }],
    [{ row := fun i => i.elim0, diff := 0, err_diff := 1 }],
    .lit (.bool false), .lit (.bool false), ?_⟩
  intro h
  -- LHS computes:
  -- cross [recL] [recR] = [crossOne recL recR]
  --   crossOne.diff = 1 * 0 = 0
  --   crossOne.err_diff = 1*1 + 0*0 + 0*1 = 1
  -- filter .bool-false: data=0 → diff=0, err_diff=1.
  -- So LHS = [{row=..., diff=0, err_diff=1}].
  --
  -- RHS computes:
  -- filter .bool-false [recL] = [{row=recL.row, diff=0, err_diff=0}]
  -- cross [...] [recR]:
  --   crossOne.diff = 0 * 0 = 0
  --   crossOne.err_diff = 0*1 + 0*0 + 0*1 = 0
  -- So RHS = [{row=..., diff=0, err_diff=0}].
  --
  -- LHS.head.err_diff = 1, RHS.head.err_diff = 0. Contradiction.
  have hLHS : (filter (.lit (.bool false))
    (cross [({ row := fun i => i.elim0, diff := 1, err_diff := 0 } :
              Update (Schema.free 0))]
           [({ row := fun i => i.elim0, diff := 0, err_diff := 1 } :
              Update (Schema.free 0))])).head?.map (·.err_diff)
    = some 1 := by decide
  have hRHS : (cross
    (filter (.lit (.bool false))
      [({ row := fun i => i.elim0, diff := 1, err_diff := 0 } :
         Update (Schema.free 0))])
    [({ row := fun i => i.elim0, diff := 0, err_diff := 1 } :
       Update (Schema.free 0))]).head?.map (·.err_diff)
    = some 0 := by decide
  rw [h] at hLHS
  rw [hLHS] at hRHS
  cases hRHS

/-! ## Filter / cross pushdown — strict recovery (open)

Soundness recovery under `NoRowErr l ∧ NoRowErr r`. The
counterexample above exploits the bilinear err-diff rule's mixed
terms `d_L · e_R` and `e_L · d_R`; under both sides being
`NoRowErr`, every cross-multiplied err-diff term vanishes
pre-cross, so the pre- and post-cross filter agree on data.
The proof requires a substitution-aware predicate lift
(`p_comb (recL ++ recR) ≃ p_left recL`) plus pointwise inversion
of `filterOne` under each `eval` case.

The mechanization is blocked on iota reduction of `filterOne`'s
match auxiliary after `cases hev : eval recL.row p_left`. Tried:
`unfold filterOne`, `simp [filterOne]`, `change`, per-case
rewrite lemmas, `generalize` + `clear`, marking `filterOne`
`@[reducible]`. None expose the iota-reduced match arm. The
match scrutinizing `Datum.err e✝` (post-cases substitution)
stays opaque even after `rw [hev]`. The fix likely requires
either marking the auto-generated `filterOne.match_1` reducible
(separate from `filterOne` itself), routing via the auto-gen
`filterOne.eq_*` equation lemmas via `simp only`, or rewriting
`filterOne` to dispatch via explicit `Datum.casesOn`. Left as
follow-up.

The counterexample (`filter_cross_pushdown_left_unsound`) remains
the load-bearing demonstration of the un-recovered direction. -/

/-! ## Row refinement

Pointwise refinement on `Env sch` cells. Lifts `Datum.refines` to
the row level. -/

/-- A row refines another iff every cell does. -/
def Row.refines {n : Nat} {sch : Schema n} (a b : Env sch) : Prop :=
  ∀ i : Fin n, (a i).refines (b i)

theorem Row.refines_refl {n : Nat} {sch : Schema n} (a : Env sch) :
    Row.refines a a := fun _ => Datum.refines_refl _

theorem Row.refines_trans {n : Nat} {sch : Schema n} {a b c : Env sch}
    (h₁ : Row.refines a b) (h₂ : Row.refines b c) :
    Row.refines a c := fun i => Datum.refines_trans (h₁ i) (h₂ i)

theorem Row.refines_of_eq {n : Nat} {sch : Schema n} {a b : Env sch}
    (h : a = b) : Row.refines a b := h ▸ Row.refines_refl a

/-! ## Update refinement (Smyth-style)

`a.refines b` iff:
* rows refine pointwise
* data multiplicities agree
* err multiplicity is at least as large on the LHS (errors are
  "more defined" on the bottom). -/

def _root_.Mz.Update.refines {n : Nat} {sch : Schema n} (a b : Update sch) : Prop :=
  Row.refines a.row b.row ∧ a.diff = b.diff ∧ a.err_diff ≥ b.err_diff

theorem _root_.Mz.Update.refines_refl {n : Nat} {sch : Schema n} (a : Update sch) :
    a.refines a :=
  ⟨Row.refines_refl _, rfl, Int.le_refl _⟩

theorem _root_.Mz.Update.refines_trans {n : Nat} {sch : Schema n}
    {a b c : Update sch} (h₁ : a.refines b) (h₂ : b.refines c) :
    a.refines c := by
  obtain ⟨hr₁, hd₁, he₁⟩ := h₁
  obtain ⟨hr₂, hd₂, he₂⟩ := h₂
  exact ⟨Row.refines_trans hr₁ hr₂, hd₁.trans hd₂, le_trans he₂ he₁⟩

theorem _root_.Mz.Update.refines_of_eq {n : Nat} {sch : Schema n}
    {a b : Update sch} (h : a = b) : a.refines b :=
  h ▸ Update.refines_refl a

/-! ## Collection refinement

Lift via `List.Forall₂` — pointwise update refinement at matching
positions. Collections refine when they have the same length and
each index-aligned pair refines. -/

def refines {n : Nat} {sch : Schema n} :
    Collection sch → Collection sch → Prop :=
  List.Forall₂ Update.refines

theorem refines_refl {n : Nat} {sch : Schema n}
    (s : Collection sch) : refines s s := by
  induction s with
  | nil => exact List.Forall₂.nil
  | cons hd tl ih => exact List.Forall₂.cons (Update.refines_refl hd) ih

theorem refines_trans {n : Nat} {sch : Schema n}
    {a b c : Collection sch}
    (h₁ : refines a b) (h₂ : refines b c) :
    refines a c := by
  induction a generalizing b c with
  | nil =>
    cases h₁
    cases h₂
    exact List.Forall₂.nil
  | cons hd tl ih =>
    cases h₁ with
    | cons hh ht =>
      cases h₂ with
      | cons hh' ht' =>
        exact List.Forall₂.cons (Update.refines_trans hh hh') (ih ht ht')

theorem refines_of_eq {n : Nat} {sch : Schema n}
    {a b : Collection sch} (h : a = b) : refines a b :=
  h ▸ refines_refl a

end Collection

/-! ## Consolidated equivalence (`Collection.Equiv`)

`Collection sch = List (Update sch)` under `=` distinguishes order
and unconsolidated pairs that the user observes as the same.
`Collection.Equiv` is the smallest equivalence relation closed
under permutation, same-row diff consolidation, and zero-update
dropping.

Demonstrators:

* `unionAll_comm_equiv` — `unionAll a b ≈ unionAll b a` via perm.
* `negate_unionAll_self` — `unionAll (negate s) s ≈ []` via perm +
  merge + drop_zero. The load-bearing retraction identity that
  strict `=` cannot witness (`[(r, 1, 0), (r, -1, 0)] ≠ []` under
  `=`). -/

namespace Collection

variable {n : Nat}

/-- Smallest equivalence on collections closed under permutation,
same-row diff consolidation, and zero-update dropping. -/
inductive Equiv {sch : Schema n} :
    Collection sch → Collection sch → Prop
  | refl (s : Collection sch) : Equiv s s
  | symm {a b : Collection sch} : Equiv a b → Equiv b a
  | trans {a b c : Collection sch} : Equiv a b → Equiv b c → Equiv a c
  | perm {a b : Collection sch} : a.Perm b → Equiv a b
  | merge (row : Env sch) (d₁ e₁ d₂ e₂ : Int) (rest : Collection sch) :
      Equiv
        (⟨row, d₁, e₁⟩ :: ⟨row, d₂, e₂⟩ :: rest)
        (⟨row, d₁ + d₂, e₁ + e₂⟩ :: rest)
  | drop_zero (row : Env sch) (rest : Collection sch) :
      Equiv (⟨row, 0, 0⟩ :: rest) rest

/-- `unionAll` is commutative under `Collection.Equiv` (perm only). -/
theorem unionAll_comm_equiv {sch : Schema n} (a b : Collection sch) :
    Equiv (unionAll a b) (unionAll b a) :=
  Equiv.perm List.perm_append_comm

/-- `negate s` cancels `s` under `Collection.Equiv`. Pair every
update with its negation (perm), merge to a zero update, drop. -/
theorem negate_unionAll_self {sch : Schema n} (s : Collection sch) :
    Equiv (unionAll (negate s) s) ([] : Collection sch) := by
  induction s with
  | nil => exact Equiv.refl []
  | cons rec rest ih =>
    rcases rec with ⟨row, d, e⟩
    show Equiv
      (⟨row, -d, -e⟩ :: negate rest ++ ⟨row, d, e⟩ :: rest)
      ([] : Collection sch)
    have hperm :
        (⟨row, -d, -e⟩ :: negate rest ++ ⟨row, d, e⟩ :: rest :
          Collection sch).Perm
        (⟨row, -d, -e⟩ :: ⟨row, d, e⟩ :: (negate rest ++ rest)) := by
      show (⟨row, -d, -e⟩ :: (negate rest ++ ⟨row, d, e⟩ :: rest)).Perm
           (⟨row, -d, -e⟩ :: ⟨row, d, e⟩ :: (negate rest ++ rest))
      exact List.Perm.cons _ List.perm_middle
    refine Equiv.trans (Equiv.perm hperm) ?_
    refine Equiv.trans (Equiv.merge row (-d) (-e) d e (negate rest ++ rest)) ?_
    have h_d : (-d : Int) + d = 0 := by omega
    have h_e : (-e : Int) + e = 0 := by omega
    rw [h_d, h_e]
    exact Equiv.trans (Equiv.drop_zero row (negate rest ++ rest)) ih

end Collection

end Mz
