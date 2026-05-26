import Mz.Indexed.Schema
import Mz.Indexed.Eval

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

namespace Mz.Indexed

open Mz

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

/-! ## NoRowErr precondition -/

/-- An update is `NoRowErr` when its row-error multiplicity is
zero. Operational regimes (sources known to produce only valid
or invalid rows) discharge this on inputs. -/
def _root_.Mz.Indexed.Update.NoRowErr {sch : Schema n} (rec : Update sch) : Prop :=
  rec.err_diff = 0

/-- A collection has `NoRowErr` when every update does. -/
def NoRowErr {sch : Schema n} (s : Collection sch) : Prop :=
  ∀ rec ∈ s, Update.NoRowErr rec

theorem NoRowErr_nil {sch : Schema n} : NoRowErr ([] : Collection sch) := by
  unfold NoRowErr
  intro _ hmem; cases hmem

end Collection

end Mz.Indexed
