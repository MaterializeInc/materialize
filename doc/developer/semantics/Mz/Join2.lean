import Mz.UnifiedStream2

/-!
# Joins on `UnifiedStream2`

Two-input relational join on the new diff-aware stream
(`Mz/UnifiedStream2.lean`). The cartesian product `cross l r` is the
building block; `join pred l r` filters the product through a join
predicate.

The carrier is now plain `Row` (no `.err` constructor), so the
combinator collapses to ordinary list concatenation: there is no
carrier-err to absorb, no left-wins rule to apply. Error propagation
moves entirely into the diff component:

* row-scoped errs ride along inside each side's `Diff.errs`; on the
  product, `Diff.mul` carries them through linearly (see
  `Mz/DiffErrCount.lean`);
* collection-scoped errs live in `DiffWithGlobal.global`, which
  absorbs on multiplication via `DiffWithGlobal.global_mul_left` /
  `global_mul_right`.

`cross` makes no commitment to row schema beyond list concatenation,
matching the old `Mz/Join.lean` `cross` on the `.row/.row` branch.
Schema-aware joins (equi-joins on named columns) would lift to this
with a column-substitution layer.
-/

namespace Mz

namespace UnifiedStream2

/-- Cartesian product of two new unified streams. For each pair
`((rL, dL), (rR, dR))`:
* row is `rL ++ rR` (plain concatenation; no carrier-err to handle);
* diff is `dL * dR` via `DiffWithGlobal.mul`, which uses `Diff.mul`
  on the `val`/`val` case and absorbs on `.global`. -/
def cross (l r : UnifiedStream2) : UnifiedStream2 :=
  l.flatMap fun rd =>
    r.map fun rd' => (rd.1 ++ rd'.1, rd.2 * rd'.2)

/-- Theta-join: cross product filtered by a predicate. The predicate
evaluates against the concatenated row; existing
`UnifiedStream2.filter` semantics apply (predicate `.err` zeros the
valid count and routes the failure into the diff's err-count). -/
def join (pred : Expr) (l r : UnifiedStream2) : UnifiedStream2 :=
  filter pred (cross l r)

/-! ## Empty cases -/

theorem cross_nil_left (r : UnifiedStream2) :
    cross [] r = [] := rfl

theorem cross_nil_right (l : UnifiedStream2) :
    cross l [] = [] := by
  induction l with
  | nil => rfl
  | cons _ tl _ih => simp [cross, List.map_nil, List.flatMap_cons]

/-! ### Reduction lemmas on the left input

Named per-list-shape reductions so downstream proofs cite these
instead of unfolding `flatMap` inline. -/

theorem cross_append_left (a b r : UnifiedStream2) :
    cross (a ++ b) r = cross a r ++ cross b r := by
  show (a ++ b).flatMap _ = a.flatMap _ ++ b.flatMap _
  exact List.flatMap_append

theorem cross_cons_left
    (hd : Row × DiffWithGlobal) (tl r : UnifiedStream2) :
    cross (hd :: tl) r
      = (r.map (fun rd => (hd.1 ++ rd.1, hd.2 * rd.2)))
          ++ cross tl r := by
  show (hd :: tl).flatMap _ = _ ++ tl.flatMap _
  simp [List.flatMap_cons]

/-! ## Cardinality -/

/-- Cross product cardinality. `cross l r` produces exactly one
output record per `(l, r)` pair. Unlike the old `Mz/Join.lean`,
there is no carrier-err short-circuit; the count argument is the
same combinatorial induction. -/
theorem cross_length (l r : UnifiedStream2) :
    (cross l r).length = l.length * r.length := by
  induction l with
  | nil => simp [cross]
  | cons hd tl ih =>
    show (cross (hd :: tl) r).length = (tl.length + 1) * r.length
    rw [Nat.succ_mul]
    show ((hd :: tl).flatMap fun ld =>
            r.map fun rd => (ld.1 ++ rd.1, ld.2 * rd.2)).length
        = tl.length * r.length + r.length
    rw [List.flatMap_cons, List.length_append, List.length_map]
    show r.length + (cross tl r).length = tl.length * r.length + r.length
    rw [ih]
    exact Nat.add_comm _ _

end UnifiedStream2

end Mz
