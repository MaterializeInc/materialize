import Mz.UnifiedStream2

/-!
# Set operations on `UnifiedStream2`

Parallel to `Mz/SetOps.lean`, but specialized to the new
`UnifiedStream2` encoding from `Mz/UnifiedStream2.lean`, where the
carrier is `Row` and the diff is `DiffWithGlobal` (a `Diff` payload
plus an absorbing `global` marker).

This file lands only `UNION ALL`, signed negation, and the signed
flavor of `EXCEPT ALL`. The remaining clamp/distinct/intersect
flavors require inspecting `Diff.val` through the `DiffWithGlobal`
wrapper and are deferred to a follow-up.

## `UNION ALL`

Bag union is list concatenation. Each record passes through with its
diff (whether `.val _` or `.global`) unchanged. Both row-scoped errs
(carried inside `Diff.errs`) and the collection-scoped `.global`
marker propagate verbatim through the union.

## Negation

Pointwise diff negation through `DiffWithGlobal`'s `Neg` instance.
`.val d` becomes `.val (-d)`; `.global` absorbs negation per
`DiffWithGlobal.neg_global`, so a collection-scoped error survives a
sign flip — it cannot be retracted away.

## `EXCEPT ALL`

Differential dataflow's signed-diff bag-difference: negate every
diff of the right input and concatenate. The clamp-to-nonnegative
step that pure bag semantics demands is deferred along with the
other clamp operators; this file states the signed flavor only.
-/

namespace Mz

namespace UnifiedStream2

/-! ## `UNION ALL` -/

/-- Bag union: concatenate two unified streams. Order is left input
first, then right input. Every record passes through with its diff
unchanged. -/
def unionAll (l r : UnifiedStream2) : UnifiedStream2 := l ++ r

/-! ### Reduction lemmas -/

theorem unionAll_nil_left (r : UnifiedStream2) :
    unionAll [] r = r := List.nil_append r

theorem unionAll_nil_right (l : UnifiedStream2) :
    unionAll l [] = l := List.append_nil l

theorem unionAll_assoc (a b c : UnifiedStream2) :
    unionAll (unionAll a b) c = unionAll a (unionAll b c) :=
  List.append_assoc a b c

/-! ## Negation

Negate every diff in the stream via `DiffWithGlobal`'s `Neg`. `.val
d` becomes `.val (-d)`; `.global` is absorbing under negation. -/

def negate (us : UnifiedStream2) : UnifiedStream2 :=
  us.map fun rd => (rd.1, -rd.2)

/-! ### Reduction lemmas -/

theorem negate_nil : negate [] = [] := rfl

/-- `negate` distributes over `++`. -/
theorem negate_append (a b : UnifiedStream2) :
    negate (a ++ b) = negate a ++ negate b := by
  show (a ++ b).map _ = a.map _ ++ b.map _
  exact List.map_append

/-- Double negation is the identity. Lifted from
`DiffWithGlobal.neg_neg`. -/
theorem negate_negate (us : UnifiedStream2) :
    negate (negate us) = us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨r, d⟩ := hd
    show ((r, - -d) :: negate (negate tl)) = (r, d) :: tl
    rw [ih, DiffWithGlobal.neg_neg]

/-! ## `EXCEPT ALL` (signed)

Bag difference via signed diffs: left ∪ (negation of right). The
clamp-to-nonnegative normalize step is deferred — this is the signed
flavor only. -/

def exceptAll (l r : UnifiedStream2) : UnifiedStream2 :=
  unionAll l (negate r)

/-! ### Reduction lemmas -/

theorem exceptAll_nil_right (l : UnifiedStream2) :
    exceptAll l [] = l := by
  show unionAll l (negate []) = l
  rw [negate_nil, unionAll_nil_right]

end UnifiedStream2

end Mz
