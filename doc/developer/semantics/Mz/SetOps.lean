import Mz.UnifiedStream
import Mz.UnifiedConsolidate
import Mz.DiffSemiring

/-!
# Set operations on `UnifiedStream`

`UNION ALL`, `UNION` derived via consolidation. The bag-multiplicity
flavors (`INTERSECT ALL`, `EXCEPT ALL`) require subtraction-aware
multiplicity arithmetic and are deferred to a later iteration.

## `UNION ALL`

Bag union is concatenation. Each input record passes through with
its diff unchanged. Collection-scoped errors propagate via the
union of the inputs' error-bearing records; row-scoped errors flow
through carriers; ordinary `.val` diffs preserve.

## `UNION`

Set union is `UNION ALL` followed by `consolidate` — duplicate
carriers fold into one record whose diff is the sum of the input
diffs. The semiring's `.error` absorber ensures collection-scoped
errors in either input become collection-scoped errors per carrier
in the output. This file gives the operator and lifts the existing
`UnifiedConsolidate` theorems to the union setting.

## Out of scope

Bag-difference flavors (`INTERSECT ALL`, `EXCEPT ALL`) require
counting multiplicities and producing diffs that subtract.
Differential dataflow encodes these via `consolidate` with negative
diffs and per-carrier clamping, which the diff-semiring's `Int`
slice supports but the skeleton's `DiffWithError` semiring does not
yet expose. Landing them is additive against the current proofs.
-/

namespace Mz

/-! ## `UNION ALL` -/

/-- Bag union: concatenate two unified streams. Order is left
input first, then right input. Every record passes through with
its diff unchanged. -/
def UnifiedStream.unionAll (l r : UnifiedStream) : UnifiedStream :=
  l ++ r

/-! ### Reduction lemmas -/

theorem UnifiedStream.unionAll_nil_left (r : UnifiedStream) :
    UnifiedStream.unionAll [] r = r := List.nil_append r

theorem UnifiedStream.unionAll_nil_right (l : UnifiedStream) :
    UnifiedStream.unionAll l [] = l := List.append_nil l

theorem UnifiedStream.unionAll_length (l r : UnifiedStream) :
    (UnifiedStream.unionAll l r).length = l.length + r.length :=
  List.length_append

theorem UnifiedStream.unionAll_assoc (a b c : UnifiedStream) :
    UnifiedStream.unionAll (UnifiedStream.unionAll a b) c
      = UnifiedStream.unionAll a (UnifiedStream.unionAll b c) :=
  List.append_assoc a b c

/-! ### Diff propagation

`.error` diffs on either side survive the union. The companion
no-error preservation states that an all-`.val` pair of inputs
yields an all-`.val` output. -/

theorem UnifiedStream.unionAll_preserves_error_diff_left
    (l r : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ l) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.unionAll l r :=
  List.mem_append.mpr (Or.inl h)

theorem UnifiedStream.unionAll_preserves_error_diff_right
    (l r : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ r) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.unionAll l r :=
  List.mem_append.mpr (Or.inr h)

theorem UnifiedStream.unionAll_no_error
    (l r : UnifiedStream)
    (hL : ∀ x ∈ l, ∃ n : Int, x.2 = DiffWithError.val n)
    (hR : ∀ x ∈ r, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.unionAll l r,
      ∃ n : Int, x.2 = DiffWithError.val n := by
  intro x hMem
  rcases List.mem_append.mp hMem with hL' | hR'
  · exact hL x hL'
  · exact hR x hR'

/-! ## `UNION` (set semantics)

Set union: union-all then consolidate. The duplicate-carrier
buckets fold into single records via diff addition. The semiring
laws in `Mz/DiffSemiring.lean` carry the absorption: any `.error`
diff in either input survives into the corresponding bucket of
the output. -/

def UnifiedStream.union (l r : UnifiedStream) : UnifiedStream :=
  UnifiedStream.consolidate (UnifiedStream.unionAll l r)

theorem UnifiedStream.union_length_le (l r : UnifiedStream) :
    (UnifiedStream.union l r).length ≤ l.length + r.length := by
  show (UnifiedStream.consolidate (UnifiedStream.unionAll l r)).length
        ≤ l.length + r.length
  have hCons := UnifiedStream.consolidate_length_le
                  (UnifiedStream.unionAll l r)
  have hConcat : (UnifiedStream.unionAll l r).length = l.length + r.length :=
    UnifiedStream.unionAll_length l r
  exact hConcat ▸ hCons

/-- `.error` diff on the left input survives the set union for its
carrier. The proof chains the `unionAll` lift with the existing
`consolidate_preserves_error`. -/
theorem UnifiedStream.union_preserves_error_diff_left
    (l r : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ l) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.union l r :=
  UnifiedStream.consolidate_preserves_error _ uc
    (UnifiedStream.unionAll_preserves_error_diff_left l r uc h)

theorem UnifiedStream.union_preserves_error_diff_right
    (l r : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ r) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.union l r :=
  UnifiedStream.consolidate_preserves_error _ uc
    (UnifiedStream.unionAll_preserves_error_diff_right l r uc h)

/-- All-`.val` diffs on both inputs yield all-`.val` diffs on the
output. -/
theorem UnifiedStream.union_no_error
    (l r : UnifiedStream)
    (hL : ∀ x ∈ l, ∃ n : Int, x.2 = DiffWithError.val n)
    (hR : ∀ x ∈ r, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.union l r,
      ∃ n : Int, x.2 = DiffWithError.val n :=
  UnifiedStream.consolidate_no_error _
    (UnifiedStream.unionAll_no_error l r hL hR)

end Mz
