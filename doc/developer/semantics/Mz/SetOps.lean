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

## `EXCEPT ALL`

Differential dataflow's signed-diff representation makes
bag-difference natural: negate every diff of the right-hand input
and union the result with the left. Consolidation merges per
carrier, summing per-bucket diffs:

* Carrier in L only: output diff = +L.
* Carrier in R only: output diff = -R.
* Carrier in both:   output diff = L - R.

Negative output diffs encode "this carrier has `n` fewer copies in
the result than in the input" — the standard retraction signal in
differential dataflow. Bag-semantics `EXCEPT ALL` clamps negatives
to zero; that clamp is a separate normalize step (sign
normalization), deferred to a later iteration. The skeleton states
the signed flavor.

## Out of scope

`INTERSECT ALL` requires per-carrier `min(L, R)` on diffs, which
cannot be expressed via the additive / multiplicative / negation
primitives alone. Landing it is additive against the current
proofs but needs a new diff combinator.
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

/-! ## Negation

Negate every diff in the stream. `.error` diffs absorb the negation
(the marker survives — a collection-scoped error cannot be
retracted away); `.val n` diffs become `.val (-n)`. -/

def UnifiedStream.negate (us : UnifiedStream) : UnifiedStream :=
  us.map fun ud => (ud.1, -ud.2)

theorem UnifiedStream.negate_length (us : UnifiedStream) :
    (UnifiedStream.negate us).length = us.length :=
  List.length_map _

/-- Double negation is the identity (lifted from `Int.neg_neg`
through `DiffWithError.neg_neg_int`). -/
theorem UnifiedStream.negate_negate (us : UnifiedStream) :
    UnifiedStream.negate (UnifiedStream.negate us) = us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    show ((uc, - -d) :: UnifiedStream.negate (UnifiedStream.negate tl))
          = (uc, d) :: tl
    rw [ih, DiffWithError.neg_neg_int]

/-- `.error` diffs survive negation; the carrier is preserved. -/
theorem UnifiedStream.negate_preserves_error_diff
    (us : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ us) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.negate us := by
  unfold UnifiedStream.negate
  rw [List.mem_map]
  refine ⟨(uc, DiffWithError.error), h, ?_⟩
  show (uc, -DiffWithError.error) = (uc, DiffWithError.error)
  rw [DiffWithError.neg_error]

/-- Negation preserves the `.val` slice: an all-`.val` input yields
an all-`.val` output (with values negated). -/
theorem UnifiedStream.negate_no_error
    (us : UnifiedStream)
    (h : ∀ x ∈ us, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.negate us, ∃ n : Int, x.2 = DiffWithError.val n := by
  intro x hMem
  unfold UnifiedStream.negate at hMem
  obtain ⟨y, hY, hEq⟩ := List.mem_map.mp hMem
  obtain ⟨n, hN⟩ := h y hY
  refine ⟨-n, ?_⟩
  rw [← hEq]
  show -y.2 = DiffWithError.val (-n)
  rw [hN]
  rfl

/-! ## `EXCEPT ALL` (signed-diff flavor)

`exceptAll L R = consolidate (unionAll L (negate R))`. Output diffs
are signed: positive for net-present carriers, negative for
net-absent. Bag-semantics `EXCEPT ALL` clamps negative diffs to
zero in a follow-up normalize step. -/

def UnifiedStream.exceptAll (l r : UnifiedStream) : UnifiedStream :=
  UnifiedStream.consolidate
    (UnifiedStream.unionAll l (UnifiedStream.negate r))

theorem UnifiedStream.exceptAll_length_le (l r : UnifiedStream) :
    (UnifiedStream.exceptAll l r).length ≤ l.length + r.length := by
  show (UnifiedStream.consolidate
          (UnifiedStream.unionAll l (UnifiedStream.negate r))).length
        ≤ l.length + r.length
  have hCons := UnifiedStream.consolidate_length_le
                  (UnifiedStream.unionAll l (UnifiedStream.negate r))
  have hConcat : (UnifiedStream.unionAll l (UnifiedStream.negate r)).length
                = l.length + (UnifiedStream.negate r).length :=
    UnifiedStream.unionAll_length _ _
  have hNeg : (UnifiedStream.negate r).length = r.length :=
    UnifiedStream.negate_length r
  rw [hNeg] at hConcat
  exact hConcat ▸ hCons

/-- `.error` diff on the left input survives `exceptAll`. -/
theorem UnifiedStream.exceptAll_preserves_error_diff_left
    (l r : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ l) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.exceptAll l r :=
  UnifiedStream.consolidate_preserves_error _ uc
    (UnifiedStream.unionAll_preserves_error_diff_left l _ uc h)

/-- `.error` diff on the right input also survives `exceptAll`:
negation is absorbed by `.error`, so the negated right-hand input
still carries the marker. -/
theorem UnifiedStream.exceptAll_preserves_error_diff_right
    (l r : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ r) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.exceptAll l r :=
  UnifiedStream.consolidate_preserves_error _ uc
    (UnifiedStream.unionAll_preserves_error_diff_right l _ uc
      (UnifiedStream.negate_preserves_error_diff r uc h))

/-- All-`.val` inputs yield all-`.val` outputs. -/
theorem UnifiedStream.exceptAll_no_error
    (l r : UnifiedStream)
    (hL : ∀ x ∈ l, ∃ n : Int, x.2 = DiffWithError.val n)
    (hR : ∀ x ∈ r, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.exceptAll l r,
      ∃ n : Int, x.2 = DiffWithError.val n :=
  UnifiedStream.consolidate_no_error _
    (UnifiedStream.unionAll_no_error l _ hL
      (UnifiedStream.negate_no_error r hR))

end Mz
