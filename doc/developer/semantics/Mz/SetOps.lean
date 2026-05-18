import Mz.UnifiedStream
import Mz.UnifiedConsolidate
import Mz.DiffSemiring
import Mz.Join

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

theorem UnifiedStream.union_nil_left (r : UnifiedStream) :
    UnifiedStream.union [] r = UnifiedStream.consolidate r := by
  show UnifiedStream.consolidate (UnifiedStream.unionAll [] r)
      = UnifiedStream.consolidate r
  rw [UnifiedStream.unionAll_nil_left]

theorem UnifiedStream.union_nil_right (l : UnifiedStream) :
    UnifiedStream.union l [] = UnifiedStream.consolidate l := by
  show UnifiedStream.consolidate (UnifiedStream.unionAll l [])
      = UnifiedStream.consolidate l
  rw [UnifiedStream.unionAll_nil_right]

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

/-- `negate` distributes over `++`. The dedicated map-append
reduction so downstream proofs cite a single lemma. -/
theorem UnifiedStream.negate_append (a b : UnifiedStream) :
    UnifiedStream.negate (a ++ b)
      = UnifiedStream.negate a ++ UnifiedStream.negate b := by
  show (a ++ b).map _ = a.map _ ++ b.map _
  exact List.map_append

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

-- `exceptAll_nil_left` is proven later, after `negate_consolidate`.

/-- Empty right input: `exceptAll l [] = consolidate l`. The
negation of the empty stream is empty, and `unionAll l [] = l`. -/
theorem UnifiedStream.exceptAll_nil_right (l : UnifiedStream) :
    UnifiedStream.exceptAll l [] = UnifiedStream.consolidate l := by
  show UnifiedStream.consolidate
        (UnifiedStream.unionAll l (UnifiedStream.negate []))
      = UnifiedStream.consolidate l
  show UnifiedStream.consolidate (UnifiedStream.unionAll l [])
      = UnifiedStream.consolidate l
  rw [UnifiedStream.unionAll_nil_right]

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

/-! ## Sign normalization

`clampPositive` drops records whose diff has consolidated to a
non-positive `.val` (zero or negative multiplicity); `.error`
records survive unconditionally — the absorbing marker cannot be
filtered away without violating the diff semiring. This is the
post-pass that turns the signed-diff `exceptAll` into the
bag-semantics `EXCEPT ALL`. -/

@[inline] private def isPositiveDiff : DiffWithError Int → Bool
  | .error => true
  | .val n => decide (0 < n)

/-- Drop records whose diff is `.val 0` or `.val n` with `n < 0`.
`.error` records pass through. The dot-notation here resolves to
`List.filter`; `UnifiedStream.filter` is the predicate-driven
operator in `Mz/UnifiedStream.lean`, which is a different
operation. -/
def UnifiedStream.clampPositive (us : UnifiedStream) : UnifiedStream :=
  List.filter (fun ud => isPositiveDiff ud.2) us

theorem UnifiedStream.clampPositive_length_le (us : UnifiedStream) :
    (UnifiedStream.clampPositive us).length ≤ us.length := by
  unfold UnifiedStream.clampPositive
  exact List.length_filter_le _ _

theorem UnifiedStream.clampPositive_preserves_error_diff
    (us : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ us) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.clampPositive us := by
  unfold UnifiedStream.clampPositive
  rw [List.mem_filter]
  refine ⟨h, ?_⟩
  show isPositiveDiff DiffWithError.error = true
  rfl

/-- `clampPositive` is idempotent. Filter twice = filter once. -/
theorem UnifiedStream.clampPositive_idem (us : UnifiedStream) :
    UnifiedStream.clampPositive (UnifiedStream.clampPositive us)
      = UnifiedStream.clampPositive us := by
  unfold UnifiedStream.clampPositive
  rw [List.filter_filter]
  congr 1
  funext ud
  exact Bool.and_self _

/-- `clampPositive` is filter-based, so no-error preservation is
trivial: surviving records keep their original diffs. -/
theorem UnifiedStream.clampPositive_no_error
    (us : UnifiedStream)
    (h : ∀ x ∈ us, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.clampPositive us,
      ∃ n : Int, x.2 = DiffWithError.val n := by
  intro x hMem
  unfold UnifiedStream.clampPositive at hMem
  exact h x (List.mem_filter.mp hMem).1

/-- The output of `clampPositive` never contains a `.val n` with
`n ≤ 0`. Equivalently, every surviving `.val` diff is strictly
positive. -/
theorem UnifiedStream.clampPositive_only_positive
    (us : UnifiedStream) :
    ∀ x ∈ UnifiedStream.clampPositive us,
      (∃ n : Int, x.2 = DiffWithError.val n ∧ 0 < n)
      ∨ x.2 = DiffWithError.error := by
  intro x hMem
  unfold UnifiedStream.clampPositive at hMem
  have hAnd := List.mem_filter.mp hMem
  have hKeep : isPositiveDiff x.2 = true := hAnd.2
  match hD : x.2 with
  | .error => exact Or.inr rfl
  | .val n =>
    refine Or.inl ⟨n, rfl, ?_⟩
    rw [hD] at hKeep
    show 0 < n
    have hDec : decide (0 < n) = true := hKeep
    exact of_decide_eq_true hDec

/-! ## Bag-semantics `EXCEPT ALL`

`bagExceptAll = clampPositive ∘ exceptAll`. The signed-diff result
of `exceptAll` is post-processed to drop non-positive
multiplicities, producing the bag-semantics output: a carrier with
output multiplicity `max(L - R, 0)`. `.error` diffs survive the
clamp (collection-scoped errors cannot be sign-normalized away). -/

def UnifiedStream.bagExceptAll (l r : UnifiedStream) : UnifiedStream :=
  UnifiedStream.clampPositive (UnifiedStream.exceptAll l r)

theorem UnifiedStream.bagExceptAll_length_le (l r : UnifiedStream) :
    (UnifiedStream.bagExceptAll l r).length ≤ l.length + r.length :=
  Nat.le_trans
    (UnifiedStream.clampPositive_length_le _)
    (UnifiedStream.exceptAll_length_le l r)

theorem UnifiedStream.bagExceptAll_preserves_error_diff_left
    (l r : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ l) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.bagExceptAll l r :=
  UnifiedStream.clampPositive_preserves_error_diff _ uc
    (UnifiedStream.exceptAll_preserves_error_diff_left l r uc h)

theorem UnifiedStream.bagExceptAll_preserves_error_diff_right
    (l r : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ r) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.bagExceptAll l r :=
  UnifiedStream.clampPositive_preserves_error_diff _ uc
    (UnifiedStream.exceptAll_preserves_error_diff_right l r uc h)

/-- Every `.val` record in the bag-semantics output has strictly
positive multiplicity (zero / negative records are sign-normalized
away). -/
theorem UnifiedStream.bagExceptAll_only_positive
    (l r : UnifiedStream) :
    ∀ x ∈ UnifiedStream.bagExceptAll l r,
      (∃ n : Int, x.2 = DiffWithError.val n ∧ 0 < n)
      ∨ x.2 = DiffWithError.error :=
  UnifiedStream.clampPositive_only_positive _

/-- `bagExceptAll l [] = clampPositive (consolidate l)`. Trivial
composition of `exceptAll_nil_right` and `bagExceptAll`'s
definition. -/
theorem UnifiedStream.bagExceptAll_nil_right (l : UnifiedStream) :
    UnifiedStream.bagExceptAll l []
      = UnifiedStream.clampPositive (UnifiedStream.consolidate l) := by
  show UnifiedStream.clampPositive (UnifiedStream.exceptAll l [])
      = UnifiedStream.clampPositive (UnifiedStream.consolidate l)
  rw [UnifiedStream.exceptAll_nil_right]

/-- All-`.val` inputs yield all-`.val` outputs through
`bagExceptAll`. -/
theorem UnifiedStream.bagExceptAll_no_error
    (l r : UnifiedStream)
    (hL : ∀ x ∈ l, ∃ n : Int, x.2 = DiffWithError.val n)
    (hR : ∀ x ∈ r, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.bagExceptAll l r,
      ∃ n : Int, x.2 = DiffWithError.val n :=
  UnifiedStream.clampPositive_no_error _
    (UnifiedStream.exceptAll_no_error l r hL hR)

/-! ## `DISTINCT`

Set semantics: collapse multiplicities so each carrier appears at
most once with `.val 1` (or `.error` if a collection-scoped error
existed for that carrier). `clampToOne` is the post-consolidation
pass — `.val n` with `n > 0` becomes `.val 1`, non-positive `.val`
is dropped, `.error` survives. `distinct = clampToOne ∘ consolidate`. -/

/-- Map positive multiplicities to one and drop non-positive
ones. `.error` survives. The recursive form is preferred over a
filter+map composition because it makes the per-output diff shape
visible to the structural-induction proofs below. -/
def UnifiedStream.clampToOne : UnifiedStream → UnifiedStream
  | []                    => []
  | (uc, .error) :: rest  => (uc, .error) :: UnifiedStream.clampToOne rest
  | (uc, .val n) :: rest  =>
    if 0 < n then (uc, .val 1) :: UnifiedStream.clampToOne rest
    else UnifiedStream.clampToOne rest

theorem UnifiedStream.clampToOne_nil :
    UnifiedStream.clampToOne [] = [] := rfl

theorem UnifiedStream.clampToOne_length_le (us : UnifiedStream) :
    (UnifiedStream.clampToOne us).length ≤ us.length := by
  induction us with
  | nil => exact Nat.le.refl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases d with
    | error =>
      simp only [UnifiedStream.clampToOne, List.length_cons]
      omega
    | val n =>
      simp only [UnifiedStream.clampToOne, List.length_cons]
      split
      · simp only [List.length_cons]; omega
      · omega

theorem UnifiedStream.clampToOne_preserves_error_diff
    (us : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ us) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.clampToOne us := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc₀, d₀⟩ := hd
    rcases List.mem_cons.mp h with hEq | hTail
    · have hUc : uc = uc₀ := (Prod.mk.injEq _ _ _ _).mp hEq |>.1
      have hD : (DiffWithError.error : DiffWithError Int) = d₀ :=
        (Prod.mk.injEq _ _ _ _).mp hEq |>.2
      subst hUc; subst hD
      show (uc, DiffWithError.error)
          ∈ ((uc, DiffWithError.error) :: UnifiedStream.clampToOne tl)
      exact List.mem_cons_self
    · cases d₀ with
      | error =>
        show (uc, DiffWithError.error)
            ∈ ((uc₀, DiffWithError.error) :: UnifiedStream.clampToOne tl)
        exact List.mem_cons_of_mem _ (ih hTail)
      | val n =>
        show (uc, DiffWithError.error)
            ∈ (if 0 < n
                then (uc₀, DiffWithError.val 1) :: UnifiedStream.clampToOne tl
                else UnifiedStream.clampToOne tl)
        split
        · exact List.mem_cons_of_mem _ (ih hTail)
        · exact ih hTail

/-- `clampToOne` is idempotent. After one pass every `.val` is
`.val 1` and every other diff is `.error`; the second pass
preserves both. -/
theorem UnifiedStream.clampToOne_idem (us : UnifiedStream) :
    UnifiedStream.clampToOne (UnifiedStream.clampToOne us)
      = UnifiedStream.clampToOne us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases d with
    | error =>
      show UnifiedStream.clampToOne
              ((uc, DiffWithError.error) :: UnifiedStream.clampToOne tl)
          = (uc, DiffWithError.error) :: UnifiedStream.clampToOne tl
      simp only [UnifiedStream.clampToOne]
      rw [ih]
    | val n =>
      simp only [UnifiedStream.clampToOne]
      split
      · rename_i hPos
        show UnifiedStream.clampToOne
                ((uc, DiffWithError.val 1) :: UnifiedStream.clampToOne tl)
            = (uc, DiffWithError.val 1) :: UnifiedStream.clampToOne tl
        simp only [UnifiedStream.clampToOne]
        rw [if_pos (by decide : (0 : Int) < 1)]
        rw [ih]
      · exact ih

/-- All-`.val` inputs yield all-`.val` outputs through `clampToOne`:
no `.error` is introduced (records with non-positive `.val` are
dropped, positive `.val` become `.val 1`). -/
theorem UnifiedStream.clampToOne_no_error
    (us : UnifiedStream)
    (h : ∀ x ∈ us, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.clampToOne us,
      ∃ n : Int, x.2 = DiffWithError.val n := by
  induction us with
  | nil => intro x hMem; exact absurd hMem List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hHd : ∃ n : Int, d = DiffWithError.val n :=
      h (uc, d) List.mem_cons_self
    have hTl : ∀ x ∈ tl, ∃ n : Int, x.2 = DiffWithError.val n :=
      fun x hMem => h x (List.mem_cons_of_mem _ hMem)
    obtain ⟨n, hN⟩ := hHd
    cases d with
    | error => exact absurd hN (by intro hEq; cases hEq)
    | val m =>
      intro x hMem
      have hMem' : x ∈ (if 0 < m
                        then (uc, DiffWithError.val 1) :: UnifiedStream.clampToOne tl
                        else UnifiedStream.clampToOne tl) := hMem
      split at hMem'
      · rcases List.mem_cons.mp hMem' with hHead | hTail
        · exact ⟨1, by rw [hHead]⟩
        · exact ih hTl x hTail
      · exact ih hTl x hMem'

/-- Every `.val` record in the output of `clampToOne` has
multiplicity exactly one. `.error` records pass through unchanged. -/
theorem UnifiedStream.clampToOne_only_one_or_error
    (us : UnifiedStream) :
    ∀ x ∈ UnifiedStream.clampToOne us,
      x.2 = DiffWithError.val 1 ∨ x.2 = DiffWithError.error := by
  induction us with
  | nil => intro x hMem; exact absurd hMem List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases d with
    | error =>
      intro x hMem
      have hMem' : x ∈ (uc, (DiffWithError.error : DiffWithError Int))
                    :: UnifiedStream.clampToOne tl := hMem
      rcases List.mem_cons.mp hMem' with hHead | hTail
      · exact Or.inr (by rw [hHead])
      · exact ih x hTail
    | val n =>
      intro x hMem
      have hMem' : x ∈ (if 0 < n
                        then ((uc, DiffWithError.val 1)
                              :: UnifiedStream.clampToOne tl)
                        else UnifiedStream.clampToOne tl) := hMem
      split at hMem'
      · rcases List.mem_cons.mp hMem' with hHead | hTail
        · exact Or.inl (by rw [hHead])
        · exact ih x hTail
      · exact ih x hMem'

/-! ### `distinct`

Pipeline: consolidate, then `clampToOne`. Each carrier appears at
most once in the output, with `.val 1` if it had positive net
multiplicity, or `.error` if a collection-scoped error existed. -/

def UnifiedStream.distinct (us : UnifiedStream) : UnifiedStream :=
  UnifiedStream.clampToOne (UnifiedStream.consolidate us)

theorem UnifiedStream.distinct_length_le (us : UnifiedStream) :
    (UnifiedStream.distinct us).length ≤ us.length :=
  Nat.le_trans
    (UnifiedStream.clampToOne_length_le _)
    (UnifiedStream.consolidate_length_le us)

theorem UnifiedStream.distinct_preserves_error_diff
    (us : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int)) ∈ us) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.distinct us :=
  UnifiedStream.clampToOne_preserves_error_diff _ uc
    (UnifiedStream.consolidate_preserves_error _ uc h)

theorem UnifiedStream.distinct_only_one_or_error (us : UnifiedStream) :
    ∀ x ∈ UnifiedStream.distinct us,
      x.2 = DiffWithError.val 1 ∨ x.2 = DiffWithError.error :=
  UnifiedStream.clampToOne_only_one_or_error _

/-- `clampPositive` is a no-op on the output of `clampToOne`:
every surviving record has `.val 1` (which is positive) or
`.error`, both of which `clampPositive` keeps. -/
theorem UnifiedStream.clampPositive_clampToOne (us : UnifiedStream) :
    UnifiedStream.clampPositive (UnifiedStream.clampToOne us)
      = UnifiedStream.clampToOne us := by
  unfold UnifiedStream.clampPositive
  apply List.filter_eq_self.mpr
  intro x hMem
  rcases UnifiedStream.clampToOne_only_one_or_error us x hMem with h | h
  · show isPositiveDiff x.2 = true
    rw [h]; rfl
  · show isPositiveDiff x.2 = true
    rw [h]; rfl

/-- All-`.val` inputs yield all-`.val` outputs through `distinct`.
Combined with `distinct_only_one_or_error`, the surviving diffs are
all `.val 1`. -/
theorem UnifiedStream.distinct_no_error
    (us : UnifiedStream)
    (h : ∀ x ∈ us, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.distinct us,
      ∃ n : Int, x.2 = DiffWithError.val n :=
  UnifiedStream.clampToOne_no_error _
    (UnifiedStream.consolidate_no_error _ h)

/-! ## Distributivity over `unionAll`

`unionAll` is concatenation, so any operator built as a `flatMap`
(`filter`, `cross`-on-left, `project`) distributes over it via the
generic `List.flatMap_append` law. The distributivity laws below
let the optimizer rearrange a pipeline whose tail is a `UNION ALL`
into per-branch pipelines that can be planned independently. -/

theorem UnifiedStream.filter_unionAll (p : Expr) (a b : UnifiedStream) :
    UnifiedStream.filter p (UnifiedStream.unionAll a b)
      = UnifiedStream.unionAll
          (UnifiedStream.filter p a)
          (UnifiedStream.filter p b) :=
  UnifiedStream.filter_append p a b

theorem UnifiedStream.cross_unionAll_left (a b r : UnifiedStream) :
    UnifiedStream.cross (UnifiedStream.unionAll a b) r
      = UnifiedStream.unionAll
          (UnifiedStream.cross a r)
          (UnifiedStream.cross b r) :=
  UnifiedStream.cross_append_left a b r

theorem UnifiedStream.project_unionAll (es : List Expr) (a b : UnifiedStream) :
    UnifiedStream.project es (UnifiedStream.unionAll a b)
      = UnifiedStream.unionAll
          (UnifiedStream.project es a)
          (UnifiedStream.project es b) :=
  UnifiedStream.project_append es a b

/-- `negate` distributes over `unionAll`. Direct citation of
`negate_append`. -/
theorem UnifiedStream.negate_unionAll (a b : UnifiedStream) :
    UnifiedStream.negate (UnifiedStream.unionAll a b)
      = UnifiedStream.unionAll
          (UnifiedStream.negate a)
          (UnifiedStream.negate b) :=
  UnifiedStream.negate_append a b

/-- `negate` commutes with `consolidateInto`: inserting a negated
diff into a negated bucket list gives the same result as inserting
the positive diff and negating the whole list. -/
private theorem negate_consolidateInto
    (uc : UnifiedRow) (d : DiffWithError Int) (xs : UnifiedStream) :
    UnifiedStream.negate (consolidateInto uc d xs)
      = consolidateInto uc (-d) (UnifiedStream.negate xs) := by
  induction xs with
  | nil =>
    show UnifiedStream.negate [(uc, d)]
        = [(uc, -d)]
    rfl
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    by_cases hEq : uc = uc'
    · subst hEq
      rw [consolidateInto_match]
      show UnifiedStream.negate ((uc, d + d') :: tl)
          = consolidateInto uc (-d) (UnifiedStream.negate ((uc, d') :: tl))
      show ((uc, -(d + d')) :: UnifiedStream.negate tl)
          = consolidateInto uc (-d) ((uc, -d') :: UnifiedStream.negate tl)
      rw [consolidateInto_match, DiffWithError.neg_add_int]
    · rw [consolidateInto_skip _ _ _ _ _ hEq]
      show UnifiedStream.negate ((uc', d') :: consolidateInto uc d tl)
          = consolidateInto uc (-d) (UnifiedStream.negate ((uc', d') :: tl))
      show ((uc', -d') :: UnifiedStream.negate (consolidateInto uc d tl))
          = consolidateInto uc (-d) ((uc', -d') :: UnifiedStream.negate tl)
      rw [consolidateInto_skip _ _ _ _ _ hEq, ih]

/-- `negate` commutes with `consolidate`: consolidating then
negating equals negating then consolidating. Negation is additive,
so it slides through the per-bucket sums. -/
theorem UnifiedStream.negate_consolidate (us : UnifiedStream) :
    UnifiedStream.negate (UnifiedStream.consolidate us)
      = UnifiedStream.consolidate (UnifiedStream.negate us) := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    show UnifiedStream.negate (consolidateInto uc d (UnifiedStream.consolidate tl))
        = UnifiedStream.consolidate ((uc, -d) :: UnifiedStream.negate tl)
    rw [negate_consolidateInto]
    show consolidateInto uc (-d) (UnifiedStream.negate (UnifiedStream.consolidate tl))
        = consolidateInto uc (-d) (UnifiedStream.consolidate (UnifiedStream.negate tl))
    rw [ih]

/-- Negating one side of a cross product is the same as negating
the cross product. The diff-semiring law `(-a) * b = -(a * b)`
carries the proof through `combineCarrier` (carrier is unchanged
by negation) and the diff arithmetic. -/
theorem UnifiedStream.cross_negate_left (l r : UnifiedStream) :
    UnifiedStream.cross (UnifiedStream.negate l) r
      = UnifiedStream.negate (UnifiedStream.cross l r) := by
  induction l with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    -- LHS: cross ((uc, -d) :: negate tl) r
    -- RHS: negate (r.map (fun rd => (combineCarrier uc rd.1, d * rd.2)) ++ cross tl r)
    show UnifiedStream.cross ((uc, -d) :: UnifiedStream.negate tl) r
        = UnifiedStream.negate
            ((r.map (fun rd => (combineCarrier uc rd.1, d * rd.2)))
              ++ UnifiedStream.cross tl r)
    -- LHS reduces to (r.map (fun rd => (combineCarrier uc rd.1, (-d) * rd.2)))
    --   ++ cross (negate tl) r
    show (r.map (fun rd => (combineCarrier uc rd.1, (-d) * rd.2)))
            ++ UnifiedStream.cross (UnifiedStream.negate tl) r
        = UnifiedStream.negate
            ((r.map (fun rd => (combineCarrier uc rd.1, d * rd.2)))
              ++ UnifiedStream.cross tl r)
    -- RHS: negate distributes over ++.
    rw [show UnifiedStream.negate
            ((r.map (fun rd => (combineCarrier uc rd.1, d * rd.2)))
              ++ UnifiedStream.cross tl r)
          = UnifiedStream.negate
              (r.map (fun rd => (combineCarrier uc rd.1, d * rd.2)))
            ++ UnifiedStream.negate (UnifiedStream.cross tl r)
        from UnifiedStream.negate_unionAll _ _]
    rw [← ih]
    -- Reduce to per-r-record equality. The two r.map terms must agree.
    congr 1
    -- negate (r.map (fun rd => (combineCarrier uc rd.1, d * rd.2)))
    -- = r.map (fun rd => (combineCarrier uc rd.1, -(d * rd.2)))
    -- = r.map (fun rd => (combineCarrier uc rd.1, (-d) * rd.2))
    show r.map (fun rd => (combineCarrier uc rd.1, (-d) * rd.2))
        = (r.map (fun rd => (combineCarrier uc rd.1, d * rd.2))).map
            (fun ud => (ud.1, -ud.2))
    rw [List.map_map]
    apply List.map_congr_left
    intro rd _
    show (combineCarrier uc rd.1, (-d) * rd.2)
        = (combineCarrier uc rd.1, -(d * rd.2))
    rw [DiffWithError.neg_mul_int]

/-- Empty left input: `exceptAll [] r = negate (consolidate r)`.
Reduces via `unionAll_nil_left` and `negate_consolidate`. -/
theorem UnifiedStream.exceptAll_nil_left (r : UnifiedStream) :
    UnifiedStream.exceptAll [] r
      = UnifiedStream.negate (UnifiedStream.consolidate r) := by
  show UnifiedStream.consolidate
        (UnifiedStream.unionAll [] (UnifiedStream.negate r))
      = UnifiedStream.negate (UnifiedStream.consolidate r)
  rw [UnifiedStream.unionAll_nil_left]
  exact (UnifiedStream.negate_consolidate r).symm

/-! ## `INTERSECT ALL`

Bag intersection on the diff-aware stream. Implementation:
consolidate both inputs, then for each carrier in the left,
look it up in the right. Output: `(carrier, min(L, R))` when
the carrier is present in both, otherwise omit. The min
combinator `DiffWithError.min` carries `.error` absorption: a
collection-scoped error in either input gives an `.error` output
for that carrier (provided the carrier exists in both). -/

/-- Lookup the diff for `uc` in a consolidated stream. Returns
`none` when the carrier is absent. The skeleton's consolidate
keeps each carrier at most once, so the first match is the only
match. -/
def UnifiedStream.lookup (uc : UnifiedRow) :
    UnifiedStream → Option (DiffWithError Int)
  | []                    => none
  | (uc', d) :: rest      => if uc = uc' then some d
                              else UnifiedStream.lookup uc rest

theorem UnifiedStream.lookup_nil (uc : UnifiedRow) :
    UnifiedStream.lookup uc [] = none := rfl

/-- If a carrier is in the stream (with some diff), the lookup
returns `some` value. The returned diff need not be the input one
(consolidated streams keep each carrier once, but `lookup` does
not depend on that invariant). -/
theorem UnifiedStream.lookup_isSome_of_mem
    (uc : UnifiedRow) (us : UnifiedStream)
    (h : ∃ d, (uc, d) ∈ us) :
    ∃ d, UnifiedStream.lookup uc us = some d := by
  induction us with
  | nil => obtain ⟨_, hMem⟩ := h; exact absurd hMem List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    by_cases hEq : uc = uc'
    · refine ⟨d', ?_⟩
      show (if uc = uc' then some d' else UnifiedStream.lookup uc tl) = some d'
      rw [if_pos hEq]
    · have hTl : ∃ d, (uc, d) ∈ tl := by
        obtain ⟨d, hMem⟩ := h
        rcases List.mem_cons.mp hMem with hHead | hTail
        · exact absurd ((Prod.mk.injEq _ _ _ _).mp hHead).1 hEq
        · exact ⟨d, hTail⟩
      obtain ⟨d, hLookup⟩ := ih hTl
      refine ⟨d, ?_⟩
      show (if uc = uc' then some d' else UnifiedStream.lookup uc tl) = some d
      rw [if_neg hEq]; exact hLookup

/-- `INTERSECT ALL` on `UnifiedStream`. Consolidates both sides,
then per left-carrier emits `(uc, min(L_diff, R_diff))` if the
carrier exists in the right's consolidate; otherwise drops it. -/
def UnifiedStream.intersectAll (l r : UnifiedStream) : UnifiedStream :=
  let rCons := UnifiedStream.consolidate r
  (UnifiedStream.consolidate l).filterMap fun ud =>
    match UnifiedStream.lookup ud.1 rCons with
    | none    => none
    | some d' => some (ud.1, DiffWithError.min ud.2 d')

/-- Length bound: at most the consolidated length of `l`. -/
theorem UnifiedStream.intersectAll_length_le (l r : UnifiedStream) :
    (UnifiedStream.intersectAll l r).length ≤ l.length := by
  have h1 : (UnifiedStream.intersectAll l r).length
              ≤ (UnifiedStream.consolidate l).length := by
    show ((UnifiedStream.consolidate l).filterMap _).length
          ≤ (UnifiedStream.consolidate l).length
    exact List.length_filterMap_le _ _
  exact Nat.le_trans h1 (UnifiedStream.consolidate_length_le l)

/-- `.error` diff in the left input survives `intersectAll`,
provided the same carrier also appears (with any diff) in the
right input. The min combinator's left-absorbing property
(`error_min_left`) ensures the output diff is `.error`. -/
theorem UnifiedStream.intersectAll_preserves_error_diff_left
    (l r : UnifiedStream) (uc : UnifiedRow)
    (hL : (uc, (DiffWithError.error : DiffWithError Int))
            ∈ UnifiedStream.consolidate l)
    (hR : ∃ d, (uc, d) ∈ UnifiedStream.consolidate r) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.intersectAll l r := by
  obtain ⟨d', hLookup⟩ :=
    UnifiedStream.lookup_isSome_of_mem uc _ hR
  show (uc, DiffWithError.error)
    ∈ (UnifiedStream.consolidate l).filterMap _
  rw [List.mem_filterMap]
  refine ⟨(uc, DiffWithError.error), hL, ?_⟩
  show (match UnifiedStream.lookup uc (UnifiedStream.consolidate r) with
        | none    => none
        | some d' => some (uc, DiffWithError.min DiffWithError.error d'))
      = some (uc, DiffWithError.error)
  rw [hLookup]
  rfl

/-- When the list has no duplicate carriers, `lookup` returns the
exact diff associated with the membership witness. -/
theorem UnifiedStream.lookup_eq_of_mem_noDup
    {uc : UnifiedRow} {d : DiffWithError Int} {us : UnifiedStream}
    (h_mem : (uc, d) ∈ us) (h_noDup : UnifiedStream.NoDupCarriers us) :
    UnifiedStream.lookup uc us = some d := by
  induction us with
  | nil => exact absurd h_mem List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    obtain ⟨hHead, hTl⟩ := List.pairwise_cons.mp h_noDup
    rcases List.mem_cons.mp h_mem with hEq | hTail
    · have hUc : uc = uc' := (Prod.mk.injEq _ _ _ _).mp hEq |>.1
      have hD  : d = d' := (Prod.mk.injEq _ _ _ _).mp hEq |>.2
      subst hUc; subst hD
      show (if uc = uc then some d else UnifiedStream.lookup uc tl) = some d
      rw [if_pos rfl]
    · have hNe : uc ≠ uc' := (hHead (uc, d) hTail).symm
      show (if uc = uc' then some d' else UnifiedStream.lookup uc tl) = some d
      rw [if_neg hNe]
      exact ih hTail hTl

/-- Symmetric: `.error` diff in the right input survives, provided
the same carrier also appears (with any diff) in the left. The
right-min-absorbing property carries the proof, using the no-dup
property of `consolidate r` to extract the exact `.error` diff. -/
theorem UnifiedStream.intersectAll_preserves_error_diff_right
    (l r : UnifiedStream) (uc : UnifiedRow)
    (hL : ∃ d, (uc, d) ∈ UnifiedStream.consolidate l)
    (hR : (uc, (DiffWithError.error : DiffWithError Int))
            ∈ UnifiedStream.consolidate r) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.intersectAll l r := by
  obtain ⟨dL, hLMem⟩ := hL
  have h_lookup_err :
      UnifiedStream.lookup uc (UnifiedStream.consolidate r)
        = some DiffWithError.error :=
    UnifiedStream.lookup_eq_of_mem_noDup hR
      (UnifiedStream.consolidate_noDup r)
  show (uc, DiffWithError.error)
    ∈ (UnifiedStream.consolidate l).filterMap _
  rw [List.mem_filterMap]
  refine ⟨(uc, dL), hLMem, ?_⟩
  show (match UnifiedStream.lookup uc (UnifiedStream.consolidate r) with
        | none    => none
        | some d' => some (uc, DiffWithError.min dL d'))
      = some (uc, DiffWithError.error)
  rw [h_lookup_err]
  show some (uc, DiffWithError.min dL DiffWithError.error)
      = some (uc, DiffWithError.error)
  rw [DiffWithError.error_min_right]

/-- `bagExceptAll [] r = clampPositive (negate (consolidate r))`.
With an all-`.val` `r`, the negation makes every diff non-positive,
which `clampPositive` then drops, yielding the spec-correct empty
result. `.error` records survive both stages. -/
theorem UnifiedStream.bagExceptAll_nil_left (r : UnifiedStream) :
    UnifiedStream.bagExceptAll [] r
      = UnifiedStream.clampPositive
          (UnifiedStream.negate (UnifiedStream.consolidate r)) := by
  show UnifiedStream.clampPositive (UnifiedStream.exceptAll [] r)
      = UnifiedStream.clampPositive
          (UnifiedStream.negate (UnifiedStream.consolidate r))
  rw [UnifiedStream.exceptAll_nil_left]

end Mz
