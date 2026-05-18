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

/-- `negate` commutes with `filter`. Both orderings agree on
every record shape; filter's row arm depends on `eval r pred`,
which is independent of the diff sign, so the diff-flip slides
through. -/
theorem UnifiedStream.negate_filter (pred : Expr) (us : UnifiedStream) :
    UnifiedStream.negate (UnifiedStream.filter pred us)
      = UnifiedStream.filter pred (UnifiedStream.negate us) := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hCons : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
    rw [hCons, UnifiedStream.filter_append, UnifiedStream.negate_append,
        ih, UnifiedStream.negate_append, UnifiedStream.filter_append]
    congr 1
    -- Per-record: negate (filter [(uc, d)]) = filter pred (negate [(uc, d)]).
    -- Strategy: compute both filter results (original and post-negate),
    -- compute negate of the original record, and equate via neg_{val,error}.
    cases d with
    | error =>
      have hF : UnifiedStream.filter pred
                  [(uc, (DiffWithError.error : DiffWithError Int))]
              = [(uc, (DiffWithError.error : DiffWithError Int))] := by
        cases uc <;> rfl
      have hN : UnifiedStream.negate
                  [(uc, (DiffWithError.error : DiffWithError Int))]
              = [(uc, (DiffWithError.error : DiffWithError Int))] := by
        show [(uc, -(DiffWithError.error : DiffWithError Int))] = _
        rw [DiffWithError.neg_error]
      rw [hF, hN, hF]
    | val n =>
      cases uc with
      | err e =>
        have hF : UnifiedStream.filter pred
                    [(UnifiedRow.err e, DiffWithError.val n)]
                = [(UnifiedRow.err e, DiffWithError.val n)] := rfl
        have hFn : UnifiedStream.filter pred
                    [(UnifiedRow.err e, DiffWithError.val (-n))]
                = [(UnifiedRow.err e, DiffWithError.val (-n))] := rfl
        have hN : UnifiedStream.negate [(UnifiedRow.err e, DiffWithError.val n)]
                = [(UnifiedRow.err e, DiffWithError.val (-n))] := by
          show [(UnifiedRow.err e, -(DiffWithError.val n : DiffWithError Int))]
              = [(UnifiedRow.err e, DiffWithError.val (-n))]
          rw [DiffWithError.neg_val]
        rw [hF, hN, hFn]
      | row r =>
        cases hEval : eval r pred with
        | bool b =>
          cases b with
          | true =>
            have hF : UnifiedStream.filter pred
                        [(UnifiedRow.row r, DiffWithError.val n)]
                    = [(UnifiedRow.row r, DiffWithError.val n)] := by
              show (match eval r pred with
                      | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                      | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                      | _          => []) ++ [] = _
              rw [hEval]; rfl
            have hFn : UnifiedStream.filter pred
                        [(UnifiedRow.row r, DiffWithError.val (-n))]
                    = [(UnifiedRow.row r, DiffWithError.val (-n))] := by
              show (match eval r pred with
                      | .bool true => [(UnifiedRow.row r, DiffWithError.val (-n))]
                      | .err e     => [(UnifiedRow.err e, DiffWithError.val (-n))]
                      | _          => []) ++ [] = _
              rw [hEval]; rfl
            have hN : UnifiedStream.negate
                        [(UnifiedRow.row r, DiffWithError.val n)]
                    = [(UnifiedRow.row r, DiffWithError.val (-n))] := by
              show [(UnifiedRow.row r, -(DiffWithError.val n : DiffWithError Int))]
                  = [(UnifiedRow.row r, DiffWithError.val (-n))]
              rw [DiffWithError.neg_val]
            rw [hF, hN, hFn]
          | false =>
            have hF : UnifiedStream.filter pred
                        [(UnifiedRow.row r, DiffWithError.val n)] = [] := by
              show (match eval r pred with
                      | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                      | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                      | _          => []) ++ [] = _
              rw [hEval]; rfl
            have hFn : UnifiedStream.filter pred
                        [(UnifiedRow.row r, DiffWithError.val (-n))] = [] := by
              show (match eval r pred with
                      | .bool true => [(UnifiedRow.row r, DiffWithError.val (-n))]
                      | .err e     => [(UnifiedRow.err e, DiffWithError.val (-n))]
                      | _          => []) ++ [] = _
              rw [hEval]; rfl
            have hN : UnifiedStream.negate
                        [(UnifiedRow.row r, DiffWithError.val n)]
                    = [(UnifiedRow.row r, DiffWithError.val (-n))] := by
              show [(UnifiedRow.row r, -(DiffWithError.val n : DiffWithError Int))]
                  = [(UnifiedRow.row r, DiffWithError.val (-n))]
              rw [DiffWithError.neg_val]
            rw [hF, hN, hFn]; rfl
        | int _ =>
          have hF : UnifiedStream.filter pred
                      [(UnifiedRow.row r, DiffWithError.val n)] = [] := by
            show (match eval r pred with
                    | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                    | _          => []) ++ [] = _
            rw [hEval]; rfl
          have hFn : UnifiedStream.filter pred
                      [(UnifiedRow.row r, DiffWithError.val (-n))] = [] := by
            show (match eval r pred with
                    | .bool true => [(UnifiedRow.row r, DiffWithError.val (-n))]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val (-n))]
                    | _          => []) ++ [] = _
            rw [hEval]; rfl
          have hN : UnifiedStream.negate
                      [(UnifiedRow.row r, DiffWithError.val n)]
                  = [(UnifiedRow.row r, DiffWithError.val (-n))] := by
            show [(UnifiedRow.row r, -(DiffWithError.val n : DiffWithError Int))]
                = [(UnifiedRow.row r, DiffWithError.val (-n))]
            rw [DiffWithError.neg_val]
          rw [hF, hN, hFn]; rfl
        | null =>
          have hF : UnifiedStream.filter pred
                      [(UnifiedRow.row r, DiffWithError.val n)] = [] := by
            show (match eval r pred with
                    | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                    | _          => []) ++ [] = _
            rw [hEval]; rfl
          have hFn : UnifiedStream.filter pred
                      [(UnifiedRow.row r, DiffWithError.val (-n))] = [] := by
            show (match eval r pred with
                    | .bool true => [(UnifiedRow.row r, DiffWithError.val (-n))]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val (-n))]
                    | _          => []) ++ [] = _
            rw [hEval]; rfl
          have hN : UnifiedStream.negate
                      [(UnifiedRow.row r, DiffWithError.val n)]
                  = [(UnifiedRow.row r, DiffWithError.val (-n))] := by
            show [(UnifiedRow.row r, -(DiffWithError.val n : DiffWithError Int))]
                = [(UnifiedRow.row r, DiffWithError.val (-n))]
            rw [DiffWithError.neg_val]
          rw [hF, hN, hFn]; rfl
        | err e_pred =>
          have hF : UnifiedStream.filter pred
                      [(UnifiedRow.row r, DiffWithError.val n)]
                  = [(UnifiedRow.err e_pred, DiffWithError.val n)] := by
            show (match eval r pred with
                    | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                    | _          => []) ++ [] = _
            rw [hEval]; rfl
          have hFn : UnifiedStream.filter pred
                      [(UnifiedRow.row r, DiffWithError.val (-n))]
                  = [(UnifiedRow.err e_pred, DiffWithError.val (-n))] := by
            show (match eval r pred with
                    | .bool true => [(UnifiedRow.row r, DiffWithError.val (-n))]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val (-n))]
                    | _          => []) ++ [] = _
            rw [hEval]; rfl
          have hN : UnifiedStream.negate
                      [(UnifiedRow.row r, DiffWithError.val n)]
                  = [(UnifiedRow.row r, DiffWithError.val (-n))] := by
            show [(UnifiedRow.row r, -(DiffWithError.val n : DiffWithError Int))]
                = [(UnifiedRow.row r, DiffWithError.val (-n))]
            rw [DiffWithError.neg_val]
          have hNE : UnifiedStream.negate
                      [(UnifiedRow.err e_pred, DiffWithError.val n)]
                  = [(UnifiedRow.err e_pred, DiffWithError.val (-n))] := by
            show [(UnifiedRow.err e_pred, -(DiffWithError.val n : DiffWithError Int))]
                = [(UnifiedRow.err e_pred, DiffWithError.val (-n))]
            rw [DiffWithError.neg_val]
          rw [hF, hN, hFn, hNE]

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

/-- Converse direction: a successful `lookup` witnesses
membership. The returned diff is paired with the queried carrier
in the input list. -/
theorem UnifiedStream.mem_of_lookup_eq_some
    {uc : UnifiedRow} {d : DiffWithError Int} {us : UnifiedStream}
    (h : UnifiedStream.lookup uc us = some d) :
    (uc, d) ∈ us := by
  induction us with
  | nil => exact absurd h (by simp [UnifiedStream.lookup])
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    show (uc, d) ∈ (uc', d') :: tl
    by_cases hEq : uc = uc'
    · subst hEq
      have hRed : UnifiedStream.lookup uc ((uc, d') :: tl) = some d' := by
        show (if uc = uc then some d' else UnifiedStream.lookup uc tl) = some d'
        rw [if_pos rfl]
      rw [hRed] at h
      have hEq' : d' = d := by injection h
      rw [← hEq']
      exact List.mem_cons_self
    · have hRed : UnifiedStream.lookup uc ((uc', d') :: tl)
                = UnifiedStream.lookup uc tl := by
        show (if uc = uc' then some d' else UnifiedStream.lookup uc tl)
            = UnifiedStream.lookup uc tl
        rw [if_neg hEq]
      rw [hRed] at h
      exact List.mem_cons_of_mem _ (ih h)

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

/-! ## NoDupCarriers closure laws

Operators that preserve carriers (or filter them) preserve
`NoDupCarriers`. The closure lets the optimizer compose set-op
pipelines without re-deriving uniqueness at each step. -/

theorem UnifiedStream.negate_noDup (us : UnifiedStream)
    (h : UnifiedStream.NoDupCarriers us) :
    UnifiedStream.NoDupCarriers (UnifiedStream.negate us) := by
  induction us with
  | nil => exact UnifiedStream.NoDupCarriers.nil
  | cons hd tl ih =>
    obtain ⟨hHead, hTl⟩ := List.pairwise_cons.mp h
    show List.Pairwise _ ((hd.1, -hd.2) :: UnifiedStream.negate tl)
    apply List.Pairwise.cons
    · intro y hY
      obtain ⟨orig, hOrigMem, hY_eq⟩ := List.mem_map.mp hY
      rw [← hY_eq]
      exact hHead orig hOrigMem
    · exact ih hTl

theorem UnifiedStream.clampPositive_noDup (us : UnifiedStream)
    (h : UnifiedStream.NoDupCarriers us) :
    UnifiedStream.NoDupCarriers (UnifiedStream.clampPositive us) := by
  unfold UnifiedStream.clampPositive
  exact List.Pairwise.sublist List.filter_sublist h

-- `filter_noDup` is *false* in general: two distinct `.row r1` /
-- `.row r2` records with `r1 ≠ r2` may both pred-err with the same
-- payload `e`, producing two output records carrying the same
-- `.err e`. The collapse violates NoDup. Filter is therefore not a
-- NoDup-preserving operator; downstream proofs that need NoDup on
-- filter output must rely on additional structure (e.g., that the
-- predicate never errs, or that consolidate is applied after).

/-- Carriers of `clampToOne` output are a subset of input carriers
(via the first component). Used by `clampToOne_noDup`. -/
private theorem mem_clampToOne_carrier (us : UnifiedStream)
    (x : UnifiedRow × DiffWithError Int) (h : x ∈ UnifiedStream.clampToOne us) :
    ∃ orig ∈ us, x.1 = orig.1 := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases d with
    | error =>
      -- clampToOne ((uc, .error) :: tl) = (uc, .error) :: clampToOne tl
      have h' : x ∈ (uc, (DiffWithError.error : DiffWithError Int))
                    :: UnifiedStream.clampToOne tl := h
      rcases List.mem_cons.mp h' with hHead | hTail
      · refine ⟨(uc, DiffWithError.error), List.mem_cons_self, ?_⟩
        rw [hHead]
      · obtain ⟨orig, hOrigMem, hCarr⟩ := ih hTail
        exact ⟨orig, List.mem_cons_of_mem _ hOrigMem, hCarr⟩
    | val n =>
      have h' : x ∈ (if 0 < n
                      then (uc, DiffWithError.val 1) :: UnifiedStream.clampToOne tl
                      else UnifiedStream.clampToOne tl) := h
      split at h'
      · rcases List.mem_cons.mp h' with hHead | hTail
        · refine ⟨(uc, DiffWithError.val n), List.mem_cons_self, ?_⟩
          rw [hHead]
        · obtain ⟨orig, hOrigMem, hCarr⟩ := ih hTail
          exact ⟨orig, List.mem_cons_of_mem _ hOrigMem, hCarr⟩
      · obtain ⟨orig, hOrigMem, hCarr⟩ := ih h'
        exact ⟨orig, List.mem_cons_of_mem _ hOrigMem, hCarr⟩

/-- `clampToOne` preserves NoDup: each output record corresponds to
exactly one input record (or none), with carrier unchanged. -/
theorem UnifiedStream.clampToOne_noDup (us : UnifiedStream)
    (h : UnifiedStream.NoDupCarriers us) :
    UnifiedStream.NoDupCarriers (UnifiedStream.clampToOne us) := by
  induction us with
  | nil => exact UnifiedStream.NoDupCarriers.nil
  | cons hd tl ih =>
    obtain ⟨hHead, hTl⟩ := List.pairwise_cons.mp h
    obtain ⟨uc, d⟩ := hd
    cases d with
    | error =>
      show List.Pairwise _ ((uc, DiffWithError.error)
                            :: UnifiedStream.clampToOne tl)
      apply List.Pairwise.cons
      · intro y hY
        obtain ⟨orig, hOrigMem, hCarr⟩ := mem_clampToOne_carrier tl y hY
        rw [hCarr] at *
        exact hHead orig hOrigMem
      · exact ih hTl
    | val n =>
      show List.Pairwise _ (if 0 < n
                            then (uc, DiffWithError.val 1) :: UnifiedStream.clampToOne tl
                            else UnifiedStream.clampToOne tl)
      split
      · apply List.Pairwise.cons
        · intro y hY
          obtain ⟨orig, hOrigMem, hCarr⟩ := mem_clampToOne_carrier tl y hY
          rw [hCarr] at *
          exact hHead orig hOrigMem
        · exact ih hTl
      · exact ih hTl

/-- All-`.val` inputs yield all-`.val` outputs through
`intersectAll`. Each output diff is `.val (Min.min n m)` for some
`n` from the left and `m` from the right. -/
theorem UnifiedStream.intersectAll_no_error
    (l r : UnifiedStream)
    (hL : ∀ x ∈ l, ∃ n : Int, x.2 = DiffWithError.val n)
    (hR : ∀ x ∈ r, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.intersectAll l r,
      ∃ n : Int, x.2 = DiffWithError.val n := by
  intro x hMem
  obtain ⟨ud, hUdMem, hMatch⟩ :=
    List.mem_filterMap.mp (show x ∈ (UnifiedStream.consolidate l).filterMap _ from hMem)
  -- ud ∈ consolidate l → ud.2 = .val nL.
  obtain ⟨nL, hNL⟩ :=
    UnifiedStream.consolidate_no_error l hL ud hUdMem
  -- The match in hMatch must resolve to `some` for hMatch to be `some x`.
  cases h_lookup : UnifiedStream.lookup ud.1 (UnifiedStream.consolidate r) with
  | none =>
    rw [h_lookup] at hMatch
    cases hMatch
  | some d' =>
    rw [h_lookup] at hMatch
    -- hMatch : some (ud.1, min ud.2 d') = some x
    have hxEq : x = (ud.1, DiffWithError.min ud.2 d') := by
      injection hMatch with hPair
      exact hPair.symm
    -- d' ∈ consolidate r → d' = .val nR.
    have hMemR : (ud.1, d') ∈ UnifiedStream.consolidate r :=
      UnifiedStream.mem_of_lookup_eq_some h_lookup
    obtain ⟨nR, hNR⟩ :=
      UnifiedStream.consolidate_no_error r hR (ud.1, d') hMemR
    have hNR' : d' = DiffWithError.val nR := hNR
    -- min (.val nL) (.val nR) = .val (Min.min nL nR).
    refine ⟨Min.min nL nR, ?_⟩
    rw [hxEq]
    show DiffWithError.min ud.2 d' = DiffWithError.val (Min.min nL nR)
    rw [hNL, hNR']
    rfl

/-! ## Bag-semantics `INTERSECT ALL`

`bagIntersectAll = clampPositive ∘ intersectAll`. The signed-diff
output of `intersectAll` may have non-positive `.val` diffs when
either side's consolidated count is non-positive; the clamp drops
those, producing the bag-semantics result. -/

def UnifiedStream.bagIntersectAll (l r : UnifiedStream) : UnifiedStream :=
  UnifiedStream.clampPositive (UnifiedStream.intersectAll l r)

theorem UnifiedStream.bagIntersectAll_length_le (l r : UnifiedStream) :
    (UnifiedStream.bagIntersectAll l r).length ≤ l.length :=
  Nat.le_trans
    (UnifiedStream.clampPositive_length_le _)
    (UnifiedStream.intersectAll_length_le l r)

theorem UnifiedStream.bagIntersectAll_preserves_error_diff_left
    (l r : UnifiedStream) (uc : UnifiedRow)
    (hL : (uc, (DiffWithError.error : DiffWithError Int))
            ∈ UnifiedStream.consolidate l)
    (hR : ∃ d, (uc, d) ∈ UnifiedStream.consolidate r) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.bagIntersectAll l r :=
  UnifiedStream.clampPositive_preserves_error_diff _ uc
    (UnifiedStream.intersectAll_preserves_error_diff_left l r uc hL hR)

theorem UnifiedStream.bagIntersectAll_preserves_error_diff_right
    (l r : UnifiedStream) (uc : UnifiedRow)
    (hL : ∃ d, (uc, d) ∈ UnifiedStream.consolidate l)
    (hR : (uc, (DiffWithError.error : DiffWithError Int))
            ∈ UnifiedStream.consolidate r) :
    (uc, (DiffWithError.error : DiffWithError Int))
      ∈ UnifiedStream.bagIntersectAll l r :=
  UnifiedStream.clampPositive_preserves_error_diff _ uc
    (UnifiedStream.intersectAll_preserves_error_diff_right l r uc hL hR)

theorem UnifiedStream.bagIntersectAll_no_error
    (l r : UnifiedStream)
    (hL : ∀ x ∈ l, ∃ n : Int, x.2 = DiffWithError.val n)
    (hR : ∀ x ∈ r, ∃ n : Int, x.2 = DiffWithError.val n) :
    ∀ x ∈ UnifiedStream.bagIntersectAll l r,
      ∃ n : Int, x.2 = DiffWithError.val n :=
  UnifiedStream.clampPositive_no_error _
    (UnifiedStream.intersectAll_no_error l r hL hR)

theorem UnifiedStream.bagIntersectAll_only_positive
    (l r : UnifiedStream) :
    ∀ x ∈ UnifiedStream.bagIntersectAll l r,
      (∃ n : Int, x.2 = DiffWithError.val n ∧ 0 < n)
      ∨ x.2 = DiffWithError.error :=
  UnifiedStream.clampPositive_only_positive _

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

/-! ## NoDup closure on derived set operators

`union`, `exceptAll`, `intersectAll`, `distinct`, and their
bag-clamped variants all produce NoDupCarriers output. Each
factors through `consolidate` (or `clampToOne` on a consolidated
list), which gives the closure for free. -/

theorem UnifiedStream.union_noDup (l r : UnifiedStream) :
    UnifiedStream.NoDupCarriers (UnifiedStream.union l r) :=
  UnifiedStream.consolidate_noDup _

theorem UnifiedStream.exceptAll_noDup (l r : UnifiedStream) :
    UnifiedStream.NoDupCarriers (UnifiedStream.exceptAll l r) :=
  UnifiedStream.consolidate_noDup _

theorem UnifiedStream.bagExceptAll_noDup (l r : UnifiedStream) :
    UnifiedStream.NoDupCarriers (UnifiedStream.bagExceptAll l r) :=
  UnifiedStream.clampPositive_noDup _ (UnifiedStream.exceptAll_noDup l r)

theorem UnifiedStream.distinct_noDup (us : UnifiedStream) :
    UnifiedStream.NoDupCarriers (UnifiedStream.distinct us) :=
  UnifiedStream.clampToOne_noDup _ (UnifiedStream.consolidate_noDup us)

/-- `intersectAll`'s output is a filterMap of a NoDupCarriers list
whose mapping preserves the first component. The filterMap
therefore also has NoDupCarriers. -/
private theorem filterMap_carrier_noDup
    {f : UnifiedRow × DiffWithError Int → Option (UnifiedRow × DiffWithError Int)}
    (h_f : ∀ ud out, f ud = some out → out.1 = ud.1)
    {us : UnifiedStream} (h : UnifiedStream.NoDupCarriers us) :
    UnifiedStream.NoDupCarriers (us.filterMap f) := by
  induction us with
  | nil => exact UnifiedStream.NoDupCarriers.nil
  | cons hd tl ih =>
    obtain ⟨hHead, hTl⟩ := List.pairwise_cons.mp h
    rw [List.filterMap_cons]
    cases h_eq : f hd with
    | none => exact ih hTl
    | some out =>
      have hCarr : out.1 = hd.1 := h_f hd out h_eq
      apply List.Pairwise.cons
      · intro y hY
        obtain ⟨origTl, hOrigMem, hY_eq⟩ := List.mem_filterMap.mp hY
        have hYCarr : y.1 = origTl.1 := h_f origTl y hY_eq
        rw [hYCarr, hCarr]
        exact hHead origTl hOrigMem
      · exact ih hTl

theorem UnifiedStream.intersectAll_noDup (l r : UnifiedStream) :
    UnifiedStream.NoDupCarriers (UnifiedStream.intersectAll l r) := by
  apply filterMap_carrier_noDup ?_ (UnifiedStream.consolidate_noDup l)
  intro ud out hSome
  cases h_lookup : UnifiedStream.lookup ud.1 (UnifiedStream.consolidate r) with
  | none =>
    rw [h_lookup] at hSome
    cases hSome
  | some d' =>
    rw [h_lookup] at hSome
    injection hSome with hPair
    rw [← hPair]

theorem UnifiedStream.bagIntersectAll_noDup (l r : UnifiedStream) :
    UnifiedStream.NoDupCarriers (UnifiedStream.bagIntersectAll l r) :=
  UnifiedStream.clampPositive_noDup _ (UnifiedStream.intersectAll_noDup l r)

/-! ## Error-scope invariance

Negation preserves both the row-scoped err set (`errCarriers`)
and the collection-scoped err set (`errorDiffCarriers`):
negation flips diffs but `.error` absorbs, and carriers are
unchanged by definition. Negation cannot escalate or rescue
either error scope. -/

theorem UnifiedStream.negate_errCarriers (us : UnifiedStream) :
    UnifiedStream.errCarriers (UnifiedStream.negate us)
      = UnifiedStream.errCarriers us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases uc with
    | row r =>
      show List.filterMap _ ((UnifiedRow.row r, -d) :: UnifiedStream.negate tl)
          = List.filterMap _ ((UnifiedRow.row r, d) :: tl)
      simp only [List.filterMap_cons]
      exact ih
    | err e =>
      show List.filterMap _ ((UnifiedRow.err e, -d) :: UnifiedStream.negate tl)
          = List.filterMap _ ((UnifiedRow.err e, d) :: tl)
      simp only [List.filterMap_cons]
      show e :: UnifiedStream.errCarriers (UnifiedStream.negate tl)
          = e :: UnifiedStream.errCarriers tl
      rw [ih]

theorem UnifiedStream.negate_errorDiffCarriers (us : UnifiedStream) :
    UnifiedStream.errorDiffCarriers (UnifiedStream.negate us)
      = UnifiedStream.errorDiffCarriers us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    cases d with
    | val n =>
      show List.filterMap _ ((uc, -DiffWithError.val n)
                              :: UnifiedStream.negate tl)
          = List.filterMap _ ((uc, DiffWithError.val n) :: tl)
      simp only [List.filterMap_cons]
      exact ih
    | error =>
      show List.filterMap _ ((uc, -(DiffWithError.error : DiffWithError Int))
                              :: UnifiedStream.negate tl)
          = List.filterMap _ ((uc, (DiffWithError.error : DiffWithError Int))
                              :: tl)
      simp only [List.filterMap_cons]
      show uc :: UnifiedStream.errorDiffCarriers (UnifiedStream.negate tl)
          = uc :: UnifiedStream.errorDiffCarriers tl
      rw [ih]

/-- `unionAll` concatenates both error-scope sets. The row-err set
of `unionAll a b` is `errCarriers a ++ errCarriers b`; same for
the collection-err set. Each scope is tracked independently. -/
theorem UnifiedStream.unionAll_errCarriers (a b : UnifiedStream) :
    UnifiedStream.errCarriers (UnifiedStream.unionAll a b)
      = UnifiedStream.errCarriers a ++ UnifiedStream.errCarriers b :=
  UnifiedStream.errCarriers_append a b

theorem UnifiedStream.unionAll_errorDiffCarriers (a b : UnifiedStream) :
    UnifiedStream.errorDiffCarriers (UnifiedStream.unionAll a b)
      = UnifiedStream.errorDiffCarriers a ++ UnifiedStream.errorDiffCarriers b :=
  UnifiedStream.errorDiffCarriers_append a b

/-! ## `filter` and error scopes

`filter` is the canonical site of *cell-to-row promotion*: a
`.row r` record whose predicate evaluates to `.err e` becomes a
`.err e` carrier in the output. Filter therefore *grows* the
row-scoped error set in general; it does not shrink it
(row-scoped errors pass through, and collection-scoped errors
pass through unconditionally).

`filter_errorDiffCarriers` is an equality: filter preserves the
collection-scoped error set exactly. The first arm matches
`.error` diffs and outputs them unchanged; every other arm
produces `.val`-diff outputs only. So the `.error` carriers are
neither added nor removed.

`filter_errCarriers_mono` is the monotone direction for the
row-scoped error set: every row-err present in input is present
in output. The reverse inclusion fails because cell-to-row
promotion can add fresh errors. -/

private theorem filter_singleton_errorDiffCarriers
    (pred : Expr) (uc : UnifiedRow) (d : DiffWithError Int) :
    UnifiedStream.errorDiffCarriers (UnifiedStream.filter pred [(uc, d)])
      = UnifiedStream.errorDiffCarriers [(uc, d)] := by
  cases d with
  | error =>
    -- Filter passes `.error`-diff records unchanged.
    cases uc <;> rfl
  | val n =>
    cases uc with
    | err _ => rfl
    | row r =>
      -- Filter's `.row r, .val n` arm produces zero or one `.val`-diff records,
      -- depending on `eval r pred`. Case-split on the eval, then reduce filter
      -- with the resulting equation to compute the concrete singleton output.
      cases hEval : eval r pred with
      | bool b =>
        cases b with
        | true =>
          have hF : UnifiedStream.filter pred
                      [(UnifiedRow.row r, DiffWithError.val n)]
                  = [(UnifiedRow.row r, DiffWithError.val n)] := by
            show (match eval r pred with
                    | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                    | _          => []) ++ [] = _
            rw [hEval]; rfl
          rw [hF]
        | false =>
          have hF : UnifiedStream.filter pred
                      [(UnifiedRow.row r, DiffWithError.val n)] = [] := by
            show (match eval r pred with
                    | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                    | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                    | _          => []) ++ [] = _
            rw [hEval]; rfl
          rw [hF]; rfl
      | int _  =>
        have hF : UnifiedStream.filter pred
                    [(UnifiedRow.row r, DiffWithError.val n)] = [] := by
          show (match eval r pred with
                  | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                  | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                  | _          => []) ++ [] = _
          rw [hEval]; rfl
        rw [hF]; rfl
      | null   =>
        have hF : UnifiedStream.filter pred
                    [(UnifiedRow.row r, DiffWithError.val n)] = [] := by
          show (match eval r pred with
                  | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                  | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                  | _          => []) ++ [] = _
          rw [hEval]; rfl
        rw [hF]; rfl
      | err e_pred =>
        have hF : UnifiedStream.filter pred
                    [(UnifiedRow.row r, DiffWithError.val n)]
                = [(UnifiedRow.err e_pred, DiffWithError.val n)] := by
          show (match eval r pred with
                  | .bool true => [(UnifiedRow.row r, DiffWithError.val n)]
                  | .err e     => [(UnifiedRow.err e, DiffWithError.val n)]
                  | _          => []) ++ [] = _
          rw [hEval]; rfl
        rw [hF]; rfl

theorem UnifiedStream.filter_errorDiffCarriers
    (pred : Expr) (us : UnifiedStream) :
    UnifiedStream.errorDiffCarriers (UnifiedStream.filter pred us)
      = UnifiedStream.errorDiffCarriers us := by
  induction us with
  | nil => rfl
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hCons : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
    rw [hCons, UnifiedStream.filter_append,
        UnifiedStream.errorDiffCarriers_append,
        UnifiedStream.errorDiffCarriers_append, ih,
        filter_singleton_errorDiffCarriers pred uc d]

private theorem filter_singleton_errCarriers_mono
    (pred : Expr) (uc : UnifiedRow) (d : DiffWithError Int) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers [(uc, d)]) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.filter pred [(uc, d)]) := by
  cases uc with
  | row r =>
    -- `errCarriers [(.row r, d)] = []`, so `h` is vacuous.
    have hEmpty : UnifiedStream.errCarriers [(UnifiedRow.row r, d)] = [] := rfl
    rw [hEmpty] at h
    exact absurd h List.not_mem_nil
  | err e0 =>
    -- `errCarriers [(.err e0, d)] = [e0]`, so `e = e0`.
    have hSingle : UnifiedStream.errCarriers [(UnifiedRow.err e0, d)] = [e0] := rfl
    rw [hSingle] at h
    have hEq : e = e0 := List.mem_singleton.mp h
    subst hEq
    cases d with
    | error =>
      -- filter passes `(.err e, .error)` through.
      have : UnifiedStream.filter pred [(UnifiedRow.err e, DiffWithError.error)]
              = [(UnifiedRow.err e, DiffWithError.error)] := rfl
      rw [this]
      exact List.mem_singleton.mpr rfl
    | val n =>
      have : UnifiedStream.filter pred [(UnifiedRow.err e, DiffWithError.val n)]
              = [(UnifiedRow.err e, DiffWithError.val n)] := rfl
      rw [this]
      exact List.mem_singleton.mpr rfl

theorem UnifiedStream.filter_errCarriers_mono
    (pred : Expr) (us : UnifiedStream) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers us) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.filter pred us) := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc, d⟩ := hd
    have hCons : ((uc, d) :: tl : UnifiedStream) = [(uc, d)] ++ tl := rfl
    rw [hCons, UnifiedStream.errCarriers_append] at h
    rw [hCons, UnifiedStream.filter_append, UnifiedStream.errCarriers_append]
    rcases List.mem_append.mp h with hHead | hTail
    · exact List.mem_append.mpr
        (Or.inl (filter_singleton_errCarriers_mono pred uc d e hHead))
    · exact List.mem_append.mpr (Or.inr (ih hTail))

/-! ## `consolidate` and error scopes

`consolidate` buckets records by carrier and sums diffs. As a
list, this can shrink (multiple `.err e` carriers fold to one)
and the row-err set may have multiplicity collapsed. But the
*set* of row-err payloads and the *set* of `.error`-diff carriers
is preserved.

Forward direction for row-err is from `mem_consolidate_of_mem`:
every input carrier survives. Backward direction is from
`mem_of_mem_consolidate`. For `.error`-diff carriers, the forward
direction is `consolidate_preserves_error`. -/

theorem UnifiedStream.consolidate_errCarriers_iff
    (us : UnifiedStream) (e : EvalError) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.consolidate us)
      ↔ e ∈ UnifiedStream.errCarriers us := by
  rw [UnifiedStream.mem_errCarriers, UnifiedStream.mem_errCarriers]
  constructor
  · intro h
    exact UnifiedStream.mem_of_mem_consolidate us (UnifiedRow.err e) h
  · intro ⟨d, hMem⟩
    exact UnifiedStream.mem_consolidate_of_mem us (UnifiedRow.err e) d hMem

/-- Forward direction for collection-scoped errors: every input
`.error`-diff carrier shows up in the consolidated output. Direct
consequence of `consolidate_preserves_error`. -/
theorem UnifiedStream.consolidate_errorDiffCarriers_mono
    (us : UnifiedStream) (uc : UnifiedRow)
    (h : uc ∈ UnifiedStream.errorDiffCarriers us) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.consolidate us) := by
  rw [UnifiedStream.mem_errorDiffCarriers] at h
  rw [UnifiedStream.mem_errorDiffCarriers]
  exact UnifiedStream.consolidate_preserves_error us uc h

/-- Full equivalence: the collection-err set of the consolidated
stream equals the input's. Combines `consolidate_preserves_error`
(forward) with `consolidate_error_inv` (reverse — no spurious
`.error` emerges from `.val + .val`). -/
theorem UnifiedStream.consolidate_errorDiffCarriers_iff
    (us : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.consolidate us)
      ↔ uc ∈ UnifiedStream.errorDiffCarriers us := by
  rw [UnifiedStream.mem_errorDiffCarriers,
      UnifiedStream.mem_errorDiffCarriers]
  constructor
  · intro h; exact UnifiedStream.consolidate_error_inv us uc h
  · intro h; exact UnifiedStream.consolidate_preserves_error us uc h

/-! ## Join and error scopes

`join pred l r = filter pred (cross l r)`. Error-scope behavior
composes from `filter` and `cross` theorems above:

* Collection-err set equals that of `cross l r` (filter preserves
  the `.error` diff set exactly).
* Row-err set grows monotonically over `cross l r` via `filter`'s
  cell-to-row promotion. -/

theorem UnifiedStream.join_errorDiffCarriers
    (pred : Expr) (l r : UnifiedStream) :
    UnifiedStream.errorDiffCarriers (UnifiedStream.join pred l r)
      = UnifiedStream.errorDiffCarriers (UnifiedStream.cross l r) :=
  UnifiedStream.filter_errorDiffCarriers pred (UnifiedStream.cross l r)

theorem UnifiedStream.join_errCarriers_mono
    (pred : Expr) (l r : UnifiedStream) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers (UnifiedStream.cross l r)) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.join pred l r) :=
  UnifiedStream.filter_errCarriers_mono pred (UnifiedStream.cross l r) e h

/-! ## `union` and error scopes

`union := consolidate ∘ unionAll`. Compose
`unionAll_errCarriers` (concat) with `consolidate_errCarriers_iff`
(set-preserving). The result: the row-err set of `union L R` is
the disjoint union of `L`'s and `R`'s row-err sets (as a set;
multiplicity collapses via consolidate).

Same shape for collection-err. -/

theorem UnifiedStream.union_errCarriers_iff
    (l r : UnifiedStream) (e : EvalError) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.union l r)
      ↔ e ∈ UnifiedStream.errCarriers l ∨ e ∈ UnifiedStream.errCarriers r := by
  show e ∈ UnifiedStream.errCarriers
            (UnifiedStream.consolidate (UnifiedStream.unionAll l r)) ↔ _
  rw [UnifiedStream.consolidate_errCarriers_iff,
      UnifiedStream.unionAll_errCarriers, List.mem_append]

theorem UnifiedStream.union_errorDiffCarriers_iff
    (l r : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.union l r)
      ↔ uc ∈ UnifiedStream.errorDiffCarriers l
        ∨ uc ∈ UnifiedStream.errorDiffCarriers r := by
  show uc ∈ UnifiedStream.errorDiffCarriers
              (UnifiedStream.consolidate (UnifiedStream.unionAll l r)) ↔ _
  rw [UnifiedStream.consolidate_errorDiffCarriers_iff,
      UnifiedStream.unionAll_errorDiffCarriers, List.mem_append]

/-! ## `exceptAll` and error scopes

`exceptAll L R := consolidate (unionAll L (negate R))`. The
right side flows through `negate`, which preserves both error
scopes (proven above). The composition with `consolidate`
preserves both scopes as sets. So `exceptAll`'s row-err set is
the disjoint union of `L`'s and `R`'s, and same for
collection-err. -/

theorem UnifiedStream.exceptAll_errCarriers_iff
    (l r : UnifiedStream) (e : EvalError) :
    e ∈ UnifiedStream.errCarriers (UnifiedStream.exceptAll l r)
      ↔ e ∈ UnifiedStream.errCarriers l ∨ e ∈ UnifiedStream.errCarriers r := by
  show e ∈ UnifiedStream.errCarriers
            (UnifiedStream.consolidate
              (UnifiedStream.unionAll l (UnifiedStream.negate r))) ↔ _
  rw [UnifiedStream.consolidate_errCarriers_iff,
      UnifiedStream.unionAll_errCarriers,
      UnifiedStream.negate_errCarriers, List.mem_append]

theorem UnifiedStream.exceptAll_errorDiffCarriers_iff
    (l r : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.exceptAll l r)
      ↔ uc ∈ UnifiedStream.errorDiffCarriers l
        ∨ uc ∈ UnifiedStream.errorDiffCarriers r := by
  show uc ∈ UnifiedStream.errorDiffCarriers
              (UnifiedStream.consolidate
                (UnifiedStream.unionAll l (UnifiedStream.negate r))) ↔ _
  rw [UnifiedStream.consolidate_errorDiffCarriers_iff,
      UnifiedStream.unionAll_errorDiffCarriers,
      UnifiedStream.negate_errorDiffCarriers, List.mem_append]

/-! ## `clampPositive` and error scopes

`clampPositive` keeps `.error` records and `.val n` records with
`0 < n`; drops everything else.

* Collection-err set: preserved exactly. `.error`-diff records
  always pass through (`isPositiveDiff .error = true`).
* Row-err set: not preserved — a `(.err e, .val 0)` or
  `(.err e, .val (-n))` record is dropped, so the row-err set
  may shrink. Only the reverse direction (output ⊆ input) holds. -/

theorem UnifiedStream.clampPositive_errorDiffCarriers_iff
    (us : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.clampPositive us)
      ↔ uc ∈ UnifiedStream.errorDiffCarriers us := by
  rw [UnifiedStream.mem_errorDiffCarriers,
      UnifiedStream.mem_errorDiffCarriers]
  constructor
  · intro h
    -- (uc, .error) ∈ clampPositive us = List.filter _ us ⇒ (uc, .error) ∈ us.
    exact (List.mem_filter.mp h).1
  · intro h
    exact UnifiedStream.clampPositive_preserves_error_diff us uc h

/-- Reverse direction for row-err: every err in the clamped
output was in the input. The forward direction fails because
clampPositive can drop `(.err e, .val 0)` records. -/
theorem UnifiedStream.clampPositive_errCarriers_of_mem
    (us : UnifiedStream) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers (UnifiedStream.clampPositive us)) :
    e ∈ UnifiedStream.errCarriers us := by
  rw [UnifiedStream.mem_errCarriers] at h
  rw [UnifiedStream.mem_errCarriers]
  obtain ⟨d, hMem⟩ := h
  exact ⟨d, (List.mem_filter.mp hMem).1⟩

/-! ## `clampToOne` and error scopes

`clampToOne` collapses positive `.val n > 0` to `.val 1`, drops
non-positive `.val`, and preserves `.error`. Same error-scope
behavior as `clampPositive`. -/

private theorem clampToOne_preserves_record_carrier
    (us : UnifiedStream) (rec : UnifiedRow × DiffWithError Int)
    (h : rec ∈ UnifiedStream.clampToOne us) :
    ∃ d, (rec.1, d) ∈ us := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    cases d' with
    | error =>
      -- clampToOne ((uc', .error) :: tl) = (uc', .error) :: clampToOne tl.
      have hEq : UnifiedStream.clampToOne ((uc', DiffWithError.error) :: tl)
              = (uc', DiffWithError.error) :: UnifiedStream.clampToOne tl := rfl
      rw [hEq] at h
      rcases List.mem_cons.mp h with hHead | hTail
      · subst hHead
        exact ⟨DiffWithError.error, List.mem_cons_self⟩
      · obtain ⟨d, hMem⟩ := ih hTail
        exact ⟨d, List.mem_cons_of_mem _ hMem⟩
    | val n =>
      by_cases hPos : 0 < n
      · have hEq : UnifiedStream.clampToOne ((uc', DiffWithError.val n) :: tl)
                = (uc', DiffWithError.val 1)
                    :: UnifiedStream.clampToOne tl := by
          show (if 0 < n then (uc', DiffWithError.val 1)
                          :: UnifiedStream.clampToOne tl
                else UnifiedStream.clampToOne tl)
              = _
          rw [if_pos hPos]
        rw [hEq] at h
        rcases List.mem_cons.mp h with hHead | hTail
        · -- rec = (uc', .val 1); carrier uc' is in input head.
          have hUc : rec.1 = uc' := by rw [hHead]
          rw [hUc]
          exact ⟨DiffWithError.val n, List.mem_cons_self⟩
        · obtain ⟨d, hMem⟩ := ih hTail
          exact ⟨d, List.mem_cons_of_mem _ hMem⟩
      · have hEq : UnifiedStream.clampToOne ((uc', DiffWithError.val n) :: tl)
                = UnifiedStream.clampToOne tl := by
          show (if 0 < n then (uc', DiffWithError.val 1)
                          :: UnifiedStream.clampToOne tl
                else UnifiedStream.clampToOne tl)
              = _
          rw [if_neg hPos]
        rw [hEq] at h
        obtain ⟨d, hMem⟩ := ih h
        exact ⟨d, List.mem_cons_of_mem _ hMem⟩

theorem UnifiedStream.clampToOne_errCarriers_of_mem
    (us : UnifiedStream) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers (UnifiedStream.clampToOne us)) :
    e ∈ UnifiedStream.errCarriers us := by
  rw [UnifiedStream.mem_errCarriers] at h
  rw [UnifiedStream.mem_errCarriers]
  obtain ⟨d, hMem⟩ := h
  exact clampToOne_preserves_record_carrier us (UnifiedRow.err e, d) hMem

/-- Reverse direction of clampToOne / `.error`-diff carriers:
a `(uc, .error)` in the clamped output came from a `(uc, .error)`
in the input. The `.val n` arms of clampToOne never emit `.error`,
so the `.error` output had to come from an `.error` input. -/
private theorem clampToOne_error_inv
    (us : UnifiedStream) (uc : UnifiedRow)
    (h : (uc, (DiffWithError.error : DiffWithError Int))
            ∈ UnifiedStream.clampToOne us) :
    (uc, (DiffWithError.error : DiffWithError Int)) ∈ us := by
  induction us with
  | nil => exact absurd h List.not_mem_nil
  | cons hd tl ih =>
    obtain ⟨uc', d'⟩ := hd
    cases d' with
    | error =>
      have hEq : UnifiedStream.clampToOne ((uc', DiffWithError.error) :: tl)
              = (uc', DiffWithError.error)
                  :: UnifiedStream.clampToOne tl := rfl
      rw [hEq] at h
      rcases List.mem_cons.mp h with hHead | hTail
      · have huc : uc = uc' := (Prod.mk.injEq _ _ _ _).mp hHead |>.1
        subst huc
        exact List.mem_cons_self
      · exact List.mem_cons_of_mem _ (ih hTail)
    | val n =>
      by_cases hPos : 0 < n
      · have hEq : UnifiedStream.clampToOne ((uc', DiffWithError.val n) :: tl)
                = (uc', DiffWithError.val 1)
                    :: UnifiedStream.clampToOne tl := by
          show (if 0 < n then (uc', DiffWithError.val 1)
                          :: UnifiedStream.clampToOne tl
                else UnifiedStream.clampToOne tl) = _
          rw [if_pos hPos]
        rw [hEq] at h
        rcases List.mem_cons.mp h with hHead | hTail
        · -- (uc, .error) = (uc', .val 1) impossible.
          have hDiff : (DiffWithError.error : DiffWithError Int)
                     = DiffWithError.val 1 :=
            (Prod.mk.injEq _ _ _ _).mp hHead |>.2
          cases hDiff
        · exact List.mem_cons_of_mem _ (ih hTail)
      · have hEq : UnifiedStream.clampToOne ((uc', DiffWithError.val n) :: tl)
                = UnifiedStream.clampToOne tl := by
          show (if 0 < n then (uc', DiffWithError.val 1)
                          :: UnifiedStream.clampToOne tl
                else UnifiedStream.clampToOne tl) = _
          rw [if_neg hPos]
        rw [hEq] at h
        exact List.mem_cons_of_mem _ (ih h)

/-- `clampToOne` preserves collection-err carriers. The
`(uc, .error)` records always survive the clamp (the `.error`
branch of clampToOne's recursion). Forward via the existing
`clampToOne_preserves_error_diff`; reverse via `clampToOne_error_inv`. -/
theorem UnifiedStream.clampToOne_errorDiffCarriers_iff
    (us : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.clampToOne us)
      ↔ uc ∈ UnifiedStream.errorDiffCarriers us := by
  rw [UnifiedStream.mem_errorDiffCarriers,
      UnifiedStream.mem_errorDiffCarriers]
  exact ⟨clampToOne_error_inv us uc,
         UnifiedStream.clampToOne_preserves_error_diff us uc⟩

/-! ## `distinct`, `bagExceptAll`, `bagIntersectAll`: composed theorems

`distinct = clampToOne ∘ consolidate`, `bagExceptAll = clampPositive ∘ exceptAll`,
`bagIntersectAll = clampPositive ∘ intersectAll`. Error-scope behavior
composes from the parts. The collection-err set is preserved exactly
(both parts preserve iff). The row-err set can only shrink. -/

theorem UnifiedStream.distinct_errorDiffCarriers_iff
    (us : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.distinct us)
      ↔ uc ∈ UnifiedStream.errorDiffCarriers us := by
  show uc ∈ UnifiedStream.errorDiffCarriers
              (UnifiedStream.clampToOne (UnifiedStream.consolidate us)) ↔ _
  rw [UnifiedStream.clampToOne_errorDiffCarriers_iff,
      UnifiedStream.consolidate_errorDiffCarriers_iff]

theorem UnifiedStream.distinct_errCarriers_of_mem
    (us : UnifiedStream) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers (UnifiedStream.distinct us)) :
    e ∈ UnifiedStream.errCarriers us := by
  -- distinct us = clampToOne (consolidate us).
  -- clampToOne_errCarriers_of_mem → in consolidate us.
  -- consolidate_errCarriers_iff → in us.
  have h1 : e ∈ UnifiedStream.errCarriers (UnifiedStream.consolidate us) :=
    UnifiedStream.clampToOne_errCarriers_of_mem _ e h
  exact (UnifiedStream.consolidate_errCarriers_iff us e).mp h1

theorem UnifiedStream.bagExceptAll_errorDiffCarriers_iff
    (l r : UnifiedStream) (uc : UnifiedRow) :
    uc ∈ UnifiedStream.errorDiffCarriers (UnifiedStream.bagExceptAll l r)
      ↔ uc ∈ UnifiedStream.errorDiffCarriers l
        ∨ uc ∈ UnifiedStream.errorDiffCarriers r := by
  show uc ∈ UnifiedStream.errorDiffCarriers
              (UnifiedStream.clampPositive (UnifiedStream.exceptAll l r)) ↔ _
  rw [UnifiedStream.clampPositive_errorDiffCarriers_iff,
      UnifiedStream.exceptAll_errorDiffCarriers_iff]

theorem UnifiedStream.bagExceptAll_errCarriers_of_mem
    (l r : UnifiedStream) (e : EvalError)
    (h : e ∈ UnifiedStream.errCarriers (UnifiedStream.bagExceptAll l r)) :
    e ∈ UnifiedStream.errCarriers l ∨ e ∈ UnifiedStream.errCarriers r := by
  have h1 : e ∈ UnifiedStream.errCarriers (UnifiedStream.exceptAll l r) :=
    UnifiedStream.clampPositive_errCarriers_of_mem _ e h
  exact (UnifiedStream.exceptAll_errCarriers_iff l r e).mp h1

end Mz
