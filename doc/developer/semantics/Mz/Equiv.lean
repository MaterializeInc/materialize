import Mz.UnifiedStream2

/-!
# Stream equivalence: data-multiplicity equality

The denotational equality on `UnifiedStream2` is *data-multiplicity
equality* plus *global-marker equality*. Per-row err counts in
`Diff.errs` drop out of the relation.

The design doc
(`doc/developer/design/20260517_error_handling_semantics.md`,
§"Stream equivalence: data-multiplicity equality") motivates the
choice: many natural optimizer rewrites — `predicate_pushdown` over
`Cross(L, R)` is the canonical example — shuffle `ErrCount`
components without changing data multiplicities. Under strict list
equality these rewrites are unsound; under `Equiv` they are sound,
because the data side is unchanged and the SQL standard does not
observe err counts directly.

## Skeleton scope

This file is a foundation. It defines:

* `dataMult` — per-row sum of valid counts across all records.
* `hasGlobal` — whether any record carries a `.global` diff.
* `UnifiedStream2.Equiv` — the equivalence relation.

Plus immediate `Equiv` lemmas (reflexivity, symmetry, transitivity,
preservation under `++`).

The harder follow-up theorems — every operator (filter, project,
cross, negate, unionAll, consolidate, ...) is well-defined modulo
`Equiv`, and rewrites like `filter_cross_pushdown_left` hold modulo
`Equiv` rather than strict list equality — are deferred to future
work. -/

namespace Mz

namespace UnifiedStream2

/-- Sum of valid counts over all records carrying row `r`. A
`.global` diff contributes nothing — it is not data, and its
existence is captured separately by `hasGlobal`. -/
def dataMult (s : UnifiedStream2) (r : Row) : Int :=
  match s with
  | []        => 0
  | (r', d) :: rest =>
    let contrib : Int :=
      if r' = r then
        match d with
        | DiffWithGlobal.val d => d.val
        | DiffWithGlobal.global => 0
      else 0
    contrib + dataMult rest r

/-- Does the stream contain a `.global` diff anywhere? -/
def hasGlobal (s : UnifiedStream2) : Bool :=
  s.any fun rd => match rd.2 with
    | DiffWithGlobal.global => true
    | _ => false

/-! ## Equivalence

Two streams are equivalent iff they agree on every row's data
multiplicity and on whether they carry a `.global` marker.
Per-row err counts are *not* part of the relation. -/

/-- Stream equivalence. The denotational equality. -/
def Equiv (s₁ s₂ : UnifiedStream2) : Prop :=
  (∀ r : Row, dataMult s₁ r = dataMult s₂ r)
  ∧ hasGlobal s₁ = hasGlobal s₂

@[inherit_doc] infix:50 " ≡ " => Equiv

/-! ### Reflexivity / symmetry / transitivity -/

theorem Equiv.refl (s : UnifiedStream2) : s ≡ s :=
  ⟨fun _ => rfl, rfl⟩

theorem Equiv.symm {s t : UnifiedStream2} (h : s ≡ t) : t ≡ s :=
  ⟨fun r => (h.1 r).symm, h.2.symm⟩

theorem Equiv.trans {s t u : UnifiedStream2} (h₁ : s ≡ t) (h₂ : t ≡ u) : s ≡ u :=
  ⟨fun r => (h₁.1 r).trans (h₂.1 r), h₁.2.trans h₂.2⟩

instance : Trans (α := UnifiedStream2) Equiv Equiv Equiv :=
  ⟨Equiv.trans⟩

/-! ### Compatibility with `++`

The data-multiplicity and global-marker components both
distribute over concatenation. Concretely:

* `dataMult (s ++ t) r = dataMult s r + dataMult t r`
* `hasGlobal (s ++ t) = hasGlobal s || hasGlobal t`

These propagate to congruence: if `s₁ ≡ s₂` and `t₁ ≡ t₂` then
`s₁ ++ t₁ ≡ s₂ ++ t₂`. -/

theorem dataMult_append (s t : UnifiedStream2) (r : Row) :
    dataMult (s ++ t) r = dataMult s r + dataMult t r := by
  induction s with
  | nil =>
    show dataMult t r = 0 + dataMult t r
    rw [Int.zero_add]
  | cons hd tl ih =>
    obtain ⟨r', d⟩ := hd
    show (if r' = r then
            match d with
            | DiffWithGlobal.val d => d.val
            | DiffWithGlobal.global => 0
          else 0) + dataMult (tl ++ t) r
        = (if r' = r then
            match d with
            | DiffWithGlobal.val d => d.val
            | DiffWithGlobal.global => 0
          else 0) + dataMult tl r + dataMult t r
    rw [ih, Int.add_assoc]

theorem hasGlobal_append (s t : UnifiedStream2) :
    hasGlobal (s ++ t) = (hasGlobal s || hasGlobal t) := by
  unfold hasGlobal
  exact List.any_append

theorem Equiv.append {s₁ s₂ t₁ t₂ : UnifiedStream2}
    (hs : s₁ ≡ s₂) (ht : t₁ ≡ t₂) :
    s₁ ++ t₁ ≡ s₂ ++ t₂ := by
  refine ⟨?_, ?_⟩
  · intro r
    rw [dataMult_append, dataMult_append, hs.1 r, ht.1 r]
  · rw [hasGlobal_append, hasGlobal_append, hs.2, ht.2]

/-! ### Err-count irrelevance

The hallmark property: changing the `ErrCount` component of a
single record — keeping the row, the val count, and the
`.global`-or-not flag the same — produces an `Equiv` stream.

This is the lemma that makes the `predicate_pushdown` over
`Cross(L, R)` rewrite sound modulo `Equiv`. Strictly equal output
lists differ in `ErrCount`; `Equiv` ignores the difference. -/

theorem Equiv.errsChange_singleton
    (r : Row) (n : Int) (m m' : ErrCount) :
    [(r, DiffWithGlobal.val ⟨n, m⟩)] ≡ [(r, DiffWithGlobal.val ⟨n, m'⟩)] := by
  refine ⟨?_, rfl⟩
  intro r'
  show (if r = r' then n else 0) + 0
      = (if r = r' then n else 0) + 0
  rfl

/-! ### Zero-diff record absorption

A `(r, val ⟨0, m⟩)` record contributes nothing to data
multiplicity. Adding or removing one from the stream produces an
`Equiv` stream (provided the err counts can be relocated — but
under `Equiv` they need not be relocated at all). -/

theorem Equiv.cons_zero
    (r : Row) (m : ErrCount) (s : UnifiedStream2) :
    (r, DiffWithGlobal.val ⟨0, m⟩) :: s ≡ s := by
  refine ⟨?_, ?_⟩
  · intro r'
    show (if r = r' then (0 : Int) else 0) + dataMult s r' = dataMult s r'
    have : (if r = r' then (0 : Int) else 0) = 0 := by
      split <;> rfl
    rw [this, Int.zero_add]
  · show ((false : Bool) || hasGlobal s) = hasGlobal s
    rw [Bool.false_or]

/-! ## Future work (not in this skeleton)

* `filter_equiv_congr`: `s ≡ t → filter pred s ≡ filter pred t`.
* `cross_equiv_congr`: same for cross on both sides.
* `negate_equiv_congr`, `unionAll_equiv_congr`, `project_equiv_congr`.
* `filter_cross_pushdown_left_equiv`: the rewrite that fails under
  strict equality but holds modulo `Equiv`.
* `consolidate_equiv_id`: `s ≡ consolidate s` (consolidate is a
  representative-picking function within the equivalence class).
* `Equiv ↔ Multiset` — show `Equiv` is exactly the relation induced
  by viewing the stream as a multiset of `(row, val)` pairs plus
  the global flag.

These are deferred. The current file is the foundation. -/

end UnifiedStream2

end Mz
