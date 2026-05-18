# Transform catalog

Mechanized equational and inclusion laws for `UnifiedStream` / `TimedUnifiedStream` operators.
Grouped by algebraic shape so optimizer rewrites have a single index.

Each entry links theorem name to source file.
`L = R` denotes equality, `L ⊆ R` denotes one-direction membership (forward), `L = R ↔ P` denotes a logical iff over membership.

## Append / unionAll distribution

Operators that distribute over concatenation.
Each is direct from `List.flatMap_append` or `List.map_append` on the underlying carrier.

| Theorem | Statement | File |
| --- | --- | --- |
| `unionAll_assoc` | `(a ⊎ b) ⊎ c = a ⊎ (b ⊎ c)` | `Mz/SetOps.lean` |
| `filter_unionAll` | `filter p (a ⊎ b) = filter p a ⊎ filter p b` | `Mz/SetOps.lean` |
| `project_unionAll` | `project es (a ⊎ b) = project es a ⊎ project es b` | `Mz/SetOps.lean` |
| `negate_unionAll` | `negate (a ⊎ b) = negate a ⊎ negate b` | `Mz/SetOps.lean` |
| `cross_unionAll_left` | `cross (a ⊎ b) r = cross a r ⊎ cross b r` | `Mz/SetOps.lean` |
| `filter_append` | `filter p (a ++ b) = filter p a ++ filter p b` | `Mz/UnifiedStream.lean` |
| `project_append` | `project es (a ++ b) = project es a ++ project es b` | `Mz/UnifiedStream.lean` |
| `negate_append` | `negate (a ++ b) = negate a ++ negate b` | `Mz/SetOps.lean` |
| `cross_append_left` | `cross (a ++ b) r = cross a r ++ cross b r` | `Mz/Join.lean` |
| `errCarriers_append` | `errCarriers (a ++ b) = errCarriers a ++ errCarriers b` | `Mz/UnifiedStream.lean` |
| `errorDiffCarriers_append` | symmetric for collection-err | `Mz/UnifiedStream.lean` |
| `unionAll_errCarriers` | corollary | `Mz/SetOps.lean` |
| `unionAll_errorDiffCarriers` | corollary | `Mz/SetOps.lean` |
| `TimedUnifiedStream.errCarriers_append` | timed lift | `Mz/TimedConsolidate.lean` |
| `TimedUnifiedStream.errorDiffCarriers_append` | timed lift | `Mz/TimedConsolidate.lean` |

## Commutativity / sliding through

Two operators commute as `f ∘ g = g ∘ f`.

| Theorem | Statement | File |
| --- | --- | --- |
| `negate_filter` | `negate (filter p us) = filter p (negate us)` | `Mz/SetOps.lean` |
| `negate_project` | `negate (project es us) = project es (negate us)` | `Mz/SetOps.lean` |
| `negate_consolidate` | `negate (consolidate us) = consolidate (negate us)` | `Mz/SetOps.lean` |
| `negate_consolidateInto` (private) | step lemma for above | `Mz/SetOps.lean` |

## Pushdown

Rewrites that move an outer operator inside a join or product.

| Theorem | Statement | File |
| --- | --- | --- |
| `filter_cross_pushdown_left` | `filter p (cross l r) = cross (filter p l) r` when `p` bounded by left widths and `r` `IsPureData` | `Mz/JoinPushdown.lean` |

## Bilinearity (cross with negate)

Negation slides through cross from either side.

| Theorem | Statement | File |
| --- | --- | --- |
| `cross_negate_left` | `cross (negate l) r = negate (cross l r)` via `(-a) * b = -(a * b)` | `Mz/SetOps.lean` |
| `cross_negate_right` | `cross l (negate r) = negate (cross l r)` via `a * (-b) = -(a * b)` | `Mz/SetOps.lean` |

## Associativity

| Theorem | Statement | File |
| --- | --- | --- |
| `cross_assoc` | `cross (cross a b) c = cross a (cross b c)` modulo carrier-append associativity | `Mz/Join.lean` |
| `unionAll_assoc` | concat associative | `Mz/SetOps.lean` |
| `combineCarrier_assoc` | carrier-side associativity for cross | `Mz/Join.lean` |

## Involution / Idempotence

Applying an operator twice equals once (or zero times).

| Theorem | Statement | File |
| --- | --- | --- |
| `negate_negate` | `negate (negate us) = us` (involution) | `Mz/SetOps.lean` |
| `clampPositive_idem` | `clampPositive (clampPositive us) = clampPositive us` | `Mz/SetOps.lean` |
| `clampToOne_idem` | `clampToOne (clampToOne us) = clampToOne us` | `Mz/SetOps.lean` |
| `escalateRowErrs_idem` | `escalateRowErrs (escalateRowErrs us) = escalateRowErrs us` | `Mz/UnifiedStream.lean` |
| `advanceFrontier_idem` | `advanceFrontier f (advanceFrontier f s) = advanceFrontier f s` | `Mz/TimedConsolidate.lean` |
| `advanceFrontier_zero` | `advanceFrontier 0 s = s` (zero-frontier identity) | `Mz/TimedConsolidate.lean` |
| `advanceFrontier_advanceFrontier` | `advanceFrontier g (advanceFrontier f s) = advanceFrontier (max f g) s` | `Mz/TimedConsolidate.lean` |
| `clampPositive_clampToOne` | `clampPositive ∘ clampToOne = clampToOne` | `Mz/SetOps.lean` |

## Length / cardinality

Bounds on output cardinality. `_length` is equality, `_length_le` is upper bound.

| Theorem | Statement | File |
| --- | --- | --- |
| `unionAll_length` | `|a ⊎ b| = |a| + |b|` | `Mz/SetOps.lean` |
| `negate_length` | `|negate us| = |us|` | `Mz/SetOps.lean` |
| `cross_length` | `|cross l r| = |l| * |r|` | `Mz/Join.lean` |
| `filter_length_le` | filter non-expanding | `Mz/Join.lean` |
| `join_length_le` | `|join p l r| ≤ |l| * |r|` | `Mz/Join.lean` |
| `union_length_le` | bound via consolidate | `Mz/SetOps.lean` |
| `exceptAll_length_le` | `≤ |l| + |r|` | `Mz/SetOps.lean` |
| `clampPositive_length_le` | non-expanding | `Mz/SetOps.lean` |
| `clampToOne_length_le` | non-expanding | `Mz/SetOps.lean` |
| `distinct_length_le` | non-expanding | `Mz/SetOps.lean` |
| `intersectAll_length_le` | `≤ |l|` | `Mz/SetOps.lean` |
| `bagExceptAll_length_le` | composed bound | `Mz/SetOps.lean` |
| `bagIntersectAll_length_le` | composed bound | `Mz/SetOps.lean` |
| `consolidate_length_le` | merging never expands | `Mz/UnifiedConsolidate.lean` |
| `consolidate_strict_length_dup` | strict shrink on adjacent duplicate | `Mz/UnifiedConsolidate.lean` |
| `escalateRowErrs_length` | length-preserving | `Mz/UnifiedStream.lean` |
| `advanceFrontier_length` | length-preserving | `Mz/TimedConsolidate.lean` |
| `atTime_length_le` | non-expanding | `Mz/TimedConsolidate.lean` |
| `consolidateAtTime_length_le` | non-expanding | `Mz/TimedConsolidate.lean` |

## Trivial cases

Reductions on empty / singleton inputs.

| Theorem | Statement | File |
| --- | --- | --- |
| `*_nil` | operator applied to `[]` returns `[]` | various |
| `filter_nil`, `project_nil_stream`, `cross_nil_left`, `cross_nil_right`, `negate_nil` (implicit), `consolidate_nil`, `errCarriers_nil`, `errorDiffCarriers_nil`, `clampToOne_nil`, `escalateRowErrs_nil`, `advanceFrontier_nil`, `atTime_nil`, `consolidateAtTime_nil`, `consolidateInto_nil`, `unionAll_nil_left`, `unionAll_nil_right`, `union_nil_left`, `union_nil_right`, `bagExceptAll_nil_left`, `bagExceptAll_nil_right`, `exceptAll_nil_left`, `exceptAll_nil_right`, `lookup_nil` | | various |
| `consolidate_singleton` | `consolidate [(uc, d)] = [(uc, d)]` | `Mz/UnifiedConsolidate.lean` |
| `cross_singleton` | `cross [(uc, d)] r = r.map (combineCarrier uc rd.1, d * rd.2)` | `Mz/JoinPushdown.lean` |
| `project_nil_es` | empty projection list collapses rows | `Mz/UnifiedStream.lean` |

## Cons / step reductions

Named per-shape reductions of recursive operators.

| Theorem | Statement | File |
| --- | --- | --- |
| `cross_cons_left` | cross unfolding on `(hd :: tl)` left | `Mz/Join.lean` |
| `consolidateInto_match` | matching head folds into bucket | `Mz/UnifiedConsolidate.lean` |
| `consolidateInto_skip` | non-matching head recurses | `Mz/UnifiedConsolidate.lean` |
| `consolidateInto_nil` | trivial | `Mz/UnifiedConsolidate.lean` |

## Error-scope: row-err (`errCarriers`)

Set of row-scoped error payloads (carrier = `.err e`).
Iff = preserved exactly as set. Mono = forward inclusion only (one direction). `_of_mem` = reverse inclusion only.

| Theorem | Direction | File |
| --- | --- | --- |
| `unionAll_errCarriers` | `=` concat | `Mz/SetOps.lean` |
| `negate_errCarriers` | `=` | `Mz/SetOps.lean` |
| `consolidate_errCarriers_iff` | `↔` (set) | `Mz/UnifiedConsolidate.lean` |
| `union_errCarriers_iff` | disjoint union | `Mz/SetOps.lean` |
| `exceptAll_errCarriers_iff` | disjoint union | `Mz/SetOps.lean` |
| `filter_errCarriers_mono` | mono (cell→row promotion adds) | `Mz/SetOps.lean` |
| `project_errCarriers_mono` | mono (scalar errs add) | `Mz/UnifiedStream.lean` |
| `cross_errCarriers_from_left` | mono propagation | `Mz/Join.lean` |
| `cross_errCarriers_from_right` | mono propagation (left = `.row`) | `Mz/Join.lean` |
| `join_errCarriers_mono` | mono (filter of cross) | `Mz/SetOps.lean` |
| `clampPositive_errCarriers_of_mem` | reverse (clamps drop) | `Mz/SetOps.lean` |
| `clampToOne_errCarriers_of_mem` | reverse | `Mz/SetOps.lean` |
| `distinct_errCarriers_of_mem` | reverse | `Mz/SetOps.lean` |
| `bagExceptAll_errCarriers_of_mem` | reverse | `Mz/SetOps.lean` |
| `intersectAll_errCarriers_of_mem` | reverse (and: in both) | `Mz/SetOps.lean` |
| `bagIntersectAll_errCarriers_of_mem` | reverse (and: in both) | `Mz/SetOps.lean` |
| `escalateRowErrs_errCarriers` | `=` (carriers untouched) | `Mz/UnifiedStream.lean` |
| `TimedUnifiedStream.advanceFrontier_errCarriers` | `=` (time-only op) | `Mz/TimedConsolidate.lean` |
| `TimedUnifiedStream.atTime_errCarriers_subset` | reverse (slice drops) | `Mz/TimedConsolidate.lean` |
| `TimedUnifiedStream.consolidateAtTime_errCarriers_subset` | reverse | `Mz/TimedConsolidate.lean` |

## Error-scope: collection-err (`errorDiffCarriers`)

Carriers whose diff is `.error`.

| Theorem | Direction | File |
| --- | --- | --- |
| `unionAll_errorDiffCarriers` | `=` concat | `Mz/SetOps.lean` |
| `negate_errorDiffCarriers` | `=` | `Mz/SetOps.lean` |
| `consolidate_errorDiffCarriers_iff` | `↔` exact | `Mz/UnifiedConsolidate.lean` |
| `consolidate_errorDiffCarriers_mono` | forward (corollary) | `Mz/SetOps.lean` |
| `union_errorDiffCarriers_iff` | disjoint union | `Mz/SetOps.lean` |
| `exceptAll_errorDiffCarriers_iff` | disjoint union | `Mz/SetOps.lean` |
| `filter_errorDiffCarriers` | `=` (.error passes through) | `Mz/SetOps.lean` |
| `project_errorDiffCarriers` | `=` | `Mz/UnifiedStream.lean` |
| `cross_errorDiffCarriers_from_left` | forward propagation | `Mz/Join.lean` |
| `cross_errorDiffCarriers_from_right` | forward propagation | `Mz/Join.lean` |
| `join_errorDiffCarriers` | `=` (= cross) | `Mz/SetOps.lean` |
| `clampPositive_errorDiffCarriers_iff` | `↔` exact | `Mz/SetOps.lean` |
| `clampToOne_errorDiffCarriers_iff` | `↔` exact | `Mz/SetOps.lean` |
| `distinct_errorDiffCarriers_iff` | `↔` exact | `Mz/SetOps.lean` |
| `bagExceptAll_errorDiffCarriers_iff` | disjoint union | `Mz/SetOps.lean` |
| `intersectAll_errorDiffCarriers_of_mem` | reverse (in both) | `Mz/SetOps.lean` |
| `escalateRowErrs_errCarriers_in_errorDiff` | forward (promotion) | `Mz/UnifiedStream.lean` |
| `TimedUnifiedStream.advanceFrontier_errorDiffCarriers` | `=` | `Mz/TimedConsolidate.lean` |
| `TimedUnifiedStream.atTime_errorDiffCarriers_subset` | reverse | `Mz/TimedConsolidate.lean` |
| `TimedUnifiedStream.consolidateAtTime_errorDiffCarriers_subset` | reverse | `Mz/TimedConsolidate.lean` |

## Error-diff record-level absorption (forward)

`.error` diff survives the operator on the same carrier.

| Theorem | File |
| --- | --- |
| `consolidate_preserves_error` | `Mz/UnifiedConsolidate.lean` |
| `project_preserves_error_diff` | `Mz/UnifiedStream.lean` |
| `filter_preserves_error_diff` | `Mz/Join.lean` |
| `cross_diff_error_left` / `cross_diff_error_right` | `Mz/Join.lean` |
| `unionAll_preserves_error_diff_left` / `_right` | `Mz/SetOps.lean` |
| `union_preserves_error_diff_left` / `_right` | `Mz/SetOps.lean` |
| `exceptAll_preserves_error_diff_left` / `_right` | `Mz/SetOps.lean` |
| `intersectAll_preserves_error_diff_left` / `_right` | `Mz/SetOps.lean` |
| `bagExceptAll_preserves_error_diff_left` / `_right` | `Mz/SetOps.lean` |
| `bagIntersectAll_preserves_error_diff_left` / `_right` | `Mz/SetOps.lean` |
| `clampPositive_preserves_error_diff` | `Mz/SetOps.lean` |
| `clampToOne_preserves_error_diff` | `Mz/SetOps.lean` |
| `distinct_preserves_error_diff` | `Mz/SetOps.lean` |
| `negate_preserves_error_diff` | `Mz/SetOps.lean` |
| `TimedUnifiedStream.consolidateAtTime_preserves_error` | `Mz/TimedConsolidate.lean` |

## Error-diff inversion (reverse)

If output has `.error`, input had `.error` at that carrier.

| Theorem | File |
| --- | --- |
| `consolidate_error_inv` | `Mz/UnifiedConsolidate.lean` |
| `consolidateInto_error_inv` (private) | `Mz/UnifiedConsolidate.lean` |

## No-error preservation

All-`.val` inputs yield all-`.val` outputs (collection-err free).

| Theorem | File |
| --- | --- |
| `filter_no_error` | `Mz/Join.lean` |
| `project_no_error` | `Mz/UnifiedStream.lean` |
| `cross_no_error` | `Mz/Join.lean` |
| `consolidate_no_error` | `Mz/UnifiedConsolidate.lean` |
| `negate_no_error` | `Mz/SetOps.lean` |
| `unionAll_no_error` | `Mz/SetOps.lean` |
| `union_no_error` | `Mz/SetOps.lean` |
| `exceptAll_no_error` | `Mz/SetOps.lean` |
| `intersectAll_no_error` | `Mz/SetOps.lean` |
| `clampPositive_no_error` | `Mz/SetOps.lean` |
| `clampToOne_no_error` | `Mz/SetOps.lean` |
| `distinct_no_error` | `Mz/SetOps.lean` |
| `bagExceptAll_no_error` | `Mz/SetOps.lean` |
| `bagIntersectAll_no_error` | `Mz/SetOps.lean` |

## Multiplicity / shape constraints

| Theorem | Statement | File |
| --- | --- | --- |
| `clampPositive_only_positive` | output `.val` is strictly positive | `Mz/SetOps.lean` |
| `clampToOne_only_one_or_error` | output diff is `.val 1` or `.error` | `Mz/SetOps.lean` |
| `bagExceptAll_only_positive` | composed | `Mz/SetOps.lean` |
| `bagIntersectAll_only_positive` | composed | `Mz/SetOps.lean` |
| `distinct_only_one_or_error` | composed | `Mz/SetOps.lean` |

## Carrier uniqueness (NoDup)

| Theorem | File |
| --- | --- |
| `NoDupCarriers.nil` | `Mz/UnifiedConsolidate.lean` |
| `consolidate_noDup` | `Mz/UnifiedConsolidate.lean` |
| `union_noDup` | `Mz/SetOps.lean` |
| `exceptAll_noDup` | `Mz/SetOps.lean` |
| `bagExceptAll_noDup` | `Mz/SetOps.lean` |
| `intersectAll_noDup` | `Mz/SetOps.lean` |
| `bagIntersectAll_noDup` | `Mz/SetOps.lean` |
| `distinct_noDup` | `Mz/SetOps.lean` |
| `clampPositive_noDup` | `Mz/SetOps.lean` |
| `clampToOne_noDup` | `Mz/SetOps.lean` |
| `negate_noDup` | `Mz/SetOps.lean` |

## Membership bridges

Convert extractor / structural membership.

| Theorem | File |
| --- | --- |
| `mem_errCarriers` | `Mz/UnifiedStream.lean` |
| `mem_errorDiffCarriers` | `Mz/UnifiedStream.lean` |
| `mem_consolidate_of_mem` | forward carrier preservation | `Mz/UnifiedConsolidate.lean` |
| `mem_of_mem_consolidate` | reverse | `Mz/UnifiedConsolidate.lean` |
| `mem_cross_of_mems` | pair-membership in cross | `Mz/Join.lean` |
| `lookup_isSome_of_mem` | lookup characterization | `Mz/SetOps.lean` |
| `mem_of_lookup_eq_some` | lookup converse | `Mz/SetOps.lean` |
| `lookup_eq_of_mem_noDup` | exact diff under NoDup | `Mz/SetOps.lean` |
| `TimedUnifiedStream.mem_atTime_of_mem` | timed lift | `Mz/TimedConsolidate.lean` |

## Round-trip / iff

Combine forward and reverse direction.

| Theorem | File |
| --- | --- |
| `split_ofBag` | `BagStream` round-trip | `Mz/UnifiedStream.lean` |
| `split_data_ofBag`, `split_errors_ofBag` | components | `Mz/UnifiedStream.lean` |
| `TimedUnifiedStream.consolidateAll_eq_error_iff` | flat absorption | `Mz/Triple.lean` |
| `TimedUnifiedStream.consolidateAll_eq_error_iff_errorDiffCarriers` | extractor bridge | `Mz/Triple.lean` |
| `TimedUnifiedStream.consolidateAtTimeFlat_eq_error_iff` | per-time | `Mz/Triple.lean` |
| `TimedUnifiedStream.consolidateAll_error_inv` | reverse half | `Mz/Triple.lean` |
| `TimedUnifiedStream.consolidateAtTimeFlat_error_inv` | reverse half | `Mz/Triple.lean` |

## DiffWithError underlying laws

The semiring layer that operator proofs cite.

| Theorem | File |
| --- | --- |
| `error_add_left` / `error_add_right` | `Mz/DiffSemiring.lean` |
| `error_mul_left` / `error_mul_right` | `Mz/DiffSemiring.lean` |
| `error_min_left` / `error_min_right` | `Mz/DiffSemiring.lean` |
| `add_eq_error_left_or_right` | inversion | `Mz/DiffSemiring.lean` |
| `neg_error`, `neg_val`, `neg_neg_val` | negation laws | `Mz/DiffSemiring.lean` |
| `val_add_neg_val` | self-cancellation | `Mz/DiffSemiring.lean` |
| `neg_mul`, `mul_neg`, `neg_add` | distributive negation | `Mz/DiffSemiring.lean` |
| `min_val_val` | min on `.val` | `Mz/DiffSemiring.lean` |
| `mul_add`, `mul_assoc`, `mul_comm` | semiring laws | `Mz/DiffSemiring.lean` |
| `add_comm`, `add_assoc`, `zero_add_val`, `val_add_zero` | additive laws | `Mz/DiffSemiring.lean` |
| `*_int` specializations | base hypotheses discharged at `Int` | `Mz/DiffSemiring.lean` |
| `sumAll_eq_error_of_mem` | forward absorption | `Mz/Consolidate.lean` |
| `sumAll_error_inv` | reverse inversion | `Mz/Consolidate.lean` |
| `sumAll_val_of_all_val` | all-`.val` total | `Mz/Consolidate.lean` |

## Column-reference analyzers

Static analyses used by pushdown.

| Theorem | File |
| --- | --- |
| `colReferencesBoundedBy_mono` | bound is monotone | `Mz/ColRefs.lean` |
| `eval_append_left_of_bounded` | eval-on-left agreement | `Mz/ColRefs.lean` |
| `eval_append_right_shift` | shifted eval on right | `Mz/ColRefs.lean` |
| `colShift` monoid laws | various | `Mz/ColRefs.lean` |

## Notes

* `⊎` denotes `UnifiedStream.unionAll` (defined as `++` on the carrier).
* Extractors / scopes:
  * `errCarriers us` — list of row-scoped `.err e` payloads.
  * `errorDiffCarriers us` — list of carriers whose diff is `.error`.
* Iff vs forward vs reverse: many operators preserve the *set* of errors but not the multiset.
  Forward-only theorems hold when the operator can introduce new errs (cell-to-row promotion in `filter`, `project`).
  Reverse-only theorems hold when the operator can drop errs (the clamps).
* The pushdown / commutativity laws are the consumable API for an optimizer.
  Length and NoDup laws are invariants needed by cost models and uniqueness reasoning.
  Error-scope laws are observable-behavior guarantees for the error model.
