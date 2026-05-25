# Transform catalog

Catalog of transforms we want the optimizer to be able to apply to
a `Collection n` and the soundness windows under which each holds.
This file is intentionally a *spec* rather than an index of Lean
theorem names: the model evolves, and the canonical answer to "is
this mechanized" is `Mz/*.lean`.
Each transform is stated as an equation or inclusion on collections,
independent of the mechanization path.
Where a sketch or mechanization exists in the skeleton, a pointer is
given; absence means "not yet attempted at this layer".

The scope is the time-stripped collection model: a multiset of rows
with `(diff, err_diff)` multiplicities, no time dimension.
Time-aware transforms (consolidation by `(row, time)` bucket,
frontier advancement) are described in `../platform/formalism.md`
and are out of scope here.

## Soundness windows

Several transforms are sound under more than one relation.
The catalog below tags each with the *strongest* relation under
which it has been mechanized or is expected to hold.

* `=` — strict equality on collections, update-by-update.
* `eraseErr` — data-side erasure: ignore `err_diff`, compare `row`
  and `diff`.
* `refines` — errors-as-bottom: transformed result has no error the
  original did not have.
* `NoRowErr P` — strict equality under a precondition that some
  side of the transform has no row-level errors (`err_diff = 0`
  for every update).
* `Schema` — discharged by a schema fact (typically a derived
  `cellErrFree` or `rowErrFree` claim).

See `model.md` for the full equivalence-relation discussion.

## Distribution

Operators that distribute over `unionAll` (`++`) or composition
with the empty collection.

| Transform | Statement | Relation |
| --- | --- | --- |
| `filter_append` | `filter p (a ⊎ b) = filter p a ⊎ filter p b` | `=` |
| `project_append` | `project es (a ⊎ b) = project es a ⊎ project es b` | `=` |
| `negate_append` | `negate (a ⊎ b) = negate a ⊎ negate b` | `=` |
| `cross_append_left` | `cross (a ⊎ b) r = cross a r ⊎ cross b r` | `=` |
| `cross_nil_left`, `cross_nil_right` | `cross ∅ r = cross l ∅ = ∅` | `=` |
| `filter_nil`, `project_nil`, `negate_nil` | operator applied to `∅` returns `∅` | `=` |
| `unionAll_nil_left`, `unionAll_nil_right` | `∅` is the identity for `⊎` | `=` |

## Associativity

| Transform | Statement | Relation |
| --- | --- | --- |
| `unionAll_assoc` | `(a ⊎ b) ⊎ c = a ⊎ (b ⊎ c)` | `=` |
| `cross_assoc` | `cross (cross a b) c = castCollection h (cross a (cross b c))` | `=` modulo arity cast `n+m+k = n+(m+k)` |
| `crossOne_assoc` | update-level cross associativity, same cast | `=` |

## Involution / idempotence

| Transform | Statement | Relation |
| --- | --- | --- |
| `negate_negate` | `negate (negate s) = s` | `=` |
| `filter_idem` (open) | `filter p (filter p s) = filter p s` | `=` |
| `filter_filter_comm` (open) | `filter p (filter q s) = filter q (filter p s)` | `=` when both predicates are cell-err-free; otherwise an err-multiplicity reorder under `eraseErr` |

## Filter / project / cross pushdown

The classical relational rewrites that move an operator across a
join or product.
The skeleton has uncovered a soundness gap on `filter ∘ cross`
which other pushdowns share if they multiply err multiplicities
against data multiplicities.

| Transform | Statement | Relation |
| --- | --- | --- |
| `filter_cross_pushdown_left` | `filter p (cross l r) = cross (filter p l) r` when `p`'s columns are bounded by `l`'s arity | unsound under `=` (counterexample `filterOne_cross_pushdown_left_unsound`); sound under `eraseErr` (`filter_cross_pushdown_left_data`); sound under `=` given `NoRowErr r` (`filter_cross_pushdown_left_strict`); plausibly sound under `refines` LHS → RHS (open — `Datum.refines` not yet lifted to `Update`/`Collection`) |
| `filter_cross_pushdown_right` (open) | symmetric, via `colShift` to realign the predicate; substrate `eval_append_right_shift` mechanized in `Mz/ColRefs.lean` | mirror of the left form |
| `project_cross_pushdown` (open) | `project es (cross l r) = cross (project es_l l) (project es_r r)` when `es` splits cleanly into left and right column-sets | expected `=` when the split is clean |
| `filter_project_pushdown` (open) | `filter p (project es s) = project es (filter (p.subst es) s)` | data side under `=` via `eval_subst`; err side asks the same multiplicity question as the cross pushdown |

The pushdown gap. `cross`'s err-diff is bilinear in data and err
multiplicities (`dL · eR + eL · dR + eL · eR`). A filter that zeroes
`dL` before the cross drops the `dL · eR` term that the post-cross
filter preserves. Three recovery windows:

* **Data-side equivalence (`eraseErr`).** Drop `err_diff` from the
  comparison. Sound; loses the user-visible "row errored" signal.
* **`NoRowErr` precondition.** Assume the right collection has
  `err_diff = 0` on every update. Sound at `=`. Discharged by a
  schema's `rowErrFree` bit (`NoRowErr_of_satisfies_rowErrFree`).
* **`refines` posture.** Allow the transformed side to lose
  errors. Plausibly sound LHS → RHS only, but the lift of
  `Datum.refines` to `Update` / `Collection` is not yet
  mechanized; the comment block at `Mz/Equiv.lean` lines 267-296
  also flags carrier-row shape mismatches under `eqErrSet`
  lifted pointwise, so the plausibility is not yet underwritten.

## Substitution

`Expr.subst` realigns column references in a predicate against an
explicit row of expressions.
The headline theorem (mechanized in `Mz/Subst.lean`) is the value
form:

| Transform | Statement | Relation |
| --- | --- | --- |
| `eval_subst` | `eval env (p.subst es) = eval (es.map (eval env)) p`, unconditionally on `env : Env` and `es : List Expr` | `=` |

This is the substrate for the project pushdown above.

## Column-pruning

Dropping a column nothing reads must be invisible.
The kernel lemma is mechanized; the lift to operators on
`Collection` is open.

| Transform | Statement | Relation |
| --- | --- | --- |
| `eval_replaceAt_of_unused` | replacing the value at an unused column index leaves `eval` unchanged (`Mz/ColRefs.lean`) | `=` |
| `project_drop_unused` (open) | `project es s = project es s'` where `s'` is `s` with an unused-by-`es` column erased | `=` via lifting `eval_replaceAt_of_unused` over the collection |
| `filter_drop_unused` (open) | symmetric, for a column unused by the filter predicate | `=` |

## Eval-on-append substrates

Two mechanized facts in `Mz/ColRefs.lean` underwrite the pushdown
rewrites; they are not operator-level transforms in themselves but
are what makes the pushdown statable.

| Lemma | Statement | Used by |
| --- | --- | --- |
| `eval_append_left_of_bounded` | `eval (envL ++ envR) p = eval envL p` when `p`'s columns are bounded by `envL.length` | `filter_cross_pushdown_left_*` (via `eval_crossOne_left_bounded`) |
| `eval_append_right_shift` | `eval (envL ++ envR) (p.colShift envL.length) = eval envR p` | substrate for the open right-side pushdown |

## Negate and cross bilinearity

| Transform | Statement | Relation |
| --- | --- | --- |
| `cross_negate_left` (open) | `cross (negate l) r = negate (cross l r)` | expected `=` via `(-a) * b = -(a * b)` |
| `cross_negate_right` (open) | `cross l (negate r) = negate (cross l r)` | expected `=` |
| `negate_filter` (open) | `negate (filter p s) = filter p (negate s)` | `=` (filter touches multiplicities, not signs) |
| `negate_project` (open) | `negate (project es s) = project es (negate s)` | `=` |

## Schema-driven rewrites

Transforms whose soundness depends on schema facts about the
inputs.
Each entry assumes the input collection satisfies a `Schema n` (see
`model.md`).

| Transform | Statement | Schema premise |
| --- | --- | --- |
| `coalesce_collapse` | `coalesce(a, b) = a` when `a` evaluates concrete | `a`'s `outputType` has `nullable = false ∧ errable = false`; mechanized via `eval_coalesce_pair_of_a_concrete` |
| `NoRowErr_filter` | `filter p` preserves `NoRowErr s` | `s` satisfies a schema with `cellErrFree`, and `p.might_error = false` |
| `NoRowErr_cross` | `cross l r` preserves `NoRowErr l ∧ NoRowErr r` | unconditional |
| `is_null_fold` (open) | `is_null(a) = false` when `a`'s column is non-nullable | `a`'s `outputType.nullable = false` |
| `non_null_join_key` (open) | join key need not check NULL | both sides' key columns are non-nullable |

## Output-schema propagation

`Expr.outputType sch e` derives the output `ColSchema` for a scalar
expression on a row satisfying input schema `sch`.
The mechanized soundness statement is:

| Transform | Statement |
| --- | --- |
| `eval_satisfies_outputType` | `DatumSatisfies (Expr.outputType sch e) (eval row.toList e)` whenever `row` satisfies `sch` |

Currently precise on `.lit` and `.col`; conservative `{ nullable :=
true, errable := true }` on every other constructor.
Per-constructor refinements (`.not` preserves; arithmetic propagates
`errable`; `.divide` always errable; `.ifThen` is the disjunction
of arms; variadic constructors via mutual recursion) are open work
listed in `model.md`.

## Constructor-level laws

Recorded for completeness; the scalar layer also carries equational
laws that downstream collection rewrites depend on.

* Identity and absorption for variadic `andN`, `orN`, `coalesce`
  (`Mz/Variadic.lean`).
* Four-valued absorption order `FALSE > ERROR > NULL > TRUE` for
  `evalAnd` and the dual for `evalOr` (`Mz/Boolean.lean`).
* `evalPlus` associativity over unbounded `Int`; failure of
  associativity at the bounded-int boundary
  (`Mz/EquivBounded.lean`).
* Substitution laws on `colShift` and `colReferencesBoundedBy`
  (`Mz/ColRefs.lean`).

## Out of scope at this layer

* **Consolidation.** Folding updates to the same `(row, time)`
  bucket. Requires the time dimension. See `../platform/formalism.md`.
* **Distinct, intersect, except.** Multiplicity-clamping operators
  whose definitions presume consolidation.
* **Aggregates / reduce / top-k.** Need an ordering or a fold
  operator on multisets.
* **Frontier advancement and compaction.** Time-aware operators.

## Notes

* `⊎` denotes `Collection.unionAll` (defined as `++` on the
  underlying list).
* "Open" in the relation column means the transform is stated here
  as expected to hold but is not yet mechanized.
* Cases where a transform fails to close at `=` are the most
  interesting: they signal a non-equality-preserving rewrite and
  force a choice of equivalence relation. The `filter ∘ cross`
  pushdown is the canonical example; see `model.md` for the four
  postures it surfaces.
