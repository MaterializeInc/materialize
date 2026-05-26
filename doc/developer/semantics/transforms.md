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
* `eraseRowErr` — data-side erasure: ignore `err_diff`, compare `row`
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
| `unionAll_comm_equiv` | `unionAll a b ≈ unionAll b a` | `Collection.Equiv` (perm only); fails at `=` |
| `cross_assoc` | `cross (cross a b) c = castCollection h (cross a (cross b c))` | `=` modulo arity cast `n+m+k = n+(m+k)` |
| `crossOne_assoc` | update-level cross associativity, same cast | `=` |
| `negate_unionAll_self` | `unionAll (negate s) s ≈ []` | `Collection.Equiv` (perm + merge + drop_zero); fails at `=` (no consolidation) |

## Involution / idempotence

| Transform | Statement | Relation |
| --- | --- | --- |
| `negate_negate` | `negate (negate s) = s` | `=` |
| `filter_idem` (open) | `filter p (filter p s) = filter p s` | `=` |
| `filter_filter_comm` (open) | `filter p (filter q s) = filter q (filter p s)` | `=` when both predicates are cell-err-free; otherwise an err-multiplicity reorder under `eraseRowErr` |

## Filter / project / cross pushdown

The classical relational rewrites that move an operator across a
join or product.
The skeleton has uncovered a soundness gap on `filter ∘ cross`
which other pushdowns share if they multiply err multiplicities
against data multiplicities.

| Transform | Statement | Status |
| --- | --- | --- |
| `filter_cross_pushdown_left` (open) | `filter p (cross l r) = cross (filter p l) r` when `p`'s columns are bounded by `l`'s arity | **open in indexed model.** Counterexample (`filterOne_cross_pushdown_left_unsound`) and three soundness windows (strict via `NoRowErr`, data-side via `eraseRowErr`, refinement via `SignOK`) were all mechanized in the untyped predecessor; not yet ported. Depends on `cross` (also open). |
| `filter_cross_pushdown_right` (open) | symmetric, via `colShift` to realign the predicate | mirror of the left form |
| `project_cross_pushdown` (open) | `project es (cross l r) = cross (project es_l l) (project es_r r)` when `es` splits cleanly into left and right column-sets | expected `=` when the split is clean |
| `filter_project_pushdown` (open) | `filter p (project es s) = project es (filter (p.subst es) s)` | data side under `=` via `eval_subst`; err side asks the same multiplicity question as the cross pushdown |

The pushdown gap. `cross`'s err-diff is bilinear in data and err
multiplicities (`dL · eR + eL · dR + eL · eR`). A filter that zeroes
`dL` before the cross drops the `dL · eR` term that the post-cross
filter preserves. Three recovery windows (all mechanized in the
untyped predecessor; not yet ported to the indexed model):

* **Data-side equivalence (`eraseRowErr`).** Drop `err_diff` from
  the comparison. Sound; loses the user-visible "row errored"
  signal.
* **`NoRowErr` precondition.** Assume the right collection has
  `err_diff = 0` on every update. Sound at `=`. Intended discharge
  via schema-fact bridge.
* **`refines` posture.** Allow the transformed side to lose
  errors. Intended theorem name
  `filter_cross_pushdown_left_refines` under a sign side
  condition `SignOK l r := ∀ recL ∈ l, ∀ recR ∈ r,
  0 ≤ recL.diff * recR.err_diff`.

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

Dropping a column nothing reads must be invisible. With the GADT,
column references use `Fin n` indices; out-of-bounds is
unconstructible. Column-pruning rewrites that need to reshape the
schema (drop column `i` and reindex the rest) are
schema-transforming and handled via `Subst` with a smaller schema.

| Transform | Statement | Status |
| --- | --- | --- |
| `project_drop_unused` (open) | `project es s = project es s'` where `s'` is `s` with an unused-by-`es` column erased | open; substrate is `Subst` and `Schema.append` lemmas |
| `filter_drop_unused` (open) | symmetric, for a column unused by the filter predicate | open |

Column-reference analyses of the untyped predecessor
(`colReferencesBoundedBy`, `colShift`, `colReferencesUnused`,
`eval_replaceAt_of_unused`) are subsumed by the GADT — `.col i`
uses `Fin n` and `Subst` is schema-transforming.

## Eval-on-append substrates

Pre-GADT, these were lemmas about untyped `Env` (list) append. In
the indexed model, the analogue is `Env.append` over schema-typed
lookups, with `Schema.append`-level type casts. Not yet
mechanized; tracked with `cross`.

| Lemma (open) | Statement | Used by |
| --- | --- | --- |
| `Env.append_left` (open) | accessing index `i < n` of an appended env returns the left cell | `cross`'s row composition |
| `Env.append_right` (open) | accessing index `n ≤ i < n + m` returns the right cell at `i - n` | `cross`'s row composition |

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
| `coalesce_collapse` (open) | `coalesce(a, b) = a` when `a` evaluates concrete | open; untyped predecessor mechanized via `eval_coalesce_pair_of_a_concrete`. Indexed re-port pending |
| `NoRowErr_filter` (open) | `filter p` preserves `NoRowErr s` when `p.might_error = false` over `s` | open; needs `RowSatisfies → EnvErrFree` bridge ported |
| `NoRowErr_cross` (open) | `cross l r` preserves `NoRowErr l ∧ NoRowErr r` | open; depends on `cross` |
| `NoRowErr_project` | `project es s` preserves `NoRowErr s` | mechanized (unconditional) |
| `NoRowErr_unionAll` | `unionAll a b` preserves `NoRowErr a ∧ NoRowErr b` | mechanized (conjunctive) |
| `NoRowErr_negate` | `negate s` preserves `NoRowErr s` | mechanized (unconditional) |
| `is_null_fold` (open) | `is_null(a) = false` when `a`'s column is non-nullable | `a`'s `outputCols.nullable = false` |
| `non_null_join_key` (open) | join key need not check NULL | both sides' key columns are non-nullable |
| `schema_filter` (open) | `filter p s` output schema preserves `s.cols`, sets `rowErrFree` from `p`'s err-freedom | `p.might_error = false` over `s` |
| `schema_project` (open) | `project es s` output schema lifts `Expr.outputType` over the projection vector | unconditional |
| `schema_cross` (open) | `cross l r` output schema is `Schema.append l_sch r_sch` | unconditional |
| `cellErrFree_propagation` (open) | `cellErrFree` of output of `filter` / `project` / `cross` from `cellErrFree` inputs, modulo per-cell err analysis on projection expressions | per-operator |

## Output-schema propagation

`Expr.outputCols sch e` derives the output `ColSchema` for a
schema-indexed `Expr`. Soundness theorem is open in the indexed
model.

| Transform | Statement | Status |
| --- | --- | --- |
| `eval_satisfies_outputCols` (open) | `DatumSatisfies (Expr.outputCols sch e) (eval env e)` whenever `env : Env sch` satisfies `sch` | open; untyped predecessor had `eval_satisfies_outputType` |

Per-constructor precision (`Mz/OutputType.lean`):
* `.lit` / `.col` — precise.
* `.not a` — preserves both bits of `a` (`evalNot` is strict on
  `.null` and `.err _`).
* `.plus` / `.minus` / `.times` / `.eq` / `.lt` — `errable` is the
  OR of the inputs' `errable`. `nullable := true` because the
  four-valued lattice routes type-mismatched operands to `.null`
  (`evalPlus (.bool true) (.int 5) = .null`) even when no input is
  nullable.
* `.ifThen c t e` — `errable` is the OR over the three arms.
  `nullable := true` (a non-bool `c` routes to `.null`).
* `.divide` — both bits `true`. Division-by-zero forces
  `errable`; type-mismatch routes to `.null` for `nullable`.
* `.andN` / `.orN` / `.coalesce` — conservative weakest schema.
  Tighter rules need mutual recursion mirroring
  `Expr.argsMightError` (open follow-up).

Open output-schema obligations:

* Variadic `.andN` / `.orN` / `.coalesce` — mutual recursion to
  express `errable := args.any errable` (or, for `.coalesce`,
  `errable := args.all errable` with the rescue rule).
* `.divide` static-divisor tightening — when the divisor is a
  statically-safe literal, `errable := false`. Overlaps with
  `might_error`'s `divisorIsSafe` analysis.
* Cell-error-free row schema: an analogue of `NoRowErr` for the
  per-cell `Datum.IsErr` condition. Distinct from `NoRowErr`
  (row-level err multiplicity).
* Precision direction on `Expr.outputType` per non-foundational
  constructor — `eval_not_satisfies_precise` in `Mz/WellTyped.lean`
  is the demonstration; arithmetic / comparison constructors
  follow the same pattern (each needs its own per-case proof under
  `WellTyped`).

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

## Scalar rewrites driving operator simplification

Local rewrite rules on `MirScalarExpr` that the Rust optimizer
applies to a fixed point (`src/expr/src/scalar/reduce.rs` and the
sibling `unary`/`binary`/`variadic`/`if_then` modules).
Each rewrite is an `Expr`-level equivalence; downstream operators
benefit because `filter p`, `project es`, and `Reduce` all consume
`eval ... p` or `eval ... es.get i`.
The catalog below uses the relation `=` on `Datum` under the
existing `Mz/Eval.lean` evaluator unless noted.

### Generic folds

| Rewrite | Statement | Status |
| --- | --- | --- |
| `constant_fold` | all-literal operands → evaluate to a literal | implicit via `Expr.eval`; no standalone theorem stating "fold-when-all-args-literal" — open if the optimizer wants to cite one |
| `null_propagate` | strict func + any operand `.null` → `.null` | partial (`Mz/Strict.lean` `NullPropagatingBinary` / `NullStrictUnary` for the modeled funcs) |
| `err_propagate` | strict func + any operand `.err _` → `.err _` | partial (`Mz/Strict.lean` `ErrPropagatingBinary` / `ErrStrictUnary` for the modeled funcs) |

### Boolean fragment

| Rewrite | Statement | Status |
| --- | --- | --- |
| `not_not` | `Not(Not x) = x` | mechanized (`Mz/Boolean.lean` `not_not`) |
| `not_binary_negate` | `Not(a <op> b) = a <neg-op> b` for `op ∈ {<, ≤, =, ≠, ...}` | open (per-`BinaryFunc::negate` rewrites; need a per-op equivalence pair) |
| `demorgan` | `Not(And ...) = Or (Not ...)`; dual for `Or` | open |
| `andN_flatten` | nested `andN` chains flatten to one variadic | open (existing variadic shape is already flat; the Rust source-form rewrite is structural and is unnecessary in this skeleton) |
| `andN_canonicalize` | dedup operands; drop `true`; absorb `false`; sort | partial (`Mz/Variadic.lean` `evalAndN_false_absorbs` + identity); full canonicalization is open |
| `orN_canonicalize` | dual of above | partial (`Mz/Variadic.lean` `evalOrN_true_absorbs` + identity) |
| `undistribute_and_or` | factor common conjuncts/disjuncts across `Or`/`And` | open |

### If-then

| Rewrite | Statement | Status |
| --- | --- | --- |
| `if_lit_true` | `if .bool true then t else e = t` | mechanized (case of `evalIfThen` in `Mz/Eval.lean`) |
| `if_lit_false_or_null` | `if .bool false then t else e = e`; `if .null then t else e = e` | mechanized (cases of `evalIfThen`) |
| `if_lit_err` | `if .err e then _ else _ = .err e` | mechanized (case of `evalIfThen`) |
| `if_equal_arms` | `if c then t else t = t` | open |
| `if_bool_arm` | `if c then true else e = (c IS NOT NULL ∧ c) ∨ e` and three duals | open; preserves the asymmetric NULL handling that `evalIfThen` already encodes |
| `if_arm_factor_unary` | `if c then f x else f y = f (if c then x else y)` | open |
| `if_arm_factor_binary_shared` | `if c then f(a,x) else f(a,y) = f(a, if c then x else y)` and right-shared mirror | open |

### Coalesce

| Rewrite | Statement | Status |
| --- | --- | --- |
| `coalesce_all_null` | `coalesce(null, ..., null) = null` | mechanized (`Mz/Coalesce.lean` `coalesce_nil` + variadic walk in `PrimEval.lean`) |
| `coalesce_drop_null` | drop `.null` operands; preserve order | open |
| `coalesce_truncate_after_concrete` | truncate after first operand known non-null non-err | mechanized at the head (`Mz/Schema.lean` `evalCoalesce_cons_of_concrete`); generalization across an arbitrary prefix is open |
| `coalesce_dedup` | dedup adjacent duplicate operands | open |
| `coalesce_singleton` | `coalesce(a) = a` | mechanized (`Mz/Coalesce.lean` `coalesce_singleton_*`) |

### Schema-aware scalar folds

Many of these require `Expr.WellTyped sch e` as a precondition —
without it, the four-valued evaluator routes type-mismatched
operands to `.null`, and the schema-driven `nullable := false`
conclusion can't be drawn.

| Rewrite | Statement | Status |
| --- | --- | --- |
| `is_null_non_nullable` | `IsNull e = .bool false` when `Expr.outputType e .nullable = false` | open (would ride on `Mz/OutputType.lean` once `IsNull` is added to `Expr`; `IsNull` is currently not modeled) |
| `is_null_decompose` | `IsNull(f a b) = IsNull a ∨ IsNull b` when `f` is null-propagating | open; needs `WellTyped` to rule out type-mismatch routing |
| `outputType_precision` (open) | under `WellTyped sch e`, arithmetic / comparison `outputType` tightens `nullable := OR-of-inputs.nullable` (no spurious `.null` from type-mismatch) | open; `WellTyped` predicate landed in `Mz/WellTyped.lean`, precision direction rides on it |

### Out of scope at the scalar layer

* **Records and `RecordGet`/`RecordCreate` rewrites.** `Datum` does
  not model records; rewrites of the form
  `RecordGet(i)(RecordCreate(args)) = args[i]` and the
  `record_create(...) = record_create(...) → AND` decomposition
  require lifting `Datum` to nested values.
* **Lists and `ListCreate`/`ListIndex` partial evaluation.** Same
  reason.
* **Date / time / timezone / regex / like / format
  specialization.** These rewrites bake a literal argument into a
  unary-func variant. The substitution is semantic (`f c x =
  (specialize f c) x`) but each func is type-specific and not
  modeled at the four-valued-plus-`Int` `Datum` layer.
* **`Eq` / `NotEq` argument canonical ordering.** Cosmetic
  reordering with no semantic content beyond `eqErrSet`
  commutativity (already discussed in `model.md`).

## From the lowering (MIR → LIR)

Operator-level transforms that the Rust lowering pipeline
(`src/compute-types/src/plan/lowering.rs`) applies as it converts
`MirRelationExpr` to `Plan`.
The lowering bundles map/filter/project into a canonical MFP form
and threads it through every absorbing operator, so most of these
are *fusion* and *absorption* rules.

| Rewrite | Statement | Status |
| --- | --- | --- |
| `mfp_canonicalize` | every `(Map | Filter | Project)*` chain fuses into a single `MapFilterProject(map_exprs, filter_preds, project_perm)`; composition is associative with identity | open (requires an MFP datatype on `Collection`; the per-operator pieces — `filter ∘ filter` fusion, `project ∘ project` via `eval_subst`, `filter_project_pushdown` — are listed elsewhere in this file) |
| `empty_input_collapse` | when an operator's input is provably the empty collection, the whole subtree is `∅` | partial — pointwise `filter_nil`, `project_nil`, `cross_nil_left`/`right`, `negate_nil` mechanized; the "if input provably empty, replace subtree" meta-statement is open |
| `predicate_split_around_op` | when `op` admits some conjuncts and not others (e.g. `Join`, `Reduce` cannot absorb temporal predicates), split `p = p_keep ∧ p_push`; push only `p_push` inside | open; requires an "operator-admissible predicate" static analysis (analogue of `colReferencesBoundedBy` for temporal-freeness) |
| `mfp_into_reduce` | `Reduce ∘ MFP_after = Reduce_with_post_MFP` when `MFP_after` is non-temporal and operates only on Reduce-produced columns | open (requires `Reduce`) |
| `reduce_unnest_list_fusion` | `FlatMap UnnestList ∘ Project[k] ∘ Reduce window_fn = Reduce_with_unnested_output`; changes the rule "Reduce emits one row per group" — semantically a different operator | open (requires `Reduce`, `FlatMap`, and window-function aggregates) |

### Out of scope from the lowering

* **Arrangements and `AvailableCollections`.** `ArrangeBy`
  insertion / fusion, key selection, `permutation_for_arrangement`,
  thinning. The same collection in two arrangements is the same
  multiset; the denotational spec is invariant.
* **`has_future_updates` propagation.** Time dimension; defer with
  the rest of the time-stripped layer's omissions.
* **`GetPlan` selection** (`PassArrangements` / `Arrangement` /
  `Collection`) and **`IndexedFilter` / `Delta` / `Differential`
  join implementation choices.** Physical planning; all produce
  the same multiset.
* **`LiteralConstraints` extraction for indexed sources.** Useful
  but requires modeling indexed sources at this layer.

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
