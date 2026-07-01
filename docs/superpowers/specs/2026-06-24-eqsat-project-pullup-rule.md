# Eqsat projection pull-up across joins

## Problem

On LDBC BI (Q20 family) the eqsat physical pass converts two production delta joins to differential.
The verified cause is that eqsat hands `JoinImplementation` a join whose input is a narrowing projection over an index-backed relation (`Project(#1,#2)(Get person_knows_person)`), instead of the bare multi-index relation production hands it (`Get::PassArrangements person_knows_person`).
`JoinImplementation` reads index availability off the input's structure, so it cannot see that `person_knows_person` carries `person1id`, `person2id`, and `(person1id, person2id)` indexes through the frozen projection.
The delta path needs both `person1id` and `person2id` arranged; with only one visible it must build a new arrangement, so the non-free delta loses to differential.
See `project_egraph_delta_to_differential_regression` for the full investigation.

Production avoids this because `ProjectionPushdown::skip_joins` and `JoinImplementation`'s MFP lifting keep the projection *above* the join, leaving the join input a bare index-backed `Get`.
The e-graph has no rule relating the projection-below-join-input form to the projection-above-join form, so the good variant is never generated and never extractable.

## Goal

Add a rule that pulls a projection from a join input up above the join.
With the projection lifted, the join input becomes a bare index-backed `Get`, which the existing CSE guard (`reads_index_backed_get`, `cse.rs`) keeps inlined and uncommitted, so `JoinImplementation` sees every maintained index and can choose a free delta join.
Frame the rule so it drops into a bidirectional `<=>` form once the DSL supports it.

## Why pull-up alone restores the index

The join input today is `Project[o](Get R)`, narrowed and (because the narrowed read is shared into a single-key CTE) committed to one index.
After pull-up the join input is the bare `Get R`.
The existing `reads_index_backed_get` guard already keeps a bare index-backed `Get` inlined per use rather than hoisting it into a shared single-key binding, so the join gets its own uncommitted multi-index read.
Pull-up removes the projection barrier; the existing guard then prevents the single-index commitment.
The projection moves to `Project[lifted]` above the join, exactly where production keeps it.

## The rule (binary case, concrete deliverable)

```
rule pull_project_out_of_join_first {
    doc "join(project(o, a), b) = project(m, join(a, b)),  m = o ++ shift(iota(|b|), |a|)"
    Join[e](Project[o] a, b)
        => Project[concat(o, shift(iota(arity(b)), arity(a)))](
               Join[remap_equivs(e, concat(o, shift(iota(arity(b)), arity(a))))](a, b))
    where is_binary_join()
    where non_identity_projection(o, a)
    where reads_indexed_global(a)
}
```

`a` is the relation under the projection (the bare `Get R` after pull-up), arity `arity(a)`.
`o` is the projection list on the first input, length `|o| <= arity(a)`.
`b` is the second input, arity `arity(b)`.

### The shared remap expression `m`

Both the lifted output projection and the equivalence remap are the same column map `m` from old join-output space to new join-output space.

Old output space: input-0 projected at `[0, |o|)`, then `b` at `[|o|, |o| + arity(b))`.
New output space: `a` full at `[0, arity(a))`, then `b` at `[arity(a), arity(a) + arity(b))`.

`m[c] = o[c]` for `c < |o|` (a projected column maps to its underlying column in the full `a`).
`m[c] = c + (arity(a) - |o|)` for `c >= |o|` (a `b` column shifts up by how much `a` widened).

In DSL primitives:

```
m = concat(o, shift(iota(arity(b)), arity(a)))
```

`iota(arity(b))` is `[0, 1, ..., arity(b)-1]`; `shift(_, arity(a))` makes it `[arity(a), ..., arity(a)+arity(b)-1]`, the new positions of `b`'s columns.
`concat(o, _)` prepends `o`, the underlying columns of `a`'s projected positions.

`m` serves two roles, both correct because both index by the old-space column:

* As the lifted output projection `Project[m]`: output position `i` reads new join column `m[i]`, recovering the original column at position `i`.
* As `remap_equivs(e, m)`: every column reference `c` in every equivalence class maps to its new-space position `m[c]`.

### New primitives

`remap_equivs(equivs, outs)` (new `PExpr::RemapEquivs`).
Apply the existing `Remap` column map (`c -> outs[c]`) to every scalar across every equivalence class.
This is the equivalence-nesting lift of `Remap`, exactly as `SwapEquivs` is the equivalence lift of a swap-shift.
Sound because it only renumbers columns inside opaque scalars, the same operation `swap_equivs`/`shift` already perform.

`reads_indexed_global(rel)` (new `Cond`).
True iff `rel`, after stripping column- and row-preserving wrappers (`Project`, `Filter`, `Map`, `ArrangeBy`, `ArrangeByMany`), is an `Opaque` global `Get` covered by a maintained index.
Mirrors `cse.rs::reads_index_backed_get` but is exposed as a rule guard.
Requires threading the index `available` map (already held by the cost model and `build_availability`) into condition evaluation.

`non_identity_projection(o, rel)` (new `Cond`, negation of the existing `identity_projection`).
The projection drops at least one column, so pull-up is not a no-op and cannot loop with `drop_identity_project`.

### Reused primitives, with one thing to verify

`Concat`, `Iota`, `Arity` are used as-is.
`Shift(Iota(arity(b)), arity(a))` assumes `Shift` shifts the entries of a projection payload (the entries are themselves column indices).
`Shift` is documented for column indices in payloads; confirm it applies to a projection payload, and if not add a small `iota_from(lo, count)` index-range builder instead.

## Bidirectional framing

The engine is a real e-graph (`egraph.rs` union-find plus congruence), so a single directed `=>` already unions both forms into one class and makes both extractable.
Direction matters only for *firing*: a rule fires only where its LHS pattern matches, and our plans contain the projection-below form, so we must write the rule with that as the LHS (the pull-up direction above).

The eventual DSL `<=>` should pair this with the push-down direction (`project(m, join(a, b)) => join(project(o, a), b)`).
The push-down side matches a more constrained pattern: it fires only on a projection whose shape is `o ++ shift(iota(|b|), |a|)`, recovering `o` from the lifted form.
Until the constrained reverse is written, the pull-up direction alone suffices, because the union it creates makes both forms available; the reverse is only needed to fire pull-up when saturation produces the lifted form first, which does not happen on our inputs.

## Containing e-graph blowup

Bidirectional MFP movement is the canonical saturation-explosion case, and equivalence saturation is already the eqsat bottleneck (`project_egraph_eq_analysis_perf`).
Three guards bound this rule:

* `reads_indexed_global(a)` restricts firing to join inputs where pull-up exposes an index, the only case with payoff.
* `non_identity_projection(o, a)` prevents firing on a no-op projection.
* `is_binary_join()` scopes the first deliverable to two-input joins; n-ary needs the splice-arity work below.

Termination holds because the rule produces a hash-consed bounded structure: re-firing yields the same node and adds nothing, so the class saturates.
If saturation time regresses despite the guards, gate the rule behind a bounded phase or ruleset rather than the main fixpoint.

## Dependency on cost-model fidelity

This rule is necessary but not sufficient.
It makes the pulled-up variant *available*; extraction must *prefer* it.
The pulled-up variant flows the full (wider) relation through the join, which a naive memory cost penalizes, even though it unlocks the free delta join that avoids the differential intermediate.
Extraction picks it only when the cost model values "join input exposes multiple existing indexes, enabling a free delta" over "narrower join intermediate."
That is the arrangement-availability fidelity tracked in `project_egraph_arrangement_sharing_redesign` and the doc-1 cost work.
Land this rule together with that cost change, or the variant is generated and ignored.

## Generalizations (follow-up)

* N-ary first input: the `b` slice becomes `rest...`, so `m`'s tail is `shift(iota(arity(rest...)), arity(a))`.
  `Arity` takes a single bound relation, not a splice, so this needs a splice-arity index expression (total join input arity minus `|o|`).
* Other input positions: pulling a projection off input `k` shifts the column map by the offset `sum(arity(inputs[0..k]))`, analogous to `push_filter_into_join_second`'s `shift(p, -arity(a))`.
* `Map` and `Filter` pull-up: `Filter` pull-up off the first input needs no shift and could be the reverse of `push_filter_into_join_first`; `Map` appends columns, so its pull-up rewrites the appended scalars with `Shift` and adjusts the lifted projection.

## Verification

* Rewrite `test/sqllogictest/ldbc_bi_physical.slt` with eqsat on; the two Q20 joins must return to `Join::Delta`.
  With the eager-delta policy the total may exceed production's 37; that is expected and acceptable as long as arrangement count does not grow.
* Blast radius: `tpch_select`, `catalog_server_explain`, and logical `ldbc_bi` unchanged in pass/fail.
* Saturation time: measure optimize time on the physical corpus before and after; the guards should keep it flat.
* Unit test the rule in isolation (a binary join over a projected index-backed `Get`) plus `remap_equivs` column-map correctness.

## Risks

* `remap_equivs` must match `Remap`'s column semantics exactly across the equivalence nesting, or join equivalences corrupt silently.
  Cover with a direct unit test and the `ldbc_bi_physical` goldens as a tripwire.
* If the cost model is not updated in the same change, the rule is dead weight (and a small saturation cost) with no plan effect.
* If `Shift` does not apply to projection payloads, fall back to a dedicated `iota_from(lo, count)` builder.
