# Leaf dedup report

## Dedup mechanism

Added a `leaf_dedup: BTreeMap<MirRelationExpr, LeafId>` field to `Interner`.
`MirRelationExpr` derives `Ord` and `PartialOrd`, so a `BTreeMap` key lookup is
straightforward -- same pattern as `scalar_dedup`.

`intern_leaf` now checks `leaf_dedup` before allocating a new id.
On a hit it returns the existing `LeafId`; on a miss it inserts into `leaf_dedup`,
pushes the subtree to `self.leaves`, and pushes the arity to `self.leaf_arity`.
The two side tables stay in lockstep.

Files changed:
* `src/transform-egraph/src/interner.rs` -- `Interner` struct, `intern_leaf`, doc comment, tests
* `src/transform-egraph/src/lower.rs` -- renamed test, added distinct-subtrees test
* `src/transform-egraph/tests/roundtrip.rs` -- two new tests
* `src/transform-egraph/rules/relational.rewrite` -- `merge_filters` guard (see below)

## Test changes

### `interner.rs` tests
* Renamed `leaves_record_arity` (kept, still passes).
* Added `identical_leaves_share_leaf_id`: two equal subtrees return the same `LeafId`.
* Added `different_leaves_get_distinct_leaf_ids`: two subtrees of different arity return distinct ids.

### `lower.rs` tests
* Renamed `each_bail_gets_distinct_leaf_id` -> `identical_bails_share_leaf_id`:
  asserts two equal bails lower to the same `leaf:N` name.
* Added `different_bails_get_distinct_leaf_ids`:
  asserts two structurally different bails get distinct `leaf:N` names.

### `tests/roundtrip.rs` tests
* Added `union_cancel_then_empty_collapses`:
  `Union(src(1,2), Negate(src(1,2)))` collapses to an empty constant.
* Added `union_cancel_under_filter_and_map_terminates`:
  Regression test -- the same pattern wrapped in `Filter+Map` must terminate
  (was hanging before the `merge_filters` guard fix).

## Termination issue found and fixed

Leaf dedup caused `union_cancel` to fire for global Get sources.
For `Filter[p](Union(a, Negate a))` with a false predicate on `p`:

1. `union_cancel` added `Constant{card:0}` to the Union class.
2. `empty_false_filter` added the same `Constant{card:0}` (hash-conse'd, same node) to the Filter class.
3. Both `Constant` references pointed to the same e-graph node, so the Union class
   and Filter class merged -- the filter's input class became the same class as the
   filter itself.
4. `merge_filters` then matched `Filter[p](Filter[p](R))` where R = R (self-referential),
   producing `Filter[concat(p,p)](R)` -- a new node in the same class.
5. Each subsequent iteration doubled the predicate list: `[p]`, `[p,p]`, `[p,p,p,p]`, ...
   The e-graph grew without bound until OOM or the test harness killed the process.

Fix: added `where not_rel_empty(r)` to `merge_filters`.
When `r` is empty, both consecutive filters are already in the merged-empty class;
applying merge_filters in that situation is semantically a no-op (both sides equal
empty regardless), so skipping it is safe.
This guard is analogous to the existing `not_rel_empty` guards on
`union_drop_empty_left` and `union_drop_empty_right`, which already had a comment
explaining exactly this class of problem.

## BEFORE / AFTER harness counts

Before (leaf dedup absent): 3 wins / 6 losses / 11 ties / 0 skips
After  (leaf dedup + guard): 3 wins / 4 losses / 13 ties / 0 skips

Losses closed (became TIEs):
* `union_cancel` (case 9): `Union(src, Negate src)` now collapses to Empty.
* `threshold_over_union_cancel` (case 12): same pattern under Threshold, also collapses.

Remaining losses (cases 10, 13, 20):
* `union_cancel_under_filter_map` (case 10): eqsat produces `Project(Empty)` instead
  of `Empty` because there is no `project_empty` rule; the real optimizer folds away
  the outer Project.
* `threshold_over_union_filtered_inputs` (case 13): the two filtered inputs have
  different arities/sources; union_cancel does not apply.
* `filter_then_project` (case 20): the real optimizer eliminates a redundant column
  via project pushdown; eqsat lacks the column-arithmetic rules (M1 disabled).

## Concerns

The `merge_filters` guard might suppress fusion in rare cases where both filters
are in the same empty class but the outer filter has meaningful predicates for a
non-empty representative in the class. However: when a class contains an empty
Constant, extraction will always pick that Constant (0 cost), so the Filter node
in the same class will never be extracted. Suppressing merge_filters is therefore
semantically neutral for extracted plans.

## Commit hash

0c48a35ef2
