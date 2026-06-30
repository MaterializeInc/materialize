# Task 1 Report: Surface cost-model join order via `binary_join_order`

## Status
DONE

## Commit
`315dee298f` — "eqsat: surface cost-model join order via binary_join_order"

## What was implemented

Added to `src/transform/src/eqsat/cost.rs`:

1. **`pub(crate) struct JoinStep`** — `{ input: usize, key_cols: BTreeSet<usize> }`;
   global input index + local key columns for one step in the left-deep plan.

2. **`pub(crate) struct JoinOrder`** — `{ steps: Vec<JoinStep> }`; the full left-deep
   sequence with per-step arrangement keys.

3. **`fn frontier_key_cols`** — free fn that extracts local key cols of an input relative
   to the already-placed frontier, by scanning equivalences for classes that touch both
   the new input's column range and the frontier.

4. **`fn best_left_deep_sequence`** — mirrors the DpSub recurrence but stores backpointers
   `(terms, pred_subset, last_input)` so the cheapest left-deep order can be reconstructed
   as an input sequence.

5. **`CostModel::binary_join_order`** — `pub(crate)` method placed next to
   `binary_join_terms`; single-input shortcut, wide-join fallback (chain in input order),
   and exact DpSub-backed path for `n <= MAX_EXACT_JOIN_INPUTS`. Returns `None` only for
   0 inputs.

6. **Two tests** in `mod tests`:
   - `binary_join_order_voj_local_is_keyed` — 3-input VOJ with local spelling; asserts
     all post-start steps have non-empty keys.
   - `binary_join_order_disconnected_returns_order_with_cross` — 2 inputs, no equivalences;
     asserts second step has empty key (cross) but `Some` is returned.

## TDD Evidence

### RED (before implementation)

```
error[E0599]: no method named `binary_join_order` found for struct `cost::CostModel`
error: could not compile `mz-transform` (lib test) due to 2 previous errors
```

### GREEN (after implementation)

```
bin/cargo-test -p mz-transform binary_join_order
  PASS mz-transform eqsat::cost::tests::binary_join_order_disconnected_returns_order_with_cross
  PASS mz-transform eqsat::cost::tests::binary_join_order_voj_local_is_keyed
  Summary: 2 tests run: 2 passed
```

### Full suite (no regressions)

```
bin/cargo-test -p mz-transform eqsat::cost
  Summary: 31 tests run: 31 passed, 351 skipped
```
(29 pre-existing + 2 new)

## Files changed

- `src/transform/src/eqsat/cost.rs`: +191 lines

## Deviations from brief

The brief's test code uses local `get`/`col`/`eq` closures. Per brief instructions,
these were replaced by reusing the already-existing `get`/`col` free functions and
`eq_expr` helper from the test module. No behavioral difference.

## Warnings

Five `dead_code` warnings for `JoinStep`, `JoinOrder`, `frontier_key_cols`,
`best_left_deep_sequence`, and `binary_join_order`. Expected — these are
`pub(crate)` items to be consumed by Task 2's emitter.

## Concerns

None.
