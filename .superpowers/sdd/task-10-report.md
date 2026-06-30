# Task 10 Report: Subsume-or-include audit execution

## Status

DONE_WITH_CONCERNS — coalesce_mfp NOT removed; documented as known gap.

## Gap inventory

### Rules already covering MFP shapes

| Shape | Rule | Subsumed? |
|---|---|---|
| Map-Map fusion | `fuse_maps` | YES (cheaper, extractor picks it) |
| Filter-Filter fusion | `merge_filters` | YES (cheaper, extractor picks it) |
| Project-Project fusion | `fuse_projects` | YES |
| Filter-above-Map ordering | `push_filter_through_map` | PARTIAL (both in same e-class, tied cost) |

### Shapes NOT subsumed by saturation (confirmed gaps)

**Gap 1: Scalar inlining through Map/Project boundaries**

`MapFilterProject::extract_non_errors_from_expr_mut` followed by `mfp.optimize()` inlines
a Project's column selections into the Map scalars above it (composing expressions).
For example, `Project([0,2]) (Map([#1, #0]) r)` becomes `Map([#0]) r`.
No saturation rule handles this: the rules treat scalar payloads as opaque lists.
CanonicalizeMfp needs type information (column types, nullability) for this composition.

**Gap 2: Predicate simplification**

`CanonicalizeMfp::rebuild_mfp` calls `Filter::action` which splits conjuncts, sorts
predicates, deduplicates, and removes predicates trivially true given the column type
context (e.g., `IS NOT NULL` on a non-null column is removed). The rules treat predicate
lists as opaque; they cannot simplify based on type.

**Gap 3: Post-demand_pushdown cleanup (second coalesce_mfp)**

`demand_pushdown` runs AFTER extraction. Saturation cannot anticipate the new Project nodes
it introduces. The second `coalesce_mfp` call in `eqsat.rs` is definitively unsubsumed.

## Experimental evidence for gaps 1 and 2

Removed the first `coalesce_mfp` call from `eqsat.rs` (line 179) and ran:
`bin/sqllogictest --optimized -- test/sqllogictest/catalog_server_explain.slt`

Result: 2 failures (baseline had 0 failures in this file).

**Failure 1** (`mz_console_cluster_utilization_overview`):
- Expected: `Project: #1..=#3` (no filter)
- Actual: `Project: #1..=#3\nFilter: (#1) IS NOT NULL`
- Cause: Gap 2 — IS NOT NULL filter not removed when column is non-null.

**Failure 2** (`mz_cluster_schedules`):
- Expected: scalar references `#3` (a composed reference through Map)
- Actual: scalar references `#1` through the full chain (unsubstituted)
- Cause: Gap 1 — Map scalars not inlined into the expression above.

Both are genuine regressions (less canonical plans, not just equivalent spellings).

## TDD step (no new rules added)

No rules were added. The gaps cannot be expressed as saturation rules without type
information. Adding rules for the typed cases would require the scalar IR to carry type
annotations. The existing rules (`fuse_maps`, `merge_filters`, `fuse_projects`) already
cover structural fusion. The remaining gaps are semantic/typed and outside this task scope.

## What was done

**Step 1 (Gap inventory):** Complete. Two unsubsumed gaps identified and confirmed.

**Step 2 (TDD rules):** Skipped — no expressible gaps remain. The structural fusion cases
are already covered; the remaining gaps need type information.

**Step 3 (Crutch removal):** BLOCKED by regression. The first `coalesce_mfp` was removed
experimentally, confirmed 2 regressions in `catalog_server_explain.slt`, and restored.
Both `coalesce_mfp` calls in `eqsat.rs` remain unchanged.

**Step 4 (transform.rs audit comment):** Added a block comment above `EqSatTransform` in
`src/transform/src/eqsat/transform.rs` recording that `Demand` and `ProjectionPushdown`
remain production passes (include-for-now), why they cannot be removed, and the condition
under which they could be.

**Comment update in `eqsat.rs`:** Updated the first `coalesce_mfp` comment from the
original inaccurate "do not reorder them" description to an accurate description of the
two specific unsubsumed gaps (scalar inlining, predicate simplification).

## Golden classification

- `test/sqllogictest/explain/optimized_plan_as_text.slt`: PASS (119/119)
- `test/sqllogictest/transform/projection_lifting.slt`: PASS (3/3)
- `test/sqllogictest/catalog_server_explain.slt`: PASS (289/289) with both coalesce_mfp calls in place
- `test/sqllogictest/explain/default.slt`: 3 pre-existing failures (present before this task on original branch; unrelated to this work)

No golden rewrites performed. Crutch not removed (regressions confirmed).

## Files changed

- `src/transform/src/eqsat.rs`: Updated comment on first `coalesce_mfp` call to accurately document the two unsubsumed gaps.
- `src/transform/src/eqsat/transform.rs`: Added subsume-or-include audit result comment above `EqSatTransform`.

## Self-review

- No em-dashes in any comments.
- Doc comments state contracts; inline reasoning is at decision points.
- No vendor names in user-facing surfaces.
- No `unsafe` added. No `as` conversions added.
- `cargo fmt -p mz-transform`: clean.
- `cargo clippy -p mz-transform`: clean.
- `bin/lint`: all checks pass except pre-existing `buf`/`trufflehog` toolchain not installed (environmental, not related to this change; same failure baseline before this task).

## Known gaps (unsubsumed, for future work)

1. **Scalar inlining through Map/Project boundaries**: Needs type-aware MFP composition during or after extraction. Could be addressed by a typed extraction phase that runs CanonicalizeMfp on each extracted subtree, or by extending the scalar IR with type annotations.

2. **Predicate simplification**: Null-check removal based on column nullability. Requires column type metadata in the saturation context. Could be addressed by threading `RelationTypes` into the saturation analysis layer.
