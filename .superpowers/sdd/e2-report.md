# Workstream E, Task 2: Report

## Doc edits (docs/superpowers/specs/2026-06-19-mir-egraph-status.md)

* **Covered table**: added `RelationCSE` row.
  Mechanism: extraction-time CSE via `cse::eliminate_common_subexpressions`, runs between extraction and raise, hoists shared e-classes into `Rel::Let` bindings with fresh ids, `is_closed` guard, raise emits real `MirRelationExpr::Let` + local `Get` (type-threaded).
* **Partial table**: added `NormalizeLets` row.
  Gap: eqsat CSE produces well-formed `Let`/`Get` with fresh ids; a subsequent `NormalizeLets` is reduced to renumbering and inlining only.
* **Missing section**: removed `RelationCSE`.
  Added **Deferred** paragraph for `FlatMap`, `ArrangeBy`, `LetRec`, non-empty `Constant`: no active rules see through them, engine already peels recursive scopes via `optimize_scope`, `Constant` rows not tracked.
  `NormalizeLets` removed from Irreducible (now Partial).
* **Workstream E entry**: marked `(done)`, updated description to reflect actual scope: extraction-time CSE done, structural de-opaquing deferred.
* **Deletion phase 2**: updated to note both C and E are complete, supplying `CanonicalizeMfp`/`RelationCSE`/`NormalizeLets`; deletion gated on flag-on SLT parity; `FlatMapElimination` stays until de-opaquing is done.

## Harness measurement

Command: `cargo test -p mz-transform --test compare_real -- --nocapture`

```
SUMMARY: 3 wins / 0 losses / 17 ties / 0 skips
```

Matches the previously recorded result.
Run took ~38s.

## Commit

SHA: `ee9bee026f`
Subject: `eqsat: document CSE coverage (RelationCSE), defer de-opaque variants`

## Files changed

* `docs/superpowers/specs/2026-06-19-mir-egraph-status.md` — 1 file, +13/-6 lines
