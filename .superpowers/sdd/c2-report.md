# Workstream C Task 2 report

## Doc edits (docs/superpowers/specs/2026-06-19-mir-egraph-status.md)

**Coverage matrix — Covered table:** Added `CanonicalizeMfp` row.
Mechanism: raise-time MFP coalescing; each maximal Map/Filter/Project run is extracted into `mz_expr::MapFilterProject`, optimized via `MapFilterProject::optimize`, and re-emitted via `CanonicalizeMfp::rebuild_mfp` (the full production trio).

**Coverage matrix — Partial table:** Added `LiteralLifting` row.
`MapFilterProject::optimize` performs the within-MFP literal-lifting part of `LiteralLifting`.
Cross-operator literal lifting (hoisting constants past joins and reductions) remains out of scope.

**Coverage matrix — Missing section:** Removed `CanonicalizeMfp` and `LiteralLifting` from the scalar-layer bullet.
Updated to: "`LiteralConstraints` and cross-operator literal lifting (the within-MFP literal lifting is now partial via MFP coalescing)."

**Coverage matrix — Irreducible:** Removed "the final MFP canonicalization the renderer demands" (was referring to `CanonicalizeMfp`, now covered).

**Roadmap — Deletion phase 2:** Added two sub-bullets: workstream C is complete, and the `logical_cleanup_pass` MFP canonicalization can be deleted once SLT parity with the flag on is confirmed.

## Harness measurement

`cargo test -p mz-transform --test compare_real -- --nocapture`

```
SUMMARY: 3 wins / 0 losses / 17 ties / 0 skips
```

Unchanged from the last recorded value of 3 wins / 0 losses / 17 ties.

## Files changed

* `docs/superpowers/specs/2026-06-19-mir-egraph-status.md` — 6 insertions, 2 deletions

## Commit

`1678e9cf5a` — `eqsat: document MFP coalescing coverage (CanonicalizeMfp)`
