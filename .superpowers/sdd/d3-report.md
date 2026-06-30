# D-T3 report: documentation + harness measurement

## Status

Done.
Commit `a8516bfc0e`: `eqsat: document physical placement + index-aware cost coverage`.

## Harness measurement

`cargo test -p mz-transform --test compare_real -- --nocapture`

```
SUMMARY: 3 wins / 0 losses / 17 ties / 0 skips
```

Counts unchanged from prior record.
Test run time: ~27.5s.

## Doc edits (docs/superpowers/specs/2026-06-19-mir-egraph-status.md)

* **Coverage matrix `JoinImplementation` row**: updated from "offline-only, index-blind" to reflect `PhysicalEqSatTransform` (flag `enable_eqsat_physical_optimizer`, default off) committing `WcoJoin`-to-`DeltaQuery` live with an index-aware cost model.
  Remains Partial because the flag is default-off pending SLT validation and `JoinImplementation` still runs for non-`WcoJoin` joins.
  Performance concern noted: ~6.5s seen on large plans; `MAX_PLAN_SIZE` caps worst cases; tuning needed before flag-on promotion.
* **Key findings "offline-only" bullet**: updated to reflect that the physical pass ships the `WcoJoin` win live behind the flag.
  Multi-sentence form to fit house style (one sentence per line).
* **Workstream D**: marked `(done)`, with description of what was delivered (index-aware cost model + physical placement + `JoinImplementation` skip of `DeltaQuery` joins).
* **Deletion phase 3**: updated to name workstream D as the supplier of the `JoinImplementation`/`WcoJoin` capability and to make clear that deletion is gated on flag-on SLT parity.

## Files changed

* `docs/superpowers/specs/2026-06-19-mir-egraph-status.md` (1 file, 15 insertions, 4 deletions)

## Concerns

* Physical pass latency (~6.5s on builtin-index plans) must be addressed before `enable_eqsat_physical_optimizer` can be promoted to default-on.
* Broader SLT validation with the flag on is the remaining gate for deletion phase 3.
