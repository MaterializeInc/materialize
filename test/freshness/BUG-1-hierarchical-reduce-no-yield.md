# BUG 1 — Hierarchical (min/max) reduce does not yield while processing a large diff, starving co-located objects' freshness

**Component:** compute / dataflow rendering
**Severity:** high (freshness + availability of *unrelated* objects on the same cluster, and downstream across clusters)
**Source:** `src/compute/src/render/reduce.rs` — `build_bucketed` (~L854) and `build_bucketed_stage` (~L1067), the bucketed hierarchical min/max reduction tree.

## Summary

When a hierarchical reduce (`min`/`max` grouped aggregation) processes a large
single diff — its initial snapshot at hydration, **or** a single large
steady-state transaction — it runs to completion in one operator activation
without yielding. While it runs, the timely worker services nothing else, so the
**write frontier of every other object on the same cluster freezes** for the
duration. Cheap, trivially-maintained objects (a 1-row index/MV) go stale for
many seconds even though their own work is negligible.

Accumulable reduces (`count`, `sum`, `avg`), `distinct`, `join` (even a 200M-pair
blowup), `index` (arrangement), `FlatMap`, `EXCEPT`, and `count(DISTINCT)` absorb
the same diffs without starving anything — so this is specific to the bucketed
hierarchical min/max path, not reduces in general.

**Other SQL surfaces of this same bug:** a correlated subquery over `min`/`max`
(e.g. `LATERAL (SELECT max(...) WHERE b.key=a.key)`) decorrelates to a join + a
hierarchical reduce and starves the same way (verified: 6–8 s stall).

## Reproduce

```bash
bin/mzcompose --find freshness down -v
bin/mzcompose --find freshness run default \
  --expensive-ops max --probe-kinds index --actions hydration \
  --rows 3000000 --window-seconds 10
```

What it does: on a single-worker cluster, create a trivial `index` "probe" on a
1-row table (kept fresh by tiny writes from a separate connection), then build an
expensive `SELECT key, max(id) FROM big GROUP BY key` MV on the **same** cluster
and watch the probe's `now() - write_frontier` (via `mz_internal.mz_frontiers`)
from a separate connection on the default cluster. The probe's max
frontier-stall is the assertion (`stall > --max-stall-ms` ⇒ FAIL).

Steady-state variant (proves it is *not* hydration-specific — a single bulk DML
into a hydrated reduce starves identically):

```bash
bin/mzcompose --find freshness run default \
  --expensive-ops max --probe-kinds index --actions big_txn \
  --rows 3000000 --big-txn-rows 3000000 --window-seconds 20
```

## Verification (minimal repro, re-run to confirm)

Re-ran the minimal command above (`max`, index probe, hydration, 3M rows,
`scale=1,workers=1`) — reproduced cleanly:

```
size               op    probe   action      stall   max_lag  expensive  result
scale=1,workers=1  max   index   hydration  18018ms  22484ms    28029ms  FAIL (frontier stalled 18018ms > threshold 5000ms)
```

The trivial 1-row index probe's write frontier stayed **frozen for 18.0 s**
while the co-located `max` MV hydrated (28.0 s) — ~0.64× of the hydration. A
healthy probe should stall ≤ ~1 s (the oracle interval). Exit code 1 (assertion
failed), as expected for a red repro.

## Expected vs actual

- **Expected:** the cheap probe stays fresh (frontier advances every oracle
  tick, ~1 s) regardless of co-located heavy work — differential operators are
  supposed to be fuel-limited and yield so co-resident dataflows make progress.
- **Actual:** the probe's frontier freezes for ~0.8× of the reduce's
  processing time.

## Key characteristics (from the freshness harness)

- **Trigger is total diff/snapshot size, not group skew.** Starves at both tiny
  groups (1M groups × ~5 rows) and one giant group; skew does not worsen it.
- **Diff size scales the stall (~linear), to minutes/hours at scale.** big_txn:
  500k → ~3 s, 3M → ~21 s, 6M → ~34 s. Hydration scale sweep: 5M → 30 s, 15M →
  92 s, **30M → 189 s (~3 min)**; ~6.3 ms / 1k rows, so 100M ≈ 10 min and **1B ≈
  ~1.5 h** of co-located stall. The seconds-scale repros understate production.
- **Liveness, not correctness.** During the stall, MV-vs-source reads stay
  consistent at the same timestamp and the final result matches ground truth
  exactly — co-located objects go *stale*, never *wrong*. (Bounds the bug:
  availability/freshness, not data corruption.)
- **Visible to monitoring, but under-reported:** `mz_wallclock_global_lag`
  detects the stall (not blind) but under-reports its magnitude (~11 s shown vs
  ~29 s actual at 5M) — a customer sees the incident but underestimates its
  severity.
- **Compute, not persist:** an `index` probe (no persist output) starves the
  same as a materialized-view probe — the worker is simply unavailable.
- **Worker scaling shrinks but does not remove it:** stall/hydration ratio
  0.86 (1 worker) → 0.54 (8) → 0.37 (16); the probe still degrades to several ×
  baseline at 16 workers (it only "passes" a fixed threshold because the work
  also finishes faster).

## Blast radius / amplifiers (why severity is high)

- **Crosses clusters undiminished.** A materialized view on cluster B that reads
  an input on starved cluster A stalls by the same amount; in a c0→c1→c2 chain,
  starving the head stalls all three equally. Repro: `run xchain`.
- **Surfaces to users, not just metrics.** During the stall a strict-serializable
  `SELECT` against the probe hangs ~the stall (38 s observed); a `SUBSCRIBE`
  goes silent ~the stall (27 s). Repro: `run rtr`, `run subscribe`.
- **Multiplies at restart/0dt.** N such MVs rehydrating after a replica restart
  give a fleet rehydration that grows ~linearly (10 MVs → ~3 min, 20 → ~6 min)
  with the probe flickering stale 11–19 s throughout. Repro: `run recovery`.

## Corroboration: the codebase already fuels the operators that DON'T starve

Materialize has an explicit operator-fueling framework, and it has been applied
to exactly the operators this harness finds *fresh*, but not to the ones it finds
*starving*:

- Fueled (and fresh in our tests): **linear join** (`LINEAR_JOIN_YIELDING`,
  `dyncfgs.rs`), **flat-map / `unnest`** (`COMPUTE_FLAT_MAP_FUEL`,
  `ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION`; the fuel mechanism is
  documented on `context.rs::flat_map` ~L276 — accumulate output-produced counts,
  yield at a refuel threshold).
- **No yielding/fuel config exists for reduce, hierarchical reduce, top-k, or
  window.** `context.rs` ~L235 even flags a non-fueled path: *"This function is
  not fueled and hence risks flattening the whole arrangement."*

So the empirical result matches the codebase's own fueling decisions exactly,
which is independent confirmation of the bug.

## Suggested fix direction

Apply the **existing** fuel pattern to `build_bucketed` / `build_bucketed_stage`:
process the snapshot/diff in bounded chunks and yield at a refuel threshold based
on output produced, exactly as `context.rs::flat_map` and `LINEAR_JOIN_YIELDING`
already do. The accumulable path (`build_accumulable`) and the fueled flat-map are
working references.

## Related

BUG 2 (`BUG-2-window-function-no-yield.md`) is the same shape in two further
operators — window functions via `reduce.rs::build_basic_aggregate` and top-k via
the separate `top_k.rs::build_topk` (not the Basic reduce path). Both are tracked
by the `test/freshness` harness; full context in `test/freshness/FINDINGS.md`.
