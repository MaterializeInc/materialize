# BUG 2 — Window functions / top-k do not yield while processing a large partition, starving co-located objects' freshness

**Component:** compute / dataflow rendering
**Severity:** high (same blast radius as BUG 1: freshness of unrelated co-located objects, cross-cluster, hung reads)
**Source:** `src/compute/src/render/reduce.rs` — `build_basic_aggregate` (~L481), the `Basic` reduction type that evaluates window functions over a whole partition via `window_agg_helpers` / `eval_with_fast_window_agg` (~L611, L2513); and `src/compute/src/render/top_k.rs` — `build_topk` (~L304) for top-k (`row_number() … <= 1`).

## Summary

A window function (`lag`, `lead`, `sum() OVER`, `rank`, `row_number`, …) is
rendered as a "basic" reduce that gathers all rows of a **partition** and
computes the result over the whole list. Processing one partition is a single
non-yielding step, so a **large partition** (a hot key) is processed in one
operator activation that does not yield — freezing the **write frontier of every
co-located object on the cluster** for that time. top-k is `row_number() … <= 1`,
so it is the same operator family and starves the same way.

This is the same root shape as BUG 1 (a non-yielding reduce), but in **two
further code paths** that are independent of each other and of BUG 1: window
functions render through `reduce.rs::build_basic_aggregate`, while top-k renders
through the separate `top_k.rs::build_topk` operator. They share the family
(materialize a whole partition, then emit) and the same fuel-pattern fix, but the
fix touches **two distinct operators/files**, not one. The trigger also differs
from BUG 1: BUG 1 (hierarchical reduce) starves on **total snapshot size**; these
starve on **per-partition size**.

## Reproduce

```bash
bin/mzcompose --find freshness down -v
bin/mzcompose --find freshness run default \
  --expensive-ops window_sum --probe-kinds index --actions hydration \
  --rows 4000000 --skew 0.95 --window-seconds 10
```

What it does: `--skew 0.95` puts 95 % of `big`'s rows on a single key, so the MV
`SELECT key, sum(id) OVER (PARTITION BY key ORDER BY id) FROM big` has one
~3.8M-row partition. The window operator processes that partition in one
non-yielding step on a single-worker cluster while we watch a trivial co-located
`index` probe's frontier stall (assertion: `stall > --max-stall-ms` ⇒ FAIL).

Other affected operators (same family): `window_lag`, `window_rank`, `topk`, and
**`DISTINCT ON (key)`** (top-1 per group — verified 9–11 s stall).

## Verification (minimal repro, re-run to confirm)

Re-ran the minimal command above (`window_sum`, index probe, hydration, 4M rows,
`--skew 0.95`, `scale=1,workers=1`) — reproduced cleanly:

```
size               op          probe   action      stall   max_lag  expensive  result
scale=1,workers=1  window_sum  index   hydration   9008ms   9966ms    19330ms  FAIL (frontier stalled 9008ms > threshold 5000ms)
```

The trivial 1-row index probe's write frontier stayed **frozen for 9.0 s** while
the co-located `sum() OVER (PARTITION BY key)` MV hydrated (19.3 s) over the
single ~3.8M-row partition. A healthy probe should stall ≤ ~1 s. Exit code 1
(assertion failed), as expected for a red repro.

## Expected vs actual

- **Expected:** the cheap probe stays fresh regardless of co-located window work;
  the window operator yields while building a large partition.
- **Actual:** the probe's frontier freezes for several seconds, scaling with
  partition size.

## Key characteristics (from the freshness harness)

- **Trigger is per-partition size.** With tiny partitions (uniform keys, ~2
  rows/partition) the window functions are fresh (~1–2 s); the stall grows as a
  single partition grows:

  | operator | uniform (2/part) | skew 0.9 | skew 0.95 / 4M (~3.8M/part) |
  |---|---|---|---|
  | `sum() OVER` | 2.0 s | 4.0 s | 9.0 s FAIL |
  | `rank()` | 1.0 s | 3.0 s | 6.0 s FAIL |
  | `lag()` | 1.0 s | 2.0 s | 6.0 s FAIL |

- **Unifies with top-k:** top-k (`row_number() … <= 1`) is a window function and
  starves on a large partition the same way (the original "top-k starves"
  result).
- **FlatMap (`unnest`) does NOT starve** (streams record-by-record) — so the
  issue is specific to operators that materialize a whole partition before
  emitting, not all high-fan-out operators.
- **Compute, not persist** (index probe starves the same as an MV probe).
- **Liveness, not correctness** (same as BUG 1): during the stall, co-located
  objects are *stale* but never *wrong* — MV-vs-source reads stay consistent at
  the same timestamp and final results match ground truth.
- **Visible to monitoring, but under-reported:** `mz_wallclock_global_lag`
  detects the stall but under-reports its magnitude (~11 s shown vs ~29 s actual)
  — detectable, but severity is understated.
- **Scales with partition size, to minutes/hours at production scale.** The
  per-partition stall grows ~linearly with the partition; a hot key holding a
  large fraction of a billion-row input would freeze co-located freshness for
  minutes (cf. BUG 1's snapshot-size scaling: 30M → ~3 min).
- **Adding workers does not help.** At `scale=1,workers=4` the skewed
  `sum() OVER` still stalls 8.0 s (vs 9.0 s at 1 worker): a single partition is
  processed by a single worker while the others idle, and the cluster frontier is
  the min across workers, so one stalled worker freezes it. (Contrast BUG 1's
  hierarchical reduce, which parallelizes its buckets and recovers at 4 workers.)
  So scaling the cluster out is **not** a mitigation for this bug.

## Blast radius / amplifiers

Identical to BUG 1: cross-cluster contagion (`run xchain`), hung
strict-serializable reads and silent `SUBSCRIBE` (`run rtr`, `run subscribe`),
and multi-minute flickering recovery at restart/0dt (`run recovery`) when the
co-located object is a window/top-k MV over a skewed key.

## Corroboration: the codebase already fuels the operators that DON'T starve

Same as BUG 1: linear join (`LINEAR_JOIN_YIELDING`) and flat-map
(`COMPUTE_FLAT_MAP_FUEL`) are explicitly fueled and stay fresh in our tests;
there is **no** yielding/fuel config for the Basic-reduce / window / top-k paths,
and they starve. The fueling mechanism (`context.rs::flat_map`, output-count
based, ~L276) is the established pattern. This matches the empirical results and
confirms the bug.

## Suggested fix direction

Apply the existing fuel pattern — as **two independent patches, one per
operator** — to the Basic-reduce window path (`build_basic_aggregate` /
`eval_with_fast_window_agg`) and to `build_topk`: process a large partition in
bounded chunks, yielding between them, rather than one activation per partition. Harder than BUG 1 because a window result is defined
over the whole ordered partition, so chunked processing must preserve correctness
(e.g. yield while accumulating the partition, then emit) — but the fuel/refuel
machinery and `LINEAR_JOIN_YIELDING` precedent apply.

## Related

BUG 1 (`BUG-1-hierarchical-reduce-no-yield.md`) — the hierarchical min/max reduce
is the same non-yielding shape with a snapshot-size trigger. Full context in
`test/freshness/FINDINGS.md`.
