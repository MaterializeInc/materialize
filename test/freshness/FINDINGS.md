# Freshness-under-contention findings

Results from `test/freshness/mzcompose.py`. The test runs a cheap "probe" object
and an expensive object **on the same cluster** (`IN CLUSTER test`, sharing the
timely/tokio workers) and measures the probe's freshness from a *separate*
connection on the default cluster, so the measurement never competes for the
workers under test.

Two metrics per run:
- **max lag**: peak `now() - write_frontier` (ms). Reported as context, but
  confounded by how long the expensive work runs.
- **max stall**: the longest wallclock interval the probe's `write_frontier`
  stayed *frozen* — the duration-independent starvation signal we **assert** on.
  A perfectly fresh probe advances its frontier every oracle tick (~1 s), so a
  multi-second stall means the workers were busy elsewhere. (Switching to this
  metric removed the threshold/worker-count confound: the same starvation no
  longer "passes" merely because the expensive work finished faster.)

## Bottom line

The severe freshness pathology is **certain operators not yielding while
processing a large chunk of data**, which freezes the write frontier of every
co-located object. There are **two non-yielding operator families**, with two
different triggers:

1. **Bucketed hierarchical reduce (`min`/`max`)** — starves on total *snapshot*
   size (group size barely matters). Source: `reduce.rs::build_bucketed`.
2. **Window functions / top-k (`lag`/`lead`/`sum() OVER`/`rank`/`row_number`)** —
   starve on per-*partition* size (one large partition / hot key). top-k is
   `row_number() … <= 1` at the SQL level, but it renders through a **separate**
   operator (`top_k.rs::build_topk`) from window functions
   (`reduce.rs::build_basic_aggregate`), so this family is **two distinct
   non-yielding code paths** — a two-file fix, not one.

The triggering chunk can be the initial snapshot (**hydration**) *or* a single
large steady-state transaction (**big_txn**) — same mechanism, stall scales
**linearly** with the chunk size (~3 min at 30M rows; extrapolates to ~1.5 h at
1B). The bug is **liveness-only** — co-located objects go *stale*, never return
*wrong* or inconsistent results — and the user-facing `mz_wallclock_global_lag`
view detects the stall but reports well under its peak (~11 s shown vs ~29 s
actual) — **by design**, because it surfaces the per-minute *minimum* lag, not
the peak (a monitoring caveat, not a defect; see #5 below). Accumulable reduces,
FlatMap, joins, distinct, and
indexes absorb the same diffs fine. Other candidate shapes did **not** degrade
freshness (several are working-as-designed):

| Shape | Result |
|---|---|
| **Hierarchical (min/max) reduce & top-k — large diff** (hydration OR one big transaction) | **STARVES** co-located objects for ~0.8× of the diff's processing time |
| ↳ **same starvation, across clusters** (consumer MV on cluster B reads an input on starved cluster A) | **STALLS the other cluster** by the same amount — separate-cluster isolation does not hold for freshness through a shared input; **cascades the full downstream chain undiminished** (c0→c1→c2 all stall equally) |
| ↳ **surfaces to readers** | strict-serializable SELECT hangs ~the stall (38 s); SUBSCRIBE goes silent ~the stall (27 s) — both pull and push reads, not just dashboards |
| Accumulable (count/sum/avg), distinct, basic (array_agg/string_agg), join, index — same diffs | fresh |
| `DISTINCT ON` (top-1 per group) | **STARVES** — top-k family (BUG 2) |
| `LATERAL` / correlated `max` subquery | **STARVES** — decorrelates to join + `max` (BUG 1) |
| FlatMap (`unnest`), join blowup, `count(DISTINCT)`, `EXCEPT` — large diff | fresh — stream/fueled/accumulable |
| Temporal `mz_now()` whole-window expiry into a reduce | fresh (2 s) — retract-to-empty is cheap; only retracting the *running max* (recompute) starves |
| **Window functions (`lag`/`sum OVER`/`rank`) — large partition** | **STARVES** — a second non-yielding family; processes a whole partition as one chunk, stall scales with partition size (→9 s). Unifies with top-k (which *is* a window function). **Not mitigated by adding workers** (single partition → one worker; frontier = min) |
| Coordinator / timestamp-oracle contention (24× DDL or 32× peek flood) | fresh — idle objects on all clusters keep advancing; no global stall at this intensity (local metadata store; not fully saturated) |
| Combined workload (ingest + peekers + reduce hydration) | stall ≈ reduce-alone baseline — no emergent amplification; the non-yielding reduce dominates |
| Hierarchical reduce, **small** steady-state updates (retraction churn, even hot-key skew) | fresh — small diffs yield |
| WITH MUTUALLY RECURSIVE (50k-iteration fixpoint, 16 s) | fresh — recursive operator yields between iterations; "long" ≠ "starving" |
| Fleet of 50 cheap MVs hydrating together (125 s) | fresh — scheduler round-robins fairly; no collective fleet starvation |
| Restart/0dt with N starving MVs | first-recovery fast (~0.5 s) but probe flickers stale 11–19 s and fleet rehydration grows ~linearly (10 MVs → 3 min, 20 → 6 min) — the bug × the fleet, at deploy time |
| RETAIN HISTORY (60 s / 600 s) under churn | write freshness flat (0 ms) — retention holds the read frontier back, not the write frontier |
| Sink backpressure (throttled Kafka sink) | input MV stays fresh (0 ms) while only the sink lags — MZ does not backpressure a sink's input |
| Memory pressure (1.5 GB arrangement, no OOM) | fresh (988 ms) — Rust/no-GC, so pressure → hard OOM (→ rehydration), not gradual freshness lag |
| Dependency chains (8-hop MV) | fresh — propagation adds no measurable lag |
| Storage / blob latency (≤ 2 s), low volume | fresh — persist batches blob writes off the frontier path |
| Storage / blob latency × heavy write volume | **mild, bounded** (~2–3 s) MV-specific lag — a distinct shape, but not severe |
| Source ingestion (Kafka, Postgres/MySQL/SQL-Server CDC) + Kafka sink, under co-located compute | fresh — ingestion rides the storage/persist-async path; compute starvation does not reach it (engine-agnostic across all 4 engines) |
| Kafka source, high arrival rate (≤ ~1M msg/s) | fresh — keeps up (host-producer-limited) |
| Kafka source, large backlog catch-up | **transient, bounded** — ~1.5 s per 1M-msg backlog, then fresh (expected catch-up cost, not a bug) |
| Source upstream-link latency (MZ↔Postgres throttled) | **bounded, proportional** — <250 ms absorbed; beyond that lag ≈ 7× the one-way latency and stable (expected slow-link cost, not a bug) |
| Source upstream-link **bandwidth** cap | **unbounded** once the cap < data-arrival rate (sharp cliff at link-throughput = arrival); fresh above it. The genuine "can't keep up" case |
| Source upstream-link **flap** (disconnect/reconnect) | recovers cleanly — frontier freezes for the down-interval, then catches up < 1 s from the replication slot |
| Upsert-source key-state (hot / 1M / unbounded) | fresh — state maintenance doesn't lag ingestion |

The key reframing: it is **diff size**, not "hydration", that triggers it. A
bulk load, batch-ingest, or large `INSERT…SELECT`/`UPDATE`/`DELETE` into an
input feeding a hierarchical reduce or top-k will stall every co-located object
on that cluster, in steady state, not just at object creation.

**And it crosses clusters.** When the starving operator runs on cluster A and a
materialized view on cluster B reads an input that lives on A, B's freshness
stalls too — the starvation propagates through the shared input's frozen write
frontier, so an unrelated heavy MV on one cluster silently stalls a *different
team's* downstream MV on another cluster (see "Cross-cluster contagion" below).
This widens the blast radius from one cluster to any dependency graph spanning
the starved one.

---

# The confirmed pathology: hierarchical reduce / top-k hydration

## Operator taxonomy (`scale=1,workers=1`, hydration; stall ÷ hydration ratio)

| op | ratio | op | ratio |
|---|---|---|---|
| **max** | **0.83** | array_agg | 0.29 |
| **minmax** | **0.82** | avg | 0.24 |
| **min** | **0.81** | count_distinct | 0.38 |
| **reduce** (count+max) | **0.77** | distinct | 0.32 |
| **topk** | **0.52** | sum | 0.15 |
| stddev | 0.39 | count | 0.13 |
| string_agg | 0.27 | join | 0.13 |
| | | index | 0.11 |

Hierarchical min/max reduces (`max`/`min`/`minmax`) and `topk` starve (ratio
0.5–0.8); everything else sits near the ~0.1 oracle-interval baseline.

## Smoking gun: aggregate decomposition (`workers=1`, 5M)

| op | ratio | reading |
|---|---|---|
| `count` | 0.08 | accumulable — fresh |
| `sum` | 0.10 | accumulable — fresh |
| `max` | 0.86 | hierarchical — **starves** |
| `count`+`max` (`reduce`) | 0.80 | **= `max` alone**; the count half adds nothing |
| `count(DISTINCT)` | 0.47 | partial (distinct-then-accumulate) |
| `DISTINCT key` | 0.25 | reduce *without* aggregate — fresh |

The starvation is entirely the hierarchical aggregate, not grouping.

## A SECOND non-yielding family: window functions over a large partition

Testing whether other "process a whole group/partition in one step" operators
share the non-yielding shape (index probe, hydration). The window functions'
stall **scales with partition size** and fully crosses into starvation once a
single partition is large enough:

| operator | uniform (2M, ~2/part) | skew 0.9 (2M) | skew 0.95 (4M, ~3.8M/part) |
|---|---|---|---|
| `max` (hierarchical reduce, control) | 12 s FAIL | 5 s FAIL | — |
| `sum() OVER` window | 2.0 s | 4.0 s | **9.0 s FAIL** |
| `rank()` window | 1.0 s | 3.0 s | **6.0 s FAIL** |
| `lag()` window | 1.0 s | 2.0 s | **6.0 s FAIL** |
| `unnest` (FlatMap) | 0 ms | 0 ms | — |

- **Window functions are a genuine second non-yielding family.** They process a
  whole *partition* as one non-yielding chunk: tiny partitions are fine, but a
  single large partition (a hot key) stalls co-located objects, and the stall
  scales with partition size (sum OVER: 2 → 4 → 9 s as the partition grows).
- **This unifies the earlier top-k result**: top-k is `row_number() … <= 1`, a
  window function. So the "top-k starves" finding and these window-function
  results are the *same* operator family — windowed operators that materialize a
  partition before emitting.
- **Two non-yielding families, two different triggers.** (1) The bucketed
  hierarchical **min/max reduce** starves on total *snapshot* size (group size
  barely matters — it starves at both tiny and giant groups). (2) **Window
  functions / top-k** starve on per-*partition* size (a large single partition).
- **FlatMap (`unnest`) never starves** — it streams record-by-record and yields
  freely (0 ms even expanding 3×), confirming the issue is specific to operators
  that accumulate a group/partition, not all high-output operators.

### Adding workers does NOT mitigate the window-function case

Re-running the skewed (one giant partition) hydration at `scale=1,workers=4`:

| op | stall @ workers=1 | stall @ workers=4 |
|---|---|---|
| `sum() OVER` (one ~3.8M-row partition) | 9.0 s FAIL | **8.0 s FAIL** |
| `max` (hierarchical, skewed groups) | (starves) | 1.0 s PASS |

A single partition is processed by a **single** worker (the others sit idle),
and the cluster's write frontier is the **min across workers**, so one stalled
worker still freezes the frontier — scaling the cluster out barely helps
(`sum() OVER`: 9 s → 8 s). The hierarchical `max`, by contrast, distributes its
buckets across workers and recovers (1 s) at workers=4. **Scale-out is a partial
mitigation for BUG 1 (reduce) but essentially none for BUG 2 (window/top-k) on a
hot partition** — an important severity difference.

## Completeness sweep: every other operator maps to one of the two families

Confirming no *third* non-yielding family exists — each additional operator
either lowers to a known starving family or stays fresh (index probe, hydration):

| op | uniform (2M) | skew 0.95 (4M) | verdict |
|---|---|---|---|
| `join_blowup` (200M intermediate pairs, counted) | 0 ms | — | **fresh** — join fueling holds even under a huge intermediate |
| `count(DISTINCT)` | 0 ms | 1.0 s | **fresh** |
| `EXCEPT` (set difference) | 2.0 s | 4.0 s | **fresh-ish** (distinct/negate reduce; like `DISTINCT`) |
| `DISTINCT ON (key)` | **11.0 s FAIL** | **9.0 s FAIL** | → **BUG 2** — top-1-per-group *is* top-k |
| `LATERAL` (correlated `max`) | **6.0 s FAIL** | **8.0 s FAIL** | → **BUG 1** — decorrelates to join + `max` reduce |

So `DISTINCT ON` is another surface of the window/top-k bug, and a correlated
subquery over `max` is another surface of the hierarchical-reduce bug. No new
family; the two-bug model absorbs all of them. (Joins — even a 200M-pair blowup —
stay fresh, reconfirming the existing join fueling.)

## Temporal filter (`mz_now()`) window expiry — does NOT starve (the `temporal` workflow)

A sliding-window pattern `WHERE mz_now() < expiry` feeding `max(id) GROUP BY key`,
with 3M rows all sharing one expiry instant so the whole window retracts at once:

| event | probe max stall |
|---|---|
| 3M-row window expiring simultaneously into a `max` reduce | **2.0 s (PASS)** |

Surprising negative, and instructive: retracting whole groups to *empty* is cheap
for the hierarchical reduce (the max simply disappears — no re-scan), unlike the
`big_txn` **delete-of-top** case which retracts each group's *current max* and
forces a re-scan of the remainder (that starved, 14 s). So the dangerous temporal
shape is a window that drops the **running max while the group persists** (forcing
recompute), not a clean whole-window expiry. As tested (simultaneous full expiry)
it does not trigger BUG 1.

## Skew within hydration

`--skew 0.9` puts 90 % of rows on one key. For the hierarchical `max` it does
**not** worsen the stall (5 s skewed vs 12 s uniform — actually lower, because
the skewed hydration finishes faster). So the reduce's starvation is driven by
total snapshot size, not group skew. Skew matters for the *window* operators
(above), where a giant partition is a single large chunk, not for the reduce.

## Source corroboration

`src/compute/src/render/reduce.rs::build_bucketed` (hierarchical min/max),
`reduce.rs::build_basic_aggregate` (window functions, via
`eval_with_fast_window_agg`), and `src/compute/src/render/top_k.rs::build_topk`
(top-k) are **three distinct rendering paths** that each process a whole
group/partition in one `mz_reduce_abelian` activation with **no fuel/yield
logic**. The fueled paths stay fresh: `build_accumulable` (count/sum), the
fueled `flat_map` (`context.rs`, `REFUEL`), and `LINEAR_JOIN_YIELDING`. The code
split matches the data split exactly.

## It's the operator, not the duration (confound-buster, 20M)

| op | stall/hyd | hydration |
|---|---|---|
| `join` | 0.10 | 21.7 s |
| `index` | 0.12 | 16.7 s |
| `distinct` | 0.10 | 7.0 s |

A 21.7 s join and 16.7 s index keep the probe fresh, while a 14 s top-k and 46 s
max do not. Duration is ruled out.

## Same pathology in steady state: one large transaction (`workers=1`, 3M base)

Hydrate the expensive object, then issue a *single* large `INSERT` and watch the
probe while the operator absorbs that one diff:

| op | workers | inserted rows | stall |
|---|---|---|---|
| `max` | 1 | 500k | 3.0 s |
| `max` | 1 | 3M | **21–23 s** |
| `max` | 1 | 6M | **34.0 s** |
| `topk` | 1 | 3M | **9.0 s** |
| `count` | 1 | 3M | 1.0 s |
| `sum` | 1 | 3M | 2.0 s |
| `max` | 4 | 3M | 4.0 s |
| `topk` | 4 | 3M | 2.0 s |

Findings:
- The hierarchical `max` stalls in proportion to the transaction size (~linear);
  accumulable `count`/`sum` absorb the same 3M-row diff with no stall. So the
  non-yielding behavior is **per single diff**, and hydration is just the
  special case where the diff is the whole snapshot.
- It **generalizes to `topk`** (9 s on the same 3M diff) — both bucketed
  operators, as at hydration.
- It **shrinks with workers exactly like hydration** (`max` 23 s → 4 s, `topk`
  9 s → 2 s from 1 → 4 workers) — more workers fragment the diff into shorter
  non-yielding activations, but the mechanism is unchanged.
- It is **independent of diff shape** — a large `DELETE` (14 s) and `UPDATE`
  (21 s) of the top 2M ids starve `max` just like the `INSERT` does, while
  `count` stays fresh (≤ 1 s) for all three. Any large insertion / retraction /
  mixed diff triggers it.

This is the production-relevant form: a bulk DML statement (`INSERT…SELECT`,
`UPDATE`, `DELETE`) stalls co-located objects in steady state.

## Cross-cluster contagion (the `xcluster` workflow)

Topology: `shared` MV on cluster **up** (fed by a table), `consumer` MV on a
**separate** cluster **down** that reads `shared`. We place the expensive `max`
on one cluster and watch both:

| expensive op on | `shared` stall (up) | `consumer` stall (down) | |
|---|---|---|---|
| down (same cluster as consumer) | 0 ms | 32,029 ms | within-cluster control |
| **up (cluster of the shared input)** | **25,028 ms** | **25,028 ms** | **CROSS-CLUSTER** |

When `max` runs on **up**, it starves `shared` there (25 s) — and `consumer` on
**down stalls by the same 25 s**, despite `down` having spare capacity. The
consumer cannot advance past its input's frozen write frontier, so the
starvation **leaks across the cluster boundary** through the shared persist
collection. Operational impact: a heavy MV one team runs on their cluster can
silently stall another team's downstream MV on a different cluster — the
isolation that separate clusters are supposed to provide does not hold for
*freshness* when they share an input. (Note: this is propagation of the
underlying reduce/top-k bug, not a separate bug — but it is the reason that bug
matters beyond a single cluster.)

## User-facing severity: hung SELECTs (the `rtr` workflow)

The stall is not just dashboard lag. The freshness lag is exactly how stale a
`serializable` read is; a `strict serializable` read instead *waits* for the
object's write frontier to reach real time. Firing both against the cheap probe
while the `max` hydrates (46 s):

| isolation | worst SELECT latency on the probe |
|---|---|
| serializable | **5 ms** (returns stale immediately) |
| strict serializable | **38,565 ms** (blocks ~the stall) |

So under strict-serializable isolation the compute stall surfaces to the user as
a **~38 s hung query** against an otherwise-trivial object — the operationally
visible form of the bug, not merely a lagging metric.

## Restart / 0dt recovery with many starving MVs (the `recovery` workflow)

Hydrate N co-located `max` MVs, drop+recreate the replica (RF 0→1) so all
rehydrate from persist at once, and watch the cheap probe:

| n MVs | first recovery | post-recovery stall | fleet rehydrate |
|---|---|---|---|
| 1 | 544 ms | 11.0 s | 18.9 s |
| 10 | 659 ms | **19.0 s** | 176.9 s (~3 min) |
| 20 | 906 ms | (≥ that) | **373.9 s (6 min)** |

The probe's *first* return-to-fresh is fast (sub-second) regardless of fleet
size — but it then **stalls repeatedly (11–19 s) for the duration of the fleet
rehydration**, which itself grows linearly with the number of starving MVs (20
hierarchical reduces ⇒ a **6-minute** rehydration). Operationally: a restart or
blue-green deploy of a cluster carrying many hierarchical-reduce MVs takes
minutes to fully recover, with the cheap co-located objects flickering stale
throughout — the single-operator bug multiplied across the fleet, exactly the
moment (a deploy) when freshness matters most.

## Scale: the stall grows linearly toward production magnitude (the scale sweep)

`max` hydration, single worker, escalating row counts:

| rows | probe stall | hydration |
|---|---|---|
| 5,000,000 | 30 s | 45 s |
| 15,000,000 | 92 s | 132 s |
| 30,000,000 | **189 s (~3 min)** | 271 s |

The stall scales **linearly** with snapshot size (~6.3 ms per 1k rows, ~0.7× of
the hydration). Extrapolating: 100M rows ≈ a 10-minute stall, **1B rows ≈ ~1.5
hours**. Production arrangements are routinely that large, so a single bulk load
or object (re)build feeding a hierarchical reduce would freeze co-located
freshness — and cascade downstream / hang reads — for **minutes to hours**, not
the seconds seen in the small repros. (True billion-row scale is not reachable
in this local harness; the 30M-row, 3-minute data point is the largest measured
confirmation of the linear trend.)

## Correctness: the bug is liveness-only, not wrong results (the `validate` workflow)

During a `max`-hydration stall we checked, on a separate connection: (a) the
probe MV's `count(*)` vs its source table's `count(*)` read in **one serializable
transaction** (same timestamp), repeatedly through the stall; and (b) after
settling, the expensive MV's result vs a fresh recomputation from `big`.

- **MV-vs-table consistency during the stall: OK** (no violation across the whole
  window) — a read is stale but never inconsistent with its own timestamp.
- **Expensive MV result vs ground truth: exact match** (`(1000000,
  4500000500000)` both sides).

So this is purely a **liveness** problem (data is *stale*, never *wrong* or
inconsistent) — an availability/freshness bug, not data corruption. That bounds
the blast radius: queries block or return stale-but-correct data, they do not
return incorrect results.

## Observability: the lag view shows the per-minute MINIMUM, not the peak (#5 — by design)

With a clean measurement (lightweight 200 ms `FreshnessMonitor` for the direct
stall, a separate light loop sampling the view), during a 5M `max` hydration:

| metric | value |
|---|---|
| direct frontier stall (`now()-write_frontier`) | **29.0 s** |
| `mz_internal.mz_wallclock_global_lag` (user-facing) | **11.0 s** (`reported = True`) |

So the user-facing freshness view is **not blind** — it shows a real lag spike —
but it reports a number well below the true peak (11 s vs 29 s here). This is
**by design, not a defect**, and the earlier "coarser sampling cadence" guess was
wrong: `mz_wallclock_global_lag` is derived from
`mz_wallclock_global_lag_history`, which records the per-minute **`min(lag)`**
(`SELECT object_id, min(lag) … GROUP BY object_id, occurred_at`, see
`src/catalog/src/builtin/mz_internal.rs`; the column is documented as "the
minimum wallclock lag observed for the object during the minute"), and the
top-level view returns the most recent such minute. So it structurally surfaces
the *floor* of a stall, not its peak — deliberately, so a single-tick transient
spike does not trip an alert. The operational consequence is a **monitoring
caveat, not a bug**: a customer watching `mz_wallclock_global_lag` mid-incident
will systematically underestimate how stale an object got. Peak / per-replica
data is available in `mz_wallclock_global_lag_histogram` and the raw
`mz_wallclock_lag_history`. (Do **not** report this alongside BUG 1 / BUG 2.)

## Cross-cluster contagion cascades the full chain (the `xchain` workflow)

A 3-cluster chain `c0 → c1 → c2` (m0 on c0 from a table, m1 on c1 reads m0, m2
on c2 reads m1), placing the expensive `max` on one cluster:

| expensive op on | m0 stall (c0) | m1 stall (c1) | m2 stall (c2) |
|---|---|---|---|
| c0 (head) | 25,026 ms | 25,026 ms | **25,026 ms** |
| c1 (middle) | **0 ms** | 32,031 ms | **32,031 ms** |

The stall propagates **strictly downstream, undiminished, across every cluster
boundary**: starving the head stalls all three clusters equally (m2 is two hops
and two cluster-boundaries away); starving the middle leaves the upstream m0
fresh but stalls m1 and m2. So a single starving operator's blast radius is the
*entire transitive downstream dependency graph*, no matter how many clusters it
spans — there is no attenuation per hop.

## SUBSCRIBE goes silent too (the `subscribe` workflow)

Holding a `SUBSCRIBE … WITH (PROGRESS)` open on the cheap probe while `max`
hydrates (46 s): the largest gap between progress messages was **27,449 ms**.
A streaming consumer sees the probe go silent for ~the stall — the streaming
analog of the strict-serializable hung read. Both pull-reads (RTR) and
push-reads (SUBSCRIBE) surface the stall to users, not just dashboards.

## It's compute, not persist

Under the same expensive `max` at `workers=1`, an **index probe** (compute-only,
no persist output) stalled 39 s — essentially the same as a **materialized-view
probe** at 41 s. The victim need not write to persist to be starved.

## Worker scaling: shrinks but does not vanish (`max`, 3M)

| workers | stall/hyd | | topology | stall/hyd |
|---|---|---|---|---|
| 1 | 0.81 | | scale=2,workers=2 (4) | 0.45 |
| 2 | 0.70 | | scale=2,workers=4 (8) | 0.42 |
| 4 | 0.52 | | scale=4,workers=4 (16) | 0.29 |
| 8 | 0.34 | | | |
| 16 | 0.22 | | | |
| 32 | 0.19 | | | |

More workers means each handles fewer buckets, so non-yielding activations are
shorter and the probe slips through more often — but even at 16–32 workers the
probe still degrades to several × baseline. Multi-process (`scale>1`) behaves
like thread scaling at equal total worker count, so distributing across
processes doesn't fix it either. (Past 8 workers the hydration time plateaus on
this 16-core host — `workers=32` oversubscribes.)

---

# Shapes that did NOT reproduce a freshness issue

These are valuable negatives: they narrow where the real risk is.

## Steady-state retraction churn (even with hot-key skew)

`--skew 0.9` puts 90 % of `big` in one key (a 2.7M-row group); the `retraction`
action repeatedly retracts that group's current max and reinserts a smaller
value, forcing the hierarchical reduce to recompute the max.

| op | action | stall |
|---|---|---|
| `max` | retraction (skew 0.9) | **0 ms** |
| `count` | retraction (skew 0.9) | 0 ms |

**No starvation.** Small per-update diffs (one retraction every 200 ms) are
absorbed cheaply and yield fine. This is *not* a contradiction of the `big_txn`
result above: what starves is a **large single diff**, not steady-state activity
per se. Small, frequent updates → fresh; one big update → starves. The variable
is diff size.

## WITH MUTUALLY RECURSIVE — long iteration, but yields

A WMR fixpoint that iterates 50,000 rounds (`SELECT 0 UNION SELECT n+1 FROM it
WHERE n < 50000`) takes **16 s** to hydrate on one worker — yet a co-located
index probe stays **perfectly fresh (0 ms stall)** throughout:

| op | hydration | probe stall | result |
|---|---|---|---|
| wmr (50k iterations) | 16,040 ms | 0 ms | PASS |

So the recursive operator **yields between iterations**. This is an important
negative: it shows the non-yielding pathology is **not** a general property of
long-running operators (WMR runs 16 s and stays cooperative) — it is specific to
the bucketed hierarchical reduce / top-k snapshot path. "Long" ≠ "starving."

## Fleet of many objects — fair scheduling holds (the `manyobjects` workflow)

50 accumulable `count`-reduce MVs created at once on one worker took **125 s** to
hydrate collectively, yet the co-located index probe's max stall was **1008 ms**
(≈ the oracle-interval baseline) — PASS. The worker round-robins a fleet of
cheap dataflows fairly; there is **no collective fleet starvation**. Together
with the WMR result, this shows the timely scheduler is fair *when operators
yield* — the bug is specific to the operators (hierarchical reduce / top-k) that
fail to yield on a large diff, not a general scheduling-fairness gap.

## Sink backpressure does not stall the input (the `sink` workflow)

A Kafka sink's produce path (MZ → toxiproxy → kafka via the TOXI listener) is
bandwidth-capped under a heavy write load; we watch both the sink's frontier and
its input MV's:

| sink produce-path cap | input MV lag | sink lag |
|---|---|---|
| unlimited | 601 ms | 601 ms |
| 200 KB/s | 0 ms | 0 ms (keeps up) |
| 20 KB/s | **0 ms** | **18,317 ms** |

At 20 KB/s the sink falls **18 s** behind (it can't emit fast enough), but its
input MV stays **perfectly fresh (0 ms)**. MZ does **not** backpressure a sink's
input: only the sink itself lags. The sink's read hold blocks its input's
*compaction* (`since` can't advance past un-emitted data), not the input's
*freshness* (write frontier keeps advancing), so other readers of that input are
unaffected. A slow downstream sink cannot poison upstream freshness.

## Coordinator / timestamp-oracle contention — no global stall (the `oracle` workflow)

The coordinator and timestamp oracle are environment-global (they advance table
frontiers and hand out query timestamps), so contention there could stall *every*
object on *every* cluster at once — a shape orthogonal to per-cluster operator
starvation. We hammered them and watched two **idle** MVs (no writes, no
co-located compute — pure coordinator/oracle-advancement signals), one on `test`
and one on the default cluster:

| flood | idle MV stall (test) | idle MV stall (default) |
|---|---|---|
| baseline (quiet) | ~0.6 s | ~0.6 s |
| 24× DDL (CREATE/DROP TABLE loop) | **0 ms** | **0 ms** |
| 32× peek (SELECT loop) | **0.68 s** (≈ baseline) | 0.68 s |

**Negative — frontier advancement held.** Neither a 24-thread DDL flood nor a
32-thread peek flood made the idle objects stale: their write frontiers kept
advancing at the ~oracle-interval baseline on both clusters. So
coordinator/oracle frontier-advancement is well-isolated from DDL and peek load
at this intensity — the global-stall shape did **not** reproduce. Caveats: a
single-node local metadata store was used (a slow/contended remote
consensus/oracle store is untested), and the flood may not have fully saturated
the coordinator. So this is "no stall at this intensity," not "impossible."

## Combined workload: no emergent amplification (the `combined` workflow)

Running a realistic mix on one cluster simultaneously — continuous ingest into a
second MV, 8 concurrent peekers, **and** the `max` reduce hydration — gave a
probe stall of **17 s**, ≈ the hydration-alone baseline (~18 s at 3M). The
combination is **not** super-additive: once the non-yielding reduce has the
worker, it already freezes everything for the hydration's duration, so adding
ingest/peek load does not lengthen the stall. The dominant factor is the single
non-yielding operator, not emergent contention among many cooperative ones (and
a fleet of *cooperative* loads stays fair — see `manyobjects`).

## Memory pressure does not gradually stall freshness (the `memory` workflow)

Holding a ~1.5 GB arrangement on a 4 GiB cluster co-located with the probe: no
OOM, probe max stall **988 ms** (≈ baseline, fresh). MZ is Rust (no managed GC),
so memory pressure does **not** produce creeping freshness lag the way a GC'd
runtime might — it surfaces as a hard cgroup **OOM kill** once the limit is
exceeded, which restarts the replica and triggers rehydration (covered by the
`recovery` workflow). So the memory-related freshness failure mode is
binary/availability (OOM → rehydrate), not gradual staleness.

## RETAIN HISTORY — read frontier held, write freshness unaffected (`retain`)

A passthrough MV under heavy sliding-window churn, swept over retention windows:

| retention | write stall | write lag | read_lag (since) |
|---|---|---|---|
| default | 0 ms | 40 ms | 0.9 s |
| 60 s | 0 ms | 0 ms | ~22 s |
| 600 s | 0 ms | 0 ms | ~22 s |

`RETAIN HISTORY` does **not** degrade live (write-frontier) freshness — the write
stall stays flat at 0 ms regardless of window. It holds the **read** frontier
(`since`) back so AS OF / time-travel reads work (read_lag grows with the window,
here capped by the ~20 s run length). So retention trades readable history for
memory, not for live freshness. Read-side freshness is a separate axis from
write-frontier freshness, and the costly read-side case (hung strict-serializable
reads) is the `rtr` result above, not retention.

## Dependency chains (8-hop pass-through MV chain)

| depth | action | stall |
|---|---|---|
| 1 | steady_state | 0 ms |
| 8 | steady_state | **0 ms** |
| 8 | `max` hydration | 26 s (same as a single probe) |

Chain depth adds **no measurable freshness lag** in steady state — frontier
propagation through 8 hops is sub-oracle-tick. Under a co-located expensive
hydration the whole chain just freezes together. Propagation latency is not a
distinct shape here; a genuinely slow middle layer would just be the starvation
finding again.

## Storage / blob latency (persist blob routed through toxiproxy → minio)

Consensus stays on the metadata store; only blob I/O is delayed.

| blob latency | idx_lag | mv_lag | stall |
|---|---|---|---|
| 0 | 0 | 0 | 0 |
| 150 ms | 538 ms* | 538 ms* | 0 |
| 500 ms | 495 ms* | 495 ms* | 0 |
| 1000 ms | 0 | 0 | 0 |
| 2000 ms | 0 | 0 | 0 |

\* one-time transient at toxic application; not sustained.

Blob **latency** up to 2 s produces **no sustained freshness lag** for a
low-volume object: persist's async **batched** blob writes plus
**consensus-gated** frontier advancement keep blob latency off the
frontier-advancement critical path. Index and MV lag identically (the shared
input table's commits are the gating factor, not the MV's extra output write).

### Storage throughput (heavy write volume × blob latency)

With ~250k rows/s into a write-heavy pass-through MV (`--heavy-rows-per-batch`):

| blob latency | idx_stall | idx_lag | mv_stall | mv_lag |
|---|---|---|---|---|
| 0 ms | 408 ms | 1650 ms | 408 ms | 1652 ms |
| 100 ms | 0 | 1161 ms | 0 | 1161 ms |
| 300 ms | 999 ms | 2157 ms | **2003 ms** | **3156 ms** |

Now the index-vs-MV **discriminator separates**: at 300 ms blob latency the MV
lags ~1 s more than the index (3156 vs 2157), because the MV's *output* persist
writes are blob-bound while the index has no output writes. So storage-bound
freshness is real, distinct, and hits persisted objects specifically — but it is
**mild and bounded** (~2–3 s here), not the unbounded multi-second-per-diff stall
of the compute pathology. persist's batching keeps it in check; a severe
storage-bound stall would need far higher latency/volume or true blob-bandwidth
saturation.

---

# Source / sink ingestion freshness is NOT starved by co-located compute

`workflow_ingestion` runs a Kafka source + Kafka sink `IN CLUSTER test` (a host
producer streams to the topic), alongside a co-located **index** as a compute
reference, and monitors all three while the expensive `max` MV hydrates on the
same cluster. One run, same cluster, same expensive op (5M rows):

| phase | src_stall | sink_stall | idx_stall (compute ref) | expensive |
|---|---|---|---|---|
| steady_state | 0 ms | 0 ms | 0 ms | — |
| `max` hydration | **0 ms** | **0 ms** | **32,033 ms** | 49,019 ms |

The index (timely-worker compute) stalls **32 s**; the Kafka **source and sink
ingestion frontiers do not stall at all** (0 ms) through the same 49 s of the
expensive hydration. So:

- **The starvation pathology is compute-specific.** Source/sink ingestion rides
  the storage / persist-async path (reclocking + persist append/read happen off
  the timely worker), so a non-yielding compute operator on that worker does not
  block ingestion-frontier advancement.
- This also means an index-vs-source/sink probe pair is a clean discriminator:
  co-located compute starvation hits compute objects only, leaving ingestion
  freshness intact.

`kafka-source`, `kafka-sink`, and `pg-source` (Postgres CDC) are now first-class
**probe kinds in the main matrix**, so every expensive_op × action × size applies
to them, not just a separate workflow. The unified matrix reproduces the contrast
in one table (5M rows, op=`max`, ~46–47 s hydration):

| probe | action | stall | result |
|---|---|---|---|
| index | hydration | 30,023–31,044 ms | FAIL |
| kafka-source | hydration | 817 ms | PASS |
| kafka-sink | hydration | 408 ms | PASS |
| pg-source (Postgres CDC) | hydration | 816 ms | PASS |
| mysql-source (MySQL CDC) | hydration | 814 ms | PASS |
| sql-server-source (SQL Server CDC) | hydration | 813 ms | PASS |

**All four ingestion engines behave identically** — Kafka, Postgres
logical-replication, MySQL binlog, and SQL Server CDC sources all stay ~0.8 s
fresh through the same ~46 s `max` hydration that stalls the co-located index
~31 s. The compute-immunity of ingestion is fully **engine-agnostic**: every
source reclocks its upstream offset/LSN onto the wallclock oracle and advances
its frontier off the timely worker, untouched by a non-yielding co-located
compute operator. (SQL Server's CDC-capture-job cadence did not add measurable
lag here — its steady-state freshness, ~0.6 s, matches the others.)

## Ingestion-intrinsic lag: source vs. its own arrival rate

Can a source fall behind its *own* broker (no co-located compute)? Sweeping the
produce rate at `steady_state` on one worker (`--msgs-per-tick`):

| produce rate | src stall | src lag |
|---|---|---|
| ~5k msg/s | 613 ms | 1,587 ms |
| ~200k msg/s | 621 ms | 1,910 ms |
| ~1M msg/s | 1,639 ms | 3,316 ms |

A single-worker Kafka source absorbs up to ~1M small msgs/s with only mild,
sub-linear lag growth (200× the rate → ~2× the lag) and never goes stale.
**Caveat:** the single host producer thread is likely the bottleneck at the top
end, so this shows the source keeps up with a substantial real producer, not its
absolute ceiling.

### Backlog catch-up (the `backlog` action)

The conclusive ingestion-bound test: pre-load a backlog, *then* create the
source, and measure time-to-fresh (catch-up). One worker:

| backlog (messages) | catch-up time |
|---|---|
| 1,000,000 | 1.5 s |
| 5,000,000 | 7.9 s |
| 20,000,000 | 29.5 s |

Catch-up scales **~linearly** with the backlog (~1.5 s per 1M small messages).
The source always catches up (it never gets permanently stuck), so this is
**real but bounded, transient** ingestion lag: a source that starts behind by a
big backlog is stale for a time proportional to the backlog, then fresh. This is
ingestion-intrinsic — present with no co-located compute at all — and is the
expected catch-up cost, not a yielding bug.

Measurement note: during a Kafka *snapshot* the write frontier sits at its
initial value until the first commit, so `now() - write_frontier` reads a bogus
~epoch value mid-catch-up; time-to-fresh (when it first reaches wallclock) is the
correct metric, which is what the `backlog` action reports.

### Upstream link throttling (the `upstream` workflow)

The one freshness vector tied to the upstream link, not the cluster. MZ's
Postgres connection is routed through toxiproxy (MZ → toxiproxy → postgres) and
latency is injected on that link; the host writer inserts *directly* into
Postgres, so only MZ's ingestion path is throttled. One worker, no co-located
compute, latency added each way:

| added latency (each way) | src stall | src lag |
|---|---|---|
| 0 (baseline) | 0.6 s | 1.8 s |
| 250 ms | ~0 | 1.4 s (absorbed) |
| 1000 ms | 3.0 s | 7.4 s |
| 2000 ms | 7.0 s | 15.2 s |
| 4000 ms | 13.8 s | 27.0 s |

Findings:
- **Small latency is absorbed.** Up to ~250 ms each way the streaming replication
  pipeline hides it entirely (lag stays at the ~1.8 s baseline).
- **Large latency adds bounded, proportional lag.** Beyond ~1 s the source lag
  scales ~linearly at **≈7× the one-way latency** (≈3.5× the round trip) and
  **stabilizes** at that level — it does not run away. Each replication progress
  step costs a few round-trips, so the frontier advances less often (the stall
  metric grows with latency too) but always keeps advancing.
- This is ingestion-intrinsic and the expected cost of a slow link, not a
  yielding bug. A source behind a slow upstream is proportionally stale but
  stable.

#### Bandwidth cap (`--mode bandwidth`) — the one unbounded case

A *narrow* link behaves qualitatively differently from a *slow* one. Capping the
downstream byte-rate under a ~10k row/s write load:

| downstream rate cap | src stall | src lag |
|---|---|---|
| unlimited | 0.6 s | 1.0 s |
| 500 KB/s | 0 | 0 (keeps up) |
| 100 KB/s | 18.9 s | 22.3 s |
| 20 KB/s | 19.0 s | **44.6 s (and climbing)** |

Unlike latency, a bandwidth cap **below the WAL arrival rate makes lag grow
without bound** — the source accumulates a backlog faster than it can drain, so
within the 20 s window lag already reaches 44 s at 20 KB/s and keeps rising.
Above the throughput threshold (here ~500 KB/s) it stays perfectly fresh. So
there is a **sharp cliff at link-throughput = data-arrival-rate**: this is the
genuine "source can't keep up" condition, and it is the *only* ingestion vector
that produces unbounded staleness (still expected behavior — a too-narrow pipe —
but the operationally important one to alert on).

#### Upsert-source state (the `upsert` workflow)

An UPSERT Kafka source keeps per-key state. Sweeping the key space at 20k msg/s:

| keyspace | src stall | src lag |
|---|---|---|
| 1,000 (hot updates) | 611 ms | 1.5 s |
| 1,000,000 | 204 ms | 1.1 s |
| unbounded (state grows) | 204 ms | 1.1 s |

Upsert-state maintenance does **not** lag ingestion freshness, whether the state
is hot (few keys, many updates) or growing without bound. Fresh throughout.

#### Link flap (`--mode flap`) — recovers cleanly

Severing the link for 5 s, restoring for 8 s, ×3 cycles:

| | max stall | max lag | recovery after last restore |
|---|---|---|---|
| 3 cycles | 5.6 s | 6.4 s | **0.86 s** |

The frontier freezes for ~the down-interval (5 s), then on restore the source
**resumes from its Postgres replication slot and catches up in < 1 s** — no
permanent staleness, no manual intervention. Transient disconnects are handled
gracefully.

# Measurement note: freshness is object-type-dependent (source frontier domains)

Extending the oracle to sources surfaced a measurement subtlety. The freshness
metric `now() - write_frontier` assumes the write frontier lives in the
**wallclock-millis** timestamp domain (the timestamp oracle). That holds for
tables, indexes, materialized views, and **Kafka sources** (which *reclock*
Kafka offsets onto oracle timestamps). It does **not** hold for a
**load-generator `COUNTER` source**: its write frontier is a **logical offset**
that starts at 0 and never approaches wallclock, so `now() - write_frontier`
is meaningless (~`now()` forever) — and `mz_internal.mz_wallclock_global_lag`
correctly reports **NULL** for it. Verified directly: counter source
`write_frontier = 0`, an MV's `write_frontier = 1781958528254` (≈ now, ms).

Consequence: a load-generator source is *not* a valid freshness probe for this
oracle. Real source-ingestion-lag testing needs a wallclock-reclocked source
(Kafka), which is what `workflow_ingestion` uses.

# Caveats / still untested

- **Bug vs. expected** for the reduce/top-k hydration stall wants a compute/timely
  owner's sign-off, but the source (no fuel/yield in `build_bucketed`/`top_k.rs`)
  plus persistence across worker counts argue it is a real cooperative-yielding
  gap.
- **Storage throughput** (not latency) is the untested storage-bound vector.
- **Coordinator/oracle contention** (DDL floods, many concurrent peeks stalling
  global frontier advancement) is a global shape this harness does not exercise.
- Ingestion is now covered end-to-end: 4 engines under co-located compute (all
  fresh, engine-agnostic), arrival-rate, backlog catch-up, and the upstream link
  under latency (bounded), bandwidth (unbounded past the throughput cliff), and
  flap (recovers < 1 s). The remaining ingestion vectors are higher-order
  (schema-change / replication-slot-stuck / multi-table publication contention).
- Absolute hydration/catch-up times are host-load-noisy; the ratios and the
  scaling trends are the robust signal.

# Repro

```bash
# Confirmed pathology — accumulable (fresh) vs hierarchical (starves):
bin/mzcompose --find freshness run default --expensive-ops count,sum,max,min,distinct,reduce \
  --actions hydration --rows 5000000

# It's the operator, not duration:
bin/mzcompose --find freshness run default --expensive-ops distinct,join,index \
  --actions hydration --rows 20000000

# Same pathology from one large steady-state transaction (not just hydration):
bin/mzcompose --find freshness run default --expensive-ops max,count \
  --actions big_txn --rows 3000000 --big-txn-rows 6000000 --window-seconds 40

# Worker scaling:
bin/mzcompose --find freshness run default \
  --cluster-sizes 'scale=1,workers=1;scale=1,workers=4;scale=1,workers=16' \
  --expensive-ops max --actions hydration --rows 5000000

# Negatives:
bin/mzcompose --find freshness run default --expensive-ops max --actions retraction --skew 0.9 --rows 3000000
bin/mzcompose --find freshness run default --expensive-ops max --actions steady_state,hydration --chain-depth 8 --rows 3000000
bin/mzcompose --find freshness run storage --latencies-ms 0,500,2000
# storage throughput (MV lags more than index under heavy writes + blob latency):
bin/mzcompose --find freshness run storage --latencies-ms 0,100,300 --heavy-rows-per-batch 50000

# source/sink ingestion freshness vs co-located compute (Kafka), focused workflow:
bin/mzcompose --find freshness run ingestion --rows 5000000 --window-seconds 15

# source/sink are also first-class probe kinds in the main matrix; all four
# ingestion engines (Kafka, Postgres, MySQL, SQL Server CDC) behave identically.
# (SQL Server CDC needs a more lenient stall threshold only due to host-driver-
# less sqlcmd-based load; freshness itself matches the others.)
bin/mzcompose --find freshness run default \
  --probe-kinds index,kafka-source,kafka-sink,pg-source,mysql-source,sql-server-source \
  --expensive-ops max --actions steady_state,hydration --rows 5000000 \
  --freshness-threshold-ms 15000 --max-stall-ms 15000

# ingestion-intrinsic lag: arrival-rate sweep and backlog catch-up:
bin/mzcompose --find freshness run default --probe-kinds kafka-source --actions steady_state \
  --msgs-per-tick 100000 --window-seconds 25
bin/mzcompose --find freshness run default --probe-kinds kafka-source --actions backlog \
  --expensive-ops max --backlog-rows 20000000

# upstream link throttling (MZ -> Postgres via toxiproxy):
bin/mzcompose --find freshness run upstream --mode latency --latencies-ms 0,250,1000,2000,4000
bin/mzcompose --find freshness run upstream --mode bandwidth --rates-kbps 0,500,100,20 --msgs-per-tick 2000
bin/mzcompose --find freshness run upstream --mode flap --flap-cycles 3 --flap-down-s 5 --flap-up-s 8
bin/mzcompose --find freshness run upsert --keyspaces 1000,1000000,unbounded --msgs-per-tick 2000

# read-side retention, fleet fairness, sink backpressure:
bin/mzcompose --find freshness run retain --retentions default,60s,600s
bin/mzcompose --find freshness run manyobjects --fan-count 50 --rows 1000000
bin/mzcompose --find freshness run sink --rates-kbps 0,200,20 --msgs-per-tick 2000

# WMR (yields, fresh) and cross-cluster contagion (starvation leaks across clusters):
bin/mzcompose --find freshness run default --expensive-ops wmr --probe-kinds index --actions hydration --rows 1000000
bin/mzcompose --find freshness run xcluster --starve-on down,up --rows 5000000

# user-facing amplification: strict-serializable reads hang during the stall:
bin/mzcompose --find freshness run rtr --rows 5000000

# second non-yielding family (window funcs starve on a large partition):
bin/mzcompose --find freshness run default --expensive-ops window_sum,window_rank,window_lag \
  --probe-kinds index --actions hydration --rows 4000000 --skew 0.95
# cross-cluster cascade depth, streaming reads, restart/0dt recovery:
bin/mzcompose --find freshness run xchain --starve-on c0,c1 --rows 5000000
bin/mzcompose --find freshness run subscribe --rows 5000000
bin/mzcompose --find freshness run recovery --counts 1,5,20 --rows 2000000
bin/mzcompose --find freshness run memory --rows 6000000 --payload-bytes 256

# multi-worker doesn't fix the window-function case; coordinator/oracle is robust:
bin/mzcompose --find freshness run default --cluster-sizes 'scale=1,workers=4' \
  --expensive-ops window_sum,max --probe-kinds index --actions hydration --rows 4000000 --skew 0.95
bin/mzcompose --find freshness run oracle --mode ddl --concurrency 24
bin/mzcompose --find freshness run oracle --mode peek --concurrency 32

# completeness: DISTINCT ON / LATERAL starve (map to BUG 2 / BUG 1); rest fresh:
bin/mzcompose --find freshness run default \
  --expensive-ops join_blowup,except_op,count_distinct,distinct_on,lateral \
  --probe-kinds index --actions hydration --rows 2000000
# temporal window expiry into a reduce (does not starve as a clean expiry):
bin/mzcompose --find freshness run temporal --rows 3000000 --expire-in-s 15

# scale (stall grows ~linearly), correctness+observability, combined workload:
bin/mzcompose --find freshness run default --expensive-ops max --probe-kinds index \
  --actions hydration --rows 30000000 --hydration-timeout-s 1200
bin/mzcompose --find freshness run validate --rows 5000000
bin/mzcompose --find freshness run combined --rows 3000000 --peekers 8
```
