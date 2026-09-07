# Two-runtime read isolation

## Summary

This document describes the **interactive runtime**: a second, in-process timely runtime
that renders temporary dataflows and serves reads directly off the arrangements the
maintenance runtime builds, zero-copy through a per-process sharing registry. It is a
placement choice for rendered dataflows.

A second mechanism, **peek execution**, decides how a fast-path index peek's *walk* runs:
in budgeted slices on the serving worker, or offloaded to a blocking task once it exceeds
its budget. The two were originally conceived as one and measurement separated them: they
fix different problems, are distinguished by different workloads, and are flagged and
rolled separately. Peek execution needs no second runtime and shipped on its own, with its
own design document (`20260825_peek_execution.md`, #38449). It appears here only where the
comparison bounds what the interactive runtime is claimed to buy. See
[Peek placement is a separate axis](#peek-placement-is-a-separate-axis).

The feature is gated by the `enable_compute_interactive_runtime` dyncfg, off in production
and on by default in CI. With the dyncfg off, a replica runs a single `Solo` runtime that
takes the same code paths, publishes nothing, and carries no `role` metric label. With it
on, every index the maintenance runtime renders gains a publisher, which is visible in the
goldens CI runs with the flag on: `relations.slt` lists the publisher operators, and the
arrangement-size bound in `introspection-sources.td` moved.

The implementation lands as a stack of eight pull requests, each a self-contained layer,
followed by two that add the benchmarks. [Implementation history](#implementation-history)
lists them.

This document is the single design of record and is self-contained. The measurements it
cites by `E` number live in the project document "Interactive read isolation: experimental
evaluation", along with the experiment definitions and what remains unmeasured.

## Motivation

Reads and index maintenance compete inside a single timely runtime. Timely does
not preempt a running operator, so a maintenance operator that runs to
completion over a large input blocks any read interleaved on the same worker.
The read waits, not because the machine is out of CPU, but because the one run
loop is busy and cannot be interrupted.

The sharpest form of the problem is introspection. `mz_introspection` and the
logging dataflows describe a replica's own dataflow state, so they cannot be
served from any other replica. They are exactly what an operator reaches for
during hydration or a burst of batchy work, which is precisely when the
maintenance runtime is pinned and the introspection read blocks. Today we fly
dark at the moment we most need to see.

## Problems and mechanisms

The symptoms people report are more numerous than their causes, and grouping the
symptoms by cause changes which solution applies to each. This section is the
spine: the mechanisms, what evidence there is for each, and which of the available
solutions actually reaches it. The rest of the document argues for some of those
solutions, and this table is what bounds that argument.

Two conventions. Every claim carries an evidence label, and a cell that is argued
from mechanism rather than measured says so. And the table deliberately includes
mechanisms no solution here addresses, because a decomposition that only lists the
causes we have answers for is not a decomposition.

### The symptoms

The third column is the one to read first when prioritizing. The same worker
occupancy produces several of these symptoms, but the blast radius differs by an
order of magnitude between them, and only some of it lands in a number a customer
sees.

| Symptom | Evidence | Who pays | Mechanism |
|---|---|---|---|
| Peeks queue behind other peeks on a busy replica | measured, E1: 6163 ms worst case at a 2170 ms walk | the queued query | M1 |
| `WHERE key = <lit> ORDER BY .. LIMIT 1` on a skewed key stalls every lookup behind it | reported from the field, then measured, E11: 58 of 261 over 200 ms becomes 0 of 261 | every concurrent lookup on the replica | M1 |
| A point lookup on a resident index stalls behind walks of a swap-resident one | measured, E8b: 29.2 s worst case becomes 152 ms | every concurrent lookup on the replica | M1 |
| A peek runs to completion once started and cannot be cancelled | confirmed in the code | the replica's CPU and its compaction holds, after the client has left | M1, M9 |
| A swap-resident walk is slower and far less predictable inline than offloaded | measured, E8b: 2.3 s against 3.6, 4.7 and 56.4 s | the query itself | **M11, two candidates** |
| Peeks show jitter on a replica managing large state | measured, E12: a 3.9 ms lookup reaches p90 129.5 ms and p99 278.4 ms, with 17.1% of requests above ten times the idle median | the queued query | M2 |
| Interactive dataflows are slow, and introspection is unavailable, while a replica is busy | measured, E7 and E9: 2835 to 1456 ms, and 4.4 to 7.5 s polls to about 160 ms | whoever is diagnosing an incident, while it is happening | M3 |
| A temporary dataflow costs about 900 ms to create and tear down | measured, E7: a floor of 850 to 950 ms in every cell, quiet or loaded, either runtime, for about 120 rows | every interactive query, unconditionally | M4 |
| A read cannot be answered until the frontier passes its timestamp | measured, E12: with the peek moved off the busy worker, strict serializable still reaches p99 185.8 ms against 5.8 ms at serializable. Also bounded by E9's staleness column, 170 to 1589 ms while hydrating | every default-isolation reader | M5 |
| Peeks serialize behind DDL on one coordinator thread | asserted elsewhere in this document, not measured here | **every query in the environment** | M6 |
| A default-isolation read pays a timestamp-oracle round trip | not measured here | every default-isolation reader | M7 |
| One expensive query makes every object on the replica look stale | measured, E13: a 2.24 s walk drives reported lag from 55 ms to 2340 ms, a 43x amplification, as a ramp of slope one | **every consumer of every object on the replica, and it is the number we report** | M8 |
| `SUBSCRIBE` delivers nothing usable until its initial snapshot completes | not measured | the subscriber, before it has received anything | M12, and M4 and M5 on top |
| A `SUBSCRIBE` snapshot stalls the replica the way an expensive peek does, and cannot be moved off it | not measured, `check`ed in the code: subscribes are excluded from the interactive runtime by an explicit condition | every consumer of every object on the replica | M2, M8, M9 |

Two rows have the widest radius and they are the two least addressed here. M6 spans
the environment and nothing in this document touches it. M8 spans the replica from a
single query and is the only mechanism whose cost appears in a customer's dashboard.

### The mechanisms

* **M1, non-preemptive queueing among peeks on one worker thread.** Four symptoms
  are this one defect. The skewed lookup is M1 with an extreme service time. The
  uncancellable peek is M1 seen from the client's side. And the swap case is M1
  with a service time dominated by blocking rather than computing, which matters
  because it is a *victim* latency: E8b's 29.2 s is a point lookup on a **resident**
  index queued behind two swap-resident walks, not a swapped walk itself.
* **M2, a peek queues behind a long operator activation.** Not M1, because the work
  ahead of the peek is a dataflow rather than another peek, and that difference
  decides which solutions reach it. Two sources of long activations. Operators with
  no fuel or yield at all, which is `reduce`, `top_k` and `threshold`. And spine
  merges, which amortize against the size of the arriving batch and are therefore
  long exactly when the batch is large, whether that is hydration or a bulk insert.
  E12 measured the second source: 500,000-row insert cycles produced steps
  approaching one second, 206 of them above 128 ms and 25 above 512 ms in a two
  minute window, and a peek arriving inside one waits for it. A trickle of small
  writes would produce neither, which is why the mechanism is about batch size
  rather than about state size as such.
* **M3, one dataflow scheduler, saturated.** Interactive rendering and introspection
  have nowhere to run while maintenance occupies the worker.
* **M4, temporary-dataflow creation and teardown.** Measured as a floor of 850 to
  950 ms for a 120-row late-materialization query, present when quiet and when
  loaded, on either runtime. It is larger than the tail that runtime placement
  recovers, so for the workload M3's remedy is justified by, this is the dominant
  term.
* **M5, a read cannot be answered until the relevant frontier passes its
  timestamp.** The timestamp itself is chosen by the coordinator from the timestamp
  oracle rather than by the replica, so the mechanism is not that the frontier
  *sets* the timestamp but that the frontier decides when the peek can be
  *answered*. A strict serializable read takes a timestamp at the write frontier,
  and an index's `upper` advances only when the maintenance worker steps, so a busy
  maintenance worker delays the answer whichever thread would serve it.
* **M6, control-plane serialization.** Peeks pass through one coordinator thread
  and serialize behind DDL there. This document states elsewhere that for
  non-introspection reads under load the control plane can be the first-order
  bottleneck, so it belongs in the decomposition even though nothing here touches
  it.
* **M7, linearized-timestamp acquisition.** At the default isolation level the
  coordinator additionally fetches a linearized read timestamp from the oracle, a
  round trip that is distinct from M5 and is known to be slow enough to warn about
  in the code.
* **M8, serving a peek costs freshness.** The dual of M1 and M2, and the one
  mechanism here whose cost is *reported to customers*. A maintained collection's
  write frontier advances only when the worker steps and processes input, so
  anything occupying the worker holds the frontier still and inflates the reported
  lag for every object on that replica. One expensive query is therefore a
  replica-wide freshness event, and the same occupancy that makes a peek slow makes
  everything else stale. M5 is the return path of the same loop: a stalled frontier
  then delays the next strict serializable read. A freshness stall is always a ramp
  of slope one rather than a step, because a frozen frontier means the lag is
  elapsed time since it froze, and it recovers in a single tick when the walk ends.
* **M9, held-back compaction lengthens later operator activations.** The only
  mechanism here that the *solutions* cause rather than cure. An in-flight or parked
  walk pins the batches it reads, so merges are deferred, so more batches accumulate,
  so subsequent operator activations run longer, which feeds back into M2 and M8. It
  applies to every solution that holds a cursor across time, which is the inline path,
  the sliced path and the offloaded path alike, and it is unbounded in the sliced path
  because parked scans are not admission-controlled. Unmeasured, and worth stating
  because a table in which every row only ever helps is hiding something.
* **M10, the replica has no CPU headroom.** Not a queueing mechanism but a validity
  condition, and it belongs in the table because two of the measured results depend on
  it. When the box is CPU-bound, moving work between threads reorders it rather than
  removing it, so a solution that relies on somewhere else to run has nothing to rely
  on. E12 and E13 both ran on a 32-core box where the offloaded thread never competed
  for a core. The experiment for this was planned as E3 and never run. Nothing reaches
  it, because no scheduling change manufactures CPU.
* **M11, why an offloaded swap-resident walk is faster than an inline one.** A
  measured effect with two candidate mechanisms and no verdict. Either the interleaved
  timely working set re-evicts the walk's pages, which is a locality effect, or the
  offload achieves more outstanding faults, which is a queue-depth effect. This
  document states elsewhere that preemption cannot explain it. The discriminator is
  the per-walk major-fault bracket in
  [the incremental path](#the-incremental-path-from-here): equal fault counts with
  different durations means queue depth, and more faults inline means locality. It has
  a column because it is the last effect uniquely attributable to the offload, and
  scoring the offload without one hides that.
* **M12, time to first usable output is proportional to collection size rather than
  result size, and no consistent prefix can be delivered early.** The `SUBSCRIBE`
  case. A peek's cost is bounded by its result, since a `LIMIT` thins it and an
  MFP filters it, but a subscribe's initial snapshot is the whole collection every
  time. Worse, the wait is not merely long but *indivisible*: every update at the
  chosen `as_of` must be complete before the frontier passes it, so while rows may
  arrive, nothing is actionable until progress advances past the snapshot timestamp.
  A consumer that needs a consistent starting state therefore waits for all of it.
  This is a different shape from every other mechanism here, which are all about
  *whose turn it is*. This one is about the size of an atomic unit of output.

  Two things compound it and one already solves it. M4 applies first, since a
  subscribe builds a temporary dataflow and pays the creation floor before anything
  happens, and M5 applies next, since the snapshot cannot be emitted until the
  frontier passes the `as_of`. Where the snapshot's data comes from changes the cost
  profile rather than the mechanism: with an arrangement on the cluster it is a
  cursor walk and worker-bound, without one it is a persist read and fetch-bound.

  The dual matters more than the symptom. A subscribe snapshot is a large operator
  activation, so it *causes* M2, M8 and M9 for everything else on the replica, on a
  scale bounded by collection size rather than by result size. And unlike a peek it
  cannot be moved: `Multiplexer` routes a dataflow to the interactive runtime only
  when `desc.subscribe_ids().next().is_none()`, so **subscribes stay on maintenance
  by construction and the second runtime cannot reach this at all.** That exclusion
  is load-bearing and its rationale is not recorded. An unbounded `until` is already
  excluded by the neighbouring condition, so the subscribe check only bites for
  `SUBSCRIBE ... UP TO`, and why that case must stay on maintenance should be written
  down or the condition removed.

### What each solution reaches

A blank cell means the solution does not reach that mechanism, so only the meaningful
cells carry text. `argued` means derived from the mechanism and not measured, and
`check` means it can be settled by reading code rather than by running anything. Note
how many cells are not measured.

| | M1 | M2 | M3 | M4 | M5 | M6 | M7 | M8 | M9 | M10 | M11 | M12 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| S0, another replica | statistically | statistically | yes | | **yes** | | | masks it? `check` | | | | |
| S1, cooperative peek slicing | yes, `argued` | | | | | | | **mostly**, E13: 2274 to 365 ms peak, at a light write load | **worsens it**, parked scans are not capped | | | |
| S2, cancellable peeks | cancellation only | | | | | | | for cancelled peeks, `argued` | helps, releases the hold early | | | |
| S3, interactive dataflows on a second runtime | | | **yes**, E7/E9 | | | | | dataflow-caused only, `argued` | | | | |
| S4, peeks routed to the interactive runtime | relocates the queue | **yes**, E12: p90 129.5 to 4.5 ms | | | | | | **yes**, E13: 102 ms peak, maintenance never sees the walk | | | | |
| S5, peeks on another thread | **yes**, E1/E11/E8b | **worse**, E12: p90 148.2 against 129.5 | | | | | | **yes**, E13: 101 ms peak, with core headroom | **worsens it**, holds for the whole walk | | **produces the effect** | |
| S6, budgeting long operator activations | | yes, `argued` | partial, `argued` | | | | | | | | | |
| S7, a bounded-seek plan for the skewed case | removes the work, `check` | | | | | | | removes the work, `argued` | | | | |
| S8, a re-entrant point-lookup structure | yes, `argued` | **yes**, `argued` | | | | | | yes, `argued` | | | | |
| S9, size- or residency-aware routing | | **removes S5's regression** | | | | | | | | | | |
| S10, fast-path or pooled temporary dataflows | | | | **the only candidate** | | | | | | | | |
| S11, coordinator sharding | | | | | | **the only candidate**, measured elsewhere at about +25% peek throughput | | | | | | |
| S12, oracle batching or avoidance | | | | | | | **the only candidate** | | | | | |
| S13, `SUBSCRIBE ... WITH (SNAPSHOT = false)` | | removes the snapshot's cost | | | | | | removes the snapshot's cost | | | | **removes the work, and already ships** |
| S14, allowing unbounded *transient* dataflows on the interactive runtime | | the snapshot's cost only | | | | | | the snapshot's cost only | | | | |
| S15, a chunked snapshot with partial-progress semantics | | | | | | | | | | | | `argued`, and a contract change |

M10 has no row at all, deliberately: no scheduling change manufactures CPU. M9 has no
row that cures it, one that partly helps, and two that cause it. M11 has one row that
produces the effect and none that explains it. M12 has an incumbent escape hatch that
works only for consumers not needing initial state, and nothing that makes a consistent
prefix available early without changing what `SUBSCRIBE` promises.

S14 is the cheapest thing that reaches an ordinary `SUBSCRIBE`, and it is not the
change it first looks like. Dropping the subscribe clause alone buys almost nothing,
because an ordinary subscribe's `until` is *already* empty and so is already excluded
by the neighbouring finite-`until` condition: `optimize/subscribe.rs` sets
`until = {MIN}` and joins each sink's `up_to`, which is empty without an explicit
`UP TO`. The subscribe clause therefore only bites for `SUBSCRIBE ... UP TO`.

What reaches the actual case is dropping **both** the finite-`until` clause and the
subscribe clause while *keeping* transience, so the predicate becomes
`desc.is_transient() && desc.copy_to_ids().next().is_none()`. Keeping transience is
what makes it cheap: transient ids are never retained by reconciliation, so nothing
regresses there, they pass the multiplexer's frontier filter unchanged, and an export
that is the imported arrangement aliases its publication point. It relies on the
compaction feedback described in
[Compaction feedback flows through the reader's handle](#compaction-feedback-flows-through-the-readers-handle),
and on closing the `SnapshotMode` gap recorded in the open findings. A `SUBSCRIBE` sink
needs the stream and not the trace, so it is the easiest case of that feedback, but the
feedback is what any other long-lived interactive dataflow needs and it is built for the
general case.

Maintained collections on the interactive runtime are a strictly larger change and one
piece of it has no home in the current protocol. See the reconciliation finding in
[the open findings](#open-findings-from-adversarial-review).

**M8 was predicted to invert the M1 ordering. It inverts one row, not the ordering,
and the prediction about slicing was wrong.** Registered before E13 ran: cooperative
slicing would buy little or nothing on freshness, because the total worker time the
walk consumes is unchanged and a frontier cannot advance past unprocessed data. E13
refuted that. Slicing cuts the peak from 2274 to 365 ms and the debt from 2572 to
656 ms·s, because the worker processes input *between* slices, so the lag is bounded
by the yielding quantum rather than by the walk duration. The reasoning confused
throughput with recency. What it was reaching for is still latent and untested: E13
writes one row per 100 ms, so a small share of the worker is ample, and the ramp
argument would only apply under a write load heavy enough to need most of it.

What did invert is **the offload's rank**. E12 measured it 17% worse than inline on
peek latency under operator contention; E13 measures it about 20x better than inline
on freshness. So it is not dominated on every axis at once, which is more than the
peek results alone left it with.

But the ordering as a whole does not reverse, because **S4 is best or tied-best on
both axes**, and two further findings close the gap that would have justified S5 on
freshness alone. Slicing's residual excursion is 3.6x the offload's peak, and
`mz_wallclock_lag_history` rounds lag up to whole seconds and reports the maximum over
its interval (`src/cluster-client/src/lib.rs:41-43`), so **a 55 ms baseline and a
365 ms excursion both surface as one second and the entire gap is below the resolution
of the metric we report.** Only the multi-second inline stall is visible at all. And
E13 ran on a 32-core box, so the offloaded thread never competed for a core, which is
the condition its own configuration documentation warns about.

Six entries carry the weight, and two of them correct earlier claims in this
document.

**S5 does not reach M2, and measurably makes it worse.** The worker loop is
`step_or_park`, then `handle_pending_commands`, then `process_peeks`
(`src/compute/src/server.rs:513-543`). An offloaded walk is *dispatched* inside
`process_peek` (`src/compute/src/compute_state.rs:1467`), reachable only from
`process_peeks`, and its result is *sent* by the worker when it polls the oneshot
(`:1600`), with the blocking task only firing an activator. So a peek arriving
while a long operator activation is in progress inside `step_or_park` cannot even
begin its offloaded walk until that activation finishes. Offloaded latency under M2
is the residual activation plus the walk plus one step, against inline's residual
plus walk. E12 registered that as a prediction before measuring and confirmed it:
p90 148.2 ms against inline's 129.5 ms, a 17% regression against 3% within-arm
variance across repeats. No substrate choice gets a peek past a long operator
activation, and the argument this document makes against S1 on M2 applies verbatim
to S5.

**S4 is not optional, and it is the only mechanism here that reaches M2.** With two
runtimes, `src/compute-client/src/multiplex.rs:357-359` routes *every* peek to the
interactive runtime unconditionally. So S4 is not a component that can be cut, it is
how the design already works, and it reaches M2 for the reason S5 does not: the
interactive worker's `step_or_park` is not running the maintenance operator. E12
measured it at p90 4.5 ms against inline's 129.5 ms, matching a control that runs
identical write traffic with the merging index on another cluster, which is the
floor achievable while writes happen at all. The step histogram shows why: the
maintenance runtime still recorded 206 steps above 128 ms and 25 above 512 ms while
the interactive runtime looked idle. The long work did not get cheaper, it moved off
the serving thread.

An earlier draft of this section proposed cutting S4 on the strength of E2. That was
wrong twice over, because E2's fixture is a point lookup behind concurrent scans,
which is M1, and at the time no experiment addressed M2 at all. E2 and E12 are not
in tension. Together they are the cleanest demonstration available that M1 and M2 are
distinct mechanisms with disjoint remedies.

**S1 against S5 on M1 is the peek program's choice, not this design's**, and it is
unsettled: S1's reach on M1 is predicted from its quantum and never measured, and what is
left uniquely to S5 is the swapped walk's own duration, which is the unattributed M11
effect. Both are tracked with the peek work.

**M4, M6 and M7 each have exactly one candidate row and no work behind it**, which is
better than the blank they had but is not an answer. M4 is the worst of the three,
because it is measured, it is unconditional, and it is the dominant term for the only
workload S3 is now justified by: about 900 ms of creation and teardown against the
roughly 1400 ms of tail that placement recovers.

**M5 is reached only by S0.** An untargeted peek is broadcast to every replica and
the first response wins, so peek latency is a minimum over replicas. Since the
timestamp comes from the oracle rather than from any replica's frontier, a replica
whose index frontier is current answers while a hydrating one is still catching up.
That makes an additional replica the incumbent answer these solutions have to beat,
and the only one that reaches M5. This document argues against a read replica on
memory cost and on introspection being replica-local, both of which hold, but that
is an argument about price rather than about reach.

**M5 is now measured, on one fixture.** E12 ran its inline and two-runtime arms at
both isolation levels. For single-runtime inline the level makes no difference at
all, 129.5 against 132.4 ms at p90, because M2 already dominates. With the peek moved
off the busy worker, strict serializable's tail returns: p99 185.8 ms against 5.8 ms,
and 4.5% of requests above ten times the idle median against 0%. So M5 costs real
latency, and it is only visible once M2 is removed. This is the mechanism that bounds
how much any peek-placement work can deliver at the default isolation level.

The other peek experiments still do not record their isolation level. The
parallel-benchmark scenario passes `strict_serializable=False` and E9 states its level
in prose, but **E1, E2, E8b and E11 do not**, and a strict serializable arm
pre-registered for E2 does not appear in E2's results. E9's staleness column puts a
second bound on the cost, 170 to 1589 ms of seal lag while hydrating.

### What each solution costs

| | Memory | CPU | Threads | Implementation | Non-isolation |
|---|---|---|---|---|---|
| S0 | a full second copy of the state | a full second copy of the maintenance work | a second process | none, it is the incumbent | introspection is replica-local, so it cannot be offloaded to the copy |
| S1 | accumulated rows are bounded, at the 10 KiB stash threshold when streamable and at twice `limit + offset` otherwise. What is unbounded is **pinned batches and delayed compaction**, since k parked scans hold k batch sets and k is not admission-controlled | one timely step per pass, so peeks take `Q/(Q+step)` of the worker and the dataflow *share* barely moves | none | self-contained | the quantum floor rises with dataflow and worker count, which bounds throughput overhead rather than victim delay |
| S2 | none | none | none | small, but an out-of-band flag is unsafe for dataflows because a `GlobalId` is reused, and safe for peeks because a uuid is not | on the offload path it cannot stop the walk, only the waiting |
| S3 | E6 measured *import* as nearly free, 4.5 MiB for 48 interactive dataflows over a 95 MiB index, cleanly and within one phase. The *publication* question is separate and inconclusive, because that comparison spanned builds. The doubled arrangement-size report is unresolved | 2N timely threads, fixed by the equal-peer requirement rather than tunable | 2N | the largest of these by an order of magnitude | shared fate, one memory limit for both runtimes, and M4 untouched |
| S4 | the registry peek path | none | none | already how the design works, not a separable component | its worker can still be busy with interactive rendering |
| S5 | bounded by the in-flight limit | needs a core, so on a saturated box it only reorders work | yes, and today they come from a pool shared with persist's blocking IO | needs `Send` batches, so it depends on the Arc-backed spines | past the in-flight limit it falls back to the non-preemptive walk |
| S6 | none | fragments downstream batches, which is differential's least efficient mode | none | one yield point per operator, with resumable state each time | coverage grows one operator at a time and never completes |
| S7 | an additional index | removes the work rather than relocating it, so it survives saturation | none | needs checking whether the fast path exploits the ordering | only reaches the one query shape |
| S8 | disk plus a bounded cache instead of resident memory | reads are re-entrant from any thread, so the worker leaves the read path | none in the worker | large, and multi-versioning is required, see [What a serving layer would need](#what-a-serving-layer-would-need) | a second copy of the data to keep current |

S1 and S5 fail in opposite directions and each is the other's fix. S1 has no
admission control, so parked scans pin batch sets and delay compaction without
bound. S5 has admission control and then falls off a cliff into the original
non-preemptive behavior for whichever peek arrives past the limit. That is the main
reason they are complements rather than alternatives, and it survives the
corrections above.

### What follows

Two conclusions bear on this design. The rest of the matrix belongs to the peek program and
is tracked in the project's peek issues.

* **S3 stands alone on M3, and its structural argument is stronger than its measurements.**
  Cooperative yielding needs a yield point retrofitted per operator. `linear_join_yielding`
  covers linear joins and `storage_source_decode_fuel` covers the persist decode, while
  nothing covers reduce, top-k, arrange, threshold or delta joins. Coverage grows one
  operator at a time and never completes, whereas a second runtime covers every operator at
  once. But **M4 is the dominant term for the workload S3 is justified by**, and nothing
  here addresses it.
* **S4 is not separable and should not be cut.** With two runtimes the multiplexer routes
  *every* peek to the interactive runtime unconditionally, so S4 is not a component that can
  be removed. It is also the only shipped mechanism that reaches M2, and it reaches it for
  the reason S5 does not: the interactive worker's `step_or_park` is not the one running the
  maintenance operator. E12 measured p90 4.5 ms against inline's 129.5 ms, matching a control
  that runs identical write traffic with the merging index on another cluster, which is the
  floor achievable while writes happen at all. The step histogram shows why: the maintenance
  runtime still recorded 206 steps above 128 ms and 25 above 512 ms while the interactive
  runtime looked idle. The long work did not get cheaper, it moved off the serving thread.

An earlier draft proposed cutting S4 on the strength of E2. That was wrong twice over,
because E2's fixture is a point lookup behind concurrent *peeks*, which is M1, and at the
time no experiment addressed M2 at all. E2 and E12 are not in tension. Together they are the
cleanest demonstration available that M1 and M2 are distinct mechanisms with disjoint
remedies.

**S0 is the baseline these have to beat, and the only one that reaches M5.** An untargeted
peek is broadcast to every replica and the first response wins, so peek latency is a minimum
over replicas, and because the timestamp comes from the oracle rather than from any replica's
frontier, a replica whose index frontier is current answers while a hydrating one is still
catching up. The argument against a read replica below is about price and about introspection
being replica-local, not about reach.

**M5 is measured on one fixture.** E12 ran its inline and two-runtime arms at both isolation
levels. For single-runtime inline the level makes no difference at all, 129.5 against
132.4 ms at p90, because M2 already dominates. With the peek moved off the busy worker,
strict serializable's tail returns: p99 185.8 ms against 5.8 ms, and 4.5% of requests above
ten times the idle median against none. So M5 costs real latency, and it is only visible
once M2 is removed. It is the mechanism that bounds how much any peek-placement work can
deliver at the default isolation level.

S1 and S5 fail in opposite directions and each is the other's fix. S1 has no admission
control, so parked scans pin batch sets and delay compaction without bound. S5 has admission
control and then falls off a cliff into the original non-preemptive behavior for whichever
peek arrives past the limit.

### What the matrix does not settle

The evidence behind every cell, the experiment definitions, and the full list of what remains
unmeasured live in the project document "Interactive read isolation: experimental
evaluation". Four gaps bear directly on this design.

* **Making the peek walk cooperative is unmeasured on peek-versus-peek queueing.** It is the
  only unmeasured candidate that could retire something already built, so nothing in the peek
  program should be ordered ahead of measuring it.
* **No arm ran on a CPU-saturated replica.** Every measured win here was taken with core
  headroom, which is the condition both mechanisms depend on, and the experiment for it was
  planned and never run.
* **M4 is measured, unconditional and unaddressed**, and it is the dominant term for the
  workload the second runtime is justified by: about 900 ms of creation and teardown against
  roughly 1400 ms of tail that placement recovers.
* **Two cheap code checks could each move a row.** Whether the reported freshness number
  aggregates across replicas as a minimum, which decides whether S0 reaches M8 at all. And
  whether the fast path exploits an index on the key together with the ordering column, which
  would make the skewed-lookup fixture a plan defect rather than a scheduling one.
## Why this architecture

This is a deliberate architectural commitment, not an isolated feature. It is
close to a one-way door (see [The commitment](#the-commitment)), so the rationale
matters as much as the mechanism.

The problem it addresses is M3 in
[Problems and mechanisms](#problems-and-mechanisms), and routing peeks to it is the
only shipped mechanism that reaches M2. The peek mechanisms in this document
address M1. Nothing here addresses M4, M5, M6 or M7.

### The thesis is separation of concerns

Two runtimes do not add CPU and do not magically isolate reads. Both runtimes
share the same cores. The second runtime buys one precise thing, a separate,
OS-preemptible run loop, so an interactive read is not trapped behind a
run-to-completion maintenance step.

The right frame is separation of concerns:

* The maintenance runtime stays a pure run-to-completion batch engine. Operators
  consume their inputs fully. The only sanctioned yield is before an exchange
  edge, to let downstream operators reduce memory. Yielding for interactivity is
  an anti-pattern we do not want in that runtime.
* The interactive runtime is a pure, preemptible, low-latency reader over the
  maintained arrangements.

A single runtime cannot be both without compromising one of them. Two runtimes
let each be pure.

### Peek placement is a separate axis

The original thesis was that a second runtime delivers read isolation, and that peeks were
one of the reads it would isolate. Measurement separated two mechanisms that had been
conceived as one, and only one of them needs a second runtime.

| | Peek offloading | Dataflow offloading |
|---|---|---|
| What it changes | which thread walks a fast-path index peek | which runtime renders a dataflow |
| Unit of work | one peek's cursor walk | a whole temporary dataflow |
| Fixes | head-of-line blocking between peeks on one worker | interference between maintenance work and interactive rendering |
| Needs the second runtime | no | yes, it *is* the second runtime |
| Deployment cost | a dyncfg, no restart | a port, a fleet roll, doubled timely worker threads, the sharing registry, and the protocol invariant that goes with them |

No measured scenario is moved by both, and one is moved in *opposite* directions: a point
lookup on a replica taking 500,000-row insert batches reaches p90 4.5 ms on the second
runtime, and 148.2 ms on the offload against inline's 129.5 ms. So the split is not "peeks
against dataflows". It is which queue the work is stuck in.

The two properties behind the split are worth naming, because a third remedy follows from
them. *Preemptibility* is whether a short request can displace a long one already running.
In a non-preemptive work-conserving server a short request waits for the residual of the job
in progress, and a timely worker's per-activation service time spans six orders of magnitude
from a point lookup to a full arrangement scan, so that residual rather than the load level
sets the shape of the waiting-time tail. E11's timeline is the signature: lookups arriving
behind a skewed one complete at a fixed *instant* rather than after a fixed delay.
*Capacity* is whether a core is free to run on, and neither mechanism supplies it.

Two remedies exist for a large residual, not one: preempt the long job, or dispatch on size
so it never lands in front of a short one. Peek offloading does neither. It adds a *server*.
The walk never yields, it runs on a different OS thread, so the interleaving is delegated to
the kernel and how well that works depends on runnable threads against available cores. That
is why its measured wins arrive on a replica with CPU headroom, and why its behaviour on a
saturated box is a separate and unmeasured question.

**Peek placement is therefore orthogonal to this design and is not settled here.** It was
settled by the peek execution work, which shipped independently: the walk runs in budgeted
slices on the worker (`INDEX_PEEK_INLINE_BUDGET`, `INDEX_PEEK_ACTIVATION_BUDGET`) and is
offloaded to a blocking task past its budget (`ENABLE_INDEX_PEEK_OFFLOAD`). The interactive
runtime inherits that path unchanged, see
[The interactive serving path](#the-interactive-serving-path). Note that chunking a peek
walk is not the same move as
[yielding in maintenance](#why-not-yield-for-interactivity-in-one-runtime), which is ruled
out on different grounds: a peek walk consumes no input and consolidates nothing, so
run-to-completion is not part of its contract.

What the second runtime is left with is what it should be judged on: **temporary dataflows
and observability, not peek latency.** Any review weighing the sharing registry and its
protocol against a peek-latency claim is weighing them against the wrong benefit. The claim
to defend is that a replica stays useful, and stays *introspectable*, while it is busy
maintaining collections. The observability case is the strongest form of it: a replica that
cannot answer introspection while something is wrong with it cannot be diagnosed while
something is wrong with it, and the alternative to an answer is not a slower answer but a
misleading one.

One asymmetry under saturation. Peek offloading moves a walk to a thread that still needs a
core, so on a CPU-bound box it only reorders work, and its best case is a walk that is
*blocked* rather than computing, which is why the swap result is its largest margin. The
second runtime doubles timely worker threads at every replica size, so it is not free even
when idle, and that doubling is fixed by the equal-peer requirement rather than chosen, so
it is not a knob to turn down.
### Why not a read replica

Introspection cannot be offloaded. A replica's introspection describes that
replica, so a second replica cannot answer the first replica's introspection
reads. Only an in-process second runtime can keep introspection answerable while
maintenance is busy.

Separately, replicas in the fleet mostly redline on memory, not CPU. A second
replica doubles the binding resource, because it maintains its own copy of every
arrangement. The sharing approach here duplicates no arrangement memory. And
because those replicas are memory-bound, they usually have spare CPU, which is
exactly the headroom the interactive runtime needs. The CPU-saturated case,
where a second runtime helps least, is not the common one.

### Why not yield for interactivity in one runtime

Making the maintenance runtime yield finely so reads interleave would violate its
core contract. Run-to-completion is what lets an operator consume its inputs and
consolidate. The only yield we want in maintenance is the pre-exchange,
memory-reduction yield. Yielding for interactivity would degrade the maintenance
runtime to buy latency it should not be responsible for.

### What it does not buy

It does not add CPU. On a CPU-saturated box the interactive runtime cannot get a
core either, and reads back up just as they would in a single runtime. The
benefit is real but conditional on CPU headroom. A benchmark that pins every
core with synthetic churn models a CPU-bound box and understates the feature,
because the fleet is memory-bound with spare CPU.

**It does not improve peek latency when the peek is queued behind another peek.**
That was the original expectation and it did not survive measurement: the walk
substrate does that. It *does* improve peek latency when the peek is queued behind a
long operator activation, where the substrate cannot help and makes matters slightly
worse, measured in E12 as p90 129.5 to 4.5 ms. See
[Peek placement is a separate axis](#peek-placement-is-a-separate-axis).

It also does not touch the control plane. Peeks still serialize behind DDL on the
single coordinator thread. For non-introspection reads under load that control
plane can be the first-order bottleneck, and this work is necessary but not
sufficient there. See [Known limitations](#known-limitations-and-follow-ups).

Most importantly, it does not remove the dependency on maintenance sealing the
read timestamp. An interactive peek at `T` waits for the published arrangement's
`upper` to pass `T`, and that `upper` is the maintenance stream frontier, which
advances only when the maintenance worker steps. So the win is scoped by
isolation level:

* Stale and serializable reads take a timestamp at or below an already-sealed
  frontier. Full win.
* Strict serializable reads, the default isolation level, take their timestamp at
  the write frontier, so the peek waits for maintenance to seal it either way.
  Close to no win.

The flagship introspection-during-hydration case falls under the same rule. The
logging dataflows sit on the same stalled maintenance workers, so their frontiers
stall too, and "introspection stays answerable" means answerable with stale data.
That is the useful property during an incident, but it is a staleness claim, not a
freshness one.

### CPU is shareable, memory is not

Colocation can only ever be a CPU story, and that bounds what this architecture
can be asked to deliver.

CPU is preemptible, so a latency-sensitive thread can be made to win against a
batch thread by scheduling alone, at no cost when nothing contends. An allocation
cannot be preempted, there is no fair share for resident memory, and the kernel's
remedy is to kill the process. E10 measured hydration memory as a sawtooth
overshooting its steady state by about 3.9x at container level, and the first swap
fixture lost a replica while carrying exactly that transient, though every
termination reported `Error` rather than `OOMKilled` and the container logs were not
retained, so that cause is consistent with the evidence rather than established by
it. A colocated interactive path dies with the process whatever killed it, and the
shared-fate panic makes that structural rather than incidental.

So the useful split is by resource rather than by workload.

* Colocate for CPU. Latency isolation inside one replica is a preemption problem,
  it is solvable in process, and it duplicates no state.
* Separate processes for memory and availability. Anything carrying an
  availability target needs its own memory limit, because no amount of scheduling
  work substitutes for one.

This also sharpens what a serving replica would have to be. Routing peeks to a
second ordinary replica does isolate memory, and it pays a full second copy of the
state and of the maintenance CPU to do it, which is what an ordinary replica
already costs. The only version of the idea that is more than a routing policy is
one that holds the data in a cheaper form than an arrangement. See
[What a serving layer would need](#what-a-serving-layer-would-need).

### What the platform actually isolates

Replicas declare CPU requests and no CPU limit, and the scheduler admits pods to a
node only while the sum of their requests fits allocatable CPU. Memory limits are
omitted, which is what makes swap available. Many pods share a node. Every
isolation property below follows from that shape, and the shape is deliberate. It
is recorded here because the queue this work fixes is the innermost of three, and
the outer two are not ours.

Declaring a request without a limit is Burstable rather than BestEffort in
Kubernetes' own terms, and the distinction matters because the two classes differ
on exactly the properties at issue. It also explains the swap grant independently:
the kubelet's limited swap behavior gives swap only to Burstable pods, sized in
proportion to the memory request, and gives Guaranteed and BestEffort pods none.

Kubernetes isolates two resources, and only when asked for them. Memory capacity,
through `memory.max`, enforced by the kernel killing the container. And CPU share,
through a `cpu.weight` derived from the CPU request. Exclusive cores are a third,
available only to Guaranteed pods with integer CPU requests under the static CPU
manager policy.

What that leaves:

* **CPU: a real floor at the request, and nothing above it.** The request sets
  `cpu.weight`, and because admission keeps the sum of requests inside allocatable
  CPU, every pod on the node can hold its request simultaneously even under full
  contention. The floor is therefore the replica's nominal size rather than a
  fraction of it. What is opportunistic is everything *above* the request, which is
  the headroom both mechanisms here spend. Omitting the limit also means no
  `cpu.max` quota, so the pod escapes the quota-throttling tail latency a CPU limit
  imposes, which is a benefit of this shape and not only a cost. One property is
  worth stating because it is counterintuitive: CPU accounting is hierarchical, so
  adding threads inside the pod does not increase the pod's share. More threads buy
  parallelism within our slice and queue depth for I/O, never more CPU. That is
  precisely why threads can help a swap-bound walk, whose threads are blocked
  rather than computing, and cannot help a CPU-bound one.
* **Memory: no reservation.** Global reclaim is node-wide LRU rather than
  per-cgroup fair, so a neighbor's allocation can swap out our arrangement, which
  means our swap depth is not purely a function of our own behavior. Reclaim
  protection would come from `memory.min`, which the kubelet derives from the
  memory request only under the memory QoS feature gate, so whether we have any is
  a cluster configuration question rather than a property of the class. Eviction
  and out-of-memory ranking are better than BestEffort without being good. The
  kubelet ranks Burstable pods by usage above their request, and a heavily swapping
  replica is above it, while the kernel's `oom_score_adj` is computed from the
  memory request rather than pinned at the maximum.
* **Swap device bandwidth: nothing, and this is the weakest link.** There is no
  per-pod disk throughput API. `ephemeral-storage` bounds capacity rather than
  IOPS, and the cgroup `io` controller that could throttle a pod is not configured
  by the kubelet. Even configured it would be unreliable here, because swap-out is
  driven by kswapd or by direct reclaim rather than by the pod whose growth caused
  it. So the swap device is shared, unmanaged and unbounded. A neighbor thrashing it
  adds queueing delay to every one of our swap-in faults, and that delay is
  invisible in our own counters: the fault count is unchanged and only the service
  time per fault grows.
* **Network: nothing.** There is no in-tree bandwidth request or limit. The
  annotations that exist are implemented by some network plugins and are part of
  neither the resource model nor scheduling. Persist fetch throughput during
  hydration is therefore not isolated either, which matters wherever hydration is
  fetch-clocked rather than CPU-clocked.

Two consequences for this work.

The absent CPU quota means `num_cpus::get()` finds no quota to read and falls back
to the node's CPU count, so `clusterd` sizes its tokio worker pool to the *node*
rather than to the replica. On a large node that is dozens of worker threads for a
small replica, on top of two runtimes' timely workers and tokio's 512-thread
blocking pool default. Nothing accounts for this, and it argues for giving interactive work
a bounded pool of its own rather than sharing tokio's.

It also refines the section above rather than contradicting it. CPU is shareable
and the request makes that floor real, so colocating for CPU is sound. What is not
guaranteed is the headroom above the request, and both mechanisms spend headroom:
an offloaded walk still needs a core, and a second runtime needs cores for a second
set of workers. So the benefit is largest when the node is quiet and smallest when
it is not, which is the conditionality already recorded in
[What it does not buy](#what-it-does-not-buy). The resources with no bound at all
are swap bandwidth and network, and those are the ones the swap strategy makes
critical. A serving tier carrying a latency or availability target still needs its
own pod, and the reason is memory, swap I/O and shared fate rather than CPU share.

One thing to establish rather than assume: whether a memory request is declared
alongside the CPU request. If it is, swap is bounded per pod at roughly the memory
request over node capacity times the size of the swap device, and the class is
Burstable on both axes. If it is not, the kubelet's limited swap behavior would
grant this pod no swap at all, so the grant would be coming from elsewhere and
`memory.swap.max` would be unbounded, in which case one replica can consume the
node's whole swap device and starve every other pod's swap. Either way the
userspace limiter in `src/compute/src/memory_limiter.rs` bounds our own
consumption, because it counts physical memory plus swap rather than physical
memory alone.

### The commitment

Two properties make this close to irreversible.

* The off switch is not a clean exit. `Solo` keeps single-runtime deployments on
  the same code paths, but once users depend on the isolated low-latency reads,
  turning the feature off is a visible query-latency regression, not a no-op.
  NOTE: this argument is weaker than it was written to be. For *peeks* the off
  switch is now clean, because peek offloading carries that benefit and survives
  independently. What does not survive is temporary-dataflow isolation and
  introspection under load.
* The capability we lean on will atrophy. Once reads live in the interactive
  runtime, the maintenance runtime no longer needs to accommodate interactivity
  at all, and it will be built to be maximally batchy because it was freed to be.
  Single-runtime interactive-read behavior rots from disuse and hardened
  assumptions. Recovering it later is a rebuild, not a revert.

That irreversibility is acceptable only because the end-state, maintenance as a
pure batch runtime and interactive as a pure reader, is the architecture we would
choose deliberately given the points above. The bar for adopting this is
therefore "we would design it this way on purpose," not "we can back out."

## Design principles

Three principles govern the protocol between the runtimes.

1. **Build on a correct protocol, and panic outside it.** Compute trusts the
   controller's read-hold discipline. A maintained arrangement is dropped only
   after every reader has completed, so an import never outlives the arrangement
   it reads. There is no cross-runtime lease and no refcount. A panic on any
   worker or reader thread of either runtime takes down the whole process, which
   is correct: the two runtimes share fate, and there is nothing to isolate.
2. **The multiplexer splits one endpoint into two well-defined sub-protocols.**
   The maintenance sub-protocol is the ordinary compute protocol. The interactive
   sub-protocol is a variant whose index imports are shared registry imports, a
   self-describing import kind that references a maintained id without a prior
   local `CreateDataflow`.
3. **Deterministic construction.** Timely allocates exchange-channel identifiers
   from a per-worker, construction-order counter, so every worker must build
   dataflows in the same order. We render in command arrival order and never
   reorder or defer a build. An import that depends on a not-yet-published
   arrangement binds a real but empty publication point at construction time and
   is filled in place later.

A fourth, structural fact underlies the whole design: **sharing is per-process.**
The shared batches are `Arc`-backed in memory, so the interactive runtime reads
only the maintenance arrangements published in that same process.

## Protocol invariants

The design principle above says compute builds on a correct protocol and panics outside it.
That is only meaningful if the protocol's invariants are written down, because the runtime split
silently invalidates one of the invariants the single-runtime protocol relied on.

### The invariant compute relies on

**I1.** For every dataflow `D` created at `as_of X` importing index `I`, the replica's trace for `I`
has `since <= X` from the moment `D` is created until `D` is dropped.

Single-runtime, I1 holds for two independent reasons.

* **I1a, the controller.** The controller holds a read hold on `I` at `X` for `D`'s lifetime, so it
  never sends an `AllowCompaction` for `I` past `X` while `D` lives.
* **I1b, ordering.** `CreateDataflow(D, X)` and any later `AllowCompaction(I, F)` arrive on one
  ordered command stream, so the replica renders `D` before it can compact `I`.

### What the runtime split breaks

I1a survives. I1b does not.

A `CreateDataflow` for an interactive dataflow is routed only to the interactive runtime, while
`AllowCompaction` is a lifecycle command routed to both. The two runtimes have independent command
streams and no cross-runtime ordering, so maintenance can apply a compaction for `I` at a point in
its stream that has no defined relationship to where interactive is in its stream. I1a does not
rescue this: a read hold is a promise about what the controller *sends*, and the replica-side
realization of that promise now happens on a different runtime, arbitrarily later.

The failure is loud, and should stay loud. An interactive import asserts `since <= as_of` before
building, mirroring the maintenance path. A violation is a protocol-ordering failure, not a read
that cannot be served, so turning it into a user-visible error would hide a broken invariant behind
a degraded query.

### The general form

**I2.** Any resource whose lifetime is governed by commands delivered to one runtime, but consumed
by the other, needs its lifetime bound made visible on the *governing* runtime's stream, at a point
ordered before the command that would violate it.

Three known symptoms are the same missing invariant, not three separate problems.

| Symptom | Resource | Lifetime governed by | Consumed by |
|---|---|---|---|
| An imported index compacts past a dataflow's `as_of` | the index's `since` | maintenance, through `AllowCompaction` | an interactive import |
| Reconciliation drops and recreates an index under the same `GlobalId` | slot identity | maintenance, through drop and re-render | an interactive import |
| A never-adopted placeholder is evicted | slot existence | whoever evicts | an interactive import |

### The fix: broadcast compaction and a standing hold

Reconstructing I1b is the wrong move. I1b was lost because of a routing decision, not
because two runtimes cannot have it. `CreateDataflow` goes only to the rendering runtime and
`AllowCompaction` only to the owning one, so the two commands that must be ordered are
placed on different streams. Send compaction to both runtimes and each runtime has them on
one ordered stream again, which is I1b restored rather than simulated.

Two changes.

**Broadcast compaction.** The multiplexer forwards `AllowCompaction` to both runtimes
instead of routing it to the owning one. It keeps routing `CreateDataflow` by ownership.

**A standing hold per shared collection.** The rendering runtime holds every shared
collection it may import at the last compaction frontier *it* has applied. The publisher
bounds the owning runtime's compaction by that hold, alongside the reader registrations it
already meets.

> **I1c.** A shared arrangement compacts only as fast as the slowest runtime's stream
> position.

That is what makes an individual read correct, and it is derived from the importing runtime
rather than from the controller's per-dataflow bookkeeping, so the direct dependency is the
mechanism.

**Broadcast alone is not enough**, and the counterexample is what decided the design.
Restoring the ordering *within* each stream does not restore it *between* them, because the
runtimes still drain at their own rates. The controller creates a dataflow at `as_of = 0`,
drops it at once as a cancelled peek does, releasing its own read hold, and then allows
compaction to 1. The owning runtime applies that and publishes it while the create is still
queued on the rendering runtime. When the rendering runtime reaches the create, it builds a
dataflow at `as_of = 0` over a collection compacted to 1. The controller did nothing wrong:
from its point of view the dataflow is gone.

The standing hold closes exactly that window. The bound is the rendering runtime's applied
frontier, still 0, and it advances only when that runtime applies the broadcast compaction,
which is queued *behind* the create.

**The standing hold costs nothing.** It pins each collection at the controller's own
compaction frontier, which the controller already guarantees is readable, and which the
publisher's writer-driven fallback already targets when no reader is registered. So it adds
no pin that was not there. What it removes is the publisher's freedom to run ahead of the
rendering runtime.

**What it needs from the runtimes.** The rendering runtime must accept `AllowCompaction` for
collections it does not host and treat it as advancing the standing hold rather than as
compacting a local trace. It must keep not reporting frontiers for shared collections, which
`ComputeState::report_frontiers` already handles through its transient-only filter. And a
collection's standing hold is installed when the collection first appears and removed when
its compaction reaches the empty frontier, so it needs no per-dataflow identity.

#### As implemented

The standing hold is one frontier per publication point
(`SharedTraceState::standing_hold`), joined so it only rises, and the publisher's logical
target is met against it. The published `since` is then bounded by it without further work,
because `since` derives from the publisher's own agent hold and that hold is the join of
targets it has already forwarded. A `debug_assert` in the publisher states that, so an edit
letting the target escape the bound fails there rather than admitting a reader below what
the trace holds.

Two things were not obvious from the design.

**The hold must be seeded at the adoption floor, not at the minimum time.** An arrangement
whose importing runtime has not yet applied any compaction for it would otherwise be pinned
at the minimum time for as long as that lasts, which for a collection the controller never
compacts again is forever. The publisher's own compaction frontier at adoption is the right
seed: the controller does not offer an `as_of` below a collection's `since`, so no importer
can need a frontier below it. That also makes the no-broadcast-yet behaviour identical to
the writer-driven fallback.

**The rendering runtime tells the peer's publications apart by whether it hosts the
collection.** It installs no logging dataflow and renders only the transient dataflows the
multiplexer routes to it, so "do I have a collection for this id" answers no exactly for
the ids whose publication is the peer's. For those it records the standing hold and does
nothing else: it must not apply the broadcast frontier as the writer-driven floor, which is
the peer's to drive, and it must not run `drop_collection` for a broadcast drop, which
would remove the peer's slot from the registry.

#### The price: compaction is coupled to the rendering runtime's drain rate

A stalled rendering runtime stalls compaction on every shared arrangement. That is the price
of I1c and it is the intended behaviour, but it is new: before the split a slow reader could
not hold maintenance back. It wants a metric on the gap between the two runtimes' applied
frontiers, so the coupling is observable before it becomes a memory incident.

#### Rejected alternatives

Every earlier mechanism reconstructed I1b instead of restoring it, and all are deleted.

* **Cap the frontier at the multiplexer.** Record a hold per interactive dataflow at its
  `as_of`, cap any later `AllowCompaction` at the lowest such hold, and forward the deferred
  frontier when the dataflow drops. Under-compacting is always safe, so the capped value
  needed no agreement from the controller. It failed at its *retirement* point rather than at
  the cap: `send` is an unbounded push with no ack, so maintenance could be told to compact
  before interactive dequeued the create the hold was taken for. Capping is safe without an
  ack because the frontier is withheld entirely. Releasing is not.
* **Synthesize an `AcquireHolds` command on the owning runtime's stream**, with a matching
  release. The release can overtake a create the rendering runtime has not processed, which
  is why the release had to travel on the rendering runtime's stream, and that asymmetry is
  what made the mechanism hard to reason about.
* **An in-process sequence barrier**, where interactive publishes the command sequence
  number it has processed and maintenance defers publishing a compaction until interactive
  passes it. It needs no protocol change and fails on two counts. It couples maintenance's
  compaction to interactive's progress, which the non-goals refuse. And a replica can span
  processes, so an in-process barrier cannot observe the runtimes in the other processes at
  all, which makes it insufficient rather than merely undesirable.

The deletion removed `ComputeCommand::AcquireHolds` and `ReleaseHolds`,
`compute_state/command_hold.rs`, the registry's holder-reclaim path, the multiplexer's
`held_exports`, `hold_floor`, `deferred_compaction` and `compaction_floor`, and
`SharedTraceState::writer_since` with its derived `refresh_since`. It also dissolved the
epoch-boundary question rather than solving it: a standing hold is per collection and
carries no dataflow identity, so a replayed dataflow receiving a fresh transient id cannot
conflate two holders, and the replica clears nothing at a reconnection.
## The bounded-read boundary

The interactive runtime serves a read only when the dataflow is **wholly transient**
*and* its `until` is a finite non-empty frontier *and* it carries no `SUBSCRIBE` sink
*and* it carries no `COPY TO` sink. Everything else runs on the maintenance runtime.

Transience is not redundant with `until`-finiteness. A durable dataflow can also carry a
finite `until`, since a `REFRESH AT` materialized view sets one, and interactive's frontier
reports are forwarded only for transient ids. Routing such a dataflow to interactive would
get its frontier reports dropped, so it stays on maintenance regardless of `until`.

Of the two sink exclusions, only `COPY TO`'s has a recorded reason, its S3 sink being
refused by reconciliation. The subscribe clause turns out to bite only for
`SUBSCRIBE ... UP TO`, because an ordinary subscribe's `until` is already empty. A
mixed or non-homogeneous dataflow is treated as maintained by construction, which is
the safe default.

## The sharing primitive

The cross-runtime sharing primitive lives entirely in Materialize. It builds
against a released differential-dataflow, with no fork and no `[patch.crates-io]`.

* `mz_row_spine::ArcBatch` is a local newtype around `Arc<B>` that carries the
  differential batch traits, so a batch whose contents are `Send + Sync` can be
  read from a thread other than the one maintaining the trace. The orphan rule
  forbids the blanket `impl Trait for Arc<B>` in Materialize, which is why the
  newtype exists rather than a bare `Arc<B>`.
* `mz_compute::shared_trace` holds the primitive proper: `Published`,
  `SharedTraceHandle`, `SharedTrace`, the `PublishArrangement` extension trait,
  and `import_snapshot_at`.

An earlier prototype consumed these from a differential-dataflow fork. The only
capability that kept the fork alive was reading `agent.trace_box_unstable()`, an
upstream API documented as unstable and undefined behavior to mutate, to compute
a compaction floor. Materialize already has authoritative sources for that
information, so the read was replaced (see [Compaction](#compaction)) and the fork
was dropped.

### Placeholder and adopt

A publication point is an `Arc<SharedTrace>`. It can be created empty, as an unbacked
placeholder, and later adopted by a publisher in place, filling the same `Arc`.
This is what makes arrival-order construction work. A differential import captures
its input trace by value at construction time, so the import must have a real
trace to hold even before the arrangement it reads exists. `Published::new`
gives it one. A later `PublishArrangement::adopt` installs the real publisher into
that same point, and the by-value handle observes the fill because it is a live
proxy into the shared state, not a snapshot. `Published::handle_at` checks the
published `since` against the reader's `as_of` and registers the reader's hold under
one acquisition of the state lock, so the publisher cannot advance `since` between
the check and the registration.

The `TraceAgent` that writes the arrangement lives in the publisher's sink
closure, not in `SharedTrace`, so the writer is decoupled from the shared state.

Placeholder frontiers are `Antichain::from_elem(Timestamp::minimum())`, never
`Antichain::new()`. The empty antichain reads as sealed through the end of time,
which would make every snapshot wait vacuously true and return empty results.

### Single-sourced replay feed

The publisher replays batches and frontiers to importers from a single
authoritative source. The hazard it avoids: the trace's `map_batches` upper can
run ahead of the arrangement stream within a worker step, so splicing batches
from the stream with frontiers from the trace can enqueue a `Frontier(upper)`
ahead of a `Batch` whose time is below it, which makes the importer's delayed
capability panic. Feeding a batch and the frontier that closes it from one source
keeps them mutually ordered. For the same reason the replay is incremental rather
than a one-shot dump of the whole chain under a single capability, which would be
the record-doubling bug.

### Compaction

Publishing carries no independent compaction floor. In Materialize the controller
drives `since` through the maintained trace's own handle. Only a live importer's
registered hold may hold the shared view back, and it releases on drop. The
publisher keeps a holding agent solely so importer holds have somewhere to forward
to, so that hold must follow the writer rather than pin the trace.

The publisher takes its writer-driven floors from sources Materialize already has:

* Logical compaction comes from the controller's `AllowCompaction` frontier,
  forwarded into the published slot by
  `ArrangementSharingRegistry::note_allow_compaction`, which
  `compute_state::handle_allow_compaction` calls alongside the local
  `TraceManager` update. `SharedTraceState.writer_logical` holds it, seeded
  `None` so the publisher falls back to its own current hold, the dataflow
  `as_of`, before the first command arrives.
* Physical compaction follows the chain's *coverage*, the upper of the last
  published batch, mirroring `TraceManager::maintenance`, which sets physical
  compaction to the trace upper to enable batch merging.

With no reader hold on a dimension, the target follows that writer floor, so with
zero readers compaction follows the writer. The published `since` is the meet of
the publisher's post-forward hold and the writer floor, which keeps a registering
reader from latching an anti-conservative `since` that claims accuracy at
already-merged times. An index publishes two independent arrangements, so
readiness and `since` gating operate on `meet(oks, errs)`.

A reader's handle mirrors `TraceAgent` rather than reimplementing its policy, because
the two axes carry different frontiers and `since` is never the right physical one. A
handle registers its logical hold at its `as_of` and its physical hold at the chain
coverage, not at `since` and not at `as_of`: an import is seeded with the whole chain
and wrapped in `TraceFrontier`, which advances times rather than cutting, so cuts only
ever happen at or above the coverage. Both setters join rather than assign and report
the join, so a consumer can never lower its own hold below a frontier the trace was
already told it could compact past, and `get_physical_compaction` reports exactly the
frontier the trace honours, which is what the join operator's own assertion checks
against `map_batches`.

### The import follows the dataflow's until

`SharedTraceHandle::import_snapshot_at(scope, name, as_of, until)` bounds the import
by `until`, and `shared_trace.rs` states the contract directly: "For a single-time
interactive read pass `until = as_of.step_forward()` ... An empty `until` performs no
bounding and the import stays live with the trace." The bound is checked as
`frontier.is_empty() || until.less_equal(&frontier)`, and an empty antichain is
`less_equal` to nothing but the empty frontier, so an empty `until` never fires the
bound except on the publisher's terminal signal. The signature is the analogue of the
maintenance path's `TraceAgent::import_frontier_core(outer, name, as_of, until)`, into
which maintained dataflows pass an empty `until` routinely.

The feed is live already. `adopt_named` is a sink on the arrangement stream that on every
activation pushes arrived batches to every registered queue, pushes a `Frontier` instruction
when `upper` advances, and activates every importer.

**What makes interactive imports bounded is the routing predicate, not the import.**
`import_index_shared` in `render.rs` passes the dataflow's own `until`, as the
maintenance import does. The multiplexer routes only single-time dataflows to the
interactive runtime, so that `until` is always one step past `as_of` and every import
completes once the trace seals past it. A dataflow with an empty `until` would follow the
trace for as long as it lived, and nothing at the import stops it.

The reason that matters is that it places the obstacle. Following imports are not the
hard part. See
[Compaction feedback flows through the reader's handle](#compaction-feedback-flows-through-the-readers-handle).

Import is pairwise: importer worker `i` reads publisher worker `i`. That is sound
only when both sides shard keys the same way, `key.hashed() % peers`, with equal
total peers, so `import_snapshot_at` asserts equal peers and panics otherwise. It
also asserts `since <= as_of`: a published slot whose `since` already sits above
the requested `as_of` means the controller offered an unreadable `as_of`, a
protocol error, and the import must panic rather than silently read coalesced
data.

### Compaction feedback flows through the reader's handle

An arranged collection is two things: a stream of batches, and a handle to the trace. A
long-running dataflow generally needs both. A join looks up each side's incoming batches
against the *other* side's trace, so it holds a trace handle for its whole life and cannot
release it. Such an importer must instead **downgrade** logical and physical compaction as
its own frontier advances, or the publisher can never merge or truncate in the background.
So compaction information has to flow backwards, from the importing runtime to the
publishing one.

That backward channel exists, and it is shared memory rather than protocol, which is why it
needs no new command.

* `SharedTraceHandle` implements `TraceReader`, and its `set_logical_compaction` and
  `set_physical_compaction` mirror the handle's frontier into
  `SharedTraceState::logical_holds[id]` and `physical_holds[id]`.
* On every activation the publisher computes the meet of those holds against its
  writer-driven floor and the standing hold, and forwards the result to its own
  `TraceAgent`.
* Handles handed downstream behave normally. Differential's join and reduce downgrade their
  trace handles as their frontiers advance, and `TraceFrontier` forwards the downgrades
  through, so a well-behaved importer already causes the maintenance trace to compact.

**One frozen hold would defeat it**, because the target is a **meet** across all holds: a
single hold pinned at `as_of` dominates it no matter how far every other hold advances.
So the import keeps no separate hold. The read hold is each returned `Arranged`'s own
trace handle, registered at `as_of`, and the dataflow token retains only the registry
slot's `Arc`, whose strong count is the registry's measure of a live reader. A consumer
that keeps the trace downgrades that handle as its frontier advances, and the publisher
compacts behind it. For a single-time read the distinction is unobservable. For a
long-lived importer it is what lets the imported index's `since` advance for the
importer's whole life, so it is in place before any such importer exists rather than
retrofitted for one.

Separately, and not a compaction problem: the import queue is unbounded with no
backpressure, deliberately, so that maintenance progress is never coupled to a slow reader.
A long-lived importer that lags holds a second copy of every undrained batch for as long as
it lags.
## The registry

`ArrangementSharingRegistry` (`src/compute/src/sharing.rs`) maps a `GlobalId` to a
per-worker slot holding the published `oks` and `errs` points.

* **Get-or-create is symmetric.** Whichever runtime touches an id first creates
  its publication point. Maintenance-first creates the point and adopts it.
  Interactive-first creates a placeholder and builds a live import over it, which
  a later maintenance adopt fills in place rather than overwriting.
* **Notification-driven, no polling.** Each interactive worker registers a
  coalescing `SyncActivator` and a dirty-id inbox. A read whose dependency is not
  yet published or sealed is enqueued, not blocked. Publication (`insert`) and
  seal (`note_frontier`, fired from the maintenance export's frontier probe on
  both the `oks` and `errs` streams) mark the id dirty and wake the worker, which
  re-examines only the affected pending work. The `map` and `wakers` locks are
  independent, and the lost-wakeup argument that lets them stay separate is a
  map-lock total order plus drain-before-reread plus a sticky activation token.
  A shared peek that cannot yet be answered leaves the sweep's queue and parks in
  `pending_work` keyed by its target id, so only a dirty mark for that id re-examines
  it.
* **One close, no withdrawal command.** An adopted point closes when its publisher
  drops, so no explicit withdrawal command is needed. A placeholder that is never
  adopted, because the index creation it anticipated was cancelled, leaves an empty
  slot in the registry for the life of the process. Correctness does not depend on
  reclaiming it: whether an imported index may compact or drop rests on the
  controller's read-hold discipline, and the leaked slot is an empty publication
  point, not a retained arrangement. Reclaiming it would need a reader-teardown
  hygiene path that does not exist.
* **Re-exports are aliases.** An index whose dataflow imports another index on the
  same relation and key re-exports the imported arrangement rather than arranging
  again, and on a publishing runtime `publish_alias` registers the new id as a second
  name for the target's slot. Readers of either id share one publication point, and
  the re-export dataflow needs no operators of its own, which is what keeps
  `mz_compute_error_counts` attributing the target's errors to it, and what keeps the
  resident cost of a re-export near zero. Before aliasing, when every re-export
  published through its own import of the shared traces, that cost measured about
  150 KiB per re-export. `publish_alias` refuses when a reader already created a slot
  for the alias id, because that slot is the point the reader imported and only a
  publisher writing into it can back it. The caller then falls back to importing the
  shared traces and publishing them under the alias id, which is the one case where a
  re-export dataflow has operators. While the target lives, its frontiers govern the
  shared point: the alias dataflow imports the target, so the controller never
  advances the target's `since` past an alias's. Once the target drops, the point
  stays reachable under the alias ids and its frontiers move to the meet of what the
  aliases have noted. A seal on the target notifies its aliases too. On the interactive
  runtime an export whose arrangement is an imported shared arrangement aliases the
  same way, and since it has no trace of its own, `report_frontiers` reads its frontier
  through the registry. The registry's state sits behind one lock.

## The interactive serving path

The interactive runtime serves everything through the registry.

* **Fast-path index peeks** read the published arrangement through the same `IndexPeek`
  path the maintenance runtime uses for its local traces. `IndexTraces` names the
  provenance, `Local` for a pinned `TraceBundle` and `Shared` for handles minted from the
  registry on each attempt, and everything past that point, the budgeted walk, the
  offload past the budget, the stash and the result limits, is one code path. The only
  difference is where an unanswerable peek waits: a local peek stays in the sweep's
  queue, a shared one parks in `pending_work` until its target's dirty mark. Once reads
  live in a separate runtime, the runtime itself is the isolation mechanism, and the walk
  substrate stays an independent choice on either runtime, which is the orthogonal axis
  above.
* **Slow-path query dataflows** import the maintenance arrangements as real
  `ArrangementFlavor::SharedTrace` arrangements and render joins and reduces over
  them. Importing as a real arrangement, not a substituted collection, is
  required for correctness. Downstream operators, delta joins especially, are
  rendered assuming the arrangement, its key, and its permutation exist.
  Substituting a collection loses that contract.

  `src/compute/CLAUDE.md` asks that rendering stay generic and that special-interest
  structures be absorbed elsewhere, so a new `ArrangementFlavor` variant on the
  generic surface needs a justification. It is not special-interest data, it is a
  second provenance for the same thing rendering already consumes: an arrangement
  whose trace handle is `Send` and shared rather than worker-local. The flavor enum
  is exactly where rendering already discriminates trace provenance, and the
  alternative, hiding a shared trace behind the existing `Trace` variant, would
  require the two trace types to unify, which they do not (`dyn TraceReader` is not
  object safe, so every consumer is monomorphized over the concrete handle). The
  visible cost is that the linear join now spells nine `(stream, lookup)`
  combinations so mixed pairs that never occur still type-check. Absorbing the
  variant is worth revisiting if trace handles ever unify behind one type.
* **Transient outputs are republished.** A query's own transient output is
  published into the registry, so its result peek is served the same way as any
  other read.
* **Late-bound imports, never a deferred build.** A query dataflow whose imported
  dependencies are not yet published is built immediately anyway, against a real
  but empty publication point that a maintenance publisher later adopts in place.
  Deferring the build would break the deterministic-construction principle above.

## The multiplexer

`src/compute-client/src/multiplex.rs` presents one controller endpoint over the
two runtimes.

* It routes peeks to interactive, `CreateDataflow` by
  `DataflowDescription::is_peek_dataflow` (see
  [The bounded-read boundary](#the-bounded-read-boundary)), maintained work to
  maintenance, and lifecycle commands to both. A peek dataflow reads published
  maintenance indexes and never another temporary collection, and nothing in the
  protocol enforces that, so the multiplexer soft-asserts that a peek dataflow imports
  no transient id rather than render one that silently finds no input. Naming the
  runtime in the dataflow description, so that placement is the control plane's
  decision, is tracked as CPU-216.
* It broadcasts `AllowCompaction` for every non-transient collection to both runtimes,
  see [Protocol invariants](#protocol-invariants), and forwards it for transient ones to
  the owner. An empty frontier is a drop and evicts the owner entry, so the state does
  not grow without bound.
* It does not deduplicate peek responses, and keeps no per-peek state. The
  exactly-one-`PeekResponse`-per-uuid contract is upheld below and above it, by the
  per-worker `PartitionedComputeState` in each process and the controller's
  per-process one. Peeks route only to the interactive runtime, so the multiplexer
  sees exactly one response per uuid and forwards it verbatim.
* It forwards each collection's `Frontiers` only from the runtime that owns the
  collection. Only the hosting runtime reports a collection's frontier, so this is a
  guard on that invariant: a report leaking from the other runtime would regress the
  controller's per-collection frontier. State: `transient_owner`, the set of transient ids the
  interactive runtime renders; every other id is maintenance's. It is per connection
  and cleared by `Hello`.

## Roles and process globals

`ComputeRuntimeRole` distinguishes the runtimes.

* `Solo` is the sole runtime of a single-runtime process. It owns maintenance and
  the process globals, and it is behaviorally identical to a deployment from
  before this work. Its metric and log label is `None`, so a single-runtime
  registration collides with nothing and looks unchanged. Process-global
  initializers guard on role, so `Solo` still runs them exactly once.
* `Maintenance` owns index maintenance and the process globals in a two-runtime
  process. It publishes its maintained indexes, and its logging and introspection
  indexes, into the registry. Publication is decided by the role rather than a
  separate flag: `Maintenance` and `Interactive` publish, `Solo` does not, since
  it has no registry peer to read what it would publish.
* `Interactive` shares the process globals owned by `Maintenance` and reads only
  from the registry. It runs with logging disabled and serves introspection peeks
  from maintenance's published copies, so introspection during hydration returns
  promptly, possibly stale, instead of blocking. It publishes its own transient
  query outputs so their result peeks route through the registry.

The interactive runtime distinguishes itself in tracing with the span name
`compute-interactive`. Note that this is a span name, not an OS thread name (see
[Known limitations](#known-limitations-and-follow-ups)).

## Failure model

There is no cross-runtime lease. A process-global panic hook
(`mz_ore::panic::install_enhanced_handler`) is installed in `clusterd::main`
before either `serve` call, so a panic on any worker or reader thread of either
runtime aborts the whole process. A stuck or torn read hold can never outlive the
process, which is what makes the import hold safe without a lease-expiry
mechanism.

## Configuration

* `ENABLE_COMPUTE_INTERACTIVE_RUNTIME` (dyncfg, `mz-controller-types`) is off in
  production and on by default in the variable CI system parameters, so the suite
  exercises the two-runtime path broadly. The compiled default stays off, so
  production is unaffected.
* In CI the flag is a `VariableSystemParameter` defaulting to `true`, so sqllogictest,
  testdrive and the mzcompose suites provision two-runtime replicas, and the parallel
  workload flips it at random. Unmanaged replicas are launched by the test itself
  rather than by the controller, so the flag never reaches them. A suite that wants the
  second runtime on one, as the feature benchmark does, passes
  `--interactive-compute-timely-config` to its `clusterd` service directly.
* When enabled, the controller launches replicas with a second interactive
  runtime configured by the `--interactive-compute-timely-config` CLI argument
  (its own worker ports). The dyncfg controls whether the controller passes that
  argument.
* Flipping the dyncfg changes `ServiceConfig::ports`, but it is read when a replica is
  PROVISIONED, not applied to running ones. An existing replica keeps its old configuration
  until it is recreated, so the flip is not a live toggle and is not a rolling restart either.
  Plan it as a flip followed by an explicit recreation of every replica that should pick it up.
* The interactive port is named `interactive`, not something more descriptive, because
  Kubernetes rejects a container port name longer than 15 characters. The process orchestrator
  that local runs and mzcompose use has no such limit, so an over-long name passes every test
  and then fails to schedule in cloud.

## Non-goals

* `SUBSCRIBE` is out of scope. The routing predicate admits only single-time
  dataflows. The import itself follows the dataflow's `until`, so a subscribe
  migration is a routing change plus the `SnapshotMode` gap recorded below.
* Cross-process and replica-to-replica sharing are out of scope. Sharing is
  per-process because the batches are `Arc`-backed in memory.
* The import and replay queue is unbounded, with no overflow handling, in this
  first cut. This is deliberate: maintenance progress must never be coupled to a
  slow interactive-side reader. The cost, unbounded memory growth for a
  pathological long-lived importer, is accepted for now and recorded as deferred
  work, not silently ignored.

  Note that this decouples maintenance *progress*, not maintenance *memory*.
  Memory is coupled in both directions: beyond the unbounded queue, an interactive
  reader's hold forwards into the maintenance trace's compaction, so a clogged
  interactive step loop delays the hold's release and with it maintenance
  compaction.

## Known limitations and follow-ups

### Open findings from adversarial review

Five independent reviewers went at the branch. The defects that could be verified
against the code are fixed. These are the ones that remain, kept here because each
is a real hazard with a known mechanism rather than a speculation, and each needs a
decision rather than a patch.

* **`import_index_shared` silently drops `SnapshotMode`.** The maintenance import
  honours `SnapshotMode::Exclude`, which is what `WITH (SNAPSHOT = false)` lowers to.
  The shared import's signature has no `snapshot_mode` parameter at all, so the mode is
  discarded. Unreachable today, because no `Exclude` dataflow routes to interactive.
  Reachable the instant a subscribe does, and the failure is a **silently wrong
  answer**: the snapshot is included when the user asked for it to be skipped. This
  wants fixing before, not with, any routing relaxation.
* **Reconciliation cannot retain any cross-runtime importer, and this is stronger than
  the I2 row that records it.** `dependencies_retained` requires every imported index
  id to appear in `retain_ids`, and `retain_ids` is populated only from matches within
  *the same runtime's* new commands. An interactive dataflow's imported maintenance
  index is routed to maintenance, so it never appears in interactive's stream, so the
  check is **always false**. Every interactive dataflow with an index import is
  replaced on every controller reconnect. Harmless for an ephemeral read and fatal for
  the premise of a maintained collection on interactive, which would rehydrate on every
  reconnect. Fixing it needs the two runtimes to exchange retained-id sets, and the
  protocol has no place for that. This is the piece of "maintained dataflows on
  interactive" that is structurally foreclosed rather than merely unbuilt.
* **A peek against a never-published shared id waits until it is cancelled.** The
  registry returns no handle, the peek parks in `pending_work`, and only a
  publication, a seal or a cancel for that id re-examines it. The local path fails
  loudly on the same condition. The controller's read-hold discipline keeps a published
  id from dropping while a peek targets it, so the case is an index whose creation was
  cancelled before it rendered. Never-adopted placeholders are also never evicted.
* **The coordinator control plane is a parallel, unsolved bottleneck.** Peeks
  serialize behind DDL on the single coordinator thread, upstream of compute.
  Two-runtime fixes the data plane and does not touch this. For non-introspection
  reads under load the coordinator can dominate, so this must stay on the roadmap.
* **The interactive runtime is a single step loop.** Its read throughput has a
  ceiling, and a heavy scan can clog light point reads sharing the loop. A future
  admission or lane policy would protect a cheap-read lane and decide where
  expensive reads spill.
* **Thread names collide across runtimes.** Timely names worker threads
  `timely:work-{index}` by a per-instance-local index, so the maintenance and
  interactive runtimes (and storage) emit identical OS thread names in one
  process. Profilers cannot tell them apart by name. A per-worker rename in the
  worker entrypoint would fix it, and would fix the pre-existing storage and
  compute collision as a side effect.
* **The interactive runtime is an introspection blind spot** (CPU-222). It runs with
  logging disabled and serves introspection from maintenance's published copies,
  which is what keeps introspection answerable during hydration. The cost is that
  the interactive runtime's own dataflows, arrangement sizes, and scheduling are
  not visible in introspection at all. Restoring visibility must not reintroduce
  the hydration-blocking the forwarding avoids, so it likely means the interactive
  runtime publishing its own logging arrangements for a reader to consume, or a
  separate introspection channel, rather than turning its local logging back on.
* **Per-runtime memory attribution.** Arrangement-size introspection does not yet
  attribute memory per runtime, a specific case of the blind spot above.
* **Publishing an index doubles its reported arrangement size.** With the feature
  on, a published index reports twice the heap size, capacity, and allocations of
  the same index with the feature off, while its record and batch counts are
  unchanged (measured on a 16-worker replica: a one-record index reports 8740 bytes
  and 132 allocations against 4370 and 66). What is established: it is not the
  `Rc` to `Arc` migration, since an unpublished materialized-view arrangement is
  byte-identical either way. It is not a reader, since it is present before anything
  imports the index. It is not the published chain lagging a spine merge, since
  re-reading the chain after compaction is forwarded does not change it. The
  arrangement-size logger identifies batches by address and sums every batch it can
  still upgrade a `Weak` to, so a second live batch per worker is being held
  somewhere in the publish path. **It did not reproduce on staging**, where E6 measured both
  the reported size and the resident set coming slightly *down* with the flag on, and
  measured import as nearly free at 4.5 MiB for 48 interactive dataflows over a 95 MiB
  index. That run crossed two builds, so it establishes only that no doubling appeared,
  rather than that the effect is absent. Whether it is a reporting artifact or real
  retention decides whether the feature carries a memory regression, so it should be
  settled on a single build before the flag is considered for production.
  `test/testdrive/introspection-sources.td` carries the raised bound and a pointer to
  this entry. The `ManyIndexesIdle` feature benchmark, 200 published one-key indexes
  against a single-runtime image on one build, put clusterd's resident memory within
  2% in two of three nightly runs and 16% above in the third, so at that scale the
  doubling is at most partly resident.
* **Storage introspection is patched into maintenance introspection.** A
  pre-existing coupling, where storage's introspection is merged into the compute
  runtime's introspection, is inherited unchanged by the split. It complicates
  per-runtime attribution and wants untangling independently of this work.
* **Two-runtime adds a metric label that breaks existing dashboards.** `Solo`
  omits the role label, so a single-runtime deployment is unchanged. But with the
  feature enabled the maintenance runtime's metrics carry `role="maintenance"`, a
  new label on existing series that breaks exact-match dashboards and alerts. The
  clean fix is to keep the maintenance runtime label-free, matching the
  pre-existing series, and label only the interactive runtime, so enabling the
  feature adds new series rather than relabeling existing ones.

### The incremental path from here

What is on the branch is deliberately the least opinionated version of the idea. It gives
interactive work its own run loop and nothing else: it asserts no scheduling priority,
reserves no core, and measures no shared-cache interference. That is the right first step
rather than a shortcut, because each item below is a separate decision with its own evidence
requirement, and none has to land with the first one. Recorded here so the sequence is a plan
rather than a rediscovery.

All three are about how the two runtimes share CPU, which is the one resource colocation can
actually arbitrate. Ordered by expected value per line of change.

1. **Scheduler priority instead of a reservation.** Nice values under CFS express
   only weight ratios and are too weak to be useful here, but the fleet's kernels
   run EEVDF, where `sched_setattr` with a short request and a low latency-nice
   gives a thread an earlier deadline and lets it preempt a batch thread promptly
   rather than at a slice boundary. It costs nothing when nothing contends, it is
   per-thread so it applies to exactly the interactive runtime's workers, and the usual caveat
   that priority only orders within a cgroup's share does not bite because both
   runtimes sit in one pod. This is the cheapest lever not yet pulled.
2. **A core reservation and pinning, both conditional on a change of QoS class.**
   Neither is available today and the reason is the class, not the code. Swap is
   granted only to Burstable pods, sized from the memory request. Exclusive cores
   are granted only to Guaranteed pods with integer CPU requests under the static
   CPU manager policy. Those classes are disjoint, so **swap and pinning are
   mutually exclusive**, and choosing swap chose against pinning. The code already
   encodes this correctly: pinning is gated on
   `location.allocation.cpu_exclusive && enable_worker_core_affinity` in
   `src/controller/src/clusters.rs`, and `cpu_exclusive` is false throughout the
   size configuration, so the flag is inert for the right reason.

   Worth stating that enabling it anyway would be actively harmful rather than
   merely useless. A Burstable pod is never assigned exclusive CPUs and runs in the
   node's shared pool, and `core_affinity::get_core_ids()` returns the whole
   affinity mask, so a worker would be pinned to a shared CPU that neighbors use
   too. Pinning does not grant the core. It only removes the scheduler's ability to
   migrate the thread off a busy one, and migration is the only defense available to
   a cgroup that does not own its cores. The CPU request buys a share of the node,
   not a particular CPU on it.

   In a world where the QoS trade is revisited, the reservation comes first.
   Timely is barrier-synchronous, so removing a fraction of one core does not cost
   that fraction of throughput. It desynchronizes the workers and the penalty
   amplifies at the barrier. This is the long-standing
   operating-system noise result from high-performance computing, where daemons
   occupying one core cost far more than their CPU share, and the remedy there was
   to reserve one core out of many. At 32 workers that is 3% and worth it. At 2
   workers it is 50% and absurd, so any reservation is conditional on replica
   size.

   What is not available is reserving that core by shrinking the interactive
   runtime. Equal peer counts across the two runtimes are a soundness requirement
   and not a sizing choice. Import is pairwise, importer worker `i` reads publisher
   worker `i`, which is correct only when both sides shard keys identically over an
   equal total peer count, and `import_snapshot_at` asserts it. Peek serving has
   the same structure, because resolving a key to the worker that holds it uses the
   same partitioning. Making the runtimes unequal would require that partitioning
   to become a contract between them, visible to whatever re-routed across the
   mismatch, and it has to stay an implementation detail of the compute layer
   instead. See [The import follows the dataflow's until](#the-import-follows-the-dataflows-until).

   So the reservation is expressed by sizing *both* runtimes one worker below the
   core count and leaving a core for the interactive threads and for tokio.
   Maintenance pays one worker of parallelism, which is the honest price and is
   exactly the reserve-one-core prescription. The doubled thread count is therefore
   a fixed cost of the architecture rather than a knob. It is also not a doubled
   CPU cost, since an idle worker parks in `step_or_park` between maintenance
   ticks. What doubles is thread stacks, per-worker progress tracking, and the
   frontier-following work that gives an idle replica its resting utilization.

   If pinning is ever available, the asymmetric form is the wrong one. Pinning
   maintenance and floating interactive sounds right because throughput work wants
   locality and latency work wants placement freedom, but that reasoning applies to
   a thread pool and the interactive runtime is not one. It is a second
   barrier-synchronous engine with the same peer count, so floating its workers
   moves the jitter into its own barrier rather than removing it. Pinning
   maintenance to every core is also not a reservation. It only fixes where
   maintenance runs, so interactive lands on a core holding a pinned runnable
   worker the scheduler can no longer balance away, which is worse than pinning
   nothing.

   The form that follows from the design is to co-pin worker `i` of both runtimes
   to the same core. The equal-peer requirement is not only a soundness pairing, it
   is a locality pairing: interactive worker `i` reads publisher worker `i`'s
   batches, and under first-touch those pages live wherever the publisher
   allocated them. Co-pinning keeps a bandwidth-bound cursor walk on the same
   core's cache hierarchy and the same NUMA node as the data, where pinning the two
   apart guarantees remote traffic for every cursor step. This also needs no new
   plumbing, because `set_core_affinity` maps the global peer index modulo the core
   count and both runtimes agree on that index, so enabling the existing flag on
   both runtimes already co-pins. The asymmetric variant is the one that would need
   new code. Contention on the shared core is tolerable because interactive worker
   `i` is mostly parked, and where it is not, items 3 and the reservation above are
   what govern the steal.

   Two things to measure rather than assume. Placing maintenance `i` and
   interactive `i` on sibling hyperthreads of one physical core would give the
   pairwise read a shared L1 and L2, but siblings share execution resources and a
   bandwidth-heavy maintenance worker degrades its sibling, which is one of the
   interference channels the colocation literature says must be controlled
   explicitly. And `core_affinity::get_core_ids()` enumerates logical CPUs with no
   guaranteed order, which is why the existing code sorts them, so whether the
   first N ids are N distinct physical cores is platform-dependent.
3. **Shared-cache and memory-bandwidth interference during hydration.** Core
   partitioning is not sufficient on its own. A batch task streaming through the
   last-level cache degrades a colocated latency-sensitive task's tail even when
   the two never share a core, and the published remedy is to throttle the batch
   task rather than to fence it harder. Hydration is the batch task here, it is
   pure throughput work with no deadline, and the knobs already exist in
   `compute_hydration_concurrency` and `dataflow_max_inflight_bytes`. The
   measurement is peek p99 against cache-miss rate during a hydration, and if the
   effect is material the answer is a feedback loop from interactive queueing to
   hydration concurrency. Deliberately last of the mechanisms, because it is the
   only one that needs a controller.
### What a serving layer would need

A serving layer is latency-sensitive and cannot be cleanly separated from index
maintenance, because the thing it serves is the maintained index. The preceding
sections cover the scheduling half of that problem. This section records the
other half, which is that a serving structure is a different data structure and
not a different scheduler, and what our consistency model demands of it.

The constraint that shapes everything is multi-versioning, and it is stronger than
it first appears. It is tempting to assume a serving structure can hold one
consolidated snapshot at the latest timestamp, on the grounds that a point lookup
names a single time. It cannot. Timestamp selection picks a timestamp valid across
every object the query reads, and inside a transaction across the entire
timedomain the transaction may go on to touch, which is fixed before those objects
are known. A read that cannot be satisfied in that domain gets
`RelationOutsideTimeDomain` rather than a slower answer. A single-version store
collapses `[since, upper]` to a point, and a point almost never intersects the
range a multi-object query needs. This applies to serializable reads and not only
to strict serializable ones, so it is not avoidable by scoping the feature to a
weaker isolation level.

That removes most of the naive cost advantage, and it is worth being precise about
which part survives.

* Versions must stay. Retaining history back to `since` is the expensive part of
  an arrangement and it is not optional for a structure that answers reads.
* Diffs need not. A serving structure can hold consolidated values per version
  rather than a stream of updates to be consolidated at read time.
* The spine need not. An arrangement is a log-structured merge of updates, so a
  point lookup consults every batch and the write side pays merge amortization. A
  serving structure can be hash-keyed per version and immutable between refreshes.

So the shape is multi-version concurrency control over a hash index, not a
snapshot map. That is meaningfully cheaper on point-lookup cost and on read-side
constant factors, and only modestly cheaper on memory.

This is also where the closest precedent stops applying. Noria's reader nodes are
the same idea, read-optimized state derived from the dataflow and read without
entering the dataflow scheduler at all, but they hold the latest version only.
The point where that design diverges from what we need is exactly our consistency
model, so it should be read for the structure and not for the storage.

Two candidate substrates, with what each is actually good for.

* RocksDB, already in the tree for upsert state. It turns index memory into disk
  plus a bounded block cache, which changes the cost curve rather than the
  latency, and its reads are re-entrant from any thread, so the timely worker
  leaves the read path entirely instead of being scheduled around. Versioning has
  to be built on top and keyed by our timestamps, at which point retention becomes
  the cost driver just as it is for an arrangement.
* Persist directly. Parts are key-sorted and carry column statistics, and filter
  pushdown already prunes on them, so a lookup on the shard's sort key is feasible
  with no index at all. It is bounded by object-store latency, so it sits in the
  tens to hundreds of milliseconds. That is a cost play for workloads that cannot
  justify an index, in a different latency class from an arrangement, and it should
  not be presented as a serving tier.

An external key-value cache is not on that list. It adds a second consistency
domain and a cache-invalidation story in order to keep the guarantee that is the
only reason to own the store. Sinking into whatever store a user already runs is
the existing answer and a better one.

## Testing strategy

* Unit tests cover the sharing primitive and registry: the single-source feed,
  placeholder-adopted-late joins, cross-thread reads, the compaction invariants, a join
  and a reduce over a chain the publisher's spine has merged across read at a stale
  `as_of`, and the alias rules, where an alias shares the target's slot, is refused once a
  reader holds its own point, and inherits the target's frontiers until the target drops
  and the aliases' meet takes over. The multiplexer's routing and frontier filtering and
  the index peek sweep have their own.
* A shared-fate subprocess test (`two_runtime_shared_fate.rs`) verifies a panic in either
  runtime aborts the process.
* Five `clusterd-test-driver` specs, run at one and two workers, cover the runtime
  boundary: a fast-path read through a published index, a query dataflow that binds to
  an index before it is published and resolves after, a read through an index that
  re-exports another's arrangement and so aliases its publication point, a query
  dataflow that binds to such a re-export before it renders, which is the one case
  where the re-export publishes through an import, and a query dataflow on the
  interactive runtime whose export is the arrangement it imports, which aliases the
  shared publication point.
* `interactive_runtime.slt` pins the flag before creating a two-worker cluster and reads
  through indexes, re-exports and introspection relations on it.
* Feature benchmarks under the `InteractiveRuntime` group price each read path on a quiet
  replica against a single-runtime image: a join that needs a peek dataflow, an
  introspection read, and clusterd memory with 200 published indexes idle, after a
  restart, and with 200 re-exports of one arrangement. The point lookup and `CREATE
  INDEX` plus the first read through it are the main set's `FastPathFilterIndex` and
  `CreateIndex`, which run on the same container. The benchmark's default cluster is an
  unmanaged replica, so its `clusterd` service is launched with the second runtime
  explicitly.
* Parallel-benchmark scenarios measure the isolation claims under contention and gate on
  regression thresholds. `ReadIsolationUnderHydration` and `IntrospectionUnderHydration`
  read at a fixed rate while hydration churn saturates the maintenance workers.
  `TemporaryDataflowFloor` is the quiet-replica cost of a peek dataflow.
  `FreshnessUnderPeekWalks` is how far full index walks hold back a maintained index's
  frontier. `MaintenanceUnderPeekSaturation` is the converse: a peek load saturates every
  worker of an eight-worker cluster on a sixteen-core agent while a materialized view's
  freshness is measured from an idle cluster, which prices the oversubscription of two
  runtimes' worker threads. Scenarios whose measurement is peek service time read at
  SERIALIZABLE, because a strict serializable read waits on the frontier the contention
  stalls, which is M5 rather than the mechanism under test.
* The `bounded-memory` composition's `swap` workflow runs a replica whose arrangements
  exceed its memory limit by 1.5x, 2x, 3x and 6x with unlimited swap, and asserts that
  the read completes and that the replica's cgroup reports swap use, so the paged-out
  case has a regression test.

What one nightly run measured on the CI agents, two runtimes against one, reported as
p50 / p99 unless stated:

| Scenario, measured query | Two runtimes | Single runtime |
|---|---|---|
| Point lookup under hydration churn | 5.7 / 46 ms | 225 / 1711 ms |
| Range count under hydration churn | 53 / 84 ms | 2001 / 5536 ms |
| Hydration churn cycle, same scenario | 7.2 s | 11.0 s |
| Introspection read under hydration churn | 94 ms / 2.4 s | 595 s / 629 s |
| Strict serializable read after write, under full index walks | 13.9 / 22 ms | 15.1 / 75 ms |
| Peek dataflow on a quiet replica | 19.9 / 25 ms | 19.9 / 53 ms |
| Point lookup behind expensive peeks | 4.7 / 6.5 ms | 4.8 / 7.0 ms |
| Materialized view freshness under peek saturation | 61 / 1449 ms | 642 / 1249 ms |
| Clusterd memory, 200 re-exports idle, before aliasing | +45% to +64% | baseline |
| Clusterd memory, 200 re-exports idle, aliased | +1.3% | baseline |

The introspection row is the case the design is justified by: with one runtime the
introspection reads queued behind the churn for the whole load phase. The saturation row
says the sixteen worker threads on sixteen cores cost nothing measurable at that size, since
the tails were within 16% of each other and the churn throughput was equal, and that the
median moved because the view's writes no longer wait behind peeks. The expensive-peeks row is peek-versus-peek queueing,
which the second runtime does not address and the equal numbers confirm.

## Implementation history

The feature was first built against a differential-dataflow fork that supplied
Arc-backed batches and a `sharing` module, stacked on an Arc-batches base branch.
The sharing primitive was then reimplemented natively in `mz_compute::shared_trace`
plus `mz_row_spine::ArcBatch`, and a lifecycle correctness redesign fixed a set of
concurrency bugs through a single-source publisher feed and placeholder-plus-adopt
construction. Finally the fork was dropped entirely: the publisher's compaction
floor moved from the fork's `trace_box_unstable` read to the controller's
`AllowCompaction` and the stream upper, so the build now depends only on released
differential-dataflow.

The read-hold protocol went through three mechanisms before the current one. Two
reconstructed the lost command ordering rather than restoring it, and both are recorded
under [Rejected alternatives](#rejected-alternatives).

Re-exports became aliases late. The first cut published each re-export through its own
import of the shared traces, which cost about 150 KiB of resident memory per re-export and
gave the re-export dataflow operators, so `mz_compute_error_counts` stopped attributing
the target's errors to it. Aliasing restored both, and the import path survives only as
the fallback for a reader that bound to the alias id first. The interactive runtime's
export of an imported shared arrangement was an `unreachable!` until review asked what
guaranteed it. Nothing did, and it became an alias as well.

The peek path was unified with the peek execution work: the interactive runtime's own
`PendingPeek` variant was folded into `IndexPeek`, with `IndexTraces` naming the trace
provenance, so budgeting, offload and the stash apply to shared and local traces alike.

The implementation is split into a stack of layers, each reviewable on its own and each
compiling and passing its tests without the ones above it:

1. the shared-trace primitive (#38386),
2. the per-process sharing registry (#38387),
3. the multiplexer with broadcast compaction (#38388),
4. publication of maintained indexes, with re-exports as aliases (#38389),
5. import of published indexes as a shared arrangement (#38390),
6. the second runtime, its role, and the shared-fate test (#38391),
7. fast-path peeks on the interactive runtime (#38392),
8. the flag, on in test configurations, with the driver specs and the moved goldens
   (#38393),

followed by the benchmarks that guard the read paths (#38676) and price oversubscription
and publication at scale (#38678). The swap scenarios in `bounded-memory` are independent
of the stack (#38683).

Every planning and evaluation document that preceded this one has been folded in or moved
out, so this directory holds one design document and nothing else. The planning documents
(`implementation-plan.md`, `stage2-detailed-plan.md`,
`arrangement-sharing-lifecycle-design.md`, `arrangement-sharing-lifecycle-plan.md`,
`read-holds.md`, `broadcast-compaction.md`, `pr-split.md`) are superseded: their task
checklists are executed and their fork-era mechanics no longer reflect the code. The
experimental evaluation moved to the project document named in the summary, and the peek
placement and stash-plumbing documents moved with the peek-offload work they belong to.
