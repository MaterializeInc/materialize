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
on, a replica runs a `Maintenance` and an `Interactive` runtime, and every index the
maintenance runtime renders, logging indexes included, is attached to a publication point.
Publication adds no operator, so the dataflow goldens do not change with the flag. A second
dyncfg, `enable_compute_interactive_dataflows`, on by default, decides whether peek
dataflows render on the interactive runtime or as maintained work. A peek dataflow can be
arbitrarily expensive, and without a cost model nothing bounds what it takes from the
fast-path peeks the interactive runtime serves.

The implementation lands as a stack of seven pull requests, each a self-contained layer,
followed by three that add the benchmarks and a compaction metric.
[Implementation history](#implementation-history) lists them.

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
  for a core. The experiment for this was planned as E3 and never run as specified.
  Nothing reaches it, because no scheduling change manufactures CPU. One saturated run
  since, recorded under [What the matrix does not settle](#what-the-matrix-does-not-settle),
  shows that reordering is itself the win when the wait is a run-to-completion step rather
  than a shortage of cores.
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
  cannot be moved. A dataflow renders on the interactive runtime only when its
  `DataflowClass` is `OneShotRead`, the adapter sets that class only on peek dataflows,
  and the compute controller soft-asserts that a `OneShotRead` dataflow carries no
  subscribe sink. So **subscribes are maintained work by construction and the second
  runtime cannot reach this at all.** The recorded reason is that a subscribe never
  stops, while the interactive runtime's imports of maintained indexes end one step past
  the dataflow's `as_of`. See [The one-shot-read boundary](#the-one-shot-read-boundary).

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
change it first looks like. It needs a class, or a relaxed `OneShotRead`, for a transient
dataflow with an empty `until`, set by the adapter on the subscribe path. The import
already supports such a dataflow. It follows the dataflow's `until`, so an empty `until`
follows the trace, and it honours `SnapshotMode`, because the shared and the local import
are one `import_index`. What it relies on is the compaction feedback described in
[Compaction feedback flows through the reader's handle](#compaction-feedback-flows-through-the-readers-handle),
since such an importer holds the publisher for its whole life. A `SUBSCRIBE` sink needs
the stream and not the trace, so it is the easiest case of that feedback, but the feedback
is what any other long-lived interactive dataflow needs and it is built for the general
case. Reconciliation already refuses to retain a subscribe, so one on the interactive
runtime is replaced on a reconnect exactly as one on maintenance is.

Maintained collections on the interactive runtime are a strictly larger change. The
interactive runtime would have to publish and the maintenance runtime read, and the roles
give each runtime one direction only. See [Roles and process globals](#roles-and-process-globals).

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
runtimes, the multiplexer forwards *every* `Peek` and `CancelPeek` to the interactive
runtime alone (`src/compute-client/src/multiplex.rs`). So S4 is not a component that can be cut, it is
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
* **One arm has run on a CPU-saturated replica, and it moves the boundary rather than the
  claim.** On staging, a 31-worker replica hydrating an sf100 `lineitem` index, with the
  buffer pool's 8 spill threads on and CPU saturated at about 34 CPU-seconds per second
  against a 31-core limit, kept fast-path peeks and introspection peek dataflows at the 2.2
  to 2.7 s client floor for the whole hydration. With the maintenance runtime alone the
  same probes reached 24 s and 33 to 54 s. The maintenance-only latency came from operator
  schedulings of 2 to 8 s in the hydrating arrangement, which every command and peek waits
  out on all workers, so it is queueing on the run loop rather than a shortage of cores.
  A peek dataflow over a persist-backed collection took 40 to 56 s either way, because it
  waits on the process-wide persist fetch semaphore and the hydrating source held every
  permit. See [Known limitations](#known-limitations-and-follow-ups). One run each, so
  the hydration times, 151 s with the second runtime against 176 s without, say only that
  the second runtime did not slow it.
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

It does not add CPU. The expectation was that on a CPU-saturated box the
interactive runtime cannot get a core either and reads back up as they would in a
single runtime. The one saturated run contradicts that for reads served off
arrangements: they stayed at the client floor through a hydration that took
maintenance-only reads to 24 s, because what they had waited on was a
run-to-completion step on every worker, and the kernel interleaves the interactive
threads with it. Reads that need a resource the process shares, such as persist fetch
permits, still back up. See
[What the matrix does not settle](#what-the-matrix-does-not-settle).

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
2. **Placement is the control plane's decision, and the multiplexer is passive.**
   The compute controller tags every dataflow with a `DataflowClass`. The
   multiplexer forwards every command to both runtimes, except `Peek` and
   `CancelPeek`, which only the interactive runtime receives, and each runtime
   decides from the class whether it renders a dataflow. Both runtimes see the same
   command stream, so a command means the same thing on either, and the command
   history stays valid for a replica with either runtime layout.
3. **Deterministic construction.** Timely allocates exchange-channel identifiers
   from a per-worker, construction-order counter, so every worker must build
   dataflows in the same order. We render in command arrival order and never
   reorder or defer a build. A runtime decides placement before anything allocates
   a dataflow index, so a dataflow it does not render builds nothing. A read of an
   index the other runtime has not yet published binds a real but empty publication
   point, which the publishing trace later fills in place.

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

Route a `CreateDataflow` for an interactive dataflow only to the interactive runtime, while the
maintenance runtime applies `AllowCompaction` for the index it maintains, and the two commands land
on different streams. The runtimes have independent command streams and no cross-runtime ordering,
so maintenance can apply a compaction for `I` at a point in its stream that has no defined
relationship to where interactive is in its stream. I1a does not
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

### The fix: broadcast every command and hold peers at their `as_of`

Reconstructing I1b is the wrong move. I1b was lost because of a routing decision, not
because two runtimes cannot have it. Send every command to both runtimes and each runtime
has the `CreateDataflow` and the `AllowCompaction` on one ordered stream again, which is I1b
restored rather than simulated.

Two changes.

**Broadcast.** The multiplexer forwards every command to both runtimes. A runtime renders
a `CreateDataflow` only if its placement covers the dataflow's class, and otherwise records
the dataflow's exports as *peers*.

**A peer hold per published index.** A runtime that reads its peer's indexes holds each
one at the `as_of` of the index's `CreateDataflow`, through a logical-only reader on the
publication point. The publishing runtime registers that hold when it publishes the index,
which comes before any `AllowCompaction` for the index on its own stream, and the reading
runtime takes it over when it records the peer. The hold is then the index's trace in
`compute_state.traces`, so `AllowCompaction` moves it exactly as it compacts a local trace,
and an empty frontier removes it. The publishing trace compacts to the meet of its local
holds and its readers' holds, so the peer hold bounds it.

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

The peer hold closes exactly that window. The bound is the reading runtime's applied
frontier, still 0, and it advances only when that runtime applies the broadcast compaction,
which is queued *behind* the create. It has to exist before the reading runtime reaches the
index's own create, too. A reading runtime that far behind would otherwise register its
hold only after maintenance had compacted past the read, which is why the publisher
registers it.

**The peer hold costs nothing.** It pins each index at the controller's compaction
frontier as the reading runtime has applied it, which the controller already guarantees is
readable, and it holds nothing physically, so the publisher keeps merging. What it removes
is the trace's freedom to compact ahead of the reading runtime.

**What it needs from the runtimes.** A runtime accepts every command for a peer.
`AllowCompaction` moves the peer's hold, an empty frontier forgets the peer, and `Schedule`
and `AllowWrites` change nothing, since the peer's work happens on the other runtime. A peer
never enters `collections`, which drives frontier reporting, so a collection's frontiers
come only from the runtime that renders it. A peer is keyed by its collection and carries no
dataflow identity.

#### As implemented

The peer hold is `Shared::reader_at_least`, a reader whose logical hold is the join of the
`as_of` and the published `since`, with no physical hold, so it never refuses. Each slot
pairs its publications with their peers in order: `publish` registers a hold unless a peer
was recorded first, and `peer_bundle` takes the oldest registered hold or, when there is
none, registers its own and marks the publication to come as already held. A hold nobody
takes dies with the slot. `peer_bundle` wraps the `oks` and `errs` readers in a
`TraceBundle` of `IndexTrace::Shared` traces that also retains the registry slot. Imports never read through the peer hold itself.
`IndexTrace::import_frontier_core` mints a fresh reader at the hold's frontier that also
holds the chain physically at the coverage it seeds the import with, as an import must.

Every export of a peer dataflow is a peer, not only its indexes, so `AllowWrites` and a
dropping `AllowCompaction` for a materialized view or a sink find a known id. A command for
an id a runtime neither hosts nor holds as a peer is a protocol violation and soft-panics.

Logging indexes are peers too. The interactive runtime renders no logging dataflow, so when
the instance's logging configuration arrives it records every log index as a peer at the
minimum time, and the maintenance runtime publishes them.

#### The price: compaction is coupled to the rendering runtime's drain rate

A stalled reading runtime stalls compaction on every shared arrangement. That is the price
of I1c and it is the intended behaviour, but it is new: before the split a slow reader could
not hold maintenance back.

Two per-worker gauges make the coupling observable before it becomes a memory incident.
`mz_compute_shared_arrangement_hold_gap_ms` is the largest difference, over the arrangements a
worker publishes, between the logical compaction frontier the controller asked for and the one
the holds let the trace apply. `mz_compute_shared_arrangement_held_count` is how many are held
at all. Only the publishing runtime reports them, from the registry its worker shares with its
peer, so the `interactive` series stays at zero and a `sum` or a `max` over the label stays
correct.

Zero is the healthy reading rather than a broken metric: both runtimes receive the same
`AllowCompaction`, so an interactive runtime that drains promptly leaves the two frontiers
equal. On an 8-worker replica under continuous churn the gauges read zero with no reader, and
peaked at one compaction round with 31 arrangements held while 509 interactive joins ran over
the published index. The lag did not grow with time under that load.

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

* **Route `CreateDataflow` by placement and broadcast only `AllowCompaction`**, with a
  standing hold per shared collection at the last compaction the rendering runtime applied.
  It restored I1b for compaction and kept the multiplexer deciding placement from the
  dataflow's shape, which made a command's meaning depend on which runtime read it: the
  rendering runtime had to treat `AllowCompaction` for an id it never saw created as
  advancing the hold. Broadcasting every command gives each runtime the whole stream, and
  the class gives the decision to the controller.

A peer hold is per collection and carries no dataflow identity, so a replayed dataflow
receiving a fresh transient id cannot conflate two holders, and the replica clears nothing at
a reconnection.
## The one-shot-read boundary

The interactive runtime renders a dataflow only when the compute controller has tagged it
`DataflowClass::OneShotRead`. The adapter sets that class on one kind of dataflow, a peek
dataflow, together with an `until` one step past its `as_of` (`optimize/peek.rs`). Every
other dataflow keeps the default, `Maintained`. The controller soft-asserts the shape a
`OneShotRead` dataflow must have (`DataflowDescription::class_fits_shape`): transient exports,
a single-time read, and no subscribe or copy-to sink.

Each condition has a reason. A single time, because the interactive runtime's imports of
maintained indexes end one step past the `as_of`, so they cannot feed a dataflow that runs
further. Transient exports, because the runtime that renders a dataflow is the only one that
reports its frontiers. Transience alone is not the property, because it says nothing about
when the dataflow stops: an introspection subscribe is transient and never stops. No copy-to,
because reconciliation refuses its S3 sink.

`DataflowDescription::compatible_with` does not compare the class. A class change across a
reconnect must not rebuild a dataflow a runtime already renders, so each runtime keeps the
placement it acted on.

With `enable_compute_interactive_dataflows` off, the controller installs a `OneShotRead`
dataflow as `Maintained`, after the shape assertion. The maintenance runtime renders it and
publishes its index exports, the interactive runtime records them as peers, and since every
peek still goes to the interactive runtime, it serves the result peek from the published
index.

## The sharing primitive

The cross-runtime sharing primitive lives entirely in Materialize. It builds
against a released differential-dataflow, with no fork and no `[patch.crates-io]`.

* `mz_row_spine::ArcBatch` is a local newtype around `Arc<B>` that carries the
  differential batch traits, so a batch whose contents are `Send + Sync` can be
  read from a thread other than the one maintaining the trace. The orphan rule
  forbids the blanket `impl Trait for Arc<B>` in Materialize, which is why the
  newtype exists rather than a bare `Arc<B>`.
* `mz_timely_util::shared_trace` holds the primitive proper: `SharedSpine`, a `Trace`
  wrapper around the spine, `Shared`, the publication point it mirrors into, and
  `SharedReader`, the `Clone + Send` reader, which implements `TraceReader` and imports
  the arrangement into another scope with `import_frontier_core`. Every spine in
  `mz_compute::typedefs` is a `SharedSpine`, so any arrangement can be published.
  Unattached, the wrapper costs one branch per trace call.
* `mz_compute::shared_trace` is the Materialize glue: `Published`, the owner-facing
  publication point, which also mints the logical-only peer handle, and `adopt_trace`,
  which attaches an arrangement's trace to a point on the owning worker and takes the
  callback fired on every seal.

An earlier prototype consumed these from a differential-dataflow fork, whose only
remaining purpose was reading a compaction floor off `agent.trace_box_unstable()`,
an upstream API documented as unstable and undefined behavior to mutate. The wrapper
reaches the trace through that same accessor exactly once, to attach it, the way
arrangement-size logging already reaches it to walk batches, and reads no floor from
it: compaction frontiers reach the wrapper through the `Trace` methods the `TraceBox`
calls (see [Compaction](#compaction)).

### Why a wrapper and not a shared spine

A spine behind a mutex was the obvious shape, and the spine's own structure rules it
out. `introduce_batch` spends fuel on in-progress merges, and `roll_up` and
`complete_at` finish merges synchronously with unbounded fuel as part of structural
changes, so a lock guarding the spine is held for whole merges, and detaching the
in-progress mergers would cover only the fuel path. Batches, though, are immutable
and reference counted, so a chain of them plus the trace's frontiers is a consistent
view that any thread can read.

`SharedSpine` therefore keeps the spine private to its worker and mirrors the chain,
`upper`, and compaction frontiers into the point inside every mutation. The lock is
held for a chain rebuild, one reference count per spine level, never for merge work.
Measured on a churn workload with two runtimes in one process (release build, 1M to
8M keys, every key rewritten every round): writer mutations averaged 0.5 to 33 ms
and peaked at 276 ms, which is what a reader of a shared spine would have waited,
while the wrapper's readers acquired cursors in 1.5 µs at p99 and 174 µs at worst,
and the writer held the view lock for under 1 µs per publish.

### Placeholder and attach

A publication point is an `Arc<Shared>`. It can be created empty, as an unbacked
placeholder, and later attached by a trace in place, filling the same `Arc`.
This is what makes arrival-order construction work. A differential import captures
its input trace by value at construction time, so the import must have a real
trace to hold even before the arrangement it reads exists. `Published::new`
gives it one. A later `adopt_trace` attaches the arrangement's `SharedSpine` to that
same point, publishing its chain and seeding every importer already registered, and
the by-value handle observes the fill because it is a live proxy into the shared state,
not a snapshot. `Shared::reader_at` checks the published `since` against the reader's
`as_of` and registers the reader's hold under one acquisition of the state lock, so the
trace cannot advance `since` between the check and the registration.

The point holds no trace agent and the trace reaches the point only through its
attachment, so no cycle keeps either alive. A trace published under several ids carries
one attachment per id. Once nothing but the trace references a point, the trace detaches
it at its next mutation (`SharedSpine::detach_unreachable`), since nothing can mint a
reader for it any more. When the trace drops, readers keep the `upper` it last
published rather than advancing to the empty frontier: a dropped writer says nothing
about the times past its `upper`, and completing would present them as empty.

Placeholder frontiers are `Antichain::from_elem(Timestamp::minimum())`, never
`Antichain::new()`. The empty antichain reads as sealed through the end of time,
which would make every snapshot wait vacuously true and return empty results.

### One lock for the chain and the feed

The trace enqueues a batch to every importer under the same lock in which it
publishes the chain containing that batch, so an importer registering concurrently
either seeds a chain with the batch or receives it through its queue, never both and
never neither. Frontiers follow the chain: the published `upper` is the last batch's
upper, and each enqueued batch is followed by the frontier that closes it, so an
importer's delayed capability never meets a frontier ahead of a batch below it. The
replay is incremental rather than a one-shot dump of the whole chain under a single
capability, which would be the record-doubling bug: the returned arrangement's stream
frontier tracks the trace's `upper`, so the trace never runs ahead of the stream and a
join counts each match once.

### Compaction

Publishing carries no compaction floor of its own. The wrapper sits where
differential's `TraceBox` calls the trace: `set_logical_compaction` and
`set_physical_compaction` arrive with the meet of the local agents' holds, the
controller's `AllowCompaction` among them through the trace manager, and the wrapper
applies the meet of that frontier and the readers' holds to the inner spine. The
published `since` and physical frontier are read off the spine after the call, so they
are the trace's own frontiers rather than an approximation of them, and a registering
reader cannot latch a `since` claiming accuracy at already-merged times. Nothing is
forwarded through the registry.

Readers' holds accumulate in two `MutableAntichain`s on the point, one per axis, and
each reader adjusts them by a delta the way a `TraceAgent` adjusts the `TraceBox`. A
reader whose adjustment moves the meet of the holds wakes the arrange operator through a
`SyncActivator`, whose next `exert` applies the new meet. A move that leaves the meet
where it was wakes nothing. With no reader hold on an axis the meet is the local frontier, so with
zero readers compaction follows the trace manager exactly, physical compaction included:
`TraceManager::maintenance` sets it to the trace upper to enable batch merging. An index
publishes two independent arrangements, so readiness and `since` gating operate on
`meet(oks, errs)`.

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

`SharedReader::import_frontier_core(scope, name, as_of, until)` bounds the import by
`until`. For a single-time read, `until = as_of.step_forward()`, the capability drops once
`upper` passes `as_of` and the read completes. An empty `until` performs no bounding and the
import stays live with the trace. The signature is the analogue of the maintenance path's
`TraceAgent::import_frontier_core(outer, name, as_of, until)`, into which maintained
dataflows pass an empty `until` routinely, and both are reached through one
`import_index` in `render.rs`, which dispatches on `IndexTrace::Local` or
`IndexTrace::Shared` and treats `SnapshotMode` the same either way.

The feed is live already. Every `insert` into the trace enqueues the batch and the frontier
that closes it to every registered importer and activates them.

**What makes interactive imports bounded is the class, not the import.** `import_index`
passes the dataflow's own `until`. Only `OneShotRead` dataflows render on the interactive
runtime, so that `until` is always one step past `as_of` and every import completes once
the trace seals past it. A dataflow with an empty `until` would follow the trace for as long
as it lived, and nothing at the import stops it.

The reason that matters is that it places the obstacle. Following imports are not the
hard part. See
[Compaction feedback flows through the reader's handle](#compaction-feedback-flows-through-the-readers-handle).

Import is pairwise: importer worker `i` reads publisher worker `i`, through the registry
the two workers share. That is sound only when both sides shard keys the same way,
`key.hashed() % peers`, with equal total peers, so `clusterd` asserts that the two runtimes
run equally many workers per process over equally many processes before it builds the
second runtime. `import_index` also asserts that the index's compaction frontier is at or
below the dataflow's `as_of`. For a peer index that frontier is the peer hold: a violation
means the controller offered an unreadable `as_of`, a protocol error, and the import must
panic rather than silently read coalesced data.

### Compaction feedback flows through the reader's handle

An arranged collection is two things: a stream of batches, and a handle to the trace. A
long-running dataflow generally needs both. A join looks up each side's incoming batches
against the *other* side's trace, so it holds a trace handle for its whole life and cannot
release it. Such an importer must instead **downgrade** logical and physical compaction as
its own frontier advances, or the trace can never merge or truncate in the background.
So compaction information has to flow backwards, from the importing runtime to the
publishing one.

That backward channel exists, and it is shared memory rather than protocol, which is why it
needs no new command.

* `SharedReader` implements `TraceReader`, and its `set_logical_compaction` and
  `set_physical_compaction` move the reader's hold in the point's accumulations and wake
  the arrange operator when the meet moves.
* The trace applies the meet of its local `TraceBox` frontier and those accumulations to
  the spine on that activation, and on every mutation after.
* Handles handed downstream behave normally. Differential's join and reduce downgrade their
  trace handles as their frontiers advance, and `TraceFrontier` forwards the downgrades
  through, so a well-behaved importer already causes the maintenance trace to compact.

**One frozen hold would defeat it**, because the target is a **meet** across all holds: a
single hold pinned at `as_of` dominates it no matter how far every other hold advances.
So the import keeps no separate hold. The read hold is each returned `Arranged`'s own
trace handle, registered at `as_of`, and the dataflow token retains only the peer bundle's
drop token, which keeps the registry slot alive. A consumer
that keeps the trace downgrades that handle as its frontier advances, and the trace
compacts behind it. For a single-time read the distinction is unobservable. For a
long-lived importer it is what lets the imported index's `since` advance for the
importer's whole life, so it is in place before any such importer exists rather than
retrofitted for one.

The prize is small and fixed, which is worth knowing before such an importer is built. The
concrete long-lived import today is the coordinator's four permanent introspection subscribes.
They are installed per replica, `catalog_serving.rs` refuses to auto-route anything with a
per-replica introspection dependency so they land on the busy replica, and they are
maintained work, so they render on maintenance. On an idle 8-worker replica they cost 4.9% of one core,
measured as process CPU with them on against off and confirmed independently by their own
`mz_scheduling_elapsed_per_worker`, and about 57 KiB of arrangement. During hydration of a
20M-row index they cost nothing measurable: the index dataflow's own scheduling stayed within
0.6% whether they were present or not. So moving them buys back a fixed fraction of a core per
replica and no hydration time.

Separately, and not a compaction problem: the import queue is unbounded with no
backpressure, deliberately, so that maintenance progress is never coupled to a slow reader.
A long-lived importer that lags holds a second copy of every undrained batch for as long as
it lags.
## The registry

`ArrangementSharingRegistry` (`src/compute/src/sharing.rs`) maps a `GlobalId` to a slot
holding the published `oks` and `errs` points of one index on one worker.

* **One registry per local worker ordinal.** `clusterd` creates them once, before either
  runtime, and worker `i` of each runtime holds registry `i`. Each registry has exactly one
  publishing and one reading thread, and `attach_publisher` and `attach_reader` assert it:
  two publishing workers would back one slot with different shards of an index, and a
  second reader would take over the wake, so the first one's waiting peeks would never
  wake.
* **A slot is one incarnation of an id.** The slot records the `as_of` of the dataflow that
  exports the index, and both runtimes look it up by the id and that `as_of`, which they read
  off the same `CreateDataflow`. A reconnect can recreate an index under its id with an
  `as_of` below the old incarnation's `since`, and the two runtimes reconcile in either
  order, so keying on the `as_of` keeps a read of the new incarnation off the old one's
  compacted chain.
* **Get-or-create is symmetric.** Whichever runtime touches an incarnation first creates its slot.
  The maintenance runtime publishing an index adopts the slot's points. The interactive
  runtime recording a peer index takes its peer bundle on the same slot, and when that
  happens first the bundle holds unbacked points that the later adopt fills in place
  rather than overwriting.
* **A slot lives exactly as long as someone holds it.** The registry maps ids to `Weak`
  slots and prunes dead entries whenever it creates one. The publisher holds a slot through
  the `PublishToken` in the index's `TraceBundle`, and the reading runtime through the peer
  bundle. Once neither does, the slot is gone and the trace detaches its points. So no
  withdrawal command is needed and a slot for an index whose creation never rendered
  cannot outlive its holders.
* **Every published id gets its own pair of points**, including an index that re-exports
  another index's arrangement. The point's writer frontier and peer holds are per
  collection, and the controller compacts two collections independently even when they
  share a trace, so a re-export publishes its own point over the shared trace and the
  trace applies the meet of both points' holds. On the interactive runtime an export
  whose arrangement is an imported shared one is the shared reader itself, so the
  runtime reports its frontiers off that reader.
* **A wake, not a dirty set.** A publication and every seal of either half unpark the
  registry's reading thread with `Thread::unpark`. The reading worker re-reads every
  waiting peek's trace each time it runs, before it parks, and the publisher updates the
  point before it wakes the reader, so no wake is lost: one that lands after the read is
  remembered by `unpark`, and the next park returns at once. `Thread::unpark` rather than a
  `SyncActivator`, because every timely allocator's `await_events` bottoms out in
  `std::thread::park`, and a root-path activator would additionally mark the worker's
  dataflows schedulable, which this wake does not need.

The registry's state sits behind one lock, held for a few map operations at a time.

## The interactive serving path

The interactive runtime serves every peek and renders every `OneShotRead` dataflow.

* **Fast-path index peeks** read the published arrangement through the same `IndexPeek`
  path the maintenance runtime uses for its local traces. An index's traces in the
  `TraceManager` are `OksTrace` and `ErrsTrace`, each an `IndexTrace` that is either
  `Local`, a trace this runtime maintains, or `Shared`, a reader of the other runtime's
  publication. The two share their batch type, so cursors over either are the same and
  dispatch costs one branch per trace call rather than per record. Everything past that
  point, the budgeted walk, the offload past the budget, the stash and the result limits,
  is one code path. A peek that cannot be answered yet waits in `queued_peeks`, which the
  worker re-reads on every sweep, and the registry's wake makes sure a sweep follows every
  publication. Once reads live in a separate runtime, the runtime itself is the isolation
  mechanism, and the walk substrate stays an independent choice on either runtime, which is
  the orthogonal axis above.
* **Slow-path query dataflows** import the maintenance arrangements as real arrangements
  and render joins and reduces over them. Importing as a real arrangement, not a
  substituted collection, is required for correctness. Downstream operators, delta joins
  especially, are rendered assuming the arrangement, its key, and its permutation exist.
  The shared provenance lives in the trace type rather than in a new `ArrangementFlavor`
  variant, so rendering sees an ordinary imported arrangement and the joins keep the arms
  they had.
* **Introspection reads the maintenance runtime's logging.** The interactive runtime
  renders no logging dataflow. It records every log index as a peer and serves
  introspection peeks from the indexes the maintenance runtime publishes, so introspection
  during hydration returns promptly, possibly stale, instead of blocking. Reconciliation
  pads the peer bundles of logging indexes as it pads local logging traces, because the
  maintenance runtime's padding stays on its own handles and never reaches the publication.
* **Late-bound imports, never a deferred build.** A query dataflow whose imported indexes
  are not yet published is built immediately anyway, against the real but empty points the
  peer bundle holds, which the maintenance trace later attaches to in place. Deferring the
  build would break the deterministic-construction principle above.

## The multiplexer

`src/compute-client/src/multiplex.rs` presents one controller endpoint over the two runtimes
and decides nothing about placement. One is built per controller connection.

* It forwards every command to both runtimes, maintenance first, except `Peek` and
  `CancelPeek`, which go to the interactive runtime, because in a two-runtime process it
  serves every peek. Each runtime decides from a `CreateDataflow`'s class whether it
  renders the dataflow.
* It merges the responses. Each runtime reports frontiers only for the collections it
  renders, which are disjoint, so frontier reports are forwarded verbatim.
* It does not deduplicate peek responses, and keeps no per-peek state. The
  exactly-one-`PeekResponse`-per-uuid contract is upheld below and above it, by the
  per-worker `PartitionedComputeState` in each process and the controller's per-process
  one. Peeks reach only the interactive runtime, so the multiplexer sees exactly one
  response per uuid and forwards it verbatim.

The ordering the controller relies on, that an index's `since` does not pass the `as_of` of a
dataflow importing it, follows from the broadcast, see
[Protocol invariants](#protocol-invariants).

## Roles and process globals

`ComputeRuntimeRole` distinguishes the runtimes, and each runtime turns its role into
components when it is built rather than branching on it in shared functions.

| Role | Placement | Publishes its indexes | Reads its peer's | Process globals |
|---|---|---|---|---|
| `Solo` | every class | no | no | applies them |
| `Maintenance` | `Maintained`, logging included | yes | no | applies them |
| `Interactive` | `OneShotRead` | no | yes | inherits them |

* `Solo` is the sole runtime of a single-runtime process and is behaviorally identical to a
  deployment from before this work. Its metric and log label is `None`, so a single-runtime
  registration collides with nothing and looks unchanged.
* `ProcessGlobals` owns the settings that have one value per process: lgalloc, the memory
  limiter, the columnation lgalloc region, the overflowing behavior, the pager and its
  buffer pool, arrangement dictionary compression, and the metrics registry's workload
  class label. Applying them from both runtimes would double-apply effects that are not
  idempotent or race the first. Both runtimes receive every `UpdateConfiguration`, so the
  applied values are the controller's configuration whichever runtime applies them, and a
  setting two runtimes could want to differ on still has one value per process.
* The interactive runtime's tracing span is named `compute-interactive` and its worker
  threads `interactive:<index>`, so profilers and logs tell the runtimes apart. Linux
  truncates a thread name to 15 bytes, which is why the prefix is short.

## Failure model

There is no cross-runtime lease. A process-global panic hook
(`mz_ore::panic::install_enhanced_handler`) is installed in `clusterd::main`
before either `serve` call, so a panic on any worker or reader thread of either
runtime aborts the whole process. A stuck or torn read hold can never outlive the
process, which is what makes the import hold safe without a lease-expiry
mechanism.

## Configuration

* `ENABLE_COMPUTE_INTERACTIVE_RUNTIME` (dyncfg, `mz-controller-types`, replica-scoped) is off
  in production. When it is on, the controller launches a replica with an `interactive` port
  and an `--interactive-compute-timely-config` argument, which configures the second runtime
  with the maintenance runtime's worker count and its own worker addresses.
* The dyncfg is read when a replica is provisioned, not applied to running ones, because the
  controller decides `ServiceConfig::ports` before the replica exists. Replica-scoped
  overrides are therefore resolved in `environmentd`. An existing replica keeps its old
  configuration until it is recreated, so the flip is not a live toggle and is not a rolling
  restart either. Plan it as a flip followed by an explicit recreation of every replica that
  should pick it up.
* `ENABLE_COMPUTE_INTERACTIVE_DATAFLOWS` (dyncfg, `mz-controller-types`, environment-scoped)
  is on by default. It is read when the controller creates a dataflow, so a change applies to
  dataflows created after it. Off, peek dataflows render as maintained work, see
  [The one-shot-read boundary](#the-one-shot-read-boundary). It is environment-scoped because
  a cluster-scoped value would have to be resolved when the query is planned, like the
  optimizer features.
* In CI both flags are `VariableSystemParameter`s defaulting to `true`, so sqllogictest,
  testdrive and the mzcompose suites provision two-runtime replicas, and the parallel
  workload flips them at random. Unmanaged replicas are launched by the test itself rather
  than by the controller, so the runtime flag never reaches them. A suite that wants the
  second runtime on one, as the feature benchmark does, configures its `clusterd` service
  directly, and the feature benchmark reads the flag from its effective system parameters so
  a `--this-params` or `--other-params` override reaches the replica it measures.
* The interactive port is named `interactive`, not something more descriptive, because
  Kubernetes rejects a container port name longer than 15 characters. The process orchestrator
  that local runs and mzcompose use has no such limit, so an over-long name passes every test
  and then fails to schedule in cloud.

## Non-goals

* `SUBSCRIBE` is out of scope. Only peek dataflows are `OneShotRead`. The import
  follows the dataflow's `until` and honours `SnapshotMode`, so a subscribe migration is
  a placement change plus the compaction feedback a long-lived importer needs.
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

The defects that could be verified against the code are fixed. These are the ones that
remain, kept here because each is a real hazard with a known mechanism rather than a
speculation, and each needs a decision rather than a patch.

* **Persist-backed reads are not isolated from hydration** (CPU-298). Persist bounds the
  bytes being fetched and parsed in a process with one first-come, first-served semaphore,
  sized to the memory limit times `persist_fetch_semaphore_permit_adjustment`, and a part
  holds its permits until it is consumed downstream. Both runtimes draw from it. On the
  saturated staging run a hydrating source held every permit, blocked acquisitions averaged
  about 88 s, and a peek dataflow over a persist-backed collection took 40 to 56 s while the
  interactive workers sat idle. Isolating these reads needs a budget of their own, carved out
  of the limit rather than added to it, or a priority for one-shot reads, which persist can
  already recognize by their finite `until`. A bounded hydration read-ahead (CPU-291) removes
  the cause but does not by itself isolate the reads, since several hydrating sources can
  still drain the one semaphore together.
* **Other process-wide resources are shared too.** The buffer pool's spill threads are one
  set per process, so during hydration they compress at full speed alongside the maintenance
  workers and compete with the interactive runtime for cores. The tokio runtime that drives
  persist I/O is shared the same way.
* **A peek dataflow on the interactive runtime has no cost bound.** It shares the
  interactive workers with the fast-path peeks, so one expensive peek dataflow can delay
  them. `enable_compute_interactive_dataflows` turns placement off as a whole, and nothing
  finer exists until there is a cost model.
* **The coordinator control plane is a parallel, unsolved bottleneck.** Peeks
  serialize behind DDL on the single coordinator thread, upstream of compute.
  Two-runtime fixes the data plane and does not touch this. For non-introspection
  reads under load the coordinator can dominate, so this must stay on the roadmap.
* **The interactive runtime is a single step loop.** Its read throughput has a
  ceiling, and a heavy scan can clog light point reads sharing the loop. A future
  admission or lane policy would protect a cheap-read lane and decide where
  expensive reads spill.
* **The interactive runtime is an introspection blind spot** (CPU-222). It runs with
  logging disabled and serves introspection from maintenance's published copies,
  which is what keeps introspection answerable during hydration. The cost is that
  the interactive runtime's own dataflows, arrangement sizes, and scheduling are
  not visible in introspection at all. Restoring visibility must not reintroduce
  the hydration-blocking the forwarding avoids, so the plan is for interactive worker
  `i` to forward its log events into maintenance worker `i`'s logging dataflow, with
  operator ids and dataflow indices offset into a range of their own, rather than turning
  its local logging back on.
* **Per-runtime memory attribution.** Arrangement-size introspection does not yet
  attribute memory per runtime, a specific case of the blind spot above.
* **Publishing an index costs nothing in reported arrangement size.** The sink-based
  publisher doubled it: with that publisher on, a published index reported twice the heap
  size, capacity, and allocations of the same index with the feature off, while its record
  and batch counts were unchanged (measured on a 16-worker replica: a one-record index
  reported 8740 bytes and 132 allocations against 4370 and 66). It was not the `Rc` to
  `Arc` migration, since an unpublished materialized-view arrangement is byte-identical
  either way, and not a reader, since it was present before anything imported the index.
  The trace wrapper adds no operator and holds the chain the spine already holds, and the
  doubling is gone with the sink. Measured on one process and one binary, alternating the
  replica-scoped dyncfg and taking a fresh 16-worker cluster per point, a published index
  reports the same size and the same allocation count as an unpublished one at 1, 1000,
  100000, and 1000000 records, so what the sink cost is neither a constant nor a slope now.
  `enable_compute_interactive_runtime` is read when a replica is provisioned, so
  `ALTER SYSTEM SET` plus a new cluster A/Bs it inside one build, which is what every earlier
  comparison lacked, including the staging run where E6 measured the reported size and the
  resident set coming slightly *down* with the flag on. `introspection-sources.td` asserts the
  one-record bound of 16 KiB with the flag on, and that assertion is what watches for a
  regression. `jemalloc_allocated` cannot resolve this: its deltas run 15 to 27 MiB against a
  200 MiB baseline with no consistent sign. The `ManyIndexesIdle` feature benchmark, 200
  published one-key indexes against a single-runtime image on one build, put clusterd's
  resident memory within 2% in two of three nightly runs and 16% above in the third.
* **An attached reader roughly doubles maintenance's arrangement-maintenance CPU, and
  the charge saturates at the first one.** Back-to-back one-shot joins over a published
  index leave the index's batch count, heap size, and capacity byte-identical to a quiet
  arm, so the merge schedule is untouched and nothing extra is retained. What changes is
  the work: `mz_arrangement_maintenance_seconds_total` under `role="maintenance"` ran
  0.13 to 0.16 CPU-seconds per second with readers attached against 0.05 to 0.07 quiet,
  while the interactive runtime's own arrangement maintenance stayed three orders of
  magnitude below that. Sweeping reader concurrency 1, 2 and 4, and adding an arm at
  concurrency 4 against a join a hundred times smaller, moves it 0.164, 0.144, 0.143,
  0.129: flat, and falling slightly as the workers get busier, since merge work is
  opportunistic `exert`. So it is not per read, per row, per dataflow install, or per
  reader, but a fixed 6 to 10% of one core for having any reader attached. That is about
  1% of what the reads themselves cost the process, so it bounds nothing. Every
  `set_physical_compaction` and `exert` on a `SharedSpine` runs `apply_holds` and then
  `publish_chain`, and a reader's handle moves `remote_physical`, which is the obvious
  suspect but is not confirmed. The measurement predates the writer waking only when the
  meet of the holds moves, so it is an upper bound on the current cost.
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
   equal total peer count, and `clusterd` asserts it. Peek serving has
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

   No pinning layout has been measured, so none is argued for here. What the code
   does is fixed: `set_core_affinity` maps the global peer index modulo the core
   count, both runtimes agree on that index, and both pass the same
   `worker_core_affinity` setting, so enabling the flag pins worker `i` of both
   runtimes to the same core. That layout keeps a pairwise read on the cache
   hierarchy and NUMA node where the publisher allocated the batches, and it puts
   two runnable threads on one core the scheduler can no longer balance apart when
   the interactive worker is busy. Pinning maintenance and floating interactive, or
   the reverse, trades those the other way. Which wins is an experiment: peek p99 and
   maintenance throughput under each layout, during hydration and quiet. Two
   platform facts bear on it. Sibling hyperthreads share execution resources, so a
   bandwidth-heavy maintenance worker degrades its sibling. And
   `core_affinity::get_core_ids()` enumerates logical CPUs with no guaranteed order,
   which is why the existing code sorts them, so whether the first N ids are N
   distinct physical cores is platform-dependent.
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

* Unit tests cover the sharing primitive and registry: the single-lock feed,
  placeholder-attached-late joins, cross-thread reads, the compaction invariants, a join
  and a reduce over a chain the writer's spine has merged across read at a stale
  `as_of`, a trace published under several ids, a hold registered below an attaching trace's
  `since`, incarnations of one id, and one publishing and one reading thread per registry. The multiplexer's broadcast and peek routing and the index peek sweep have
  their own.
* `compute_state` tests drive a maintenance and an interactive `ActiveComputeState` over one
  registry with the same command stream, which covers placement, peers and their holds, and
  peeks served from a peer index. `render` tests cover the shared import, joins over shared
  arrangements, the publisher's reserved peer hold in either order, and padded peer bundles.
* Four `clusterd-test-driver` specs, run at one and two workers, cover the runtime boundary:
  a fast-path read through a published index, a query dataflow that binds to an index before
  it has produced anything and resolves after, a read through an index that re-exports
  another's arrangement under a point of its own, and a query dataflow on the interactive
  runtime whose export is the arrangement it imports. They also run the reconciliation
  spec, which reconnects with peers on the runtime that does not render a dataflow. The
  driver marks a dataflow `OneShotRead` when it reads a single time, as the adapter does.
* `interactive_runtime.slt` pins the flag before creating a two-worker cluster and reads
  through indexes, re-exports, errors on both paths, a join that needs a peek dataflow, with
  `enable_compute_interactive_dataflows` on and off, strict serializable reads, and
  introspection relations.
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
  `MaintenanceUnderPeekSaturation` saturates every worker of an eight-worker cluster on a
  sixteen-core agent with peeks while a materialized view's freshness is measured from an
  idle cluster, which prices the oversubscription of two runtimes' worker threads.
  `FreshnessUnderPeekDataflows` runs peek dataflows next to a materialized view and reads
  the view from another cluster. `PeekIsolationUnderExpensivePeeks` and
  `FreshnessUnderPeekWalks` guard peek offload, so they turn the interactive runtime off for
  their own cluster: a second runtime would take the walks off the maintenance worker whether
  or not the offload bounds them. Scenarios whose measurement is peek service time read at
  SERIALIZABLE, because a strict serializable read waits on the frontier the contention
  stalls, which is M5 rather than the mechanism under test.
* The `bounded-memory` composition's `swap` workflow runs a replica whose arrangements
  exceed its memory limit by 1.5x, 2x, 3x and 6x with unlimited swap, and asserts that
  the read completes and that the replica's cgroup reports swap use, so the paged-out
  case has a regression test.

What two nightly runs on the CI agents measured, the stack against its merge base, reported
as p50 / p99:

| Scenario, measured query | Stack | Merge base |
|---|---|---|
| Point lookup under hydration churn | 3.3 / 28 ms | 186 / 1683 ms |
| Range count under hydration churn | 27 / 56 ms | 1635 / 4884 ms |
| Introspection read under hydration churn | 99 / 168 ms | 481 / 519 s |
| Hydration churn cycle, same scenario | 6.9 s | 277 s |
| Peek dataflow on a quiet replica | 19.3 / 24.9 ms | 20.6 / 29.9 ms |
| Materialized view read next to peek dataflows | 30.2 / 50.4 ms | 34.8 / 163.8 ms |
| Materialized view freshness under peek saturation | 194 / 1042 ms | 356 / 1282 ms |
| Join under peek saturation | 70 / 401 ms | 675 / 1227 ms |
| Point lookup behind expensive peeks, one runtime on both sides | 2.8 / 4.4 ms | 2.9 / 4.6 ms |
| Freshness probe under full index walks, one runtime on both sides | 9.3 / 14.0 ms | 9.3 / 14.0 ms |

The introspection row is the case the design is justified by: with one runtime the
introspection reads queued behind the churn for the whole load phase, and the churn itself
completed two cycles in the time the stack completed eighteen. The last two rows run a single
runtime on both sides and guard the peek offload, which is why they are flat.

## Implementation history

The feature was first built against a differential-dataflow fork that supplied
Arc-backed batches and a `sharing` module, stacked on an Arc-batches base branch.
The sharing primitive was then reimplemented natively in `mz_compute::shared_trace`
plus `mz_row_spine::ArcBatch`, and a lifecycle correctness redesign fixed a set of
concurrency bugs through a single-source publisher feed and placeholder-plus-adopt
construction. Then the fork was dropped entirely: the publisher's compaction
floor moved from the fork's `trace_box_unstable` read to the controller's
`AllowCompaction` and the stream upper, so the build depends only on released
differential-dataflow. Then the sink-based publisher gave way to `SharedSpine`, a
`Trace` wrapper that publishes from inside the trace's own mutations. The sink lagged
the trace by an activation, which is where the chain-retention and
stream-versus-trace-upper cases came from, and it computed `since` as a meet it could
only approximate. The wrapper reads both off the spine, and the registry stopped
forwarding the controller's frontier.

The read-hold protocol went through four mechanisms before the current one. Three
reconstructed the lost command ordering rather than restoring it, and the fourth restored
it for compaction alone. All are recorded under [Rejected alternatives](#rejected-alternatives).

Last, placement moved to the control plane. The multiplexer had decided placement from a
dataflow's shape, routed `CreateDataflow` by it, broadcast `AllowCompaction`, and kept a
per-connection set of the transient ids the interactive runtime owned. The controller now
tags each dataflow with its class, the multiplexer broadcasts every command and keeps no
state, and the runtime that does not render a dataflow records its exports as peers. The
standing hold became the peer hold, held in the `TraceManager` like any trace, so
`AllowCompaction` means the same thing on either runtime. The same rework removed the
registry's aliases, so a re-export publishes a point of its own over the shared trace,
replaced the registry's dirty set and per-worker wakers with an unpark of the one reading
thread, made the registry one per worker ordinal, folded the shared trace into the
`TraceManager` as `IndexTrace::Shared` rather than an `ArrangementFlavor` variant, and gave
the shared import the maintenance import's `SnapshotMode` handling for free. Role
branches in shared functions became components chosen at construction: `Placement`,
`ProcessGlobals`, the publisher, and the peer traces.

A review of the reworked stack found that the peer hold, taken when the reading runtime
applied the index's create, came too late when that runtime lagged by a whole create, and
that a reconnect could hand the new incarnation of an index the old one's slot. The
publisher now registers the hold, slots are keyed by incarnation, and the trace no longer
rewinds to a hold registered below its `since`.

The peek path was unified with the peek execution work: the interactive runtime's own
`PendingPeek` variant was folded into `IndexPeek`, so budgeting, offload and the stash apply
to shared and local traces alike.

The implementation is split into a stack of layers, each reviewable on its own and each
compiling and passing its tests without the ones above it:

1. the shared-trace primitive (#38386),
2. the per-worker sharing registry (#38387),
3. the dataflow class and the broadcast multiplexer (#38388),
4. publication of maintained indexes (#38389),
5. import of published indexes as a shared trace (#38390),
6. the second runtime, its placement, peers and process globals, and fast-path peeks on it
   (#38391),
7. the flag, on in test configurations, with the driver specs and the moved goldens
   (#38393),

followed by the benchmarks that guard the read paths (#38676), the ones that price
oversubscription and publication at scale (#38678), and the hold-gap metric (#38736). The
swap scenarios in `bounded-memory` are independent of the stack (#38683).

Every planning and evaluation document that preceded this one has been folded in or moved
out, so this directory holds one design document and nothing else. The planning documents
(`implementation-plan.md`, `stage2-detailed-plan.md`,
`arrangement-sharing-lifecycle-design.md`, `arrangement-sharing-lifecycle-plan.md`,
`read-holds.md`, `broadcast-compaction.md`, `pr-split.md`) are superseded: their task
checklists are executed and their fork-era mechanics no longer reflect the code. The
experimental evaluation moved to the project document named in the summary, and the peek
placement and stash-plumbing documents moved with the peek-offload work they belong to.
