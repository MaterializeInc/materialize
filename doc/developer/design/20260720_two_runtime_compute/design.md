# Two-runtime read isolation

## Summary

This document describes **two independent mechanisms** that were originally
conceived as one, and that measurement has since separated. Keeping them
distinct is the most important thing a reader can take from it.

* **Peek offloading** moves a fast-path index peek's *walk* off the serving
  timely worker onto a blocking task. It is a substrate choice for one kind of
  work, needs no second runtime, and is gated by `ENABLE_INDEX_PEEK_OFFLOAD`.
* **Dataflow offloading** runs a second, in-process "interactive" timely runtime
  that renders temporary dataflows and serves reads directly off the
  arrangements the "maintenance" runtime builds, zero-copy through a per-process
  sharing registry. It is a placement choice for rendered dataflows, and is
  gated by `ENABLE_TWO_RUNTIME_COMPUTE`.

They fix different problems, they are distinguished by different workloads, and
they should be adopted, flagged and rolled separately. See [Two mechanisms, not
one](#two-mechanisms-not-one) for what each one buys and the scenarios that tell
them apart.

The feature is gated by the `ENABLE_TWO_RUNTIME_COMPUTE` dyncfg, off in
production and on by default in CI. With the dyncfg off, a replica runs a single
`Solo` runtime that takes the same code paths, with no sharing registry, no second
runtime, and no `role` metric label. It is not a byte-identical deployment: the
`Rc` to `Arc` spine migration is unconditional and applies with the feature off,
which the goldens show (`relations.slt` prints batch type names, and the `ii_t4`
arrangement-size bound moved).

This document is the single design of record. It supersedes the four planning
and design documents that preceded it in this directory (see [Implementation
history](#implementation-history)).

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

| Symptom | Evidence | Mechanism |
|---|---|---|
| Peeks queue behind other peeks on a busy replica | measured, E1: 6163 ms worst case at a 2170 ms walk | M1 |
| `WHERE key = <lit> ORDER BY .. LIMIT 1` on a skewed key stalls every lookup behind it | reported from the field, then measured, E11: 58 of 261 over 200 ms becomes 0 of 261 | M1 |
| A point lookup on a resident index stalls behind walks of a swap-resident one | measured, E8b: 29.2 s worst case becomes 152 ms | M1 |
| A peek runs to completion once started and cannot be cancelled | confirmed in the code | M1 |
| A swap-resident walk is slower and far less predictable inline than offloaded | measured, E8b: 2.3 s against 3.6, 4.7 and 56.4 s, **mechanism unattributed** | unattributed |
| Peeks show jitter on a replica managing large state | measured, E12: a 3.9 ms lookup reaches p90 129.5 ms and p99 278.4 ms, with 17.1% of requests above ten times the idle median | M2 |
| Interactive dataflows are slow, and introspection is unavailable, while a replica is busy | measured, E7 and E9: 2835 to 1456 ms, and 4.4 to 7.5 s polls to about 160 ms | M3 |
| A temporary dataflow costs about 900 ms to create and tear down | measured, E7: a floor of 850 to 950 ms in every cell, quiet or loaded, either runtime, for about 120 rows | M4 |
| A read cannot be answered until the frontier passes its timestamp | measured, E12: with the peek moved off the busy worker, strict serializable still reaches p99 185.8 ms against 5.8 ms at serializable. Also bounded by E9's staleness column, 170 to 1589 ms while hydrating | M5 |
| Peeks serialize behind DDL on one coordinator thread | asserted elsewhere in this document, not measured here | M6 |
| A default-isolation read pays a timestamp-oracle round trip | not measured here | M7 |
| One expensive query makes every object on the replica look stale | **not measured**, E13 running with predictions registered | M8 |

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
  then delays the next strict serializable read.

### What each solution reaches

`argued` means derived from the mechanism and not measured. Note how many cells
that is.

| | M1 | M2 | M3 | M4 | M5 | M6 | M7 | M8 |
|---|---|---|---|---|---|---|---|---|
| S0, another replica | yes, statistically | yes, statistically | yes | no | **yes** | no | no | masks it, `predicted` |
| S1, cooperative peek slicing | yes, `argued` | **no** | no | no | no | no | no | **little or none, `predicted`** |
| S2, cancellable peeks | the cancellation symptom only | no | no | no | no | no | no | for cancelled peeks, `argued` |
| S3, interactive dataflows on a second runtime | no | no | **yes**, measured E7/E9 | no | no | no | no | for dataflow-caused occupancy, `argued` |
| S4, peeks routed to the interactive runtime | no, it relocates the queue | **yes**, measured E12: p90 129.5 to 4.5 ms | no | no | no | no | no | **yes, `predicted`** |
| S5, peeks on another thread | **yes**, measured E1/E11/E8b | **no, measured worse**, E12: p90 148.2 against 129.5 | no | no | no | no | no | **yes, `predicted`** |
| S6, budgeting long operator activations | no | yes, `argued` | partial, `argued` | no | no | no | no | no |
| S7, a bounded-seek plan for the skewed case | removes the work, `argued` | no | no | no | no | no | no | removes the work, `argued` |
| S8, a re-entrant point-lookup structure | yes, `argued` | **yes**, `argued` | no | no | no | no | no | yes, `argued` |

**M8 is predicted to invert the M1 ordering, and that is the point of measuring it.**
On M1, cooperative slicing wins because it costs no core and bounds the victim's
wait. On M8 it is predicted to buy little or nothing, because the total worker time
the walk consumes is unchanged and a frontier cannot advance past data that has not
been processed. With `peek_yielding_total` at `work:1000000,time:100` against roughly
one step per pass, peeks take on the order of 99% of the worker while a scan runs, so
the expected shape is a ramp to a similar peak rather than a step to it. Removing the
work from the worker, whether by another thread or another runtime, is the only thing
predicted to hold the frontier moving. If that holds, **neither mechanism dominates
across both dimensions, which is the strongest argument available for landing both**,
and it restores a justification the offload lost on the peek-latency side. E13 is
running against exactly these predictions.

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

**S1 reaches M1, and its quantum should be stated rather than characterized.** PR
#38040 ships `peek_yielding = work:100000,time:10` per peek per activation and
`peek_yielding_total = work:1000000,time:100` across all peeks, with a round-robin
resume so a peek that missed its turn goes first next time. Applied to this
document's own fixtures: on E1's three concurrent 2170 ms scans a point read is
reached within one activation, so its added wait is roughly 30 to 40 ms against the
offload's measured 184.5 ms; on E11 it is roughly 10 to 20 ms against 109.2 ms. **So
on the two fixtures that carry the peek argument, S1 is predicted to match or beat
S5.** The quantum floor rising with dataflow and worker count bounds S1's
*throughput* overhead, not the victim's delay, and an earlier draft conflated the
two.

**S1 also reaches the swap case, and the residual claim for S5 is narrow.** A sliced
swapped walk yields between chunks, so the victim gets in. That no scheme yields out
of a single page fault is true and irrelevant to a 29 s stall, which is tens of
thousands of faults. What is left uniquely to S5 is the swapped walk's *own*
duration, 2.3 s offloaded against 3.6, 4.7 and 56.4 s inline, and this document
states elsewhere that this effect is unattributed and that preemption cannot explain
it. **So S5's unique justification currently rests on one unattributed measurement
and on an unmeasured claim about S1's quantum floor.**

**Nothing here reaches M4, M6 or M7**, and M4 is measured and dominant for the
workload S3 is justified by.

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

* **S1 is the right default.** It costs no core and no thread, it reaches M1, it is
  predicted to match or beat S5 on both fixtures this document leans on, and it
  removes S5's cliff for free by making the fallback path preemptible.
* **S5's unique justification is now narrow, and narrower than earlier drafts of
  this document claimed.** It does not reach M2, and E12 measured it 17% worse there
  than doing nothing. On M1 it competes with S1 and is predicted to lose on E1 and
  E11. What is left is the swapped walk's own duration, which is unattributed, and
  the possibility that S1's quantum floor is too high on a busy replica, which is
  unmeasured. It should not be described as the peek fix until one of those two is
  established, and if it ships it wants a routing rule that keeps it away from M2.
* **S2 is real under S1 and cosmetic under S5**, the reverse of what an earlier
  draft said. A started `spawn_blocking` closure cannot be aborted, as this
  document's own follow-up list records, so on the offload path a cancel flag ends
  the client's wait while the CPU is still spent and the compaction hold is still
  held for the full walk. Under S1 the scan stops.
* **S3 stands alone on M3, and its structural argument is stronger than its
  measurements.** Cooperative yielding needs a yield point retrofitted per operator.
  `linear_join_yielding` covers linear joins and `storage_source_decode_fuel` covers
  the persist decode, while nothing covers reduce, top-k, arrange, threshold or
  delta joins. Coverage grows one operator at a time and never completes, whereas a
  second runtime covers every operator at once. But **M4 is the dominant term for
  the workload S3 is justified by**, and nothing here addresses it.
* **S4 is not separable and should not be cut.** It is how peeks are routed once
  two runtimes exist, and it is the only shipped mechanism that reaches M2.
* **S0 is the baseline these have to beat**, and the only one that reaches M5.
* **S7 deserves a check before the field case is used as an argument.** The skewed
  lookup costs what it costs because a minimum over one key's millions of values is
  a linear walk. An index on the key and the ordering column together would make it
  a bounded seek, which removes the work rather than relocating it. This document
  calls that fixture the strongest argument for enabling the offload by default
  without asking whether the query has an index problem.

Three things would still change this table, in order of how much.

1. **S1 measured rather than argued**, on E1, E11 and E12, against the predictions
   above. It is the only unmeasured row that could displace a shipped mechanism, and
   on E12 it is the one open question: slicing lets the *peek* yield, which does
   nothing for a peek stuck behind an operator, so S1 should look like the inline arm
   there. If it does, M2 belongs to placement alone.
2. **A single-phase rerun of E2.** Its null is the basis for calling the second
   runtime inert for peek latency, and it was measured across two deployments with
   the stash off. E7 had the same confound and was rerun; E2 was not.
3. **The strict serializable arms for E1, E11 and E8b.** E12 supplies the level for
   one fixture and shows M5 is only visible once M2 is removed, so the others are
   worth the repeat rather than urgent.

E12 settled what was item one on this list. Its result inverted two cells and cost
S5 its remaining claim on M2.

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

### Two mechanisms, not one

The original thesis was that a second runtime delivers read isolation, and that
peeks were one of the reads it would isolate. Measurement did not support the
second half. The two mechanisms turn out to be independent, and only one of them
is needed for peeks.

| | Peek offloading | Dataflow offloading |
|---|---|---|
| What it changes | which thread walks a fast-path index peek | which runtime renders a dataflow |
| Unit of work | one peek's cursor walk | a whole temporary dataflow |
| Fixes | head-of-line blocking between peeks on one worker | interference between maintenance work and interactive rendering |
| Needs the second runtime | no | yes, it *is* the second runtime |
| Deployment cost | a dyncfg, no restart | a port, a fleet roll, doubled timely worker threads, the sharing registry, and the command-ordering invariant and capping that go with it |
| Flag | `ENABLE_INDEX_PEEK_OFFLOAD` | `ENABLE_TWO_RUNTIME_COMPUTE` |

Peek offloading applies on either runtime, so the two compose. The single place
they meet is serving a peek from the sharing registry, where an interactive peek
takes an owned cursor over a published arrangement and may then be walked on a
blocking task like any other.

#### The scenarios that tell them apart

Each row is a measured experiment; the numbers and method are in
`evaluation.md`. What matters here is which mechanism moves the result.

| Scenario | Peek offloading | Dataflow offloading |
|---|---|---|
| Point lookups behind concurrent full scans, walk cost swept 23 ms to 2170 ms | fixes it: tail flat at 185 ms against 6163 ms | no effect |
| `WHERE key = <lit> ORDER BY .. LIMIT 1` where one key holds millions of values | fixes it: 58 of 261 lookups over 200 ms become 0 of 261 | no effect |
| A walk over an arrangement resident in swap | fixes it, by the largest margin measured: 29.2 s worst case becomes 152 ms | no effect |
| Late-materialization join while a 60M-row index hydrates | cannot apply, this is a dataflow and not a peek | fixes it: tail halves, 2835 ms to 1456 ms |
| Introspection and `EXPLAIN ANALYZE` while a replica hydrates | cannot apply | fixes it: 4.4 to 7.5 s polls become about 160 ms |
| Observing the shape of hydration memory at all | cannot apply | enables it: at 5 s per poll the sawtooth is invisible and reads as a smooth ramp |
| A point lookup on a replica taking 500k-row insert batches | **makes it worse**: p90 148.2 against 129.5 ms | fixes it: p90 4.5 ms, matching a write-traffic-only control |

No row is moved by both, and the last row is moved in *opposite directions*. That is
the fork. Note that the last row is a peek fixed by the runtime and made worse by the
substrate, so the fork is not "peeks against dataflows". It is which queue the work is
stuck in.

#### Why the fork exists: preemption, not capacity

The fork is not an artifact of which experiments happened to be run. It follows
from there being two independent properties, and each mechanism supplying exactly
one of them.

*Preemptibility* is whether a short request can displace a long one that is
already running. In a non-preemptive work-conserving server, what a short request
waits for is the residual of the job in progress. Mean waiting time in an M/G/1
FCFS queue is `λE[S²] / (2(1-ρ))`, so it depends on the second moment of the
service-time distribution *and* on arrival rate and utilization. A timely worker's
per-activation service time spans six orders of magnitude, from a point lookup to a
full arrangement scan, so the second moment is enormous, and what that buys is a
waiting-time tail whose *shape* is set by the residual rather than by how loaded the
server is. The arrival rate still decides how many requests are victims, which
E11's write-up states correctly in those terms. E11's timeline is the signature.
Lookups arriving behind the skewed one complete at a fixed instant rather than after
a fixed delay, 2019.9 ms then 1921, 1821, 1721 and downward to 235.8 ms. Every
arrival is waiting for the same completion, which is residual service time and not
contention.

Two remedies exist for that, not one. Preempt the long job, or dispatch on size so
it never lands in front of a short one. This document reaches for the second in
[the incremental path](#the-incremental-path-from-here) and should not claim the
first is the only option.

*Capacity* is whether a core is free to run on. It is a separate question and
neither mechanism supplies it.

Peek offloading is best described as adding a *server* rather than as adding
preemption. The walk itself never yields. What changes is that it runs on a
different OS thread, so the interleaving is delegated to the kernel, and how well
that works depends on runnable threads against available cores. That is why the
measured 33x arrives on a replica with CPU headroom, and why the behavior on a
CPU-saturated box is a separate question. **That question is unmeasured.** The
experiment for it was planned and not run.

A second timely runtime adds another non-preemptive server too, so for peeks it
relocates a queue rather than removing one, which is what the direct A/B found. That
A/B is itself weaker than it reads, see the rerun listed in
[Problems and mechanisms](#problems-and-mechanisms). Where the second runtime does
win, the unit of work is a rendered dataflow, and there the problem was never
queueing behind one long activation. It was that the only dataflow scheduler
available was saturated.

Read that way, there are exactly two ways to get a short peek past a long one, and
an OS thread is one of them rather than the only one. Three schedulers exist in the
process. Timely yields at operator boundaries, tokio yields at await points, and a
cursor walk offers neither, so an uncooperative walk can only be displaced by the
kernel. The alternative is to make the walk cooperative by chunking it and checking
for other pending work every so many rows, which needs no thread but does need a
re-entrant cursor position and a quantum to tune. **That alternative is implemented**
and is scored as S1 in
[Problems and mechanisms](#problems-and-mechanisms), where it is predicted to match
or beat the thread on the fixtures measured here. Chunking a peek walk is also not
the same move as
[yielding in maintenance](#why-not-yield-for-interactivity-in-one-runtime), which
is ruled out on different grounds. A peek walk consumes no input and consolidates
nothing, so run-to-completion is not part of its contract.

#### Implications

**Of the two mechanisms in this document, peek offloading is the cheaper one.** It
carries the measured peek benefit here, it costs a thread and a dyncfg, and it needs
no fleet roll. A direct A/B found the second runtime adds nothing on top of it for
peek latency, single-runtime-with-offload and two-runtime-with-offload being close
at every scan cost tested, though that A/B spanned two deployments and wants a rerun.

NOTE: this comparison is between the two mechanisms *in this document* and is not a
claim that peek offloading is the best available answer to peek latency. Cooperative
slicing is a third option, it is implemented outside this branch, and it is predicted
to match or beat the offload on both fixtures measured here at a fraction of the
cost. See [Problems and mechanisms](#problems-and-mechanisms) for the scoring and for
what would have to be measured to settle it.

**The second runtime's justification is temporary dataflows and observability,
not peek latency.** Any review that judges the sharing registry, the protocol
invariant and the capping against a peek-latency claim is judging them against
the wrong benefit. The claim to defend is that a replica stays useful, and stays
*introspectable*, while it is busy maintaining collections. The observability
case is the strongest form of it: a replica that cannot answer introspection
while something is wrong with it cannot be diagnosed while something is wrong
with it, and the alternative to an answer is not a slower answer but a
misleading one.

**Neither mechanism adds CPU**, but they degrade differently under saturation.
Peek offloading moves a walk to a thread that still needs a core, so on a
CPU-bound box it only reorders work. Its best case is therefore a walk that is
*blocked* rather than computing, which is why the swap result is its largest
margin. The second runtime doubles timely worker threads at every replica size,
so it is not free even when idle, and that doubling is fixed by the equal-peer
requirement rather than chosen, so it is not a knob to turn down.

**The routing policy should follow the fork.** Sending everything to one place
throws away the distinction: peeks want a substrate, rendered dataflows want a
runtime.

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
[Two mechanisms, not one](#two-mechanisms-not-one).

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
blocking pool default. Nothing accounts for this, and it argues for the dedicated
pool in the follow-ups rather than against it.

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

### The fix: make the requirement visible on the governing stream

The multiplexer is the one place that observes both runtimes' command streams, and its send path is
sequential. So it can restore the ordering without a new command at all.

When it routes `CreateDataflow(D, X, imports = [I])` to interactive, it first records a hold on each
import at `X`. Any later `AllowCompaction(I, F)` it sends to maintenance is capped at the lowest
`as_of` any in-flight interactive dataflow still reads `I` at, and the uncapped frontier is
remembered. When `D` is dropped, which the controller signals by allowing `D`'s export to compact to
the empty frontier, its holds release and the deferred compaction is forwarded so `I` is not pinned.

Under-compacting is always safe, so the capped value needs no agreement from the controller: the
controller's frontier accounting is unchanged, and the replica simply keeps more history than it was
told it could discard.

Capping rather than a new `HoldFor` command matters for a multi-process replica. Only `Hello` and
`UpdateConfiguration` are broadcast to every process; every other command, including both
`CreateDataflow` and `AllowCompaction`, goes to process 0 and reaches the other processes through the
intra-runtime channel. So process 0's multiplexer sees the whole ordering problem and can fix it
alone, and a hold command would have had to travel the same path to no additional effect.

This restores I1b by construction, on the stream where the compaction is actually applied. It keeps
maintenance decoupled from interactive: maintenance never waits for interactive to make progress, it
only learns earlier what interactive will need. It generalizes to I2's other two rows, since a hold
that pins `since` can pin slot identity as well.

The alternative considered and rejected was an in-process sequence barrier, where interactive
publishes the command sequence number it has processed and maintenance defers publishing a
compaction until interactive passes it. It needs no protocol change, but it fails on two counts. It
couples maintenance's compaction to interactive's progress, which the non-goals refuse. And a replica
can span processes, so an in-process barrier cannot observe the runtimes in the other processes at
all, which makes it not merely undesirable but insufficient.

### A model to check it against

`protocol.tla` in this directory sketches the protocol: one index, one interactive dataflow, the
controller's read hold, two independent command streams, and the point at which each runtime realizes
a command. I1 is stated as an invariant and the capping is a constant.

**It has not been run.** No TLC configuration is committed and no model-checking run has been
performed, so the file is a specification sketch rather than a verification result. Do not cite it as
evidence that I1 holds.

Review of the sketch against the implementation found it unfaithful in ways that matter, and the
discrepancies point at real gaps rather than at modelling detail:

* There is no `deferred_compaction` variable, so the model cannot express the implementation's release
  path at all. `CtlDrop` clears the hold while leaving `dfAsOf` and `rendered` untouched, which admits
  the trace `CtlCreate(1)`, `CtlDrop`, `CtlCompact(2)`, `MaintStep`. That ends with an unrendered
  dataflow at `as_of` 1 and `since` 2, so **the committed model refutes I1 with capping on**. That
  counterexample is not a modelling artefact, it is the release-ordering hole described below.
* `NoPermanentPin` is vacuous. `dfAsOf` is never reset, and `CtlCreate` requires it unset, so only one
  dataflow ever exists and the property's antecedent is false forever.

The model is still worth keeping, because the failure it is about is an interleaving and interleavings
are what review misses. But it needs to be corrected and actually checked, with a committed TLC
configuration, before any claim rests on it. The remaining rows of I2, index replacement under
reconciliation and placeholder eviction, should be added once it models the implemented algorithm.

## The bounded-read boundary

The interactive runtime serves a read only when it is bounded, meaning its
`until` is a finite frontier, it is not a `SUBSCRIBE`, and it is not a
`COPY TO`. Everything else runs on the maintenance runtime.

The routing predicate keys on `until`-finiteness, not on whether the target id is
transient. A maintained index has an unbounded `until` and so always lands on
maintenance regardless of catalog transience. `COPY TO` is bounded but still
excluded, for reconciliation and S3-sink reasons rather than frontier reasons. A
mixed or non-homogeneous dataflow is treated as maintained by construction, which
is the safe default.

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

A publication point is an `Arc<SharedTrace>`. It can be created empty as a
placeholder and later adopted by a publisher in place, filling the same `Arc`.
This is what makes arrival-order construction work. A differential import captures
its input trace by value at construction time, so the import must have a real
trace to hold even before the arrangement it reads exists. `Published::placeholder`
gives it one. A later `PublishArrangement::adopt` installs the real publisher into
that same point, and the by-value handle observes the fill because it is a live
proxy into the shared state, not a snapshot.

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
* Physical compaction follows the stream `upper`, mirroring
  `TraceManager::maintenance`, which sets physical compaction to the trace upper
  to enable batch merging.

With no reader hold on a dimension, the target follows that writer floor, so with
zero readers compaction follows the writer. The published `since` is the meet of
the publisher's post-forward hold and the writer floor, which keeps a registering
reader from latching an anti-conservative `since` that claims accuracy at
already-merged times. An index publishes two independent arrangements, so
readiness and `since` gating operate on `meet(oks, errs)`.

### Bounded import, not live replay

`SharedTraceHandle::import_snapshot_at` imports the shared arrangement as a static
snapshot at `as_of`, bounded by `until`. The interactive runtime only ever answers
bounded reads, so a live-following import would track the source's live frontier,
gain nothing over the maintenance seal rate, and still consume interactive-lane
resources. The unbounded live `import` was dead code and was removed.

Import is pairwise: importer worker `i` reads publisher worker `i`. That is sound
only when both sides shard keys the same way, `key.hashed() % peers`, with equal
total peers, so `import_snapshot_at` asserts equal peers and panics otherwise. It
also asserts `since <= as_of`: a published slot whose `since` already sits above
the requested `as_of` means the controller offered an unreadable `as_of`, a
protocol error, and the import must panic rather than silently read coalesced
data.

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
  The per-step `process_peeks` scan is removed on the interactive runtime.
* **One close, no withdrawal command.** An adopted point closes when its publisher
  drops, so no explicit withdrawal command is needed. A placeholder that is never
  adopted, because the index creation it anticipated was cancelled, leaves an empty
  slot in the registry for the life of the process. Correctness does not depend on
  reclaiming it: whether an imported index may compact or drop rests on the
  controller's read-hold discipline, and the leaked slot is an empty publication
  point, not a retained arrangement. Reclaiming it would need a reader-teardown
  hygiene path that does not exist.
* **Re-exports.** A `Trace` re-export, where one index aliases another's
  arrangement, shares the existing `Arc` under the new id rather than
  republishing. The source's seal signal wakes the re-export transitively.

## The interactive serving path

The interactive runtime serves everything through the registry.

* **Fast-path index peeks** read the published arrangement directly, served inline
  on the interactive runtime's own worker step (`PendingPeek::IndexShared`). An
  earlier plan offloaded the arrangement walk to a tokio task off a maintenance
  worker. That was abandoned: once reads live in a separate runtime, the runtime
  itself is the isolation mechanism, and no async hand-off is needed.
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

* It routes peeks and one-shot work to interactive, maintained work to
  maintenance, and lifecycle commands to both.
* It does not deduplicate peek responses, and keeps no per-peek state. The
  exactly-one-`PeekResponse`-per-uuid contract is upheld below and above it, by the
  per-worker `PartitionedComputeState` in each process and the controller's
  per-process one. Peeks route only to the interactive runtime, so the multiplexer
  sees exactly one response per uuid and forwards it verbatim.
* It forwards each collection's `Frontiers` only from the runtime that owns the
  collection. Both runtimes install the internal logging dataflows, so without
  this rule the interactive runtime's empty copies would regress the controller's
  per-collection frontier. State: `transient_owner` maps a `GlobalId` to its
  owning runtime.

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

* `ENABLE_TWO_RUNTIME_COMPUTE` (dyncfg, `mz-controller-types`) is off in
  production and on by default in the variable CI system parameters, so the suite
  exercises the two-runtime path broadly. The compiled default stays off, so
  production is unaffected.
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

* `SUBSCRIBE` is out of scope. All interactive work is single-time, so the shared
  import applies no `until` or `as_of` coalescing. A future subscribe migration
  must add it.
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

* **A reader's physical hold on a published trace is inert, so the publisher's
  spine can merge across a reader's cut.** `TraceAgent::set_physical_compaction`
  joins monotonically, and with no readers the publisher forwards the stream upper
  as its physical fallback, so a reader registering a hold below that has no
  effect. A merge completing inside one join invocation can then make
  `batches_through` observe a batch whose bounds straddle the reader's cut, which
  is a hard assert and, under shared fate, aborts the process. The logical
  dimension is correct for the mirror-image reason. This is the most serious open
  item and it is in the sharing primitive.
* **Releasing an interactive hold is ordered against command enqueue, not against
  the interactive runtime having rendered.** `send` is an unbounded push with no
  ack, so maintenance can be told to compact before interactive dequeues the
  create it is holding for. Capping is safe without an ack because the frontier is
  withheld entirely; releasing is not. The correct release point is a signal from
  the interactive runtime, which the current protocol does not have. This is the
  same class of bug as the one the capping was introduced to fix, one level up.
* **`compaction_floor` is never evicted**, so the multiplexer retains one entry per
  collection id ever seen, including every transient peek dataflow, for the life of
  the connection. The neighbouring `transient_owner` is evicted precisely to avoid
  this.
* **A peek against a dropped or never-published shared id hangs silently.** The
  registry returns no handle, the peek reports not-ready and is re-enqueued, and
  nothing will mark it again. The local path fails loudly on the same condition.
  Never-adopted placeholders are also never evicted.
* **`reexport`'s failure is discarded at both call sites**, so an alias that was
  never established becomes a permanently unresolvable peek rather than an error.
* **The in-flight offload cap is invisible.** A walk declined because the cap is
  reached increments the same counter as one declined because the flag is off, so
  saturation cannot be distinguished from the feature being disabled. It wants a
  gauge.
* **`protocol.tla` does not model the implemented algorithm.** It has no
  `deferred_compaction`, its `CtlDrop` leaves the dataflow live, and in that state
  it refutes I1 *with capping on* by exactly the release-ordering trace above. Its
  liveness property is vacuous because the model admits only one dataflow. It needs
  correcting and actually running, with a committed TLC configuration, before
  anything rests on it.
* **The stash diversion has no test.** The equivalence test that compares an
  offloaded walk against an inline one passes `want_stash: false` in every arm, so
  the diverting path and `upload_blocking` are unexercised.

* **The coordinator control plane is a parallel, unsolved bottleneck.** Peeks
  serialize behind DDL on the single coordinator thread, upstream of compute.
  Two-runtime fixes the data plane and does not touch this. For non-introspection
  reads under load the coordinator can dominate, so this must stay on the roadmap.
* **The interactive runtime is a single step loop.** Its read throughput has a
  ceiling, and a heavy scan can clog light point reads sharing the loop. A future
  admission or lane policy would protect a cheap-read lane and decide where
  expensive reads spill.
* **Peek placement is static.** All peeks go to interactive today. Whether some
  should be admitted to maintenance (work-stealing when interactive saturates and
  maintenance idles) is a deferrable optimization. It is purely additive and
  changes no correctness. The signal to build it is a measured workload that
  saturates the interactive loop while maintenance sits idle. Routing a read to
  maintenance trades against the isolation guarantee, so the policy is
  workload-dependent, isolation-first for latency-SLA reads and
  placement-for-throughput for bulk reads.
* **Thread names collide across runtimes.** Timely names worker threads
  `timely:work-{index}` by a per-instance-local index, so the maintenance and
  interactive runtimes (and storage) emit identical OS thread names in one
  process. Profilers cannot tell them apart by name. A per-worker rename in the
  worker entrypoint would fix it, and would fix the pre-existing storage and
  compute collision as a side effect.
* **The interactive runtime is an introspection blind spot.** It runs with
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
  somewhere in the publish path. Whether that is a reporting artifact or real
  retention decides whether the feature carries a memory regression, so it should be
  settled before the flag is considered for production. `test/testdrive/introspection-sources.td`
  carries the raised bound and a pointer to this entry.
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

What is on the branch is deliberately the least opinionated version of the idea.
It moves a walk to another thread and it counts which substrate ran it. It picks
no pool sizing policy, asserts no scheduling priority, measures no shared-cache
interference, and encodes no routing policy beyond a flag. That is the right first
step rather than a shortcut, because each item below is a separate decision with
its own evidence requirement, and none of them has to land with the first one.
Recorded here so the sequence is a plan rather than a rediscovery.

Ordered by expected value per line of change.

1. **A dedicated bounded pool for interactive work, with a chunked and cancellable
   walk.** The walk runs on tokio's blocking pool, which is built for IO-blocking
   work and is documented as unsuitable for CPU-bound work. Three consequences.
   The pool is effectively unbounded at its 512-thread default, so the in-flight
   limit has to be a hand-maintained counter rather than queue depth, which is
   where a leak already occurred and was fixed. The pool is shared with persist's
   blocking IO, so a burst of walks and a burst of blob operations contend with no
   discipline and no way to express a preference. And a started blocking closure
   cannot be aborted, so a cancelled peek still spends its full walk and still
   holds its compaction hold for the duration. That last one matters most:
   cancellability is the one structural advantage interactive work has over
   maintenance work, and the current substrate discards it. A fixed pool behind a
   bounded queue makes the limit structural, unshares from persist, and gives the
   walk somewhere to poll a cancel flag.
2. **Per-walk fault and context-switch accounting.** `getrusage(RUSAGE_THREAD)`
   bracketed around the walk in both arms, reported under the `substrate` label
   the walk counter already carries. `src/metrics/src/rusage.rs` already does the
   same call with `RUSAGE_SELF`, so the dependency and the pattern exist. This is
   worth doing early because E8b is the largest margin on the branch and its
   mechanism is unattributed. The offloaded walk was not merely more predictable
   but faster on the same data at the same swap depth, 2.3 s against 3.6, 4.7 and
   56.4 s, and preemption cannot explain that, because preemption governs the
   other peeks' latency rather than this walk's own duration. Equal major-fault
   counts with different durations would mean fault queue depth, since swap-in is
   a synchronous per-thread major fault and concurrency comes only from the number
   of threads faulting at once. Inline taking *more* faults for the same walk
   would mean the interleaved timely working set is re-evicting the walk's pages,
   and the lesson would be locality rather than parallelism. The two point at
   opposite pool sizings, so this measurement has to precede item 1's sizing
   decision. It also settles whether `index_peek_offload_max_inflight` has one
   sensible default at all, since a swap-bound walk is blocked rather than
   computing and wants a limit far above the core count that a resident walk
   wants.

   The counter is unambiguous here in a way it would not be elsewhere.
   `ru_majflt` counts any read from backing store, but `clusterd` uses no
   file-backed mappings for arrangement data, so every major fault it takes is
   anonymous swap-in and no disentangling is required.

   The same measurement points at a remedy that neither a pool nor a thread count
   can reach. A swap-in is synchronous *because* it is a fault, so the queue depth
   a walk achieves is bounded by how many threads are faulting. Prefetching the
   next batch's region with `madvise(MADV_WILLNEED)` while consuming the current
   one converts those faults into asynchronous readahead, which raises queue depth
   from one thread and needs no sizing decision at all. That attacks the mechanism
   rather than buying depth with threads, and it is testable against the E8b
   fixture directly.
3. **Scheduler priority instead of a reservation.** Nice values under CFS express
   only weight ratios and are too weak to be useful here, but the fleet's kernels
   run EEVDF, where `sched_setattr` with a short request and a low latency-nice
   gives a thread an earlier deadline and lets it preempt a batch thread promptly
   rather than at a slice boundary. It costs nothing when nothing contends, it is
   per-thread so it applies to exactly the interactive pool, and the usual caveat
   that priority only orders within a cgroup's share does not bite because both
   runtimes sit in one pod. This is the cheapest lever not yet pulled.
4. **A core reservation and pinning, both conditional on a change of QoS class.**
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
   instead. See [Bounded import, not live replay](#bounded-import-not-live-replay).

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
5. **Shared-cache and memory-bandwidth interference during hydration.** Core
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
6. **Routing and admission that follow the fork.** Peeks want a substrate and
   rendered dataflows want a runtime, so one routing decision for both throws the
   distinction away. Beyond that, offloading unconditionally pays a handoff for
   peeks that would have finished immediately, which is visible as the flat 105 ms
   floor where the offload buys nothing. Response time is minimized by serving
   short requests first, which requires a size estimate, and the estimate already
   exists: the arrangement key bounds behind `EXPLAIN MEMORY BOUND` and
   `mz_arrangement_distinct_keys` classify a peek as cheap or expensive before
   dispatch. Short peeks stay inline, long peeks offload. That reuses shipped
   machinery rather than adding a heuristic.

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

* A `clusterd-test-driver` workflow drives an interactive query dataflow that
  imports an unpublished maintenance index, scheduled before the index publishes,
  and asserts its result peek resolves correctly only after publication. This
  proves the bind, fill, resolve read path is served off the maintenance worker.
* A shared-fate subprocess test verifies a panic in either runtime aborts the
  process.
* Unit tests cover the sharing primitive and registry, including the single-source
  feed, placeholder-adopted-late joins, cross-thread reads, the compaction
  invariants, and a join and a reduce over a chain the publisher's spine has merged
  across, read at a stale `as_of`.
* The `TwoRuntimeReadIsolation` parallel-benchmark scenario measures read latency
  while the maintenance runtime is saturated by hydration churn, at a read rate
  below the two-runtime serving drain. It reads `strict_serializable=False`, so it
  measures the sealed-timestamp population the feature helps, not the strict
  serializable one it does not. On a box with CPU headroom, two-runtime holds the
  point-read p50 flat while a single-runtime baseline backlogs without bound. Above
  the two-runtime drain both configurations backlog and the comparison degenerates
  into a statement about offered rate, which is why the scenario's rate was lowered
  rather than left where the baseline's percentiles were pure queueing artifacts.

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

The prior planning documents in this directory (`implementation-plan.md`,
`stage2-detailed-plan.md`, `arrangement-sharing-lifecycle-design.md`,
`arrangement-sharing-lifecycle-plan.md`) are superseded by this document. Their
task checklists are fully executed and their fork-era mechanics no longer reflect
the code.
