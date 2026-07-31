# Benchmarks to ground the two-runtime read isolation decision

## Status

Working plan. Delete once the experiments have run and their results are folded into
`design.md`. Companion to `read-routing-policy.md`, which this is meant to settle with data
rather than argument.

## What these experiments have to decide

1. Where the win lands, decomposed by isolation level, rather than as one aggregate number.
2. Whether the interactive runtime's single step loop is a tail-latency source, and at what
   read mix.
3. Whether a frontier-derived routing hint earns its complexity over a static
   classification of reads.
4. Whether the flagship claim (introspection stays answerable during hydration) holds on
   both axes that matter, latency *and* staleness.
5. What the feature costs when it cannot help.
6. Whether the publisher retains memory it should not.

## Methodology constraints

These matter more than the individual experiments, because the existing
`TwoRuntimeReadIsolation` scenario gets two of them wrong and its headline number is
therefore not the number it appears to be.

### The harness will print tail percentiles it cannot support

`test/parallel-benchmark/mzcompose.py` already computes `p99`, `p99_9`, `p99_99`,
`p99_999`, `p99_9999`, `p99_99999` and `p99_999999`, and it stores raw per-query durations
(`MemoryStore` or `SQLiteStore`), so nothing needs to be added to *compute* the tail. The
trap is that it will compute all of them from any sample count. `numpy.percentile` on 6000
samples at 99.9999 just returns the maximum.

The current scenario runs 50/s and 12/s for 120s, so 6000 and 1440 samples. At 6000
samples `p99` rests on 60 observations, `p99_9` on 6, and everything beyond it is the
maximum wearing a different label.

Rule of thumb for a percentile worth reporting: at least 10/(1-q) samples, preferably
100/(1-q).

| Percentile | Minimum samples | Comfortable | At 500 reads/s |
|---|---|---|---|
| p99 | 1e3 | 1e4 | 20s |
| p99.9 | 1e4 | 1e5 | 200s |
| p99.99 | 1e5 | 1e6 | 33 min |
| p99.999 | 1e6 | 1e7 | 5.5 h |

So p99.9 is reachable in a normal run, p99.99 needs a long run or aggregation across
repetitions, and p99.999 and beyond are not honestly reachable in CI. Every arm should
declare the deepest percentile its sample count supports and report nothing past it.

### For the extreme tail, measure the mechanism instead of the percentile

The reason p99.99 and beyond matter here is a specific mechanism: the interactive runtime is
a single step loop, so a read that arrives while a heavy scan is mid-step waits for that
step to finish, and a read that parks waiting for a seal is re-examined only when the
worker next reaches the top of its loop. The floor under the read tail is therefore the
interactive runtime's *step duration* distribution.

Step duration has orders of magnitude more samples than query latency. Millions per minute
rather than thousands. So the tractable route to a statement about p99.99 and max latency is
to measure the step-duration distribution directly and treat it as the bound, rather than
trying to observe a 1-in-10^5 query.

This needs instrumentation that does not exist: a per-runtime step-duration histogram
exported to Prometheus. The `role` label added by this PR (`ComputeRuntimeRole::label`)
already provides the dimension. This is small, independently useful for the interactive
runtime's introspection blind spot, and it is the prerequisite for any credible tail claim.
Do it first.

### Percentiles under overload measure queueing, not service

`TwoRuntimeReadIsolation` runs `OpenLoop` at a fixed rate. When the offered rate exceeds
service capacity the queue grows without bound and latency grows roughly linearly with
elapsed time, so every percentile becomes a statement about where in the run the sample
fell. That is what produces the 80 second baseline p50 and the reported "99.98% reduction".
The comparison establishes that the single-runtime baseline was driven past capacity. It
does not separate "isolation improves latency" from "we overloaded the control".

For every latency arm: measure saturation capacity first, then run the latency arm at a
fixed fraction of it, around 30%. Report utilization alongside the percentiles. Keep an
explicit overload arm if capacity is interesting, but label it as a capacity measurement
and do not read percentiles off it.

### Other requirements

* **Matched controls in the same session.** Each two-runtime arm needs its single-runtime
  control on the same hardware in the same session. Tail numbers are too
  hardware-sensitive and noise-sensitive to compare across sessions.
* **Repetitions.** Report the distribution of per-run p99.9 across N runs plus max-of-N,
  not one run's max. A single max is one draw from a heavy tail.
* **One process and four processes.** Several mechanisms here exist only on multi-process
  replicas, including the routing hint's false positives.
* **Separate queue wait from service time** in the harness so an overload arm is
  distinguishable from a tail arm at a glance.

## The routing race, worked through

The user-facing worry is that routing on "is the index readable right now" races the
maintenance runtime ticking asynchronously. Working out which direction the race goes
narrows what needs measuring considerably.

Reported frontiers never regress (a protocol requirement). The multiplexer's tracked
frontier for an id is therefore never ahead of that process's true frontier. So:

* **On a single-process replica the positive hint is sound.** "Tracked beyond `T`" implies
  "true beyond `T`". The only error is a false negative from report lag, which costs a lost
  isolation opportunity, not a park.
* **On a multi-process replica false positives become possible**, because only process 0
  receives a `Peek` and its multiplexer tracks process 0's meet, while the peek is answered
  by every worker of every process. Process 0 can be ahead of process 3.

That gives a clean experimental separation: measure false negatives at one process, false
positives at four.

There is a second structural result that matters more. For any read whose timestamp comes
from the controller's read frontier (serializable, stale, introspection), the multiplexer
sees the same response stream the controller does and sees it *earlier*, being upstream. So
the tracked frontier is at or beyond the controller's view, which is at or beyond the chosen
`T`. **The hint is positive by construction for exactly the read population that benefits.**
Strict serializable reads take their timestamp at the write frontier, ahead of what has
been reported, so their hint is negative, which is also the right answer because they must
wait for sealing regardless.

If that holds, the policy collapses to a static classification (strict serializable to
maintenance, everything else to interactive) and the dynamic hint earns its keep only by
catching strict-serializable reads whose timestamp got sealed during the flight from
timestamp selection to the replica. Under load that flight is long, so the fraction could be
large. B-C is designed to measure exactly that fraction, because it is the whole argument
for the dynamic hint over the static rule.

It also means the "a frontier hint self-defeats under saturation, because the same stalled
workers emit the reports" worry does not apply to the reads that matter. Their timestamps
are derived from the same stalled reports, so hint and timestamp move together. Worth
confirming in B-D rather than assuming.

## Experiments

### B-A: Where the win lands, by isolation level

The primary result, and the one that grounds or refutes the doc's headline claim.

Fixture as in `TwoRuntimeReadIsolation`: a small indexed table as the read target, churn
materialized views saturating maintenance. Four read arms at ~30% of measured capacity,
crossed with the feature on and off.

1. Strict serializable point read.
2. Serializable point read.
3. Explicitly stale read.
4. Introspection read against the saturated replica.

Report the full distribution per arm to the deepest supported percentile.

**Pre-registered prediction:** arms 2, 3 and 4 improve substantially at every percentile.
Arm 1 improves little or not at all, because its timestamp is at the write frontier and
sealing is still maintenance-bound. If arm 1 improves materially, my scoping analysis is
wrong and the doc's general framing is right.

### B-B: Head-of-line blocking in the interactive lane

The tail experiment. Interactive is one step loop, so a heavy scan and a light point read
share it with no admission control.

Fixture: a point-lookup target plus a large indexed relation for full scans. Point reads at
a fixed low rate. Heavy scans injected at a known low rate, with scan cost swept across
roughly 50ms, 250ms, 1s and 5s of interactive step time.

Measure point-read p50 through p99.9 and max as a function of scan cost and scan rate, plus
the interactive step-duration histogram, against the single-runtime control where the same
scan competes with maintenance instead.

**Pre-registered prediction:** point-read max tracks the injected scan's step duration, and
p99.9 rises toward it once (scan rate x scan duration) approaches the sampling density. The
deliverable is a quantitative statement of the form "a T-millisecond interactive scan puts a
T-millisecond floor under the point-read max", which is what an SLA conversation needs and
what the doc's "read throughput has a ceiling" limitation currently leaves unquantified.

Falsifiable in an interesting way: it is possible that point-read tails are *worse* here
than single-runtime, because maintenance has the same worker count but is not also the
serving lane. If so, the single-step-loop limitation is not deferrable.

### B-C: What the routing hint actually costs

Compare three policies on identical workloads at identical rates, across B-A's four arms:
all-interactive (today), all-maintenance, and hinted.

Needs per-peek instrumentation: which runtime served it, whether it parked before being
served, and on the maintenance path the already-computed comparison of `upper` against
`peek.timestamp` at first attempt, which gives ground truth for "was it in fact sealed".

Derive:

* False-negative rate, meaning routed to maintenance while already sealed. Run at one
  process to isolate report lag.
* False-positive rate, meaning routed to interactive and parked. Run at four processes to
  isolate the cross-process meet.
* Latency penalty conditional on each misroute class, at every percentile. This is where
  the tail lives, because a false negative during saturation costs a full maintenance step,
  which is the pathological quantity.
* The fraction of strict-serializable reads whose timestamp is already sealed on arrival.
  This single number decides whether the dynamic hint is worth building at all, versus the
  static classification.

**Pre-registered prediction:** the hinted policy is indistinguishable from a static
"strict serializable to maintenance, else interactive" rule on arms 2 through 4, and differs
only on arm 1 by the flight-time fraction. If the difference on arm 1 is small, build the
static rule and drop the hint.

Note that this experiment can overturn the recommendation in `read-routing-policy.md`. I
recommended routing to interactive only on a positive hint, on reversibility grounds. If
misroute penalties concentrate in the tail, that recommendation is wrong and all-interactive
with the collateral losses fixed is better. The measurement decides it, not the argument.

### B-D: Frontier-report staleness against load

Instrumentation run rather than a latency benchmark. Measure the distribution of the delay
between a frontier advancing on a worker and the multiplexer observing it, as maintenance
load rises from idle to saturated.

Decides between the frontier-derived policies and P3 bounce-back in
`read-routing-policy.md`. If staleness grows sharply with load *and* B-C shows it matters,
prediction is the wrong mechanism and observation (bounce-back) is required.

### B-E: Introspection during hydration, on both axes

The missing acceptance test, made quantitative. Fixture: a replica hydrating a large
materialized view so maintenance is pinned for a known duration. Reads: the introspection
queries actually used during an incident, over hydration times, operator listings joined
against arrangement sizes, and scheduling elapsed.

Measure **latency and staleness**, meaning the read timestamp against wall clock. "Returns
promptly" only helps if the answer is fresh enough to act on. A 5ms response describing
state from 90 seconds ago does not resolve an incident.

**Pre-registered prediction:** latency improves dramatically and staleness degrades, because
the logging dataflows sit on the same stalled workers. The interesting output is the
staleness distribution, since it determines whether the flagship claim is a real
operational win or a restatement of "we returned stale data faster".

### B-F: Cost when it cannot help

1. CPU-saturated box, workers equal to cores, no headroom. Hydration time for a fixed set
   of materialized views with the feature on and off, at 1x, 2x and 4x oversubscription.
   Tests the doc's own admission that the benefit is conditional on headroom, and checks it
   is not negative without it.
2. Steady-state publisher overhead. Maintenance step-duration distribution with N maintained
   indexes, feature on and off. Every maintained index gains a `PublishShared` sink plus a
   full chain clone per activation.

### B-G: Memory

Cheap, and gates the merge because the review raised concrete falsifiable hypotheses.

1. **Publisher chain retention.** Write heavily to an index, stop writing, trigger
   exertion-driven merges, and sample RSS and `mz_arrangement_sizes`. The hypothesis is that
   superseded batches stay alive in the published chain until the next publisher activation,
   which for an idle-but-merging index is unbounded. Prediction: with the feature off RSS
   drops after merging, with it on RSS stays elevated until the next write. This is also the
   test for whether the `ii_t4` bound doubling is real memory or double counting in
   `log_arrangement_size_inner`.
2. **Read-hold-driven compaction delay.** Hold a long interactive scan open and measure how
   far the maintenance index's `since` falls behind the feature-off control.

## Decision table

| Result | Consequence |
|---|---|
| B-A arm 1 does not improve | Doc must scope the claim to non-strict-serializable reads. Static routing rule becomes the default. |
| B-A arm 1 improves materially | My scoping analysis is wrong. Keep unconditional routing and fix the collateral losses. |
| B-B shows point-read max tracking scan step duration | Single step loop is not a deferrable limitation. Needs an admission or lane policy before this is on by default. |
| B-B shows interactive tails worse than single-runtime | Blocks enabling for latency-SLA reads entirely until lanes exist. |
| B-C shows the flight-time fraction is small | Build the static classification. Drop the dynamic hint and the frontier tracking. |
| B-C shows misroute penalties concentrate in the tail | The positive-hint-only recommendation in the routing note is wrong. Prefer all-interactive plus fixed collateral. |
| B-D shows staleness grows with load and B-C says it matters | Prediction is the wrong mechanism. Build P3 bounce-back. |
| B-E shows large staleness | The flagship claim needs restating, and restoring interactive-side introspection moves up in priority. |
| B-F shows negative maintenance impact | The dyncfg default stays off beyond the memory-bound fleet, and replica sizing needs guidance. |
| B-G confirms retention | Blocks merge. Publisher must drop or refresh its chain on merge, not only on stream activation. |

## Instrumentation to land before running any of this

1. Per-runtime step-duration histogram, labelled by `role`. Prerequisite for every tail
   claim, and independently useful given the interactive runtime's introspection blind spot.
2. Per-peek routing outcome: serving runtime, parked or not, and sealed-on-arrival ground
   truth.
3. Frontier-observation delay, for B-D.
4. Utilization reported alongside latency percentiles in the harness, so an overload arm is
   never mistaken for a tail arm.
5. A guard, or at minimum a convention, against reporting a percentile the sample count
   cannot support.
