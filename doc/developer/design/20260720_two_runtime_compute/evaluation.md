# Experimental evaluation of read placement

## Status

Working plan, to be deleted once its results are folded into `design.md`.

Extends `benchmark-plan.md` rather than replacing it.
That document's methodology constraints all still apply and are not repeated here: percentiles need the sample count to support them, capacity is measured before latency, controls run in the same session on the same hardware, and an overload arm is never read as a tail arm.
Its experiments B-A and B-C through B-G still stand as written.
Two things have changed since it was written, and they are what this document covers.

First, the walk substrate is now selectable.
A fast-path index peek can be walked inline on the serving worker or dispatched to a blocking task, on either runtime.
B-B was written to characterise head-of-line blocking in the interactive lane as an accepted limitation.
It is now a treatment comparison, because the offload exists specifically to remove that effect.

Second, there are four reachable configurations rather than two, so "does two-runtime help" is no longer the question.
The question is which cell should be the default, and the cells differ in deployment cost as much as in latency.

## The configuration matrix

| Cell | `enable_two_runtime_compute` | `enable_index_peek_offload` | Peek received by | Peek walked by | Temporary dataflows | Deployment cost |
|---|---|---|---|---|---|---|
| **V0** | off | off | maintenance loop | maintenance worker | maintenance | none, this is today |
| **V1** | off | on | maintenance loop | blocking task | maintenance | dyncfg only, no restart |
| **V2** | on | off | interactive loop | interactive worker | interactive | new port, rolls every replica |
| **V3** | on | on | interactive loop | blocking task | interactive | new port, rolls every replica |

Both flags are live dyncfgs, but only one is a live toggle.
Flipping `enable_two_runtime_compute` changes `ServiceConfig::ports`, so moving between the V0/V1 row and the V2/V3 row restarts every compute replica in the environment.
Moving along a row does not.
Plan the arms so that the expensive transition happens once, not per arm.

`index_peek_offload_max_inflight` (default 16) is a third knob, swept only in E4.

## Scoping, and what it means for the arms

The two peek flags are declared `ParameterScope::Replica`, so a targeting rule can give a single replica a different value without touching the rest of the environment.
The override is resolved against a replica evaluation context and pushed to that replica as a `ConfigUpdates`, which lands in the `worker_config` both flags are read from inside `clusterd`.
Scoped overrides require `enable_scoped_system_parameters` to be on in the environment.

`enable_two_runtime_compute` is **not** replica-scopable, and marking it so would be inert.
It is consumed in `environmentd`, in the controller's replica provisioning path, against the controller's environment-wide config, to decide `ServiceConfig::ports` and the `--interactive-compute-timely-config` argument.
Replica-scoped overrides are delivered to replicas, not consulted by the controller when it builds them, so declaring a scope on this flag would advertise a capability that does not exist.
Nothing else consumed at replica-launch time is replica-scoped either, including `enable_timely_zero_copy`, so this is a property of the mechanism rather than an oversight in this flag.

That splits the matrix into two axes with different granularity.

* The runtime axis (V0/V1 against V2/V3) is **environment-wide and is a phase**, not an arm.
  Moving between phases rolls every replica.
* The walk-substrate axis and the in-flight cap are **per replica and are arms**.
  They can vary between replicas of the same cluster with no restart.

Making the runtime axis per-replica would mean plumbing the replica override map into the controller's provisioning path.
That is worth doing only if running all four cells concurrently turns out to matter, and the phase structure below avoids needing it.

### Arms are replicas of one cluster

Every arm is a replica of the *same* cluster, and a session pins to one with `SET cluster_replica = '<name>'`.

Replicas of a cluster maintain identical collections from identical inputs, so the arms differ in exactly the flag under test and in nothing else.
This is the strongest form of the matched-control requirement in `benchmark-plan.md`: same data, same dataflows, same hydration work, same wall-clock, and no cross-session hardware drift to argue about.

Two consequences to design around.
Total maintenance work scales with the number of replicas, since each one maintains everything, so each replica needs its own resources or the arms contend.
And the load generator must set `cluster_replica` per connection, since an unpinned session may be served by any replica and would blend the arms.

## Experiment arms

Phase A and phase B are separated in time by one flag flip that rolls the fleet.
Within a phase, all listed replicas run concurrently in one cluster.

### Phase A: `enable_two_runtime_compute = false` (environment-wide)

| Variation | Replica name | Replica-scoped overrides | Serves |
|---|---|---|---|
| V0 | `eval-v0-inline` | none, all defaults | E1, E2 baseline |
| V1 | `eval-v1-offload` | `enable_index_peek_offload=true` | E1, E2 |

### Phase B: `enable_two_runtime_compute = true` (environment-wide)

| Variation | Replica name | Replica-scoped overrides | Serves |
|---|---|---|---|
| V2 | `eval-v2-inline` | `enable_index_peek_offload=false` | E1, E2, E5 |
| V3 | `eval-v3-offload` | `enable_index_peek_offload=true` | E1, E2, E5 |

Phase B needs `enable_index_peek_offload=false` stated explicitly on `eval-v2-inline` rather than left to the default, because the CI and evaluation environments set the environment-wide value to true.
An arm must never depend on the environment default being what it was when the plan was written.

### E4 cap sweep, phase B

| Variation | Replica name | Replica-scoped overrides |
|---|---|---|
| V3, cap 1 | `eval-cap-1` | `enable_index_peek_offload=true`, `index_peek_offload_max_inflight=1` |
| V3, cap 4 | `eval-cap-4` | `enable_index_peek_offload=true`, `index_peek_offload_max_inflight=4` |
| V3, cap 16 | `eval-cap-16` | `enable_index_peek_offload=true`, `index_peek_offload_max_inflight=16` |
| V3, cap 64 | `eval-cap-64` | `enable_index_peek_offload=true`, `index_peek_offload_max_inflight=64` |
| V3, uncapped | `eval-cap-max` | `enable_index_peek_offload=true`, `index_peek_offload_max_inflight=4096` |

`eval-cap-16` restates the default deliberately, so the sweep contains its own control and does not depend on the default staying at 16.

### E3, oversubscription

E3 varies machine resources rather than flags, so its arms are replica *sizes* rather than overrides.
Run the phase A pair and the phase B pair at each of 1x, 2x and 4x oversubscription, keeping the flag overrides above unchanged.

### E6, memory attribution

Needs one published and one unpublished arrangement observed in the same process, so it runs on a phase B replica (`eval-v3-offload` will do) and compares an index against a materialized view's internal arrangements.
No flag overrides beyond the phase.

## Verifying the overrides landed

Do not assume a targeting rule applied.
Before every run, confirm from inside the environment that each replica sees the value the table claims, and record it with the results.
An arm that silently ran with the environment default is worse than a missing arm, because it looks like a null result.

## What these experiments have to decide

1. Whether the offload removes head-of-line blocking between peeks, which is its entire justification.
2. Whether V1 captures enough of the win to make the fleet roll unnecessary for most reads.
3. What the offload costs on a CPU-saturated replica, where it increases concurrent demand rather than moving it.
4. Where the in-flight cap should sit, meaning the point at which pinned memory and delayed compaction stop being worth the latency.
5. Whether publishing an arrangement retains memory or only misreports it.

Question 5 is a merge blocker.
The others decide the default, not the merge.

## Prerequisites

Neither of these is optional, and both are small.

**A per-runtime step-duration histogram, labelled by `role`.**
Carried over from `benchmark-plan.md`, where it is the prerequisite for any tail claim.
It matters more now, not less: the offload's predicted effect is that point-read tails stop tracking the interactive step-duration distribution, and that prediction cannot be checked without observing both.

**Peek metrics on the interactive runtime.**
`index_peek_total_seconds` is now recorded on the offloaded path, but the per-phase histograms pass `metrics: None` on the shared inline walk, and `mz_active_peeks` and `mz_peek_durations_histogram` are empty on interactive because its logging is disabled.
So V2 is the one cell that cannot report its own peek latency from inside the replica.
Comparing V2 against V3 with client-side latency only is possible but attribution-blind, which defeats the purpose.

**An image.**
The PR is a draft, so Buildkite has not built one.
Either take it out of draft or build and push an image for the branch head by hand.
Nothing runs on staging until this exists.

## Venue

The experiments split cleanly, and mixing them up would waste the staging environment.

**Tail latency runs locally, on a dedicated machine.**
Every question above is about the shape of a latency distribution above p99, and a client on the far side of a network from the environment adds RTT jitter at exactly that scale.
The difference between arms survives, but the absolute floor and the max do not, and the max is the deliverable in E1.
Local mzcompose with the parallel-benchmark harness, matched controls in one session, is the right instrument.

**Staging supplies realism, not percentiles.**
Real catalogs, real introspection relation sizes, real hydration durations, real memory profiles, and real query shapes.
Use it for E5 (introspection during hydration) and E6 (memory), where the mechanism depends on scale that a synthetic fixture does not reproduce, and where the measurement is a level or a ratio rather than a tail percentile.

**Polar Signals is on-CPU only.**
Good for E3, where the question is whose CPU the offloaded walk consumes.
Useless for latency attribution, since queueing and parked reads are off-CPU and invisible to it.

## Experiments

### E1: Does the offload remove head-of-line blocking between peeks

The central experiment, and the one that decides whether the offload earns its place.

Restructures B-B as a two-by-two rather than a description.
Fixture unchanged from B-B: a point-lookup target plus a large indexed relation, point reads at a fixed low rate, heavy scans injected at a known rate with scan cost swept across roughly 50ms, 250ms, 1s and 5s of walk time.
Arms: V2 and V3, plus V0 and V1 as the single-runtime pair, all in one session.

Measure point-read p50 through the deepest percentile the sample count supports, plus max, as a function of scan cost.
Report the step-duration histogram of the serving runtime alongside.

**Pre-registered prediction.**
In V2, point-read max tracks the injected scan's walk duration, reproducing B-B's predicted effect.
In V3, point-read max decouples from scan cost and flattens, because the serving loop only snapshots and dispatches.
The same relationship holds between V0 and V1 with maintenance as the serving loop.

**What would falsify it.**
If V3's point-read tail still tracks scan cost, the remaining serialization is somewhere I have not accounted for, most likely the command-drain path or the snapshot itself under a large chain, and the offload does not do what it claims.
If V2 and V3 are indistinguishable, the head-of-line effect is not present at the rates tested, and B-B's premise was wrong rather than the fix being unnecessary.

### E2: How much of the win does V1 capture

The deployment question.
V1 needs no port change, no replica roll, and no publication, so if it captures most of the read-latency win it is the configuration to default to, and the second runtime becomes a feature for temporary dataflows alone rather than for reads.

Arms: V0, V1, V3, driven by the `TwoRuntimeReadIsolation` workload at a rate set from measured capacity rather than the current fixed rate.
Both isolation levels from B-A that the feature is predicted to help, serializable and stale, plus strict serializable as the negative control.

**Pre-registered prediction.**
V1 improves on V0 substantially but not fully, because it removes the walk from the maintenance worker without removing the wait for that worker to reach the peek command.
V3 improves on V1 by the residual, and the size of that residual is the price of the fleet roll.

This is the experiment whose answer I am least able to guess, and it is the one that decides the default.

### E3: What the offload costs when there is no headroom

Extends B-F to the substrate axis.

Inline, a walk consumes the worker it would have occupied anyway.
Offloaded, it consumes a blocking-pool thread and frees the worker to continue, so concurrent CPU demand strictly increases.
On a replica with spare cores that is the point.
On a saturated one it is a regression, and the fleet is described as memory-bound with spare CPU, which is an assumption worth testing rather than assuming.

Arms: V0 against V1 and V2 against V3, at 1x, 2x and 4x CPU oversubscription, measuring hydration throughput and maintenance step duration while a read load runs.
Polar Signals for attribution of where the CPU went.

**Pre-registered prediction.**
No measurable hydration cost at 1x, a measurable one at 4x.
If hydration degrades at 1x, the offload needs to be per-role rather than one flag, enabled on interactive and not on maintenance.

### E4: Where the in-flight cap belongs

`index_peek_offload_max_inflight` defaults to 16 per worker on no evidence at all.

Each in-flight walk pins the batches its cursor covers and holds the trace back from compacting past its read time.
Sweep the cap across 1, 4, 16, 64 and unbounded under a concurrent read load, measuring read p99, resident memory, and compaction lag on the read arrangements.

**Pre-registered prediction.**
Latency improves sharply from 1 and flattens well before 16, while memory and compaction lag grow roughly linearly with the cap.
If so, the default should sit at the latency knee, which I expect below 16, and the documentation should describe the cap as a memory bound rather than a concurrency tuning parameter.

### E5: Introspection during hydration, on staging

B-E unchanged in intent, but run where hydration takes long enough to matter and introspection relations are large enough to be slow.

Measure both axes.
Latency of introspection queries during a large hydration, and the staleness of the answers they return, since the logging dataflows sit on the same stalled maintenance workers.
Arms V0 and V3, since this is the flagship claim and V3 is the configuration it would ship in.

**Pre-registered prediction.**
Latency improves dramatically and staleness degrades.
The deliverable is the staleness distribution, because "answered promptly with data from ninety seconds ago" is a different product claim from "answered promptly", and the design doc currently makes the second one.

### E6: Attribute the arrangement-size doubling

The merge blocker, and the one experiment whose question is already half answered.

Established by measurement on a sixteen-worker replica: publishing an index doubles its reported heap size, capacity and allocations, records and batches unchanged, 8740 bytes and 132 allocations against 4370 and 66.
Ruled out: the `Rc` to `Arc` migration, since an unpublished materialized view's arrangements are byte-identical either way.
Ruled out: a reader, since the doubling is present before anything imports the index.
Ruled out: the published chain lagging a spine merge, since re-reading the chain after compaction is forwarded changes nothing.

What remains is to identify which allocation the second batch is, which needs instrumentation inside `log_arrangement_size_inner` rather than another A/B.
Dump the live batch addresses the size logger is summing, alongside the addresses the trace currently holds, on a published and an unpublished arrangement.
The set difference is the answer.

Then the deciding measurement: whether resident memory follows the reported number.
If it does, publication carries a real per-arrangement memory tax on a memory-bound fleet, and that is a merge blocker for enabling the feature rather than for landing the code.
If it does not, it is a reporting bug in the size logger, the `ii_t4` threshold bump reverts, and the metric is wrong for every deployment with the feature on.

Staging is the right venue for the second half, because the question is whether a per-arrangement tax matters at real arrangement counts.

## Decision table

Extends the one in `benchmark-plan.md`.

| Result | Consequence |
|---|---|
| E1 shows V3 decoupled from scan cost, V2 not | The offload is the default for any cell that serves peeks. V2 stops being a configuration we ship. |
| E1 shows V2 and V3 indistinguishable | The offload is unjustified complexity at these rates. Keep it behind a flag, default off, and revisit if a workload appears that needs it. |
| E1 shows V3 still tracking scan cost | Serialization remains somewhere unaccounted for. Do not claim read isolation for mixed workloads until it is found. |
| E2 shows V1 captures most of the win | Default to V1. The second runtime is justified by temporary dataflows alone, and the routing policy should keep peeks off it. |
| E2 shows a large residual for V3 | The fleet roll is justified for read-latency-sensitive environments, and B4's routing policy decides per read rather than per replica. |
| E3 shows hydration degrading at 1x | Split the offload flag per role. Interactive on, maintenance off. |
| E4 knee below the default | Lower `index_peek_offload_max_inflight` and document it as a memory bound. |
| E5 shows large staleness | Restate the flagship claim in `design.md` as a staleness claim, and raise the priority of restoring interactive introspection. |
| E6 shows resident memory follows the metric | Publication carries a real memory tax. Blocks enabling the feature by default until fixed. |
| E6 shows resident memory flat | Reporting bug. Revert the `ii_t4` bound and fix the size logger. |

## Order

E6's instrumentation half comes first, because it is a merge blocker and it is cheap.
Then the two prerequisites, since every latency experiment depends on them.
Then E1, which decides whether the offload stays at all.
Then E2, which decides the default.
E3, E4 and E5 follow, and none of them blocks a decision the earlier ones do not already settle.
