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

`enable_two_runtime_compute` is replica-scoped too, but it reaches the replica by a different route.
It is consumed in `environmentd`, in the controller's provisioning path, to decide `ServiceConfig::ports` and the `--interactive-compute-timely-config` argument, and that decision is made before the replica exists.
A scoped override delivered to a running replica would arrive too late to change either.
So the coordinator resolves the per-replica value from the scoped working copy and passes it into `create_replica`, exactly as it already does for `enable_worker_core_affinity` and `enable_storage_introspection_logs`.

That keeps both axes at replica granularity, with one asymmetry.

* The walk-substrate axis and the in-flight cap are read from a replica's `worker_config` and take effect with no restart.
* The runtime axis is read at provisioning time, so changing it re-provisions the replicas whose value changed, and only those.

The experiments in this document predate that scoping and ran the runtime axis as a sequential phase over the whole environment.
E7 in particular would have been a single-phase A/B with a shared maintenance load rather than two phases separated by a replica recycle, which is strictly the better design: the same hydration hits both arms at once instead of being reproduced across phases.

### Peeks reach every replica, whatever the session targets

`Instance::target_replica` returns `None` for `ComputeCommand::Peek`, so a peek is broadcast to every replica of the cluster.
Targeting acts on the response rather than the request: `handle_peek_response` drops responses from replicas other than the targeted one and waits for the target.
`CreateDataflow` is the opposite and does honour the target, so a `SELECT` that renders a dataflow runs on one replica while a fast-path peek runs on all of them.

Measured latency is still the targeted replica's own latency, so an A/B across replicas of one cluster is sound, but only for arms that share a single offered load.
Every replica of a cluster sees every peek, so the load level is a property of the cluster rather than of the arm.
An idle control arm is impossible, and so is any arm that wants a different scan rate or scan cost from its neighbour.

Arms that need independent load therefore need a cluster each, not a replica each.
That is what a scan-cost sweep needs, since each point of the sweep is a different offered load, and running it across replicas of one cluster applies every point to every arm at once.

### Arms are replicas of one cluster

Every arm is a replica of the *same* cluster, and a session pins to one with `SET cluster_replica = '<name>'`.

Replicas of a cluster maintain identical collections from identical inputs, so the arms differ in exactly the flag under test and in nothing else.
This is the strongest form of the matched-control requirement in `benchmark-plan.md`: same data, same dataflows, same hydration work, same wall-clock, and no cross-session hardware drift to argue about.

This holds only for arms that share one offered load, for the reason above: a peek reaches every replica, so load is a property of the cluster.
An arm that needs its own load level needs its own cluster, at the cost of a second copy of every collection and the matched-control property that made replicas attractive.

Two further consequences to design around.
Total maintenance work scales with the number of replicas, since each one maintains everything, so each replica needs its own resources or the arms contend.
And `SET cluster_replica` decides which response is read rather than which replica does the work, so pinning a load generator does not keep its load off the other arms.

## Experiment arms

Phase A and phase B are separated in time by one flag flip that rolls the fleet.
Within a phase, all listed replicas run concurrently in one cluster.

This phase split is no longer forced, now that the runtime axis is replica-scoped.
It is kept here as the record of how the measurements below were actually taken.

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

## Measured results on staging

Venue: a personal staging region in `aws/us-east-1`, build `140494e39a`.
Cluster `eval` carries two `100cc` replicas named `eval-v0-inline` and `eval-v1-offload`.
The fixture is view `e6_li`, three columns of `sf1.lineitem` at 6,003,692 rows, indexed by `e6_li_idx`, plus materialized view `e6_agg`.
`enable_two_runtime_compute` was on environment-wide for phase B, so both replicas ran two runtimes and differed only in `enable_index_peek_offload`.

`enable_two_runtime_compute` was environment-scoped when these runs were taken, so no two replicas of one environment could disagree on it, and every comparison across that flag was sequential and picked up whatever else changed between the two deployments.
It is replica-scoped now, so a rerun would not have that limitation.

### E6: neither the reported size nor resident memory follows publication

| Measurement | Phase A, flag off | Phase B, flag on |
|---|---|---|
| `e6_li_idx` records | 6,003,750 | 6,003,706 |
| `e6_li_idx` size | 112,441,810 | 100,102,051 |
| `e6_li_idx` allocations | 30 | 29 |
| `e6_agg` records | 3,000,015 | 3,000,000 |
| `e6_agg` size | 226,654,658 | 218,527,492 |
| `e6_agg` allocations | 165 | 161 |
| Resident set, two replicas | 583.7 and 597.3 MiB | 491.9 and 503.1 MiB |

The reported doubling did not reproduce.
Both figures came down rather than up, which is within the noise of a rebuild and a fresh hydration, and phase A ran on a different build, so the comparison cannot carry more weight than "no doubling appeared".

The import half is cleaner, because it is a within-phase measurement.
Driving 48 full-scan aggregate dataflows through the interactive runtime, eight concurrent for a minute, moved the resident set by 4.5 MiB on one replica and 1.4 MiB on the other, against a published index of 95 MiB.
Importing a published trace costs approximately nothing, which is what an `Arc`-backed share predicts and what the merge blocker was asking about.

### E1: the offload removes head-of-line blocking, once it can run

Two artefacts had to be removed before the experiment measured anything.

The first is the stash gate.
`should_offload_peek` declines whenever the peek response stash is usable, and usability is a property of the finishing rather than of the result size, so with the stash on every peek with an empty `order_by` and an identity projection declines.
Running the experiment at all required `enable_compute_peek_response_stash = false` for the environment.

The second is peek broadcast.
Arms that were replicas of one cluster shared every scan, so the treatment was applied to both arms at once.
The arms here are a cluster each, `eval_inline` and `eval_offload`, one `400cc` replica apiece, with identical indexes: 6,003,750 rows for the point-lookup target and 750,224 and 60,000,098 rows for the two extra scan targets.

Scan cost is set by index size, not by the predicate, since the filter is evaluated per row and the walk is the whole arrangement either way.
Solo scan latency is 128 ms, 297 ms and 2274 ms against a round-trip floor of about 105 ms, so the walks are roughly 23 ms, 190 ms and 2170 ms.

Point-lookup latency, sixty samples per cell, three concurrent scanners.

| Scan walk | Arm | p50 | p90 | max |
|---|---|---|---|---|
| none | V2 inline | 105.6 | 105.8 | 106.0 |
| none | V3 offload | 106.1 | 106.5 | 106.7 |
| 23 ms | V2 inline | 107.6 | 113.2 | 133.7 |
| 23 ms | V3 offload | 105.1 | 105.6 | 105.9 |
| 190 ms | V2 inline | 105.7 | 382.3 | 586.1 |
| 190 ms | V3 offload | 105.3 | 131.9 | 146.1 |
| 2170 ms | V2 inline | 107.8 | 3896.8 | 5783.7 |
| 2170 ms | V3 offload | 105.8 | 177.0 | 180.4 |

This is the pre-registered prediction, confirmed.
V2's tail tracks the walk it is queued behind, rising from 134 ms to 586 ms to 5784 ms as the walk grows.
V3's tail is flat, 106 ms to 146 ms to 180 ms, a 32-fold reduction at the largest walk.
p50 is round-trip bound in every cell and carries no signal, which is expected when the offered point rate is low.

### E2: the offload alone captures the win

Same fixture and same sweep with `enable_two_runtime_compute = false`, so both arms are single-runtime.
The replicas carry no `role` metric label, which is the `Solo` signature and confirms the flip reached them.

| Scan walk | Arm | p50 | p90 | max |
|---|---|---|---|---|
| none | V0 inline | 105.8 | 106.1 | 106.4 |
| none | V1 offload | 105.5 | 106.1 | 107.5 |
| 23 ms | V0 inline | 107.4 | 108.1 | 129.6 |
| 23 ms | V1 offload | 105.6 | 106.1 | 109.2 |
| 190 ms | V0 inline | 105.6 | 377.8 | 415.6 |
| 190 ms | V1 offload | 107.8 | 113.8 | 180.4 |
| 2170 ms | V0 inline | 105.8 | 3785.0 | 5669.1 |
| 2170 ms | V1 offload | 126.7 | 164.4 | 167.6 |

V1 and V3 are the same within noise at every scan cost, 167.6 against 180.4 ms at the largest walk, and V0 and V2 are likewise the same.
The second runtime contributes nothing to peek tail latency.
What removes the blocking is the substrate the walk runs on, and that is settled by a dyncfg that needs no restart, no new port and no fleet roll.

### E7: temporary dataflows, the second runtime's remaining justification

E1 and E2 settle peeks and leave the second runtime unjustified by them.
Temporary dataflows are the other thing it moves, and they cannot use the offload at all, since the offload is a peek walk substrate and these render dataflows.

Fixture: one `400cc` cluster, a `6,003,750` row index as the lookup target, and two temporary-dataflow probes.
Late materialization is a differential join of about 120 keys against that index, confirmed by `EXPLAIN` to render rather than take a fast path.
The introspection probe is a count over `mz_introspection.mz_scheduling_elapsed`.
`enable_two_runtime_compute` was environment-scoped at the time of this run, so the two arms are sequential phases with a replica recycle between them.

The first maintenance load, repeatedly creating and dropping an index over six million rows, moved almost nothing in either phase.
It never saturated eight workers, and a load that does not saturate cannot demonstrate isolation.
The load that does is a single hydration of a sixty million row index, sampled while it runs.

| Probe | Metric | Single runtime | Two runtime |
|---|---|---|---|
| Late materialization | p50 | 1274.1 | 1236.7 |
| Late materialization | p90 | 2942.2 | 1672.4 |
| Late materialization | max | 3893.8 | 1812.3 |
| Introspection | p90 | 116.4 | 114.4 |
| Introspection | max | 427.9 | 114.6 |

Under maintenance saturation the second runtime halves the temporary-dataflow tail, 2942 to 1672 ms at p90 and 3894 to 1812 ms at the maximum.
The introspection probe is flat on two runtimes, 114.6 ms at the maximum against 427.9 ms on one, so introspection stops being collateral damage of hydration.
p50 barely moves in either probe, which is consistent with the isolation claim: what maintenance work does to a temporary dataflow is a tail effect, not a median one.

This is the justification the second runtime has left.
It is real, and it is a different claim from the one the design doc leads with.
The feature is worth having for temporary dataflows under maintenance load, and it is worth nothing for peek latency, which E2 settled.

A caveat that matters more than the ratio.
Late materialization has a floor of roughly 850 to 950 ms in every cell, quiet or loaded, on either runtime.
The query returns about 120 rows, so that floor is dataflow creation and teardown, not data work.
Runtime placement cannot touch it, and it is larger than the tail the placement recovers.
The bigger prize for interactive latency is the cost of creating a temporary dataflow at all.

### What the two results decide, and what still blocks them

The decision table's V1 row applies for peeks: default to V1, since everything the second runtime costs, the port, the fleet roll, the command-ordering invariant and the capping that enforces it, buys nothing for peek latency that the offload does not buy on its own.

E7 supplies the justification the peek results withdraw.
Temporary dataflows are insulated from maintenance hydration only by the second runtime, and the effect is a halving of their tail.
So the two mechanisms are not competing, they serve different work: the offload for peeks, the second runtime for rendered temporary dataflows.
The routing policy should follow that split rather than sending everything to one place.

Neither arm delivers this in a production configuration today.
Production runs the peek response stash on, and the stash gate disables the offload for exactly the streamable peeks that make up ordinary traffic.
The measured win is real and large, and it is currently unreachable outside an environment with the stash turned off.
Resolving that gate is the blocking design question, and it is worth more than any further latency measurement.

Two limits on these numbers.
Sixty samples per cell supports p90 and makes p99 indistinguishable from the observed max, so the tail is reported as p90 and max rather than as p99.
Both sweeps ran on a build without the walk counter, so engagement is established behaviourally, by the size of the difference between arms, rather than by instrumentation.
## Reruns with the stash on, and disk contention

All of the above ran with `enable_compute_peek_response_stash = false`, because the offload declined any peek the stash could take.
That gate is gone, and `mz_index_peek_walks_total{substrate}` now makes engagement observable rather than inferred.

Build `5708416d62`. With the stash left **on**, the counter reads `offload=568, inline=0` on the offload arm and `inline=2067, offload=0` on the inline arm, and every system replica is inline only.
The offload runs in a production configuration, which is what the change was for.

### E1 with the stash on

Same fixture and sweep as before, one `400cc` cluster per arm.

| Scan walk | Arm | p50 | p90 | max |
|---|---|---|---|---|
| none | inline | 105.0 | 105.3 | 105.5 |
| none | offload | 107.4 | 107.7 | 107.8 |
| 23 ms | inline | 104.4 | 121.7 | 142.2 |
| 23 ms | offload | 104.7 | 105.2 | 105.9 |
| 190 ms | inline | 106.9 | 311.5 | 473.8 |
| 190 ms | offload | 105.7 | 106.1 | 159.8 |
| 2170 ms | inline | 105.3 | 4025.9 | 6162.8 |
| 2170 ms | offload | 108.2 | 182.6 | 184.5 |

The result holds with the stash on: the inline tail tracks the walk it queues behind, the offloaded tail is flat, and the gap at the largest walk is 33-fold.

### E7 as a single-phase A/B

The runtime axis is replica-scoped now, so both arms are replicas of one cluster and share one maintenance load rather than reproducing it across two phases.
`r-two` carries the `role` labels and `r-solo` carries none, which is the `Solo` signature, so the per-replica split reached the replicas.

| Probe | Metric | `r-solo` | `r-two` |
|---|---|---|---|
| Late materialization, quiet | max | 1616.4 | 1333.7 |
| Late materialization, one shared hydration | p90 | 2135.5 | 1346.3 |
| Late materialization, one shared hydration | max | 2835.1 | 1455.9 |
| Introspection, one shared hydration | p90 | 776.6 | 114.9 |
| Introspection, one shared hydration | max | 778.4 | 115.3 |

Same conclusion as the two-phase version, now without the phase confound.
The second runtime halves the temporary-dataflow tail and keeps introspection flat while an index hydrates.

### E8, disk contention: one measurement and two failed fixtures

The question is how a walk behaves when the arrangement it reads is on disk rather than in memory.
Venue: `M.1-nano`, one worker, half a core, 4.07 GB of memory and 24.4 GB of disk, one cluster per arm.

**First fixture, too slow to measure.** All sixteen columns of `sf10.lineitem`, 60M rows, about 13.5 GB against 4.07 GB of memory, reaching 10.4 GB of swap.
That is the depth wanted, and a single walk did not finish in eight minutes, because walk cost scales with rows and 60M rows on half a core is the wrong end of the trade.
Depth needs bytes, speed needs few rows, so the fixture has to be few rows and very wide.

**Second fixture, not a disk test at all.** Three million rows of `repeat(l_comment, 28)`, about 2.4 GB, which fits in memory.
Swap had drained to 0.17 GB by the time the measurement ran, so its result, a p50 of 1580.6 ms inline against 107.4 ms offloaded, is E1's effect on a one-worker replica rather than anything about disk.
Recorded because the numbers are otherwise easy to mistake for a disk result.

**Third fixture, swap-resident and survivable.** Six million rows of `repeat(l_comment, 30)`, about 4.6 GB, leaving 3.2 GB of swap on one arm and 6.4 GB on the other on top of roughly 3.8 GB resident.

The offloaded arm walked it in 49.1 and 51.1 seconds with no restarts.
The inline arm produced no sample in 400 seconds, and its replica restarted during the attempt, its third restart of the session.
Every termination reported `Error` rather than `OOMKilled`, and the dying container's logs were not retained, so the cause is not established.

What this does and does not support.
It does not support a claim about arms, because the two arms sat at different swap depths, 3.2 against 6.4 GB, which is not a matched control.
The plausible mechanism, that a fifty-second inline walk starves the single timely worker until the replica is declared unhealthy while an offloaded walk leaves it free to keep stepping, is consistent with everything observed and is not demonstrated by it.
A follow-up needs equal swap depth on both arms and the pre-restart container logs captured, and until then the honest statement is that an inline walk over a swap-resident arrangement did not complete on a one-worker replica while the offloaded walk of the same data did.

### E8b, swap with a matched control: no regression, and a large win

The first attempt failed on the fixture, not the question.
Building one huge index spikes the working set during hydration, which is what killed a replica and left the two arms at unequal swap depth.
With `compute_hydration_concurrency = 1` in this environment, several smaller indexes hydrate in sequence instead, so the peak is one index rather than the whole set.

Fixture: six indexes of `repeat(l_comment, 8)` over `sf1.lineitem`, about 1.3 GB each, plus a small resident index for the point probe, on `M.1-nano` with 4.07 GB of memory.
That settles at roughly 3.8 GB resident and **5.05 GB of swap on one arm against 5.08 GB on the other**, about 2.2 times memory, and **neither replica restarted**.
Equal depth is what the earlier attempt lacked.

Point-lookup latency on the resident index, two concurrent walks of a swap-resident index, two repeats.

| Cell | Arm | p50 | p90 | max |
|---|---|---|---|---|
| quiet | inline | 105.9 | 138.1 | 149.9 |
| quiet | offload | 106.4 | 106.7 | 107.0 |
| 2 swapped walks | inline | 107.8 | 185.8 | 29151.7 |
| 2 swapped walks | offload | 105.5 | 141.6 | 152.4 |
| 2 swapped walks, repeat | inline | 2216.6 | 4429.1 | 4501.3 |
| 2 swapped walks, repeat | offload | 105.4 | 105.9 | 106.5 |

The offload does not regress under swap, which was the question, and the margin is larger here than anywhere else measured.
Inline shows a 29 second worst case in one repeat and a 2.2 second median in the other, while the offloaded arm stays at its quiet latency in both, 105 to 152 ms throughout.

The walks themselves tell the same story.
A single swapped walk takes 3.6, 4.7 and 56.4 seconds inline against 2.3 seconds three times over offloaded, so the offloaded walk is both faster and far more predictable on the same data at the same depth.

Why the margin is largest here is the interesting part.
A walk that faults on swapped pages spends its time blocked rather than computing, so the thread it blocks is not doing useful work either way.
Moving it off the serving worker therefore costs nothing and recovers everything, which is not true in the CPU-bound case where the offloaded walk still competes for a core.
Disk pressure is the regime where this feature has the least to lose and the most to gain.

### E9, the console under load: answers stay fast, and stay about a second stale

The console polls introspection, `EXPLAIN ANALYZE`, and dataflow sizes.
The question is whether those answers keep arriving while a replica hydrates, and if they do, how stale they are.
Everything here runs in `serializable`, which is what lets a read pick a timestamp that is already available instead of waiting for the latest one.

Arms are replicas of one cluster, `r-solo` single-runtime and `r-two` two-runtime, so one 600M-row index hydration loads both at once.
Staleness is the wall clock at the moment of the probe minus the `query timestamp` that `EXPLAIN TIMESTAMP` reports for the same query, with the `EXPLAIN TIMESTAMP` issued last so the two readings are adjacent.

| Probe | Idle | Hydrating, `r-solo` | Hydrating, `r-two` |
|---|---|---|---|
| Introspection query | 161 to 167 ms | 4445, 4824, 4959, 5262, 6695, 6945, 7518 ms | 158 to 162 ms, one 1547 |
| `EXPLAIN ANALYZE CLUSTER MEMORY` | 297 to 356 ms | 4935, 5224, 5991, 6107, 8692, **13301** ms | 295 to 773 ms |
| `EXPLAIN ANALYZE CLUSTER CPU` | 219 to 511 ms | 4238, 5723, 6467, 7538, **10510** ms | 174 to 525 ms |
| Staleness | -120 to 427 ms | 170 to 1589 ms | -141 to 1456 ms |

Two separate findings, and they answer different questions.

**Latency.** On the single-runtime replica a console poll takes seconds while an index hydrates, up to 13.3 seconds for `EXPLAIN ANALYZE CLUSTER MEMORY`, which is well past the point where a UI has given up.
On the two-runtime replica the same polls stay at their idle cost, around 160 ms for introspection and around 300 ms for `EXPLAIN ANALYZE`.
That is a thirty-fold difference on introspection and it is the concrete reason to want the second runtime for a console.

**Staleness.** Both arms answer from a timestamp about one second behind real time while hydrating, against roughly zero when idle, and the two arms are indistinguishable on this axis, 170 to 1589 ms against -141 to 1456 ms.
So the second runtime does not buy fresher answers, it buys prompt answers at the same freshness.
A console showing data about a second old during hydration is a different and much weaker claim than a console showing live data, and it is the accurate one.

Values straddling zero are clock skew between the probe host and the environment plus the round trip, so idle staleness should be read as "no measurable lag" rather than as a negative number.
`can respond immediately` was `false` on two of the `r-two` probes during the heaviest part of the hydration while the introspection query itself still returned in 161 ms, which is not a contradiction: the two statements are issued a moment apart and the flag describes the timestamp available at plan time.

`EXPLAIN TIMESTAMP` is the right instrument for this and should be how the staleness claim in `design.md` is stated, rather than leaving "read isolation" to imply freshness.

### E10, the shape of memory during hydration: a sawtooth that overshoots about fourfold

Prompted by the replica that died building one large index. The question is whether hydration memory grows smoothly or in jumps, because a replica sized for the steady state is only safe if the path there is monotone.

It is not. Polling `mz_dataflow_arrangement_sizes` at 1 Hz on a `400cc` replica while a 600M-row index hydrates, 289 samples over 258 seconds:

* **82 increases above 200 MiB**, the largest a single-second jump of **9.1 GiB**.
* **17 decreases above 50 MiB**, the largest **36.6 GiB**.
* Arrangement bytes peak at **35.7 GiB** and settle at **13.2 GiB**, so the transient is 2.7 times the final size.
* Allocations peak near 2.9 million and settle at 136, so the settled trace is a few large regions while the build is millions of small ones.

The container tells the same story with a larger factor, since it also carries the batcher, input buffers and persist fetch. Working set goes 19.9 GiB, then **55.4 GiB** at peak, then 47.1, 18.3 and finally **14.3 GiB**. That is a **3.9-fold overshoot** over the settled footprint, reaching 85 percent of a `400cc` replica's 65 GiB limit to build something that ends up needing 14 GiB.

The sawtooth is merge behaviour: batches accumulate, a merge consolidates them and frees the inputs, and the cycle repeats. The final cliff, where the series drops to zero and allocations fall from 2.9 million to about a hundred, is the build's temporary arrangements being released as the consolidated trace takes over.

Two consequences.

The memory freed by each merge does come back, 55.4 GiB down to 14.3 GiB, so this is a transient rather than a leak. What it is not is safe to ignore when sizing: **a replica provisioned for the steady state can die during hydration**, which is what happened on `M.1-nano` and what the earlier disk fixture blamed on the walk substrate before the matched control showed otherwise.

It also explains why building several smaller indexes is the right way to reach a large footprint. With `compute_hydration_concurrency = 1` the peak is one index's transient rather than the whole set's, which is what made the swap fixture reachable at all.

**The result worth keeping is that this was observable at all.** The shape above was measured by polling introspection once a second on the replica that was doing the hydrating, and the polls came back in about 160 ms throughout, so 289 of an intended 258 samples landed. E9 measured what the same poll costs without the second runtime: 4.4 to 7.5 seconds during hydration. A sampler running at that period cannot resolve a one-second 9.1 GiB jump, and would have reported a smooth ramp between widely spaced points.

So the second runtime does not only serve a console. It makes the replica introspectable precisely when something is wrong with it, which is when introspection is worth having and exactly when the single-runtime replica stops answering. Explaining the sawtooth is separate work. Being able to see it is the capability this feature buys, and it is a better argument for the second runtime than any of the latency numbers, because the alternative is not a slower answer but a wrong one.

### E11, the skewed point lookup: one bad key stops stalling everyone

A real customer pattern, and the sharpest case for the offload because it needs none of the machinery above to be reachable. The shape is `SELECT ... WHERE key = <literal> ORDER BY ... LIMIT 1`. Most keys hold one value, a few hold millions. An `ORDER BY` makes the finishing non-streamable, so the peek response stash never applied to it and the offload was always eligible.

Fixture: a view whose hot key `0` holds 6,003,692 distinct values and whose other 1,500,000 keys hold exactly one each, indexed on the key, 7,503,698 records total on `400cc`. Both query shapes plan as fast-path index lookups with literal constraints, which is the customer's plan. Keys for the normal lookups are drawn from the table rather than generated, because TPC-H order keys are sparse and a generated key mostly misses and measures an empty seek.

Isolated, a normal lookup costs 105 ms, essentially the round trip. The hot key costs **2020 ms**, because finding the minimum of six million values for one key is one worker's walk.

Load is open loop: arrivals fire on a fixed wall-clock schedule regardless of how many requests are outstanding, from a pre-opened connection pool. A closed loop would throttle itself and hide precisely the queueing being measured. Client-side queue delay stayed at 1.2 ms and nothing was dropped in any run, so the pile-up below is entirely server side.

Three skewed lookups injected into a steady 10 per second stream of normal lookups:

| Arm | normal p50 | p90 | p99 | max | normals over 200 ms |
|---|---|---|---|---|---|
| inline | 107.7 | 1223.0 | 2019.9 | 2024.2 | **58 of 261** |
| offload | 107.8 | 108.7 | 109.0 | 109.2 | **0 of 261** |

The same at 25 per second, two injections:

| Arm | normal p50 | p90 | p99 | max |
|---|---|---|---|---|
| inline | 107.0 | 1027.1 | 1940.1 | 2024.2 |
| offload | 105.5 | 106.0 | 106.3 | 118.2 |

The inline timeline shows the mechanism rather than just its size. After a skewed lookup arrives at t=5.0, the normals arriving behind it complete at a fixed time rather than after a fixed delay: 2019.9 ms for the one arriving at 5.0, then 1921, 1821, 1721, 1622, 1522 and so on down to 235.8 ms for the one arriving at 6.8. Latency is the remaining walk, so every arrival waits for the same completion instant. The pattern repeats identically at 12.0 and 19.0.

That is the customer's complaint exactly: one lookup on a bad key stalls every lookup behind it, and the number of victims is the arrival rate times the walk, which is why it looks like a latency spike across the board rather than one slow query.

The offloaded arm has no such window. The skewed lookups still take about two seconds, 1971, 2021 and 2003 ms, because the work is unchanged. They simply stop being in anyone else's way, and no normal lookup exceeds 200 ms in any run.

`ORDER BY` peeks are the one shape the offload could always serve, so this improvement was available before the stash work and is not contingent on it. It is also the strongest argument in this document for turning the offload on by default: the cost is a thread, and the benefit is that a single skewed key stops being a cluster-wide latency event.

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
