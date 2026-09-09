# Value-Based Pricing: GiB-Hours and Memory-Driven Cluster Scaling

- Associated: (TBD)

> [!NOTE]
> This is a **spike**. It records what the existing code already gives us, what
> the numbers in the current rate card actually say, and which of the two halves
> of the idea carries the risk. It proposes an increment order rather than a
> committed design. Sections marked *finding* are statements about code or data
> in the tree today, not proposals.

## The Problem

Billing today is a step function over provisioned capacity:
`credits_per_hour(size) x replica-hours`, integrated from
`mz_internal.mz_cluster_replica_history` (`created_at`, `dropped_at`,
`credits_per_hour`). A replica costs the same whether it holds 2 GiB of
arrangements or 200.

Two consequences:

1. **The user pays for headroom they had to guess at.** Sizing a cluster is a
   capacity-planning exercise with an asymmetric penalty: undersize and the
   dataflow OOMs, oversize and the bill is silently 4x. Users rationally
   oversize, then pay for the margin.
2. **The price does not track delivered value.** A cluster's cost is a function
   of an operator's provisioning decision, not of how much state Materialize is
   maintaining for the user.

The ask is to charge on GiB-hours of memory actually consumed, and to have a
controller keep provisioned size close to consumption so the two do not diverge.

## Finding 1: the rate card is already GiB-hours, of *provisioned* memory

Derived from the size map in `misc/python/materialize/workload_replay/config.py`
(credits per provisioned GiB-hour = `credits_per_hour / memory_limit_GiB`):

> [!WARNING]
> **Provenance.** That file is the fixture used to replay captured production
> workloads, not the authoritative production size map, which lives in cloud
> infrastructure rather than this repo. It carries real cloud size names and
> plausible allocations, and `6400cc` sharing both its memory and its credits
> with `3200cc` is a tell that the largest entries are approximations or
> aliases. The *shape* of the finding below (credits proportional to provisioned
> memory within a family) is robust to that: it holds across four independent
> families and dozens of sizes, which no fixture would reproduce by accident.
> The exact constants, and especially the cap behaviour at the top of each
> ladder, must be re-derived from the production map before any pricing decision
> rests on them.

| family | sizes | mem GiB range | credits per provisioned GiB-hour |
| --- | --- | --- | --- |
| `cc` | `25cc` .. `1200cc` | 3.79 .. 181.93 | 0.06596 (constant) |
| `cc` | `1600cc`, `3200cc` | 235 .. 470 | 0.06809 |
| `cc` | `6400cc`, `128C`, `256C`, `512C` | 470 (capped) | 0.06809 (see warning) |
| `M.1` | `nano` .. `3xlarge` | 3.79 .. 181.93 | 0.19789 (constant) |
| `M.1` | `4xlarge`, `8xlarge` | 235 .. 470 | 0.20426 |
| `M.1` | `16xlarge` .. `128xlarge` | 470 (capped) | 0.40851 .. 3.26809 |
| `balanced` | all | 6.63 .. 390 | ~0.164 (constant) |
| `highcpu` | all | 7.58 .. 26.52 | ~0.528 (constant) |
| `source_ingestion` | all | 0.95 .. 15.16 | ~0.264 (constant) |

Within a family and below the memory cap, credits per provisioned GiB-hour is
**constant to five decimal places**. The pricing formula is already
`rate(family) x GiB x hours`.

So value-based pricing is not a new formula. It changes the *integrand* from
provisioned bytes to consumed bytes, and leaves `rate(family)` alone. That is a
much smaller change than "introduce a new pricing dimension", and it means the
existing rate card is the migration path rather than an obstacle.

Two caveats fall straight out of the same table, and both constrain the design:

**Caveat A: a single global $/GiB-hour is wrong by 8x.** The family constant
encodes the CPU:memory ratio. `highcpu` charges 0.528 and `cc` charges 0.066 per
provisioned GiB-hour: an 8x spread. A flat GiB-hour price would make `highcpu`
nearly free and overcharge `cc`. Either the rate stays per-family, or the price
becomes two-dimensional (GiB-hours + CPU-hours). Keeping the per-family rate is
the smaller change and preserves every existing commercial commitment.

**Caveat B: the top of each ladder sells spill capacity, not RAM.** `M.1-16xlarge`
through `M.1-128xlarge` all hold `memory_limit` at 470 GiB while
`disk_limit / memory_limit` climbs 14x, 28x, 56x, 111x; `cc` above `3200cc` caps
memory the same way. Those sizes exist to buy swap-backed spill. A metric based
on resident memory under-bills them by up to 16x.

Caveat B is answered by the choice of integrand in Finding 2.

## Finding 2: the metering pipeline already exists, and `heap_bytes` is the integrand

`ServiceProcessMetrics` (`src/orchestrator/src/lib.rs:153`) carries
`heap_bytes` and `heap_limit` alongside `memory_bytes` and `disk_bytes`.
`src/orchestrator-kubernetes/src/lib.rs:1585` computes
`heap_bytes = clusterd's own memory_bytes + swap_bytes`, read from clusterd's
`/api/usage-metrics` endpoint rather than from cAdvisor.

Be precise about what that is, because it is easy to assume it is more than it
is. `Collector::collect` (`src/clusterd/src/usage_metrics.rs:29`) fills those
two fields from `/proc/self/status`: `memory_bytes = VmRSS` and
`swap_bytes = VmSwap`. So `heap_bytes` is **the clusterd process's resident set
plus its swapped-out pages**. It is *not* allocator-accounted. `heap_limit` is
the cgroup's memory + swap limit, further reduced by the in-process memory
limiter when one is configured.

Two reasons still make it the right billable integrand, neither of which is
"it measures logical bytes":

* **It counts swap.** `memory_bytes` from the pod metrics API does not, and
  Caveat B is exactly the case where the resource being sold is spill capacity.
  A metric blind to swap under-bills the top of each ladder by up to 16x.
* **It is the same quantity that decides whether the replica lives.** The memory
  limiter "obtains the current memory utilization from proc stats"
  (`src/compute/src/memory_limiter.rs`) and terminates the process when the
  burst budget is exhausted. Billing on an allocator-accounted measure while
  killing on RSS would charge a user less than the resource that can kill their
  cluster, which is not defensible. Billing and enforcement should agree on
  what "memory used" means.

**The risk this leaves open.** jemalloc does not eagerly return freed pages, so
RSS is closer to a high-water mark than to current demand: dropping a large
arrangement may not lower `heap_bytes` promptly. Under consumption-based pricing
that means a user can keep paying for state they have released, which is the
single most likely source of "my bill did not drop when I dropped the view"
complaints. This is *not* solved by the choice above and must be quantified
before repricing. It is measurable today without new plumbing:
`jemalloc_allocated`, `jemalloc_resident` and `jemalloc_retained` are already
registered as Prometheus metrics (`src/prof/src/jemalloc.rs:133`), so the
allocated-versus-resident gap and its decay after a drop can be characterised
across the fleet now. If the gap turns out to be large or slow to decay, the
options are to bill on a floor-and-decay model, to make the metering endpoint
also report `jemalloc_allocated`, or to tune the allocator's page-return
behaviour so the enforcement metric tracks demand more closely (which would fix
this for billing and provisioning at the same time).

`heap_bytes` is already plumbed all the way to SQL:
`mz_internal.mz_cluster_replica_metrics_history` carries
`(replica_id, process_id, cpu_nano_cores, memory_bytes, disk_bytes, occurred_at,
heap_bytes, heap_limit)`, appended by
`Controller::record_replica_metrics` (`src/controller/src/lib.rs:645`) on a
60-second tick (`METRICS_INTERVAL`, `src/controller/src/clusters.rs:951`), with
30-day retention (`replica_metrics_history_retention_interval`).

The precedent for a value-based dimension also already exists:
`mz_internal.mz_storage_usage_by_shard` is byte-hours of storage, collected on a
coordinator interval and pruned against a retention period. The compute-side
metering record should be built to that same shape, and
`mz_internal.mz_object_arrangement_size_history` (per-object, per-replica bytes,
with a `hydration_complete` flag) is already there for per-object showback.

### What is missing on the metering side

1. **A durable billing record.** `mz_cluster_replica_metrics_history` is a
   retained-metrics source under a dyncfg retention. Billing needs an
   append-only aggregate at a coarser grain that is not subject to that
   truncation. Proposal: a builtin table `mz_internal.mz_replica_memory_usage`,
   one row per replica per hour, written by a coordinator interval task modeled
   on `Coordinator::arrangement_sizes_snapshot` / the storage-usage collector.

   ```
   replica_id, cluster_id, size, bucket_start,
   billable_gib_hours,      -- the integral, after the floor in (3)
   consumed_gib_hours,      -- integral of summed heap_bytes, unfloored
   provisioned_gib_hours,   -- today's bill, for reconciliation
   sample_count,            -- integrity signal
   expected_sample_count
   ```

   `provisioned_gib_hours` in the same row is what makes a bill-neutral rollout
   auditable: finance can diff the two columns before anything changes.

2. **A defined gap policy.** The samples are 60-second point observations and
   `heap_bytes` is `Option<u64>`: a metrics-API failure or a clusterd endpoint
   timeout yields `None`. Billing must say what an incomplete bucket costs.
   Charging the provisioned rate when *our* metrics pipeline fails is not
   defensible to a customer; charging zero is not defensible internally. The
   neutral choice is to integrate the observed samples' mean over the full
   bucket and record `sample_count` so an incomplete bucket is visible
   downstream. A bucket with zero samples falls back to the floor term.

3. **A floor.** A pure GiB-hours price makes an idle replica free, but an idle
   replica still holds a CPU reservation and a scheduling slot, and the floor is
   also the anti-gaming term (nothing should reward keeping a dataflow just below
   a threshold). Proposal:

   ```
   billable_gib = max(consumed_gib, floor_fraction x provisioned_gib)
   ```

   with `floor_fraction` a dyncfg. **Ship it at `1.0`**, which reproduces today's
   bill exactly, then lower it per-environment. That gives a code path in
   production, months of real `consumed_gib_hours` data to price against, and a
   repricing decision that is a config change rather than a release.

4. **Aggregation rules**, all mechanical but each a revenue decision that should
   be written down once: sum `heap_bytes` across `process_id` within a replica
   (multi-process replicas shard arrangements); exclude `INTERNAL` and
   `BILLED AS` replicas, as today; and bill each replica independently, so
   `replication_factor = 2` costs 2x. The last is correct and should stay:
   replicas are full copies, so two replicas really do hold two copies of the
   state. It does mean HA still costs 2x under value-based pricing, same as now.

## Finding 3: "scale replicas up and down" is the wrong axis, and the controller already has the right one

Pushing back on the framing in the ask. In Materialize, replicas within a cluster
are **redundant full copies**, not shards. Every replica of a cluster renders
every dataflow and holds every arrangement. Adding a replica adds availability;
it does not add capacity, and it multiplies memory consumption. An autoscaler
that reacts to memory pressure by raising `replication_factor` makes the
condition it is reacting to strictly worse, and under GiB-hours pricing it
doubles the bill to fix an over-cost problem.

The capacity axis is the replica **size**, which is where `memory_limit`, `scale`
(processes) and `workers` live (`ReplicaAllocation`,
`src/controller/src/clusters.rs:75`). Scaling a cluster means changing its size,
and changing size without downtime is exactly what graceful reconfiguration
already does: run the target shape alongside the realized one, wait for
hydration, cut over.

And the existing controller composes this for free. `mz-cluster-controller` is a
pure reconciler over a set of `Strategy` implementations
(`src/cluster-controller/src/strategy.rs`), where a strategy is
`signal_request` + `update_state` + `desired_replicas`, all pure.
`GracefulReconfigurationStrategy` engages whenever the durable `reconfiguration`
record is in progress, and `StateWrite.reconfiguration`
(`src/cluster-controller/src/ctx.rs:289`) is writable by *any* strategy.

So a memory-driven strategy does not implement replica swapping at all. It
decides *when* and *to what size*, writes a `reconfiguration` record, and the
existing graceful-reconfiguration strategy performs the overlap, the hydration
wait, and the cut-over. The scale-up/scale-down mechanism is already built.

This also hands us OOM safety for free. A record written with
`on_timeout: Rollback` (the default) drops the whole target set and leaves the
realized config untouched if the target never hydrates. A scale-down to a size
that cannot hold the arrangements therefore never cuts over: it times out and
reverts, with the cluster serving from the old replicas throughout. No OOM-loop
detection needed, as the autoscaling design already notes for the user-initiated
path.

### What the controller crate genuinely lacks: an ordered size ladder

`ClusterState` carries `size: String`, and strategies compare shapes by string
equality (`ReplicaShape::matches`). There is no notion of "the next size up" or
"how much heap does this size have". A memory-driven strategy needs both.

The crate deliberately "depends only on primitive id/shape types and the
`ClusterControllerCtx` trait", so handing it a `ClusterReplicaSizeMap` is out.
The right seam is `ConfigSignals`, which is already documented as
"environment-wide dyncfg values latched by the kernel once per tick" and
explicitly "not durable state, so never witness material". Add:

```rust
/// The environment's size ladder, projected to what a scaling decision needs.
/// Latched once per tick like every other config signal, so all clusters
/// evaluate against one consistent ladder.
pub struct SizeLadder { /* per-family ordered sizes + heap_limit per size */ }

impl SizeLadder {
    fn heap_limit(&self, size: &str) -> Option<u64>;
    fn next_up(&self, size: &str) -> Option<&str>;
    fn next_down(&self, size: &str) -> Option<&str>;
}
```

Deriving it from `ClusterReplicaSizeMap` happens on the Coordinator side of the
ctx boundary, where that map already lives. The strategy stays pure and stays
testable against the existing fake ctx. Critically, the ladder must **not** join
`ExpectedClusterState`: it is environment config, not per-cluster durable state,
and putting it in the compare-and-append witness would invalidate every
in-flight write on a size-map rollout.

Ordering within a family is by `memory_bytes`. Cross-family moves are out of
scope: a family encodes a CPU:memory ratio the user chose, and silently changing
it changes both performance and the billing rate.

### The strategy

SQL surface, extending the deliberately-extensible `AUTO SCALING STRATEGY` block
(`AutoScalingPolicy` is already "extensible: future strategies are additional
optional sub-policies"):

```sql
CREATE CLUSTER c (
  SIZE = '400cc',
  AUTO SCALING STRATEGY = (
    ON HYDRATION (HYDRATION SIZE = '3200cc'),
    ON MEMORY (
      MIN SIZE = '100cc',
      MAX SIZE = '3200cc',
      SCALE UP UTILIZATION = 0.75,    -- of peak heap over the window below
      SCALE DOWN UTILIZATION = 0.40,
      SCALE UP AFTER = '2m',
      SCALE DOWN AFTER = '30m'
    )
  )
);
```

The parser already has the right shape for this. `AUTO SCALING STRATEGY` is
parsed as "a comma-separated set of sub-policies, each named by a leading
keyword" (`parse_cluster_option_auto_scaling_strategy`), with a hard-coded
`expect_keywords(&[ON, HYDRATION])` where a dispatch on the keyword after `ON`
belongs. Of the words above, `Memory`, `Scale`, `Up`, `After`, `Max`, `Size` and
`Duration` are already in `src/sql-lexer/src/keywords.txt`; only `Down`, `Min`
and `Utilization` are new, and the phrasing above was chosen to keep that list
short. New keywords have to clear `src/sql-parser/tests/keyword_audit.rs`.

Gating needs two new knobs, and cannot reuse the existing ones.
`enable_auto_scaling_strategy` is already `default: true`, so putting `ON MEMORY`
behind it would ship the sub-policy ungated. So:

* A **new** `feature_flags!` entry for `ON MEMORY` acceptance, with
  `enable_for_item_parsing: true` like its siblings. That property is not
  optional: stored `CREATE CLUSTER` statements are re-parsed at catalog
  rehydration, where dyncfgs are not consulted, so a dyncfg gate could leave a
  stored statement unparseable once the flag went off.
* A **break-glass dyncfg** for the strategy itself, mirroring
  `enable_hydration_burst`, so the strategy can be stopped environment-wide
  without touching graceful reconfiguration, `ON REFRESH`, or burst.

Both should default off in production and on in the test/CI configuration
(via `system_parameter_default`), so sqllogictest and testdrive exercise the
path before it earns trust.

The asymmetry is the design, not a tuning detail. Scaling up is cheap and safe:
the target hydrates alongside a serving replica set, and the failure mode is a
slightly larger bill. Scaling down is expensive and risky: it costs a full
rehydration, and the target may not fit. So up is fast and down is slow, with a
wide dead band between the thresholds so a workload oscillating around one level
does not thrash.

**The signal must be a windowed peak, not an instantaneous sample.** Memory is a
high-water-mark resource: a mean utilization of 40% with a 95% peak does not fit
on a smaller replica. A scale-down decision has to be made against
`max(heap_bytes / heap_limit)` over the trailing `SCALE DOWN AFTER` window,
taken over steady-state (realized-config) replicas only, and summed across
processes within each replica.

Keeping the strategy pure means the window lives in the signal, not in the
strategy. Two consequences for existing types:

* `SignalRequest` is currently three bools. It grows a
  `peak_heap_utilization: Option<Duration>` (the trailing window), and the
  kernel's union takes the max window across strategies. This is the first
  parameterized signal request; the `union` function's exhaustive destructure
  already forces the change to be spelled out.
* `LiveSignals` gains `peak_heap_utilization: Option<f64>`, documented as the
  max over the requested trailing window across steady-state replicas. The
  Coordinator side of the ctx can serve it from the same `ServiceProcessMetrics`
  it already pushes into `mz_cluster_replica_metrics_history`, so no new
  collection path is needed.

**Durable state.** The strategy is pure and holds nothing between ticks, so the
dwell clock must be durable, exactly as `BurstRecord.steady_hydrated_at` is.
A new `scaling` record alongside `reconfiguration` and `burst`, in
`ExpectedClusterState`:

```rust
pub struct ScalingRecord {
    /// When the peak first crossed the scale-up threshold, cleared when it
    /// falls back inside the dead band.
    pub above_since: Option<Timestamp>,
    pub below_since: Option<Timestamp>,
    /// The size in flight, so the strategy can tell its own reconfiguration
    /// from a user's.
    pub in_flight_target: Option<String>,
    /// A size below which scale-down is not retried, learned from a target
    /// that failed to hydrate. See "thrash after a failed scale-down".
    pub floor_size: Option<String>,
}
```

**Gates the strategy must respect**, each reusing a signal that already exists:

* No scale-down while any object is un-hydrated on the steady set (the existing
  `hydration` signal). A mid-hydration replica has not reached steady memory, so
  its utilization is meaningless for sizing.
* No scale-down while a `burst` record is present, for the same reason.
* A user-initiated `reconfiguration` wins and suspends the policy until it
  settles. The user's explicit intent outranks a policy inference.

### The two genuinely new problems

**Thrash after a failed scale-down.** Rollback-on-timeout protects correctness
but not cost: the strategy sees the same low utilization on the next tick and
retries the same doomed target, paying for an overlap replica set every
`SCALE DOWN AFTER`. This needs a learned floor. Proposal: when a reconfiguration
the strategy itself initiated resolves `TimedOut`, write
`scaling.floor_size = that target` and never propose at or below it again until
the policy changes or the cluster's realized size moves up past it. This is the
one safety mechanism the existing framework does not already provide.

**Attribution on the reconfiguration record.** `ReconfigurationRecord` has no
field naming who wrote it, and the strategy must distinguish "the user ALTERed"
from "I scaled" to implement both of the above. `scaling.in_flight_target` covers
it without touching the shared record, and is the narrower change. An
`initiator` field on `ReconfigurationRecord` would be cleaner but widens the
witness for every writer.

## Success Criteria

* A recorded, per-replica, per-hour, durable measure of consumed GiB-hours that
  finance can reconcile against today's provisioned-hours bill before any
  invoice changes.
* A defined, written-down answer for every incomplete metering bucket, with the
  incompleteness visible in the record rather than smoothed away.
* A characterised gap between allocator-allocated bytes and resident bytes, and
  its decay after a large drop, before any invoice depends on the resident
  measure.
* The rollout is bill-neutral by construction: the first shipped state
  reproduces today's invoice exactly, and repricing is a config change.
* A cluster with `ON MEMORY` converges to the smallest size in its family that
  holds its peak working set, without a hydration gap, and without thrashing
  when a target fails to hydrate.
* An oversized cluster's bill falls without any user action.
* No new mechanism for replica swapping: scaling reuses graceful reconfiguration.

## Out of Scope

* Cross-family scaling.
* `ON MEMORY` combined with `SCHEDULE = ('on-refresh', ...)`, matching the
  existing exclusion for `ON HYDRATION`.
* Scaling `replication_factor` (see Finding 3: it is a redundancy knob).
* Storage (persist) pricing, which is already byte-hours.
* Per-object cost showback, though the inputs exist. Strong follow-on: see
  Alternatives.

## Recommended increment order

The two halves of the ask carry very different risk, and they separate cleanly.

1. **Measure only.** Turn on the metering record with `floor_fraction = 1.0`.
   Zero invoice change, zero pricing negotiation, and it produces the dataset
   that every subsequent decision depends on. Answerable questions after one
   month of data: what is fleet-wide mean utilization, what would a GiB-hours
   bill have been per account, and how much of the gap is idle-but-provisioned
   versus genuinely-used.
2. **Autoscale only.** Ship `ON MEMORY` while still billing provisioned
   capacity. This is where most of the user-visible value is: the bill drops
   because the controller right-sizes the cluster, using the pricing formula and
   the billing pipeline that already exist. No metering-integrity problem, no
   repricing, no contract renegotiation. If only one of the two halves ships,
   this is the one.
3. **Reprice.** Lower `floor_fraction` once (1) has produced the data to price
   against and (2) has removed the failure mode where a user's bill *rises*
   because they were provisioned tight and consumption spikes.

Doing (3) before (2) is the dangerous order: value-based pricing without
autoscaling shifts variance onto the customer's invoice without giving them a
mechanism to control it.

## Minimal Viable Prototype

Two pieces, both cheap, and the first needs no code at all.

**(a) Price the change against real data.** Everything needed is already in the
catalog. Run against a staging or production environment with history:

```sql
-- Consumed vs. provisioned GiB-hours, priced at each size's own
-- credits-per-provisioned-GiB-hour so the family rate is preserved.
--
-- Both columns are integrated over the same sampled instants, so the
-- comparison is apples to apples, but neither equals the invoice: a
-- sampling gap drops an instant from both. `sampled_hours` against the
-- replica's actual lifetime in the window is the gap measure. Note also
-- that `mz_cluster_replica_history` already excludes system clusters but
-- does not exclude INTERNAL or BILLED AS replicas.
WITH samples AS (
    SELECT
        m.replica_id,
        m.occurred_at,
        SUM(m.heap_bytes)::numeric AS heap_bytes
    FROM mz_internal.mz_cluster_replica_metrics_history m
    WHERE m.occurred_at > now() - INTERVAL '7 days'
      AND m.heap_bytes IS NOT NULL
    GROUP BY m.replica_id, m.occurred_at
),
priced AS (
    SELECT
        r.cluster_name,
        r.size,
        -- One 60s sample stands for 1/60 hour.
        s.heap_bytes / (1024*1024*1024) / 60                  AS consumed_gib_hours,
        (rs.memory_bytes * rs.processes)::numeric
            / (1024*1024*1024) / 60                           AS provisioned_gib_hours,
        rs.credits_per_hour
            / ((rs.memory_bytes * rs.processes)::numeric / (1024*1024*1024))
                                                              AS credits_per_gib_hour
    FROM samples s
    JOIN mz_internal.mz_cluster_replica_history r ON r.replica_id = s.replica_id
    JOIN mz_catalog.mz_cluster_replica_sizes  rs ON rs.size = r.size
)
SELECT
    cluster_name,
    size,
    -- Replica-hours actually sampled, the denominator any gap check needs.
    ROUND(COUNT(*)::numeric / 60, 1)                            AS sampled_hours,
    ROUND(SUM(provisioned_gib_hours))                           AS provisioned_gib_hours,
    ROUND(SUM(consumed_gib_hours))                              AS consumed_gib_hours,
    ROUND(SUM(consumed_gib_hours) / NULLIF(SUM(provisioned_gib_hours), 0), 3)
                                                                AS utilization,
    ROUND(SUM(provisioned_gib_hours * credits_per_gib_hour), 2) AS credits_today,
    ROUND(SUM(consumed_gib_hours    * credits_per_gib_hour), 2) AS credits_value_based
FROM priced
GROUP BY cluster_name, size
ORDER BY credits_today DESC;
```

`credits_today` should land close to the invoice for the window; the residual is
the sampling gap, and `sampled_hours` against each replica's actual uptime
quantifies it. The fleet-wide `credits_value_based / credits_today` ratio is the
revenue impact of the change at `floor_fraction = 0`, and the *distribution* of
that ratio across accounts is the risk: a fleet-wide 0.5 is a pricing decision,
but a handful of accounts above 1.0 is a churn conversation.

**(b) The strategy, against the existing fake ctx.**
`src/cluster-controller/src/tests.rs` is 3723 lines of tests driving the
reconciler through a fake `ClusterControllerCtx`. `MemoryScalingStrategy` is a
pure function; the interesting behaviors are all expressible there with no
cluster and no metrics pipeline: dwell elapsing writes a `reconfiguration`
record, the graceful strategy converges it, a target that never hydrates times
out and sets `floor_size`, and the floor suppresses the retry. That test is the
real de-risking step for Finding 3.

## Alternatives

* **Autoscale, keep billing provisioned capacity** (increment 2 alone). Most of
  the user value, almost none of the risk. Called out in the increment order
  above as the recommended first shippable change, not a rejected option.

* **Bill on `mz_object_arrangement_size_history`** (logical arrangement bytes)
  rather than heap. Rejected as the primary metric: arrangements are not all of
  a replica's memory (batchers, source/sink state, operator scratch, the
  allocator's own overhead), so it systematically under-bills, and the gap is
  workload-dependent, which makes it gameable. It is however the right input for
  *showback*: per-object bytes let a user see which MV drives their cost, which
  is what makes value-based pricing actionable rather than merely cheaper. The
  attribution must display an explicit unattributed remainder rather than
  normalizing to 100%, precisely because arrangements are a subset of heap.

* **Bill on peak GiB per window rather than mean.** Rejected. Peak is what
  *provisioning* must cover, and the autoscaler correctly decides on peak, but
  billing on peak reintroduces the incentive to smooth workloads to game the
  invoice and charges an hour of steady 10 GiB the same as an hour with one
  60-second spike.

* **Bill on container RSS (`memory_bytes`) rather than `heap_bytes`.** Rejected,
  but only because it excludes swap and so under-bills the spill-oriented top of
  each ladder by up to 16x (Caveat B). Both quantities are resident-set
  measures, so this choice does not address allocator retention either way.

* **Bill on `jemalloc_allocated` rather than resident bytes.** Tempting: it is a
  genuinely logical measure and would drop promptly when state is released. Not
  chosen for now because it would diverge from the metric the memory limiter
  enforces on, so a user could be terminated for memory they were not billed
  for. Worth revisiting *together with* the limiter's accounting, since changing
  both at once keeps billing and enforcement aligned.

* **CPU-seconds as the dimension.** Rejected. Memory is the binding constraint
  for materialized state; CPU is near-idle on many steady workloads, so the
  price would not move with the thing the user is buying.

* **A single flat $/GiB-hour across all families.** Rejected on the data: an 8x
  spread in credits per provisioned GiB-hour between `cc` and `highcpu`
  (Caveat A).

* **`replication_factor` autoscaling.** Rejected (Finding 3).

* **Ship repricing before autoscaling.** Rejected. It moves cost variance onto
  the customer's invoice before giving them the mechanism to control it.

## Open questions

1. **Per-family rate, or two dimensions?** Keeping `rate(family)` preserves every
   existing commitment and is the smaller change, but it leaves the price
   sensitive to a family choice the user may not think about. A two-dimensional
   `GiB-hours + CPU-hours` price is more honest and is a much larger commercial
   change. The data in Finding 1 rules out the third option (one flat rate).

2. **How large is the allocator-retention gap, and how fast does it decay?**
   This is the highest-priority unknown, because a slow decay makes
   consumption-based pricing feel broken to the user who just dropped a view and
   saw no change. Measurable now from the existing jemalloc metrics (Finding 2).
   If the gap is bad, does the fix belong in billing (a decay model), in the
   metering endpoint (report allocated bytes too), or in the allocator's
   page-return tuning (which would also improve provisioning)?

3. **Should a swap GiB-hour cost the same as a RAM GiB-hour?** `heap_bytes`
   deliberately counts both, which is what makes the spill-oriented sizes bill at
   all. But swap is materially cheaper to provide, and charging the same rate
   removes the user's incentive to use it. A two-tier rate is possible since
   `disk_bytes` and `memory_bytes` arrive separately; it costs a second rate to
   explain.

4. **Is the learned scale-down floor the right mechanism**, or should a failed
   scale-down instead trigger exponential backoff on the retry interval? The
   floor is stable and cheap but permanently forecloses a size that might fit
   after the workload shrinks; backoff keeps retrying at a cost.

5. **`SignalRequest` growing a parameterized field.** It is three bools today,
   and the peak-utilization window makes it carry data. Acceptable, or should the
   window be a dyncfg in `ConfigSignals` instead, giving up per-cluster
   `SCALE DOWN AFTER`?

6. **What does a bill-neutral rollout do about a *tightly* provisioned cluster?**
   Value-based pricing lowers most bills, but a cluster running at 95%
   utilization with a spiky peak could bill *more* under a mean-consumption
   metric than under its provisioned rate if the floor is set below 1.0 and the
   peak lands inside the window. The floor's `max()` protects against billing
   below provisioned-times-floor; nothing protects against billing above it. Does
   the billable quantity need a ceiling at `provisioned_gib` too?

7. **Interaction with blue-green and 0dt upgrades.** During a 0dt upgrade the new
   environment's controller provisions nothing (its catalog writes are gated in
   read-only mode), so `ON MEMORY` is inert there, matching `ON HYDRATION`. But
   the *metering* side runs in both environments during the overlap. Which one
   owns the billing record for the overlap window?

8. **Does `ReconfigurationRecord` need an `initiator`?** `scaling.in_flight_target`
   is the narrower change and is probably sufficient, but an explicit initiator
   would also let the introspection view tell a user why their cluster resized.
