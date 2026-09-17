---
title: "Load testing"
description: "How to load test Materialize: what sits in the read critical path, how to build a trustworthy load generator, and how to reach the best throughput and latency."
menu:
  main:
    parent: serve-results
    weight: 47
    name: "Load testing"
---

This guide explains how to design a load test that measures what Materialize
can actually do: what sits in the critical path of a query, which knobs move
throughput and latency, and — just as importantly — how to build a load
generator that doesn't become the bottleneck itself.

In our experience, the majority of disappointing load-test results are not
produced by Materialize. They are produced by the test harness: an
under-provisioned load generator, a CPU-throttled proxy, or an unindexed query
compiling a new dataflow on every request.
This guide helps you rule all of that out, so that whatever number you end up
with is a number you can trust.

## The read critical path

Every `SELECT` travels this path:

```
client / driver
   │  (network, TLS)
   ▼
balancerd            ── pgwire proxy, TLS termination
   │
   ▼
environmentd         ── parse & plan, timestamp selection,
   │                    peek dispatch  (the "coordinator")
   ▼
cluster replica      ── reads the result out of an
   │                    index (arrangement) in memory
   ▼
back through environmentd → balancerd → client
```

Two properties of this path shape everything else in this guide:

1. **For well-indexed queries, per-query cost is dominated by coordination,
   not computation.** A *fast path* query (see below) is answered by looking
   up a key in an index the cluster already maintains — microseconds of
   replica work. Most of the remaining per-query cost is protocol work in the
   coordinator: parsing, planning, and timestamp selection. This is why the
   biggest wins in this guide come from query and workload shape, not from
   hardware.

2. **The coordinator sequences all queries in the environment.** For fast
   path workloads, throughput eventually plateaus at the rate the coordinator
   can sequence statements, no matter how many serving clusters or client
   connections you add. Past that plateau, additional concurrency only adds
   queueing latency. (Standard path workloads usually saturate the serving
   cluster first — see the sizing section below.)

### What you control

| Lever | Materialize Cloud | Self-managed |
|---|---|---|
| Query shape / indexes (fast path) | ✔ | ✔ |
| Isolation level | ✔ | ✔ |
| Connection count / pooling | ✔ | ✔ |
| Cluster (replica) sizes | ✔ | ✔ |
| Load generator design | ✔ | ✔ |
| `environmentd` resources | managed for you | ✔ |
| `balancerd` resources | managed for you | ✔ |

Everything in the top section of the table applies identically to Cloud and
self-managed deployments — and those levers are where most of the performance
lives.

## Make every serving query a fast path query

Materialize executes a `SELECT` in one of two ways:

- **Fast path**: the query can be answered by an equality lookup against an
  existing [index](/fundamentals/concepts/indexes/). No dataflow is built; the coordinator
  sends a "peek" for the key to the serving replica, which reads the rows out
  of the in-memory arrangement.
- **Standard path**: the query cannot be served from an existing index as
  written, so Materialize compiles a **temporary dataflow** on the cluster,
  runs it, returns the result, and tears it down. This is correct but orders
  of magnitude more expensive per query.

A serving workload that is 90% fast path and 10% standard path will spend
most of its cluster CPU on the 10%. Before load testing, verify **every**
query in your serving mix:

```mzsql
EXPLAIN SELECT * FROM winning_bids WHERE item_id = 42;
```

Fast path queries say so explicitly:

```
Explained Query (fast path):
  ...
  →Index Lookup on materialize.public.winning_bids ...
```

If a query you expect to be cheap is *not* fast path, reshape the maintained
objects so that it is:

- **Index the exact equality key the query filters on.** A filter on
  `customer_id` needs an index on `customer_id` — an index on a different
  column, or on an expression, will not be used for the lookup.
- **Push work into the view, not the query.** A query that unions, joins, or
  aggregates at `SELECT` time compiles a dataflow. Define a view that
  produces the final shape, index it on the lookup key, and query that view
  with a simple equality filter. For example, a "find all edges touching this
  node, in either direction" query written as `... WHERE src = $1 UNION ALL
  ... WHERE dst = $1` is standard path; a view that pre-unions both
  directions keyed by a single `node_id` column, indexed on `node_id`, makes
  the same question a fast path lookup.
- **Isolate unavoidable ad-hoc queries.** If your workload genuinely includes
  ad-hoc joins or traversals, run them on a separate cluster so their
  dataflow cost cannot interfere with the latency of the fast path serving
  traffic.

## Choose the isolation level deliberately

Materialize defaults to [strict
serializable](/serve-results/isolation-level/) isolation, which guarantees
linearizability at the cost of extra coordination on every read. Many serving
workloads — dashboards, entity lookups, API reads that don't need to
read-their-own-writes across sessions — are well served by `serializable`,
which is cheaper on the read path:

```mzsql
SET transaction_isolation = 'serializable';
```

Whichever level you choose, **pin it explicitly in the load test and record
it with your results**. Isolation level materially affects both latency and
throughput, and an unstated default makes results impossible to compare.

## Understand concurrency: more connections ≠ more throughput

A closed-loop load test (every user waits for a response before sending the
next query) obeys Little's law:

```
throughput ≈ concurrent connections ÷ average latency
```

Once you saturate the environment's statement pipeline, throughput stops
rising — and every additional connection simply waits longer. The signature
is unmistakable: doubling users leaves throughput flat while median latency
doubles, and *every* query type inflates by the same amount regardless of its
cost (because the wait happens in a shared queue before execution).

Sweep concurrency to find the knee: run the same fixed-duration test at
increasing connection counts and plot throughput and p50/p99. The best
operating point is the lowest concurrency that reaches the plateau —
typically a modest number (low hundreds of connections, not thousands).
Beyond the knee you are purchasing latency, not throughput.

## Size the infrastructure for serving

### Serving clusters: size for the work your queries actually do

How cluster size relates to QPS depends entirely on which execution path your
workload takes:

- **Fast path workloads: size for state, not QPS.** Fast path lookups barely
  use the serving cluster — the arrangements are already maintained, and a
  peek is a keyed read. In our testing, the same fast path workload measured
  **identical throughput on clusters four times apart in size**, with the
  larger replica nearly idle; the throughput plateau lived in the coordinator.
  For these workloads, size the cluster for the **memory** to hold your
  indexed views (watch sustained memory utilization) and for **hydration**
  (rebuilding state after a restart or resize) — not for query throughput.
  Per-peek coordination overhead even grows slightly with worker count, so an
  oversized cluster can cost a little throughput on peek-heavy workloads.
- **Standard path workloads: cluster CPU is the throughput.** Every standard
  path query compiles and runs a dataflow on the cluster, so QPS scales with
  the cluster's compute — here, scaling the cluster up (or isolating these
  queries on their own cluster) directly raises throughput and protects the
  latency of any fast path traffic sharing the environment.

If your mix contains both, measure them separately: the standard path fraction
will dominate cluster CPU, and its ceiling responds to cluster sizing, while
the fast path fraction's ceiling does not.

### Self-managed: `environmentd` and `balancerd`

{{< note >}}
This section applies to self-managed deployments only. In Materialize Cloud,
these components are sized and managed by Materialize.
{{< /note >}}

- **`environmentd`** hosts the coordinator, and its CPU is where fast path
  throughput saturates. Watch `environmentd` container CPU during the test:
  if it is pinned near its allocation while serving clusters idle, it is the
  ceiling. Give it dedicated headroom (several full cores; production serving
  deployments commonly run 8–16 CPUs) and note that coordinator scaling is
  sublinear — at high statement rates, reducing per-statement work (fast path
  query shapes) often buys more than adding cores.
- **`balancerd`** terminates TLS and proxies every byte of every session.
  Give it a real CPU allocation (at least 1 CPU per replica, 2+ replicas)
  and **do not set CPU limits** on it. Its memory scales with connection
  count — budget for your peak connection count, and check the pods'
  restart counts after load tests: a periodically OOM-killed proxy
  manufactures latency and connection errors that look like server problems.

{{< warning >}}
A CPU-throttled container **looks idle on usage graphs**. CPU usage can never
exceed the configured limit, so a starved `balancerd` (or load generator)
shows a calm, flat line while requests queue. To detect throttling, look at
the CFS throttling counters (`container_cpu_cfs_throttled_periods_total` in
cAdvisor/Prometheus metrics), not at CPU usage.
{{< /warning >}}

## Build a load generator you can trust

The load generator is the least glamorous part of the test and the most
common source of wrong conclusions. The failure mode is always the same: the
generator saturates, requests queue *inside the client*, and the queueing is
reported as "database latency."

- **Never CPU-limit the generator.** Kubernetes CPU limits and
  container-platform vCPU allocations (for example, Azure Container Apps
  enforces its vCPU setting as a hard limit, defaulting to 0.5 vCPU) throttle
  the client in ~100 ms scheduling quanta. The symptom is a hard throughput
  plateau with uniformly inflated latency across all query types — while the
  client's own CPU graph looks unremarkable (see the warning above).
- **Distribute across processes.** Python-based tools such as Locust are
  effectively single-core per process; a single process saturates at a few
  hundred to a couple thousand requests per second depending on the driver
  and query mix. Run multiple worker processes (Locust's distributed mode, or
  simply several independent processes) and keep each process's user count
  modest. Locust prints `CPU usage above 90%` when it is the bottleneck —
  treat that warning as invalidating the run.
- **Ramp gradually and warm up.** Establishing hundreds of TLS connections
  at once is itself a load spike (and on a throttled client can starve the
  test entirely). Ramp connections over tens of seconds, and exclude the
  ramp/warmup window from the measured results.
- **Keep the network path honest.** Run the generator in the same region and
  network as Materialize. Never measure through developer tunnels
  (`kubectl port-forward`, SSH tunnels): a single forwarded TCP stream caps
  out far below what the deployment can do.
- **Load shared test data once.** If each simulated user bootstraps its own
  key pool with a table scan, a large ramp becomes a self-inflicted load
  spike that pollutes the measurement window. Load pools once per process
  and share them.

## Measure server-side, not just client-side

The single most valuable cross-check in any load test: compare the latency
the **server** measured against the latency the **client** reported, during
the same window.

```mzsql
-- Statement latency as measured inside Materialize (last 60s).
SELECT
  count(*) AS statements,
  round(avg(extract(epoch FROM finished_at - began_at)) * 1000, 1) AS avg_ms,
  round(max(extract(epoch FROM finished_at - began_at)) * 1000, 1) AS max_ms
FROM mz_internal.mz_recent_activity_log
WHERE began_at > now() - INTERVAL '60 seconds'
  AND sql LIKE 'SELECT%'
  AND finished_status = 'success';
```

If the server reports single-digit milliseconds while your load tool reports
hundreds, the time is being spent in the client, the network path, or a
throttled proxy — not in query execution. (Note that statement logging is
sampled; the sample is ample for a latency cross-check.) The
[Console](/developer-tools/console/)'s Query History provides the same information
interactively.

Alongside client metrics, record during every run:

- `environmentd` CPU (self-managed) — the fast path ceiling indicator,
- serving cluster CPU and memory (`mz_internal.mz_cluster_replica_utilization`),
- `balancerd` CPU, memory, and restart count (self-managed),
- the generator's own CPU and any throttling counters.

## Load testing the write side (CDC / ingestion)

Read QPS is half the story; the other half is whether results stay fresh
while the source database is busy. When testing combined read + write load:

- **Hydrate fully before measuring.** A cluster that is still hydrating (or
  a source still snapshotting) is not in steady state. Check
  `mz_internal.mz_hydration_statuses` and your source's snapshot progress
  before starting the measurement window.
- **Measure freshness, not just throughput.** Track
  `mz_internal.mz_wallclock_global_lag` for the objects you serve from,
  and report it alongside read latency. Bounded single-digit-second lag
  under sustained write load is the healthy pattern; lag that climbs without
  recovering means the ingestion or transformation clusters need attention.
- **Watch transformation-cluster memory, not just CPU.** Sustained memory
  above ~90% on a cluster is the primary early-warning signal — query latency
  and freshness often look healthy right up until an out-of-memory restart.
  Size for headroom before pushing write rates.
- **Mind source-side retention windows.** For CDC sources (e.g. SQL Server),
  ensure the retention window on the source comfortably exceeds any planned
  pause or outage during a multi-day test, or the source may become
  unrecoverable and require a re-snapshot.
- **Expect the source database to be a bottleneck too.** At high write rates,
  the upstream database's own CPU, log throughput, and engine settings often
  cap the test before Materialize does. Record source-side metrics with the
  same care as Materialize-side ones.

## Checklist

Before trusting a load-test number:

- [ ] Every serving query shows `Explained Query (fast path)` under `EXPLAIN`.
- [ ] Isolation level is set explicitly and recorded.
- [ ] Concurrency was swept; you're reporting the knee, not an arbitrary point.
- [ ] The generator is distributed, un-throttled, in-region, and never
      printed a CPU warning.
- [ ] Server-side latency (`mz_recent_activity_log`) was compared against
      client-reported latency.
- [ ] (Self-managed) `environmentd` CPU and `balancerd`
      CPU/memory/restarts were captured; no CPU limits on `balancerd`.
- [ ] (With writes) clusters were fully hydrated first; freshness lag and
      transformation-cluster memory were recorded.

## Related pages

- [Indexes](/fundamentals/concepts/indexes/)
- [Isolation levels](/serve-results/isolation-level/)
- [Connection pooling](/serve-results/connection-pooling/)
- [Optimize query results serving](/transform-data/optimization/)
- [Observability](/observability/)
