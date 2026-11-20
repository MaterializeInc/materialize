# Cluster query routing

- Associated:
  - [#40771](https://github.com/MaterializeInc/materialize/pull/40771), the prototype
  - [#38857](https://github.com/MaterializeInc/materialize/pull/38857), which sends replica-targeted peeks only to their target, and which this design builds on
  - [#40721](https://github.com/MaterializeInc/materialize/pull/40721), the catalog serving cluster spike, which motivated this work

## The Problem

A cluster replica runs every query sent to its cluster. The compute
controller broadcasts each peek and each transient dataflow to all replicas
and returns the first response
(`doc/developer/reference/compute/active-replication.md`). Replicas exist to
mask crashes and gray failures. They add no read capacity.

This has three consequences for clusters that serve queries.

### 1. Adding a replica does not add throughput.
Every replica does the full work
of every query, so a cluster with three replicas answers the same number of
queries per second as one with a single replica, at three times the cost.
Measured locally on a 3-replica cluster (`scale=1,workers=1`), six clients
running a slow-path aggregate reached 29.7 qps with a p50 latency of 203 ms.

### 2. Adding a replica does not isolate expensive queries.
A slow-path query builds its dataflow on every replica, so one query that exhausts memory or holds
a worker without yielding ([CPU-333](https://linear.app/materializeinc/issue/CPU-333)) stalls all
of them at once. On staging, a heavy ad-hoc query that pushed a 1-replica cluster into swap left
that cluster without a successful query for 138 s. The equivalent broadcast measurement with more
replicas was not taken, but every replica runs the same dataflow, so every replica stalls.

### 3. The only way to scale reads today is to scale up, which puts an upper bound on reads.
 Users who want horizontal read scaling create several single-replica clusters, duplicate their
indexes on each, and round-robin between them in the client. That does not work for traffic the
client cannot steer. In particular, catalog queries from the Console and from `SHOW` commands
are auto-routed to a single cluster, which is the case behind the catalog serving cluster spike
(#40721).

## Success Criteria

- Adding a replica to a serving cluster increases its throughput for reads
  that do not target a replica.
- One expensive read affects the other reads on at most one replica.
- Every replica still holds every maintained collection, so losing a
  replica costs no hydration time for indexes and materialized views.
- Losing a replica does not fail in-flight fast-path reads.
- Existing clusters behave exactly as today unless their owner opts in.
- Builtin clusters can opt in before the feature is available to users.

## Out of Scope

- Balancing maintained dataflows (indexes, materialized views, sinks). A
  replica that lacks a maintained collection cannot serve reads of it, and
  the active replication model depends on every replica replaying the same
  history of maintained collections.
- Load-aware routing, session or role affinity, and hedged requests. Each
  fits later as a nested option, for example
  `QUERY ROUTING = BALANCED (AFFINITY = ROLE)`. The prototype shows that
  round-robin with a readiness check already meets the throughput criterion.
- Moving slow-path reads, `SUBSCRIBE`, and `COPY TO` to another replica when
  their replica fails. Moving them means rebuilding their dataflow on another
  replica from its `as_of`, and for `SUBSCRIBE` resuming the output where the
  client left off. They error, as targeted reads of the same kinds do today.
- Changing the number of replicas automatically. This is the subject of
  `20260522_cluster_autoscaling.md`.
- Routing across clusters.

## Solution Proposal

A new cluster option, `QUERY ROUTING`, will choose between today's behavior
and sending each read to one replica.

```sql
CREATE CLUSTER serving (SIZE = 'M.1-small', REPLICATION FACTOR = 3, QUERY ROUTING = BALANCED);
ALTER CLUSTER serving SET (QUERY ROUTING = BROADCAST);
ALTER CLUSTER serving RESET (QUERY ROUTING);
```

`BROADCAST` will be the default and will keep today's behavior. Under
`BALANCED`, the compute controller will send each untargeted peek, and each
dataflow whose exports are all transient, to a single replica. Maintained
dataflows will still go to every replica.

### Statement behavior

The rule is that a dataflow serving one query goes to one replica, and a
maintained dataflow goes to every replica.

| Statement | Placement under `BALANCED` | When its replica fails |
|---|---|---|
| `CREATE INDEX`, `CREATE MATERIALIZED VIEW`, sinks | every replica, as today | unchanged |
| `SELECT`, fast path | the peek goes to one replica | the peek is re-sent to another replica |
| `SELECT`, slow path | the transient dataflow and its peek go to one replica | the read errors |
| Reads of `UPDATE`, `DELETE`, `INSERT ... SELECT` | as `SELECT` | as `SELECT` |
| `SUBSCRIBE`, `COPY TO` | the transient dataflow goes to one replica | errors with "target replica failed or was dropped", as targeted subscribes do today |
| Any read with `SET cluster_replica` | the named replica | as today |
| Reads of replica-targeted materialized views | their target replica | as today |

An explicit `cluster_replica` will always take precedence over the cluster
option, so introspection queries keep working unchanged.

### Mechanism

The controller already routes commands for replica-targeted collections to
one replica, and history replay to a new replica skips commands targeted at
another replica. #38857 extends this to peeks and to the transient index of
a slow-path peek. `BALANCED` will reuse that path by choosing a target when
the adapter did not supply one:

- When the adapter creates a dataflow whose exports are all transient, the
  controller will choose a replica from the dataflow's index imports at its
  `as_of`. The slow-path peek that follows reads the transient index, which
  now has that target, so it goes to the same replica.
- When the adapter issues a peek on an untargeted collection, the controller
  will choose a replica from the peeked collection at the peek timestamp.

Read holds will not change. The controller already holds each peek's inputs
until the peek finishes, whichever replica serves it.

### Replica choice

The controller will choose round-robin among, in order of preference:

1. replicas that host every input and whose write frontier for each input is
   already past the read time,
2. if there are none, any replica that hosts every input,
3. if there are none, any replica.

The first tier keeps reads off replicas that are hydrating, restarting, or
lagging, without a separate health signal. A replica stuck while still
connected, for example deep in swap, drops out of the first tier only while
another replica is ahead of it. See Open questions.

### Failure handling

When a replica is dropped or disconnects, the controller will re-send its
in-flight balanced fast-path peeks to a replica chosen by the same rule.
This is safe because every replica hosts the peeked collection. Under
#38857, peeks that target a removed replica are finished with an error. The
re-send will run before that cleanup, and only for peeks whose target the
controller chose. Peeks with a user-supplied target will still error.

### Relation to active replication

`doc/developer/reference/compute/active-replication.md` describes the model
this design changes. The compute controller keeps one command history per
cluster and applies it to every replica. Replicas process commands
deterministically, so the controller needs a response from only one of
them. The model has one primary goal and three secondary goals. `BALANCED`
keeps all of them for maintained collections and gives up one of them for
reads.

| Goal | Under `BALANCED` |
|---|---|
| Mask rehydration time after a replica crash | Kept. Every replica still hydrates every index and materialized view. |
| (a) Resize by adding and dropping replicas | Kept. A new replica starts receiving reads once its frontiers pass the read time. |
| (b) Mask a partition or slow network link to some replicas | Partly kept. A disconnect moves in-flight fast-path reads to another replica. A slow link that does not disconnect delays the reads sent over it. |
| (c) Mask slow processing on some replicas | Given up for reads. Under broadcast the first response wins, so a slow replica costs nothing. Under `BALANCED`, a read sent to a slow replica waits for it. The readiness tier covers slowness that shows as frontier lag, but not a replica that is current and slow. |

The reference doc also states that a replica which applies the full history
reaches the same state as every other replica. Replica targeting already
narrowed this before this design. Targeted subscribes, targeted `COPY TO`,
and replica-targeted materialized views are commands only one replica
receives, and history replay skips them on the others. With `BALANCED`, the
invariant will hold for maintained collections, and a transient collection
will exist only on the replica serving it. Balanced commands do not
accumulate in the history. Reduction drops a peek once its `CancelPeek` is
recorded, and drops a transient dataflow once its outputs compact away, so a
replica added later never replays a finished read.

The reference doc lists high availability as out of scope, and this design
does not change that. `active-replication.md` will be updated to describe
targeted and balanced commands as the exceptions to the every-replica rule.

### Catalog and SQL

The option will be stored in the cluster's durable config, for managed and
unmanaged clusters, and will need a catalog version bump with a migration
that sets every existing cluster to `BROADCAST`. `SHOW CREATE CLUSTER` will
render the option only when it is not the default. The controller will
receive the setting at bootstrap, at cluster creation, and on `ALTER`.
Changing it will not restart replicas. It will apply to reads issued after
the change.

### Rollout

A new feature flag, `enable_cluster_query_routing`, will gate setting the
option. It will be off in production and on in CI. System roles will bypass
the flag, so builtin clusters, which only system roles can alter, can use
`BALANCED` before users can. `RESET (QUERY ROUTING)` will never be gated,
so a cluster can always return to the default after the flag is turned off.

The first planned user is the catalog serving cluster from #40721. User
availability will follow once the open questions about stuck replicas and
visibility are settled.

### Observability

`mz_compute_commands_total` already counts peeks and dataflows per replica,
which shows the distribution. Whether the setting needs a column in
`mz_clusters` is an open question.

### Testing

- A sqllogictest file covering flag gating, system roles with the flag off,
  `RESET` while gated, invalid values, `SHOW CREATE CLUSTER`, correct
  results for fast-path, slow-path and join reads under `BALANCED`,
  explicit replica targets, and unmanaged clusters.
- Parser test cases.
- Property tests for the catalog migration.
- TODO(author): a test that counts per-replica peeks under `BALANCED`, in
  the style of the targeting test in #38857, and a test that drops a replica
  during fast-path reads.

## Minimal Viable Prototype

#40771 implements this design, stacked on the targeting change. Measured
locally on 3 replicas (`scale=1,workers=1`) with an index on a 200k-row
table.

Compute commands per replica for 60 reads:

| | `BROADCAST` | `BALANCED` |
|---|---|---|
| fast path | 60 peeks on each replica | 20 / 20 / 20 |
| slow path | 60 peeks and 60 dataflows on each replica | 20 / 19 / 21 |

Six untargeted clients running a slow-path aggregate for 20 s:

| | `BROADCAST` | `BALANCED` |
|---|---|---|
| throughput | 29.7 qps | 83.1 qps |
| p50 / p99 latency | 203 / 215 ms | 72 / 114 ms |

Dropping from 3 replicas to 2 while four clients ran fast-path reads under
`BALANCED` completed 44,274 reads with 0 errors.

On staging, the catalog serving cluster spike balanced reads across `M.1-micro`
replicas while one client ran a heavy query that pushed its replica into
swap. The longest stretch without a successful read was 138 s with 1
replica, 5.0 s with 2, and 4.9 s with 3.

## Alternatives

### Separate single-replica clusters

Users can create N single-replica clusters, create every index on each, and
round-robin in the client. This works today and gives the same capacity.
It needs a client that can steer traffic and retry on failure, which
auto-routed catalog queries and most tools do not have. It also multiplies
the DDL: every index is created and kept in sync N times, and scaling means
creating a cluster and its indexes instead of changing the replication
factor.

### A session variable

A session setting such as `SET query_routing = balanced` would let each
client choose. Used alone, it fails the clients that most need balancing,
like the Console and auto-routed catalog queries, which do not set session
variables per cluster.

Used as an override of the cluster default, it would let a session opt in
to `BALANCED` on a `BROADCAST` cluster, or opt out to `BROADCAST` on a
`BALANCED` cluster. Opting in only reduces the work a cluster does. Opting
out multiplies a session's cost by the replication factor on a cluster
whose owner chose to balance, so the owner would need a way to forbid it.
`SET cluster_replica` already gives a session one override, pinning to a
named replica. The first version will ship only the cluster option, and a
session override can be added on top of it without changing the option.

### Separate routing for peeks and subscribes

The option could route peeks and `SUBSCRIBE` independently, for example
`QUERY ROUTING = BALANCED (SUBSCRIBE = BROADCAST)`. The two kinds of read
trade differently. A peek is short, so balancing it costs little when its
replica fails, and fast-path peeks move to another replica. A `SUBSCRIBE`
runs for minutes to days. Under broadcast it survives the loss of any
replica but one. Under `BALANCED` it errors when its one replica fails, and
the client has to resubscribe. Broadcasting it also costs the replication
factor in resources for its whole lifetime, which is the cost balancing
removes. The first version will route both the same way to keep one
setting, and the nested option can be added later. See Open questions.

### Deterministic replica choice

The controller could choose a replica by hashing an identifier, in place of
round-robin. Hashing the query, for example its peek UUID, spreads reads as
evenly as round-robin without a cursor. Hashing the connection ID balances
connections and pins all of a session's reads to one replica.

| | Round-robin per read | Hash per read | Hash per connection |
|---|---|---|---|
| Evenness with few clients | even | even in expectation | uneven, a few busy sessions can share a replica |
| Damage from one heavy session | spread across replicas | spread across replicas | confined to its replica |
| Which replica served a read | not reproducible | reproducible from the read's ID | reproducible from the connection ID |
| `SUBSCRIBE` and reads of one session | can land on different replicas | can land on different replicas | same replica |

Per-connection hashing isolates a misbehaving client better, since its
expensive reads stall one replica and the other sessions on that replica,
while round-robin lets one session reach every replica in turn. It balances
poorly when a few connections carry most of the traffic, as with the
connection pool of an application server.
Any hash also has to respect the readiness tier, so it has to be computed
over the replicas that are currently ready. A rendezvous hash keeps most
sessions on their replica when that set changes.

`BALANCED` will balance reads, with round-robin. It gives the even split the
throughput criterion needs however many connections carry the traffic.
Balancing connections also has to move sessions off a replica that stops
being ready, such as one that restarts or falls behind. Per-read choice
handles that for free, since each read picks among the replicas ready at
that moment. Per-connection affinity fits later as a nested option, as
listed in Out of Scope.

### A separate query policy object

A catalog object holding routing rules, attached to clusters, would allow
richer policies. Nothing in this design needs a policy shared across
clusters, and a nested option on the cluster can grow to cover affinity and
hedging without a new object type.

### A system variable listing clusters

The catalog serving cluster spike used a system variable naming the
clusters to balance. It needs `mz_system` to change, is not visible in
`SHOW CREATE CLUSTER`, and does not survive renaming a cluster.

### Larger replicas

Scaling up a single replica adds capacity without any of this. It does not
isolate expensive reads, it is bounded by the largest size, and hydration
time grows with the replica.

## Open questions

- How should the controller handle a replica that is stuck but connected?
  Candidates are excluding replicas that lag the most advanced replica by
  more than a bound, and re-sending peeks that are pending past a timeout.
  This interacts with CPU-333, where a non-yielding operator keeps a replica
  busy after the read is cancelled.
- Should slow-path reads move to another replica when theirs fails, and
  should the controller or the adapter drive the move?
- Should `SUBSCRIBE` stay broadcast on a `BALANCED` cluster, so that
  long-lived subscriptions keep surviving replica loss?
- Should `mz_clusters` expose the setting?
- Should `BALANCED` become the default for new clusters? This is a product
  decision for after the flag is on in production.
