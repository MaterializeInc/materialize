# Zero-Downtime Upgrades, Physical Isolation, and High Availability

Zero-downtime upgrades, high availability, and physical isolation are related
goals that all require multiple `environmentd` instances to coexist and
collaborate on shared durable state. One key insight connects them:

**Zero-downtime upgrades are high availability across two versions.**

The ultimate goal for zero-downtime upgrades is to have two versions of
Materialize running concurrently, serving traffic with no downtime, where we
can cut over network routes when we're confident the new version is ready. This
will enable us to roll out a new version, observe its behavior, and abandon the
update if something isn't right -- true flexibility in version management.

This document proposes an incremental step toward that vision: a "lame-duck"
upgrade procedure that achieves true zero-downtime for DML and DQL queries.
This is not a detour from the end goal but a direct stepping stone: it requires
us to build the foundational capabilities that all three initiatives need --
running multiple `environmentd` instances with concurrent access to shared
state.

We currently have a brief window (10-30 seconds) where an environment is
unreachable during upgrades (see Context for details). This proposal eliminates
that unreachability for DML and DQL, with DDL seeing a brief window where it
cannot proceed.

## Goals

- True zero-downtime upgrades for DML and DQL

The lived experience for users will be: **no perceived downtime for DQL and
DML, and an error message about DDL not being allowed during version cutover,
which should be on the order of 10s of seconds**.

Alternative: we can think about instead delaying DDL statements, they will
eventually be cancelled when we cut over network connections and then succeed
on the new version.

## Non-Goals

- True zero-downtime upgrades for DDL
- High availability, so no downtime during upgrades or any other time
- Physical isolation

## Context

### Current Zero-Downtime Upgrade Procedure

1. New `environmentd` starts with higher `deploy_generation`, higher `version`
2. Boots in read-only mode: opens catalog in read-only mode, spawns `clusterd`
   processes at new version, hydrates dataflows, everything is kept in
   read-only mode
3. Signals readiness: once clusters report hydrated and caught up
4. Orchestrator triggers promotion: new `environmentd` opens catalog in
   read/write mode, which writes its `deploy_generation`/`version` to the
   catalog, **then halts and is restarted in read/write mode by orchestrator**
5. Old `environmentd` is fenced out: it notices the newer version in the
   catalog and **halts**
6. New `environmentd` re-establishes connection to clusters, brings them out of
   read-only mode
7. Cutover: network routes updated to point at new generation, orchestrator
   reaps old deployment

Why there's downtime:

- Old `environmentd` exits immediately when being fenced
- New `environmentd` transitions from read-only to read/write using the
  **halt-and-restart** approach
- Network routes update to new process
- During this window, environment is unreachable

### Version Compatibility and Catalog Migrations

When a new version first establishes itself in the durable catalog by writing
down its version and deploy generation, it sometimes has to modify the schema
or contents of parts of the catalog. We call these _catalog migrations_. These
are a large reason for why currently we can't have an older version read or
interact with the catalog once a new version has "touched" it.

### High Availability

Technically, high availability would include zero-downtime upgrades because it
means that the system is always "highly" available. We historically
differentiated between the two because they require slightly different
engineering work, although with overlap.

Zero-downtime upgrades means that we don't want to impose downtime when doing
upgrades, and high availability means we want to protect from unavailability
that can happen at any other time, say from an `environmentd` process crashing
or a machine failing.

Thinking in terms of our current architecture, high availability requires
multiple live `environmentd` processes, so that in case of a failure the load
can be immediately moved to another `environmentd`.

### Physical Isolation

The goal of physical isolation is to isolate the query/control workload of one
use case from other use cases. Today, users can create separate clusters and
replicas, but all work of query processing and controlling the clusters happens
on a single `environmentd` instance.

Physical isolation would require to shard the workload across multiple
`environmentd` instances, which is how this is related to both high
availability and zero-downtime upgrades.

## Conceptual Framework: How These Initiatives Relate

Given that zero-downtime upgrades are high availability across two versions,
this framing clarifies what makes each initiative unique:

| Initiative | # of Versions | Duration | Primary Challenge |
|------------|---------------|----------|-------------------|
| zero-downtime upgrades | 2 (old + new) | temporary (cutover window) | version compatibility |
| high availability | 1 | permanent | failover coordination |
| physical isolation | 1 (typically) | permanent | workload routing |

All three share a common foundation: the ability to run multiple `environmentd`
instances that can read/write shared state concurrently. The work proposed in
this document directly builds that foundation, making it a stepping stone toward
the full vision rather than a parallel effort.

## Proposal (lame-duck upgrades)

As the next incremental milestone for zero-downtime upgrades, I propose that we
change from our current upgrade procedure to this flow:

1. New `environmentd` starts with higher `deploy_generation`/`version`
2. Boots in read-only mode: opens catalog in read-only mode, spawns `clusterd`
   processes at new version, hydrates dataflows, everything is kept in
   read-only mode
3. Signals readiness: once clusters report hydrated and caught up
4. Orchestrator triggers promotion: new `environmentd` opens catalog in
   read/write mode, which writes its `deploy_generation`/`version` to the
   catalog, **then halts and is restarted in read/write mode**
5. Old `environmentd` notices the new version in the catalog and enters
   **lame-duck mode**: it does not halt, and none of its cluster processes are
   reaped, it can still serve queries but not apply writes to the catalog
   anymore, so cannot process DDL queries
6. New `environmentd` re-establishes connection to clusters, brings them out of
   read-only mode
7. Cutover: once orchestration determines that the new-version `environmentd`
   is ready to serve queries, we update network routes
8. Eventually: resources of old-version deployment are reaped

The big difference to before is that the fenced deployment is not immediately
halted/reaped but can still serve queries. This includes DML and DQL (so
INSERTs and SELECTs), but not DDL.

We cut over network routes once the new deployment is fully ready, so any
residual downtime is the route change itself. During that window the old
deployment still accepts connections but rejects DDL with an error message.
When cutting over, we drop connections and rely on reconnects to reach the new
version.

Implicit in this proposal is that we initially still don't want to support DDL
during the upgrade window. In addition to all the proposed work, this would
require two additional pieces of engineering work. The lame-duck approach lets
us deliver immediate value while building the foundation those pieces will need.

The proposed upgrade flow and the other initiatives requires a number of
changes across different components. I will sketch these below, but each of the
sub-sections will require a small-ish design document of its own or at the very
least a thorough GitHub issue.

## Work required for Zero-Downtime Upgrades (lame-duck upgrades)

### Lame-Duck `environmentd` at Old Version

The observation that makes this proposal work is that neither the schema nor
the contents of user collections (so tables, sources, etc.) change between
versions. So both the old version and the new version _should_ be able to
collaborate in writing down source data, accepting INSERTs for tables and
serving SELECT queries.

DDL will not work because the new-version deployment will potentially have
migrated (applied catalog migrations) the catalog and so the old version cannot
be allowed to write to it anymore. And it wouldn't currently be able to read
newer changes. Backward/forward compatibility for catalog changes is one of the
things we need to get true zero-downtime working for all types of queries.

Once an `environmentd` notices that there is a newer version in the catalog it
enters lame-duck mode, where it does not allow writes to the catalog anymore
and will serve DQL/DML workload off of the catalog snapshot that it has. An
important detail to figure out here is what happens when the old-version
`environmentd` process crashes while we're in a lame-duck phase. If the since
of the catalog shard has advanced, it will not be able to restart and read the
catalog at the old version that it understands. This may require holding back
the since of the catalog shard during upgrades or a similar mechanism. On the
other hand, this might be okay if we assume that the new version restarts about
as fast as the old version, so the new version will be ready to take over about
as fast as the old `environmentd` could restart.

TODO: It could even be the case that builtin tables are compatible for writing
between the versions, because of how persist schema backward compatibility
works. We have to audit whether it would work for both the old and new version
to write at the same time. This is important for builtin tables that are not
derived from catalog state, for example `mz_sessions`, the storage-usage table,
and probably others.

TODO: Figure out if we want to allow background tasks to keep writing. This
includes, but is not limited to storage usage collection and all the
storage-managed collections.

### Change How Old-Version Processes are Reaped

Currently, the fenced-out `environmentd` halts itself when the new version
fences it via the catalog. And a newly establishing `environmentd` will reap
replica processes (`clusterd`) of any versions older than itself.

We can't have this because the lame-duck `environmentd` still needs all its
processes alive to serve traffic.

Instead we need to change the orchestration logic to determine when the
new-version `environmentd` is ready to serve traffic, then cut over, and
eventually reap processes of the old version.

### Get Orchestration Ready for Managing Version Cutover

We need to update the orchestration logic to use the new flow as outlined
above.

TODO: What's the latency incurred by us cutting over between `environmentd`s
when everything is ready. That's how close to zero we will get with this
approach.

## Foundation Work (Required for This Proposal)

The following changes are required for this proposal and form the foundation
for working towards the full goal for zero-downtime upgrades, high
availability, and physical isolation:

### Get Builtin Tables Ready for Concurrent Writers

There are two types of builtin tables:

1. Tables that mirror catalog state
2. Other tables, which are largely append-only

We have to audit and find out which is which. For builtin tables that mirror
catalog state we can use the self-correcting approach that we also use for
materialized views and for a number of builtin storage-managed collections. For
others, we have to find other ways to make them work.

The difficult tables/collections will be those that are not derived from
catalog state. For example:
- Storage-usage data
- Current sessions (`mz_sessions`)

One approach that can work: we add a (possibly hidden) column to collections
that need to be written to concurrently. That column identifies the responsible
writer. Each writer only touches (adds or removes) rows that it is responsible
for. We would need to introduce a continual process whereby currently live
writers can reap data of stale writers. For example a new version would
eventually want to clean out entries in `mz_sessions` that were written by
previous versions.

### Get Builtin Sources Ready for Concurrent Writers

Similar to tables, there are two types:

1. "Differential sources", where the controller knows what the desired state is
   and updates the content to match that when needed
2. Append-only sources

Here again, we have to audit what works with concurrent writers and change when
needed. The same approach that we end up using for tables should work here as
well.

NOTE: Builtin Sources are also called storage-managed collections because
writing to them is mediated by the storage controller via async background
tasks.

### Concurrent User-Table Writes

We need to get user tables ready for concurrent writers. Blind writes are easy.
Read-then-write is hard, but we can do OCC (optimistic concurrency control),
with a loop that does:

1. Subscribe to changes
2. Read latest changes, derive writes
3. Get write timestamp based on latest read timestamp
4. Attempt write; go back to 2 if that fails; succeed otherwise

We can even render that as a dataflow and attempt the read-then-write directly
on a cluster, if you squint this is almost a "one-shot continual task" (if
you're familiar with that). But we could initially keep that loop in
`environmentd`, closer to how the current implementation works.

### Ensure Sources/Sinks are Ready for Concurrent Instances

I think they are ready, but sources will have a bit of a fight over who gets to
read, for example for Postgres. Kafka is already fine because it already
supports concurrently running ingestion instances.

Kafka Sinks use a transactional producer ID, so they would also fight over this
but settle down when the old-version cluster processes are eventually reaped.

Alternative: instead of letting sources and sinks fight, we _could_ shut them
down on the lame-duck deployment to give the new version room to work.

TODO: Figure out how big the impact of the above-mentioned squabbling would be.

### Critical Persist Handles for Concurrent Environmentd Instances

`StorageCollections`, the component that is responsible for managing the
critical since handles of storage collections, currently has a single critical
handle per collection. When a new version comes online it "takes ownership" of
that handle.

We have to figure out how that works in the lame-duck phase, where we have two
`environmentd` instances running concurrently, and hence also two instances of
`StorageCollections`. This is closely related (if not the same) to how multiple
instances of `environmentd` have to have handles for physical isolation and
high availability.

The solution is to track critical handle IDs in the catalog rather than pushing
this complexity into persist. Each version or instance of `environmentd` writes
down its critical handle IDs in the catalog before acquiring them. This allows
other instances or versions to clean up handles when they determine that a
given version or instance can no longer be running. For example, during version
cutover, the new version can retire handles belonging to the old version once
the old version's processes have been reaped.

This approach unifies handle management across zero-downtime upgrades, high
availability, and physical isolation: in all cases, the catalog serves as the
coordination point for tracking which handles exist and which can be retired.

### Persist Pubsub

Currently, `environmentd` acts as the persist pub-sub server. It's the
component that makes the latency between persist writes and readers noticing
changes snappy.

When we have both the old and new `environmentd` handling traffic, and both
writing at different times, we would see an increase in read latency because
writes from one `environmentd` (and its cluster processes) are not routed to
the other `environmentd` and vice-versa.

We have to somehow address this, or accept the fact that we will have degraded
latency for the short period where both the old and new `environmentd` serve
traffic (the lame-duck phase).

## Shared Work Required for all of Physical Isolation, High Availability, and Full Zero-Downtime Upgrades

> [!NOTE]
> Beyond here we are talking about future work that is not part of this
> proposal.

The major work required for all of the related future initiatives is enabling
multiple actors to interact with and modify the catalog. Multiple instances of
`environmentd` need to be able to:

- subscribe to catalog changes and apply their implications
- collaborate in writing down changes to the catalog

The foundational ideas behind this are explained in [platform v2
architecture](20231127_pv2_uci_logical_architecture.md), and there is ongoing
work towards allowing the adapter to subscribe to catalog changes and apply
their implications.

## Work required for Zero-Downtime Upgrades (full vision)

Beyond the ability for multiple actors to work with the durable catalog, for
zero-downtime upgrades with DDL we need the ability for _multiple versions_ to
interact with the catalog. This is currently not possible because of catalog
migrations and because a new version fences out old versions.

A jumping-off point for this work is the recent work that allows multiple
versions to work with persist shards. Here we already have forward and backward
compatibility by tracking what versions are still "touching" a shard and
deferring the equivalent of migrations to a moment when no older versions are
touching the shard. For Materialize as a whole, this means that changes or new
features need to be gated by what version the catalog currently has, and that
catalog version can be different from the version of Materialize that is
currently running. We would then have gating both by regular feature flags and
by catalog version.

## Work required for Physical Isolation

WIP: Maybe leave out of this proposal?

## Work required for High Availability

WIP: Maybe leave out of this proposal?

## Alternatives

### Cut over to new version without halt-and-restart

Instead of the halt-and-restart approach, the new-version `environmentd` would
listen to catalog changes, apply migrations live, and when promoted fences out
the old `environmentd` and gets all its in-memory structures, controllers, and
the like out of read-only mode.

I contend that this can still take on the order of seconds and only an approach
where we use HA across versions can bring us down to true zero downtime.

Additionally, the parts required for live-listening to changes and acting on
them will be tricky to write, though we will need it long term for the other
initiatives.

### Focus on further improving `environmentd` bootstrap times

We currently take on the order of 30s for bootstrapping `environmentd` in our
largest customer environments. Going from there to sub-second restart times
looks prohibitively hard. Plus, there is never a guarantee that you will be
able to restart in time.

Here again I content that only an approach with HA across versions can deliver
true zero downtime.

### True zero-downtime for everything, including DDL

One could say that we should skip lame-duck mode and deliver the full vision of
zero-downtime upgrades for all commands immediately.

I think the lame-duck proposal is more feasible in the short term, and it
directly builds toward that goal by establishing the foundation of concurrent
`environmentd` instances. True zero-downtime upgrades including DDL require us
to solve two additional hard engineering problems:
- subscribing to and applying catalog changes
- forward/backward compatibility for the catalog

These are hard problems, and the lame-duck approach lets us deliver value
incrementally while making progress on the shared foundation.

## Open Questions

- How much downtime is incurred by rerouting network traffic?
