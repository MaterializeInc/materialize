# More Zero-Downtime Upgrades

We currently do zero-downtime upgrades, where we hydrate an `environmentd` and
its `clusterd` processes at a new version before cutting over to that version.
This makes it so that computation running on clusters is ready when we cut over
and there is no downtime in processing, no lag in freshness. However, due to an
implementation detail of how cutting over from one version to the next works
(see context below) we have an interval of 10s of seconds (typically below 30
seconds) where an environment is not reachable by the user.

This document proposes a design where we can achieve true zero-downtime
upgrades for DML and DQL queries, with the caveat that we still have a window
(again on the order of 10s of seconds) where users cannot issue DDL.

Achieving true zero-downtime upgrades for everything should be our ultimate
goal, but for reasons explained below this is a decently hard engineering
challenge and so we have to try and get there incrementally, as laid out here.

## Goals

- True zero-downtime upgrades for DML and DQL

The lived experience for users should be: **no perceived downtime for DQL and
DML, and an error message about DDL not being allowed during version cutover,
which should be on the order of 10s of seconds**.

A meta goal of this document is to sketch what we need for our larger,
long-term goals, and to lay out an incremental path for getting there,
extracting incremental customer value along the way.

## Non-Goals

- True zero-downtime upgrades for DDL
- High Availability, so no downtime during upgrades or any other time
- Physical Isolation

## Context

I will sketch the current approach as well as some of the related and desirable
features.

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

### Catalog Migrations

When a new version first establishes itself in the durable catalog by writing
down its version and deploy generation, it sometimes has to modify the schema
or contents of parts of the catalog. We call these _catalog migrations_. These
are a large reason for why currently we can't have an older version read or
interact with the catalog once a new version has "touched" it.

There are largely two types of migrations:

- _Streaming Migrations_: an entry of the catalog is modified without looking
  at the whole catalog. This can be adding fields or some other way of changing
  the contents.
- _Global Migrations_: we modify the catalog after having observed all of the
  current content. This can be adding certain entries once we see that they are
  not there, or updating entries with some globally learned fact.

As the name suggests, the first kind is easier to apply to a stream of catalog
changes as you read them, while the second one can only be applied on a full
snapshot.

### Why Halt-and-Restart?

An alternative that we considered would have been to let the read-only
`environmentd` follow all the catalog changes that the leader `environmentd` is
doing, live apply any needed migrations to in-memory state, and then at the end
apply migrations durably before transitioning from read-only to read/write
_within_ the running process.

There are two main things that made us choose the halt-and-restart approach, and
they still hold today:

- We think it is decently hard to follow catalog changes (in the read-only
  `environmentd`) _and_ incrementally apply migrations.
- We liked the "clean slate" that the halt-and-restart approach gives. The
  `environmentd` that is coming up at the new version in read/write mode
  behaves like an `environmentd` that had to restart for any number of
  other reasons. It's exactly the same code path.

In the context of this new proposal, I now think that even with a _within
process_ cutover, this might take on the order of seconds to get everything in
place. Which would mean we still have on the order of seconds of downtime
instead of the "almost zero" that I propose below.

### Related Features

- **High Availability**: No downtime, both during upgrades or any other time.
  Thinking in terms of our current architecture, this requires multiple
  `environmentd`-flavored processes, so that in case of a failure the load can
  be immediately moved to another `environmentd`. This requires the same
  engineering work that I propose in this document, plus more work on top.
  Ultimately we want to achieve this but I think we can get there
  incrementally.
- **Physical Isolation**: Closely related to the above, where we also have
  multiple `environmentd`-flavored processes, but here in order to isolate the
  query/control workload of one use case from other use cases. This also
  requires much of the same work as this proposal and High Availability, plus
  other, more specific work on top.

The thing that both of these features require is the ability to follow catalog
changes and react to them, applying their implications.

The work that they share with this proposal is the work around enabling a
number of components to handle concurrent instances (see below).

Further, to achieve true zero-downtime upgrades for DQL, DML, **and DDL**, we
need something like High Availability to work across multiple versions. The
thing that is hard here is forward/backward compatibility so that multiple
instances of `environmentd` at different versions can interact with the catalog
and builtin tables.

## Proposal

I propose that we change from our current upgrade procedure to this flow:

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

This modified flow requires a number of changes in different components. I will
sketch these below, but each of these sections will require a small-ish design
document of its own or at the very least a thorough GitHub issue.

## Lame-Duck `environmentd` at Old Version

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
catalog at the old version that it understands. We might have to do some
trickery around holding back the since of the catalog shard around upgrades or
similar.

TODO: It could even be the case that builtin tables are compatible for writing
between the versions, because of how persist schema backward compatibility
works. We have to audit whether it would work for both the old and new version
to write at the same time. This is important for builtin tables that are not
derived from catalog state, for example `mz_sessions`, the audit log, and
probably others.

TODO: Figure out if we want to allow background tasks to keep writing. This
includes, but is not limited to storage usage collection.

## Change How Old-Version Processes are Reaped

Currently, the fenced-out `environmentd` halts itself when the new version
fences it via the catalog. And a newly establishing `environmentd` will reap
replica processes (`clusterd`) of any versions older than itself.

We can't have this because the lame-duck `environmentd` still needs all its
processes alive to serve traffic.

Instead we need to change the orchestration logic to determine when the
new-version `environmentd` is ready to serve traffic, then cut over, and
eventually reap processes of the old version.

## Get Orchestration Ready for Managing Version Cutover

We need to update the orchestration logic to use the new flow as outlined
above.

TODO: What's the latency incurred by us cutting over between `environmentd`s
when everything is ready. That's how close to zero we will get with this
approach.

## Get Builtin Tables Ready for Concurrent Writers

There are two types of builtin tables:

1. Tables that mirror catalog state
2. Other tables, which are largely append-only

We have to audit and find out which is which. For builtin tables that mirror
catalog state we can use the self-correcting approach that we also use for
materialized views and for a number of builtin storage-managed collections. For
others, we have to see if concurrent append-only writers would work.

One of the thorny tables here is probably storage-usage data.

## Get Builtin Sources Ready for Concurrent Writers

Similar to tables, there are two types:

1. "Differential sources", where the controller knows what the desired state is
   and updates the content to match that when needed
2. Append-only sources

Here again, we have to audit what works with concurrent writers and whether
append-only sources remain correct.

NOTE: Builtin Sources are variously called storage-managed collections, because
writing to them is mediated by the storage controller. Specifically async
background tasks that it starts for this purpose.

## Concurrent User-Table Writes

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

## Ensure Sources/Sinks are Ready for Concurrent Instances

I think they are ready, but sources will have a bit of a fight over who gets to
read, for example for Postgres. Kafka is already fine because it already
supports concurrently running ingestion instances.

Kafka Sinks use a transactional producer ID, so they would also fight over this
but settle down when the old-version cluster processes are eventually reaped.

TODO: Figure out how big the impact of the above-mentioned squabbling would be.

## Critical Persist Handles for Everyone / Concurrent Instances of StorageCollections

`StorageCollections`, the component that is responsible for managing the critical
since handles of storage collections currently has a single critical handle per
collection. And when a new version comes online it "takes ownership" of that
handle.

We have to figure out how that works in the lame-duck phase. This is closely
related (if not the same) to how multiple instances of `environmentd` have to
have handles for Physical Isolation and High Availability.

Hand-waving, I can imagine that we extend the ID of critical handles to be a
pair of `(<id>, <opaque>)`, where the opaque is something that only the user of
persist (that is Materialize) understands. This can be an identifier of a
version or of different `environmentd` instances. We then acquire critical
since handles per version, and we introduce a mechanism that allows retiring
since handles of older versions (or given opaques), which would require persist
API that allows enumerating IDs (or the opaque part) of a given persist shard.

One side effect of this is that we could get rid of a special case where
`StorageCollections` will acquire leased read handles when in read-only mode
and instead acquire critical since handles at its new version even in read-only
mode.

TODO: Figure this out! It's the one change required by this proposal that
affects durable state, which is always something we need to think carefully
about.

## Persist Pubsub

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

## Alternatives

### Read-Only `environmentd` Follows Catalog Changes

Instead of the halt-and-restart approach, the new-version `environmentd` would
listen to catalog changes, apply migrations live, and when promoted fences out
the old `environmentd` and gets all its in-memory structures, controllers, and
the like out of read-only mode.

This would still have the downside that bringing it out of read-only mode can
take on the order of seconds, but we would shorten the window during which DDL
is not available.

As described above, this part is hard.

### Fully Operational `environmentd` at different versions

This is an extension of the above, and basically all the different work streams
mentioned above (High Availability, Physical Isolation, following catalog
changes and applying migrations).

The only observable downtime would be when we cut over how traffic gets routed
to different `environmentd`s.

In the fullness of time we want this, but it's a decent amount of work.
Especially the forward/backward compatibility across a range of versions. On top
of the other hard challenges.

## Open Questions

- How much downtime is incurred by rerouting network traffic?
