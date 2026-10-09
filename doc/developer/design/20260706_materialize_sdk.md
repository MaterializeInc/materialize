# Materialize SDK: correct `SUBSCRIBE` consumption and a framework for custom sinks

- Associated:
  - Product requirements: [Sink SDK PRD](https://app.notion.com/p/materialize/Sink-SDK-3dd13f48d37b8024985ce70b1d96e9cd) (PRD-85). Overlapping proposal: PRD-87 (`mzq`).
  - Tracking: Linear project "Subscribe SDK" (DEX).
  - [#38468](https://github.com/MaterializeInc/materialize/pull/38468): durable subscribe design, the server primitive this SDK moves onto later.
  - [#37483](https://github.com/MaterializeInc/materialize/pull/37483), [#37517](https://github.com/MaterializeInc/materialize/pull/37517): prototype Rust and Python packages.
  - [#37905](https://github.com/MaterializeInc/materialize/pull/37905): server-side bound on subscribe output buffering.
  - SQL-528: `SUBSCRIBE ... WITH (PROGRESS) ... UP TO` emits no final progress row.
  - [database-issues#5182](https://github.com/MaterializeInc/database-issues/issues/5182): dataflow errors poison a subscribe.
  - [Durable subscriptions pattern](https://materialize.com/docs/transform-data/patterns/durable-subscriptions/): the manual protocol this SDK packages.
  - Prior art: [mz-redis-sync](https://github.com/MaterializeIncLabs/mz-redis-sync), [novu-materialize-integration](https://github.com/MaterializeIncLabs/novu-materialize-integration), [mz-turbopuffer-sink](https://github.com/MaterializeInc/mz-turbopuffer-sink).

## The Problem

Customers want Materialize results in systems we do not sink to natively:
caches, search indexes, their own Postgres, notification services, live UIs,
and agents. `SUBSCRIBE` is the primitive for all of them, and every team that
uses it re-derives the same protocol by hand. The use cases behind the product
requirements are representative:

- A cache that keeps a Valkey or Redis keyspace equal to a materialized view.
  It needs the initial snapshot and a fast, gap-free restart.
- A search index that keeps two turbopuffer namespaces equal to two views, at
  one consistent timestamp, exactly once. Today this runs through Kafka.
- A relational target that writes several views into an application's
  Postgres, transactionally across tables.

The protocol for consuming `SUBSCRIBE` durably is:

1. Subscribe `WITH (PROGRESS, SNAPSHOT = true)`.
2. Buffer updates until a progress message proves a timestamp is closed.
3. Apply the closed batch and persist its frontier atomically.
4. On restart, resume `WITH (PROGRESS, SNAPSHOT = false) AS OF frontier - 1`.
5. Keep history readable at the stored frontier, and fail loudly when it is not.

Each step has a failure mode that is invisible in testing. Every existing
implementation we could find gets a different one wrong:

| Codebase | Commit discipline | Resume | Idempotency | Retractions |
| --- | --- | --- | --- | --- |
| mz-redis-sync | Correct: data and frontier commit together at progress rows | Omits `SNAPSHOT` on resume, so every restart replays the full snapshot | Inherent (keyed writes) | Delegated to `ENVELOPE UPSERT`, but `key_violation` crashes the process and poisons restarts |
| novu-materialize-integration | Persists the timestamp of the first row of a batch, so a crash mid-batch loses the rest of that timestamp | Wall-clock guess of whether the checkpoint is still retained | Content-hash keys, which false-dedup identical payloads across time | Maps a retraction to a revoke, with two bugs |
| A work-queue subscriber in an internal project | No `PROGRESS`, so batch boundaries are a heuristic | Re-snapshots on every restart | Relies on the target being idempotent | Ignored |

None of them reconnects on failure, and none has tests. Our per-language client
docs teach a bare `DECLARE`/`FETCH` loop with no progress handling and no
resume. The durable-subscriptions pattern page is correct, but it is prose that
every user turns into code again.

The rest of this section describes the server behavior a correct consumer has
to account for. Each item was checked against the code.

### Resume arithmetic

With `SNAPSHOT = false`, an `AS OF t` emits times strictly greater than `t`
(`src/compute/src/sink/subscribe.rs:115-122`). A consumer that holds everything
below frontier `F` resumes with `AS OF F - 1`.

### Retention window

`RETAIN HISTORY` sets `since = upper - window`. After an environment outage or
upgrade the upper catches up and the window slides with it, so a one-hour
window can expire during a one-hour outage. A `REFRESH EVERY` view's upper
jumps to the next refresh, so a sink that is down across a refresh can lose
history however short the downtime. Retention belongs to the view's owner, and
any `ALTER` changes every sink's margin without telling it.

A connected subscribe holds its input readable up to what it has emitted, not up
to what the consumer has committed. The data between those two points needs
retention even while the consumer is connected, because after a disconnect the
consumer resumes from its committed frontier.

### Indexed targets

If the subscribed object has an index on the cluster the subscribe runs on, the
dataflow imports the index instead of the persist shard
(`src/adapter/src/optimize/dataflows.rs:326-343`,
`src/adapter/src/coord/indexes.rs:88-93`), and `AS OF` is checked against the
index's `since` (`src/adapter/src/coord/timestamp_selection.rs:277-297`). An
index's default window is one second (`src/adapter-types/src/compaction.rs:20`),
and the view's `RETAIN HISTORY` does not carry over to its indexes. So
`AS OF F - 1` fails on an indexed view once the checkpoint is more than about a
second behind, however long the view's retention. An
index on a different cluster does not count. Index-level `RETAIN HISTORY`
requires `enable_index_options`, which is off by default.

### Snapshot elision

`SUBSCRIBE <object>` with `SNAPSHOT = false` never reads the snapshot
(`src/transform/src/dataflow.rs:464-522`,
`src/persist-client/src/operators/shard_source.rs:438-448`). Any query form,
including `SELECT * FROM mv` or a projection and filter, reads the whole
snapshot at the `AS OF` before emitting anything. A non-materialized view is
inlined, so its inputs' snapshots are read too.

### Stream shape

Diff output is ordered by (time, row bytes) and consolidated
(`src/compute/src/sink/subscribe.rs:212-217`). Upsert and Debezium envelopes
order by (time, key, row). Retractions do not come first.
`WITHIN TIMESTAMP ORDER BY` changes the order but requires
`enable_within_timestamp_order_by_in_subscribe`, off by default.

Everything between two progress messages arrives together, so one batch spans
many timestamps. "At most one row per key" holds per timestamp, not per batch.

`UP TO` ends without a final progress row (SQL-528), so data after the last
progress row is never closed by the server.

### Buffering limits

When a subscribe's backlog in `environmentd` exceeds
`subscribe_max_buffered_bytes` (default 128 MiB), the subscribe is retired with
`SubscribeFellBehind` (#37905).

`environmentd` also collects every update between two progress messages before
passing them on, and fails the subscribe with "total result exceeds max size"
once that collection exceeds `max_result_size`, a system setting with a default
of 1 GB (`PendingSubscribe::stash` in `src/compute-client/src/service.rs:537-556`,
which also runs in each `clusterd` process).
An initial snapshot is one timestamp, so the whole snapshot must fit. A resume
after long downtime can hit the same limit when the frontier advances in one
step. The failure is deterministic, so a retry repeats it, and nothing the client
does with chunks can avoid it.

### Errors

| Condition | SQLSTATE | Message |
| --- | --- | --- |
| `AS OF` below the readable frontier | `22000` (`DATA_EXCEPTION`, shared) | "could not find a valid timestamp for the query" |
| Subscribed object or cluster dropped | `42704` | "relation 'x' was dropped" and similar |
| Client fell behind | `53200` (shared with the adapter's result-size error) | `SubscribeFellBehind` |
| Result over `max_result_size` | `XX000` (`INTERNAL_ERROR`, shared) | "total result exceeds max size of ..." |
| Dataflow error | `XX000` (`INTERNAL_ERROR`, shared) | the evaluation error, repeated on every retry |

An error raised inside a running subscribe reaches the client as an
unstructured adapter error, which maps to `XX000`
(`src/adapter/src/active_compute_sink.rs:283`, `src/adapter/src/error.rs:1066`).
So a dataflow error, a result-size failure, and a genuine internal error share
one code. `53200` covers both `SubscribeFellBehind` and the adapter's
result-size error (`error.rs:1017-1018`), but a subscribe's size error takes the
`XX000` path, so during a subscribe `53200` means the client fell behind. The
history-loss message has
changed once already (#34712), which broke the one client that matched its
text. A dataflow error repeats until the data or the view changes
(database-issues#5182), so retrying cannot fix it.

## Success Criteria

"PRD" marks a requirement from the product requirements. "Design" marks one
this document adds.

| # | Requirement | Source |
| --- | --- | --- |
| R1 | Exactly-once state in the target, across restarts and crashes | PRD |
| R2 | Back off and retry while the target is unavailable | PRD |
| R3 | Dead-letter a batch after too many retries, and reject single rows the target cannot take | PRD |
| R4 | One consistent timestamp across several views | PRD |
| R5 | Transactional writes when the target supports them | PRD |
| R6 | Every change carries its `mz_timestamp` | PRD |
| R7 | Live streaming with no target (UIs, caches, agents) | Design |
| R8 | Bounded client memory, and no server-side buffering on behalf of a slow client | Design |
| R9 | A small set of typed errors, with history loss stopping the consumer by default | Design |
| R10 | Several languages with identical behavior, checked by machines | Design |
| R11 | Sink authors can test without a live environment | Design |

R4 is met in full by targets that can write the whole cut atomically, such as
one Postgres transaction. A target with no transaction over the whole cut, such
as turbopuffer, where only one write request is atomic, gets convergence: after
each completed batch it equals the cut, but readers can see part of a batch
while it is written (see "turbopuffer sink").

A user following the happy path cannot commit an unclosed timestamp, cannot
get the `AS OF` arithmetic wrong, and cannot silently lose or duplicate updates
across a restart. The check is a sink that, after being killed at random points,
holds exactly `SELECT ... AS OF F - 1`, where `F` is its committed frontier.
The frontier is exclusive, so `AS OF F` would also include changes at `F`.

## Out of Scope

- Rewriting subscribe internals. The server changes this program depends on are
  listed under "Materialize-side workstream".
- Exactly-once side effects. No client can make an HTTP call exactly once, so
  event targets get at-least-once delivery with idempotency keys.
- A general Materialize client. Drivers already run queries well. See "Naming".
- Scale-out of one subscription across parallel workers, which has no server
  support.
- Stateless workers (functions, lambdas). They need history kept across idle
  gaps without a hand-sized `RETAIN HISTORY` window, which durable subscriptions
  (#38468) provide.
- Private-preview subscribe features (`ENVELOPE DEBEZIUM`,
  `WITHIN TIMESTAMP ORDER BY`), which are behind flags. Durable subscriptions
  support both envelopes across a resume, so `ENVELOPE DEBEZIUM` can move in once
  its flag lifts.

## Solution Proposal

The SDK will hold the subscribe protocol in one Rust library, the protocol core,
which performs no I/O and is compiled into a thin package per language. Each
package will use that language's own database driver for the connection and
will expose two modules: `subscribe` for live and durable consumption, and
`sink` for writing to targets. Every change will keep its `mz_timestamp`.
Durable consumption, which powers sinks, will store its checkpoint in the target,
fenced by an epoch, and will read one table, source, or materialized view
directly with `SUBSCRIBE <object>`, the same objects `CREATE SINK` accepts. Live
streams that never resume will accept any query. The Rust package comes first,
with Python and Node to follow, and the first sink will be turbopuffer, written
in Rust. The spec, conformance vectors, and an end-to-end
suite will live in this repository and run in the nightlies.

### Naming

This document proposes **Materialize SDK**, with `subscribe` and `sink` as its
first modules. This is the decision we most want reviewers to push on.

"Sink SDK" names the most visible use and excludes live UIs, cache warmers, and
agents that watch a view with no sink at all (R7). "Subscribe SDK" names the
primitive accurately, but the package will need things that are not
`SUBSCRIBE`: checking a view's retention margin, choosing a cluster, attaching
to a durable subscription, and later the WebSocket transport. "Materialize SDK"
gives those a home without renaming the package after the first publish, which
is expensive once users pin it. The risk is that the name reads as a general
client. The scope rules that out: drivers stay the way to run queries, and the
SDK only adds modules for things drivers get wrong.

Package names will be `materialize-sdk` on PyPI, `@materializeinc/sdk` on npm,
and `materialize-sdk` on crates.io.

### Architecture

```
   +---------------------------------------------------------------+
   |  sink:       targets (turbopuffer first), retry, dead-letter  |
   +---------------------------------------------------------------+
   |  subscribe:  durable consumption (checkpoints, fencing,       |
   |              history-loss policy) and live streams            |
   +---------------------------------------------------------------+
   |  transport, per language: the native driver                   |
   |  (psycopg, node-postgres, tokio-postgres), owns connections   |
   +---------------------------------------------------------------+
   |  protocol core, Rust, no I/O: decode, release engine,         |
   |  multi-view cut, tokens, statements, error classification     |
   +---------------------------------------------------------------+
```

The protocol core is a library, not a service. It runs inside the user's
process, compiled into whichever package they install, and never opens a
connection. The package's driver fetches rows from Materialize and passes them
to the protocol core in a function call. The protocol core returns decoded
changes, closed batches, resume tokens, SQL text, and typed errors. "Language
and generation strategy" explains the choice.

### Batches

A batch will hold every change below its frontier that was not in an earlier
batch, and nothing at or above it. Apart from the partial snapshot chunks
described under "Live streams", users will not be able to observe a
half-delivered timestamp.

Every change will keep its `mz_timestamp` (R6). Changes in a batch will be
ordered by timestamp and consolidated per timestamp, never across timestamps.
Consolidating across a batch drops the timestamps that targets need to stamp
rows, to order a delete before a re-insert of the same key, and to derive
idempotency keys that survive a resume. A helper will give the net change per
key across the batch for keyed targets that only want the final state.

Progress will be an exclusive frontier: everything below `F` is committed. The
`- 1` will exist in exactly one function in the protocol core.

Committing and fetching will be separate calls. `commit(frontier)` will run
inside the target's transaction and `next()` outside it. A combined call would
keep the target transaction open while the SDK waits for Materialize, which can
take seconds, or hours for a `REFRESH` view.

```mermaid
sequenceDiagram
    participant M as Materialize
    participant D as Driver
    participant C as Protocol core
    participant S as Sink code
    participant T as Target
    D->>M: FETCH n c
    M-->>D: data rows, progress rows
    D->>C: rows, one call per FETCH
    Note over C: holds rows until a progress row closes them
    C-->>S: batch below frontier F, each change with its mz_timestamp
    rect rgba(127, 127, 127, 0.12)
        Note over S,T: one target transaction
        S->>T: write changes
        S->>T: commit(F), conditional on the epoch
    end
    S->>C: next()
    Note over M,T: after a crash or restart
    S->>T: load checkpoint
    T-->>S: F and epoch
    S->>C: resume(F)
    C-->>D: SUBSCRIBE ... AS OF F - 1
    D->>M: DECLARE c CURSOR
```

### Subscription scope

The SDK will separate two uses of `SUBSCRIBE`:

- A live subscribe, for a UI, a cache warmer, or an agent watching results,
  never resumes. It will accept any query.
- A sink, or any durable subscription that checkpoints and resumes, will read
  one table, source, or materialized view directly, as `SUBSCRIBE <object>` with
  an optional envelope. It accepts no query, projection, or filter. These are the
  objects `CREATE SINK` accepts (`plan_create_sink` in
  `src/sql/src/plan/statement/ddl.rs`), so sinks built on the SDK follow the same
  rule as Kafka sinks.

A sink that needs a projection, a filter, or a join will read a materialized view
that computes it. Resuming a query rehydrates its whole dataflow and needs
history on every input of the query, and a plain view is inlined, so resuming it
costs the same. Only the plain object form skips the snapshot on resume (see
"Snapshot elision"), so with this rule a sink's resume never reads the snapshot.
Reading an object backed by a storage shard also leaves room for server-side
optimizations aimed at that case.

Durable subscriptions (#38468) accept one storage collection with an optional
projection and filter, a wider surface than this. Starting narrow keeps the later
move a transport change, because widening later is compatible and narrowing is
not. Building the statement from an object and an envelope also lets the SDK
place `ENVELOPE` before `WITH` without parsing user SQL.

The SDK will refuse an indexed object at startup. It will check whether the
subscribed object has an index on the subscribing cluster and fail with an error
that names the remedy: a dedicated subscribe cluster with no index on the object.
An index created later makes the next resume fail with the timestamp-selection
error, so the SDK will check again before it treats that error as history loss.
Durable subscriptions attach to storage, so this check applies only before them.

### Live streams

A background loop will fetch with `FETCH <n> c WITH (timeout = ...)` into a
bounded buffer, independent of the consumer's pace. If the consumer falls
behind, the client will fail with a typed error. Pausing the fetch loop instead
would push the backlog into `environmentd` until the server retires the
subscribe (R8).

Durable consumption will use the same fetch loop, so a slow target write fills
the client buffer, not `environmentd`. When the client buffer fills, or the
server retires the subscribe with `FellBehind`, the SDK will resume from the last
commit with exponential backoff and report a metric. Each cycle restarts a
dataflow, so a target that stays slower than the stream needs a larger buffer
or a longer `commit_interval`.

The initial snapshot is one timestamp and can exceed client memory. It will
arrive as chunks marked partial, with the token on the closing chunk. Chunks
bound client memory only. A snapshot or catch-up larger than `max_result_size`
fails on the server before the first chunk arrives (see "Buffering limits"), and
the SDK will report it as `ResultTooLarge` with the remedies: subscribe to a
smaller materialized view, for example one with fewer columns, or have an
administrator raise `max_result_size`. Server-side
chunking would remove the limit (see "Materialize-side workstream"). The sink
module's generations (see "Sink module") give atomic snapshot visibility to
targets that need it.

A frontier advance with no data will yield an empty batch with a fresh token,
so checkpoints keep moving through quiet hours.

`UP TO` will be supported. On server versions without the SQL-528 fix, the SDK
will release everything below `UP TO` once a bounded stream has ended, so no
closed data is lost. An empty `FETCH` with a timeout does not prove the end,
because a timeout and an exhausted cursor return the same empty result
(`src/pgwire/src/protocol.rs:2607-2624`). So after an empty timed fetch on a
bounded stream, the SDK will issue a `FETCH` without a timeout, which waits for
data or the end of the stream (`src/sql/src/plan/statement/scl.rs:262`), and
only an empty result there counts as the end. Gating on the server version
switches the workaround off once the fix ships.

### Checkpoints and fencing

A checkpoint store will have `load(name)` and
`commit(name, epoch, expected, frontier)`. The target is the recommended store,
because writing data and checkpoint in one transaction gives exactly-once state
(R1, R5). A Materialize-table store and a local file store will ship for targets
with no transaction. Neither commits atomically with the target, so both give
`at_least_once` only.

Each worker start will take the next epoch. A commit will succeed only if the
stored epoch equals the worker's and the stored frontier equals `expected`, the
frontier the batch started from. A stale worker gets a typed `Fenced` error.
Without the fence, a second instance of one sink would overwrite the first one's
progress. Under `transactional`, this check runs in the same transaction as the
data, so a batch whose frontier is already committed rolls back as a whole. That
matters when a commit succeeds but its response is lost: the retry finds the
stored frontier already past `expected`, rolls back, and the SDK reads the
checkpoint and treats the batch as committed instead of applying it twice.

Every durable subscription will require an explicit name, which is its
checkpoint identity. A default derived from a class name would give two
deployments one checkpoint, and they would fence each other. Rows from several
views will be tagged by view name, because tagging by list position remaps rows
when the list is reordered.

The checkpoint will also record a fingerprint of the subscription: the object's
name, catalog id, and storage shard (from `mz_internal.mz_storage_shards`, joined
through `mz_internal.mz_object_global_ids`), and the envelope and output column
types. A resume compares it
with the object the name resolves to now, and "Object identity" decides what
each difference means. The shard matters because blue/green deploys use
`ALTER SCHEMA ... SWAP`, which moves names and leaves ids unchanged. After a swap
the same name points at a different object with the same columns, and without
the shard a reconnect would switch objects silently.

### Delivery guarantees

The user will choose one of two guarantees. Under `transactional`, the target
commits the batch and the checkpoint together, so state is exactly once. Under
`at_least_once`, effects run first and the checkpoint commits after. The SDK
will provide an idempotency key built from the subscription name,
`mz_timestamp`, the key columns, and an ordinal within the timestamp. An ordinal
within the batch would break deduplication, because batch boundaries move
across a resume.

When delivery fails, a policy will return `retry` (back off and deliver the
same batch again), `skip` (the user has dead-lettered it, advance past it), or
`stall` (exit, and an operator resumes from the checkpoint). The default will be
exponential backoff from 1s to 1m for ten attempts, then `stall` (R2, R3).
`skip` moves the checkpoint past real data, so it will never be a default and
will always be logged. A target that cannot represent a single row will call
`reject(row, error)`. The row will be logged with its key and timestamp, and the
rest of the batch will commit.

`commit_interval` will be the minimum time between commits, for targets that
prefer fewer, larger writes.

### History loss and retention margin

When the checkpoint is older than the readable frontier, a policy will decide:
`stall` (the default), `resnapshot` (rebuild the target, for keyed targets that
can reconcile), or `skip_to_now` (accept a gap, for notification-style
consumers). Detection will use the error type (R9).

At startup and periodically, the SDK will compare the checkpoint to the
object's readable frontier and report the margin as a metric, with a warning
below a configured threshold. A connected subscribe holds history only up to
what it has emitted, not up to what the sink has committed (see "Retention
window"). The margin therefore has to cover the commit lag (the client buffer,
`commit_interval`, and the target's write time) as well as the retry budget and
an operator's response to a stall. A resume fails once `F - 1` falls below the
object's `since`. With the default one-second window that happens after about a
second of commit lag or downtime, so the SDK will refuse to start durable
consumption when the object's retention is below the commit lag.

Under durable subscriptions the margin becomes the time since the last
acknowledgement against the subscription's `ACKNOWLEDGE WITHIN` deadline. A
subscription that has caught up does not age (#38468), so a healthy sink on a
`REFRESH EVERY` view, a paused source, or a cluster with no replicas does not
expire. The deadline must exceed the retry budget plus an operator's response to
a stall, and the SDK will warn at startup when it does not.

### Object identity

The checkpoint fingerprint lets the SDK tell three kinds of change apart:

| Change | Example | Default | With `refollow` |
| --- | --- | --- | --- |
| Different output columns | the view's definition changed | stop with `SchemaMismatch` | stop |
| Same columns, same storage shard, new catalog id | `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT` | re-run the retention-margin check, then resume | same |
| Same columns, new storage shard | `ALTER SCHEMA ... SWAP` in a blue/green deploy | stop | re-snapshot from the new object, into a new namespace for targets without transactions |
| The name resolves to nothing | the object was dropped while the sink was offline | stop with `ObjectDropped` | stop |

The SDK will look the name up in the catalog before each `SUBSCRIBE`, and it will
classify errors by the statement that raised them. A name that resolves to
nothing, and an id that no longer exists, both fail planning at `DECLARE` with
`XX000` (`src/adapter/src/error.rs:1003`), the same code a dataflow error has. So
any failure of `DECLARE` sends the SDK back to the name lookup and the table
above, and only errors raised during `FETCH` are classified as `StreamPoisoned`
or by the rest of "Typed errors". `42704` arrives only for a drop during a
running stream.

A name swap needs a re-snapshot even when the new object's history covers the
checkpoint. The target holds the old object's state at `F - 1`, and resuming the
new object without a snapshot delivers only its changes after that point. A key
the old object had and the new one never had would then survive forever. The
generation sweep after the re-snapshot removes such keys from a target that
writes data and checkpoint in one transaction, because the epoch check in that
transaction also rolls back a stale worker still reading the old object. A
target without such a transaction cannot fence those writes per document: a
stale worker could write a key the new object never has, at a timestamp after
the snapshot, and nothing would remove it. For such targets, turbopuffer among
them, `refollow` will write the new object into a new namespace and switch
readers when the snapshot completes.

The lookup and the `SUBSCRIBE` are two statements, so a swap between them would
subscribe to the new object after the check passed. The SDK will therefore
subscribe by the catalog id it fingerprinted, with the bracket syntax the catalog
uses for stored definitions: `SUBSCRIBE [u123 AS "db"."schema"."name"]`. That
resolves by id, not by name, and fails with `InvalidId` once the id is gone
(`src/sql/src/names.rs:1585-1608`). A swap leaves ids unchanged, so the stream
keeps reading the object that was checked, and an `InvalidId` failure at
`DECLARE` follows the rule above. The syntax is not documented for
users, so deferred question 2 asks whether the SDK can rely on it.

A replacement keeps the storage shard and gives the view a new catalog id
(`src/adapter/src/catalog/transact.rs:1334`), so its data history is continuous.
It takes its retention and refresh schedule from the replacement object, though
(`MaterializedView::apply_replacement` in `src/catalog/src/memory/objects.rs`).
A replacement created without `RETAIN HISTORY` drops the shard to the
one-second default, so the SDK will re-run the retention-margin check after it
sees a new catalog id, and deploy tooling has to carry `RETAIN HISTORY` onto the
replacement. After a successful resume, the SDK will store the new catalog id in
the checkpoint, so later resumes compare against it. A name swap moves the name
to a different object. The running stream keeps
reading the old object until the deploy drops it, which ends the stream with
`ObjectDropped` (`42704`) and sends it through the same table. Different columns
always stop, because resuming would mix rows of two shapes in one target, even
when the history-loss policy is `resnapshot`.

On a signal, the SDK will finish the in-flight batch, commit, close the cursor,
and disconnect.

### Multi-view consistency

A multi-view subscription (R4) will release changes only up to the minimum
frontier across its members, so every released moment is a consistent cut
across all views. A Materialize timestamp is a global commit order, so one
stored frontier names that cut. Log-position systems cannot do this from
positions alone, which is worth saying in the product story.

The SDK will pick one `AS OF` and pass it to every member, and startup will fail
if any member cannot be read there. Members that pick their own `AS OF` values
hand out one view's snapshot before another's exists.

The lag budget will count only changes the joint frontier holds back. A member's
own snapshot waiting for its own progress row will not count.

Each view's `RETAIN HISTORY` counts from its own upper, so a lagging view can
leave the stored cut readable on one view and compacted on another. The
retention-margin check will run per member.

Members must share the default `EpochMilliseconds` timeline, because timestamps
from different timelines are not comparable and the cut would be unsound. Only
sources with `ENVELOPE MATERIALIZE`, which is behind a flag, get another
timeline (`src/sql/src/plan/statement/ddl.rs:1000-1004`), and objects built on
them inherit it. The SDK will reject a member that is such a source or depends
on one, using `mz_sources.envelope_type`, the envelope of source tables in
`mz_internal.mz_kafka_source_tables.envelope_type` (a table created
`FROM SOURCE ... ENVELOPE MATERIALIZE` carries the envelope, not its source),
and `mz_internal.mz_object_transitive_dependencies`.

A multi-view subscription will hold one connection per member. Any member error
will end all members, and recovery will resume every member at the stored cut.
If one member's history no longer covers the cut and the history-loss policy is
`resnapshot`, the SDK will follow the recipe in #38468. It will re-snapshot each
expired member at a new timestamp `t_i` while the others resume from the cut,
re-establish the cut at `t*`, the largest `t_i`, and buffer every stream until
its progress passes `t*`. It will then apply in one step each re-snapshotted
member's snapshot, replacing its rows through a generation sweep, and every
member's changes through `t*`. The cost is buffering. The live members must hold
every change from the old cut to `t*`, which spans the whole outage, and the
re-snapshotted members hold their changes between `t_i` and `t*`. Targets with
generations will stage these changes in the target and make them visible at
`t*`, and other targets buffer them in memory. The catch-up can still hit
`max_result_size` (see "Buffering limits").

### Typed errors

| Error | Detected by | Default |
| --- | --- | --- |
| `Transient` | no SQLSTATE, `08000`, `08001`, `08003`, `08004`, `08006` | reconnect through the connection factory and resume, within the retry budget, then stall |
| `CredentialsExpired` | `28000` | one reconnect through the connection factory, then `Fatal` |
| `Canceled` | `57014` (a cancel or a timeout) | stop |
| `FellBehind` | `53200` | resume from the last commit with backoff, report a metric |
| `ResultTooLarge` | `XX000` plus the "exceeds max size" message | stop with remedy |
| `HistoryLost` | `22000` plus the timestamp-selection message, after the index re-check | history-loss policy |
| `ObjectDropped` | `42704` | see "Object identity" |
| `SchemaMismatch` | checkpoint fingerprint | see "Object identity" |
| `StreamPoisoned` | `XX000` during `FETCH` with any other message, which includes genuine internal errors | stop |
| `Fenced` | checkpoint commit | stop |
| `IndexedTarget` | startup check, and the re-check before `HistoryLost` | stop with remedy |
| `Fatal` | anything else (auth, TLS, SQL) | stop |

Reconnects go through the connection factory. balancerd refuses connections
with `08004` while `environmentd` restarts (`src/balancerd/src/lib.rs:964-1004`),
so that is retried. The same code also means an unsupported protocol version,
which is why every reconnect counts against the retry budget and a deterministic
failure stalls instead of looping. `08P01` protocol errors are `Fatal`. A session
whose credentials expired ends with `28000` "authentication expired"
(`src/pgwire/src/protocol.rs:667`). Bad credentials share that code, so the SDK
reconnects once with credentials from the factory and treats a second `28000`
as `Fatal`. `57014` covers an operator's cancel and statement timeouts, and
retrying it would make a sink impossible to stop. During
a subscribe, `53200` means `FellBehind`, because the subscribe's own size error
takes the `XX000` path. Until dedicated SQLSTATEs exist, two errors need message
text: `HistoryLost` and `ResultTooLarge`. All message matching will live in one
function, gated on server version, with conformance vectors pinned to each
message. The workstream asks for dedicated codes so this can go away.

Durable subscriptions add three failures: attaching to an expired subscription
and an `AS OF` outside its window both map to `HistoryLost`, and an attach by a
newer reader, which ends the older reader's stream, maps to `Fenced`. Their
detection waits on the error codes #38468 settles on.

### Connections

Each subscription will hold one session for its whole life. Transaction-mode
poolers break `DECLARE`/`FETCH`, and a session-mode pool gains nothing because
the session is never returned. The SDK will take a connection factory as well as
a URL, so rotating credentials (OIDC tokens, app passwords) can plug in. A
multi-view subscription of N views will use N connections.

The WebSocket SQL API supports `SUBSCRIBE` but is experimental. It is the
natural transport for browsers and functions. The protocol core is
transport-agnostic, so adding it later will not change the model.

### Sink module

A sink author will implement three things: open the target, write a batch and
call `commit(frontier)` inside the target's transaction, and optionally
`reject` rows the target cannot take. The SDK will drive everything else:
snapshot, resume, retry, fencing, and the history-loss policy.

Keyed-state targets (caches, search indexes, tables) will read the upsert
envelope. When they re-snapshot, the SDK will write the snapshot under a new
generation and, at the end, call a sweep that removes keys from older
generations. The generation is a counter stored in the checkpoint, raised by a
conditional commit before each re-snapshot attempt starts, and writes after the
snapshot carry it too. A snapshot's `AS OF` cannot serve as the generation,
because a retry can pick the same `AS OF` and a new object after a swap can
have an older one. With a counter, a re-snapshot that crashed and was retried
never reuses a generation, and keys only the failed attempt wrote are swept.
That removes keys deleted while the sink was offline, which a plain
re-snapshot leaves behind. A target that needs the snapshot to appear at once
will make the new generation visible only at the sweep.

The upsert envelope emits `key_violation` rows when a key has more than one
value. The SDK will pass them to `reject` by default, and a sink can choose
`stall` instead, so a key violation never crashes the process.

Event targets (webhooks, queues, notifications) will read the diff envelope
under `at_least_once`, with the idempotency key above. They will declare what a
retraction means: ignore it, or call a compensating function. Compensation will
use a correlation key built from the row's key columns only, because the
idempotency key includes the timestamp and so differs between an insert and its
later retraction.

### turbopuffer sink

The first sink will keep turbopuffer namespaces equal to views. It answers the
search-index use case and will run against our internal context graph, which
has no Kafka, so the existing Kafka-based sink does not fit there. The sink will
write only to namespaces it creates, so every document carries `mz_timestamp`.

The October sink will be written in Rust on the Rust package and will call
turbopuffer's HTTP API directly. Its transforms will be Rust functions that call
an embedding provider over HTTP, so the existing sink's Python transforms are not
reused. The tombstone, condition, and checkpoint rules below do not depend on
the language, so a Python version can follow the same design.

turbopuffer's documentation states that one write request to one namespace is
applied atomically and is durable on return, and that there are no transactions
across namespaces. Conditional writes compare each stored document with the
incoming one (`$ref_new`) and silently skip a row whose condition fails. For
patches, the response counts only the rows whose condition held. An upsert to a
document that does not exist is applied unconditionally, a patch to one is
skipped, and in a namespace with vector attributes every upserted document must
carry every vector.

Plain deletes therefore cannot make writes safe to repeat. If a later batch
deleted a key, replaying an earlier upsert of that key finds no document and
recreates it. A stale worker can do the same before it learns it was fenced,
because conditions are evaluated per document, so its data writes cannot be
conditioned on the epoch in a checkpoint.

The sink will write tombstones in place of deletes. A delete will upsert the
key's document with `deleted = true`, the delete's `mz_timestamp`, a wall-clock
`written_at`, and a placeholder vector where the namespace requires one. Every
upsert, of data or of a tombstone, will be conditional on the stored
`mz_timestamp` being older than the new one. A replayed or stale write of an
older change then meets a newer document or tombstone and is skipped. Before each
request, the sink will net its changes to the latest change per key, because a
batch spans timestamps and turbopuffer rejects a request that names one id
twice. The conditions only ever let a newer timestamp win, so dropping the older
change from a request is safe. While tombstones are kept, every write is then
safe to repeat and ordered by timestamp, so each namespace reaches exactly-once
state under `at_least_once` delivery, and a batch can be split across requests.

A re-snapshot's generation sweep will turn documents from older generations into
tombstones instead of deleting them, so the same protection holds after a
re-snapshot. A patch by filter has no `$ref_new`, so it will set constants:
`deleted = true`, `mz_timestamp` to the snapshot's `AS OF` `t_s`, and
`written_at` to the current time, on documents matching
`generation < g AND deleted = false AND mz_timestamp < t_s`, where `g` is the
attempt's generation. The last clause
keeps the sweep from moving a timestamp backwards: a stale worker from before
the re-snapshot may have written a key at a time after `t_s`, the snapshot's
older upsert of that key is then skipped, and without the clause the sweep would
tombstone a correct document. The patch re-evaluates its filter before applying
(turbopuffer's guarantees page), so a document a live write moves into the new
generation meanwhile is left alone.

Searches will filter on `deleted = false`. By default the sink will keep
tombstones forever, because removing one reopens the hole: a fenced worker that
was paused before an older upsert, and resumes after the tombstone is gone,
recreates the key, and nothing deletes it again until the key changes upstream.
A sink can opt into a sweep that removes a tombstone only when its `written_at`
is older than a grace period and its `mz_timestamp` is below the sink's
committed frontier. The grace period runs from when the tombstone was written,
not from its `mz_timestamp`, because a sink that is catching up writes tombstones
for old timestamps. With the sweep on, exactly-once state holds only if no
worker writes later than the grace period after it was fenced. The SDK cannot
enforce that bound, because a process can pause between checking its epoch and
sending a write, so the sink's docs will state the condition and the grace
period has to exceed `commit_interval`, the retry budget, and the longest pause
the deployment can see. Filter operations are capped per call (5 million rows
for a delete by filter, 50 thousand for a patch by filter), so sweeps loop.

The checkpoint will live in a separate checkpoint namespace, one document per
sink, created once when the sink is set up. Workers will change it only with
patches, which never create a document, so a stale worker cannot recreate a
deleted checkpoint. A worker will take the next epoch with a patch conditional
on the stored epoch being strictly lower than its own, so of two workers starting
at once only one wins. A commit follows the general contract (see "Checkpoints
and fencing"): a patch conditional on the stored epoch being equal to its own and
the stored frontier being equal to `expected`. So a delayed retry of an older
commit cannot move the frontier back, which matters because the tombstone sweep
reads that frontier. On a patch count of zero the worker reads the checkpoint
back: a different epoch means `Fenced`, the same epoch with a frontier at or past
the requested one means the commit already happened, and anything else is a
conflict that sends the worker back to the stored checkpoint. The data
namespaces hold no reserved documents.

Only one write request is atomic, and turbopuffer keeps one version of each
document. A batch can span several requests, even within one namespace, and a
cut spans one request per namespace at least. Between those requests a reader
can see part of a batch: two keys that changed at the same timestamp, one new
and one old, or one namespace at the new cut and another at the old one.
Filtering on `mz_timestamp` cannot rebuild an earlier cut, because an upsert
replaces the earlier version. The guarantee for turbopuffer is therefore
convergence: after each completed batch, every namespace equals the cut. The
sink does not give readers a consistent view while a batch is being written. The
sink's docs will state this, and deferred question 5 asks whether that is enough.

After a name swap, the sink writes the new object into new namespaces (see
"Object identity"). Namespace names carry an incarnation number, for example
`articles__i3`, and the checkpoint document records the active incarnation and
the list of retired ones. The checkpoint document is the reader contract: a
reader looks up the active incarnation there, with a small helper the sink
package ships, and derives every namespace name from it. When the snapshot
completes, the sink switches readers by patching the active incarnation, one
write to one document, so the switch is atomic for every namespace at once for
any reader that looks it up again. Until then, readers of the old namespaces see
data frozen at the swap, and the sink's docs will say so. A stale worker still
reading the old object keeps writing only to the retired namespaces. The sink
deletes them after a grace period, using the durable list in the checkpoint
document. turbopuffer creates a namespace on its first write, so a stale write
after that recreates an unused namespace. Retired incarnations therefore stay on
the list, and every later cleanup removes their namespaces again if they exist.

Embedding cost will follow the existing sink's transform model: a transform
declares the columns it reads, and runs only for rows where those columns
changed. The sink will store a hash of each transform's source
columns on the document. For each netted change it will first make a patch of
the attributes without vectors, conditional on the stored `mz_timestamp` being
older, every stored source hash being equal to the new one, and the document not
being a tombstone. The patch keeps the stored vectors, which are then known to
match the source columns. If the patch count shows it was not applied, because a
source column changed, the document is missing or a tombstone, or a newer
version exists, the sink will compute the document's vectors and make the
conditional upsert. Comparing with the stored hash, not with the previous change
in the stream, stays correct when netting drops intermediate changes and after a
replay. Tombstones skip transforms. A replay re-runs transforms for the replayed rows,
which costs embedding calls but does not affect correctness.

### Durable subscriptions

Durable subscriptions (#38468) give the server a per-consumer hold advanced by
`ACKNOWLEDGE`, with a wall-clock deadline (`ACKNOWLEDGE WITHIN`) in place of a
window measured from the upper. When they land, the SDK will attach with
`SUBSCRIBE USING DURABLE SUBSCRIPTION` and still pass `AS OF F - 1`, positioning
the read at its own checkpoint. If a recreate or `RESET` has moved the
subscription's hold past the checkpoint, that attach fails loudly. Resuming from
the server's position and filtering below the checkpoint would hide the same gap
whenever the opening progress check is missed. The SDK will acknowledge only
after the target commit, including for empty batches. The target-side checkpoint
will stay mandatory, because the server alone is at-least-once. `START AT` will
migrate an existing sink at its stored frontier without a gap.

Subscriptions are provisioned per logical consumer, because creating one is a
catalog transaction (#38468). The SDK will attach to an existing subscription by
name and will not create one on start. Creating it, and choosing its deadline,
is a deploy step.

A blue/green cutover needs a new subscription on the new object, and the sink
re-snapshots from it, for the reason "Object identity" gives. Dropping the old
object needs `CASCADE` while the old subscription exists. Deploy tooling will own
both steps, and `refollow` then attaches to the new subscription with a
snapshot.

Durable subscriptions will remove the retention sizing problem and turn the
retention-margin check into a deadline check (see "History loss and retention
margin"). Stateless workers become possible, because the server holds history
for each consumer. `IndexedTarget` should no longer apply if the durable attach
reads storage, which is still an open question in #38468. The rest of the
protocol core stays. Rows still need decoding and
release at progress, the server can resend data the target already committed, a
multi-view cut still spans several subscriptions, and fencing, retry, and
dead-lettering do not depend on where history lives.

### Security

The SDK will hold Materialize credentials and target credentials in the user's
process. The docs will recommend a dedicated role with `SELECT` on the object and
`USAGE` on the subscribe cluster. The Materialize-table checkpoint store adds
`SELECT`, `INSERT`, and `UPDATE` on its checkpoint table, and durable
subscriptions add the privileges #38468 defines for attaching and acknowledging.
Creating a subscription needs more, which is one more reason it is a deploy step
and not something the SDK does on start. The protocol core performs no network
I/O, so it adds no network surface of its own.

A resume token is not secret but is also not authenticated. Anyone who can
write the checkpoint can move the frontier forward and make the sink skip data,
so the checkpoint needs the same write protection as the target data it sits
next to. A browser transport would put credentials in the browser, which is one
reason it waits for a stable WebSocket API with scoped credentials.

## Language and generation strategy

The options, from least to most shared:

- A. Hand-written per language, one spec, shared test vectors.
- B. Types, errors, tokens, and statement templates generated from one schema,
  with the engine hand-written per language.
- C. One protocol core in Rust with no I/O, compiled into each language package.
- D. SDKs generated by an agent from a precise spec, accepted when they pass the
  conformance suite.

The SDK will use C. The prototype Rust package is already most of this protocol
core: its decoder, release engine, multi-view engine, tokens, statements, and
classification are pure, and only the transport does I/O. A gives every
language its own copy of the state machine, and the prototypes already show the
drift that follows (text values in Rust, native values in Python). B removes
drift in the generated parts but leaves the engine, where the bugs found so far
live, duplicated. D puts all the weight on the spec and suite, which C needs
anyway.

The usual objection to a shared core is that it owns TLS, authentication, and
async integration in every runtime. A protocol core with no I/O owns none of
that. Each language keeps its native driver for the connection, which users
already trust, and feeds rows into the protocol core.

The protocol core will ship in two builds. Python will bind the native build
through PyO3 and maturin (abi3 wheels), and the Rust package will use the
library directly. Node will use the WebAssembly build, the same one browsers and
edge runtimes use, so the Node package is one artifact for every platform. This
repository already publishes Rust compiled to WebAssembly to npm
(`ci/deploy/npm.py`). WebAssembly runs slower than native code, which matters
little here because rows will cross into the protocol core in one call per
`FETCH`, not one call per row. The protocol core will not panic across the
boundary, and its errors will become each language's native exceptions. It will
not depend on Materialize workspace crates, so it builds and versions on its
own.

The WebAssembly build also covers later languages without native builds. Go can
run it through wazero and JVM languages through Chicory, both written in their
own language, which avoids cgo and JNI. If that proves too slow, the language is
hand-written against the conformance vectors.

The costs are a Python wheel per platform, Rust stack traces in Python bug
reports, and a source build that needs a Rust toolchain wherever no wheel
exists. If native packaging proves too costly, the fallback is B plus A.

### Generated and hand-written parts

The binding glue and each package's type definitions will be generated from the
protocol core. wasm-bindgen emits TypeScript declarations, and the Python package
will ship type stubs generated from the PyO3 module. Type mapping lives in the
protocol core: it decodes each Materialize type into one documented value
model, and each binding converts that model to the language's native types
(for example `numeric` to `Decimal` in Python), with the conversions covered by
the vectors. `mz_timestamp` is a u64, so the Node package will expose it as a
`BigInt`, which holds every u64 value where a JavaScript `number` does not.
Each package will hand-write only the transport over its driver,
the idiomatic API surface (iterators in Python, async iterators in Node), and
the sink modules.

### Repository layout and releases

```
misc/materialize-sdk/
  core/          protocol core (Rust library)
  spec/          behavior spec, versioned
  conformance/   vectors, shared by every package
  python/        package: PyO3 binding, psycopg transport, sinks
  node/          package: WebAssembly build, node-postgres transport, sinks
  rust/          package: protocol core directly, tokio-postgres transport, sinks
  test/          mzcompose end-to-end suite, run in the nightlies
```

The SDK will start in its own Cargo workspace under `misc/`, outside the
Materialize workspace, so its dependencies and lockfile stay independent. A
spec change and its fallout in every package then land in one PR, and server
changes run against every package in the nightlies.

Releases will work the way `dbt-materialize` releases do. A pull request bumps
the SDK version and merges to `main`. The deploy pipeline, which runs on every
`main` build (`ci/deploy/pipeline.template.yml`), publishes each package whose
version is not yet on its registry, as `ci/deploy/pypi.py` does today. Releases
need no tags. Python wheels will be built on the existing Linux x86-64, Linux
ARM, and macOS ARM Buildkite queues, with macOS x86-64 cross-compiled on the ARM
agent. Windows has no queue, so it gets the source package, which needs a Rust
toolchain to install. Publishing to crates.io needs a new deploy step. Go is the
exception to the pattern: Go modules are versioned by git tags, so a Go package
needs tags in this repository or a repository of its own.

Each release will publish every package at the same version, built from the
same protocol core, so a version number means the same behavior in every
language. Versions follow semantic versioning. Resume tokens and checkpoints
carry a format version, so a checkpoint written by an older release resumes
after an upgrade or fails with a clear error. The nightlies define which
Materialize versions a release supports.

## Testing

The conformance suite is the contract, whatever the generation strategy.

1. Conformance vectors: language-agnostic JSON files, each with inputs and
   expected outputs for one function (decode, batch release, multi-view release,
   token encoding, statement SQL, and error classification from SQLSTATE and
   message). The protocol core will run them natively, and each package will run
   them through its binding.
2. End-to-end suite: the spec, vectors, and end-to-end suite will live in this
   repository and run in the nightly pipeline against `main` through mzcompose.
   One runner will drive a thin adapter per language over stdin and stdout.
   Scenarios: snapshot then stream, resume after a kill, resume after a server
   restart, history loss, dropped object, poisoned dataflow, fell-behind,
   result over `max_result_size`, indexed object refused at startup and after an
   index appears mid-run, a name swap, a replacement materialized view, and a
   bounded stream. A server change that
   breaks the SDK,
   such as a changed error message, then breaks a nightly before it reaches a
   customer.
3. Convergence test: kill a sink at random points (mid-batch, between write and
   commit, mid-snapshot), restart, repeat, then compare the target with
   `SELECT ... AS OF F - 1` for the committed frontier `F`, for both guarantees.
4. Fencing test: two workers share one checkpoint, the stale one is refused, and
   the target stays consistent. For turbopuffer, the stale worker also writes
   an older upsert of a key the live worker has deleted, and with tombstones
   kept the key must stay deleted. With the sweep on, the same holds when the
   stale write comes within the grace period.
5. Lost-response test: a transactional commit succeeds but its response is
   dropped, and the retry must find the frontier already committed and apply
   nothing twice.
6. User test kit: the recorded-stream player from the vectors will ship to users,
   so sink authors can test their targets offline (R11).

Keeping the vectors and the end-to-end suite next to the server is what lets a
server change fail a test before it fails a customer. See "Repository layout and
releases".

## Observability

The SDK will report frontier lag (wall clock minus frontier), retention margin,
batches and changes applied, retries, dead-letters, reconnects, checkpoint age,
and buffer occupancy, the same in every language. They will be exposed as
callbacks in the libraries and as OpenTelemetry metrics and spans for connect,
snapshot, apply, and commit.

## Materialize-side workstream

Where correct behavior needs the database, the database change is part of this
program.

1. Dedicated SQLSTATEs for history loss, dataflow errors, a subscribe that fell
   behind, and a result over `max_result_size`. Today these share codes with
   other errors (see "Errors").
2. SQL-528: a final progress row clamped to `UP TO`.
3. Server-side chunking: pass the updates between two progress messages on in
   bounded pieces, so a snapshot or catch-up larger than `max_result_size` can be
   delivered. Durable subscriptions do not change this, because a snapshot is one
   timestamp.
4. Durable subscriptions (#38468).
5. Snapshot elision for a projection and filter without a temporal predicate.
   #38468 needs it, and it would let sinks accept a projection and filter later
   without a snapshot read on every resume.
6. Non-poisoning subscribe errors (database-issues#5182).
7. Docs that cross-link the durable-subscriptions pattern from every client page
   now, and lead with the SDK once it ships.
8. A stable WebSocket `SUBSCRIBE`, the gate for browser and function transports.

The server-side buffering bound (#37905) has landed and needs no further work.

## Minimal Viable Prototype

The prototype will be the protocol core extracted from the Rust prototype, the
Rust package on top of it, and the turbopuffer sink, written in Rust, running
against the internal context graph. Python and Node packages are stretch goals
for the same period. It tests three of the riskiest claims:

- that a protocol core with no I/O carries a full SDK and a real sink with only a
  thin transport around it,
- that tombstones and conditional writes give each turbopuffer namespace
  exactly-once state under random kills and a stale worker,
- that the nightly end-to-end suite catches server changes that break the SDK.

A Python or Node package, if it lands in the same period, also tests that the
protocol core binds into another language without packaging or performance
problems.

## Delivery plan

October: the prototype above, with the end-to-end suite in the nightlies, and
the Python and Node packages as stretch goals. Exit criteria are a passing
convergence test, a passing fencing test, and every typed error reproduced in
the end-to-end suite.

November and December: the Python and Node packages if they did not land in
October, durable subscriptions as they land, and a Redis or Postgres reference
sink chosen by demand.

GA requires dedicated SQLSTATEs released, the client docs rewritten on the SDK,
and every published language passing the same vectors and end-to-end suite.

## Future work

The name leaves room for modules beyond `subscribe` and `sink`. The candidates
are places where an ordinary driver behaves unexpectedly against Materialize:

- After its first query, a read transaction is confined to one time domain, and
  reading objects outside it fails with SQLSTATE 25000
  (`RelationOutsideTimeDomain` in `src/adapter/src/error.rs`). Drivers and ORMs
  that open a transaction for every query hit this. A module could default reads
  to autocommit and offer one call that reads several views at one timestamp.
- A transaction becomes write-only after its first write, and some DDL refuses to
  run inside a transaction. Typed errors could name the fix for each.
- A cursor cannot outlive its transaction (`WITH HOLD` is rejected in
  `src/sql-parser/src/parser.rs`), and only `FETCH ... WITH (timeout = ...)` lets a
  cursor loop idle. `subscribe` already handles both.
- Drivers return types they do not know, such as `mz_timestamp`, as text. The
  protocol core's value model could serve plain queries too.

Each module adds maintenance: it tracks server behavior, needs vectors and
end-to-end scenarios, and multiplies across languages. A module will be added
only when support or field evidence shows users hitting the problem, and only
with nightly end-to-end coverage. Plain query execution, DDL management (owned
by mz-deploy and the Terraform provider), and ORM integration stay out of scope.

## Alternatives

### The product strawman

The product requirements include a strawman API with a `Cursor` and a
`SinkConnector`. Its boundary between tracking progress and delivering data is
right, and this design keeps it. The parts that change:

| Strawman | This design | Why |
| --- | --- | --- |
| `update()` commits and fetches | `commit()` inside the transaction, `next()` outside | A target transaction must not wait on Materialize |
| `queries` as SQL strings, split at `ENVELOPE` | One table, source, or materialized view plus an envelope, as `CREATE SINK` takes | Resume never reads the snapshot, durable subscription compatibility, no string splicing |
| One cursor per query | One `AS OF` for all members, release at the minimum frontier | Independent cursors produce torn cuts |
| Rows stamped with the batch frontier | Every change keeps its `mz_timestamp` | Batches span timestamps |
| Retractions come first | Order by timestamp, net per key within a timestamp | Not true by default |
| `name` defaults to the class name, rows keyed by list index | Required names, rows tagged by view name | Shared checkpoints fence each other, reordering remaps rows |
| `commit_interval` in progress messages | Minimum time between commits | Progress cadence is not a user-facing unit |
| Snapshot handling left to the user | Partial snapshot chunks, generation sweep, history-loss policy | Large snapshots and orphan keys are the common failure |
| Conditional turbopuffer writes for replay safety | Tombstones plus conditional writes, with the checkpoint in its own namespace | A replayed or stale upsert recreates a key a later batch deleted |

### Build only on durable subscriptions

The semantics would be simplest, but nothing ships until the server does, and
the target-side checkpoint is needed either way. The narrow subscription scope
lets the SDK ship now and move over without an API break.

### Native server-side sinks per target

This is the best eventual experience for a few high-volume targets. Each target
becomes a server feature with its own release train, and the long tail of
targets never ends. The SDK serves the long tail and shows which targets deserve
native sinks.

### Kafka and the connector ecosystem

This is right for organizations already running Kafka Connect. It makes everyone
else run Kafka to reach a cache, adds a hop, and loses the mapping between target
state and a Materialize timestamp unless the connector is consistency-aware.

### A native message queue

`mzq` (PRD-87) overlaps with this design. If it lands, it is another transport
under the same protocol core and sink API, not a replacement for them. This
needs settling with the product owner.

### Docs only

The pattern doc already exists. The table in "The Problem" shows what docs alone
produce.

## Open questions

Each question is either resolved by the review or deferred with an owner and a
point by which it has to be answered. None of the deferred ones blocks the
October work, because the doc picks a safe default for each.

### Resolved during review

- First language: the Rust package comes first, with Python and Node as stretch
  goals, and the October turbopuffer sink is written in Rust against
  turbopuffer's HTTP API.
- Generation strategy: one protocol core with bindings (option C). If native
  packaging proves too costly when the second language lands, the fallback is B
  plus A.
- Catalog access for the startup checks: every relation the SDK reads is
  readable by `PUBLIC`: `mz_internal.mz_frontiers` (the readable frontier),
  `mz_catalog.mz_indexes`, `mz_internal.mz_storage_shards`,
  `mz_internal.mz_object_global_ids`,
  `mz_internal.mz_object_transitive_dependencies`, and
  `mz_internal.mz_kafka_source_tables`. A sink role needs no extra grants for
  them.
- Sink scope: sinks read one table, source, or materialized view with
  `SUBSCRIBE <object>`, the same objects `CREATE SINK` accepts.

### Deferred

1. Name. The doc proposes Materialize SDK (see "Naming"). Owner: product.
   Answer before the first package is published, since renaming after that is
   expensive.
2. Subscribing by id. Resolving `[u123 AS "db"."schema"."name"]` by catalog id,
   with the name used only as an alias, is confirmed in review and covered for
   user queries by `test/sqllogictest/id.slt`. Owner: database team. Confirm it
   is a supported contract for `SUBSCRIBE` before the Rust package ships, since
   it is what makes the identity check race-free.
3. Object swaps. Until answered, a name swap stops the sink by default, and
   `refollow` re-snapshots into new namespaces, which is safe but costs a full
   snapshot per deploy. Owner: deploy tooling with the database team. Answer
   before `refollow` ships: can deploy tooling confirm that the old and new
   objects are equal at the handoff, so the sink can skip the snapshot, and
   carry `RETAIN HISTORY` onto staged views and replacement materialized views?
   For a replacement, does a running subscribe end at the switch, and does the
   new catalog id's readable frontier cover a checkpoint taken before it?
4. turbopuffer tombstones. Tombstones are kept forever by default and carry a
   placeholder vector in namespaces with vectors. Owner: DevEx, with the first
   turbopuffer users, during the sink build. Is the storage cost and the
   placeholder vector acceptable, and what grace period should the docs
   recommend for sinks that opt into the sweep?
5. turbopuffer visibility. The sink documents convergence after each batch.
   Owner: DevEx, with the first search users. Do readers need a consistent view
   while a batch is written? That would need versioned documents and a
   visible-cut pointer that readers filter on.
6. Repository home. The doc proposes keeping the packages under `misc/` in this
   repository. Owner: DevEx with the engineering leads, once the spec settles:
   do the packages move to their own repository, keeping the protocol core,
   vectors, and end-to-end suite here?
7. `mzq` (PRD-87). Owner: product. Is it a transport under this SDK, or a
   separate path?
8. Egress cost. Owner: product with the cloud team, before the GA docs. What
   does a sink with a large snapshot cost a customer, and do the docs need a
   sizing page?
9. Stateless workers. Out of scope until durable subscriptions land. Owner:
   product. Is there demand that cannot wait?
10. Future-work modules. The rule for adding one is in "Future work". Owner:
    DevEx, per module, when evidence appears.
