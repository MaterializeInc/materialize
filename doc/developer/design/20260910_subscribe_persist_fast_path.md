# SUBSCRIBE persist fast path

## Context

Every `SUBSCRIBE` today ships a dataflow to a cluster: it imports the
subscribed collection, renders a subscribe sink, and the compute controller
forwards the sink's batches to the coordinator, which formats them for the
session. For `SUBSCRIBE <collection>` on a table, source, or materialized view
the dataflow computes nothing. The collection's persist shard already holds the
snapshot and receives every later update, and the dataflow only re-reads that
shard on the cluster.

The per-subscribe dataflow is what makes fleets of subscribers expensive. A
browser or a distributed service with thousands of connections that all
reconnect after a network blip creates thousands of dataflows at once, each
with its own persist reader on the cluster, its own snapshot read, and its own
controller and coordinator traffic. The cost is paid per client, so it cannot
be amortized by adding clients to one cluster, and it lands on the cluster
that also maintains the collection. A middleman service that keeps one
subscribe open and serves its clients from a local copy solves this outside
Materialize. This design does the same inside `environmentd`, behind the
`enable_subscribe_persist_fast_path` flag.

## Design

### Decision

A subscribe takes the fast path when the flag is on and the plan is
`SUBSCRIBE <id>` where `<id>` is a storage collection: a table, a source, a
materialized view, or a continual task. Views, queries, and log sources still
get a dataflow, as does `EXPLAIN`, which describes the dataflow the subscribe
would run as. Both sequencing paths, the session task and the coordinator's
staged sequencer, decide with `persist_tail_source`, so they agree.

The decision is made before timestamp selection, so the timestamp and the
read holds are chosen for the collection itself rather than for an index the
dataflow might have read. The optimizer is skipped for the fast path.

### Storage seam

`StorageCollections::subscribe(id, as_of, with_snapshot)` returns a stream of
the collection's snapshot at `as_of` followed by every later update and
strictly advancing progress frontiers, ending after an empty frontier. It is
the streaming sibling of `snapshot_and_stream` and hides persist and txn-wal
behind the same boundary. For txn-wal backed tables the shard's physical upper
only moves when a write is applied, so the stream combines a persist listen
with the txns shard's remap entries the same way `txns_progress` does in
`persist_source`, driven from a task instead of a dataflow.

Concurrent subscribers of one collection share a single persist listen. A
`SharedTail` per collection opens one listen reader and one snapshot reader,
decodes each listen batch once, and fans the decoded batches out to every
subscriber. It retains a bounded window of past batches so a subscriber whose
`as_of` trails the shared listen still gets a complete stream: its own
snapshot at `as_of` through the shared snapshot reader, the retained batches
past `as_of`, then the live batches. A subscriber whose `as_of` is older than
the retained window gets a private reader. Sharing matters because registering
a persist reader is a compare-and-set on the shard's state, so a storm of
private readers serializes on it, and because it decodes each part once
instead of once per subscriber.

A subscriber's snapshot is read through persist's consolidating cursor
(`snapshot_cursor`), which merges the shard's runs in bounded memory and
yields the snapshot in pieces, already consolidated and in key order. The
subscriber's stream hands those pieces out as it is polled, so the snapshot is
never held whole. Persist consolidates the encoded updates, so two encodings
of one row reach different pieces and do not cancel; the subscriber
consolidates each piece after decoding it, which leaves only a duplicate that
spans pieces, the case `snapshot_and_fetch` covers by re-consolidating the
whole snapshot and counts in `unconsolidated_snapshot`.

The snapshot reader is driven by one task that drains requests from a queue.
A lock around the reader hands it from waiter to waiter through the scheduler,
and with hundreds of waiters each hop costs more than leasing a snapshot does.
Waiting for the shard's upper to pass a subscriber's `as_of`, which a
subscriber whose `as_of` came from the timestamp oracle usually has to,
happens before the request is queued, against the tail's own frontier.

Events are pushed into a per-subscriber queue as the shard advances and pulled
as the subscriber's stream is polled, so the tail never waits on a slow
subscriber and no subscriber slows another.

### Adapter

The client-facing formatting of subscribe batches moves out of
`ActiveSubscribe` into a `SubscribeFormatter`, so it can run wherever the rows
are needed. For the fast path that is the session: `PersistTailStream` is the
row stream the session polls during `FETCH`, and it pulls from the storage
stream, batches, and formats only when polled. The coordinator keeps an
`ActiveSubscribe` for cancellation, dependency drops, and the
`mz_subscriptions` row, and reaches the client through a control channel the
stream checks ahead of data. An `ActiveSubscribe` records whether a dataflow
or a persist tail produces its batches, so retiring the sink skips the compute
controller.

`PersistTailBatcher` cuts the storage stream into `SubscribeBatch`es with the
contents and boundaries the compute subscribe sink produces, including the
`SNAPSHOT`, `AS OF`, and `UP TO` semantics, error poisoning, and the
`max_result_size` check.

The snapshot is the exception: its pieces ship as they arrive, each as a batch
whose bounds are both the `as_of`, so the formatter emits the rows and holds
the timestamp's progress message until the frontier advances. The client sees
the same rows in the same order and the same progress messages, but the first
of them without waiting for the rest, and neither the batcher nor the
formatter ever holds the whole snapshot. Shipping a timestamp in pieces is
only sound for an output whose rows are independent of one another within a
timestamp, so it is limited to diff output: `WITHIN TIMESTAMP ORDER BY` sorts
a timestamp and the envelopes group it by key, and both still buffer it
whole. The client therefore sees exactly what a
dataflow-backed subscribe produces, which the testdrive file
`subscribe-persist-fast-path.td` checks by running the same statements under
both settings. The stream tells the coordinator to retire the sink when it has
produced its last message.

### Slow clients

Fetching drives the reads, so a client that stops fetching costs only its
queue of decoded events in the shared tail, which is shared memory counted
against the existing `subscribe_max_buffered_bytes` budget, with the same
tolerance for a single oversized event that the coordinator applies today.
When the queue exceeds the budget the tail drops it. The client is not cut
off: on its next fetch the stream resumes below the frontier it had delivered,
without a snapshot, from the tail's retained window if that still covers the
frontier and otherwise from the shard itself. The collection's `RETAIN
HISTORY` is therefore what bounds how far behind a client may fall, sized and
paid for per collection by whoever owns it. Only when the collection has
compacted past the frontier does the client get an error, which points at
`RETAIN HISTORY`. A collection with the default one-second window degrades to
today's behavior; nothing new holds back compaction.

The first read of the collection is opened before the stream exists, off the
coordinator loop, so a client that never fetches does not hold its read holds.
Its snapshot cursor is leased there too, which pins the parts it has yet to
read until it is drained or dropped.

Shipping the snapshot in pieces gives up the frontier's account of what a
client has seen: the frontier does not advance until the snapshot's timestamp
completes, so a client detached partway through one cannot be resumed, and
gets the same error a client that falls behind gets today. This is the one
case the tail cannot resume. It is also where the dataflow path errors today,
since a client that slow accumulates the whole formatted snapshot in the
coordinator.

### Hosting the tail on a replica

Persist peeks were once served from `environmentd` and moved to `clusterd`
because their memory is shaped by the shard, not by anything `environmentd`
controls. The same holds for a subscribe's snapshot: what has to be resident
is one part per overlapping run of the shard, times the number of concurrent
cursors, and in `environmentd` that is a bound it cannot enforce. So the same
tail can run on a replica instead, behind
`enable_subscribe_persist_fast_path_on_cluster`, and the two placements are
measured against each other below.

The replica-hosted variant follows the persist peek. `ComputeCommand::Subscribe`
carries what `PeekTarget::Persist` carries plus the subscribe's `as_of`,
`up_to`, snapshot flag, row order, and buffer budget, and one worker per
replica runs it as a tokio task rather than a dataflow. The worker is chosen
by hashing the target collection, so every subscribe of one collection on a
replica lands on the same worker and shares its `SharedTail`. The task cuts the
tail's events with the same `PersistTailBatcher` and emits
`SubscribeResponse` batches through the worker's response channel, so from the
controller onward the subscribe is a subscribe sink: the controller tracks it
as a write-only collection, forwards its batches, and ends it by allowing
compaction to the empty frontier, which the task answers with `DroppedAt`.

Two protocol details are load-bearing. A subscribe's responses are merged
twice, over the workers of a process by the cluster client and over processes
by the controller, and each merge waits for every stream it expects before
its frontier moves. So every worker speaks for a subscribe: the chosen one
with its batches, and each other worker with the empty frontier at once, which
lets both merges be driven by the chosen worker alone. And an error ends the
subscribe with a batch at the empty frontier, because both merges forward a
batch only when it moves the frontier.

That second detail is also why the snapshot ships whole on this path for now.
A piece of the snapshot moves no frontier, and the frontier is what
deduplicates batches across the replicas of an untargeted subscribe and
across a replica's reconnect, so pieces would be delivered from every replica
and again after a restart mid-snapshot. Both merges pass a piece straight
through when asked to, so the plumbing is in place, but the controller has to
pin pieces to one replica and fail the subscribe if that replica goes away
mid-snapshot before it can be turned on.

`environmentd` needs less on this path than on its own: `ActiveSubscribe`,
coordinator formatting, cancellation, dependency drops, and
`mz_subscriptions` all work unchanged, and only the choice of command differs
from shipping a dataflow. What it gives up is fetch-driven backpressure. The
replica pushes batches as the shard produces them, the coordinator formats
them into the client's backlog, and a client that stops reading is retired at
`subscribe_max_buffered_bytes` with the fell-behind error, exactly as on the
dataflow path today. The replica's queue budget and resume are never reached.
Persist readers live on the replica, which survives an `environmentd` crash,
so the stale-reader window of the `environmentd` variant does not arise.

On reconciliation a replica aborts its subscribe tasks without a word and the
controller reissues the retained `Subscribe` commands, so a subscribe restarts
from its `as_of` and the controller drops what the client already has by
frontier, as it does for a recreated subscribe sink.

## What stays the same

Cancellation, dependency drops, `mz_subscriptions`, statement logging, and
progress messages work as before. The subscribe still requires a cluster with
a replica and reports that cluster in `mz_subscriptions`, although a persist
tail uses no cluster resources.

## Measurements

Local `bin/environmentd --optimized` on a laptop, one `quickstart` replica, a
materialized view over a table of 10,000 rows of 64 bytes. `N` clients, spread
over eight load-generator processes, connect at once and each runs
`COPY (SUBSCRIBE mv WITH (PROGRESS)) TO STDOUT`. "Statement to snapshot" is
the time from a client sending its statement until it holds its complete
snapshot, "storm" the time until every client does. "Fan-out" is the time from
an `INSERT` into the table until a client has the row. Each cell is the median
of two runs. The dataflow path is the flag off, which is the unchanged code
path.

| Clients | Path | Statement to snapshot p50 / p95 | Storm | Fan-out p50 | CPU s in storm, `environmentd` + `clusterd` |
|---|---|---|---|---|---|
| 16 | dataflow | 0.36 s / 0.38 s | 0.42 s | 62 ms | 0.2 + 0.1 |
| 16 | persist | 0.16 s / 0.17 s | 0.20 s | 60 ms | 0.3 + 0.1 |
| 64 | dataflow | 0.44 s / 0.67 s | 0.73 s | 89 ms | 0.6 + 0.4 |
| 64 | persist | 0.23 s / 0.25 s | 0.28 s | 100 ms | 1.2 + 0.1 |
| 256 | dataflow | 2.21 s / 3.10 s | 3.23 s | 110 ms | 2.5 + 1.8 |
| 256 | persist | 0.56 s / 0.63 s | 0.69 s | 38 ms | 4.5 + 0.1 |
| 512 | dataflow | 5.98 s / 8.71 s | 9.06 s | 116 ms | 5.2 + 5.0 |
| 512 | persist | 1.03 s / 1.08 s | 1.14 s | 57 ms | 9.3 + 0.2 |
| 1024 | dataflow | 12.8 s / 18.9 s | 19.5 s | 159 ms | 10.5 + 14.0 |
| 1024 | persist | 1.50 s / 2.03 s | 3.84 s | 60 ms | 18.7 + 0.6 |
| 2048 | dataflow | 30.3 s / 45.9 s | 47.9 s | 302 ms | 23.6 + 45.1 |
| 2048 | persist | 3.49 s / 4.19 s | 5.81 s | 93 ms | 39.1 + 1.6 |

Below 16 clients the two paths are indistinguishable. From there the dataflow
path's storm time grows faster than the client count, roughly 2.4x per
doubling, while the persist path grows about linearly until `environmentd`
runs out of cores at 2048, where it averaged six. The storm is a maximum over
clients and catches a straggler: at 1024 the persist path's p95 was 2.03 s
against a 3.84 s storm. The persist path uses less
CPU in total but all of it in `environmentd`, since formatting for the client
moves there from the cluster. Nothing is left on the cluster: no dataflows,
and its CPU stays at the maintenance baseline.

Inside `environmentd`, the persist path spends about 7 ms per subscriber on
its 10,000-row snapshot once the shared tail exists. Sharing the listen
matters: with a private persist reader per subscriber, 500 subscribers spent
most of their time in the reader registration compare-and-set on the shard's
state, and the storm took as long as the dataflow path.

A client that stopped fetching while 400 rows were written against a 16 KiB
queue budget resumed with every row exactly once, both from the tail's window
after a 2 s stall and from the shard after a 40 s stall on a table with
`RETAIN HISTORY FOR '10 minutes'`. The same 40 s stall on a table without
retained history produced the compacted-history error.

A second setup measures what one client waits for on a large snapshot: a
materialized view of 1,000,000 rows of 64 bytes, subscribed by a single
client on a freshly started `environmentd`, with one subscribe run first on
each path so that neither pays for the other's cold blob cache. Every row
arrived exactly once on both paths.

| One client, 1,000,000 rows | Dataflow | Persist |
|---|---|---|
| `FETCH 10` from a new cursor | 0.97 s | 0.22 s |
| First row over `COPY` | 1.00 s | 0.10 s |
| Whole snapshot delivered | 2.50 s | 1.59 s |
| `environmentd` resident growth | 160 MB | 130 MB |
| `clusterd` resident growth | 181 MB | 22 MB |

Resident growth is a weak instrument here: the allocator reuses what the
warm-up claimed, and the same measurement on a cold process reads 356 MB
against 160 MB in `environmentd`. Read the figures as a bound on the
difference rather than a measurement of it. What the latencies show is that
the client no longer waits for the whole snapshot to be read, consolidated
and formatted before its first rows.

### The tail on a replica, compared

The same sweep with the tail hosted on the replica, at the sizes where the
placements diverge. Memory is what one run at that size added to a process's
resident set, as a range over the runs, in processes that do not return
memory promptly.

| 2048 clients, 10,000 rows | Dataflow | Tail in `environmentd` | Tail on replica |
|---|---|---|---|
| Storm | 47.9 s | 5.8 s | 7.6 s |
| Statement to snapshot p95 | 45.9 s | 4.2 s | 6.5 s |
| `environmentd` CPU s | 23.6 | 39.1 | 21.2 |
| `clusterd` CPU s | 45.1 | 1.6 | 22.4 |
| `environmentd` memory added per run | 100 to 270 MB | about 1500 MB | 20 to 340 MB |
| `clusterd` memory added per run | 100 to 240 MB | 0 to 130 MB | about 1400 MB |

| Clients | Tail on replica, p50 / p95 | Storm | CPU s, `environmentd` + `clusterd` |
|---|---|---|---|
| 256 | 0.52 s / 0.83 s | 0.93 s | 2.5 + 2.5 |
| 512 | 0.95 s / 1.56 s | 1.75 s | 4.9 + 5.1 |
| 1024 | 1.86 s / 3.16 s | 3.51 s | 10.1 + 10.7 |
| 2048 | 4.01 s / 6.54 s | 7.62 s | 21.2 + 22.4 |

The replica-hosted tail keeps most of the storm win, about a third slower
than the `environmentd` tail for the protocol hop and the coordinator's
formatting, and costs the same total CPU. What moves is the per-subscriber
memory: the cursors' 1.4 GB at 2048 clients lands on the replica instead of
in `environmentd`, whose growth falls to what the connections cost. It did
not shrink, since nothing yet shares the snapshot between subscribers.

One client against the 1,000,000-row view, each placement on a freshly
started `environmentd` with a warm-up pass, all against the same shard:

| One client, 1,000,000 rows | Dataflow | Tail in `environmentd` | Tail on replica |
|---|---|---|---|
| `FETCH 10` from a new cursor | 0.49 s | 0.29 s | 1.32 s |
| First row over `COPY` | 0.46 s | 0.28 s | 1.35 s |
| Whole snapshot delivered | 1.28 s | 1.48 s | 2.13 s |
| `environmentd` resident growth | 157 MB | 122 MB | 155 MB |
| `clusterd` resident growth | 197 MB | 21 MB | 220 MB |

On this path the snapshot ships whole, see above, so the first row waits for
it, and the batch is materialized once in `environmentd` on its way through
the merge and the coordinator's formatting. These sub-second figures vary by
up to 2x between runs, so the columns should be read against one another.

Chunked delivery was measured on this path too, with the merges passing pieces
through. It brought the first row to about the `environmentd` tail's and kept
the snapshot out of `environmentd` entirely, at 4 MB of growth for a million
rows, but it is unsound across replicas and reconnects as described above, so
it is off until the controller pins pieces to a replica.

## Follow-ups

* Serve `SUBSCRIBE (SELECT ... FROM <collection> WHERE ...)` when the query is
  a non-temporal map, filter, and project over one storage collection, the way
  `PeekPersist` does for peeks.
* Let a persist-tail subscribe run without a cluster replica.
* Share snapshot decoding between subscribers with the same `as_of`.
* Resume a client detached partway through a chunked snapshot, by recording
  how far into the snapshot it read and skipping that far on the new cursor.
* Expire the previous process's subscribe readers on startup. A subscribe's
  persist readers live in `environmentd` rather than on a cluster, so a crash
  leaves them registered until persist force-expires them, which is
  `persist_reader_lease_duration` (15 minutes) after the last heartbeat. Until
  then they pin the shard's since. There are two per subscribed collection,
  not one per subscribe, and nothing is incorrect in the meantime, since a
  held-back since only retains more history.
* Bound the snapshot cursor's memory per subscriber. It inherits persist's
  compaction memory bound, which is a ceiling for one merge, not a budget
  across the subscribers of a collection.
* Report the fast path in `EXPLAIN` and in a metric, and count fallbacks to a
  private reader.
