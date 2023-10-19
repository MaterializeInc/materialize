# Platform V2: Txn Management

## Context

Our SQL Table Transactions Management still shows some of its lineage from the
single-binary. At times, this conflicts with the "Platform V2" goals of
_use-case isolation_ and _zero-downtime upgrades (0dt)_.

## Goals

Use-case isolation. Make it possible to prevent pgwire sessions from interfering
with each other through:

- Large reads
- Large writes
- Frequent reads
- Frequent writes
- Long-running transactions

Zero-downtime upgrades. Nothing in particular motivating here. Don't introduce
anything that interferes with 0dt.

Don't break things that work today:
- Explicit SQL transactions that are entirely reads or entirely writes (plus
  "constant" reads)
- Multi-table read transactions
- Single-table write transactions
- Reads include from `MATERIALIZED VIEW`s
- Transactions that are implicitly reads followed by writes:
  - `INSERT INTO ... SELECT FROM ...`
  - `UPDATE`
  - `DELETE`
- Don't regress performance "too much". Moving to a distributed system is
  necessary to provide isolation and distributed transactions are necessarily
  less performant.

Nice to haves, while we're revisiting SQL tables and transactions:
- A path to multi-table write transactions. It need not end at the global
  optimum, just be possible.
- Bounded memory usage for large transactions
- Highly optimized `COPY INTO`/`COPY FROM`

## Non-Goals

- Preventing pgwire session disconnection during upgrades and other restarts.
  Node failures and loadbalancer termination are rare but impossible to
  eliminate, so users will always need a connection retry loop.
- Performance under highly-contended writes. Materialize will never be as good
  at this as a dedicated OLTP database.
- Transactions that arbitrarily interleave reads and writes. This could
  potentially be done for special cases, but the general case of arbitrary
  SELECTs requires a way to include uncommitted data in compute dataflow inputs.
  This could potentially be modeled as a compound timestamp, but that's an
  immense change.
- Align table and source upper advancements. This is a lovely thing to do, but
  seems independent of this work.

## Overview

Each table is (and will continue to be) backed by an individual persist _shard_
with a unique _ShardId_ address.

Currently:
- The data in each of these shards is written already _reclocked_ (roughly, with
  a timestamp suitable for use directly in compute dataflows). One of the
  seriously considered Alternatives involve changing this but the proposed
  design does not.
- The _upper_ of all of these shards are advanced in lockstep: whenever a table
  write happens OR if it has been `X` time since the last round of upper
  advancements. This allow joins of tables to be immediately served after a
  write and joins of tables and sources to be served within `X`.
- Timestamps for reads and system writes are selected by an in-process timestamp
  oracle.
- Timestamps for user writes are selected by an in-process timestamp oracle
  protected by a mutex.
  - Notably, a read + write transaction holds this lock during its entire
    execution.

We break the problem of transaction management into two conceptual pieces,
_timestamp selection_ and _atomic durability_, by introducing the following
model of read and write operations:

- `subscribe(table_id: ShardId, as_of: T, until: Option<T>)` with the usual
  meaning of `as_of` and `until`.
- `commit_at(ts: T, updates: Vec<ShardId, Vec<(Row, i64)>>) -> bool` which
  makes the requested timestamp (and all lesser ones) immediately available for
  reads or returns false if that's no longer possible (because of contention).
- Note that the actual interface in code will look slightly different (more
  complicated) than this for performance reasons, but it's conceptually the same
  thing.

Timestamp selection is then concerned with snapshot vs serializable vs
strict-serializable isolation as well as minimizing contention; various
strategies can be discussed in terms of `subscribe` and `commit_at`. Atomic
durability is then an implementation of `subscribe` and `commit_at`.

Example: read-only single-table txn

```rust
let read_ts = oracle.read_ts();
subscribe(table_id, read_ts, Some(read_ts+1))
```

Example: write-only single-table txn

```rust
loop {
    if commit_at(oracle.write_ts(), updates) {
        break;
    }
}
```

Example: read-then-write single-table txn

```rust
let (read_ts, write_ts) = oracle.read_and_write_ts();
let mut s = subscribe(table_id, read_ts, None);
loop {
    if commit_at(write_ts, update_fn(s, read_ts)) {
        break;
    }
    (read_ts, write_ts) = oracle.read_and_write_ts();
}
```

- If `update_fn` can be computed incrementally, there are various tricks we can
  play to avoid re-writing all the data to s3 on each retry.
- On retry, there's a potential performance special case to check if any of the
  inputs to `update_fn` have changed since the last read_ts and skip recomputing
  update_fn if not.
  - Note that with this optimization, this does exactly the same thing as
    "write-only single-table txn".

The rest of this document concerns the implementation of the durability
component. Additional detail on the timestamp selection is left for a separate
design document.

## Detailed description

See accompanying prototype in [#20954].

[#20954]: https://github.com/MaterializeInc/materialize/pull/20954

Efficient atomic multi-shard writes are accomplished through a new
singleton-per-environment _txns shard_ that coordinates writes to a (potentially
large) number of _data shards_. Data shards may be added to and removed from the
set at any time.

Benefits of these txns:
- _Key idea_: A transactional write costs in proportion to the total size of
  data written, and the number of data shards involved (plus one for the txns
  shard).
- _Key idea_: As time progresses, the upper of every data shard is logically
  (but not physically) advanced en masse with a single write to the txns shard.
  (Data writes may also be bundled into this, if desired.)
- Transactions are write-only, but read-then-write transactions can be built on
  top by using read and write timestamps selected to have no possible writes in
  between (e.g. `write_ts/commit_ts = read_ts + 1`).
- Transactions of any size are supported in bounded memory. This is done though
  the usual persist mechanism of spilling to s3. These spilled batched are
  efficiently re-timestamped when a commit must be retried at a higher
  timestamp.
- The data shards may be read independently of each other.
- The persist "maintenance" work assigned on behalf of the committed txn is
  (usually, see below) assigned to the txn committer.
- It is possible to implement any of snapshot, serializable, or
  strict-serializable isolation on top of this interface via write and read
  timestamp selections.
- It is possible to serialize and communicate an uncommitted `Txn` between
  processes and also to merge uncommitted `Txn`s, if necessary (e.g.
  consolidating all monitoring collections, statement logging, etc into the
  periodic timestamp advancement).

Restrictions:
- Data shards must all use the same codecs for `K, V, T, D`. However, each data
  shard may have a independent `K` and `V` schemas. The txns shard inherits the
  `T` codec from the data shards (and uses its own `K, V, D` ones).
- All txn writes are linearized through the txns shard, so there is some limit
  to horizontal and geographical scale out.
- Performance has been tuned for _throughput_ and _un-contended latency_.
  Latency on contended workloads will likely be quite bad. At a high level, if N
  txns are run concurrently, 1 will commit and N-1 will have to (usually
  cheaply) retry. (However, note that it is also possible to combine and commit
  multiple txns at the same timestamp, as mentioned above, which gives us some
  amount of knobs for doing something different here.)

### Intuition and Jargon

- The _txns shard_ is the source of truth for what has (and has not) committed
  to a set of _data shards_.
- Each data shard must be _registered_ at some `register_ts` before being used
  in transactions. Registration is for bookkeeping only, there is no particular
  meaning to the timestamp other than it being a lower bound on when txns using
  this data shard can commit. Registration only needs to be run once-ever per
  data shard, but it is idempotent, so can also be run at-least-once.
- A txn is broken into three phases:
  - (Elided: A pre-txn phase where MZ might perform reads for read-then-write
    txns or might buffer writes.)
  - _commit_: The txn is committed by writing lightweight pointers to
    (potentially large) batches of data as updates in txns_shard with a
    timestamp of `commit_ts`. Feel free to think of this as a WAL. This makes
    the txn durable (thus "definite") and also advances the _logical upper_ of
    every data shard registered at a timestamp before commit_ts, including those
    not involved in the txn. However, at this point, it is not yet possible to
    read at the commit ts.
  - _apply_: We could serve reads of data shards from the information in the
    txns shard, but instead we choose to serve them from the physical data shard
    itself so that we may reuse existing persist infrastructure (e.g.
    multi-worker persist-source). This means we must take the batch pointers
    written to the txns shard and, in commit_ts order, "denormalize" them into
    each data shard with `compare_and_append`. We call this process applying the
    txn. Feel free to think of this as applying the WAL.

    (Note that this means each data shard's _physical upper_ reflects the last
    committed txn touching that shard, and so the _logical upper_ may be greater
    than this.)
  - _tidy_: After a committed txn has been applied, the updates for that txn are
    retracted from the txns shard. (To handle races, both application and
    retraction are written to be idempotent.) This prevents the txns shard from
    growing unboundedly and also means that, at any given time, the txns shard
    contains the set of txns that need to be applied (as well as the set of
    registered data shards).
- A data shard may be _forgotten_ at some `forget_ts` to reclaim it from the
  txns system. This allows us to delete it (e.g. when a table is dropped). Like
  registration, forget is idempotent.

### Usage

```rust
// Open a txns shard, initializing it if necessary.
let mut txns = TxnsHandle::open(0u64, client, ShardId::new()).await;

// Register data shards to the txn set.
let (d0, d1) = (ShardId::new(), ShardId::new());
txns.register(d0, 1u64).await.expect("not previously initialized");
txns.register(d1, 2u64).await.expect("not previously initialized");

// Commit a txn. This is durable if/when the `commit_at` succeeds, but reads
// at the commit ts will _block_ until after the txn is applied. Users are
// free to pass up the commit ack (e.g. to pgwire) to get a bit of latency
// back. NB: It is expected that the txn committer will run the apply step,
// but in the event of a crash, neither correctness nor liveness depend on
// it.
let mut txn = txns.begin();
txn.write(&d0, vec![0], 1);
txn.write(&d1, vec![1], -1);
txn.commit_at(&mut txns, 3).await.expect("ts 3 available")
    // And make it available to reads by applying it.
    .apply(&mut txns).await

// Commit a contended txn at a higher timestamp. Note that the upper of `d1`
// is also advanced by this.
let mut txn = txns.begin();
txn.write(&d0, vec![2], 1);
txn.commit_at(&mut txns, 3).await.expect_err("ts 3 not available");
txn.commit_at(&mut txns, 4).await.expect("ts 4 available")
    .apply(&mut txns).await;

// Read data shard(s) at some `read_ts`.
let updates = d1_read.snapshot_and_fetch(
    vec![txns.read_cache().to_data_inclusive(&d1, 4).unwrap()].into()
).await.unwrap();
```

### Writes

The structure of the txns shard is `(ShardId, Vec<u8>)` updates.

The core mechanism is that a txn commits a set of transmittable persist _batch
handles_ as `(ShardId, <opaque blob>)` pairs at a single timestamp. This
contractually both commits the txn and advances the logical upper of _every_
data shard (not just the ones involved in the txn).

Example:

```text
// A txn to only d0 at ts=1
(d0, <opaque blob A>, 1, 1)
// A txn to d0 (two blobs) and d1 (one blob) at ts=4
(d0, <opaque blob B>, 4, 1)
(d0, <opaque blob C>, 4, 1)
(d1, <opaque blob D>, 4, 1)
```

However, the new commit is not yet readable until the txn apply has run, which
is expected to be promptly done by the committer, except in the event of a
crash. This, in ts order, moves the batch handles into the data shards with a
`compare_and_append_batch` (similar to how the multi-worker persist_sink works).

Once apply is run, we "tidy" the txns shard by retracting the update adding the
batch. As a result, the contents of the txns shard at any given timestamp is
exactly the set of outstanding apply work (plus registrations, see below).

Example (building on the above):

```text
// Tidy for the first txn at ts=3
(d0, <opaque blob A>, 3, -1)
// Tidy for the second txn (the timestamps can be different for each
// retraction in a txn, but don't need to be)
(d0, <opaque blob B>, 5, -1)
(d0, <opaque blob C>, 6, -1)
(d1, <opaque blob D>, 6, -1)
```

To make it easy to reason about exactly which data shards are registered in the
txn set at any given moment, the data shard is added to the set with a
`(ShardId, <empty>)` pair. The data may not be read before the timestamp of the
update (which starts at the time it was initialized, but it may later be
forwarded).

Example (building on both of the above):

```text
// d0 and d1 were both initialized before they were used above
(d0, <empty>, 0, 1)
(d1, <empty>, 2, 1)
```

### Reads

Reads of data shards are almost as straightforward as writes. A data shard may
be read normally, using snapshots, subscriptions, shard_source, etc, through the
most recent non-empty write. However, the upper of the txns shard (and thus the
logical upper of the data shard) may be arbitrarily far ahead of the physical
upper of the data shard. As a result, we do the following:

- To take a snapshot of a data shard, the `as_of` is passed through unchanged if
  the timestamp of that shard's latest non-empty write is past it. Otherwise, we
  know the times between them have no writes and can fill them with empty
  updates. Concretely, to read a snapshot as of `T`:
  - We read the txns shard contents up through and including `T`, blocking until
    the upper passes `T` if necessary.
  - We then find, for the requested data shard, the latest non-empty write at a
    timestamp `T' <= T`.
  - We wait for `T'` to be applied by watching the data shard upper.
  - We `compare_and_append` empty updates for `(T', T]`, which is known by the
    txn system to not have writes for this shard (otherwise we'd have picked a
    different `T'`).
  - We read the snapshot at `T` as normal.
- To iterate a listen on a data shard, when writes haven't been read yet they
  are passed through unchanged, otherwise if the txns shard indicates that there
  are ranges of empty time progress is returned, otherwise progress to the txns
  shard will indicate when new information is available.

Note that all of the above can be determined solely by information in the txns
shard. In particular, non-empty writes are indicated by updates with positive
diffs.

Also note that the above is structured such that it is possible to write a
timely operator with the data shard as an input, passing on all payloads
unchanged and simply manipulating capabilities in response to data and txns
shard progress.

Initially, an alternative was considered where we'd "translate" the as_of and
read the snapshot at `T'`, but this had an important defect: we'd forever have
to consider this when reasoning about later persist changes, such as a future
consolidated shard_source operator. It's quite counter-intuitive for reads to
involve writes, but I think this is fine. In particular, because writing empty
updates to a persist shard is a metadata-only operation (we'll need to document
a new guarantee that a CaA of empty updates never results in compaction work,
but this seems like a reasonable guarantee). It might result in things like GC
maintenance or a CRDB write, but this is also true for registering a reader. On
the balance, I think this is a _much_ better set of tradeoffs than the original
plan.

### Compaction

Compaction of data shards is initially delegated to the txns user (the storage
controller). Because txn writes intentionally never read data shards and in no
way depend on the sinces, the since of a data shard is free to be arbitrarily
far ahead of or behind the txns upper. Data shard reads, when run through the
above process, then follow the usual rules (can read at times beyond the since
but not beyond the upper).

Compaction of the txns shard relies on the following invariant that is carefully
maintained: every write less than the since of the txns shard has been applied.
Mechanically, this is accomplished by a critical since capability held
internally by the txns system. Any txn writer is free to advance it to a time
once it has proven that all writes before that time have been applied.

It is advantageous to compact the txns shard aggressively so that applied writes
are promptly consolidated out, minimizing the size. For a snapshot read at
`as_of`, we need to be able to distinguish when the latest write `<= as_of` has
been applied. The above invariant enables this as follows:

- If `as_of <= txns_shard.since()`, then the invariant guarantees that all
  writes `<= as_of` have been applied, so we're free to read as described in the
  section above.
- Otherwise, we haven't compacted `as_of` in the txns shard yet, and still have
  perfect information about which writes happened when. We can look at the data shard upper to determine which have been applied.

### Forget

A data shard is removed from the txns set using a `forget` operation that writes
a retraction of the registration update at some `forget_ts`. After this, the
shard may be used through normal means, such as direct `compare_and_append`
writes or tombstone-ing it. To prevent accidental misuse, the forget operation
ensures that all writes to the data shard have been applied before writing the
retraction.

The code will support repeatedly registering and forgetting the same data shard,
but this is not expected to be used in normal operation.

## Alternatives

### Alternative 1: Put all tables in a single persist shard

- Model each update in the shard as something like `(GlobalId, Row)`
- Reads would then pull out the table(s) they are interested in, likely
  optimized in some way by pushdown.
- Can be implemented entirely on top of persist.
- Atomic multi-table writes are trivial, as is advancing the upper of all tables
  at once.
- Unintuitive. Nothing else works this way.
- There are likely to be "gotchas" from putting small+large, low+high
  throughput tables all in one shard. E.g. when individual retractions from some
  table consolidate out would be dependent on all the other tables.

### Alternative 2: Put all tables in a single persist "state"

- Currently, persist has a 1:1 relationship between shards and the metadata
  (batches, capabilities, etc) kept by the state machine. Instead, make the
  metadata a map of `ShardId -> ShardMetadata`.
- Atomic multi-table writes are trivial, as is advancing the upper of all tables
  at once.
- Greatly adds complexity to the internals of persist, which is already complex.
- Greatly complicates the API of persist.
- Membership changes (the mapping between a shard and which state it lives in)
  are tricky.
- Noteworthy risk of destabilizing persist, which has to work perfectly for
  basically anything in Materialize to work.
- Reading or writing a single table would require loading the metadata of all
  co-located shards, which scales arbitrarily large with the number of tables.
  This likely implies an upper bound on the number of tables we can support.
- Also likely includes a rewrite of how persist monitoring and debugging tools
  work.

### Alternative 3: Consensus::compare_and_set_multi

- Currently, persist updates each shard's state independently through a
  `Consensus::compare_and_set` operation implemented in producing using CRDB.
- We could build multi-shard writes by leaning on CRDB's existing implementation
  of scalable distributed transactions and implementing
  `Consensus::compare_and_set_multi` which commits all or none of a set of shard
  updates.
- Fairly complicates the API of persist.
- Compared to compare_and_set, likely to be slower in the happy case (we'd lose
  an important CRDB performance optimization) and with higher tail latencies
  (due to increased contention footprint).
- Greatly limits future flexibility in swapping in a different implementation of
  Consensus: e.g Kafka/Redpanda.

### Alternative 4: Table remap shard

- A single _table remap_ shard is introduced per environment, which maps _data
  shard_ uppers to realtime.
- Intuition: A data shard's upper is no longer the source of truth for when you
  can read it. Instead, the remap shard is. This allows us to store uncommitted
  data in the data shards.
- The data shard is only written when new data arrives, _not_ as time passes.
- This corresponds closely to the [reclocking doc].
- This was extensively prototyped during the design process:
  - It is very similar to the proposed design, but recording uppers instead of
    batch pointers.
  - It is definitely workable. If the design presented in this doc hasn't
    surfaced, this is what we would have done.
  - However, it became clear through the prototyping process that it's much more
    finicky to keep an implementation of this design correct (also more
    difficult to keep performant) than the one presented here.

[reclocking doc]:
    https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210714_reclocking.md

### Alternative 5: Timestamp magic

- Differential dataflow's computation model is powerful enough to express
  interesting computer science algorithms.
- This could potentially include building something like [Calvin] as a
  differential dataflow.
- It's unclear how to fit the timestamp structure necessary to make this work
  into the rest of Materialize.

[Calvin]: https://www.cs.umd.edu/~abadi/papers/calvin-sigmod12.pdf
