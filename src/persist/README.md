# Persist

The primary abstraction of the persist library is a set of named collections,
made and kept definite through durable storage. Each of these collections is
closely analogous to (and internally modeled after) differential dataflow's
[arrangement], or more generally its [Collection]. In the language of the
[correctness doc], these are each are described by a `[since, upper)`.

[correctness doc]: https://github.com/MaterializeInc/materialize/blob/v0.13.0/doc/developer/design/20210831_correctness.md#description
[Collection]: differential_dataflow::collection::Collection
[arrangement]: differential_dataflow::operators::arrange::arrangement

NB: This is written to reflect the (moving) target of a finished product.
Several bits of what is written below are not yet reflected in the code.

NB2: If you're looking at this on GitHub, far more of this document is linked if
you view it as [rustdoc for the persist crate].

[rustdoc for the persist crate]: https://dev.materialize.com/api/rust/persist/index.html


# Jargon

  - _record_: A differential dataflow `((key, value), timestamp, diff)` tuple.
    Persist requires that the data field has a (key, value) structure, though
    the value can be the unit type to mean "empty": `()`. TODO: The code
    currently also calls this "update" in some places and "entry" in others.
  - _persisted collection_: A durable and definite differential dataflow
    [Collection]. More specifically, a persisted [arrangement] structured to
    allow indexed access to the collection's records. Within the src/persist
    code, this is often simply called a "collection." TODO: Most places in the
    code still call this a "stream". We also may want to reserve the "persisted
    collection" jargon for the equivalent concept at the level of the platform
    architecture's [storage layer] (design doc in progress), and so call this
    something else.
  - _definite_: Quoting the [correctness doc], "A collection is definite when
    all uses of any one identification will yield exactly the same time-varying
    collection of data (identical contents at each logical time). A handle to a
    collection can be "definite from since until upper" (names pending) if it
    reflects the definite results for times greater or equal to since but not
    greater or equal to upper; this intentionally allows for compaction of
    historical detail and unavailability of future data, without ambiguity about
    which logical times can be described with certainty."
  - _(external) collection name_: A human-readable, process-unique `&str`
    identifier for a persisted collection. Internally, this name gets
    transparently mapped 1:1 to a smaller collection id.
  - _(internal) collection id_: The internal equivalent of a collection name.
    TODO: For efficiency, this is currently exposed in the public API in one
    place, but it'd be nice to eliminate this.
  - _write_: Durably adding a set of records to a persisted collection.
  - _seal_: Durably advancing the frontier of a persisted collection. NB: A
    common initial misconception when encountering persist is that _write_ is
    like a filesystem write with minimal durability guarantees and _seal_ like
    is like fsync, but this incorrect. A write whose future has resolved as
    successful is durable, regardless of when or if it's sealed.
  - _storage_: An external source of data durability. All of persist's
    interactions with the outside world are abstracted through the [Blob] and
    [Log] traits, together collectively called storage. One valid way to think
    of persist is as a layer that maps simpler durable storage semantics to ones
    that are more native to differential dataflow.
  - _blob_: A durable `&str` key -> (possibly large) `&[u8]` value store. The
    production implementation is S3 (or other cloud storage). File and in-mem
    HashMap implementations exist for testing.
  - _log_: A durable log with `&[u8]` entries. Currently unused, but eventually
    this will be an optional latency optimization. No production implementations
    exist yet.
  - _storage location_: A logical root of data storage. Examples include an s3
    bucket + prefix or a filesystem folder.
  - _runtime_: An instantiation of persist. Accessed externally via a
    thread-safe and clone-able "client".

[OrdKeySpine]: differential_dataflow::trace::implementations::ord::OrdKeySpine
[Blob]: crate::storage::Blob
[Log]: crate::storage::Log
[storage layer]: #integration-with-platform


# Async API

Persist has 2 public APIs: one based on rust's async [Future] and another one
made of a set of differential dataflow operators built on top of it.

The Async API client lives in [`mz_persist::client`]. Most methods have a
correspondence to part of the following dataflow:

[Future]: std::future::Future
[`mz_persist::client`]: crate::client

```rust,no_run
    use differential_dataflow::operators::arrange::ArrangeByKey;
    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::Input;
    use timely::*;

    timely::execute(Config::thread(), |worker| {
        worker.dataflow(|scope| {
            let (input, stream) = scope.new_input::<((String, String), u64, isize)>();
            let arranged = stream.as_collection().arrange_by_key();
        });
    });
```

- `input.send_batch` is roughly equivalent to persist's
  [StreamWriteHandle::write]. The write call returns a [std::future::Future].
  When this Future has resolved as successful, then this write is guaranteed to
  be durable, regardless of seals.
- `input.advance_to` is roughly equivalent to persist's
  [StreamWriteHandle::seal]. The seal call returns a [std::future::Future]. When
  this Future has resolved as successful, then this frontier advancement is
  guaranteed to be durable.
- `arranged.trace.set_logical_compaction` is roughly equivalent to persist's
  [StreamWriteHandle::allow_compaction]. The seal call returns a
  [std::future::Future]. When this Future has resolved as successful, then this
  is guaranteed to be durable.
- `arranged.stream` is (very) roughly equivalent to persist's
  [StreamReadHandle::listen].

[StreamWriteHandle::write]: crate::client::StreamWriteHandle::write
[StreamWriteHandle::seal]: crate::client::StreamWriteHandle::seal
[StreamWriteHandle::allow_compaction]: crate::client::StreamWriteHandle::allow_compaction
[StreamReadHandle::listen]: crate::client::StreamReadHandle::listen

FAQ: Note that `write` is independent of `seal`, meaning that the Future
returned by write can resolve before the timestamps of the data written have
been sealed.

TODO: There is currently a bit of a mismatch between the "data-parallel" nature
of timely/differential and persist. E.g. persist's `StreamWriteHandle::seal`
directly advances the persisted collection, but the `advance_to` call above
works locally to each worker and the collection as a whole can only be said to
be advanced to some point once every worker has advanced to (or past) that
point. How we'll address this is still an open question. In the meantime, the
user of persist is responsible for adapting the semantics.


# Operator API

Built on top of the async API is a set of dataflow operators that all live in
[`mz_persist::operators`], collectively called the operator API. The most notable
of these is [PersistedSource]. The rest are more in flux and specific to the
needs of source persistence and are individually documented in the rustdoc.

[PersistedSource]: crate::operators::source::PersistedSource
[`mz_persist::operators`]: crate::operators

The `PersistedSource` operator is a zero-input operator that reflects the
contents of a persisted collection in its single output. As records are written
to the collection, persist forwards them to the operator output. As the
collection is progressively sealed, persist forwards that as a capability
downgrade on the operator output.

```rust,no_run
    use timely::dataflow::operators::Inspect;
    use timely::progress::frontier::Antichain;
    use timely::*;

    use mz_persist::client::RuntimeClient;
    use mz_persist::operators::source::PersistedSource;

    let p: RuntimeClient = unimplemented!("start runtime");
    let (_, read) = p.create_or_load::<String, String>("collection name");
    timely::execute(Config::thread(), move |worker| {
        let read = read.clone();
        let as_of: Antichain<u64> = unimplemented!("compute as_of");
        worker.dataflow(|scope| {
            scope
                .persisted_source(read, &as_of)
                .inspect(|x| println!("{:?}", x));
        });
    });
```


# Codec

Persist allows for arbitrary key and value types in persisted collections
(timestamp and diff are hardcoded to `u64` and `isize`, but this can change
if/when necessary). Internally, it translates these to `&[u8]` via a user
supplied implementation of the [Codec] trait.

[Codec]: mz_persist_types::Codec

`Codec` implementors are responsible for backward compatibility. If an encoding
changes, the decode side needs to be able to detect and handle the old format.


# Persist Guarantees: Definite

Persisted collections are [definite] and maintain a declared `[since, upper)`.
Given a `(storage location, persist name)`, all reads at "times greater or equal
to `since` but not greater or equal to `upper`" will "yield exactly the same
time-varying collection of data (identical contents at each logical time)".

This is true regardless of the definite-ness of the input to persist. As a
result, persist can be used to make indefinite streams of data into definite
ones (this is one of its primary development goals).

[definite]: https://github.com/MaterializeInc/materialize/blob/v0.13.0/doc/developer/design/20210831_correctness.md#description


# Persist Guarantees: Ordering

Currently, within an instance of persist, calls to `write`, `seal`, and
`allow_compaction` are linearized. For example: if a call to `write` has
returned a Future and then a `seal` command is started, the write is guaranteed
to be applied before the seal, even if the write's Future hasn't resolved when
the seal call started. (In practice, we internally batch up mutations to the
underlying storage. This means that a write immediately followed by a seal will
often see their futures resolved at the same time.)

Note: It's possible that in the future, we'll relax this guarantee to something
more directly tied to dataflow semantics, where e.g. a write can be reordered
forward or backward as long as it doesn't cross a seal that makes a valid write
invalid, or vice-versa.


# Persist Guarantees: Atomic

Each call to a method of the async API is atomic. [MultiWriteHandle] allows
atomic operations involving multiple persisted collections.

[MultiWriteHandle]: crate::client::MultiWriteHandle


# Transient Errors

With any sort of durable storage, the possibility for transient errors exists.
The design for how we'll handle this is still TODO, but it will likely lean
toward pushing retries into persist with user-level introspection. Any accepted
solution here will ensure that the output of the PersistedSource operator is
definite.


# Data Integrity

All data and metadata in storage is checksummed when written and verified when
read. However, this only detects corruption. The ultimate responsibility for
preventing it is left to the storage implementation (as opposed to something
like error correcting codes). S3 provides [99.999999999% durability] by storing
multiple copies.

[99.999999999% durability]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/DataDurability.html

TODO: To guard against the possibility of bugs in persist (and similarly source
decoding), it may be desirable for us to store raw incoming source data as early
as possible in the pipeline.


# Optimistic Concurrency Control (aka Pipelining)

There are several places Materialize could be along the OCC spectrum. On one
end, all data and frontier information are made durable before being passed
along to dataflow and starting computation. In this case, the durability
frontier stays always in advance of the frontier of determined results. This is
where we've started and it means that users will only see results derived from
persisted sources after those inputs are durable without any changes to e.g.
coord. It's also the easiest to get right.

On the other end of the OCC spectrum, persist would immediately hand off all
data and frontier information to dataflow before it's durable. To serve a
computed result at a given time and maintain the above semantics, coord would
have to also ensure that the persistance frontiers of the inputs had advanced
past that point. This allows durability and compute to proceed in parallel. It
also allows coord flexibility to choose its own policy, even on a read-by-read
basis: bold and knowledgeable users could be allowed to retrieve results before
their inputs are durable. (I'm not suggesting we do this.)

There are also various available points in between these two extremes.


# Starting Persist

Persist may be started in either of two modes: read-write and read-only. These
are sometimes called "full" and "read" in the code. For a given storage
location, persist only allows one writer at a time, but supports any number of
concurrent readers (including while there is a writer).

The [persist write lock] is "reentrant" to allow Kubernetes deployments to crash
processes without cleanly releasing the lock. The code generalizes this to a
"reentrance ID", but in single-node Materialize, the catalog ID is used (this
will have to be changed for platform). If reentrance is misused to allow
multiple persist instances to think they each have exclusive writer access, then
the behavior of the system is undefined (in practice, almost certain data loss).
Be careful. When in any doubt, it's safer to use the lock without reentrance,
which simply requires operator intervention when the process cannot cleanly shut
down.

The expected deployment pattern for a read-only instance of persist is in a
separate process from the writer of that storage location. This allows
decoupling of data durability and definiteness from its use. In a platform
world, `ingestd` will hold persist writers for sources, `coordd`(?) will hold
them for tables, and `dataflowd` will exclusively use read-only persist.

A read-only persist instance can connect via RPC to the write half to receive
low-latency notification of changes. It can also be configured to operate
completely independently of the write half, such that it cannot effect it in any
way whatsoever. The latter is intended for e.g. blue/green or prod/staging
deployments.

To avoid certain scaling bottlenecks, persist is structured entirely as a "fat
client". The (lean) persist runtime is run in a thread inside the same process
as that runtime's user. This allows the persist user to talk directly to storage
(e.g. S3), avoiding a middle-man persist service. Additionally, a small,
configurable local cache is used to avoid many storage reads.

The runtime stops when every copy of the [RuntimeClient] has been dropped (or
when any client's [RuntimeClient::stop] method has been called), attempting to
first gracefully finish any pending work, but rejecting new work. However, this
is not required for correctness; persist will not lose or corrupt data as a
result of process crash, including e.g. sudden power loss.

[persist write lock]: crate::storage::LockInfo
[RuntimeClient]: crate::client::RuntimeClient
[RuntimeClient::stop]: crate::client::RuntimeClient::stop

# Temporary data wipe mechanism

While in development, persist keeps an [ENCODING_VERSION] mechanism to
transparently wipe all data in storage the first time a new Materialize release
is started. This is to allow rapid iteration before we offer backward
compatibility, and we aim to use this in at most one release per month. All of
our user tests are structured so that wiping persist storage may result in
computation being repeated but not in data loss. This mechanism will be removed
before we offer backward compatibility (which we will).

[ENCODING_VERSION]: crate::gen::persist::ProtoMeta::ENCODING_VERSION

# Integration with Tables

Internally in coord, an INSERT against a persisted table is held up until the
persist write has been made durable (the future has resolved). If the persist
write fails, then the INSERT also fails. This happens at transaction commit time
(NB: In Materialize, a bare INSERT outside a txn internally gets an "implicit
transaction" wrapped around it.) A SQL transaction involving multiple persisted
tables is written to persist atomically, i.e. all or nothing.

Note: Another option here is to hold up the INSERT until the timestamp of the
write has been durable sealed by persist. This would make persisted tables
operate a bit more like persisted sources.

# Integration with Sources
- TODO copy in [aljoscha's doc]

[aljoscha's doc]: [https://github.com/MaterializeInc/materialize/pull/8414

# Integration with Platform

The persist crate is intentionally a composable abstraction with a minimal
surface area. There exists a need within Materialize (and particular as part of
the platform effort) for a higher level abstraction. The additional complexity
of this could be pushed down into persist, but there are already enough tricky
problems (fallible storage, backward compatibility, compaction heuristics) in
the current scope that it makes sense to silo it.

As a result, we've decided to build a "Storage" layer on top of the persist
crate that handles (among other things):

- Cloud-level concepts like accounts
- Mapping processes to persist storage locations and reentrance IDs
- Re-sharding

One (imperfect, we already have exceptions to this) rule of thumb for what
belongs where is that persist should be general enough that it could be used by
other differential dataflow users. Storage's feature set will be very much
exactly "what the Materialize Platform needs".
