# Materialize Dataplane Persistence

This is an in-progress implementation for a general persistence abstraction for
Materialize.

### Overview

A persistence user would start by constructing a `Persister` which is a registry
for multiple streams (each corresponding to a table, source, or operator) in a
single process. This allows us to funnel everything a process is persisting through
a single place for batching and rate limiting.

Given a unique `Id`, `Persister` will hand back a `Token`. This
can be handed in (consumed) to create a timely operator that persists and passes
through its input.

### V1 and V2

`Persister` corresponds to [`PersisterV1` in the prototype][persister v1] and
basically just supports synchronously writing data out, grabbing a snapshot of
everything written so far, and unblocking compaction of everything less than a
given timestamp. It could implemented on top of a SQLite table with something
like the following schema:

[persister v1]: https://github.com/danhhz/differential-dataflow/blob/02673114b05933341893ab603327237a9583e432/persist/src/persister.rs#L30-L39

```sql
CREATE TABLE persisted (persisted_id INT, key BYTES, val BYTES, ts BYTES, diff BYTES);
CREATE INDEX ON persisted (persisted_id, ts, key);
```

A second phase of the project will extend `Persister` to look more like
[`PersisterV2`][persister v2], which notably adds support for accessing the
persisted data as an arrangement. This would be useful in source persistence
because it could performantly answer "have we assigned this data a timestamp
before?" (for timestamp persistence) and "what was the previous value for this
key?" (for de-upserting). The arrangement is also a key piece of Lorenzo's
thesis on operator persistence. (Still missing is an analog of Lorenzo's
FutureLog, which could be added to `PersisterV2` but we haven't gotten there
yet.)

[persister v2]: https://github.com/danhhz/differential-dataflow/blob/02673114b05933341893ab603327237a9583e432/persist/src/persister.rs#L41-L44

In the prototype, `PersisterV2` has one implementation, which is itself in terms
of the `Buffer` and `Blob` traits. Internally, any data is immediately handed to
the Buffer, which could be durable for source persistence (a WAL on EBS or maybe
redPanda) or not durable for operator persistence. As timestamps are closed,
data from the buffer is moved into a persistent trace built on top of `Blob`
which is a KV abstraction over S3 and files. For operator persistence, we'll
have to add a FutureLog holding pen which immediately slurps everything from the
Buffer (or they're tee'd?) and indexes it in the necessary way.
