# Persist Inline Writes

- Associated: [#24832](https://github.com/MaterializeInc/materialize/pull/24832)

## The Problem

A non-empty persist write currently always requires a write to S3 and a write to
CockroachDB. S3 writes are much slower than CRDB writes and incur a per-PUT
charge, so in the case of very small writes, this is wasteful of both latency
and cost.

Persist latencies directly impact the user experience of Materialize in a number
of ways. The above waste is particularly egregious in DDL, which may serially
perform a number of small persist writes.

## Success Criteria

Eligible persist writes have latency of `O(crdb write)` and not `O(s3 write)`.

## Out of Scope

- Latency guarantees: In particular, to keep persist metadata small, inline
  writes are an optimization, they are not guaranteed.
- Read latencies.

## Solution Proposal

Persist internally has the concept of a _part_, which corresponds 1:1:1 with a
persist _blob_ and an _object_ in S3. Currently, all data in a persist shard is
stored in S3 and then a _hollow_ reference to it is stored in CRDB. This
reference also includes metadata, such as pushdown statistics and the encoded
size.

We make this reference instead a two variant enum: `Hollow` and `Inline`. The
`Inline` variant stores the same data as would be written to s3, but in an
encoded protobuf. This protobuf is only decoded in data fetch paths, and is
otherwise passed around as opaque bytes to save allocations and cpu cycles.
Pushdown statistics and the encoded size are both unnecessary for inline parts.

The persist state stored in CRDB is a control plane concept, so there is both a
performance and a stability risk from mixing the data plane into it. We reduce
the inline parts over time by making compaction flush them out to S3, never
inlining them. However, nothing prevents new writes from arriving faster than
compaction can pull them out. We protect the control plane with the following
two limits to create a hard upper bound on how much data can be inline:

- `persist_inline_writes_single_max_bytes`: An (exclusive) maximum size of a
  write that persist will inline in metadata.
- `persist_inline_writes_total_max_bytes`: An (inclusive) maximum total size of
  inline writes in metadata. Any attempted writes beyond this threshold will
  instead fall through to the s3 path.

## Alternatives

- In addition to S3, also store _blobs_ in CRDB (or a third technology).

  CRDB [self-documents as not a good fit][crdb-large-blob] for workloads
  involving large binary blobs: "it's recommended to keep values under 1 MB to
  ensure adequate performance". That said, given that inline writes are small
  and directly translate to a CRDB write anyway, this would likely be a totally
  workable alternative.

  That said, CRDB is already a dominant cost of persist and a large part of that
  is a function of the rate of SQL values changed, regardless of the size or
  batching of those writes.

  Additionally, putting the writes inline in persist state allows pubsub to push
  them around for us, meaning that a reader that is subscribed/listening to the
  shard doesn't hit the network when it gets the new seqno.

  A third technology would not be worth the additional operational burden.

- Make S3 faster. This is not actionable in the short term.

[crdb-large-blob]: https://www.cockroachlabs.com/docs/stable/bytes#size

## Open Questions

- How do we tune the two thresholds?
- Should every inline write result in a compaction request for the new batch
  immediately flushing it out to s3?
