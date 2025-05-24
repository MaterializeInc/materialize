# Large Select Result Size

## Context

Currently, results of SELECT queries are sent back from the cluster to
`environmentd` via the compute protocol and these results are fully
materialized (stored in memory) in `environmentd` before sending them out to
the client.

This has several implications:

- Sending large results "clogs up" the cluster/controller communication
- The amount of memory we want to give to `environmentd` limits the size of
  results we can return

In practice the above make it so we limit the size of results using a
`max_result_size` parameter, and some larger customers are chafing against
that.

Today, we always materialize the results of a query in `environmentd` memory
before sending them on to the client. This is only required for some types of
queries, though: only queries that require _post processing_ need
materialization while queries that don't need post processing could be streamed
through to the client. Queries that need an ordering established on the whole
result require post processing, which are queries that have at least one of
ORDER BY, LIMIT, or OFFSET. This is handled by `RowSetFinishing` in the code.

Below, we will use _streamable queries_ for queries that don't require post
processing, and _non-streamable queries_ for those that do require it.

We think it is easier to lift the result size limitation for streamable
queries, so want to approach that first, but there are ways we can lift the
limitation for non-streamable queries as well, for example by moving the post
processing to the cluster side or by implementing an approach where we can sort
and merge large results within bounded memory on the `environmentd` side.

## Technical Context

There are two ways of getting query results and sending them from the cluster
back to `environmentd`:

1. Extracting from an arrangement (the data structure backing indexes)
2. Extracting directly from persist

This leads to there being at least three ways a SELECT can be executed from the
`environmentd` side:

1. Fast-path SELECT: there is an existing arrangement (index) that we can
   extract our result from.
2. Slow-path SELECT: in order to get our result we first need to render a
   temporary dataflow with an index that will contain the results. Then we can
   use the same logic as a fast-path SELECT to extract the result. Afterwards
   the temporary dataflow is torn down again.
3. Fast-path persist SELECT: The query is simple enough that we can directly
   extract it from a persist shard. With potentially some amount of
   map-filter-project applied. This happens on the `clusterd` side and is
   shipped back using the same mechanism that is used for shipping back other
   SELECT results.

1 and 2 here imply that the result must fit into memory on the cluster side,
unless we want to, say, change how results can be extracted on the cluster
side.

## Goals

The user facing goal is:

- Lift result-size limitation for streamable queries

And we have technical goals as well, enabling potential future work and making
sure we have the right abstractions in place:

- Create re-usable building blocks for shipping data/results out-of-band, these
  can then be used in other places, such as for SUBSCRIBE
- Make persist Batches more of a "first-class" citizen, eventually allowing us
  to decouple where and how batches are written from where and how they are
  appended to a shard

## Non-Goals

- Lift limitation on result having to fit into cluster memory: for non-persist
  queries, the result is still first staged in an arrangement before we read it
  out and send it to `environmentd`. Or we read out of an existing arrangement
- Change how results are extracted from "the dataflows" on the cluster side
- Lift result-size limitation for non-streamable queries
- Change result-size limitations for SUBSCRIBE
- Change how we ship around and append data to shards, when handling INSERT
- Change how we ship around and append data for sources

## Implementation

Purposefully high level, but the rough idea is that we a) need to stop sending
large amounts of data through the compute protocol, and b) need to stop
materializing large results in `environmentd` memory.

We want to re-use the existing abstractions we have around the persist blob
store and batches. We use persist batches to "stash" data that we can then read
out again in other places, without intending to append these batches to a
persist shard. Though the latter is desirable in future work of decoupling
creating and appending batches.

For our user-facing goal specifically: on the cluster side, we "stash" query
results as persist batches, send a handle to that data back via the compute
protocol, and then (in `environmentd`) stream those results from the persist
blob/batch store back to the client.

We would use a (configurable) threshold for determining whether to send a
result inline in the compute protocol or out-of-band via the blob store.

It's a given that we would gate the feature behind a launch darkly flag and
roll out gradually.

## Future Work

A selection of the non-goals from above:

- Lift result-size limitation for non-streamable queries
- Change how results are extracted from "the dataflows" on the cluster side

These will require changing where and how we apply post processing. We would
want to apply it on the cluster side but that requires changing how we extract
results. That is, we shouldn't extract results from an index anymore but
instead install a dataflow fragment that does the extraction and applies the
post processing, for example by shipping all results to one worker before
shipping results back to `environmentd`, possible also via the persist blob
store.

## Alternatives

### Add a result-specific blob store (or similar)

Don't use persist blob store for these results. Arguably, the above idea is
slightly abusing the persist blob store for other purposes, but it's a thing
that we have and works. We could add another protocol that allows sending back
larger responses out-of-band. Or add a separate blob store that is tailor-made
for handling results.

Using persist blob store and the existing code around `Batch` has the benefit
that we're re-using a lot of existing infrastructure/code.

### Come up with a protocol for streaming results straight from `clusterd` out via `balancerd` to the client

We would need to decide where/how we eventually apply ORDER BY, LIMIT and
friends. And it would be a lot more work/uncertain how much work it is.

### Use temporary materialized views for large results

Moritz pointed out this idea: instead of implementing new code on the
compute/`clusterd` side for stashing results as persist batches we would do
more work on the adapter side. When we expect a large query result we don't use
the normal SELECT paths but instead render a temporary materialized view (which
writes results directly to persist) and then stream results from that out to
the client.

I'd open to being convinced, but the challenges I see with this are:

- When setting off the query we don't yet know whether the result is big. The
  main proposal would normally send results back via the compute protocol but
  fall back to out-of-band when we notice that the result is too big. We can't
  do that with the materialized view approach.
- The optimizer/rendering treat peeks differently from long-running dataflows
  (indexes and materialized views): we use the fact that the input is monotonic
  and that we don't have to maintain an arrangement for multiple times,
  continually. We would loose those optimizations.
- One of the technical goals is to create the building blocks for decoupling
  writing data and appending data. I think this will be useful for future
  user-facing projects, and we wouldn't build it now if we go with another
  approach.

## Open Questions

- Is this even a good idea? One could argue that materialize is not meant to
  serve big multi-gigabyte results from SELECT queries and that instead
  customers should use other means for extracting larger amounts of data.
