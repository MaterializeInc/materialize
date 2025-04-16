# Lift Max Result Size Limitation

## Context

Currently, results of SELECT queries are sent back from the cluster to
`environmentd` via the compute protocol and these results are fully
materialized (stored in memory) in `environmentd` before sending them out to
the client.

This has several implications:

- Sending large results "clogs up" the cluster/controller communication
- The amount of memory we want to give to `environmentd` limits the size of
  results we can return

In practice the above make it so we currently limit the size of results using a
`max_result_size` parameter, and some larger customers are chafing against
that.

Currently, we always materialize the results of a query in `environmentd`
memory before sending them on to the client. This is not required for all kinds
of queries, though: only queries that require _post processing_ need
materialization while queries that don't need post processing could be streamed
through to the client. Today, queries that need an ordering established on the
whole result require post processing, which are queries that have at least one
of ORDER BY, LIMIT, or OFFSET. This is handled by `RowSetFinishing` in the
code.

Below, we will use _streamable queries_ for queries that don't require post
processing, and _non-streamable queries_ for those that do require it.

We think it is easier to lift the result size limitation for streamable
queries, so want to approach that first, but there are ways we can lift the
limitation for non-streamable queries as well, for example by moving the post
processing to the cluster side.

## Technical Context

There are currently two ways of getting query results and sending them from the
cluster back to `environmentd`:

1. Extracting from an arrangement (the data structure backing indexes)
2. Extracting directly from persist

This leads to there being at least three ways a SELECT can be executed from the
`environmentd` side:

1. Fast-path SELECT: there is an existing arrangement (index) that we can
   extract our result from.
2. Slow-path SELECT: in order to get our result we first need to render a
   dataflow with an index that will contain the results. Then we can use the
   same logic as a fast-path SELECT to extract the result. Afterwards the
   temporary dataflow is torn down again.
3. Fast-path persist SELECT: The query is simple enough that we can directly
   extract it from a persist shard. With potentially some amount of
   map-filter-project applied.

1 and 2 here imply that the result must fit into memory on the cluster side,
unless we want to, say, change how results can be extracted on the cluster
side.

## Goals

- Lift result-size limitation for streamable queries.

## Non-Goals

- Lift limitation on result having to fit into cluster memory: For non-persist
  queries, the result is still first staged in an arrangement before we read it
  out and send it to `environmentd`. Or we read out of an existing arrangement.
- Change how results are extracted from "the dataflows" on the cluster side.
- Lift result-size limitation for non-streamable queries.

## Implementation

More details need to be filled in, but the rough idea is that we a) need to
stop sending large amounts of data through the compute protocol, and b) need to
stop materializing large results in `environmentd` memory.

On the cluster side, we "stash" query results in the blob store that is used by
persist, send a handle to that data back via the compute protocol, and then (in
`environmentd`) stream those results from persist back to the client.

We would use a (configurable) threshold for determining whether to send a
result inline in the compute protocol or out of band via the blob store.

It's a given that we would gate the feature behind a launch darkly flag and
roll out gradually.

## Sequencing, Subtasks & Estimation

tbd!

## Future Work

The non-goals from above:

- Lift result-size limitation for non-streamable queries.
- Change how results are extracted from "the dataflows" on the cluster side.

These will require changing where and how we apply post processing. We would
want to apply it on the cluster side but that requires changing how we extract
results. That is, we shouldn't extract results from an index anymore but
instead install a dataflow fragment that does the extraction and applies the
post processing, for example by shipping all results to one worker before
shipping results back to `environmentd`, possible also via the persist blob
store.

## Alternatives

- Don't use persist blob store for these results. Arguably, the above idea is
  slightly abusing the persist blob store for other purposes, but it's a thing
  that we have and works. We could add another protocol that allows sending
  back larger responses out of band. Or add a separate blob store that is
  tailor-made for handling results.

## Open Questions

- Is this even a good idea? One could argue that materialize is not meant to
  serve big multi-gigabyte results from SELECT queries and that instead
  customers should use other means for extracting larger amounts of data.
