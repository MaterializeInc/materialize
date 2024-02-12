# Fast-path reads from Persist-backed collections

Related: this design refers heavily to the [consolidate on read](https://github.com/bkirwi/materialize/blob/consolidation-on-read/doc/developer/design/20230317_consolidation_on_read.md) design doc; you may want to skim that first.

## Context

When is a query like `SELECT * FROM relation LIMIT n` fast?

In the general case, a query spins up an anonymous dataflow that writes its results into an arrangement. Once the arrangement has caught up to a specific frontier, we read the data back out of the arrangement, apply the limit, and return it the user.  If `relation` is an arbitrary view, this could be arbitrarily expensive.

However, if `relation` is indexed, this query behaves very differently. There’s no need to spin up a dataflow or build a new arrangement: we can just read the data from the pre-existing arrangement that’s maintained by the index. This type of “fast path” query can only apply very simple transformations on the indexed data: maps, filters, projections, limits, and a couple others. Anything more complex falls back to the more general dataflow-based query machinery.

Users often seem to expect this same sort of behaviour when `relation` is a durable collection, like a source or a materialized view. This is understandable: in an ordinary database, this query might just need to scan through the first few entries in a file before returning a result. However, these sorts of queries currently fall back to the general-case logic, spinning up a full dataflow that reads in the entire collection just to filter it down to a few rows. This design proposes adding a new fast path, similar to the index-based fast-path, that can make these sorts of queries more performant.

## Goals

- Fast responses to `SELECT * FROM large_collection LIMIT small_n` queries.
  -  In particular, we want the cost of this query to be proportional to `small_n` and not the size of `large_collection`. (The constant factors may be much higher than the equivalent query against an index, though.)

## Non-Goals

- Improving the performance of `LIMIT` clause on a more general range of queries.

## Overview

Supporting a fast query path for select-limit queries against Persist shards will require changes to both our query planning and execution.

- Generalize the existing fast-path query planning logic. As of today, fast-path plans are only generated when the queried collection has at least one index. We’ll extend this logic to also generate these plans for unindexed collections that are backed by a Persist shard.
- When we execute a peek against a Persist-backed collection — as opposed to an arrangement, which is always the case today — it will create a new `PersistPeek` operation that streams the data from a Persist shard. While it’s possible to efficiently and incrementally stream consolidated data from a Persist shard, no such API exists today; we’ll need to add one.

## Detailed description

### Planning

Today, `environmentd` will generate a fast-path plan when a dataflow can be represented as: a `Get` from an indexed collection, a map-filter-project stage, and some “finishing” logic including order-by/limit. We’ll change this to also allow the `Get` to get from a non-indexed collection backed by a Persist shard.

Today, every select query generates a peek request into an arrangement. (Even non-fast-path queries work by creating a new dataflow that writes out its data into an arrangement, then peeking into that arrangement.) This design introduces a second type of peek: one that peeks directly into the backing Persist shard. Concretely, this means that `environmentd` will send `ComputeCommand::Peek` commands that reference a persisted collection’s `Id`, not the id of an index or arrangement.

### Peeking

When `clusterd` receives a peek command, it stores and tracks a `PendingPeek` struct with the arrangement and request metadata in the compute state, polls until that arrangement has data for the requested timestamp, then reads that data synchronously back out of the arrangement before returning a `ComputeResponse::Peek` with the results.

Since Persist’s API is asynchronous, we can fork off a new task for every incoming Peek, and hold on to the task handle. We can periodically poll the task for completion, and cancel it if the peek itself is cancelled by the user.

When executing an ordinary peek, each worker reads a subset of the results out of their local arrangement… so much of the work is spread equally across all workers/processes of a replica. However, with Persist-backed peeks, we’d really only be doing the reading in a single task running on just one of the replica's processes. We’ll need to hash these reads across the compute nodes to avoid unduly skewing the workload. This will also shift work from the timely worker threads to the background task pool; we should cap the number of peeks that run concurrently to avoid monopolizing these limited resources.

### Persist

For background on the relevant bits of Persist, see [the background section of the consolidation-on-read design doc](https://github.com/bkirwi/materialize/blob/consolidation-on-read/doc/developer/design/20230317_consolidation_on_read.md#background). That section mentions:

> A single worker can efficiently and fully consolidate a set of runs using a streaming merge: iterat[ing] through each of the runs concurrently, pulling from whichever run has the smallest next key-value, and consolidating updates with equal key-value-time as you go.
>

We can’t actually get away with this in the distributed context, since the Persist source is spread across many workers. However, for a little peek, running in a single worker or task should be fine… and this approach is more or less what we intend to implement.

Which is not to say this code is trivial: it’s still very correctness- and performance-sensitive, and requires managing a lot of concurrent work. However, the main consolidate on read implementation deals with many similar issues… and should be possible to hide this complexity behind a simpler API within the Persist codebase, to avoid exposing this complexity to Compute.

## Open questions

### Limits?

Existing fast-path queries are distributed: every worker contributes to the results. This is necessary because the arrangement backing the index is itself distributed. Persist peeks, on the other hand, could be run from a single node. (And, if querying from `environmentd` or some future serving layer, will necessarily be run on a single node.) The downside of this is that an expensive peek could cause disproportionate resource use on a single node.

If we want to ensure that the cost of the query is proportional to the result set size and not the size of the collection, we'll need to introduce some additional restrictions:
- No filters. Index-based fast-path queries are often used for single-key lookups, like `select * from indexed_collection where id = 'XXX'`. Persist cannot currently support efficient queries with this shape.
- No `ORDER BY`. This would require us to load and sort the full collection, then apply the limit.

We may be able to relax these requirements in the future. (For example, if we order the Persist data by the data's "logical" ordering and not its serialized representation, we could support primary-key lookups more efficiently.)

### `clusterd` or `environmentd`?

We have the option of running a Persist peek either in `clusterd` or `environmentd`.

Advantages to `environmentd`:
- Fewer moving parts: we'd avoid touching the cluster command protocol and processing at all, for example.
- Similarly, it might be easier to observe the process of a peek if it happens entirely in the `environmentd` process.

Advantages to `clusterd`:
- Persist fast-path peeks could likely reuse much of the infrastructure for existing peeks.
- Generally speaking, it is worse to knock over `enviromentd` than `clusterd`. If we have resource-intensive work to do, it's ideal to be able to fan it out.
- `clusterd` is handling these reads already for the non-fast-path version, so it's less of a change to the global distribution of the workload.

It's possible that we'll spin out a dedicated serving layer at some point in the future. It seems very likely that we'd want to process peeks entirely in that serving layer if possible.

### Consolidate on read?

The consolidation on read design is a more general-purpose way to improve `LIMIT` performance. However:

- It’s a bit tricky, and our Persist source is already very complex. It’s likely to be more work up front, plus more work to maintain in an ongoing way.
- The minimum latency is likely to be worse than the fast-path approach: we’d still be spinning up a new dataflow and reading and writing a new arrangement. The fast path just does less work overall.

These two projects are not mutually exclusive… we may *also* want to tackle consolidation in the Persist source to improve the performance of monotonic oneshot selects, or limit queries on more complex dataflows.

We could also decide to create a new, non-distributed `persist_source` type that uses the simple merging logic described here. This new source variant could be used for small, low-cost queries where we want to optimize for latency at the expense of scalability. (Perhaps our new cost estimations will help?) This helps mitigate the first bullet above, but not the second.

### Limit pushdown

Today, Compute has the ability to pass an MFP to the Persist source. The hope is that the source will be able to implement the mapping, filtering, and projection more efficiently than if we added a separate operator to do this on the source's output. (As seen in the recent [filter pushdown project](./20230306_persist_mfp_pushdown.md).)

One can imagine a similar "limit pushdown" feature -- Compute could notify the source that it only needed N rows from the source, and the source could shut down early once N rows had been emitted.

This requires the source to consolidate data: it would be bad if we shut down after emitting N rows, but those rows had been retracted in a part we hadn't gotten to yet. From a Persist perspective, limit pushdown involves similar work and similar tradeoffs to the original consolidate-on-read design above. However, it may be a more convenient API for Compute to program against.

### Do nothing?

Always an option!

Historically, we’ve been hesitant to introduce new features with “performance cliffs”, where a small change to a query can cause a dramatic change in performance, and this design certainly includes such a cliff. However, selects against indexed collections have the same cliff; we’ve been able to explain that feature to users in such a way that they are not frequently surprised by the performance, and many of the major use cases for indices rely on it. The same reasoning may apply to a persist-backed fast path.

Similarly, we could also train users to solve their problems using other more-efficient tools than select-limit queries. (It makes sense that the same problems may have a different “best” solution in different databases!) However, select-limit queries are often the first thing that a new user will try on their brand new source or materialized view; these users may not understand Materialize well enough yet to know about our more differentiated tools, but a slow response to a “simple” query may leave them with a bad first impression. If an efficient select-limit is possible without torquing Materialize’s data model too much, we can save our learning curve budget for more fundamental parts of the Materialize experience.

**Peek from `environmentd`?** This design suggests doing the Persist read from `clusterd`, but nothing forces us to read the shard from there; the Persist logic could run just as easily in `environmentd`. This design keeps the read in `clusterd` partly to mirror the existing query logic more closely, and partly to help minimize the load on (and risk to) `environmentd`. In a future where we run many redundant `environmentd` nodes, we may want to revisit this decision.

### Deterministic serialization?

If we want to rely on Persist for consolidation, our data codecs need to be deterministic. (Specifically: two `[u8]` must decode to the equal values if and only if they are identical.) Otherwise, consolidating before decoding might not give the same results as decoding before consolidating. (And all our ideas for making Persist-source consolidation faster rely on consolidating first.)

Currently, we use Protobuf to serialize our rows, and Protobuf does not guarantee deterministic serialization. (However, our Protobuf library seems to give us deterministic results in practice, given an unchanged schema.) We'll need to add other checks that our serialization is stable to be comfortable relying on this in production.
