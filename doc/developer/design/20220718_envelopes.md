# `UPSERT` and `DEBEZIUM` envelopes in Platform

## Summary

Support the following envelope types for Avro data over Kafka:

* `UPSERT` (O(data) memory cost)
* `DEBEZIUM UPSERT` (O(data) memory cost)
* `DEBEZIUM` (O(1) memory cost in the steady state; O(keys) during initial read).

### Timelines (Executive Summary)

* I am pretty confident (but I wouldn't stake my reputation on it
before the implementation is actually written) that we can
support `UPSERT` and `DEBEZIUM UPSERT` by the Alpha launch.

* I am extremely confident that we can support both of those by GA.

* I am weakly hopeful that we can support the O(1) classic
  `DEBEZIUM` envelope by GA, subject to the detailed constraints expressed below.

* I am weakly pessimistic about being able to support the transaction
  topic by GA. I originally felt strongly about supporting this,
  but after further discussion with Nikhil and others,
  I now agree that it's not worth the effort unless we
  run out of other stuff to work on.


## Goals

**Hard Requirement**: We should be able to ingest flat-formatted data with upsert semantics (`ENVELOPE UPSERT`)
with no additional persisted state,
and with in-memory state proportional to the size of the output collection.

**Hard Requirement**: We should be able to ingest Debezium-formatted data (`ENVELOPE DEBEZIUM UPSERT`)
with the same space requirements as above, assuming the upstream tables
have primary keys.

**Nice to Have**: We should be able to ingest Debezium-formatted data (`ENVELOPE DEBEZIUM`)
with only O(1) persisted and in-memory space requirements in the steady state
(O(keys) when reading Debezium's initial snapshot),
subject to the following constraints:
  * The upstream source of the data is either Postgres or MySQL
  * The version of Debezium used is relatively recent
    (~summer 2021, TODO look up the exact version)
  * The upstream tables have primary keys.
  * The data does not have arbitrarily bad reordering and duplication,
    but only that which can result from expected use of Debezium
	(i.e., a contiguous stream of messages is repeated on restart).
  * The data is uncompacted at the time we begin reading it
    (it's okay if things get compacted away after we're done reading them
	once, because we make state definite in `persist`).
  * The data has both `before` and `after` fields, which requires
    the upstream database to be configured in a particular way (e.g.,
	`FULL` replication mode for Postgres).

The reason for these requirements is that Debezium is expected to emit duplicate messages
when Kafka or Kafka Connect crash, because it does not commit its "last written offset"
state transactionally with its data output. I have attempted to enumerate here the conditions
necessary for correct deduplication of the data, based on our long experience with
Debezium.

These requirements are _not_ necessary in the `ENVELOPE DEBEZIUM UPSERT` case, because
we don't actually need to deduplicate messages; the additional upsert state gives us
enough data to emit the proper retractions while just taking the most recently seen
value for any given key as the correct state.

**Nice to Have**: We should support ingestion of the Debezium transaction metadata topic
and use it for assigning timestamps to messages such that transaction boundaries are
respected. As there is currently no known way to deduplicate this topic, it's unclear
to what extent this is possible. This is discussed further below.

**Hard Requirement**: Our results are _correct_, given the following definition of correctness:
  * For `UPSERT` sources, the value for a key `k` at time `t` is the latest
    value for that key written to the topic at any time less than or equal to `t`.

    There is no guarantee of consistency across multiple sources, because
    there is no means to express such relationships in the data; it would require us creating
    a language for users to tell us themselves what timestamps their data has.
  * For `DEBEZIUM` and `DEBEZIUM UPSERT` sources with no transaction metadata
    topic (or if we never figure out how to read that topic correctly and performantly),
	the results are _eventually consistent_: if the value for key `k` stops changing
	in the upstream data, then the value reported by Materialize for key `k` will
	eventually converge to its true value in the upstream database.
  * For `DEBEZIUM` and `DEBEZIUM UPSERT` sources _with_ a transaction metadata topic
    (assuming we are able to implement the corresponding feature), the results are
	fully consistent: the state observed in Materialize at any timestamp `t` corresponds
	to the committed result of some transaction in the upstream database.

**Hard Requirement**: Materialize is _robust_: although the result
of ingesting arbitrarily corrupted data is unspecified, the damage
should be scoped to the affected set of sources. It should not cause Materialize to crash,
nor should it cause general undefined behavior (e.g., incorrect results when querying
unrelated sources or views).

## Non-Goals

It is a permanent non-goal
to support arbitrarily corrupted data, or to support the classic `DEBEZIUM` envelope in
situations that do not fit the constraints described above. The famous quote by
Charles Babbage applies here.

It is a permanent non-goal to support the exotic custom deduplication options
that formerly existed for `DEBEZIUM` sources, like `full` or `full_in_range`.
These were designed to account for the specific requirements of one particular
customer, and were never documented nor widely supported or used; furthermore, nobody
presently working at Materialize understands their intended semantics in detail.
Although the one customer they were designed for is indeed important, we
were happily able to confirm recently that they are able to use `DEBEZIUM UPSERT`
for all of their topics, making the issue moot.

It is _currently_ a non-goal to support Microsoft SQL Server in the classic `DEBEZIUM` envelope,
due to [this issue](https://issues.redhat.com/browse/DBZ-3375) preventing us from
precisely duplicating records. If we come up with a way to deduplicate records that we're
confident in, we will re-evaluate this. Note that Debezium data from
Microsoft SQL Server should work
fine with `DEBEZIUM UPSERT`

Solving the issue of [bounded input reliance](https://github.com/MaterializeInc/materialize/issues/13534)
is outside the scope of this project; until that issue is solved, we must assume
that data is not being compacted away while we're busy reading it, which is
somewhat problematic as we have no way to express our frontiers back upstream to
the user's Kafka installation.

Out-of-core scaling of the O(data)-memory envelopes (`UPSERT` and `UPSERT DEBEZIUM`)
is a future goal, but outside the scope of this document. This will probably involve relying
on the state kept in `persist`, probably with a local caching layer for frequently-used
keys on top.

## Description

For `UPSERT` and `DEBEZIUM UPSERT`, we continue maintaining, as we do now,
an in-memory hash map of the entire state of the relation (keys and values). We use
this hash map to discover when a new record is an update (as opposed to an insert)
and to emit a retraction of the old value.

The main new requirement is that on restart, we must hydrate this state by
reading the old committed value of the collection from `persist`. This is simple
to do with `persist`'s current APIs.

For the classic `DEBEZIUM` envelope, we must validate that the topic meets the constraints
described above in the "Goals" section, and emit an error if it does not. We will
continue using the `ordered` deduplication logic that exists now, which tracks the
data with the highest "offset", and drops data from below that "offset". I use "offset"
in quotes because this does not correspond to a Kafka offset, but is instead a variety of metadata
included inline with the data, which we _only_ know how to correctly interpret assuming the
constraints described above are true.

For deduplicating the initial snapshot in classic `DEBEZIUM` mode,
we need to keep a set of seen keys, and drop any
keys that are seen again. The reason for this is that there is no other known way of
deduplicating messages from the initial snapshot, and Debezium is not known to emit any
metadata that might be useful here (for example, there is no transaction ID or LSN
corresponding to the records). It's okay not to durably store this data anywhere,
since if we crash while ingesting the initial snapshot we can just restart from zero.

The main wrinkle for classic `DEBEZIUM` mode is that not only do we need to persist
the output data, but we also need to persist the in-memory deduplication state, since it's
not possible to recover this without re-reading the upstream topic. This will require a sidecar
"metadata" persist shard, which we must write to while emitting the data for every timestamp.
On restart, we will re-ingest that state as of the restart timestamp

The most difficult part of the project is supporting full consistency; i.e.,
correctly interpreting the transaction metadata topic. Currently, that topic
cannot be deduplicated without keeping O(n) in-memory state in the number of transations
that have ever been processed in the lifetime of the upstream database; clearly,
this cost will be unacceptable for many users. One possible approach is to
change Debezium upstream to improve this situation; this is very feasible, but
would mean we can't expect to support anyone but users of extremely recent Debezium versions.
Another possibility is tolerating some amount of duplication; it's possible that we
can still be correct while only using memory proportional to the _duplicated_ state,
but this needs to be thought through in more detail.

Another wrinkle with the transaction metadata topic is that we need custom logic for compacting it;
briefly, we want to drop all data for transactions that have been fully processed,
but this won't be possible if some of the transaction messages have been duplicated
(and we only see the duplicate record after the transaction has been fully processed). Also, it has
implications for creating new sources: if we've compacted away some past state, we won't be able to
create new sources that are valid for those timestamps; thus we need some machinery for expressing
that the `since` frontier of newly created sources depends on the frontier of their transaction
topic. I spoke with Nikhil and Chae about this some and believe it's possible (though I'm
still concerned about how it interacts with the deduplication story). At any rate it needs
to be thought through further, which is why I've marked transaction topic support as a stretch goal.

## Alternatives

The main alternative is to keep the full log of upstream data in persist,
which will obviously allow us to resume trivially (by restarting from the beginning).

I advocate for rejecting this alternative because the space
requirement and restart time requirement are unacceptable for many real-world use cases.

## Open questions

Robustness - bad data with the classic `DEBEZIUM` envelope
can cause us to end up with negatively many copies of a given row.
We need to ensure that this is handled gracefully by all downstream operators, and
can at worst cause Materialize to emit errors for affected sources, but not to crash.

The open issues for the transaction metadata topic as described in the "Description"
section.

Petros had an idea last year about doing deduplication by reading Debezium's internal offset
commit topic, and only keeping state proportional to the size of data _between_ any two offsets.
This might work, but it seems like relying on internal Debezium state is suboptimal unless
it's unavoidable. But maybe it could let us more ingest the initial snapshot without requiring
O(keys) state? That could be a game-changer for use cases where that data is too
much to keep in memory even on the largest storage host.
