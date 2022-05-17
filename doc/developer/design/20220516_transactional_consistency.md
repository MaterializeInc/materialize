# Materialize Transactional Consistency

## Summary

A description of Materialize’s consistency guarantees with respect to the operations that occur inside of Materialize.
Operations include: `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements (but not `TAIL`)

This design document is in service of [Epic #11631](https://github.com/MaterializeInc/materialize/issues/11631)
and [Correctness Property #1](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/ux.md#correctness)
. Specifically for the first bullet point from the correctness property, “`SELECT`, `INSERT`, `UPDATE`, and `DELETE`
statements (but not `TAIL`)". A follow-up design document will be created for the second bullet point, "Materialize
initiated acknowledgements of upstream sources (e.g., the commit of an offset by a Kafka source, the acknowledge of an
LSN by a PostgresSQL source, etcetera)".

This document does not take any opinion on whether the changes proposed should be the default behavior or an opt-in
behavior via some configuration.

## Goals

The goal of this design document is to describe the transactional consistency guarantees that are provided by
Materialize and describe what changes are needed to provide higher levels of transaction consistency. This document will
focus on Read Uncommitted, Read Committed, Repeatable Read, Serializable, and Strict Serializable consistency levels.

## Non-Goals

This design document does not focus on levels of consistency not mentioned in the [Goals](#Goals) sections unless it is
in service of achieving one of the mentioned consistency levels.

Sinks are left out of this document and not considered for consistency.

`TAIL` is left out of this document and not considered for consistency.

`AS OF` allows clients to specify the timestamp of a query. Queries with `AS OF` do not constrain the timestamp
selection of other queries. This allows clients to circumvent our consistency guarantees, so they’re left out of this
document.

This document does not include Materialize initiated acknowledgements of upstream sources (e.g., the commit of an offset
by a Kafka source, the acknowledgment of an LSN by a PostgresSQL source, etcetera). For the purposes of this document we
will consider all of these events to have happened at the timestamp assigned to them by timely workers. This is not
accurate for multiple reasons, some of which include:

- Clock Skew: Each timely worker has their own clock that is used to timestamp events, that may be out of sync with
  other clocks.
- Event Time vs Process Time: Many events are timestamped with the time they arrive at Materialize not when they
  actually happen. Network and other delays may make this timestamp inaccurate.
- Reading from these sources almost always happen at a timestamp lower than what was just written to these source.
- Etc.

There will be a follow-up design document that goes into depth for these operations.

## Description

Strict Serializability: All reads and writes occur at specific timestamps, and these timestamps are non-decreasing in
real-time order, with strict increases for writes following reads. Each timestamp is assigned to an event at some
real-time moment within the event's bounds (its start and stop). Each timestamp transition is made durable before any
subsequent response is issued.

WIP: EVERYTHING BELOW THIS IS A WORK IN PROGRESS.

NEW PERSPECTIVE APPROACH TO DOC:

### Global Timestamp

All reads and all writes to user tables are assigned a timestamp by the Coordinator on a single thread. The timestamped
returned must satisfy the following properties:

- It is larger than or equal to the `since` of all objects involved in the query.
- It is larger than or equal to the previous timestamp returned.
    - If the query is a write and the previous query was a read, then the timestamp must be strictly larger than the
      previous timestamp returned.

Consulting the `global_timeline` of type `TimestampOracle<Timestamp>` for all queries will help ensure these properties.

OLD DESCRIPTIVE APPROACH TO DOC:

#### Strict Serializable

See [https://jepsen.io/consistency/models/strict-serializable](https://jepsen.io/consistency/models/strict-serializable)
for an in depth formal discussion of Strict Serializable. Strict Serializable is a combination of Serializable and
Linearizable, which means that the total order formed by all transactions must be constrained by their real time
occurrences. So if transaction t1 happened in real time before transaction t2, then t1 must be ordered before t2 in the
total ordering.

Operations that only include user tables, and include at least one user table, are currently Strict Serializable. This
is accomplished by using a single global timestamp and a single thread in the coordinator to order operations. Writes
happen at a timestamp larger than all previous operations. Reads happen at a timestamp equal to or larger than all
previous operations.

Operations that involve reading from objects over upstream data are not currently Strict Serializable. It is possible to
read from a view at one timestamp and then later read from the same view at an earlier timestamp. Consider the following
scenario:

1. Client reads from view v1 that has an upper of `ts + 1` and a since of `ts - 10`. The read will happen at `ts`.
2. Client reads from a join over v1 and view v2. v2 has an upper of `ts - 5` and some since less or equal to `ts - 6`.
   The read will happen at `ts - 6`.

Reference [Timestamp Selection](#Timestamp Selection) to see why these timestamps are selected. This is an obvious
violation of strict serializability because the second read happens later than the first read in real time, but it is
assigned an earlier timestamp.

In order to achieve Strict Serializability in the general case, Materialize must always select a timestamp greater than
or equal to all previously selected timestamps. This will ensure that if an operation happens later in real time, then
it will be assigned an equal or later timestamp. Consecutive reads can (but don't have to) all be assigned an equal
timestamp as long as there’s no writes in between them. Consecutive writes can (but don't have to) all be assigned an
equal timestamp as long as there's no reads in between them. This can be accomplished by the fact that all timestamp
selection happens on a single thread so no coordination is needed. One implementation for this is to consult the global
timestamp for all reads, not just ones involving user tables.

If we use the global timestamp for all reads, then the global timestamp will have to advance in the following cases:

1. Client writes to a user table.
2. One second passes and the global timestamp is below the wall clock.
3. Client reads from a view that requires a timestamp larger than the current global timestamp. Depending on how a
   timestamp is selected for this view, this can occur if `since` is larger than the global timestamp, `upper` is larger
   than the global timestamp, or both are larger than the global timestamp.

For the purposes of this document, the approach described above is sufficient to achieve a consistency level of Strict
Serializability. However, it introduces a performance issue that we will now discuss. Increasing the global timestamp
past the current wall clock can cause reads to be blocked. For example if a client performs many consecutive writes to a
user table then the global timestamp can be pushed far past the wall clock. If the client tries to read from a view that
has a [`since`, `upper`) closely tracking the wall clock, then the read will have to be blocked until the view catches
up to the global timestamp. Another example is if the client reads from a view that has a [`since`, `upper`) larger than
the wall clock, then the global timestamp must be advanced to somewhere in the [`since`, `upper`) range, and the next
read must be at the new timestamp. If the next read was from an object that had a [`since`, `upper`) that closely
tracked the wall clock, it would be blocked until the view caught up to the global timestamp. Below I provide some
solutions to fixing this.

NOTE: This problem already exists when joining between a user table and a view over an upstream source as discussed
in [#12198](https://github.com/MaterializeInc/materialize/issues/12198).

TODO: For now all the alternatives are in the Description section (here), because I wanted to present all options before
selecting one. Once we converge on one approach, I’ll move the rest to the Alternatives section.

##### Do Nothing

The “Do Nothing” approach involves doing nothing to solve this problem. We just allow reads to block if the global
timestamp is too high.

**Pros:**

- Simple to implement.
- Does not limit write throughput.

**Cons:**

- Allows unbounded delay on reads.

##### Block Operations that Advance the Global Timestamp

Before performing any operation that would advance the global timestamp ahead of the current wall time, we block the
operation until the current wall time catches up to the value we want to advance the global timestamp to. These
operations include writing to a user table or reading from a view over an upstream source. As an optimization we can
implement a form of group commit or write coalescing, where all writes to user tables within the same millisecond commit
together at the same timestamp.
[#11150](https://github.com/MaterializeInc/materialize/pull/11150)
and [#10173](https://github.com/MaterializeInc/materialize/pull/10173) are previous pull requests that try and do
something similar.

**Pros:**

- Prevents some reads from being blocked.

**Cons:**

- Limits user table write throughput (1 write per millisecond without group commit) or limits user table write latency (
  1 millisecond per write with group commit).
- Blocks some reads.

##### Hybrid Logical Timestamps

Note: There might be an actual name for this concept, but I chose the name above because it’s similar to Hybrid Logical
Clocks.

Currently, the timestamps used in Materialize for `upper`, `since`, and the global timestamp are 64 bit unsigned
integers that are interpreted as an epoch in milliseconds. Instead, we can change the representation of these timestamps
so that the X most significant bits are used for the epoch and the Y least significant bits are used for a counter.
Every 1 millisecond the X bits are increased by 1 and the Y bytes are all reset to 0. Every write increments the Y bits
by 1. This would allow us to have 2^Y writes per millisecond per object, without blocking any queries. Any write in
excess of 2^Y per millisecond per object must be blocked until the next millisecond. This way no object's `upper` would
have to advance past the current wall clock.

Note: Increasing the size of the timestamp type would give more flexibility (for example to 128 bits).

CockroachDB uses similar timestamps
here: https://github.com/cockroachdb/cockroach/blob/711ccb5b8fba4e6a826ed6ba46c0fc346233a0f7/pkg/sql/sem/builtins/builtins.go#L8548-L8560

**Pros:**

- With a sufficiently high Y, no queries or writes would be blocked.
- May be helpful if/when we wanted to make the coordinator distributed.

**Cons:**

- Complicated to implement and likely has a large blast radius.
- Requires [#8426](https://github.com/MaterializeInc/materialize/issues/8426)
  to be completed first.
- If X is low enough we will overflow our timestamps at some point in the future. This can be mitigated with a high
  enough X.
- Each distinct timestamp written has a significant impact on the underlying view maintenance engine, which will perform
  work to correctly update for each distinct time it is presented with. Spraying lots of distinct times at it introduces
  a substantial amount of potentially zero-value work for it to do.

##### Write Ahead Log (WAL)

This approach involves the coordinator buffering all writes to user tables in a durable write ahead log. All writes to
user tables that occur in the same millisecond can be assigned the same timestamp, as long as there are no reads
in-between them. Then at the end of the millisecond, or after X milliseconds, all writes are sent to STORAGE in a batch.
If Materialize crashes/restarts before sending the writes to STORAGE, then it can just read the unsent writes from the
durable logs after starting back up. This prevents us from having to increment the global timestamp on every write to a
user table. However, this does not solve the problem if there are reads in-between the writes or if we read from a view
that's ahead of the wall clock.

**Pros:**

- No queries are blocked from consecutive writes to user tables.
- A WAL would be useful for many other reasons not in scope for this document. It’s likely that no matter what solution
  we pick, we’d still want to implement a WAL eventually.

**Cons:**

- Only solves part of the problem.
- Requires the WAL backend to have a distributed consensus operation.

##### Fast Forward

Timestamps in Materialize are meant to closely track the wall clock, but are not guaranteed to be exactly equal to the
wall clock. This approach would involve forcibly fast-forwarding the `upper`s of all objects. Whenever an operation
would push the global timestamp above the `upper` of other objects by some threshold, all `upper`s are jumped to the
global timestamp’s new value. The threshold could be set to 0 to prevent blocking any queries.

**Pros:**

- No queries are blocked by more than the assigned threshold.

**Cons:**

- Certain operations will require a broadcast to all nodes.
- I’m not actually sure if this is possible, but if it is possible then it’s probably complicated and has a large blast
  radius.

##### Hold Back Compaction

## Alternatives

- TODO

## Open Questions

- This document does not go into great detail on fault tolerance and how crashes/restarts affect consistency. All read
  and write operations are atomic and idempotent so crashing/restarting will not leave the system in an intermediate or
  invalid state due to read/writes. However, DDLs are not idempotent or atomic. What happens if the system crashes in
  the middle of DDL? Some of this is being thought about by the command reconciliation work.
- How does the global timestamp get re-established after a restart?
