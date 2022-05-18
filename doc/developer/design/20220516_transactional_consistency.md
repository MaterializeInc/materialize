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

The goal of this design document is to describe what changes are needed to provide Strict Serializability to the
operations mentioned above.

## Non-Goals

Sinks are left out of this document and not considered for consistency.

`TAIL` is left out of this document and not considered for consistency.

`AS OF` allows clients to specify the timestamp of a query. Queries with `AS OF` do not constrain the timestamp
selection of other queries. These properties allow clients to circumvent our consistency guarantees, so `AS OF` is left
out of this document.

This document does not include Materialize initiated acknowledgements of upstream sources (e.g., the commit of an offset
by a Kafka source, the acknowledgment of an LSN by a PostgresSQL source, etcetera). For the purposes of this document we
will consider all of these events to have happened at the timestamp assigned to them by timely workers. This is not
accurate for varius reasons. There will be a follow-up design document that goes into depth for these operations.

## Description

Strict Serializability means that all reads and writes occur at specific timestamps, and these timestamps are
non-decreasing in real-time order, with strict increases for writes following reads. Each timestamp is assigned to an
event at some real-time moment within the event's bounds (its start and stop). Each timestamp transition is made durable
before any subsequent response is issued.

### Global Timestamp

All reads to any object and all writes to user tables are assigned a timestamp by the Coordinator on a single thread.
The logic for determining what timestamp to return must be altered to satisfy the following properties:

- If it's a read, then it is larger than or equal to the `since` of all objects involved in the query (currently
  satisfied).
- If it's a write, then it is larger than or equal to the `upper` of the table (currently satisfied).
- It is larger than or equal to the timestamp of the most recently completed query (not currently satisfied).
    - If the query is a write and the most recently completed query was a read, then the timestamp returned must be
      strictly larger than the previous timestamp.

In order to achieve this, all timestamps returned must be larger than or equal to the global timestamp. Currently, we
only look at the global timestamp for user table related queries, however we need to start looking at it for all
queries.

If some operation is given timestamp `ts`, then once that operation completes the global timestamp must be larger than
or equal to `ts`. How this specific property is achieved for reads and writes is described in sections below.

Additionally, the `global_timeline` should continue to increase every X seconds by X seconds to roughly track the system
clock. As of writing this document, the current value for X is 1.

The properties described in this section are sufficient to ensure Strict Serializability across one start of
Materialize (i.e. not between restarts).

- [X] All reads and writes occur at specific timestamps: satisfied by the Coordinator assigning timestamps to all
  operations.
- [X] These timestamps are non-decreasing in real-time order: all timestamps are assigned sequentially on a single
  thread. Each timestamp is guaranteed to be larger than or equal to the timestamp of the most recently completed query.
- [X] With strict increases for writes following reads: the global timestamp explicitly increases after all writes.
- [X] Each timestamp is assigned to an event at some real-time moment within the event's bounds: For any two events e1
  and e2, if e2 starts after e1 finished, then e2 will have a larger or equal timestamp.
- [X] Each timestamp transition is made durable before any subsequent response is issued: writes wait for an `append`
  command to be processed before returning a response to the client.
    - NOTE: There is ongoing work to ensure that an `append` command doesn't return until it is durable.

### Global Timestamp Recovery

After a restart the Coordinator must reestablish the global timestamp to some value greater than or equal to the value
of the previous global timestamp before the restart. This ensures that a read or write after the restart is assigned a
timestamp greater than or equal to all reads and writes before the restart.

TODO (The Simplest way is to make each timestamp durable but this is inefficient).

The properties described in this section and the [Global Timestamp](#Global Timestamp) section are sufficient to ensure
Strict Serializability across restarts.

### Group Commit/Write Coalesce

Write read cycles (write followed by read followed by write followed by read etc) and consecutive writes currently cause
the global timestamp to increase in an unbounded fashion. Instead, all writes across all sessions should be blocked and
added to a queue. At the end of 1 millisecond all pending writes are sent in a batch to STORAGE and committed together
at the same timestamp. The commits are all assigned a timestamp 1 larger than the global timestamp and the global
timestamp should be updated to this new value.

This approach limits the per session write throughput to one write transaction per millisecond for user tables.

This approach guarantees that when a write completes, the global timestamp is larger than or equal the timestamp of that
write. Also, when a write completes, all previous reads were at a lower timestamp than the write.

NOTE: If the client had multiple writes to a single table known ahead of time, then grouping them in a single
multi-statement write transaction would increase throughput. There may be some user education needed for this.

NOTE: This would also fix the problem discussed in [#12198](https://github.com/MaterializeInc/materialize/issues/12198).

##### Hold Back Compaction

Currently, the compaction process increases the `since` of every object such that it is equal
to `upper - logical_compaction_window` if there are no read holds, otherwise `since` will not increase past any read
hold. This approach adds a constraint to compaction such that `since` is never increased past the global timestamp. This
will ensure that all objects have some valid timestamp to read from that is less than or equal to the global timestamp.

With the approach described in [Group Commit/Write Coalesce](# Group Commit/Write Coalesce) and this section, the global
timestamp will never advance past the current wall time of the coordinator node. Therefore, nodes can use the current
wall time as a proxy for the global timestamp. In order to prevent compaction past the global timestamp, each object
just needs to prevent compaction past the current wall time.

Due to clock skew and other clock related issues, the wall time isn't a completely accurate proxy for the global
timestamp. A node's current wall time will be close but not always equal to the coordinator's current wall time. Any
query involving an object who's `since` is greater than the global timestamp must be assigned a timestamp `ts` s.t.
`ts` is greater than to the global timestamp and `ts` is greater than or equal to `since`. This query should be blocked
until the global timestamp advances to be greater than or equal to `ts`
. [#11150](https://github.com/MaterializeInc/materialize/pull/11150)
and [#10173](https://github.com/MaterializeInc/materialize/pull/10173) are previous pull requests that try and implement
this query blocking.

This approach guarantees that when a read completes, the global timestamp is equal to or larger than the timestamp of
that read.

## Alternatives

- We don't hold back compaction and just block all queries that require a timestamp larger than the global timestamp.
- If we didn't hold back compaction, then we could always advance the global timestamp to the most recent operation.
  Then any operation on objects that haven't caught up to the global timestamp must be blocked until the object catches
  up to the global timestamp.
- Instead of using the wall time on each node as a proxy for the global timestamp, the Coordinator could broadcast the
  global timestamp every time it is updated.
- Instead of using the wall time on each node as a proxy for the global timestamp, the Coordinator can include the
  current global timestamp in every message sent to dataflow nodes. The nodes would then prevent compaction up to the
  most recently seen global timestamp. After the node learns of a new timestamp they can allow compaction of every
  object up to the new timestamp. However, if a node doesn't receive any messages from the Coordinator then memory usage
  will balloon up until it hears from the Coordinator again.
- Instead of using group commit/write coalesce, the coordinator could buffer all writes to user tables in a durable
  write ahead log. All writes to user tables that occur in the same millisecond can be assigned the same timestamp, as
  long as there are no reads in-between them. Then at the end of the millisecond, or after X milliseconds, all writes
  are sent to STORAGE in a batch. If Materialize crashes/restarts before sending the writes to STORAGE, then it can just
  read the unsent writes from the durable logs after starting back up. This prevents us from having to increment the
  global timestamp on every write to a user table. However, this does not solve the problem if there are reads
  in-between the writes or if we read from a view that's ahead of the wall clock.
- Hybrid Logical Timestamps are timestamps where the X most significant bits are used for the epoch and the Y least
  significant bits are used for a counter. Every 1 millisecond the X bits are increased by 1 and the Y bytes are all
  reset to 0. Every write increments the Y bits by 1. This would allow us to have 2^Y writes per millisecond per object,
  without blocking any queries. Any write in excess of 2^Y per millisecond per object must be blocked until the next
  millisecond. This way no object's `upper` would have to advance past the current global timestamp if the global
  timestamp tracked the wall clock. CockroachDB uses similar timestamps
  here: https://github.com/cockroachdb/cockroach/blob/711ccb5b8fba4e6a826ed6ba46c0fc346233a0f7/pkg/sql/sem/builtins/builtins.go#L8548-L8560

## Open Questions

- This document does not go into great detail on how crashes/restarts in the middle of an operation affect consistency.
  All read and write operations are atomic and idempotent so crashing/restarting will not leave the system in an
  intermediate or invalid state due to read/writes. However, DDLs are not idempotent or atomic. What happens if the
  system crashes in the middle of DDL? Some of this is being thought about by the command reconciliation work.
