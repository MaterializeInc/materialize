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

Time units of milliseconds are used below for convenience to reflect current implementations. However, this document
does not take any opinion on what time units should be used for the design proposed.

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
by a Kafka source, the acknowledgment of an LSN by a PostgresSQL source, etcetera) or the data being acknowledged. For
the purposes of this document we will consider all of these events to have happened at the timestamp assigned to them by
STORAGE. This is not accurate for varius reasons. There will be a follow-up design document that goes into depth for
these operations.

## Description

Strict Serializability (in simplified terms) means that you can assign a total order to all transactions in the system,
and that the total order is constrained by the real time occurrences of those transactions. In other words, if
transaction A finishes in real time before transaction B starts in real time, then transaction A MUST be ordered before
transaction B in the total order. Concurrent transaction can be ordered arbitrarily. For a more in depth discussion
please
read [https://jepsen.io/consistency/models/strict-serializable](https://jepsen.io/consistency/models/strict-serializable)
and chapters 7 and 9 of [Designing Data-Intensive Applications](https://dataintensive.net/).

Materialize can satisfy the constraints of Strict Serializability if all reads and writes occur at specific timestamps,
and these timestamps are non-decreasing in real-time order, with strict increases for writes following reads. Each
timestamp is assigned to an event at some real-time moment within the event's bounds (its start and stop). Each
timestamp transition is made durable before any subsequent response is issued.

We can then use these timestamp to form a total order constrained by real time:

- Transaction T1 is ordered before Transaction T2 if the timestamp of T1 < the timestamp of T2.
- Transaction T1 is ordered before Transaction T2 if the timestamp of T1 == the timestamp of T2, T1 is write-only, and
  T2 is read-only.
- Transaction T1 can be arbitrarily ordered with Transaction T2 if the timestamp of T1 == the timestamp of T2, T1 is
  read-only, and T2 is read-only.
- Transaction T1 can be arbitrarily ordered with Transaction T2 if the timestamp of T1 == the timestamp of T2, T1 is
  write-only, and T2 is write-only.

### Global Timestamp

All reads to any object and all writes to user tables are assigned a timestamp by the Coordinator on a single thread.
The logic for determining what timestamp to return must satisfy:

- If it's a read, then it is larger than or equal to the `since` of all objects involved in the query.
- If it's a write, then it is larger than or equal to the `upper` of the table.
- It is larger than or equal to the global timestamp
- It is larger than or equal to the timestamp of the most recently completed query.
    - If the query is a write and the most recently completed query was a read, then the timestamp returned must be
      strictly larger than the previous timestamp.

If some operation is given timestamp `ts`, then once that operation completes the global timestamp must be larger than
or equal to `ts`. How this specific property is achieved for reads is described
in [Read Capabilities on Global Timestamp](#Read Capabilities on Global Timestamp) and how it's achieved for writes is
described in [Group Commit/Write Coalesce](#Group Commit/Write Coalesce).

The properties described in this section are sufficient to ensure Strict Serializability across one start of
Materialize (i.e. not between restarts).

- [X] All reads and writes occur at specific timestamps: satisfied by the Coordinator assigning timestamps to all
  operations.
- [X] These timestamps are non-decreasing in real-time order: all timestamps are assigned sequentially on a single
  thread. Each timestamp is guaranteed to be larger than or equal to the timestamp of the most recently completed query.
- [X] With strict increases for writes following reads: the global timestamp explicitly increases after all writes (
  See [Group Commit/Write Coalesce](#Group Commit/Write Coalesce)).
- [X] Each timestamp is assigned to an event at some real-time moment within the event's bounds: For any two events e1
  and e2, if e2 starts after e1 finished, then e2's timestamp will be larger or equal timestamp than e1's timestamp.
- [X] Each timestamp transition is made durable before any subsequent response is issued: writes wait for an `append`
  command to be processed before returning a response to the client.
    - NOTE: There is ongoing work to ensure that an `append` command doesn't return until it is durable.

### Global Timestamp Recovery

After a restart the Coordinator must reestablish the global timestamp to some value greater than or equal to the value
of the previous global timestamp before the restart. This ensures that a read or write after the restart is assigned a
timestamp greater than or equal to all reads and writes before the restart.

For an initial implementation the Coordinator will record all global timestamps in the catalog. On restart the
Coordinator can read this value from the catalog.

TODO: better implementation.

The properties described in this section and the [Global Timestamp](#Global Timestamp) section are sufficient to ensure
Strict Serializability across restarts.

### Group Commit/Write Coalesce

Write read cycles (write followed by read followed by write followed by read etc) and consecutive writes can cause the
global timestamp to increase in an unbounded fashion.

Proposal: All writes across all sessions should be blocked and added to a queue. At the end of X millisecond all pending
writes are sent in a batch to STORAGE and committed together at the same timestamp. The commits are all assigned a
timestamp 1 larger than the current global timestamp and the global timestamp should be increased to this new value.

This approach limits the per session write throughput to 1 write transaction per X milliseconds for user tables.

This approach guarantees that when a write completes, the global timestamp is larger than or equal the timestamp of that
write. Also, when a write completes, all previous writes and reads were at a lower timestamp than the write.

NOTE: If the client had multiple writes to a single table known ahead of time, then grouping them in a single
multi-statement write transaction would increase throughput. There may be some user education needed for this.

NOTE: This would also fix the problem discussed in [#12198](https://github.com/MaterializeInc/materialize/issues/12198).

### Read Capabilities on Global Timestamp

Proposal: Explicitly hold read capabilities for the global timestamp for all sources. All reads are assigned a timestamp
equal to the global timestamp.

Periodically the Coordinator will need to update the read capabilities to the current global timestamp. As the interval
between read capability updates increase, the memory usage of each node goes up and the communication between each node
goes down, and vice versa.

This approach guarantees that when a read completes, the global timestamp is equal to or larger than the timestamp of
that read.

## Alternatives

- We don't hold read capabilities at the global timestamp. Instead, we block all queries that require a timestamp larger
  than the global timestamp until the global timestamp advances to the timestamp of that
  query. [#11150](https://github.com/MaterializeInc/materialize/pull/11150)
  and [#10173](https://github.com/MaterializeInc/materialize/pull/10173) are previous pull requests that try and
  implement this query blocking.
- If we didn't hold read capabilities, then we could always advance the global timestamp to the timestamp of most recent
  operation. Any operation on objects that haven't caught up to the global timestamp must be blocked until the object
  catches up to the global timestamp.
- Instead of explicitly holding read capabilities at the global timestamp, the Coordinator can include the current
  global timestamp in every message sent to dataflow nodes. The nodes would then prevent compaction up to the most
  recently seen global timestamp. After the node learns of a new timestamp they can allow compaction of every object up
  to the new timestamp. However, if a node doesn't receive any messages from the Coordinator then memory usage will
  balloon up until it hears from the Coordinator again.
- Instead of using group commit/write coalesce, the coordinator could buffer all writes to user tables in a durable
  write ahead log. All writes to user tables that occur in the same millisecond can be assigned the same timestamp, as
  long as there are no reads in-between them. Then at the end of the millisecond, or after X milliseconds, all writes
  are sent to STORAGE in a batch. If Materialize crashes/restarts before sending the writes to STORAGE, then it can just
  read the unsent writes from the durable logs after starting back up. This prevents us from having to increment the
  global timestamp on every write to a user table. However, this does not solve the problem if there are reads
  in-between the writes or if we read from a view that's ahead of the wall clock.
- Hybrid Logical Timestamps are timestamps where the X most significant bits are used for the epoch and the Y least
  significant bits are used for a counter. Every clock tick the X bits are increased by 1 and the Y bytes are all reset
  to 0. Every write increments the Y bits by 1. This would allow us to have 2^Y writes per clock tick per object,
  without blocking any queries. Any write in excess of 2^Y per clock tick per object must be blocked until the next
  clock tick. This way no object's `upper` would have to advance past the current global timestamp if the global
  timestamp tracked the wall clock. CockroachDB uses similar timestamps
  here: https://github.com/cockroachdb/cockroach/blob/711ccb5b8fba4e6a826ed6ba46c0fc346233a0f7/pkg/sql/sem/builtins/builtins.go#L8548-L8560

## Open Questions

- This document does not go into great detail on how crashes/restarts in the middle of an operation affect consistency.
  All read and write operations are atomic and idempotent so crashing/restarting will not leave the system in an
  intermediate or invalid state due to read/writes. However, DDLs are not idempotent or atomic. What happens if the
  system crashes in the middle of DDL? Some of this is being thought about by the command reconciliation work.
