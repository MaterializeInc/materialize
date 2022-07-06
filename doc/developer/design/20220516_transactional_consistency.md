# Materialize Transactional Consistency

## Summary

A description of Materialize’s consistency guarantees with respect to the operations that occur inside of Materialize.
Operations include: `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements (but not `TAIL`)

This design document is in service of [Epic #11631](https://github.com/MaterializeInc/materialize/issues/11631)
and [Correctness Property #1](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/ux.md#correctness)
(copied below).

> *Transactions in Materialize are strictly serializable with respect to the operations that occur inside of Materialize.* Operations include:
> - `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements (but not `TAIL`)
> - Materialize initiated acknowledgements of upstream sources (*e.g., the commit of an offset by a Kafka source, the acknowledge of an LSN by a PostgresSQL source, etcetera*)

This document does not take any opinion on whether the changes proposed should be the default behavior or an opt-in
behavior via some configuration.

Time units of milliseconds are used below for convenience to reflect current implementations. However, this document
does not take any opinion on what time units should be used for the design proposed.

## Goals

The goal of this design document is to describe what changes are needed to provide Strict Serializability to the
operations mentioned in correctness property 1.

## Non-Goals

The following items' impact on consistency is not considered:

- `TAIL`
- Sinks.
- `AS OF`, which allows clients to specify the timestamp of a query and do not constrain the timestamp selection of
  other queries. These properties allow clients to circumvent our consistency guarantees.

## Description

Strict Serializability (in simplified terms) means that you can assign a total order to all transactions in the system,
and that the total order is constrained by the real time occurrences of those transactions. In other words, if
transaction A finishes in real time before transaction B starts in real time, then transaction A MUST be ordered before
transaction B in the total order. Concurrent transactions can be ordered arbitrarily. For a more in depth discussion
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
- Transaction T1 is ordered before Transaction T2 if the timestamp of T1 == the timestamp of T2, the timestamp for T1
  was assigned before the timestamp of T2, T1 is read-only, and T2 is read-only.
- Transaction T1 is ordered before Transaction T2 if the timestamp of T1 == the timestamp of T2, the Global ID of T1 is
  less than the Global ID of T2, T1 is write-only, and T2 is write-only. NOTE: This works because currently you can only
  write to a single table in a transaction.

### Timelines

Timelines are the unit of consistency that Materialize provides and can be read about in detail in the
[Timeline design doc](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210408_timelines.md)
. Every database object belongs to a single timeline and each read transaction can only include objects from a single
timeline. Each timeline defines their own notion of time and consistency guarantees are only provided within a single
timeline. There can only be at most a single Coordinator thread assigning timestamps per timeline (though multiple
timelines can share a thread if they want). All the properties discussed below need to happen on a per-timeline basis,
and require no communication across timelines (except to serialize access to the catalog).

Each timeline needs some definition for the current time. For example timelines that track real time can use the system
clock for the current time. The current time can go backwards, but timestamps assigned must be non-decreasing.

The document also allows user tables to exist in any timeline.

### Global Timestamp

All reads to any object and all writes to user tables are assigned a timestamp by the Coordinator on a single thread.
The logic for determining what timestamp to return must satisfy:

- If it's a read, then it is larger than or equal to the `since` of all objects involved in the query.
- If it's a write, then it is larger than or equal to the `upper` of the table.
- It is larger than or equal to the timestamp of the most recently completed query in the same timeline.
    - If the query is a write and the most recently completed query was a read, then the timestamp returned must be
      strictly larger than the previous timestamp.

We can accomplish this by using a single global timestamp per timeline. The timestamp will be initialized to some valid
initial value and be advanced periodically to some valid value that is higher than the previous value. The periodic
advancements should maintain the property that the global timestamp is within some error bound of all the timestamps
being assigned to data from upstream sources. All reads are given a timestamp equal to the global timestamp, which is
greater than or equal to the `since` of all involved objects as described
in [Read Capabilities on Global Timestamp](#Read Capabilities on Global Timestamp). All writes to user tables are given
a timestamp larger than the global timestamp, and when the write completes then the timestamp should be advanced to the
timestamp of that write.

As an example, a timeline's timestamp can be initialized to the current system time and be advanced periodically to the
current value of a monotonically increasing system clock. Another example is that a timeline's timestamps can be
initialized to the timestamp's minimum value and can be periodically advanced in the following
way: `global_timestamp = max(min(uppers) - 1, global_timestamp)`, where `uppers` is the list of all `upper`s in the
timeline. These are just potential implementations and not the only correct implementations.

If some operation is given timestamp `ts`, then once that operation completes the global timestamp must be larger than
or equal to `ts`. How this specific property is achieved for reads is described
in [Read Capabilities on Global Timestamp](#Read Capabilities on Global Timestamp) and how it's achieved for writes to
user tables is described in [Group Commit/Write Coalesce](#Group Commit/Write Coalesce).

The properties described in this section are sufficient to ensure Strict Serializability across one start of
Materialize (i.e. not between restarts).

- [X] All reads and writes occur at specific timestamps: satisfied by the Coordinator assigning timestamps to all
  operations.
- [X] These timestamps are non-decreasing in real-time order: all timestamps are assigned sequentially on a single
  thread. Each timestamp is guaranteed to be larger than or equal to the timestamp of the most recently completed query.
- [X] With strict increases for writes following reads: the timestamp assigned to all writes is larger than all previous
  timestamps (See [Group Commit/Write Coalesce](#Group Commit/Write Coalesce)).
- [X] Each timestamp is assigned to an event at some real-time moment within the event's bounds: For any two events e1
  and e2, if e2 starts after e1 finished, then e2's timestamp will be larger or equal timestamp to e1's timestamp.
- [X] Each timestamp transition is made durable before any subsequent response is issued: writes wait for an `append`
  command to be made durable before returning a response to the client, additionally timestamp transitions are persisted
  to disk (See [Global Timestamp Recovery](#Global Timestamp Recovery)).

### Global Timestamp Recovery

After a restart the Coordinator must reestablish the global timestamp to some value greater than or equal to the value
of the previous global timestamp before the restart. This ensures that a read or write after the restart is assigned a
timestamp greater than or equal to all reads and writes before the restart.

Proposal: Use a [Percolator](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf) inspired
timestamp recovery protocol (See section 2.3 Timestamps). Periodically we durably store some value greater than the
current global timestamp. We never allocate a timestamp larger than or equal the one durably stored, without first
updating the durably stored timestamp. When Materialize restarts, it uses the value durably stored as the initial
timestamp.

The properties described in this section and the [Global Timestamp](#Global Timestamp) section are sufficient to ensure
Strict Serializability across restarts.

### Group Commit/Write Coalesce

Write read cycles (write followed by read followed by write followed by read etc) and consecutive writes can cause the
global timestamp to increase in an unbounded fashion.

NOTE: Recall that each timeline has some definiton of the current time (i.e. the system clock) and a current timestamp
(the timestamp most recently assigned to an operation).

Proposal: All writes across all sessions per timeline should be added to a queue. If the current timestamp of the global
timeline is less than or equal to the current time of the global timeline, then all writes in the queue can be executed
and committed immediately. Otherwise the queue must wait until the current time is equal to or greater than the current
timestamp, before executing and committing the writes in the queue. All writes in a queue are executed and committed
together in a batch and assigned the same timestamp. The commits are all assigned the current global write timestamp.

NOTE: The `TimestampOracle` provides us the property that the global write timestamp will be higher than all previous
reads.

This approach limits the per session write throughput to 1 write transaction per 1 time unit for user tables.

This approach guarantees that when a write completes, the global timestamp is larger than or equal the timestamp of that
write. Also, when a write completes, all previous writes and reads were at a lower timestamp than the write. Also, it
places a bounds on how much faster the global timestamp can advance compared to the current time.

NOTE: If the client had multiple writes to a single table known ahead of time, then grouping them in a single
multi-statement write transaction would increase throughput. There may be some user education needed for this.

NOTE: This would also fix the problem discussed in [#12198](https://github.com/MaterializeInc/materialize/issues/12198).

### Read Capabilities on Global Timestamp

Proposal: Explicitly hold read capabilities for a timestamp less than or equal to the global timestamp on all objects.
All reads are assigned a timestamp equal to the global timestamp.

This approach guarantees that when a read completes, the global timestamp is equal to or larger than the timestamp of
that read.

We can still allow non-strict serializable reads to exist at the same time. The timestamps of these reads will not be
constrained to the global timestamp and will be selected using the old method of timestamp selection. This will allow
clients to trade off consistency for recency and performance on a per-query basis.

Periodically the Coordinator will want to update the read capabilities to allow compaction to happen. There are multiple
dimensions to these update with their own tradeoffs:

- As the interval between updates increase, the memory usage of each node goes up and the communication between each
  node goes down, and vice versa.
- A higher value chosen for the new read capability will decrease the amount of memory used, but potentially increase
  the latency of lower consistency reads, and vice versa.

### Upstream Source Acknowledgements

To achieve strict serializability of acknowledgements of upstream sources, acknowledgements should behave the same as a
write transaction. The data being acknowledged is the write of the transaction and the acknowledgement is the commit.
Materialize therefore must satisfy the following properties:

1. If data with a timestamp `ts` has been acknowledged by Materialize, then all reads must have a timestamp greater than
   or equal to `ts`.
2. If data with a timestamp `ts` has not been acknowledged by Materialize, then no read can have a timestamp greater
   than or equal to `ts`.

Property 1 prevents the following scenario: a client goes to an upstream source and sees that some data has been
acknowledged by Materialize but then can't see that data in Materialize.

Property 2 prevents the following scenario: a client sees some data in Materialize, but doesn't see that it's been
acknowledged in an upstream source.

This guarantee is actually too strong for our needs and has very negative availability ramifications. Once an
acknowledgement has been sent by Materialize, then no reads can be serviced until the upstream source has confirmed that
it has received our acknowledgement. Before the confirmation, Materialize has no way of knowing if the acknowledgement
has been received, and any timestamp selected for a read will be susceptible to one of the negative scenarios described
above. This means is that if any upstream source is unresponsive to an acknowledgement, then all reads will be halted
until that source is responsive again.

Property 1 alone is still a useful property, because property 1 allows us to provide guarantees like the following: A
client is using synchronous replication with an upstream source. When their transaction commits in the upstream source,
they are guaranteed to also be able to see the transaction in Materialize. However, they may actually see the
transaction in Materialize before their transaction commits in the upstream source. This is still a powerful and useful
guarantee to provide. Therefore, we should break the second bullet point into the two properties described above and
indicate that we only plan to support the first property.

Proposal: When STORAGE wants to acknowledge some data that has a timestamp `ts`, it must wait until it knows that the
global timestamp is larger than or equal to `ts`. STORAGE can discover this by one of the following ways (NOTE: these
aren't separate proposals, they can all work together):

- STORAGE will send an `ACK` request to the Coordinator timestamped with `ts`. The Coordinator will wait for the global
  timestamp to advance to `ts` and then send a response back to STORAGE indicating that it's OK to send the
  acknowledgement.
- If STORAGE receives a write or read that doesn't use `AS OF`, is in the same timeline as the data being acknowledged,
  and has a timestamp larger than or equal to `ts`, then it knows that the global timestamp must have advanced to `ts`
  or greater, and it's safe to send the acknowledgement.

NOTE: There are some race conditions here such as:

1. STORAGE sends an ACK request for `ts` to the Coordinator.
2. STORAGE receives a read at `ts`.
3. STORAGE sends the ACK for `ts` to an upstream source.
4. STORAGES receives ACK OK response from the Coordinator.

This is fine and STORAGE can just ignore the ACK OK response.

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
- Hybrid Logical Timestamps (HLT) are timestamps where the X most significant bits are used for the epoch and the Y
  least significant bits are used for a counter. Every clock tick the X bits are increased by 1 and the Y bytes are all
  reset to 0. Every write increments the Y bits by 1. This would allow us to have 2^Y writes per clock tick, without
  blocking any queries. Any write in excess of 2^Y per clock tick must be blocked until the next clock tick. This way no
  object's `upper` would have to advance past the current global timestamp if the global timestamp tracked the wall
  clock. CockroachDB uses similar timestamps
  here: https://github.com/cockroachdb/cockroach/blob/711ccb5b8fba4e6a826ed6ba46c0fc346233a0f7/pkg/sql/sem/builtins/builtins.go#L8548-L8560

## Open Questions

- This document does not go into great detail on how crashes/restarts in the middle of an operation affect consistency.
  All read and write operations are atomic and idempotent so crashing/restarting will not leave the system in an
  intermediate or invalid state due to read/writes. However, DDLs are not idempotent or atomic. What happens if the
  system crashes in the middle of DDL? Some of this is being thought about by the command reconciliation work.
- Is it possible to mix non-strict serializable writes with strict serializable writes? I don't think so.
