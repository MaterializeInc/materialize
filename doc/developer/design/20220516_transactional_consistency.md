# Materialize Transactional Consistency

## Summary

A description of Materialize’s consistency guarantees with respect to the operations that occur inside of Materialize.
Operations include:

- `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements (but not `TAIL`)

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

`AS OF` allows clients to specify the timestamp of a query. This allows clients to circumvent our consistency
guarantees, so they’re left out of this document.

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

### Data Manipulation and Definition

The main interface Materialize provides to clients for data manipulation and data definition is SQL. SQL provides
transactions which allows a client to group multiple statements into a single atomic operation. Statements that are not
explicitly inside a transaction should be considered to be implicitly inside a single statement transaction.

#### Transactions

Materialize supports two kinds of transactions, read-only and write-only transactions.

##### Read Only Transactions

Read only transactions allows a client to read from multiple objects at a single timestamp. When a client performs the
first read of the transaction, a timestamp is selected for all subsequent reads and a read hold is taken on all objects
in the same schema for that timestamp. A read hold prevents compaction from happening on an object up to that timestamp.
A consequence of this is that read only transactions can only read from objects that are in the same schema and that
existed at the time of the first read (due to implementation details on the read holds).

There is an additional restriction that objects on different timelines cannot be read in the same transaction. Details
on this are out of scope for this document but more details can be found
here: [https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210408_timelines.md](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210408_timelines.md)

##### Write Only Transactions

Write only transaction allows a client to perform multiple writes atomically. They are currently very limited and only
allow writes to a single table. They also only allow `INSERT` statements (no `DELETE`, `UPDATE`, `CREATE`
, `INSERT INTO SELECT`, etc). All writes within a transaction are buffered in a session local variable and are not
visible to any other session. When the transaction commits, a timestamp is selected by the coordinator and all writes
are sent to STORAGE with the selected timestamp. Commits do not return an OK message to the client until all writes have
been made durable.

NOTE: This is written with the assumption that `append` STORAGE commands for a single object are idempotent, atomic, and
don’t return until they are durable. While this isn’t currently the case in Materialize, it is being actively worked on
and should be completed soon.

#### Single Read/Write Statements

- `INSERT` statements behave identically to write-only transactions containing a single statement.
- `SELECT` statements behave very similarly to read-only transactions containing a single statement. A timestamp is
  selected for the read, the read is performed at that timestamp, and the results are sent back to the client. However,
  no read holds are taken.
- `COPY TO` statements perform a read and then writes the results to an external source. The read is executed exactly as
  described in the `SELECT` bullet above (`TAIL` is excluded from this document). The write is excluded from this
  discussion of consistency because it is to an external system.
- `COPY FROM` statements read from an external source and then writes the results to a table. The write is executed
  exactly as described in the `INSERT` bullet above. The read is excluded from this discussion of consistency because it
  is from an external source.

#### DDL

- `CREATE` statements create new objects such as tables, sources, and views. They have five steps
    1. Update the in-memory catalog representation.
    2. Update on disk stash backed catalog with changes in a transaction (transactions here refer to stash implemented
       transaction, not general Materialize SQL transactions).
    3. Select a timestamp and send updates to built-in tables.
    4. Send command to STORAGE to create sources if necessary.
        1. Tables have an initial `since` matching the timestamp selected for the built-in table updates.
        2. All other objects have an initial `since` of 0 (`Timestamp::Minimum`)
    5. Send command to COMPUTE to create dataflows if necessary.

  All five steps happen on the main coordinator thread and no other operation can execute in the meantime.

- `DROP` statements remove objects such as tables, sources, and views. They have 4 steps
    1. Update the in-memory catalog representation.
    2. Update on disk stash backed catalog with changes in a transaction (transactions here refer to stash implemented
       transaction, not general Materialize SQL transactions).
    3. Select a timestamp and send updates to built-in tables.
    4. Send command to STORAGE to drop sources if necessary.

All five steps happen on the main coordinator thread and no other operation can execute in the meantime.

#### Read Then Write Statements

`UPDATE`, `DELETE`, and `INSERT INTO SELECT` queries are all classified as ReadThenWrite statements, because they all
first perform a read and then perform a write. Materialize does not currently support general purpose Read/Write
transactions, but has a specific implementation for these types of statements.

The first thing a ReadThenWrite statement does is acquire a lock on the Coordinator. This lock prevents all write-only
transactions and other ReadThenWrite statements from executing until the current ReadThenWrite statement has fully
completed. Then a timestamp is selected and the read is performed at that timestamp. Finally, the write behaves
identically to a write-only transaction containing a single statement (a new timestamp is selected for the write).

For more information you can read the design document for these
statements: [https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210715_table_update.md](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210715_table_update.md)

NOTE: There is active discussion on making these not require a global lock on the Coordinator to help with performance.

### Timestamp Selection

All reads and writes happen at a specific timestamp that is selected by Materialize. This section describes how
Materialize selects timestamps. All timestamp selection happens on a single Coordinator thread, so no two timestamps can
be selected concurrently. The [Strict Serializable](#Strict Serializable) section will propose some changes to achieve a
consistency level of Strict Serializable.

#### Since and Upper

Every object maintains a range of timestamps, [`since`, `upper`). All reads to that object must take place at a
timestamp greater than or equal to `since`, but less than `upper`. All writes to that object must take place at a
timestamp greater than or equal to `upper`. Throughout the lifetime of the object, both `since` and `upper` can
increase, but they never decrease. This ensures that a read at timestamp `ts` will always return the same value, because
if `ts` is readable, then all future writes must take place at a timestamp greater than `ts`.

If a timestamp larger than `upper` is selected to read an object, the read will block until `upper` has advanced past
the selected timestamp.

NOTE: When an object is first created the `upper` may be initialized below the `since`. This will quickly correct itself
and, as mentioned above, all reads will block until the upper has advanced.

#### User Tables

The Coordinator maintains a global timestamp for all user tables. Every one second this timestamp is increased by one
second and all tables are advanced to the new timestamp. Advancing a table to `ts` causes it’s `upper` to be set to `ts`
, and it’s `since` to be set to `logical_compaction_window` less than `ts` (`logical_compaction_window` is a
configuration variable that determines how often to compact old data).

When determining a timestamp to use to read from one or more user tables, the Coordinator uses the current value of the
global timestamp. Additionally, the Coordinator advances all tables to a timestamp larger than the timestamp selected
for the read.

When determining a timestamp to use to write to a user table, the Coordinator uses a timestamp larger than the previous
timestamp used (either for a read or a write) and advances all tables to this timestamp.

#### Upstream Sources

When determining a timestamp to use to read from a view that consists of one or more upstream sources (but no user
tables), the Coordinator chooses the maximum of:

- The largest `since` of all sources
- One unit less than the smallest `upper` of all sources.

#### Combination of User Tables and Upstream Sources

When determining a timestamp to use to read from a view that consists of one or more upstream sources and one or more
user tables, the Coordinator chooses the maximum of:

- The largest `since` of all sources
- The global timestamp.

As a side effect all user tables will be advanced to a timestamp greater than the global timestamp.

#### DDL

Whenever a timestamp is selected for DDL, we use the same method as determining a timestamp for a write to a user table.

### Views

This section will briefly discuss views. If a write happens on a table at timestamp `ts`, then Differential Dataflow
provides the guarantee that the write will happen on all downstream views at timestamp `ts` too. This helps us ensure
that if we write to a table at timestamp `ts` and then read from a downstream view at timestamp `ts + 1` then we are
guaranteed to see the results of that write.

### Consistency

For details on consistency models please read: [https://jepsen.io/consistency](https://jepsen.io/consistency) and
chapters 7 and 9, of [Designing Data-Intensive Applications](https://dataintensive.net/). This document focuses on
transactional consistency levels included in the ANSI SQL spec.

#### Read Uncommitted

See [https://jepsen.io/consistency/models/read-uncommitted](https://jepsen.io/consistency/models/read-uncommitted) for
an in depth formal discussion of Read Uncommitted. Read Uncommitted prevents Dirty Writes, which means that one
transaction cannot update the values of another transactions uncommitted values.

This is solved by disallowing updates in transactions and by write-only transactions buffering their writes until
commit. No update can view the buffered data until after the write-only transaction has committed. While the write-only
transaction is committing, no other queries can execute. Once the commit is complete, all writes are guaranteed to be
durable.

#### Read Committed

See [https://jepsen.io/consistency/models/read-committed](https://jepsen.io/consistency/models/read-committed) for an in
depth formal discussion of Read Committed. Read Committed prevents Dirty Reads, which means that one transaction cannot
view the uncommitted data of another transaction.

This property holds from how Materialize satisfies [Read Uncommitted](#Read Uncommitted). No read can view uncommitted
buffered data until after the commit is complete.

#### Repeatable Read

See [https://jepsen.io/consistency/models/repeatable-read](https://jepsen.io/consistency/models/repeatable-read) for an
in depth formal discussion of Repeatable Reads. Repeatable Reads prevents Fuzzy/Non-Repeatable Reads, which means that
once a transactions reads some value for an object, it will always read that same value, regardless if it’s been
overwritten by another transaction.

Differential Dataflow provides us with some useful properties to achieve this.

1. All reads and writes take place at a provided timestamp.
2. All reads at timestamp `ts` will return all writes at timestamp ≤ `ts`.
3. All reads at timestamp `ts` will not return any of the writes at timestamps > `ts`.

As described in the [Timestamp Selection](#Timestamp Selection) section, whenever a read happens at timestamp `ts`, all
subsequent writes will happen at a timestamp > `ts`. Since a read transaction performs all of it’s read at the same
timestamp, we can guarantee that subsequent reads will always read the same value since all writes will happen at a
later timestamp.

#### Serializable

See [https://jepsen.io/consistency/models/serializable](https://jepsen.io/consistency/models/serializable) for an in
depth formal discussion of Serializable. Serializability prevents Phantom Reads, which means that once a transaction
reads some value that satisfy some predicate, it will always read the same values, regardless if some other transaction
has made a write that affects the predicate. Informally this means that you can apply some total order to all
transactions.

This property holds from how Materialize satisfies [Repeatable Read](#Repeatable Read). All reads in a read only
transaction happen at the same timestamp. All writes that happen after the first read of a read-only transaction will
happen at a later timestamp, and therefore will not affect any reads of the read-only transaction.

Informally we can use the transaction timestamps to provide a total ordering over all transactions.

- Transaction T1 is ordered before Transaction T2 if the timestamp of T1 < the timestamp of T2.
- Transaction T1 is ordered before Transaction T2 if the timestamp of T1 == the timestamp of T2, T1 is write-only, and
  T2 is read-only.
- Transaction T1 can be arbitrarily ordered with Transaction T2 if the timestamp of T1 == the timestamp of T2, T1 is
  write-only, and T2 is write-only.
- Transaction T1 can be arbitrarily ordered with Transaction T2 if the timestamp of T1 == the timestamp of T2, T1 is
  read-only, and T2 is read-only.

#### Strict Serializable

See [https://jepsen.io/consistency/models/strict-serializable](https://jepsen.io/consistency/models/strict-serializable)
for an in depth formal discussion of Strict Serializable. Strict Serializable is a combination of Serializable and
Linearizable, which means that the total order formed by all transactions must be constrained by their real time
occurrences. So if transaction t1 happened in real time before transaction t2, then t1 must be ordered before t2 in the
total ordering.

Operations that only include user tables are currently Strict Serializable. This is made easy by using a single global
timestamp and a single thread in the coordinator to order operations. Writes happen at a timestamp larger than all
previous operations. Reads happen at a timestamp equal to or larger than all previous operations.

Operations that involve reading from views over upstream data are not currently Strict Serializable. It is possible to
read from a view at one timestamp and then later read from the same view at an earlier timestamp. Consider the following
scenario:

1. Client reads from view v1 that has an upper of `ts + 1` and a since of `ts - 10`. The read will happen at `ts`.
2. Client reads from a join over v1 and view v2. v2 has an upper of `ts - 5` and some since less or equal to `ts - 6`.
   The read will happen at `ts - 6`.

Reference [Timestamp Selection](#Timestamp Selection) to see why these timestamps are selected. This is an obvious
violation of strict serializability because the second read happens later than the first read in real time, but it is
assigned an earlier timestamp.

In order to achieve Strict Serializability Materialize must always select a timestamp greater than all previously
selected timestamps. This will ensure that if an operation happens later in real time, then it will be assigned a later
timestamp. Consecutive reads can all be assigned an equal timestamp as long as there’s no writes in between them. This
is made somewhat easy by the fact that all timestamp selection happens on a single thread so no coordination is needed.
On implementation for this is to consult the global timestamp for all reads, not just ones involving user tables. If
the `[since, upper)` of any object drifts ahead of the global timestamp, causing the selected timestamp to be greater
than the global timestamp, then the global timestamp can be fast-forwarded to the selected timestamp.

For the purposes of this document, the approach described above is sufficient to achieve a consistency level of Strict
Serializability. However, it introduces a performance issue that we will now discuss. Consecutive writes or read/write
cycles (read followed by write, followed by read, followed by write, etc) will increase the global timestamp in an
unbounded fashion. If the global timestamp is larger than some object’s `upper`, and a client tries to read from that
object using the global timestamp, then the read must be blocked until the object’s upper advances to the global
timestamp. The larger the difference between these two values, the larger the read must be blocked for. Below I provide
some solutions to fixing this.

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

##### Block Writes

Before performing a write that would advance the global timestamp ahead of the current wall time, we block the write
until the current wall time catches up to the global timestamp. As an optimization we can implement a form of group
commit, where all writes within the same millisecond commit together at the same timestamp.

**Pros:**

- Prevents reads from being blocked.

**Cons:**

- Limits write throughput (1 write per millisecond without group commit) or limits write latency (1 millisecond per
  write with group commit).

##### Hybrid Logical Timestamps

Note: There might be an actual name for this concept, but I chose the name above because it’s similar to Hybrid Logical
Clocks.

Currently, the timestamps used to in Materialize for `upper`, `since`, and the global timestamp are 64 bit unsigned
integers that are interpreted as an epoch in milliseconds. Instead we can change the representation of these timestamps
so that the X most significant bits are used for the epoch and the Y least significant bits are used for a counter.
Every 1 millisecond the X bits are increased by 1 and the Y bytes are all reset to 0. Every write increments the Y bits
by 1. This would allow us to have 2^Y writes per millisecond, without blocking any queries. Any write in
excess of 2^Y per millisecond must be blocked until the next millisecond.

Note: Increasing the size of the timestamp type would give more flexibility (for example to 128 bits).

**Pros:**

- With a sufficiently high Y, no queries would be blocked.

**Cons:**

- Complicated to implement and likely has a large blast radius.
- If X is low enough we will overflow our timestamps at some point in the future. This can be mitigated with a high
  enough X.

##### Write Ahead Log (WAL)

This approach involves the coordinator buffering all writes in a durable write ahead log. All writes that occur in the
same millisecond can be assigned the same timestamp, as long as there are no reads in-between them. Then at the end of
the millisecond, or after X milliseconds, all writes are sent to STORAGE in a batch. If Materialize crashes/restarts
before sending the writes to STORAGE, then it can just read the unsent write from the durable logs after starting back
up. This prevents us from having to increment the global timestamp on every write. However, this does not solve the
problem for read/write cycles.

**Pros:**

- No queries are blocked from consecutive writes.
- A WAL would be useful for many other reasons not in scope for this document. It’s likely that no matter what solution
  we pick, we’d still want to implement a WAL.

**Cons:**

- Only solves half the problem.

##### Timely Fast Forward

Timestamps in Materialize are meant to closely track the wall clock, but are not guaranteed to be exactly equal to the
wall clock. This approach would involve forcibly fast-forwarding the `upper`s of all objects. Whenever a write would
push the global timestamp above the wall clock by some threshold, all `upper`s are jumped to the global timestamp’s new
value. The threshold could be set to 0 to prevent blocking any queries.

**Pros:**

- No queries are blocked by more than the assigned threshold.

**Cons:**

- Certain writes will require a broadcast to all nodes.
- I’m not actually sure if this is possible, but if it is possible then it’s probably complicated and has a large blast
  radius.

## Alternatives

- TODO

## Open Questions

- This document does not go into great detail on fault tolerance and how crashes/restarts affect consistency. All read
  and write operations are atomic and idempotent so crashing/restarting will not leave the system in an intermediate or
  invalid state due to read/writes. However, DDLs are not idempotent or atomic. What happens if the system crashes in
  the middle of DDL? Some of this is being thought about by the command reconciliation work.
- How does the global timestamp get re-established after a restart?
