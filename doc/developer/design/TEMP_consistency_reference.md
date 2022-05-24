# Consistency Reference

## Data Manipulation and Definition

The main interface Materialize provides to clients for data manipulation and data definition is SQL. SQL provides
transactions which allows a client to group multiple statements into a single atomic operation. Statements that are not
explicitly inside a transaction are inside a single statement transaction.

### Multi-statement Transactions

Materialize supports two kinds of multi-statement transactions, read-only and write-only transactions. All other
transaction types must be executed as single statement transactions.

#### Read Only Transactions

Read only transactions allow a client to read from multiple objects at a single timestamp. When a client performs the
first read of the transaction, a timestamp is selected for all subsequent reads and a read hold is taken on all objects
in the same schema for that timestamp. A read hold prevents compaction from happening on an object up to that timestamp.
When selecting the timestamp we consider all objects in the schema. A consequence of this is that read only transactions
can only read from objects that are in the same schema and that existed at the time of the first read (due to
implementation details on the read holds).

There is an additional restriction that objects on different timelines cannot currently be read in the same transaction.
Details on this are out of scope for this document but more details can be found
here: [https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210408_timelines.md](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210408_timelines.md)

#### Write-Only Transactions

Write-only transactions allow a client to perform one or more writes atomically. They currently only allow writes to a
single table. They also only allow `INSERT` statements (no `DELETE`, `UPDATE`, `CREATE`
, `INSERT INTO SELECT`, etc). All writes within a transaction are buffered in a session local variable and are not
visible to any other session. When the transaction commits, a timestamp is selected by the coordinator and all writes
are sent to STORAGE with the selected timestamp. Commits do not return an OK message to the client until all writes have
been made durable.

NOTE: This is written with the assumption that `append` STORAGE commands for a single object are idempotent, atomic, and
don’t return until they are durable. While this isn’t currently the case in Materialize, it is being actively worked on
and should be completed soon.

### Single Read/Write Statements

- `INSERT` statements behave identically to write-only transactions containing a single statement.
- `SELECT` statements behave very similarly to read-only transactions containing a single statement. A timestamp is
  selected for the read, the read is performed at that timestamp, and the results are sent back to the client. However,
  no read holds are taken and the timestamp is selected only using the objects in the query (not all objects in the
  schema like read-only transactions).
- `COPY TO` statements perform a read and then writes the results to an external source. Currently, we only
  support `COPY TO STDOUT` which behaves identically to a normal `SELECT`. In the future if we add more destinations
  then the read will be executed exactly as described in the `SELECT` bullet above, and we can exclude the write from
  this discussion because it is to an external system.
- `COPY FROM` statements read from an external source and then writes the results to a table. Currently, we only
  support `COPY FROM STDIN` which behaves identically to a normal `INSERT`. In the future if we add more sources then
  the the write will be executed exactly as described in the `INSERT` bullet above, and we can exclude the read from
  this discussion of consistency because it is from an external source.

### DDL

NOTE: Sinks are excluded from the discussion in this section.

- `CREATE` statements create new objects such as tables, sources, and views by:
    1. Updating the in-memory catalog representation.
    2. Updating on disk stash backed catalog with changes in a transaction (transactions here refer to stash implemented
       transaction, not general Materialize SQL transactions).
    3. Selecting a timestamp and sending updates to built-in tables.
    4. Sending command to STORAGE to create sources if necessary.
        1. Tables have an initial `since` matching the timestamp selected for the built-in table updates.
        2. All other objects have an initial `since` of 0 (`Timestamp::Minimum`)
    5. Sending command to COMPUTE to create dataflows if necessary.

All five steps happen on the main coordinator thread and no other operation can execute in the meantime.

- `DROP` statements remove objects such as tables, sources, and views by:
    1. Updating the in-memory catalog representation.
    2. Updating on disk stash backed catalog with changes in a transaction (transactions here refer to stash implemented
       transaction, not general Materialize SQL transactions).
    3. Selecting a timestamp and sending updates to built-in tables.
    4. Sending command to STORAGE to drop sources if necessary.

All five steps happen on the main coordinator thread and no other operation can execute in the meantime.

### Read Then Write Statements

`UPDATE`, `DELETE`, and `INSERT INTO SELECT` queries are all classified as ReadThenWrite statements, because they all
first perform a read and then perform a write. Materialize does not currently support general purpose Read/Write
transactions, but has a specific implementation for these types of statements.

The first thing a ReadThenWrite statement does is acquire a lock on the Coordinator. This lock prevents all write-only
transactions and other ReadThenWrite statements from executing until the current ReadThenWrite statement has fully
completed. The lock does block any other operation. Then a timestamp is selected and the read is performed at that
timestamp. Finally, the write behaves identically to a write-only transaction containing a single statement (a new
timestamp is selected for the write).

For more information you can read the design document for these
statements: [https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210715_table_update.md](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210715_table_update.md)

NOTE: There is active discussion on making these not require a global lock on the Coordinator to help with performance.

## Timestamp Selection

All reads and writes happen at a specific timestamp that is selected by Materialize. This section describes how
Materialize selects timestamps. All timestamp selection happens on a single Coordinator thread, so no two timestamps can
be selected concurrently. The [Strict Serializable](#Strict Serializable) section will propose some changes to achieve a
consistency level of Strict Serializable.

### Since and Upper

Every object maintains a range of timestamps, [`since`, `upper`). All reads to that object must take place at a
timestamp greater than or equal to `since`. If a timestamp larger than `upper` is selected to read an object, the read
will block until `upper` has advanced past the selected timestamp. All writes to that object must take place at a
timestamp greater than or equal to `upper`. Throughout the lifetime of the object, both `since` and `upper` can
increase, but they never decrease. This ensures that a read at timestamp `ts` will always return the same value, because
if `ts` is readable, then all future writes must take place at a timestamp greater than `ts`.

NOTE: When an object is first created the `upper` may be initialized below the `since`. This will quickly correct itself
and, as mentioned above, all reads will block until the upper has advanced.

### User Tables

The Coordinator maintains a global timestamp for all user tables. Every one second this timestamp is increased by one
second and all tables are advanced to the new timestamp (reads and writes can also increase the timestamp and/or advance
tables as described below). Advancing a table to `ts` causes its `upper` to be set to `ts`, and its `since` to be set
to `logical_compaction_window` less than `ts` (`logical_compaction_window` is a configuration variable that determines
how often to compact old data) if there are no read holds. If there is a read hold on some table, a read hold will
prevent `since` from advancing past that read hold. Once the read hold is removed `since` will advance to the next
highest read hold or `upper - logical_compaction_window` if there are no read holds.

When determining a timestamp to use to read from one or more user tables, the Coordinator uses the current value of the
global timestamp. Additionally, the Coordinator advances all tables to a timestamp larger than the timestamp selected
for the read. Multiple consecutive reads can all be done at the same timestamp, which only requires advancing the tables
on the first read.

When determining a timestamp to use to write to a user table, the Coordinator increases the global timestamp and uses
the new value. Additionally, the Coordinator advances all tables to this new timestamp.

### Upstream Sources

When determining a timestamp to use to read from a view that consists of one or more upstream sources (but no user
tables), the Coordinator chooses the maximum of:

- The largest `since` of all sources
- One unit less than the smallest `upper` of all sources.

### Combination of User Tables and Upstream Sources

When determining a timestamp to use to read from a view that consists of one or more upstream sources and one or more
user tables, the Coordinator chooses the maximum of:

- The largest `since` of all sources
- The global timestamp.

As a side effect all user tables will be advanced to a timestamp greater than the global timestamp.

### DDL

As described above timestamps are used in two places during DDL:

1. Writing to builtin tables.
2. Setting an initial `since` for table sources.

Whenever a timestamp is selected for DDL, we use the same method as determining a timestamp for a write to a user table.

## Views

This section will briefly discuss views. If a write happens on a table at timestamp `ts`, then Differential Dataflow
provides the guarantee that the write will happen on all downstream views at timestamp `ts` too. This helps us ensure
that if we write to a table at timestamp `ts` and then read from a downstream view at timestamp `ts` then we are
guaranteed to see the results of that write.

## Consistency

For details on consistency models please read: [https://jepsen.io/consistency](https://jepsen.io/consistency) and
chapters 7 and 9, of [Designing Data-Intensive Applications](https://dataintensive.net/). This document focuses on
transactional consistency levels included in the ANSI SQL spec.

### Read Uncommitted

See [https://jepsen.io/consistency/models/read-uncommitted](https://jepsen.io/consistency/models/read-uncommitted) for
an in depth formal discussion of Read Uncommitted. Read Uncommitted prevents Dirty Writes, which means that one
transaction cannot update the values of another transactions uncommitted values.

This is solved by disallowing updates in transactions and by write-only transactions buffering their writes until
commit. No update can view the buffered data until after the write-only transaction has committed. While the write-only
transaction is committing, no other queries can execute. Once the commit is complete, all writes are guaranteed to be
durable. Additionally, the timestamp is chosen for the write transaction at the time that the commit occurs (not at the
start of the transaction like read transactions).

### Read Committed

See [https://jepsen.io/consistency/models/read-committed](https://jepsen.io/consistency/models/read-committed) for an in
depth formal discussion of Read Committed. Read Committed prevents Dirty Reads, which means that one transaction cannot
view the uncommitted data of another transaction.

This property holds from how Materialize satisfies [Read Uncommitted](#Read Uncommitted). No read can view uncommitted
buffered data until after the commit is complete.

### Repeatable Read

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

### Serializable

See [https://jepsen.io/consistency/models/serializable](https://jepsen.io/consistency/models/serializable) for an in
depth formal discussion of Serializable. Serializability prevents Phantom Reads, which means that once a transaction
reads some value that satisfy some predicate, it will always read the same values, regardless if some other transaction
has made a write that affects the predicate. Additionally, this means that you can apply some total order to all
transactions.

This property holds from how Materialize satisfies [Repeatable Read](#Repeatable Read). All reads in a read only
transaction happen at the same timestamp. All writes that happen after the first read of a read-only transaction will
happen at a later timestamp, and therefore will not affect any reads of the read-only transaction.

We can use the transaction timestamps to provide a total ordering over all transactions.

- Transaction T1 is ordered before Transaction T2 if the timestamp of T1 < the timestamp of T2.
- Transaction T1 is ordered before Transaction T2 if the timestamp of T1 == the timestamp of T2, T1 is write-only, and
  T2 is read-only.
- Transaction T1 can be arbitrarily ordered with Transaction T2 if the timestamp of T1 == the timestamp of T2, T1 is
  read-only, and T2 is read-only.

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
