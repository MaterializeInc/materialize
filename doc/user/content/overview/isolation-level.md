---
title: "Consistency guarantees"
description: "Materialize supports different isolation levels that determine how it isolates the execution of transactions."
aliases:
 - /sql/consistency
 - /sql/isolation-level
menu:
  main:
    parent: 'advanced'
    weight: 6
---

SQL defines four levels of transaction isolation from least to most permissive:

  * Read Uncommitted
  * Read Committed
  * Repeatable Read
  * Serializable

Materialize allows requests from any isolation level and treats each request at
the Serializable level. Materialize also defines a [Strict Serializable](#strict-serializable) isolation level.


The default isolation level in Materialize is Scrit Serializable, but you can
configure isolation levels per session as a variable. 

## Syntax

{{< diagram "set-transaction-isolation.svg" >}}

| Valid Isolation Levels                      |
|---------------------------------------------|
| [Read Uncommitted](#serializable)           |
| [Read Committed](#serializable)             |
| [Repeatable Read](#serializable)            |
| [Serializable](#serializable)               |
| [Strict Serializable](#strict-serializable) |

## Examples

```sql
SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
```
```sql
SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';
```

## Serializable

The Serializable isolation level prevents the following phenomena:

- **P1 (”Dirty Read”):**
  > "SQL-transaction T1 modifies a row. SQL-transaction T2 then reads that row before T1 performs a
  COMMIT. If T1 then performs a ROLLBACK, T2 will have read a row that was never committed and that may thus be
  considered to have never existed."
  (ISO/IEC 9075-2:1999 (E) 4.32 SQL-transactions)

- **P2 (”Non-repeatable read”):**

  > "SQL-transaction T1 reads a row. SQL-transaction T2 then modifies or deletes that row and performs
  a COMMIT. If T1 then attempts to reread the row, it may receive the modified value or discover that the row has been
  deleted."
  (ISO/IEC 9075-2:1999 (E) 4.32 SQL-transactions)

- **P3 (”Phantom”):**

  > "SQL-transaction T1 reads the set of rows N that satisfy some \<search condition\>. SQL-transaction
  T2 then executes SQL-statements that generate one or more rows that satisfy the \<search condition\> used by
  SQL-transaction T1. If SQL-transaction T1 then repeats the initial read with the same \<search condition\>, it obtains a
  different collection of rows."
  (ISO/IEC 9075-2:1999 (E) 4.32 SQL-transactions)

Serializable also guarantees the result of executing a group of concurrent
SQL-transactions produces the same effect as some serial execution of the same
transactions. A serial execution is when each SQL-transaction executes to
completion before the next operation beings. These transactions are not
linearizable under the Serializable isolation level because there is no
guarantee that the serial ordering corresponds to the real-time order of the
transactions.

For example, if SQL-transaction T1 happens before T2 in real time, the result
may capture T2 first.

Non-linearizable orders occur more often when querying from indexes and
materialized views with long propagation delays. In the example above, if T1
happens before T2 in real time, T1 queries table `t`, and T2 an exensive
materialized view that includes `t`, T2 may not have access to all rows
available to T1 if executed shortly after each other in real time.

If all objects in a query cannot access a consistent snapshot, the engine will
block the query until a snapshot becaomes available. 

If a snapshot is available, the engine will execute the query immediately.
Consistent snapshots are guaranteed for queries that involve a single object.
For example, queries of a single materialized view created with multiple objects
would be considered single object and will always be executed immediately.

## Strict serializable

The Scrit Serializable isolation level provides the same guarantees as
Serializable and guarantees linearizable transactions.

For example, if SQL transaction T1 happens before T2 in real time, the
transactions execute sequentially with T1 before T2. SQL transaction T1 queries
table `t` and T2 queries an expensive materialiezd view including `t`, T2 is
still guaranteed access to all rows available to T1.

!> The linearizable guarantee only applies to transactions (including single statement SQL
queries), not to data written while ingesting from upstream sources. If an
upstream source ingests data, it is not guaranteed to appear in the next read transaction. For more information, review [real-time recency](https://github.com/MaterializeInc/materialize/issues/11531) and [strengthening correctness](https://github.com/MaterializeInc/materialize/issues/13107) for more details. If an upstream source ingests data AND that data included in the results of some read transaction
THEN all subsequent read transactions are guaranteed to see the data.

## Choosing the right isolation level

Materialzie recommends you start with the default Strict Serializable isolation
leve. If you notice performance issues on reads or your application does not
require linearizable transactions, consider downgrading to the Seiarlizable
isolation level.

Strict Serializable provides stronger consistency guarantees but may have slower reads than Serializable because Strict Serializable may need to wait for writes to propagate through materialized views and indexes, while Serializable does not.

For more information on isolation levels and how to change them, review [the]()
documentation.

## Learn more

Check out:

- [PostgreSQL documentation](https://www.postgresql.org/docs/current/transaction-iso.html) for more information on
  isolation levels.
- [Jepsen Consistency Models documentation](https://jepsen.io/consistency) for more information on consistency models.
