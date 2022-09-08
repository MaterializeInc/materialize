---
title: "Isolation Level"
description: "The isolation level defines how Materialize isolates the execution of transactions."
aliases:
 - /sql/consistency
menu:
  main:
    parent: 'reference'
    weight: 160
---

The SQL standard defines four levels of transaction isolation. In order of least strict to most strict they are:

  * Read Uncommitted
  * Read Committed
  * Repeatable Read
  * Serializable. 

In Materialize, you can request any of these isolation
levels, but they all behave the same as the Serializable isolation level. In addition to the four levels defined in the
SQL Standard, Materialize also defines a [Strict Serializable](#strict-serializable) isolation level.

Isolation level is a per session configurable variable. To set the current session’s isolation level you can
execute `SET TRANSACTION_ISOLATION TO '<isolation-level>';`. The default isolation level is Strict Serializable.

## Serializable

The SQL standard defines the Serializable isolation level as preventing the following three phenomenons:

- **P1 (”Dirty Read”):**
  > SQL-transaction T1 modifies a row. SQL-transaction T2 then reads that row before T1 performs a
  COMMIT. If T1 then performs a ROLLBACK, T2 will have read a row that was never committed and that may thus be
  considered to have never existed.

- **P2 (”Non-repeatable read”):**

  > SQL-transaction T1 reads a row. SQL-transaction T2 then modifies or deletes that row and performs
  a COMMIT. If T1 then attempts to reread the row, it may receive the modified value or discover that the row has been
  deleted.

- **P3 (”Phantom”):**

  > SQL-transaction T1 reads the set of rows N that satisfy some <search condition>. SQL-transaction
  T2 then executes SQL-statements that generate one or more rows that satisfy the <search condition> used by
  SQL-transaction T1. If SQL-transaction T1 then repeats the initial read with the same <search condition>, it obtains a
  different collection of rows.

Furthermore, Serializable also guarantees that the result of executing a group of concurrent SQL-transactions produces
the same effect as some serial execution of those same transactions. A serial execution is one where each
SQL-transaction executes to completion before the next one begins. There is no guarantee that this serial ordering is
consistent with the real time ordering of the transactions, in other words transactions are not linearizable under the
Serializable isolation level. For example if SQL-transaction T1 happens before SQL-transaction T2 in real time, then the
result may be equivalent to a serial order where T2 was executed first.

In practice non-linearizable orderings are unlikely to happen, but still possible, when querying directly from tables
and sources. Non-linearizable orderings are likely to surface when querying from indexes and materialized views with
large propagation delays. For example, if SQL-transaction T1 happens before SQL-transaction T2 in real time, T1 queries
table t, and T2 queries materialized view mv where mv is an expensive materialized view including t, then T2 may not see
all the rows that were seen by T1 if they are executed close enough together in real time.

## Strict Serializable

The Strict Serializable isolation level provides all the same guarantees as Serializable, with the addition that
transactions are linearizable. That means that if SQL-transaction T1 happens before SQL-transaction T2 in real time,
then the execution is equivalent to a serial execution where T1 comes before T2.

For example, if SQL-transaction T1 happens before SQL-transaction T2 in real time, T1 queries table t, and
T2 queries materialized view mv where mv is an expensive materialized view including t, then T2 is guaranteed to see all
of the rows that were seen by T1.

It’s important to note that the linearizable guarantee only applies to transactions (including single statement SQL
queries which are implicitly single statement transactions), not to data written while ingesting from upstream sources.
So if some piece of data has been fully ingested from an upstream source, then it is not guaranteed to appear in the
next read transaction. See [real-time recency](https://github.com/MaterializeInc/materialize/issues/11531)
and [strengthening correctness](https://github.com/MaterializeInc/materialize/issues/13107) for more details. If some
piece of data has been fully ingested from an upstream source AND is included in the results of some read transaction
THEN all subsequent read transactions are guaranteed to see that piece of data.

## Choosing the Right Isolation Level

It may not be immediately clear which isolation level is right for you. Materialize recommends to start with the default
Strict Serializable isolation level. If you are noticing performance issues on reads, and your application does not need
linearizable transactions, then you should downgrade to the Serializable isolation level.

Strict Serializable provides stronger consistency guarantees but may have slower reads than Serializable. This is
because Strict Serializable may need to wait for writes to propagate through materialized views and indexes, while
Serializable does not.


## Learn more

Check out:

- [PostgreSQL documentation](https://www.postgresql.org/docs/current/transaction-iso.html) for more information on
  isolation levels.
- [Jepsen Consistency Models documentation](https://jepsen.io/consistency) for more information on consistency models.
