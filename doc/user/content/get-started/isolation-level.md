---
title: "Consistency guarantees"
description: "Materialize supports different isolation levels that determine how it isolates the execution of transactions."
aliases:
    - /sql/consistency
    - /sql/isolation-level
    - /overview/isolation-level/
---

The SQL standard defines four levels of transaction isolation. In order of least strict to most strict they are:

-   Read Uncommitted
-   Read Committed
-   Repeatable Read
-   Serializable

In Materialize, you can request any of these isolation
levels, but they all behave the same as the Serializable isolation level. In addition to the four levels defined in the
SQL Standard, Materialize also defines a [Strict Serializable](#strict-serializable) isolation level.

Isolation level is a configuration parameter that can be set by the user on a session-by-session basis. The default isolation level is
[Strict Serializable](#strict-serializable).

## Syntax

{{< diagram "set-transaction-isolation.svg" >}}

| Valid Isolation Levels                      |
| ------------------------------------------- |
| [Read Uncommitted](#serializable)           |
| [Read Committed](#serializable)             |
| [Repeatable Read](#serializable)            |
| [Serializable](#serializable)               |
| [Strict Serializable](#strict-serializable) |

## Examples

```mzsql
SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
```

```mzsql
SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';
```

## Serializable

The SQL standard defines the Serializable isolation level as preventing the following three phenomenons:

-   **P1 (”Dirty Read”):**

    > "SQL-transaction T1 modifies a row. SQL-transaction T2 then reads that row before T1 performs a
    > COMMIT. If T1 then performs a ROLLBACK, T2 will have read a row that was never committed and that may thus be
    > considered to have never existed."
    > (ISO/IEC 9075-2:1999 (E) 4.32 SQL-transactions)

-   **P2 (”Non-repeatable read”):**

    > "SQL-transaction T1 reads a row. SQL-transaction T2 then modifies or deletes that row and performs
    > a COMMIT. If T1 then attempts to reread the row, it may receive the modified value or discover that the row has been
    > deleted."
    > (ISO/IEC 9075-2:1999 (E) 4.32 SQL-transactions)

-   **P3 (”Phantom”):**

    > "SQL-transaction T1 reads the set of rows N that satisfy some \<search condition\>. SQL-transaction
    > T2 then executes SQL-statements that generate one or more rows that satisfy the \<search condition\> used by
    > SQL-transaction T1. If SQL-transaction T1 then repeats the initial read with the same \<search condition\>, it obtains a
    > different collection of rows."
    > (ISO/IEC 9075-2:1999 (E) 4.32 SQL-transactions)

Furthermore, Serializable also guarantees that the result of executing a group of concurrent SQL-transactions produces
the same effect as some serial execution of those same transactions. A serial execution is one where each
SQL-transaction executes to completion before the next one begins. There is no guarantee that this serial ordering is
consistent with the real time ordering of the transactions, in other words transactions are not
[linearizable](https://jepsen.io/consistency/models/linearizable) under the Serializable isolation level. For example
if SQL-transaction T1 happens before SQL-transaction T2 in real time, then the result may be equivalent to a serial
order where T2 was executed first.

Non-linearizable orderings are more likely to surface when querying from indexes and materialized views with
large propagation delays. For example, if SQL-transaction T1 happens before SQL-transaction T2 in real time, T1 queries
table t, and T2 queries materialized view mv where mv is an expensive materialized view including t, then T2 may not see
all the rows that were seen by T1 if they are executed close enough together in real time.

If a consistent snapshot is not available across all objects in a query and all other objects in
the current transaction, then the query will be blocked until one becomes available. On the other
hand, if a consistent snapshot is available, then the query will be executed immediately. A
consistent snapshot is guaranteed to be available for transactions that are known ahead of time to
involve a single object (which includes transactions against a single materialized view that was
created using multiple objects). Such transactions will therefore never block, and always be
executed immediately. A transaction can only be known ahead of time to involve a single object when
using auto-commit (i.e. omitting `BEGIN` and `COMMIT`) or when using `SUBSCRIBE`. When using
explicit transactions (i.e. starting a transaction with `BEGIN`) with `SELECT`, then it is assumed
that all objects that share a schema with any object mentioned in the first query of the
transaction may be used later in the transaction. Therefore, we use a consistent snapshot that is
available across all such objects.

## Strict serializable

The Strict Serializable isolation level provides all the same guarantees as Serializable, with the addition that
transactions are linearizable. That means that if SQL-transaction T1 happens before SQL-transaction T2 in real time,
then the execution is equivalent to a serial execution where T1 comes before T2.

For example, if SQL-transaction T1 happens before SQL-transaction T2 in real time, T1 queries table t, and
T2 queries materialized view mv where mv is an expensive materialized view including t, then T2 is guaranteed to see all
of the rows that were seen by T1.

It’s important to note that the linearizable guarantee only applies to transactions (including single statement SQL
queries which are implicitly single statement transactions), not to data written while ingesting from upstream sources.
So if some piece of data has been fully ingested from an upstream source, then it is not guaranteed to appear in the
next read transaction. See [real-time recency](#real-time-recency) for more details. If some
piece of data has been fully ingested from an upstream source AND is included in the results of some read transaction
THEN all subsequent read transactions are guaranteed to see that piece of data.

### Real-time recency

{{< private-preview />}}

Materialize offers a form of "end-to-end linearizability" known as real-time
recency. When using real-time recency, all client-issued `SELECT` statements
include at least all data visible to Materialize in any external source (i.e.,
sources created with `CREATE SOURCE` that use `CONNECTION`s, such as Kafka,
MySQL, and PostgreSQL sources) after Materialize receives the query. This is
what we mean by linearizable––the results are guaranteed to contain all visible
data according to physical time.

For example, real-time recency ensures that if you have just performed an
`INSERT` into a PostgreSQL database that Materialize ingests as a source, all of
your real-time recency queries will include the just-written data in their
results.

Note that real-time recency only guarantees that the results will contain _at
least_ the data visible to Materialize when we receive the query. We cannot
guarantee that the results will contain _only_ the data visible when Materialize
receives the query. For instance, the rate at which Materialize ingests data
might include additional data made visible after the timestamp we determined to
be "real-time recent." Another example is that, due to network latency, the
timestamp from the external system that we determine to be "real-time recent"
might be later (i.e. include more data) than you expected.

Because Materialize waits until it ingests the data from the external system,
real-time recency queries can have additional latency. This latency is
introduced by both the time it takes us to ingest and commit data from the
source and the time spent communicating with it to determine what data it has
made available to us (e.g., querying PostgreSQL for the replication slot's LSN).

**Details**

-   Real-time recency is only available with sessions running at the [strict
    serializable isolation level](#strict-serializable).
-   Enable this feature per session using `SET real_time_recency = true`.
-   Control the timeout for connecting to the external source to determine the
    timestamp with the `real_time_recency_timeout` session variable.
-   Real-time recency queries only guarantee visibility of data from external
    systems (e.g., sources like Kafka, MySQL, and PostgreSQL). Real-time recency
    queries do not offer any form of guarantee when querying Materialize-local
    objects, such as [`LOAD
    GENERATOR`sources](/sql/create-source/load-generator/) sources or system
    tables.
-   Each real-time recency query connects to each external source transitively
    referenced in the query. The more external sources that are referenced, the
    greater the likelihood of latency caused by the network or ingestion rates.
-   Materialize doesn't currently offer a mechanism to provide a "lower bound" on
    the data we consider to be "real-time recent" in an external source. Real-time
    recency queries return at least all data visible to Materialize when our
    client connection communicates with the external system.

## Choosing the right isolation level

It may not be immediately clear which isolation level is right for you. Materialize recommends to start with the default
Strict Serializable isolation level. If you are noticing performance issues on reads, and your application does not need
linearizable transactions, then you should downgrade to the Serializable isolation level.

Strict Serializable provides stronger consistency guarantees but may have slower reads than Serializable. This is
because Strict Serializable may need to wait for writes to propagate through materialized views and indexes, while
Serializable does not. For details about this behavior, consult the documentation
on [logical timestamp selection](/sql/functions/now_and_mz_now#logical-timestamp-selection).

In Serializable mode, a single auto-committed `SELECT` statement or a `SUBSCRIBE`
statement that references a single object (which includes transactions against a single
materialized view that was created using multiple objects) will be executed immediately. Otherwise,
the statement may block until a consistent snapshot is available. If you know you will be executing
single `SELECT` statement transactions in Serializable mode, then it is strongly
recommended to use auto-commit instead of explicit transactions.

## Learn more

Check out:

-   [PostgreSQL documentation](https://www.postgresql.org/docs/current/transaction-iso.html) for more information on
    isolation levels.
-   [Jepsen Consistency Models documentation](https://jepsen.io/consistency) for more information on consistency models.
