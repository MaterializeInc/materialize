---
title: "Isolation levels"
description: "Materialize supports different isolation levels that determine how it isolates the execution of transactions."
aliases:
    - /sql/consistency
    - /sql/isolation-level
    - /overview/isolation-level/
    - /get-started/isolation-level/
menu:
  main:
    parent: reference
    identifier: 'isolation-levels'
    weight: 150
---

An *isolation level* determines which effects of concurrent transactions are
visible to a transaction during its execution.

## Supported isolation levels

Materialize accepts the following isolation levels:

| Isolation level | Behavior in Materialize |
| --- | --- |
| [**Strict Serializable**](#strict-serializable) | **Default.** Provides serializability and linearizability. |
| [**Serializable**](#serializable) | Provides serializability but not linearizability. |
| Read Uncommitted, Read Committed, Repeatable Read | Accepted for compatibility; treated as Serializable. |

## Serializable

Serializable prevents the following three phenomena[^1]:

| Phenomenon | Description |
| --- | --- |
| **P1 (Dirty read)** | A transaction **T1** modifies a row; another transaction **T2** reads the row before **T1** commits. If **T1** rolls back, **T2** has read a row that was never committed. |
| **P2 (Non-repeatable read)** | A transaction **T1** reads a row; another transaction **T2** modifies or deletes that row and commits. If **T1** attempts to reread the row, it may see the modified value or discover that the row no longer exists.|
| **P3 (Phantom)** | A transaction **T1** reads a set of rows that match a specific search condition; another transaction **T2** inserts rows that also match the condition and commits. If **T1** repeats the read with the same search condition, it gets a different set of rows. |

[^1]: Phenomenon descriptions adapted from ISO/IEC 9075-2:1999 (E), §4.32
    "SQL-transactions."

Serializable also guarantees that the result of concurrently executing
transactions is equivalent to some serial execution of those transactions. A
serial execution is one in which each transaction completes before the next one
begins. However, Serializable does not guarantee
[linearizability](https://jepsen.io/consistency/models/linearizable); that is,
it does not guarantee that the serial order matches the real-time order of the
transactions. For example, if transaction **T1** completes before transaction
**T2** begins in real time, the result may be equivalent to a serial execution
in which **T2** executes before **T1**.

Non-linearizable orderings are more likely to occur when querying indexes and
materialized views with large propagation delays. For example, suppose
transaction **T1** queries table `t` and is followed in real time by transaction
**T2**, which queries a computationally expensive materialized view `mv` defined
over `t`. If the two transactions execute sufficiently close together, `mv` may
not yet reflect the latest updates to `t` that **T1** observed, so **T2** may
not observe all rows visible to **T1**.

### Logical timestamp selection {#serializable-logical-timestamp-selection}

{{% include-headless "/headless/logical-timestamp-selection-serializable" %}}

## Strict Serializable

Strict Serializable provides all the guarantees of Serializable isolation and
additionally guarantees
[linearizability](https://jepsen.io/consistency/models/linearizable). With
linearizability, the serial order matches the real-time order of the
transactions. For example, if transaction **T1** completes before transaction
**T2** begins in real time, the result is equivalent to a serial execution in
which **T1** executes before **T2**.

More concretely, suppose transaction **T1** queries table `t` and is followed in
real time by transaction **T2**, which queries a computationally expensive
materialized view `mv` defined over `t`. Under Strict Serializable, **T2** is
guaranteed to observe all rows visible to **T1**.

The linearizable guarantee applies only to transactions (including single-statement SQL queries, which are implicitly single-statement transactions), not to data written while ingesting from upstream sources.

- If a piece of data has been fully ingested from an upstream source, it is not
  guaranteed to appear in the next read transaction. See [real-time
  recency](#real-time-recency) for more details.

- However, once that data is included in the results of a read transaction, all
  subsequent read transactions are guaranteed to see it.

### Logical timestamp selection {#strict-serializable-logical-timestamp-selection}

{{% include-headless "/headless/logical-timestamp-selection-strict-serializable"
%}}

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
    objects, such as [`LOAD GENERATOR`](/sql/create-source/load-generator/)
    sources or system tables.
-   Each real-time recency query connects to each external source transitively
    referenced in the query. The more external sources that are referenced, the
    greater the likelihood of latency caused by the network or ingestion rates.
-   Materialize doesn't currently offer a mechanism to provide a "lower bound"
    on the data we consider to be "real-time recent" in an external source.
    Real-time recency queries return at least all data visible to Materialize
    when our client connection communicates with the external system.

## Isolation levels and query latency

Strict Serializable provides stronger consistency guarantees but may have slower
reads than Serializable.

- **Strict Serializable** (the default) may need to wait for recent writes to
  propagate through materialized views and indexes before serving a read, so
  that the read reflects the real-time order of transactions.

  - [Real-time recency](#real-time-recency) (available only with Strict
    Serializable) introduces additional latency, since Materialize waits to
    determine and ingest the latest data available in upstream sources before
    serving the query.

- **Serializable** does not wait for writes to propagate. It reads a consistent
  (but possibly slightly stale) snapshot, which avoids that latency at the cost
  of linearizability. However, if a consistent snapshot is not available, the
  query blocks until one becomes available.

## Setting isolation level

{{< tip >}}
Materialize recommends starting with the default Strict Serializable isolation
level.
{{< /tip >}}

You can set the isolation level using the session-level [configuration parameter
`TRANSACTION_ISOLATION`](/sql/set/); for example:

```mzsql
SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';
```

You can also set the isolation level for an explicit transaction block as part
of the [`BEGIN` statement](/sql/begin); for example:

```mzsql
BEGIN ISOLATION LEVEL STRICT SERIALIZABLE;
--- ...
--- ...
--- ...
COMMIT;
```

## Learn more

Check out:

-   [PostgreSQL documentation](https://www.postgresql.org/docs/current/transaction-iso.html) for more information on
    isolation levels.
-   [Jepsen Consistency Models documentation](https://jepsen.io/consistency) for more information on consistency models.
