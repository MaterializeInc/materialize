# Postgres sources

## Summary

Many users are keen to use materialize with a dataset existing in a postgres
upstream database. The current best-practice to ingest the data in realtime is
to setup a Kafka cluster and use Debezium to convert the postgres CDC format
into Kafka topics that can then be consumed by materialize.

This design doc proposes to add a new type of source called `POSTGRES` where
materialized directly connects to an upstream database and interprets the
replication protocol, ingesting row updates in real time.

Users should be able to try materialize out simply by running the materialize
binary and pointing it to their postgres database, no extra infrastructure
needed.

## Goals

The goal is to release a way for users to connect to a postgres database,
covering most common setups and use cases. Since the configuration space is
quite large this work focuses on a small set of features and makes a few
assumptions that will get 80% of the way there.

Specifically, the goals of postgres sources are to:

* Connect to an upstream database with simple username/password authentication
* Sync the initial state of the database and seamlessly switch to streaming
* Guarantee that transaction boundaries across tables will be preserved
* Support a suite of common column data types that cover most use cases
* Materialize all synced tables by default. That is, all data must fit in RAM

## Non-Goals

In order to keep the initial scope manageable a few assumptions are made.
Lifting those assumptions will be part of the goals for future work.

* **The upstream database doesn't require complex authentication or TLS
  connections.** This constraint will be lifted shortly after the MVP. This
  will involve adding syntax (possibly WITH options) in the connector syntax and
  threading them through to the postgres driver.

* **The schema will remain static for the duration of the source.** When
  creating a postgres source a list of upstream tables will be specified either
  explicitly by the user or by introspecting the upstream database. After that
  point no upstream schema changes will be supported. If such change is
  detected, materialize will put the relevant source in an errored state and
  wait for an administrator to fix the problem. In order to lift this constraint
  materialize needs to gain support for DDLs (like ALTER TABLE). The replication
  stream includes messages with the exact points where schema changes so the
  mechanism could be based on that.

* **All synced tables will be materialized.** Normally, sources in materialized
  can be instantiated on demand and each instance talks to external systems
  independently. This is a bad pattern for an upstream postgres database because
  each instance would have to create a replication slot, which is a scarce
  resource, and would receive data for all tables, blowing up the bandwidth
  used. In order to lift this restrictions we need to implement the [Dynamic
  Resource Sharing](https://github.com/MaterializeInc/materialize/pull/6450)
  proposal.

* **All tables will have `REPLICA IDENTITY` set to `FULL`.** This will force
  postgres to stream both the old and the new row data whenever there is an
  update. While this is a bit inefficient it makes for a much simpler source
  implementation. This can be avoided with some `UPSERT`-like logic, if the
  memory overhead is acceptable Future optimization could lift this constraint.

* **no columns with TOASTed text**. TOAST is a way for postgres to store large
  amounts of text. However, these values are not present in the replication
  stream even when `REPLICA IDENTITY` is set to `FULL`. If a TOASTed value is
  encountered the source will enter an errored state. In order to lift this
  restriction we'd have to use some sort of UPSERT logic that can recover the
  current TOAST value and re-issue it in the dataflow to perform the UPDATE.

## Description

### Terminology

Logical replication involves a few different postgres objects that work together
to move data from one postgres instance to another.

1. **Logical replication**: This is a special mode of postgres streaming
   replication where data is streamed transaction by transaction in commit
   order. This is in contrast with normal replication where binary segments of
   the WAL are sent to the downstream database.
2. **Decoding plugin**: An component that runs inside the upstream database that
   is responsible for reading the raw WAL segments and reconstructing the
   logical transaction messages before sending them over to the logical
   replication receiver.
3. **pgoutput**: The specific decoder that postgres uses when logical
   replication is used. There are a lot of third-party plugins available but
   pgoutput is offered by postgres itself.
4. **PUBLICATION**: It is an object that tells the upstream database which
   tables are to be published over a replication stream. This can be an explicit
   list of table names or a special "all tables" publication that gets
   everything.
5. **replication slot**: It is an object that keeps track of how far along a
   replica is with its stream consumption. Normally the upstream database has
   one such object for each downstream replica. Each replica periodically sends
   updates with the WAL offset up to which it has consumed which then lets the
   upstream database safely clean up segments from disk.

### Replication mechanism

In order to replicate a set of tables from an upstream database a replica needs
to orchestrate two phases, making sure that there is no data loss during the
handover. The first phase involves the initial table sync which is done with
normal `COPY` bulk transfer commands. Once the replica has the initial snapshot
of a table it can switch to reading updates from a replication slot in order to
keep its table synced.

Ensuring that there is no gap between finishing the initial COPY and starting
reading from the replication slot is done by synchronizing the database snapshot
at which the `COPY` is performed with the starting point of the replication
slot. The simplest way to do this is by using the following query structure:

```sql
-- Initial sync phase
BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ
CREATE_REPLICATION_SLOT <foo> LOGICAL pgoutput USE_SNAPSHOT;
COPY `table` TO STDOUT;
COMMIT

-- Streaming phase
START_REPLICATION SLOT <foo>
```

By creating the replication slot as the first statement in a transaction and
setting the `USE_SNAPSHOT` option we ensure that the transaction and the slot
are at the exact same point. The existence of the slot will prevent any WAL
segments from being cleaned up while our replica is busy reading the initial
snapshot.

Once the initial sync is done the transaction is committed to release the
snapshot and the replica can start streaming the slot normally with no data
loss.

### Transaction boundaries and timestamps

A key design goal is to maintain the  transaction boundaries from the upstream
database so that no query on materialize can observe states that never existed
upstream. At the same time, the data ingested from postgres should be join-able
with other sources in materialize. This means that all updates must have a
timestamp that is related to the wall-clock time.

The implementation is greatly simplified by the logical replication protocol
that guarantees that we'll receive full transactions in commit order. In the
replication stream each transaction is represented by a BEGIN message, followed
by one or more row updates, and finally by a COMMIT message.

Each transaction is timestamped with the wall-clock time at the time of the
BEGIN message. This allows the source implementation to emit rows as it receives
them and ensure that transaction boundaries are guaranteed.

### Adaptation to multiple tables

If the sources naively applied the mechanism described above for every source
instance they would [very quickly
exhaust](https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-REPLICATION-SLOTS)
the available replication slots from upstream and also multiply the bandwidth
used by several times.

In order to have one independent `SOURCE` object for each upstream table while
also maintaining a single replication connection between them a new SQL syntax
will be introduced that allows creating multiple sources at once.

Specifically, a user should be able to do `CREATE SOURCES FROM POSTGRES..` and
have one `SOURCE` object created for each upstream table. By introducing this
syntax the SQL planner will have an opportunity to create all the relevant
sources at once and therefore give them some shared state. The shared state
could be some form of `Arc<RwLock<Option<T>>>` where the first source instance
that grabs the write lock will initialize it and afterwards all the source
instances will keep a read lock around to share the stream.

The concrete syntax for `CREATE SOURCES` can be seen in
https://github.com/MaterializeInc/materialize/pull/6299.

### Testing strategy

Testing this source properly will require simulating various network failures.
At the time of writing there are existing happy path tests using mzcompose but
all of them run on the stable network provided by docker-compose.

In order to simulate network failures an intermediary container will be used
that can manipulate the network on command. A good candidate for that is
Spotify's [toxiproxy](https://github.com/Shopify/toxiproxy). Using mzcompose we
can setup materialized to connect to postgres through toxiproxy which will be
controlled using its HTTP API.

In a parallel effort, support for issuing `curl` commands directly from
testdrive [is being worked
on](https://github.com/MaterializeInc/materialize/pull/6508). Together, these
two will allow us to run testdrive tests that simulate flaky networks,
connections drops, slow connections, etc.

## Open questions

### Initial sync resume logic

A strategy for handling connection disconnects is missing for the initial
syncing phase. This isn't a huge problem as this only happens once at start up
but having a solution would make the system more robust.

When postgres itself encounters a disconnection as a read replica during the
initial sync phase it can just throw away its intermediate results and start
from the beginning. This however is not something we can do once we have issued
data into the dataflow. Differential collections have no "truncate" operation
and the only way to remove data from them is to issue the corresponding
retractions.

A naive implementation of resumption during initial sync would be to buffer all
the rows of a table outside of the dataflow (e.g in a `Vec<Row>`) and issue them
all at once when we are certain that no interruption occured. In the case of a
disconnection the Vec can be discarded and start from the beginning.
