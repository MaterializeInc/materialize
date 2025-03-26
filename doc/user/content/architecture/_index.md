---
title: "Architecture"
description: "Software Architecture of Materialize"
disable_list: true
menu:
  main:
    parent: get-started
    weight: 90
    identifier: architecture
---

Materialize is divided into three *logical* component processes:

| Logical component     | Description    |
| --------------------- | -------------- |
| [Adapter](#adapter) | <ul><li>Translates user SQL into commands/instructions for [Storage](#storage) and [Compute](#compute)</li></ul>|
| [Storage](#storage) | <ul><li>Maintains [partial time-varying collection (pTVC)](#ptvcs-and-frontiers)</li><li>Provides APIs for external interactions (e.g., sources and sinks)</li></ul> |
| [Compute](#compute) | |

These are supported by two *physical* component processes:

| Physical component            | Description |
| ----------------------------- | ----------- |
| `environmentd`                | Contains all of [Adapter](#adapter) as well as part of [Compute](#compute) and [Storage](#storage) (including the controllers that maintain the durable metadata of these components)       |
| `clusterd`                    | Contains the rest of [Compute](#compute) and [Storage](#storage)  (including the operators that actually process data)          |

## Background

### pTVCs and frontiers

A ***collection*** is a set of rows along with their counts (counts can be
positive or negative). Other than the fact that a count can be negative,
collections are analogous to tables and materialized views in other database
systems.

A ***time-varying collection*** (TVC) is a collection that varies by time.
Materialize models this by a set of sequenced versions of the collection, where
each version represents the collection at a point in time. It is not possible to
physically represent most TVCs in their entirety since the set of possible
times is nearly unbounded. Instead, Materialize uses *partial time-varying
collection* (pTVC).

A ***partial time-varying collection*** (pTVC) is a TVC restricted to a
particular interval. Every pTVC is associated with a lower bound (called ***read
frontier***) and an upper bound (called ***write frontier***).

- As readers of a pTVC declare that they are no longer interested in past times,
  the read frontier advances; i.e., the read frontier advances when past version
  is no longer needed.

  {{< note >}}

  When the read frontier advances, you cannot access versions older than the
  read frontier. For example, if a record is inserted (R<sub>`t0`</sub>) and
  later updated (R<sub>`t1`</sub>), and then the read frontier moves past the
  update timestamp (`t1`), the old value of the record (R<sub>`t0`</sub>) can no
  longer be recovered.

  {{</ note >}}

- As writers declare that they have finished writing data for a given timestamp,
  the write frontier advances; i.e., the write frontier advances as new data is
  written.

pTVCs are physically represented as a stream of **diffs**. That is, instead of
storing full version of a collection for each timestamp, Materialize associates
each timestamp with the list of rows that were added or removed at that
timestamp. The key insight behind [Differential Dataflow] is that this
representation makes it possible for result sets to be **incrementally
maintained**. Materialize's compute operators translate lists of input diffs to
lists of output diffs rather than whole input relations to whole output
relations. This allows Materialize to compute updated results with effort
proportional to the sizes of the inputs **changes** and output **changes**
rather than proportional to the sizes of the inputs and outputs themselves.

### Persist

*Persist* is a library that maintains TVCs/pTVCs and is distributed across all Materialize processes; that is, across:

- [Storage](#storage) (which write data from the outside world into Persist);

- [Compute](#compute) (which read the inputs and write the outputs of
  computations); and

- [`environmentd`](#environmentd) (which uses metadata from Persist to determine
  the timestamps at which queries may validly be executed).

These processes' Persist instances store their pTVCs in S3 and maintain
consensus among themselves using a distributed transactional database.

## Logical components

### Storage

The Storage component is responsible for maintaining pTVCs, as well as providing
an API connecting them to the outside world. It is thus considered to include
both [Persist](#persist) as well as source and sinks.

[**Sources**](/concepts/sources/) handles [ingestion of data](/ingest-data/)
from external sources into Materialize. A fundamental role of sources is to make
data **definite**: any arbitrary decisions taken while ingesting data (for
example, assigning timestamps derived from the system clock to new records) must
be durably recorded in Persist so that the results of downstream computations do
not change if re-run after process restart.

[**Sinks**](/concepts/sinks/) handles emission of data to downstream systems
like Redpanda or Kafka.

Since durable relations in Materialize are represented as pTVCs maintained by
Persist, another way to describe the Storage component  is to say that it
translates between Persist's native representation and those understood by the
outside world. Storage workflows run on [clusters](#clusters-and-replicas).

### Adapter

The Adapter is responsible for the Postgres protocol termination logic, SQL
interpretation and catalog management, and timestamp selection. That is, the
Adapter is responsible for taking user SQL, translating them into instructions
that it sends to [Storage](#storage) and [Compute](#compute).

#### Postgres protocol termination

The Adapter component contains the relevant code that allows Materialize to
present itself to the network as a PostgreSQL database, enabling users to
connect from a variety of tools (such as `psql`) and libraries (such as
`psycopg`).

#### SQL interpretation and catalog management

Queries to Materialize arrive as SQL text; the Adapter parses and interprets
this SQL in order to issue instructions to [Storage](#storage) (writes) and
[Compute](#compute) (reads).

The Adapter is responsible for managing the catalog of metadata about visible
objects (e.g., tables, views, and materialized views), performing name
resolution, and translating relational queries into the internal representation
(IR) understood by [Storage](#storage) and [Compute](#compute).

#### Timestamp selection

Every one-off query in Materialize occurs at a particular logical
timestamp, and every long-running computation is valid beginning at a
particular logical timestamp.

As discussed in [Persist](#persist), durable relations are valid for a
range of timestamps, and this range is not necessarily the same for every
collection. To select a timestamp for which it will be possible to compute the
desired result, the Adapter must track the available read and write frontiers
for all collections. This task is further complicated by the requirements of our
consistency model; for example, with the default [`STRICT SERIALIZABLE`
isolation level](/get-started/isolation-level/#strict-serializable), time cannot
go backwards: a query must never return a state that occurred earlier than a
state already reflected by a previous query.

### Compute

When a user instructs Materialize to perform a computation (e.g., a one-off
`SELECT` query, a materialized view, or an in-memory index), the
[Adapter](#adapter) supplies Compute with a compiled description of the query,
consisting of: an IR(internal representation) program describing the computation
to run; a logical timestamp at which the computation should begin; and a set of
Persist identifiers for all the durable inputs and outputs of the computation.

Compute then:

- Performs a series of optimization passes to the IR program, and

- Compiles it into a [Differential Dataflow] program, which streams input data
  from [Persist](#persist) and emits the required result.

- Either:

  - Returns the results to the Adapter in the case of a one-off `SELECT` query.

  - Arranges the results in memory in the case of an
    [index](/concepts/indexes/).

  - Writes the results back to Persist in the case of a materialized view.

## Physical components

### `environmentd` and `clusterd`

There are two classes of process in a Materialize deployment:

| Process        | Description                                   |
| -------------- | ----------------------------------------------|
| `environmentd` | <a name="environmentd"></a> <ul><li>Handles control plane operations (e.g., instructing clusterd to perform various operations in response to user commands, maintaining the catalog of SQL-accessible objects, etc.)</li><li>Contains all of Adapter as well as part of Compute and Storage (in particular, the controllers that maintain the durable metadata of those components).</li></ul>|
| `clusterd`  |  <a name="clusterd"></a><ul><li>Handles data plane operations (which run in [Timely Dataflow]).</li><li>`clusterd` deployments are controlled with commands like [`CREATE CLUSTER`](/sql/create-cluster/) and [`CREATE SOURCE`](/sql/create-source/).<li>Contains parts of Compute and Storage that are not in `environmentd` (in particular, the operators that actually process data).</li>|

Furthermore, all Materialize processes run the Persist library, which handles
storing and retrieving data in a durable store.

### Clusters and replicas

`clusterd` processes are organized into [clusters](/concepts/clusters/).

A *cluster* is associated with a set of dataflow programs, which describe
either:

- [Compute](#compute) tasks (such as responding to a query, maintaining an
  in-memory index or materialized view, etc.), or

- [Storage](#storage) tasks (such as ingesting data from an external source into
  Persist or emitting data from Persist to a Kafka sink).

Each cluster is associated with zero or more
[replicas](/concepts/clusters/#fault-tolerance), which contain the actual
machines processing data. A cluster with one replica in Materialize is analogous
to an "unreplicated cluster" in other systems.

{{< note >}}

A cluster with zero replica is not associated with any machines and does not do
any work (e.g., does not ingest data, does not perform incremental updates for
indexes and materialized views, etc.)

{{</ note >}}

Each replica may, depending on its size, consist of one or more physical
machines across which indexes (both user-visible indexes and internal operator
state) are distributed. Despite the distribution, the the final results are
assembled by `environmentd` into a cohesive whole.

`environmentd` includes the compute and storage controllers that ensure that
each replica of a given cluster is always executing an identical set of dataflow
programs. For a given query, the controller simply accepts the results of
whichever replica returns first. Since Compute queries are deterministic, this
does not affect correctness. For data that is written by Compute to Persist (to
maintain a materialized view), Persist's consensus logic ensures that the data
for a given range of timestamps is only written once.

### Communication among processes

#### Direct communications

The following Materialize processes communicate directly:

- Processes within the same replica exchange data via the Timely Dataflow
  network protocol.

- The Compute and Storage controllers in `environmentd` communicate with each
  `clusterd` to issue commands and receive responses.

#### Via Persist and S3

There is no direct network communication between different clusters or between
different replicas of the same cluster. The only way for `clusterd` processes to
consume their inputs or emit their outputs is by reading them from S3 via
[Persist](#persist) or writing them in S3 via
[Persist](#persist).

That is, to share data between Compute workflows on different clusters, a user
creates a materialized view in one cluster and reads it from another; this
causes the data to be transferred via Persist and S3. A in-memory index on a
cluster is not visible to other clusters.

[Timely Dataflow]:
    https://github.com/TimelyDataflow/timely-dataflow#timely-dataflow

[Differential Dataflow]:
    https://github.com/timelydataflow/differential-dataflow#differential-dataflow
