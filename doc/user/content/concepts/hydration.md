---
title: "Hydration"
description: "Learn about snapshotting and hydration) in Materialize."
menu:
  main:
    parent: 'concepts'
    weight: 50
    identifier: 'concepts-hydration'
---

## Overview

Hydration is an overarching term that refers to populating
[sources](/concepts/sources/), [materialized
views](/concepts/views/#materialized-views), and [indexes](/concepts/indexes/)
in Materialize. Hydration in Materialize occurs via two specific processes, the
snapshotting process and the hydration process.

Snapshotting and hydration processes refers to the process of populating
[sources](/concepts/sources/), [materialized
views](/concepts/views/#materialized-views), and [indexes](/concepts/indexes/)
in Materialize:

- **Snapshotting**: Occurs when you create or recreate a source, and refers
  to the process of populating a source in Materialize by reading the [upstream
  (i.e., external) source's](/concepts/sources/) history and catching up to a
  chosen offset.

- **Hydration**: Occurs during cluster restarts (such as during [cluster
  resizing](/sql/alter-cluster/#resizing) or Materialize's [weekly
  upgrades](/releases/#schedule)), and refers to the process of populating
  [sources](/concepts/sources/), [materialized
  views](/concepts/views/#materialized-views), and [indexes](/concepts/indexes/)
  from persisted data in Materialize.

{{< annotation type="Disambiguation" >}}

Hydration can refer to either the overarching term or the specific process of
populating objects in Materialize. For the remainder of this page, hydration
refers to the specific process.

{{</ annotation>}}

## Snapshotting

Snapshotting occurs when you create or recreate a source. Snapshotting refers to
initial population of a source in Materialize by reading the [upstream (i.e.,
external) source](/concepts/sources/).

### Process

Materialize chooses the latest available offset upstream. For the latest
available offset, Materialize uses:

- Log Sequence Number (LSN) for [PostgreSQL](/ingest-data/postgres/) sources

- Global Transaction Identifier (GTID) for [MySQL](/ingest-data/mysql/) sources

- Kafka offset for Kafka sources

Materialize then captures/commits a complete snapshot of all historical data up
to that point. All records in this initial load have the same timestamp.

Once Materialize has committed the snapshot, the source transitions from a
**Snapshotting** status to a **Running** status, and Materialize resumes
real-time ingestion from that offset.

### Snapshotting duration and status

Generally, the snapshotting duration is proportional to data volume. However,
for sources with upsert envelope, the snapshotting may take longer while the
upsert envelope effectively dedupes your records.

You can view the snapshotting status of an object in the [Materialize
console](/console/). For an object, go to its **Overview** page in the [Database
object explorer](/console/data/).

Alternatively, you can query the following system catalog views,
[`mz_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_hydration_statuses)
and
[`mz_compute_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_compute_hydration_statuses).

### Reads during snapshotting

While a source is in a **Snapshotting** status, the source cannot serve queries,
and queries to a  **Snapshotting** status will hang. You can, however, create
views, materialized views, and indexes on the source, but these objects also
cannot serve queries until the underlying source finishes snapshotting and the
objects hydrate.

## Hydration

Hydration occurs when a cluster restarts (such as during [cluster
resizing](/sql/alter-cluster/#resizing) or Materialize's [weekly
upgrades](/releases/#schedule)).

- For sources, hydration refers to the process of reconstructing state in-memory
  (or on-disk), from the existing persisted source data.

- For materialized views and indexes, hydration refers to reconstructing
  in-memory state from persisted data.

### Process

To reconstruct the object's state, during hydration:

- Internal data structures are re-created.

- Necessary processes are initiated. These may also require re-reading of their
  required in-memory state.

### Hydration duration and status

Generally, the hydration time for an object is proportional to data volume
and query complexity.

You can view the hydration status of an object in the [Materialize
console](/console/). For an object, go to its **Workflow** page in the
[Database object explorer](/console/data/).

Alternatively, you can query the following system catalog views,
[`mz_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_hydration_statuses)
and
[`mz_compute_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_compute_hydration_statuses).

### Reads during hydration

While an object is hydrating:

- With default *strict serializable* isolation, a hydrating object cannot serve
  queries, and queries that use the hydrating objects will hang until the
  objects finish hydrating.

- With *serializable* isolation, a hydrating object can serve queries but with
  **stale** data.

### Memory considerations for hydration

During hydration:

- Materialized views require memory proportional to ~2x the output size (to
  recalculate the output from scratch and compare with existing output to ensure
  correctness).

- Sinks require memory proportional to ~1x the output size to load an entire
  snapshot of the data in memory when they start up.
