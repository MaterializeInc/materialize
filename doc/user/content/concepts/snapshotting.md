---
title: Snapshotting
description: "Learn about snapshotting in Materialize: the initial sync of a source's data from an upstream system."
menu:
  main:
    parent: concepts
    weight: 40
    identifier: 'concepts-snapshotting'
---

{{% include-headless "/headless/ingestion/snapshotting-definition" %}}

## When snapshotting occurs

{{% include-headless "/headless/ingestion/snapshotting-occurrence" %}}

## Snapshot duration

{{% include-headless "/headless/ingestion/snapshotting-duration" %}}

### Parallelism

{{% include-headless "/headless/ingestion/snapshotting-parallelism" %}}

## Queries during snapshotting

{{% include-headless "/headless/ingestion/snapshotting-queries" %}}

## Impact on upstream system

Snapshotting has the following upstream impacts:

- **Read load.** Snapshotting puts read, CPU, and network load on the upstream
  system. The total load is proportional to the volume of data being
  snapshotted, while the source cluster's [parallelism](#parallelism) affects
  the peak load: more workers compress the reads into a shorter window.

- **Change-log retention for CDC database sources.** When ingesting data from
  CDC database sources (PostgreSQL, MySQL, SQL Server), the upstream system must
  retain its change-log data until Materialize consumes it. During the initial
  snapshot, changes accumulate from the source's starting position until the
  snapshot completes and Materialize has consumed the accumulated changes. A
  stalled or long-running snapshot can therefore increase disk usage on the
  upstream database.

## Related pages

- [Ingest data](/ingest-data/)
- [Sources](/concepts/sources/)
- [Troubleshooting data ingestion](/ingest-data/troubleshooting/)
