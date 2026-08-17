---
title: Sources
description: Learn about sources in Materialize.
menu:
  main:
    parent: concepts
    weight: 10
    identifier: 'concepts-sources'
aliases:
  - /get-started/key-concepts/#sources
  - /self-managed/v25.2/concepts/sources/
---

## Overview

{{% include-headless "/headless/source-definition" %}}

## Supported external systems

Materialize supports ingesting data from the following external systems:

{{% include-headless "/headless/ingest-connectors-table" %}}

## Creating a source

### Prerequisites

{{% include-headless "/headless/source-upstream-prereq" %}}

### CREATE SOURCE syntax

To create a source, you use the [`CREATE SOURCE`](/sql/create-source/) syntax.
There are two versions of the syntax:

- *Recommended.* The new [`CREATE SOURCE`](/sql/create-source/#new-syntax)
  syntax, used with [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/). The
  new syntax allows Materialize to handle certain upstream schema changes,
  specifically adding or dropping columns, **without** downtime.

- The legacy [`CREATE SOURCE ... FOR <ALL
  TABLES|TABLES|SCHEMAS>`](/sql/create-source/#legacy-syntax) syntax, which
  creates a source and its subsources. *Subsource* is the legacy term for the
  read-only tables created from a source. With the legacy `CREATE SOURCE ...
  FOR ...` syntax, the subsources are automatically created when the `CREATE
  SOURCE ...` command is issued.

### Tables and subsources

A source makes external data available in Materialize through:

- The [tables](/sql/create-table/) created from it, when using the new
  `CREATE SOURCE` syntax.

- The subsources, when using the legacy `CREATE SOURCE` syntax.

Both the tables and subsources created from a source are **read-only**.
Materialize populates them by ingesting changes from the upstream system, and
you cannot insert, update, or delete their data directly.

## Snapshotting

When you create a table from a source (or, with the legacy syntax, when the
subsources are created), Materialize [snapshots](/concepts/snapshotting/) the
data currently available in the upstream system for that table.

{{% include-headless "/headless/ingestion/snapshotting-queries" %}}

See [Snapshotting](/concepts/snapshotting/) for more information.

## Hydration

{{% include-from-yaml data="hydration-details" name="definition" %}}

- For Kafka upsert sources, their associated read-only tables (or subsources if
  using the legacy syntax) rebuild their internal upsert index from storage on
  replica (re)start or cluster resize.

- For other sources, the hydration process is negligible or not applicable.

See [Hydration](/concepts/hydration/) for more information.

## Sources and clusters

Sources require compute resources in Materialize. That is, sources must be
associated with a [cluster](/concepts/clusters/). If possible, dedicate a
cluster just for sources.

See also [Operational guidelines](/manage/operational-guidelines/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE TABLE`](/sql/create-table)
- [Snapshotting](/concepts/snapshotting/)
- [Hydration](/concepts/hydration/)
