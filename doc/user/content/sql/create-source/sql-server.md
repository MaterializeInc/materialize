---
title: "CREATE SOURCE: SQL Server"
description: "Connecting Materialize to a SQL Server version 2016+."
menu:
  main:
    parent: 'create-source'
    name: "SQL Server"
    identifier: 'create-source-sql-server'
---

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source-v1/sql-server/)" include_blurb=true >}}

{{% create-source-intro external_source="SQL Server" version="2016+"
create_table="/sql/create-table/sql-server/" %}}

## Prerequisites

{{< include-md file="shared-content/sql-server-source-prereq.md" >}}

## Syntax

{{% include-example file="examples/create_source/example_sql_server_source"
 example="syntax" %}}

{{% include-example file="examples/create_source/example_sql_server_source"
 example="syntax-options" %}}

## Details

### Schema metadata

Materialize ingests the CDC stream for all (or a specific set of) tables in your
upstream SQL Server database that have [Change Data Capture enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server).

It's important to note that the schema metadata is captured when the source is
initially created, and is validated against the upstream schema upon restart.
If you create new tables upstream after creating a SQL Server source and want to
replicate them to Materialize, the source must be dropped and recreated.


### Monitoring source progress

By default, SQL Server sources expose progress metadata as a subsource that you
can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field     | Type                          | Details
----------|-------------------------------|--------------
`lsn`     | [`bytea`](/sql/types/bytea/)  | The upper-bound [Log Sequence Number](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-log-architecture-and-management-guide) replicated thus far into Materialize.


And can be queried using:

```mzsql
SELECT lsn
FROM <src_name>_progress;
```

The reported `lsn` should increase as Materialize consumes **new** CDC events
from the upstream SQL Server database. For more details on monitoring source
ingestion progress and debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

## Known limitations

{{% include-md file="shared-content/sql-server-considerations-v2.md" %}}

## Examples

### Prerequisites

{{< include-md file="shared-content/sql-server-source-prereq.md" >}}

### Create a source {#create-source-example}

{{% include-example file="examples/create_source/example_sql_server_source"
 example="create-source" %}}

{{% include-example file="examples/create_source/example_sql_server_source"
 example="create-table" %}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
