---
title: "ALTER SOURCE"
description: "`ALTER SOURCE` changes certain characteristics of a source."
menu:
  main:
    parent: 'commands'
---

Use `ALTER SOURCE` to:

- Add a subsource to a source.
- Refresh the upstream references available to a source.
- Rename a source.
- Change owner of a source.
- Change retain history configuration for the source.
- Change timestamp interval for the source.

## Syntax

{{< tabs level=3 >}}
{{< tab "Add subsource" >}}

To add the specified upstream table(s) to the specified PostgreSQL/MySQL/SQL Server source:

{{% include-syntax file="examples/alter_source" example="syntax-add-subsource" %}}

{{< note >}}
{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
{{< /note >}}

{{< /tab >}}

{{< tab "Refresh references" >}}

To refresh the list of upstream objects available to a source:

{{% include-syntax file="examples/alter_source" example="syntax-refresh-references" %}}

{{< /tab >}}

{{< tab "Rename" >}}

To rename a source:

{{% include-syntax file="examples/alter_source" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

To change the owner of a source:

{{% include-syntax file="examples/alter_source" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

To set the retention history for a source:

{{% include-syntax file="examples/alter_source" example="syntax-set-retain-history" %}}

To reset the retention history to the default for a source:

{{% include-syntax file="examples/alter_source" example="syntax-reset-retain-history" %}}

{{< /tab >}}
{{< tab "(Re)Set timestamp interval" >}}

To set the timestamp interval for a source:

{{% include-syntax file="examples/alter_source" example="syntax-set-timestamp-interval" %}}

To reset the timestamp interval to the system default for a source:

{{% include-syntax file="examples/alter_source" example="syntax-reset-timestamp-interval" %}}

{{< /tab >}}
{{< /tabs >}}


## Context

### Adding subsources to a PostgreSQL/MySQL/SQL Server source

Note that using a combination of dropping and adding subsources lets you change
the schema of the PostgreSQL/MySQL/SQL Server tables that are ingested.

{{< important >}}
{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
{{< /important >}}

### Dropping subsources from a PostgreSQL/MySQL/SQL Server source

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table
(such as its contents).

If a subsource encounters a deterministic error, such as an incompatible schema
change (e.g. dropping an ingested column), you can drop the subsource. If you
want to ingest it with its new schema, you can then add it as a new subsource.

You cannot drop the "progress subsource".

### Refreshing available upstream references

When you create a source, Materialize records the objects that source could read
in `mz_internal.mz_source_references`. For a PostgreSQL, MySQL, or SQL Server
source, that list comes from querying the upstream database. Either way the list
is a snapshot taken at creation time, and Materialize does not update it as the
upstream changes. A table added to a PostgreSQL publication after the source was
created, for example, does not show up there.

`ALTER SOURCE ... REFRESH REFERENCES` recomputes that list and replaces the
recorded references for the source. Objects that have appeared since the last
refresh are added, and objects that no longer exist are removed.

Refreshing references only updates this metadata. It neither starts nor stops
ingesting anything. To ingest a newly available object, create a table from the
source with [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/); to
stop ingesting one, drop the corresponding table.

The statement is accepted for any source that ingests from an external system,
but what it recomputes depends on the source type:

| Source type | Effect of a refresh |
| --- | --- |
| PostgreSQL, MySQL, SQL Server | Queries the upstream database for the tables the source can read. |
| Kafka | No practical effect. The only reference is the topic the source was configured with. |
| Load generator | Re-reads the load generator's built-in views, which change only when a Materialize upgrade adds views. |

[Webhook sources](/sql/create-source/webhook/), which are written to rather than
read from, return an error.

For PostgreSQL, MySQL, and SQL Server sources, the refresh connects to the
upstream database, so it fails if that database is unreachable or the source's
[connection](/sql/create-connection/) is no longer valid. For PostgreSQL
sources, it also fails if the source's publication is empty.

## Examples

### Adding subsources

```mzsql
ALTER SOURCE pg_src ADD SUBSOURCE tbl_a, tbl_b AS b WITH (TEXT COLUMNS [tbl_a.col]);
```

{{< important >}}
{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
{{< /important >}}

### Dropping subsources

To drop a subsource, use the [`DROP SOURCE`](/sql/drop-source/) command:

```mzsql
DROP SOURCE tbl_a, b CASCADE;
```

### Refreshing references

To refresh the upstream objects Materialize records for a source:

```mzsql
ALTER SOURCE pg_src REFRESH REFERENCES;
```

To then inspect the refreshed references:

```mzsql
SELECT refs.namespace, refs.name, refs.columns, refs.updated_at
FROM mz_internal.mz_source_references refs, mz_sources s
WHERE s.name = 'pg_src'
AND refs.source_id = s.id;
```

### Changing the timestamp interval

To set a custom timestamp interval for a source:

```mzsql
ALTER SOURCE kafka_src SET (TIMESTAMP INTERVAL = '500ms');
```

To reset the timestamp interval to the system default:

```mzsql
ALTER SOURCE kafka_src RESET (TIMESTAMP INTERVAL);
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/alter-source" %}}

## See also

- [`CREATE SOURCE`](/sql/create-source/)
- [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/)
- [`DROP SOURCE`](/sql/drop-source/)
- [`SHOW SOURCES`](/sql/show-sources)
