---
title: "ALTER SOURCE"
description: "`ALTER SOURCE` changes certain characteristics of a source."
menu:
  main:
    parent: 'commands'
---

Use `ALTER SOURCE` to:

- Add a subsource to a source.
- Rename a source.
- Change owner of a source.
- Change retain history configuration for the source.

## Syntax

{{< tabs >}}
{{< tab "Add subsource" >}}

### Add subsource

To add the specified upstream table(s) to the specified PostgreSQL/MySQL/SQL Server source:

{{% include-syntax file="examples/alter_source" example="syntax-add-subsource" %}}

{{< note >}}
{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
{{< /note >}}

{{< /tab >}}

{{< tab "Rename" >}}

### Rename

To rename a source:

{{% include-syntax file="examples/alter_source" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a source:

{{% include-syntax file="examples/alter_source" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a source:

{{% include-syntax file="examples/alter_source" example="syntax-set-retain-history" %}}

To reset the retention history to the default for a source:

{{% include-syntax file="examples/alter_source" example="syntax-reset-retain-history" %}}

{{< /tab >}}
{{< /tabs >}}


## Context

### Adding subsources to a PostgreSQL/MySQL/SQL Server source

Note that using a combination of dropping and adding subsources lets you change
the schema of the PostgreSQL/MySQL/SQL Server tables that are ingested.

{{< important >}}
{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
{{< /important >}}

### Dropping subsources from a PostgreSQL/MySQL/SQL Server source

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table
(such as its contents).

If a subsource encounters a deterministic error, such as an incompatible schema
change (e.g. dropping an ingested column), you can drop the subsource. If you
want to ingest it with its new schema, you can then add it as a new subsource.

You cannot drop the "progress subsource".

## Examples

### Adding subsources

```mzsql
ALTER SOURCE pg_src ADD SUBSOURCE tbl_a, tbl_b AS b WITH (TEXT COLUMNS [tbl_a.col]);
```

{{< important >}}
{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
{{< /important >}}

### Dropping subsources

To drop a subsource, use the [`DROP SOURCE`](/sql/drop-source/) command:

```mzsql
DROP SOURCE tbl_a, b CASCADE;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-source.md" >}}

## See also

- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP SOURCE`](/sql/drop-source/)
- [`SHOW SOURCES`](/sql/show-sources)
