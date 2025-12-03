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


```mzsql
ALTER SOURCE [IF EXISTS] <name>
  ADD SUBSOURCE|TABLE <table> [AS <subsrc>] [, ...]
  [WITH (<options>)]
;
```

Syntax element | Description
---------------|------------
`<name>`       | The name of the PostgreSQL/MySQL/SQL Server source you want to alter.
`<table>`      | The upstream table to add to the source.
**AS** `<subsrc>` | Optional. The name for the subsource in Materialize.
**WITH (TEXT COLUMNS (\<col\> [, ...]))** | Optional. List of columns to decode as `text` for types that are unsupported in Materialize.

{{< note >}}
{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
{{< /note >}}

{{< /tab >}}

{{< tab "Rename" >}}

### Rename

To rename a source:

```mzsql
ALTER SOURCE <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the source you want to alter.
`<new_name>`| The new name of the source.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a source:

```mzsql
ALTER SOURCE <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the source you want to change ownership of.
`<new_owner_role>`| The new owner of the source.

To change the owner of a source, you must be the owner of the source and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a source:

```mzsql
ALTER SOURCE [IF EXISTS] <name> SET (RETAIN HISTORY [=] FOR <retention_period>);
```

To reset the retention history to the default for a source:

```mzsql
ALTER SOURCE [IF EXISTS] <name>  RESET (RETAIN HISTORY);
```

Syntax element | Description
---------------|------------
`<name>`| The name of the source you want to alter.
`<retention_period>` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

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
