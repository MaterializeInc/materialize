---
title: "ALTER TABLE"
description: "`ALTER TABLE` changes properties of a table."
menu:
  main:
    parent: 'commands'
---

Use `ALTER TABLE` to:

- Rename a table.
- Change owner of a table.
- Change retain history configuration for the table.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a table:

```mzsql
ALTER TABLE <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the table you want to alter.
`<new_name>`| The new name of the table.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a table:

```mzsql
ALTER TABLE <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the table you want to change ownership of.
`<new_owner_role>`| The new owner of the table.

To change the owner of a table, you must be the owner of the table and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a user-populated table:

```mzsql
ALTER TABLE <name> SET (RETAIN HISTORY [=] FOR <retention_period>);
```

To reset the retention history to the default for a user-populated table:

```mzsql
ALTER TABLE <name> RESET (RETAIN HISTORY);
```

Syntax element | Description
---------------|------------
`<name>`| The name of the table you want to alter.
`<retention_period>` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

{{< /tab >}}
{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-table.md" >}}
