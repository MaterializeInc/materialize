---
title: "ALTER MATERIALIZED VIEW"
description: "`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view."
menu:
  main:
    parent: 'commands'
---

Use `ALTER  MATERIALIZED VIEW` to:

- Rename a materialized view.
- Change owner of a materialized view.
- Change retain history configuration for the materialized view.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a materialized view:

```mzsql
ALTER MATERIALIZED VIEW <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the materialized view you want to alter.
`<new_name>`| The new name of the materialized view.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a materialized view:

```mzsql
ALTER MATERIALIZED VIEW <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the materialized view you want to change ownership of.
`<new_owner_role>`| The new owner of the materialized view.

To change the owner of a materialized view, you must be the owner of the materialized view and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a materialized view:

```mzsql
ALTER MATERIALIZED VIEW <name> SET (RETAIN HISTORY [=] FOR <retention_period>);
```

To reset the retention history to the default for a materialized view:

```mzsql
ALTER MATERIALIZED VIEW <name> RESET (RETAIN HISTORY);
```

Syntax element | Description
---------------|------------
`<name>`| The name of the materialized view you want to alter.
`<retention_period>` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

{{< /tab >}}
{{< /tabs >}}

## Details

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-materialized-view.md" >}}

## Related pages

- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)
