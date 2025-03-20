---
title: "ALTER TABLE"
description: "`ALTER TABLE` changes the parameters of a table."
menu:
  main:
    parent: 'commands'
---

`ALTER TABLE` changes the parameters of a table.

## Syntax

{{< diagram "alter-table-set.svg" >}}
{{< diagram "alter-table-reset.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the table you want to alter.
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

## Details
