---
title: "ALTER MATERIALIZED VIEW"
description: "`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view."
menu:
  main:
    parent: 'commands'
---

`ALTER  MATERIALIZED VIEW` changes the parameters of a materialized view.

## Syntax

{{< diagram "alter-materialized-view-set.svg" >}}
{{< diagram "alter-materialized-view-reset.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the materialized view you want to alter.
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

## Details

## Privileges

The privileges required to execute this statement are:

- Ownership of the materialized view.

## Related pages

- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)
