---
title: "ALTER MATERIALIZED VIEW"
description: "`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view."
menu:
  main:
    parent: 'commands'
---

`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view.

## Syntax

{{< diagram "alter-materialized-view.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the materialized view you want to alter.
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data for performing [time travel queries](/transform-data/patterns/time-travel-queries). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

## Examples

```sql
ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '1hr');
```

## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`DROP MATERIALIZED VIEW`](../drop-materialized-view)
