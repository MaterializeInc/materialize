---
title: "SHOW CREATE SOURCE"
description: "`SHOW CREATE SOURCE` returns the statement used to create the source."
menu:
  main:
    parent: commands
---

`SHOW CREATE SOURCE` returns the DDL statement used to create the source.

## Syntax

{{< diagram "show-create-source.svg" >}}

Field | Use
------|-----
_source&lowbar;name_ | The source you want use. You can find available source names through [`SHOW SOURCES`](../show-sources).

## Examples

```sql
SHOW CREATE SOURCE my_source;
```

```nofmt
    Source   |        Create Source
-------------+--------------------------
 my_source   | CREATE SOURCE "materialize"."public"."market_orders_raw" FROM LOAD GENERATOR counter;
```

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`CREATE SOURCE`](../create-source)
