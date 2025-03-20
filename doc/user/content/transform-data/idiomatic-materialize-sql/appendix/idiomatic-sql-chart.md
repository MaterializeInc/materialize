---
title: "Idiomatic Materialize SQL chart"
description: "Cheatsheet of idiomatic Materialize SQL."
menu:
  main:
    parent: idiomatic-materialize-appendix
    weight: 10
    identifier: idiomatic-materialize-appendix-idiomatic-materialize-sql-chart
---

Materialize follows the SQL standard (SQL-92) implementation and strives for
compatibility with the PostgreSQL dialect. However, for some use cases,
Materialize provides its own idiomatic query patterns that can provide better
performance.

## General

### Query Patterns

{{% idiomatic-sql/general-syntax-table %}}

### Examples

{{% idiomatic-sql/general-example-table %}}

## Window Functions
{{< callout >}}

### Materialize and window functions

{{< idiomatic-sql/materialize-window-functions >}}

{{</ callout >}}

### Query Patterns

{{% idiomatic-sql/window-functions-syntax-table %}}

### Examples

{{% idiomatic-sql/window-functions-example-table %}}

## See also

- [SQL Functions](/sql/functions/)
- [SQL Types](/sql/types/)
- [SELECT](/sql/select/)
- [DISTINCT](/sql/select/#select-distinct)
- [DISTINCT ON](/sql/select/#select-distinct-on)
