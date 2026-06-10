---
title: "Idiomatic Materialize SQL"
description: "Learn about idiomatic Materialize SQL. Materialize offers various idiomatic query patterns, such as for top-k query pattern, first value/last value query paterrns, etc."
disable_list: true
menu:
  main:
    parent: transform-data
    weight: 10
    identifier: idiomatic-materialize-sql

aliases:
  - /transform-data/patterns/window-functions/
---

Materialize follows the SQL standard (SQL-92) implementation and strives for
compatibility with the PostgreSQL dialect. However, for some use cases,
Materialize provides its own idiomatic query patterns that can provide better
performance.

## Window functions

{{< yaml-table data="idiomatic_mzsql/toc_window_functions" >}}

## General query patterns

{{< yaml-table data="idiomatic_mzsql/toc_query_patterns" >}}
