---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-source/
complexity: intermediate
description: '`SHOW CREATE SOURCE` returns the statement used to create the source.'
doc_type: reference
keywords:
- CREATE THE
- SHOW CREATE SOURCE
- SHOW CREATE
product_area: Sources
status: stable
title: SHOW CREATE SOURCE
---

# SHOW CREATE SOURCE

## Purpose
`SHOW CREATE SOURCE` returns the statement used to create the source.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE SOURCE` returns the statement used to create the source.



`SHOW CREATE SOURCE` returns the DDL statement used to create the source.

## Syntax

This section covers syntax.

```sql
SHOW [REDACTED] CREATE SOURCE <source_name>;
```text

<!-- Dynamic table: show_create_redacted_option - see original docs -->

For available source names, see [`SHOW SOURCES`](/sql/show-sources).

## Examples

This section covers examples.

```mzsql
SHOW CREATE SOURCE market_orders_raw;
```text

```nofmt
                 name                 |                                      create_sql
--------------------------------------+--------------------------------------------------------------------------------------------------------------
 materialize.public.market_orders_raw | CREATE SOURCE "materialize"."public"."market_orders_raw" IN CLUSTER "c" FROM LOAD GENERATOR COUNTER
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the source.


## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`CREATE SOURCE`](../create-source)

