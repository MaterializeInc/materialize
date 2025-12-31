---
audience: developer
canonical_url: https://materialize.com/docs/sql/show/
complexity: beginner
description: Display the value of a configuration parameter.
doc_type: reference
keywords:
- SHOW TRANSACTION
- SHOW ACTIVE
- SHOW CLUSTER
- SHOW
- SHOW TRANSACTION_ISOLATION
product_area: Indexes
status: stable
title: SHOW
---

# SHOW

## Purpose
Display the value of a configuration parameter.

If you need to understand the syntax and options for this command, you're in the right place.


Display the value of a configuration parameter.


`SHOW` displays the value of either a specified configuration parameter or all
configuration parameters.

## Syntax

This section covers syntax.

```sql
SHOW [ <name> | ALL ];
```bash

### Aliased configuration parameters

There are a few configuration parameters that act as aliases for other
configuration parameters.

- `schema`: an alias for showing the first resolvable schema in `search_path`
- `time zone`: an alias for `timezone`

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See configuration parameters documentation --> --> -->

## Examples

This section covers examples.

### Show active cluster

```mzsql
SHOW cluster;
```text
```text
 cluster
---------
 quickstart
```bash

### Show transaction isolation level

```mzsql
SHOW transaction_isolation;
```text
```text
 transaction_isolation
-----------------------
 strict serializable
```

## Related pages

- [`RESET`](../reset)
- [`SET`](../set)