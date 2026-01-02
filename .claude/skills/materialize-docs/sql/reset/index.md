---
audience: developer
canonical_url: https://materialize.com/docs/sql/reset/
complexity: intermediate
description: Reset a configuration parameter to its default value.
doc_type: reference
keywords:
- SHOW SEARCH_PATH
- RESET
product_area: Indexes
status: stable
title: RESET
---

# RESET

## Purpose
Reset a configuration parameter to its default value.

If you need to understand the syntax and options for this command, you're in the right place.


Reset a configuration parameter to its default value.


`RESET` restores the value of a configuration parameter to its default value.
This command is an alternative spelling for [`SET...TO DEFAULT`](../set).

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

This section covers syntax.

```mzsql
RESET <parameter_name>;
```text


Syntax element | Description
---------------|------------
`<parameter_name>` | The configuration parameter's name.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See configuration parameters documentation --> --> -->

## Examples

This section covers examples.

### Reset search path

```mzsql
SHOW search_path;

 search_path
-------------
 qck

RESET search_path;

SHOW search_path;

 search_path
-------------
 public
```

## Related pages

- [`SHOW`](../show)
- [`SET`](../set)