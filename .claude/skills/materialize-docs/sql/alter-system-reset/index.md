---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-system-reset/
complexity: intermediate
description: Globally reset a configuration parameter to its default value.
doc_type: reference
keywords:
- ALTER SYSTEM RESET
- ALTER SYSTEM
product_area: Indexes
status: stable
title: ALTER SYSTEM RESET
---

# ALTER SYSTEM RESET

## Purpose
Globally reset a configuration parameter to its default value.

If you need to understand the syntax and options for this command, you're in the right place.


Globally reset a configuration parameter to its default value.


Use `ALTER SYSTEM RESET` to globally restore the value of a configuration
parameter to its default value. This command is an alternative spelling for
[`ALTER SYSTEM SET...TO DEFAULT`](../alter-system-set).

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

This section covers syntax.

```mzsql
ALTER SYSTEM RESET <config>;
```

Syntax element | Description
---------------|------------
`<config>`     | The configuration parameter's name.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See configuration parameters documentation --> --> -->

## Privileges

The privileges required to execute this statement are:

- [_Superuser_ privileges](/security/cloud/users-service-accounts/#organization-roles)


## Related pages

- [`SHOW`](../show)
- [`ALTER SYSTEM SET`](../alter-system-set)