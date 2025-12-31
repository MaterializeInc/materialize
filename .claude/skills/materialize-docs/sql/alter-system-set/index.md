---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-system-set/
complexity: intermediate
description: '`ALTER SYSTEM SET` globally modifies the value of a configuration parameter.'
doc_type: reference
keywords:
- ALTER SYSTEM SET
- DEFAULT
- ALTER SYSTEM
product_area: Indexes
status: stable
title: ALTER SYSTEM SET
---

# ALTER SYSTEM SET

## Purpose
`ALTER SYSTEM SET` globally modifies the value of a configuration parameter.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER SYSTEM SET` globally modifies the value of a configuration parameter.


Use `ALTER SYSTEM SET` to globally modify the value of a configuration parameter.

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

This section covers syntax.

```mzsql
ALTER SYSTEM SET <config> [TO|=] <value|DEFAULT>
```

Syntax element | Description
---------------|------------
`<config>`              | The name of the configuration parameter to modify.
`<value>`               | The value to assign to the configuration parameter.
**DEFAULT**             | Reset the configuration parameter's default value. Equivalent to [`ALTER SYSTEM RESET`](../alter-system-reset).

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See configuration parameters documentation --> --> -->

## Privileges

The privileges required to execute this statement are:

- [_Superuser_ privileges](/security/cloud/users-service-accounts/#organization-roles)


## Related pages

- [`ALTER SYSTEM RESET`](../alter-system-reset)
- [`SHOW`](../show)