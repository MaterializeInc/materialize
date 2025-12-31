---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-network-policy/
complexity: intermediate
description: '`DROP NETWORK POLICY` removes an existing network policy from Materialize.'
doc_type: reference
keywords:
- DROP NETWORK POLICY (Cloud)
- ALTER NETWORK
- IF EXISTS
- DROP NETWORK
- ALTER THE
product_area: Indexes
status: stable
title: DROP NETWORK POLICY (Cloud)
---

# DROP NETWORK POLICY (Cloud)

## Purpose
`DROP NETWORK POLICY` removes an existing network policy from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP NETWORK POLICY` removes an existing network policy from Materialize.



*Available for Materialize Cloud only*

`DROP NETWORK POLICY` removes an existing network policy from Materialize.
Network policies are part of Materialize's framework for [access control](/security/cloud/).

To alter the rules in a network policy, use the [`ALTER NETWORK POLICY`](../alter-network-policy)
command.

## Syntax

This section covers syntax.

```mzsql
DROP NETWORK POLICY [IF EXISTS] <name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified network policy does not exist.
`<name>`        | The network policy you want to drop. For available network policies, see [`SHOW NETWORK POLICIES`](../show-network-policies).

## Privileges

The privileges required to execute this statement are:

- `CREATENETWORKPOLICY` privileges on the system.


## Related pages

- [`SHOW NETWORK POLICIES`](../show-network-policies)
- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`CREATE NETWORK POLICY`](../create-network-policy)

