---
title: "DROP NETWORK POLICY (Cloud)"
description: "`DROP NETWORK POLICY` removes an existing network policy from Materialize."
menu:
  main:
    parent: commands
---

*Available for Materialize Cloud only*

`DROP NETWORK POLICY` removes an existing network policy from Materialize.
Network policies are part of Materialize's framework for [access control](/security/cloud/).

To alter the rules in a network policy, use the [`ALTER NETWORK POLICY`](../alter-network-policy)
command.

## Syntax

```mzsql
DROP NETWORK POLICY [IF EXISTS] <name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified network policy does not exist.
`<name>`        | The network policy you want to drop. For available network policies, see [`SHOW NETWORK POLICIES`](../show-network-policies).

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/drop-network-policy.md" >}}

## Related pages

- [`SHOW NETWORK POLICIES`](../show-network-policies)
- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`CREATE NETWORK POLICY`](../create-network-policy)
