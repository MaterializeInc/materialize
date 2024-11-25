---
title: "DROP NETWORK POLICY"
description: "`DROP NETWORK POLICY` removes an existing network policy from Materialize."
menu:
  main:
    parent: commands
---

{{< private-preview />}}

`DROP NETWORK POLICY` removes an existing network policy from Materialize.
Network policies are part of Materialize's framework for [access control](/manage/access-control/).

To alter the rules in a network policy, use the [`ALTER NETWORK POLICY`](../alter-network-policy)
command.

## Syntax

{{< diagram "drop-network-policy.svg" >}}

Field         | Use
--------------|-----
**IF EXISTS** | Do not return an error if the specified network policy does not exist.
_name_        | The network policy you want to drop. For available network policies, see [`SHOW NETWORK POLICIES`](../show-network-policies).

## Privileges

The privileges required to execute this statement are:

- `CREATENETWORKPOLICY` privileges on the system.

## Related pages

- [`SHOW NETWORK POLICIES`](../show-network-policies)
- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`CREATE NETWORK POLICY`](../create-network-policy)
