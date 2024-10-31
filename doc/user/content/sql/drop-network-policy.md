---
title: "DROP NETWORK POLICY"
description: "`DROP NETWORK POLICY` removes a network policy from Materialize."
menu:
  main:
    parent: commands
---

{{< warn-if-unreleased-inline "v0.123.0" >}}
{{< public-preview />}}

`DROP NETWORK POLICY` removes a network policy from Materialize.

## Syntax

{{< diagram "drop-network-policy.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified network policy does not exist.
_network_policy_name_ | The network policy you want to drop.

## Details

You cannot drop the system `network_policy`.

## Privileges

The privileges required to execute this statement are:

- `CREATENETWORKPOLICY` privileges on the system.

## Related pages

- [ALTER NETWORK POLICY](../alter-network-policy)
- [CREATE NETWORK POLICY](../create-network-policy)
