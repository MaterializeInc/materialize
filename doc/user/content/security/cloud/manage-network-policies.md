---
title: "Manage network policies"
description: "Manage/configure network policies to restrict access to a Materialize region using IP-based rules."
aliases:
  - /manage/access-control/manage-network-policies/
  - /security/manage-network-policies/
menu:
  main:
    parent: "security-cloud"
    weight: 14
    name: "Manage network policies"
    identifier: "manage-network-policies"
---

{{< tip >}}
We recommend using [Terraform](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/network_policy)
to configure and manage network policies.
{{< /tip >}}

By default, Materialize is available on the public internet without any
network-layer access control. As an **administrator** of a Materialize
organization, you can configure network policies to restrict access to a
Materialize region using IP-based rules.

## Create a network policy

{{< note >}}
Network policies are applied **globally** (i.e., at the region level) and rules
can only be configured for **ingress traffic**.
{{< /note >}}

To create a new network policy, use the [`CREATE NETWORK POLICY`](/sql/create-network-policy)
statement to provide a list of rules for allowed ingress traffic.

```sql
CREATE NETWORK POLICY office_access_policy (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32')
  )
);
```

## Alter a network policy

To alter an existing network policy, use the [`ALTER NETWORK POLICY`](/sql/alter-network-policy)
statement. Changes to a network policy will only affect new connections
and **will not** terminate active connections.

```mzsql
ALTER NETWORK POLICY office_access_policy SET (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32'),
    boston (action='allow',direction='ingress',address='4.5.6.7/32')
  )
);
```

### Lockout prevention

To prevent lockout, the IP of the active user is validated against the policy
changes requested. This prevents users from modifying network policies in a way
that could lock them out of the system.

## Drop a network policy

To drop an existing network policy, use the [`DROP NETWORK POLICY`](/sql/drop-network-policy) statement.

```mzsql
DROP NETWORK POLICY office_access_policy;
```

To drop the pre-installed `default` network policy (or the network policy
subsequently set as default), you must first set a new system default using
the [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) statement.
