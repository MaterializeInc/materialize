---
title: "ALTER NETWORK POLICY (Cloud)"
description: "`ALTER NETWORK POLICY` alters an existing network policy."
menu:
  main:
    parent: commands
---

*Available for Materialize Cloud only*

`ALTER NETWORK POLICY` alters an existing network policy. Network policies are
part of Materialize's framework for [access control](/security/cloud/).

Changes to a network policy will only affect new connections
and **will not** terminate active connections.

## Syntax

```mzsql
ALTER NETWORK POLICY <name> SET (
  RULES (
    <rule_name> (action='allow', direction='ingress', address=<address>)
    [, ...]
  )
)
;
```

Syntax element | Description
---------------|------------
`<name>`       | The name of the network policy to modify.
`<rule_name>`  | The name for the network policy rule. Must be unique within the network policy.
`<address>`    | The Classless Inter-Domain Routing (CIDR) block to which the rule applies.

## Details

### Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

{{< note >}}
The default value for the `network_policy` session parameter is `default`.
Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
change the default value.
{{< /note >}}

### Lockout prevention

To prevent lockout, the IP of the active user is validated against the policy
changes requested. This prevents users from modifying network policies in a way
that could lock them out of the system.

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-network-policy.md" >}}

## Examples

```mzsql
CREATE NETWORK POLICY office_access_policy (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32')
  )
);
```

```mzsql
ALTER NETWORK POLICY office_access_policy SET (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32'),
    boston (action='allow',direction='ingress',address='4.5.6.7/32')
  )
);
```

```mzsql
ALTER SYSTEM SET network_policy = office_access_policy;
```

## Related pages
- [`CREATE NETWORK POLICY`](../create-network-policy)
- [`DROP NETWORK POLICY`](../drop-network-policy)
