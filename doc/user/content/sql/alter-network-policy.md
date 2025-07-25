---
title: "ALTER NETWORK POLICY"
description: "`ALTER NETWORK POLICY` alters an existing network policy."
menu:
  main:
    parent: commands
---

`ALTER NETWORK POLICY` alters an existing network policy. Network policies are
part of Materialize's framework for [access control](/manage/access-control/).

Changes to a network policy will only affect new connections
and **will not** terminate active connections.

## Syntax

{{< diagram "alter-network-policy.svg" >}}

### `network_policy_rule`

{{< diagram "network-policy-rule.svg" >}}

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------------------------
| _name_                                    | `text`           | A name for the Network Policy.
| `RULES`                                   | `text[]`         | A comma-separated list of Network Policy Rules.

#### Network policy rule options

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------------------------
| _name_                                    | `text`           | A name for the network policy rule. Must be unique within the network policy.
| `ACTION`                                  | `text`           | The action to take for this rule. `ALLOW` is the only valid option.
| `DIRECTION`                               | `text`           | The direction of traffic the rule applies to. `INGRESS` is the only valid option.
| `ADDRESS`                                 | `text`           | The Classless Inter-Domain Routing (CIDR) block the rule will be applied to.

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
