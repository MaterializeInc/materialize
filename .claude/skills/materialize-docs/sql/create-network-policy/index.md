---
audience: developer
canonical_url: https://materialize.com/docs/sql/create-network-policy/
complexity: intermediate
description: '`CREATE NETWORK POLICY` creates a network policy that restricts access
  to a Materialize Cloud region using IP-based rules.'
doc_type: reference
keywords:
- ALTER SYSTEM
- CREATE NETWORK
- DROP THIS
- CREATE NETWORK POLICY (Cloud)
- 'Note:'
product_area: Indexes
status: stable
title: CREATE NETWORK POLICY (Cloud)
---

# CREATE NETWORK POLICY (Cloud)

## Purpose
`CREATE NETWORK POLICY` creates a network policy that restricts access to a Materialize Cloud region using IP-based rules.

If you need to understand the syntax and options for this command, you're in the right place.


`CREATE NETWORK POLICY` creates a network policy that restricts access to a Materialize Cloud region using IP-based rules.



*Available for Materialize Cloud only*

`CREATE NETWORK POLICY` creates a network policy that restricts access to a
Materialize region using IP-based rules. Network policies are part of
Materialize's framework for [access control](/security/cloud/).

## Syntax

[See diagram: create-network-policy.svg]

### `network_policy_rule`

[See diagram: network-policy-rule.svg]

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------------------------
| _name_                                    | `text`           | A name for the network policy.
| `RULES`                                   | `text[]`         | A comma-separated list of network policy rules.

#### Network policy rule options

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------------------------
| _name_                                    | `text`           | A name for the network policy rule.
| `ACTION`                                  | `text`           | The action to take for this rule. `ALLOW` is the only valid option.
| `DIRECTION`                               | `text`           | The direction of traffic the rule applies to. `INGRESS` is the only valid option.
| `ADDRESS`                                 | `text`           | The Classless Inter-Domain Routing (CIDR) block the rule will be applied to.

## Details

This section covers details.

### Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

> **Note:** 
The default value for the `network_policy` session parameter is `default`.
Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
change the default value.


## Privileges

The privileges required to execute this statement are:

- `CREATENETWORKPOLICY` privileges on the system.


## Examples

This section covers examples.

```mzsql
CREATE NETWORK POLICY office_access_policy (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32')
  )
);
```text

```mzsql
ALTER SYSTEM SET network_policy = office_access_policy;
```

## Related pages

- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`DROP NETWORK POLICY`](../drop-network-policy)
- [`GRANT ROLE`](../grant-role)

