---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-network-policy/
complexity: intermediate
description: '`ALTER NETWORK POLICY` alters an existing network policy.'
doc_type: reference
keywords:
- ALTER NETWORK
- ALTER NETWORK POLICY (Cloud)
- ALTER SYSTEM
- DROP THIS
- will not
- 'Note:'
product_area: Indexes
status: stable
title: ALTER NETWORK POLICY (Cloud)
---

# ALTER NETWORK POLICY (Cloud)

## Purpose
`ALTER NETWORK POLICY` alters an existing network policy.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER NETWORK POLICY` alters an existing network policy.



*Available for Materialize Cloud only*

`ALTER NETWORK POLICY` alters an existing network policy. Network policies are
part of Materialize's framework for [access control](/security/cloud/).

Changes to a network policy will only affect new connections
and **will not** terminate active connections.

## Syntax

<!-- Syntax example: examples/alter_network_policy / syntax -->

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


### Lockout prevention

To prevent lockout, the IP of the active user is validated against the policy
changes requested. This prevents users from modifying network policies in a way
that could lock them out of the system.

## Privileges

The privileges required to execute this statement are:

- Ownership of the network policy.


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
ALTER NETWORK POLICY office_access_policy SET (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32'),
    boston (action='allow',direction='ingress',address='4.5.6.7/32')
  )
);
```text

```mzsql
ALTER SYSTEM SET network_policy = office_access_policy;
```

## Related pages
- [`CREATE NETWORK POLICY`](../create-network-policy)
- [`DROP NETWORK POLICY`](../drop-network-policy)

