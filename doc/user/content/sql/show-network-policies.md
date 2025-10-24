---
title: "SHOW NETWORK POLICIES"
description: "`SHOW NETWORK POLICIES` returns a list of all network policies configured in Materialize."
menu:
  main:
    parent: commands
---

`SHOW NETWORK POLICIES` returns a list of all network policies configured in
Materialize. Network policies are part of Materialize's framework for
[access control](/security/access-control/).

## Syntax

```mzsql
SHOW NETWORK POLICIES [ LIKE <pattern> ]
```

Option                     | Description
---------------------------|------------
**LIKE** \<pattern\>       | If specified, only show network policies whose name matches the pattern.

## Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

{{< note >}}
The default value for the `network_policy` session parameter is `default`.
Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
change the default value.
{{< /note >}}

## Examples

```mzsql
SHOW NETWORK POLICIES;
```
```nofmt
| name                 | rules              | comment |
| -------------------- | ------------------ | ------- |
| default              | open_ingress       |         |
| office_access_policy | minnesota,new_york |         |
```

To see details for each rule in a network policy, you can query the
[`mz_internal.mz_network_policy_rules`](/sql/system-catalog/mz_internal/#mz_network_policy_rules)
system catalog table.

```mzsql
SELECT * FROM mz_internal.mz_network_policy_rules;
```
```nofmt
| name         | policy_id | action | address    | direction |
| ------------ | --------- | ------ | ---------- | --------- |
| new_york     | u3        | allow  | 1.2.3.4/28 | ingress   |
| minnesota    | u3        | allow  | 2.3.4.5/32 | ingress   |
| open_ingress | u1        | allow  | 0.0.0.0/0  | ingress   |
```

## Related pages

- [`CREATE NETWORK POLICY`](../create-network-policy)
- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`DROP NETWORK POLICY`](../drop-network-policy)
