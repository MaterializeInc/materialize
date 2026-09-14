---
title: "DROP QUERY POLICY"
description: "`DROP QUERY POLICY` removes an existing query policy from Materialize."
menu:
  main:
    parent: commands
---

`DROP QUERY POLICY` removes an existing query policy from Materialize. Detach the
policy from all clusters and roles before dropping it.

{{< warning >}}
Query policies are experimental. The default-off `enable_query_policies` flag
enables management; the separate default-off `enable_query_policy_enforcement`
flag enables warnings and rejections.
{{< /warning >}}

## Syntax

```mzsql
DROP QUERY POLICY [IF EXISTS] <name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. Do not return an error if the named policy does not exist. This does not bypass the requirement to detach an existing policy.
`<name>` | The name of the query policy to drop. List policies in `mz_internal.mz_query_policies`.

## Privileges

You must own the query policy to drop it.

## Examples

If `serving` and `app` are the only attachments to `serving_policy`, detach both
and then drop the policy:

```mzsql
ALTER CLUSTER serving RESET (QUERY POLICY);
ALTER ROLE app RESET query_policy;
DROP QUERY POLICY IF EXISTS serving_policy;
```

## Related pages

- [`CREATE QUERY POLICY`](../create-query-policy)
- [`ALTER QUERY POLICY`](../alter-query-policy)
- [`ALTER CLUSTER`](../alter-cluster)
- [`ALTER ROLE`](../alter-role)
- [Protect a production cluster](/clusters/protect-a-production-cluster/)
