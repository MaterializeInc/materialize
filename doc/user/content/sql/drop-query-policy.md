---
title: "DROP QUERY POLICY"
description: "`DROP QUERY POLICY` removes an existing query policy from Materialize."
menu:
  main:
    parent: commands
---

`DROP QUERY POLICY` removes an existing query policy from Materialize. Use
`CASCADE` to detach it from clusters and roles as part of the drop.

{{< warning >}}
Query policies are experimental. The default-off `enable_query_policies` flag
enables management; the separate default-off `enable_query_policy_enforcement`
flag enables warnings and rejections.
{{< /warning >}}

## Syntax

```mzsql
DROP QUERY POLICY [IF EXISTS] <name> [CASCADE | RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. Do not return an error if the named policy does not exist.
`<name>` | The name of the query policy to drop. List policies in `mz_internal.mz_query_policies`.
**CASCADE** | Detach the policy from all clusters and roles, without dropping those objects.
**RESTRICT** | Default. Reject the drop if a surviving cluster or role has the policy attached.

`DROP OWNED ... CASCADE` also detaches policies being dropped. An attachment to
a cluster that is itself being dropped does not block `DROP OWNED ... RESTRICT`.

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

Alternatively, detach all attachments and drop the policy atomically:

```mzsql
DROP QUERY POLICY IF EXISTS serving_policy CASCADE;
```

## Related pages

- [`CREATE QUERY POLICY`](../create-query-policy)
- [`ALTER QUERY POLICY`](../alter-query-policy)
- [`ALTER CLUSTER`](../alter-cluster)
- [`ALTER ROLE`](../alter-role)
- [Protect a production cluster](/clusters/protect-a-production-cluster/)
