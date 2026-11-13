---
title: "ALTER QUERY POLICY"
description: "`ALTER QUERY POLICY` changes the mode, rules, or owner of a query policy."
menu:
  main:
    parent: commands
---

{{< private-preview />}}

`ALTER QUERY POLICY` changes the mode, rules, or owner of a [query
policy](/clusters/query-policies/). Changes apply to queries admitted after
the change, on every cluster the policy is attached to. Queries that are
already running are not affected.

## Syntax

{{< tabs >}}
{{< tab "Set mode or rules" >}}

{{% include-syntax file="examples/alter_query_policy" example="syntax" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

{{% include-syntax file="examples/alter_query_policy" example="syntax-owner" %}}

{{< /tab >}}
{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/alter-query-policy" %}}

To change the owner, you must also be a member of the new owner role.

## Examples

Switch a policy from `warn` to `enforce`:

```mzsql
ALTER QUERY POLICY indexed_reads_only SET (MODE = 'enforce');
```

Replace a policy's rules:

```mzsql
ALTER QUERY POLICY indexed_reads_only SET (
  RULES (
    no_storage_reads (action = 'reject', metric = 'query_plan_includes', value = 'storage_read')
  )
);
```

## Related pages

- [Query policies](/clusters/query-policies/)
- [`CREATE QUERY POLICY`](../create-query-policy)
- [`DROP QUERY POLICY`](../drop-query-policy)
- [`SHOW QUERY POLICIES`](../show-query-policies)
