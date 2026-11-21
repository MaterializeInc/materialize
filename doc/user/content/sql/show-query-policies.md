---
title: "SHOW QUERY POLICIES"
description: "`SHOW QUERY POLICIES` lists the query policies in Materialize."
menu:
  main:
    parent: commands
---

{{< private-preview />}}

`SHOW QUERY POLICIES` lists the [query policies](/clusters/query-policies/) in
Materialize.

## Syntax

```mzsql
SHOW QUERY POLICIES [ LIKE <pattern> | WHERE <condition expression> ];
```

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show query policies whose name matches the pattern.
**WHERE** \<condition expression\> | If specified, only show query policies that match the condition expression.

## Examples

```mzsql
SHOW QUERY POLICIES;
```
```nofmt
        name        |  mode   |                  rules                  | comment
--------------------+---------+-----------------------------------------+---------
 indexed_reads_only | enforce | no_storage_reads,no_temporary_dataflows |
```

To see the rules in each policy and the clusters each policy is attached to,
query [`mz_internal.mz_query_policy_rules`](/sql/system-catalog/mz_internal/#mz_query_policy_rules)
and [`mz_internal.mz_query_policy_attachments`](/sql/system-catalog/mz_internal/#mz_query_policy_attachments):

```mzsql
SELECT p.name AS policy, c.name AS cluster
FROM mz_internal.mz_query_policy_attachments a
JOIN mz_internal.mz_query_policies p ON p.id = a.policy_id
JOIN mz_catalog.mz_clusters c ON c.id = a.object_id;
```

## Related pages

- [Query policies](/clusters/query-policies/)
- [`CREATE QUERY POLICY`](../create-query-policy)
- [`ALTER QUERY POLICY`](../alter-query-policy)
- [`DROP QUERY POLICY`](../drop-query-policy)
