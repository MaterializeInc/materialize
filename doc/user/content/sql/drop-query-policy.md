---
title: "DROP QUERY POLICY"
description: "`DROP QUERY POLICY` removes a query policy from Materialize."
menu:
  main:
    parent: commands
---

{{< private-preview />}}

`DROP QUERY POLICY` removes a [query policy](/clusters/query-policies/) from
Materialize.

## Syntax

```mzsql
DROP QUERY POLICY [IF EXISTS] <name> [, ...] [CASCADE | RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified query policy does not exist.
`<name>` | The query policy to drop. For available query policies, see [`SHOW QUERY POLICIES`](../show-query-policies).
**CASCADE** | Optional. If specified, detach the query policy from all clusters, then drop it. The clusters are not dropped.
**RESTRICT** | Optional. Do not drop the query policy if it is attached to a cluster. _(Default.)_

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/drop-query-policy" %}}

## Examples

```mzsql
DROP QUERY POLICY indexed_reads_only CASCADE;
```

## Related pages

- [Query policies](/clusters/query-policies/)
- [`CREATE QUERY POLICY`](../create-query-policy)
- [`ALTER QUERY POLICY`](../alter-query-policy)
- [`SHOW QUERY POLICIES`](../show-query-policies)
