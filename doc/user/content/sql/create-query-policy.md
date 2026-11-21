---
title: "CREATE QUERY POLICY"
description: "`CREATE QUERY POLICY` creates a query policy that warns about or rejects queries whose execution plan builds temporary dataflows or reads from object storage."
menu:
  main:
    parent: commands
---

{{< private-preview />}}

`CREATE QUERY POLICY` creates a [query policy](/clusters/query-policies/). A
query policy contains rules that warn about or reject queries based on their
execution plan. A policy has no effect until you attach it to a cluster with
[`ALTER CLUSTER`](/sql/alter-cluster/) or [`CREATE
CLUSTER`](/sql/create-cluster/).

## Syntax

{{% include-syntax file="examples/create_query_policy" example="syntax" %}}

## Details

Query policies are plan-based admission controls, not resource limits.
Materialize checks the optimized execution plan of a query before it runs. A
[fast path](/sql/explain-plan/#fast-path-queries) read from an existing index
matches neither rule, even if it returns many rows.

For which statements a policy checks, see [Query
policies](/clusters/query-policies/#what-a-policy-checks).

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-query-policy" %}}

## Examples

```mzsql
CREATE QUERY POLICY indexed_reads_only (
  MODE = 'enforce',
  RULES (
    no_temporary_dataflows (action = 'reject', metric = 'query_plan_includes', value = 'temporary_dataflow')
  )
);

ALTER CLUSTER serving SET (QUERY POLICY = indexed_reads_only);
```

## Related pages

- [Query policies](/clusters/query-policies/)
- [`ALTER QUERY POLICY`](../alter-query-policy)
- [`DROP QUERY POLICY`](../drop-query-policy)
- [`SHOW QUERY POLICIES`](../show-query-policies)
