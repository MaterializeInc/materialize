---
title: "Protect a production cluster"
description: "Roll out query policies to restrict execution plans on production clusters."
menu:
  main:
    parent: "clusters"
---

Query policies help keep unexpected query plans off production clusters. Use
them alongside [workload isolation and operational
guidelines](/clusters/operational-guidelines/), not as comprehensive memory
protection: even fast index reads can be expensive.

{{< warning >}}
Query policies are experimental. Policy management requires
`enable_query_policies`; warnings and rejections also require
`enable_query_policy_enforcement`. Both flags are off by default. Confirm that
both are enabled for your environment before relying on a policy.
{{< /warning >}}

## Choose the boundary

Keep ongoing transformations and materialized views on a **compute cluster**,
and indexed application queries on a separate **serving cluster**. Start with a
serving-cluster policy rather than restricting workloads indiscriminately.
Policies govern query plans, not DDL or the ongoing work of maintaining compute
objects.

A query passes through these stages:

1. **Parse and plan:** resolve the SQL statement and its referenced objects.
2. **Optimize:** choose how to obtain and process the results.
3. **Classify and admit:** compare the plan with the cluster and current-role
   policies. Either policy can reject it.
4. **Execute:** run an admitted plan.
5. **Return:** deliver results to the client.

Admission applies to `SELECT`, `COPY`, `SUBSCRIBE`, and read-then-write plans for
`INSERT ... SELECT`, `UPDATE`, and `DELETE`.

## 1. Create a warning-mode policy

Choose one of these templates. Both allow constant results and fast index peeks.

**Reject query-scoped temporary dataflows:** use this for serving workloads that
should use fast peeks. In enforcement mode, this also rejects `COPY TO S3` and
`SUBSCRIBE`, which always install query dataflows.

```mzsql
CREATE QUERY POLICY serving_fast_peeks (
  MODE = 'warn',
  RULES (
    no_temporary_dataflows (
      action = 'reject',
      metric = 'query_plan_includes',
      value = 'slow_path_query'
    )
  )
);
```

**Reject Persist reads:** use this when queries may install temporary dataflows
but must read only indexes. This matches direct Persist fast peeks and temporary
dataflows with Persist inputs. Unlike the first template, it allows index-only
temporary dataflows.

```mzsql
CREATE QUERY POLICY serving_no_persist (
  MODE = 'warn',
  RULES (
    no_persist_reads (
      action = 'reject',
      metric = 'query_plan_includes',
      value = 'persist_read'
    )
  )
);
```

For syntax and privileges, see
[`CREATE QUERY POLICY`](/sql/create-query-policy/).

## 2. Attach and observe

Attach the selected policy to an existing serving cluster. This example selects
the fast-peek template:

```mzsql
ALTER CLUSTER serving SET (QUERY POLICY = serving_fast_peeks);
```

Attachment requires **CREATE** on the cluster and **USAGE** on the policy. If you
instead need a policy to follow a role across clusters, use a durable role
attachment, which requires **CREATEROLE** and policy **USAGE**:

```mzsql
ALTER ROLE app SET query_policy = serving_fast_peeks;
```

This is not a user-settable session variable. If both the cluster and current
role have policies, both apply; a permissive role policy cannot bypass a cluster
policy.

Run representative application queries and inspect client `NOTICE` messages.
Warning mode allows the queries to return results. Diagnostics identify the
policy, rule, and cluster, with a generic `EXPLAIN`/index hint. Use
[`EXPLAIN PLAN`](/sql/explain-plan/) to review affected plans, add suitable indexes where
appropriate, or move unsuitable workloads to another cluster.

The internal Prometheus counter `mz_query_policy_queries_total` reports
`outcome="warned"` and `outcome="rejected"`. Each affected query is counted once,
even if multiple rules or policies match; rejection takes precedence over a
warning. These are aggregate counts, not a per-query violation log. Inspect
definitions in `mz_internal.mz_query_policies` and
`mz_internal.mz_query_policy_rules`; there is no active-statement view or
violation log for policies.

## 3. Enforce and adjust

After reviewing warnings over a representative workload, switch the selected
policy to enforcement:

```mzsql
ALTER QUERY POLICY serving_fast_peeks SET (MODE = 'enforce');
```

Matching queries now fail with SQLSTATE `53000`. Monitor rejection counts and
client errors. To return to observation without detaching the policy:

```mzsql
ALTER QUERY POLICY serving_fast_peeks SET (MODE = 'warn');
```

Policy edits affect subsequent queries, not in-flight work. Changing `RULES`
replaces the entire rule set; changing only `MODE` preserves the rules. See
[`ALTER QUERY POLICY`](/sql/alter-query-policy/).

To remove a policy, first detach every cluster and role that uses it:

```mzsql
ALTER CLUSTER serving RESET (QUERY POLICY);
ALTER ROLE app RESET query_policy;
DROP QUERY POLICY serving_fast_peeks;
```

Altering or dropping a policy requires ownership. See
[`DROP QUERY POLICY`](/sql/drop-query-policy/),
[`ALTER CLUSTER`](/sql/alter-cluster/), and [`ALTER ROLE`](/sql/alter-role/).
