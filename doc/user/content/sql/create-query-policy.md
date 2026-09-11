---
title: "CREATE QUERY POLICY"
description: "`CREATE QUERY POLICY` creates a policy that warns about or rejects queries based on their execution plans."
menu:
  main:
    parent: commands
---

`CREATE QUERY POLICY` creates a named policy that warns about or rejects queries
based on their execution plans. See [Protect a production
cluster](/clusters/protect-a-production-cluster/) for a rollout guide.

{{< warning >}}
Query policies are experimental. Both feature flags are off by default:
`enable_query_policies` enables policy management, and
`enable_query_policy_enforcement` enables query warnings and rejections. Creating
and attaching a policy alone does not protect a cluster when enforcement is
disabled.
{{< /warning >}}

## Syntax

```mzsql
CREATE QUERY POLICY <name> (
  MODE = 'warn' | 'enforce',
  RULES (
    <rule_name> (
      action = 'reject',
      metric = 'query_plan_includes',
      value = 'slow_path_query' | 'persist_read'
    ) [, ...]
  )
);
```

Syntax element | Description
---------------|------------
`<name>` | The name of the query policy to create.
`MODE` | Optional. Defaults to `warn`. In `warn` mode, matching rules produce a `NOTICE` and the query can return results. In `enforce` mode, matching rules reject the query.
`RULES` | Named rules to evaluate against the query plan.
`action` | Must be `reject`. The policy mode determines whether to warn or reject.
`metric` | Must be `query_plan_includes`.
`value` | The plan characteristic to match: `slow_path_query` or `persist_read`.

## Details

### Plan classification

Policies evaluate optimized execution plans, not SQL text or estimated resource
usage:

| Rule value | Matches |
|------------|---------|
| `slow_path_query` | Any query-scoped temporary dataflow. |
| `persist_read` | A direct Persist fast peek, or a query-scoped temporary dataflow with Persist inputs. |

Constant results and fast index peeks match neither rule. A temporary dataflow
that reads only indexes matches `slow_path_query` but not `persist_read`. Thus,
a policy that rejects only Persist reads can allow index-only temporary
dataflows.

Policies apply to `SELECT`, `COPY`, `SUBSCRIBE`, and the read-then-write plans of
`INSERT ... SELECT`, `UPDATE`, and `DELETE`. They do not apply to DDL. `COPY TO S3`
and `SUBSCRIBE` always install a query dataflow, so a slow-path rule rejects them
in `enforce` mode.

Query policies are not comprehensive memory protection. Fast index reads can
still be expensive.

### Attaching a policy

A policy takes effect through a cluster or role attachment:

```mzsql
ALTER CLUSTER serving SET (QUERY POLICY = serving_policy);
ALTER ROLE app SET query_policy = serving_policy;
```

The cluster policy and the current role's policy both apply. Either policy can
reject a query; a role policy does not override a cluster policy. Role attachments
are durable configuration, not user-settable session variables.

To detach a policy:

```mzsql
ALTER CLUSTER serving RESET (QUERY POLICY);
ALTER ROLE app RESET query_policy;
```

### Warnings and rejections

In `warn` mode, a matching rule emits a `NOTICE` without rejecting the query. In
`enforce` mode, rejection uses SQLSTATE `53000`. Diagnostics identify the policy,
rule, and cluster and provide a generic hint to inspect the plan with `EXPLAIN`
and consider indexes. The hint does not guarantee that an index will make a query
admissible.

Policy changes affect subsequent queries; they do not stop in-flight work.

### Introspection

The internal catalogs expose policy definitions and rules:

| Catalog | Columns |
|---------|---------|
| `mz_internal.mz_query_policies` | `id`, `name`, `mode`, `owner_id`, `privileges`, `oid` |
| `mz_internal.mz_query_policy_rules` | `policy_id`, `name`, `action`, `metric`, `value` |

These catalogs do not expose active statements or a violation log.

```mzsql
SELECT p.name AS policy_name, p.mode, r.name AS rule_name,
       r.action, r.metric, r.value
FROM mz_internal.mz_query_policies AS p
JOIN mz_internal.mz_query_policy_rules AS r ON r.policy_id = p.id;
```

## Privileges

Creating a policy requires the **CREATEQUERYPOLICY** system privilege.

```mzsql
GRANT CREATEQUERYPOLICY ON SYSTEM TO policy_admin;
```

Attaching a policy to a cluster requires **CREATE** on the cluster and **USAGE**
on the policy. Attaching a policy to a role requires **CREATEROLE** and **USAGE**
on the policy. Altering or dropping a policy requires ownership of the policy.

## Examples

Create a policy that warns about query-scoped temporary dataflows:

```mzsql
CREATE QUERY POLICY serving_policy (
  MODE = 'warn',
  RULES (
    no_temporary_dataflows (
      action = 'reject',
      metric = 'query_plan_includes',
      value = 'slow_path_query'
    )
  )
);

ALTER CLUSTER serving SET (QUERY POLICY = serving_policy);
```

## Related pages

- [Protect a production cluster](/clusters/protect-a-production-cluster/)
- [`ALTER QUERY POLICY`](../alter-query-policy)
- [`DROP QUERY POLICY`](../drop-query-policy)
- [`ALTER CLUSTER`](../alter-cluster)
- [`ALTER ROLE`](../alter-role)
