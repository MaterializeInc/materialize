---
title: "ALTER QUERY POLICY"
description: "`ALTER QUERY POLICY` changes the mode or rules of an existing query policy."
menu:
  main:
    parent: commands
---

`ALTER QUERY POLICY` changes an existing query policy. Changes affect subsequent
queries, not in-flight work.

{{< warning >}}
Query policies are experimental. The default-off `enable_query_policies` flag
enables management; the separate default-off `enable_query_policy_enforcement`
flag enables warnings and rejections.
{{< /warning >}}

## Syntax

```mzsql
ALTER QUERY POLICY <name> SET (
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

Specify `MODE`, `RULES`, or both. Each specified option replaces its previous
value. Unspecified options are unchanged. In particular, `RULES` replaces the
entire rule set; it does not append rules.

For mode and rule semantics, see [`CREATE QUERY POLICY`](../create-query-policy).

## Privileges

You must own the query policy to alter it.

## Examples

Enforce an existing policy without changing its rules:

```mzsql
ALTER QUERY POLICY serving_policy SET (MODE = 'enforce');
```

Replace its rules with a Persist-only restriction, leaving the mode unchanged:

```mzsql
ALTER QUERY POLICY serving_policy SET (
  RULES (
    no_persist_reads (
      action = 'reject',
      metric = 'query_plan_includes',
      value = 'persist_read'
    )
  )
);
```

Return to warning mode:

```mzsql
ALTER QUERY POLICY serving_policy SET (MODE = 'warn');
```

## Related pages

- [`CREATE QUERY POLICY`](../create-query-policy)
- [`DROP QUERY POLICY`](../drop-query-policy)
- [Protect a production cluster](/clusters/protect-a-production-cluster/)
