---
title: Developer MCP server tools
description: "Tools exposed by the `materialize-developer` MCP server."
menu:
  main:
    parent: "mcp-server-developer"
    weight: 10
    identifier: "developer-endpoint-tools"
    name: "Available tools"
---

## Tools

### `query_system_catalog`

Execute a read-only SQL query restricted to system catalog tables (`mz_*`,
`pg_catalog`, `information_schema`). No cluster argument; prefer this for
catalog lookups that do not need a cluster to run on.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql_query` | string | Yes | `SELECT`, `SHOW`, or `EXPLAIN` query using only system catalog tables. |

Only one statement per call is allowed. Write operations (`INSERT`, `UPDATE`,
`CREATE`, etc.) are rejected.

**Example response:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[\n  [\n    \"quickstart\",\n    \"ready\"\n  ],\n  [\n    \"mcp_cluster\",\n    \"ready\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```

### `query`

Execute a read-only SQL query (`SELECT`, `SHOW`, or `EXPLAIN`) against any
object the role can access, including system catalog and user objects. A
cluster is required, which is what enables `EXPLAIN ANALYZE` and queries
against indexed user objects.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster` | string | Yes | Exact cluster name the query should run on. |
| `sql_query` | string | Yes | `SELECT`, `SHOW`, or `EXPLAIN` statement. |

Only one statement per call is allowed. Write operations (`INSERT`, `UPDATE`,
`CREATE`, etc.) are rejected. The tool is hidden when the operator has
disabled it via [`enable_mcp_developer_query_tool`](/integrations/mcp-server/mcp-developer-config/).

For pure system catalog lookups that do not need a cluster, prefer
[`query_system_catalog`](#query_system_catalog).

**Example response:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[\n  [\n    \"Explained Query (fast path):\\n  →Constant (1 rows)\\n\\nTarget cluster: quickstart\\n\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```

### Key system catalog tables

| Scenario | Tables |
|----------|--------|
| Freshness / lag | `mz_internal.mz_materialization_lag`, `mz_internal.mz_wallclock_global_lag_recent_history`, `mz_internal.mz_hydration_statuses` |
| Memory / resources | `mz_internal.mz_cluster_replica_utilization`, `mz_internal.mz_cluster_replica_metrics` |
| Cluster health | `mz_internal.mz_cluster_replica_statuses`, `mz_catalog.mz_cluster_replicas` |
| Source / Sink health | `mz_internal.mz_source_statuses`, `mz_internal.mz_sink_statuses`, `mz_internal.mz_source_statistics` |
| Object inventory | `mz_catalog.mz_materialized_views`, `mz_catalog.mz_sources`, `mz_catalog.mz_sinks`, `mz_catalog.mz_indexes` |
| Optimization | `mz_internal.mz_index_advice`, `mz_catalog.mz_cluster_replica_sizes` |

Use `SHOW TABLES FROM mz_internal` or `SHOW TABLES FROM mz_catalog` to
discover more tables.

## See also

- [System catalog](/reference/system-catalog/)
