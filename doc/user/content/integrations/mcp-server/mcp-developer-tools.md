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
`pg_catalog`, `information_schema`). The tool does not take a cluster argument;
the request runs on the catalog server cluster (`mz_catalog_server`).

{{< tip >}}
For system catalog lookups that can run on the `mz_catalog_server` cluster,
prefer `query_system_catalog` over the
[`query`](#query) tool.
{{</ tip >}}

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

Available starting in v26.30. Execute a read-only SQL query (`SELECT`, `SHOW`,
or `EXPLAIN`) against any object the role can access, including system catalog
and user objects. You must specify a cluster to run `EXPLAIN ANALYZE` and
queries against user objects. On clusters with more than one replica,
`EXPLAIN ANALYZE` additionally requires targeting a single replica via
`cluster_replica`, since [introspection
data](/reference/system-catalog/mz_introspection/) is replica-specific.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster` | string | Yes | Exact cluster name the query should run on. |
| `cluster_replica` | string | No | Available starting in v26.33. Replica name (e.g. `r1`) to target one replica of the cluster. Required for `EXPLAIN ANALYZE` on clusters with more than one replica. Find replica names in `mz_catalog.mz_cluster_replicas`. |
| `sql_query` | string | Yes | `SELECT`, `SHOW`, or `EXPLAIN` statement. |

Only one statement per call is allowed. Write operations (`INSERT`, `UPDATE`,
`CREATE`, etc.) are rejected. To disable the tool, see
[`enable_mcp_developer_query_tool`](/integrations/mcp-server/mcp-developer-config/).

{{< tip >}}
For system catalog lookups that can run on the `mz_catalog_server` cluster,
prefer [`query_system_catalog`](#query_system_catalog) over `query`.
{{</ tip >}}

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
