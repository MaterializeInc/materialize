---
title: MCP server tools
description: "Tools exposed by the `materialize-mcp` MCP server at /api/mcp."
menu:
  main:
    parent: "mcp-server-unified"
    weight: 10
    identifier: "unified-endpoint-tools"
    name: "Available tools"
---

The `materialize-mcp` server at `/api/mcp` has three tools. Every role sees all
three. Each call runs as the connected role, so it can only read what that role
can read. See [Limit access by
role](/developer-tools/mcp-server/materialize-mcp-access/).

All tools are read-only. Each response is limited by
[`mcp_max_response_size`](/developer-tools/mcp-server/materialize-mcp-config/).

## Tools

### `list_data_products`

Returns the data products your role can read. Each row has:

- The fully qualified name, for example `"materialize"."mcp_schema"."payment_status"`.
- The cluster that hosts the object or its index. This is null when your role
  does not have `USAGE` on that cluster.
- The description, taken from the index comment if there is one, otherwise from
  the object comment.

A data product is a materialized view, or an indexed view whose index is on a
cluster your role can use, that your role has `SELECT` on. Views without an
index are not listed.

**Parameters:** None.

This tool is named `get_data_products` on the [agent
server](/developer-tools/mcp-server/mcp-agent-tools/#get_data_products).
`/api/mcp` also accepts `get_data_products` in `tools/call`, but lists only
`list_data_products`.

### `get_data_product_details`

Returns the details of one data product: its name, cluster, description, a JSON
schema of its columns, and a `hydration` object.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Exact name from the `list_data_products` result. |

The `hydration` object has the fields `hydrated`, `replica_count`, and
`hydrated_replica_count`. Reads never return partial data. A read of a data
product that is not hydrated waits until the dataflow catches up, and can time
out. If `replica_count` is 0, the cluster has no replicas, and reads cannot
finish until a replica is added.

### `query`

Runs one read-only `SELECT`, `SHOW`, or `EXPLAIN` statement on any object the
role can read. This includes data products, other user objects, and the system
catalog. Statements that write (`INSERT`, `UPDATE`, `CREATE`, and so on) and
requests with more than one statement are rejected.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql_query` | string | Yes | PostgreSQL-compatible `SELECT`, `SHOW`, or `EXPLAIN` statement. |
| `cluster` | string | No | Exact name of the cluster to run on. If you leave it out, the query runs on the session's default cluster. |
| `cluster_replica` | string | No | Replica name, for example `r1`, to run on one replica of `cluster`. Requires `cluster`. |

- **Reading data products:** Pass the `cluster` from `get_data_product_details`
  so that reads of indexed views use the index. If that cluster is null, leave
  out `cluster`.
- **Reading the system catalog:** Write system object names with their schema,
  for example `mz_internal.mz_source_statuses`. The `query` tool does not add
  the system schemas to the search path.
- **Introspection and `EXPLAIN ANALYZE`:** [`mz_introspection`
  relations](/sql/system-catalog/mz_introspection/) are specific to a cluster
  replica. Set `cluster`, and set `cluster_replica` on a cluster with more than
  one replica. `EXPLAIN ANALYZE` needs `cluster`. Find replica names in
  `mz_catalog.mz_cluster_replicas`.

The `query` tool is always available on `/api/mcp`. The
`enable_mcp_agent_query_tool` and `enable_mcp_developer_query_tool` settings do
not apply to it. To stop a role from reading the system catalog, set
[`restrict_to_user_objects`](/developer-tools/mcp-server/materialize-mcp-access/)
on the role.

**Example request:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "query",
    "arguments": {
      "sql_query": "SELECT name FROM mz_catalog.mz_databases WHERE name = 'materialize'"
    }
  }
}
```

**Example response:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[\n  [\"materialize\"]\n]"
      }
    ],
    "isError": false
  }
}
```

## Tools on the agent and developer servers

The following tools are not available on `/api/mcp`. A call to them returns a
`Tool not found` error.

| Tool | Server | Use on `/api/mcp` |
|------|--------|-------------------|
| `read_data_product` | [Agent](/developer-tools/mcp-server/mcp-agent-tools/#read_data_product) | `query`, with the data product's `cluster`. |
| `query_system_catalog` | [Developer](/developer-tools/mcp-server/mcp-developer-tools/#query_system_catalog) | `query`, with schema-qualified system object names. |

## See also

- [System catalog](/sql/system-catalog/)
- [Key system catalog tables](/developer-tools/mcp-server/mcp-developer-tools/#key-system-catalog-tables)
