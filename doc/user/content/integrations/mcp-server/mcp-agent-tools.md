---
title: Agent MCP server tools
description: "Tools exposed by the `materialize-agent` MCP server."
menu:
  main:
    parent: "mcp-server-agent"
    weight: 10
    identifier: "agent-endpoint-tools"
    name: "Available tools"
---

## Tools

### `get_data_products`

Discover all available data products. Returns a lightweight list with name,
cluster, and description for each product.

**Parameters:** None.

**Example response:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[\n  [\n    \"\\\"materialize\\\".\\\"mcp_schema\\\".\\\"payment_status\\\"\",\n    \"mcp_cluster\",\n    \"Given an order ID, return the current payment status.\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```

### `get_data_product_details`

Get the full details for a specific data product, including its JSON schema
with column names, types, and descriptions.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Exact name from the `get_data_products` list. |

**Example response:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[\n  [\n    \"\\\"materialize\\\".\\\"mcp_schema\\\".\\\"payment_status\\\"\",\n    \"mcp_cluster\",\n    \"Given an order ID, return the current payment status.\",\n    \"{\\\"order_id\\\": {\\\"type\\\": \\\"integer\\\", \\\"position\\\": 1}, \\\"status\\\": {\\\"type\\\": \\\"text\\\", \\\"position\\\": 3}}\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```

### `read_data_product`

Read rows from a data product.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Fully-qualified name, e.g. `"materialize"."public"."payment_status"`. |
| `limit` | integer | No | Maximum rows to return. Default: 500, max: 1000. |
| `cluster` | string | No | Cluster override. If omitted, uses the cluster from the catalog. |

**Example response:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[\n  [\n    1001,\n    42,\n    \"shipped\",\n    \"2026-03-26T10:30:00Z\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```

### `query`

{{< note >}}
The `query` tool is **disabled by default** because it lets the agent run
arbitrary SQL against data products. To enable it, set the
`enable_mcp_agent_query_tool` system parameter to `true`.
{{< /note >}}

Execute a SQL `SELECT` statement against your data products. Useful for
joining multiple data products together that are hosted on the same cluster.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster` | string | Yes | Exact cluster name from the data product details. |
| `sql_query` | string | Yes | PostgreSQL-compatible `SELECT` statement. |

**Example response:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[\n  [\n    \"42\",\n    \"shipped\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```
