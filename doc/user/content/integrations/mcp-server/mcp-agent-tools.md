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

Returns the list of data products discoverable by the tool. Materialized views
and indexed views are discoverable by `get_data_products`. Regular views must
have an index to be discoverable.

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

Returns the full details for a specific data product, including its JSON schema
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

Reads rows from a data product.

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

{{< warning >}}
{{% include-headless "/headless/mcp-agent-query-tool-warning" %}}
{{< /warning >}}

Allows the agent to run arbitrary `SELECT` statements (including joins) against
**any** object for which the agent has the appropriate privileges (`SELECT` on
the object, `USAGE` on the object's schema), not just the objects
discoverable by `get_data_products`. Starting in v26.27, it is enabled by
default.

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

{{< note >}}

- *Recommended*. To prevent an agent from querying the system catalog objects
  (`mz_catalog.*`, `mz_internal.*`, `pg_catalog.*`, and `information_schema.*`),
  see [Restrict `query` tool access to user objects
  only](#restrict-to-user-objects).

- To disable the tool, set the [`enable_mcp_agent_query_tool`
  configuration](/integrations/mcp-server/mcp-agent-config/#enable_mcp_agent_query_tool)
  system parameter to `false`. Once disabled, you can only query data products
  that are discoverable by [`get_data_products`](#get_data_products).

{{< /note >}}


#### Restricting `query` tool access to user objects only {#restrict-to-user-objects}

When the [`query` tool](/integrations/mcp-server/mcp-agent-tools/#query) is
enabled, a role can, by default, query any object for which it has appropriate
privileges, including system catalog objects (`mz_catalog.*`, `mz_internal.*`,
`pg_catalog.*`, and `information_schema.*`).

To prevent an agent role from reading system catalog objects, a **superuser**
can set the `restrict_to_user_objects` parameter to `true` on both the
functional role and each individual agent role. Setting the parameter on the
functional role is recommended as a precaution in case the role is ever used
directly to run queries. Because role configuration in Materialize is not
inherited, the parameter must be set explicitly on each individual agent role:

```mzsql
ALTER ROLE mcp_agent SET restrict_to_user_objects = true;
ALTER ROLE my_agent SET restrict_to_user_objects = true;
```

This setting takes effect on the next connection. Once active:

- Queries referencing system catalog objects are rejected with a permission
  error.
- Data product discovery (`get_data_products`, `get_data_product_details`,
  `read_data_product`) continues to work normally.
- The restriction cannot be bypassed by the role itself; only a superuser can
  change or remove it.

To remove the restriction for an agent, a superuser can reset the parameter (or
set it to `false`):

```mzsql
ALTER ROLE my_agent RESET restrict_to_user_objects;
```
