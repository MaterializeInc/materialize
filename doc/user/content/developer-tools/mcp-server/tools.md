---
title: "Available tools"
description: "Tools exposed by the Materialize MCP server, their parameters, and the privileges each one needs."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    name: "Available tools"
    weight: 30
    identifier: "mcp-server-tools"
aliases:
  - /integrations/mcp-server/mcp-agent-tools/
  - /integrations/mcp-server/mcp-developer-tools/
  - /developer-tools/mcp-server/mcp-agent-tools/
  - /developer-tools/mcp-server/mcp-developer-tools/
---

{{< public-preview />}}

The Materialize MCP server at `/api/mcp` exposes the following tools. All tools
are read-only.

| Tool | What it does | Privileges checked |
|------|--------------|--------------------|
| [`get_permissions`](#get_permissions) | Reports the privileges the connected role holds. | None beyond a successful connection. |
| [`get_settings`](#get_settings) | Reports the session settings that change what the role can do. | None beyond a successful connection. |
| [`list_data_products`](#list_data_products) | Lists the data products the role can read. | Results are filtered to objects the role has `SELECT` on. |
| [`get_data_product_details`](#get_data_product_details) | Returns a data product's columns, types, comments, and lookup keys. | `SELECT` on the data product. |
| [`query`](#query) | Runs a read-only `SELECT`, `SHOW`, or `EXPLAIN` on objects the role can read. | `SELECT` on each referenced object, `USAGE` on its schema, `USAGE` on the target cluster. |
| [`query_system_tables`](#query_system_tables) | Runs a read-only query against the system catalog. | `SELECT` on the referenced catalog relations. Refused when the role has `restrict_to_user_objects` set. |

## Tool visibility and permissions

`tools/list` returns the same six tools to every connected role. The tool list
is not filtered by grants, because hiding a tool is not an access control:
permissions are enforced when a tool runs, on every call, against the role's
grants and settings. A tool the role may not use appears in the list and returns
a permission error when called.

Agents should call [`get_permissions`](#get_permissions) and
[`get_settings`](#get_settings) once after connecting to learn what the role can
reach and why a call might be refused. For how administrators shape those
grants, see [MCP access controls](/developer-tools/mcp-server/access-control/).

## Tools

### `get_permissions`

Returns the privileges the connected role holds, including privileges inherited
through role membership. Use it to find out which clusters, schemas, and objects
the role can use before issuing queries.

**Parameters:** None.

{{< note >}}
**$TODO: Figure out ahead of launch.** Document the response shape (object
name, object type, privilege list) and add an example response once the tool's
output format is final.
{{< /note >}}

### `get_settings`

Returns the effective session settings that change what the role can do:

| Setting | Meaning |
|---------|---------|
| `restrict_to_user_objects` | When `true`, the role is confined to user objects and the MCP data product views. `query_system_tables` and catalog reads through `query` are refused. See [Confine a role to user data](/developer-tools/mcp-server/access-control/#restrict-to-user-objects). |
| `cluster` | The cluster used when a tool does not name one. |

**Parameters:** None.

{{< note >}}
**$TODO: Figure out ahead of launch.** Confirm the final tool name
(`get_settings` in the engineering design, `get_session_settings` in the PRD),
the complete list of settings returned, and add an example response.
{{< /note >}}

### `list_data_products`

Returns the data products the connected role can read. Materialized views and
indexed views are discoverable. Regular views must have an index to be
discoverable.

For each data product, the response includes its fully qualified name, the
cluster that maintains its index, and its comment.

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
        "text": "[\n  [\n    \"\\\"materialize\\\".\\\"data_products\\\".\\\"payment_status\\\"\",\n    \"mcp_cluster\",\n    \"Given an order ID, return the current payment status.\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```

{{< tip >}}
Data products are ordinary Materialize objects. To make an object useful to an
agent, index it in a cluster the agent's role can use, and add
[comments](/sql/comment-on/) to the object and its columns. Comments are
surfaced to the agent to help it decide when and how to use each data product.
See [Define data
products](/developer-tools/mcp-server/access-control/#define-data-products).
{{< /tip >}}

### `get_data_product_details`

Returns the full details for one data product: its JSON schema with column
names, types, positions, and column comments. When the data product is indexed,
the indexed columns are surfaced as preferred lookup keys, enabling [index
point lookups](/fundamentals/concepts/indexes/#point-lookups) instead of scans.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Exact fully qualified name from the `list_data_products` result. |

**Example response:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[\n  [\n    \"\\\"materialize\\\".\\\"data_products\\\".\\\"payment_status\\\"\",\n    \"mcp_cluster\",\n    \"Given an order ID, return the current payment status.\",\n    \"{\\\"order_id\\\": {\\\"type\\\": \\\"integer\\\", \\\"position\\\": 1}, \\\"status\\\": {\\\"type\\\": \\\"text\\\", \\\"position\\\": 3}}\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```

### `query`

Runs a read-only SQL statement (`SELECT`, `SHOW`, or `EXPLAIN`) against any
object the role can read, including joins across objects, on the cluster you
name. Use it for point lookups, joins across data products, and `EXPLAIN
ANALYZE` against a materialized view or index.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster` | string | Yes | Exact name of the cluster to run the query on. The role needs `USAGE` on it. |
| `cluster_replica` | string | No | Replica name (for example, `r1`) to target one replica of the cluster. Required for `EXPLAIN ANALYZE` on clusters with more than one replica, because [introspection data](/sql/system-catalog/mz_introspection/) is replica-specific. Find replica names in `mz_catalog.mz_cluster_replicas`. |
| `sql_query` | string | Yes | A single `SELECT`, `SHOW`, or `EXPLAIN` statement. |

Only one statement per call is allowed. Write statements (`INSERT`, `UPDATE`,
`DELETE`, `CREATE`, and so on) are rejected regardless of the role's grants.

The query runs with the role's ordinary grants: `SELECT` on each referenced
object, `USAGE` on the schemas that contain them, and `USAGE` on the cluster.
If the role has `restrict_to_user_objects` set, references to system catalog
objects are refused.

{{< tip >}}
Because `query` can join across objects, consider maintaining an [ontology
table](/transform-data/patterns/ontology/): a curated catalog of the join
relationships in your schema that the agent can read to confirm exact join keys
before writing multi-table SQL.
{{< /tip >}}

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

### `query_system_tables`

Runs a read-only SQL statement (`SELECT`, `SHOW`, or `EXPLAIN`) against the
system catalog (`mz_catalog`, `mz_internal`, `mz_introspection`, `pg_catalog`,
and `information_schema`) for troubleshooting and observability. The tool takes
no cluster argument. Requests run on the catalog server cluster,
`mz_catalog_server`, which every role can use.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql_query` | string | Yes | A single `SELECT`, `SHOW`, or `EXPLAIN` statement over system catalog relations. |

Only one statement per call is allowed. Write statements are rejected.

Before the statement runs, the tool pins `search_path` to the system schemas,
so an unqualified name like `mz_sources` always resolves to the system catalog
and never to a user-created object of the same name. This is why the tool
exists alongside `query`: it lets an agent write unqualified `mz_*` names
safely.

Roles with `restrict_to_user_objects` set cannot use this tool. Calls return a
permission error.

{{< tip >}}
For catalog lookups, prefer `query_system_tables` over `query`. Use `query` when
the question needs a specific cluster, for example `EXPLAIN ANALYZE` on a
materialized view, or replica-specific `mz_introspection` data.
{{< /tip >}}

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

#### Key system catalog tables

| Scenario | Tables |
|----------|--------|
| Freshness / lag | `mz_internal.mz_materialization_lag`, `mz_internal.mz_wallclock_global_lag_recent_history`, `mz_internal.mz_hydration_statuses` |
| Memory / resources | `mz_internal.mz_cluster_replica_utilization`, `mz_internal.mz_cluster_replica_metrics` |
| Cluster health | `mz_internal.mz_cluster_replica_statuses`, `mz_catalog.mz_cluster_replicas` |
| Source / sink health | `mz_internal.mz_source_statuses`, `mz_internal.mz_sink_statuses`, `mz_internal.mz_source_statistics` |
| Object inventory | `mz_catalog.mz_materialized_views`, `mz_catalog.mz_sources`, `mz_catalog.mz_sinks`, `mz_catalog.mz_indexes` |
| Optimization | `mz_internal.mz_index_advice`, `mz_catalog.mz_cluster_replica_sizes` |

Use `SHOW TABLES FROM mz_internal` or `SHOW TABLES FROM mz_catalog` to discover
more tables. See also the [system catalog reference](/sql/system-catalog/).

## Response format

Query results are returned as a JSON array of rows inside a single `text`
content block, as shown in the examples above. Responses larger than
`mcp_max_response_size` bytes (default 1,000,000) return an error instead of a
truncated result. See [Environment-wide
configuration](/developer-tools/mcp-server/access-control/#environment-configuration).

{{< note >}}
**$TODO: Figure out ahead of launch.** Confirm whether read results are wrapped
in `<untrusted-data-{uuid}>` boundaries to blunt prompt injection, as the PRD
proposes, and document the exact format here if so.
{{< /note >}}

## Legacy tool names

The legacy endpoints `/api/mcp/agent` and `/api/mcp/developer` introduced some
tools under different names. The old names are accepted on `tools/call` as
aliases on every endpoint, but are not advertised in `tools/list`, so the list
stays at the six tools above.

| Current name | Legacy name | Notes |
|--------------|-------------|-------|
| `list_data_products` | `get_data_products` | Same behavior. |
| `get_data_product_details` | `get_data_product_details` | Unchanged. |
| `query` | `query` | Unchanged. On the legacy `/api/mcp/agent` endpoint, `cluster_replica` is ignored. |
| `query_system_tables` | `query_system_catalog` | Same behavior. |
| None | `read_data_product` | Deprecated. Use `query` with a `SELECT` against the data product instead. |

{{< note >}}
**$TODO: Figure out ahead of launch.** Confirm when `read_data_product` is
removed and whether it remains callable on the legacy endpoints until then.
{{< /note >}}

## Example questions

Once connected, you can ask natural language questions and the agent picks the
appropriate tool:

| Question | Tools used |
|----------|------------|
| *What can I access?* | `get_permissions`, `get_settings` |
| *What data products can I query?* | `list_data_products` |
| *What's the `total_revenue` for product 42?* | `get_data_product_details`, `query` |
| *Join orders with customers and show the top ten by revenue.* | `query` |
| *Why is my materialized view stale?* | `query_system_tables`, plus `query` for `EXPLAIN ANALYZE` |
| *How much memory is my cluster using?* | `query_system_tables` |
| *Using the `quickstart` cluster, examine the memory usage of `my_mat_view` with skew.* | `query` for `EXPLAIN ANALYZE MEMORY WITH SKEW` |
