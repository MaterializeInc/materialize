---
title: Built-in MCP Server
description: "Connect AI agents directly to Materialize's built-in MCP endpoints for real-time data access."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "integrations"
    weight: 23
---

Materialize includes built-in [Model Context Protocol
(MCP)](https://modelcontextprotocol.io/) endpoints that let AI agents discover
and query your real-time data products over HTTP. No sidecar process or external
server is required — the MCP interface is served directly by the database.

Two endpoints are available:

| Endpoint | Path | Purpose |
|----------|------|---------|
| **Agents** | `/api/mcp/agents` | Discover and read data products (indexed views with comments). Designed for customer-facing AI agents. |
| **Observatory** | `/api/mcp/observatory` | Query `mz_*` system catalog tables for troubleshooting and observability. |

Both endpoints speak [JSON-RPC 2.0](https://www.jsonrpc.org/specification) over
HTTP POST and support the MCP `initialize`, `tools/list`, and `tools/call`
methods.

## Authentication

Accessing the MCP endpoints requires [basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme),
just as connecting via a SQL client (e.g. `psql`). The authenticated role
determines which data products are visible based on [RBAC privileges](#rbac).

{{< tabs >}}

{{< tab "Cloud" >}}

Use the credentials of a Materialize user or
[service account](/security/cloud/users-service-accounts/create-service-accounts/):

* **User ID:** Your email address or service account name.
* **Password:** An [app password](/security/cloud/users-service-accounts/create-service-accounts/).

```
https://<region-host>/api/mcp/agents
```

For production use, we recommend creating a dedicated service account and
granting it a role with [limited privileges](#rbac).

{{< /tab >}}

{{< tab "Self-Managed" >}}

Use the credentials of a Materialize role with `LOGIN`:

```mzsql
CREATE ROLE mcp_agent LOGIN PASSWORD 'secret';
```

The MCP endpoints are available on the HTTP listener port (default `6876`):

```
http://<host>:6876/api/mcp/agents
```

{{< /tab >}}

{{< /tabs >}}

## Define and document data products for your agents

The MCP server allows agents to discover and query documented data products. In
Materialize, you can make a data product discoverable to agents by creating a
materialized view, adding a [comment](/sql/comment-on/), and optionally adding
an [index](/concepts/indexes/) for faster lookups. A comment is required, so
agents understand the purpose of each data product.

### 1. Create a dedicated cluster and schema

Use a dedicated [cluster](/concepts/clusters/) and schema to isolate agent
workloads. This ensures agent queries do not consume resources from your other
clusters, and limits visibility to only the data products you choose to expose.

```mzsql
CREATE CLUSTER mcp_cluster SIZE 'xsmall';
CREATE SCHEMA mcp_schema;
```

### 2. Create and index a view

Create a materialized view in the dedicated schema. We recommend installing an
[index](/concepts/indexes/) on the dedicated cluster for faster lookups. Every
indexed column becomes a required input parameter in the tool's schema.

```mzsql
SET cluster = mcp_cluster;

CREATE MATERIALIZED VIEW mcp_schema.payment_status AS
  SELECT order_id, user_id, status, updated_at
  FROM orders
  JOIN payments USING (order_id);

CREATE INDEX ON mcp_schema.payment_status (order_id);
```

### 3. Add a comment

The top-level comment becomes the tool's description. Column comments become
parameter descriptions. Write comments that help a language model understand
**when** and **how** to use the tool.

```mzsql
COMMENT ON MATERIALIZED VIEW mcp_schema.payment_status IS
  'Given an order ID, return the current payment status and last update time.
   Use this tool to drive user-facing payment tracking.';

COMMENT ON COLUMN mcp_schema.payment_status.order_id IS
  'The unique identifier for the order';
```

### 4. Create a role and permissions for your agent {#rbac}

The role used to authenticate with the MCP endpoint must have:

- `USAGE` on the database and schema containing the view.
- `SELECT` on the view.
- `USAGE` on the cluster where the index is installed.

Lock the role to the dedicated cluster and schema so that all agent queries
are isolated.

{{< tabs >}}

{{< tab "Cloud" >}}

On Cloud, create a functional role and grant it to a
[service account](/security/cloud/users-service-accounts/create-service-accounts/).
The service account's app password is used for MCP authentication.

```mzsql
CREATE ROLE mcp_agent;

GRANT USAGE ON DATABASE materialize TO mcp_agent;
GRANT USAGE ON SCHEMA mcp_schema TO mcp_agent;
GRANT SELECT ON ALL TABLES IN SCHEMA mcp_schema TO mcp_agent;
GRANT USAGE ON CLUSTER mcp_cluster TO mcp_agent;

-- Lock the role to the dedicated cluster and schema.
-- This ensures all queries from this role run on mcp_cluster
-- and only see objects in mcp_schema by default.
ALTER ROLE mcp_agent SET cluster TO mcp_cluster;
ALTER ROLE mcp_agent SET search_path TO mcp_schema;

-- Grant the role to your service account
GRANT mcp_agent TO '<service-account-name>';
```

{{< /tab >}}

{{< tab "Self-Managed" >}}

```mzsql
CREATE ROLE mcp_agent LOGIN PASSWORD 'secret';

GRANT USAGE ON DATABASE materialize TO mcp_agent;
GRANT USAGE ON SCHEMA mcp_schema TO mcp_agent;
GRANT SELECT ON ALL TABLES IN SCHEMA mcp_schema TO mcp_agent;
GRANT USAGE ON CLUSTER mcp_cluster TO mcp_agent;

-- Lock the role to the dedicated cluster and schema.
-- This ensures all queries from this role run on mcp_cluster
-- and only see objects in mcp_schema by default.
ALTER ROLE mcp_agent SET cluster TO mcp_cluster;
ALTER ROLE mcp_agent SET search_path TO mcp_schema;
```

{{< /tab >}}

{{< /tabs >}}

If any privilege is missing, the data product will not appear in the agent's
tool list.

## Agents endpoint

**`POST /api/mcp/agents`**

The agents endpoint exposes your data products as MCP tools. It provides the
following tools:

### `get_data_products`

Discover all available data products. Returns a list of names and descriptions.

**Parameters:** None.

### `get_data_product_details`

Get the full schema (columns, types, index keys) for a specific data product.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Exact name from the `get_data_products` list. |

### `read_data_product`

Read rows from a data product.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Fully-qualified name, e.g. `"materialize"."public"."payment_status"`. |
| `limit` | integer | No | Maximum rows to return. Default: 500, max: 1000. |
| `cluster` | string | No | Cluster override. If omitted, uses the cluster from the catalog. |

### `query`

{{< note >}}
The `query` tool is disabled by default. To enable it, set the
`enable_mcp_agents_query_tool` system parameter to `true`.
{{< /note >}}

Execute a SQL `SELECT` statement against your data products.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cluster` | string | Yes | Exact cluster name from the data product details. |
| `sql_query` | string | Yes | PostgreSQL-compatible `SELECT` statement. |

## Observatory endpoint

**`POST /api/mcp/observatory`**

The observatory endpoint gives agents read-only access to the Materialize
system catalog for troubleshooting and observability.

### `query_system_catalog`

Execute a SQL query restricted to `mz_*` system tables.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql_query` | string | Yes | `SELECT` query using only `mz_*` system tables. |

## Client configuration

Replace `<host>` with your Materialize hostname (or region host for Cloud) and
encode your credentials in Base64:

```bash
printf 'user:app_password' | base64
```

### Claude Desktop

Add the following to your Claude Desktop MCP configuration
(`claude_desktop_config.json`):

{{< tabs >}}

{{< tab "Cloud" >}}

```json
{
  "mcpServers": {
    "materialize": {
      "url": "https://<region-host>/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64(user:app_password)>"
      }
    }
  }
}
```

{{< /tab >}}

{{< tab "Self-Managed" >}}

```json
{
  "mcpServers": {
    "materialize": {
      "url": "http://<host>:6876/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64(username:password)>"
      }
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

### Cursor

In Cursor's MCP settings (`.cursor/mcp.json`):

{{< tabs >}}

{{< tab "Cloud" >}}

```json
{
  "mcpServers": {
    "materialize-agents": {
      "url": "https://<region-host>/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64(user:app_password)>"
      }
    },
    "materialize-observatory": {
      "url": "https://<region-host>/api/mcp/observatory",
      "headers": {
        "Authorization": "Basic <base64(user:app_password)>"
      }
    }
  }
}
```

{{< /tab >}}

{{< tab "Self-Managed" >}}

```json
{
  "mcpServers": {
    "materialize-agents": {
      "url": "http://<host>:6876/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64(username:password)>"
      }
    },
    "materialize-observatory": {
      "url": "http://<host>:6876/api/mcp/observatory",
      "headers": {
        "Authorization": "Basic <base64(username:password)>"
      }
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

### Claude Code

In your project's `.mcp.json`:

{{< tabs >}}

{{< tab "Cloud" >}}

```json
{
  "mcpServers": {
    "materialize-agents": {
      "type": "http",
      "url": "https://<region-host>/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64(user:app_password)>"
      }
    },
    "materialize-observatory": {
      "type": "http",
      "url": "https://<region-host>/api/mcp/observatory",
      "headers": {
        "Authorization": "Basic <base64(user:app_password)>"
      }
    }
  }
}
```

{{< /tab >}}

{{< tab "Self-Managed" >}}

```json
{
  "mcpServers": {
    "materialize-agents": {
      "type": "http",
      "url": "http://<host>:6876/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64(username:password)>"
      }
    },
    "materialize-observatory": {
      "type": "http",
      "url": "http://<host>:6876/api/mcp/observatory",
      "headers": {
        "Authorization": "Basic <base64(username:password)>"
      }
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

### Generic HTTP client

Any MCP-compatible client can connect by sending JSON-RPC 2.0 requests:

{{< tabs >}}

{{< tab "Cloud" >}}

```bash
curl -X POST https://<region-host>/api/mcp/agents \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic <base64(user:app_password)>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```

{{< /tab >}}

{{< tab "Self-Managed" >}}

```bash
curl -X POST http://<host>:6876/api/mcp/agents \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic <base64(username:password)>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```

{{< /tab >}}

{{< /tabs >}}

## Enabling / Disabling the MCP endpoints

MCP endpoints can be toggled at runtime using system parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_mcp_agents` | `true` | Enable or disable the `/api/mcp/agents` endpoint. |
| `enable_mcp_observatory` | `true` | Enable or disable the `/api/mcp/observatory` endpoint. |
| `enable_mcp_agents_query_tool` | `false` | Show or hide the `query` tool on the agents endpoint. |

```mzsql
-- Disable the agents endpoint
ALTER SYSTEM SET enable_mcp_agents = false;

-- Enable the query tool
ALTER SYSTEM SET enable_mcp_agents_query_tool = true;
```

When an endpoint is disabled, requests return HTTP 503 (Service Unavailable).

## Related Pages

- [MCP Server (Python)](/integrations/llm/) — standalone Python MCP server
  for `stdio` and `sse` transports
- [Coding Agent Skills](/integrations/coding-agent-skills/)
- [CREATE INDEX](/sql/create-index)
- [COMMENT ON](/sql/comment-on)
- [CREATE ROLE](/sql/create-role)
- [GRANT PRIVILEGE](/sql/grant-privilege)
