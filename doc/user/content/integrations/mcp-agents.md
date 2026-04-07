---
title: MCP Server for Agents
description: "Expose real-time data products to AI agents via Materialize's built-in MCP endpoint."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "integrations"
    weight: 23
---

{{< private-preview />}}

Materialize provides a built-in [Model Context Protocol
(MCP)](https://modelcontextprotocol.io/) endpoint that lets AI agents discover
and query your real-time data products over HTTP. The MCP interface is served
directly by the database; no sidecar process or external server is required.

**Endpoint:** `POST /api/mcp/agents` (HTTP port, default `6876`)

The endpoint uses [JSON-RPC 2.0](https://www.jsonrpc.org/specification) over
HTTP POST and supports the MCP `initialize`, `tools/list`, and `tools/call`
methods.

## Connect to the MCP server

### Step 1. Get your credentials

{{< tabs >}}

{{< tab "Cloud" >}}

You need your **email address** (or service account name) and an
[app password](/security/cloud/users-service-accounts/create-service-accounts/).

To create an app password:

1. Log in to the [Materialize Console](https://console.materialize.com/).
2. Go to **Account** > **App Passwords** > **New app password**.
3. Copy the generated password — you won't be able to see it again.

{{< /tab >}}

{{< tab "Self-Managed" >}}

Create a login role for MCP access:

```mzsql
CREATE ROLE my_agent LOGIN PASSWORD 'your_password_here';
```

Use `my_agent` as the user and the password you set. See
[Authentication and access control](#rbac) for setting up privileges.

{{< /tab >}}

{{< /tabs >}}

### Step 2. Encode your credentials in Base64

MCP clients send credentials as a Base64-encoded `user:password` string. Run
this in your terminal:

```bash
printf '<user>:<app_password>' | base64
```

For example:

```bash
printf 'svc-mcp-agent@mycompany.com:my_app_password_here' | base64
# Output: c3ZjLW1jcC1hZ2VudEBteWNvbXBhbnkuY29tOm15X2FwcF9wYXNzd29yZF9oZXJl
```

### Step 3. Get your hostname

{{< tabs >}}

{{< tab "Cloud" >}}

Find your region hostname in the [Materialize Console](https://console.materialize.com/)
under **Connect**. It looks like:

```
<region-id>.materialize.cloud
```

Your full MCP endpoint URL is:

```
https://<region-id>.materialize.cloud/api/mcp/agents
```

{{< /tab >}}

{{< tab "Self-Managed" >}}

Your MCP endpoint URL is:

```
http://<host>:6876/api/mcp/agents
```

Where `<host>` is the load balancer address for your Materialize deployment.
To find it, run the Terraform output command for your cloud provider:

```bash
# AWS
terraform output -raw nlb_dns_name

# GCP
terraform output -raw balancerd_load_balancer_ip

# Azure
terraform output -raw balancerd_load_balancer_ip
```

For local [kind](/self-managed-deployments/installation/install-on-local-kind/)
clusters, use port forwarding:

```bash
kubectl port-forward svc/<instance-name>-balancerd 6876:6876 -n materialize-environment
```

Then connect to `http://localhost:6876/api/mcp/agents`.

{{< /tab >}}

{{< /tabs >}}

### Step 4. Configure your MCP client

{{< tabs >}}

{{< tab "Claude Code" >}}

Create a `.mcp.json` file in your project directory:

```json
{
  "mcpServers": {
    "materialize-agents": {
      "type": "http",
      "url": "https://<region-id>.materialize.cloud/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64-token>"
      }
    }
  }
}
```

{{< /tab >}}

{{< tab "Claude Desktop" >}}

Add to your Claude Desktop MCP configuration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "materialize-agents": {
      "url": "https://<region-id>.materialize.cloud/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64-token>"
      }
    }
  }
}
```

{{< /tab >}}

{{< tab "Cursor" >}}

In Cursor's MCP settings (`.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "materialize-agents": {
      "url": "https://<region-id>.materialize.cloud/api/mcp/agents",
      "headers": {
        "Authorization": "Basic <base64-token>"
      }
    }
  }
}
```

{{< /tab >}}

{{< tab "Generic HTTP" >}}

Any MCP-compatible client can connect by sending JSON-RPC 2.0 requests:

```bash
curl -X POST https://<region-id>.materialize.cloud/api/mcp/agents \
  -H "Content-Type: application/json" \
  -H "Authorization: "Basic <base64-token>"" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```

{{< /tab >}}

{{< /tabs >}}

### Enabling the endpoint

The agents endpoint is disabled by default.

{{< tabs >}}

{{< tab "Cloud" >}}

Contact [Materialize support](https://materialize.com/docs/support/) to enable
the MCP agents endpoint for your environment. The following parameters control
MCP behavior:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_mcp_agents` | `false` | Enable or disable the `/api/mcp/agents` endpoint. |
| `enable_mcp_agents_query_tool` | `false` | Show or hide the `query` tool on the agents endpoint. |
| `mcp_max_response_size` | `1000000` | Maximum response size in bytes. Queries exceeding this limit return an error. |

{{< /tab >}}

{{< tab "Self-Managed" >}}

Enable the endpoint using one of these methods:

**Option 1: Configuration file**

Set the parameter in your
[system parameters configuration file](/self-managed-deployments/configuration-system-parameters/):

```yaml
system_parameters:
  enable_mcp_agents: "true"
```

**Option 2: Terraform**

Set the parameter via the [Materialize Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed):

```hcl
system_parameters = {
  enable_mcp_agents = "true"
}
```

**Option 3: SQL**

Connect as `mz_system` and run:

```mzsql
ALTER SYSTEM SET enable_mcp_agents = true;
```

{{< note >}}
These parameters are only accessible to the `mz_system` and `mz_support`
roles. Regular database users cannot view or modify them.
{{< /note >}}

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_mcp_agents` | `false` | Enable or disable the `/api/mcp/agents` endpoint. |
| `enable_mcp_agents_query_tool` | `false` | Show or hide the `query` tool on the agents endpoint. |
| `mcp_max_response_size` | `1000000` | Maximum response size in bytes. Queries exceeding this limit return an error. |

{{< /tab >}}

{{< /tabs >}}

When the endpoint is disabled, requests return HTTP 503 (Service Unavailable).

## Authentication and access control {#rbac}

Accessing the MCP endpoint requires [basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme),
just as connecting via a SQL client (e.g. `psql`). The authenticated role
determines which data products are visible based on RBAC privileges.

{{< tabs >}}

{{< tab "Cloud" >}}

Use the credentials of a Materialize user or
[service account](/security/cloud/users-service-accounts/create-service-accounts/):

* **User ID:** Your email address or service account name.
* **Password:** An [app password](/security/cloud/users-service-accounts/create-service-accounts/).

For production use, we recommend creating a dedicated service account and
granting it a role with limited privileges (see [Required privileges](#required-privileges)).

{{< /tab >}}

{{< tab "Self-Managed" >}}

Create a functional role for MCP privileges, then assign it to a login role:

```mzsql
CREATE ROLE mcp_agent;
CREATE ROLE my_agent LOGIN PASSWORD 'secret';
GRANT mcp_agent TO my_agent;
```

Authenticate using the login role credentials (`my_agent`). You can create
additional login roles and grant them the same `mcp_agent` role as needed.

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
CREATE CLUSTER mcp_cluster SIZE '25cc';
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

CREATE INDEX payment_status_order_id_idx ON mcp_schema.payment_status (order_id);
```

### 3. Add a comment

The comment on the **index** becomes the data product's description. Column
comments on the underlying view become parameter descriptions. Write comments
that help a language model understand **when** and **how** to use the tool.

```mzsql
COMMENT ON INDEX mcp_schema.payment_status_order_id_idx IS
  'Given an order ID, return the current payment status and last update time.
   Use this tool to drive user-facing payment tracking.';

COMMENT ON COLUMN mcp_schema.payment_status.order_id IS
  'The unique identifier for the order';
```

### 4. Verify your data products

To confirm which data products are visible to your agent role, run:

```mzsql
SET ROLE mcp_agent;
SELECT * FROM mz_internal.mz_mcp_data_products;
```

If a data product is missing, check that:

- The view has an index with a [comment](/sql/comment-on/).
- The role has `USAGE` on the database, schema, and cluster.
- The role has `SELECT` on the view.

### Required privileges

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

Create a functional role for privileges, then assign it to a login role:

```mzsql
-- Functional role (cannot log in, holds privileges)
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

-- Login role (used for authentication)
CREATE ROLE my_agent LOGIN PASSWORD 'secret';
GRANT mcp_agent TO my_agent;
```

You can create additional login roles and grant them the same `mcp_agent` role
as needed.

{{< /tab >}}

{{< /tabs >}}

If any privilege is missing, the data product will not appear in the agent's
tool list.

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
The `query` tool is disabled by default. To enable it, set the
`enable_mcp_agents_query_tool` system parameter to `true`.
{{< /note >}}

Execute a SQL `SELECT` statement against your data products.

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
        "text": "[\n  [\n    42,\n    \"shipped\"\n  ]\n]"
      }
    ],
    "isError": false
  }
}
```

## Related pages

- [MCP Server for Observability](/integrations/mcp-observatory/)
- [Coding Agent Skills](/integrations/coding-agent-skills/)
- [CREATE INDEX](/sql/create-index)
- [COMMENT ON](/sql/comment-on)
- [CREATE ROLE](/sql/create-role)
- [GRANT PRIVILEGE](/sql/grant-privilege)
