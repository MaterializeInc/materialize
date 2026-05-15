---
title: MCP Server for Agents
description: "Expose real-time data products to AI agents via Materialize's built-in MCP endpoint."
make_table_row_headers_searchable: true
draft: true
menu:
  main:
    parent: "mcp-server"
    weight: 20
    identifier: "mcp-server-agent"
---

{{< public-preview />}}

Materialize provides a built-in [Model Context Protocol
(MCP)](https://modelcontextprotocol.io/) endpoint for discovery and querying of
data products. The MCP interface is served directly by the database; no sidecar
process or external server is required.

## Overview

**Endpoint**: `/api/mcp/agent`

- Lets AI agents discover and query your real-time data products over HTTP.

- Uses [JSON-RPC 2.0](https://www.jsonrpc.org/specification) over HTTP (default
port 6876)

- Supports the `initialize`, `tools/list`, and `tools/call` methods.

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

## Define and document data products for discovery

The MCP server allows agents to discover and query documented (i.e.,
[commented](/sql/comment-on/)) data products. In Materialize, you can make a
materialized view discoverable by indexing the materialized view and adding
[comments](/sql/comment-on/) to the **index**. You can, optionally, add comments
columns of the materialized view.

- The [comments on](/sql/comment-on/) the **index** makes the materialized view
  discoverable and becomes its description.

- The [comment on](/sql/comment-on/) the view's columns become parameter
  descriptions.

### 1. Create a dedicated cluster and schema

Use a dedicated [cluster](/concepts/clusters/) to isolate agent workloads. This
ensures agent queries do not consume resources from your other clusters, and
limits visibility to only the data products you choose to expose.

```mzsql
CREATE CLUSTER mcp_cluster SIZE '25cc';
```

### 2. Index a materialized view

Every indexed column becomes a required input parameter in the tool's schema.

```mzsql
SET CLUSTER mcp_cluster;

CREATE INDEX payment_status_order_id_idx ON mcp_schema.payment_status (order_id);
```

### 3. Comment on the index and columns.

The [comment on](/sql/comment-on/) the **index** becomes the data product's description.

The [comment on](/sql/comment-on/) the view's columns become parameter
descriptions.

Write comments that help a language model understand **when** and **how** to use
the tool.

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

## Connect to the MCP server

### Step 1. Get connection details

{{< tabs >}}

{{< tab "Cloud" >}}

1. Log in to the [Materialize Console](https://console.materialize.com/).
1. Click the **Connect** link to open the [**Connect**
    modal](/console/connect/).
1. Click on the **MCP Server** tab.

1. Select **Agent** for your Endpoint.

1. To get your base64-encoded token:
   - To use an existing app password, generate a base64-encoded token.

     ```bash
     printf '<user>:<app_password>' | base64
     ```

   - To create a new app password to use, click on the **Create app password**
     to generate a new app password and token for MCP Server. **Copy the app
     password and token** as they cannot be displayed again.

{{< /tab >}}
{{< tab "Self-Managed" >}}

1. You can connect using either an existing or new login role with password.

   - To use an existing role, go to the next step.
   - To create a new login role with password:

     ```mzsql
     CREATE ROLE my_agent LOGIN PASSWORD 'your_password_here';
     ```

1. Encode your credentials in Base64. MCP clients send credentials as a
   Base64-encoded `user:password` string.

   ```bash
   printf '<user>:<app_password>' | base64
   ```

   For example:
   ```bash
   printf 'svc-mcp-agent@mycompany.com:my_app_password_here' | base64
   # Output: c3ZjLW1jcC1hZ2VudEBteWNvbXBhbnkuY29tOm15X2FwcF9wYXNzd29yZF9oZXJl
   ```

1. Find your deployment's host name to use in the MCP endpoint URL; that is,
   your MCP endpoint URL is:

   ```
   http://<host>:6876/api/mcp/agent
   ```

   - For your Self-Managed Materialize deployment in AWS/GCP/Azure, the `<host>`
   is the load balancer address. If [deployed via
   Terraform](/self-managed-deployments/installation/#install-using-terraform-modules),
   run the Terraform output command for your cloud provider:

     ```bash
     # AWS
     terraform output -raw nlb_dns_name

     # GCP
     terraform output -raw balancerd_load_balancer_ip

     # Azure
     terraform output -raw balancerd_load_balancer_ip
     ```

   - For local
     [kind](/self-managed-deployments/installation/install-on-local-kind/)
     clusters, use port forwarding and use `localhost` for `<host>`:

     ```bash
     kubectl port-forward svc/<instance-name>-balancerd 6876:6876 -n materialize-environment
     ```


{{< /tab >}}

{{< /tabs >}}


### Step 2. Configure your MCP client

{{< tip >}}
You can copy the `.json` content from the **MCP Server** tab in the Console's
**Connect** modal.
- Replace `<baseURL>` with your value.
  - If Cloud, there is nothing to replace as the `.json` content
    includes your specific baseURL value of the form
    `https://<region-id>.materialize.cloud`.
  - If Self-Managed, replace with the `http://<host>:6876` found in the previous
    step.
- Replace `<base64-token>` with your value.
{{< /tip >}}

{{< tabs >}}

{{< tab "Claude Code" >}}

Create a `.mcp.json` file in your project directory:

```json
{
  "mcpServers": {
    "materialize-agent": {
      "type": "http",
      "url": "https://<region-id>.materialize.cloud/api/mcp/agent",
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
    "materialize-agent": {
      "url": "https://<region-id>.materialize.cloud/api/mcp/agent",
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
    "materialize-agent": {
      "url": "https://<region-id>.materialize.cloud/api/mcp/agent",
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
curl -X POST <baseURL>/api/mcp/agent \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic <base64-token>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```

{{< /tab >}}

{{< /tabs >}}



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

## Related pages

- [MCP Server for Developers](/integrations/mcp-server/mcp-developer/)
- [Coding Agent Skills](/integrations/coding-agent-skills/)
- [CREATE INDEX](/sql/create-index)
- [COMMENT ON](/sql/comment-on)
- [CREATE ROLE](/sql/create-role)
- [GRANT PRIVILEGE](/sql/grant-privilege)
