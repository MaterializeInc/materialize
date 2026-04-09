---
title: MCP Server for Developers
description: "Query Materialize system catalog tables for troubleshooting and observability via the built-in MCP developer endpoint."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    weight: 25
    identifier: "mcp-server-developer"
---

{{< public-preview />}}

Materialize provides a built-in [Model Context Protocol
(MCP)](https://modelcontextprotocol.io/) endpoint for troubleshooting and
observability. The MCP interface is served directly by the database; no sidecar
process or external server is required.

## Overview

**Endpoint:** `/api/mcp/developer`

- Provides read-only access to the system catalog (`mz_*` tables).
- Uses [JSON-RPC 2.0](https://www.jsonrpc.org/specification) over HTTP POST
(default port 6876).
- Supports the MCP `initialize`, `tools/list`, and `tools/call` methods.

## Connect to the MCP server

### Step 1. Get connection details

{{< tabs >}}

{{< tab "Cloud" >}}

1. Log in to the [Materialize Console](https://console.materialize.com/).
1. Click the **Connect** link to open the [**Connect**
    modal](/console/connect/).
1. Click on the **MCP Server** tab.

1. Select **Developer** for your Endpoint.

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
   http://<host>:6876/api/mcp/developer
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
    "materialize-developer": {
      "type": "http",
      "url": "<baseURL>/api/mcp/developer",
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
    "materialize-developer": {
      "url": "<baseURL>/api/mcp/developer",
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
    "materialize-developer": {
      "url": "<baseURL>/api/mcp/developer",
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
curl -X POST <baseURL>/api/mcp/developer \
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

## What you can ask

After connecting an MCP-compatible AI agent (such as Claude Code, Claude
Desktop, or Cursor) to the developer endpoint, you can ask natural language
questions like:

| Question | What the agent does |
|----------|---------------------|
| **Why is my materialized view stale?** | Checks materialization lag, hydration status, replica health, and source errors. |
| **Why is my cluster running out of memory?** | Checks replica utilization, identifies the largest dataflows, and finds optimization           opportunities via the built-in index advisor. |
| **Has my source finished snapshotting yet?** | Checks source statistics and status. |
| **How much memory is my cluster using?** | Checks replica utilization metrics across all clusters. |
| **What's the health of my environment?** | Checks replica statuses, source and sink health, and resource utilization. |
| **What can I optimize to save costs?** | Queries the index advisor for materialized views that can be dematerialized and indexes that can be  dropped. |

The agent translates these questions into the appropriate system catalog
queries, runs them via the `query_system_catalog` tool, and synthesizes the
results.

## Tools

### `query_system_catalog`

Execute a read-only SQL query restricted to system catalog tables (`mz_*`,
`pg_catalog`, `information_schema`).

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

## Related pages

- [Developer endpoint
  configuration](/integrations/mcp-server/mcp-developer-config/)
- [Troubleshooting](/integrations/mcp-server/mcp-server-troubleshooting/)
- [Coding Agent Skills](/integrations/coding-agent-skills/)
