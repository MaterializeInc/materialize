---
title: MCP Server for Observability
description: "Query Materialize system catalog tables for troubleshooting and observability via the built-in MCP endpoint."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "integrations"
    weight: 24
---

{{< private-preview />}}

Materialize provides a built-in [Model Context Protocol
(MCP)](https://modelcontextprotocol.io/) endpoint that gives AI agents read-only
access to the system catalog (`mz_*` tables) for troubleshooting and
observability. The MCP interface is served directly by the database; no sidecar
process or external server is required.

**Endpoint:** `POST /api/mcp/observatory` (HTTP port, default `6876`)

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

Use the credentials of a login role with access to system catalog tables.

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
https://<region-id>.materialize.cloud/api/mcp/observatory
```

{{< /tab >}}

{{< tab "Self-Managed" >}}

Use your Materialize host and the HTTP port (default `6876`):

```
http://<host>:6876/api/mcp/observatory
```

{{< /tab >}}

{{< /tabs >}}

### Step 4. Configure your MCP client

{{< tabs >}}

{{< tab "Claude Code" >}}

Create a `.mcp.json` file in your project directory:

```json
{
  "mcpServers": {
    "materialize-observatory": {
      "type": "http",
      "url": "https://<region-id>.materialize.cloud/api/mcp/observatory",
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
    "materialize-observatory": {
      "url": "https://<region-id>.materialize.cloud/api/mcp/observatory",
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
    "materialize-observatory": {
      "url": "https://<region-id>.materialize.cloud/api/mcp/observatory",
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
curl -X POST https://<region-id>.materialize.cloud/api/mcp/observatory \
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

The observatory endpoint is disabled by default.

{{< tabs >}}

{{< tab "Cloud" >}}

Contact [Materialize support](https://materialize.com/docs/support/) to enable
the MCP observatory endpoint for your environment. The following parameters
control MCP behavior:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_mcp_observatory` | `false` | Enable or disable the `/api/mcp/observatory` endpoint. |
| `mcp_max_response_size` | `1000000` | Maximum response size in bytes. Queries exceeding this limit return an error. |

{{< /tab >}}

{{< tab "Self-Managed" >}}

Enable the endpoint by setting system parameters in your
[configuration file](/self-managed-deployments/configuration-system-parameters/):

```yaml
system_parameters:
  enable_mcp_observatory: "true"
```

Or via the [Materialize Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed):

```hcl
system_parameters = {
  enable_mcp_observatory = "true"
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_mcp_observatory` | `false` | Enable or disable the `/api/mcp/observatory` endpoint. |
| `mcp_max_response_size` | `1000000` | Maximum response size in bytes. Queries exceeding this limit return an error. |

{{< /tab >}}

{{< /tabs >}}

When the endpoint is disabled, requests return HTTP 503 (Service Unavailable).

## What you can ask

After connecting an MCP-compatible AI agent (such as Claude Code, Claude
Desktop, or Cursor) to the observatory endpoint, you can ask natural language
questions like:

- **"Why is my materialized view stale?"** — The agent checks materialization
  lag, hydration status, replica health, and source errors.
- **"Why is my cluster running out of memory?"** — The agent checks replica
  utilization, identifies the largest dataflows, and finds optimization
  opportunities via the built-in index advisor.
- **"Has my source finished snapshotting yet?"** — The agent checks source
  statistics and status.
- **"How much memory is my cluster using?"** — The agent checks replica
  utilization metrics across all clusters.
- **"What's the health of my environment?"** — The agent checks replica
  statuses, source and sink health, and resource utilization.
- **"What can I optimize to save costs?"** — The agent queries the index
  advisor for materialized views that can be dematerialized and indexes that
  can be dropped.

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

- [MCP Server for Agents](/integrations/mcp-agents/)
- [Coding Agent Skills](/integrations/coding-agent-skills/)
