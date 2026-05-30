---
title: Agent endpoint configuration
description: "Configuration for /api/mcp/agent endpoint."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server-agent"
    weight: 20
    identifier: "agent-endpoint-configuration"
---

## Available configuration parameters

The following configurations are available for the `/api/mcp/agent` endpoint:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_mcp_agent` | `true` | Enable or disable the `/api/mcp/agent` endpoint. When disabled, requests return `HTTP 503 (Service Unavailable)`.|
| `enable_mcp_agent_query_tool` <a name="enable_mcp_agent_query_tool"></a> | `true` | Enable or disable the [`query` tool](/integrations/mcp-server/mcp-agent-tools/#query), which allows for queries with joins. Enabled by default; to confine agents to user objects (and block system catalog access), set [`restrict_to_user_objects`](/integrations/mcp-server/mcp-agent-tools/#restrict-to-user-objects) on each agent role. {{< include-headless "/headless/mcp-agent-query-tool-warning" >}}|
| `mcp_max_response_size` | `1000000` | Maximum response size in bytes. Queries exceeding this limit return an error. |

## Disabling the endpoint

The `materialize-agent` endpoint is enabled by default. To disable it:

{{< tabs >}}

{{< tab "Cloud" >}}

Contact [Materialize support](https://materialize.com/docs/support/) to
enable/disable the MCP agent endpoint for your environment.
{{< /tab >}}

{{< tab "Self-Managed" >}}

Enable the endpoint using one of these methods:

**Option 1: Configuration file**

Set the parameter in your
[system parameters configuration file](/self-managed-deployments/configuration-system-parameters/):

```yaml
system_parameters:
  enable_mcp_agent: "false"
```

**Option 2: Terraform**

Set the parameter via the [Materialize Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed):

```hcl
system_parameters = {
  enable_mcp_agent = "false"
}
```

**Option 3: SQL**

Connect as `mz_system` and run:

```mzsql
ALTER SYSTEM SET enable_mcp_agent = false;
```

{{< note >}}
These parameters are only accessible to the `mz_system` and `mz_support`
roles. Regular database users cannot view or modify them.
{{< /note >}}

{{< /tab >}}

{{< /tabs >}}
