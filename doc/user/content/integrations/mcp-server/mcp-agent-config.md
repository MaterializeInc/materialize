---
title: Agent endpoint configuration
description: "Configuration for /api/mcp/agent endpoint."
make_table_row_headers_searchable: true
draft: true
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
| `enable_mcp_agent_query_tool` | `true` | Show or hide the `query` tool on the agent endpoint. |
| `mcp_max_response_size` | `1000000` | Maximum response size in bytes. Queries exceeding this limit return an error. |

## Example: Enable endpoint

To enable the `/api/mcp/agent` endpoint:

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
  enable_mcp_agent: "true"
```

**Option 2: Terraform**

Set the parameter via the [Materialize Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed):

```hcl
system_parameters = {
  enable_mcp_agent = "true"
}
```

**Option 3: SQL**

Connect as `mz_system` and run:

```mzsql
ALTER SYSTEM SET enable_mcp_agent = true;
```

{{< note >}}
These parameters are only accessible to the `mz_system` and `mz_support`
roles. Regular database users cannot view or modify them.
{{< /note >}}

{{< /tab >}}

{{< /tabs >}}
