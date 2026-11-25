---
title: MCP server configuration
description: "Configuration for the /api/mcp endpoint."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server-unified"
    weight: 30
    identifier: "unified-endpoint-configuration"
    name: "Endpoint configuration"
---

## Available configuration parameters

The following configurations are available for the `/api/mcp` endpoint:

<!-- TODO: verify the enable_mcp_unified default. It is false in src/adapter-dyncfgs/src/dyncfgs.rs today. This page assumes the launch change turns it on. -->

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_mcp_unified` | `true` | Available starting in v26.NN. Enable or disable the `/api/mcp` endpoint. When disabled, requests return HTTP 503 (Service Unavailable). This parameter does not affect `/api/mcp/agent` or `/api/mcp/developer`. |
| `mcp_max_response_size` | `1000000` | Maximum response size in bytes. Queries exceeding this limit return an error. This parameter applies to all MCP endpoints. |

The [`query` tool](/developer-tools/mcp-server/materialize-mcp-tools/#query) on
`/api/mcp` has no parameter of its own. It is available whenever the endpoint
is enabled. To control what a role can read, see [Limit access by
role](/developer-tools/mcp-server/materialize-mcp-access/).

## Disabling the endpoint

{{< tabs >}}

{{< tab "Cloud" >}}

Contact [Materialize support](https://materialize.com/support/) to enable or
disable the `/api/mcp` endpoint for your environment.

{{< /tab >}}

{{< tab "Self-Managed" >}}

Disable the endpoint using one of these methods:

**Option 1: Configuration file**

Set the parameter in your
[system parameters configuration file](/self-managed-deployments/configuration-system-parameters/):

```yaml
system_parameters:
  enable_mcp_unified: "false"
```

**Option 2: Terraform**

Set the parameter via the [Materialize Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed):

```hcl
system_parameters = {
  enable_mcp_unified = "false"
}
```

**Option 3: SQL**

Connect as `mz_system` and run:

```mzsql
ALTER SYSTEM SET enable_mcp_unified = false;
```

{{< note >}}
These parameters are only accessible to the `mz_system` and `mz_support`
roles. Regular database users cannot view or modify them.
{{< /note >}}

{{< /tab >}}

{{< /tabs >}}

## Listener configuration (Self-Managed)

Each HTTP listener serves `/api/mcp` only if its `mcp` route group is enabled.
This route group sits next to the `mcp_agent` and `mcp_developer` route groups.

- **Materialize Operator:** The Operator enables the `mcp` route group on every
  listener that serves `/api/mcp/agent` or `/api/mcp/developer`. An older
  Operator does not know the `mcp` route group, so `/api/mcp` is not served,
  even when `enable_mcp_unified` is on. Upgrade the Operator to v26.NN or
  later.

- **Your own listener configuration file:** A listener without an `mcp` key
  does not serve `/api/mcp`. Add the `mcp` route group to each HTTP listener
  that should serve it, with the same `allowed_roles` as your `mcp_agent` and
  `mcp_developer` route groups. The following example shows only the MCP route
  groups of a listener's `routes` object:

  ```json
  "routes": {
    "mcp_agent": { "enabled": true, "allowed_roles": "Normal" },
    "mcp_developer": { "enabled": true, "allowed_roles": "Normal" },
    "mcp": { "enabled": true, "allowed_roles": "Normal" }
  }
  ```

<!-- TODO: verify the Operator version that first writes the mcp route group (added in #40477 and #40478). -->
<!-- TODO: verify that a custom listener configuration file is a supported Self-Managed setup. No other user doc describes the listener file. -->
