---
title: MCP Server
description: "Connect AI agents to data products and the system catalog through Materialize's built-in materialize-mcp MCP server at /api/mcp."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    name: "MCP server"
    weight: 10
    identifier: "mcp-server-unified"
---

*Available starting in v26.NN*

Materialize provides a built-in `materialize-mcp` Model Context Protocol (MCP)
server at `/api/mcp` (port 6876). The server runs inside Materialize. You do
not need a sidecar process or an external server.

We recommend this server for new setups. It replaces the [MCP server for
agents](/developer-tools/mcp-server/mcp-agent/) (`/api/mcp/agent`) and the [MCP
server for developers](/developer-tools/mcp-server/mcp-developer/)
(`/api/mcp/developer`) with one URL. Those servers are still available.

## Overview

You can connect an MCP-compatible client (such as Claude Code, Claude Cowork,
or Cursor) to the server and ask questions in natural language or SQL:

- *What data products can I query?*
- *What's the `total_revenue` for product 42?*
- *Why is my materialized view stale?*
- *Using the `quickstart` cluster, examine the memory usage of `my_mat_view`.*

The server has three tools:

| Tool | Use it to |
|------|-----------|
| [`list_data_products`](/developer-tools/mcp-server/materialize-mcp-tools/#list_data_products) | List the data products your role can read. |
| [`get_data_product_details`](/developer-tools/mcp-server/materialize-mcp-tools/#get_data_product_details) | Get the columns, cluster, and hydration status of one data product. |
| [`query`](/developer-tools/mcp-server/materialize-mcp-tools/#query) | Run a read-only `SELECT`, `SHOW`, or `EXPLAIN` on data products and the system catalog. |

Every role sees the same three tools. What a tool call can read depends on the
role's privileges and role settings. To limit a role to data products, see
[Limit access by role](/developer-tools/mcp-server/materialize-mcp-access/).

## Connection methods

There are three ways to connect:

- **OAuth**: Your MCP client signs you in through your browser. The agent
  connects as **your user role** with your existing privileges. Available for
  **Cloud** and for **Self-Managed** using
  [SSO](/self-managed-deployments/sso/oidc/).

- **Token-based**: You give the client Base64-encoded credentials (the MCP
  token). The agent connects as the role in the token. Available for **Cloud**,
  **Self-Managed**, and the **Emulator**.

- **No authentication**: Available for the **Emulator** only. Requests run as
  the `anonymous_http_user` role.

To give an agent its own least-privilege role, connect with a token for a
dedicated service account. To set one up, see [Set up the agent query
environment and data
products](/developer-tools/mcp-server/mcp-agent/#set-up-the-agent-query-environment-and-data-products)
and [Create the specific agent
role](/developer-tools/mcp-server/mcp-agent/#step-1-create-the-specific-agent-role).

## Connect to the MCP server

### Step 1. Get your MCP server URL

The MCP server URL is `<baseURL>/api/mcp`.

{{< tabs >}}

{{< tab "Cloud" >}}

The base URL has the format `https://<region-id>.materialize.cloud`.

To find it in the [Materialize Console](https://console.materialize.com/):

1. Click the **Connect** link (lower-left corner) to open the **Connect** modal
   and click on the **MCP Server** tab.

1. In the **Connect your client** section, find the MCP server URL in the code
   block. The base URL is the part before `/api/mcp/...`.

<!-- TODO: verify the Console Connect modal once it shows /api/mcp. Today it lists only the Agent and Developer tabs (console/src/components/connect/connectOptions.ts). -->

{{< /tab >}}

{{< tab "Self-Managed" >}}

- **If your deployment uses TLS**, find the base URL in the Materialize Console
  as described in the **Cloud** tab.

- **If your deployment does not use TLS**, the MCP server URL is
  `http://<host>:6876/api/mcp`. To find `<host>`, see the **Self-Managed** tab
  of [Get connection
  details](/developer-tools/mcp-server/mcp-agent/#step-2-get-connection-details).

OAuth for Self-Managed requires SSO, and your identity provider may need extra
configuration for MCP clients. See [Connecting MCP
clients](/self-managed-deployments/sso/oidc/#connecting-mcp-clients), or with
the advanced SSO stack, [Connect MCP
clients](/self-managed-deployments/sso/advanced/operations/#connect-mcp-clients).

If `/api/mcp` is not served, check your [listener
configuration](/developer-tools/mcp-server/materialize-mcp-config/#listener-configuration-self-managed).

{{< /tab >}}

{{< tab "Emulator" >}}

The MCP server URL is `http://localhost:6876/api/mcp`.

{{< /tab >}}

{{< /tabs >}}

### Step 2. Get your MCP token

*Skip this step if you connect with OAuth, or to the Emulator without
authentication.*

The MCP token is the Base64 encoding of `<role>:<password>`. The token is only
encoded, not encrypted. Treat it like a password.

{{< tabs >}}

{{< tab "Cloud" >}}

When you create an app password in the Materialize Console (**Create New** →
**App Password**), the Console also shows the **MCP Token**. Copy it before you
leave the page. It does not display again.

For an agent, use a [service
account](/security/cloud/users-service-accounts/create-service-accounts/)
instead of a personal app password. A personal app password carries all of
your roles and privileges.

To make a token from an existing app password:

```bash
printf '<user>:<app_password>' | base64
```

{{< /tab >}}

{{< tab "Self-Managed" >}}

Use a login role with a password. For example, to create one:

```mzsql
CREATE ROLE my_agent LOGIN PASSWORD '<your_app_password>';
```

Then encode the credentials:

```bash
printf 'my_agent:<your_app_password>' | base64
```

{{< /tab >}}

{{< tab "Emulator" >}}

The Emulator does not require authentication. To run requests as a specific
role instead of `anonymous_http_user`, create the role and encode it with an
empty password:

```mzsql
CREATE ROLE my_agent;
```

```bash
printf 'my_agent:' | base64
```

{{< /tab >}}

{{< /tabs >}}

### Step 3. Configure your MCP client

{{< warning >}}
When saving your credentials or other sensitive information in a config file, do
**not** commit these files to version control or share them publicly.
{{< /warning >}}

In the following, replace `<baseURL>` with the base URL from [Step
1](#step-1-get-your-mcp-server-url) and `<mcp-token>` with the token from [Step
2](#step-2-get-your-mcp-token).

{{< tabs >}}
{{< tab "Claude Code" >}}

1. Add the server as a [local-scoped
   server](https://code.claude.com/docs/en/mcp#local-scope). The configuration
   is stored in `~/.claude.json`.

   - **OAuth**, or the Emulator without authentication:

     ```sh
     claude mcp add --transport http materialize "<baseURL>/api/mcp"
     ```

     For Self-Managed OAuth with a pre-registered OIDC client, add
     `--client-id <YOUR_CLIENT_ID> --callback-port 8080`. The port must match
     the `http://localhost:<port>/callback` redirect URI on the OIDC client.

   - **Token-based**:

     ```sh
     claude mcp add --transport http materialize "<baseURL>/api/mcp" \
       --header "Authorization: Basic <mcp-token>"
     ```

1. Restart Claude Code. With OAuth, your browser opens on first connection to
   complete sign-in.

{{< /tab >}}

{{< tab "Claude Cowork/Chrome" >}}

Add a custom connector and enter `<baseURL>/api/mcp` as the **Remote MCP server
URL**. The exact steps depend on your Claude plan. See [Get started with custom
connectors using Remote
MCP](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp).

For a token-based connection from Claude Cowork, follow the `mcp-remote` steps
in the **Claude Cowork** tab of [Configure your MCP
client](/developer-tools/mcp-server/mcp-agent/#step-3-configure-your-mcp-client),
and use `<baseURL>/api/mcp` as the URL.

{{< /tab >}}

{{< tab "Cursor" >}}

1. Add the server to your local MCP settings file (`~/.cursor/mcp.json`). If
   the file already has an `mcpServers` object, add the entry to it.

   - **OAuth**, or the Emulator without authentication:

     ```json
     {
       "mcpServers": {
         "materialize": {
           "url": "<baseURL>/api/mcp"
         }
       }
     }
     ```

   - **Token-based**:

     ```json
     {
       "mcpServers": {
         "materialize": {
           "url": "<baseURL>/api/mcp",
           "headers": {
             "Authorization": "Basic <mcp-token>"
           }
         }
       }
     }
     ```

1. Restart Cursor. With OAuth, your browser opens on first connection to
   complete sign-in.

{{< /tab >}}

{{< tab "Generic HTTP" >}}

Any MCP-compatible client can send JSON-RPC 2.0 requests over HTTP POST. Leave
out the `Authorization` header for the Emulator without authentication.

```bash
curl -X POST <baseURL>/api/mcp \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic <mcp-token>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```

This example uses MCP protocol version 2025-11-25. For the headers and
`params._meta` that a 2026-07-28 request adds, see [Protocol
versions](/developer-tools/mcp-server/#protocol-versions).

{{< /tab >}}
{{< /tabs >}}

## Start asking questions

Once connected, you can ask questions like:

| Question | What the agent does |
|----------|---------------------|
| **What data products can I query?** | Calls `list_data_products`. |
| **What's the `total_revenue` for product 42?** | Calls `get_data_product_details` for the cluster and columns, then `query` on that cluster. |
| **Why is my materialized view stale?** | Uses `query` to check materialization lag, hydration, replica health, and source errors in the system catalog. |
| **How much memory is my cluster using?** | Uses `query` to read replica utilization from the system catalog. |

{{< tip >}}
Because `query` can join across objects, consider maintaining an [ontology
table](/transform-data/patterns/ontology/): a curated catalog of the join
relationships in your schema that the agent can query before writing
multi-table SQL.
{{< /tip >}}

## Related pages

- [Available tools](/developer-tools/mcp-server/materialize-mcp-tools/)
- [Limit access by role](/developer-tools/mcp-server/materialize-mcp-access/)
- [Endpoint configuration](/developer-tools/mcp-server/materialize-mcp-config/)
- [MCP Server Troubleshooting](/developer-tools/mcp-server/mcp-server-troubleshooting/)
- [Model Context Protocol (MCP)](https://modelcontextprotocol.io/)
