---
title: MCP Server for Developers
description: "Query Materialize system catalog tables for troubleshooting and observability via the built-in materialize-developer MCP server."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    weight: 25
    identifier: "mcp-server-developer"
---

{{< public-preview />}}

Materialize provides a built-in `materialize-developer` Model Context Protocol
(MCP) server (`/api/mcp/developer`, port 6876) for troubleshooting and
observability. The server is provided directly by Materialize; no sidecar
process or external server is required.

## Overview

You can connect an MCP-compatible client (such as Claude Code, Claude Cowork,
or Cursor) to the MCP server to:

- Ask questions about the Materialize system
  - *Why is my materialized view stale?*
  - *How much memory is my cluster using?*
- Run queries on your objects (Available starting in v26.30)
  - *Using the quickstart cluster, SELECT * from my_mat_view;*
  - *Using the quickstart cluster, examine the memory usage of my_mat_view with skew.*

## Connect to the MCP server

There are two ways to authenticate to the `materialize-developer` MCP server:

- **OAuth**: Starting in v26.30, your MCP client can sign you in through your
  browser; no token to generate or store. Available for **Cloud** and for
  **Self-Managed** [using SSO](/security/self-managed/sso/).

- **Token-based**: You provide Base64-encoded credentials (the MCP token) to the
  client. Available for **Cloud**, **Self-Managed**, and the **Emulator**.

### Method 1: OAuth

*Available starting in v26.30*

{{< note >}}

The OAuth method is available for **Cloud** and for **Self-Managed** deployments
using [SSO](/security/self-managed/sso/). For the **Emulator** (or Self-Managed
not using SSO), use [Method 2: Token-based
authentication](#method-2-token-based-authentication).

{{< /note >}}

#### Step 1. Get your MCP server URL

To connect, the MCP-compatible client needs the `materialize-developer` MCP
server URL: `<baseURL>/api/mcp/developer`.

{{< tabs >}}

{{< tab "Cloud" >}}

1. Log in to the [Materialize Console](https://console.materialize.com/).
1. Click the **Connect** link (lower-left corner) to open the **Connect** modal
   and click on the **MCP Server** tab.

1. In the **Connect your client** section, click on the **Developer** tab.

   You can find your `materialize-developer` MCP server URL
   `<baseURL>/api/mcp/developer` as part of the code block.

   If using Claude Code as your MCP-compatible client, you can copy the code
   block wholesale for the next step.

{{< /tab >}}

{{< tab "Self-Managed" >}}

Self-Managed deployments using OAuth require SSO, which uses TLS. Your
identity provider may also need additional configuration for MCP clients, such
as a pre-registered OAuth client if your IdP does not support anonymous
dynamic client registration. See [Connecting MCP
clients](/security/self-managed/sso/#connecting-mcp-clients).

Get your MCP server URL from the Materialize Console:

1. Log in via the Materialize Console.
1. Click the **Connect** link (lower-left corner) to open the **Connect** modal
   and click on the **MCP Server** tab.

1. In the **Connect your client** section, click on the **Developer** tab.

   You can find your `materialize-developer` MCP server URL
   `<baseURL>/api/mcp/developer` as part of the code block.

   If using Claude Code as your MCP-compatible client, you can copy the code
   block wholesale for the next step.

{{< /tab >}}
{{< /tabs >}}

#### Step 2. Configure your MCP client

Once you have your `materialize-developer` MCP server URL, you can configure
your MCP client. The `materialize-developer` MCP server URL has the form:
`<baseURL>/api/mcp/developer`.

{{< tabs >}}
{{< tab "Claude Code" >}}

1. Add the `materialize-developer` MCP server as [local-scoped
   server](https://code.claude.com/docs/en/mcp#local-scope) (i.e., the
   configurations are stored in `~/.claude.json`):

   ```sh
   claude mcp add --transport http materialize-developer \
     <baseURL>/api/mcp/developer
   ```

   {{% include-headless "/headless/mcp-endpoint-baseurl-replacements" %}}

1. Restart Claude Code. On first connection, your browser opens to complete
   sign-in and connect.

1. Upon successful connection, you can [Start asking
   questions](#start-asking-questions).

{{< /tab >}}

{{< tab "Claude Cowork/Chrome" >}}

To configure Claude Cowork/Chrome, add a custom connector. The exact steps
depend on your Claude plan; for example:

- **Organization settings** → **Connectors** → **Add** → **Custom** → **Web**,
  or
- **Customize** → **Connectors** → **+** → **Add custom connector**.

Refer to the [Add a custom
connector](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp#h_3d1a65aded)
section of the [Get started with custom connectors using Remote
MCP](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp#h_3d1a65aded)
guide to get the exact steps for your plan. For the **Remote MCP server URL**
field, enter your `materialize-developer` MCP server URL.

For additional information, including network requirements and security and
privacy concerns, see the [Get started with custom connectors using Remote
MCP](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp)
article.

{{< /tab >}}

{{< tab "Cursor" >}}

1. Add the `materialize-developer` MCP server entry to your local MCP settings
   file (`~/.cursor/mcp.json`).
   - When merging into an existing `mcpServers` object, remember to add commas
     between entries.
   - If the `mcpServers` field does not already exist, add it as well.

   ```json {hl_lines="3-5"}
   {
     "mcpServers": {
       "materialize-developer": {
         "url": "<baseURL>/api/mcp/developer"
       }
     }
   }
   ```

   {{% include-headless "/headless/mcp-endpoint-baseurl-replacements" %}}

1. Restart Cursor. On first connection, your browser opens to complete sign-in
   and connect.

1. Upon successful connection, you can [Start asking
   questions](#start-asking-questions).

{{< /tab >}}
{{< /tabs >}}

### Method 2: Token-based authentication

When connecting to the MCP server, the MCP-compatible client needs:

- The Base64-encoded credentials (i.e., the MCP token).

- The `materialize-developer` MCP server URL: `<baseURL>/api/mcp/developer`.

#### Step 1. Get your MCP token

{{< tabs >}}

{{< tab "Cloud" >}}

The MCP token is your base64-encoded credentials. Prefer using a personal app
password over encoding your account credentials as the token is only
base64-encoded and not encrypted.

1. Log in to the [Materialize Console](https://console.materialize.com/).

1. Get your base64-encoded token for your personal app.

   - If you already have an MCP token for your personal app, copy the token.
   - If you want to create a new personal app password to use, the MCP token is
     generated when you create the new app password (**Create New** → **App
     Password**). **Copy the token** as you will use the token to connect.
     Once you navigate away, the token will not display again.

   - If using an existing personal app password, manually generate the
     base64-encoded token.

     ```bash
     printf '<user>:<app_password>' | base64 -w0
     ```

{{< /tab >}}
{{< tab "Self-Managed" >}}

The MCP token is your base64-encoded credentials. Prefer using a separate role's
login credentials over encoding your own credentials as the token is only
base64-encoded and not encrypted.

1. For the MCP token, you can use either an existing or new app login role with
   password.

   - To use an existing login role with password, go to the next step.
   - To create a new login role with password:

     ```mzsql
     CREATE ROLE my_dev_agent LOGIN PASSWORD '<your_app_password>';
     ```

1. Encode your role's credentials `<role>:<password>` in Base64 to create the
   MCP token, replacing `<your_app_password>` with the actual password:

   ```bash
   printf 'my_dev_agent:<your_app_password>' | base64
   ```

{{< /tab >}}

{{< tab "Emulator" >}}

To connect to the MCP server for your Emulator, you can create a role for your
specific AI agent or use the default `materialize` user:

1. You can create a role for your specific AI agent (the Emulator does not
   support the `LOGIN PASSWORD` option):

   ```mzsql
   CREATE ROLE my_dev_agent;
   ```

1. Base64-encode your agent role's credentials `<role>:<password>` to create the
   MCP token (the Emulator does not support passwords):

   ```bash
   printf 'my_dev_agent:' | base64
   ```

{{< /tab >}}
{{< /tabs >}}

#### Step 2. Get your MCP server URL

To connect, the MCP-compatible client needs the `materialize-developer` MCP
server URL: `<baseURL>/api/mcp/developer`.

{{< tabs >}}

{{< tab "Cloud" >}}

1. Log in to the [Materialize Console](https://console.materialize.com/).
1. Click the **Connect** link (lower-left corner) to open the **Connect** modal
   and click on the **MCP Server** tab.

1. In the **Connect your client** section, click on the **Developer** tab.

   You can find your `materialize-developer` MCP server URL
   `<baseURL>/api/mcp/developer` as part of the code block.

   If using Claude Code as your MCP-compatible client, you can copy the code
   block wholesale for the next step.

{{< /tab >}}

{{< tab "Self-Managed" >}}

{{< tabs >}}
{{< tab "Deployment using TLS" >}}
**If your Self-Managed deployment is using TLS**:

1. Log in via the Materialize Console.
1. Click the **Connect** link (lower-left corner) to open the **Connect** modal
   and click on the **MCP Server** tab.

1. In the **Connect your client** section, click on the **Developer** tab.

   You can find your `materialize-developer` MCP server URL
   `<baseURL>/api/mcp/developer` as part of the code block.

   If using Claude Code as your MCP-compatible client, you can copy the code
   block wholesale for the next step.

{{< /tab >}}
{{< tab "Deployment not using TLS" >}}
**If your Self-Managed deployment is not using TLS**:

1. Find your deployment's host name to determine your `materialize-developer`
   MCP URL:

   - For your Self-Managed Materialize deployment in AWS/GCP/Azure, the hostname
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
     clusters,
     use port forwarding and `localhost` is your hostname:

     ```bash
     kubectl port-forward svc/<instance-name>-balancerd 6876:6876 -n materialize-environment
     ```

1. Determine the value of your MCP URL using your hostname:

   ```
   http://<host>:6876/api/mcp/developer
   ```

   where `http://<host>:6876` is your base URL.

{{< /tab >}}
{{< /tabs >}}
{{< /tab >}}
{{< tab "Emulator" >}}

For the Emulator, your MCP URL is:

```
http://localhost:6876/api/mcp/developer
```

where `http://localhost:6876` is your base URL.

{{< /tab >}}

{{< /tabs >}}

#### Step 3. Configure your MCP client

{{< warning >}}
When saving your credentials or other sensitive information in a config file, do
**not** commit these files to version control or share them publicly.
{{< /warning >}}

{{< tabs >}}
{{< tab "Claude Code" >}}

1. Add the `materialize-developer` MCP server as [local-scoped
   server](https://code.claude.com/docs/en/mcp#local-scope) (i.e., the
   configurations are stored in `~/.claude.json`):

   ```sh
   claude mcp add --transport http materialize-developer \
     <baseURL>/api/mcp/developer \
     --header "Authorization: Basic <mcp-token>"
   ```

   {{% include-headless "/headless/mcp-endpoint-config-replacements" %}}

1. Restart Claude Code to pick up the new setting.

1. Upon successful connection, you can [Start asking
   questions](#start-asking-questions).

{{< /tab >}}

{{< tab "Claude Cowork" >}}

Claude Cowork's `claude_desktop_config.json` does not connect to a remote MCP
server directly. Use the
[`mcp-remote`](https://www.npmjs.com/package/mcp-remote) bridge, which runs
locally and forwards requests to the `materialize-developer` MCP server over
HTTP. `mcp-remote` is invoked with `npx` and requires
[Node.js](https://nodejs.org/).

{{< note >}}
[`mcp-remote`](https://github.com/geelen/mcp-remote) is a third-party,
community-maintained tool. It is not maintained by Anthropic or Materialize.
Your MCP token is passed to it on each launch. The configuration below pins a
specific version rather than pulling the latest release. Review the tool and
update the pinned version as appropriate for your environment.
{{< /note >}}

1. Add the `materialize-developer` MCP server entry to your Claude Cowork
   configuration (`claude_desktop_config.json`).
   - When merging into an existing `mcpServers` object, remember to add commas
     between entries.
   - If the `mcpServers` field does not already exist, add it as well.

   ```json {hl_lines="3-14"}
   {
     "mcpServers": {
       "materialize-developer": {
         "command": "npx",
         "args": [
           "-y", "mcp-remote@0.1.38",
           "<baseURL>/api/mcp/developer",
           "--header", "Authorization:${AUTH_HEADER}"
         ],
         "env": {
           "AUTH_HEADER": "Basic <mcp-token>"
         }
       }
     }
   }
   ```

   The `Authorization` header value is passed through the `AUTH_HEADER`
   environment variable. This avoids a known `mcp-remote` issue where a space in
   a `--header` argument (such as the space in `Basic <mcp-token>`) is
   mishandled on some platforms. The colon in `"Authorization:${AUTH_HEADER}"`
   has no trailing space.

   {{% include-headless "/headless/mcp-endpoint-config-replacements" %}}

1. Restart Claude Cowork to pick up the new setting.

1. Upon successful connection, you can [Start asking
   questions](#start-asking-questions).

{{< /tab >}}

{{< tab "Cursor" >}}

1. Add the `materialize-developer` MCP server entry to your local MCP settings
   file (`~/.cursor/mcp.json`).
   - When merging into an existing `mcpServers` object, remember to add commas
     between entries.
   - If the `mcpServers` field does not already exist, add it as well.

   ```json {hl_lines="3-8"}
   {
     "mcpServers": {
       "materialize-developer": {
         "url": "<baseURL>/api/mcp/developer",
         "headers": {
           "Authorization": "Basic <mcp-token>"
         }
       }
     }
   }
   ```

   {{% include-headless "/headless/mcp-endpoint-config-replacements" %}}

1. Restart Cursor to pick up the new setting.

1. Upon successful connection, you can [Start asking
   questions](#start-asking-questions).

{{< /tab >}}

{{< tab "Generic HTTP" >}}

Any MCP-compatible client can connect by sending JSON-RPC 2.0 requests; update
the `<baseURL>` and `<mcp-token>` placeholders with your values:

```bash
curl -X POST <baseURL>/api/mcp/developer \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic <mcp-token>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```

{{< /tab >}}
{{< /tabs >}}

## Start asking questions

{{< tip >}}
When the agent reads your user objects with the `query` tool, an [ontology
table](/architecture-patterns/ontology/) of curated join relationships in your
schema helps it confirm exact join keys before writing multi-table SQL.
{{< /tip >}}

Once connected to the MCP server, you can ask natural language questions like:

| Question | What the agent does | Tool |
|----------|---------------------|------|
| **Why is my materialized view stale?** | Checks materialization lag, hydration status, replica health, and source errors. Optionally runs `EXPLAIN ANALYZE MEMORY` on the materialized view. | `query_system_catalog`, plus `query` if the agent needs `EXPLAIN ANALYZE` |
| **Why is my cluster running out of memory?** | Checks replica utilization, identifies the largest dataflows, and finds optimization opportunities via the built-in index advisor. | `query_system_catalog`, plus `query` for `EXPLAIN ANALYZE MEMORY` |
| **Has my source finished snapshotting yet?** | Checks source statistics and status. | `query_system_catalog` |
| **How much memory is my cluster using?** | Checks replica utilization metrics across all clusters. | `query_system_catalog` |
| **What's the health of my environment?** | Checks replica statuses, source and sink health, and resource utilization. | `query_system_catalog` |
| **What can I optimize to save costs?** | Queries the index advisor for materialized views that can be dematerialized and indexes that can be dropped. | `query_system_catalog` |
| **Using the `quickstart` cluster, examine the memory usage of `my_mat_view` with skew.** | Runs `EXPLAIN ANALYZE MEMORY WITH SKEW` on the materialized view to report its memory usage and highlight data skew across workers. | `query` for `EXPLAIN ANALYZE MEMORY WITH SKEW` |

The agent picks the appropriate tool for each question. Most catalog lookups run
on the catalog server cluster via
[`query_system_catalog`](/integrations/mcp-server/mcp-developer-tools/#query_system_catalog);
[`query`](/integrations/mcp-server/mcp-developer-tools/#query) (available
starting in v26.30) is used when the question needs a specific cluster (for
example, `EXPLAIN ANALYZE` against a materialized view or index, or reading user
objects).

## Privileges

The privileges required to use the `materialize-developer` MCP server are:

* `USAGE` on system catalog schemas and `SELECT` on system catalog objects.
  These privileges are granted by default.

* If agents also need access to replica-specific metrics from
  `mz_introspection`, `USAGE` privileges on the corresponding cluster.

## Related pages

- [Use an ontology table](/architecture-patterns/ontology/)
- [`materialize-developer` MCP Server available
  tools](/integrations/mcp-server/mcp-developer-tools/)
- [`materialize-developer` MCP Server
  configuration](/integrations/mcp-server/mcp-developer-config/)
- [Troubleshooting](/integrations/mcp-server/mcp-server-troubleshooting/)
- [Agent Skills](/integrations/coding-agent-skills/)
- [Model Context Protocol (MCP)](https://modelcontextprotocol.io/)
