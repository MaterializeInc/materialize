---
title: "Materialize MCP server"
description: "Connect AI agents to Materialize through the built-in Model Context Protocol (MCP) server. One URL for your whole organization, governed by the roles and grants you already have."
disable_list: true
menu:
  main:
    name: "Set up agents"
    identifier: mcp-server
    parent: developer-tools
    weight: 10
aliases:
  - /integrations/mcp-server/llm/
  - /integrations/llm/
  - /integrations/mcp-server/
  - /integrations/mcp-server/mcp-agent/
  - /integrations/mcp-server/mcp-developer/
  - /developer-tools/mcp-server/mcp-agent/
  - /developer-tools/mcp-server/mcp-developer/
---

{{< public-preview />}}

{{< note >}}
**$TODO: Figure out ahead of launch.** Confirm the release version in which the
unified `/api/mcp` endpoint ships and add an *Available starting in vX.Y*
marker here and on each page in this section.
{{< /note >}}

Materialize provides a built-in [Model Context Protocol
(MCP)](https://modelcontextprotocol.io/) server that AI agents use to discover
and query your data and to inspect the health of your deployment. The server is
served directly by the database over HTTP. There is no sidecar process or
external server to run, and there is nothing to install per user.

An administrator turns Materialize on once for the whole organization. Every
user then sees Materialize in their AI client, connects with their own identity,
and gets exactly the access their database role already has.

## One URL for your organization

The MCP server is served at a single path, `/api/mcp`, on the same host your
Materialize Console uses:

{{% include-headless "/headless/mcp-servers-table" %}}

The same URL serves every user. What each user can do is decided by their
database role, not by which URL they connect to. See [MCP access
controls](/developer-tools/mcp-server/access-control/).

## What agents can do

The MCP server exposes a small set of read-only tools:

| Tool | What it does |
|------|--------------|
| [`get_permissions`](/developer-tools/mcp-server/tools/#get_permissions) | Reports the privileges the connected role holds, so the agent knows what it may access. |
| [`get_settings`](/developer-tools/mcp-server/tools/#get_settings) | Reports the session settings that change what the role can do, such as `restrict_to_user_objects` and the default cluster. |
| [`list_data_products`](/developer-tools/mcp-server/tools/#list_data_products) | Lists the curated data products the role can read. |
| [`get_data_product_details`](/developer-tools/mcp-server/tools/#get_data_product_details) | Returns a data product's columns, types, comments, and lookup keys. |
| [`query`](/developer-tools/mcp-server/tools/#query) | Runs a read-only `SELECT`, `SHOW`, or `EXPLAIN` on any object the role can read, on a cluster the role can use. |
| [`query_system_tables`](/developer-tools/mcp-server/tools/#query_system_tables) | Runs a read-only query against the system catalog for troubleshooting and observability. |

All tools are read-only. Write statements are rejected regardless of the role's
grants. For parameters, response shapes, and examples, see [Available
tools](/developer-tools/mcp-server/tools/).

## How access is governed

There is no separate permission system for MCP. Every tool call runs as the
authenticated user's database role and is checked against the same
[role-based access control](/security/) grants that govern SQL and the Console:

- `SELECT` on the objects the role may read.
- `USAGE` on the schemas that contain them and on the cluster the query runs on.
- Optionally, `restrict_to_user_objects` on the role, which confines it to user
  data and hides the system catalog from it.

The agent can ask the server what the role is allowed to do through
`get_permissions` and `get_settings`, so it learns up front what it may access
and why a call was refused. See [MCP access
controls](/developer-tools/mcp-server/access-control/).

## Get started

{{< multilinkbox >}}
{{< linkbox title="For administrators" >}}
- [Set up MCP for your organization on Cloud](/developer-tools/mcp-server/setup-cloud/)
- [Set up MCP for your organization on Self-Managed](/developer-tools/mcp-server/setup-self-managed/)
- [MCP access controls](/developer-tools/mcp-server/access-control/)
{{</ linkbox >}}

{{< linkbox title="For agent builders" >}}
- [Available tools](/developer-tools/mcp-server/tools/)
- [Install agent skills](/developer-tools/mcp-server/coding-agent-skills/)
- [Curate join relationships with an ontology table](/transform-data/patterns/ontology/)
{{</ linkbox >}}

{{< linkbox title="Reference" >}}
- [Troubleshoot the MCP server](/developer-tools/mcp-server/mcp-server-troubleshooting/)
- [Model Context Protocol specification](https://modelcontextprotocol.io/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Agent skills

Materialize also provides open-source [agent
skills](https://github.com/MaterializeInc/agent-skills) that give coding agents
like Claude Code, Codex, and Cursor access to Materialize documentation and
reference material. Skills complement the MCP server: the MCP server gives an
agent access to your data and deployment, and skills teach it how to work with
Materialize. See [Agent skills](/developer-tools/mcp-server/coding-agent-skills/).

## Legacy endpoints

Before the unified endpoint, Materialize served two MCP endpoints,
`/api/mcp/agent` and `/api/mcp/developer`, with different tool lists. Both
continue to work and are served by the same implementation as `/api/mcp`:

- Clients connected to a legacy endpoint keep seeing the tools they see today,
  under the names they use today. No client change is required.
- The legacy tool names `get_data_products` and `query_system_catalog` are
  accepted as aliases for `list_data_products` and `query_system_tables` on
  every endpoint.
- New connections should use `/api/mcp`. The Console and these docs only refer
  to the unified endpoint.

{{< note >}}
**$TODO: Figure out ahead of launch.** Confirm the deprecation timeline for the
legacy endpoints and for the `read_data_product` tool, and whether the legacy
endpoints announce the new URL in their `initialize` instructions.
{{< /note >}}

For the full mapping of legacy names, see [Legacy tool
names](/developer-tools/mcp-server/tools/#legacy-tool-names).
