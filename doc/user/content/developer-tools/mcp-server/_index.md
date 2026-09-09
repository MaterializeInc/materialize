---
title: "MCP Servers and agent skills"
description: "This section contains guides for installing Materialize Agent skills and integrating with Materialize's built-in MCP servers."
disable_list: true
menu:
  main:
    name: "Setup agents"
    identifier: mcp-server
    parent: developer-tools
    weight: 10
aliases:
  - /integrations/mcp-server/llm/
  - /integrations/llm/
  - /integrations/mcp-server/
---


## Agent skills

Materialize provides open-source [agent
skills](https://github.com/MaterializeInc/agent-skills) that give coding agents
like Claude Code, Codex, and Cursor access to Materialize documentation and
reference material. For the list of available skills and installation
instructions, see [Agent Skills](/developer-tools/mcp-server/coding-agent-skills/).

## MCP servers

Materialize provides built-in Model Context Protocol (MCP) servers that AI
agents can use. The MCP interface is served directly by the database; no sidecar
process or external server is required. These endpoints use [JSON-RPC
 2.0](https://www.jsonrpc.org/specification) over HTTP POST (default port 6876)
and support the MCP `initialize`, `tools/list`, and `tools/call` methods.

{{% include-headless "/headless/mcp-servers-table" %}}

## See also

- [Use an ontology table](/transform-data/patterns/ontology/) to curate join
  relationships that agents query through the `query` tool before writing
  multi-table SQL.
- [MCP Server
  Troubleshooting](/developer-tools/mcp-server/mcp-server-troubleshooting/)
