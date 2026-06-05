---
title: "MCP Servers and agent skills"
description: "This section contains guides for installing Materialize Agent skills and integrating with Materialize's built-in MCP servers."
disable_list: true
menu:
  main:
    parent: integrations
    name: "MCP Server and skills"
    identifier: mcp-server
    weight: 6
---


## Agent skills

Materialize provides the following open-source [agent
skills](https://github.com/MaterializeInc/agent-skills) to help developers build
with Materialize.

{{% include-headless "/headless/agent-skills-table" %}}

## MCP servers

Materialize provides built-in Model Context Protocol (MCP) servers that AI
agents can use. The MCP interface is served directly by the database; no sidecar
process or external server is required. These endpoints use [JSON-RPC
 2.0](https://www.jsonrpc.org/specification) over HTTP POST (default port 6876)
and support the MCP `initialize`, `tools/list`, and `tools/call` methods.

{{% include-headless "/headless/mcp-servers-table" %}}

## See also

- [MCP Server
  Troubleshooting](/integrations/mcp-server/mcp-server-troubleshooting/)
- [Appendix: MCP Server (Python)](/integrations/mcp-server/llm) for locally-run,
  separate MCP Server.
